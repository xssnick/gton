package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

var (
	errArchivePeerScoutDeferred   = errors.New("archive peer scout deferred")
	errArchivePeerInvalidResponse = errors.New("archive peer returned invalid data")
)

type archivePeerEvidence uint8

const (
	archivePeerEvidenceUnknown archivePeerEvidence = iota
	archivePeerEvidenceAvailable
	archivePeerEvidenceProven
)

type archivePeerProbeResult struct {
	probe          archivePeerProbe
	evidence       archivePeerEvidence
	at             time.Time
	bytes          int64
	elapsed        time.Duration
	bytesPerSecond float64
}

type archivePeerOffer struct {
	node     overlay.Node
	hasNode  bool
	identity overlayNodeIdentity
	endpoint string
	peer     *overlayPeer
	valuable bool
}

type archivePeerOfferStatus uint8

const (
	archivePeerOfferUnknown archivePeerOfferStatus = iota
	archivePeerOfferQueued
	archivePeerOfferKnown
	archivePeerOfferBackoff
	archivePeerOfferDuplicate
	archivePeerOfferQueueFull
	archivePeerOfferInvalid
)

type archivePeerAdmissionResult struct {
	admitted    bool
	replaced    bool
	stale       bool
	deferred    bool
	evicted     *overlayPeer
	roster      int
	freshProven int
}

type archiveScoutCounters struct {
	attempts                      atomic.Uint64
	proven                        atomic.Uint64
	available                     atomic.Uint64
	admitted                      atomic.Uint64
	replaced                      atomic.Uint64
	notAvailable                  atomic.Uint64
	transportFailure              atomic.Uint64
	busy                          atomic.Uint64
	invalid                       atomic.Uint64
	noBenefit                     atomic.Uint64
	transientRandomQueries        atomic.Uint64
	transientRandomResponses      atomic.Uint64
	transientRandomReceivedNodes  atomic.Uint64
	transientRandomProcessedNodes atomic.Uint64
	transientRandomQueued         atomic.Uint64
}

type archiveScoutCounterSnapshot struct {
	attempts                      uint64
	proven                        uint64
	available                     uint64
	admitted                      uint64
	replaced                      uint64
	notAvailable                  uint64
	transportFailure              uint64
	busy                          uint64
	invalid                       uint64
	noBenefit                     uint64
	transientRandomQueries        uint64
	transientRandomResponses      uint64
	transientRandomReceivedNodes  uint64
	transientRandomProcessedNodes uint64
	transientRandomQueued         uint64
}

func (c *archiveScoutCounters) snapshot() archiveScoutCounterSnapshot {
	return archiveScoutCounterSnapshot{
		attempts:                      c.attempts.Load(),
		proven:                        c.proven.Load(),
		available:                     c.available.Load(),
		admitted:                      c.admitted.Load(),
		replaced:                      c.replaced.Load(),
		notAvailable:                  c.notAvailable.Load(),
		transportFailure:              c.transportFailure.Load(),
		busy:                          c.busy.Load(),
		invalid:                       c.invalid.Load(),
		noBenefit:                     c.noBenefit.Load(),
		transientRandomQueries:        c.transientRandomQueries.Load(),
		transientRandomResponses:      c.transientRandomResponses.Load(),
		transientRandomReceivedNodes:  c.transientRandomReceivedNodes.Load(),
		transientRandomProcessedNodes: c.transientRandomProcessedNodes.Load(),
		transientRandomQueued:         c.transientRandomQueued.Load(),
	}
}

type archiveScoutShared struct {
	slots chan struct{}
	retry archivePeerRetryCache
}

type archivePeerRetryCache struct {
	mx        sync.Mutex
	peers     map[PeerID]time.Time
	endpoints map[string]time.Time
	inflight  map[PeerID]struct{}
}

func newArchiveScoutShared() *archiveScoutShared {
	return &archiveScoutShared{
		slots: make(chan struct{}, archiveConcurrentScoutLimit),
		retry: archivePeerRetryCache{
			peers:     map[PeerID]time.Time{},
			endpoints: map[string]time.Time{},
			inflight:  map[PeerID]struct{}{},
		},
	}
}

func (s *archiveScoutShared) acquire(ctx context.Context, peerID PeerID) (func(), error) {
	if !s.retry.begin(peerID, time.Now()) {
		return nil, errArchivePeerScoutDeferred
	}

	select {
	case s.slots <- struct{}{}:
		return func() {
			<-s.slots
			s.retry.finish(peerID)
		}, nil
	case <-ctx.Done():
		s.retry.finish(peerID)
		return nil, ctx.Err()
	}
}

func (s *archiveScoutShared) peerBlocked(peerID PeerID, now time.Time) bool {
	return s.retry.peerBlocked(peerID, now)
}

func (s *archiveScoutShared) endpointBlocked(endpoint string, now time.Time) bool {
	return s.retry.endpointBlocked(endpoint, now)
}

func (s *archiveScoutShared) rememberPeer(peerID PeerID, ttl time.Duration) {
	s.retry.rememberPeer(peerID, time.Now().Add(ttl))
}

func (s *archiveScoutShared) rememberEndpoint(endpoint string, ttl time.Duration) {
	s.retry.rememberEndpoint(endpoint, time.Now().Add(ttl))
}

func (s *archiveScoutShared) clearPeer(peerID PeerID) {
	s.retry.clearPeer(peerID)
}

func (s *archiveScoutShared) clearEndpoint(endpoint string) {
	s.retry.clearEndpoint(endpoint)
}

func (c *archivePeerRetryCache) begin(peerID PeerID, now time.Time) bool {
	c.mx.Lock()
	defer c.mx.Unlock()

	if until, ok := c.peers[peerID]; ok {
		if now.Before(until) {
			return false
		}
		delete(c.peers, peerID)
	}
	if _, ok := c.inflight[peerID]; ok {
		return false
	}
	c.inflight[peerID] = struct{}{}
	return true
}

func (c *archivePeerRetryCache) finish(peerID PeerID) {
	c.mx.Lock()
	delete(c.inflight, peerID)
	c.mx.Unlock()
}

func (c *archivePeerRetryCache) peerBlocked(peerID PeerID, now time.Time) bool {
	c.mx.Lock()
	defer c.mx.Unlock()

	until, ok := c.peers[peerID]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(c.peers, peerID)
	return false
}

func (c *archivePeerRetryCache) endpointBlocked(endpoint string, now time.Time) bool {
	c.mx.Lock()
	defer c.mx.Unlock()

	until, ok := c.endpoints[endpoint]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(c.endpoints, endpoint)
	return false
}

func (c *archivePeerRetryCache) rememberPeer(peerID PeerID, until time.Time) {
	c.mx.Lock()
	if current := c.peers[peerID]; !current.IsZero() {
		if current.Before(until) {
			c.peers[peerID] = until
		}
		c.mx.Unlock()
		return
	}
	archiveBoundPeerRetryCache(c.peers, time.Now(), archivePeerRetryCacheLimit)
	c.peers[peerID] = until
	c.mx.Unlock()
}

func (c *archivePeerRetryCache) rememberEndpoint(endpoint string, until time.Time) {
	c.mx.Lock()
	if current := c.endpoints[endpoint]; !current.IsZero() {
		if current.Before(until) {
			c.endpoints[endpoint] = until
		}
		c.mx.Unlock()
		return
	}
	archiveBoundEndpointRetryCache(c.endpoints, time.Now(), archivePeerRetryCacheLimit)
	c.endpoints[endpoint] = until
	c.mx.Unlock()
}

func (c *archivePeerRetryCache) clearPeer(peerID PeerID) {
	c.mx.Lock()
	delete(c.peers, peerID)
	c.mx.Unlock()
}

func (c *archivePeerRetryCache) clearEndpoint(endpoint string) {
	c.mx.Lock()
	delete(c.endpoints, endpoint)
	c.mx.Unlock()
}

func archiveBoundPeerRetryCache(entries map[PeerID]time.Time, now time.Time, limit int) {
	if len(entries) < limit {
		return
	}

	var oldestID PeerID
	var oldest time.Time
	for peerID, until := range entries {
		if !now.Before(until) {
			delete(entries, peerID)
			continue
		}
		if oldest.IsZero() || until.Before(oldest) {
			oldestID = peerID
			oldest = until
		}
	}
	if len(entries) >= limit && !oldestID.IsZero() {
		delete(entries, oldestID)
	}
}

func archiveBoundEndpointRetryCache(entries map[string]time.Time, now time.Time, limit int) {
	if len(entries) < limit {
		return
	}

	oldestEndpoint := ""
	var oldest time.Time
	for endpoint, until := range entries {
		if !now.Before(until) {
			delete(entries, endpoint)
			continue
		}
		if oldest.IsZero() || until.Before(oldest) {
			oldestEndpoint = endpoint
			oldest = until
		}
	}
	if len(entries) >= limit && oldestEndpoint != "" {
		delete(entries, oldestEndpoint)
	}
}

func (p *archivePeerPool) startScoutWorkers() {
	for range archivePeerScoutWorkers {
		p.scoutWorkers.Go(p.runScoutWorker)
	}
}

func (p *archivePeerPool) runScoutWorker() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case offer := <-p.offers:
			p.scoutArchiveOffer(offer)
			p.finishArchivePeerScout(offer.identity.peerID)
		}
	}
}

func (p *archivePeerPool) offerArchiveNode(node overlay.Node) archivePeerOfferStatus {
	if p.ctx.Err() != nil || !p.sub.isActive() {
		return archivePeerOfferInvalid
	}

	identity, err := p.sub.overlayNodeIdentity(node)
	if err != nil || identity.self || identity.peerID.IsZero() {
		return archivePeerOfferInvalid
	}
	endpoint := ""
	if live := p.sub.peerByID(identity.peerID); live != nil {
		endpoint = live.addr
	}
	return p.offerArchiveIdentity(&node, identity, endpoint)
}

func (p *archivePeerPool) offerArchiveLivePeer(peer *overlayPeer) archivePeerOfferStatus {
	if peer == nil || peer.id.IsZero() || peer.addr == "" || len(peer.pub) != ed25519.PublicKeySize {
		return archivePeerOfferInvalid
	}
	node := peer.advertisedNodeSnapshot(time.Now())
	if node == nil {
		return archivePeerOfferInvalid
	}
	return p.offerArchiveIdentity(node, overlayNodeIdentity{
		pub:    append(ed25519.PublicKey(nil), peer.pub...),
		peerID: peer.id,
	}, peer.addr)
}

func (p *archivePeerPool) offerArchiveIdentity(node *overlay.Node, identity overlayNodeIdentity, endpoint string) archivePeerOfferStatus {
	now := time.Now()
	if p.scout.peerBlocked(identity.peerID, now) {
		return archivePeerOfferBackoff
	}

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return archivePeerOfferInvalid
	}
	entry := p.peers[identity.peerID]
	archiveProbeBlocked := len(p.demands) == 0 || p.recentlyRejectedLocked(identity.peerID, now) || p.peerCoveredOrBlockedForAllDemandsLocked(identity.peerID, now)
	if archiveProbeBlocked {
		var peer *overlayPeer
		if entry != nil {
			peer = entry.peer
		}
		relayDue := p.sub.spec.RandomPeers && peer == nil && !now.Before(p.randomExpandedUntil[identity.peerID])
		if !relayDue {
			p.mx.Unlock()
			if peer != nil {
				peer.mergeAnnouncement(node)
				p.sub.node.runAsync(func() {
					p.exchangeTransientRandomPeers(p.ctx, peer)
				})
			}
			return archivePeerOfferBackoff
		}
		// Lack of a current archive probe or archive availability backoff must
		// not hide a disconnected peer from the recursive getRandomPeers crawl.
		// Reconnect it as a relay; the scout still skips archive probes.
	}
	if _, ok := p.scouting[identity.peerID]; ok {
		p.mx.Unlock()
		return archivePeerOfferDuplicate
	}
	p.scouting[identity.peerID] = struct{}{}
	offer := archivePeerOffer{identity: identity, endpoint: endpoint}
	if node != nil {
		offer.node = *node
		offer.hasNode = true
	}
	if entry != nil {
		offer.peer = entry.peer
	}
	p.mx.Unlock()
	if offer.peer != nil && node != nil {
		offer.peer.mergeAnnouncement(node)
	}

	select {
	case p.offers <- offer:
		return archivePeerOfferQueued
	case <-p.ctx.Done():
		p.finishArchivePeerScout(identity.peerID)
		return archivePeerOfferInvalid
	default:
		p.finishArchivePeerScout(identity.peerID)
		return archivePeerOfferQueueFull
	}
}

func (p *archivePeerPool) offerArchiveValuable(valuable archiveValuablePeer) archivePeerOfferStatus {
	if p.ctx.Err() != nil || valuable.peerID.IsZero() || valuable.endpoint == "" || len(valuable.pub) != ed25519.PublicKeySize {
		return archivePeerOfferInvalid
	}

	p.scout.clearPeer(valuable.peerID)
	p.mx.Lock()
	if p.closed || p.peers[valuable.peerID] != nil || len(p.demands) == 0 {
		p.mx.Unlock()
		return archivePeerOfferKnown
	}
	if _, ok := p.scouting[valuable.peerID]; ok {
		p.mx.Unlock()
		return archivePeerOfferDuplicate
	}
	p.scouting[valuable.peerID] = struct{}{}
	p.mx.Unlock()

	offer := archivePeerOffer{
		identity: overlayNodeIdentity{
			pub:    append(ed25519.PublicKey(nil), valuable.pub...),
			peerID: valuable.peerID,
		},
		endpoint: valuable.endpoint,
		valuable: true,
	}
	select {
	case p.offers <- offer:
		return archivePeerOfferQueued
	case <-p.ctx.Done():
		p.finishArchivePeerScout(valuable.peerID)
		return archivePeerOfferInvalid
	default:
		p.finishArchivePeerScout(valuable.peerID)
		return archivePeerOfferQueueFull
	}
}

func (p *archivePeerPool) peerCoveredOrBlockedForAllDemandsLocked(peerID PeerID, now time.Time) bool {
	for _, demand := range p.demands {
		state := demand.peers[peerID]
		if state.evidence >= archivePeerDemandAvailable || p.demandRetryBlockedLocked(demand.key, peerID, now) || now.Before(state.rejectedUntil) || now.Before(state.noBenefitUntil) {
			continue
		}
		return false
	}
	return true
}

func (p *archivePeerPool) finishArchivePeerScout(peerID PeerID) {
	p.mx.Lock()
	delete(p.scouting, peerID)
	p.mx.Unlock()
}

func (p *archivePeerPool) scoutingSize() int {
	p.mx.Lock()
	defer p.mx.Unlock()

	return len(p.scouting)
}

func (p *archivePeerPool) scoutArchiveOffer(offer archivePeerOffer) {
	release, err := p.scout.acquire(p.ctx, offer.identity.peerID)
	if err != nil {
		return
	}
	defer release()
	p.scoutStats.attempts.Add(1)

	if offer.peer != nil {
		p.scoutExistingArchivePeer(offer.peer)
		return
	}
	p.scoutTransientArchivePeer(offer)
}

func (p *archivePeerPool) scoutExistingArchivePeer(peer *overlayPeer) {
	var randomPeers sync.WaitGroup
	randomPeers.Go(func() {
		p.exchangeTransientRandomPeers(p.ctx, peer)
	})
	defer randomPeers.Wait()
	if p.transportBlocked(peer.id, time.Now()) {
		return
	}
	if p.recentlyRejected(peer.id, time.Now()) {
		return
	}

	release, ok := p.acquireQuery(peer)
	if !ok {
		return
	}

	probes := p.probeSnapshots(4)
	for _, probe := range probes {
		if p.demandPeerBlocked(probe, peer.id, time.Now()) {
			continue
		}
		result, err := p.probeArchivePeerEvidence(p.ctx, peer, probe)
		if err != nil {
			if errors.Is(err, archive.ErrNotAvailable) || errors.Is(err, ErrStateNotAvailable) {
				p.scoutStats.notAvailable.Add(1)
				p.recordDemandNotAvailable(probe, peer.id, archivePeerNotAvailableTTL)
				continue
			}
			release()
			p.noteScoutFailure(probe, peer, err)
			return
		}
		p.applyArchivePeerEvidence(peer, result)
		release()
		return
	}
	release()
}

func (p *archivePeerPool) scoutTransientArchivePeer(offer archivePeerOffer) {
	endpoint := offer.endpoint
	if endpoint == "" {
		addressCtx, cancel := context.WithTimeout(p.ctx, archiveDHTAddressTimeout)
		addrList, _, err := findPeerAddresses(addressCtx, p.sub.node.dht, offer.identity.peerID[:])
		cancel()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				p.scout.rememberPeer(offer.identity.peerID, archivePeerUnreachableTTL)
				p.scoutStats.transportFailure.Add(1)
			}
			return
		}
		if len(addrList.Addresses) == 0 {
			p.scout.rememberPeer(offer.identity.peerID, archivePeerUnreachableTTL)
			p.scoutStats.transportFailure.Add(1)
			return
		}

		// A PeerID has one canonical endpoint. Do not spend discovery time trying
		// alternate addresses for the same transport identity.
		endpoint, err = firstPeerEndpoint(addrList.Addresses)
		if err != nil {
			p.scout.rememberPeer(offer.identity.peerID, archivePeerUnreachableTTL)
			p.scoutStats.transportFailure.Add(1)
			return
		}
	}

	probes := p.probeSnapshots(4)
	if p.scout.endpointBlocked(endpoint, time.Now()) {
		p.scout.rememberPeer(offer.identity.peerID, archivePeerUnreachableTTL)
		p.scoutStats.transportFailure.Add(1)
		return
	}

	pooled, releaseEndpoint, err := p.sub.node.acquireArchivePeerEndpoint(offer.identity.peerID, endpoint, offer.identity.pub)
	if err != nil {
		if errors.Is(err, errPeerEndpointBusy) {
			p.scoutStats.busy.Add(1)
			return
		}
		p.scout.rememberEndpoint(endpoint, archivePeerUnreachableTTL)
		p.scout.rememberPeer(offer.identity.peerID, archivePeerUnreachableTTL)
		p.scoutStats.transportFailure.Add(1)
		return
	}
	endpoint = pooled.addr
	var announced *overlay.Node
	if offer.hasNode && !offer.valuable {
		announced = &offer.node
	}
	peer := p.sub.newOverlayPeer(pooled, announced, false, p.sub.spec.Kind != overlayKindCustomFixed)
	peer.addr = endpoint
	admitted := p.scoutConnectedTransientArchivePeer(peer, probes, offer.valuable)
	if !admitted {
		closeArchiveOnlyPeer(peer)
	}
	releaseEndpoint()
}

func (p *archivePeerPool) scoutConnectedTransientArchivePeer(peer *overlayPeer, probes []archivePeerProbe, forceProbe bool) bool {
	var randomPeers sync.WaitGroup
	var randomReachable bool
	randomPeers.Go(func() {
		randomReachable = p.exchangeTransientRandomPeers(p.ctx, peer)
	})
	var waitRandomPeersOnce sync.Once
	waitRandomPeers := func() bool {
		waitRandomPeersOnce.Do(randomPeers.Wait)
		return randomReachable
	}

	transportError := false
	defer func() {
		reachable := waitRandomPeers()
		if !transportError {
			return
		}

		p.scoutStats.transportFailure.Add(1)
		if reachable {
			p.rememberTransportBlocked(peer.id, archivePeerUnreachableTTL)
			return
		}
		p.scout.rememberEndpoint(peer.addr, archivePeerUnreachableTTL)
		p.scout.rememberPeer(peer.id, archivePeerUnreachableTTL)
	}()
	if p.transportBlocked(peer.id, time.Now()) {
		return false
	}
	archiveRejected := p.recentlyRejected(peer.id, time.Now())

	for _, probe := range probes {
		if !forceProbe && (archiveRejected || p.demandPeerBlocked(probe, peer.id, time.Now())) {
			continue
		}
		result, err := p.probeArchivePeerEvidence(p.ctx, peer, probe)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return false
			}
			if errors.Is(err, archive.ErrNotAvailable) || errors.Is(err, ErrStateNotAvailable) {
				p.scoutStats.notAvailable.Add(1)
				p.recordDemandNotAvailable(probe, peer.id, archivePeerNotAvailableTTL)
				continue
			}
			if errors.Is(err, errArchivePeerInvalidResponse) {
				p.scoutStats.invalid.Add(1)
				p.recordDemandNoBenefit(probe, peer.id, archivePeerNoBenefitTTL)
				return false
			}
			transportError = true
			break
		}

		p.scout.clearEndpoint(peer.addr)
		waitRandomPeers()
		admission := p.admitArchiveOnlyPeer(peer, result)
		if admission.evicted != nil {
			closeArchiveOnlyPeer(admission.evicted)
		}
		if !admission.admitted {
			if admission.stale {
				continue
			}
			if admission.deferred {
				p.scoutStats.busy.Add(1)
				continue
			}
			p.scoutStats.noBenefit.Add(1)
			p.recordDemandNoBenefit(result.probe, peer.id, archivePeerNoBenefitTTL)
			continue
		}

		p.noteArchivePeerEvidence(peer, result)
		p.logArchivePeerAdmission(peer, result, admission)
		p.scoutStats.admitted.Add(1)
		if admission.replaced {
			p.scoutStats.replaced.Add(1)
		}
		return true
	}
	return false
}

func (p *archivePeerPool) exchangeTransientRandomPeers(ctx context.Context, peer *overlayPeer) bool {
	if !p.sub.isActive() || !p.sub.spec.RandomPeers || peer.overlay == nil {
		return false
	}

	advertised, err := p.sub.randomPeerAdvertisement()
	if err != nil {
		p.log.Debug().Err(err).Msg("failed to create self overlay node")
		return false
	}
	if ctx.Err() != nil || !p.reserveTransientRandomExpansion(peer.id, time.Now()) {
		return false
	}

	queryCtx, cancel := context.WithTimeout(ctx, archiveRandomPeerQueryTimeout)
	defer cancel()

	p.scoutStats.transientRandomQueries.Add(1)
	var res overlay.NodesList
	if err = peer.overlay.Query(queryCtx, overlay.GetRandomPeers{List: advertised}, &res); err != nil {
		p.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("transient archive overlay.getRandomPeers failed")
		return false
	}

	nodes := res.List
	if len(nodes) > archiveTransientRandomReplyLimit {
		nodes = nodes[:archiveTransientRandomReplyLimit]
	}
	p.scoutStats.transientRandomResponses.Add(1)
	p.scoutStats.transientRandomReceivedNodes.Add(uint64(len(res.List)))
	p.scoutStats.transientRandomProcessedNodes.Add(uint64(len(nodes)))
	queued := 0
	for _, node := range nodes {
		if ctx.Err() != nil {
			break
		}
		if p.offerArchiveNode(node) == archivePeerOfferQueued {
			queued++
		}
	}
	p.scoutStats.transientRandomQueued.Add(uint64(queued))
	p.log.Debug().
		Str("peer", peer.addr).
		Int("received_nodes", len(res.List)).
		Int("processed_nodes", len(nodes)).
		Int("queued", queued).
		Msg("transient archive random peers received")
	return true
}

func (p *archivePeerPool) expandArchiveRandomPeers(ctx context.Context, limit int) {
	if limit <= 0 || ctx.Err() != nil {
		return
	}

	now := time.Now()
	p.mx.Lock()
	peers := make([]*overlayPeer, 0, limit)
	for peerID, entry := range p.peers {
		if len(peers) == limit {
			break
		}
		if entry == nil || entry.peer == nil || !entry.peer.hasOpenConnection() || now.Before(p.randomExpandedUntil[peerID]) {
			continue
		}
		if _, scouting := p.scouting[peerID]; scouting {
			continue
		}
		peers = append(peers, entry.peer)
	}
	p.mx.Unlock()

	for _, peer := range peers {
		p.sub.node.runAsync(func() {
			p.exchangeTransientRandomPeers(p.ctx, peer)
		})
	}
}

func (p *archivePeerPool) reserveTransientRandomExpansion(peerID PeerID, now time.Time) bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.closed || peerID.IsZero() {
		return false
	}

	if until := p.randomExpandedUntil[peerID]; now.Before(until) {
		return false
	}
	delete(p.randomExpandedUntil, peerID)

	archiveBoundPeerRetryCache(p.randomExpandedUntil, now, archivePeerLocalRetryCacheLimit)
	p.randomExpandedUntil[peerID] = now.Add(archiveTransientRandomExpansionTTL)
	return true
}

func (p *archivePeerPool) probeArchivePeerEvidence(ctx context.Context, peer *overlayPeer, probe archivePeerProbe) (archivePeerProbeResult, error) {
	result := archivePeerProbeResult{probe: probe}
	if probe.zeroState {
		resp, err := p.sub.queryArchiveFromPeerWithLimits(ctx, peer, PrepareZeroState{Block: probe.block}, archiveInfoTimeout, persistentStateSmallAnswerMax)
		if err != nil {
			return archivePeerProbeResult{}, err
		}
		switch resp.(type) {
		case PreparedState:
			result.at = time.Now()
			result.evidence = archivePeerEvidenceAvailable
			return result, nil
		case NotFoundState:
			return archivePeerProbeResult{}, ErrStateNotAvailable
		default:
			return archivePeerProbeResult{}, fmt.Errorf("%w: unexpected prepare zero state response %T", errArchivePeerInvalidResponse, resp)
		}
	}

	info, err := p.sub.queryArchiveInfo(ctx, peer, probe.seqno, probe.shard, archiveInfoTimeout)
	if err != nil {
		return archivePeerProbeResult{}, err
	}
	startedAt := time.Now()
	data, err := p.sub.queryArchiveSliceWithTimeout(
		ctx,
		peer,
		info.ID,
		0,
		archiveSliceProbeSize,
		archiveSliceProbeTimeout,
	)
	if err != nil {
		return archivePeerProbeResult{}, fmt.Errorf("probe archive slice: %w", err)
	}
	if len(data) == 0 {
		return archivePeerProbeResult{}, fmt.Errorf("probe archive slice returned no data: %w", archive.ErrNotAvailable)
	}
	if len(data) > archiveSliceProbeSize {
		return archivePeerProbeResult{}, fmt.Errorf("%w: archive probe response too large: %d", errArchivePeerInvalidResponse, len(data))
	}
	if err = checkArchivePackMagic(data); err != nil {
		return archivePeerProbeResult{}, fmt.Errorf("%w: %v", errArchivePeerInvalidResponse, err)
	}

	result.at = time.Now()
	result.evidence = archivePeerEvidenceProven
	result.bytes = int64(len(data))
	result.elapsed = time.Since(startedAt)
	if archiveSpeedSampleReliable(result.bytes) && result.elapsed > 0 {
		result.bytesPerSecond = float64(result.bytes) / result.elapsed.Seconds()
	}
	return result, nil
}

func (p *archivePeerPool) noteArchivePeerEvidence(peer *overlayPeer, result archivePeerProbeResult) {
	if result.evidence == archivePeerEvidenceProven {
		p.scoutStats.proven.Add(1)
		p.noteArchiveSeedSuccess(result.probe.shard, peer, result.bytes, result.elapsed)
		return
	}
	if result.evidence == archivePeerEvidenceAvailable {
		p.scoutStats.available.Add(1)
	}
}

func (p *archivePeerPool) applyArchivePeerEvidence(peer *overlayPeer, result archivePeerProbeResult) bool {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return false
	}

	now := result.at
	if now.IsZero() {
		now = time.Now()
	}
	stateKey := archivePeerPoolKey(result.probe.shard)
	p.mx.Lock()
	entry := p.peers[peerID]
	demand := p.demands[result.probe.demandID]
	if p.closed || demand == nil || entry == nil || entry.peer != peer {
		p.mx.Unlock()
		return false
	}
	health := p.shardPeerLocked(stateKey, peerID)
	if result.evidence == archivePeerEvidenceProven {
		health.lastBytesAt = now
		health.probeSuccesses++
		delete(p.rejectedUntil, peerID)
		p.relaxProbeFailureLocked(stateKey, peerID)
	}
	delete(p.transportBlockedUntil, peerID)
	demandEvidence := archivePeerDemandAvailable
	if result.evidence == archivePeerEvidenceProven {
		demandEvidence = archivePeerDemandProven
	}
	demand.peers[peerID] = archivePeerDemandPeer{
		evidence: demandEvidence,
		at:       now,
	}
	p.clearDemandRetryLocked(demand.key, peerID)
	p.mx.Unlock()

	p.noteArchivePeerEvidence(peer, result)
	if result.evidence == archivePeerEvidenceProven {
		p.scout.clearPeer(peerID)
	}
	return true
}

func (p *archivePeerPool) noteScoutFailure(probe archivePeerProbe, peer *overlayPeer, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, archive.ErrNotAvailable) || errors.Is(err, ErrStateNotAvailable) {
		p.scoutStats.notAvailable.Add(1)
		p.recordDemandNotAvailable(probe, peer.id, archivePeerNotAvailableTTL)
		return
	}

	if errors.Is(err, errArchivePeerInvalidResponse) {
		p.scoutStats.invalid.Add(1)
		p.recordDemandNoBenefit(probe, peer.id, archivePeerNoBenefitTTL)
	} else {
		p.scoutStats.transportFailure.Add(1)
		p.rememberTransportBlocked(peer.id, archivePeerUnreachableTTL)
	}
	verdict := p.noteFailure(probe.shard, peer, archivePeerRejectCandidateFailed)
	if verdict.useless {
		p.rotateUseless(probe.shard)
	}
}

func (p *archivePeerPool) admitArchiveOnlyPeer(peer *overlayPeer, result archivePeerProbeResult) archivePeerAdmissionResult {
	if peer == nil || peer.id.IsZero() || !peer.hasOpenConnection() || result.evidence == archivePeerEvidenceUnknown {
		return archivePeerAdmissionResult{}
	}

	now := result.at
	if now.IsZero() {
		now = time.Now()
	}
	entry := &archivePeer{
		peer:    peer,
		addedAt: now,
	}

	p.mx.Lock()
	demand := p.demands[result.probe.demandID]
	if p.closed || demand == nil {
		p.mx.Unlock()
		return archivePeerAdmissionResult{stale: true}
	}
	if !peer.hasOpenConnection() || p.peers[peer.id] != nil || p.archivePeerByAddressLocked(peer.addr) != nil {
		p.mx.Unlock()
		return archivePeerAdmissionResult{}
	}

	roster := p.archiveOnlySizeLocked()
	needsReplacement := roster >= archivePeerRosterLimit
	var evicted *overlayPeer
	var evictedID PeerID
	var evictedValuable bool
	replaced := false
	if needsReplacement {
		evictID, worst, coverageDelta := p.archivePeerEvictionCandidateForDemandLocked(demand, now)
		if worst == nil {
			p.mx.Unlock()
			return archivePeerAdmissionResult{deferred: true}
		}
		_, candidateValuable := p.valuable[peer.id]
		valuableUpgrade := candidateValuable && worst.archiveDownloads == 0
		if coverageDelta < 0 || coverageDelta == 0 && !valuableUpgrade && !p.archivePeerShouldReplaceLocked(evictID, worst, peer.id, result) {
			p.mx.Unlock()
			return archivePeerAdmissionResult{}
		}
		evictedValuable = p.provenPeerLocked(evictID)
		evicted = p.removePeerLocked(evictID)
		replaced = evicted != nil
		if replaced {
			evictedID = evictID
			if evictedValuable {
				valuable := p.valuable[evictedID]
				valuable.nextTryAt = now.Add(archiveValuablePeerRetry)
				p.valuable[evictedID] = valuable
			} else {
				p.rememberRejectedLocked(evictedID, now, archivePeerReplacementCooldown)
			}
		}
	}

	if _, valuable := p.valuable[peer.id]; valuable {
		entry.archiveDownloads = 1
	}
	p.peers[peer.id] = entry
	health := p.shardPeerLocked(archivePeerPoolKey(result.probe.shard), peer.id)
	if result.evidence == archivePeerEvidenceProven {
		health.lastBytesAt = now
		health.probeSuccesses++
		health.probeBytesPerSec = result.bytesPerSecond
		p.relaxProbeFailureLocked(archivePeerPoolKey(result.probe.shard), peer.id)
	}
	delete(p.rejectedUntil, peer.id)
	delete(p.transportBlockedUntil, peer.id)
	demandEvidence := archivePeerDemandAvailable
	if result.evidence == archivePeerEvidenceProven {
		demandEvidence = archivePeerDemandProven
	}
	demand.peers[peer.id] = archivePeerDemandPeer{
		evidence: demandEvidence,
		at:       now,
	}
	p.clearDemandRetryLocked(demand.key, peer.id)
	roster = p.archiveOnlySizeLocked()
	freshProven := p.provenUsableSizeLocked(now)
	p.mx.Unlock()

	p.scout.clearPeer(peer.id)
	return archivePeerAdmissionResult{
		admitted:    true,
		replaced:    replaced,
		evicted:     evicted,
		roster:      roster,
		freshProven: freshProven,
	}
}

func (p *archivePeerPool) archivePeerEvictionCandidateForDemandLocked(candidateDemand *archivePeerDemand, now time.Time) (PeerID, *archivePeer, int) {
	before := make(map[uint64]int, len(p.demands))
	for demandID, demand := range p.demands {
		before[demandID] = p.demandPositiveRosterPeersLocked(demand, now)
	}

	var bestID PeerID
	var best *archivePeer
	bestDelta := 0
	for peerID, entry := range p.peers {
		if entry == nil || entry.leases > 0 {
			continue
		}

		delta := 0
		preservesCoverage := true
		for demandID, demand := range p.demands {
			countBefore := before[demandID]
			countAfter := countBefore
			state := demand.peers[peerID]
			if state.evidence >= archivePeerDemandAvailable && archivePeerUsable(entry, now) {
				countAfter--
			}
			if demand == candidateDemand {
				countAfter++
			}
			if countBefore > 0 && countAfter == 0 {
				preservesCoverage = false
				break
			}
			delta += archiveDemandCoverageScore(countAfter) - archiveDemandCoverageScore(countBefore)
		}
		if !preservesCoverage {
			continue
		}
		if best == nil || delta > bestDelta || delta == bestDelta && p.archivePeerLessUsefulLocked(peerID, entry, bestID, best) {
			bestID = peerID
			best = entry
			bestDelta = delta
		}
	}
	return bestID, best, bestDelta
}

func archiveDemandCoverageScore(peers int) int {
	switch {
	case peers <= 0:
		return 0
	case peers == 1:
		return 8
	case peers == 2:
		return 14
	case peers == 3:
		return 18
	default:
		return 20
	}
}

func (p *archivePeerPool) archivePeerByAddressLocked(addr string) *archivePeer {
	if addr == "" {
		return nil
	}
	for _, entry := range p.peers {
		if entry != nil && entry.peer != nil && entry.peer.addr == addr {
			return entry
		}
	}
	return nil
}

func (p *archivePeerPool) archivePeerEvictionCandidateLocked(now time.Time) (PeerID, *archivePeer) {
	var worstID PeerID
	var worst *archivePeer
	for peerID, entry := range p.peers {
		if entry == nil || entry.leases > 0 {
			continue
		}
		if worst == nil || p.archivePeerLessUsefulLocked(peerID, entry, worstID, worst) {
			worstID = peerID
			worst = entry
		}
	}
	return worstID, worst
}

func (p *archivePeerPool) archivePeerLessUsefulLocked(leftID PeerID, left *archivePeer, rightID PeerID, right *archivePeer) bool {
	leftClosed := left.peer == nil || !left.peer.hasOpenConnection()
	rightClosed := right.peer == nil || !right.peer.hasOpenConnection()
	if leftClosed != rightClosed {
		return leftClosed
	}

	leftValuable := left.archiveDownloads > 0
	rightValuable := right.archiveDownloads > 0
	if leftValuable != rightValuable {
		return !leftValuable
	}

	leftFailures := p.archivePeerHardFailuresLocked(leftID)
	rightFailures := p.archivePeerHardFailuresLocked(rightID)
	if leftFailures != rightFailures {
		return leftFailures > rightFailures
	}

	leftPerformance, leftAt := p.aggregateArchivePeerPerformanceLocked(leftID)
	rightPerformance, rightAt := p.aggregateArchivePeerPerformanceLocked(rightID)
	leftRate := leftPerformance.bytesPerSecond()
	rightRate := rightPerformance.bytesPerSecond()
	if leftRate != rightRate {
		return leftRate < rightRate
	}
	if !leftAt.Equal(rightAt) {
		return leftAt.Before(rightAt)
	}
	return bytes.Compare(leftID[:], rightID[:]) > 0
}

func (p *archivePeerPool) aggregateArchivePeerPerformanceLocked(peerID PeerID) (archivePeerPerformance, time.Time) {
	var performance archivePeerPerformance
	var lastBytesAt time.Time
	for _, state := range p.shards {
		if state == nil || state.peers[peerID] == nil {
			continue
		}
		peer := state.peers[peerID]
		performance.probeSuccesses += peer.probeSuccesses
		performance.archiveDownloads += peer.archiveDownloads
		performance.bytes += peer.bytes
		performance.downloadElapsed += peer.downloadElapsed
		if peer.probeBytesPerSec > performance.probeBytesPerSec {
			performance.probeBytesPerSec = peer.probeBytesPerSec
		}
		if peer.lastBytesAt.After(lastBytesAt) {
			lastBytesAt = peer.lastBytesAt
		}
	}
	return performance, lastBytesAt
}

func (p *archivePeerPool) archivePeerHardFailuresLocked(peerID PeerID) int {
	total := 0
	for _, state := range p.shards {
		if state == nil {
			continue
		}
		peer := state.peers[peerID]
		if peer == nil {
			continue
		}
		total += archivePeerFailureErrors(peer.failure) + peer.failure.badImports
	}
	return total
}

func (p *archivePeerPool) archivePeerShouldReplaceLocked(worstID PeerID, worst *archivePeer, candidateID PeerID, candidate archivePeerProbeResult) bool {
	if worst.peer == nil || !worst.peer.hasOpenConnection() || worst.archiveDownloads == 0 && !p.peerHasArchiveBytesLocked(worstID) {
		return true
	}
	if candidate.evidence != archivePeerEvidenceProven || candidate.bytesPerSecond <= 0 {
		return false
	}
	if p.archivePeerHardFailuresLocked(worstID) > 0 {
		return true
	}
	worstPerformance, _ := p.aggregateArchivePeerPerformanceLocked(worstID)
	worstRate := worstPerformance.bytesPerSecond()
	if worstRate <= 0 {
		return true
	}
	return candidate.bytesPerSecond >= worstRate*archivePeerReplacementFactor || bytes.Compare(candidateID[:], worstID[:]) < 0 && candidate.bytesPerSecond == worstRate
}

func (p *archivePeerPool) logArchivePeerAdmission(peer *overlayPeer, result archivePeerProbeResult, admission archivePeerAdmissionResult) {
	event := p.log.Debug()
	importantRosterSize := admission.freshProven <= 1 || admission.roster == archivePeerRosterLimit
	if admission.replaced || importantRosterSize {
		event = p.log.Info()
	}
	event.
		Str("peer", peer.addr).
		Str("peer_id", peer.id.String()).
		Bool("proven", result.evidence == archivePeerEvidenceProven).
		Bool("replaced", admission.replaced).
		Int("archive_only_peers", admission.roster).
		Int("fresh_proven_peers", admission.freshProven).
		Int("scouting_peers", p.scoutingSize()).
		Str("probe_speed", formatArchiveRate(result.bytesPerSecond)).
		Msg("archive scout admitted peer")
}

func formatArchiveRate(bytesPerSecond float64) string {
	if bytesPerSecond <= 0 {
		return "unknown"
	}
	return logutil.FormatByteRate(int64(bytesPerSecond), time.Second)
}
