package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type overlaySubscription struct {
	node *Node
	spec overlaySpec
	log  zerolog.Logger

	mx                  sync.Mutex
	cancel              context.CancelFunc
	runToken            *struct{}
	inactive            bool
	inactiveDeleteAt    time.Time
	activityLeases      uint32
	pendingInactive     bool
	pendingDeleteAt     time.Time
	removed             bool
	seedMx              sync.Mutex
	seedRunning         bool
	seedScheduled       bool
	seedTarget          int
	nextSeedAt          time.Time
	chainDownloadMx     sync.Mutex
	maintenanceMx       sync.Mutex
	peers               map[PeerID]*overlayPeer
	neighbours          []PeerID
	lastPingedNeighbour PeerID
	chainDownloads      map[chainDownloadKey]*chainDownloadState
	liveNextPeers       map[PeerID]*liveNextPeerState
	peerNotify          chan struct{}
	broadcastTargets    atomic.Pointer[broadcastTargetsSnapshot]
	fecRelayState       *overlay.BroadcastFECRelayState
	twoStepState        *overlay.BroadcastTwoStepState
	twoStepQueue        *boundedQueue[rebroadcastRequest]
	twoStepQueueClosed  bool
	refreshRunning      bool
	peerPingRunning     bool
	fixedProbeRunning   bool
	softRecoveries      uint64
	hardRecoveries      uint64
}

func (s *overlaySubscription) isActive() bool {
	s.mx.Lock()
	defer s.mx.Unlock()

	return !s.inactive
}

func (s *overlaySubscription) setActive(active bool, deleteAt time.Time) bool {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.setActiveLocked(active, deleteAt)
}

func (s *overlaySubscription) setActiveLocked(active bool, deleteAt time.Time) bool {
	if s.removed {
		return false
	}

	if active {
		changed := s.inactive || s.pendingInactive
		s.inactive = false
		s.inactiveDeleteAt = time.Time{}
		s.pendingInactive = false
		s.pendingDeleteAt = time.Time{}
		return changed
	}

	if s.activityLeases > 0 {
		if !s.pendingInactive {
			s.pendingInactive = true
			s.pendingDeleteAt = deleteAt
		} else if s.pendingDeleteAt.IsZero() {
			s.pendingDeleteAt = deleteAt
		}
		return false
	}

	if s.inactive {
		if s.inactiveDeleteAt.IsZero() {
			s.inactiveDeleteAt = deleteAt
		}
		return false
	}

	s.inactive = true
	s.inactiveDeleteAt = deleteAt
	return true
}

func (s *overlaySubscription) inactiveExpired(now time.Time) bool {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.inactiveExpiredLocked(now)
}

func (s *overlaySubscription) inactiveExpiredLocked(now time.Time) bool {
	return !s.removed && s.activityLeases == 0 && s.inactive && !s.inactiveDeleteAt.IsZero() && !now.Before(s.inactiveDeleteAt)
}

func (s *overlaySubscription) beginArchiveUse() (func(), error) {
	s.mx.Lock()
	if s.removed {
		s.mx.Unlock()
		return nil, errors.New("overlay subscription was removed")
	}

	if s.inactive {
		s.pendingInactive = true
		s.pendingDeleteAt = s.inactiveDeleteAt
		s.inactive = false
		s.inactiveDeleteAt = time.Time{}
	}
	s.activityLeases++
	s.mx.Unlock()

	var once sync.Once
	return func() {
		once.Do(s.releaseArchiveUse)
	}, nil
}

func (s *overlaySubscription) releaseArchiveUse() {
	s.mx.Lock()
	s.activityLeases--
	if s.activityLeases == 0 && s.pendingInactive {
		s.inactive = true
		s.inactiveDeleteAt = s.pendingDeleteAt
		s.pendingInactive = false
		s.pendingDeleteAt = time.Time{}
	}
	s.mx.Unlock()
}

func (s *overlaySubscription) setRunCancel(cancel context.CancelFunc) (*struct{}, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.cancel != nil {
		return nil, false
	}
	token := &struct{}{}
	s.cancel = cancel
	s.runToken = token
	return token, true
}

func (s *overlaySubscription) clearRunCancel(token *struct{}) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.runToken == token {
		s.cancel = nil
		s.runToken = nil
	}
}

func (s *overlaySubscription) stopRun() bool {
	s.mx.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.runToken = nil
	s.mx.Unlock()

	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (s *overlaySubscription) close() {
	s.stopRun()

	s.mx.Lock()
	peers := make([]*overlayPeer, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	s.peers = map[PeerID]*overlayPeer{}
	s.neighbours = nil
	s.notifyPeersChangedLocked()
	twoStepQueue := s.twoStepQueue
	s.twoStepQueueClosed = true
	s.mx.Unlock()

	if twoStepQueue != nil {
		twoStepQueue.Close()
	}
	for _, peer := range peers {
		peer.close()
	}
}

type chainDownloadState struct {
	peer        *overlayPeer
	speed       float64
	unavailable map[PeerID]chainUnavailablePeer
}

type chainUnavailablePeer struct {
	peer *overlayPeer
	at   time.Time
}

type liveNextPeerState struct {
	successes        uint64
	latency          time.Duration
	bytesSec         float64
	availability     float64
	unavailableUntil time.Time
}

type overlayPeer struct {
	id          PeerID
	addr        string
	pub         ed25519.PublicKey
	announced   *overlay.Node
	fixedMember bool
	overlay     *overlay.ADNLOverlayWrapper
	rldp        *overlay.RLDPWrapper
	rldpOverlay *overlay.RLDPOverlayWrapper
	release     func()

	statsMx           sync.Mutex
	versionMajor      int32
	versionMinor      int32
	capabilitiesFlags uint32
	roundtrip         time.Duration
	unreliability     float64
	missedPings       uint32
	alive             bool
	pending           bool
	warmupRunning     bool
	lastReceiveAt     time.Time
	lastSuccessAt     time.Time
	failedQueries     uint64
	downloadBytesSec  float64
	downloadCount     uint64
	downloadSlowUntil time.Time

	attachedAt              time.Time
	lastPongAt              time.Time
	probeFailures           uint32
	lastSoftRecoveryAt      time.Time
	lastHardRecoveryAt      time.Time
	softRecoveriesSinceSeen uint32

	rebroadcastMx         sync.Mutex
	localRebroadcastQueue *boundedQueue[rebroadcastRequest]
	rebroadcastQueue      *boundedQueue[rebroadcastRequest]
	rebroadcastClosed     bool
}

func (p *overlayPeer) close() {
	p.closeRebroadcastQueues()
	if p.release != nil {
		p.release()
	}
}

func (s *overlaySubscription) run(ctx context.Context) {
	s.log.Info().Msg("starting overlay peer discovery")
	s.startPeerDiscovery(ctx)

	dhtTicker := time.NewTicker(dhtRefreshInterval)
	defer dhtTicker.Stop()
	refreshTimer := time.NewTimer(nextPeerRefreshDelay())
	defer refreshTimer.Stop()
	neighbourTimer := time.NewTimer(0)
	defer neighbourTimer.Stop()
	pingTimer := time.NewTimer(nextPeerPingDelay())
	defer pingTimer.Stop()

	var probeTimer *time.Timer
	var probeC <-chan time.Time
	if s.spec.Kind == overlayKindCustomFixed {
		probeTimer = time.NewTimer(nextFixedProbeDelay())
		defer probeTimer.Stop()
		probeC = probeTimer.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-dhtTicker.C:
			s.startPeerDiscovery(ctx)
		case <-refreshTimer.C:
			s.startRefreshPeers(ctx)
			refreshTimer.Reset(nextPeerRefreshDelay())
		case <-neighbourTimer.C:
			s.reloadNeighbours()
			neighbourTimer.Reset(nextNeighbourReloadDelay())
		case <-pingTimer.C:
			s.startPingPeers(ctx)
			pingTimer.Reset(nextPeerPingDelay())
		case <-probeC:
			s.startProbeFixedPeers(ctx)
			probeTimer.Reset(nextFixedProbeDelay())
		}
	}
}

func (s *overlaySubscription) startPeerDiscovery(ctx context.Context) {
	if s.spec.Kind == overlayKindCustomFixed {
		s.startSeedFromFixedNodes(ctx)
		return
	}
	s.startSeedFromDHT(ctx)
}

func (s *overlaySubscription) startSeedFromDHT(ctx context.Context) {
	s.startSeedFromDHTTarget(ctx, maxPeersPerOverlay)
}

func (s *overlaySubscription) startSeedFromDHTTarget(ctx context.Context, targetPeers int) {
	if !s.isActive() {
		return
	}
	if targetPeers < maxPeersPerOverlay {
		targetPeers = maxPeersPerOverlay
	}

	now := time.Now()

	s.seedMx.Lock()
	if targetPeers > s.seedTarget {
		s.seedTarget = targetPeers
	}
	if s.seedRunning {
		s.seedMx.Unlock()
		return
	}
	if !s.nextSeedAt.IsZero() && now.Before(s.nextSeedAt) {
		if s.seedScheduled {
			s.seedMx.Unlock()
			return
		}
		delay := time.Until(s.nextSeedAt)
		s.seedScheduled = true
		s.seedMx.Unlock()
		s.scheduleSeedFromDHT(ctx, delay)
		return
	}
	s.seedRunning = true
	targetPeers = s.seedTarget
	s.seedMx.Unlock()

	s.runSeedFromDHT(ctx, targetPeers)
}

func (s *overlaySubscription) runSeedFromDHT(ctx context.Context, targetPeers int) {
	run := func() {
		defer s.finishSeedFromDHT()
		s.seedFromDHT(ctx, targetPeers)
	}
	s.node.runAsync(run)
}

func (s *overlaySubscription) scheduleSeedFromDHT(ctx context.Context, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}

	run := func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			s.clearScheduledSeedFromDHT()
			return
		case <-s.node.runCtx.Done():
			s.clearScheduledSeedFromDHT()
			return
		case <-timer.C:
		}

		s.seedMx.Lock()
		targetPeers := s.seedTarget
		s.seedScheduled = false
		s.seedMx.Unlock()

		s.startSeedFromDHTTarget(ctx, targetPeers)
	}
	s.node.runAsync(run)
}

func (s *overlaySubscription) clearScheduledSeedFromDHT() {
	s.seedMx.Lock()
	s.seedScheduled = false
	s.seedMx.Unlock()
}

func (s *overlaySubscription) finishSeedFromDHT() {
	s.seedMx.Lock()
	s.seedRunning = false
	s.seedTarget = 0
	s.nextSeedAt = time.Now().Add(nextDHTSeedCooldownDelay())
	s.seedMx.Unlock()
}

func (s *overlaySubscription) startSeedFromFixedNodes(ctx context.Context) {
	if !s.isActive() || s.node.dht == nil {
		return
	}

	s.seedMx.Lock()
	if s.seedRunning {
		s.seedMx.Unlock()
		return
	}
	s.seedRunning = true
	s.seedMx.Unlock()

	run := func() {
		defer s.finishFixedNodeSeed()
		s.seedFromFixedNodes(ctx)
	}
	s.node.runAsync(run)
}

func (s *overlaySubscription) finishFixedNodeSeed() {
	s.seedMx.Lock()
	s.seedRunning = false
	s.seedMx.Unlock()
}

func (s *overlaySubscription) currentSeedTarget(defaultTarget int) int {
	s.seedMx.Lock()
	defer s.seedMx.Unlock()

	if s.seedTarget > defaultTarget {
		return s.seedTarget
	}
	return defaultTarget
}

type seedConnectResult struct {
	attached bool
	err      error
}

// seedConnectPool fans overlay nodes found during a DHT seed search out to a
// bounded set of connect workers and counts successful attachments. It is
// shared by the subscription seed search and the archive peer pool discovery.
type seedConnectPool struct {
	ctx       context.Context
	jobs      chan overlay.Node
	results   chan seedConnectResult
	workers   sync.WaitGroup
	collector sync.WaitGroup
	connected atomic.Int64
	finished  bool
}

// runSeedConnectPool starts parallelism connect workers plus a collector that
// logs failed connects with connectErrMsg. Feed nodes with send and always
// call finish (idempotent, single-goroutine use) before reading connected.
func runSeedConnectPool(ctx context.Context, log zerolog.Logger, connectErrMsg string, parallelism int, connect func(overlay.Node) (bool, error)) *seedConnectPool {
	p := &seedConnectPool{
		ctx:     ctx,
		jobs:    make(chan overlay.Node),
		results: make(chan seedConnectResult, parallelism),
	}

	for i := 0; i < parallelism; i++ {
		p.workers.Add(1)
		go func() {
			defer p.workers.Done()
			for node := range p.jobs {
				attached, err := connect(node)
				p.results <- seedConnectResult{attached: attached, err: err}
			}
		}()
	}

	p.collector.Add(1)
	go func() {
		defer p.collector.Done()
		for res := range p.results {
			if res.err != nil {
				log.Debug().Err(res.err).Msg(connectErrMsg)
				continue
			}
			if res.attached {
				p.connected.Add(1)
			}
		}
	}()

	return p
}

// send hands a node to the workers; it reports false once ctx is done.
func (p *seedConnectPool) send(node overlay.Node) bool {
	select {
	case p.jobs <- node:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// finish stops accepting nodes and waits for workers and collector to drain.
func (p *seedConnectPool) finish() {
	if p.finished {
		return
	}
	p.finished = true

	close(p.jobs)
	p.workers.Wait()
	close(p.results)
	p.collector.Wait()
}

type adnlNopper interface {
	SendNop(context.Context) error
}

type overlayNodeIdentity struct {
	pub    ed25519.PublicKey
	peerID PeerID
	self   bool
}

func (s *overlaySubscription) seedFromDHT(ctx context.Context, targetPeers int) {
	if s.node.dht == nil {
		return
	}
	if !s.isActive() {
		return
	}

	var (
		cont       *dht.Continuation
		err        error
		requests   int
		nodesSeen  int
		startedAt  = time.Now()
		knownStart = len(s.knownPeersSnapshot())
		aliveStart = s.aliveKnownPeerCount()
	)

	logSearch := aliveStart == 0
	refreshOnly := aliveStart >= s.currentSeedTarget(targetPeers)
	if logSearch {
		s.log.Info().
			Int("known_peers", knownStart).
			Int("alive_peers", aliveStart).
			Msg("searching overlay peers in DHT")
	}

	seedPool := runSeedConnectPool(ctx, s.log, "failed to connect overlay node", dhtSeedConnectParallelism, func(node overlay.Node) (bool, error) {
		return s.connectDHTOverlayNode(ctx, node)
	})
	defer seedPool.finish()

	maxRequests := 8
	if refreshOnly {
		maxRequests = 1
	}

	replacements := 0
	for i := 0; i < maxRequests && (refreshOnly || s.aliveKnownPeerCount() < s.currentSeedTarget(targetPeers)); i++ {
		lookupCtx, cancel := context.WithTimeout(ctx, dhtFindTimeout)
		var nodes *overlay.NodesList
		if cont == nil {
			nodes, cont, err = s.node.dht.FindOverlayNodes(lookupCtx, s.spec.FullID)
		} else {
			nodes, cont, err = s.node.dht.FindOverlayNodes(lookupCtx, s.spec.FullID, cont)
		}
		cancel()
		if err != nil {
			if logSearch {
				s.log.Debug().
					Err(err).
					Dur("elapsed", time.Since(startedAt)).
					Int("dht_requests", requests+1).
					Msg("DHT overlay peer search failed")
				return
			}
			s.log.Debug().Err(err).Msg("DHT lookup failed")
			return
		}

		requests++
		nodesSeen += len(nodes.List)

		for _, node := range nodes.List {
			if !refreshOnly && s.aliveKnownPeerCount() >= s.currentSeedTarget(targetPeers) {
				break
			}
			if refreshOnly {
				send, replaced := s.prepareDHTRefreshNode(node, replacements)
				if !send {
					continue
				}
				if replaced {
					replacements++
				}
			}
			if !seedPool.send(node) {
				break
			}
		}

		if cont == nil || refreshOnly {
			break
		}
	}

	seedPool.finish()

	if logSearch || seedPool.connected.Load() > 0 {
		s.log.Debug().
			Dur("elapsed", time.Since(startedAt)).
			Int("dht_requests", requests).
			Int("dht_nodes", nodesSeen).
			Int64("connected_peers", seedPool.connected.Load()).
			Int("known_peers", len(s.knownPeersSnapshot())).
			Int("alive_peers", s.aliveKnownPeerCount()).
			Msg("overlay peer DHT search finished")
	}
}

func (s *overlaySubscription) prepareDHTRefreshNode(node overlay.Node, replacements int) (bool, bool) {
	identity, err := s.overlayNodeIdentity(node)
	if err != nil || identity.self {
		return false, false
	}
	if s.hasPeer(identity.peerID) {
		return true, false
	}
	if replacements >= dhtRefreshReplacementLimit {
		return false, false
	}
	if !s.hasPeerReplacementCandidate(identity.peerID) {
		return false, false
	}
	return true, true
}

func (s *overlaySubscription) hasPeer(id PeerID) bool {
	s.mx.Lock()
	defer s.mx.Unlock()

	return s.peers[id] != nil
}

func (s *overlaySubscription) hasPeerReplacementCandidate(candidateID PeerID) bool {
	s.mx.Lock()
	if s.peers[candidateID] != nil {
		s.mx.Unlock()
		return true
	}

	protected := s.protectedPeerIDs()
	now := time.Now()
	evictID, _ := s.peerReplacementCandidateLocked(candidateID, now, protected)
	s.mx.Unlock()

	return !evictID.IsZero()
}

func (s *overlaySubscription) connectDHTOverlayNode(ctx context.Context, node overlay.Node) (bool, error) {
	connectCtx, cancel := context.WithTimeout(ctx, dhtSeedPeerTimeout)
	defer cancel()
	return s.connectOverlayNodeV1(connectCtx, node)
}

func (s *overlaySubscription) connectOverlayNodeV1(ctx context.Context, node overlay.Node) (bool, error) {
	if !s.isActive() {
		return false, errors.New("shard is inactive")
	}
	identity, err := s.overlayNodeIdentity(node)
	if err != nil {
		return false, err
	}
	if identity.self {
		return false, nil
	}

	peerID := identity.peerID

	s.mx.Lock()
	if peer := s.peers[peerID]; peer != nil {
		peer.mergeAnnouncement(&node)
		s.mx.Unlock()
		s.reloadNeighbours()
		s.retryPendingPeerWarmup(peer)
		return false, nil
	}
	s.mx.Unlock()

	addrList, _, err := findPeerAddresses(ctx, s.node.dht, identity.peerID[:])
	if err != nil {
		return false, fmt.Errorf("find ADNL addresses: %w", err)
	}
	if len(addrList.Addresses) == 0 {
		return false, fmt.Errorf("overlay node has no addresses")
	}

	udpAddr, err := firstPeerEndpoint(addrList.Addresses)
	if err != nil {
		return false, err
	}
	pooled, releaseEndpoint, err := s.node.acquirePeerEndpoint(identity.peerID, udpAddr, identity.pub)
	if err != nil {
		return false, fmt.Errorf("connect overlay peer endpoint: %w", err)
	}
	attached := s.attachPooledPeer(pooled, &node)
	releaseEndpoint()
	if attached {
		event := s.log.Debug()
		if s.aliveKnownPeerCount() <= 3 {
			event = s.log.Info()
		}
		event.
			Str("peer", pooled.addr).
			Str("peer_id", pooled.id.String()).
			Msg("connected overlay peer")
	}
	return attached, nil
}

func (s *overlaySubscription) seedFromFixedNodes(ctx context.Context) {
	if s.node.dht == nil || !s.isActive() {
		return
	}
	if s.aliveKnownPeerCount() >= s.peerLimit()-1 {
		return
	}

	connected := 0
	for _, id := range s.spec.FixedNodes {
		if ctx.Err() != nil {
			return
		}
		if id == s.node.localID || s.hasPeer(id) {
			continue
		}

		connectCtx, cancel := context.WithTimeout(ctx, dhtSeedPeerTimeout)
		attached, err := s.connectFixedNode(connectCtx, id)
		cancel()
		if err != nil {
			s.log.Debug().
				Err(err).
				Str("peer_id", id.String()).
				Msg("failed to connect fixed custom overlay peer")
			continue
		}
		if attached {
			connected++
		}
	}
	if connected > 0 {
		s.log.Debug().
			Int("connected_peers", connected).
			Int("known_peers", len(s.knownPeersSnapshot())).
			Msg("custom overlay fixed peer search finished")
	}
}

func (s *overlaySubscription) connectFixedNode(ctx context.Context, nodeID PeerID) (bool, error) {
	addrList, pub, err := findPeerAddresses(ctx, s.node.dht, nodeID[:])
	if err != nil {
		return false, fmt.Errorf("find ADNL addresses: %w", err)
	}
	if len(addrList.Addresses) == 0 {
		return false, fmt.Errorf("fixed node has no addresses")
	}

	pubID, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		return false, fmt.Errorf("hash fixed node public key: %w", err)
	}
	if !bytes.Equal(pubID, nodeID[:]) {
		return false, fmt.Errorf("fixed node public key does not match requested ADNL id")
	}

	udpAddr, err := firstPeerEndpoint(addrList.Addresses)
	if err != nil {
		return false, err
	}
	pooled, releaseEndpoint, err := s.node.acquirePeerEndpoint(nodeID, udpAddr, pub)
	if err != nil {
		return false, fmt.Errorf("connect fixed node endpoint: %w", err)
	}
	reinitFreshFixedTransport(pooled)
	attached := s.attachPooledPeer(pooled, nil)
	releaseEndpoint()
	if attached {
		s.log.Debug().
			Str("peer", pooled.addr).
			Str("peer_id", pooled.id.String()).
			Msg("connected fixed custom overlay peer")
	}
	return attached, nil
}

// reinitFreshFixedTransport restores restart semantics for an
// outbound-created transport: registerClient copies the gateway address
// list, whose ReinitDate is the process start time, so without this a
// rebuilt connection would keep the previous reinit epoch and a remote that
// remembers our old sequence numbers would drop the fresh low-seqno packets
// as duplicates. Reinit is applied only before any traffic in either
// direction: inbound means the pair already works, outbound means a channel
// may be pending and resetting it mid-handshake could collide with another
// overlay attaching the same peer.
func reinitFreshFixedTransport(pooled *pooledPeer) {
	if pooled == nil || pooled.adnl == nil {
		return
	}
	stats := pooled.adnl.Stats()
	if stats.Inbound.Packets != 0 || stats.Outbound.Packets != 0 {
		return
	}
	if reiniter, ok := pooled.adnl.ADNL.(adnlReiniter); ok {
		reiniter.Reinit()
	}
}

func firstPeerEndpoint(addresses []adnladdr.Address) (string, error) {
	for _, addr := range addresses {
		endpoint, err := adnladdr.DialString(addr)
		if err == nil {
			return endpoint, nil
		}
	}
	return "", errors.New("peer has no supported endpoint")
}

func (s *overlaySubscription) overlayNodeIdentity(node overlay.Node) (overlayNodeIdentity, error) {
	pub, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		return overlayNodeIdentity{}, fmt.Errorf("unsupported overlay node key type %T", node.ID)
	}
	if len(pub.Key) != ed25519.PublicKeySize {
		return overlayNodeIdentity{}, fmt.Errorf("invalid overlay node key size %d", len(pub.Key))
	}

	if err := node.CheckSignature(); err != nil {
		return overlayNodeIdentity{}, fmt.Errorf("overlay node signature: %w", err)
	}
	now := time.Now()
	nodeTime := time.Unix(int64(node.Version), 0)
	if nodeTime.Before(now.Add(-overlayPeerTTL)) || nodeTime.After(now.Add(overlayFutureSkew)) {
		return overlayNodeIdentity{}, fmt.Errorf("stale overlay node version: %d", node.Version)
	}
	if !bytes.Equal(node.Overlay, s.spec.ShortID) {
		return overlayNodeIdentity{}, fmt.Errorf("overlay id mismatch")
	}

	if selfPub, ok := s.node.privKey.Public().(ed25519.PublicKey); ok && bytes.Equal(pub.Key, selfPub) {
		return overlayNodeIdentity{self: true}, nil
	}

	nodeID, err := tl.Hash(node.ID)
	if err != nil {
		return overlayNodeIdentity{}, fmt.Errorf("hash overlay node id: %w", err)
	}
	peerID, err := NewPeerID(nodeID)
	if err != nil {
		return overlayNodeIdentity{}, fmt.Errorf("parse overlay node id: %w", err)
	}

	return overlayNodeIdentity{
		pub:    append(ed25519.PublicKey(nil), pub.Key...),
		peerID: peerID,
	}, nil
}

func (s *overlaySubscription) attachPooledPeer(pooled *pooledPeer, announced *overlay.Node) bool {
	if !s.acceptsPeerID(pooled.id) {
		return false
	}

	s.mx.Lock()
	if peer := s.peers[pooled.id]; peer != nil {
		peer.mergeAnnouncement(announced)
		s.mx.Unlock()
		return false
	}

	var evicted *overlayPeer
	if len(s.peers) >= s.peerLimit() {
		protected := s.protectedPeerIDs()
		evictID := s.attachPeerEvictionCandidateLocked(pooled.id, protected)
		if evictID.IsZero() {
			s.mx.Unlock()
			return false
		}
		evicted = s.peers[evictID]
		delete(s.peers, evictID)
		s.removeNeighbourLocked(evictID)
	}

	state := s.newOverlayPeer(pooled, announced, s.spec.Kind == overlayKindCustomFixed, s.spec.Kind != overlayKindCustomFixed)
	if s.pendingOnAttach() {
		state.markPending()
	}
	if len(s.spec.AuthorizedKeys) > 0 {
		state.overlay.SetAuthorizedKeys(s.spec.AuthorizedKeys)
	}
	state.initRebroadcastQueues()
	s.peers[pooled.id] = state
	s.notifyPeersChangedLocked()
	s.mx.Unlock()

	if evicted != nil {
		evicted.close()
	}
	s.configureBroadcastFECRelay(state)
	s.configureCustomTwoStepBroadcast(state)
	s.installHandlers(state)
	s.startPeerRebroadcastWorker(state)
	if announced != nil || s.spec.Kind == overlayKindCustomFixed || s.spec.Kind == overlayKindPublicShard {
		s.reloadNeighbours()
		s.startPeerWarmup(state)
	}
	return true
}

func (s *overlaySubscription) pendingOnAttach() bool {
	return s.spec.Kind == overlayKindPublicShard
}

func (s *overlaySubscription) newOverlayPeer(pooled *pooledPeer, announced *overlay.Node, fixedMember bool, allowBroadcastFEC bool) *overlayPeer {
	adnlOverlay, rldpOverlay, release := s.node.pool.acquireOverlay(pooled, s.spec.ShortID, maxOverlayPayloadSize, allowBroadcastFEC, false)

	return &overlayPeer{
		id:          pooled.id,
		addr:        pooled.addr,
		pub:         append(ed25519.PublicKey(nil), pooled.pub...),
		announced:   cloneOverlayNode(announced),
		fixedMember: fixedMember,
		overlay:     adnlOverlay,
		rldp:        pooled.rldp,
		rldpOverlay: rldpOverlay,
		release:     release,
		alive:       !fixedMember,
		attachedAt:  time.Now(),
	}
}

func (s *overlaySubscription) acceptsPeerID(id PeerID) bool {
	if s.spec.Kind != overlayKindCustomFixed {
		return true
	}
	_, ok := s.spec.FixedNodeIDs[id]
	return ok
}

func (s *overlaySubscription) peerLimit() int {
	if s.spec.Kind == overlayKindCustomFixed && len(s.spec.FixedNodes) > maxPeersPerOverlay {
		return len(s.spec.FixedNodes)
	}
	return maxPeersPerOverlay
}

func (s *overlaySubscription) protectedPeerIDs() map[PeerID]struct{} {
	return s.node.protectedPeerIDs()
}

func (s *overlaySubscription) attachPeerEvictionCandidateLocked(candidateID PeerID, protected map[PeerID]struct{}) PeerID {
	now := time.Now()
	evictID, _ := s.peerReplacementCandidateLocked(candidateID, now, protected)
	return evictID
}

func (s *overlaySubscription) peerReplacementCandidateLocked(candidateID PeerID, now time.Time, protected map[PeerID]struct{}) (PeerID, float64) {
	evictID := PeerID{}
	evictScore := -1.0
	for id, peer := range s.peers {
		if id == candidateID {
			continue
		}
		if _, ok := protected[id]; ok {
			continue
		}
		if peer == nil {
			return id, peerFailUnreliability * 10
		}

		score := peerReplacementScore(peer, now)
		if evictID.IsZero() || score > evictScore {
			evictID = id
			evictScore = score
		}
	}
	if evictScore < peerStopUnreliability {
		return PeerID{}, evictScore
	}
	return evictID, evictScore
}

func peerReplacementScore(peer *overlayPeer, now time.Time) float64 {
	stats := peer.statsSnapshot()
	score := stats.unreliability
	if !peer.hasOpenConnection() {
		score += peerFailUnreliability * 4
	}
	if !peer.isKnownOverlayPeer(now) {
		score += peerFailUnreliability * 3
	}
	if !stats.alive {
		score += peerFailUnreliability * 2
	}
	if stats.downloadSlowUntil.After(now) {
		score += peerSlowEvictionScore
	}

	failedScore := float64(stats.failedQueries)
	if failedScore > peerFailUnreliability {
		failedScore = peerFailUnreliability
	}
	return score + failedScore
}

func (s *overlaySubscription) peerNotifySnapshot() <-chan struct{} {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.peerNotify == nil {
		s.peerNotify = make(chan struct{}, 1)
	}
	return s.peerNotify
}

func (s *overlaySubscription) notifyPeersChangedLocked() {
	s.broadcastTargets.Store(nil)
	if s.peerNotify == nil {
		s.peerNotify = make(chan struct{}, 1)
	}
	select {
	case s.peerNotify <- struct{}{}:
	default:
	}
}

func (s *overlaySubscription) startPeerWarmup(peer *overlayPeer) {
	if peer == nil || !s.isActive() {
		return
	}
	if !peer.tryBeginWarmup() {
		return
	}

	s.node.runAsync(func() {
		defer peer.finishWarmup()

		ctx, cancel := context.WithTimeout(s.node.runCtx, attachWarmupTimeout)
		defer cancel()

		if s.spec.Kind == overlayKindCustomFixed {
			s.warmupCustomFixedPeer(ctx, peer)
			return
		}
		if s.spec.QueryCapabilities {
			s.pingPeer(ctx, peer)
		}
		if ctx.Err() == nil && s.spec.RandomPeers {
			s.exchangeRandomPeers(ctx, peer)
		}
	})
}

func (s *overlaySubscription) warmupCustomFixedPeer(ctx context.Context, peer *overlayPeer) {
	s.sendCustomFixedNop(ctx, peer)
}

func (s *overlaySubscription) sendCustomFixedNop(ctx context.Context, peer *overlayPeer) {
	if peer == nil || peer.overlay == nil || peer.overlay.ADNLWrapper == nil {
		return
	}
	nopper, ok := peer.overlay.ADNL.(adnlNopper)
	if !ok {
		return
	}

	if err := nopper.SendNop(ctx); err != nil {
		s.handlePeerQueryFailure(peer, err)
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("custom fixed ADNL nop failed")
		return
	}
}

func (s *overlaySubscription) retryPendingPeerWarmup(peer *overlayPeer) {
	if s.spec.Kind != overlayKindPublicShard || peer == nil || !peer.isPending() {
		return
	}
	s.startPeerWarmup(peer)
}

func (s *overlaySubscription) startRefreshPeers(ctx context.Context) {
	if !s.beginRefreshPeers() {
		return
	}
	s.runMaintenanceAsync(func() {
		defer s.endRefreshPeers()
		s.refreshPeers(ctx)
	})
}

func (s *overlaySubscription) startPingPeers(ctx context.Context) {
	if !s.beginPeerPing() {
		return
	}
	s.runMaintenanceAsync(func() {
		defer s.endPeerPing()
		s.pingPeers(ctx)
	})
}

func (s *overlaySubscription) runMaintenanceAsync(fn func()) {
	s.node.runAsync(fn)
}

func (s *overlaySubscription) beginRefreshPeers() bool {
	s.maintenanceMx.Lock()
	defer s.maintenanceMx.Unlock()

	if s.refreshRunning {
		return false
	}
	s.refreshRunning = true
	return true
}

func (s *overlaySubscription) endRefreshPeers() {
	s.maintenanceMx.Lock()
	s.refreshRunning = false
	s.maintenanceMx.Unlock()
}

func (s *overlaySubscription) beginPeerPing() bool {
	s.maintenanceMx.Lock()
	defer s.maintenanceMx.Unlock()

	if s.peerPingRunning {
		return false
	}
	s.peerPingRunning = true
	return true
}

func (s *overlaySubscription) endPeerPing() {
	s.maintenanceMx.Lock()
	s.peerPingRunning = false
	s.maintenanceMx.Unlock()
}

func (s *overlaySubscription) beginFixedProbe() bool {
	s.maintenanceMx.Lock()
	defer s.maintenanceMx.Unlock()

	if s.fixedProbeRunning {
		return false
	}
	s.fixedProbeRunning = true
	return true
}

func (s *overlaySubscription) endFixedProbe() {
	s.maintenanceMx.Lock()
	s.fixedProbeRunning = false
	s.maintenanceMx.Unlock()
}

func (s *overlaySubscription) installHandlers(peer *overlayPeer) {
	peer.overlay.SetBroadcastHandlerWithInfo(func(msg tl.Serializable, info overlay.BroadcastInfo) error {
		var sourcePeerID PeerID
		if len(info.SourceID) > 0 {
			id, err := NewPeerID(info.SourceID)
			if err == nil {
				sourcePeerID = id
			}
		}
		delivery := DeliveryFEC
		if info.TwoStep {
			delivery = DeliveryTwoStep
		}
		if info.DecodeTime > 0 {
			s.node.observeBroadcastPipelineStageDuration(broadcastPipelineStageFECDecode, broadcastKindLabel(msg), delivery, broadcastPipelineResultSuccess, info.DecodeTime)
		}
		return s.handleOverlayBroadcastPayload(peer, msg, newIdentifiedBroadcastPayload(msg, info.BroadcastID), delivery, info.Trusted, sourcePeerID)
	})
	peer.overlay.SetCustomMessageHandler(func(msg *adnl.MessageCustom) error {
		switch data := msg.Data.(type) {
		case overlay.Broadcast:
			return s.handleSimpleBroadcast(peer, data)
		case ForgetPeer:
			s.removePeerIfCurrent(peer)
			return nil
		default:
			return nil
		}
	})
	peer.overlay.SetQueryHandler(func(msg *adnl.MessageQuery) error {
		return s.answerADNLQuery(peer, msg)
	})
	if s.spec.Kind != overlayKindCustomFixed {
		peer.rldpOverlay.SetOnQuery(func(transferID []byte, query *rldp.Query) error {
			return s.answerRLDPQuery(peer, transferID, query)
		})
	}
	peer.overlay.SetDisconnectHandler(func(_ string, _ ed25519.PublicKey) {
		// Remove by pointer, not by id: the disconnect cascade runs
		// asynchronously and a recovery path may have already re-attached a
		// fresh peer object for the same id, which must survive this
		// stale callback.
		s.removePeerIfCurrent(peer)
	})
}

func (s *overlaySubscription) refreshPeers(ctx context.Context) {
	if !s.isActive() || !s.spec.RandomPeers {
		return
	}
	peers := s.refreshTargets()
	if len(peers) == 0 {
		return
	}

	s.runPeerMaintenance(ctx, peers, peerRefreshFanout, s.exchangeRandomPeers)
}

func (s *overlaySubscription) exchangeRandomPeers(ctx context.Context, peer *overlayPeer) {
	if !s.isActive() {
		return
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	advertised, err := s.randomPeerAdvertisement()
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to create self overlay node")
		return
	}

	startedAt := time.Now()
	var res overlay.NodesList
	err = peer.overlay.Query(queryCtx, overlay.GetRandomPeers{
		List: advertised,
	}, &res)
	if err != nil {
		s.handlePeerQueryFailure(peer, err)
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("overlay.getRandomPeers failed")
		return
	}
	if peer.querySuccess(time.Since(startedAt)) {
		s.peerPromoted(peer)
	}

	for _, node := range res.List {
		if !s.canLearnAdvertisedPeer(node) {
			continue
		}
		if _, err := s.connectOverlayNodeV1(ctx, node); err != nil {
			s.log.Debug().Err(err).Msg("failed to connect peer learned from overlay")
		}
	}
}

func (s *overlaySubscription) pingPeers(ctx context.Context) {
	if !s.isActive() || !s.spec.QueryCapabilities {
		return
	}
	s.runPeerMaintenance(ctx, s.pingTargets(), peerPingFanout, s.pingPeer)
}

func (s *overlaySubscription) pingPeer(ctx context.Context, peer *overlayPeer) {
	queryCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	startedAt := time.Now()
	var res Capabilities
	if err := peer.overlay.Query(queryCtx, GetCapabilities{}, &res); err != nil {
		s.handlePeerQueryFailure(peer, err)
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("tonNode.getCapabilities failed")
		return
	}

	peer.applyCapabilities(res)
	if peer.querySuccess(time.Since(startedAt)) {
		s.peerPromoted(peer)
	}
}

func (s *overlaySubscription) runPeerMaintenance(ctx context.Context, peers []*overlayPeer, parallelism int, run func(context.Context, *overlayPeer)) {
	if len(peers) == 0 {
		return
	}
	if parallelism <= 0 || parallelism > len(peers) {
		parallelism = len(peers)
	}

	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

loop:
	for _, peer := range peers {
		if !s.peerReadyForMaintenance(ctx, peer) {
			continue
		}
		select {
		case <-ctx.Done():
			break loop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(peer *overlayPeer) {
			defer wg.Done()
			defer func() { <-sem }()

			if !s.peerReadyForMaintenance(ctx, peer) {
				return
			}
			run(ctx, peer)
		}(peer)
	}

	wg.Wait()
}

func (s *overlaySubscription) peerReadyForMaintenance(ctx context.Context, peer *overlayPeer) bool {
	if peer == nil || peer.overlay == nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case <-peer.overlay.GetCloserCtx().Done():
		s.removePeerIfCurrent(peer)
		return false
	default:
		return true
	}
}

func (s *overlaySubscription) handleSimpleBroadcast(peer *overlayPeer, msg overlay.Broadcast) error {
	if peer != nil && peer.noteReceive() {
		s.peerPromoted(peer)
	}
	if !checkSimpleBroadcastDate(msg.Date) {
		return nil
	}

	source, ok := msg.Source.(keys.PublicKeyED25519)
	if !ok {
		return nil
	}
	if len(msg.Data) == 0 || len(msg.Data) > maxOverlayPayloadSize {
		return nil
	}

	actualSourceID, err := tl.Hash(msg.Source)
	if err != nil {
		return nil
	}

	sourceID := make([]byte, 32)
	if msg.Flags&1 == 0 {
		sourceID = actualSourceID
	}

	broadcastHash, err := tl.Hash(OverlayBroadcastID{
		Source:   sourceID,
		DataHash: hashSimpleBroadcastPayload(msg.Data),
		Flags:    msg.Flags,
	})
	if err != nil {
		return nil
	}

	toSign, err := tl.Serialize(overlay.BroadcastToSign{
		Hash: broadcastHash,
		Date: uint32(msg.Date),
	}, true)
	if err != nil {
		return nil
	}

	if !ed25519.Verify(source.Key, toSign, msg.Signature) {
		return nil
	}

	var parsed any
	if _, err = tl.Parse(&parsed, msg.Data, true); err != nil {
		return nil
	}

	sourcePeerID, err := NewPeerID(actualSourceID)
	if err != nil {
		return nil
	}

	return s.handleOverlayBroadcastPayload(peer, parsed, newKnownBroadcastPayload(msg.Data), DeliverySimple, false, sourcePeerID)
}

func (s *overlaySubscription) peerPromoted(peer *overlayPeer) {
	if peer == nil {
		return
	}

	s.mx.Lock()
	if s.peers[peer.id] != peer {
		s.mx.Unlock()
		return
	}
	s.notifyPeersChangedLocked()
	s.mx.Unlock()

	s.reloadNeighbours()
}

func (s *overlaySubscription) removePeerIfCurrent(peer *overlayPeer) {
	s.mx.Lock()
	if s.peers[peer.id] != peer {
		s.mx.Unlock()
		return
	}
	delete(s.peers, peer.id)
	s.removeNeighbourLocked(peer.id)
	s.notifyPeersChangedLocked()
	s.mx.Unlock()

	peer.close()
}

func (s *overlaySubscription) aliveKnownPeerCount() int {
	s.mx.Lock()
	defer s.mx.Unlock()

	count := 0
	now := time.Now()
	for _, peer := range s.peers {
		if !peer.isAliveKnownOverlayPeer(now) {
			continue
		}
		count++
	}
	return count
}

func (s *overlaySubscription) ensurePeers(ctx context.Context) error {
	if !s.isActive() {
		return errors.New("shard is inactive")
	}

	s.startPeerDiscovery(ctx)

	for {
		if s.aliveKnownPeerCount() > 0 {
			return nil
		}

		notify := s.peerNotifySnapshot()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		case <-time.After(downloadRetryDelay):
			s.startPeerDiscovery(ctx)
		}
	}
}

func (s *overlaySubscription) peersSnapshot() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers := make([]*overlayPeer, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) knownPeersSnapshot() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers := make([]*overlayPeer, 0, len(s.peers))
	now := time.Now()
	for _, peer := range s.peers {
		if !peer.isKnownOverlayPeer(now) || !peer.hasOpenConnection() {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) aliveKnownPeersSnapshot() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers := make([]*overlayPeer, 0, len(s.peers))
	now := time.Now()
	for _, peer := range s.peers {
		if !peer.isAliveKnownOverlayPeer(now) || !peer.hasOpenConnection() {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) overlayNodesSnapshot() []overlay.Node {
	s.mx.Lock()
	defer s.mx.Unlock()

	list := make([]overlay.Node, 0, len(s.peers))
	now := time.Now()
	for _, peer := range s.peers {
		node := peer.advertisedNodeSnapshot(now)
		if node == nil || !overlayNodeHasSerializableID(node) {
			continue
		}
		list = append(list, *node)
	}
	return list
}

func (s *overlaySubscription) randomPeerAdvertisement() (overlay.NodesList, error) {
	self, err := s.node.selfOverlayNode(s.spec)
	if err != nil {
		return overlay.NodesList{}, err
	}

	list := make([]overlay.Node, 0, maxRandomPeerReply)
	list = append(list, *self)
	list = append(list, s.randomOverlayNodes(maxRandomPeerReply-len(list))...)

	return overlay.NodesList{List: list}, nil
}

func (s *overlaySubscription) randomOverlayNodes(limit int) []overlay.Node {
	if limit <= 0 {
		return nil
	}

	list := s.overlayNodesSnapshot()
	if len(list) > 1 {
		rand.Shuffle(len(list), func(i, j int) {
			list[i], list[j] = list[j], list[i]
		})
	}
	if len(list) > limit {
		list = list[:limit]
	}
	return list
}

func (s *overlaySubscription) canLearnAdvertisedPeer(node overlay.Node) bool {
	if s.aliveKnownPeerCount() < maxPeersPerOverlay {
		return true
	}

	identity, err := s.overlayNodeIdentity(node)
	if err != nil || identity.self {
		return false
	}
	return s.hasPeer(identity.peerID)
}
