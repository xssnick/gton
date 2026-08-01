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
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
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
	lastQueryProbed     PeerID
	chainDownloads      map[chainDownloadKey]*chainDownloadState
	liveNextPeers       map[PeerID]*liveNextPeerState
	peerNotify          chan struct{}
	directory           map[PeerID]*directoryEntry
	peerCacheMu         sync.Mutex
	peerCachePrev       map[PeerID]*peerCacheEntry
	peerCacheSeedOnce   sync.Once
	broadcastTargetsMx  sync.Mutex
	broadcastTargets    atomic.Pointer[broadcastTargetsSnapshot]
	broadcastTargetsGen atomic.Uint64
	broadcastReceiver   *overlay.BroadcastReceiver
	plumtree            *plumtreeRuntime
	fastSync            *fastSyncOverlayRuntime
	quicEnvelope        *quicOverlayEnvelope
	twoStepQueue        *boundedQueue[rebroadcastRequest]
	twoStepQueueClosed  bool
	refreshRunning      bool
	peerPingRunning     bool
	fixedProbeRunning   bool
	softRecoveries      uint64
	hardRecoveries      uint64

	advertisedPeerLearning atomic.Bool
}

func (s *overlaySubscription) isActive() bool {
	s.mx.Lock()
	defer s.mx.Unlock()

	return !s.removed && !s.inactive
}

// rosterPeerIfActive resolves a roster peer and the subscription's active
// state in one critical section; the broadcast receive path calls it for
// every inbound broadcast.
func (s *overlaySubscription) rosterPeerIfActive(id PeerID) (*overlayPeer, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	return s.peers[id], !s.removed && !s.inactive
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
		s.broadcastReceiver.SetActive(true)
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
	s.broadcastReceiver.SetActive(false)
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
		s.broadcastReceiver.SetActive(true)
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
		s.broadcastReceiver.SetActive(false)
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
	if s.plumtree != nil {
		s.plumtree.engine.Close()
	}

	s.mx.Lock()
	s.removed = true
	s.inactive = true
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

	s.broadcastReceiver.Close()
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
	node           *Node
	id             PeerID
	addr           string
	route          *peerRoute
	pub            ed25519.PublicKey
	overlayID      []byte
	announced      *overlay.Node
	fixedMember    bool
	overlay        *overlay.ADNLOverlayWrapper
	rldp           *overlay.RLDPWrapper
	rldpOverlay    *overlay.RLDPOverlayWrapper
	queryTransport peerQueryTransport
	broadcastPeer  overlay.BroadcastPeer
	release        func()

	// Lock-free counters feeding the persistent peer cache: srcScore counts
	// first-accepted broadcasts delivered by this peer, outboundOK marks that
	// the peer answered an outbound query of ours (cache eligibility).
	srcScore   atomic.Uint32
	outboundOK atomic.Bool

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
	queryResponds     bool
	queryIgnoreUntil  time.Time

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
	p.release()
}

func (s *overlaySubscription) run(ctx context.Context) {
	var plumtreeDone chan struct{}
	if s.plumtree != nil {
		plumtreeDone = make(chan struct{})
		go func() {
			s.plumtree.run(ctx)
			close(plumtreeDone)
		}()
		defer func() {
			<-plumtreeDone
		}()
	}

	s.log.Info().Msg("starting overlay peer discovery")
	s.startPeerDiscovery(ctx)

	dhtTicker := time.NewTicker(dhtRefreshInterval)
	defer dhtTicker.Stop()
	refreshTimer := time.NewTimer(nextPeerRefreshDelay())
	defer refreshTimer.Stop()
	neighbourTimer := time.NewTimer(0)
	defer neighbourTimer.Stop()
	var peerCacheC <-chan time.Time
	if s.peerCacheEnabled() {
		peerCacheTicker := time.NewTicker(peerCacheSaveInterval)
		defer peerCacheTicker.Stop()
		peerCacheC = peerCacheTicker.C
	}
	var pingTimer *time.Timer
	var pingC <-chan time.Time
	if s.spec.probesPeersPeriodically() {
		pingTimer = time.NewTimer(s.spec.nextQueryProbeDelay())
		defer pingTimer.Stop()
		pingC = pingTimer.C
	}

	var probeTimer *time.Timer
	var probeC <-chan time.Time
	var probePolicy fixedProbePolicy
	if s.spec.runsFixedPeerProbes() {
		probePolicy = defaultFixedProbePolicy()
		probeTimer = time.NewTimer(nextFixedProbeDelay(probePolicy))
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
		case <-peerCacheC:
			s.node.runAsync(s.savePeerCacheSnapshot)
		case <-pingC:
			s.startPingPeers(ctx)
			pingTimer.Reset(s.spec.nextQueryProbeDelay())
		case <-probeC:
			s.startProbeFixedPeers(ctx, probePolicy)
			probeTimer.Reset(nextFixedProbeDelay(probePolicy))
		}
	}
}

func (s *overlaySubscription) startPeerDiscovery(ctx context.Context) {
	if s.spec.seedsFromFixedNodes() {
		s.startSeedFromFixedNodes(ctx)
		return
	}
	if s.peerCacheEnabled() {
		// Once per subscription: redial the persisted roster alongside the
		// DHT seed so restart warm-up does not wait on discovery.
		s.peerCacheSeedOnce.Do(func() {
			s.node.runAsync(func() {
				s.seedFromPeerCache(ctx)
			})
		})
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
		knownStart = s.knownPeerCount()
		aliveStart = s.aliveKnownPeerCount()
	)

	logSearch := aliveStart == 0
	refreshOnly := knownStart >= s.currentSeedTarget(targetPeers)
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
	for i := 0; i < maxRequests && (refreshOnly || s.knownPeerCount() < s.currentSeedTarget(targetPeers)); i++ {
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
			if !refreshOnly && s.knownPeerCount() >= s.currentSeedTarget(targetPeers) {
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

	protected := s.node.protectedPeerIDs()
	now := time.Now()
	evictID := s.peerReplacementCandidateLocked(candidateID, now, protected)
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

	// Resolve rather than reuse the filed address. A DHT lookup is expensive,
	// but nothing else re-checks an endpoint: acquirePeerEndpoint does no I/O
	// (the ADNL handshake is lazy), so a dial to a peer that has moved fails
	// silently later, and attachPooledPeer writes the same dead address back
	// into the directory. Re-resolving on every gossip mention is what keeps a
	// moved peer reachable at all. The other discovery paths that dial straight
	// from a filed address - promotion and the peer cache - are seeded by this
	// one, so this is where the address has to stay honest.
	endpoint, err := s.resolvePeerEndpoint(ctx, peerID)
	if err != nil {
		return false, err
	}

	// File what we learned before deciding whether to dial: the address and the
	// announcement are the valuable part, and they cost nothing to keep. A full
	// live tier may still dial an eligible replacement; attachPooledPeer repeats
	// the selection under the same lock before it commits the swap.
	s.mx.Lock()
	s.rememberDirectoryPeerLocked(
		identity.peerID,
		identity.pub,
		endpoint.adnlAddr,
		endpoint.quicAddr,
		&node,
		time.Now(),
	)
	canAttach := len(s.peers) < s.livePeerLimit()
	if !canAttach {
		protected := s.node.protectedPeerIDs()
		evictID := s.peerReplacementCandidateLocked(peerID, time.Now(), protected)
		canAttach = !evictID.IsZero()
	}
	s.mx.Unlock()
	if !canAttach {
		return false, nil
	}

	pooled, releaseEndpoint, err := s.node.acquirePeerEndpoint(identity.peerID, endpoint, identity.pub)
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

// resolvePeerEndpoint finds a peer's endpoint in the DHT.
func (s *overlaySubscription) resolvePeerEndpoint(ctx context.Context, id PeerID) (peerEndpoint, error) {
	addrList, _, err := s.node.resolvePeerAddresses(ctx, id)
	if err != nil {
		return peerEndpoint{}, fmt.Errorf("find ADNL addresses: %w", err)
	}
	if len(addrList.Addresses) == 0 {
		return peerEndpoint{}, fmt.Errorf("overlay node has no addresses")
	}

	endpoint, err := peerEndpointFromAddresses(addrList.Addresses)
	if err != nil {
		return peerEndpoint{}, err
	}
	if quicAddr, quicErr := peerQUICRouteFromAddresses(addrList.Addresses); quicErr == nil {
		endpoint.quicAddr = quicAddr
	}
	return endpoint, nil
}

func (s *overlaySubscription) seedFromFixedNodes(ctx context.Context) {
	if s.node.dht == nil || !s.isActive() {
		return
	}

	atPeerLimit := s.aliveKnownPeerCount() >= s.peerLimit()-1
	if atPeerLimit && !s.hasMissingFixedQUICRoute() {
		return
	}

	candidates := make([]PeerID, 0, len(s.spec.FixedNodes))
	for _, id := range s.spec.FixedNodes {
		if id == s.node.localID {
			continue
		}

		peer := s.peerByID(id)
		if peer != nil {
			if !s.spec.UseQUIC || peer.route.quicAddr() != "" {
				continue
			}
		} else if atPeerLimit {
			continue
		}
		candidates = append(candidates, id)
	}
	if s.spec.shufflesSeedOrder() {
		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})
	}

	jobs := make(chan PeerID)
	var (
		workers   sync.WaitGroup
		connected atomic.Int64
	)
	for range min(dhtSeedConnectParallelism, len(candidates)) {
		workers.Add(1)
		go func() {
			defer workers.Done()

			for id := range jobs {
				connectCtx, cancel := context.WithTimeout(
					ctx,
					dhtSeedPeerTimeout,
				)
				attached, err := s.connectFixedNode(connectCtx, id)
				cancel()
				if err != nil {
					s.log.Debug().
						Err(err).
						Str("peer_id", id.String()).
						Msg("failed to connect fixed overlay peer")
					continue
				}
				if attached {
					connected.Add(1)
				}
			}
		}()
	}
	for _, id := range candidates {
		select {
		case jobs <- id:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return
		}
	}
	close(jobs)
	workers.Wait()

	if connected.Load() > 0 {
		s.log.Debug().
			Int64("connected_peers", connected.Load()).
			Int("known_peers", len(s.knownPeersSnapshot())).
			Msg("fixed overlay peer search finished")
	}
}

func (s *overlaySubscription) hasMissingFixedQUICRoute() bool {
	if !s.spec.UseQUIC {
		return false
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	for _, id := range s.spec.FixedNodes {
		peer := s.peers[id]
		if peer != nil && peer.route.quicAddr() == "" {
			return true
		}
	}
	return false
}

func (s *overlaySubscription) connectFixedNode(ctx context.Context, nodeID PeerID) (bool, error) {
	addrList, pub, err := s.node.resolvePeerAddresses(ctx, nodeID)
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

	endpoint, err := peerEndpointFromAddresses(addrList.Addresses)
	if err != nil {
		return false, err
	}
	quicAddr, quicErr := peerQUICRouteFromAddresses(addrList.Addresses)
	if quicErr != nil {
		if s.spec.UseQUIC {
			return false, quicErr
		}
	} else {
		endpoint.quicAddr = quicAddr
	}
	pooled, releaseEndpoint, err := s.node.acquirePeerEndpoint(nodeID, endpoint, pub)
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
	stats := pooled.adnl.Stats()
	if stats.Inbound.Packets != 0 || stats.Outbound.Packets != 0 {
		return
	}
	if reiniter, ok := pooled.adnl.ADNL.(adnlReiniter); ok {
		reiniter.Reinit()
	}
}

func firstADNLEndpoint(addresses []adnladdr.Address) (string, error) {
	for _, addr := range addresses {
		endpoint, err := adnladdr.DialString(addr)
		if err == nil {
			return endpoint, nil
		}
	}
	return "", errors.New("peer has no supported endpoint")
}

func peerEndpointFromAddresses(addresses []adnladdr.Address) (peerEndpoint, error) {
	adnlAddr, err := firstADNLEndpoint(addresses)
	if err != nil {
		return peerEndpoint{}, err
	}

	return peerEndpoint{adnlAddr: adnlAddr}, nil
}

func peerQUICRouteFromAddresses(addresses []adnladdr.Address) (string, error) {
	endpoint, err := adnlquic.PeerEndpoint(addresses)
	if err != nil {
		return "", fmt.Errorf("peer QUIC endpoint: %w", err)
	}
	return endpoint.String(), nil
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
	if s.removed {
		s.mx.Unlock()
		return false
	}
	if peer := s.peers[pooled.id]; peer != nil {
		peer.mergeAnnouncement(announced)
		s.mx.Unlock()
		return false
	}

	var evicted *overlayPeer
	var evictID PeerID
	if len(s.peers) >= s.livePeerLimit() {
		protected := s.node.protectedPeerIDs()
		evictID = s.attachPeerEvictionCandidateLocked(pooled.id, protected)
		if evictID.IsZero() {
			s.mx.Unlock()
			return false
		}
	}

	fixedMember := s.spec.membersArePermanent()
	if s.fastSync != nil {
		fixedMember = s.fastSync.permanent(pooled.id)
	}
	state, err := s.newOverlayPeer(pooled, announced, fixedMember)
	if err != nil {
		s.mx.Unlock()
		s.log.Warn().
			Err(err).
			Str("peer", pooled.addr).
			Msg("failed to attach peer to overlay")
		return false
	}
	if !evictID.IsZero() {
		evicted = s.peers[evictID]
		delete(s.peers, evictID)
		s.removeNeighbourLocked(evictID)
		// Demotion, not amnesia: the directory row stays, so the peer is still
		// advertised, still a promotion candidate, and still served through the
		// detached query path.
		s.markDirectoryLiveLocked(evictID, false)
	}
	if s.spec.peersStartPending() {
		state.markPending()
	}
	state.initRebroadcastQueues()
	s.peers[pooled.id] = state
	s.rememberDirectoryPeerLocked(
		pooled.id,
		pooled.pub,
		pooled.addr,
		pooled.route.quicAddr(),
		announced,
		time.Now(),
	)
	s.markDirectoryLiveLocked(pooled.id, true)
	s.notifyPeersChangedLocked()
	s.mx.Unlock()

	if evicted != nil {
		evicted.close()
	}
	s.installHandlers(state)
	s.startPeerRebroadcastWorker(state)
	s.reloadNeighbours()
	s.startPeerWarmup(state)
	return true
}

func (s *overlaySubscription) newOverlayPeer(pooled *pooledPeer, announced *overlay.Node, fixedMember bool) (*overlayPeer, error) {
	adnlOverlay, rldpOverlay, release, err := s.node.pool.acquireOverlay(pooled, s.broadcastReceiver, fixedMember)
	if err != nil {
		return nil, err
	}

	peer := &overlayPeer{
		node:        s.node,
		id:          pooled.id,
		addr:        pooled.addr,
		route:       pooled.route,
		pub:         pooled.pub,
		overlayID:   s.spec.ShortID,
		announced:   cloneOverlayNode(announced),
		fixedMember: fixedMember,
		overlay:     adnlOverlay,
		rldp:        pooled.rldp,
		rldpOverlay: rldpOverlay,
		release:     release,
		alive:       !fixedMember,
		attachedAt:  time.Now(),
	}
	if s.spec.UseQUIC {
		peer.queryTransport = quicPeerQueryTransport{
			peer:     peer,
			envelope: s.quicEnvelope,
		}
		peer.broadcastPeer = quicRouteBroadcastPeer{
			peer:     peer,
			envelope: s.quicEnvelope,
		}
	} else {
		peer.queryTransport = rldpPeerQueryTransport{
			overlay:   rldpOverlay,
			overlayID: s.spec.ShortID,
		}
		peer.broadcastPeer = peer.overlay
	}
	return peer, nil
}

func (s *overlaySubscription) acceptsPeerID(id PeerID) bool {
	if s.fastSync != nil {
		return s.fastSync.contains(id)
	}
	if !s.spec.restrictsPeerIDs() {
		return true
	}
	_, ok := s.spec.FixedNodeIDs[id]
	return ok
}

// adoptsInboundPeer reports whether a raw inbound transport joins this
// overlay's roster directly. Only custom-fixed overlays adopt inbound peers
// (membership-listed ones); public overlays serve unlisted ingress through
// the broadcast-receiver resolver instead.
func (s *overlaySubscription) adoptsInboundPeer(id PeerID) bool {
	return s.spec.adoptsInboundPeers() && s.acceptsPeerID(id)
}

// livePeerLimit bounds the peers that hold a transport. It must cover every
// subsystem that talks to peers at once — query neighbours, the plumtree active
// set and the transient download/proof sets — with room to rotate; everything
// beyond that is knowledge, which belongs in the directory. Fixed-membership
// overlays are sized by their roster instead, since every member is expected to
// be reachable.
func (s *overlaySubscription) livePeerLimit() int {
	if !s.spec.hasDirectoryTier() {
		return s.peerLimit()
	}
	return maxLivePeersPerOverlay
}

func (s *overlaySubscription) peerLimit() int {
	if s.spec.usesFastSyncRoster() {
		expected := len(s.spec.FixedNodes) +
			s.fastSync.spec.roster.rootCount()*
				FastSyncMemberSlotCount
		return max(maxPeersPerOverlay, expected)
	}
	if s.spec.rosterSizesPeerLimit() &&
		len(s.spec.FixedNodes) > maxPeersPerOverlay {
		return len(s.spec.FixedNodes)
	}
	return maxPeersPerOverlay
}

func (s *overlaySubscription) attachPeerEvictionCandidateLocked(candidateID PeerID, protected map[PeerID]struct{}) PeerID {
	now := time.Now()
	return s.peerReplacementCandidateLocked(candidateID, now, protected)
}

func (s *overlaySubscription) peerReplacementCandidateLocked(candidateID PeerID, now time.Time, protected map[PeerID]struct{}) PeerID {
	evictID := PeerID{}
	evictScore := -1.0
	for id, peer := range s.peers {
		if id == candidateID {
			continue
		}
		if _, ok := protected[id]; ok {
			continue
		}

		score := peerReplacementScore(peer, now)
		if evictID.IsZero() || score > evictScore {
			evictID = id
			evictScore = score
		}
	}
	if evictScore < peerStopUnreliability {
		return PeerID{}
	}
	return evictID
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

	return s.peerNotify
}

func (s *overlaySubscription) notifyPeersChangedLocked() {
	s.broadcastTargetsGen.Add(1)
	select {
	case s.peerNotify <- struct{}{}:
	default:
	}
}

func (s *overlaySubscription) startPeerWarmup(peer *overlayPeer) {
	if !s.isActive() {
		return
	}
	if !peer.tryBeginWarmup() {
		return
	}

	s.node.runAsync(func() {
		defer peer.finishWarmup()

		ctx, cancel := context.WithTimeout(s.node.runCtx, attachWarmupTimeout)
		defer cancel()

		s.warmupPeer(ctx, peer)
	})
}

// warmupPublicPeer is the default arm of warmupPeer. The two gates are
// independent on purpose: a public overlay may advertise capabilities without
// participating in peer gossip, and the ctx check keeps a timed-out warmup from
// spending its whole budget on the exchange.
func (s *overlaySubscription) warmupPublicPeer(ctx context.Context, peer *overlayPeer) {
	if s.spec.QueryCapabilities {
		s.pingPeer(ctx, peer)
	}
	if ctx.Err() == nil && s.spec.RandomPeers {
		s.exchangeRandomPeers(ctx, peer)
	}
}

func (s *overlaySubscription) warmupCustomFixedPeer(ctx context.Context, peer *overlayPeer) {
	s.sendCustomFixedNop(ctx, peer)
}

func (s *overlaySubscription) sendCustomFixedNop(ctx context.Context, peer *overlayPeer) {
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
	if !s.spec.peersStartPending() || !peer.isPending() {
		return
	}
	s.startPeerWarmup(peer)
}

func (s *overlaySubscription) startRefreshPeers(ctx context.Context) {
	if !s.beginRefreshPeers() {
		return
	}
	s.node.runAsync(func() {
		defer s.endRefreshPeers()
		s.refreshPeers(ctx)
	})
}

func (s *overlaySubscription) startPingPeers(ctx context.Context) {
	if !s.beginPeerPing() {
		return
	}
	s.node.runAsync(func() {
		defer s.endPeerPing()
		s.pingPeers(ctx)
	})
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
	peer.overlay.SetCustomMessageHandler(func(msg *adnl.MessageCustom) error {
		switch msg.Data.(type) {
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
	peer.rldpOverlay.SetOnQuery(func(transferID []byte, query *rldp.Query) error {
		return s.answerRLDPQuery(peer, transferID, query)
	})
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
	s.topUpLiveTier(ctx)
	s.gossipWithDirectoryPeers(ctx)
	peers := s.refreshTargets()
	if len(peers) == 0 {
		return
	}

	exchange := s.exchangeRandomPeers
	if s.spec.usesFastSyncPeerGossip() {
		exchange = s.exchangeFastSyncRandomPeers
	}
	s.runPeerMaintenance(ctx, peers, peerRefreshFanout, exchange)
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
		s.learnAdvertisedPeer(ctx, node)
	}
}

// learnAdvertisedPeer files a gossiped node in the directory and attaches it
// only while the live tier is short. Learning is what makes us a useful gossip
// surface; attaching is what costs a handshake, wrappers and goroutines, so the
// two are deliberately decoupled — C++ likewise records up to max_peers_ rows
// while ever touching only its small working set.
func (s *overlaySubscription) learnAdvertisedPeer(ctx context.Context, node overlay.Node) {
	identity, err := s.overlayNodeIdentity(node)
	if err != nil || identity.self {
		return
	}
	s.mx.Lock()
	if peer := s.peers[identity.peerID]; peer != nil {
		// Already live: refresh both the directory row and the peer's own
		// announcement. The peer object is what isKnownOverlayPeer reads, and
		// its announcement expires after overlayPeerTTL - so a live peer whose
		// announcement is never refreshed silently drops out of the known set,
		// and with it out of neighbours, broadcast targets and the plumtree
		// roster, while we keep gossiping with it every second.
		peer.mergeAnnouncement(&node)
		s.rememberDirectoryPeerLocked(identity.peerID, identity.pub, "", "", &node, time.Now())
		s.mx.Unlock()
		return
	}
	s.mx.Unlock()

	s.mx.Lock()
	s.rememberDirectoryPeerLocked(identity.peerID, identity.pub, "", "", &node, time.Now())
	shortOfLive := len(s.peers) < s.livePeerLimit()
	s.mx.Unlock()

	if !shortOfLive {
		return
	}
	if _, err := s.connectOverlayNodeV1(ctx, node); err != nil {
		s.log.Debug().Err(err).Msg("failed to connect peer learned from overlay")
	}
}

func (s *overlaySubscription) pingPublicPeers(ctx context.Context) {
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

func (s *overlaySubscription) peerPromoted(peer *overlayPeer) {
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
	s.markDirectoryLiveLocked(peer.id, false)
	s.notifyPeersChangedLocked()
	s.mx.Unlock()

	if s.fastSync != nil {
		s.fastSync.setPeerAlive(peer.id, false)
	}
	if s.plumtree != nil {
		s.plumtree.engine.RemovePeer(peer.id)
		s.plumtree.notifyAlarmChanged()
	}
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
		if s.aliveKnownPeerCount() > 0 ||
			s.publicRandomQueryFallbackPeer(0, 0) != nil {
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

func (s *overlaySubscription) overlayNodesSnapshot(limit int) []overlay.Node {
	// Drawn from the directory, not the live set: what we gossip is how other
	// nodes learn about our peers and, symmetrically, how wide a surface we
	// keep in the network. Narrowing it to the peers we happen to hold
	// transports for is exactly what starved broadcast fanout at the old cap.
	//
	// The limit is pushed all the way down so the sampling, not the copying,
	// is what runs under the subscription lock.
	return s.advertisedDirectoryNodes(time.Now(), limit)
}

func (s *overlaySubscription) randomPeerAdvertisement() (overlay.NodesList, error) {
	self, err := overlay.NewNode(s.spec.FullID, s.node.privKey)
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

	// Already a uniform sample of size limit; the shuffle only randomises the
	// order we hand them out in.
	list := s.overlayNodesSnapshot(limit)
	if len(list) > 1 {
		rand.Shuffle(len(list), func(i, j int) {
			list[i], list[j] = list[j], list[i]
		})
	}
	return list
}
