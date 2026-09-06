package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/p2p/internal/peerroute"
	"github.com/xssnick/tonutils-go/adnl/overlay"
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
	private             *privateOverlayRuntime
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
	closeOnce              sync.Once
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

	if s.cancel != nil || s.removed {
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
	s.closeOnce.Do(func() {
		s.closeInner()
	})
}

func (s *overlaySubscription) closeInner() {
	s.stopRun()

	// Close admission before stopping the runtime. Callers that have already
	// entered remain owned by the node's inbound lifecycle, while no new
	// Plumtree work can pass isActive once runtime shutdown begins.
	s.mx.Lock()
	s.removed = true
	s.inactive = true
	s.mx.Unlock()

	if s.plumtree != nil {
		s.plumtree.Close()
	}

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

	s.broadcastReceiver.Close()
	if twoStepQueue != nil {
		twoStepQueue.Close()
	}
	for _, peer := range peers {
		peer.close()
	}
	if s.private != nil {
		s.private.close()
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
	route          *peerroute.Route
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

	// prioritySend is raised while this node's own candidate symbol is being
	// written to the peer; relay writes to the peer defer to it.
	prioritySend quicPrioritySendLatch

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
	if s.plumtree != nil {
		s.plumtree.Start(ctx)
		defer s.plumtree.Wait()
	}

	s.log.Info().
		Hex("short_id", s.spec.ShortID).
		Msg("starting overlay peer discovery")
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
