package p2p

import (
	"context"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type adnlNopper interface {
	SendNop(context.Context) error
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

	s.noteDirectoryActivity(peer.id, peer.addr)
	s.learnAdvertisedNodes(ctx, res.List)
}

// learnAdvertisedNodes files the peers one exchange told us about, bounded the
// way an honest answer is bounded.
func (s *overlaySubscription) learnAdvertisedNodes(ctx context.Context, nodes []overlay.Node) {
	bounded := boundedAdvertisedNodes(nodes)
	for i := range bounded {
		if ctx.Err() != nil {
			return
		}
		s.learnAdvertisedPeer(ctx, bounded[i])
	}
}

// learnAdvertisedPeer files a gossiped node in the directory and attaches it
// only while the live tier is short. Learning is what makes us a useful gossip
// surface; attaching is what costs a handshake, wrappers and goroutines, so the
// two are deliberately decoupled — C++ likewise records up to max_peers_ rows
// while ever touching only its small working set.
//
// A node we only heard about is filed as hearsay: if the directory is full of
// rows that proved themselves, the write is refused rather than allowed to
// displace one, and we do not spend a DHT lookup dialling it either.
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
		s.rememberDirectoryPeerLocked(identity.peerID, identity.pub, "", "", &node, time.Now(), directoryProven)
		s.mx.Unlock()
		return
	}
	s.mx.Unlock()

	s.mx.Lock()
	filed := s.rememberDirectoryPeerLocked(identity.peerID, identity.pub, "", "", &node, time.Now(), directoryHearsay)
	shortOfLive := len(s.peers) < s.livePeerLimit()
	s.mx.Unlock()

	if !filed || !shortOfLive {
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
