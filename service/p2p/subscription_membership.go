package p2p

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

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
		s.plumtree.RemovePeer(peer.id)
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
