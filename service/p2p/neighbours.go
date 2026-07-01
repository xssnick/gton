package p2p

import (
	"bytes"
	"math/rand/v2"
	"sort"
	"time"
)

func (s *overlaySubscription) neighbourPeerSnapshots() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	peers := make([]*overlayPeer, 0, len(s.neighbours))
	for _, id := range s.neighbours {
		peer := s.peers[id]
		if peer == nil || !peer.hasOpenConnection() {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) hasNeighbourLocked(id PeerID) bool {
	for _, current := range s.neighbours {
		if current == id {
			return true
		}
	}
	return false
}

func (s *overlaySubscription) removeNeighbourLocked(id PeerID) bool {
	for idx, current := range s.neighbours {
		if current != id {
			continue
		}
		s.neighbours = append(s.neighbours[:idx], s.neighbours[idx+1:]...)
		if s.lastPingedNeighbour == id {
			s.lastPingedNeighbour = PeerID{}
		}
		return true
	}
	return false
}

func (s *overlaySubscription) replaceNeighbourLocked(prevID, nextID PeerID) bool {
	if prevID.IsZero() || nextID.IsZero() || prevID == nextID || s.hasNeighbourLocked(nextID) {
		return false
	}
	for idx, current := range s.neighbours {
		if current != prevID {
			continue
		}
		s.neighbours[idx] = nextID
		if s.lastPingedNeighbour == prevID {
			s.lastPingedNeighbour = nextID
		}
		return true
	}
	return false
}

func (s *overlaySubscription) pruneNeighboursLocked() {
	now := time.Now()
	filtered := s.neighbours[:0]
	for _, id := range s.neighbours {
		peer := s.peers[id]
		if peer == nil || !peer.isAliveKnownOverlayPeer(now) || !peer.hasOpenConnection() {
			if s.lastPingedNeighbour == id {
				s.lastPingedNeighbour = PeerID{}
			}
			continue
		}
		filtered = append(filtered, id)
	}
	s.neighbours = filtered
}

func (s *overlaySubscription) reloadNeighbours() {
	s.mx.Lock()
	before := len(s.neighbours)
	s.pruneNeighboursLocked()
	s.mx.Unlock()

	candidates := s.aliveKnownPeersSnapshot()
	if len(candidates) == 0 {
		return
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	s.mx.Lock()

	s.pruneNeighboursLocked()
	protected := s.protectedPeerIDs()

	exchanged := false
	excluded := map[PeerID]struct{}{}
	for _, peer := range candidates {
		if peer.id.IsZero() || peer.overlay == nil {
			continue
		}
		if _, skip := excluded[peer.id]; skip {
			continue
		}
		if s.hasNeighbourLocked(peer.id) {
			continue
		}
		if len(s.neighbours) < maxQueryNeighbours {
			s.neighbours = append(s.neighbours, peer.id)
			continue
		}

		worstID, worstUnreliability := s.worstRotatableNeighbourLocked(protected)
		if worstID.IsZero() {
			break
		}
		if worstUnreliability > peerStopUnreliability {
			if s.replaceNeighbourLocked(worstID, peer.id) {
				excluded[worstID] = struct{}{}
			}
			continue
		}
		if !peer.statsSnapshot().alive {
			continue
		}

		idx, ok := s.randomRotatableNeighbourIndexLocked(protected)
		if !ok {
			break
		}
		replaced := s.neighbours[idx]
		s.neighbours[idx] = peer.id
		if s.lastPingedNeighbour == replaced {
			s.lastPingedNeighbour = peer.id
		}
		exchanged = true
		break
	}

	if exchanged {
		s.pruneNeighboursLocked()
	}

	after := len(s.neighbours)
	s.mx.Unlock()

	if before == 0 && after > 0 {
		s.log.Info().
			Int("neighbours", after).
			Int("alive_peers", len(candidates)).
			Msg("selected overlay neighbours")
	}
}

func (s *overlaySubscription) worstRotatableNeighbourLocked(protected map[PeerID]struct{}) (PeerID, float64) {
	worstID := PeerID{}
	worstUnreliability := -1.0
	for _, id := range s.neighbours {
		if _, ok := protected[id]; ok {
			continue
		}
		peer := s.peers[id]
		if peer == nil {
			continue
		}
		stats := peer.statsSnapshot()
		if stats.unreliability > worstUnreliability {
			worstID = id
			worstUnreliability = stats.unreliability
		}
	}
	return worstID, worstUnreliability
}

func (s *overlaySubscription) randomRotatableNeighbourIndexLocked(protected map[PeerID]struct{}) (int, bool) {
	rotatable := make([]int, 0, len(s.neighbours))
	for idx, id := range s.neighbours {
		if _, ok := protected[id]; ok {
			continue
		}
		rotatable = append(rotatable, idx)
	}
	if len(rotatable) == 0 {
		return 0, false
	}
	return rotatable[rand.IntN(len(rotatable))], true
}

func (s *overlaySubscription) preferredNeighbourPeers(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	candidates := s.neighbourPeerSnapshots()
	return s.orderPreferredPeers(candidates, requiredVersionMajor, requiredVersionMinor)
}

func (s *overlaySubscription) aliveNeighbourPeers(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	candidates := s.neighbourPeerSnapshots()
	if len(candidates) == 0 {
		return nil
	}

	now := time.Now()
	alive := candidates[:0]
	for _, peer := range candidates {
		if !peer.isAliveKnownOverlayPeer(now) {
			continue
		}
		if !peerEligible(peer.statsSnapshot(), requiredVersionMajor, requiredVersionMinor) {
			continue
		}
		alive = append(alive, peer)
	}

	return s.orderPreferredPeers(alive, requiredVersionMajor, requiredVersionMinor)
}

func (s *overlaySubscription) pingTargets() []*overlayPeer {
	peers := s.preferredNeighbourPeers(0, 0)
	if len(peers) == 0 {
		return nil
	}

	sort.SliceStable(peers, func(i, j int) bool {
		return bytes.Compare(peers[i].id[:], peers[j].id[:]) < 0
	})

	s.mx.Lock()
	defer s.mx.Unlock()

	start := 0
	if !s.lastPingedNeighbour.IsZero() {
		for idx, peer := range peers {
			if bytes.Compare(peer.id[:], s.lastPingedNeighbour[:]) <= 0 {
				continue
			}
			start = idx
			goto selected
		}
	}
selected:

	limit := minInt(peerPingFanout, len(peers))
	res := make([]*overlayPeer, 0, limit)
	for i := 0; i < limit; i++ {
		peer := peers[(start+i)%len(peers)]
		res = append(res, peer)
		s.lastPingedNeighbour = peer.id
	}
	return res
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *overlaySubscription) markPeerQueryFailed(peer *overlayPeer) {
	peer.queryFailed()
	if !peer.shouldStopQuerying() {
		return
	}

	s.mx.Lock()
	removed := s.removeNeighbourLocked(peer.id)
	s.mx.Unlock()
	if removed {
		s.reloadNeighbours()
	}
}
