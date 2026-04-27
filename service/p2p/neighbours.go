package p2p

import (
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

func (s *overlaySubscription) hasNeighbourLocked(id string) bool {
	for _, current := range s.neighbours {
		if current == id {
			return true
		}
	}
	return false
}

func (s *overlaySubscription) addNeighbourLocked(id string) bool {
	if id == "" || s.hasNeighbourLocked(id) || len(s.neighbours) >= maxQueryNeighbours {
		return false
	}
	s.neighbours = append(s.neighbours, id)
	return true
}

func (s *overlaySubscription) removeNeighbourLocked(id string) bool {
	for idx, current := range s.neighbours {
		if current != id {
			continue
		}
		s.neighbours = append(s.neighbours[:idx], s.neighbours[idx+1:]...)
		if s.lastPingedNeighbour == id {
			s.lastPingedNeighbour = ""
		}
		return true
	}
	return false
}

func (s *overlaySubscription) replaceNeighbourLocked(prevID, nextID string) bool {
	if prevID == "" || nextID == "" || prevID == nextID || s.hasNeighbourLocked(nextID) {
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
				s.lastPingedNeighbour = ""
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

	exchanged := false
	excluded := map[string]struct{}{}
	for _, peer := range candidates {
		if peer.id == "" || peer.overlay == nil {
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

		worstID, worstUnreliability := s.worstNeighbourLocked()
		if worstID == "" {
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

		idx := rand.IntN(len(s.neighbours))
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

func (s *overlaySubscription) worstNeighbourLocked() (string, float64) {
	worstID := ""
	worstUnreliability := -1.0
	for _, id := range s.neighbours {
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

func (s *overlaySubscription) preferredNeighbourPeers(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	candidates := s.neighbourPeerSnapshots()
	return s.orderPreferredPeers(candidates, requiredVersionMajor, requiredVersionMinor)
}

func (s *overlaySubscription) pingTargets() []*overlayPeer {
	peers := s.preferredNeighbourPeers(0, 0)
	if len(peers) == 0 {
		return nil
	}

	sort.SliceStable(peers, func(i, j int) bool {
		return peers[i].id < peers[j].id
	})

	s.mx.Lock()
	defer s.mx.Unlock()

	start := 0
	if s.lastPingedNeighbour != "" {
		for idx, peer := range peers {
			if peer.id <= s.lastPingedNeighbour {
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
