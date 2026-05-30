package p2p

import (
	"math"
	"sort"
	"time"
)

const (
	stateSlowPeerSpeed   = float64(1 << 20)
	stateSlowPeerPenalty = archiveSlowPeerPenalty
)

func downloadPeerID(peer *overlayPeer) PeerID {
	if peer == nil {
		return PeerID{}
	}
	return peer.id
}

func (n *Node) acquireDownloadPeer(peer *overlayPeer) func() {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return func() {}
	}

	n.downloadPeerMx.Lock()
	if n.downloadPeerLeases == nil {
		n.downloadPeerLeases = map[PeerID]int{}
	}
	n.downloadPeerLeases[peerID]++
	n.downloadPeerMx.Unlock()

	return func() {
		n.downloadPeerMx.Lock()
		defer n.downloadPeerMx.Unlock()

		count := n.downloadPeerLeases[peerID]
		if count <= 1 {
			delete(n.downloadPeerLeases, peerID)
			return
		}
		n.downloadPeerLeases[peerID] = count - 1
	}
}

func (n *Node) downloadPeerLeaseCount(peer *overlayPeer) int {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return 0
	}

	n.downloadPeerMx.RLock()
	defer n.downloadPeerMx.RUnlock()
	return n.downloadPeerLeases[peerID]
}

func (n *Node) downloadPeerLeaseSnapshot(peers []*overlayPeer) map[PeerID]int {
	leases := make(map[PeerID]int, len(peers))
	n.downloadPeerMx.RLock()
	defer n.downloadPeerMx.RUnlock()
	for _, peer := range peers {
		peerID := downloadPeerID(peer)
		if peerID.IsZero() {
			continue
		}
		leases[peerID] = n.downloadPeerLeases[peerID]
	}
	return leases
}

func (n *Node) prioritizeStateSnapshotPeers(peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	leases := n.downloadPeerLeaseSnapshot(peers)
	now := time.Now()
	prioritized := append([]*overlayPeer(nil), peers...)
	sort.SliceStable(prioritized, func(i, j int) bool {
		left := prioritized[i]
		right := prioritized[j]
		leftStats := left.statsSnapshot()
		rightStats := right.statsSnapshot()
		leftLeases := leases[left.id]
		rightLeases := leases[right.id]

		leftSlow := leftStats.downloadSlowUntil.After(now)
		rightSlow := rightStats.downloadSlowUntil.After(now)
		if leftSlow != rightSlow {
			return !leftSlow
		}

		leftScore := downloadPeerScore(leftStats, statePeerSpeed(leftStats), leftLeases, now)
		rightScore := downloadPeerScore(rightStats, statePeerSpeed(rightStats), rightLeases, now)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		if leftLeases != rightLeases {
			return leftLeases < rightLeases
		}
		return false
	})
	return prioritized
}

func (n *Node) prioritizeBlockDownloadPeers(peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	leases := n.downloadPeerLeaseSnapshot(peers)
	now := time.Now()
	prioritized := append([]*overlayPeer(nil), peers...)
	sort.SliceStable(prioritized, func(i, j int) bool {
		left := prioritized[i]
		right := prioritized[j]
		leftStats := left.statsSnapshot()
		rightStats := right.statsSnapshot()
		leftLeases := leases[left.id]
		rightLeases := leases[right.id]

		leftSlow := leftStats.downloadSlowUntil.After(now)
		rightSlow := rightStats.downloadSlowUntil.After(now)
		if leftSlow != rightSlow {
			return !leftSlow
		}

		leftScore := downloadPeerScore(leftStats, blockPeerSpeed(leftStats), leftLeases, now)
		rightScore := downloadPeerScore(rightStats, blockPeerSpeed(rightStats), rightLeases, now)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		if leftLeases != rightLeases {
			return leftLeases < rightLeases
		}
		if leftStats.downloadBytesSec != rightStats.downloadBytesSec {
			return leftStats.downloadBytesSec > rightStats.downloadBytesSec
		}
		return false
	})
	return prioritized
}

func (s *overlaySubscription) prioritizeLiveNextPeers(peers []*overlayPeer, preferredPeerID PeerID, now time.Time) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	states := s.liveNextPeerSnapshot(peers)
	leases := map[PeerID]int{}
	if s.node != nil {
		leases = s.node.downloadPeerLeaseSnapshot(peers)
	}

	prioritized := append([]*overlayPeer(nil), peers...)
	sort.SliceStable(prioritized, func(i, j int) bool {
		leftPeer := prioritized[i]
		rightPeer := prioritized[j]
		left := liveNextPeerRankFor(leftPeer, preferredPeerID, states[leftPeer.id], leases[leftPeer.id], now)
		right := liveNextPeerRankFor(rightPeer, preferredPeerID, states[rightPeer.id], leases[rightPeer.id], now)

		if left.unavailable != right.unavailable {
			return !left.unavailable
		}
		if left.alive != right.alive {
			return left.alive
		}
		if left.slow != right.slow {
			return !left.slow
		}
		if left.unreliability != right.unreliability {
			return left.unreliability < right.unreliability
		}
		if left.leases != right.leases {
			return left.leases < right.leases
		}
		if left.preferred != right.preferred {
			return left.preferred
		}
		if left.known != right.known {
			return left.known
		}
		if left.availability != right.availability {
			return left.availability > right.availability
		}
		if left.bytesSec != right.bytesSec {
			return left.bytesSec > right.bytesSec
		}
		if left.known && left.latency != right.latency {
			return left.latency < right.latency
		}
		if left.successes != right.successes {
			return left.successes > right.successes
		}
		return false
	})
	return prioritized
}

func (s *overlaySubscription) liveNextPeerSnapshot(peers []*overlayPeer) map[PeerID]liveNextPeerState {
	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	if len(s.liveNextPeers) == 0 {
		return nil
	}

	snapshot := make(map[PeerID]liveNextPeerState, len(peers))
	for _, peer := range peers {
		if peer == nil || peer.id.IsZero() {
			continue
		}
		if state := s.liveNextPeers[peer.id]; state != nil {
			snapshot[peer.id] = *state
		}
	}
	return snapshot
}

type liveNextPeerRank struct {
	preferred     bool
	unavailable   bool
	alive         bool
	slow          bool
	unreliability float64
	leases        int
	known         bool
	successes     uint64
	latency       time.Duration
	bytesSec      float64
	availability  float64
}

func liveNextPeerRankFor(peer *overlayPeer, preferredPeerID PeerID, state liveNextPeerState, leases int, now time.Time) liveNextPeerRank {
	stats := peer.statsSnapshot()
	rank := liveNextPeerRank{
		preferred:     !preferredPeerID.IsZero() && peer.id == preferredPeerID,
		alive:         stats.alive,
		slow:          stats.downloadSlowUntil.After(now),
		unreliability: stats.unreliability,
		leases:        leases,
		successes:     state.successes,
		latency:       state.latency,
		bytesSec:      state.bytesSec,
		availability:  state.availability,
	}
	rank.unavailable = state.unavailableUntil.After(now)
	rank.known = state.successes > 0 && state.latency > 0
	if rank.unavailable {
		rank.preferred = false
	}
	return rank
}

func (n *Node) acquirePreferredStateSnapshotProbe(probes []persistentStatePeerProbe) (persistentStatePeerProbe, func()) {
	if len(probes) == 0 {
		return persistentStatePeerProbe{}, func() {}
	}

	n.downloadPeerMx.Lock()
	defer n.downloadPeerMx.Unlock()

	bestIdx := 0
	bestScore := math.Inf(-1)
	bestLeases := 0
	bestSpeed := 0.0

	for i, probe := range probes {
		leases := n.downloadPeerLeases[downloadPeerID(probe.candidate.peer)]
		speed := probeBytesPerSecond(probe)
		score := speed / float64(leases+1)

		if score > bestScore || (score == bestScore && (leases < bestLeases || (leases == bestLeases && speed > bestSpeed))) {
			bestIdx = i
			bestScore = score
			bestLeases = leases
			bestSpeed = speed
		}
	}

	selected := probes[bestIdx]
	selectedPeerID := downloadPeerID(selected.candidate.peer)
	if selectedPeerID.IsZero() {
		return selected, func() {}
	}
	if n.downloadPeerLeases == nil {
		n.downloadPeerLeases = map[PeerID]int{}
	}
	n.downloadPeerLeases[selectedPeerID]++

	return selected, func() {
		n.downloadPeerMx.Lock()
		defer n.downloadPeerMx.Unlock()

		count := n.downloadPeerLeases[selectedPeerID]
		if count <= 1 {
			delete(n.downloadPeerLeases, selectedPeerID)
			return
		}
		n.downloadPeerLeases[selectedPeerID] = count - 1
	}
}

func statePeerSpeed(stats peerStats) float64 {
	if stats.downloadBytesSec > 0 {
		return stats.downloadBytesSec
	}
	return 0
}

func blockPeerSpeed(stats peerStats) float64 {
	if stats.downloadBytesSec > 0 {
		return stats.downloadBytesSec
	}
	return blockUnknownPeerSpeed
}
