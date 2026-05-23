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

func downloadPeerKey(peer *overlayPeer) string {
	if peer == nil {
		return ""
	}
	if peer.id != "" {
		return peer.id
	}
	return peer.addr
}

func (n *Node) acquireDownloadPeer(peer *overlayPeer) func() {
	if peer == nil {
		return func() {}
	}

	key := downloadPeerKey(peer)

	n.downloadPeerMx.Lock()
	if n.downloadPeerLeases == nil {
		n.downloadPeerLeases = map[string]int{}
	}
	n.downloadPeerLeases[key]++
	n.downloadPeerMx.Unlock()

	return func() {
		n.downloadPeerMx.Lock()
		defer n.downloadPeerMx.Unlock()

		count := n.downloadPeerLeases[key]
		if count <= 1 {
			delete(n.downloadPeerLeases, key)
			return
		}
		n.downloadPeerLeases[key] = count - 1
	}
}

func (n *Node) downloadPeerLeaseCount(peer *overlayPeer) int {
	if peer == nil {
		return 0
	}

	n.downloadPeerMx.RLock()
	defer n.downloadPeerMx.RUnlock()
	return n.downloadPeerLeases[downloadPeerKey(peer)]
}

func (n *Node) downloadPeerLeaseSnapshot(peers []*overlayPeer) map[string]int {
	leases := make(map[string]int, len(peers))
	n.downloadPeerMx.RLock()
	defer n.downloadPeerMx.RUnlock()
	for _, peer := range peers {
		leases[downloadPeerKey(peer)] = n.downloadPeerLeases[downloadPeerKey(peer)]
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
		leftLeases := leases[downloadPeerKey(left)]
		rightLeases := leases[downloadPeerKey(right)]

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
		leftLeases := leases[downloadPeerKey(left)]
		rightLeases := leases[downloadPeerKey(right)]

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

func (s *overlaySubscription) prioritizeLiveNextPeers(peers []*overlayPeer, preferredKey string, now time.Time) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	states := s.liveNextPeerSnapshot(peers)
	leases := map[string]int{}
	if s.node != nil {
		leases = s.node.downloadPeerLeaseSnapshot(peers)
	}

	prioritized := append([]*overlayPeer(nil), peers...)
	sort.SliceStable(prioritized, func(i, j int) bool {
		leftKey := downloadPeerKey(prioritized[i])
		rightKey := downloadPeerKey(prioritized[j])
		left := liveNextPeerRankFor(prioritized[i], preferredKey, states[leftKey], leases[leftKey], now)
		right := liveNextPeerRankFor(prioritized[j], preferredKey, states[rightKey], leases[rightKey], now)

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

func (s *overlaySubscription) liveNextPeerSnapshot(peers []*overlayPeer) map[string]liveNextPeerState {
	s.chainDownloadMx.Lock()
	defer s.chainDownloadMx.Unlock()

	if len(s.liveNextPeers) == 0 {
		return nil
	}

	snapshot := make(map[string]liveNextPeerState, len(peers))
	for _, peer := range peers {
		key := downloadPeerKey(peer)
		if state := s.liveNextPeers[key]; state != nil {
			snapshot[key] = *state
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

func liveNextPeerRankFor(peer *overlayPeer, preferredKey string, state liveNextPeerState, leases int, now time.Time) liveNextPeerRank {
	key := downloadPeerKey(peer)
	stats := peer.statsSnapshot()
	rank := liveNextPeerRank{
		preferred:     preferredKey != "" && key == preferredKey,
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
		leases := n.downloadPeerLeases[downloadPeerKey(probe.candidate.peer)]
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
	selectedKey := downloadPeerKey(selected.candidate.peer)
	if n.downloadPeerLeases == nil {
		n.downloadPeerLeases = map[string]int{}
	}
	n.downloadPeerLeases[selectedKey]++

	return selected, func() {
		n.downloadPeerMx.Lock()
		defer n.downloadPeerMx.Unlock()

		count := n.downloadPeerLeases[selectedKey]
		if count <= 1 {
			delete(n.downloadPeerLeases, selectedKey)
			return
		}
		n.downloadPeerLeases[selectedKey] = count - 1
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
