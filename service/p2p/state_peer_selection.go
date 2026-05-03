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
