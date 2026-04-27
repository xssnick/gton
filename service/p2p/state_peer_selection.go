package p2p

import (
	"math"
	"sort"
)

func (n *Node) acquireStateSnapshotPeer(peer *overlayPeer) func() {
	if peer == nil {
		return func() {}
	}

	n.statePeerLeasesMx.Lock()
	n.statePeerLeases[peer.addr]++
	n.statePeerLeasesMx.Unlock()

	return func() {
		n.statePeerLeasesMx.Lock()
		defer n.statePeerLeasesMx.Unlock()

		count := n.statePeerLeases[peer.addr]
		if count <= 1 {
			delete(n.statePeerLeases, peer.addr)
			return
		}
		n.statePeerLeases[peer.addr] = count - 1
	}
}

func (n *Node) stateSnapshotPeerLeases(peer *overlayPeer) int {
	if peer == nil {
		return 0
	}

	n.statePeerLeasesMx.RLock()
	defer n.statePeerLeasesMx.RUnlock()
	return n.statePeerLeases[peer.addr]
}

func (n *Node) prioritizeStateSnapshotPeers(peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	n.statePeerLeasesMx.RLock()
	hasActiveDownloads := false
	for _, peer := range peers {
		if n.statePeerLeases[peer.addr] > 0 {
			hasActiveDownloads = true
			break
		}
	}
	if !hasActiveDownloads {
		n.statePeerLeasesMx.RUnlock()
		return peers
	}

	leases := make(map[string]int, len(peers))
	for _, peer := range peers {
		leases[peer.addr] = n.statePeerLeases[peer.addr]
	}
	n.statePeerLeasesMx.RUnlock()

	prioritized := append([]*overlayPeer(nil), peers...)

	sort.SliceStable(prioritized, func(i, j int) bool {
		return leases[prioritized[i].addr] < leases[prioritized[j].addr]
	})
	return prioritized
}

func (n *Node) acquirePreferredStateSnapshotProbe(probes []persistentStatePeerProbe) (persistentStatePeerProbe, func()) {
	if len(probes) == 0 {
		return persistentStatePeerProbe{}, func() {}
	}

	n.statePeerLeasesMx.Lock()
	defer n.statePeerLeasesMx.Unlock()

	bestIdx := 0
	bestScore := math.Inf(-1)
	bestLeases := 0
	bestSpeed := 0.0

	for i, probe := range probes {
		leases := n.statePeerLeases[probe.candidate.peer.addr]
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
	n.statePeerLeases[selected.candidate.peer.addr]++

	return selected, func() {
		n.statePeerLeasesMx.Lock()
		defer n.statePeerLeasesMx.Unlock()

		count := n.statePeerLeases[selected.candidate.peer.addr]
		if count <= 1 {
			delete(n.statePeerLeases, selected.candidate.peer.addr)
			return
		}
		n.statePeerLeases[selected.candidate.peer.addr] = count - 1
	}
}
