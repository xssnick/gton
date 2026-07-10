package p2p

import (
	"math"
	"sort"
	"time"
)

const (
	stateSlowPeerSpeed   = float64(1 << 20)
	stateSlowPeerPenalty = largeDownloadSlowPeerPenalty
)

func downloadPeerID(peer *overlayPeer) PeerID {
	if peer == nil {
		return PeerID{}
	}
	return peer.id
}

type peerUse struct {
	downloads int
	pins      int
}

func (n *Node) acquireDownloadPeer(peer *overlayPeer) func() {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return func() {}
	}

	n.peerUseMx.Lock()
	if n.peerUse == nil {
		n.peerUse = map[PeerID]peerUse{}
	}
	use := n.peerUse[peerID]
	use.downloads++
	n.peerUse[peerID] = use
	n.peerUseMx.Unlock()

	return func() {
		n.peerUseMx.Lock()
		defer n.peerUseMx.Unlock()

		use := n.peerUse[peerID]
		if use.downloads > 0 {
			use.downloads--
		}
		if use.downloads == 0 && use.pins == 0 {
			delete(n.peerUse, peerID)
			return
		}
		n.peerUse[peerID] = use
	}
}

func (n *Node) downloadPeerLeaseCount(peer *overlayPeer) int {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return 0
	}

	n.peerUseMx.RLock()
	defer n.peerUseMx.RUnlock()
	return n.peerUse[peerID].downloads
}

func (n *Node) downloadPeerLeaseSnapshot(peers []*overlayPeer) map[PeerID]int {
	leases := make(map[PeerID]int, len(peers))
	n.peerUseMx.RLock()
	defer n.peerUseMx.RUnlock()
	for _, peer := range peers {
		peerID := downloadPeerID(peer)
		if peerID.IsZero() {
			continue
		}
		leases[peerID] = n.peerUse[peerID].downloads
	}
	return leases
}

func (n *Node) protectedPeerIDs() map[PeerID]struct{} {
	n.peerUseMx.RLock()
	defer n.peerUseMx.RUnlock()

	if len(n.peerUse) == 0 {
		return nil
	}

	protected := make(map[PeerID]struct{}, len(n.peerUse))
	for peerID, use := range n.peerUse {
		if use.downloads > 0 || use.pins > 0 {
			protected[peerID] = struct{}{}
		}
	}
	return protected
}

func (n *Node) pinPeer(peerID PeerID) func() {
	if peerID.IsZero() {
		return func() {}
	}

	n.peerUseMx.Lock()
	if n.peerUse == nil {
		n.peerUse = map[PeerID]peerUse{}
	}
	use := n.peerUse[peerID]
	use.pins++
	n.peerUse[peerID] = use
	n.peerUseMx.Unlock()

	return func() {
		n.peerUseMx.Lock()
		defer n.peerUseMx.Unlock()

		use := n.peerUse[peerID]
		if use.pins > 0 {
			use.pins--
		}
		if use.downloads == 0 && use.pins == 0 {
			delete(n.peerUse, peerID)
			return
		}
		n.peerUse[peerID] = use
	}
}

func (n *Node) prioritizeStateSnapshotPeers(peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	leases := n.downloadPeerLeaseSnapshot(peers)
	now := time.Now()

	type rankedStatePeer struct {
		peer   *overlayPeer
		slow   bool
		score  float64
		leases int
	}
	ranked := make([]rankedStatePeer, len(peers))
	for i, peer := range peers {
		stats := peer.statsSnapshot()
		peerLeases := leases[peer.id]
		ranked[i] = rankedStatePeer{
			peer:   peer,
			slow:   stats.downloadSlowUntil.After(now),
			score:  downloadPeerScore(stats, statePeerSpeed(stats), peerLeases, now),
			leases: peerLeases,
		}
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		left := ranked[i]
		right := ranked[j]

		if left.slow != right.slow {
			return !left.slow
		}
		if left.score != right.score {
			return left.score > right.score
		}
		if left.leases != right.leases {
			return left.leases < right.leases
		}
		return false
	})

	prioritized := make([]*overlayPeer, len(ranked))
	for i, entry := range ranked {
		prioritized[i] = entry.peer
	}
	return prioritized
}

func (n *Node) prioritizeBlockDownloadPeers(peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	leases := n.downloadPeerLeaseSnapshot(peers)
	now := time.Now()

	type rankedBlockPeer struct {
		peer     *overlayPeer
		slow     bool
		score    float64
		leases   int
		bytesSec float64
	}
	ranked := make([]rankedBlockPeer, len(peers))
	for i, peer := range peers {
		stats := peer.statsSnapshot()
		peerLeases := leases[peer.id]
		ranked[i] = rankedBlockPeer{
			peer:     peer,
			slow:     stats.downloadSlowUntil.After(now),
			score:    downloadPeerScore(stats, blockPeerSpeed(stats), peerLeases, now),
			leases:   peerLeases,
			bytesSec: stats.downloadBytesSec,
		}
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		left := ranked[i]
		right := ranked[j]

		if left.slow != right.slow {
			return !left.slow
		}
		if left.score != right.score {
			return left.score > right.score
		}
		if left.leases != right.leases {
			return left.leases < right.leases
		}
		if left.bytesSec != right.bytesSec {
			return left.bytesSec > right.bytesSec
		}
		return false
	})

	prioritized := make([]*overlayPeer, len(ranked))
	for i, entry := range ranked {
		prioritized[i] = entry.peer
	}
	return prioritized
}

func (s *overlaySubscription) prioritizeLiveNextPeers(peers []*overlayPeer, preferredPeerID PeerID, now time.Time) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	states := s.liveNextPeerSnapshot(peers)
	leases := s.node.downloadPeerLeaseSnapshot(peers)

	// Rank once per peer (one statsSnapshot each) instead of per comparison.
	type rankedLiveNextPeer struct {
		peer *overlayPeer
		rank liveNextPeerRank
	}
	ranked := make([]rankedLiveNextPeer, len(peers))
	for i, peer := range peers {
		ranked[i] = rankedLiveNextPeer{
			peer: peer,
			rank: liveNextPeerRankFor(peer, preferredPeerID, states[peer.id], leases[peer.id], now),
		}
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		left := ranked[i].rank
		right := ranked[j].rank

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

	prioritized := make([]*overlayPeer, len(ranked))
	for i, entry := range ranked {
		prioritized[i] = entry.peer
	}
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

	n.peerUseMx.Lock()
	defer n.peerUseMx.Unlock()

	bestIdx := 0
	bestScore := math.Inf(-1)
	bestLeases := 0
	bestSpeed := 0.0

	for i, probe := range probes {
		leases := n.peerUse[downloadPeerID(probe.candidate.peer)].downloads
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
	if n.peerUse == nil {
		n.peerUse = map[PeerID]peerUse{}
	}
	use := n.peerUse[selectedPeerID]
	use.downloads++
	n.peerUse[selectedPeerID] = use

	return selected, func() {
		n.peerUseMx.Lock()
		defer n.peerUseMx.Unlock()

		use := n.peerUse[selectedPeerID]
		if use.downloads > 0 {
			use.downloads--
		}
		if use.downloads == 0 && use.pins == 0 {
			delete(n.peerUse, selectedPeerID)
			return
		}
		n.peerUse[selectedPeerID] = use
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
