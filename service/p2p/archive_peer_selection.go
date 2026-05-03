package p2p

import (
	"flexserver/service/archive"
	"sort"
	"time"
)

func (n *Node) prioritizeArchivePeers(shard archive.ShardID, peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	leases := n.downloadPeerLeaseSnapshot(peers)
	now := time.Now()
	basechain := shard.Workchain == 0
	prioritized := append([]*overlayPeer(nil), peers...)
	sort.SliceStable(prioritized, func(i, j int) bool {
		left := prioritized[i]
		right := prioritized[j]

		leftStats := left.statsSnapshot()
		rightStats := right.statsSnapshot()

		leftLeases := leases[downloadPeerKey(left)]
		rightLeases := leases[downloadPeerKey(right)]

		leftTier := archivePeerTier(leftStats, basechain, now)
		rightTier := archivePeerTier(rightStats, basechain, now)
		if leftTier != rightTier {
			return leftTier < rightTier
		}

		leftSpeed := archivePeerSpeed(leftStats, basechain)
		rightSpeed := archivePeerSpeed(rightStats, basechain)
		leftScore := downloadPeerScore(leftStats, leftSpeed, leftLeases, now)
		rightScore := downloadPeerScore(rightStats, rightSpeed, rightLeases, now)
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

func (s *overlaySubscription) archiveQueryCandidates() []*overlayPeer {
	return s.aliveNeighbourPeers(0, 0)
}

func archivePeerTier(stats peerStats, basechain bool, now time.Time) int {
	if stats.downloadSlowUntil.After(now) {
		return 3
	}
	if !basechain {
		return 0
	}
	if stats.downloadCount == 0 {
		return 2
	}
	if stats.downloadBytesSec >= archiveSlowThreshold(basechain) {
		return 0
	}
	return 1
}

func archivePeerSpeed(stats peerStats, basechain bool) float64 {
	if stats.downloadBytesSec > 0 {
		return stats.downloadBytesSec
	}
	if basechain {
		return archiveUnknownPeerSpeed
	}
	return defaultArchivePeerSpeed
}

func downloadPeerScore(stats peerStats, speed float64, leases int, now time.Time) float64 {
	score := speed / float64(leases+1)
	if stats.unreliability > 0 {
		score /= 1 + stats.unreliability
	}
	if stats.roundtrip > 0 {
		score /= 1 + stats.roundtrip.Seconds()
	}
	if !stats.lastSuccessAt.IsZero() && now.Sub(stats.lastSuccessAt) > 30*time.Second {
		score *= 0.5
	}
	if !stats.alive {
		score *= 0.25
	}
	return score
}

func (s *overlaySubscription) currentArchivePeer(shard archive.ShardID, peers []*overlayPeer) *overlayPeer {
	if len(peers) == 0 {
		return nil
	}

	now := time.Now()
	stateKey := archivePeerPoolKey(shard)

	s.archivePeerMx.Lock()
	defer s.archivePeerMx.Unlock()

	state := s.archivePeerState(stateKey)
	if state.peer == nil {
		return nil
	}

	stickyKey := archivePeerKey(state.peer)
	if archivePeerDeniedLocked(state, stickyKey, now) {
		s.clearArchivePreferredPeerLocked(stateKey, state)
		return nil
	}

	for _, peer := range peers {
		if archivePeerKey(peer) != stickyKey {
			continue
		}

		stats := peer.statsSnapshot()
		if stats.downloadSlowUntil.After(now) || !stats.alive {
			s.clearArchivePreferredPeerLocked(stateKey, state)
			return nil
		}

		state.peer = peer
		if state.speed == 0 && stats.downloadBytesSec > 0 {
			state.speed = stats.downloadBytesSec
		}
		return peer
	}

	s.clearArchivePreferredPeerLocked(stateKey, state)
	return nil
}

func (s *overlaySubscription) chooseArchivePeer(shard archive.ShardID, peers []*overlayPeer) *overlayPeer {
	if len(peers) == 0 {
		return nil
	}

	now := time.Now()
	stateKey := archivePeerPoolKey(shard)

	s.archivePeerMx.Lock()
	defer s.archivePeerMx.Unlock()

	state := s.archivePeerState(stateKey)
	if state.peer == nil {
		return peers[0]
	}

	stickyKey := archivePeerKey(state.peer)
	if archivePeerDeniedLocked(state, stickyKey, now) {
		s.clearArchivePreferredPeerLocked(stateKey, state)
		return peers[0]
	}

	var sticky *overlayPeer
	for _, peer := range peers {
		if archivePeerKey(peer) == stickyKey {
			sticky = peer
			break
		}
	}
	if sticky == nil {
		s.clearArchivePreferredPeerLocked(stateKey, state)
		return peers[0]
	}

	stats := sticky.statsSnapshot()
	if stats.downloadSlowUntil.After(now) || !stats.alive {
		s.clearArchivePreferredPeerLocked(stateKey, state)
		return peers[0]
	}

	state.peer = sticky
	if state.probeAt.IsZero() {
		state.probeAt = now.Add(archiveStickyProbeInterval)
		return sticky
	}
	if now.Before(state.probeAt) {
		return sticky
	}

	state.probeAt = now.Add(archiveStickyProbeInterval)
	for _, peer := range peers {
		if archivePeerKey(peer) != stickyKey {
			s.log.Debug().
				Str("current_peer", sticky.addr).
				Str("probe_peer", peer.addr).
				Str("archive_pool", stateKey).
				Msg("probing alternative archive peer")
			return peer
		}
	}
	return sticky
}

func (s *overlaySubscription) noteArchivePeerSuccess(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration) {
	if peer == nil || bytes <= 0 || elapsed <= 0 {
		return
	}

	speed := float64(bytes) / elapsed.Seconds()
	now := time.Now()
	peerKey := archivePeerKey(peer)
	stateKey := archivePeerPoolKey(shard)
	slowThreshold := archiveSlowThreshold(shard.Workchain == 0)

	s.archivePeerMx.Lock()
	defer s.archivePeerMx.Unlock()

	state := s.archivePeerState(stateKey)
	if state.peer == nil {
		state.peer = peer
		state.speed = speed
		state.probeAt = now.Add(archiveStickyProbeInterval)

		s.log.Debug().
			Str("peer", peer.addr).
			Str("speed", formatByteRate(bytes, elapsed)).
			Str("archive_pool", stateKey).
			Msg("selected archive peer")
		return
	}

	currentKey := archivePeerKey(state.peer)
	if currentKey == peerKey {
		state.peer = peer
		if state.speed == 0 {
			state.speed = speed
		} else {
			state.speed = state.speed*0.7 + speed*0.3
		}
		state.probeAt = now.Add(archiveStickyProbeInterval)
		return
	}

	currentSpeed := state.speed
	if currentSpeed == 0 {
		stats := state.peer.statsSnapshot()
		currentSpeed = stats.downloadBytesSec
	}
	if currentSpeed == 0 {
		currentSpeed = defaultArchivePeerSpeed
	}
	if speed < currentSpeed*archiveStickySwitchRatio && currentSpeed >= slowThreshold {
		return
	}

	oldPeer := state.peer
	state.peer = peer
	state.speed = speed
	state.probeAt = now.Add(archiveStickyProbeInterval)

	s.log.Debug().
		Str("old_peer", oldPeer.addr).
		Str("peer", peer.addr).
		Str("old_speed", formatByteRate(int64(currentSpeed), time.Second)).
		Str("speed", formatByteRate(bytes, elapsed)).
		Str("archive_pool", stateKey).
		Msg("switched archive peer")
}

func (s *overlaySubscription) noteArchivePeerFailure(shard archive.ShardID, peer *overlayPeer) {
	if peer == nil {
		return
	}

	peerKey := archivePeerKey(peer)
	stateKey := archivePeerPoolKey(shard)

	s.archivePeerMx.Lock()
	defer s.archivePeerMx.Unlock()

	state := s.archivePeers[stateKey]
	if state == nil || state.peer == nil || archivePeerKey(state.peer) != peerKey {
		return
	}

	s.log.Debug().
		Str("peer", peer.addr).
		Str("archive_pool", stateKey).
		Msg("archive peer failed, clearing preferred peer")

	s.clearArchivePreferredPeerLocked(stateKey, state)
}

func (s *overlaySubscription) archivePeerState(key string) *archivePeerState {
	if s.archivePeers == nil {
		s.archivePeers = map[string]*archivePeerState{}
	}
	state := s.archivePeers[key]
	if state == nil {
		state = &archivePeerState{}
		s.archivePeers[key] = state
	}
	return state
}

func (s *overlaySubscription) availableArchivePeers(shard archive.ShardID, peers []*overlayPeer) []*overlayPeer {
	if len(peers) == 0 {
		return nil
	}

	now := time.Now()
	stateKey := archivePeerPoolKey(shard)

	s.archivePeerMx.Lock()
	defer s.archivePeerMx.Unlock()

	state := s.archivePeers[stateKey]
	if state == nil || len(state.deniedPeers) == 0 {
		return peers
	}

	available := make([]*overlayPeer, 0, len(peers))
	for _, peer := range peers {
		if !archivePeerDeniedLocked(state, archivePeerKey(peer), now) {
			available = append(available, peer)
		}
	}
	if len(state.deniedPeers) == 0 && state.peer == nil {
		delete(s.archivePeers, stateKey)
	}
	return available
}

func (s *overlaySubscription) denyArchivePeer(shard archive.ShardID, peer *overlayPeer, reason string) {
	if peer == nil {
		return
	}

	until := time.Now().Add(archiveSlowPeerPenalty)
	stateKey := archivePeerPoolKey(shard)
	peerKey := archivePeerKey(peer)

	s.archivePeerMx.Lock()
	state := s.archivePeerState(stateKey)
	if state.deniedPeers == nil {
		state.deniedPeers = map[string]time.Time{}
	}
	state.deniedPeers[peerKey] = until
	if state.peer != nil && archivePeerKey(state.peer) == peerKey {
		s.clearArchivePreferredPeerLocked(stateKey, state)
	}
	s.archivePeerMx.Unlock()

	s.log.Debug().
		Str("peer", peer.addr).
		Str("archive_pool", stateKey).
		Str("reason", reason).
		Dur("duration", archiveSlowPeerPenalty).
		Msg("temporarily denied archive peer")
}

func (s *overlaySubscription) archivePeerDenied(shard archive.ShardID, peer *overlayPeer) bool {
	if peer == nil {
		return false
	}

	now := time.Now()
	stateKey := archivePeerPoolKey(shard)

	s.archivePeerMx.Lock()
	defer s.archivePeerMx.Unlock()

	state := s.archivePeers[stateKey]
	if state == nil {
		return false
	}
	denied := archivePeerDeniedLocked(state, archivePeerKey(peer), now)
	if len(state.deniedPeers) == 0 && state.peer == nil {
		delete(s.archivePeers, stateKey)
	}
	return denied
}

func (s *overlaySubscription) clearArchivePreferredPeerLocked(stateKey string, state *archivePeerState) {
	if state == nil {
		return
	}

	state.peer = nil
	state.speed = 0
	state.probeAt = time.Time{}
	if len(state.deniedPeers) == 0 {
		delete(s.archivePeers, stateKey)
	}
}

func archivePeerDeniedLocked(state *archivePeerState, peerKey string, now time.Time) bool {
	if state == nil || len(state.deniedPeers) == 0 {
		return false
	}

	until, ok := state.deniedPeers[peerKey]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}

	delete(state.deniedPeers, peerKey)
	return false
}

func archivePeerPoolKey(shard archive.ShardID) string {
	if shard.Workchain == -1 {
		return "master"
	}
	return shard.String()
}

func archiveSlowThreshold(basechain bool) float64 {
	if basechain {
		return basechainSlowPeerSpeed
	}
	return archiveSlowPeerSpeed
}

func archiveGoodThreshold(basechain bool) float64 {
	if basechain {
		return basechainGoodPeerSpeed
	}
	return archiveGoodPeerSpeed
}

func shouldRaceArchiveDownload(shard archive.ShardID, peers []*overlayPeer) bool {
	if len(peers) < 2 {
		return false
	}

	stats := peers[0].statsSnapshot()
	now := time.Now()
	if !stats.alive || stats.downloadSlowUntil.After(now) {
		return true
	}
	if stats.downloadCount == 0 {
		return true
	}
	return stats.downloadBytesSec < archiveGoodThreshold(shard.Workchain == 0)
}

func shouldUseCurrentArchivePeerWithoutRace(shard archive.ShardID, peer *overlayPeer) bool {
	if peer == nil {
		return false
	}

	stats := peer.statsSnapshot()
	if !stats.alive || stats.downloadSlowUntil.After(time.Now()) {
		return false
	}
	if stats.downloadCount == 0 {
		return false
	}
	return stats.downloadBytesSec >= archiveGoodThreshold(shard.Workchain == 0)
}

func archivePeerKey(peer *overlayPeer) string {
	return downloadPeerKey(peer)
}
