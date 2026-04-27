package p2p

import (
	"flexserver/service/archive"
	"sort"
	"time"
)

func (n *Node) acquireArchivePeer(peer *overlayPeer) func() {
	if peer == nil {
		return func() {}
	}

	key := archivePeerKey(peer)

	n.archivePeerLeasesMx.Lock()
	n.archivePeerLeases[key]++
	n.archivePeerLeasesMx.Unlock()

	return func() {
		n.archivePeerLeasesMx.Lock()
		defer n.archivePeerLeasesMx.Unlock()

		count := n.archivePeerLeases[key]
		if count <= 1 {
			delete(n.archivePeerLeases, key)
			return
		}
		n.archivePeerLeases[key] = count - 1
	}
}

func (n *Node) prioritizeArchivePeers(shard archive.ShardID, peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	n.archivePeerLeasesMx.RLock()
	leases := make(map[string]int, len(peers))
	for _, peer := range peers {
		leases[archivePeerKey(peer)] = n.archivePeerLeases[archivePeerKey(peer)]
	}
	n.archivePeerLeasesMx.RUnlock()

	now := time.Now()
	basechain := shard.Workchain == 0
	prioritized := append([]*overlayPeer(nil), peers...)
	sort.SliceStable(prioritized, func(i, j int) bool {
		left := prioritized[i]
		right := prioritized[j]

		leftStats := left.statsSnapshot()
		rightStats := right.statsSnapshot()

		leftLeases := leases[archivePeerKey(left)]
		rightLeases := leases[archivePeerKey(right)]

		leftTier := archivePeerTier(leftStats, basechain, now)
		rightTier := archivePeerTier(rightStats, basechain, now)
		if leftTier != rightTier {
			return leftTier < rightTier
		}

		leftSpeed := archivePeerSpeed(leftStats, basechain)
		rightSpeed := archivePeerSpeed(rightStats, basechain)
		leftScore := archivePeerScore(leftStats, leftSpeed, leftLeases, now)
		rightScore := archivePeerScore(rightStats, rightSpeed, rightLeases, now)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		if leftLeases != rightLeases {
			return leftLeases < rightLeases
		}
		if leftStats.archiveBytesSec != rightStats.archiveBytesSec {
			return leftStats.archiveBytesSec > rightStats.archiveBytesSec
		}
		return false
	})
	return prioritized
}

func archivePeerTier(stats peerStats, basechain bool, now time.Time) int {
	if stats.archiveSlowUntil.After(now) {
		return 3
	}
	if !basechain {
		return 0
	}
	if stats.archiveDownloads == 0 {
		return 2
	}
	if stats.archiveBytesSec >= archiveSlowThreshold(basechain) {
		return 0
	}
	return 1
}

func archivePeerSpeed(stats peerStats, basechain bool) float64 {
	if stats.archiveBytesSec > 0 {
		return stats.archiveBytesSec
	}
	if basechain {
		return archiveUnknownPeerSpeed
	}
	return defaultArchivePeerSpeed
}

func archivePeerScore(stats peerStats, speed float64, leases int, now time.Time) float64 {
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
	for _, peer := range peers {
		if archivePeerKey(peer) != stickyKey {
			continue
		}

		stats := peer.statsSnapshot()
		if stats.archiveSlowUntil.After(now) || !stats.alive {
			delete(s.archivePeers, stateKey)
			return nil
		}

		state.peer = peer
		if state.speed == 0 && stats.archiveBytesSec > 0 {
			state.speed = stats.archiveBytesSec
		}
		return peer
	}

	delete(s.archivePeers, stateKey)
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
	var sticky *overlayPeer
	for _, peer := range peers {
		if archivePeerKey(peer) == stickyKey {
			sticky = peer
			break
		}
	}
	if sticky == nil {
		delete(s.archivePeers, stateKey)
		return peers[0]
	}

	stats := sticky.statsSnapshot()
	if stats.archiveSlowUntil.After(now) || !stats.alive {
		delete(s.archivePeers, stateKey)
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
		currentSpeed = stats.archiveBytesSec
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

	delete(s.archivePeers, stateKey)
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

func archivePeerKey(peer *overlayPeer) string {
	if peer == nil {
		return ""
	}
	if peer.id != "" {
		return peer.id
	}
	return peer.addr
}
