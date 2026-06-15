package p2p

import (
	"context"
	"sort"
	"time"

	"github.com/xssnick/gton/service/archive"
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

		leftLeases := leases[left.id]
		rightLeases := leases[right.id]

		leftTier := archivePeerTier(leftStats, basechain, now)
		rightTier := archivePeerTier(rightStats, basechain, now)
		if leftTier != rightTier {
			return leftTier < rightTier
		}

		leftLarge := archivePeerHasAvailableLargeCapacity(leftStats, leftLeases, basechain)
		rightLarge := archivePeerHasAvailableLargeCapacity(rightStats, rightLeases, basechain)
		if leftLarge != rightLarge {
			return leftLarge
		}

		leftSpeed := archivePeerSpeed(leftStats, basechain)
		rightSpeed := archivePeerSpeed(rightStats, basechain)
		leftScore := archiveDownloadPeerScore(leftStats, leftSpeed, leftLeases, basechain, now)
		rightScore := archiveDownloadPeerScore(rightStats, rightSpeed, rightLeases, basechain, now)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		if leftLeases != rightLeases {
			return leftLeases < rightLeases
		}
		return leftSpeed > rightSpeed
	})
	return prioritized
}

func (s *overlaySubscription) archiveQueryCandidates() []*overlayPeer {
	return s.aliveNeighbourPeers(0, 0)
}

func (s *overlaySubscription) peerByAddr(addr string) *overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	for _, peer := range s.peers {
		if peer.addr == addr {
			return peer
		}
	}
	return nil
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
	if archivePeerSpeed(stats, basechain) >= archiveSlowThreshold(basechain) {
		return 0
	}
	return 1
}

func archivePeerSpeed(stats peerStats, basechain bool) float64 {
	if stats.archiveLargeBytesSec > 0 {
		return stats.archiveLargeBytesSec
	}
	if stats.downloadBytesSec > 0 {
		return stats.downloadBytesSec
	}
	if basechain {
		return archiveUnknownPeerSpeed
	}
	return defaultArchivePeerSpeed
}

func archivePeerHasLargeSpeed(stats peerStats, basechain bool) bool {
	return stats.archiveLargeBytesSec >= archiveSlowThreshold(basechain)
}

func archivePeerHasAvailableLargeCapacity(stats peerStats, leases int, basechain bool) bool {
	return archivePeerHasLargeSpeed(stats, basechain) && leases < archivePeerParallelCapacity(stats, basechain)
}

func archivePeerParallelCapacity(stats peerStats, basechain bool) int {
	if !basechain || stats.archiveLargeBytesSec <= 0 {
		return 1
	}
	if stats.archiveLargeBytesSec >= basechainVeryGoodPeerSpeed {
		return 3
	}
	if stats.archiveLargeBytesSec >= archiveGoodThreshold(basechain) {
		return 2
	}
	return 1
}

func archiveDownloadPeerScore(stats peerStats, speed float64, leases int, basechain bool, now time.Time) float64 {
	return downloadPeerScoreWithCapacity(stats, speed, leases, archivePeerParallelCapacity(stats, basechain), now)
}

func downloadPeerScore(stats peerStats, speed float64, leases int, now time.Time) float64 {
	return downloadPeerScoreWithCapacity(stats, speed, leases, 1, now)
}

func downloadPeerScoreWithCapacity(stats peerStats, speed float64, leases int, capacity int, now time.Time) float64 {
	if capacity < 1 {
		capacity = 1
	}

	score := speed / (1 + float64(leases)/float64(capacity))
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

func noteArchivePeerSeedSuccess(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration) {
	if !archiveSpeedSampleReliable(bytes) {
		return
	}

	peer.downloadSuccess(bytes, elapsed, archiveSlowThreshold(shard.Workchain == 0), archiveSlowPeerPenalty)
}

func noteArchivePeerDownload(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration) bool {
	if peer == nil || bytes <= 0 || elapsed <= 0 {
		return false
	}

	if archiveSpeedSampleReliable(bytes) {
		peer.downloadSuccess(bytes, elapsed, archiveSlowThreshold(shard.Workchain == 0), archiveSlowPeerPenalty)
		peer.archiveLargeDownloadSuccess(bytes, elapsed)
		return archiveDownloadTooSlow(shard, bytes, elapsed)
	}

	if archiveDownloadTooSlow(shard, bytes, elapsed) {
		peer.downloadFailed(archiveSlowPeerPenalty)
		return true
	}
	return false
}

func (s *overlaySubscription) archivePeerPoolState(key string) *archivePeerPoolState {
	if s.archivePeers == nil {
		s.archivePeers = map[string]*archivePeerPoolState{}
	}
	state := s.archivePeers[key]
	if state == nil {
		state = &archivePeerPoolState{}
		s.archivePeers[key] = state
	}
	return state
}

func archivePeerPoolStateEmpty(state *archivePeerPoolState) bool {
	return state == nil || len(state.cooldownUntil) == 0
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
	if state == nil || len(state.cooldownUntil) == 0 {
		return peers
	}

	available := make([]*overlayPeer, 0, len(peers))
	for _, peer := range peers {
		if !archivePeerCoolingDownLocked(state, archivePeerID(peer), now) {
			available = append(available, peer)
		}
	}
	if archivePeerPoolStateEmpty(state) {
		delete(s.archivePeers, stateKey)
	}
	return available
}

func (s *overlaySubscription) refreshEmptyArchivePeerPool(ctx context.Context, shard archive.ShardID) {
	rotated := s.rotateUnavailableArchivePeers(shard)
	if rotated == 0 {
		return
	}

	if s.node != nil && s.node.dht != nil {
		s.forceSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)
	}
	s.reloadNeighbours()
}

func (s *overlaySubscription) rotateUnavailableArchivePeers(shard archive.ShardID) int {
	if s.node == nil {
		return 0
	}

	candidates := s.archiveQueryCandidates()
	if len(candidates) == 0 {
		return 0
	}
	if available := s.availableArchivePeers(shard, s.node.prioritizeArchivePeers(shard, candidates)); len(available) > 0 {
		return 0
	}

	protected := s.protectedNeighbourPeerIDs()
	rotated := 0
	for _, peer := range candidates {
		if peer == nil || peer.id.IsZero() {
			continue
		}
		if _, ok := protected[peer.id]; ok {
			continue
		}
		if !s.archivePeerCoolingDown(shard, peer) {
			continue
		}
		s.removePeer(peer.id)
		rotated++
	}
	if rotated == 0 {
		return 0
	}

	s.log.Info().
		Int("rotated_peers", rotated).
		Str("archive_pool", archivePeerPoolKey(shard)).
		Msg("rotated unavailable archive peers")
	return rotated
}

func (s *overlaySubscription) cooldownArchivePeer(shard archive.ShardID, peer *overlayPeer, reason string) {
	if peer == nil {
		return
	}

	until := time.Now().Add(archiveSlowPeerPenalty)
	stateKey := archivePeerPoolKey(shard)
	peerID := archivePeerID(peer)
	if peerID.IsZero() {
		return
	}

	s.archivePeerMx.Lock()
	state := s.archivePeerPoolState(stateKey)
	if state.cooldownUntil == nil {
		state.cooldownUntil = map[PeerID]time.Time{}
	}
	state.cooldownUntil[peerID] = until
	s.archivePeerMx.Unlock()

	s.log.Debug().
		Str("peer", peer.addr).
		Str("archive_pool", stateKey).
		Str("reason", reason).
		Dur("duration", archiveSlowPeerPenalty).
		Msg("temporarily cooled down archive peer")
}

func (s *overlaySubscription) archivePeerCoolingDown(shard archive.ShardID, peer *overlayPeer) bool {
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
	coolingDown := archivePeerCoolingDownLocked(state, archivePeerID(peer), now)
	if archivePeerPoolStateEmpty(state) {
		delete(s.archivePeers, stateKey)
	}
	return coolingDown
}

func archivePeerCoolingDownLocked(state *archivePeerPoolState, peerID PeerID, now time.Time) bool {
	if state == nil || len(state.cooldownUntil) == 0 {
		return false
	}
	if peerID.IsZero() {
		return false
	}

	until, ok := state.cooldownUntil[peerID]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}

	delete(state.cooldownUntil, peerID)
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

func archiveSpeedSampleReliable(bytes int64) bool {
	return bytes >= archiveSpeedSampleMinBytes
}

func archiveDownloadTooSlow(shard archive.ShardID, bytes int64, elapsed time.Duration) bool {
	if bytes <= 0 || elapsed <= 0 {
		return false
	}
	if archiveSpeedSampleReliable(bytes) {
		return float64(bytes)/elapsed.Seconds() < archiveSlowThreshold(shard.Workchain == 0)
	}
	return elapsed >= archiveSmallPackSlowElapsed
}

func archivePeerCanStayPinned(shard archive.ShardID, bytes int64, elapsed time.Duration) bool {
	return bytes > 0 && elapsed > 0 && !archiveDownloadTooSlow(shard, bytes, elapsed)
}

func archivePeerID(peer *overlayPeer) PeerID {
	return downloadPeerID(peer)
}
