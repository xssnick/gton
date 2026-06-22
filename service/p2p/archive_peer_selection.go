package p2p

import (
	"sort"
	"time"

	"github.com/xssnick/gton/service/archive"
)

func prioritizeArchivePeersWithLeases(shard archive.ShardID, peers []*overlayPeer, leases map[PeerID]int) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

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

func noteArchivePeerSeedSuccess(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration) {
	if peer == nil || bytes <= 0 || elapsed <= 0 {
		return
	}

	if !archiveSpeedSampleReliable(bytes) {
		peer.querySuccess(0)
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

	peer.querySuccess(0)
	if archiveDownloadTooSlow(shard, bytes, elapsed) {
		peer.downloadFailed(archiveSlowPeerPenalty)
		return true
	}
	return false
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
