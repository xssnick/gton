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

	// Decorate-sort-undecorate: one statsSnapshot and one score computation per
	// peer instead of per comparison, which also keeps the comparator
	// consistent when stats change mid-sort.
	type rankedArchivePeer struct {
		peer          *overlayPeer
		tier          int
		largeCapacity bool
		score         float64
		speed         float64
		leases        int
	}
	ranked := make([]rankedArchivePeer, len(peers))
	for i, peer := range peers {
		stats := peer.statsSnapshot()
		peerLeases := leases[peer.id]
		speed := archivePeerSpeed(stats, basechain)
		ranked[i] = rankedArchivePeer{
			peer:          peer,
			tier:          archivePeerTier(stats, basechain, now),
			largeCapacity: archivePeerHasAvailableLargeCapacity(stats, peerLeases, basechain),
			score:         archiveDownloadPeerScore(stats, speed, peerLeases, basechain, now),
			speed:         speed,
			leases:        peerLeases,
		}
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		left := ranked[i]
		right := ranked[j]

		if left.tier != right.tier {
			return left.tier < right.tier
		}
		if left.largeCapacity != right.largeCapacity {
			return left.largeCapacity
		}
		if left.score != right.score {
			return left.score > right.score
		}
		if left.leases != right.leases {
			return left.leases < right.leases
		}
		return left.speed > right.speed
	})

	prioritized := make([]*overlayPeer, len(ranked))
	for i, entry := range ranked {
		prioritized[i] = entry.peer
	}
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

	peer.downloadSuccess(bytes, elapsed, 0, archiveSlowPeerPenalty)
}

func noteArchivePeerDownload(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration) {
	if peer == nil || bytes <= 0 || elapsed <= 0 {
		return
	}

	if archiveSpeedSampleReliable(bytes) {
		peer.downloadSuccess(bytes, elapsed, 0, archiveSlowPeerPenalty)
		peer.archiveLargeDownloadSuccess(bytes, elapsed)
		return
	}

	peer.querySuccess(0)
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
