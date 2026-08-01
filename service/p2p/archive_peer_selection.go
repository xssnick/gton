package p2p

import (
	"sort"
	"time"

	"github.com/xssnick/gton/service/archive"
)

// archivePeerPerformance is owned by the archive pool. Archive traffic must
// never update overlayPeer stats because those stats drive the ordinary live
// and get-next peer selection.
type archivePeerPerformance struct {
	probeSuccesses   uint64
	archiveDownloads uint64
	bytes            int64
	downloadElapsed  time.Duration
	probeBytesPerSec float64
}

func (p archivePeerPerformance) bytesPerSecond() float64 {
	if p.probeBytesPerSec > 0 {
		return p.probeBytesPerSec
	}
	if p.bytes > 0 && p.downloadElapsed > 0 {
		return float64(p.bytes) / p.downloadElapsed.Seconds()
	}
	return 0
}

func prioritizeArchivePeersWithPerformance(_ archive.ShardID, peers []*overlayPeer, leases map[PeerID]int, performance map[PeerID]archivePeerPerformance) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}

	type rankedArchivePeer struct {
		peer     *overlayPeer
		tier     int
		valuable bool
		rate     float64
		leases   int
		score    float64
	}
	ranked := make([]rankedArchivePeer, len(peers))
	for i, peer := range peers {
		local := performance[peer.id]
		tier := 1
		if local.probeSuccesses > 0 || local.archiveDownloads > 0 {
			tier = 0
		}
		peerLeases := leases[peer.id]
		rate := local.bytesPerSecond()
		ranked[i] = rankedArchivePeer{
			peer:     peer,
			tier:     tier,
			valuable: local.archiveDownloads > 0,
			rate:     rate,
			leases:   peerLeases,
			score:    rate / float64(peerLeases+1),
		}
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		left := ranked[i]
		right := ranked[j]
		if left.tier != right.tier {
			return left.tier < right.tier
		}
		if left.score != right.score {
			return left.score > right.score
		}
		if left.valuable != right.valuable {
			return left.valuable
		}
		if left.leases != right.leases {
			return left.leases < right.leases
		}
		return left.rate > right.rate
	})

	ordered := make([]*overlayPeer, len(ranked))
	for i, entry := range ranked {
		ordered[i] = entry.peer
	}
	return ordered
}

func archiveSpeedSampleReliable(bytes int64) bool {
	return bytes >= int64(archiveSliceProbeSize)
}
