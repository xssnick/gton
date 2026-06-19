package p2p

import "time"

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
