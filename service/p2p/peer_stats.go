package p2p

import (
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type peerStats struct {
	versionMajor      int32
	versionMinor      int32
	capabilitiesFlags uint32
	roundtrip         time.Duration
	unreliability     float64
	alive             bool
	lastReceiveAt     time.Time
	lastSuccessAt     time.Time
	failedQueries     uint64
	downloadBytesSec  float64
	downloadCount     uint64
	downloadSlowUntil time.Time
}

func (p *overlayPeer) hasOpenConnection() bool {
	if p == nil || p.overlay == nil {
		return false
	}
	if p.overlay.ADNLWrapper == nil || p.overlay.ADNLWrapper.ADNL == nil {
		return true
	}

	select {
	case <-p.overlay.GetCloserCtx().Done():
		return false
	default:
		return true
	}
}

func (p *overlayPeer) isKnownOverlayPeer(now time.Time) bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return p.announced != nil && announcedNodeIsFresh(p.announced, now)
}

func (p *overlayPeer) isAliveKnownOverlayPeer(now time.Time) bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return p.announced != nil && announcedNodeIsFresh(p.announced, now) && p.alive
}

func (p *overlayPeer) canAdvertise(now time.Time) bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return p.announced != nil && announcedNodeIsFresh(p.announced, now) && p.alive
}

func (p *overlayPeer) mergeAnnouncement(v1 *overlay.Node) {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	if v1 != nil && (p.announced == nil || v1.Version >= p.announced.Version) {
		p.announced = cloneOverlayNode(v1)
	}
}

func (p *overlayPeer) statsSnapshot() peerStats {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return peerStats{
		versionMajor:      p.versionMajor,
		versionMinor:      p.versionMinor,
		capabilitiesFlags: p.capabilitiesFlags,
		roundtrip:         p.roundtrip,
		unreliability:     p.unreliability,
		alive:             p.alive,
		lastReceiveAt:     p.lastReceiveAt,
		lastSuccessAt:     p.lastSuccessAt,
		failedQueries:     p.failedQueries,
		downloadBytesSec:  p.downloadBytesSec,
		downloadCount:     p.downloadCount,
		downloadSlowUntil: p.downloadSlowUntil,
	}
}

func (p *overlayPeer) applyCapabilities(cap Capabilities) {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.versionMajor = cap.VersionMajor
	p.versionMinor = cap.VersionMinor
	p.capabilitiesFlags = cap.Flags
}

func (p *overlayPeer) querySuccess(rtt time.Duration) {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	now := time.Now()
	p.missedPings = 0
	p.alive = true
	p.lastReceiveAt = now
	p.lastSuccessAt = now
	p.unreliability--
	if p.unreliability < 0 {
		p.unreliability = 0
	}
	if rtt <= 0 {
		return
	}
	if p.roundtrip == 0 {
		p.roundtrip = rtt
		return
	}
	p.roundtrip = (p.roundtrip + rtt) / 2
}

func (p *overlayPeer) queryFailed() {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.missedPings++
	p.unreliability++
	p.failedQueries++
	if p.missedPings >= 3 && !p.lastReceiveAt.IsZero() && time.Since(p.lastReceiveAt) >= 15*time.Second {
		p.alive = false
	}
}

func (p *overlayPeer) downloadSuccess(bytes int64, elapsed time.Duration, slowThreshold float64, slowPenalty time.Duration) {
	if p == nil || bytes <= 0 || elapsed <= 0 {
		return
	}

	speed := float64(bytes) / elapsed.Seconds()

	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	now := time.Now()
	p.downloadCount++
	if p.downloadBytesSec == 0 {
		p.downloadBytesSec = speed
	} else {
		p.downloadBytesSec = p.downloadBytesSec*0.7 + speed*0.3
	}
	if speed < slowThreshold {
		p.downloadSlowUntil = now.Add(slowPenalty)
	} else {
		p.downloadSlowUntil = time.Time{}
	}
}

func (p *overlayPeer) downloadFailed(slowPenalty time.Duration) {
	if p == nil {
		return
	}

	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.downloadSlowUntil = time.Now().Add(slowPenalty)
}

func (p *overlayPeer) noteReceive() {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.missedPings = 0
	p.alive = true
	p.lastReceiveAt = time.Now()
}

func (p *overlayPeer) shouldStopQuerying() bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return p.unreliability > peerStopUnreliability
}

func announcedNodeIsFresh(node *overlay.Node, now time.Time) bool {
	if node == nil {
		return false
	}
	nodeTime := time.Unix(int64(node.Version), 0)
	return !nodeTime.Before(now.Add(-overlayPeerTTL)) && !nodeTime.After(now.Add(overlayFutureSkew))
}

func (s *overlaySubscription) preferredPeers(requiredVersionMajor, requiredVersionMinor int32, allow func(*overlayPeer, peerStats) bool) []*overlayPeer {
	candidates := make([]*overlayPeer, 0, len(s.peers))
	for _, peer := range s.knownPeersSnapshot() {
		if peer.overlay == nil {
			continue
		}

		stats := peer.statsSnapshot()
		if !peerEligible(stats, requiredVersionMajor, requiredVersionMinor) {
			continue
		}
		if allow != nil && !allow(peer, stats) {
			continue
		}
		candidates = append(candidates, peer)
	}
	return s.orderPreferredPeers(candidates, requiredVersionMajor, requiredVersionMinor)
}

func (s *overlaySubscription) orderPreferredPeers(candidates []*overlayPeer, requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	if len(candidates) == 0 {
		return nil
	}

	selected := make([]*overlayPeer, 0, len(candidates))
	remaining := append([]*overlayPeer(nil), candidates...)
	for len(remaining) > 0 {
		chosen := s.choosePeer(remaining, requiredVersionMajor, requiredVersionMinor)
		if chosen == nil {
			sortPeersByPreference(remaining)
			selected = append(selected, remaining...)
			break
		}

		selected = append(selected, chosen)
		for idx, peer := range remaining {
			if peer == chosen {
				remaining = append(remaining[:idx], remaining[idx+1:]...)
				break
			}
		}
	}
	return selected
}

func (s *overlaySubscription) choosePeer(peers []*overlayPeer, requiredVersionMajor, requiredVersionMinor int32) *overlayPeer {
	if len(peers) == 0 {
		return nil
	}

	minUnreliability := math.MaxFloat64
	for _, peer := range peers {
		stats := peer.statsSnapshot()
		if !peerEligible(stats, requiredVersionMajor, requiredVersionMinor) {
			continue
		}
		if stats.unreliability < minUnreliability {
			minUnreliability = stats.unreliability
		}
	}
	if minUnreliability == math.MaxFloat64 {
		return nil
	}
	minRoundtrip := minPeerRoundtrip(peers, requiredVersionMajor, requiredVersionMinor)

	var (
		best *overlayPeer
		sum  uint32
	)
	for _, peer := range peers {
		stats := peer.statsSnapshot()
		if !peerEligible(stats, requiredVersionMajor, requiredVersionMinor) {
			continue
		}

		unr := uint32(math.Max(0, stats.unreliability-minUnreliability))
		if stats.versionMajor < s.spec.ProtoVersionMajor {
			unr += 4
		} else if stats.versionMajor == s.spec.ProtoVersionMajor && stats.versionMinor < s.spec.ProtoVersionMinor {
			unr += 2
		}
		unr += peerRoundtripPenalty(stats.roundtrip, minRoundtrip)

		if unr > uint32(peerFailUnreliability) {
			continue
		}

		weight := uint32(1) << (uint32(peerFailUnreliability) - unr)
		sum += weight
		if sum == weight || rand.IntN(int(sum)) < int(weight) {
			best = peer
		}
	}

	return best
}

func minPeerRoundtrip(peers []*overlayPeer, requiredVersionMajor, requiredVersionMinor int32) time.Duration {
	var minRTT time.Duration
	for _, peer := range peers {
		stats := peer.statsSnapshot()
		if !peerEligible(stats, requiredVersionMajor, requiredVersionMinor) || stats.roundtrip <= 0 {
			continue
		}
		if minRTT == 0 || stats.roundtrip < minRTT {
			minRTT = stats.roundtrip
		}
	}
	return minRTT
}

func peerRoundtripPenalty(roundtrip, minRoundtrip time.Duration) uint32 {
	if roundtrip <= 0 || minRoundtrip <= 0 {
		return 0
	}

	var penalty uint32
	if roundtrip > minRoundtrip*4 && roundtrip-minRoundtrip > 250*time.Millisecond {
		penalty += 2
	} else if roundtrip > minRoundtrip*2 && roundtrip-minRoundtrip > 150*time.Millisecond {
		penalty++
	}
	if roundtrip > 2*time.Second {
		penalty += 2
	}
	return penalty
}

func peerEligible(stats peerStats, requiredVersionMajor, requiredVersionMinor int32) bool {
	return stats.versionMajor > requiredVersionMajor ||
		(stats.versionMajor == requiredVersionMajor && stats.versionMinor >= requiredVersionMinor)
}

func nextNeighbourReloadDelay() time.Duration {
	if neighbourReloadJitter <= 0 {
		return neighbourReloadMinDelay
	}
	return neighbourReloadMinDelay + time.Duration(rand.Int64N(int64(neighbourReloadJitter)))
}

func nextPeerRefreshDelay() time.Duration {
	if peerRefreshJitter <= 0 {
		return peerRefreshMinDelay
	}
	return peerRefreshMinDelay + time.Duration(rand.Int64N(int64(peerRefreshJitter)))
}

func sortPeersByPreference(peers []*overlayPeer) {
	sort.SliceStable(peers, func(i, j int) bool {
		left := peers[i].statsSnapshot()
		right := peers[j].statsSnapshot()
		if left.unreliability != right.unreliability {
			return left.unreliability < right.unreliability
		}
		if left.versionMajor != right.versionMajor {
			return left.versionMajor > right.versionMajor
		}
		if left.versionMinor != right.versionMinor {
			return left.versionMinor > right.versionMinor
		}
		if peerRoundtripLess(left, right) {
			return true
		}
		if peerRoundtripLess(right, left) {
			return false
		}
		return peers[i].id < peers[j].id
	})
}

func peerRoundtripLess(left, right peerStats) bool {
	if left.roundtrip == right.roundtrip {
		return false
	}
	if left.roundtrip == 0 {
		return false
	}
	if right.roundtrip == 0 {
		return true
	}
	return left.roundtrip < right.roundtrip
}

func nextPeerPingDelay() time.Duration {
	if peerPingJitter <= 0 {
		return peerPingMinDelay
	}
	return peerPingMinDelay + time.Duration(rand.Int64N(int64(peerPingJitter)))
}

func (s *overlaySubscription) refreshTargets() []*overlayPeer {
	peers := s.preferredPeers(0, 0, func(_ *overlayPeer, stats peerStats) bool {
		return stats.alive
	})
	if len(peers) == 0 {
		peers = s.preferredPeers(0, 0, nil)
	}
	if len(peers) > peerRefreshFanout {
		peers = peers[:peerRefreshFanout]
	}
	return peers
}

func (s *overlaySubscription) queryCandidates(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	neighbours := s.preferredNeighbourPeers(requiredVersionMajor, requiredVersionMinor)
	others := s.preferredPeers(requiredVersionMajor, requiredVersionMinor, nil)
	if len(neighbours) == 0 {
		return others
	}
	if len(others) == 0 {
		return neighbours
	}

	seen := make(map[string]struct{}, len(neighbours))
	res := make([]*overlayPeer, 0, len(neighbours)+len(others))
	for _, peer := range neighbours {
		seen[peer.id] = struct{}{}
		res = append(res, peer)
	}

	for _, peer := range others {
		if _, ok := seen[peer.id]; ok {
			continue
		}
		res = append(res, peer)
	}
	return res
}

func (s *overlaySubscription) hedgedQueryCandidates(requiredVersionMajor, requiredVersionMinor int32, limit int) []*overlayPeer {
	peers := s.queryCandidates(requiredVersionMajor, requiredVersionMinor)
	if limit <= 0 || len(peers) <= limit {
		return peers
	}

	neighbours := s.preferredNeighbourPeers(requiredVersionMajor, requiredVersionMinor)
	neighbourSlots := limit / 2
	if neighbourSlots < 1 {
		neighbourSlots = 1
	}
	if len(neighbours) < neighbourSlots {
		neighbourSlots = len(neighbours)
	}

	res := make([]*overlayPeer, 0, limit)
	seen := make(map[string]struct{}, limit)
	for _, peer := range neighbours[:neighbourSlots] {
		seen[peer.id] = struct{}{}
		res = append(res, peer)
	}

	preferred := s.preferredPeers(requiredVersionMajor, requiredVersionMinor, nil)
	sortPeersByPreference(preferred)
	for _, peer := range preferred {
		if len(res) >= limit {
			break
		}
		if _, ok := seen[peer.id]; ok {
			continue
		}
		seen[peer.id] = struct{}{}
		res = append(res, peer)
	}

	return res
}

func (s *overlaySubscription) rebroadcastCandidates() []*overlayPeer {
	peers := s.preferredPeers(0, 0, func(_ *overlayPeer, stats peerStats) bool {
		return stats.alive
	})
	if len(peers) > 0 {
		return peers
	}
	return s.preferredPeers(0, 0, nil)
}
