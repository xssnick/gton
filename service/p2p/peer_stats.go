package p2p

import (
	"bytes"
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
	pending           bool
	lastReceiveAt     time.Time
	lastSuccessAt     time.Time
	failedQueries     uint64
	downloadBytesSec  float64
	downloadCount     uint64
	downloadSlowUntil time.Time
	lastPongAt        time.Time
	probeFailures     uint32
	queryResponds     bool
	queryIgnoreUntil  time.Time
}

func (p *overlayPeer) hasOpenConnection() bool {
	if p.overlay.ADNLWrapper == nil || p.overlay.ADNL == nil {
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
	if p.fixedMember {
		return true
	}

	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return !p.pending && p.announced != nil && announcedNodeIsFresh(p.announced, now)
}

func (p *overlayPeer) isAliveKnownOverlayPeer(now time.Time) bool {
	if p.fixedMember {
		if !p.hasOpenConnection() {
			return false
		}

		p.statsMx.Lock()
		defer p.statsMx.Unlock()

		return p.alive && (!p.lastReceiveAt.IsZero() || !p.lastSuccessAt.IsZero())
	}

	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return !p.pending && p.announced != nil && announcedNodeIsFresh(p.announced, now) && p.alive
}

func (p *overlayPeer) advertisedNodeSnapshot(now time.Time) *overlay.Node {
	return cloneOverlayNode(p.advertisedNodeRef(now))
}

// advertisedNodeRef is advertisedNodeSnapshot without the copy. p.announced is
// only ever replaced by mergeAnnouncement, never mutated in place, so the
// returned node is safe to read after the lock is dropped - but it must not be
// handed to a caller that mutates or retains it, hence the clone above for
// everyone who does not clone the selection itself.
func (p *overlayPeer) advertisedNodeRef(now time.Time) *overlay.Node {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	if p.pending || p.announced == nil || !announcedNodeIsFresh(p.announced, now) || !p.alive {
		return nil
	}
	return p.announced
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
		pending:           p.pending,
		lastReceiveAt:     p.lastReceiveAt,
		lastSuccessAt:     p.lastSuccessAt,
		failedQueries:     p.failedQueries,
		downloadBytesSec:  p.downloadBytesSec,
		downloadCount:     p.downloadCount,
		downloadSlowUntil: p.downloadSlowUntil,
		lastPongAt:        p.lastPongAt,
		probeFailures:     p.probeFailures,
		queryResponds:     p.queryResponds,
		queryIgnoreUntil:  p.queryIgnoreUntil,
	}
}

func (p *overlayPeer) applyCapabilities(cap Capabilities) {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.versionMajor = cap.VersionMajor
	p.versionMinor = cap.VersionMinor
	p.capabilitiesFlags = cap.Flags
}

func (p *overlayPeer) queryProbeSuccess(cap Capabilities, rtt time.Duration) (bool, bool) {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	becameReady := !p.queryResponds
	p.versionMajor = cap.VersionMajor
	p.versionMinor = cap.VersionMinor
	p.capabilitiesFlags = cap.Flags
	p.queryResponds = true

	promoted := p.noteSuccessLocked(time.Now())
	if rtt <= 0 {
		return promoted, becameReady
	}
	if p.roundtrip == 0 {
		p.roundtrip = rtt
		return promoted, becameReady
	}
	p.roundtrip = (p.roundtrip + rtt) / 2
	return promoted, becameReady
}

func (p *overlayPeer) queryProbeFailed() bool {
	p.statsMx.Lock()
	changed := p.queryResponds
	p.queryResponds = false
	p.statsMx.Unlock()
	return changed
}

func (p *overlayPeer) ignoreApplicationQueries(until time.Time) {
	p.statsMx.Lock()
	p.queryIgnoreUntil = until
	p.statsMx.Unlock()
}

func (p *overlayPeer) queryReady(now time.Time, requiredVersionMajor, requiredVersionMinor int32) bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return p.queryResponds &&
		!now.Before(p.queryIgnoreUntil) &&
		peerEligibleVersion(p.versionMajor, p.versionMinor, requiredVersionMajor, requiredVersionMinor)
}

func (p *overlayPeer) querySuccess(rtt time.Duration) bool {
	p.outboundOK.Store(true)
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	now := time.Now()
	promoted := p.noteSuccessLocked(now)
	if rtt <= 0 {
		return promoted
	}
	if p.roundtrip == 0 {
		p.roundtrip = rtt
		return promoted
	}
	p.roundtrip = (p.roundtrip + rtt) / 2
	return promoted
}

func (p *overlayPeer) queryFailed() {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.missedPings++
	p.unreliability++
	p.failedQueries++
	if p.fixedMember {
		if p.lastReceiveAt.IsZero() && p.lastSuccessAt.IsZero() {
			p.alive = false
		}
		return
	}
	if p.missedPings >= 3 && !p.lastReceiveAt.IsZero() && time.Since(p.lastReceiveAt) >= 15*time.Second {
		p.alive = false
	}
}

func (p *overlayPeer) downloadSuccess(bytes int64, elapsed time.Duration, slowThreshold float64, slowPenalty time.Duration) {
	if bytes <= 0 || elapsed <= 0 {
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

func (p *overlayPeer) noteSuccessLocked(now time.Time) bool {
	promoted := p.pending
	p.pending = false
	p.missedPings = 0
	p.alive = true
	p.lastReceiveAt = now
	p.lastSuccessAt = now
	p.unreliability--
	if p.unreliability < 0 {
		p.unreliability = 0
	}
	return promoted
}

func (p *overlayPeer) downloadFailed(slowPenalty time.Duration) {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.downloadSlowUntil = time.Now().Add(slowPenalty)
}

func (p *overlayPeer) noteReceive() bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	promoted := p.pending
	p.pending = false
	p.missedPings = 0
	p.alive = true
	p.lastReceiveAt = time.Now()
	return promoted
}

func (p *overlayPeer) markPending() {
	if p.fixedMember {
		return
	}

	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.pending = true
	p.alive = false
	p.lastReceiveAt = time.Time{}
	p.lastSuccessAt = time.Time{}
}

func (p *overlayPeer) isPending() bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	return p.pending
}

func (p *overlayPeer) tryBeginWarmup() bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	if p.warmupRunning {
		return false
	}
	p.warmupRunning = true
	return true
}

func (p *overlayPeer) finishWarmup() {
	p.statsMx.Lock()
	p.warmupRunning = false
	p.statsMx.Unlock()
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

func (s *overlaySubscription) preferredPeers(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	known := s.knownPeersSnapshot()
	candidates := make([]*overlayPeer, 0, len(known))
	for _, peer := range known {
		if !peerEligible(peer.statsSnapshot(), requiredVersionMajor, requiredVersionMinor) {
			continue
		}
		candidates = append(candidates, peer)
	}
	return s.orderPreferredPeers(candidates, requiredVersionMajor, requiredVersionMinor)
}

type rankedPeer struct {
	peer  *overlayPeer
	stats peerStats
}

func (s *overlaySubscription) orderPreferredPeers(candidates []*overlayPeer, requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	if len(candidates) == 0 {
		return nil
	}

	remaining := make([]rankedPeer, len(candidates))
	for i, peer := range candidates {
		remaining[i] = rankedPeer{peer: peer, stats: peer.statsSnapshot()}
	}

	selected := make([]*overlayPeer, 0, len(candidates))
	for len(remaining) > 0 {
		// The weighted draw is quadratic, and no consumer randomises deeper
		// than a couple of dozen candidates (the largest peer limit in the
		// tree is 24, and hedgedQueryCandidates already re-sorts the tail
		// deterministically). Past the limit fall through to the same
		// preference sort the ineligible-tail branch below uses.
		idx := -1
		if len(selected) < weightedPeerDrawLimit {
			idx = s.choosePeerIndex(remaining, requiredVersionMajor, requiredVersionMinor)
		}
		if idx < 0 {
			sortRankedPeersByPreference(remaining)
			for _, ranked := range remaining {
				selected = append(selected, ranked.peer)
			}
			break
		}

		selected = append(selected, remaining[idx].peer)
		// Swap-remove: every scan below is order-independent, and the fallback
		// sort tie-breaks on peer id (a total order), so compacting the slice
		// would only pay an O(n) memmove per selected peer for nothing.
		remaining[idx] = remaining[len(remaining)-1]
		remaining = remaining[:len(remaining)-1]
	}
	return selected
}

func (s *overlaySubscription) choosePeerIndex(peers []rankedPeer, requiredVersionMajor, requiredVersionMinor int32) int {
	if len(peers) == 0 {
		return -1
	}

	// Everything below iterates by index: rankedPeer embeds peerStats (five
	// time.Time fields), so ranging by value copies ~200 bytes per peer per
	// scan, and this runs once per selected peer — quadratic in the roster,
	// which is now 300 rather than 20.
	minUnreliability := math.MaxFloat64
	for i := range peers {
		stats := &peers[i].stats
		if !peerEligibleVersion(stats.versionMajor, stats.versionMinor, requiredVersionMajor, requiredVersionMinor) {
			continue
		}
		if stats.unreliability < minUnreliability {
			minUnreliability = stats.unreliability
		}
	}
	if minUnreliability == math.MaxFloat64 {
		return -1
	}
	minRoundtrip := minRankedPeerRoundtrip(peers, requiredVersionMajor, requiredVersionMinor)

	best := -1
	var sum uint32
	for idx := range peers {
		stats := &peers[idx].stats
		if !peerEligibleVersion(stats.versionMajor, stats.versionMinor, requiredVersionMajor, requiredVersionMinor) {
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
			best = idx
		}
	}

	return best
}

func minRankedPeerRoundtrip(peers []rankedPeer, requiredVersionMajor, requiredVersionMinor int32) time.Duration {
	var minRTT time.Duration
	for i := range peers {
		stats := &peers[i].stats
		if stats.roundtrip <= 0 ||
			!peerEligibleVersion(stats.versionMajor, stats.versionMinor, requiredVersionMajor, requiredVersionMinor) {
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
	return peerEligibleVersion(stats.versionMajor, stats.versionMinor, requiredVersionMajor, requiredVersionMinor)
}

func peerEligibleVersion(versionMajor, versionMinor, requiredVersionMajor, requiredVersionMinor int32) bool {
	return versionMajor > requiredVersionMajor ||
		(versionMajor == requiredVersionMajor && versionMinor >= requiredVersionMinor)
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
	if len(peers) < 2 {
		return
	}

	ranked := make([]rankedPeer, len(peers))
	for i, peer := range peers {
		ranked[i] = rankedPeer{peer: peer, stats: peer.statsSnapshot()}
	}
	sortRankedPeersByPreference(ranked)
	for i, entry := range ranked {
		peers[i] = entry.peer
	}
}

func sortRankedPeersByPreference(peers []rankedPeer) {
	sort.SliceStable(peers, func(i, j int) bool {
		left := peers[i].stats
		right := peers[j].stats
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
		return bytes.Compare(peers[i].peer.id[:], peers[j].peer.id[:]) < 0
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

func nextDHTSeedCooldownDelay() time.Duration {
	if dhtSeedCooldownJitter <= 0 {
		return dhtSeedCooldownMinDelay
	}
	return dhtSeedCooldownMinDelay + time.Duration(rand.Int64N(int64(dhtSeedCooldownJitter)))
}

func (s *overlaySubscription) refreshTargets() []*overlayPeer {
	peers := s.knownPeersSnapshot()
	if len(peers) == 0 {
		return nil
	}

	alive := peers[:0]
	for _, peer := range peers {
		if peer.statsSnapshot().alive {
			alive = append(alive, peer)
		}
	}
	if len(alive) > 0 {
		peers = alive
	}
	if len(peers) > 1 {
		rand.Shuffle(len(peers), func(i, j int) {
			peers[i], peers[j] = peers[j], peers[i]
		})
	}
	if len(peers) > peerRefreshFanout {
		peers = peers[:peerRefreshFanout]
	}
	return peers
}

func (s *overlaySubscription) publicQueryCandidates(requiredVersionMajor, requiredVersionMinor int32) []*overlayPeer {
	neighbours := s.preferredNeighbourPeers(requiredVersionMajor, requiredVersionMinor)
	others := s.preferredPeers(requiredVersionMajor, requiredVersionMinor)
	peers := mergePeerCandidates(neighbours, others)
	if len(peers) > 0 {
		return peers
	}

	// C++ has a direct random-peer fallback after its neighbour roster is
	// exhausted. It may still return a previously verified alive peer whose
	// announcement is older than the normal 10-minute admission window.
	return s.publicRandomQueryFallback(requiredVersionMajor, requiredVersionMinor)
}

func (s *overlaySubscription) hedgedQueryCandidates(requiredVersionMajor, requiredVersionMinor int32, limit int) []*overlayPeer {
	peers := s.queryCandidates(requiredVersionMajor, requiredVersionMinor)
	if limit <= 0 || len(peers) <= limit {
		return peers
	}
	if s.spec.preservesQueryCandidateOrder() {
		return peers[:limit]
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
	seen := make(map[PeerID]struct{}, limit)
	res = appendUniquePeerCandidates(res, seen, neighbours[:neighbourSlots], limit)

	preferred := s.preferredPeers(requiredVersionMajor, requiredVersionMinor)
	sortPeersByPreference(preferred)
	return appendUniquePeerCandidates(res, seen, preferred, limit)
}

// broadcastTargetsTTL bounds how stale the cached broadcast fan-out peer set
// may be: FEC relay peer sets are consulted for every received FEC part, so
// the set is rebuilt at most once per TTL instead of per part. Membership
// changes additionally invalidate the cache via notifyPeersChangedLocked.
// weightedPeerDrawLimit bounds how many peers are drawn with the O(n) weighted
// selection; the remainder is appended in deterministic preference order.
const weightedPeerDrawLimit = 32

const broadcastTargetsTTL = 200 * time.Millisecond

type broadcastTargetsSnapshot struct {
	generation uint64
	builtAt    time.Time
	peers      []*overlayPeer
	broadcast  []overlay.BroadcastPeer
	// relay is the bounded subset of broadcast used to forward FEC parts of
	// somebody else's broadcast. It must stay small and independent of the
	// roster size: the relay fires per received FEC part, so a roster-sized
	// set multiplies egress by the roster (C++ forwards each part to
	// propagate_broadcast_to=5 of its neighbours regardless of peer count).
	relay    []overlay.BroadcastPeer
	plumtree []PeerID
}

// broadcastTargetsSnapshot returns the deduplicated union of neighbour and
// known peers. Whole-roster overlays keep every known member; mesh overlays
// prefer alive peers when any exist and use everything known otherwise. The
// slices are cached and shared between callers and must not be modified.
func (s *overlaySubscription) broadcastTargetsSnapshot() *broadcastTargetsSnapshot {
	generation := s.broadcastTargetsGen.Load()
	if snap := s.broadcastTargets.Load(); snap != nil && snap.generation == generation && time.Since(snap.builtAt) < broadcastTargetsTTL {
		return snap
	}

	s.broadcastTargetsMx.Lock()
	defer s.broadcastTargetsMx.Unlock()

	generation = s.broadcastTargetsGen.Load()
	if snap := s.broadcastTargets.Load(); snap != nil && snap.generation == generation && time.Since(snap.builtAt) < broadcastTargetsTTL {
		return snap
	}

	// A membership change during the build leaves the snapshot stamped with
	// the older generation, so the next caller rebuilds; consumers already
	// tolerate snapshots up to the TTL stale.
	snap := s.buildBroadcastTargetsSnapshot()
	snap.generation = generation
	s.broadcastTargets.Store(snap)
	return snap
}

func (s *overlaySubscription) buildBroadcastTargetsSnapshot() *broadcastTargetsSnapshot {
	now := time.Now()
	neighbours := s.neighbourPeerSnapshots()
	known := s.knownPeersSnapshot()

	capacity := len(neighbours) + len(known)
	all := make([]*overlayPeer, 0, capacity)
	alive := make([]*overlayPeer, 0, capacity)
	seen := make(map[PeerID]struct{}, capacity)
	// Neighbours are iterated first and dedup keeps the first occurrence, so
	// they form a prefix of both lists; these are the prefix lengths.
	allNeighbours, aliveNeighbours := 0, 0
	for listIdx, list := range [2][]*overlayPeer{neighbours, known} {
		for _, peer := range list {
			if _, ok := seen[peer.id]; ok {
				continue
			}
			seen[peer.id] = struct{}{}
			all = append(all, peer)
			isNeighbour := listIdx == 0
			if isNeighbour {
				allNeighbours++
			}
			if peer.statsSnapshot().alive {
				alive = append(alive, peer)
				if isNeighbour {
					aliveNeighbours++
				}
			}
		}
	}

	peers, neighbourPrefix := all, allNeighbours
	if len(alive) > 0 && !s.spec.floodsWholeRoster() {
		peers, neighbourPrefix = alive, aliveNeighbours
	}

	receivers := make([]*overlayPeer, 0, len(peers))
	broadcast := make([]overlay.BroadcastPeer, 0, len(peers))
	plumtree := make([]PeerID, 0, len(peers))
	// Kept in sync with the neighbour prefix as the fastSync filter drops peers.
	neighbourTargets := 0
	for i, peer := range peers {
		if s.fastSync != nil &&
			!s.fastSync.peerReceivesBroadcasts(peer.id) {
			continue
		}
		if i < neighbourPrefix {
			neighbourTargets++
		}
		receivers = append(receivers, peer)
		broadcast = append(broadcast, peer.broadcastPeer)
		if peer.route.quicReady(now) {
			plumtree = append(plumtree, peer.id)
		}
	}
	return &broadcastTargetsSnapshot{
		builtAt:   now,
		peers:     receivers,
		broadcast: broadcast,
		relay:     sampleBroadcastRelayTargets(broadcast, neighbourTargets),
		plumtree:  plumtree,
	}
}

// sampleBroadcastRelayTargets picks the bounded relay set: a random sample of
// the neighbour prefix, topped up from the rest of the roster when there are
// not enough neighbours. Runs at most once per broadcastTargetsTTL, never per
// broadcast part.
func sampleBroadcastRelayTargets(broadcast []overlay.BroadcastPeer, neighbourTargets int) []overlay.BroadcastPeer {
	if len(broadcast) <= broadcastFECRelayFanout {
		return broadcast
	}
	if neighbourTargets > len(broadcast) {
		neighbourTargets = len(broadcast)
	}

	pool := make([]overlay.BroadcastPeer, len(broadcast))
	copy(pool, broadcast)
	// Partial Fisher-Yates over the neighbour prefix first, then over the
	// remainder, so neighbours fill the sample before anyone else.
	relay := make([]overlay.BroadcastPeer, 0, broadcastFECRelayFanout)
	take := func(from, to int) {
		for i := from; i < to && len(relay) < broadcastFECRelayFanout; i++ {
			j := i + rand.IntN(to-i)
			pool[i], pool[j] = pool[j], pool[i]
			relay = append(relay, pool[i])
		}
	}
	take(0, neighbourTargets)
	take(neighbourTargets, len(pool))
	return relay
}

func mergePeerCandidates(first, second []*overlayPeer) []*overlayPeer {
	if len(first) == 0 {
		return second
	}
	if len(second) == 0 {
		return first
	}

	seen := make(map[PeerID]struct{}, len(first))
	res := make([]*overlayPeer, 0, len(first)+len(second))
	res = appendUniquePeerCandidates(res, seen, first, 0)
	return appendUniquePeerCandidates(res, seen, second, 0)
}

func appendUniquePeerCandidates(res []*overlayPeer, seen map[PeerID]struct{}, peers []*overlayPeer, limit int) []*overlayPeer {
	for _, peer := range peers {
		if limit > 0 && len(res) >= limit {
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
