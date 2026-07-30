package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	archivePeerErrorRotateThreshold        = 3
	archivePeerNotAvailableRotateThreshold = 2
	archivePeerBadImportRotateThreshold    = 2
	archivePeerRosterLimit                 = 40
	archivePeerPendingLimit                = 128
	archivePeerScoutWorkers                = 4
	archiveConcurrentScoutLimit            = 8
	archivePeerRetryCacheLimit             = 4096
	archivePeerLocalRetryCacheLimit        = 2048

	archiveDiscoveryInterval           = time.Minute
	archiveDiscoveryUrgentInterval     = 10 * time.Second
	archiveDiscoveryLoopInterval       = 10 * time.Second
	archivePeerNotAvailableTTL         = 20 * time.Minute
	archivePeerUnreachableTTL          = 5 * time.Minute
	archivePeerNoBenefitTTL            = 5 * time.Minute
	archivePeerReplacementCooldown     = time.Minute
	archivePoolInactiveGrace           = time.Minute
	archiveDHTAddressTimeout           = 15 * time.Second
	archiveRandomPeerQueryTimeout      = 5 * time.Second
	archiveTransientRandomReplyLimit   = maxRandomPeerReply
	archivePeerReplacementFactor       = 1.25
	archiveTransientRandomExpansionTTL = time.Minute
	archiveRandomPeerCalmFanout        = 1
	archiveRandomPeerUrgentFanout      = 4
	archiveValuablePeerRetry           = 5 * time.Minute
	archiveValuablePeerStarvedRetry    = time.Minute
	archiveValuablePeerLimit           = 256
)

// archiveNotAvailableCooldowns holds the per-shard cooldown ladder for
// consecutive not-available answers; archival peers are scarce, so repeated
// misses back off instead of banishing the peer.
var archiveNotAvailableCooldowns = [...]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

type archivePeerPool struct {
	sub    *overlaySubscription
	log    zerolog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	scout  *archiveScoutShared

	mx                    sync.Mutex
	closed                bool
	closeDone             chan struct{}
	peers                 map[PeerID]*archivePeer
	shards                map[string]*archiveShardPeerState
	valuable              map[PeerID]archiveValuablePeer
	demands               map[uint64]*archivePeerDemand
	demandKeys            map[string]uint64
	demandBlockedUntil    map[archivePeerDemandRetryKey]time.Time
	nextDemandID          uint64
	rejectedUntil         map[PeerID]time.Time
	transportBlockedUntil map[PeerID]time.Time
	randomExpandedUntil   map[PeerID]time.Time
	scouting              map[PeerID]struct{}
	offers                chan archivePeerOffer
	scoutWorkers          sync.WaitGroup
	lastUsedAt            time.Time
	activeUsers           int
	continuousDiscovery   bool
	scoutStats            archiveScoutCounters

	discoveryMx      sync.Mutex
	discoveryRunning bool
	discoveryDone    chan struct{}
	nextDiscoveryAt  time.Time
	lastDiscoveryAt  time.Time
}

// archivePeerProbe captures what the pool is currently syncing so freshly
// discovered peers can be classified as archival with a single query.
type archivePeerProbe struct {
	shard     archive.ShardID
	seqno     uint32
	block     ton.BlockIDExt
	zeroState bool
	demandID  uint64
}

type archivePeerDemandEvidence uint8

const (
	archivePeerDemandUnknown archivePeerDemandEvidence = iota
	archivePeerDemandNotAvailable
	archivePeerDemandAvailable
	archivePeerDemandProven
)

type archivePeerDemandPeer struct {
	evidence       archivePeerDemandEvidence
	at             time.Time
	rejectedUntil  time.Time
	noBenefitUntil time.Time
}

type archivePeerDemand struct {
	key         string
	probe       archivePeerProbe
	refs        int
	createdAt   time.Time
	lastScoutAt time.Time
	peers       map[PeerID]archivePeerDemandPeer
}

type archivePeerDemandRetryKey struct {
	demand string
	peerID PeerID
}

type archivePeer struct {
	peer             *overlayPeer
	owned            bool
	leases           int
	addedAt          time.Time
	archiveDownloads uint64
}

type archiveShardPeerState struct {
	peers map[PeerID]*archiveShardPeer
}

type archiveShardPeer struct {
	lastBytesAt      time.Time
	probeSuccesses   uint64
	archiveDownloads uint64
	bytes            int64
	downloadElapsed  time.Duration
	probeBytesPerSec float64
	cooldownUntil    time.Time
	failure          archivePeerFailure
}

type archiveValuablePeer struct {
	peerID        PeerID
	endpoint      string
	pub           ed25519.PublicKey
	lastSuccessAt time.Time
	nextTryAt     time.Time
}

type archivePeerFailure struct {
	notAvailable   int
	probeErrors    int
	downloadErrors int
	badImports     int
	reason         string
}

type archivePeerFailureVerdict struct {
	useless  bool
	cooldown time.Duration
}

type archivePeerRotationCandidate struct {
	peerID  PeerID
	failure archivePeerFailure
}

func newArchivePeerPool(sub *overlaySubscription) *archivePeerPool {
	ctx, cancel := context.WithCancel(sub.node.runCtx)
	now := time.Now()
	pool := &archivePeerPool{
		sub:                   sub,
		log:                   sub.log.With().Str("peer_pool", "archive").Logger(),
		ctx:                   ctx,
		cancel:                cancel,
		scout:                 newArchiveScoutShared(),
		closeDone:             make(chan struct{}),
		peers:                 map[PeerID]*archivePeer{},
		shards:                map[string]*archiveShardPeerState{},
		valuable:              map[PeerID]archiveValuablePeer{},
		demands:               map[uint64]*archivePeerDemand{},
		demandKeys:            map[string]uint64{},
		demandBlockedUntil:    map[archivePeerDemandRetryKey]time.Time{},
		rejectedUntil:         map[PeerID]time.Time{},
		transportBlockedUntil: map[PeerID]time.Time{},
		randomExpandedUntil:   map[PeerID]time.Time{},
		scouting:              map[PeerID]struct{}{},
		offers:                make(chan archivePeerOffer, archivePeerPendingLimit),
		lastUsedAt:            now,
	}
	if !sub.spec.usesDedicatedQueryPeers() {
		pool.startScoutWorkers()
		pool.scoutWorkers.Go(pool.runArchiveDiscoveryLoop)
	}
	return pool
}

func (p *archivePeerPool) Close() {
	p.mx.Lock()
	if p.closed {
		done := p.closeDone
		p.mx.Unlock()
		<-done
		return
	}
	done := p.closeDone
	p.closed = true
	p.mx.Unlock()
	defer close(done)
	p.cancel()

	p.discoveryMx.Lock()
	discoveryDone := p.discoveryDone
	p.discoveryMx.Unlock()

	if discoveryDone != nil {
		<-discoveryDone
	}
	p.scoutWorkers.Wait()

	p.mx.Lock()
	archiveOnly := make([]*overlayPeer, 0, len(p.peers))
	for peerID, entry := range p.peers {
		if entry != nil && entry.owned && entry.peer != nil {
			archiveOnly = append(archiveOnly, entry.peer)
		}
		delete(p.peers, peerID)
	}
	p.shards = map[string]*archiveShardPeerState{}
	p.valuable = map[PeerID]archiveValuablePeer{}
	p.transportBlockedUntil = map[PeerID]time.Time{}
	p.demands = map[uint64]*archivePeerDemand{}
	p.demandKeys = map[string]uint64{}
	p.demandBlockedUntil = map[archivePeerDemandRetryKey]time.Time{}
	p.scouting = map[PeerID]struct{}{}
	p.mx.Unlock()

	for _, peer := range archiveOnly {
		closeArchiveOnlyPeer(peer)
	}
}

func (p *archivePeerPool) isClosed() bool {
	p.mx.Lock()
	defer p.mx.Unlock()

	return p.closed
}

func (p *archivePeerPool) canRetire(now time.Time) bool {
	if p.sub.isActive() {
		return false
	}

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed || p.activeUsers > 0 || now.Sub(p.lastUsedAt) < archivePoolInactiveGrace {
		return p.closed
	}
	for _, entry := range p.peers {
		if entry != nil && entry.leases > 0 {
			return false
		}
	}
	return true
}

func (p *archivePeerPool) beginUse(now time.Time) (func(), error) {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return nil, errArchiveSessionClosed
	}
	p.activeUsers++
	p.lastUsedAt = now
	p.mx.Unlock()

	return func() {
		p.mx.Lock()
		p.activeUsers--
		p.lastUsedAt = time.Now()
		p.mx.Unlock()
	}, nil
}

func (p *archivePeerPool) touch(now time.Time) {
	p.mx.Lock()
	if !p.closed {
		p.lastUsedAt = now
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) enableContinuousDiscovery() {
	p.mx.Lock()
	if !p.closed && !p.sub.spec.usesDedicatedQueryPeers() {
		p.continuousDiscovery = true
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) bootstrapLivePeers() {
	if p.sub.spec.usesDedicatedQueryPeers() {
		p.bootstrapCustomQueryPeers()
		return
	}

	for _, peer := range p.sub.peersSnapshot() {
		if peer.id.IsZero() || !peer.hasOpenConnection() || len(peer.pub) != ed25519.PublicKeySize {
			continue
		}
		p.offerArchiveLivePeer(peer)
	}
}

func (p *archivePeerPool) bootstrapCustomQueryPeers() {
	peers := p.sub.customQueryCandidates(0, 0)
	now := time.Now()

	p.mx.Lock()
	defer p.mx.Unlock()
	if p.closed {
		return
	}
	for _, peer := range peers {
		entry := p.peers[peer.id]
		if entry == nil {
			p.peers[peer.id] = &archivePeer{
				peer:    peer,
				addedAt: now,
			}
			continue
		}
		if !entry.owned && entry.peer != peer && entry.leases == 0 {
			entry.peer = peer
			entry.addedAt = now
		}
	}
}

func (p *archivePeerPool) peerByAddr(addr string) *overlayPeer {
	if addr == "" {
		return nil
	}

	p.mx.Lock()
	defer p.mx.Unlock()

	for _, entry := range p.peers {
		if entry.peer.addr == addr {
			return entry.peer
		}
	}
	return nil
}

func (p *archivePeerPool) candidates(shard archive.ShardID) []*overlayPeer {
	p.bootstrapLivePeers()
	p.pruneClosedPeers()

	now := time.Now()
	stateKey := archivePeerPoolKey(shard)

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return nil
	}

	state := p.shards[stateKey]
	peers := make([]*overlayPeer, 0, len(p.peers))
	for peerID, entry := range p.peers {
		if !archivePeerUsable(entry, now) {
			continue
		}
		if p.sub.spec.usesDedicatedQueryPeers() &&
			!entry.peer.queryReady(now, 0, 0) {
			continue
		}
		if p.recentlyRejectedLocked(peerID, now) || p.transportBlockedLocked(peerID, now) || archivePeerCoolingDownLocked(state, peerID, now) {
			continue
		}
		peers = append(peers, entry.peer)
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}
	return peers
}

func (p *archivePeerPool) candidatesForArchive(shard archive.ShardID, masterchainSeqno uint32) []*overlayPeer {
	return p.filterArchiveDemandCandidates(shard, masterchainSeqno, p.candidates(shard))
}

func (p *archivePeerPool) downloadCandidatesForArchive(session *ArchiveSession, shard archive.ShardID, masterchainSeqno uint32, peers []*overlayPeer) []*overlayPeer {
	return p.filterArchiveDemandCandidates(shard, masterchainSeqno, p.downloadCandidates(session, shard, peers))
}

func (p *archivePeerPool) filterArchiveDemandCandidates(shard archive.ShardID, masterchainSeqno uint32, peers []*overlayPeer) []*overlayPeer {
	if len(peers) == 0 {
		return nil
	}

	now := time.Now()
	demandKey := archiveDemandKey(shard, masterchainSeqno)
	p.mx.Lock()
	demand := p.demands[p.demandKeys[demandKey]]
	filtered := peers[:0]
	for _, peer := range peers {
		state := archivePeerDemandPeer{}
		if demand != nil {
			state = demand.peers[peer.id]
		}
		if p.demandRetryBlockedLocked(demandKey, peer.id, now) || now.Before(state.rejectedUntil) || now.Before(state.noBenefitUntil) {
			continue
		}
		filtered = append(filtered, peer)
	}
	p.mx.Unlock()
	return filtered
}

func (p *archivePeerPool) downloadCandidates(session *ArchiveSession, shard archive.ShardID, peers []*overlayPeer) []*overlayPeer {
	peers = p.prioritize(shard, peers)
	if session == nil {
		return peers
	}

	selected := p.selectedPeer(session, shard)
	if selected == nil {
		return peers
	}
	if len(peers) > 0 && !p.selectedPeerCanStayFirst(shard, selected) {
		return peers
	}

	peerID := downloadPeerID(selected)
	ordered := make([]*overlayPeer, 0, len(peers)+1)
	ordered = append(ordered, selected)
	for _, peer := range peers {
		if downloadPeerID(peer) == peerID {
			continue
		}
		ordered = append(ordered, peer)
	}
	return ordered
}

func (p *archivePeerPool) selectedPeerCanStayFirst(shard archive.ShardID, peer *overlayPeer) bool {
	peerID := downloadPeerID(peer)
	p.mx.Lock()
	defer p.mx.Unlock()

	entry := p.peers[peerID]
	if entry == nil || entry.peer != peer {
		return false
	}
	state := p.shards[archivePeerPoolKey(shard)]
	selected := archivePeerPerformanceFromState(state, peerID)
	selectedRate := selected.bytesPerSecond()
	for candidateID, candidateEntry := range p.peers {
		if candidateID == peerID || !archivePeerUsable(candidateEntry, time.Now()) {
			continue
		}
		if archivePeerCoolingDownLocked(state, candidateID, time.Now()) {
			continue
		}
		candidate := archivePeerPerformanceFromState(state, candidateID)
		if entry.leases > 0 && candidateEntry.leases == 0 {
			return false
		}
		candidateRate := candidate.bytesPerSecond()
		if selectedRate <= 0 && candidateRate > 0 {
			return false
		}
		if selectedRate > 0 && candidateRate >= selectedRate*archivePeerReplacementFactor {
			return false
		}
	}
	return true
}

func (p *archivePeerPool) selectedPeer(session *ArchiveSession, shard archive.ShardID) *overlayPeer {
	peerID := session.selectedArchivePeerID(shard)
	if peerID.IsZero() {
		return nil
	}
	if selectedPool := session.selectedArchivePeerPool(shard); selectedPool != nil && selectedPool != p {
		if !selectedPool.isClosed() {
			return nil
		}
		session.clearSelectedArchivePeerID(shard, peerID)
		return nil
	}

	stateKey := archivePeerPoolKey(shard)
	now := time.Now()
	peer, reason := p.peerIfAvailable(shard, peerID, now)
	if reason == "" {
		return peer
	}

	session.clearSelectedArchivePeerID(shard, peerID)
	event := p.log.Info().
		Str("peer_id", peerID.String()).
		Str("archive_pool", stateKey).
		Str("reason", reason)
	if peer != nil {
		event.Str("peer", peer.addr)
	}
	event.Msg("dropped selected archive peer")
	return nil
}

func (p *archivePeerPool) peerIfAvailable(shard archive.ShardID, peerID PeerID, now time.Time) (*overlayPeer, string) {
	p.mx.Lock()
	defer p.mx.Unlock()

	entry := p.peers[peerID]
	if entry == nil || entry.peer == nil {
		return nil, "missing"
	}
	peer := entry.peer
	if !peer.hasOpenConnection() {
		return peer, "closed"
	}
	if !peer.isAliveKnownOverlayPeer(now) && entry.archiveDownloads == 0 {
		return peer, "not_alive"
	}
	if p.transportBlockedLocked(peerID, now) {
		return peer, "transport_backoff"
	}
	state := p.shards[archivePeerPoolKey(shard)]
	if archivePeerCoolingDownLocked(state, peerID, now) {
		return peer, "cooldown"
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, archivePeerPoolKey(shard))
	}
	return peer, ""
}

func (p *archivePeerPool) prioritize(shard archive.ShardID, peers []*overlayPeer) []*overlayPeer {
	if len(peers) < 2 {
		return peers
	}
	return prioritizeArchivePeersWithPerformance(shard, peers, p.leaseSnapshot(peers), p.performanceSnapshot(shard, peers))
}

func (p *archivePeerPool) performanceSnapshot(shard archive.ShardID, peers []*overlayPeer) map[PeerID]archivePeerPerformance {
	p.mx.Lock()
	defer p.mx.Unlock()

	state := p.shards[archivePeerPoolKey(shard)]
	performance := make(map[PeerID]archivePeerPerformance, len(peers))
	for _, peer := range peers {
		entry := p.peers[downloadPeerID(peer)]
		if entry == nil || entry.peer != peer {
			continue
		}
		performance[peer.id] = archivePeerPerformanceFromState(state, peer.id)
	}
	return performance
}

func archivePeerPerformanceFromState(state *archiveShardPeerState, peerID PeerID) archivePeerPerformance {
	if state == nil || state.peers == nil || state.peers[peerID] == nil {
		return archivePeerPerformance{}
	}
	peer := state.peers[peerID]
	return archivePeerPerformance{
		probeSuccesses:   peer.probeSuccesses,
		archiveDownloads: peer.archiveDownloads,
		bytes:            peer.bytes,
		downloadElapsed:  peer.downloadElapsed,
		probeBytesPerSec: peer.probeBytesPerSec,
	}
}

func (p *archivePeerPool) leaseSnapshot(peers []*overlayPeer) map[PeerID]int {
	p.mx.Lock()
	defer p.mx.Unlock()

	leases := make(map[PeerID]int, len(peers))
	for _, peer := range peers {
		peerID := downloadPeerID(peer)
		if peerID.IsZero() {
			continue
		}
		if entry := p.peers[peerID]; entry != nil {
			leases[peerID] = entry.leases
		}
	}
	return leases
}

func (p *archivePeerPool) acquire(peer *overlayPeer) (func(), bool) {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return nil, false
	}

	p.mx.Lock()
	entry := p.peers[peerID]
	if p.closed || entry == nil || entry.peer != peer || !peer.hasOpenConnection() {
		p.mx.Unlock()
		return nil, false
	}
	entry.leases++
	p.mx.Unlock()

	return func() {
		p.mx.Lock()
		defer p.mx.Unlock()

		entry := p.peers[peerID]
		if entry == nil || entry.peer != peer || entry.leases == 0 {
			return
		}
		entry.leases--
	}, true
}

func (p *archivePeerPool) noteArchiveSeedSuccess(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration) {
	p.noteArchiveSpeed(shard, peer, bytes, elapsed, false)
}

func (p *archivePeerPool) noteArchiveDownload(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration) {
	p.noteArchiveSpeed(shard, peer, bytes, elapsed, true)
}

func (p *archivePeerPool) noteArchiveSpeed(shard archive.ShardID, peer *overlayPeer, bytes int64, elapsed time.Duration, complete bool) {
	if peer == nil || bytes <= 0 || elapsed <= 0 {
		return
	}

	peerID := downloadPeerID(peer)
	p.mx.Lock()
	entry := p.peers[peerID]
	if !p.closed && entry != nil && entry.peer == peer {
		state := p.shardPeerLocked(archivePeerPoolKey(shard), peerID)
		state.lastBytesAt = time.Now()
		if complete {
			state.bytes += bytes
			state.downloadElapsed += elapsed
		} else if archiveSpeedSampleReliable(bytes) {
			speed := float64(bytes) / elapsed.Seconds()
			state.probeBytesPerSec = speed
		}
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) markProven(shard archive.ShardID, peer *overlayPeer) {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return
	}

	now := time.Now()
	stateKey := archivePeerPoolKey(shard)

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}
	marked := p.markProvenLocked(stateKey, peer, peerID, now)
	if marked {
		p.relaxProbeFailureLocked(stateKey, peerID)
	}
	p.mx.Unlock()
	if marked {
		p.clearTransportBlocked(peerID)
		p.scout.retry.clearPeer(peerID)
	}
}

func (p *archivePeerPool) markSuccess(shard archive.ShardID, peer *overlayPeer) {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return
	}

	now := time.Now()
	stateKey := archivePeerPoolKey(shard)

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}
	marked := p.markSuccessLocked(stateKey, peer, peerID, now)
	if marked {
		p.relaxFailureLocked(stateKey, peerID)
	}
	p.mx.Unlock()
	if marked {
		p.clearTransportBlocked(peerID)
		p.scout.retry.clearPeer(peerID)
	}
}

func (p *archivePeerPool) markProvenLocked(stateKey string, peer *overlayPeer, peerID PeerID, now time.Time) bool {
	entry := p.peers[peerID]
	if entry == nil || entry.peer != peer {
		return false
	}
	state := p.shardPeerLocked(stateKey, peerID)
	state.lastBytesAt = now
	state.probeSuccesses++
	delete(p.rejectedUntil, peerID)
	return true
}

func (p *archivePeerPool) markSuccessLocked(stateKey string, peer *overlayPeer, peerID PeerID, now time.Time) bool {
	if !p.markProvenLocked(stateKey, peer, peerID, now) {
		return false
	}
	entry := p.peers[peerID]
	entry.archiveDownloads++
	state := p.shardPeerLocked(stateKey, peerID)
	state.archiveDownloads++
	p.rememberValuablePeerLocked(entry, now)
	return true
}

// relaxFailureLocked rewards a completed archive download: the shard cooldown
// lifts and transport failures reset. Bad-import strikes stay because a valid
// download says nothing about whether the package covers the requested blocks.
func (p *archivePeerPool) relaxFailureLocked(stateKey string, peerID PeerID) {
	state := p.shards[stateKey]
	if state == nil {
		return
	}
	peer := state.peers[peerID]
	if peer != nil {
		peer.failure.notAvailable = 0
		peer.failure.probeErrors = 0
		peer.failure.downloadErrors = 0
		peer.cooldownUntil = time.Time{}
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}
}

// relaxProbeFailureLocked rewards a peer that returned actual archive probe
// bytes without forgiving failures from full-size slices. A peer that serves a
// small prefix and repeatedly stalls later must still rotate out.
func (p *archivePeerPool) relaxProbeFailureLocked(stateKey string, peerID PeerID) {
	state := p.shards[stateKey]
	if state == nil {
		return
	}
	peer := state.peers[peerID]
	if peer == nil {
		return
	}
	peer.failure.notAvailable = 0
	peer.failure.probeErrors = 0
	if peer.failure.downloadErrors == 0 && peer.failure.badImports == 0 {
		peer.cooldownUntil = time.Time{}
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}
}

func (p *archivePeerPool) noteFailure(shard archive.ShardID, peer *overlayPeer, reason string) archivePeerFailureVerdict {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return archivePeerFailureVerdict{}
	}

	stateKey := archivePeerPoolKey(shard)
	now := time.Now()

	p.mx.Lock()
	entry := p.peers[peerID]
	if p.closed || entry == nil || entry.peer != peer {
		p.mx.Unlock()
		return archivePeerFailureVerdict{}
	}
	verdict := p.noteFailureLocked(stateKey, peerID, reason, now)
	p.mx.Unlock()
	p.logArchivePeerCooldown(peer, stateKey, reason, verdict.cooldown)
	return verdict
}

func (p *archivePeerPool) noteFailureLocked(stateKey string, peerID PeerID, reason string, now time.Time) archivePeerFailureVerdict {
	peer := p.shardPeerLocked(stateKey, peerID)
	recordArchivePeerFailure(&peer.failure, reason)
	verdict := archivePeerFailureVerdict{
		useless: archivePeerFailureUseless(peer.failure),
	}
	verdict.cooldown = archivePeerFailureCooldown(peer.failure, reason, verdict.useless)
	if verdict.cooldown > 0 {
		peer.cooldownUntil = now.Add(verdict.cooldown)
	}
	return verdict
}

func (p *archivePeerPool) logArchivePeerCooldown(peer *overlayPeer, stateKey string, reason string, cooldown time.Duration) {
	if cooldown <= 0 {
		return
	}
	p.log.Debug().
		Str("peer", peer.addr).
		Str("archive_pool", stateKey).
		Str("reason", reason).
		Dur("duration", cooldown).
		Msg("temporarily cooled down archive peer")
}

// provenPeerLocked reports whether the peer has served real archive bytes.
func (p *archivePeerPool) provenPeerLocked(peerID PeerID) bool {
	entry := p.peers[peerID]
	if entry != nil && entry.archiveDownloads > 0 {
		return true
	}
	_, ok := p.valuable[peerID]
	return ok
}

func (p *archivePeerPool) rememberRejectedLocked(peerID PeerID, now time.Time, ttl time.Duration) {
	until := now.Add(ttl)
	if current := p.rejectedUntil[peerID]; !current.IsZero() {
		if current.Before(until) {
			p.rejectedUntil[peerID] = until
		}
		return
	}
	archiveBoundPeerRetryCache(p.rejectedUntil, now, archivePeerLocalRetryCacheLimit)
	p.rejectedUntil[peerID] = until
}

func (p *archivePeerPool) rememberTransportBlocked(peerID PeerID) {
	if peerID.IsZero() {
		return
	}
	now := time.Now()
	p.mx.Lock()
	if !p.closed {
		until := now.Add(archivePeerUnreachableTTL)
		if current := p.transportBlockedUntil[peerID]; !current.IsZero() {
			if current.Before(until) {
				p.transportBlockedUntil[peerID] = until
			}
			p.mx.Unlock()
			return
		}
		archiveBoundPeerRetryCache(p.transportBlockedUntil, now, archivePeerLocalRetryCacheLimit)
		p.transportBlockedUntil[peerID] = until
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) transportBlocked(peerID PeerID, now time.Time) bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	return p.transportBlockedLocked(peerID, now)
}

func (p *archivePeerPool) transportBlockedLocked(peerID PeerID, now time.Time) bool {
	until := p.transportBlockedUntil[peerID]
	if until.IsZero() {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(p.transportBlockedUntil, peerID)
	return false
}

func (p *archivePeerPool) clearTransportBlocked(peerID PeerID) {
	p.mx.Lock()
	delete(p.transportBlockedUntil, peerID)
	p.mx.Unlock()
}

func (p *archivePeerPool) recentlyRejected(peerID PeerID, now time.Time) bool {
	p.mx.Lock()
	defer p.mx.Unlock()

	return p.recentlyRejectedLocked(peerID, now)
}

func (p *archivePeerPool) recentlyRejectedLocked(peerID PeerID, now time.Time) bool {
	until, ok := p.rejectedUntil[peerID]
	if !ok {
		return false
	}
	if now.After(until) {
		delete(p.rejectedUntil, peerID)
		return false
	}
	return true
}

func (p *archivePeerPool) coolingDown(shard archive.ShardID, peer *overlayPeer) bool {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return false
	}

	stateKey := archivePeerPoolKey(shard)
	now := time.Now()

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return false
	}
	state := p.shards[stateKey]
	coolingDown := archivePeerCoolingDownLocked(state, peerID, now)
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}
	return coolingDown
}

func (p *archivePeerPool) rotateUseless(shard archive.ShardID) int {
	stateKey := archivePeerPoolKey(shard)

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return 0
	}
	state := p.shards[stateKey]
	if state == nil || len(state.peers) == 0 {
		p.mx.Unlock()
		return 0
	}
	candidates := make([]archivePeerRotationCandidate, 0, len(state.peers))
	for peerID, health := range state.peers {
		entry := p.peers[peerID]
		if entry == nil {
			continue
		}
		if entry.leases > 0 {
			continue
		}
		if health == nil || !archivePeerFailureUseless(health.failure) {
			continue
		}
		candidates = append(candidates, archivePeerRotationCandidate{peerID: peerID, failure: health.failure})
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}
	p.mx.Unlock()
	if len(candidates) == 0 {
		return 0
	}

	rotated := 0
	for _, candidate := range candidates {
		if p.rotatePeer(stateKey, candidate) {
			rotated++
		}
	}
	if rotated == 0 {
		return 0
	}

	p.log.Info().
		Int("rotated_peers", rotated).
		Str("archive_pool", stateKey).
		Msg("rotated useless archive peers")
	return rotated
}

func (p *archivePeerPool) rotatePeer(stateKey string, candidate archivePeerRotationCandidate) bool {
	now := time.Now()

	p.mx.Lock()
	entry := p.peers[candidate.peerID]
	if entry == nil {
		p.dropRotatedFailureLocked(stateKey, candidate.peerID)
		p.mx.Unlock()
		return false
	}
	if entry.leases > 0 {
		p.mx.Unlock()
		return false
	}
	state := p.shards[stateKey]
	if state == nil {
		p.mx.Unlock()
		return false
	}
	health := state.peers[candidate.peerID]
	if health == nil || !archivePeerFailureUseless(health.failure) {
		p.mx.Unlock()
		return false
	}
	candidate.failure = health.failure
	if p.peerUsefulOutsideShardLocked(candidate.peerID, stateKey) {
		until := now.Add(archivePeerReplacementCooldown)
		if health.cooldownUntil.Before(until) {
			health.cooldownUntil = until
		}
		p.mx.Unlock()
		return true
	}
	proven := p.provenPeerLocked(candidate.peerID)
	// removePeerLocked only hands back peers this pool owns, so capture the
	// address for logging before the entry is dropped.
	addr := entry.peer.addr
	peer, _ := p.removePeerLocked(candidate.peerID)
	retryAfter := time.Duration(0)
	switch {
	case candidate.failure.badImports >= archivePeerBadImportRotateThreshold:
		retryAfter = archivePeerNoBenefitTTL
	case archivePeerFailureErrors(candidate.failure) >= archivePeerErrorRotateThreshold:
		retryAfter = archivePeerUnreachableTTL
	case !proven:
		retryAfter = archivePeerNotAvailableTTL
	}
	if proven {
		valuable := p.valuable[candidate.peerID]
		valuable.nextTryAt = now.Add(archiveValuablePeerRetry)
		p.valuable[candidate.peerID] = valuable
	} else if retryAfter > 0 {
		p.rememberRejectedLocked(candidate.peerID, now, retryAfter)
	}
	p.mx.Unlock()
	closeArchiveOnlyPeer(peer)

	p.log.Info().
		Str("peer", addr).
		Str("peer_id", candidate.peerID.String()).
		Str("archive_pool", stateKey).
		Str("reason", candidate.failure.reason).
		Int("not_available", candidate.failure.notAvailable).
		Int("probe_errors", candidate.failure.probeErrors).
		Int("download_errors", candidate.failure.downloadErrors).
		Int("bad_imports", candidate.failure.badImports).
		Msg("rotated useless archive peer")
	return true
}

func (p *archivePeerPool) peerUsefulOutsideShardLocked(peerID PeerID, stateKey string) bool {
	for otherKey, state := range p.shards {
		if otherKey == stateKey || state == nil {
			continue
		}
		peer := state.peers[peerID]
		if peer != nil && (peer.probeSuccesses > 0 || peer.archiveDownloads > 0) {
			return true
		}
	}
	for _, demand := range p.demands {
		if archivePeerPoolKey(demand.probe.shard) == stateKey {
			continue
		}
		if demand.peers[peerID].evidence >= archivePeerDemandAvailable {
			return true
		}
	}
	return false
}

func (p *archivePeerPool) dropRotatedFailureLocked(stateKey string, peerID PeerID) {
	state := p.shards[stateKey]
	if state == nil {
		return
	}
	if health := state.peers[peerID]; health != nil {
		health.failure = archivePeerFailure{}
		health.cooldownUntil = time.Time{}
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}
}

func (p *archivePeerPool) shardStateLocked(stateKey string) *archiveShardPeerState {
	state := p.shards[stateKey]
	if state == nil {
		state = &archiveShardPeerState{peers: map[PeerID]*archiveShardPeer{}}
		p.shards[stateKey] = state
	}
	return state
}

func (p *archivePeerPool) shardPeerLocked(stateKey string, peerID PeerID) *archiveShardPeer {
	state := p.shardStateLocked(stateKey)
	peer := state.peers[peerID]
	if peer == nil {
		peer = &archiveShardPeer{}
		state.peers[peerID] = peer
	}
	return peer
}

func (p *archivePeerPool) rememberValuablePeerLocked(entry *archivePeer, now time.Time) {
	if entry == nil || entry.peer == nil || entry.peer.id.IsZero() {
		return
	}

	peerID := entry.peer.id
	valuable := p.valuable[peerID]
	valuable.peerID = peerID
	valuable.endpoint = entry.peer.addr
	valuable.pub = append(valuable.pub[:0], entry.peer.pub...)
	valuable.lastSuccessAt = now
	valuable.nextTryAt = time.Time{}
	p.valuable[peerID] = valuable

	if len(p.valuable) <= archiveValuablePeerLimit {
		return
	}
	var oldestID PeerID
	var oldestAt time.Time
	for candidateID, candidate := range p.valuable {
		if p.peers[candidateID] != nil {
			continue
		}
		if oldestID.IsZero() || candidate.lastSuccessAt.Before(oldestAt) {
			oldestID = candidateID
			oldestAt = candidate.lastSuccessAt
		}
	}
	if !oldestID.IsZero() {
		delete(p.valuable, oldestID)
	}
}

func (p *archivePeerPool) ready(shard archive.ShardID) bool {
	return len(p.candidates(shard)) > 0
}

func (p *archivePeerPool) readyArchive(shard archive.ShardID, masterchainSeqno uint32) bool {
	return len(p.candidatesForArchive(shard, masterchainSeqno)) > 0
}

func (p *archivePeerPool) refreshUseless(ctx context.Context, shard archive.ShardID) <-chan struct{} {
	if p.rotateUseless(shard) == 0 {
		return nil
	}
	return p.refill(ctx, true)
}

func (p *archivePeerPool) runArchiveDiscoveryLoop() {
	ticker := time.NewTicker(archiveDiscoveryLoopInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if p.shouldRunContinuousDiscovery() {
				p.refill(p.ctx, false)
			}
		}
	}
}

func (p *archivePeerPool) shouldRunContinuousDiscovery() bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	return !p.closed && (p.continuousDiscovery || len(p.demands) > 0)
}

func (p *archivePeerPool) refill(ctx context.Context, urgent bool) <-chan struct{} {
	if p.isClosed() {
		return nil
	}

	now := time.Now()
	p.bootstrapLivePeers()
	p.pruneClosedPeers()
	if p.sub.spec.usesDedicatedQueryPeers() {
		return nil
	}
	p.pruneUnprovenDeadArchiveOnlyPeers(now)
	p.pruneStaleArchiveOnlyPeers()
	p.offerValuablePeers(now, urgent)
	discover := p.shouldDiscoverDHT(now, urgent)
	if urgent {
		p.expandArchiveRandomPeers(ctx, archiveRandomPeerUrgentFanout)
	} else if discover {
		p.expandArchiveRandomPeers(ctx, archiveRandomPeerCalmFanout)
	}
	if discover {
		return p.startDHTDiscovery(ctx)
	}
	return nil
}

// DHT is only a periodic seed for the archive crawler. Newly learned peers are
// expanded recursively through overlay.getRandomPeers by the scout workers.
func (p *archivePeerPool) shouldDiscoverDHT(now time.Time, urgent bool) bool {
	p.discoveryMx.Lock()
	running := p.discoveryRunning
	nextDiscoveryAt := p.nextDiscoveryAt
	lastDiscoveryAt := p.lastDiscoveryAt
	p.discoveryMx.Unlock()
	if running {
		return true
	}
	if urgent {
		return lastDiscoveryAt.IsZero() || now.Sub(lastDiscoveryAt) >= archiveDiscoveryUrgentInterval
	}
	return !now.Before(nextDiscoveryAt)
}

func (p *archivePeerPool) archiveOnlySize() int {
	p.mx.Lock()
	defer p.mx.Unlock()

	return p.archiveOnlySizeLocked()
}

func (p *archivePeerPool) archiveOnlySizeLocked() int {
	return len(p.peers)
}

func (p *archivePeerPool) valuableSize() int {
	p.mx.Lock()
	defer p.mx.Unlock()
	return len(p.valuable)
}

func (p *archivePeerPool) offerValuablePeers(now time.Time, urgent bool) {
	limit := 1
	retry := archiveValuablePeerRetry
	if urgent {
		limit = 4
		retry = archiveValuablePeerStarvedRetry
	}

	p.mx.Lock()
	candidates := make([]archiveValuablePeer, 0, len(p.valuable))
	for peerID, valuable := range p.valuable {
		if p.peers[peerID] != nil {
			continue
		}
		if _, scouting := p.scouting[peerID]; scouting || now.Before(valuable.nextTryAt) {
			continue
		}
		if valuable.endpoint == "" || len(valuable.pub) != ed25519.PublicKeySize {
			continue
		}
		candidates = append(candidates, valuable)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].nextTryAt.Equal(candidates[j].nextTryAt) {
			return candidates[i].nextTryAt.Before(candidates[j].nextTryAt)
		}
		return candidates[i].lastSuccessAt.After(candidates[j].lastSuccessAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for _, candidate := range candidates {
		valuable := p.valuable[candidate.peerID]
		valuable.nextTryAt = now.Add(retry)
		p.valuable[candidate.peerID] = valuable
	}
	p.mx.Unlock()

	for _, candidate := range candidates {
		p.offerArchiveValuable(candidate)
	}
}

func (p *archivePeerPool) provenUsableSizeLocked(now time.Time) int {
	if p.closed {
		return 0
	}

	count := 0
	for peerID, entry := range p.peers {
		if !archivePeerUsable(entry, now) {
			continue
		}
		if p.peerHasArchiveBytesLocked(peerID) {
			count++
		}
	}
	return count
}

func (p *archivePeerPool) peerHasArchiveBytesLocked(peerID PeerID) bool {
	for _, state := range p.shards {
		if state == nil || state.peers[peerID] == nil {
			continue
		}
		peer := state.peers[peerID]
		if peer.probeSuccesses > 0 || peer.archiveDownloads > 0 {
			return true
		}
	}
	return false
}

func (p *archivePeerPool) pruneClosedPeers() {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}

	var archiveOnly []*overlayPeer
	for peerID, entry := range p.peers {
		if entry == nil {
			delete(p.peers, peerID)
			continue
		}
		if entry.leases > 0 {
			continue
		}
		if entry.peer != nil && entry.peer.hasOpenConnection() {
			continue
		}

		if peer, _ := p.removePeerLocked(peerID); peer != nil {
			archiveOnly = append(archiveOnly, peer)
		}
	}
	p.mx.Unlock()

	for _, peer := range archiveOnly {
		closeArchiveOnlyPeer(peer)
	}
}

func (p *archivePeerPool) pruneUnprovenDeadArchiveOnlyPeers(now time.Time) {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}

	var archiveOnly []*overlayPeer
	for peerID, entry := range p.peers {
		if !archivePeerUnprovenDeadArchiveOnly(entry, now) {
			continue
		}
		if peer, _ := p.removePeerLocked(peerID); peer != nil {
			archiveOnly = append(archiveOnly, peer)
		}
	}
	p.mx.Unlock()

	for _, peer := range archiveOnly {
		closeArchiveOnlyPeer(peer)
	}
}

func (p *archivePeerPool) pruneStaleArchiveOnlyPeers() {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}

	archiveOnly := make([]*overlayPeer, 0)
	for p.archiveOnlySizeLocked() > archivePeerRosterLimit {
		peerID, entry := p.archivePeerEvictionCandidateLocked()
		if entry == nil {
			break
		}
		if peer, _ := p.removePeerLocked(peerID); peer != nil {
			archiveOnly = append(archiveOnly, peer)
		}
	}
	p.mx.Unlock()

	for _, peer := range archiveOnly {
		closeArchiveOnlyPeer(peer)
	}
}

// removePeerLocked drops the pool entry for peerID. It reports whether an entry
// was actually removed, and separately hands back the peer only when this pool
// owns it and is therefore responsible for closing it. Callers that need "was it
// removed" must use removed, not a non-nil peer: peers shared with the
// subscription roster are removed but never returned.
func (p *archivePeerPool) removePeerLocked(peerID PeerID) (owned *overlayPeer, removed bool) {
	entry := p.peers[peerID]
	if entry == nil {
		return nil, false
	}

	delete(p.peers, peerID)
	if _, valuable := p.valuable[peerID]; !valuable {
		for stateKey, state := range p.shards {
			if state == nil {
				delete(p.shards, stateKey)
				continue
			}
			delete(state.peers, peerID)
			if archiveShardPeerStateEmpty(state) {
				delete(p.shards, stateKey)
			}
		}
		for _, demand := range p.demands {
			delete(demand.peers, peerID)
		}
	}
	if entry.owned {
		return entry.peer, true
	}
	return nil, true
}

func (p *archivePeerPool) startDHTDiscovery(ctx context.Context) <-chan struct{} {
	if ctx.Err() != nil || p.sub.node.dht == nil || !p.sub.isActive() {
		return nil
	}

	p.discoveryMx.Lock()
	if p.ctx.Err() != nil {
		p.discoveryMx.Unlock()
		return nil
	}
	if p.discoveryRunning {
		done := p.discoveryDone
		p.discoveryMx.Unlock()
		return done
	}
	p.discoveryRunning = true
	p.lastDiscoveryAt = time.Now()
	done := make(chan struct{})
	p.discoveryDone = done
	p.discoveryMx.Unlock()

	p.sub.node.runAsync(func() {
		p.discoverFromDHT()

		p.discoveryMx.Lock()
		p.discoveryRunning = false
		p.discoveryDone = nil
		p.nextDiscoveryAt = time.Now().Add(archiveDiscoveryInterval)
		close(done)
		p.discoveryMx.Unlock()
	})
	return done
}

func (p *archivePeerPool) discoverFromDHT() {
	var (
		nodesSeen    int
		queued       int
		known        int
		backoff      int
		duplicates   int
		queueFull    int
		invalid      int
		startedAt    = time.Now()
		archiveStart = p.archiveOnlySize()
	)

	now := time.Now()
	p.pruneClosedPeers()
	p.pruneUnprovenDeadArchiveOnlyPeers(now)
	lookupCtx, cancel := context.WithTimeout(p.ctx, dhtFindTimeout)
	nodes, _, err := p.sub.node.dht.FindOverlayNodes(lookupCtx, p.sub.spec.FullID)
	cancel()
	if nodes == nil && err == nil {
		err = errors.New("archive DHT returned nil node list")
	}
	if nodes != nil {
		nodesSeen = len(nodes.List)
		for _, node := range nodes.List {
			switch p.offerArchiveNode(node) {
			case archivePeerOfferQueued:
				queued++
			case archivePeerOfferKnown:
				known++
			case archivePeerOfferBackoff:
				backoff++
			case archivePeerOfferDuplicate:
				duplicates++
			case archivePeerOfferQueueFull:
				queueFull++
			default:
				invalid++
			}
		}
	}

	scoutStats := p.scoutStats.snapshot()
	p.log.Info().
		Err(err).
		Dur("elapsed", time.Since(startedAt)).
		Int("dht_seed_queries", 1).
		Int("dht_nodes", nodesSeen).
		Int("queued", queued).
		Int("known", known).
		Int("backoff", backoff).
		Int("duplicates", duplicates).
		Int("queue_full", queueFull).
		Int("invalid", invalid).
		Int("archive_only_peers_before", archiveStart).
		Int("archive_only_peers", p.archiveOnlySize()).
		Int("valuable_peers", p.valuableSize()).
		Int("scouting_peers", p.scoutingSize()).
		Uint64("scout_attempts_total", scoutStats.attempts).
		Uint64("scout_proven_total", scoutStats.proven).
		Uint64("scout_available_total", scoutStats.available).
		Uint64("scout_admitted_total", scoutStats.admitted).
		Uint64("scout_replaced_total", scoutStats.replaced).
		Uint64("scout_not_available_total", scoutStats.notAvailable).
		Uint64("scout_transport_failures_total", scoutStats.transportFailure).
		Uint64("scout_busy_total", scoutStats.busy).
		Uint64("scout_invalid_total", scoutStats.invalid).
		Uint64("scout_no_benefit_total", scoutStats.noBenefit).
		Uint64("transient_random_queries_total", scoutStats.transientRandomQueries).
		Uint64("transient_random_responses_total", scoutStats.transientRandomResponses).
		Uint64("transient_random_received_nodes_total", scoutStats.transientRandomReceivedNodes).
		Uint64("transient_random_processed_nodes_total", scoutStats.transientRandomProcessedNodes).
		Uint64("transient_random_queued_total", scoutStats.transientRandomQueued).
		Msg("archive peer seed search finished")
}

func (p *archivePeerPool) beginArchiveRequest(shard archive.ShardID, masterchainSeqno uint32) (archivePeerProbe, func(), error) {
	probe := archivePeerProbe{shard: shard, seqno: masterchainSeqno}
	return p.beginDemand(archiveDemandKey(shard, masterchainSeqno), probe)
}

func (p *archivePeerPool) beginZeroStateRequest(shard archive.ShardID, block ton.BlockIDExt) (archivePeerProbe, func(), error) {
	probe := archivePeerProbe{shard: shard, block: block, zeroState: true}
	return p.beginDemand(zeroStateDemandKey(block), probe)
}

func archiveDemandKey(shard archive.ShardID, masterchainSeqno uint32) string {
	return fmt.Sprintf("archive:%d:%016x:%d", shard.Workchain, uint64(shard.Shard), masterchainSeqno)
}

func zeroStateDemandKey(block ton.BlockIDExt) string {
	return fmt.Sprintf("zero:%d:%016x:%d:%x:%x", block.Workchain, uint64(block.Shard), block.SeqNo, block.RootHash, block.FileHash)
}

func (p *archivePeerPool) beginDemand(key string, probe archivePeerProbe) (archivePeerProbe, func(), error) {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return archivePeerProbe{}, nil, errArchiveSessionClosed
	}
	if demandID := p.demandKeys[key]; demandID != 0 {
		demand := p.demands[demandID]
		demand.refs++
		probe = demand.probe
		p.mx.Unlock()
		return probe, p.demandRelease(demandID), nil
	}

	p.nextDemandID++
	probe.demandID = p.nextDemandID
	demand := &archivePeerDemand{
		key:       key,
		probe:     probe,
		refs:      1,
		createdAt: time.Now(),
		peers:     map[PeerID]archivePeerDemandPeer{},
	}
	p.demands[probe.demandID] = demand
	p.demandKeys[key] = probe.demandID
	p.mx.Unlock()
	return probe, p.demandRelease(probe.demandID), nil
}

func (p *archivePeerPool) demandRelease(demandID uint64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mx.Lock()
			demand := p.demands[demandID]
			if demand != nil {
				demand.refs--
				if demand.refs == 0 {
					delete(p.demands, demandID)
					delete(p.demandKeys, demand.key)
				}
			}
			p.mx.Unlock()
		})
	}
}

func (p *archivePeerPool) probeSnapshots(limit int) []archivePeerProbe {
	if limit <= 0 {
		return nil
	}

	now := time.Now()
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.closed || len(p.demands) == 0 {
		return nil
	}

	selected := make(map[uint64]struct{}, limit)
	probes := make([]archivePeerProbe, 0, min(limit, len(p.demands)))
	for len(probes) < limit && len(selected) < len(p.demands) {
		var best *archivePeerDemand
		bestPositive := 0
		for demandID, demand := range p.demands {
			if _, ok := selected[demandID]; ok {
				continue
			}
			positive := p.demandPositiveRosterPeersLocked(demand, now)
			if best == nil || positive < bestPositive || positive == bestPositive && (demand.lastScoutAt.Before(best.lastScoutAt) || demand.lastScoutAt.Equal(best.lastScoutAt) && demand.probe.demandID < best.probe.demandID) {
				best = demand
				bestPositive = positive
			}
		}
		if best == nil {
			break
		}
		best.lastScoutAt = now
		selected[best.probe.demandID] = struct{}{}
		probes = append(probes, best.probe)
	}
	return probes
}

func (p *archivePeerPool) demandPositiveRosterPeersLocked(demand *archivePeerDemand, now time.Time) int {
	positive := 0
	for peerID, state := range demand.peers {
		if state.evidence < archivePeerDemandAvailable || !archivePeerUsable(p.peers[peerID], now) {
			continue
		}
		positive++
	}
	return positive
}

func (p *archivePeerPool) demandPeerBlocked(probe archivePeerProbe, peerID PeerID, now time.Time) bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	demand := p.demands[probe.demandID]
	if demand == nil {
		return true
	}
	state := demand.peers[peerID]
	return p.demandRetryBlockedLocked(demand.key, peerID, now) || now.Before(state.rejectedUntil) || now.Before(state.noBenefitUntil)
}

func (p *archivePeerPool) recordDemandNotAvailable(probe archivePeerProbe, peerID PeerID, ttl time.Duration) {
	p.mx.Lock()
	defer p.mx.Unlock()
	demand := p.demands[probe.demandID]
	if demand == nil {
		return
	}
	p.recordDemandNotAvailableLocked(demand, peerID, time.Now(), ttl)
}

func (p *archivePeerPool) recordDemandNotAvailableLocked(demand *archivePeerDemand, peerID PeerID, now time.Time, ttl time.Duration) {
	state := demand.peers[peerID]
	state.evidence = archivePeerDemandNotAvailable
	state.at = now
	until := now.Add(ttl)
	if state.rejectedUntil.Before(until) {
		state.rejectedUntil = until
	}
	demand.peers[peerID] = state
	p.rememberDemandRetryLocked(demand.key, peerID, until, now)
}

func (p *archivePeerPool) recordDemandNoBenefit(probe archivePeerProbe, peerID PeerID, ttl time.Duration) {
	p.mx.Lock()
	defer p.mx.Unlock()
	demand := p.demands[probe.demandID]
	if demand == nil {
		return
	}
	state := demand.peers[peerID]
	now := time.Now()
	until := now.Add(ttl)
	if state.noBenefitUntil.Before(until) {
		state.noBenefitUntil = until
	}
	demand.peers[peerID] = state
	p.rememberDemandRetryLocked(demand.key, peerID, until, now)
}

func (p *archivePeerPool) rememberDemandRetryLocked(demand string, peerID PeerID, until time.Time, now time.Time) {
	key := archivePeerDemandRetryKey{demand: demand, peerID: peerID}
	if current, ok := p.demandBlockedUntil[key]; ok {
		if current.Before(until) {
			p.demandBlockedUntil[key] = until
		}
		return
	}
	archiveBoundDemandRetryCache(p.demandBlockedUntil, now, archivePeerLocalRetryCacheLimit)
	p.demandBlockedUntil[key] = until
}

func (p *archivePeerPool) demandRetryBlockedLocked(demand string, peerID PeerID, now time.Time) bool {
	key := archivePeerDemandRetryKey{demand: demand, peerID: peerID}
	until := p.demandBlockedUntil[key]
	if until.IsZero() {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(p.demandBlockedUntil, key)
	return false
}

func (p *archivePeerPool) clearDemandRetryLocked(demand string, peerID PeerID) {
	delete(p.demandBlockedUntil, archivePeerDemandRetryKey{demand: demand, peerID: peerID})
}

func archiveBoundDemandRetryCache(entries map[archivePeerDemandRetryKey]time.Time, now time.Time, limit int) {
	if len(entries) < limit {
		return
	}

	var oldestKey archivePeerDemandRetryKey
	var oldest time.Time
	for key, until := range entries {
		if !now.Before(until) {
			delete(entries, key)
			continue
		}
		if oldest.IsZero() || until.Before(oldest) {
			oldestKey = key
			oldest = until
		}
	}
	if len(entries) >= limit && !oldest.IsZero() {
		delete(entries, oldestKey)
	}
}

func (p *archivePeerPool) recordArchiveDemandEvidence(shard archive.ShardID, masterchainSeqno uint32, peer *overlayPeer, evidence archivePeerDemandEvidence) {
	p.recordDemandEvidence(archiveDemandKey(shard, masterchainSeqno), peer, evidence)
}

func (p *archivePeerPool) recordZeroStateDemandEvidence(block ton.BlockIDExt, peer *overlayPeer, evidence archivePeerDemandEvidence) {
	p.recordDemandEvidence(zeroStateDemandKey(block), peer, evidence)
}

func (p *archivePeerPool) recordDemandEvidence(key string, peer *overlayPeer, evidence archivePeerDemandEvidence) {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() || evidence < archivePeerDemandAvailable {
		return
	}

	p.mx.Lock()
	demand := p.demands[p.demandKeys[key]]
	entry := p.peers[peerID]
	if !p.closed && demand != nil && entry != nil && entry.peer == peer {
		demand.peers[peerID] = archivePeerDemandPeer{
			evidence: evidence,
			at:       time.Now(),
		}
		p.clearDemandRetryLocked(key, peerID)
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) recordArchiveDemandNotAvailable(shard archive.ShardID, masterchainSeqno uint32, peer *overlayPeer) {
	p.recordDemandNotAvailableByKey(archiveDemandKey(shard, masterchainSeqno), peer)
}

func (p *archivePeerPool) recordZeroStateDemandNotAvailable(block ton.BlockIDExt, peer *overlayPeer) {
	p.recordDemandNotAvailableByKey(zeroStateDemandKey(block), peer)
}

func (p *archivePeerPool) recordDemandNotAvailableByKey(key string, peer *overlayPeer) {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return
	}

	p.mx.Lock()
	demand := p.demands[p.demandKeys[key]]
	entry := p.peers[peerID]
	if !p.closed && demand != nil && entry != nil && entry.peer == peer {
		p.recordDemandNotAvailableLocked(demand, peerID, time.Now(), archivePeerNotAvailableTTL)
	}
	p.mx.Unlock()
}

func recordArchivePeerFailure(failure *archivePeerFailure, reason string) {
	switch reason {
	case archivePeerRejectNotAvailable, archivePeerRejectStateNotAvailable:
		failure.notAvailable++
	case ArchivePeerRejectImportIncomplete:
		failure.badImports++
	case ArchivePeerRejectImportFailed:
		failure.badImports++
	case archivePeerRejectCandidateFailed:
		failure.probeErrors++
	case archivePeerRejectDownloadFailed, archivePeerRejectStateDownloadFailed:
		failure.downloadErrors++
	default:
		failure.downloadErrors++
	}
	failure.reason = reason
}

// archivePeerFailureUseless decides when an active slot should rotate. A proven
// peer is retained in the valuable reserve and can be retried later.
func archivePeerFailureUseless(failure archivePeerFailure) bool {
	if failure.badImports >= archivePeerBadImportRotateThreshold {
		return true
	}
	if archivePeerFailureErrors(failure) >= archivePeerErrorRotateThreshold {
		return true
	}
	return failure.notAvailable >= archivePeerNotAvailableRotateThreshold
}

func archivePeerFailureErrors(failure archivePeerFailure) int {
	return failure.probeErrors + failure.downloadErrors
}

func archivePeerFailureCooldown(failure archivePeerFailure, reason string, useless bool) time.Duration {
	switch reason {
	case archivePeerRejectNotAvailable, archivePeerRejectStateNotAvailable:
		return archiveNotAvailableCooldown(failure.notAvailable)
	case ArchivePeerRejectImportIncomplete, ArchivePeerRejectImportFailed:
		return archiveFailureCooldown
	case archivePeerRejectCandidateFailed, archivePeerRejectDownloadFailed, archivePeerRejectStateDownloadFailed:
		return archiveFailureCooldown
	}
	if useless {
		return archiveFailureCooldown
	}
	return 0
}

func archiveNotAvailableCooldown(strikes int) time.Duration {
	if strikes < 1 {
		strikes = 1
	}
	if strikes > len(archiveNotAvailableCooldowns) {
		strikes = len(archiveNotAvailableCooldowns)
	}
	return archiveNotAvailableCooldowns[strikes-1]
}

func archiveShardPeerStateEmpty(state *archiveShardPeerState) bool {
	return state == nil || len(state.peers) == 0
}

func archivePeerUsable(entry *archivePeer, now time.Time) bool {
	if entry == nil || entry.peer == nil || !entry.peer.hasOpenConnection() {
		return false
	}
	if entry.archiveDownloads > 0 {
		return true
	}
	return entry.peer.isAliveKnownOverlayPeer(now)
}

func archivePeerUnprovenDeadArchiveOnly(entry *archivePeer, now time.Time) bool {
	if entry == nil || entry.leases > 0 || entry.archiveDownloads > 0 {
		return false
	}
	peer := entry.peer
	return peer != nil && peer.hasOpenConnection() && !peer.isAliveKnownOverlayPeer(now)
}

func archivePeerCoolingDownLocked(state *archiveShardPeerState, peerID PeerID, now time.Time) bool {
	if state == nil || len(state.peers) == 0 {
		return false
	}
	if peerID.IsZero() {
		return false
	}

	peer := state.peers[peerID]
	if peer == nil || peer.cooldownUntil.IsZero() {
		return false
	}
	if now.Before(peer.cooldownUntil) {
		return true
	}

	peer.cooldownUntil = time.Time{}
	return false
}

func archivePeerPoolKey(shard archive.ShardID) string {
	if shard.Workchain == -1 {
		return "master"
	}
	return shard.String()
}

func closeArchiveOnlyPeer(peer *overlayPeer) {
	if peer == nil {
		return
	}

	peer.close()
}
