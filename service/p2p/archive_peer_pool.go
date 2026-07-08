package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/archive"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	archivePeerErrorRotateThreshold        = 3
	archivePeerNotAvailableRotateThreshold = 2
	archivePeerBadImportRotateThreshold    = 2
	archivePeerHardLimit                   = 256

	archiveProvenPeerTarget        = 4
	archiveDiscoveryRetryDelay     = time.Minute
	archivePeerRejectCacheTTL      = 20 * time.Minute
	archiveAvailablePeerTTL        = 30 * time.Minute
	archivePeerKeepaliveDelay      = 30 * time.Second
	archiveRandomPeerRefreshMinGap = 15 * time.Second
)

// archiveNotAvailableCooldowns holds the per-shard cooldown ladder for
// consecutive not-available answers; archival peers are scarce, so repeated
// misses back off instead of banishing the peer.
var archiveNotAvailableCooldowns = [...]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

type archivePeerOwner uint8

const (
	archivePeerBorrowedLive archivePeerOwner = iota
	archivePeerArchiveOnly
)

type archivePeerPool struct {
	sub *overlaySubscription

	mx            sync.Mutex
	closed        bool
	peers         map[PeerID]*archivePeer
	workchains    map[int32]map[PeerID]struct{}
	shards        map[string]*archiveShardPeerState
	rejectedUntil map[PeerID]time.Time
	probe         archivePeerProbe
	hasProbe      bool

	discoveryMx          sync.Mutex
	discoveryRunning     bool
	discoveryDone        chan struct{}
	nextDiscoveryAt      time.Time
	randomRefreshRunning bool
	nextRandomRefreshAt  time.Time
}

// archivePeerProbe captures what the pool is currently syncing so freshly
// discovered peers can be classified as archival with a single query.
type archivePeerProbe struct {
	shard     archive.ShardID
	seqno     uint32
	block     ton.BlockIDExt
	zeroState bool
}

type archivePeer struct {
	peer            *overlayPeer
	owner           archivePeerOwner
	leases          int
	lastArchiveAt   time.Time
	archiveSuccess  uint64
	lastAvailableAt time.Time
}

type archiveShardPeerState struct {
	cooldownUntil map[PeerID]time.Time
	failures      map[PeerID]archivePeerFailure
}

type archivePeerFailure struct {
	notAvailable int
	errors       int
	badImports   int
	reason       string
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
	pool := &archivePeerPool{
		sub:           sub,
		peers:         map[PeerID]*archivePeer{},
		workchains:    map[int32]map[PeerID]struct{}{},
		shards:        map[string]*archiveShardPeerState{},
		rejectedUntil: map[PeerID]time.Time{},
	}
	pool.bootstrapLivePeers()
	pool.startKeepalive()
	return pool
}

func (p *archivePeerPool) startKeepalive() {
	if p.sub == nil || p.sub.node == nil || p.sub.node.runCtx == nil {
		return
	}

	ctx := p.sub.node.runCtx
	p.sub.node.runAsync(func() {
		p.runKeepalive(ctx)
	})
}

func (p *archivePeerPool) Close() {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}
	p.closed = true
	archiveOnly := make([]*overlayPeer, 0, len(p.peers))
	for peerID, entry := range p.peers {
		if entry.owner == archivePeerArchiveOnly {
			archiveOnly = append(archiveOnly, entry.peer)
		}
		delete(p.peers, peerID)
	}
	p.workchains = map[int32]map[PeerID]struct{}{}
	p.shards = map[string]*archiveShardPeerState{}
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

func (p *archivePeerPool) bootstrapLivePeers() {
	for _, peer := range p.sub.peersSnapshot() {
		if peer == nil || peer.id.IsZero() || !peer.hasOpenConnection() {
			continue
		}
		p.addBorrowedPeer(peer)
	}
}

func (p *archivePeerPool) addBorrowedPeer(peer *overlayPeer) bool {
	if peer == nil || peer.id.IsZero() {
		return false
	}

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return false
	}
	if entry := p.peers[peer.id]; entry != nil {
		var closePeer *overlayPeer
		if entry.owner == archivePeerArchiveOnly && entry.leases == 0 {
			closePeer = entry.peer
			entry.owner = archivePeerBorrowedLive
			entry.peer = peer
		}
		p.mx.Unlock()
		closeArchiveOnlyPeer(closePeer)
		return false
	}
	p.peers[peer.id] = &archivePeer{peer: peer, owner: archivePeerBorrowedLive}
	p.mx.Unlock()
	return true
}

func (p *archivePeerPool) addArchiveOnlyPeer(peer *overlayPeer) bool {
	if peer == nil || peer.id.IsZero() {
		return false
	}

	p.pruneClosedPeers()
	p.pruneUnprovenDeadArchiveOnlyPeers(time.Now())

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return false
	}
	if p.peers[peer.id] != nil {
		return false
	}
	if p.recentlyRejectedLocked(peer.id, time.Now()) {
		return false
	}
	if len(p.peers) >= archivePeerHardLimit {
		return false
	}
	p.peers[peer.id] = &archivePeer{peer: peer, owner: archivePeerArchiveOnly}
	return true
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
	workchain := shard.Workchain

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return nil
	}

	state := p.shards[stateKey]
	provenIDs := p.workchains[workchain]
	proven := make([]*overlayPeer, 0, len(provenIDs))
	other := make([]*overlayPeer, 0, len(p.peers))
	for peerID, entry := range p.peers {
		if !archivePeerUsable(entry, now) {
			continue
		}
		peer := entry.peer
		if archivePeerCoolingDownLocked(state, peerID, now) {
			continue
		}
		if _, ok := provenIDs[peerID]; ok {
			proven = append(proven, peer)
			continue
		}
		other = append(other, peer)
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}

	peers := make([]*overlayPeer, 0, len(proven)+len(other))
	peers = append(peers, proven...)
	peers = append(peers, other...)
	return peers
}

func (p *archivePeerPool) downloadCandidates(session *ArchiveSession, shard archive.ShardID, peers []*overlayPeer) []*overlayPeer {
	peers = p.prioritize(shard, peers)

	selected := p.selectedPeer(session, shard)
	if selected == nil {
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

func (p *archivePeerPool) selectedPeer(session *ArchiveSession, shard archive.ShardID) *overlayPeer {
	if session == nil {
		return nil
	}

	peerID := session.selectedArchivePeerID(shard)
	if peerID.IsZero() {
		return nil
	}

	stateKey := archivePeerPoolKey(shard)
	now := time.Now()
	peer, reason := p.peerIfAvailable(shard, peerID, now)
	if reason == "" {
		return peer
	}

	session.clearSelectedArchivePeerID(shard, peerID)
	session.unpinArchivePeerID(peerID)
	event := p.sub.log.Info().
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
	if !peer.isAliveKnownOverlayPeer(now) && entry.archiveSuccess == 0 {
		return peer, "not_alive"
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
	return prioritizeArchivePeersWithLeases(shard, peers, p.leaseSnapshot(peers))
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

func (p *archivePeerPool) acquire(peer *overlayPeer) func() {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return func() {}
	}

	p.mx.Lock()
	entry := p.peers[peerID]
	if entry != nil {
		entry.leases++
	}
	p.mx.Unlock()

	return func() {
		p.mx.Lock()
		defer p.mx.Unlock()

		entry := p.peers[peerID]
		if entry == nil || entry.leases == 0 {
			return
		}
		entry.leases--
	}
}

func (p *archivePeerPool) markAvailable(shard archive.ShardID, peer *overlayPeer) {
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
	entry := p.peers[peerID]
	if entry == nil {
		entry = &archivePeer{peer: peer, owner: archivePeerBorrowedLive}
		p.peers[peerID] = entry
	}
	entry.lastAvailableAt = now
	delete(p.rejectedUntil, peerID)
	p.markWorkchainPeerLocked(shard.Workchain, peerID)
	p.relaxFailureLocked(stateKey, peerID)
	p.mx.Unlock()
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
	entry := p.peers[peerID]
	if entry == nil {
		entry = &archivePeer{peer: peer, owner: archivePeerBorrowedLive}
		p.peers[peerID] = entry
	}
	entry.lastArchiveAt = now
	entry.lastAvailableAt = now
	entry.archiveSuccess++
	delete(p.rejectedUntil, peerID)
	p.markWorkchainPeerLocked(shard.Workchain, peerID)
	p.relaxFailureLocked(stateKey, peerID)
	p.mx.Unlock()
}

func (p *archivePeerPool) markWorkchainPeerLocked(workchain int32, peerID PeerID) {
	peers := p.workchains[workchain]
	if peers == nil {
		peers = map[PeerID]struct{}{}
		p.workchains[workchain] = peers
	}
	peers[peerID] = struct{}{}
}

// relaxFailureLocked rewards a successful availability answer or download: the
// shard cooldown lifts, not-available strikes reset, and error strikes decay
// instead of clearing so a flapping peer still reaches the rotation threshold.
// Bad-import strikes stay: a download success says nothing about import health.
func (p *archivePeerPool) relaxFailureLocked(stateKey string, peerID PeerID) {
	state := p.shards[stateKey]
	if state == nil {
		return
	}
	if failure, ok := state.failures[peerID]; ok {
		failure.notAvailable = 0
		failure.errors /= 2
		if failure.notAvailable == 0 && failure.errors == 0 && failure.badImports == 0 {
			delete(state.failures, peerID)
		} else {
			state.failures[peerID] = failure
		}
	}
	if len(state.cooldownUntil) > 0 {
		delete(state.cooldownUntil, peerID)
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
	if p.closed {
		p.mx.Unlock()
		return archivePeerFailureVerdict{}
	}
	state := p.shardStateLocked(stateKey)
	if state.failures == nil {
		state.failures = map[PeerID]archivePeerFailure{}
	}
	failure := state.failures[peerID]
	recordArchivePeerFailure(&failure, reason)
	state.failures[peerID] = failure
	verdict := archivePeerFailureVerdict{
		useless: archivePeerFailureUseless(failure, p.provenPeerLocked(peerID)),
	}
	verdict.cooldown = archivePeerFailureCooldown(failure, reason, verdict.useless)
	if verdict.cooldown > 0 {
		if state.cooldownUntil == nil {
			state.cooldownUntil = map[PeerID]time.Time{}
		}
		state.cooldownUntil[peerID] = now.Add(verdict.cooldown)
	}
	p.mx.Unlock()

	if verdict.cooldown > 0 {
		p.sub.log.Debug().
			Str("peer", peer.addr).
			Str("archive_pool", stateKey).
			Str("reason", reason).
			Dur("duration", verdict.cooldown).
			Msg("temporarily cooled down archive peer")
	}
	return verdict
}

// provenPeerLocked reports whether the peer ever served archive data or
// answered archive availability for any workchain of this pool.
func (p *archivePeerPool) provenPeerLocked(peerID PeerID) bool {
	if entry := p.peers[peerID]; entry != nil && entry.archiveSuccess > 0 {
		return true
	}
	for _, peers := range p.workchains {
		if _, ok := peers[peerID]; ok {
			return true
		}
	}
	return false
}

func (p *archivePeerPool) rememberRejectedLocked(peerID PeerID, now time.Time) {
	for id, until := range p.rejectedUntil {
		if now.After(until) {
			delete(p.rejectedUntil, id)
		}
	}
	p.rejectedUntil[peerID] = now.Add(archivePeerRejectCacheTTL)
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
	if state == nil || len(state.failures) == 0 {
		p.mx.Unlock()
		return 0
	}
	candidates := make([]archivePeerRotationCandidate, 0, len(state.failures))
	for peerID, failure := range state.failures {
		entry := p.peers[peerID]
		if entry == nil {
			delete(state.failures, peerID)
			continue
		}
		// Borrowed live peers stay with the subscription; their failure
		// records keep escalating the not-available cooldown instead.
		if entry.owner != archivePeerArchiveOnly || entry.leases > 0 {
			continue
		}
		if !archivePeerFailureUseless(failure, p.provenPeerLocked(peerID)) {
			continue
		}
		candidates = append(candidates, archivePeerRotationCandidate{peerID: peerID, failure: failure})
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

	p.sub.log.Info().
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
	if entry.owner != archivePeerArchiveOnly || entry.leases > 0 {
		p.mx.Unlock()
		return false
	}
	peer := entry.peer
	proven := p.provenPeerLocked(candidate.peerID)
	delete(p.peers, candidate.peerID)
	for _, peers := range p.workchains {
		delete(peers, candidate.peerID)
	}
	p.dropRotatedFailureLocked(stateKey, candidate.peerID)
	if !proven {
		p.rememberRejectedLocked(candidate.peerID, now)
	}
	p.mx.Unlock()

	closeArchiveOnlyPeer(peer)

	p.sub.log.Info().
		Str("peer", peer.addr).
		Str("peer_id", candidate.peerID.String()).
		Str("archive_pool", stateKey).
		Str("reason", candidate.failure.reason).
		Int("not_available", candidate.failure.notAvailable).
		Int("errors", candidate.failure.errors).
		Int("bad_imports", candidate.failure.badImports).
		Msg("rotated useless archive peer")
	return true
}

func (p *archivePeerPool) dropRotatedFailureLocked(stateKey string, peerID PeerID) {
	state := p.shards[stateKey]
	if state == nil {
		return
	}
	if len(state.failures) > 0 {
		delete(state.failures, peerID)
	}
	if archiveShardPeerStateEmpty(state) {
		delete(p.shards, stateKey)
	}
}

func (p *archivePeerPool) shardStateLocked(stateKey string) *archiveShardPeerState {
	state := p.shards[stateKey]
	if state == nil {
		state = &archiveShardPeerState{}
		p.shards[stateKey] = state
	}
	return state
}

func (p *archivePeerPool) ready(shard archive.ShardID) bool {
	return len(p.candidates(shard)) > 0
}

func (p *archivePeerPool) refreshUseless(ctx context.Context, shard archive.ShardID) <-chan struct{} {
	if p.rotateUseless(shard) == 0 {
		return nil
	}
	return p.refill(ctx, true)
}

func (p *archivePeerPool) refill(ctx context.Context, forceDHT bool) <-chan struct{} {
	if p.isClosed() {
		return nil
	}

	p.bootstrapLivePeers()
	p.pruneClosedPeers()
	p.pruneUnprovenDeadArchiveOnlyPeers(time.Now())
	p.startRandomPeerRefresh(ctx)
	if p.shouldDiscoverDHT(time.Now(), forceDHT) {
		return p.startDHTDiscovery(ctx)
	}
	return nil
}

// shouldDiscoverDHT digs the DHT while the pool lacks proven archival peers.
// With zero proven peers the sync is blocked and discovery runs back to back;
// once at least one peer is serving, top-up runs are spaced out to spare the
// DHT and the overlay from constant walks.
func (p *archivePeerPool) shouldDiscoverDHT(now time.Time, force bool) bool {
	if p.discoverySatisfied(now) {
		return false
	}
	if force || p.provenUsableSize(now) == 0 {
		return true
	}
	return now.After(p.nextDiscoveryAllowedAt())
}

// discoverySatisfied reports whether DHT discovery may stop. The pool aims for
// a few proven archival peers; the raw usable count only bounds unclassified
// growth for pools that have not yet seen a request to probe against.
func (p *archivePeerPool) discoverySatisfied(now time.Time) bool {
	if p.provenUsableSize(now) >= archiveProvenPeerTarget {
		return true
	}
	if _, ok := p.probeSnapshot(); !ok {
		return p.usableSize(now) >= bootstrapDiscoveryTarget
	}
	return false
}

func (p *archivePeerPool) size() int {
	p.pruneClosedPeers()

	p.mx.Lock()
	defer p.mx.Unlock()
	return len(p.peers)
}

func (p *archivePeerPool) usableSize(now time.Time) int {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return 0
	}

	count := 0
	for _, entry := range p.peers {
		if archivePeerUsable(entry, now) {
			count++
		}
	}
	return count
}

func (p *archivePeerPool) provenUsableSize(now time.Time) int {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return 0
	}

	count := 0
	for peerID, entry := range p.peers {
		if !archivePeerUsable(entry, now) {
			continue
		}
		if p.provenPeerLocked(peerID) {
			count++
		}
	}
	return count
}

func (p *archivePeerPool) pruneClosedPeers() int {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return 0
	}

	var archiveOnly []*overlayPeer
	removed := 0
	for peerID, entry := range p.peers {
		if entry == nil {
			delete(p.peers, peerID)
			removed++
			continue
		}
		if entry.leases > 0 {
			continue
		}
		if entry.peer != nil && entry.peer.hasOpenConnection() {
			continue
		}

		if peer := p.removePeerLocked(peerID); peer != nil {
			archiveOnly = append(archiveOnly, peer)
		}
		removed++
	}
	p.mx.Unlock()

	for _, peer := range archiveOnly {
		closeArchiveOnlyPeer(peer)
	}
	return removed
}

func (p *archivePeerPool) pruneUnprovenDeadArchiveOnlyPeers(now time.Time) int {
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return 0
	}

	var archiveOnly []*overlayPeer
	removed := 0
	for peerID, entry := range p.peers {
		if !archivePeerUnprovenDeadArchiveOnly(entry, now) {
			continue
		}
		if peer := p.removePeerLocked(peerID); peer != nil {
			archiveOnly = append(archiveOnly, peer)
		}
		removed++
	}
	p.mx.Unlock()

	for _, peer := range archiveOnly {
		closeArchiveOnlyPeer(peer)
	}
	return removed
}

func (p *archivePeerPool) removePeerLocked(peerID PeerID) *overlayPeer {
	entry := p.peers[peerID]
	if entry == nil {
		return nil
	}

	delete(p.peers, peerID)
	for workchain, peers := range p.workchains {
		delete(peers, peerID)
		if len(peers) == 0 {
			delete(p.workchains, workchain)
		}
	}
	for stateKey, state := range p.shards {
		if state == nil {
			delete(p.shards, stateKey)
			continue
		}
		if len(state.failures) > 0 {
			delete(state.failures, peerID)
		}
		if len(state.cooldownUntil) > 0 {
			delete(state.cooldownUntil, peerID)
		}
		if archiveShardPeerStateEmpty(state) {
			delete(p.shards, stateKey)
		}
	}
	if entry.owner != archivePeerArchiveOnly {
		return nil
	}
	return entry.peer
}

func (p *archivePeerPool) startDHTDiscovery(ctx context.Context) <-chan struct{} {
	if p.sub.node == nil || p.sub.node.dht == nil || !p.sub.isActive() {
		return nil
	}

	p.discoveryMx.Lock()
	if p.discoveryRunning {
		done := p.discoveryDone
		p.discoveryMx.Unlock()
		return done
	}
	p.discoveryRunning = true
	done := make(chan struct{})
	p.discoveryDone = done
	p.discoveryMx.Unlock()

	run := func() {
		defer func() {
			p.discoveryMx.Lock()
			if p.discoveryDone == done {
				p.discoveryRunning = false
				p.discoveryDone = nil
			}
			p.nextDiscoveryAt = time.Now().Add(archiveDiscoveryRetryDelay)
			close(done)
			p.discoveryMx.Unlock()
		}()
		p.discoverFromDHT(ctx)
	}
	if p.sub.node != nil {
		p.sub.node.runAsync(run)
		return done
	}
	go run()
	return done
}

func (p *archivePeerPool) nextDiscoveryAllowedAt() time.Time {
	p.discoveryMx.Lock()
	defer p.discoveryMx.Unlock()

	return p.nextDiscoveryAt
}

func (p *archivePeerPool) discoverFromDHT(ctx context.Context) {
	var (
		cont      *dht.Continuation
		err       error
		requests  int
		nodesSeen int
		startedAt = time.Now()
	)

	seedPool := runSeedConnectPool(ctx, p.sub.log, "failed to connect archive overlay node", dhtSeedConnectParallelism, func(node overlay.Node) (bool, error) {
		return p.connectArchiveSeedNode(ctx, node)
	})
	defer seedPool.finish()

	now := time.Now()
	p.pruneClosedPeers()
	p.pruneUnprovenDeadArchiveOnlyPeers(now)
	for i := 0; i < 8 && !p.discoverySatisfied(time.Now()); i++ {
		lookupCtx, cancel := context.WithTimeout(ctx, dhtFindTimeout)
		var nodes *overlay.NodesList
		if cont == nil {
			nodes, cont, err = p.sub.node.dht.FindOverlayNodes(lookupCtx, p.sub.spec.FullID)
		} else {
			nodes, cont, err = p.sub.node.dht.FindOverlayNodes(lookupCtx, p.sub.spec.FullID, cont)
		}
		cancel()
		if err != nil {
			p.sub.log.Debug().
				Err(err).
				Dur("elapsed", time.Since(startedAt)).
				Int("dht_requests", requests+1).
				Msg("archive DHT peer search failed")
			return
		}

		requests++
		nodesSeen += len(nodes.List)
		for _, node := range nodes.List {
			if p.discoverySatisfied(time.Now()) {
				break
			}
			if !seedPool.send(node) {
				break
			}
		}
		if cont == nil {
			break
		}
	}

	seedPool.finish()
	if seedPool.connected.Load() > 0 {
		p.sub.log.Debug().
			Dur("elapsed", time.Since(startedAt)).
			Int("dht_requests", requests).
			Int("dht_nodes", nodesSeen).
			Int64("connected_peers", seedPool.connected.Load()).
			Int("archive_peers", p.size()).
			Int("proven_peers", p.provenUsableSize(time.Now())).
			Msg("archive peer DHT search finished")
	}
}

// startRandomPeerRefresh asks a few pooled peers for their random peers. It is
// invoked from every refill (peer notifies, zero-state waits, download rounds),
// so an in-flight flag plus a minimum gap keep it from signing a fresh
// self-advertisement and spawning exchange goroutines on every call.
func (p *archivePeerPool) startRandomPeerRefresh(ctx context.Context) {
	p.discoveryMx.Lock()
	if p.randomRefreshRunning || time.Now().Before(p.nextRandomRefreshAt) {
		p.discoveryMx.Unlock()
		return
	}
	p.randomRefreshRunning = true
	p.discoveryMx.Unlock()

	finish := func(throttle bool) {
		p.discoveryMx.Lock()
		p.randomRefreshRunning = false
		if throttle {
			p.nextRandomRefreshAt = time.Now().Add(archiveRandomPeerRefreshMinGap)
		}
		p.discoveryMx.Unlock()
	}

	peers := p.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(peers) == 0 {
		// A starved pool keeps retrying immediately, exactly as before.
		finish(false)
		return
	}
	if len(peers) > peerRefreshFanout {
		peers = peers[:peerRefreshFanout]
	}

	run := func() {
		defer finish(true)
		for _, peer := range peers {
			if ctx.Err() != nil {
				return
			}
			p.exchangeRandomPeers(ctx, peer)
		}
	}
	if p.sub.node != nil {
		p.sub.node.runAsync(run)
		return
	}
	go run()
}

func (p *archivePeerPool) exchangeRandomPeers(ctx context.Context, peer *overlayPeer) {
	if !p.sub.isActive() || !p.sub.spec.RandomPeers || peer == nil || peer.overlay == nil {
		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	advertised, err := p.sub.randomPeerAdvertisement()
	if err != nil {
		p.sub.log.Debug().Err(err).Msg("failed to create self overlay node")
		return
	}

	var res overlay.NodesList
	if err = peer.overlay.Query(queryCtx, overlay.GetRandomPeers{List: advertised}, &res); err != nil {
		peer.queryFailed()
		p.sub.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("archive overlay.getRandomPeers failed")
		return
	}
	peer.querySuccess(0)

	for _, node := range res.List {
		if ctx.Err() != nil {
			return
		}
		if _, err = p.connectArchiveSeedNode(ctx, node); err != nil {
			p.sub.log.Debug().Err(err).Msg("failed to connect archive peer learned from overlay")
		}
	}
}

func (p *archivePeerPool) connectArchiveSeedNode(ctx context.Context, node overlay.Node) (bool, error) {
	connectCtx, cancel := context.WithTimeout(ctx, dhtSeedPeerTimeout)
	defer cancel()

	return p.connectArchiveNode(connectCtx, node)
}

func (p *archivePeerPool) connectArchiveNode(ctx context.Context, node overlay.Node) (bool, error) {
	if !p.sub.isActive() {
		return false, errors.New("shard is inactive")
	}
	if p.isClosed() {
		return false, nil
	}
	identity, err := p.sub.overlayNodeIdentity(node)
	if err != nil {
		return false, err
	}
	if identity.self {
		return false, nil
	}

	p.pruneClosedPeers()
	p.pruneUnprovenDeadArchiveOnlyPeers(time.Now())
	if live := p.sub.peerByID(identity.peerID); live != nil {
		live.mergeAnnouncement(&node)
		return p.addBorrowedPeer(live), nil
	}
	if p.refreshKnownPeer(identity.peerID, &node) {
		return false, nil
	}
	if p.recentlyRejected(identity.peerID, time.Now()) {
		return false, nil
	}
	if p.size() >= archivePeerHardLimit {
		return false, nil
	}

	addrList, _, err := findPeerAddresses(ctx, p.sub.node.dht, identity.peerID[:])
	if err != nil {
		return false, fmt.Errorf("find archive ADNL addresses: %w", err)
	}
	if len(addrList.Addresses) == 0 {
		return false, fmt.Errorf("archive overlay node has no addresses")
	}

	for _, addr := range addrList.Addresses {
		udpAddr, err := adnladdr.DialString(addr)
		if err != nil {
			continue
		}
		pooled, err := p.sub.node.pool.Get(udpAddr, identity.pub)
		if err != nil {
			continue
		}
		peer := p.sub.newOverlayPeer(pooled, &node, false, p.sub.spec.Kind != overlayKindCustomFixed)
		if !p.addArchiveOnlyPeer(peer) {
			closeArchiveOnlyPeer(peer)
			return false, nil
		}
		p.sub.log.Debug().
			Str("peer", pooled.addr).
			Str("peer_id", pooled.id.String()).
			Msg("connected archive-only overlay peer")
		if !p.classifyNewArchivePeer(ctx, peer) {
			return false, nil
		}
		return true, nil
	}

	return false, fmt.Errorf("failed to dial any archive overlay peer address")
}

func (p *archivePeerPool) hasPeer(peerID PeerID) bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	return p.peers[peerID] != nil
}

// refreshKnownPeer merges a fresh announcement into an already-pooled peer so
// long-lived archive-only peers do not expire with their first announcement.
func (p *archivePeerPool) refreshKnownPeer(peerID PeerID, node *overlay.Node) bool {
	p.mx.Lock()
	entry := p.peers[peerID]
	known := entry != nil
	var peer *overlayPeer
	if known {
		peer = entry.peer
	}
	p.mx.Unlock()

	if peer != nil {
		peer.mergeAnnouncement(node)
	}
	return known
}

func (p *archivePeerPool) noteArchiveRequest(shard archive.ShardID, masterchainSeqno uint32) {
	p.mx.Lock()
	if !p.closed {
		p.probe = archivePeerProbe{shard: shard, seqno: masterchainSeqno}
		p.hasProbe = true
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) noteZeroStateRequest(shard archive.ShardID, block ton.BlockIDExt) {
	p.mx.Lock()
	if !p.closed {
		p.probe = archivePeerProbe{shard: shard, block: block, zeroState: true}
		p.hasProbe = true
	}
	p.mx.Unlock()
}

func (p *archivePeerPool) probeSnapshot() (archivePeerProbe, bool) {
	p.mx.Lock()
	defer p.mx.Unlock()

	return p.probe, p.hasProbe
}

// classifyNewArchivePeer checks whether a freshly connected archive-only peer
// actually serves the data this pool is syncing. Junk full nodes are dropped
// and negative-cached so discovery stops cycling through them; peers that
// answer become proven and count towards the discovery target immediately.
func (p *archivePeerPool) classifyNewArchivePeer(ctx context.Context, peer *overlayPeer) bool {
	probe, ok := p.probeSnapshot()
	if !ok {
		return true
	}

	available, err := p.probeArchivePeer(ctx, peer, probe)
	if err != nil {
		p.dropUnclassifiedPeer(peer, false, err.Error())
		return false
	}
	if !available {
		p.dropUnclassifiedPeer(peer, true, "probe_not_available")
		return false
	}
	p.markAvailable(probe.shard, peer)
	return true
}

func (p *archivePeerPool) probeArchivePeer(ctx context.Context, peer *overlayPeer, probe archivePeerProbe) (bool, error) {
	if probe.zeroState {
		resp, err := p.sub.queryFromPeerWithLimits(ctx, peer, PrepareZeroState{Block: probe.block}, archiveInfoTimeout, persistentStateSmallAnswerMax)
		if err != nil {
			return false, err
		}
		switch resp.(type) {
		case PreparedState:
			return true, nil
		case NotFoundState:
			return false, nil
		default:
			return false, fmt.Errorf("unexpected prepareZeroState response %T", resp)
		}
	}

	if _, err := p.sub.queryArchiveInfo(ctx, peer, probe.seqno, probe.shard, archiveInfoTimeout); err != nil {
		if errors.Is(err, archive.ErrNotAvailable) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (p *archivePeerPool) dropUnclassifiedPeer(peer *overlayPeer, reject bool, reason string) {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return
	}

	now := time.Now()
	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}
	entry := p.peers[peerID]
	if entry == nil || entry.owner != archivePeerArchiveOnly || entry.leases > 0 || entry.archiveSuccess > 0 {
		p.mx.Unlock()
		return
	}
	removed := p.removePeerLocked(peerID)
	if reject {
		p.rememberRejectedLocked(peerID, now)
	}
	p.mx.Unlock()

	closeArchiveOnlyPeer(removed)
	p.sub.log.Debug().
		Str("peer", peer.addr).
		Str("reason", reason).
		Bool("negative_cached", reject).
		Msg("dropped unclassified archive peer")
}

func (p *archivePeerPool) runKeepalive(ctx context.Context) {
	ticker := time.NewTicker(archivePeerKeepaliveDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if p.isClosed() {
			return
		}
		p.keepaliveArchivePeers(ctx)
	}
}

// keepaliveArchivePeers pings proven archive-only peers so their ADNL sessions
// survive idle stretches; the overlay subscription only maintains its own
// peers, and rare archival nodes are too costly to rediscover after a drop.
func (p *archivePeerPool) keepaliveArchivePeers(ctx context.Context) {
	for _, peer := range p.keepaliveTargets(time.Now()) {
		if ctx.Err() != nil || p.isClosed() {
			return
		}
		p.pingArchivePeer(ctx, peer)
	}
}

func (p *archivePeerPool) keepaliveTargets(now time.Time) []*overlayPeer {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return nil
	}
	var targets []*overlayPeer
	for _, entry := range p.peers {
		if entry == nil || entry.owner != archivePeerArchiveOnly || entry.peer == nil || !entry.peer.hasOpenConnection() {
			continue
		}
		if entry.archiveSuccess == 0 && !archivePeerRecentlyAvailable(entry, now) {
			continue
		}
		targets = append(targets, entry.peer)
	}
	return targets
}

func (p *archivePeerPool) pingArchivePeer(ctx context.Context, peer *overlayPeer) {
	if peer == nil || peer.overlay == nil {
		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	startedAt := time.Now()
	var res Capabilities
	if err := peer.overlay.Query(queryCtx, GetCapabilities{}, &res); err != nil {
		peer.queryFailed()
		return
	}
	peer.applyCapabilities(res)
	peer.querySuccess(time.Since(startedAt))
}

func recordArchivePeerFailure(failure *archivePeerFailure, reason string) {
	switch reason {
	case archivePeerRejectNotAvailable, archivePeerRejectStateNotAvailable:
		failure.notAvailable++
	case ArchivePeerRejectImportIncomplete:
		failure.badImports++
	case ArchivePeerRejectImportFailed:
		failure.badImports++
	case archivePeerRejectCandidateFailed, archivePeerRejectDownloadFailed, archivePeerRejectStateDownloadFailed:
		failure.errors++
	default:
		failure.errors++
	}
	failure.reason = reason
}

// archivePeerFailureUseless decides whether the peer should be rotated out of
// the pool. Proven archival peers are too valuable to drop over not-available
// answers (a missing pack is shard/seqno-local); they only rotate on repeated
// hard errors or repeated bad imports.
func archivePeerFailureUseless(failure archivePeerFailure, proven bool) bool {
	if failure.badImports >= archivePeerBadImportRotateThreshold {
		return true
	}
	if failure.errors >= archivePeerErrorRotateThreshold {
		return true
	}
	return !proven && failure.notAvailable >= archivePeerNotAvailableRotateThreshold
}

func archivePeerFailureCooldown(failure archivePeerFailure, reason string, useless bool) time.Duration {
	switch reason {
	case archivePeerRejectNotAvailable, archivePeerRejectStateNotAvailable:
		return archiveNotAvailableCooldown(failure.notAvailable)
	case ArchivePeerRejectImportIncomplete, ArchivePeerRejectImportFailed:
		return archiveSlowPeerPenalty
	}
	if useless {
		return archiveSlowPeerPenalty
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
	return state == nil || len(state.cooldownUntil) == 0 && len(state.failures) == 0
}

func archivePeerUsable(entry *archivePeer, now time.Time) bool {
	if entry == nil || entry.peer == nil || !entry.peer.hasOpenConnection() {
		return false
	}
	if entry.archiveSuccess > 0 || archivePeerRecentlyAvailable(entry, now) {
		return true
	}
	return entry.peer.isAliveKnownOverlayPeer(now)
}

// archivePeerRecentlyAvailable treats a recent archive availability answer as
// proof of life: archive-only peers get no overlay announcements refreshed for
// them, so announcement freshness alone would expire good standby peers.
func archivePeerRecentlyAvailable(entry *archivePeer, now time.Time) bool {
	return !entry.lastAvailableAt.IsZero() && now.Sub(entry.lastAvailableAt) < archiveAvailablePeerTTL
}

func archivePeerUnprovenDeadArchiveOnly(entry *archivePeer, now time.Time) bool {
	if entry == nil || entry.owner != archivePeerArchiveOnly || entry.leases > 0 || entry.archiveSuccess > 0 {
		return false
	}
	if archivePeerRecentlyAvailable(entry, now) {
		return false
	}
	peer := entry.peer
	return peer != nil && peer.hasOpenConnection() && !peer.isAliveKnownOverlayPeer(now)
}

func archivePeerCoolingDownLocked(state *archiveShardPeerState, peerID PeerID, now time.Time) bool {
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

func closeArchiveOnlyPeer(peer *overlayPeer) {
	if peer == nil {
		return
	}

	peer.close()
}
