package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/archive"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

const (
	archivePeerErrorRotateThreshold = 3
	archivePeerSoftLimit            = 128
	archivePeerHardLimit            = 256
)

type archivePeerOwner uint8

const (
	archivePeerBorrowedLive archivePeerOwner = iota
	archivePeerArchiveOnly
)

type archivePeerPool struct {
	sub *overlaySubscription

	mx         sync.Mutex
	closed     bool
	peers      map[PeerID]*archivePeer
	workchains map[int32]map[PeerID]struct{}
	shards     map[string]*archiveShardPeerState

	discoveryMx      sync.Mutex
	discoveryRunning bool
	discoveryDone    chan struct{}
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

type archivePeerRotationCandidate struct {
	peerID  PeerID
	failure archivePeerFailure
}

func newArchivePeerPool(sub *overlaySubscription) *archivePeerPool {
	pool := &archivePeerPool{
		sub:        sub,
		peers:      map[PeerID]*archivePeer{},
		workchains: map[int32]map[PeerID]struct{}{},
		shards:     map[string]*archiveShardPeerState{},
	}
	pool.bootstrapLivePeers()
	return pool
}

func (p *archivePeerPool) Close() {
	if p == nil {
		return
	}

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
	if p == nil {
		return true
	}

	p.mx.Lock()
	defer p.mx.Unlock()

	return p.closed
}

func (p *archivePeerPool) bootstrapLivePeers() {
	if p == nil || p.sub == nil {
		return
	}

	for _, peer := range p.sub.peersSnapshot() {
		if peer == nil || peer.id.IsZero() || !peer.hasOpenConnection() {
			continue
		}
		p.addBorrowedPeer(peer)
	}
}

func (p *archivePeerPool) addBorrowedPeer(peer *overlayPeer) bool {
	if p == nil || peer == nil || peer.id.IsZero() {
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
	if p == nil || peer == nil || peer.id.IsZero() {
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
	if len(p.peers) >= archivePeerHardLimit {
		return false
	}
	p.peers[peer.id] = &archivePeer{peer: peer, owner: archivePeerArchiveOnly}
	return true
}

func (p *archivePeerPool) peerByAddr(addr string) *overlayPeer {
	if p == nil || addr == "" {
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
	if p == nil {
		return nil
	}

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

	peerID := archivePeerID(selected)
	ordered := make([]*overlayPeer, 0, len(peers)+1)
	ordered = append(ordered, selected)
	for _, peer := range peers {
		if archivePeerID(peer) == peerID {
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
		peerID := archivePeerID(peer)
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
	peerID := archivePeerID(peer)
	if p == nil || peerID.IsZero() {
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
	peerID := archivePeerID(peer)
	if p == nil || peerID.IsZero() {
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
	p.markWorkchainPeerLocked(shard.Workchain, peerID)
	p.clearFailureLocked(stateKey, peerID)
	p.mx.Unlock()
}

func (p *archivePeerPool) markSuccess(shard archive.ShardID, peer *overlayPeer) {
	peerID := archivePeerID(peer)
	if p == nil || peerID.IsZero() {
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
	p.markWorkchainPeerLocked(shard.Workchain, peerID)
	p.clearFailureLocked(stateKey, peerID)
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

func (p *archivePeerPool) clearFailure(shard archive.ShardID, peer *overlayPeer) {
	peerID := archivePeerID(peer)
	if p == nil || peerID.IsZero() {
		return
	}

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}
	p.clearFailureLocked(archivePeerPoolKey(shard), peerID)
	p.mx.Unlock()
}

func (p *archivePeerPool) clearFailureLocked(stateKey string, peerID PeerID) {
	state := p.shards[stateKey]
	if state == nil {
		return
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

func (p *archivePeerPool) noteFailure(shard archive.ShardID, peer *overlayPeer, reason string) bool {
	peerID := archivePeerID(peer)
	if p == nil || peerID.IsZero() {
		return false
	}

	stateKey := archivePeerPoolKey(shard)

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return false
	}
	state := p.shardStateLocked(stateKey)
	if state.failures == nil {
		state.failures = map[PeerID]archivePeerFailure{}
	}
	failure := state.failures[peerID]
	recordArchivePeerFailure(&failure, reason)
	state.failures[peerID] = failure
	useless := archivePeerFailureUseless(failure)
	p.mx.Unlock()

	return useless
}

func (p *archivePeerPool) cooldown(shard archive.ShardID, peer *overlayPeer, reason string) {
	peerID := archivePeerID(peer)
	if p == nil || peer == nil || peerID.IsZero() {
		return
	}

	until := time.Now().Add(archiveSlowPeerPenalty)
	stateKey := archivePeerPoolKey(shard)

	p.mx.Lock()
	if p.closed {
		p.mx.Unlock()
		return
	}
	state := p.shardStateLocked(stateKey)
	if state.cooldownUntil == nil {
		state.cooldownUntil = map[PeerID]time.Time{}
	}
	state.cooldownUntil[peerID] = until
	p.mx.Unlock()

	p.sub.log.Debug().
		Str("peer", peer.addr).
		Str("archive_pool", stateKey).
		Str("reason", reason).
		Dur("duration", archiveSlowPeerPenalty).
		Msg("temporarily cooled down archive peer")
}

func (p *archivePeerPool) coolingDown(shard archive.ShardID, peer *overlayPeer) bool {
	peerID := archivePeerID(peer)
	if p == nil || peerID.IsZero() {
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
	if p == nil {
		return 0
	}

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
		if archivePeerFailureUseless(failure) {
			candidates = append(candidates, archivePeerRotationCandidate{peerID: peerID, failure: failure})
		}
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
	p.mx.Lock()
	entry := p.peers[candidate.peerID]
	if entry == nil {
		p.dropRotatedFailureLocked(stateKey, candidate.peerID)
		p.mx.Unlock()
		return false
	}
	peer := entry.peer
	removePeer := entry.owner == archivePeerArchiveOnly && entry.leases == 0
	if removePeer {
		delete(p.peers, candidate.peerID)
		for _, peers := range p.workchains {
			delete(peers, candidate.peerID)
		}
	}
	p.dropRotatedFailureLocked(stateKey, candidate.peerID)
	p.mx.Unlock()

	if !removePeer {
		return false
	}

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
	if p == nil || p.sub == nil {
		return nil
	}
	if p.isClosed() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
		if p.sub.node != nil && p.sub.node.runCtx != nil {
			ctx = p.sub.node.runCtx
		}
	}

	p.bootstrapLivePeers()
	now := time.Now()
	p.pruneClosedPeers()
	p.pruneUnprovenDeadArchiveOnlyPeers(now)
	p.startRandomPeerRefresh(ctx)
	if forceDHT || p.usableSize(now) < archivePeerSoftLimit {
		return p.startDHTDiscovery(ctx, bootstrapDiscoveryTarget)
	}
	return nil
}

func (p *archivePeerPool) size() int {
	p.pruneClosedPeers()

	p.mx.Lock()
	defer p.mx.Unlock()
	return len(p.peers)
}

func (p *archivePeerPool) usableSize(now time.Time) int {
	if p == nil {
		return 0
	}

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

func (p *archivePeerPool) pruneClosedPeers() int {
	if p == nil {
		return 0
	}

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
	if p == nil {
		return 0
	}

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

func (p *archivePeerPool) startDHTDiscovery(ctx context.Context, targetPeers int) <-chan struct{} {
	if p.sub.node == nil || p.sub.node.dht == nil || !p.sub.isActive() {
		return nil
	}
	if targetPeers < maxPeersPerOverlay {
		targetPeers = maxPeersPerOverlay
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
			close(done)
			p.discoveryMx.Unlock()
		}()
		p.discoverFromDHT(ctx, targetPeers)
	}
	if p.sub.node != nil {
		p.sub.node.runAsync(run)
		return done
	}
	go run()
	return done
}

func (p *archivePeerPool) discoverFromDHT(ctx context.Context, targetPeers int) {
	var (
		cont      *dht.Continuation
		err       error
		requests  int
		nodesSeen int
		connected atomic.Int64
		startedAt = time.Now()
	)

	jobs := make(chan overlay.Node)
	results := make(chan seedConnectResult, dhtSeedConnectParallelism)
	var workers sync.WaitGroup
	for i := 0; i < dhtSeedConnectParallelism; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for node := range jobs {
				attached, err := p.connectArchiveSeedNode(ctx, node)
				results <- seedConnectResult{attached: attached, err: err}
			}
		}()
	}

	var collector sync.WaitGroup
	collector.Add(1)
	collectorCtx, cancelCollector := context.WithCancel(ctx)
	go func() {
		defer collector.Done()
		for {
			select {
			case res, ok := <-results:
				if !ok {
					return
				}
				if res.err != nil {
					p.sub.log.Debug().Err(res.err).Msg("failed to connect archive overlay node")
					continue
				}
				if res.attached {
					connected.Add(1)
				}
			case <-collectorCtx.Done():
				for res := range results {
					if res.err != nil {
						p.sub.log.Debug().Err(res.err).Msg("failed to connect archive overlay node")
						continue
					}
					if res.attached {
						connected.Add(1)
					}
				}
				return
			}
		}
	}()

	finishWorkers := func() {
		close(jobs)
		workers.Wait()
		close(results)
		cancelCollector()
		collector.Wait()
	}

	sendNode := func(node overlay.Node) bool {
		select {
		case jobs <- node:
			return true
		case <-ctx.Done():
			return false
		}
	}

	now := time.Now()
	p.pruneClosedPeers()
	p.pruneUnprovenDeadArchiveOnlyPeers(now)
	for i := 0; i < 8 && p.usableSize(time.Now()) < targetPeers; i++ {
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
			finishWorkers()
			return
		}

		requests++
		nodesSeen += len(nodes.List)
		for _, node := range nodes.List {
			if p.usableSize(time.Now()) >= targetPeers {
				break
			}
			if !sendNode(node) {
				break
			}
		}
		if cont == nil {
			break
		}
	}

	finishWorkers()
	if connected.Load() > 0 {
		p.sub.log.Debug().
			Dur("elapsed", time.Since(startedAt)).
			Int("dht_requests", requests).
			Int("dht_nodes", nodesSeen).
			Int64("connected_peers", connected.Load()).
			Int("archive_peers", p.size()).
			Msg("archive peer DHT search finished")
	}
}

func (p *archivePeerPool) startRandomPeerRefresh(ctx context.Context) {
	peers := p.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(peers) == 0 {
		return
	}
	if len(peers) > peerRefreshFanout {
		peers = peers[:peerRefreshFanout]
	}

	run := func() {
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
	if !p.sub.isActive() || peer == nil || peer.overlay == nil {
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
	if p.hasPeer(identity.peerID) {
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
		return true, nil
	}

	return false, fmt.Errorf("failed to dial any archive overlay peer address")
}

func (p *archivePeerPool) hasPeer(peerID PeerID) bool {
	p.mx.Lock()
	defer p.mx.Unlock()
	return p.peers[peerID] != nil
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

func archivePeerFailureUseless(failure archivePeerFailure) bool {
	return failure.notAvailable > 0 ||
		failure.badImports > 0 ||
		failure.errors >= archivePeerErrorRotateThreshold
}

func archiveShardPeerStateEmpty(state *archiveShardPeerState) bool {
	return state == nil || len(state.cooldownUntil) == 0 && len(state.failures) == 0
}

func archivePeerUsable(entry *archivePeer, now time.Time) bool {
	if entry == nil || entry.peer == nil || !entry.peer.hasOpenConnection() {
		return false
	}
	return entry.archiveSuccess > 0 || entry.peer.isAliveKnownOverlayPeer(now)
}

func archivePeerUnprovenDeadArchiveOnly(entry *archivePeer, now time.Time) bool {
	if entry == nil || entry.owner != archivePeerArchiveOnly || entry.leases > 0 || entry.archiveSuccess > 0 {
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

func archivePeerID(peer *overlayPeer) PeerID {
	return downloadPeerID(peer)
}

func closeArchiveOnlyPeer(peer *overlayPeer) {
	if peer == nil {
		return
	}

	peer.close()
}
