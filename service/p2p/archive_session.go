package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveSessionDownloadRounds      = 6
	archiveSessionComparativeHedgeGap = 10 * time.Second
	archiveConcurrentHedgeLimit       = 2

	archivePeerRejectNotAvailable        = "archive_not_available"
	archivePeerRejectCandidateFailed     = "archive_candidate_failed"
	archivePeerRejectDownloadFailed      = "archive_download_failed"
	archivePeerRejectStateNotAvailable   = "state_not_available"
	archivePeerRejectStateDownloadFailed = "state_download_failed"
	ArchivePeerRejectImportFailed        = "archive_import_failed"
	ArchivePeerRejectImportIncomplete    = "archive_import_missing_block"
)

var errArchiveSessionClosed = errors.New("archive session is closed")

type ArchiveSession struct {
	node     *Node
	opCtx    context.Context
	opCancel context.CancelFunc

	mx            sync.Mutex
	closed        bool
	selected      map[string]PeerID
	selectedPools map[string]*archivePeerPool
	hedgedAt      map[string]time.Time
	pools         map[*overlaySubscription]*archivePeerPool
	hedges        chan struct{}
	closeDone     chan struct{}
	retiring      sync.WaitGroup
	operations    sync.WaitGroup
}

type ArchiveDownloadOptions struct {
	Hedge bool
}

func (n *Node) BeginArchiveSession() *ArchiveSession {
	opCtx, opCancel := context.WithCancel(n.runCtx)

	return &ArchiveSession{
		node:          n,
		opCtx:         opCtx,
		opCancel:      opCancel,
		selected:      map[string]PeerID{},
		selectedPools: map[string]*archivePeerPool{},
		hedgedAt:      map[string]time.Time{},
		pools:         map[*overlaySubscription]*archivePeerPool{},
		hedges:        make(chan struct{}, archiveConcurrentHedgeLimit),
		closeDone:     make(chan struct{}),
	}
}

func (a *ArchiveSession) Close() {
	a.mx.Lock()
	if a.closed {
		done := a.closeDone
		a.mx.Unlock()
		<-done
		return
	}
	a.closed = true
	done := a.closeDone
	a.opCancel()
	a.mx.Unlock()
	defer close(done)

	a.operations.Wait()

	a.mx.Lock()
	pools := make([]*archivePeerPool, 0, len(a.pools))
	for sub, pool := range a.pools {
		pools = append(pools, pool)
		delete(a.pools, sub)
	}
	a.selected = map[string]PeerID{}
	a.selectedPools = map[string]*archivePeerPool{}
	a.hedgedAt = map[string]time.Time{}
	a.mx.Unlock()

	for _, pool := range pools {
		pool.Close()
	}
	a.retiring.Wait()
}

func (a *ArchiveSession) DownloadArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	ctx, finish, err := a.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if a.node.IsOffline() {
		return nil, ErrOffline
	}

	// Historical master and shard archives are both served by archival peers on
	// the masterchain overlay. Availability and performance remain keyed by the
	// requested shard inside the archive pool.
	sub, err := a.node.subscriptionForOverlayBlock(ton.BlockIDExt{Workchain: -1, Shard: topShard})
	if err != nil {
		return nil, err
	}
	pool, releasePool, err := a.useArchivePeerPool(sub)
	if err != nil {
		return nil, err
	}
	defer releasePool()
	_, releaseDemand, err := pool.beginArchiveRequest(shard, masterchainSeqno)
	if err != nil {
		return nil, err
	}
	defer releaseDemand()
	pool.refill(ctx, false)

	var lastErr error
	for round := 0; round < archiveSessionDownloadRounds; round++ {
		downloaded, err := a.downloadArchiveRound(ctx, sub, pool, masterchainSeqno, shard, options)
		if err == nil {
			return downloaded, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		lastErr = err
		if pool.refreshUseless(ctx, shard) == nil {
			pool.refill(ctx, true)
		}
	}

	if lastErr == nil {
		lastErr = archive.ErrNotAvailable
	}
	return nil, lastErr
}

func (a *ArchiveSession) RejectArchivePeer(shard archive.ShardID, peerAddr string, reason string) bool {
	if peerAddr == "" {
		return false
	}
	ctx, finish, err := a.beginOperation(context.Background())
	if err != nil {
		return false
	}
	defer finish()

	a.mx.Lock()
	pools := make([]*archivePeerPool, 0, len(a.pools))
	if selected := a.selectedPools[archivePeerPoolKey(shard)]; selected != nil {
		pools = append(pools, selected)
	} else {
		for _, pool := range a.pools {
			pools = append(pools, pool)
		}
	}
	a.mx.Unlock()

	rejected := false
	for _, pool := range pools {
		peer := pool.peerByAddr(peerAddr)
		if peer == nil {
			continue
		}
		a.rejectArchivePeer(ctx, pool, shard, peer, reason)
		rejected = true
	}
	return rejected
}

func (a *ArchiveSession) downloadArchiveRound(ctx context.Context, sub *overlaySubscription, pool *archivePeerPool, masterchainSeqno uint32, shard archive.ShardID, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	var ensureElapsed time.Duration
	var resolveElapsed time.Duration
	logBootstrapTiming := func(peer string, err error) {
		event := sub.log.Debug()
		if err != nil || ensureElapsed >= archiveBootstrapSlowLog || resolveElapsed >= archiveBootstrapSlowLog {
			event = sub.log.Info()
		}
		if !event.Enabled() {
			return
		}

		// Selection paths (resolveArchive, downloadArchiveFromResolved) call
		// pool.downloadCandidates themselves right before use, so this log
		// only reports the raw candidate count and skips the prioritize sort
		// plus its sticky-selected-peer bookkeeping.
		event.
			Uint32("masterchain_seqno", masterchainSeqno).
			Int32("workchain", shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
			Int("known_peers", len(sub.knownPeersSnapshot())).
			Int("alive_peers", sub.aliveKnownPeerCount()).
			Int("neighbours", len(sub.neighbourPeerSnapshots())).
			Int("archive_candidates", len(pool.candidates(shard))).
			Dur("ensure_elapsed", ensureElapsed).
			Dur("resolve_elapsed", resolveElapsed).
			Str("peer", peer).
			Err(err).
			Msg("archive peer bootstrap timing")
	}

	ensureStarted := time.Now()
	if err := sub.ensureArchivePeers(ctx, pool, masterchainSeqno, shard); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	ensureElapsed = time.Since(ensureStarted)

	resolveStarted := time.Now()
	resolved, err := sub.resolveArchive(ctx, a, pool, masterchainSeqno, shard, options)
	resolveElapsed = time.Since(resolveStarted)
	if err != nil {
		logBootstrapTiming("", err)
		return nil, err
	}
	logBootstrapTiming(resolved.Peer, nil)

	return sub.downloadArchiveFromResolved(ctx, a, pool, resolved, options)
}

func (a *ArchiveSession) archivePeerPool(sub *overlaySubscription) (*archivePeerPool, error) {
	now := time.Now()
	var retired []*archivePeerPool

	a.mx.Lock()
	if a.closed {
		a.mx.Unlock()
		return nil, errArchiveSessionClosed
	}
	for poolSub, pool := range a.pools {
		closed := pool.isClosed()
		if !closed && (poolSub == sub || !pool.canRetire(now)) {
			continue
		}

		a.retiring.Add(1)
		delete(a.pools, poolSub)
		retired = append(retired, pool)
	}

	pool := a.pools[sub]
	if pool == nil {
		pool = newArchivePeerPool(sub)
		pool.enableContinuousDiscovery()
		a.pools[sub] = pool
	}
	pool.touch(now)
	a.mx.Unlock()

	for _, old := range retired {
		old.Close()
		a.retiring.Done()
	}
	return pool, nil
}

func (a *ArchiveSession) useArchivePeerPool(sub *overlaySubscription) (*archivePeerPool, func(), error) {
	releaseSubscription, err := sub.beginArchiveUse()
	if err != nil {
		return nil, nil, err
	}

	pool, err := a.archivePeerPool(sub)
	if err != nil {
		releaseSubscription()
		return nil, nil, err
	}

	releasePool, err := pool.beginUse(time.Now())
	if err != nil {
		releaseSubscription()
		return nil, nil, err
	}
	return pool, func() {
		releasePool()
		releaseSubscription()
	}, nil
}

func (a *ArchiveSession) beginOperation(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	a.mx.Lock()
	if a.closed {
		a.mx.Unlock()
		return nil, nil, errArchiveSessionClosed
	}
	if err := a.opCtx.Err(); err != nil {
		a.mx.Unlock()
		return nil, nil, err
	}
	a.operations.Add(1)
	opCtx := a.opCtx
	a.mx.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(opCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
		a.operations.Done()
	}, nil
}

func (a *ArchiveSession) tryAcquireArchiveHedge() func() {
	a.mx.Lock()
	defer a.mx.Unlock()

	if a.closed {
		return nil
	}
	select {
	case a.hedges <- struct{}{}:
		return func() {
			<-a.hedges
		}
	default:
		return nil
	}
}

func (a *ArchiveSession) selectedArchivePeerID(shard archive.ShardID) PeerID {
	a.mx.Lock()
	defer a.mx.Unlock()

	return a.selected[archivePeerPoolKey(shard)]
}

func (a *ArchiveSession) selectedArchivePeerPool(shard archive.ShardID) *archivePeerPool {
	a.mx.Lock()
	defer a.mx.Unlock()

	return a.selectedPools[archivePeerPoolKey(shard)]
}

func (a *ArchiveSession) selectArchivePeerFromPool(shard archive.ShardID, peer *overlayPeer, pool *archivePeerPool) {
	peerID := downloadPeerID(peer)
	if peerID.IsZero() {
		return
	}

	a.mx.Lock()
	if a.closed {
		a.mx.Unlock()
		return
	}
	key := archivePeerPoolKey(shard)
	if a.selected[key] == peerID {
		if pool != nil {
			a.selectedPools[key] = pool
		}
		a.mx.Unlock()
		return
	}
	a.selected[key] = peerID
	if pool == nil {
		delete(a.selectedPools, key)
	} else {
		a.selectedPools[key] = pool
	}
	a.mx.Unlock()
}

func (a *ArchiveSession) clearSelectedArchivePeerID(shard archive.ShardID, peerID PeerID) {
	if peerID.IsZero() {
		return
	}

	a.mx.Lock()
	key := archivePeerPoolKey(shard)
	if a.selected[key] == peerID {
		delete(a.selected, key)
		delete(a.selectedPools, key)
	}
	a.mx.Unlock()
}

func (a *ArchiveSession) shouldHedgeArchiveDownload(shard archive.ShardID, alternatives bool, now time.Time) bool {
	if !alternatives {
		return false
	}

	key := archivePeerPoolKey(shard)

	a.mx.Lock()
	defer a.mx.Unlock()

	if a.closed || a.selected[key].IsZero() {
		return false
	}
	if last := a.hedgedAt[key]; !last.IsZero() && now.Sub(last) < archiveSessionComparativeHedgeGap {
		return false
	}
	a.hedgedAt[key] = now
	return true
}

func (a *ArchiveSession) noteArchiveHedge(shard archive.ShardID, alternatives bool, now time.Time) {
	if !alternatives {
		return
	}

	a.mx.Lock()
	if !a.closed {
		a.hedgedAt[archivePeerPoolKey(shard)] = now
	}
	a.mx.Unlock()
}

func (a *ArchiveSession) rejectArchivePeer(ctx context.Context, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, reason string) {
	peerID := downloadPeerID(peer)
	selected := !peerID.IsZero() && a.selectedArchivePeerID(shard) == peerID
	verdict := pool.noteFailure(shard, peer, reason)

	// Any cooldown-earning failure leaves the peer unable to serve this
	// shard for a while, so the sticky selection moves on right away.
	if selected || verdict.useless || verdict.cooldown > 0 {
		a.clearSelectedArchivePeerID(shard, peerID)
	}
	if verdict.useless {
		pool.refreshUseless(ctx, shard)
	}
}

func archiveDownloadError(peer *overlayPeer, err error) error {
	if peer == nil || peer.addr == "" {
		return err
	}
	return fmt.Errorf("%s: %w", peer.addr, err)
}
