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
	archiveSessionPinnedDeadlineGrace = 5

	archivePeerRejectNotAvailable        = "archive_not_available"
	archivePeerRejectCandidateFailed     = "archive_candidate_failed"
	archivePeerRejectDownloadFailed      = "archive_download_failed"
	archivePeerRejectStateNotAvailable   = "state_not_available"
	archivePeerRejectStateDownloadFailed = "state_download_failed"
	ArchivePeerRejectImportFailed        = "archive_import_failed"
	ArchivePeerRejectImportIncomplete    = "archive_import_missing_block"
)

type ArchiveSession struct {
	node *Node

	mx       sync.Mutex
	closed   bool
	pins     map[PeerID]func()
	failures map[PeerID]int
	selected map[string]PeerID
	pools    map[*overlaySubscription]*archivePeerPool
}

type ArchiveDownloadOptions struct {
	Hedge bool
}

func (n *Node) BeginArchiveSession() *ArchiveSession {
	return &ArchiveSession{
		node:     n,
		pins:     map[PeerID]func(){},
		failures: map[PeerID]int{},
		selected: map[string]PeerID{},
		pools:    map[*overlaySubscription]*archivePeerPool{},
	}
}

func (a *ArchiveSession) Close() {
	if a == nil {
		return
	}

	a.mx.Lock()
	if a.closed {
		a.mx.Unlock()
		return
	}
	a.closed = true
	releases := make([]func(), 0, len(a.pins))
	for peerID, release := range a.pins {
		releases = append(releases, release)
		delete(a.pins, peerID)
	}
	pools := make([]*archivePeerPool, 0, len(a.pools))
	for sub, pool := range a.pools {
		pools = append(pools, pool)
		delete(a.pools, sub)
	}
	a.selected = map[string]PeerID{}
	a.mx.Unlock()

	for _, release := range releases {
		release()
	}
	for _, pool := range pools {
		pool.Close()
	}
}

func (a *ArchiveSession) DownloadArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	if a == nil || a.node == nil {
		return nil, errors.New("archive session is not initialized")
	}

	sub, err := a.node.subscriptionForBlock(ton.BlockIDExt{Workchain: shard.Workchain, Shard: shard.Shard})
	if err != nil {
		return nil, err
	}

	var lastErr error
	for round := 0; round < archiveSessionDownloadRounds; round++ {
		downloaded, err := a.downloadArchiveRound(ctx, sub, masterchainSeqno, shard, options)
		if err == nil {
			return downloaded, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		lastErr = err
		a.archivePeerPool(sub).refreshUseless(ctx, shard)
	}

	if lastErr == nil {
		lastErr = archive.ErrNotAvailable
	}
	return nil, lastErr
}

func (a *ArchiveSession) RejectArchivePeer(shard archive.ShardID, peerAddr string, reason string) bool {
	if a == nil || a.node == nil || peerAddr == "" {
		return false
	}

	sub, err := a.node.subscriptionForBlock(ton.BlockIDExt{Workchain: shard.Workchain, Shard: shard.Shard})
	if err != nil {
		return false
	}
	peer := sub.peerByAddr(peerAddr)
	pool := a.archivePeerPool(sub)
	if poolPeer := pool.peerByAddr(peerAddr); poolPeer != nil {
		peer = poolPeer
	}
	if peer == nil {
		return false
	}

	a.rejectArchivePeer(nil, pool, shard, peer, reason)
	return true
}

func (a *ArchiveSession) downloadArchiveRound(ctx context.Context, sub *overlaySubscription, masterchainSeqno uint32, shard archive.ShardID, options ArchiveDownloadOptions) (*archive.Downloaded, error) {
	pool := a.archivePeerPool(sub)
	var ensureElapsed time.Duration
	var resolveElapsed time.Duration
	logBootstrapTiming := func(peer string, err error) {
		candidates := pool.candidates(shard)
		available := pool.downloadCandidates(a, shard, candidates)

		event := sub.log.Debug()
		if err != nil || ensureElapsed >= archiveBootstrapSlowLog || resolveElapsed >= archiveBootstrapSlowLog {
			event = sub.log.Info()
		}
		event.
			Uint32("masterchain_seqno", masterchainSeqno).
			Int32("workchain", shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
			Int("known_peers", len(sub.knownPeersSnapshot())).
			Int("alive_peers", sub.aliveKnownPeerCount()).
			Int("neighbours", len(sub.neighbourPeerSnapshots())).
			Int("archive_candidates", len(candidates)).
			Int("archive_available", len(available)).
			Dur("ensure_elapsed", ensureElapsed).
			Dur("resolve_elapsed", resolveElapsed).
			Str("peer", peer).
			Err(err).
			Msg("archive peer bootstrap timing")
	}

	ensureStarted := time.Now()
	if err := sub.ensureArchivePeers(ctx, pool, shard); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	ensureElapsed = time.Since(ensureStarted)

	resolveStarted := time.Now()
	resolved, err := sub.resolveArchive(ctx, a, pool, masterchainSeqno, shard)
	resolveElapsed = time.Since(resolveStarted)
	if err != nil {
		logBootstrapTiming("", err)
		return nil, err
	}
	logBootstrapTiming(resolved.Peer, nil)

	return sub.downloadArchiveFromResolved(ctx, a, pool, resolved, options)
}

func (a *ArchiveSession) archivePeerPool(sub *overlaySubscription) *archivePeerPool {
	a.mx.Lock()
	defer a.mx.Unlock()

	pool := a.pools[sub]
	if pool == nil {
		pool = newArchivePeerPool(sub)
		a.pools[sub] = pool
	}
	return pool
}

func (a *ArchiveSession) pinArchivePeer(peer *overlayPeer) {
	if a == nil || a.node == nil {
		return
	}
	peerID := archivePeerID(peer)
	if peerID.IsZero() {
		return
	}

	a.mx.Lock()
	if a.closed {
		a.mx.Unlock()
		return
	}
	if _, ok := a.pins[peerID]; ok {
		a.mx.Unlock()
		return
	}
	release := a.node.pinPeer(peerID)
	a.pins[peerID] = release
	a.mx.Unlock()
}

func (a *ArchiveSession) selectedArchivePeerID(shard archive.ShardID) PeerID {
	if a == nil {
		return PeerID{}
	}

	a.mx.Lock()
	defer a.mx.Unlock()

	return a.selected[archivePeerPoolKey(shard)]
}

func (a *ArchiveSession) selectArchivePeer(shard archive.ShardID, peer *overlayPeer) {
	if a == nil {
		return
	}
	peerID := archivePeerID(peer)
	if peerID.IsZero() {
		return
	}

	a.pinArchivePeer(peer)

	a.mx.Lock()
	if !a.closed {
		a.selected[archivePeerPoolKey(shard)] = peerID
	}
	a.mx.Unlock()
}

func (a *ArchiveSession) clearSelectedArchivePeerID(shard archive.ShardID, peerID PeerID) {
	if a == nil || peerID.IsZero() {
		return
	}

	a.mx.Lock()
	key := archivePeerPoolKey(shard)
	if a.selected[key] == peerID {
		delete(a.selected, key)
	}
	a.mx.Unlock()
}

func (a *ArchiveSession) unpinArchivePeer(peer *overlayPeer) {
	if a == nil {
		return
	}
	peerID := archivePeerID(peer)
	if peerID.IsZero() {
		return
	}

	a.unpinArchivePeerID(peerID)
}

func (a *ArchiveSession) unpinArchivePeerID(peerID PeerID) {
	if a == nil || peerID.IsZero() {
		return
	}

	a.mx.Lock()
	release := a.pins[peerID]
	delete(a.pins, peerID)
	delete(a.failures, peerID)
	a.mx.Unlock()

	if release != nil {
		release()
	}
}

func (a *ArchiveSession) noteArchivePeerSuccess(peer *overlayPeer) {
	if a == nil {
		return
	}
	a.pinArchivePeer(peer)

	peerID := archivePeerID(peer)
	if peerID.IsZero() {
		return
	}

	a.mx.Lock()
	delete(a.failures, peerID)
	a.mx.Unlock()
}

func (a *ArchiveSession) noteArchivePeerAvailable(peer *overlayPeer) {
	if a == nil {
		return
	}
	a.pinArchivePeer(peer)
}

func (a *ArchiveSession) archivePeerDeadlineGrace(peer *overlayPeer, err error) bool {
	if a == nil || !errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	peerID := archivePeerID(peer)
	if peerID.IsZero() {
		return false
	}

	a.mx.Lock()
	if _, ok := a.pins[peerID]; !ok {
		a.mx.Unlock()
		return false
	}
	a.failures[peerID]++
	failures := a.failures[peerID]
	a.mx.Unlock()

	return failures <= archiveSessionPinnedDeadlineGrace
}

func (a *ArchiveSession) archivePeerInfoTimeout(peer *overlayPeer) time.Duration {
	return a.archivePeerTimeout(peer, archiveInfoTimeout, archiveInfoPinnedMaxTimeout)
}

func (a *ArchiveSession) archivePeerSliceProbeTimeout(peer *overlayPeer) time.Duration {
	return a.archivePeerTimeout(peer, archiveSliceProbeTimeout, archiveSliceProbePinnedMaxTimeout)
}

func (a *ArchiveSession) archivePeerSliceTimeout(peer *overlayPeer) time.Duration {
	return a.archivePeerTimeout(peer, archiveSliceTimeout, archiveSlicePinnedMaxTimeout)
}

func (a *ArchiveSession) archivePeerTimeout(peer *overlayPeer, base time.Duration, max time.Duration) time.Duration {
	failures, pinned := a.archivePeerDeadlineFailures(peer)
	if !pinned {
		return base
	}
	return archivePinnedPeerTimeout(base, max, failures)
}

func (a *ArchiveSession) archivePeerDeadlineFailures(peer *overlayPeer) (int, bool) {
	if a == nil {
		return 0, false
	}

	peerID := archivePeerID(peer)
	if peerID.IsZero() {
		return 0, false
	}

	a.mx.Lock()
	defer a.mx.Unlock()

	if _, ok := a.pins[peerID]; !ok {
		return 0, false
	}
	return a.failures[peerID], true
}

func archivePinnedPeerTimeout(base time.Duration, max time.Duration, failures int) time.Duration {
	if failures <= 0 || max <= base {
		return base
	}
	if failures >= archiveSessionPinnedDeadlineGrace {
		return max
	}
	return base + time.Duration(int64(max-base)*int64(failures)/int64(archiveSessionPinnedDeadlineGrace))
}

func (a *ArchiveSession) rejectArchivePeer(ctx context.Context, pool *archivePeerPool, shard archive.ShardID, peer *overlayPeer, reason string) {
	peerID := archivePeerID(peer)
	selected := a != nil && !peerID.IsZero() && a.selectedArchivePeerID(shard) == peerID
	useless := pool == nil
	if pool != nil {
		useless = pool.noteFailure(shard, peer, reason)
	}

	if a != nil {
		if useless {
			a.clearSelectedArchivePeerID(shard, peerID)
			a.unpinArchivePeer(peer)
		} else if !selected {
			a.unpinArchivePeer(peer)
		}
	}
	if pool != nil && useless {
		pool.cooldown(shard, peer, reason)
		pool.refreshUseless(ctx, shard)
	}
}

func archiveDownloadError(peer *overlayPeer, err error) error {
	if peer == nil || peer.addr == "" {
		return err
	}
	return fmt.Errorf("%s: %w", peer.addr, err)
}
