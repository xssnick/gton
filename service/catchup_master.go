package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type nextBlockBootstrapProbeState struct {
	consecutiveMisses int
	liveTail          bool
}

type nextBlockBootstrapProbeDecision struct {
	peerLimit             int
	consecutiveMisses     int
	liveTail              bool
	rawBroadcastAhead     bool
	observedAhead         bool
	seenAhead             bool
	queuedFutureAhead     bool
	preferredSourcePeerID p2p.PeerID
	aheadBlocks           uint32
	lowestMissingSeqno    uint32
	lagSeconds            int64
	hasLag                bool
}

func (s *Service) nextBlockBootstrapProbeDecision(prev ton.BlockIDExt, prevUTime int64, state nextBlockBootstrapProbeState) (nextBlockBootstrapProbeDecision, <-chan struct{}) {
	decision := nextBlockBootstrapProbeDecision{
		peerLimit:         nextBlockBootstrapProbePeers,
		consecutiveMisses: state.consecutiveMisses,
		liveTail:          state.liveTail,
	}

	var broadcastWake <-chan struct{}
	if s.node != nil {
		broadcastWake, decision.rawBroadcastAhead = s.node.MasterchainBroadcastAfter(prev.SeqNo)
		if seqno, ok := s.observedMasterchainSeqnoAhead(prev.SeqNo, s.node.ObservedMasterchainBlock); ok {
			decision.observedAhead = true
			decision.noteAheadSeqno(prev.SeqNo, seqno)
		}
		if seqno, ok := s.observedMasterchainSeqnoAhead(prev.SeqNo, s.node.SeenMasterchainBlock); ok {
			decision.seenAhead = true
			decision.noteAheadSeqno(prev.SeqNo, seqno)
		}
	}

	if queued, ok := s.queuedMasterchainFuture(prev); ok {
		decision.queuedFutureAhead = true
		decision.preferredSourcePeerID = queued.sourcePeerID
		decision.lowestMissingSeqno = queued.lowestMissingSeqno
		decision.noteAheadSeqno(prev.SeqNo, queued.block.SeqNo)
	}

	if lagSeconds, ok := masterchainBlockLagSeconds(prevUTime, time.Now().Unix()); ok {
		decision.lagSeconds = lagSeconds
		decision.hasLag = true
	}

	if decision.shouldUseWideFanout() {
		decision.peerLimit = nextBlockBootstrapWidePeers
	} else if decision.shouldUseUrgentFanout() {
		decision.peerLimit = nextBlockBootstrapUrgentPeers
	}
	if !decision.liveTail {
		decision.preferredSourcePeerID = p2p.PeerID{}
	}
	return decision, broadcastWake
}

func (s *Service) observedMasterchainSeqnoAhead(currentSeqno uint32, observe func() (ton.BlockIDExt, error)) (uint32, bool) {
	block, err := observe()
	if errors.Is(err, storage.ErrNotFound) {
		return 0, false
	}
	if err != nil {
		return 0, false
	}
	if block.Workchain != -1 || block.Shard != topShard || block.SeqNo <= currentSeqno {
		return 0, false
	}
	return block.SeqNo, true
}

func (d *nextBlockBootstrapProbeDecision) noteAheadSeqno(currentSeqno uint32, seqno uint32) {
	if seqno <= currentSeqno {
		return
	}
	gap := seqno - currentSeqno
	if gap > d.aheadBlocks {
		d.aheadBlocks = gap
	}
}

func (d nextBlockBootstrapProbeDecision) shouldUseUrgentFanout() bool {
	if d.liveTail && (d.rawBroadcastAhead || d.observedAhead || d.seenAhead || d.queuedFutureAhead) {
		return true
	}
	if d.consecutiveMisses >= nextBlockBootstrapUrgentMisses {
		return true
	}
	return d.hasLag && d.lagSeconds >= nextBlockBootstrapUrgentLagSeconds
}

func (d nextBlockBootstrapProbeDecision) shouldUseWideFanout() bool {
	if d.consecutiveMisses >= nextBlockBootstrapWideMisses {
		return true
	}
	if d.aheadBlocks >= nextBlockBootstrapWideGapBlocks {
		return true
	}
	return d.hasLag && d.lagSeconds >= nextBlockBootstrapWideLagSeconds
}

func (d nextBlockBootstrapProbeDecision) probeTimeout() time.Duration {
	if d.liveTail {
		return nextBlockBootstrapLiveProbeTimeout
	}
	return nextBlockBootstrapProbeTimeout
}

func (d nextBlockBootstrapProbeDecision) stagedPeerLimit() int {
	if !d.liveTail || d.peerLimit >= nextBlockBootstrapWidePeers {
		return d.peerLimit
	}
	return nextBlockBootstrapWidePeers
}

func nextBlockBootstrapLiveTail(blockUTime int64, nowUnix int64) bool {
	lagSeconds, ok := masterchainBlockLagSeconds(blockUTime, nowUnix)
	return ok && lagSeconds <= nextBlockBootstrapLiveLagSeconds
}

func (s *Service) publishCommittedCurrentState(current *storage.CurrentState) {
	s.observeBroadcastFlushedCurrentState(current)
	if current != nil {
		s.rememberAppliedMasterchainState(&current.Masterchain)
	}
	if !s.publishLiveCurrentStateChanged(current) {
		return
	}
	if s.liveState != nil {
		s.liveState.SetLiveCurrentState(current)
		s.liveState.MarkLiveCurrentStateFlushed(current)
	}
	s.wakeServiceMaintenance()
}

func (s *Service) publishLiveCurrentStateChanged(current *storage.CurrentState) bool {
	if current == nil {
		return false
	}

	next := storage.CloneCurrentState(current)

	s.currentStatusMu.Lock()
	if currentStateBehind(next, s.currentStatus) {
		s.currentStatusMu.Unlock()
		return false
	}
	s.currentStatus = next
	s.currentStatusMu.Unlock()

	s.observeBroadcastLiveCurrentState(next)

	if s.node != nil {
		s.node.NotifyCompressedBlockStateReady(currentStateCompressedBlockStateRefs(next)...)
	}
	return true
}

func currentStateCompressedBlockStateRefs(current *storage.CurrentState) []ton.BlockIDExt {
	if current == nil {
		return nil
	}

	refs := make([]ton.BlockIDExt, 0, len(current.Shards)+1)
	refs = append(refs, current.Masterchain.Block)
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		refs = append(refs, shard.Block)
	}
	return refs
}

func (s *Service) rememberAppliedMasterchainState(state *storage.BlockState) {
	if state == nil || state.Block.Workchain != -1 || state.Block.Shard != topShard {
		return
	}

	next := storage.CloneBlockState(state)

	s.currentStatusMu.Lock()
	if s.appliedMasterchainStatus != nil && s.appliedMasterchainStatus.Block.SeqNo >= next.Block.SeqNo {
		s.currentStatusMu.Unlock()
		return
	}
	s.appliedMasterchainStatus = next
	s.currentStatusMu.Unlock()
}

func (s *Service) appliedMasterchainStatusSnapshot() *storage.BlockState {
	s.currentStatusMu.RLock()
	state := storage.CloneBlockState(s.appliedMasterchainStatus)
	s.currentStatusMu.RUnlock()
	return state
}

func currentStateBehind(next *storage.CurrentState, current *storage.CurrentState) bool {
	if next == nil || current == nil {
		return false
	}

	nextShardClientSeqno := next.ShardClientSeqno
	if nextShardClientSeqno == 0 {
		nextShardClientSeqno = next.Masterchain.Block.SeqNo
	}
	currentShardClientSeqno := current.ShardClientSeqno
	if currentShardClientSeqno == 0 {
		currentShardClientSeqno = current.Masterchain.Block.SeqNo
	}
	if nextShardClientSeqno != currentShardClientSeqno {
		return nextShardClientSeqno < currentShardClientSeqno
	}
	return next.Masterchain.Block.SeqNo < current.Masterchain.Block.SeqNo
}

type masterchainApplyTiming struct {
	total       time.Duration
	prepare     time.Duration
	consensus   time.Duration
	stateUpdate time.Duration
}

func (s *Service) applyMasterchainTransition(ctx context.Context, current *storage.BlockState, block PreparedBlock, checked *checkedMasterchainConsensus, applier stateUpdateApplier, hook *blockApplyHookMeta) (*storage.BlockState, masterchainApplyTiming, error) {
	started := time.Now()
	var timing masterchainApplyTiming
	finish := func() masterchainApplyTiming {
		timing.total = time.Since(started)
		return timing
	}

	if block.ID.Workchain != -1 || block.ID.Shard != topShard {
		return nil, finish(), fmt.Errorf("download next masterchain block after %s returned %s", storage.FormatBlockRef(current.Block), block.BlockRef())
	}
	if block.Meta == nil || len(block.Meta.PrevRefs) != 1 {
		return nil, finish(), fmt.Errorf("masterchain block %s has no single previous ref", block.BlockRef())
	}
	prev := block.Meta.PrevRefs[0]
	if !prev.Equals(&current.Block) {
		return nil, finish(), fmt.Errorf("%w: block=%s prev=%s current=%s", errMasterchainPrevMismatch, block.BlockRef(), storage.FormatBlockRef(prev), storage.FormatBlockRef(current.Block))
	}

	if checked != nil {
		stageStarted := time.Now()
		if err := checked.validateFor(current, block.ID); err != nil {
			timing.consensus += time.Since(stageStarted)
			return nil, finish(), fmt.Errorf("checked masterchain consensus for %s: %w", block.BlockRef(), err)
		}
		timing.consensus += time.Since(stageStarted)
	}

	stageStarted := time.Now()
	next, err := s.applyBlockWithHooks(ctx, []*storage.BlockState{current}, block, applier, hook)
	timing.stateUpdate += time.Since(stageStarted)
	if err != nil {
		return nil, finish(), fmt.Errorf("apply masterchain block %s: %w", block.BlockRef(), err)
	}

	return next, finish(), nil
}

func (s *Service) persistNextBlockCurrentStateLocked(current *storage.CurrentState, timing *catchUpTiming, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache, onCommitted func(), onDone func(), lockElapsed time.Duration, queuedAt time.Time) (*storage.CurrentState, error) {
	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}

	checkpoint, err := prepareStateCheckpoint(current, entries, cells)
	if err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}
	master := current.Masterchain.Block
	shardClientSeqno := current.ShardClientSeqno
	shards := len(current.Shards)
	timing.checkpoints++

	s.log.Debug().
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("shard_client_seqno", shardClientSeqno).
		Int("shards", shards).
		Dur("lock_wait", lockElapsed).
		Msg("next-block shard-client checkpoint scheduled")

	persistCtx := s.currentStatePersistContext()
	s.runAsync(func() {
		if onDone != nil {
			defer onDone()
		}

		started := time.Now()
		committed, stages, err := s.saveStateCheckpoint(persistCtx, checkpoint.persisted, checkpoint.entries, checkpoint.cells, checkpoint.cellPrewriteTarget)
		elapsed := time.Since(started)
		if err != nil {
			wrapped := fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), err)
			s.setCurrentStatePersistError(wrapped)
			s.currentStatePersistMu.Unlock()
			s.observeSyncPersist(SyncPersistObservation{
				Mode:          "next_block_async",
				Result:        "error",
				QueueDuration: time.Since(queuedAt) - elapsed,
				Duration:      elapsed,
				States:        len(checkpoint.entries),
				Stages:        stages,
			})
			s.log.Error().
				Err(wrapped).
				Str("masterchain", storage.FormatBlockRef(master)).
				Uint32("shard_client_seqno", shardClientSeqno).
				Dur("queued_for", time.Since(queuedAt)).
				Dur("elapsed", elapsed).
				Msg("next-block shard-client checkpoint failed")
			return
		}

		if onCommitted != nil {
			onCommitted()
		}
		cells.complete()
		s.currentStatePersistMu.Unlock()

		s.observeSyncPersist(SyncPersistObservation{
			Mode:          "next_block_async",
			Result:        "success",
			QueueDuration: time.Since(queuedAt) - elapsed,
			Duration:      elapsed,
			States:        len(checkpoint.entries),
			Stages:        stages,
		})
		s.log.Debug().
			Str("masterchain", storage.FormatBlockRef(master)).
			Uint32("shard_client_seqno", shardClientSeqno).
			Int("states", len(checkpoint.entries)).
			Int("shards", shards).
			Dur("queued_for", time.Since(queuedAt)).
			Dur("elapsed", elapsed).
			Msg("next-block shard-client checkpoint persisted")
		s.publishCommittedCurrentState(committed)
		s.markLiveCheckpointStatesFlushed(checkpoint.entries)
	})

	return checkpoint.live, nil
}

func (s *Service) persistNextBlockCurrentStateSyncLocked(current *storage.CurrentState, timing *catchUpTiming, reason string, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache, onCommitted func(), onDone func(), lockElapsed time.Duration) (*storage.CurrentState, error) {
	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}
	checkpoint, err := prepareStateCheckpoint(current, entries, cells)
	if err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}
	master := current.Masterchain.Block
	shardClientSeqno := current.ShardClientSeqno
	shards := len(current.Shards)
	timing.checkpoints++
	if onDone != nil {
		defer onDone()
	}

	started := time.Now()
	persistCtx := s.currentStatePersistContext()
	committed, stages, err := s.saveStateCheckpoint(persistCtx, checkpoint.persisted, checkpoint.entries, checkpoint.cells, checkpoint.cellPrewriteTarget)
	elapsed := time.Since(started)
	if err != nil {
		s.observeSyncPersist(SyncPersistObservation{
			Mode:          "next_block_sync",
			Result:        "error",
			QueueDuration: lockElapsed,
			Duration:      elapsed,
			States:        len(checkpoint.entries),
			Stages:        stages,
		})
		s.currentStatePersistMu.Unlock()
		return nil, fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), err)
	}

	if onCommitted != nil {
		onCommitted()
	}
	cells.complete()
	s.currentStatePersistMu.Unlock()

	s.observeSyncPersist(SyncPersistObservation{
		Mode:          "next_block_sync",
		Result:        "success",
		QueueDuration: lockElapsed,
		Duration:      elapsed,
		States:        len(checkpoint.entries),
		Stages:        stages,
	})
	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("shard_client_seqno", shardClientSeqno).
		Int("shards", shards).
		Str("reason", reason).
		Dur("elapsed", elapsed).
		Int("states", len(checkpoint.entries)).
		Msg("next-block shard-client checkpoint persisted")
	s.publishCommittedCurrentState(committed)
	s.markLiveCheckpointStatesFlushed(checkpoint.entries)
	return checkpoint.live, nil
}

func (s *Service) markLiveCheckpointStatesFlushed(entries []storage.StateCheckpointBlock) {
	if len(entries) == 0 {
		return
	}

	var stateBlocks []ton.BlockIDExt
	var artifactBlocks []ton.BlockIDExt
	for _, entry := range entries {
		if entry.State != nil {
			stateBlocks = append(stateBlocks, entry.State.Block)
		}
		if entry.Artifact != nil {
			artifactBlocks = append(artifactBlocks, entry.Artifact.ID)
		}
	}
	if s.liveState != nil && len(stateBlocks) > 0 {
		s.liveState.MarkLiveBlockStatesFlushed(stateBlocks)
	}
	if s.liveState != nil {
		for _, block := range artifactBlocks {
			s.liveState.MarkLiveBlockFlushed(block)
		}
	}
	if s.liveBlockCache != nil {
		for _, block := range artifactBlocks {
			s.liveBlockCache.MarkBlockFlushed(block)
		}
	}
}

func (s *Service) currentStatePersistContext() context.Context {
	if s.shutdownContext != nil {
		return s.shutdownContext
	}
	return context.Background()
}

func (s *Service) setCurrentStatePersistError(err error) {
	if err == nil {
		return
	}
	s.currentStatePersistErrMu.Lock()
	if s.currentStatePersistErr == nil {
		s.currentStatePersistErr = err
	}
	s.currentStatePersistErrMu.Unlock()
}

func (s *Service) takeCurrentStatePersistError() error {
	s.currentStatePersistErrMu.Lock()
	defer s.currentStatePersistErrMu.Unlock()

	err := s.currentStatePersistErr
	s.currentStatePersistErr = nil
	return err
}

func (s *Service) waitCurrentStatePersist(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if s.currentStatePersistMu.TryLock() {
			s.currentStatePersistMu.Unlock()
			return s.takeCurrentStatePersistError()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) waitCurrentStatePersistOrWake(ctx context.Context) (bool, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if s.currentStatePersistMu.TryLock() {
			s.currentStatePersistMu.Unlock()
			return false, s.takeCurrentStatePersistError()
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-s.currentStateWake:
			if s.currentStatePersistMu.TryLock() {
				s.currentStatePersistMu.Unlock()
				if err := s.takeCurrentStatePersistError(); err != nil {
					return false, err
				}
			}
			return true, nil
		case <-ticker.C:
		}
	}
}

func (s *Service) downloadNextChainBlockProbe(ctx context.Context, prev ton.BlockIDExt, prevUTime int64, state nextBlockBootstrapProbeState) (PreparedBlock, string, time.Duration, error) {
	target := masterchainSeqnoTarget(^uint32(0))

	cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
	if err == nil {
		return cached.block, cached.source, cached.prepareElapsed, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return PreparedBlock{}, "", 0, err
	}

	decision, broadcastWake := s.nextBlockBootstrapProbeDecision(prev, prevUTime, state)
	stagedPeerLimit := decision.stagedPeerLimit()
	queryCtx, cancel := context.WithTimeout(ctx, decision.probeTimeout())
	if decision.peerLimit > nextBlockBootstrapProbePeers || stagedPeerLimit > decision.peerLimit {
		event := s.log.Debug().
			Str("current", storage.FormatBlockRef(prev)).
			Int("probe_peers", decision.peerLimit).
			Int("staged_probe_peers", stagedPeerLimit).
			Int("consecutive_misses", decision.consecutiveMisses).
			Bool("live_tail", decision.liveTail).
			Bool("raw_broadcast_ahead", decision.rawBroadcastAhead).
			Bool("observed_masterchain_ahead", decision.observedAhead).
			Bool("seen_masterchain_ahead", decision.seenAhead).
			Bool("queued_future_ahead", decision.queuedFutureAhead).
			Uint32("ahead_blocks", decision.aheadBlocks)
		if decision.lowestMissingSeqno != 0 {
			event.Uint32("lowest_missing_seqno", decision.lowestMissingSeqno)
		}
		if !decision.preferredSourcePeerID.IsZero() {
			event.Str("preferred_source_peer_id", decision.preferredSourcePeerID.String())
		}
		if decision.hasLag {
			event.Int64("lag_seconds", decision.lagSeconds)
		}
		event.Msg("probing next masterchain block with urgent fanout")
	}
	result := make(chan nextBlockProbeResult, 1)
	probeReturned := make(chan struct{})
	go func() {
		downloaded, err := s.node.ProbeNextBlockFull(queryCtx, prev, p2p.ProbeNextBlockFullOptions{
			PeerLimit:       decision.peerLimit,
			StagedPeerLimit: stagedPeerLimit,
			StageDelay:      nextBlockBootstrapLiveStageDelay,
			PreferredPeerID: decision.preferredSourcePeerID,
			LiveTail:        decision.liveTail,
		})
		close(probeReturned)
		result <- prepareNextBlockProbeResult(prev, downloaded, err)
	}()

	prepared, source, prepareElapsed, err := s.waitNextMasterchainApplyCandidate(
		ctx,
		queryCtx,
		prev,
		target,
		broadcastWake,
		probeReturned,
		result,
		cancel,
	)
	if err != nil {
		return PreparedBlock{}, source, prepareElapsed, err
	}
	return prepared, source, prepareElapsed, nil
}

func (s *Service) waitNextMasterchainApplyCandidate(
	ctx context.Context,
	queryCtx context.Context,
	prev, target ton.BlockIDExt,
	broadcastWake <-chan struct{},
	probeReturned <-chan struct{},
	result <-chan nextBlockProbeResult,
	cancel context.CancelFunc,
) (PreparedBlock, string, time.Duration, error) {
	queryDone := queryCtx.Done()
	for {
		select {
		case res := <-result:
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil {
				cancel()
				return cached.block, cached.source, cached.prepareElapsed, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				cancel()
				return PreparedBlock{}, "", 0, err
			}

			cancel()
			if res.err != nil {
				return PreparedBlock{}, res.source, res.prepareElapsed, res.err
			}
			return res.block, res.source, res.prepareElapsed, nil
		case <-s.currentStateWake:
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil {
				cancel()
				return cached.block, cached.source, cached.prepareElapsed, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				cancel()
				return PreparedBlock{}, "", 0, err
			}
		case <-broadcastWake:
			cached, err := s.takeCachedMasterchainBlockForApply(ctx, prev, target)
			if err == nil {
				cancel()
				return cached.block, cached.source, cached.prepareElapsed, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				cancel()
				return PreparedBlock{}, "", 0, err
			}
			broadcastWake = nil
		case <-probeReturned:
			probeReturned = nil
			queryDone = nil
		case <-queryDone:
			select {
			case <-probeReturned:
				probeReturned = nil
				queryDone = nil
				continue
			default:
			}

			cancel()
			return PreparedBlock{}, "", 0, fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), queryCtx.Err())
		case <-ctx.Done():
			cancel()
			return PreparedBlock{}, "", 0, ctx.Err()
		}
	}
}

type nextBlockProbeResult struct {
	block          PreparedBlock
	source         string
	prepareElapsed time.Duration
	err            error
}

func prepareNextBlockProbeResult(prev ton.BlockIDExt, downloaded *p2p.DownloadedBlock, err error) nextBlockProbeResult {
	if err != nil {
		return nextBlockProbeResult{err: fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), err)}
	}
	if downloaded == nil {
		return nextBlockProbeResult{err: fmt.Errorf("probe next block after %s: empty response", storage.FormatBlockRef(prev))}
	}

	prepareStarted := time.Now()
	verified, err := verifyDownloadedBlock(*downloaded)
	prepareElapsed := time.Since(prepareStarted)
	if err != nil {
		return nextBlockProbeResult{
			source:         syncBlockSourceForDownloadedBlock("peer_probe", *downloaded),
			prepareElapsed: prepareElapsed,
			err:            fmt.Errorf("verify probed next block after %s: %w", storage.FormatBlockRef(prev), err),
		}
	}

	prepared, err := prepareVerifiedMasterchainBlockForNextSync(prev, verified)
	prepareElapsed = time.Since(prepareStarted)
	if err != nil {
		return nextBlockProbeResult{
			source:         syncBlockSourceForVerifiedBlock("peer_probe", verified),
			prepareElapsed: prepareElapsed,
			err:            fmt.Errorf("prepare probed next block after %s: %w", storage.FormatBlockRef(prev), err),
		}
	}

	prepared.PrepareElapsed = prepareElapsed
	return nextBlockProbeResult{
		block:          prepared,
		source:         syncBlockSourceForKind("peer_probe", verified.Kind),
		prepareElapsed: prepared.PrepareElapsed,
	}
}

func downloadedBlockTimeLag(downloaded PreparedBlock, now time.Time) (uint32, int64, bool) {
	if downloaded.Meta == nil || downloaded.Meta.GenUTime == 0 {
		return 0, 0, false
	}
	return downloaded.Meta.GenUTime, now.Unix() - int64(downloaded.Meta.GenUTime), true
}

func (s *Service) downloadNextChainBlock(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadChainBlockWithRetry(ctx, fmt.Sprintf("download next block after %s", storage.FormatBlockRef(prev)), func(ctx context.Context) (*p2p.DownloadedBlock, error) {
		return s.node.DownloadNextBlockFull(ctx, prev)
	})
}

func (s *Service) downloadChainBlockWithRetry(ctx context.Context, label string, download func(context.Context) (*p2p.DownloadedBlock, error)) (p2p.DownloadedBlock, error) {
	var lastErr error
	for attempt := 1; attempt <= chainBlockDownloadRetries; attempt++ {
		downloaded, err := download(ctx)
		if err == nil && downloaded != nil {
			return *downloaded, nil
		}
		if err == nil {
			err = fmt.Errorf("empty response")
		}
		lastErr = err

		if attempt == chainBlockDownloadRetries {
			break
		}
		if err = waitRetry(ctx, chainBlockDownloadRetryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("%s: %w", label, lastErr)
}
