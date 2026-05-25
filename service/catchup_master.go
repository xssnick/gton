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
	peerLimit          int
	consecutiveMisses  int
	liveTail           bool
	rawBroadcastAhead  bool
	observedAhead      bool
	seenAhead          bool
	queuedFutureAhead  bool
	preferredSourceKey string
	aheadBlocks        uint32
	lowestMissingSeqno uint32
	lagSeconds         int64
	hasLag             bool
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
		decision.preferredSourceKey = queued.sourceKey
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
		decision.preferredSourceKey = ""
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
	if !d.liveTail {
		return false
	}
	if d.rawBroadcastAhead || d.observedAhead || d.seenAhead || d.queuedFutureAhead {
		return true
	}
	if d.consecutiveMisses >= nextBlockBootstrapUrgentMisses {
		return true
	}
	return d.hasLag && d.lagSeconds >= nextBlockBootstrapUrgentLagSeconds
}

func (d nextBlockBootstrapProbeDecision) shouldUseWideFanout() bool {
	if !d.liveTail {
		return false
	}
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

func (s *Service) publishLiveCurrentState(current *storage.CurrentState) {
	s.publishLiveCurrentStateChanged(current)
}

func (s *Service) publishCommittedCurrentState(current *storage.CurrentState) {
	if !s.publishLiveCurrentStateChanged(current) {
		return
	}
	if s.liveState != nil {
		s.liveState.SetLiveCurrentState(current)
		if flusher, ok := s.liveState.(CurrentStateFlusher); ok {
			flusher.MarkLiveCurrentStateFlushed(current)
		}
	}
	s.wakePersistentStateSerializer()
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

	return true
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

func (s *Service) applyMasterchainTransitionWithConsensusProof(current *storage.BlockState, downloaded p2p.DownloadedBlock, proof *masterchainConsensusProof, applier stateUpdateApplier) (*storage.BlockState, masterchainApplyTiming, error) {
	return s.applyMasterchainTransition(current, downloaded, proof, applier, true)
}

func (s *Service) applyStoredMasterchainTransition(current *storage.BlockState, downloaded p2p.DownloadedBlock, applier stateUpdateApplier) (*storage.BlockState, masterchainApplyTiming, error) {
	return s.applyMasterchainTransition(current, downloaded, nil, applier, false)
}

func (s *Service) applyMasterchainTransition(current *storage.BlockState, downloaded p2p.DownloadedBlock, proof *masterchainConsensusProof, applier stateUpdateApplier, validateConsensus bool) (*storage.BlockState, masterchainApplyTiming, error) {
	started := time.Now()
	var timing masterchainApplyTiming
	finish := func() masterchainApplyTiming {
		timing.total = time.Since(started)
		return timing
	}

	if current == nil {
		return nil, finish(), fmt.Errorf("current masterchain state is nil")
	}

	stageStarted := time.Now()
	downloaded, err := prepareDownloadedBlock(downloaded)
	timing.prepare += time.Since(stageStarted)
	if err != nil {
		return nil, finish(), err
	}
	if downloaded.ID.Workchain != -1 || downloaded.ID.Shard != topShard {
		return nil, finish(), fmt.Errorf("download next masterchain block after %s returned %s", storage.FormatBlockRef(current.Block), downloaded.BlockRef())
	}
	if len(downloaded.Meta.PrevRefs) != 1 {
		return nil, finish(), fmt.Errorf("masterchain block %s has %d previous refs", downloaded.BlockRef(), len(downloaded.Meta.PrevRefs))
	}
	prev := downloaded.Meta.PrevRefs[0]
	if !prev.Equals(&current.Block) {
		return nil, finish(), fmt.Errorf("%w: block=%s prev=%s current=%s", errMasterchainPrevMismatch, downloaded.BlockRef(), storage.FormatBlockRef(prev), storage.FormatBlockRef(current.Block))
	}

	if validateConsensus {
		stageStarted = time.Now()
		if err = s.validateMasterchainBlockConsensusWithProof(current, tonBlockForConsensus{block: downloaded.ID, proofBOC: downloaded.ProofBOC, broadcastSignatures: downloaded.BroadcastSignatures}, proof); err != nil {
			timing.consensus += time.Since(stageStarted)
			return nil, finish(), fmt.Errorf("validate masterchain consensus for %s: %w", downloaded.BlockRef(), err)
		}
		timing.consensus += time.Since(stageStarted)
	}

	stageStarted = time.Now()
	next, err := applyBlockWithPreviousStates([]*storage.BlockState{current}, downloaded, applier)
	timing.stateUpdate += time.Since(stageStarted)
	if err != nil {
		return nil, finish(), fmt.Errorf("apply masterchain block %s: %w", downloaded.BlockRef(), err)
	}

	return next, finish(), nil
}

func (s *Service) persistNextBlockCurrentStateLocked(current *storage.CurrentState, timing *catchUpTiming, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache, onCommitted func(), lockElapsed time.Duration, queuedAt time.Time) (*storage.CurrentState, error) {
	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}

	checkpoint, err := prepareStateCheckpoint(current, entries)
	if err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}
	master := current.Masterchain.Block
	artifactTarget := s.appliedBlockArtifactTarget()
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
		started := time.Now()
		committed, err := s.saveStateCheckpoint(persistCtx, checkpoint.persisted, checkpoint.entries, artifactTarget)
		elapsed := time.Since(started)
		s.currentStatePersistMu.Unlock()
		if err != nil {
			s.observeSyncPersist(SyncPersistObservation{
				Mode:          "next_block_async",
				Result:        "error",
				QueueDuration: time.Since(queuedAt) - elapsed,
				Duration:      elapsed,
				States:        len(checkpoint.entries),
			})
			wrapped := fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), err)
			s.setCurrentStatePersistError(wrapped)
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
		s.observeSyncPersist(SyncPersistObservation{
			Mode:          "next_block_async",
			Result:        "success",
			QueueDuration: time.Since(queuedAt) - elapsed,
			Duration:      elapsed,
			States:        len(checkpoint.entries),
		})
		s.log.Debug().
			Str("masterchain", storage.FormatBlockRef(master)).
			Uint32("shard_client_seqno", shardClientSeqno).
			Int("states", len(checkpoint.entries)).
			Int("shards", shards).
			Dur("queued_for", time.Since(queuedAt)).
			Dur("elapsed", elapsed).
			Msg("next-block shard-client checkpoint persisted")
		cells.complete()
		s.publishCommittedCurrentState(committed)
		s.markLiveCheckpointStatesFlushed(checkpoint.entries)
	})

	return checkpoint.live, nil
}

func (s *Service) persistNextBlockCurrentStateSyncLocked(current *storage.CurrentState, timing *catchUpTiming, reason string, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache, lockElapsed time.Duration) (*storage.CurrentState, error) {
	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}
	checkpoint, err := prepareStateCheckpoint(current, entries)
	if err != nil {
		s.currentStatePersistMu.Unlock()
		return nil, err
	}
	master := current.Masterchain.Block
	artifactTarget := s.appliedBlockArtifactTarget()
	shardClientSeqno := current.ShardClientSeqno
	shards := len(current.Shards)
	timing.checkpoints++

	started := time.Now()
	persistCtx := s.currentStatePersistContext()
	committed, err := s.saveStateCheckpoint(persistCtx, checkpoint.persisted, checkpoint.entries, artifactTarget)
	elapsed := time.Since(started)
	s.currentStatePersistMu.Unlock()
	if err != nil {
		s.observeSyncPersist(SyncPersistObservation{
			Mode:          "next_block_sync",
			Result:        "error",
			QueueDuration: lockElapsed,
			Duration:      elapsed,
			States:        len(checkpoint.entries),
		})
		return nil, fmt.Errorf("persist next-block current state %s: %w", storage.FormatBlockRef(master), err)
	}

	s.observeSyncPersist(SyncPersistObservation{
		Mode:          "next_block_sync",
		Result:        "success",
		QueueDuration: lockElapsed,
		Duration:      elapsed,
		States:        len(checkpoint.entries),
	})
	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("shard_client_seqno", shardClientSeqno).
		Int("shards", shards).
		Str("reason", reason).
		Dur("elapsed", elapsed).
		Int("states", len(checkpoint.entries)).
		Msg("next-block shard-client checkpoint persisted")
	cells.complete()
	s.publishCommittedCurrentState(committed)
	s.markLiveCheckpointStatesFlushed(checkpoint.entries)
	return checkpoint.live, nil
}

func (s *Service) markLiveCheckpointStatesFlushed(entries []storage.StateCheckpointBlock) {
	flusher, ok := s.liveState.(liveBlockStateFlusher)
	if !ok || len(entries) == 0 {
		return
	}

	blocks := make([]ton.BlockIDExt, 0, len(entries))
	for _, entry := range entries {
		if entry.State == nil {
			continue
		}
		blocks = append(blocks, entry.State.Block)
	}
	flusher.MarkLiveBlockStatesFlushed(blocks)
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

func (s *Service) downloadNextChainBlockProbe(ctx context.Context, prev ton.BlockIDExt, prevUTime int64, state nextBlockBootstrapProbeState) (p2p.DownloadedBlock, string, error) {
	if s.node == nil {
		return p2p.DownloadedBlock{}, "", fmt.Errorf("p2p node is nil")
	}

	for {
		if downloaded, source, ok, err := s.takeCachedMasterchainBlockForApply(ctx, prev, masterchainSeqnoTarget(^uint32(0))); ok || err != nil {
			return downloaded, source, err
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
			if decision.preferredSourceKey != "" {
				event.Str("preferred_source", decision.preferredSourceKey)
			}
			if decision.hasLag {
				event.Int64("lag_seconds", decision.lagSeconds)
			}
			event.Msg("probing next masterchain block with urgent fanout")
		}
		result := make(chan nextBlockProbeResult, 1)
		go func() {
			downloaded, err := s.node.ProbeNextBlockFull(queryCtx, prev, p2p.ProbeNextBlockFullOptions{
				PeerLimit:        decision.peerLimit,
				StagedPeerLimit:  stagedPeerLimit,
				StageDelay:       nextBlockBootstrapLiveStageDelay,
				PreferredPeerKey: decision.preferredSourceKey,
				LiveTail:         decision.liveTail,
			})
			result <- nextBlockProbeResult{downloaded: downloaded, err: err}
		}()

		for {
			select {
			case res := <-result:
				cancel()
				if res.err != nil {
					return p2p.DownloadedBlock{}, "", fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), res.err)
				}
				if res.downloaded == nil {
					return p2p.DownloadedBlock{}, "", fmt.Errorf("probe next block after %s: empty response", storage.FormatBlockRef(prev))
				}
				prepared, err := prepareDownloadedBlockStateCells(*res.downloaded)
				if err != nil {
					return p2p.DownloadedBlock{}, "", fmt.Errorf("prepare probed next block after %s: %w", storage.FormatBlockRef(prev), err)
				}
				return prepared, syncBlockSourceForDownloadedBlock("peer_probe", prepared), nil
			case <-s.currentStateWake:
				if downloaded, source, ok, err := s.takeCachedMasterchainBlockForApply(ctx, prev, masterchainSeqnoTarget(^uint32(0))); ok || err != nil {
					cancel()
					return downloaded, source, err
				}
			case <-broadcastWake:
				if downloaded, source, ok, err := s.takeCachedMasterchainBlockForApply(ctx, prev, masterchainSeqnoTarget(^uint32(0))); ok || err != nil {
					cancel()
					return downloaded, source, err
				}
				broadcastWake = nil
			case <-queryCtx.Done():
				cancel()
				return p2p.DownloadedBlock{}, "", fmt.Errorf("probe next block after %s: %w", storage.FormatBlockRef(prev), queryCtx.Err())
			case <-ctx.Done():
				cancel()
				return p2p.DownloadedBlock{}, "", ctx.Err()
			}
		}
	}
}

type nextBlockProbeResult struct {
	downloaded *p2p.DownloadedBlock
	err        error
}

func downloadedBlockTimeLag(downloaded p2p.DownloadedBlock, now time.Time) (uint32, int64, bool) {
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

func (s *Service) downloadExactChainBlock(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadChainBlockWithRetry(ctx, fmt.Sprintf("download block %s", storage.FormatBlockRef(block)), func(ctx context.Context) (*p2p.DownloadedBlock, error) {
		return s.node.DownloadBlockFull(ctx, block)
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
