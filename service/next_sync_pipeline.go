package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"flexserver/service/p2p"
	"flexserver/service/state"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	nextMasterchainPrefetchBlocks  = 32
	nextShardPrefetchScheduleLimit = 2048
)

type nextSyncMode int

const (
	nextSyncToTarget nextSyncMode = iota
	nextSyncBootstrap
)

type nextSyncRunner struct {
	service *Service
	ctx     context.Context
	cancel  context.CancelFunc

	mode   nextSyncMode
	method string
	target ton.BlockIDExt

	current *storage.CurrentState
	master  *storage.BlockState

	started    time.Time
	startSeqno uint32
	lastLog    time.Time

	totalBlocks uint32
	maxBlocks   uint32

	timing                       catchUpTiming
	stagedBlocks                 uint32
	checkpointMu                 sync.Mutex
	checkpointStates             appliedStateSet
	stateCells                   *stateCellWindowCache
	shardCache                   map[string]*storage.BlockState
	shardResolver                *shardStateResolver
	shardResolverSeen            shardStateResolverStats
	shardTarget                  time.Duration
	shardWall                    time.Duration
	shardApply                   time.Duration
	shardApplied                 uint64
	shardReused                  uint64
	shardPrefetchTargets         uint64
	committed                    uint32
	lastProgressMasterSeqno      uint32
	lastProgressShardClientSeqno uint32
	lastProgressShardApplied     uint64
	lastProgressShardReused      uint64
	lastProgressShardPrefetch    uint64
	lastBlockUTime               uint32
	lastLagSeconds               int64
	hasBlockTime                 bool
}

type nextMasterDownload struct {
	prev            ton.BlockIDExt
	block           p2p.DownloadedBlock
	source          string
	downloadElapsed time.Duration
	err             error
}

type nextAppliedMaster struct {
	prev                 ton.BlockIDExt
	block                p2p.DownloadedBlock
	master               *storage.BlockState
	downloadSource       string
	downloadElapsed      time.Duration
	applyTiming          masterchainApplyTiming
	stageElapsed         time.Duration
	shardTargets         []ton.BlockIDExt
	shardTargetParse     time.Duration
	shardPrefetchTargets int
	err                  error
}

func (s *Service) runNextSyncToTarget(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*storage.CurrentState, error) {
	next, _, err := s.runNextSync(ctx, current, nextSyncToTarget, target, 0, "next_block")
	return next, err
}

func (s *Service) runNextSyncBootstrap(ctx context.Context, current *storage.CurrentState) (*storage.CurrentState, bool, error) {
	next, processed, err := s.runNextSync(ctx, current, nextSyncBootstrap, ton.BlockIDExt{}, nextBlockBootstrapBlocks, "next_block_bootstrap")
	return next, processed > 0, err
}

func (s *Service) runNextSync(ctx context.Context, current *storage.CurrentState, mode nextSyncMode, target ton.BlockIDExt, maxBlocks uint32, method string) (*storage.CurrentState, uint32, error) {
	if current == nil {
		return nil, 0, fmt.Errorf("current state is nil")
	}

	master, err := s.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return nil, 0, fmt.Errorf("load current masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	if mode == nextSyncToTarget && master.Block.SeqNo >= target.SeqNo {
		return current, 0, nil
	}
	s.rememberMasterState(master)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	now := time.Now()
	totalBlocks := uint32(0)
	if mode == nextSyncToTarget {
		totalBlocks = target.SeqNo - master.Block.SeqNo
	}
	r := &nextSyncRunner{
		service:                      s,
		ctx:                          runCtx,
		cancel:                       cancel,
		mode:                         mode,
		method:                       method,
		target:                       target,
		current:                      storage.CloneCurrentState(current),
		master:                       master,
		started:                      now,
		startSeqno:                   master.Block.SeqNo,
		lastLog:                      now,
		totalBlocks:                  totalBlocks,
		maxBlocks:                    maxBlocks,
		timing:                       newCatchUpTiming(now),
		stateCells:                   newStateCellWindowCache(s.stateCellLoader()),
		shardCache:                   map[string]*storage.BlockState{},
		lastProgressMasterSeqno:      master.Block.SeqNo,
		lastProgressShardClientSeqno: current.Masterchain.Block.SeqNo,
	}
	r.shardResolver = newShardStateResolver(runCtx, shardStateResolverConfig{
		current:         r.current.Shards,
		cache:           r.shardCache,
		loadState:       s.loadBlockStateForApply,
		loadBlock:       s.loadOrDownloadBlockForApply,
		apply:           r.applyResolvedShardBlock,
		afterApplyState: r.afterApplyShardState,
	})
	return r.run()
}

func (r *nextSyncRunner) run() (*storage.CurrentState, uint32, error) {
	r.logStart()

	downloads := r.startMasterSource()
	applied := r.startMasterApply(downloads)
	prefetched := r.startShardPrefetch(applied)
	current, err := r.commitCurrent(prefetched)
	return current, r.committed, err
}

func (r *nextSyncRunner) logStart() {
	event := r.service.log.Info().
		Str("from", storage.FormatBlockRef(r.master.Block)).
		Int("master_prefetch", nextMasterchainPrefetchBlocks).
		Int("shard_apply_workers", archiveShardApplyParallelism).
		Int("shard_download_buffer", shardStateDownloadBuffer)

	if r.mode == nextSyncToTarget {
		event.
			Str("target", storage.FormatBlockRef(r.target)).
			Uint32("total_blocks", r.totalBlocks).
			Uint32("remaining", r.target.SeqNo-r.master.Block.SeqNo).
			Msg("catching up shard-client with next-block pipeline")
		return
	}

	if r.maxBlocks > 0 {
		event.Uint32("max_blocks", r.maxBlocks)
	} else {
		event.Bool("live_tail", true)
	}
	event.Msg("bootstrapping shard-client with next-block pipeline")
}

func (r *nextSyncRunner) startMasterSource() <-chan nextMasterDownload {
	out := make(chan nextMasterDownload, nextMasterchainPrefetchBlocks)
	go func() {
		defer close(out)
		if r.mode == nextSyncToTarget {
			r.runTargetMasterSource(out)
			return
		}
		r.runBootstrapMasterSource(out)
	}()
	return out
}

func (r *nextSyncRunner) runTargetMasterSource(out chan<- nextMasterDownload) {
	downloads := r.service.downloadShardStateBlocks(r.ctx, r.master.Block, r.target)
	for item := range downloads {
		next := nextMasterDownload{
			prev:            item.prev,
			block:           item.block,
			source:          item.source,
			downloadElapsed: item.downloadElapsed,
			err:             item.err,
		}
		if next.source == "" {
			next.source = "unknown"
		}
		if !r.sendMasterDownload(out, next) {
			return
		}
		if item.err != nil {
			return
		}
	}
	if err := r.ctx.Err(); err != nil {
		r.sendMasterDownload(out, nextMasterDownload{err: err})
	}
}

func (r *nextSyncRunner) runBootstrapMasterSource(out chan<- nextMasterDownload) {
	prev := r.master.Block
	for processed := uint32(0); r.maxBlocks == 0 || processed < r.maxBlocks; {
		started := time.Now()
		downloaded, source, err := r.service.downloadNextChainBlockProbe(r.ctx, prev)
		elapsed := time.Since(started)
		if err != nil {
			if isExpectedRetryError(err) {
				r.service.log.Debug().
					Err(err).
					Str("current", storage.FormatBlockRef(prev)).
					Msg("next masterchain block is not available during bootstrap")
				if !r.waitBootstrapRetry() {
					return
				}
				continue
			}
			r.sendMasterDownload(out, nextMasterDownload{err: err})
			return
		}

		item := nextMasterDownload{
			prev:            prev,
			block:           downloaded,
			source:          source,
			downloadElapsed: elapsed,
		}
		if !r.sendMasterDownload(out, item) {
			return
		}
		prev = downloaded.ID
		processed++
	}
}

func (r *nextSyncRunner) waitBootstrapRetry() bool {
	timer := time.NewTimer(currentStateLivePollDelay)
	defer timer.Stop()

	select {
	case <-r.ctx.Done():
		return false
	case <-r.service.currentStateWake:
		return true
	case <-timer.C:
		return true
	}
}

func (r *nextSyncRunner) sendMasterDownload(out chan<- nextMasterDownload, item nextMasterDownload) bool {
	select {
	case out <- item:
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *nextSyncRunner) startMasterApply(downloads <-chan nextMasterDownload) <-chan nextAppliedMaster {
	out := make(chan nextAppliedMaster, nextMasterchainPrefetchBlocks)
	start := storage.CloneBlockState(r.master)
	go func() {
		defer close(out)
		master := start
		for item := range downloads {
			if item.err != nil {
				r.sendAppliedMaster(out, nextAppliedMaster{err: item.err})
				return
			}
			if !item.prev.Equals(&master.Block) {
				r.sendAppliedMaster(out, nextAppliedMaster{
					err: fmt.Errorf("downloaded masterchain block %s follows %s while current state is %s", item.block.BlockRef(), storage.FormatBlockRef(item.prev), storage.FormatBlockRef(master.Block)),
				})
				return
			}

			applied, err := r.applyMaster(master, item)
			if err == nil {
				r.logMasterApplied(applied)
			}
			if !r.sendAppliedMaster(out, applied) {
				return
			}
			if err != nil {
				return
			}
			master = applied.master
		}
	}()
	return out
}

func (r *nextSyncRunner) applyMaster(master *storage.BlockState, item nextMasterDownload) (nextAppliedMaster, error) {
	applied := nextAppliedMaster{
		prev:            item.prev,
		block:           item.block,
		downloadSource:  item.source,
		downloadElapsed: item.downloadElapsed,
	}

	nextMaster, applyTiming, err := r.service.applyMasterchainTransitionWithConsensusProof(master, item.block, nil, r.stateCells)
	applied.applyTiming = applyTiming
	if err != nil {
		applied.err = err
		return applied, err
	}

	r.service.publishLiveBlock(item.block, false)
	if err = r.service.stageAppliedBlockArtifact(r.ctx, item.block); err != nil {
		applied.err = err
		return applied, err
	}
	if err = r.service.rememberSeenMasterchainBlock(r.ctx, nextMaster.Block); err != nil {
		applied.err = err
		return applied, err
	}
	r.service.rememberMasterState(nextMaster)
	applied.master = nextMaster
	return applied, nil
}

func (r *nextSyncRunner) sendAppliedMaster(out chan<- nextAppliedMaster, item nextAppliedMaster) bool {
	select {
	case out <- item:
		return true
	case <-r.ctx.Done():
		return false
	}
}

func (r *nextSyncRunner) startShardPrefetch(applied <-chan nextAppliedMaster) <-chan nextAppliedMaster {
	out := make(chan nextAppliedMaster, nextMasterchainPrefetchBlocks)
	go func() {
		defer close(out)

		scheduled := make(map[string]struct{}, nextMasterchainPrefetchBlocks)
		scheduledOrder := make([]string, 0, nextMasterchainPrefetchBlocks)

		for item := range applied {
			if item.err == nil && item.master != nil {
				targets, elapsed, err := r.shardTargetsForMaster(item.master)
				item.shardTargets = targets
				item.shardTargetParse = elapsed
				if err != nil {
					item.err = err
				} else {
					item.shardPrefetchTargets = r.scheduleShardPrefetch(scheduled, &scheduledOrder, item.master.Block, targets)
				}
			}
			if !r.sendAppliedMaster(out, item) {
				return
			}
			if item.err != nil {
				return
			}
		}
	}()
	return out
}

func (r *nextSyncRunner) shardTargetsForMaster(master *storage.BlockState) ([]ton.BlockIDExt, time.Duration, error) {
	started := time.Now()
	targets, err := state.ShardBlocksFromMasterState(master)
	elapsed := time.Since(started)
	if err != nil {
		return nil, elapsed, fmt.Errorf("parse next-block shard targets from master state %s: %w", storage.FormatBlockRef(master.Block), err)
	}
	return targets, elapsed, nil
}

func (r *nextSyncRunner) scheduleShardPrefetch(scheduled map[string]struct{}, scheduledOrder *[]string, master ton.BlockIDExt, targets []ton.BlockIDExt) int {
	count := 0
	for _, target := range targets {
		key := storage.BlockKey(target)
		if !rememberScheduledShardPrefetch(scheduled, scheduledOrder, key) {
			continue
		}
		count++
		r.prefetchShardTarget(master, target)
	}
	return count
}

func rememberScheduledShardPrefetch(scheduled map[string]struct{}, scheduledOrder *[]string, key string) bool {
	if _, ok := scheduled[key]; ok {
		return false
	}

	scheduled[key] = struct{}{}
	*scheduledOrder = append(*scheduledOrder, key)
	if len(scheduled) <= nextShardPrefetchScheduleLimit {
		return true
	}

	evict := (*scheduledOrder)[0]
	delete(scheduled, evict)
	copy(*scheduledOrder, (*scheduledOrder)[1:])
	*scheduledOrder = (*scheduledOrder)[:len(*scheduledOrder)-1]
	return true
}

func (r *nextSyncRunner) prefetchShardTarget(master ton.BlockIDExt, target ton.BlockIDExt) {
	go func() {
		_, err := r.shardResolver.resolve(target)
		if err != nil && r.ctx.Err() == nil {
			r.service.log.Debug().
				Err(err).
				Str("masterchain", storage.FormatBlockRef(master)).
				Str("target", storage.FormatBlockRef(target)).
				Msg("next-block shard target prefetch failed")
		}
	}()
}

func (r *nextSyncRunner) commitCurrent(applied <-chan nextAppliedMaster) (*storage.CurrentState, error) {
	idleCheckpoint := time.NewTimer(nextBlockIdleCheckpointDelay)
	if !idleCheckpoint.Stop() {
		<-idleCheckpoint.C
	}
	idleCheckpointActive := false
	defer idleCheckpoint.Stop()

	armIdleCheckpoint := func() {
		if r.stagedBlocks == 0 || idleCheckpointActive {
			return
		}
		idleCheckpoint.Reset(nextBlockIdleCheckpointDelay)
		idleCheckpointActive = true
	}
	stopIdleCheckpoint := func() {
		if !idleCheckpointActive {
			return
		}
		if !idleCheckpoint.Stop() {
			select {
			case <-idleCheckpoint.C:
			default:
			}
		}
		idleCheckpointActive = false
	}

	for {
		waitStarted := time.Now()
		armIdleCheckpoint()

		select {
		case item, ok := <-applied:
			stopIdleCheckpoint()
			masterPipelineWait := time.Since(waitStarted)
			if !ok {
				if err := r.flushStagedCurrent(); err != nil {
					return r.current, err
				}
				return r.current, nil
			}
			if item.err != nil {
				return r.current, item.err
			}
			if item.master == nil {
				return r.current, fmt.Errorf("next-block pipeline applied empty master state")
			}

			if !item.prev.Equals(&r.current.Masterchain.Block) {
				return r.current, fmt.Errorf("applied masterchain block %s follows %s while current state is %s", item.block.BlockRef(), storage.FormatBlockRef(item.prev), storage.FormatBlockRef(r.current.Masterchain.Block))
			}

			if err := r.commitOne(item, masterPipelineWait, len(applied)); err != nil {
				return r.current, err
			}

			if r.shouldStopAfterCommit() {
				r.cancel()
				if err := r.flushStagedCurrent(); err != nil {
					return r.current, err
				}
				return r.current, nil
			}

		case <-idleCheckpoint.C:
			idleCheckpointActive = false
			if err := r.flushStagedCurrent(); err != nil {
				return r.current, err
			}
		case <-r.ctx.Done():
			if err := r.flushStagedCurrentSync("shutdown"); err != nil {
				return r.current, err
			}
			return r.current, r.ctx.Err()
		}
	}

	if err := r.flushStagedCurrent(); err != nil {
		return r.current, err
	}
	return r.current, nil
}

func (r *nextSyncRunner) flushStagedCurrent() error {
	if r.stagedBlocks == 0 {
		return nil
	}
	states := r.takeCheckpointStates(r.current)
	cells := r.stateCells.beginCheckpoint()
	next, err := r.service.persistNextBlockCurrentState(r.current, &r.timing, states, cells)
	if err != nil {
		return err
	}
	r.current = next
	r.stagedBlocks = 0
	return nil
}

func (r *nextSyncRunner) flushStagedCurrentSync(reason string) error {
	if r.stagedBlocks == 0 {
		return nil
	}
	states := r.takeCheckpointStates(r.current)
	cells := r.stateCells.beginCheckpoint()
	next, err := r.service.persistNextBlockCurrentStateSync(r.current, &r.timing, reason, states, cells)
	if err != nil {
		return err
	}
	r.current = next
	r.stagedBlocks = 0
	return nil
}

func (r *nextSyncRunner) commitOne(item nextAppliedMaster, masterPipelineWait time.Duration, appliedQueue int) error {
	blockStarted := time.Now()
	applyBefore := r.timing.apply
	persistBefore := r.timing.persist
	checkpointsBefore := r.timing.checkpoints
	shardTargetBefore := r.shardTarget
	shardWallBefore := r.shardWall
	shardApplyBefore := r.shardApply
	shardAppliedBefore := r.shardApplied

	r.timing.blocks++
	r.timing.apply += item.applyTiming.total
	r.timing.persist += item.stageElapsed
	r.master = item.master
	r.shardPrefetchTargets += uint64(item.shardPrefetchTargets)

	nextCurrent, shardStats, err := r.service.currentStateForNextMasterState(r.ctx, r.current, item.master, item.shardTargets, r.shardResolver)
	if err != nil {
		return err
	}
	shardStats.targetParse += item.shardTargetParse
	resolverStats := r.takeShardResolverStats()
	shardStats.apply += resolverStats.applyElapsed
	shardStats.applied += resolverStats.blocksApplied
	shardStats.reused += resolverStats.blocksReused
	r.rememberCheckpointState(item.master)

	r.current = nextCurrent
	r.shardResolver.updateCurrent(r.current.Shards)
	r.service.publishLiveCurrentState(r.current)
	if r.service.liveState != nil {
		r.service.liveState.SetLiveCurrentState(r.current)
	}
	r.shardTarget += shardStats.targetParse
	r.shardWall += shardStats.wall
	r.shardApply += shardStats.apply
	r.shardApplied += uint64(shardStats.applied)
	r.shardReused += uint64(shardStats.reused)
	r.committed++
	r.stagedBlocks++

	if r.stagedBlocks >= nextBlockCatchUpCheckpointBlocks || r.reachedTarget() {
		states := r.takeCheckpointStates(r.current)
		cells := r.stateCells.beginCheckpoint()
		r.current, err = r.service.persistNextBlockCurrentState(r.current, &r.timing, states, cells)
		if err != nil {
			return err
		}
		r.stagedBlocks = 0
	}

	r.logShardCommit(item, shardStats, blockStarted, masterPipelineWait, appliedQueue, applyBefore, persistBefore, checkpointsBefore, shardTargetBefore, shardWallBefore, shardApplyBefore, shardAppliedBefore)
	r.logProgressIfNeeded(item)
	return nil
}

func (r *nextSyncRunner) takeShardResolverStats() shardStateResolverStats {
	stats := r.shardResolver.statsSnapshot()
	delta := shardStateResolverStats{
		applyElapsed:  stats.applyElapsed - r.shardResolverSeen.applyElapsed,
		blocksApplied: stats.blocksApplied - r.shardResolverSeen.blocksApplied,
		blocksReused:  stats.blocksReused - r.shardResolverSeen.blocksReused,
	}
	r.shardResolverSeen = stats
	return delta
}

func (r *nextSyncRunner) afterApplyShardState(ctx context.Context, state *storage.BlockState, downloaded p2p.DownloadedBlock, _ time.Duration) error {
	if err := r.service.stageAppliedBlockArtifact(ctx, downloaded); err != nil {
		return err
	}
	r.rememberCheckpointState(state)
	return nil
}

func (r *nextSyncRunner) rememberCheckpointState(state *storage.BlockState) {
	r.checkpointMu.Lock()
	r.checkpointStates.remember(state)
	r.checkpointMu.Unlock()
}

func (r *nextSyncRunner) takeCheckpointStates(current *storage.CurrentState) []*storage.BlockState {
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	return r.checkpointStates.takeWithCurrent(current)
}

func (r *nextSyncRunner) reachedTarget() bool {
	return r.mode == nextSyncToTarget && r.current.Masterchain.Block.SeqNo >= r.target.SeqNo
}

func (r *nextSyncRunner) shouldStopAfterCommit() bool {
	if r.reachedTarget() {
		return true
	}

	blockUTime := blockStateUtime(r.ctx, r.service.storage, &r.current.Masterchain)
	lagSeconds, ok := masterchainBlockLagSeconds(blockUTime, time.Now().Unix())
	if !ok || !shouldUseArchiveCatchUpByLag(lagSeconds) {
		return false
	}

	latest, err := r.latestTarget(r.current.Masterchain.Block.SeqNo)
	if err != nil {
		latest = r.current.Masterchain.Block
	}

	event := r.service.log.Info().
		Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("latest_masterchain", storage.FormatBlockRef(latest)).
		Int64("lag_seconds", lagSeconds).
		Int64("switch_to_archive_lag_seconds", nextToArchiveLagSeconds)
	if r.mode == nextSyncToTarget {
		event.Str("target", storage.FormatBlockRef(r.target))
	}
	event.Msg("stopping next-block pipeline because masterchain time is stale")
	return true
}

func (r *nextSyncRunner) latestTarget(currentSeqno uint32) (ton.BlockIDExt, error) {
	latest, err := r.service.knownMasterchainTarget(r.ctx, currentSeqno)
	if errors.Is(err, storage.ErrNotFound) {
		if r.mode == nextSyncToTarget {
			return r.target, nil
		}
		return r.current.Masterchain.Block, nil
	}
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if r.mode == nextSyncToTarget && latest.SeqNo < r.target.SeqNo {
		return r.target, nil
	}
	return latest, nil
}

func (r *nextSyncRunner) logMasterApplied(item nextAppliedMaster) {
	latest, err := r.latestTarget(item.master.Block.SeqNo)
	if err != nil {
		latest = item.master.Block
	}
	if latest.SeqNo < item.master.Block.SeqNo {
		latest = item.master.Block
	}

	blockUTime, lagSeconds, hasBlockTime := downloadedBlockTimeLag(item.block, time.Now())
	elapsed := item.downloadElapsed + item.applyTiming.total

	event := r.service.log.Debug().
		Str("block", item.block.BlockRef()).
		Str("latest_masterchain", storage.FormatBlockRef(latest)).
		Str("catchup_method", r.method).
		Str("download_source", item.downloadSource).
		Dur("download_elapsed", item.downloadElapsed).
		Dur("apply_elapsed", item.applyTiming.total).
		Dur("master_prepare_elapsed", item.applyTiming.prepare).
		Dur("master_consensus_elapsed", item.applyTiming.consensus).
		Dur("master_state_update_elapsed", item.applyTiming.stateUpdate).
		Dur("elapsed", elapsed)
	if hasBlockTime {
		event.Uint32("block_utime", blockUTime).Int64("lag_seconds", lagSeconds)
	}
	event.Msg("next-block masterchain head applied")
}

func (r *nextSyncRunner) logShardCommit(item nextAppliedMaster, shardStats nextShardClientApplyStats, blockStarted time.Time, masterPipelineWait time.Duration, appliedQueue int, applyBefore time.Duration, persistBefore time.Duration, checkpointsBefore uint32, shardTargetBefore time.Duration, shardWallBefore time.Duration, shardApplyBefore time.Duration, shardAppliedBefore uint64) {
	blockElapsed := time.Since(blockStarted)
	latest, err := r.latestTarget(r.current.Masterchain.Block.SeqNo)
	if err != nil {
		latest = r.current.Masterchain.Block
	}
	if latest.SeqNo < r.current.Masterchain.Block.SeqNo {
		latest = r.current.Masterchain.Block
	}
	lagBlocks := uint32(0)
	if latest.SeqNo > r.current.Masterchain.Block.SeqNo {
		lagBlocks = latest.SeqNo - r.current.Masterchain.Block.SeqNo
	}

	remaining := uint32(0)
	if r.mode == nextSyncToTarget && r.target.SeqNo > r.current.Masterchain.Block.SeqNo {
		remaining = r.target.SeqNo - r.current.Masterchain.Block.SeqNo
	}

	blockUTime, lagSeconds, hasBlockTime := downloadedBlockTimeLag(item.block, time.Now())
	r.lastBlockUTime = blockUTime
	r.lastLagSeconds = lagSeconds
	r.hasBlockTime = hasBlockTime

	event := r.service.log.Debug().
		Str("block", item.block.BlockRef()).
		Str("prev", storage.FormatBlockRef(item.prev)).
		Str("latest_masterchain", storage.FormatBlockRef(latest)).
		Uint32("lag_blocks", lagBlocks).
		Uint32("remaining", remaining).
		Str("catchup_method", r.method).
		Dur("master_pipeline_wait", masterPipelineWait).
		Int("master_applied_queue", appliedQueue).
		Dur("master_apply_elapsed", r.timing.apply-applyBefore).
		Dur("persist_elapsed", r.timing.persist-persistBefore).
		Dur("shard_state_elapsed", r.shardWall-shardWallBefore).
		Dur("shard_target_parse_elapsed", r.shardTarget-shardTargetBefore).
		Dur("shard_apply_elapsed", r.shardApply-shardApplyBefore).
		Uint64("shard_blocks_applied", r.shardApplied-shardAppliedBefore).
		Dur("elapsed", blockElapsed).
		Str("speed", formatBlockRate(1, blockElapsed)).
		Bool("checkpoint", r.timing.checkpoints > checkpointsBefore)
	if hasBlockTime {
		event.Uint32("block_utime", blockUTime).Int64("lag_seconds", lagSeconds)
	}
	if shardStats.resolverWait > 0 {
		event.
			Dur("shard_resolver_wait_elapsed", shardStats.resolverWait)
	}
	event.Msg("next-block shard-client state synced")
}

func (r *nextSyncRunner) logProgressIfNeeded(item nextAppliedMaster) {
	if time.Since(r.lastLog) < 5*time.Second && !r.reachedTarget() {
		return
	}

	now := time.Now()
	masterSeqno := r.master.Block.SeqNo
	shardClientSeqno := r.current.Masterchain.Block.SeqNo
	windowElapsed := now.Sub(r.timing.windowStarted)

	latest, err := r.latestTarget(shardClientSeqno)
	if err != nil {
		latest = r.current.Masterchain.Block
	}
	if latest.SeqNo < masterSeqno {
		latest = r.master.Block
	}
	shardClientLagBlocks := uint32(0)
	if latest.SeqNo > shardClientSeqno {
		shardClientLagBlocks = latest.SeqNo - shardClientSeqno
	}
	masterShardGapBlocks := uint32(0)
	if masterSeqno > shardClientSeqno {
		masterShardGapBlocks = masterSeqno - shardClientSeqno
	}
	masterLagSeconds := int64(0)
	if r.master.Parsed != nil {
		masterLagSeconds = now.Unix() - int64(r.master.Parsed.GenUTime)
	}
	shardLagSeconds := int64(0)
	if r.current.Masterchain.Parsed != nil {
		shardLagSeconds = now.Unix() - int64(r.current.Masterchain.Parsed.GenUTime)
	}

	event := r.service.log.Info().
		Str("masterchain_head", storage.FormatBlockRef(r.master.Block)).
		Str("shard_client", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("catchup_method", r.method).
		Uint32("pending_checkpoint_blocks", r.stagedBlocks).
		Uint32("shard_client_lag_blocks", shardClientLagBlocks).
		Uint32("master_shard_gap_blocks", masterShardGapBlocks).
		Uint64("shard_blocks_applied", r.shardApplied)
	if r.mode == nextSyncToTarget {
		event.Str("target", storage.FormatBlockRef(r.target))
	}
	if r.hasBlockTime {
		event.Uint32("block_utime", r.lastBlockUTime).
			Int64("master_lag_seconds", masterLagSeconds).
			Int64("shard_lag_seconds", shardLagSeconds)
	}
	event.Msg("next-block catch-up progress")

	r.service.log.Debug().
		Str("masterchain_head", storage.FormatBlockRef(r.master.Block)).
		Str("shard_client", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("latest_masterchain", storage.FormatBlockRef(latest)).
		Dur("window_elapsed", windowElapsed).
		Dur("download_wait_total", r.timing.downloadWait).
		Dur("apply_total", r.timing.apply).
		Dur("persist_total", r.timing.persist).
		Dur("shard_state_total", r.shardWall).
		Dur("shard_target_parse_total", r.shardTarget).
		Dur("shard_apply_total", r.shardApply).
		Dur("apply_avg", avgDuration(r.timing.apply, r.timing.blocks)).
		Dur("persist_avg", avgDuration(r.timing.persist, r.timing.blocks)).
		Uint32("checkpoints", r.timing.checkpoints).
		Msg("next-block shard-client catch-up progress")

	r.lastLog = now
	r.lastProgressMasterSeqno = masterSeqno
	r.lastProgressShardClientSeqno = shardClientSeqno
	r.lastProgressShardApplied = r.shardApplied
	r.lastProgressShardReused = r.shardReused
	r.lastProgressShardPrefetch = r.shardPrefetchTargets
	r.timing.reset(r.lastLog)
}
