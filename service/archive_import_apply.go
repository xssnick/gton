package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/archive"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type archiveShardBlockLoader struct {
	master ton.BlockIDExt
	blocks map[storage.BlockRootHash]PreparedBlock
	mu     *sync.Mutex
}

type shardClientArchiveWindow struct {
	startSeqno     uint32
	masterStats    *archive.ImportStats
	totalStats     *archive.ImportStats
	masterStates   map[uint32]*storage.BlockState
	masterBlocks   map[uint32]PreparedBlock
	masterSequence []PreparedBlock
	masterProofs   map[uint32]*masterchainConsensusProof
	archiveBlocks  map[storage.BlockRootHash]PreparedBlock
	archiveImports []*archiveImportResult
	// shardTargets carries the planner's per-master-seqno shard targets to the
	// runner (shared-immutable slices; written on the shard import task before
	// it completes, read by the runner after the window is emitted).
	shardTargets     map[uint32][]ton.BlockIDExt
	stateCells       *stateCellWindowCache
	appliedStates    appliedStateSet
	shardArchives    int
	splitDepth       uint32
	masterWait       time.Duration
	syncUntilReached bool

	masterApplyElapsed       time.Duration
	masterPrecheckElapsed    time.Duration
	masterPrepareElapsed     time.Duration
	masterConsensusElapsed   time.Duration
	masterStateUpdateElapsed time.Duration
	shardTargetElapsed       time.Duration
	shardApplyElapsed        time.Duration
	shardBlocksApplied       int
	shardBlocksReused        int
}

func (l archiveShardBlockLoader) load(_ context.Context, block ton.BlockIDExt) (PreparedBlock, error) {
	if block.SeqNo == 0 {
		return PreparedBlock{}, fmt.Errorf("zerostate %s is missing", storage.FormatBlockRef(block))
	}

	key := storage.BlockKey(block)
	l.mu.Lock()
	downloaded, ok := l.blocks[key]
	l.mu.Unlock()
	if !ok {
		return PreparedBlock{}, fmt.Errorf("load archive data/proof for shard block %s: %w", storage.FormatBlockRef(block), storage.ErrNotFound)
	}

	setShardBlockMasterchainRef(downloaded.Meta, l.master)
	return downloaded, nil
}

func (r *archiveCatchUpRun) startArchiveWindowShardImport(ctx context.Context, queue *archiveImportQueue, prepared archivePreparedMasterWindow, priority archiveImportPriority) *archiveWindowShardImportTask {
	task := newArchiveWindowShardImportTask()
	go func() {
		defer close(task.joined)
		task.complete(r.importArchiveWindowShards(ctx, queue, prepared, priority, task))
	}()
	return task
}

func (r *archiveCatchUpRun) importArchiveWindowShards(ctx context.Context, queue *archiveImportQueue, prepared archivePreparedMasterWindow, priority archiveImportPriority, task *archiveWindowShardImportTask) error {
	window := prepared.window
	task.setStage("split_depth")
	splitDepth, err := monitorMinSplitDepth(prepared.startMaster, 0)
	if err != nil {
		return fmt.Errorf("load archive split depth for %s: %w", storage.FormatBlockRef(prepared.startMaster.Block), err)
	}
	if splitDepth > maxArchiveMonitorSplitDepth {
		return fmt.Errorf("monitor split depth %d exceeds supported archive prefix fanout %d", splitDepth, maxArchiveMonitorSplitDepth)
	}
	window.splitDepth = splitDepth

	task.setStage("shard_targets")
	plans, inputs, err := r.archive.missingArchiveShardImportPlansForWindow(prepared.startMaster, window.masterStates, window.archiveBlocks)
	if err != nil {
		return err
	}
	window.splitDepth = inputs.splitDepth
	window.shardTargets = inputs.stateTargets
	window.shardArchives = len(plans)

	if len(plans) > 0 {
		task.setStage("shard_archives")
		importedFiles, err := r.downloadAndImportShardArchives(ctx, queue, window.startSeqno, plans, splitDepth, priority)
		if err != nil {
			return err
		}
		task.setStage("merge_imports")
		for _, imported := range importedFiles {
			mergeImportStats(window.totalStats, imported.stats, true)
			window.archiveImports = append(window.archiveImports, imported)
			for key, block := range imported.blocks {
				window.archiveBlocks[key] = block
			}
			imported.blocks = nil
		}
	}
	window.totalStats.ShardArchives = window.shardArchives

	r.archive.log.Debug().
		Uint32("archive_masterchain_seqno", window.startSeqno).
		Int64("archive_id", window.masterStats.ArchiveID).
		Str("peer", window.masterStats.Peer).
		Int("master_blocks", len(window.masterStates)).
		Int("prechecked_master_blocks", len(window.masterProofs)).
		Int("archive_blocks", len(window.archiveBlocks)).
		Int("shard_archives", window.shardArchives).
		Uint64("state_update_cells", window.totalStats.StateUpdateCells).
		Uint64("state_update_cell_bytes", window.totalStats.StateUpdateCellBytes).
		Uint32("monitor_split_depth", window.splitDepth).
		Dur("master_prefetch_wait", window.masterWait).
		Dur("master_precheck_elapsed", window.masterPrecheckElapsed).
		Int64("bytes", window.totalStats.Bytes).
		Dur("download_elapsed", window.totalStats.DownloadElapsed).
		Dur("import_elapsed", window.totalStats.ImportElapsed).
		Uint32("first_seqno", window.totalStats.FirstSeqno).
		Uint32("last_seqno", window.totalStats.LastSeqno).
		Uint32("masterchain_first_seqno", window.totalStats.MasterchainFirstSeqno).
		Uint32("masterchain_last_seqno", window.totalStats.MasterchainLastSeqno).
		Msg("archive shard-client window imported")
	return nil
}

func (r *archiveCatchUpRun) applyArchiveMasterBlocks(ctx context.Context, start *storage.BlockState, window *shardClientArchiveWindow) (*storage.BlockState, error) {
	master := start
	applier := window.stateCells
	if window.masterSequence == nil {
		sequence, err := archiveMasterBlockSequence(start.Block, r.target.SeqNo, window.startSeqno, window.masterBlocks)
		if err != nil {
			return nil, err
		}
		window.masterSequence = sequence
	}

	for idx := range window.masterSequence {
		downloaded := window.masterSequence[idx]
		if r.archive.preparedMasterBlockAfterSyncUntil(downloaded) {
			window.syncUntilReached = true
			break
		}

		proof := window.masterProofs[downloaded.ID.SeqNo]
		if proof == nil || !proof.block.Equals(&downloaded.ID) {
			var err error
			proof, err = prepareMasterchainConsensusProofBOC(downloaded.ID, downloaded.ProofBOC)
			if err != nil {
				return nil, fmt.Errorf("prepare archive master proof %s: %w", downloaded.BlockRef(), err)
			}
		}
		downloaded.consensus = proof

		next, timing, consensusElapsed, err := r.archive.masterTransitions.applyArchiveMasterBlock(ctx, master, &downloaded, proof, applier)
		window.masterApplyElapsed += timing.total
		window.masterPrepareElapsed += timing.prepare
		window.masterConsensusElapsed += consensusElapsed + timing.consensus
		window.masterStateUpdateElapsed += timing.stateUpdate
		if err != nil {
			return nil, fmt.Errorf("apply archive master block %s: %w", downloaded.BlockRef(), err)
		}

		master = next
		window.masterSequence[idx].releaseStateUpdatePayload()
		downloaded.releaseStateUpdatePayload()
		window.archiveBlocks[storage.BlockKey(downloaded.ID)] = downloaded
		window.masterStates[downloaded.ID.SeqNo] = master
		window.rememberAppliedArchiveState(master, downloaded, 0)
	}
	return master, nil
}

func (w *shardClientArchiveWindow) rememberAppliedArchiveState(state *storage.BlockState, block PreparedBlock, splitDepth uint32) {
	artifact, links := preparedBlockCheckpointArtifacts(block, splitDepth)
	w.appliedStates.rememberWithArtifacts(state, artifact, links)
}

func (r *archiveCatchUpRun) applyShardClientArchiveWindow(ctx context.Context, current *storage.CurrentState, window *shardClientArchiveWindow) (*storage.CurrentState, error) {
	applied := map[storage.BlockRootHash]*storage.BlockState{}
	next := storage.CloneCurrentState(current)
	applyStarted := time.Now()
	progressTicker := time.NewTicker(archiveCatchUpProgressInterval)
	defer progressTicker.Stop()

	seqno := current.ShardClientSeqno + 1
	for ; ; seqno++ {
		masterState := window.masterStates[seqno]
		if masterState == nil {
			break
		}

		targets, ok := window.shardTargets[seqno]
		if !ok {
			targetStarted := time.Now()
			var err error
			targets, err = state2.ShardBlocksFromMasterState(masterState)
			window.shardTargetElapsed += time.Since(targetStarted)
			if err != nil {
				return nil, fmt.Errorf("load shard blocks from archive master state %s: %w", storage.FormatBlockRef(masterState.Block), err)
			}
		}

		prevShards := next.Shards
		next = &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: seqno,
			Masterchain:      *storage.CloneBlockState(masterState),
			Shards:           make(map[storage.ShardKey]storage.BlockState, len(targets)),
		}

		shards, err := r.applyArchiveShardTargets(
			ctx,
			masterState,
			prevShards,
			applied,
			window.archiveBlocks,
			targets,
			window,
			applyStarted,
			progressTicker.C,
		)
		if err != nil {
			return nil, fmt.Errorf("apply archive shard blocks at masterchain seqno %d: %w", seqno, err)
		}
		for key, shard := range shards {
			next.Shards[key] = *storage.CloneBlockState(shard)
		}

		select {
		case now := <-progressTicker.C:
			r.logArchiveWindowApplyProgress(now, applyStarted, masterState, window, len(targets), len(targets), 0, 0)
		default:
		}
	}
	return next, nil
}

func (r *archiveCatchUpRun) applyArchiveShardTargets(
	ctx context.Context,
	masterState *storage.BlockState,
	current map[storage.ShardKey]storage.BlockState,
	applied map[storage.BlockRootHash]*storage.BlockState,
	blocks map[storage.BlockRootHash]PreparedBlock,
	targets []ton.BlockIDExt,
	window *shardClientArchiveWindow,
	applyStarted time.Time,
	progress <-chan time.Time,
) (map[storage.ShardKey]*storage.BlockState, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	master := masterState.Block

	applier := window.stateCells

	var appliedMu sync.Mutex
	var blockLoaderMu sync.Mutex
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: current,
		cache:   applied,
		loadState: func(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
			if state.Block.SeqNo == 0 {
				return r.archive.stateSync.ImportZeroState(ctx, state.Block, master)
			}
			return r.archive.loadBlockStateForApply(ctx, state)
		},
		loadBlock: archiveShardBlockLoader{
			master: master,
			blocks: blocks,
			mu:     &blockLoaderMu,
		}.load,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded PreparedBlock) (*storage.BlockState, error) {
			return r.archive.shardTransitions.applyArchiveShardBlock(ctx, target, previous, downloaded, applier, masterState)
		},
		afterApplyState: func(_ context.Context, state *storage.BlockState, downloaded PreparedBlock, _ time.Duration) error {
			setShardStateMasterchainRef(state, master)
			setShardBlockMasterchainRef(downloaded.Meta, master)

			blockLoaderMu.Lock()
			blocks[storage.BlockKey(downloaded.ID)] = downloaded
			blockLoaderMu.Unlock()

			appliedMu.Lock()
			defer appliedMu.Unlock()

			window.rememberAppliedArchiveState(state, downloaded, window.splitDepth)
			return nil
		},
	})
	workers := archiveShardApplyParallelism
	if len(targets) < workers {
		workers = len(targets)
	}

	type shardTargetResult struct {
		target ton.BlockIDExt
		state  *storage.BlockState
		err    error
	}

	jobs := make(chan ton.BlockIDExt)
	results := make(chan shardTargetResult, len(targets))
	applyCtx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range jobs {
				state, err := resolver.resolveWithContext(applyCtx, target)
				result := shardTargetResult{
					target: target,
					state:  state,
					err:    err,
				}
				select {
				case results <- result:
				case <-applyCtx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, target := range targets {
			select {
			case jobs <- target:
			case <-applyCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	shards := make(map[storage.ShardKey]*storage.BlockState, len(targets))
	var firstErr error
	completedTargets := 0
	for results != nil {
		select {
		case res, ok := <-results:
			if !ok {
				results = nil
				continue
			}

			completedTargets++
			if res.err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("apply archive shard block %s: %w", storage.FormatBlockRef(res.target), res.err)
					cancelApply()
				}
				continue
			}
			shards[storage.ShardKeyFromBlock(res.target)] = res.state
		case now := <-progress:
			stats := resolver.statsSnapshot()
			r.logArchiveWindowApplyProgress(
				now,
				applyStarted,
				masterState,
				window,
				completedTargets,
				len(targets),
				stats.blocksApplied,
				stats.blocksReused,
			)
		}
	}
	stats := resolver.statsSnapshot()
	window.shardApplyElapsed += stats.applyElapsed
	window.shardBlocksApplied += stats.blocksApplied
	window.shardBlocksReused += stats.blocksReused

	if firstErr != nil {
		return nil, firstErr
	}
	if len(shards) != len(targets) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("incomplete archive shard target apply got=%d want=%d", len(shards), len(targets))
	}
	return shards, nil
}

func (r *archiveCatchUpRun) logArchiveWindowApplyProgress(
	now time.Time,
	started time.Time,
	masterState *storage.BlockState,
	window *shardClientArchiveWindow,
	completedTargets int,
	totalTargets int,
	currentBlocksApplied int,
	currentBlocksReused int,
) {
	done := masterState.Block.SeqNo - r.startSeqno
	total := r.target.SeqNo - r.startSeqno

	r.archive.log.Info().
		Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("applying", storage.FormatBlockRef(masterState.Block)).
		Str("target", storage.FormatBlockRef(r.target)).
		Str("catchup_method", "archive_shard_client").
		Str("stage", "apply_shard_targets").
		Uint32("processed_masterchain_blocks", done).
		Uint32("total_masterchain_blocks", total).
		Str("progress", formatCatchUpProgress(done, total)).
		Str("eta", formatCatchUpETA(done, total, time.Since(r.started))).
		Uint32("window_start_seqno", window.startSeqno).
		Int("window_master_blocks", len(window.masterStates)).
		Int("completed_shard_targets", completedTargets).
		Int("total_shard_targets", totalTargets).
		Int("window_shard_blocks_applied", window.shardBlocksApplied+currentBlocksApplied).
		Int("window_shard_blocks_reused", window.shardBlocksReused+currentBlocksReused).
		Dur("window_apply_elapsed", now.Sub(started)).
		Msg("archive shard-client catch-up progress")
}

func (r *archiveCatchUpRun) persistArchiveCurrentState(current *storage.CurrentState, checkpointBlocks uint32, lockElapsed time.Duration, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache) (*storage.CurrentState, error) {
	storedCurrent := currentStateWithoutCells(current)
	started := time.Now()
	// Cells applied through the window caches were already streamed to celldb by
	// the state cell prewriter; when every checkpoint cache is fully enqueued the
	// persist only waits for the prewriter to reach the target and flushes.
	// Otherwise fall back to writing the checkpoint cells synchronously.
	checkpointCells := storage.StateCellRecords{}
	cellPrewriteTarget, prewritten := cells.prewriteTarget()
	if !prewritten {
		checkpointCells = cells.cells()
	}
	persisted, stages, err := r.archive.state.saveStateCheckpoint(r.ctx, storedCurrent, entries, checkpointCells, cellPrewriteTarget, 0)
	if err != nil {
		return nil, fmt.Errorf("persist archive current state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	cells.complete()

	event := r.archive.log.Debug().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Int("states", len(entries)).
		Int("shards", len(current.Shards)).
		Bool("cells_prewritten", prewritten).
		Dur("lock_wait", lockElapsed).
		Dur("elapsed", time.Since(started))
	for _, stage := range stages {
		event = event.Dur("stage_"+stage.Stage, stage.Duration)
	}
	event.Msg("archive shard-client checkpoint persisted")
	return persisted, nil
}
