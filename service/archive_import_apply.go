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
	startSeqno       uint32
	masterStats      *archive.ImportStats
	totalStats       *archive.ImportStats
	masterStates     map[uint32]*storage.BlockState
	masterBlocks     map[uint32]PreparedBlock
	masterSequence   []PreparedBlock
	masterProofs     map[uint32]*masterchainConsensusProof
	archiveBlocks    map[storage.BlockRootHash]PreparedBlock
	archiveImports   []*archiveImportResult
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
	downloaded, ok := l.block(key)
	if !ok {
		return PreparedBlock{}, fmt.Errorf("no archive data/proof for shard block %s", storage.FormatBlockRef(block))
	}
	setPreparedShardMasterchainRef(&downloaded, l.master)
	return downloaded, nil
}

func (l archiveShardBlockLoader) block(key storage.BlockRootHash) (PreparedBlock, bool) {
	if l.mu == nil {
		downloaded, ok := l.blocks[key]
		return downloaded, ok
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	downloaded, ok := l.blocks[key]
	return downloaded, ok
}

func (r *archiveCatchUpRunner) startArchiveWindowShardImport(ctx context.Context, queue *archiveImportQueue, prepared archivePreparedMasterWindow, priority archiveImportPriority) *archiveWindowShardImportTask {
	task := newArchiveWindowShardImportTask()
	go func() {
		task.complete(r.importArchiveWindowShards(ctx, queue, prepared, priority, task))
	}()
	return task
}

func (r *archiveCatchUpRunner) importArchiveWindowShards(ctx context.Context, queue *archiveImportQueue, prepared archivePreparedMasterWindow, priority archiveImportPriority, task *archiveWindowShardImportTask) error {
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
	plans, splitDepth, err := r.service.missingArchiveShardImportPlansForWindow(prepared.startMaster, window.masterStates, window.archiveBlocks)
	if err != nil {
		return err
	}
	window.splitDepth = splitDepth
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

	r.service.log.Debug().
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

func (r *archiveCatchUpRunner) applyArchiveMasterBlocks(ctx context.Context, start *storage.BlockState, window *shardClientArchiveWindow) (*storage.BlockState, error) {
	master := start
	applier := window.stateCells.metered(nil)
	if window.masterSequence == nil {
		sequence, err := archiveMasterBlockSequence(start, r.target.SeqNo, window.startSeqno, window.masterBlocks)
		if err != nil {
			return nil, err
		}
		window.masterSequence = sequence
	}

	for idx := range window.masterSequence {
		downloaded := window.masterSequence[idx]
		if r.service.preparedMasterBlockAfterSyncUntil(downloaded) {
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

		consensusStarted := time.Now()
		checked, err := r.service.checkMasterchainBlockConsensusWithProof(master, proof)
		window.masterConsensusElapsed += time.Since(consensusStarted)
		if err != nil {
			return nil, fmt.Errorf("validate archive master consensus %s: %w", downloaded.BlockRef(), err)
		}
		downloaded.consensusChecked = checked

		next, timing, err := r.service.applyMasterchainTransition(ctx, master, downloaded, checked, applier, &blockApplyHookMeta{})
		window.masterApplyElapsed += timing.total
		window.masterPrepareElapsed += timing.prepare
		window.masterConsensusElapsed += timing.consensus
		window.masterStateUpdateElapsed += timing.stateUpdate
		if err != nil {
			return nil, fmt.Errorf("apply archive master block %s: %w", downloaded.BlockRef(), err)
		}

		if err = r.service.updateMasterDependentCachesForKeyBlock(next, &downloaded); err != nil {
			return nil, err
		}

		r.service.publishLiveBlockArtifacts(downloaded, next, liveBlockPublishOptions{availabilityOnly: true})
		r.service.rememberAppliedMasterchainState(next)
		r.service.rememberSeenMasterchainBlock(next.Block)
		r.service.rememberMasterState(ctx, next, &downloaded, nil)

		master = next
		window.masterSequence[idx].releaseStateUpdatePayload()
		downloaded.releaseStateUpdatePayload()
		window.archiveBlocks[storage.BlockKey(downloaded.ID)] = downloaded
		window.masterStates[downloaded.ID.SeqNo] = master
		if err = window.rememberAppliedArchiveState(master, downloaded, 0); err != nil {
			return nil, fmt.Errorf("remember archive master checkpoint state %s: %w", downloaded.BlockRef(), err)
		}
	}
	return master, nil
}

func (w *shardClientArchiveWindow) rememberAppliedArchiveState(state *storage.BlockState, block PreparedBlock, splitDepth uint32) error {
	artifact, links, err := preparedBlockCheckpointArtifacts(block, splitDepth)
	if err != nil {
		return err
	}
	w.appliedStates.rememberWithArtifacts(state, artifact, links)
	return nil
}

func (r *archiveCatchUpRunner) applyShardClientArchiveWindow(ctx context.Context, current *storage.CurrentState, window *shardClientArchiveWindow) (*storage.CurrentState, error) {
	applied := map[storage.BlockRootHash]*storage.BlockState{}
	next := storage.CloneCurrentState(current)

	seqno := current.ShardClientSeqno + 1
	for ; ; seqno++ {
		masterState := window.masterStates[seqno]
		if masterState == nil {
			break
		}

		targetStarted := time.Now()
		targets, err := state2.ShardBlocksFromMasterState(masterState)
		window.shardTargetElapsed += time.Since(targetStarted)
		if err != nil {
			return nil, fmt.Errorf("load shard blocks from archive master state %s: %w", storage.FormatBlockRef(masterState.Block), err)
		}

		prevShards := next.Shards
		next = &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: seqno,
			Masterchain:      *storage.CloneBlockState(masterState),
			Shards:           make(map[storage.ShardKey]storage.BlockState, len(targets)),
		}

		shards, err := r.applyArchiveShardTargets(ctx, masterState, prevShards, applied, window.archiveBlocks, targets, window)
		if err != nil {
			return nil, fmt.Errorf("apply archive shard blocks at masterchain seqno %d: %w", seqno, err)
		}
		for key, shard := range shards {
			next.Shards[key] = *storage.CloneBlockState(shard)
		}
	}
	return next, nil
}

func (r *archiveCatchUpRunner) applyArchiveShardTargets(ctx context.Context, masterState *storage.BlockState, current map[storage.ShardKey]storage.BlockState, applied map[storage.BlockRootHash]*storage.BlockState, blocks map[storage.BlockRootHash]PreparedBlock, targets []ton.BlockIDExt, window *shardClientArchiveWindow) (map[storage.ShardKey]*storage.BlockState, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	master := masterState.Block

	applier := window.stateCells.metered(nil)

	var appliedMu sync.Mutex
	var blockLoaderMu sync.Mutex
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: current,
		cache:   applied,
		loadState: func(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
			if state.Block.SeqNo == 0 && r.service.stateSync != nil {
				return r.service.stateSync.ImportZeroState(ctx, state.Block, master)
			}
			return r.service.loadBlockStateForApply(ctx, state)
		},
		loadBlock: archiveShardBlockLoader{
			master: master,
			blocks: blocks,
			mu:     &blockLoaderMu,
		}.load,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded PreparedBlock) (*storage.BlockState, error) {
			return r.service.applyResolvedShardBlock(ctx, target, previous, downloaded, applier, &blockApplyHookMeta{
				InclusionMasterRef:   &master,
				InclusionMasterState: masterState.Cell,
			})
		},
		afterApplyState: func(_ context.Context, state *storage.BlockState, downloaded PreparedBlock, _ time.Duration) error {
			setShardStateMasterchainRef(state, master)
			setPreparedShardMasterchainRef(&downloaded, master)

			blockLoaderMu.Lock()
			blocks[storage.BlockKey(downloaded.ID)] = downloaded
			blockLoaderMu.Unlock()

			appliedMu.Lock()
			defer appliedMu.Unlock()

			if err := window.rememberAppliedArchiveState(state, downloaded, window.splitDepth); err != nil {
				return fmt.Errorf("remember archive shard checkpoint state %s: %w", downloaded.BlockRef(), err)
			}
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
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("apply archive shard block %s: %w", storage.FormatBlockRef(res.target), res.err)
				cancelApply()
			}
			continue
		}
		shards[storage.ShardKeyFromBlock(res.target)] = res.state
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

func (r *archiveCatchUpRunner) persistArchiveCurrentState(current *storage.CurrentState, checkpointBlocks uint32, lockElapsed time.Duration, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache) (*storage.CurrentState, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("archive current state has no block states")
	}

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
	persisted, stages, err := r.service.saveStateCheckpoint(r.ctx, storedCurrent, entries, checkpointCells, cellPrewriteTarget, 0)
	if err != nil {
		return nil, fmt.Errorf("persist archive current state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	cells.complete()
	r.service.markLiveCheckpointStatesFlushed(entries)

	event := r.service.log.Debug().
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
