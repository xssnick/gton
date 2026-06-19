package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type archiveMasterImportTask struct {
	startSeqno uint32
	cancel     context.CancelFunc
	done       chan archiveMasterImportResult
}

type archiveMasterImportResult struct {
	imported        *archiveImportResult
	masterSequence  []PreparedBlock
	consensusProofs map[uint32]*masterchainConsensusProof
	precheckElapsed time.Duration
	err             error
}

type archiveWindowPipeline struct {
	cancel   context.CancelFunc
	done     chan archiveWindowResult
	progress *archiveWindowPipelineProgress
}

type archiveWindowResult struct {
	window *shardClientArchiveWindow
	err    error
}

type archivePendingWindow struct {
	window *shardClientArchiveWindow
	shards *archiveWindowShardImportTask
}

type archivePreparedMasterWindow struct {
	window         *shardClientArchiveWindow
	startMaster    *storage.BlockState
	lastMaster     *storage.BlockState
	masterImport   *archiveImportResult
	nextMasterTask *archiveMasterImportTask
}

type archiveWindowShardImportTask struct {
	mu       sync.RWMutex
	done     chan error
	err      error
	stage    string
	finished bool
}

func (r *archiveCatchUpRunner) startShardClientArchiveWindowPipeline() *archiveWindowPipeline {
	pipelineCtx, cancel := context.WithCancel(r.ctx)
	progress := newArchiveWindowPipelineProgress()
	pipeline := &archiveWindowPipeline{
		cancel:   cancel,
		done:     make(chan archiveWindowResult, 1),
		progress: progress,
	}
	targetSeqno := r.target.SeqNo
	go r.runShardClientArchiveWindowPipeline(pipelineCtx, archivePipelineCurrent(r.current), targetSeqno, pipeline.done, progress)
	return pipeline
}

func (r *archiveCatchUpRunner) runShardClientArchiveWindowPipeline(ctx context.Context, current *storage.CurrentState, targetSeqno uint32, out chan<- archiveWindowResult, progress *archiveWindowPipelineProgress) {
	defer close(out)

	taskCtx, cancelTasks := context.WithCancel(ctx)
	defer cancelTasks()

	importQueue := r.startArchiveImportQueue(taskCtx)
	progress.setQueue(importQueue)
	planning := archivePipelineCurrent(current)
	emitted := archivePipelineCurrent(current)
	planningCellLoader := r.stateCells.loader()
	pending := make([]archivePendingWindow, 0, r.archivePendingWindowLimit())
	var planningErr error

	var masterTask *archiveMasterImportTask
	defer func() {
		if masterTask != nil {
			masterTask.cancel()
		}
	}()

	for planning.ShardClientSeqno < targetSeqno || len(pending) > 0 {
		for planningErr == nil && r.canScheduleArchiveWindow(planning, len(pending), targetSeqno) {
			planningProgress := progress
			if len(pending) > 0 {
				planningProgress = nil
				progress.setPending(pending, planning.ShardClientSeqno+1, "planning")
			}
			if planningProgress != nil {
				planningProgress.setPlanning(planning.ShardClientSeqno+1, "master_archive")
			}
			prepared, err := r.prepareArchiveMasterWindow(taskCtx, importQueue, planning, masterTask, targetSeqno, planningCellLoader, planningProgress)
			if err != nil {
				if len(pending) > 0 {
					if masterTask != nil {
						masterTask.cancel()
						masterTask = nil
					}
					planningErr = err
					progress.setPending(pending, planning.ShardClientSeqno+1, "draining")
					r.service.log.Info().
						Err(err).
						Uint32("current_seqno", emitted.ShardClientSeqno).
						Uint32("planning_seqno", planning.ShardClientSeqno+1).
						Int("pending_windows", len(pending)).
						Msg("archive lookahead failed, draining prepared windows before retry")
					break
				}

				cancelTasks()
				select {
				case out <- archiveWindowResult{err: err}:
				case <-ctx.Done():
				}
				return
			}
			masterTask = prepared.nextMasterTask

			if len(prepared.window.masterStates) == 0 {
				select {
				case out <- archiveWindowResult{window: prepared.window}:
				case <-ctx.Done():
				}
				return
			}

			nextPlanning, err := advanceArchivePipelineCurrent(planning, prepared.window)
			if err != nil {
				if masterTask != nil {
					masterTask.cancel()
					masterTask = nil
				}
				cancelTasks()
				select {
				case out <- archiveWindowResult{window: prepared.window, err: err}:
				case <-ctx.Done():
				}
				return
			}

			priority := archiveImportPriorityPrefetch
			if len(pending) < archiveHotLookaheadWindows {
				priority = archiveImportPriorityHot
			}
			pending = append(pending, archivePendingWindow{
				window: prepared.window,
				shards: r.startArchiveWindowShardImport(taskCtx, importQueue, prepared, priority),
			})
			planning = nextPlanning
			planningCellLoader = prepared.window.stateCells.loader()
			progress.setPending(pending, planning.ShardClientSeqno+1, "planning")
			if r.shouldEmitReadyArchiveWindow(pending) {
				break
			}
		}

		if len(pending) == 0 {
			if planningErr != nil {
				cancelTasks()
				select {
				case out <- archiveWindowResult{err: planningErr}:
				case <-ctx.Done():
				}
				return
			}

			progress.setPlanning(planning.ShardClientSeqno+1, "idle")
			return
		}

		next := pending[0]
		progress.setPending(pending, next.window.startSeqno, "shard_archives")
		if err := next.shards.wait(taskCtx); err != nil {
			cancelTasks()
			select {
			case out <- archiveWindowResult{window: next.window, err: err}:
			case <-ctx.Done():
			}
			return
		}

		nextEmitted, err := advanceArchivePipelineCurrent(emitted, next.window)
		if err != nil {
			cancelTasks()
			select {
			case out <- archiveWindowResult{window: next.window, err: err}:
			case <-ctx.Done():
			}
			return
		}

		next.shards.setStage("emit")
		progress.setPending(pending, next.window.startSeqno, "emit")
		select {
		case out <- archiveWindowResult{window: next.window}:
		case <-ctx.Done():
			return
		}
		emitted = nextEmitted
		pending = pending[1:]
		if planningErr != nil {
			progress.setPending(pending, planning.ShardClientSeqno+1, "draining")
		} else {
			progress.setPending(pending, planning.ShardClientSeqno+1, "planning")
		}
	}
}

func (r *archiveCatchUpRunner) archivePendingWindowLimit() int {
	limit := r.service.archiveCatchUpPrefetchWindows
	if limit <= 0 {
		return DefaultArchiveCatchUpPrefetchWindows
	}
	return limit
}

func (r *archiveCatchUpRunner) archiveReadyWindowBacklog() int {
	limit := r.archivePendingWindowLimit()
	if limit < archiveReadyWindowBacklog {
		return limit
	}
	return archiveReadyWindowBacklog
}

func (r *archiveCatchUpRunner) shouldEmitReadyArchiveWindow(pending []archivePendingWindow) bool {
	if len(pending) == 0 || pending[0].shards == nil || !pending[0].shards.ready() {
		return false
	}
	if pending[0].shards.err != nil {
		return true
	}
	return len(pending) >= r.archiveReadyWindowBacklog()
}

func (r *archiveCatchUpRunner) canScheduleArchiveWindow(planning *storage.CurrentState, pending int, targetSeqno uint32) bool {
	if planning.ShardClientSeqno >= targetSeqno {
		return false
	}
	if pending >= r.archivePendingWindowLimit() {
		return false
	}
	if lagSeconds, ok := archiveCurrentStateLagSeconds(planning, time.Now().Unix()); ok && shouldSwitchArchiveToNextByLag(lagSeconds) {
		return false
	}
	return true
}

func archiveCurrentStateLagSeconds(current *storage.CurrentState, nowUnix int64) (int64, bool) {
	if current == nil {
		return 0, false
	}
	if current.Masterchain.Parsed == nil || current.Masterchain.Parsed.GenUTime == 0 {
		return 0, false
	}
	return masterchainBlockLagSeconds(int64(current.Masterchain.Parsed.GenUTime), nowUnix)
}

func advanceArchivePipelineCurrent(current *storage.CurrentState, window *shardClientArchiveWindow) (*storage.CurrentState, error) {
	lastMaster := lastArchiveWindowMasterState(window)
	if lastMaster == nil {
		return nil, fmt.Errorf("archive window #%d did not advance masterchain", window.startSeqno)
	}

	return &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: lastMaster.Block.SeqNo,
		Masterchain:      *storage.CloneBlockState(lastMaster),
		Shards:           current.Shards,
	}, nil
}

func archivePipelineCurrent(current *storage.CurrentState) *storage.CurrentState {
	return &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: current.ShardClientSeqno,
		Masterchain:      *storage.CloneBlockState(&current.Masterchain),
		Shards:           current.Shards,
	}
}

func lastArchiveWindowMasterState(window *shardClientArchiveWindow) *storage.BlockState {
	var last *storage.BlockState
	for seqno, state := range window.masterStates {
		if state == nil {
			continue
		}
		if last == nil || seqno > last.Block.SeqNo {
			last = state
		}
	}
	return last
}

func (r *archiveCatchUpRunner) nextArchiveWindowWithProgress() (*shardClientArchiveWindow, error) {
	ticker := time.NewTicker(archiveCatchUpProgressInterval)
	defer ticker.Stop()
	r.startPipelineWaitProgress()

	for {
		select {
		case res, ok := <-r.pipeline.done:
			r.stopPipelineWaitProgress(time.Now())
			if !ok {
				return nil, fmt.Errorf("archive window pipeline stopped")
			}
			return res.window, res.err
		case <-ticker.C:
			if err := r.logProgress(); err != nil {
				return nil, err
			}
		case <-r.ctx.Done():
			r.stopPipelineWaitProgress(time.Now())
			r.pipeline.cancel()
			return nil, r.ctx.Err()
		}
	}
}

func (r *archiveCatchUpRunner) startArchiveMasterImport(ctx context.Context, queue *archiveImportQueue, start *storage.BlockState, targetSeqno uint32) *archiveMasterImportTask {
	taskCtx, cancel := context.WithCancel(ctx)
	startSeqno := start.Block.SeqNo + 1
	task := &archiveMasterImportTask{
		startSeqno: startSeqno,
		cancel:     cancel,
		done:       make(chan archiveMasterImportResult, 1),
	}
	go func() {
		result := archiveMasterImportResult{}
		splitDepth, err := monitorMinSplitDepth(start, 0)
		if err != nil {
			result.err = fmt.Errorf("load archive split depth for %s: %w", storage.FormatBlockRef(start.Block), err)
			task.done <- result
			return
		}
		downloaded, err := r.loadArchiveImport(taskCtx, queue, startSeqno, archive.ShardID{Workchain: -1, Shard: topShard}, splitDepth, archiveImportPriorityHot)
		if err != nil {
			result.err = fmt.Errorf("master archive #%d: %w", startSeqno, err)
			task.done <- result
			return
		}

		result.imported = downloaded.imported
		result.masterSequence, err = r.archiveMasterBlocksForWindow(start, downloaded.imported.blocks, startSeqno, targetSeqno)
		if err == nil {
			result.consensusProofs, result.precheckElapsed, err = r.precheckArchiveMasterConsensus(taskCtx, start, result.masterSequence)
		}
		result.err = err
		task.done <- result
	}()
	return task
}

func newArchiveWindowShardImportTask() *archiveWindowShardImportTask {
	return &archiveWindowShardImportTask{
		done:  make(chan error, 1),
		stage: "starting",
	}
}

func (t *archiveWindowShardImportTask) setStage(stage string) {
	if t == nil {
		return
	}

	t.mu.Lock()
	t.stage = stage
	t.mu.Unlock()
}

func (t *archiveWindowShardImportTask) stageSnapshot() string {
	if t == nil {
		return ""
	}

	t.mu.RLock()
	stage := t.stage
	t.mu.RUnlock()
	return stage
}

func (t *archiveWindowShardImportTask) finishedSnapshot() bool {
	if t == nil {
		return false
	}

	t.mu.RLock()
	finished := t.finished
	t.mu.RUnlock()
	return finished
}

func (t *archiveWindowShardImportTask) complete(err error) {
	if err != nil {
		t.finishStage("error")
	} else {
		t.finishStage("ready")
	}
	t.done <- err
}

func (t *archiveWindowShardImportTask) finishStage(stage string) {
	if t == nil {
		return
	}

	t.mu.Lock()
	t.stage = stage
	t.finished = true
	t.mu.Unlock()
}

func (r *archiveCatchUpRunner) archiveMasterBlocksForWindow(start *storage.BlockState, imported map[storage.BlockRootHash]PreparedBlock, startSeqno uint32, targetSeqno uint32) ([]PreparedBlock, error) {
	return archiveMasterBlockSequence(start, targetSeqno, startSeqno, archiveMasterBlockMap(imported))
}

func archiveMasterBlockMap(blocks map[storage.BlockRootHash]PreparedBlock) map[uint32]PreparedBlock {
	masterBlocks := make(map[uint32]PreparedBlock)
	for _, block := range blocks {
		if block.ID.Workchain == -1 && block.ID.Shard == topShard {
			masterBlocks[block.ID.SeqNo] = block
		}
	}
	return masterBlocks
}

func archiveMasterBlockSequence(start *storage.BlockState, targetSeqno uint32, startSeqno uint32, masterBlocks map[uint32]PreparedBlock) ([]PreparedBlock, error) {
	sequence := make([]PreparedBlock, 0, len(masterBlocks))
	for seqno := start.Block.SeqNo + 1; seqno != 0 && seqno <= targetSeqno; seqno++ {
		downloaded, ok := masterBlocks[seqno]
		if !ok {
			if seqno == start.Block.SeqNo+1 {
				return nil, fmt.Errorf("archive window #%d has no next masterchain block after %s", startSeqno, storage.FormatBlockRef(start.Block))
			}
			break
		}
		if downloaded.Meta == nil {
			return nil, fmt.Errorf("archive masterchain block %s is not prepared", storage.FormatBlockRef(downloaded.ID))
		}
		if seqno != startSeqno && downloaded.Meta.Has(storage.BlockMetaIsKeyBlock) {
			break
		}
		sequence = append(sequence, downloaded)
	}
	return sequence, nil
}

func (r *archiveCatchUpRunner) precheckArchiveMasterConsensus(ctx context.Context, start *storage.BlockState, blocks []PreparedBlock) (map[uint32]*masterchainConsensusProof, time.Duration, error) {
	if len(blocks) == 0 {
		return nil, 0, nil
	}
	if blocks[0].Meta != nil && blocks[0].Meta.Has(storage.BlockMetaIsKeyBlock) {
		return nil, 0, nil
	}

	started := time.Now()
	proofs := make([]*masterchainConsensusProof, len(blocks))
	validators := make([][]*tlb.ValidatorAddr, len(blocks))
	validatorsByKey := map[masterchainValidatorCacheKey][]*tlb.ValidatorAddr{}
	expectedPrev := start.Block

	for i, downloaded := range blocks {
		proof, err := prepareMasterchainConsensusProofBOC(downloaded.ID, downloaded.ProofBOC)
		if err != nil {
			return nil, time.Since(started), err
		}
		if proof.keyBlock {
			return nil, time.Since(started), nil
		}
		if proof.signaturePrepareErr != nil {
			return nil, time.Since(started), proof.signaturePrepareErr
		}
		if !proof.prevRef.Equals(&expectedPrev) {
			return nil, time.Since(started), fmt.Errorf("%w: block=%s prev=%s expected=%s", errMasterchainPrevMismatch, storage.FormatBlockRef(downloaded.ID), storage.FormatBlockRef(proof.prevRef), storage.FormatBlockRef(expectedPrev))
		}

		key := proof.validatorCacheKey
		validatorSet, ok := validatorsByKey[key]
		if !ok {
			var err error
			validatorSet, err = r.service.masterchainValidatorsForConsensus(start, downloaded.ID, key)
			if err != nil {
				return nil, time.Since(started), err
			}
			validatorsByKey[key] = validatorSet
		}

		proofs[i] = proof
		validators[i] = validatorSet
		expectedPrev = downloaded.ID
	}

	if err := precheckMasterchainSignatures(ctx, blocks, proofs, validators); err != nil {
		return nil, time.Since(started), err
	}

	checked := make(map[uint32]*masterchainConsensusProof, len(proofs))
	for i, proof := range proofs {
		proof.signaturesChecked = true
		checked[blocks[i].ID.SeqNo] = proof
	}
	return checked, time.Since(started), nil
}

func precheckMasterchainSignatures(ctx context.Context, blocks []PreparedBlock, proofs []*masterchainConsensusProof, validators [][]*tlb.ValidatorAddr) error {
	workers := archiveMasterConsensusPrecheckParallelism
	if len(blocks) < workers {
		workers = len(blocks)
	}

	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if checkCtx.Err() != nil {
					return
				}

				proof := proofs[idx]
				err := blockproof.CheckPreparedMasterchainSignaturesWithValidators(blocks[idx].ID, proof.proofSignatures, validators[idx])
				if err != nil {
					select {
					case errs <- fmt.Errorf("precheck masterchain consensus for %s: %w", storage.FormatBlockRef(blocks[idx].ID), err):
						cancel()
					default:
					}
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for idx := range blocks {
			select {
			case jobs <- idx:
			case <-checkCtx.Done():
				return
			}
		}
	}()

	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return checkCtx.Err()
	}
}

func (r *archiveCatchUpRunner) loadArchiveImport(ctx context.Context, queue *archiveImportQueue, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority) (archiveImportDownload, error) {
	key := archiveImportCacheKey{masterchainSeqno: masterchainSeqno, shard: shard, splitDepth: splitDepth}
	load := func(loadCtx context.Context) (*archiveImportResult, error) {
		if queue != nil {
			return queue.importArchive(loadCtx, masterchainSeqno, shard, splitDepth, priority)
		}
		downloaded, err := r.downloadArchiveFile(loadCtx, masterchainSeqno, shard, false)
		if err != nil {
			return nil, err
		}
		return r.prepareArchiveDownload(loadCtx, masterchainSeqno, shard, splitDepth, downloaded)
	}
	if r.importCache == nil {
		imported, err := load(ctx)
		return archiveImportDownload{imported: imported}, err
	}

	downloaded, err := r.importCache.load(ctx, key, load)
	if err != nil {
		return archiveImportDownload{}, err
	}
	if downloaded.cached {
		r.service.log.Debug().
			Uint32("masterchain_seqno", masterchainSeqno).
			Int32("workchain", shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
			Uint32("split_depth", splitDepth).
			Int("blocks", len(downloaded.imported.blocks)).
			Msg("using preloaded archive import")
	}
	return downloaded, nil
}

func (r *archiveCatchUpRunner) downloadArchiveFile(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, hedge bool) (*archive.Downloaded, error) {
	if err := r.waitArchiveDownloadBackpressure(ctx); err != nil {
		return nil, err
	}

	session := r.archiveSession
	if session == nil {
		if r.service == nil || r.service.node == nil {
			return nil, fmt.Errorf("archive session is not initialized")
		}
		session = r.service.node.BeginArchiveSession()
		defer session.Close()
	}

	downloaded, err := session.DownloadArchive(ctx, masterchainSeqno, shard, p2p.ArchiveDownloadOptions{Hedge: hedge})
	if err != nil {
		return nil, fmt.Errorf("download archive #%d %s: %w", masterchainSeqno, shard.String(), err)
	}
	return downloaded, nil
}

func (r *archiveCatchUpRunner) prepareArchiveDownload(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, downloaded *archive.Downloaded) (*archiveImportResult, error) {
	imported, err := r.service.importArchiveBlocks(ctx, downloaded, splitDepth)
	if err != nil {
		return nil, fmt.Errorf("import archive #%d %s: %w", masterchainSeqno, shard.String(), err)
	}
	return imported, nil
}

func (t *archiveMasterImportTask) wait(ctx context.Context) (archiveMasterImportResult, time.Duration, error) {
	started := time.Now()
	select {
	case res := <-t.done:
		return res, time.Since(started), res.err
	case <-ctx.Done():
		t.cancel()
		return archiveMasterImportResult{}, time.Since(started), ctx.Err()
	}
}

func (t *archiveWindowShardImportTask) wait(ctx context.Context) error {
	if t.done == nil {
		return t.err
	}

	select {
	case err := <-t.done:
		t.done = nil
		t.err = err
		if err != nil {
			t.finishStage("error")
		} else {
			t.finishStage("ready")
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *archiveWindowShardImportTask) ready() bool {
	if t == nil || t.done == nil {
		return true
	}

	select {
	case err := <-t.done:
		t.done = nil
		t.err = err
		if err != nil {
			t.finishStage("error")
		} else {
			t.finishStage("ready")
		}
		return true
	default:
		return false
	}
}

func (r *archiveCatchUpRunner) prepareArchiveMasterWindow(ctx context.Context, queue *archiveImportQueue, current *storage.CurrentState, masterTask *archiveMasterImportTask, targetSeqno uint32, baseCellLoader cell.LazyCellLoader, progress *archiveWindowPipelineProgress) (archivePreparedMasterWindow, error) {
	startSeqno := current.ShardClientSeqno + 1
	progress.setPlanning(startSeqno, "master_state")
	startMaster, err := r.service.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return archivePreparedMasterWindow{}, fmt.Errorf("load archive start masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

	progress.setPlanning(startSeqno, "master_archive")
	if masterTask == nil || masterTask.startSeqno != startSeqno {
		if masterTask != nil {
			masterTask.cancel()
		}
		masterTask = r.startArchiveMasterImport(ctx, queue, startMaster, targetSeqno)
	}

	masterResult, masterWait, err := masterTask.wait(ctx)
	if err != nil {
		return archivePreparedMasterWindow{}, err
	}
	masterImport := masterResult.imported
	if masterImport == nil {
		return archivePreparedMasterWindow{}, fmt.Errorf("archive master import #%d returned no data", startSeqno)
	}

	window := &shardClientArchiveWindow{
		startSeqno:            startSeqno,
		masterStats:           masterImport.stats,
		totalStats:            cloneImportStats(masterImport.stats),
		masterStates:          map[uint32]*storage.BlockState{},
		masterBlocks:          archiveMasterBlockMap(masterImport.blocks),
		masterSequence:        masterResult.masterSequence,
		masterProofs:          masterResult.consensusProofs,
		archiveBlocks:         masterImport.blocks,
		archiveImports:        []*archiveImportResult{masterImport},
		stateCells:            newArchiveStateCellOverlay(baseCellLoader),
		masterWait:            masterWait,
		masterPrecheckElapsed: masterResult.precheckElapsed,
	}
	masterImport.blocks = nil

	progress.setPlanning(startSeqno, "master_apply")
	lastMaster, err := r.applyArchiveMasterBlocks(ctx, startMaster, window)
	if err != nil {
		return archivePreparedMasterWindow{}, err
	}
	window.masterBlocks = nil
	if len(window.masterStates) == 0 {
		return archivePreparedMasterWindow{
			window:       window,
			startMaster:  startMaster,
			lastMaster:   lastMaster,
			masterImport: masterImport,
		}, nil
	}

	var nextMasterTask *archiveMasterImportTask
	if lastMaster.Block.SeqNo < targetSeqno {
		nextMasterTask = r.startArchiveMasterImport(ctx, queue, lastMaster, targetSeqno)
	}

	return archivePreparedMasterWindow{
		window:         window,
		startMaster:    startMaster,
		lastMaster:     lastMaster,
		masterImport:   masterImport,
		nextMasterTask: nextMasterTask,
	}, nil
}
