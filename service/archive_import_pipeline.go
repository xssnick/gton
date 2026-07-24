package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type archiveMasterImportTask struct {
	startSeqno uint32
	cancel     context.CancelFunc
	done       chan archiveMasterImportResult
}

type archiveMasterDownloadTask struct {
	startSeqno uint32
	cancel     context.CancelFunc
	done       chan archiveMasterDownloadResult

	mu      sync.Mutex
	claimed bool
}

type archiveMasterDownloadResult struct {
	downloaded *archive.Downloaded
	err        error
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
	pending := make([]archivePendingWindow, 0, r.service.archiveCatchUpPrefetchWindows)
	var planningErr error
	planningStopped := false

	var masterTask *archiveMasterImportTask
	defer func() {
		if masterTask != nil {
			masterTask.cancel()
		}
	}()

	for (!planningStopped && planning.ShardClientSeqno < targetSeqno) || len(pending) > 0 {
		for !planningStopped && planningErr == nil && r.canScheduleArchiveWindow(planning, len(pending), targetSeqno) {
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
				if masterTask != nil {
					masterTask.cancel()
					masterTask = nil
				}
				if !prepared.window.syncUntilReached {
					err = fmt.Errorf("archive window #%d did not advance masterchain", prepared.window.startSeqno)
					prepared.window.releaseImportedData()
					if len(pending) > 0 {
						planningErr = err
						progress.setPending(pending, planning.ShardClientSeqno+1, "draining")
						break
					}

					cancelTasks()
					select {
					case out <- archiveWindowResult{err: err}:
					case <-ctx.Done():
					}
					return
				}

				pending = append(pending, archivePendingWindow{
					window: prepared.window,
					shards: newCompletedArchiveWindowShardImportTask(),
				})
				planningStopped = true
				progress.setPending(pending, planning.ShardClientSeqno+1, "draining")
				break
			}

			nextPlanning := advanceArchivePipelineCurrent(planning, prepared.window)

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
			if prepared.window.syncUntilReached {
				planningStopped = true
				if masterTask != nil {
					masterTask.cancel()
					masterTask = nil
				}
				progress.setPending(pending, planning.ShardClientSeqno+1, "draining")
				break
			}
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

		nextEmitted := emitted
		if len(next.window.masterStates) > 0 {
			nextEmitted = advanceArchivePipelineCurrent(emitted, next.window)
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
		if planningErr != nil || planningStopped {
			progress.setPending(pending, planning.ShardClientSeqno+1, "draining")
		} else {
			progress.setPending(pending, planning.ShardClientSeqno+1, "planning")
		}
	}
}

func (r *archiveCatchUpRunner) archiveReadyWindowBacklog() int {
	limit := r.service.archiveCatchUpPrefetchWindows
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
	if pending >= r.service.archiveCatchUpPrefetchWindows {
		return false
	}
	if planning.Masterchain.Parsed != nil &&
		planning.Masterchain.Parsed.GenUTime != 0 &&
		shouldSwitchArchiveToNextByLag(time.Now().Unix()-int64(planning.Masterchain.Parsed.GenUTime)) {
		return false
	}
	return true
}

func advanceArchivePipelineCurrent(current *storage.CurrentState, window *shardClientArchiveWindow) *storage.CurrentState {
	lastMaster := lastArchiveWindowMasterState(window)
	return &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: lastMaster.Block.SeqNo,
		Masterchain:      *storage.CloneBlockState(lastMaster),
		Shards:           current.Shards,
	}
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

func (r *archiveCatchUpRunner) startArchiveMasterImport(ctx context.Context, queue *archiveImportQueue, start *storage.BlockState, targetSeqno uint32, prefetched *archiveMasterDownloadTask) *archiveMasterImportTask {
	taskCtx, cancel := context.WithCancel(ctx)
	startSeqno := start.Block.SeqNo + 1
	task := &archiveMasterImportTask{
		startSeqno: startSeqno,
		cancel:     cancel,
		done:       make(chan archiveMasterImportResult, 1),
	}
	go func() {
		defer cancel()
		if prefetched != nil {
			defer prefetched.discard()
		}
		splitDepth, err := monitorMinSplitDepth(start, 0)
		if err != nil {
			task.done <- archiveMasterImportResult{
				err: fmt.Errorf("load archive split depth for %s: %w", storage.FormatBlockRef(start.Block), err),
			}
			return
		}
		masterShard := archive.ShardID{Workchain: -1, Shard: topShard}
		cacheKey := archiveImportCacheKey{masterchainSeqno: startSeqno, shard: masterShard, splitDepth: splitDepth}
		for retry := 0; ; retry++ {
			result := archiveMasterImportResult{}
			downloaded, loadErr := r.loadArchiveImport(taskCtx, queue, startSeqno, masterShard, splitDepth, archiveImportPriorityHot, prefetched)
			prefetched = nil

			rejectReason := p2p.ArchivePeerRejectImportFailed
			err = loadErr
			if err == nil {
				result.imported = downloaded.imported
				result.masterSequence, err = archiveMasterBlockSequence(
					start,
					targetSeqno,
					startSeqno,
					archiveMasterBlockMap(result.imported.blocks),
				)
				if err != nil {
					rejectReason = p2p.ArchivePeerRejectImportIncomplete
				} else {
					precheckSequence := result.masterSequence
					for idx, block := range precheckSequence {
						if r.service.preparedMasterBlockAfterSyncUntil(block) {
							precheckSequence = precheckSequence[:idx]
							break
						}
					}
					result.consensusProofs, result.precheckElapsed, err = r.precheckArchiveMasterConsensus(taskCtx, start, precheckSequence)
				}
			}
			if err == nil {
				task.done <- result
				return
			}

			if taskCtx.Err() != nil {
				task.done <- archiveMasterImportResult{err: taskCtx.Err()}
				return
			}

			if loadErr == nil {
				r.importCache.drop(cacheKey)
			}

			var peer string
			var archiveID int64
			var peerErr *archiveImportPeerError
			if errors.As(err, &peerErr) {
				peer = peerErr.peer
				archiveID = peerErr.archiveID
			} else if result.imported != nil && result.imported.stats != nil {
				peer = result.imported.stats.Peer
				archiveID = result.imported.stats.ArchiveID
			}
			if peer != "" {
				r.rejectArchiveImportPeer(masterShard, peer, archiveID, rejectReason, err)
			}

			if retry < archiveImportPeerRetries {
				continue
			}

			task.done <- archiveMasterImportResult{err: fmt.Errorf("master archive #%d: %w", startSeqno, err)}
			return
		}
	}()
	return task
}

func (r *archiveCatchUpRunner) startArchiveMasterDownload(ctx context.Context, startSeqno uint32) *archiveMasterDownloadTask {
	taskCtx, cancel := context.WithCancel(ctx)
	task := &archiveMasterDownloadTask{
		startSeqno: startSeqno,
		cancel:     cancel,
		done:       make(chan archiveMasterDownloadResult),
	}
	go func() {
		defer cancel()
		downloaded, err := r.downloadArchiveFile(taskCtx, startSeqno, archive.ShardID{Workchain: -1, Shard: topShard}, false)
		task.complete(taskCtx, archiveMasterDownloadResult{downloaded: downloaded, err: err})
	}()
	return task
}

func (t *archiveMasterDownloadTask) complete(ctx context.Context, result archiveMasterDownloadResult) {
	select {
	case t.done <- result:
	case <-ctx.Done():
		releaseArchiveMasterDownloadResult(result)
	}
}

func (t *archiveMasterDownloadTask) take(ctx context.Context) (*archive.Downloaded, error) {
	if !t.claim() {
		return nil, fmt.Errorf("master archive download #%d was already consumed", t.startSeqno)
	}

	select {
	case result := <-t.done:
		if result.err != nil {
			releaseArchiveMasterDownloadResult(result)
			return nil, result.err
		}
		return result.downloaded, result.err
	case <-ctx.Done():
		t.cancel()
		return nil, ctx.Err()
	}
}

func (t *archiveMasterDownloadTask) discard() {
	if !t.claim() {
		return
	}
	t.cancel()
}

func (t *archiveMasterDownloadTask) claim() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.claimed {
		return false
	}
	t.claimed = true
	return true
}

func releaseArchiveMasterDownloadResult(result archiveMasterDownloadResult) {
	if result.downloaded != nil {
		result.downloaded.Data = nil
	}
}

func newArchiveWindowShardImportTask() *archiveWindowShardImportTask {
	return &archiveWindowShardImportTask{
		done:  make(chan error, 1),
		stage: "starting",
	}
}

func newCompletedArchiveWindowShardImportTask() *archiveWindowShardImportTask {
	return &archiveWindowShardImportTask{
		stage:    "ready",
		finished: true,
	}
}

func (t *archiveWindowShardImportTask) setStage(stage string) {
	t.mu.Lock()
	t.stage = stage
	t.mu.Unlock()
}

func (t *archiveWindowShardImportTask) stageSnapshot() string {
	t.mu.RLock()
	stage := t.stage
	t.mu.RUnlock()
	return stage
}

func (t *archiveWindowShardImportTask) finishedSnapshot() bool {
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
	t.mu.Lock()
	t.stage = stage
	t.finished = true
	t.mu.Unlock()
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

func nextArchiveMasterDownloadSeqno(sequence []PreparedBlock, targetSeqno uint32) (uint32, bool) {
	if len(sequence) == 0 {
		return 0, false
	}
	lastSeqno := sequence[len(sequence)-1].ID.SeqNo
	if lastSeqno >= targetSeqno {
		return 0, false
	}
	return lastSeqno + 1, true
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
	if blocks[0].Meta.Has(storage.BlockMetaIsKeyBlock) {
		return nil, 0, nil
	}

	started := time.Now()
	proofs := make([]*masterchainConsensusProof, len(blocks))
	validators := make([]*blockproof.PreparedValidatorSet, len(blocks))
	validatorsByKey := map[masterchainValidatorCacheKey]*blockproof.PreparedValidatorSet{}
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

func precheckMasterchainSignatures(ctx context.Context, blocks []PreparedBlock, proofs []*masterchainConsensusProof, validators []*blockproof.PreparedValidatorSet) error {
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
				err := blockproof.CheckPreparedMasterchainSignatures(blocks[idx].ID, proof.proofSignatures, validators[idx])
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

func (r *archiveCatchUpRunner) loadArchiveImport(ctx context.Context, queue *archiveImportQueue, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority, prefetched *archiveMasterDownloadTask) (archiveImportDownload, error) {
	defer func() {
		if prefetched != nil {
			prefetched.discard()
		}
	}()

	key := archiveImportCacheKey{masterchainSeqno: masterchainSeqno, shard: shard, splitDepth: splitDepth}
	load := func(loadCtx context.Context) (*archiveImportResult, error) {
		if prefetched != nil {
			task := prefetched
			prefetched = nil
			if task.startSeqno != masterchainSeqno {
				task.discard()
				return nil, fmt.Errorf("prefetched master archive starts at %d, want %d", task.startSeqno, masterchainSeqno)
			}
			downloaded, err := task.take(loadCtx)
			if err == nil {
				return r.prepareDownloadedArchive(loadCtx, masterchainSeqno, shard, splitDepth, downloaded)
			}
			if loadCtx.Err() != nil {
				return nil, loadCtx.Err()
			}
			r.service.log.Debug().
				Err(err).
				Uint32("masterchain_seqno", masterchainSeqno).
				Msg("prefetched master archive failed, downloading normally")
		}
		if queue != nil {
			return queue.importArchive(loadCtx, masterchainSeqno, shard, splitDepth, priority)
		}
		downloaded, err := r.downloadArchiveFile(loadCtx, masterchainSeqno, shard, false)
		if err != nil {
			return nil, err
		}
		return r.prepareDownloadedArchive(loadCtx, masterchainSeqno, shard, splitDepth, downloaded)
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
	if downloaded.imported != nil {
		downloaded.imported.cacheKey = key
	}
	return downloaded, nil
}

func (r *archiveCatchUpRunner) prepareDownloadedArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, downloaded *archive.Downloaded) (*archiveImportResult, error) {
	peer := downloaded.Peer
	archiveID := downloaded.ArchiveID
	imported, err := r.prepareArchiveDownload(ctx, masterchainSeqno, shard, splitDepth, downloaded)
	downloaded.Data = nil
	if err != nil && peer != "" {
		return nil, &archiveImportPeerError{
			peer:      peer,
			archiveID: archiveID,
			err:       err,
		}
	}
	return imported, err
}

func (r *archiveCatchUpRunner) downloadArchiveFile(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, hedge bool) (*archive.Downloaded, error) {
	if err := r.downloadGate.wait(ctx); err != nil {
		return nil, err
	}

	downloaded, err := r.archiveSession.DownloadArchive(ctx, masterchainSeqno, shard, p2p.ArchiveDownloadOptions{Hedge: hedge})
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
	if t.done == nil {
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
	if progress != nil {
		progress.setPlanning(startSeqno, "master_state")
	}
	startMaster, err := r.service.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return archivePreparedMasterWindow{}, fmt.Errorf("load archive start masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

	if progress != nil {
		progress.setPlanning(startSeqno, "master_archive")
	}
	if masterTask == nil || masterTask.startSeqno != startSeqno {
		if masterTask != nil {
			masterTask.cancel()
		}
		masterTask = r.startArchiveMasterImport(ctx, queue, startMaster, targetSeqno, nil)
	}

	masterResult, masterWait, err := masterTask.wait(ctx)
	if err != nil {
		return archivePreparedMasterWindow{}, err
	}
	masterImport := masterResult.imported

	windowStateCells := newStateCellWindowCache(baseCellLoader, &r.service.lazyCellLoads)
	windowStateCells.setPrewriter(r.service.stateCellPrewrite)
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
		stateCells:            windowStateCells,
		masterWait:            masterWait,
		masterPrecheckElapsed: masterResult.precheckElapsed,
	}
	masterImport.blocks = nil

	reachesSyncUntil := false
	for _, block := range masterResult.masterSequence {
		if r.service.preparedMasterBlockAfterSyncUntil(block) {
			reachesSyncUntil = true
			break
		}
	}
	var nextMasterDownload *archiveMasterDownloadTask
	if nextSeqno, ok := nextArchiveMasterDownloadSeqno(masterResult.masterSequence, targetSeqno); ok && !reachesSyncUntil {
		nextMasterDownload = r.startArchiveMasterDownload(ctx, nextSeqno)
	}
	defer func() {
		if nextMasterDownload != nil {
			nextMasterDownload.discard()
		}
	}()

	if progress != nil {
		progress.setPlanning(startSeqno, "master_apply")
	}
	lastMaster, err := r.applyArchiveMasterBlocks(ctx, startMaster, window)
	if err != nil {
		r.importCache.drop(masterImport.cacheKey)
		if stats := masterImport.stats; stats != nil && stats.Peer != "" {
			r.rejectArchiveImportPeer(
				archive.ShardID{Workchain: -1, Shard: topShard},
				stats.Peer,
				stats.ArchiveID,
				p2p.ArchivePeerRejectImportFailed,
				err,
			)
		}
		window.releaseImportedData()
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
	if !window.syncUntilReached && lastMaster.Block.SeqNo < targetSeqno {
		var prefetched *archiveMasterDownloadTask
		if nextMasterDownload != nil && nextMasterDownload.startSeqno == lastMaster.Block.SeqNo+1 {
			prefetched = nextMasterDownload
			nextMasterDownload = nil
		}
		nextMasterTask = r.startArchiveMasterImport(ctx, queue, lastMaster, targetSeqno, prefetched)
	}

	return archivePreparedMasterWindow{
		window:         window,
		startMaster:    startMaster,
		lastMaster:     lastMaster,
		masterImport:   masterImport,
		nextMasterTask: nextMasterTask,
	}, nil
}
