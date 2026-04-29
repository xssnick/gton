package service

import (
	"context"
	"errors"
	"flexserver/service/archive"
	"flexserver/service/p2p"
	state2 "flexserver/service/state"
	"flexserver/service/storage"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveCatchUpProgressInterval = 5 * time.Second

	DefaultArchiveCatchUpCheckpointBlocks = 2000
	DefaultArchiveCatchUpCheckpointPeriod = 2 * time.Minute
	DefaultArchiveCatchUpPrefetchWindows  = 12

	archiveShardApplyParallelism              = 8
	archiveCatchUpMaxAdaptiveCheckpointBlocks = 8000
	archiveCatchUpCheckpointSlowThreshold     = time.Second
	archiveCatchUpCheckpointFastThreshold     = 400 * time.Millisecond
)

type archiveImportResult struct {
	stats  *archive.ImportStats
	blocks map[string]p2p.DownloadedBlock
}

type archiveImportCacheKey struct {
	masterchainSeqno uint32
	shard            archive.ShardID
}

type archiveImportWaiter struct {
	done   chan struct{}
	result *archiveImportResult
	err    error
}

type archiveImportCache struct {
	mu       sync.Mutex
	entries  map[archiveImportCacheKey]*archiveImportResult
	waiters  map[archiveImportCacheKey]*archiveImportWaiter
	hitCount uint64
}

type archiveMasterImportTask struct {
	startSeqno uint32
	cancel     context.CancelFunc
	done       chan archiveMasterImportResult
}

type archiveMasterImportResult struct {
	imported *archiveImportResult
	err      error
}

type archiveWindowPipeline struct {
	cancel context.CancelFunc
	done   chan archiveWindowResult
}

type archiveWindowResult struct {
	window *shardClientArchiveWindow
	err    error
}

type archiveCheckpointTask struct {
	done chan archiveCheckpointResult
}

type archiveCheckpointResult struct {
	persisted        *storage.CurrentState
	checkpointBlocks uint32
	elapsed          time.Duration
	err              error
}

type archiveShardApplyTask struct {
	done  chan struct{}
	state *storage.BlockState
	err   error
}

type archiveShardApplyContext struct {
	service *Service
	ctx     context.Context
	master  ton.BlockIDExt
	current map[storage.ShardKey]storage.BlockState
	applied map[string]*storage.BlockState
	blocks  map[string]p2p.DownloadedBlock

	mu    sync.Mutex
	tasks map[string]*archiveShardApplyTask

	shardApplyElapsed  time.Duration
	shardBlocksApplied int
	shardBlocksReused  int
}

type shardClientArchiveWindow struct {
	startSeqno    uint32
	masterStats   *archive.ImportStats
	totalStats    *archive.ImportStats
	masterStates  map[uint32]*storage.BlockState
	masterBlocks  map[uint32]p2p.DownloadedBlock
	archiveBlocks map[string]p2p.DownloadedBlock
	shardArchives int
	splitDepth    uint32
	masterWait    time.Duration

	masterApplyElapsed time.Duration
	shardTargetElapsed time.Duration
	shardApplyElapsed  time.Duration
	shardBlocksApplied int
	shardBlocksReused  int
}

func newArchiveImportCache() *archiveImportCache {
	return &archiveImportCache{
		entries: map[archiveImportCacheKey]*archiveImportResult{},
		waiters: map[archiveImportCacheKey]*archiveImportWaiter{},
	}
}

func (c *archiveImportCache) load(ctx context.Context, key archiveImportCacheKey, load func(context.Context) (*archiveImportResult, error)) (*archiveImportResult, bool, error) {
	if c == nil {
		result, err := load(ctx)
		return result, false, err
	}

	for {
		result, cached, retry, err := c.loadOnce(ctx, key, load)
		if !retry {
			return result, cached, err
		}
	}
}

func (c *archiveImportCache) loadOnce(ctx context.Context, key archiveImportCacheKey, load func(context.Context) (*archiveImportResult, error)) (*archiveImportResult, bool, bool, error) {
	c.mu.Lock()
	if result := c.entries[key]; result != nil {
		c.hitCount++
		c.mu.Unlock()
		return cloneArchiveImportResult(result), true, false, nil
	}
	if waiter := c.waiters[key]; waiter != nil {
		c.mu.Unlock()
		select {
		case <-waiter.done:
			if waiter.err != nil {
				if errors.Is(waiter.err, context.Canceled) && ctx.Err() == nil {
					return nil, false, true, nil
				}
				return nil, false, false, waiter.err
			}
			return cloneArchiveImportResult(waiter.result), true, false, nil
		case <-ctx.Done():
			return nil, false, false, ctx.Err()
		}
	}

	waiter := &archiveImportWaiter{done: make(chan struct{})}
	c.waiters[key] = waiter
	c.mu.Unlock()

	result, err := load(ctx)

	c.mu.Lock()
	if err == nil && result != nil {
		c.entries[key] = cloneArchiveImportResult(result)
	}
	waiter.result = cloneArchiveImportResult(result)
	waiter.err = err
	delete(c.waiters, key)
	close(waiter.done)
	c.mu.Unlock()

	return result, false, false, err
}

func (c *archiveImportCache) dropBefore(masterchainSeqno uint32) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		if key.masterchainSeqno < masterchainSeqno {
			delete(c.entries, key)
		}
	}
}

func (c *archiveImportCache) stats() (int, uint64) {
	if c == nil {
		return 0, 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.hitCount
}

func (s *Service) startArchiveCheckpoint(ctx context.Context, current *storage.CurrentState, checkpointBlocks uint32) *archiveCheckpointTask {
	task := &archiveCheckpointTask{
		done: make(chan archiveCheckpointResult, 1),
	}
	snapshot := storage.CloneCurrentState(current)
	go func() {
		started := time.Now()
		persisted, err := s.persistArchiveCurrentState(ctx, snapshot, checkpointBlocks)
		task.done <- archiveCheckpointResult{
			persisted:        persisted,
			checkpointBlocks: checkpointBlocks,
			elapsed:          time.Since(started),
			err:              err,
		}
	}()
	return task
}

func (s *Service) adaptArchiveCheckpointBlocks(current uint32, elapsed time.Duration) uint32 {
	if current == 0 {
		current = s.archiveCatchUpCheckpointBlocks
	}

	base := s.archiveCatchUpCheckpointBlocks
	if base == 0 {
		base = DefaultArchiveCatchUpCheckpointBlocks
	}

	maxBlocks := uint32(archiveCatchUpMaxAdaptiveCheckpointBlocks)
	if maxBlocks < base {
		maxBlocks = base
	}

	next := current
	if elapsed >= archiveCatchUpCheckpointSlowThreshold && current < maxBlocks {
		next = current * 2
		if next > maxBlocks {
			next = maxBlocks
		}
	} else if elapsed <= archiveCatchUpCheckpointFastThreshold && current > base {
		next = current / 2
		if next < base {
			next = base
		}
	}
	return next
}

func newArchiveShardApplyContext(ctx context.Context, service *Service, master ton.BlockIDExt, current map[storage.ShardKey]storage.BlockState, applied map[string]*storage.BlockState, blocks map[string]p2p.DownloadedBlock) *archiveShardApplyContext {
	return &archiveShardApplyContext{
		service: service,
		ctx:     ctx,
		master:  master,
		current: current,
		applied: applied,
		blocks:  blocks,
		tasks:   map[string]*archiveShardApplyTask{},
	}
}

func (a *archiveShardApplyContext) apply(block ton.BlockIDExt) (*storage.BlockState, error) {
	return a.resolve(block)
}

func (a *archiveShardApplyContext) resolve(block ton.BlockIDExt) (*storage.BlockState, error) {
	key := storage.BlockKey(block)

	a.mu.Lock()
	if state := a.applied[key]; state != nil {
		a.shardBlocksReused++
		a.mu.Unlock()
		return state, nil
	}
	if task := a.tasks[key]; task != nil {
		a.mu.Unlock()
		select {
		case <-task.done:
			if task.err != nil {
				return nil, task.err
			}
			a.mu.Lock()
			a.shardBlocksReused++
			a.mu.Unlock()
			return task.state, nil
		case <-a.ctx.Done():
			return nil, a.ctx.Err()
		}
	}

	current, isCurrent := a.current[storage.ShardKeyFromBlock(block)]
	if isCurrent && !current.Block.Equals(&block) {
		isCurrent = false
	}

	task := &archiveShardApplyTask{done: make(chan struct{})}
	a.tasks[key] = task
	a.mu.Unlock()

	state, err := a.resolveOwned(block, isCurrent)

	a.mu.Lock()
	if err == nil {
		a.applied[key] = state
	}
	task.state = state
	task.err = err
	close(task.done)
	a.mu.Unlock()
	return state, err
}

func (a *archiveShardApplyContext) resolveOwned(block ton.BlockIDExt, isCurrent bool) (*storage.BlockState, error) {
	if isCurrent {
		state, err := a.service.loadBlockStateForApply(a.ctx, a.current[storage.ShardKeyFromBlock(block)])
		if err != nil {
			return nil, fmt.Errorf("load current shard state %s: %w", storage.FormatBlockRef(block), err)
		}

		a.mu.Lock()
		a.shardBlocksReused++
		a.mu.Unlock()
		return state, nil
	}

	if block.SeqNo == 0 {
		return nil, fmt.Errorf("zerostate %s is missing", storage.FormatBlockRef(block))
	}

	downloaded, ok := a.blocks[storage.BlockKey(block)]
	if !ok {
		return nil, fmt.Errorf("no archive data/proof for shard block %s", storage.FormatBlockRef(block))
	}
	if downloaded.Meta == nil {
		return nil, fmt.Errorf("archive shard block %s is missing metadata", downloaded.BlockRef())
	}
	if downloaded.Meta.MasterchainRef != nil && downloaded.Meta.MasterchainRef.SeqNo > a.master.SeqNo {
		return nil, fmt.Errorf("archive shard block %s references future masterchain block %s", downloaded.BlockRef(), storage.FormatBlockRef(*downloaded.Meta.MasterchainRef))
	}

	prevRefs := downloaded.Meta.PrevRefs
	if len(prevRefs) == 0 {
		return nil, fmt.Errorf("archive shard block %s has no previous refs", downloaded.BlockRef())
	}

	previous := make([]*storage.BlockState, len(prevRefs))
	if len(prevRefs) == 1 && prevRefs[0].Workchain == block.Workchain && prevRefs[0].Shard == block.Shard {
		prev, err := a.apply(prevRefs[0])
		if err != nil {
			return nil, err
		}
		previous[0] = prev
	} else {
		for i, prevRef := range prevRefs {
			prev, err := a.apply(prevRef)
			if err != nil {
				return nil, err
			}
			previous[i] = prev
		}
	}

	applyStarted := time.Now()
	next, err := ApplyBlockWithPreviousStates(previous, downloaded)
	applyElapsed := time.Since(applyStarted)
	a.mu.Lock()
	a.shardApplyElapsed += applyElapsed
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}

	a.mu.Lock()
	a.shardBlocksApplied++
	a.mu.Unlock()
	return next, nil
}

func (a *archiveShardApplyContext) addWindowStats(window *shardClientArchiveWindow) {
	a.mu.Lock()
	defer a.mu.Unlock()

	window.shardApplyElapsed += a.shardApplyElapsed
	window.shardBlocksApplied += a.shardBlocksApplied
	window.shardBlocksReused += a.shardBlocksReused
}

func (s *Service) catchUpShardClientFromArchives(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*storage.CurrentState, error) {
	current = storage.CloneCurrentState(current)
	if current.ShardClientSeqno == 0 {
		current.ShardClientSeqno = current.Masterchain.Block.SeqNo
	}
	if current.Masterchain.Block.SeqNo != current.ShardClientSeqno {
		return nil, fmt.Errorf("current masterchain seqno %d differs from shard client seqno %d", current.Masterchain.Block.SeqNo, current.ShardClientSeqno)
	}

	started := time.Now()
	startSeqno := current.ShardClientSeqno
	lastProgress := started
	lastProgressSeqno := startSeqno
	lastCheckpoint := started
	lastCheckpointSeqno := current.ShardClientSeqno
	checkpointBlocksTarget := s.archiveCatchUpCheckpointBlocks
	var checkpointTask *archiveCheckpointTask
	var shardBlocksApplied uint64
	var shardBlocksReused uint64
	var lastProgressShardBlocksApplied uint64
	var lastProgressShardBlocksReused uint64

	s.log.Info().
		Str("from", storage.FormatBlockRef(current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(target)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("total_masterchain_blocks", target.SeqNo-current.ShardClientSeqno).
		Msg("starting archive shard-client catch-up")

	importCache := newArchiveImportCache()
	pipeline := s.startShardClientArchiveWindowPipeline(ctx, current, target, importCache)
	defer func() {
		pipeline.cancel()
	}()

	completeCheckpoint := func(res archiveCheckpointResult) error {
		checkpointTask = nil
		if res.err != nil {
			return res.err
		}
		if res.persisted == nil {
			return fmt.Errorf("archive checkpoint returned empty current state")
		}

		previousTarget := checkpointBlocksTarget
		checkpointBlocksTarget = s.adaptArchiveCheckpointBlocks(checkpointBlocksTarget, res.elapsed)
		if checkpointBlocksTarget != previousTarget {
			s.log.Debug().
				Uint32("previous_checkpoint_blocks", previousTarget).
				Uint32("checkpoint_blocks", checkpointBlocksTarget).
				Dur("checkpoint_elapsed", res.elapsed).
				Msg("adjusted archive checkpoint interval")
		}

		lastCheckpoint = time.Now()
		lastCheckpointSeqno = res.persisted.ShardClientSeqno
		importCache.dropBefore(lastCheckpointSeqno + 1)
		return nil
	}

	pollCheckpoint := func(wait bool) error {
		if checkpointTask == nil {
			return nil
		}

		if wait {
			select {
			case res := <-checkpointTask.done:
				return completeCheckpoint(res)
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		select {
		case res := <-checkpointTask.done:
			return completeCheckpoint(res)
		default:
			return nil
		}
	}

	startCheckpoint := func() {
		if checkpointTask != nil {
			return
		}

		checkpointBlocks := current.ShardClientSeqno - lastCheckpointSeqno
		checkpointTask = s.startArchiveCheckpoint(ctx, current, checkpointBlocks)
		s.log.Debug().
			Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Uint32("shard_client_seqno", current.ShardClientSeqno).
			Uint32("checkpoint_blocks", checkpointBlocks).
			Uint32("checkpoint_target_blocks", checkpointBlocksTarget).
			Msg("started archive shard-client checkpoint")
	}

	persistProgressBeforeRetry := func(err error) error {
		if checkpointTask != nil {
			if waitErr := pollCheckpoint(true); waitErr != nil {
				return waitErr
			}
		}
		if err == nil || current.ShardClientSeqno <= lastCheckpointSeqno || errors.Is(err, context.Canceled) {
			return nil
		}

		checkpointBlocks := current.ShardClientSeqno - lastCheckpointSeqno
		persisted, persistErr := s.persistArchiveCurrentState(ctx, current, checkpointBlocks)
		if persistErr != nil {
			return fmt.Errorf("persist archive retry checkpoint: %w", persistErr)
		}

		s.log.Info().
			Err(err).
			Str("masterchain", storage.FormatBlockRef(persisted.Masterchain.Block)).
			Uint32("shard_client_seqno", persisted.ShardClientSeqno).
			Uint32("checkpoint_blocks", checkpointBlocks).
			Msg("persisted archive shard-client progress before retry")

		lastCheckpoint = time.Now()
		lastCheckpointSeqno = persisted.ShardClientSeqno
		checkpointBlocksTarget = s.archiveCatchUpCheckpointBlocks
		importCache.dropBefore(current.ShardClientSeqno + 1)
		return nil
	}

	returnWithProgress := func(err error) error {
		if persistErr := persistProgressBeforeRetry(err); persistErr != nil {
			return errors.Join(err, persistErr)
		}
		return err
	}

	restartPipeline := func(err error) error {
		if persistErr := persistProgressBeforeRetry(err); persistErr != nil {
			return errors.Join(err, persistErr)
		}

		pipeline.cancel()
		entries, hits := importCache.stats()
		s.log.Debug().
			Err(err).
			Uint32("current_seqno", current.ShardClientSeqno).
			Int("preloaded_archives", entries).
			Uint64("preload_cache_hits", hits).
			Msg("retrying archive shard-client preload from current state")

		if err = waitRetry(ctx, time.Second); err != nil {
			return err
		}
		pipeline = s.startShardClientArchiveWindowPipeline(ctx, current, target, importCache)
		return nil
	}

	for current.ShardClientSeqno < target.SeqNo {
		before := current.ShardClientSeqno
		window, err := pipeline.next(ctx)
		if err != nil {
			if isArchiveCatchUpRetryError(err) {
				if err = restartPipeline(err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, returnWithProgress(err)
		}
		if window.startSeqno != current.ShardClientSeqno+1 {
			return nil, fmt.Errorf("archive pipeline returned window #%d after current seqno %d", window.startSeqno, current.ShardClientSeqno)
		}
		if len(window.masterStates) == 0 {
			return nil, fmt.Errorf("archive window #%d did not provide next masterchain blocks", window.startSeqno)
		}

		applyStarted := time.Now()
		next, err := s.applyShardClientArchiveWindow(ctx, current, window)
		if err != nil {
			if isArchiveCatchUpRetryError(err) {
				if err = restartPipeline(err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, returnWithProgress(err)
		}
		s.log.Debug().
			Uint32("start_seqno", window.startSeqno).
			Int("master_blocks", len(window.masterStates)).
			Int("archive_blocks", len(window.archiveBlocks)).
			Int("shard_archives", window.shardArchives).
			Int("shard_blocks_applied", window.shardBlocksApplied).
			Int("shard_blocks_reused", window.shardBlocksReused).
			Dur("elapsed", time.Since(applyStarted)).
			Dur("master_apply_elapsed", window.masterApplyElapsed).
			Dur("shard_target_parse_elapsed", window.shardTargetElapsed).
			Dur("shard_apply_elapsed", window.shardApplyElapsed).
			Msg("archive shard-client window applied")

		if next.ShardClientSeqno <= before {
			return nil, fmt.Errorf("archive window #%d did not advance shard client seqno %d", window.startSeqno, before)
		}
		current = next
		importCache.dropBefore(current.ShardClientSeqno + 1)
		shardBlocksApplied += uint64(window.shardBlocksApplied)
		shardBlocksReused += uint64(window.shardBlocksReused)

		if err = pollCheckpoint(false); err != nil {
			return nil, err
		}
		if s.shouldPersistArchiveCatchUpCheckpoint(current.ShardClientSeqno, target.SeqNo, lastCheckpointSeqno, lastCheckpoint, checkpointBlocksTarget) {
			startCheckpoint()
		}

		if checkpointTask != nil && current.ShardClientSeqno >= target.SeqNo {
			if err = pollCheckpoint(true); err != nil {
				return nil, err
			}
		}

		if time.Since(lastProgress) >= archiveCatchUpProgressInterval || current.ShardClientSeqno >= target.SeqNo {
			now := time.Now()
			done := current.ShardClientSeqno - startSeqno
			total := target.SeqNo - startSeqno
			windowBlocks := current.ShardClientSeqno - lastProgressSeqno
			windowShardBlocksApplied := shardBlocksApplied - lastProgressShardBlocksApplied
			windowShardBlocksReused := shardBlocksReused - lastProgressShardBlocksReused
			s.log.Info().
				Str("current", storage.FormatBlockRef(current.Masterchain.Block)).
				Str("target", storage.FormatBlockRef(target)).
				Uint32("persisted_masterchain_seqno", lastCheckpointSeqno).
				Uint32("pending_checkpoint_blocks", current.ShardClientSeqno-lastCheckpointSeqno).
				Uint32("processed_masterchain_blocks", done).
				Uint64("applied_shard_blocks", shardBlocksApplied).
				Uint64("reused_shard_blocks", shardBlocksReused).
				Uint32("total_masterchain_blocks", total).
				Uint32("remaining", target.SeqNo-current.ShardClientSeqno).
				Str("progress", formatCatchUpProgress(done, total)).
				Str("speed", formatBlockRate(done, time.Since(started))).
				Str("window_speed", formatBlockRate(windowBlocks, now.Sub(lastProgress))).
				Str("shard_apply_speed", formatBlockRate64(windowShardBlocksApplied, now.Sub(lastProgress))).
				Str("shard_seen_speed", formatBlockRate64(windowShardBlocksApplied+windowShardBlocksReused, now.Sub(lastProgress))).
				Str("eta", formatCatchUpETA(done, total, time.Since(started))).
				Msg("archive shard-client catch-up progress")
			lastProgress = now
			lastProgressSeqno = current.ShardClientSeqno
			lastProgressShardBlocksApplied = shardBlocksApplied
			lastProgressShardBlocksReused = shardBlocksReused
		}
	}

	if checkpointTask != nil {
		if err := pollCheckpoint(true); err != nil {
			return nil, err
		}
	}
	if current.ShardClientSeqno > lastCheckpointSeqno {
		persisted, err := s.persistArchiveCurrentState(ctx, current, current.ShardClientSeqno-lastCheckpointSeqno)
		if err != nil {
			return nil, err
		}
		lastCheckpointSeqno = persisted.ShardClientSeqno
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Int("shards", len(current.Shards)).
		Msg("archive shard-client catch-up completed")
	return current, nil
}

func isArchiveCatchUpRetryError(err error) bool {
	return errors.Is(err, archive.ErrNotAvailable) || isExpectedRetryError(err)
}

func (s *Service) shouldPersistArchiveCatchUpCheckpoint(seqno uint32, targetSeqno uint32, lastCheckpointSeqno uint32, lastCheckpoint time.Time, checkpointBlocks uint32) bool {
	if seqno <= lastCheckpointSeqno {
		return false
	}
	if seqno >= targetSeqno {
		return true
	}
	if checkpointBlocks == 0 {
		checkpointBlocks = s.archiveCatchUpCheckpointBlocks
	}
	if seqno-lastCheckpointSeqno >= checkpointBlocks {
		return true
	}
	return time.Since(lastCheckpoint) >= s.archiveCatchUpCheckpointPeriod
}

func (s *Service) startShardClientArchiveWindowPipeline(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt, importCache *archiveImportCache) *archiveWindowPipeline {
	pipelineCtx, cancel := context.WithCancel(ctx)
	buffer := s.archiveCatchUpPrefetchWindows
	if buffer < 1 {
		buffer = 1
	}

	pipeline := &archiveWindowPipeline{
		cancel: cancel,
		done:   make(chan archiveWindowResult, buffer),
	}
	go s.runShardClientArchiveWindowPipeline(pipelineCtx, archivePipelineCurrent(current), target, importCache, pipeline.done)
	return pipeline
}

func (s *Service) runShardClientArchiveWindowPipeline(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt, importCache *archiveImportCache, out chan<- archiveWindowResult) {
	defer close(out)

	var masterImport *archiveMasterImportTask
	defer func() {
		if masterImport != nil {
			masterImport.cancel()
		}
	}()

	for current.ShardClientSeqno < target.SeqNo {
		if masterImport == nil {
			masterImport = s.startArchiveMasterImport(ctx, current.ShardClientSeqno+1, importCache)
		}

		window, nextMasterImport, err := s.importShardClientArchiveWindow(ctx, current, target, masterImport, importCache)
		masterImport = nextMasterImport
		if err == nil {
			current, err = advanceArchivePipelineCurrent(current, window)
		}

		result := archiveWindowResult{window: window, err: err}
		select {
		case out <- result:
		case <-ctx.Done():
			if nextMasterImport != nil {
				nextMasterImport.cancel()
			}
			return
		}
		if err != nil {
			return
		}
	}
}

func advanceArchivePipelineCurrent(current *storage.CurrentState, window *shardClientArchiveWindow) (*storage.CurrentState, error) {
	if window == nil {
		return nil, fmt.Errorf("archive window is nil")
	}

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

func (p *archiveWindowPipeline) next(ctx context.Context) (*shardClientArchiveWindow, error) {
	select {
	case res, ok := <-p.done:
		if !ok {
			return nil, fmt.Errorf("archive window pipeline stopped")
		}
		return res.window, res.err
	case <-ctx.Done():
		p.cancel()
		return nil, ctx.Err()
	}
}

func (s *Service) startArchiveMasterImport(ctx context.Context, startSeqno uint32, importCache *archiveImportCache) *archiveMasterImportTask {
	taskCtx, cancel := context.WithCancel(ctx)
	task := &archiveMasterImportTask{
		startSeqno: startSeqno,
		cancel:     cancel,
		done:       make(chan archiveMasterImportResult, 1),
	}
	go func() {
		imported, err := s.downloadAndImportMasterArchive(taskCtx, startSeqno, importCache)
		task.done <- archiveMasterImportResult{imported: imported, err: err}
	}()
	return task
}

func (s *Service) downloadAndImportMasterArchive(ctx context.Context, startSeqno uint32, importCache *archiveImportCache) (*archiveImportResult, error) {
	imported, _, err := s.downloadAndImportArchive(ctx, startSeqno, archive.ShardID{Workchain: -1, Shard: topShard}, importCache)
	if err != nil {
		return nil, fmt.Errorf("master archive #%d: %w", startSeqno, err)
	}
	return imported, nil
}

func (s *Service) downloadAndImportArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, importCache *archiveImportCache) (*archiveImportResult, bool, error) {
	key := archiveImportCacheKey{masterchainSeqno: masterchainSeqno, shard: shard}
	load := func(loadCtx context.Context) (*archiveImportResult, error) {
		downloaded, err := s.node.DownloadArchive(loadCtx, masterchainSeqno, shard, "")
		if err != nil {
			return nil, fmt.Errorf("download archive #%d %s: %w", masterchainSeqno, shard.String(), err)
		}

		imported, err := s.importArchiveBlocks(loadCtx, downloaded)
		if err != nil {
			return nil, fmt.Errorf("import archive #%d %s: %w", masterchainSeqno, shard.String(), err)
		}
		return imported, nil
	}
	if importCache == nil {
		imported, err := load(ctx)
		return imported, false, err
	}

	imported, cached, err := importCache.load(ctx, key, load)
	if err != nil {
		return nil, false, err
	}
	if cached {
		s.log.Debug().
			Uint32("masterchain_seqno", masterchainSeqno).
			Int32("workchain", shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
			Int("blocks", len(imported.blocks)).
			Msg("using preloaded archive import")
	}
	return imported, cached, nil
}

func (t *archiveMasterImportTask) wait(ctx context.Context) (*archiveImportResult, time.Duration, error) {
	if t == nil {
		return nil, 0, fmt.Errorf("archive master import task is nil")
	}

	started := time.Now()
	select {
	case res := <-t.done:
		return res.imported, time.Since(started), res.err
	case <-ctx.Done():
		t.cancel()
		return nil, time.Since(started), ctx.Err()
	}
}

func (s *Service) importShardClientArchiveWindow(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt, masterTask *archiveMasterImportTask, importCache *archiveImportCache) (*shardClientArchiveWindow, *archiveMasterImportTask, error) {
	startSeqno := current.ShardClientSeqno + 1
	startMaster, err := s.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return nil, masterTask, fmt.Errorf("load archive start masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

	if masterTask == nil || masterTask.startSeqno != startSeqno {
		if masterTask != nil {
			masterTask.cancel()
		}
		masterTask = s.startArchiveMasterImport(ctx, startSeqno, importCache)
	}

	masterImport, masterWait, err := masterTask.wait(ctx)
	if err != nil {
		return nil, nil, err
	}

	window := &shardClientArchiveWindow{
		startSeqno:    startSeqno,
		masterStats:   masterImport.stats,
		totalStats:    cloneImportStats(masterImport.stats),
		masterStates:  map[uint32]*storage.BlockState{},
		masterBlocks:  map[uint32]p2p.DownloadedBlock{},
		archiveBlocks: masterImport.blocks,
		masterWait:    masterWait,
	}
	for _, block := range masterImport.blocks {
		if block.ID.Workchain == -1 && block.ID.Shard == topShard {
			window.masterBlocks[block.ID.SeqNo] = block
		}
	}

	lastMaster, err := s.applyArchiveMasterBlocks(ctx, startMaster, target, window)
	if err != nil {
		return nil, nil, err
	}
	if len(window.masterStates) == 0 {
		return window, nil, nil
	}

	var nextMasterTask *archiveMasterImportTask
	startNextMasterImport := func() {
		if nextMasterTask == nil && lastMaster.Block.SeqNo < target.SeqNo {
			nextMasterTask = s.startArchiveMasterImport(ctx, lastMaster.Block.SeqNo+1, importCache)
		}
	}

	if !masterImport.stats.ContainsShardBlocks {
		startNextMasterImport()

		shards, splitDepth, err := s.archiveShardPrefixesForWindow(startMaster, lastMaster)
		if err != nil {
			if nextMasterTask != nil {
				nextMasterTask.cancel()
			}
			return nil, nil, err
		}
		window.splitDepth = splitDepth
		window.shardArchives = len(shards)

		importedFiles, err := s.downloadAndImportShardArchives(ctx, startSeqno, shards, importCache)
		if err != nil {
			if nextMasterTask != nil {
				nextMasterTask.cancel()
			}
			return nil, nil, err
		}
		for _, imported := range importedFiles {
			mergeImportStats(window.totalStats, imported.stats, true)
			for key, block := range imported.blocks {
				window.archiveBlocks[key] = block
			}
		}
	} else {
		startNextMasterImport()
	}
	window.totalStats.ShardArchives = window.shardArchives

	s.log.Debug().
		Uint32("archive_masterchain_seqno", startSeqno).
		Int64("archive_id", window.masterStats.ArchiveID).
		Str("peer", window.masterStats.Peer).
		Int("master_blocks", len(window.masterStates)).
		Int("archive_blocks", len(window.archiveBlocks)).
		Int("shard_archives", window.shardArchives).
		Uint32("monitor_split_depth", window.splitDepth).
		Dur("master_prefetch_wait", window.masterWait).
		Int64("bytes", window.totalStats.Bytes).
		Dur("download_elapsed", window.totalStats.DownloadElapsed).
		Dur("import_elapsed", window.totalStats.ImportElapsed).
		Uint32("first_seqno", window.totalStats.FirstSeqno).
		Uint32("last_seqno", window.totalStats.LastSeqno).
		Uint32("masterchain_first_seqno", window.totalStats.MasterchainFirstSeqno).
		Uint32("masterchain_last_seqno", window.totalStats.MasterchainLastSeqno).
		Msg("archive shard-client window imported")
	return window, nextMasterTask, nil
}

func (s *Service) applyArchiveMasterBlocks(ctx context.Context, start *storage.BlockState, target ton.BlockIDExt, window *shardClientArchiveWindow) (*storage.BlockState, error) {
	master := start
	for seqno := start.Block.SeqNo + 1; seqno != 0 && seqno <= target.SeqNo; seqno++ {
		downloaded, ok := window.masterBlocks[seqno]
		if !ok {
			if seqno == start.Block.SeqNo+1 {
				return nil, fmt.Errorf("archive window #%d has no next masterchain block after %s", window.startSeqno, storage.FormatBlockRef(start.Block))
			}
			break
		}
		if downloaded.Meta != nil && seqno != window.startSeqno && downloaded.Meta.Has(storage.BlockMetaIsKeyBlock) {
			break
		}

		if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 || !downloaded.Meta.PrevRefs[0].Equals(&master.Block) {
			return nil, fmt.Errorf("archive master block %s does not follow %s", downloaded.BlockRef(), storage.FormatBlockRef(master.Block))
		}

		applyStarted := time.Now()
		next, err := ApplyBlock(master, downloaded)
		window.masterApplyElapsed += time.Since(applyStarted)
		if err != nil {
			return nil, fmt.Errorf("apply archive master block %s: %w", downloaded.BlockRef(), err)
		}
		master = next
		window.masterStates[seqno] = master
	}
	return master, nil
}

func (s *Service) applyShardClientArchiveWindow(ctx context.Context, current *storage.CurrentState, window *shardClientArchiveWindow) (*storage.CurrentState, error) {
	applied := map[string]*storage.BlockState{}
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

		shards, err := s.applyArchiveShardTargets(ctx, masterState.Block, prevShards, applied, window.archiveBlocks, targets, window)
		if err != nil {
			return nil, fmt.Errorf("apply archive shard blocks at masterchain seqno %d: %w", seqno, err)
		}
		for key, shard := range shards {
			next.Shards[key] = *storage.CloneBlockState(shard)
		}
	}
	return next, nil
}

func (s *Service) applyArchiveShardTargets(ctx context.Context, master ton.BlockIDExt, current map[storage.ShardKey]storage.BlockState, applied map[string]*storage.BlockState, blocks map[string]p2p.DownloadedBlock, targets []ton.BlockIDExt, window *shardClientArchiveWindow) (map[storage.ShardKey]*storage.BlockState, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	applyCtx := newArchiveShardApplyContext(ctx, s, master, current, applied, blocks)
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
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range jobs {
				state, err := applyCtx.apply(target)
				results <- shardTargetResult{
					target: target,
					state:  state,
					err:    err,
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, target := range targets {
			select {
			case jobs <- target:
			case <-ctx.Done():
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
			}
			continue
		}
		shards[storage.ShardKeyFromBlock(res.target)] = res.state
	}
	applyCtx.addWindowStats(window)

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

func (s *Service) persistArchiveCurrentState(ctx context.Context, current *storage.CurrentState, checkpointBlocks uint32) (*storage.CurrentState, error) {
	if current == nil {
		return nil, fmt.Errorf("archive current state is nil")
	}

	states := archiveCurrentBlockStates(current)
	if len(states) == 0 {
		return nil, fmt.Errorf("archive current state has no block states")
	}

	persisted := currentStateWithoutCells(current)
	started := time.Now()
	if err := s.storage.SaveBlockStatesAndCurrentState(ctx, states, persisted); err != nil {
		return nil, fmt.Errorf("persist archive current state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

	s.log.Debug().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Int("states", len(states)).
		Int("shards", len(current.Shards)).
		Dur("elapsed", time.Since(started)).
		Msg("archive shard-client checkpoint persisted")
	return persisted, nil
}

func archiveCurrentBlockStates(current *storage.CurrentState) []*storage.BlockState {
	if current == nil {
		return nil
	}

	states := make([]*storage.BlockState, 0, 1+len(current.Shards))
	master := storage.CloneBlockState(&current.Masterchain)
	states = append(states, master)
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		states = append(states, storage.CloneBlockState(&shard))
	}
	return states
}

func currentStateWithoutCells(current *storage.CurrentState) *storage.CurrentState {
	if current == nil {
		return nil
	}

	next := &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: current.ShardClientSeqno,
		Masterchain:      blockStateWithoutCells(&current.Masterchain),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(current.Shards)),
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		next.Shards[key] = blockStateWithoutCells(&shard)
	}
	return next
}

func (s *Service) importArchiveBlocks(ctx context.Context, downloaded *archive.Downloaded) (*archiveImportResult, error) {
	if downloaded == nil {
		return nil, archive.ErrNotAvailable
	}
	storedPath, err := s.storage.SaveArchiveFile(
		int32(downloaded.MasterchainSeqno),
		downloaded.Shard.Workchain,
		downloaded.Shard.Shard,
		downloaded.ArchiveID,
		downloaded.Path,
	)
	if err != nil {
		return nil, fmt.Errorf("store archive pack %s: %w", downloaded.Path, err)
	}
	importedArchive := *downloaded
	importedArchive.Path = storedPath

	if downloaded.Imported != nil {
		downloaded.Imported.SetArtifactPath(storedPath)
		return s.storeImportedArchiveBlocks(downloaded.Imported)
	}

	blocks := map[string]p2p.DownloadedBlock{}
	stats, err := archive.ImportFile(ctx, &importedArchive, s.archiveImportSink(blocks))
	if err != nil {
		return nil, err
	}
	if stats == nil {
		return nil, fmt.Errorf("import archive %s: empty stats", downloaded.Path)
	}
	return &archiveImportResult{stats: stats, blocks: blocks}, nil
}

func (s *Service) storeImportedArchiveBlocks(imported *archive.Imported) (*archiveImportResult, error) {
	blocks := map[string]p2p.DownloadedBlock{}
	err := imported.Store(s.archiveImportSink(blocks))
	if err != nil {
		return nil, err
	}
	if imported.Stats == nil {
		return nil, fmt.Errorf("import archive: empty stats")
	}
	return &archiveImportResult{stats: imported.Stats, blocks: blocks}, nil
}

func (s *Service) archiveImportSink(blocks map[string]p2p.DownloadedBlock) archive.ImportSink {
	return archive.ImportSink{
		Writer: s.storage,
		FullBlock: func(full *storage.ServedBlockFull) error {
			if _, exists := blocks[storage.BlockKey(full.ID)]; exists {
				return nil
			}

			block, err := prepareBlockDataForApply("archive block", full.ID, full.Block)
			if err != nil {
				return err
			}
			block.ProofBOC = full.Proof
			block.IsLink = full.IsLink
			full.Meta = block.Meta
			blocks[storage.BlockKey(full.ID)] = block
			return nil
		},
	}
}

func (s *Service) archiveShardPrefixesForWindow(start *storage.BlockState, end *storage.BlockState) ([]archive.ShardID, uint32, error) {
	splitDepth, err := monitorMinSplitDepth(start, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("load monitor split depth for %s: %w", storage.FormatBlockRef(start.Block), err)
	}
	if splitDepth > maxArchiveMonitorSplitDepth {
		return nil, 0, fmt.Errorf("monitor split depth %d exceeds supported archive prefix fanout %d", splitDepth, maxArchiveMonitorSplitDepth)
	}

	startBlocks, err := state2.ShardBlocksFromMasterState(start)
	if err != nil {
		return nil, 0, fmt.Errorf("load start shard blocks from %s: %w", storage.FormatBlockRef(start.Block), err)
	}
	endBlocks, err := state2.ShardBlocksFromMasterState(end)
	if err != nil {
		return nil, 0, fmt.Errorf("load end shard blocks from %s: %w", storage.FormatBlockRef(end.Block), err)
	}

	startByShard := make(map[storage.ShardKey]ton.BlockIDExt, len(startBlocks))
	for _, block := range startBlocks {
		startByShard[storage.ShardKeyFromBlock(block)] = block
	}

	count := 1 << splitDepth
	shards := make([]archive.ShardID, 0, count)
	for i := 0; i < count; i++ {
		shard := uint64(i*2+1) << (64 - splitDepth - 1)
		prefix := archive.ShardID{
			Workchain: 0,
			Shard:     int64(shard),
		}
		if archivePrefixHasChangedShard(prefix, endBlocks, startByShard) {
			shards = append(shards, prefix)
		}
	}
	return shards, splitDepth, nil
}

func cloneImportStats(stats *archive.ImportStats) *archive.ImportStats {
	if stats == nil {
		return &archive.ImportStats{}
	}
	cloned := *stats
	cloned.MasterchainShardBlocks = append([]ton.BlockIDExt(nil), stats.MasterchainShardBlocks...)
	return &cloned
}

func cloneArchiveImportResult(result *archiveImportResult) *archiveImportResult {
	if result == nil {
		return nil
	}

	cloned := &archiveImportResult{
		stats:  cloneImportStats(result.stats),
		blocks: make(map[string]p2p.DownloadedBlock, len(result.blocks)),
	}
	for key, block := range result.blocks {
		cloned.blocks[key] = block
	}
	return cloned
}
