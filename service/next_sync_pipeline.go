package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	nextMasterchainPrefetchBlocks    = 32
	nextShardPrefetchScheduleLimit   = 2048
	nextShardPrefetchWorkers         = 8
	shardDescriptionPrefetchMaxAhead = 20
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

	timing                            catchUpTiming
	stagedBlocks                      uint32
	checkpointMu                      sync.Mutex
	checkpointStates                  appliedStateSet
	stateCells                        *stateCellWindowCache
	shardCache                        map[string]*storage.BlockState
	shardResolver                     *shardStateResolver
	shardResolverSeen                 shardStateResolverStats
	shardPrefetchSlots                chan struct{}
	shardDescriptionPrefetchMu        sync.Mutex
	shardDescriptionPrefetchScheduled map[string]struct{}
	shardDescriptionPrefetchOrder     []string
	shardTarget                       time.Duration
	shardWall                         time.Duration
	shardApply                        time.Duration
	shardApplied                      uint64
	shardReused                       uint64
	shardPrefetchTargets              uint64
	committed                         uint32
	lastProgressMasterSeqno           uint32
	lastProgressShardClientSeqno      uint32
	lastProgressShardApplied          uint64
	lastProgressShardReused           uint64
	lastProgressShardPrefetch         uint64
	lastCheckpointDeferred            time.Time
	lastBlockUTime                    uint32
	lastLagSeconds                    int64
	hasBlockTime                      bool
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
	stateUpdateCells     map[cell.Hash][]byte
	releaseApplyCells    func()
	shardTargets         []ton.BlockIDExt
	shardTargetParse     time.Duration
	shardPrefetchTargets int
	err                  error
}

type nextMasterApplyCellLayer struct {
	block   ton.BlockIDExt
	records map[cell.Hash][]byte
}

// nextMasterApplyCellWindow is private to master apply-ahead. Future cells are
// useful for applying the next master states, but they must not enter the shared
// checkpoint window until the matching metadata is ready to be committed.
type nextMasterApplyCellWindow struct {
	mu     sync.RWMutex
	layers []nextMasterApplyCellLayer
	base   cell.LazyCellLoader
}

func newNextMasterApplyCellWindow(base cell.LazyCellLoader) *nextMasterApplyCellWindow {
	return &nextMasterApplyCellWindow{base: base}
}

func (w *nextMasterApplyCellWindow) applyBlockStateUpdate(previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*cell.Cell, error) {
	updateTo, err := merkleUpdateToRef(downloaded.Parsed.StateUpdate)
	if err != nil {
		return nil, err
	}
	if downloaded.StateUpdateToCells == nil {
		return nil, fmt.Errorf("prepared state update cells are missing")
	}

	nextRoot := updateTo.Virtualize(0)
	rootHash := nextRoot.GetMetadata().Hash
	if len(downloaded.StateUpdateToCells[rootHash]) == 0 {
		return nil, fmt.Errorf("prepared state update cells do not contain destination root %x", rootHash[:])
	}

	loader := w.loaderWith(downloaded.StateUpdateToCells)
	currentRoot, err := previousStateRootWithLoader(previous, loader)
	if err != nil {
		return nil, err
	}
	if err = cell.MayApplyMerkleUpdate(currentRoot, downloaded.Parsed.StateUpdate); err != nil {
		return nil, err
	}

	loaded, err := loader(rootHash)
	if err != nil {
		return nil, fmt.Errorf("reload applied master state root %x from apply cell window: %w", rootHash[:], err)
	}
	return loaded, nil
}

func (w *nextMasterApplyCellWindow) remember(block ton.BlockIDExt, records map[cell.Hash][]byte) {
	if w == nil || len(records) == 0 {
		return
	}

	w.mu.Lock()
	w.layers = append(w.layers, nextMasterApplyCellLayer{
		block:   block,
		records: records,
	})
	w.mu.Unlock()
}

func (w *nextMasterApplyCellWindow) forget(block ton.BlockIDExt) {
	if w == nil {
		return
	}

	key := storage.BlockKey(block)
	w.mu.Lock()
	defer w.mu.Unlock()

	for i, layer := range w.layers {
		if storage.BlockKey(layer.block) != key {
			continue
		}

		copy(w.layers[i:], w.layers[i+1:])
		w.layers[len(w.layers)-1] = nextMasterApplyCellLayer{}
		w.layers = w.layers[:len(w.layers)-1]
		return
	}
}

func (w *nextMasterApplyCellWindow) loaderWith(records map[cell.Hash][]byte) cell.LazyCellLoader {
	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		if data := records[hash]; len(data) > 0 {
			return cachedLazyCell(hash, data, load)
		}
		return w.load(hash)
	}
	return load
}

func (w *nextMasterApplyCellWindow) load(hash cell.Hash) (*cell.Cell, error) {
	if w == nil {
		return nil, storage.ErrNotFound
	}

	var data []byte
	w.mu.RLock()
	for i := len(w.layers) - 1; i >= 0; i-- {
		if candidate := w.layers[i].records[hash]; len(candidate) > 0 {
			data = candidate
			break
		}
	}
	base := w.base
	w.mu.RUnlock()

	if len(data) > 0 {
		return cachedLazyCell(hash, data, w.load)
	}
	if base == nil {
		return nil, storage.ErrNotFound
	}
	return base(hash)
}

func (s *Service) runNextSyncToTarget(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*storage.CurrentState, error) {
	next, _, err := s.runNextSync(ctx, current, nextSyncToTarget, target, 0, "next_block")
	return next, err
}

type nextSyncBootstrapResult struct {
	current *storage.CurrentState
	changed bool
}

func (s *Service) runNextSyncBootstrap(ctx context.Context, current *storage.CurrentState) (nextSyncBootstrapResult, error) {
	next, processed, err := s.runNextSync(ctx, current, nextSyncBootstrap, ton.BlockIDExt{}, nextBlockBootstrapBlocks, "next_block_bootstrap")
	if err != nil {
		return nextSyncBootstrapResult{}, err
	}
	return nextSyncBootstrapResult{current: next, changed: processed > 0}, nil
}

func (s *Service) runNextSync(ctx context.Context, current *storage.CurrentState, mode nextSyncMode, target ton.BlockIDExt, maxBlocks uint32, method string) (*storage.CurrentState, uint32, error) {
	if current == nil {
		return nil, 0, fmt.Errorf("current state is nil")
	}
	if s.cellGenerationSwitchActive() {
		return nil, 0, errCellGenerationMigrationRunning
	}
	if err := s.waitSyncDiskSpace(ctx, method); err != nil {
		return nil, 0, err
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
		service:                           s,
		ctx:                               runCtx,
		cancel:                            cancel,
		mode:                              mode,
		method:                            method,
		target:                            target,
		current:                           storage.CloneCurrentState(current),
		master:                            master,
		started:                           now,
		startSeqno:                        master.Block.SeqNo,
		lastLog:                           now,
		totalBlocks:                       totalBlocks,
		maxBlocks:                         maxBlocks,
		timing:                            newCatchUpTiming(now),
		stateCells:                        newStateCellWindowCache(s.stateCellLoader()),
		shardCache:                        map[string]*storage.BlockState{},
		shardPrefetchSlots:                make(chan struct{}, nextShardPrefetchWorkers),
		shardDescriptionPrefetchScheduled: map[string]struct{}{},
		lastProgressMasterSeqno:           master.Block.SeqNo,
		lastProgressShardClientSeqno:      current.Masterchain.Block.SeqNo,
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
	r.startShardDescriptionPrefetch()

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
		Int("shard_prefetch_workers", nextShardPrefetchWorkers).
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
		if next.err != nil {
			r.service.observeSyncBlock(SyncBlockObservation{
				Pipeline:         r.method,
				Chain:            syncChainLabel(item.prev),
				Source:           next.source,
				Result:           "error",
				CatchUp:          true,
				DownloadDuration: next.downloadElapsed,
			})
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
	prevUTime := blockStateUtime(r.ctx, r.service.storage, r.master)
	probeState := nextBlockBootstrapProbeState{
		liveTail: nextBlockBootstrapLiveTail(prevUTime, time.Now().Unix()),
	}
	for processed := uint32(0); r.maxBlocks == 0 || processed < r.maxBlocks; {
		if r.service.cellGenerationSwitchRequestActive() {
			return
		}

		started := time.Now()
		downloaded, source, err := r.service.downloadNextChainBlockProbe(r.ctx, prev, prevUTime, probeState)
		elapsed := time.Since(started)
		if err != nil {
			r.service.observeSyncBlock(SyncBlockObservation{
				Pipeline:         r.method,
				Chain:            syncChainLabel(prev),
				Source:           "probe",
				Result:           "error",
				CatchUp:          false,
				DownloadDuration: elapsed,
			})
			if isExpectedRetryError(err) {
				probeState.consecutiveMisses++
				r.service.log.Debug().
					Err(err).
					Str("current", storage.FormatBlockRef(prev)).
					Int("consecutive_misses", probeState.consecutiveMisses).
					Bool("live_tail", probeState.liveTail).
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
		prevUTime = 0
		if downloaded.Meta != nil {
			prevUTime = int64(downloaded.Meta.GenUTime)
		}
		if nextBlockBootstrapLiveTail(prevUTime, time.Now().Unix()) {
			probeState.liveTail = true
		}
		probeState.consecutiveMisses = 0
		processed++
		if r.shouldYieldBootstrapToArchive(prevUTime) {
			r.service.log.Info().
				Uint32("processed_blocks", processed).
				Str("masterchain", storage.FormatBlockRef(prev)).
				Msg("yielding next-block bootstrap to reevaluate archive catch-up")
			return
		}
	}
}

func (r *nextSyncRunner) shouldYieldBootstrapToArchive(prevUTime int64) bool {
	if r.mode != nextSyncBootstrap || r.maxBlocks != 0 {
		return false
	}

	lagSeconds, ok := masterchainBlockLagSeconds(prevUTime, time.Now().Unix())
	return ok && shouldSwitchNextToArchiveByLag(lagSeconds)
}

func (r *nextSyncRunner) waitBootstrapRetry() bool {
	if r.service.cellGenerationSwitchRequestActive() {
		return false
	}

	timer := time.NewTimer(currentStateLivePollDelay)
	defer timer.Stop()

	select {
	case <-r.ctx.Done():
		return false
	case <-r.service.currentStateWake:
		return !r.service.cellGenerationSwitchRequestActive()
	case <-timer.C:
		return !r.service.cellGenerationSwitchRequestActive()
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
	applyCells := newNextMasterApplyCellWindow(func(hash cell.Hash) (*cell.Cell, error) {
		if r.stateCells == nil {
			base := r.service.stateCellLoader()
			if base == nil {
				return nil, storage.ErrNotFound
			}
			return base(hash)
		}
		return r.stateCells.loader()(hash)
	})
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

			applied, err := r.applyMaster(master, item, applyCells)
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

func (r *nextSyncRunner) applyMaster(master *storage.BlockState, item nextMasterDownload, applyCells *nextMasterApplyCellWindow) (nextAppliedMaster, error) {
	applied := nextAppliedMaster{
		prev:            item.prev,
		block:           item.block,
		downloadSource:  item.source,
		downloadElapsed: item.downloadElapsed,
	}
	observe := func(result string, applyDuration time.Duration) {
		r.service.observeSyncBlock(SyncBlockObservation{
			Pipeline:         r.method,
			Chain:            syncChainLabel(item.block.ID),
			Source:           item.source,
			Result:           result,
			CatchUp:          r.mode == nextSyncToTarget,
			DownloadDuration: item.downloadElapsed,
			ApplyDuration:    applyDuration,
		})
	}

	nextMaster, applyTiming, err := r.service.applyMasterchainTransitionWithConsensusProof(master, item.block, nil, applyCells)
	applied.applyTiming = applyTiming
	if err != nil {
		applied.err = err
		observe("error", applyTiming.total)
		return applied, err
	}

	r.service.publishLiveBlockArtifacts(item.block, nextMaster, false)
	if err = r.service.stageAppliedBlockArtifact(r.ctx, item.block, 0); err != nil {
		applied.err = err
		observe("error", applyTiming.total)
		return applied, err
	}
	r.service.rememberSeenMasterchainBlock(nextMaster.Block)
	r.service.rememberMasterState(nextMaster)
	applyCells.remember(item.block.ID, item.block.StateUpdateToCells)
	applied.master = nextMaster
	applied.stateUpdateCells = item.block.StateUpdateToCells
	applied.releaseApplyCells = func() {
		applyCells.forget(item.block.ID)
	}
	observe("success", applyTiming.total)
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
	if r.service.node == nil {
		return
	}
	if !r.tryAcquireShardPrefetchSlot() {
		return
	}

	go func() {
		defer r.releaseShardPrefetchSlot()

		err := r.service.node.PrefetchShardBlockFull(r.ctx, target)
		if err != nil && r.ctx.Err() == nil {
			r.service.log.Debug().
				Err(err).
				Str("masterchain", storage.FormatBlockRef(master)).
				Str("target", storage.FormatBlockRef(target)).
				Msg("next-block shard target prefetch failed")
		}
	}()
}

func (r *nextSyncRunner) tryAcquireShardPrefetchSlot() bool {
	if r.shardPrefetchSlots == nil {
		return true
	}

	select {
	case <-r.ctx.Done():
		return false
	case r.shardPrefetchSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (r *nextSyncRunner) releaseShardPrefetchSlot() {
	if r.shardPrefetchSlots == nil {
		return
	}
	<-r.shardPrefetchSlots
}

func (r *nextSyncRunner) startShardDescriptionPrefetch() {
	if r.service == nil || r.service.shardDescriptionWake == nil {
		return
	}

	r.prefetchShardDescriptionHints()
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-r.service.shardDescriptionWake:
				r.prefetchShardDescriptionHints()
			}
		}
	}()
}

func (r *nextSyncRunner) prefetchShardDescriptionHints() {
	r.shardDescriptionPrefetchMu.Lock()
	defer r.shardDescriptionPrefetchMu.Unlock()

	hints := r.service.shardDescriptionHintSnapshot(time.Now())
	for _, hint := range hints {
		desc, err := parseShardTopBlockDescription(hint.Block, hint.CatchainSeqno, hint.Data)
		if err != nil {
			r.service.dropShardDescriptionHint(hint.Block)
			r.service.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(hint.Block)).
				Str("overlay", hint.Overlay).
				Msg("dropping invalid shard block description broadcast")
			continue
		}

		err = r.validateShardDescriptionPrefetch(desc)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if errors.Is(err, errShardDescriptionTooNew) {
			continue
		}
		if errors.Is(err, errShardDescriptionTooOld) {
			r.service.dropShardDescriptionHint(hint.Block)
			continue
		}
		if err != nil {
			r.service.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(desc.Block)).
				Str("overlay", hint.Overlay).
				Msg("dropping shard block description broadcast")
			r.service.dropShardDescriptionHint(hint.Block)
			continue
		}

		key := storage.BlockKey(desc.Block)
		if !rememberScheduledShardPrefetch(r.shardDescriptionPrefetchScheduled, &r.shardDescriptionPrefetchOrder, key) {
			continue
		}

		r.prefetchShardDescriptionTarget(hint, desc)
	}
}

func (r *nextSyncRunner) validateShardDescriptionPrefetch(desc *shardTopBlockDescription) error {
	currentBlocks, err := r.shardResolver.currentBlocksForBlock(desc.Block)
	if err != nil {
		return err
	}

	currentSeqno := uint32(0)
	for i, block := range currentBlocks {
		if i == 0 || block.SeqNo > currentSeqno {
			currentSeqno = block.SeqNo
		}
	}
	if desc.Block.SeqNo <= currentSeqno {
		return errShardDescriptionTooOld
	}
	if desc.Block.SeqNo > currentSeqno+shardDescriptionPrefetchMaxAhead {
		return errShardDescriptionTooNew
	}
	if !shardDescriptionAnchorsCurrent(desc, currentBlocks) {
		return fmt.Errorf("shard block description is not anchored to current shard state")
	}
	return nil
}

func shardDescriptionAnchorsCurrent(desc *shardTopBlockDescription, currentBlocks []ton.BlockIDExt) bool {
	for _, current := range currentBlocks {
		if !shardDescriptionContainsBlock(desc, current) && !shardDescriptionContainsPrevRef(desc, current) {
			return false
		}
	}
	return len(currentBlocks) > 0
}

func shardDescriptionContainsBlock(desc *shardTopBlockDescription, block ton.BlockIDExt) bool {
	for _, link := range desc.Chain {
		if link.Block.Equals(&block) {
			return true
		}
	}
	return false
}

func shardDescriptionContainsPrevRef(desc *shardTopBlockDescription, block ton.BlockIDExt) bool {
	for _, link := range desc.Chain {
		for _, prev := range link.PrevRefs {
			if prev.Equals(&block) {
				return true
			}
		}
	}
	return false
}

func (r *nextSyncRunner) prefetchShardDescriptionTarget(hint shardDescriptionHint, desc *shardTopBlockDescription) {
	if r.service.node == nil {
		return
	}
	if !r.tryAcquireShardPrefetchSlot() {
		return
	}

	r.service.log.Debug().
		Str("block", storage.FormatBlockRef(desc.Block)).
		Str("overlay", hint.Overlay).
		Int("chain_links", len(desc.Chain)).
		Msg("prefetching shard block from description broadcast")

	go func() {
		defer r.releaseShardPrefetchSlot()

		err := r.service.node.PrefetchShardBlockFull(r.ctx, desc.Block)
		if err != nil && r.ctx.Err() == nil {
			r.service.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(desc.Block)).
				Str("overlay", hint.Overlay).
				Msg("shard block description prefetch failed")
		}
	}()
}

func (r *nextSyncRunner) commitCurrent(applied <-chan nextAppliedMaster) (*storage.CurrentState, error) {
	for {
		waitStarted := time.Now()

		select {
		case item, ok := <-applied:
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

			if r.shouldReturnAfterCommit() {
				r.cancel()
				if err := r.flushStagedCurrent(); err != nil {
					return r.current, err
				}
				return r.current, nil
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
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		return err
	}

	queuedAt := time.Now()
	lockStarted := time.Now()
	r.service.currentStatePersistMu.Lock()
	lockElapsed := time.Since(lockStarted)
	r.timing.persist += lockElapsed

	checkpoint := r.checkpoint()
	cells := r.stateCells.beginCheckpoint()
	next, err := r.service.persistNextBlockCurrentStateLocked(r.current, &r.timing, checkpoint.entries, cells, func() {
		r.completeCheckpoint(checkpoint)
	}, lockElapsed, queuedAt)
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
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		return err
	}

	lockStarted := time.Now()
	r.service.currentStatePersistMu.Lock()
	lockElapsed := time.Since(lockStarted)
	r.timing.persist += lockElapsed

	checkpoint := r.checkpoint()
	cells := r.stateCells.beginCheckpoint()
	next, err := r.service.persistNextBlockCurrentStateSyncLocked(r.current, &r.timing, reason, checkpoint.entries, cells, lockElapsed)
	if err != nil {
		return err
	}
	r.completeCheckpoint(checkpoint)
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
	if err = r.rememberMasterCheckpointState(item); err != nil {
		return err
	}

	r.current = nextCurrent
	r.shardResolver.updateCurrent(r.current.Shards)
	r.prefetchShardDescriptionHints()
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

	if r.reachedTarget() {
		if err := r.flushStagedCurrent(); err != nil {
			return err
		}
	} else if r.shouldBackpressureStagedCurrent() {
		if err := r.flushStagedCurrent(); err != nil {
			return err
		}
	} else if r.shouldCheckpointStagedCurrent() {
		if err := r.tryFlushStagedCurrent(); err != nil {
			return err
		}
	}

	r.logShardCommit(item, shardStats, blockStarted, masterPipelineWait, appliedQueue, applyBefore, persistBefore, checkpointsBefore, shardTargetBefore, shardWallBefore, shardApplyBefore, shardAppliedBefore)
	r.logProgressIfNeeded(item)
	return nil
}

func (r *nextSyncRunner) tryFlushStagedCurrent() error {
	if r.stagedBlocks == 0 {
		return nil
	}
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		return err
	}

	lockStarted := time.Now()
	if !r.service.currentStatePersistMu.TryLock() {
		r.logCheckpointDeferred()
		return nil
	}
	lockElapsed := time.Since(lockStarted)
	r.timing.persist += lockElapsed

	checkpoint := r.checkpoint()
	cells := r.stateCells.beginCheckpoint()
	next, err := r.service.persistNextBlockCurrentStateLocked(r.current, &r.timing, checkpoint.entries, cells, func() {
		r.completeCheckpoint(checkpoint)
	}, lockElapsed, time.Now())
	if err != nil {
		return err
	}
	r.current = next
	r.stagedBlocks = 0
	return nil
}

func (r *nextSyncRunner) logCheckpointDeferred() {
	if time.Since(r.lastCheckpointDeferred) < 5*time.Second {
		return
	}
	r.lastCheckpointDeferred = time.Now()
	r.service.log.Info().
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("pending_checkpoint_blocks", r.stagedBlocks).
		Uint64("pending_checkpoint_bytes", r.pendingCheckpointBytes()).
		Uint32("checkpoint_blocks", r.service.nextCheckpointBlocks()).
		Uint64("checkpoint_bytes", r.service.checkpointBytesTarget()).
		Uint32("checkpoint_backpressure_blocks", checkpointBackpressureBlocks(r.service.nextCheckpointBlocks())).
		Uint64("checkpoint_backpressure_bytes", checkpointBackpressureBytes(r.service.checkpointBytesTarget())).
		Msg("next-block checkpoint deferred because current state persist is busy")
}

func (r *nextSyncRunner) shouldCheckpointStagedCurrent() bool {
	if r.stagedBlocks >= r.service.nextCheckpointBlocks() {
		return true
	}
	return r.pendingCheckpointBytes() >= r.service.checkpointBytesTarget()
}

func (r *nextSyncRunner) shouldBackpressureStagedCurrent() bool {
	if r.stagedBlocks >= checkpointBackpressureBlocks(r.service.nextCheckpointBlocks()) {
		return true
	}
	return r.pendingCheckpointBytes() >= checkpointBackpressureBytes(r.service.checkpointBytesTarget())
}

func (r *nextSyncRunner) pendingCheckpointBytes() uint64 {
	if r.stateCells == nil {
		return 0
	}
	return r.stateCells.byteSize()
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
	setShardStateMasterchainRef(state, r.master.Block)
	setDownloadedShardMasterchainRef(&downloaded, r.master.Block)

	r.service.publishLiveBlockArtifacts(downloaded, state, false)

	splitDepth, err := monitorMinSplitDepth(r.master, downloaded.ID.Workchain)
	if err != nil {
		return err
	}
	if err = r.service.stageAppliedBlockArtifact(ctx, downloaded, splitDepth); err != nil {
		return err
	}
	r.rememberCheckpointStateWithCells(state, downloaded.StateUpdateToCells)
	return nil
}

func (r *nextSyncRunner) rememberMasterCheckpointState(item nextAppliedMaster) error {
	// Master apply-ahead keeps future cells private. Move this block's prepared
	// cells into the checkpoint window only when its state metadata is remembered,
	// so every metadata commit is paired with cells from the same checkpoint.
	r.stateCells.addPreparedRecords(item.stateUpdateCells)
	if item.releaseApplyCells != nil {
		item.releaseApplyCells()
	}
	r.rememberCheckpointStateWithCells(item.master, item.stateUpdateCells)
	return nil
}

func (r *nextSyncRunner) rememberCheckpointStateWithCells(state *storage.BlockState, cells map[cell.Hash][]byte) {
	r.checkpointMu.Lock()
	r.checkpointStates.rememberWithCells(state, cells)
	r.checkpointMu.Unlock()
}

func (r *nextSyncRunner) checkpoint() appliedStateCheckpoint {
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	return r.checkpointStates.checkpoint()
}

func (r *nextSyncRunner) completeCheckpoint(checkpoint appliedStateCheckpoint) {
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	r.checkpointStates.completeCheckpoint(checkpoint)
}

func (r *nextSyncRunner) reachedTarget() bool {
	return r.mode == nextSyncToTarget && r.current.Masterchain.Block.SeqNo >= r.target.SeqNo
}

func (r *nextSyncRunner) shouldReturnAfterCommit() bool {
	if r.reachedTarget() {
		return true
	}
	if r.service.shouldYieldNextBlockForCellGenerationSwitch(time.Now()) {
		r.service.log.Info().
			Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
			Str("catchup_method", r.method).
			Uint32("pending_checkpoint_blocks", r.stagedBlocks).
			Uint64("pending_checkpoint_bytes", r.pendingCheckpointBytes()).
			Msg("yielding next-block pipeline for cell generation switch")
		return true
	}

	blockUTime := blockStateUtime(r.ctx, r.service.storage, &r.current.Masterchain)
	lagSeconds, ok := masterchainBlockLagSeconds(blockUTime, time.Now().Unix())
	if !ok || !shouldSwitchNextToArchiveByLag(lagSeconds) {
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
	event.Msg("switching from next-block pipeline to archive catch-up")
	return true
}

func (r *nextSyncRunner) latestTarget(currentSeqno uint32) (ton.BlockIDExt, error) {
	latest, err := r.service.knownMasterchainTarget(currentSeqno)
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

	masterShardGapBlocks := uint32(0)
	if masterSeqno > shardClientSeqno {
		masterShardGapBlocks = masterSeqno - shardClientSeqno
	}
	masterLagSeconds := int64(0)
	if r.master.Parsed != nil {
		masterLagSeconds = now.Unix() - int64(r.master.Parsed.GenUTime)
	}
	shardClientLagSeconds := int64(0)
	if r.current.Masterchain.Parsed != nil {
		shardClientLagSeconds = now.Unix() - int64(r.current.Masterchain.Parsed.GenUTime)
	}
	basechainLagSeconds, basechainShards, hasBasechainLag := currentBasechainLagSeconds(now, r.current)

	event := r.service.log.Info().
		Str("masterchain_head", storage.FormatBlockRef(r.master.Block)).
		Str("shard_client", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("catchup_method", r.method).
		Uint32("pending_checkpoint_blocks", r.stagedBlocks).
		Uint64("pending_checkpoint_bytes", r.pendingCheckpointBytes()).
		Uint32("master_shard_gap_blocks", masterShardGapBlocks).
		Uint64("shard_blocks_applied", r.shardApplied)
	if r.mode == nextSyncToTarget {
		event.Str("target", storage.FormatBlockRef(r.target))
	}
	if r.hasBlockTime {
		event.Uint32("block_utime", r.lastBlockUTime).
			Int64("master_lag_seconds", masterLagSeconds).
			Int64("shard_client_lag_seconds", shardClientLagSeconds)
		if hasBasechainLag {
			event.Int64("basechain_lag_seconds", basechainLagSeconds).
				Int("basechain_shards", basechainShards)
		}
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

func currentBasechainLagSeconds(now time.Time, current *storage.CurrentState) (int64, int, bool) {
	if current == nil {
		return 0, 0, false
	}

	var maxLag int64
	shards := 0
	for _, shard := range current.Shards {
		if shard.Block.Workchain != 0 || shard.Parsed == nil || shard.Parsed.GenUTime == 0 {
			continue
		}

		lag := now.Unix() - int64(shard.Parsed.GenUTime)
		if shards == 0 || lag > maxLag {
			maxLag = lag
		}
		shards++
	}
	return maxLag, shards, shards > 0
}
