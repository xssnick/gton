package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	nextMasterchainPrefetchBlocks    = 16
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

	started time.Time
	lastLog time.Time

	totalBlocks uint32
	maxBlocks   uint32

	timing                            catchUpTiming
	stagedBlocks                      uint32
	checkpointMu                      sync.Mutex
	checkpointStates                  appliedStateSet
	artifactPrewriteSeq               uint64
	stateCells                        *stateCellWindowCache
	shardCache                        map[storage.BlockRootHash]*storage.BlockState
	shardResolver                     *shardStateResolver
	shardResolverSeen                 shardStateResolverStats
	shardPrefetchSlots                chan struct{}
	shardPrefetchScheduled            map[storage.BlockRootHash]struct{}
	shardPrefetchOrder                []storage.BlockRootHash
	shardDescriptionPrefetchMu        sync.Mutex
	shardDescriptionPrefetchScheduled map[storage.BlockRootHash]struct{}
	shardDescriptionPrefetchOrder     []storage.BlockRootHash
	shardAheadWake                    chan struct{}
	shardAheadMu                      sync.Mutex
	shardAheadPending                 map[uint32]shardApplyAheadJob
	shardAheadRecorders               map[storage.BlockRootHash]shardAheadRecorderEntry
	shardStageMu                      sync.Mutex
	shardStageDeferred                map[uint32][]deferredShardCheckpointState
	// committedMasterSeqno is published by the commit stage and read by the
	// apply-ahead stage to decide whether resolving a master can only touch
	// shard blocks that master itself includes.
	committedMasterSeqno   atomic.Uint32
	shardTarget            time.Duration
	shardWall              time.Duration
	shardApply             time.Duration
	shardApplied           uint64
	committed              uint32
	lastCheckpointDeferred time.Time
	lastBlockUTime         uint32
	lastLagSeconds         int64
	hasBlockTime           bool
}

type nextMasterDownload struct {
	prev            ton.BlockIDExt
	block           PreparedBlock
	source          SyncBlockSource
	downloadElapsed time.Duration
	prepareElapsed  time.Duration
	err             error
}

type nextAppliedMaster struct {
	prev                 ton.BlockIDExt
	block                PreparedBlock
	master               *storage.BlockState
	downloadSource       SyncBlockSource
	downloadElapsed      time.Duration
	prepareElapsed       time.Duration
	applyTiming          masterchainApplyTiming
	stageElapsed         time.Duration
	stateUpdateCells     storage.StateCellRecords
	releaseApplyCells    func()
	shardTargets         []ton.BlockIDExt
	shardTargetsParsed   bool
	shardTargetParse     time.Duration
	shardPrefetchTargets int
	syncUntilReached     bool
	err                  error
}

func downloadElapsedExcludingInlinePrepare(elapsed, prepareElapsed time.Duration) time.Duration {
	if prepareElapsed <= 0 {
		return elapsed
	}
	if prepareElapsed >= elapsed {
		return 0
	}
	return elapsed - prepareElapsed
}

type nextMasterApplyCellLayer struct {
	block   ton.BlockIDExt
	records storage.StateCellRecords
}

// nextMasterApplyCellWindow is private to master apply-ahead. Future cells are
// useful for applying the next master states, but they must not enter the shared
// checkpoint window until the matching metadata is ready to be committed.
type nextMasterApplyCellWindow struct {
	mu      sync.RWMutex
	layers  []nextMasterApplyCellLayer
	base    cell.LazyCellLoader
	metrics *lazyCellLoadCounters
}

func newNextMasterApplyCellWindow(base cell.LazyCellLoader, metrics ...*lazyCellLoadCounters) *nextMasterApplyCellWindow {
	var counters *lazyCellLoadCounters
	if len(metrics) > 0 {
		counters = metrics[0]
	}
	return &nextMasterApplyCellWindow{base: base, metrics: counters}
}

func (w *nextMasterApplyCellWindow) applyBlockStateUpdate(previous []*storage.BlockState, block PreparedBlock) (stateUpdateApplyResult, error) {
	updateTo, err := merkleUpdateToRef(block.StateUpdate)
	if err != nil {
		return stateUpdateApplyResult{}, err
	}
	if block.StateUpdateToCells.Empty() {
		return stateUpdateApplyResult{}, fmt.Errorf("prepared state update cells are missing")
	}

	nextRoot := updateTo.Virtualize(0)
	rootHash := nextRoot.GetMetadata().Hash
	if !block.StateUpdateToCells.Has(rootHash) {
		return stateUpdateApplyResult{}, fmt.Errorf("prepared state update cells do not contain destination root %x", rootHash[:])
	}

	loader := w.loaderWith(block.StateUpdateToCells)
	currentRoot, err := previousStateRootWithLoader(previous, loader)
	if err != nil {
		return stateUpdateApplyResult{}, err
	}
	if err = cell.MayApplyMerkleUpdate(currentRoot, block.StateUpdate); err != nil {
		return stateUpdateApplyResult{}, err
	}

	loaded, err := loader(rootHash)
	if err != nil {
		return stateUpdateApplyResult{}, fmt.Errorf("reload applied master state root %x from apply cell window: %w", rootHash[:], err)
	}
	return stateUpdateApplyResult{
		PreviousRoot: currentRoot,
		NextRoot:     loaded,
	}, nil
}

func (w *nextMasterApplyCellWindow) remember(block ton.BlockIDExt, records storage.StateCellRecords) {
	if records.Empty() {
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

func (w *nextMasterApplyCellWindow) loaderWith(records storage.StateCellRecords) cell.LazyCellLoader {
	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		if data := records.Data(hash); len(data) > 0 {
			w.metrics.observeStateWindow()
			return cachedLazyCell(hash, data, load)
		}
		return w.load(hash)
	}
	return load
}

func (w *nextMasterApplyCellWindow) load(hash cell.Hash) (*cell.Cell, error) {
	var data []byte
	w.mu.RLock()
	for i := len(w.layers) - 1; i >= 0; i-- {
		if candidate := w.layers[i].records.Data(hash); len(candidate) > 0 {
			data = candidate
			break
		}
	}
	base := w.base
	w.mu.RUnlock()

	if len(data) > 0 {
		w.metrics.observeStateWindow()
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
	if s.cellGenerationSwitchActive() {
		return nil, 0, errCellGenerationMigrationRunning
	}
	s.enableAutomaticStateSerialization()

	if err := s.waitSyncDiskSpace(ctx, method, statFSSyncDiskSpace, syncDiskSpaceRetryDelay); err != nil {
		return nil, 0, err
	}

	master, err := s.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return nil, 0, fmt.Errorf("load current masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	if mode == nextSyncToTarget && master.Block.SeqNo >= target.SeqNo {
		return current, 0, nil
	}
	s.rememberMasterState(ctx, master, nil, nil)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	now := time.Now()
	totalBlocks := uint32(0)
	if mode == nextSyncToTarget {
		totalBlocks = target.SeqNo - master.Block.SeqNo
	}
	stateCells := newStateCellWindowCache(s.stateCellLoader(), &s.lazyCellLoads)
	stateCells.setPrewriter(s.stateCellPrewrite)
	releaseStateCells := s.retainStateCellLoader(stateCells.retainedLoader(s.stateCellLoader()))
	defer releaseStateCells()

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
		lastLog:                           now,
		totalBlocks:                       totalBlocks,
		maxBlocks:                         maxBlocks,
		timing:                            newCatchUpTiming(now),
		stateCells:                        stateCells,
		shardCache:                        map[storage.BlockRootHash]*storage.BlockState{},
		shardPrefetchSlots:                make(chan struct{}, nextShardPrefetchWorkers),
		shardPrefetchScheduled:            map[storage.BlockRootHash]struct{}{},
		shardDescriptionPrefetchScheduled: map[storage.BlockRootHash]struct{}{},
	}
	// The starting master is the committed head: the apply-ahead stage may
	// resolve the block right after it without owning anything older.
	r.committedMasterSeqno.Store(r.current.Masterchain.Block.SeqNo)
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
	// Cancels the run context and drains the stage, so no shard state is applied
	// into this run's cell window after it returns.
	stopShardApplyAhead := r.startShardApplyAhead()
	defer stopShardApplyAhead()

	downloads := r.startMasterSource()
	applied := r.startMasterApply(downloads)
	current, err := r.commitCurrent(applied)
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
			prepareElapsed:  item.prepareElapsed,
			err:             item.err,
		}
		if next.source == "" {
			next.source = SyncBlockSourceUnknown
		}
		if next.err != nil {
			r.service.observeSyncBlock(SyncBlockObservation{
				Pipeline:         r.method,
				Chain:            syncChainLabel(item.prev),
				Shard:            syncShardLabel(item.prev),
				Source:           next.source,
				Origin:           syncBlockOriginForSource(next.source),
				Result:           syncBlockResultForError(next.err),
				CatchUp:          true,
				DownloadDuration: next.downloadElapsed,
				PrepareDuration:  next.prepareElapsed,
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
		probeState.liveTail = nextBlockBootstrapLiveTail(prevUTime, time.Now().Unix())

		// Taken before the probe so a block published while it runs is not
		// waited out by the retry below.
		stateWake := r.service.currentStateWakeChan()
		started := time.Now()
		downloaded, source, prepareElapsed, err := r.service.downloadNextChainBlockProbe(r.ctx, prev, prevUTime, &probeState)
		elapsed := time.Since(started)
		downloadElapsed := downloadElapsedExcludingInlinePrepare(elapsed, prepareElapsed)
		if err != nil {
			if source == "" {
				source = SyncBlockSourcePeerProbe
			}
			r.service.observeSyncBlock(SyncBlockObservation{
				Pipeline:         r.method,
				Chain:            syncChainLabel(prev),
				Shard:            syncShardLabel(prev),
				Source:           source,
				Origin:           syncBlockOriginForSource(source),
				Result:           syncBlockResultForError(err),
				CatchUp:          false,
				DownloadDuration: downloadElapsed,
				PrepareDuration:  prepareElapsed,
			})
			if isExpectedRetryError(err) {
				probeState.consecutiveMisses++
				r.service.log.Debug().
					Err(err).
					Str("current", storage.FormatBlockRef(prev)).
					Int("consecutive_misses", probeState.consecutiveMisses).
					Bool("live_tail", probeState.liveTail).
					Msg("next masterchain block is not available during bootstrap")
				if !r.waitBootstrapRetry(stateWake) {
					return
				}
				continue
			}
			r.sendMasterDownload(out, nextMasterDownload{err: err})
			return
		}

		probeState.noteObtained(time.Now(), syncBlockOriginForSource(source) == SyncBlockOriginBroadcast)
		item := nextMasterDownload{
			prev:            prev,
			block:           downloaded,
			source:          source,
			downloadElapsed: downloadElapsed,
			prepareElapsed:  prepareElapsed,
		}
		if !r.sendMasterDownload(out, item) {
			return
		}
		prev = downloaded.ID
		prevUTime = int64(downloaded.Meta.GenUTime)
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

	return prevUTime != 0 && shouldSwitchNextToArchiveByLag(time.Now().Unix()-prevUTime)
}

// waitBootstrapRetry parks until there is a reason to probe again. The caller
// passes the wake it took before the probe that missed, so state published
// while that probe ran is not waited out here.
func (r *nextSyncRunner) waitBootstrapRetry(wake <-chan struct{}) bool {
	if r.service.cellGenerationSwitchRequestActive() {
		return false
	}

	timer := time.NewTimer(currentStateLivePollDelay)
	defer timer.Stop()

	select {
	case <-r.ctx.Done():
		return false
	case <-wake:
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
	// The buffer is doubled because applied blocks feed the commit stage
	// directly; it folds in the capacity of the former pass-through forwarding
	// stage so master apply keeps the same headroom over the slower commit.
	out := make(chan nextAppliedMaster, 2*nextMasterchainPrefetchBlocks)
	start := storage.CloneBlockState(r.master)
	applyCells := newNextMasterApplyCellWindow(func(hash cell.Hash) (*cell.Cell, error) {
		return r.stateCells.loader()(hash)
	}, &r.service.lazyCellLoads)
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
			if err == nil && !applied.syncUntilReached {
				r.logMasterApplied(applied)
			}
			if !r.sendAppliedMaster(out, applied) {
				return
			}
			if err != nil || applied.syncUntilReached {
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
		downloadSource:  item.source,
		downloadElapsed: item.downloadElapsed,
		prepareElapsed:  item.prepareElapsed,
	}
	observe := func(result string, applyDuration time.Duration) {
		r.service.observeSyncBlock(SyncBlockObservation{
			Pipeline:         r.method,
			Chain:            syncChainLabel(item.block.ID),
			Shard:            syncShardLabel(item.block.ID),
			Source:           item.source,
			Origin:           item.block.Origin,
			Result:           result,
			CatchUp:          r.mode == nextSyncToTarget,
			DownloadDuration: item.downloadElapsed,
			PrepareDuration:  item.prepareElapsed,
			ApplyDuration:    applyDuration,
		})
	}

	consensusStarted := time.Now()
	applyTiming := masterchainApplyTiming{
		prepare: item.block.StateUpdateToCellsElapsed,
	}
	if r.service.preparedMasterBlockAfterSyncUntil(item.block) {
		applied.block = item.block
		applied.syncUntilReached = true
		return applied, nil
	}

	checked, err := r.service.checkedConsensusForPreparedBlock(master, item.block)
	if err != nil {
		applyTiming.consensus = time.Since(consensusStarted)
		applyTiming.total = applyTiming.consensus
		applied.err = fmt.Errorf("validate masterchain consensus for %s: %w", item.block.BlockRef(), err)
		observe(syncBlockResultForError(applied.err), applyTiming.total)
		return applied, applied.err
	}
	applyTiming.consensus = time.Since(consensusStarted)
	applyTiming.total = applyTiming.consensus
	item.block.consensusChecked = checked
	prepared := item.block
	applied.block = prepared

	targets, elapsed, err := r.shardTargetsForMasterBlock(prepared)
	applied.shardTargets = targets
	applied.shardTargetsParsed = true
	applied.shardTargetParse = elapsed
	if err != nil {
		applied.err = err
		observe(syncBlockResultForError(err), applyTiming.total)
		return applied, err
	}
	applied.shardPrefetchTargets = r.scheduleShardPrefetch(prepared.ID, targets)

	nextMaster, transitionTiming, err := r.service.applyMasterchainTransition(r.ctx, master, prepared, checked, applyCells, &blockApplyHookMeta{})
	applyTiming.prepare += transitionTiming.prepare
	applyTiming.consensus += transitionTiming.consensus
	applyTiming.stateUpdate += transitionTiming.stateUpdate
	applyTiming.total += transitionTiming.total
	applied.applyTiming = applyTiming
	if err != nil {
		applied.err = err
		observe(syncBlockResultForError(err), applyTiming.total)
		return applied, err
	}

	if err = r.service.updateMasterDependentCachesForKeyBlock(nextMaster, &prepared); err != nil {
		applied.err = err
		observe(syncBlockResultForError(err), applyTiming.total)
		return applied, err
	}

	// Publish the applied state to the caches the next block waits on before
	// doing anything else with it: the next masterchain broadcast cannot be
	// state-decompressed and its consensus proof cannot be validated until this
	// state is visible, so every millisecond spent here lands on the live head.
	// The key-block config above stays ahead of it because the FastSync roster
	// reconcile below reads it.
	r.service.rememberAppliedMasterCompressedState(nextMaster)
	// Cheap and ordering-sensitive: the active overlay set is computed at this
	// depth, and the next shard download resolves its overlay through it.
	r.service.updateP2PMonitorMinSplitDepth(nextMaster)
	// Both background consumers below retain this slice; neither may sort,
	// dedupe or otherwise mutate it, and neither may the commit stage.
	r.service.scheduleP2PShardOverlayUpdate(nextMaster, targets)

	r.service.publishLiveBlockArtifacts(prepared, nextMaster, liveBlockPublishOptions{})
	r.service.rememberAppliedMasterchainState(nextMaster)
	r.service.rememberSeenMasterchainBlock(nextMaster.Block)
	// Strictly after this master is published: the stage stamps the shard blocks
	// it applies with this master as their inclusion ref, and a proof built for
	// such a block resolves that ref by seqno, so the master must be visible
	// first. Everything the commit stage does with these targets afterwards is
	// then already done or in flight.
	r.scheduleShardApplyAhead(nextMaster, targets)
	applyCells.remember(prepared.ID, prepared.StateUpdateToCells)
	applied.master = nextMaster
	applied.stateUpdateCells = prepared.StateUpdateToCells
	applied.releaseApplyCells = func() {
		applyCells.forget(prepared.ID)
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

func (r *nextSyncRunner) shardTargetsForMasterBlock(block PreparedBlock) ([]ton.BlockIDExt, time.Duration, error) {
	started := time.Now()
	parsed, err := parsePreparedBlock(block)
	if err != nil {
		elapsed := time.Since(started)
		return nil, elapsed, fmt.Errorf("parse next-block master block %s: %w", block.BlockRef(), err)
	}
	targets, err := state.ShardBlocksFromMasterBlock(block.ID, parsed)
	elapsed := time.Since(started)
	if err != nil {
		return nil, elapsed, fmt.Errorf("parse next-block shard targets from master block %s: %w", block.BlockRef(), err)
	}
	return targets, elapsed, nil
}

func (r *nextSyncRunner) scheduleShardPrefetch(master ton.BlockIDExt, targets []ton.BlockIDExt) int {
	count := 0
	for _, target := range targets {
		key := storage.BlockKey(target)
		if _, ok := r.shardPrefetchScheduled[key]; ok {
			continue
		}
		if !r.prefetchShardTarget(master, target) {
			continue
		}
		rememberScheduledShardPrefetch(r.shardPrefetchScheduled, &r.shardPrefetchOrder, key)
		count++
	}
	return count
}

func rememberScheduledShardPrefetch(scheduled map[storage.BlockRootHash]struct{}, scheduledOrder *[]storage.BlockRootHash, key storage.BlockRootHash) {
	if _, ok := scheduled[key]; ok {
		return
	}

	scheduled[key] = struct{}{}
	*scheduledOrder = append(*scheduledOrder, key)
	if len(scheduled) <= nextShardPrefetchScheduleLimit {
		return
	}

	evict := (*scheduledOrder)[0]
	delete(scheduled, evict)
	copy(*scheduledOrder, (*scheduledOrder)[1:])
	*scheduledOrder = (*scheduledOrder)[:len(*scheduledOrder)-1]
}

func (r *nextSyncRunner) prefetchShardTarget(master ton.BlockIDExt, target ton.BlockIDExt) bool {
	if !r.tryAcquireShardPrefetchSlot() {
		return false
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
	return true
}

func (r *nextSyncRunner) tryAcquireShardPrefetchSlot() bool {
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
	<-r.shardPrefetchSlots
}

func (r *nextSyncRunner) startShardDescriptionPrefetch() {
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

type shardCurrentBlocksLookup struct {
	blocks []ton.BlockIDExt
	err    error
}

func (r *nextSyncRunner) prefetchShardDescriptionHints() {
	r.shardDescriptionPrefetchMu.Lock()
	defer r.shardDescriptionPrefetchMu.Unlock()

	hints := r.service.shardDescriptionHintSnapshot(time.Now())
	// currentBlocksForBlock takes the shard resolver mutex and clones the
	// matched block IDs; resolve every shard once per pass instead of per hint.
	currentByShard := make(map[storage.ShardKey]shardCurrentBlocksLookup)
	for _, hint := range hints {
		desc := hint.Description

		// Already-scheduled hints need no validation; check before touching the
		// shard resolver.
		key := storage.BlockKey(desc.Block)
		if _, ok := r.shardDescriptionPrefetchScheduled[key]; ok {
			continue
		}

		shardKey := storage.ShardKeyFromBlock(desc.Block)
		lookup, ok := currentByShard[shardKey]
		if !ok {
			lookup.blocks, lookup.err = r.shardResolver.currentBlocksForBlock(desc.Block)
			currentByShard[shardKey] = lookup
		}
		err := lookup.err
		if err == nil {
			err = validateShardDescriptionPrefetchAgainst(&desc, lookup.blocks)
		}
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if errors.Is(err, errShardDescriptionTooNew) {
			continue
		}
		if errors.Is(err, errShardDescriptionTooOld) {
			r.service.dropShardDescriptionHint(desc.Block)
			continue
		}
		if err != nil {
			r.service.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(desc.Block)).
				Str("overlay", hint.Overlay).
				Msg("dropping shard block description broadcast")
			r.service.dropShardDescriptionHint(desc.Block)
			continue
		}

		if !r.prefetchShardDescriptionTarget(hint, &desc) {
			continue
		}
		rememberScheduledShardPrefetch(r.shardDescriptionPrefetchScheduled, &r.shardDescriptionPrefetchOrder, key)
	}
}

func validateShardDescriptionPrefetchAgainst(desc *p2p.ShardBlockDescription, currentBlocks []ton.BlockIDExt) error {
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

func shardDescriptionAnchorsCurrent(desc *p2p.ShardBlockDescription, currentBlocks []ton.BlockIDExt) bool {
	for _, current := range currentBlocks {
		if !shardDescriptionContainsBlock(desc, current) && !shardDescriptionContainsPrevRef(desc, current) {
			return false
		}
	}
	return len(currentBlocks) > 0
}

func shardDescriptionContainsBlock(desc *p2p.ShardBlockDescription, block ton.BlockIDExt) bool {
	for _, link := range desc.Chain {
		if link.Block.Equals(&block) {
			return true
		}
	}
	return false
}

func shardDescriptionContainsPrevRef(desc *p2p.ShardBlockDescription, block ton.BlockIDExt) bool {
	for _, link := range desc.Chain {
		for _, prev := range link.PrevRefs {
			if prev.Equals(&block) {
				return true
			}
		}
	}
	return false
}

func (r *nextSyncRunner) prefetchShardDescriptionTarget(hint shardDescriptionHint, desc *p2p.ShardBlockDescription) bool {
	if !r.tryAcquireShardPrefetchSlot() {
		return false
	}

	r.service.log.Debug().
		Stringer("block", storage.BlockRef(desc.Block)).
		Str("overlay", hint.Overlay).
		Int("chain_links", len(desc.Chain)).
		Msg("prefetching shard block from description broadcast")

	go func() {
		defer r.releaseShardPrefetchSlot()

		err := r.service.node.PrefetchShardBlockFullFromBroadcastHint(r.ctx, desc.Block)
		if err != nil {
			if r.ctx.Err() == nil {
				r.service.log.Debug().
					Err(err).
					Str("block", storage.FormatBlockRef(desc.Block)).
					Str("overlay", hint.Overlay).
					Msg("shard block description prefetch failed")
			}
			return
		}
		r.service.prepareShardBlockAheadByID(r.ctx, desc.Block)
	}()
	return true
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
			if item.syncUntilReached {
				if err := r.flushStagedCurrent(); err != nil {
					return r.current, err
				}
				r.service.enterSyncUntilOffline(r.current, item.block)
				r.cancel()
				return r.current, nil
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
}

func (r *nextSyncRunner) flushStagedCurrent() error {
	return r.flushStaged(flushStagedAsync, "")
}

func (r *nextSyncRunner) flushStagedCurrentSync(reason string) error {
	return r.flushStaged(flushStagedSync, reason)
}

func (r *nextSyncRunner) tryFlushStagedCurrent() error {
	return r.flushStaged(flushStagedTry, "")
}

type flushStagedMode int

const (
	// flushStagedAsync blocks on the persist mutex and persists asynchronously.
	flushStagedAsync flushStagedMode = iota
	// flushStagedTry skips the checkpoint when the persist mutex is busy.
	flushStagedTry
	// flushStagedSync blocks on the persist mutex and persists synchronously.
	flushStagedSync
)

func (r *nextSyncRunner) flushStaged(mode flushStagedMode, reason string) error {
	if r.stagedBlocks == 0 {
		return nil
	}
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		return err
	}

	queuedAt := time.Now()
	if mode == flushStagedTry {
		if !r.service.currentStatePersistMu.TryLock() {
			r.logCheckpointDeferred()
			return nil
		}
	} else {
		r.service.currentStatePersistMu.Lock()
	}
	lockElapsed := time.Since(queuedAt)
	r.timing.persist += lockElapsed

	checkpoint, artifactPrewriteTarget := r.checkpoint()
	cells := r.stateCells.beginCheckpoint()
	releaseCells := func() {}
	if loader := cells.retainedLoader(r.service.stateCellLoader()); loader != nil {
		releaseCells = r.service.retainStateCellLoader(loader)
	}
	onCommitted := func() {
		r.completeCheckpoint(checkpoint)
	}

	var next *storage.CurrentState
	var err error
	if mode == flushStagedSync {
		next, err = r.service.persistNextBlockCurrentStateSyncLocked(r.current, &r.timing, reason, checkpoint.entries, cells, artifactPrewriteTarget, onCommitted, releaseCells, lockElapsed)
	} else {
		next, err = r.service.persistNextBlockCurrentStateLocked(r.current, &r.timing, checkpoint.entries, cells, artifactPrewriteTarget, onCommitted, releaseCells, lockElapsed, queuedAt)
	}
	if err != nil {
		releaseCells()
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

	commitCtx := r.shardApplyAheadContext(r.ctx, item.master.Block)
	nextCurrent, shardStats, err := r.service.currentStateForNextMasterState(commitCtx, r.current, item.master, item.shardTargets, r.shardResolver)
	r.observeMasterShardObtain(item, shardStats, err)
	if err != nil {
		return err
	}
	shardStats.targetParse += item.shardTargetParse
	resolverStats := r.takeShardResolverStats()
	shardStats.apply += resolverStats.applyElapsed
	shardStats.applied += resolverStats.blocksApplied

	r.current = nextCurrent
	r.shardResolver.updateCurrent(r.current.Shards)
	r.committedMasterSeqno.Store(r.current.Masterchain.Block.SeqNo)
	// The committed head moved, so a master the stage had to skip may now be
	// admissible without it receiving a new schedule.
	r.wakeShardApplyAhead()
	r.prefetchShardDescriptionHints()
	// Publish the head before any durability staging: staging below contains
	// the prewriter enqueues, which block under disk backpressure, and nothing
	// in the publish path reads state the staging creates — the master's apply
	// cells stay resolvable through the private apply window until
	// rememberMasterCheckpointState moves them into the shared one.
	snapshot := r.service.publishLiveCurrentStateChanged(r.current)
	if r.service.liveState != nil {
		if snapshot == nil {
			// The status publish rejected the state as behind; the live view has
			// its own monotonic gate, so keep feeding it a private clone.
			snapshot = storage.CloneCurrentState(r.current)
		}
		r.service.liveState.SetLiveCurrentStateSnapshot(snapshot)
	}

	// Shards strictly before their inclusion master, and both before any
	// checkpoint this commit can trigger (the flushes below are the only ones
	// on this runner), so no checkpoint can contain a shard block without its
	// staged master.
	if err = r.stageDeferredShardCheckpointStates(item.master.Block.SeqNo); err != nil {
		return err
	}
	if err = r.rememberMasterCheckpointState(item); err != nil {
		return err
	}
	r.shardTarget += shardStats.targetParse
	r.shardWall += shardStats.wall
	r.shardApply += shardStats.apply
	r.shardApplied += uint64(shardStats.applied)
	r.committed++
	r.stagedBlocks++

	if r.reachedTarget() || r.shouldBackpressureStagedCurrent() {
		if err := r.flushStagedCurrent(); err != nil {
			return err
		}
	} else if r.shouldCheckpointStagedCurrent() {
		if err := r.tryFlushStagedCurrent(); err != nil {
			return err
		}
	}

	r.logShardCommit(item, shardStats, blockStarted, masterPipelineWait, appliedQueue, applyBefore, persistBefore, checkpointsBefore, shardTargetBefore, shardWallBefore, shardApplyBefore, shardAppliedBefore)
	r.logProgressIfNeeded()
	return nil
}

func (r *nextSyncRunner) observeMasterShardObtain(item nextAppliedMaster, shardStats nextShardClientApplyStats, err error) {
	catchUp := r.mode == nextSyncToTarget
	r.service.observeSyncObtain(SyncObtainObservation{
		Pipeline: r.method,
		Stage:    "master",
		Result:   "success",
		CatchUp:  catchUp,
		Duration: item.downloadElapsed,
	})
	r.service.observeSyncObtain(SyncObtainObservation{
		Pipeline: r.method,
		Stage:    "shards",
		Result:   syncBlockResultForError(err),
		CatchUp:  catchUp,
		Duration: shardStats.obtain,
	})
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
		Uint32("checkpoint_blocks", r.service.nextBlockCheckpointBlocks).
		Uint64("checkpoint_bytes", r.service.checkpointBytes).
		Uint32("sync_backpressure_windows", r.service.syncBackpressureWindows).
		Uint32("checkpoint_backpressure_blocks", checkpointBackpressureBlocks(r.service.nextBlockCheckpointBlocks, r.service.syncBackpressureWindows)).
		Uint64("checkpoint_backpressure_bytes", checkpointBackpressureBytes(r.service.checkpointBytes, r.service.syncBackpressureWindows)).
		Msg("next-block checkpoint deferred because current state persist is busy")
}

func (r *nextSyncRunner) shouldCheckpointStagedCurrent() bool {
	if r.stagedBlocks >= r.service.nextBlockCheckpointBlocks {
		return true
	}
	return r.pendingCheckpointBytes() >= r.service.checkpointBytes
}

func (r *nextSyncRunner) shouldBackpressureStagedCurrent() bool {
	windows := r.service.syncBackpressureWindows
	if r.stagedBlocks >= checkpointBackpressureBlocks(r.service.nextBlockCheckpointBlocks, windows) {
		return true
	}
	return r.pendingCheckpointBytes() >= checkpointBackpressureBytes(r.service.checkpointBytes, windows)
}

func (r *nextSyncRunner) pendingCheckpointBytes() uint64 {
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

func (r *nextSyncRunner) afterApplyShardState(ctx context.Context, state *storage.BlockState, downloaded PreparedBlock, applyElapsed time.Duration) (err error) {
	// Resolved before the observation defer on purpose: a missing inclusion
	// master is a wiring bug, not a sync outcome worth reporting as one.
	master, err := shardApplyMasterFromContext(ctx)
	if err != nil {
		return err
	}

	observation := SyncBlockObservation{
		Pipeline:        r.method,
		Chain:           syncChainLabel(downloaded.ID),
		Shard:           syncShardLabel(downloaded.ID),
		Source:          SyncBlockSourceNextBlock,
		Origin:          downloaded.Origin,
		Result:          "success",
		CatchUp:         r.mode == nextSyncToTarget,
		PrepareDuration: downloaded.PrepareElapsed,
		ApplyDuration:   applyElapsed,
	}
	defer func() {
		if err != nil {
			observation.Result = syncBlockResultForError(err)
		}
		r.service.observeSyncBlock(observation)
	}()

	setShardStateMasterchainRef(state, master.Block)
	setShardBlockMasterchainRef(downloaded.Meta, master.Block)

	// Same reason as the masterchain path: the next shard broadcast for this
	// shard is state-compressed against this state, so publish it for decode
	// before the (heavier) live-view publish.
	if r.service.RememberCompressedBlockState(state) {
		r.service.node.NotifyCompressedBlockStateReady(state.Block)
	}
	r.service.publishLiveBlockArtifacts(downloaded, state, liveBlockPublishOptions{})

	splitDepth, err := r.service.cachedMonitorMinSplitDepth(master, downloaded.ID.Workchain)
	if err != nil {
		return err
	}
	// Durability staging is deferred to the commit of the inclusion master:
	// everything above is caches and live visibility, but a staged checkpoint
	// entry would let a checkpoint persist this shard block while the master
	// its MasterchainRefSeqno points to is still uncommitted, leaving a
	// dangling ref after an unclean restart.
	artifact, links := preparedBlockCheckpointArtifacts(downloaded, splitDepth)
	r.deferShardCheckpointState(master.Block.SeqNo, state, artifact, links)
	return nil
}

// deferredShardCheckpointState is a resolved shard block whose checkpoint
// staging waits for the commit of its inclusion master. The artifact payloads
// alias the prepared block's immutable buffers and the state is retained by
// the resolver cache anyway, so deferral adds no copies; the map holds at most
// the targets of the masters in flight (the apply-ahead stage resolves one
// master at a time plus the commit's own) and dies with the runner when a run
// stops before committing them.
type deferredShardCheckpointState struct {
	state    *storage.BlockState
	artifact *storage.ServedBlockFull
	links    []storage.ServedBlockLink
}

func (r *nextSyncRunner) deferShardCheckpointState(masterSeqno uint32, state *storage.BlockState, artifact *storage.ServedBlockFull, links []storage.ServedBlockLink) {
	r.shardStageMu.Lock()
	if r.shardStageDeferred == nil {
		r.shardStageDeferred = map[uint32][]deferredShardCheckpointState{}
	}
	r.shardStageDeferred[masterSeqno] = append(r.shardStageDeferred[masterSeqno], deferredShardCheckpointState{
		state:    state,
		artifact: artifact,
		links:    links,
	})
	r.shardStageMu.Unlock()
}

// stageDeferredShardCheckpointStates stages the shard blocks resolved for this
// master. Called by commitOne after the master's shard resolution completed —
// every afterApplyShardState for these targets runs inside the resolver tasks
// that resolution waits on, so the deferred set is complete by then, whether
// the apply-ahead stage or the commit stage owned the tasks.
func (r *nextSyncRunner) stageDeferredShardCheckpointStates(masterSeqno uint32) error {
	r.shardStageMu.Lock()
	deferred := r.shardStageDeferred[masterSeqno]
	delete(r.shardStageDeferred, masterSeqno)
	r.shardStageMu.Unlock()

	for _, shard := range deferred {
		if err := r.rememberCheckpointState(shard.state, shard.artifact, shard.links); err != nil {
			return err
		}
	}
	return nil
}

func (r *nextSyncRunner) rememberMasterCheckpointState(item nextAppliedMaster) error {
	// Master apply-ahead keeps future cells private. Move this block's prepared
	// cells into the checkpoint window only when its state metadata is remembered,
	// so every metadata commit is paired with cells from the same checkpoint.
	if err := r.stateCells.addPreparedRecords(item.stateUpdateCells); err != nil {
		return err
	}
	if item.releaseApplyCells != nil {
		item.releaseApplyCells()
	}
	artifact, links := preparedBlockCheckpointArtifacts(item.block, 0)
	return r.rememberCheckpointState(item.master, artifact, links)
}

func (r *nextSyncRunner) rememberCheckpointState(state *storage.BlockState, artifact *storage.ServedBlockFull, links []storage.ServedBlockLink) error {
	r.checkpointMu.Lock()
	r.checkpointStates.rememberWithArtifacts(state, artifact, links)
	// Stream the artifact append ahead of the checkpoint while holding
	// checkpointMu, so a checkpoint snapshot always covers exactly the staged
	// entries whose prewrite sequence it waits for. Only the append runs under
	// the mutex; the queue's backpressure wait runs after the unlock, so a
	// prewriter stalled on disk still throttles this producer but cannot block
	// the async checkpoint completion, which takes checkpointMu.
	seq, wait, err := r.service.artifactPrewrite.enqueueDetached(state, artifact)
	if err == nil && seq > r.artifactPrewriteSeq {
		r.artifactPrewriteSeq = seq
	}
	r.checkpointMu.Unlock()
	if err != nil {
		return err
	}
	return wait()
}

func (r *nextSyncRunner) checkpoint() (appliedStateCheckpoint, uint64) {
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	return r.checkpointStates.checkpoint(), r.artifactPrewriteSeq
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
	if blockUTime == 0 {
		return false
	}
	lagSeconds := time.Now().Unix() - blockUTime
	if !shouldSwitchNextToArchiveByLag(lagSeconds) {
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
	// Everything below is log-only: skip the head lookup and the field
	// formatting entirely when the level is off, since this runs per applied
	// masterchain block on the live-head path.
	event := r.service.log.Debug()
	if !event.Enabled() {
		return
	}

	latest, err := r.latestTarget(item.master.Block.SeqNo)
	if err != nil {
		latest = item.master.Block
	}
	if latest.SeqNo < item.master.Block.SeqNo {
		latest = item.master.Block
	}

	blockUTime, lagSeconds := downloadedBlockTimeLag(item.block, time.Now())
	elapsed := item.downloadElapsed + item.prepareElapsed + item.applyTiming.total

	event = event.
		Str("block", item.block.BlockRef()).
		Str("latest_masterchain", storage.FormatBlockRef(latest)).
		Str("catchup_method", r.method).
		Str("download_source", string(item.downloadSource)).
		Dur("download_elapsed", item.downloadElapsed).
		Dur("prepare_elapsed", item.prepareElapsed).
		Dur("apply_elapsed", item.applyTiming.total).
		Dur("master_prepare_elapsed", item.applyTiming.prepare).
		Dur("master_consensus_elapsed", item.applyTiming.consensus).
		Dur("master_state_update_elapsed", item.applyTiming.stateUpdate).
		Dur("elapsed", elapsed)
	if blockUTime != 0 {
		event.Uint32("block_utime", blockUTime).Int64("lag_seconds", lagSeconds)
	}
	event.Msg("next-block masterchain head applied")
}

func (r *nextSyncRunner) logShardCommit(item nextAppliedMaster, shardStats nextShardClientApplyStats, blockStarted time.Time, masterPipelineWait time.Duration, appliedQueue int, applyBefore time.Duration, persistBefore time.Duration, checkpointsBefore uint32, shardTargetBefore time.Duration, shardWallBefore time.Duration, shardApplyBefore time.Duration, shardAppliedBefore uint64) {
	blockElapsed := time.Since(blockStarted)

	// The lag fields feed the periodic Info progress log, so they are computed
	// unconditionally; everything else here is Debug-only and runs once per
	// committed masterchain block on the live-head path.
	blockUTime, lagSeconds := downloadedBlockTimeLag(item.block, time.Now())
	r.lastBlockUTime = blockUTime
	r.lastLagSeconds = lagSeconds
	r.hasBlockTime = blockUTime != 0

	event := r.service.log.Debug()
	if !event.Enabled() {
		return
	}

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

	event = event.
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
		Dur("shard_obtain_elapsed", shardStats.obtain).
		Dur("shard_target_parse_elapsed", r.shardTarget-shardTargetBefore).
		Dur("shard_apply_elapsed", r.shardApply-shardApplyBefore).
		Uint64("shard_blocks_applied", r.shardApplied-shardAppliedBefore).
		Int("shard_blocks_downloaded", shardStats.downloaded).
		Dur("elapsed", blockElapsed).
		Str("speed", formatBlockRate(1, blockElapsed)).
		Bool("checkpoint", r.timing.checkpoints > checkpointsBefore)
	if blockUTime != 0 {
		event.Uint32("block_utime", blockUTime).Int64("lag_seconds", lagSeconds)
	}
	if shardStats.resolverWait > 0 {
		event.
			Dur("shard_resolver_wait_elapsed", shardStats.resolverWait)
	}
	event.Msg("next-block shard-client state synced")
}

func (r *nextSyncRunner) logProgressIfNeeded() {
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
	basechainLagSeconds, basechainShards := currentBasechainLagSeconds(now, r.current)

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
		if basechainShards > 0 {
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
	r.timing.reset(r.lastLog)
}

func currentBasechainLagSeconds(now time.Time, current *storage.CurrentState) (int64, int) {
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
	return maxLag, shards
}
