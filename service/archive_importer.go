package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	archiveCatchUpProgressInterval = 5 * time.Second

	DefaultArchiveCatchUpCheckpointBlocks = 2000
	DefaultArchiveCatchUpCheckpointPeriod = 2 * time.Minute
	DefaultArchiveCatchUpPrefetchWindows  = 8

	archiveShardApplyParallelism              = 8
	archiveMasterConsensusPrecheckParallelism = 8
	archiveMasterLookaheadWindows             = 16
	archiveHotLookaheadWindows                = 2
	archiveReadyWindowBacklog                 = 4
	archiveDownloadWorkerMin                  = 16
	archiveDownloadWorkerMax                  = 64
	archiveDownloadWorkerMultiplier           = 4
	archiveDownloadHotWorkerMin               = 2
	archiveDownloadHotWorkerMax               = 8
	archivePrepareHotWorkerMin                = 1
	archivePrepareHotWorkerMax                = 2
	archiveCatchUpMaxAdaptiveCheckpointBlocks = 4000
	archiveCatchUpCheckpointSlowThreshold     = time.Second
	archiveCatchUpCheckpointFastThreshold     = 400 * time.Millisecond
	archiveCheckpointWaitLogInterval          = 10 * time.Second
)

type archiveImportResult struct {
	stats      *archive.ImportStats
	blocks     map[string]p2p.DownloadedBlock
	stored     storage.ServedArchiveImport
	splitDepth uint32
}

type archiveImportCacheKey struct {
	masterchainSeqno uint32
	shard            archive.ShardID
	splitDepth       uint32
}

type archiveImportWaiter struct {
	done   chan struct{}
	result *archiveImportResult
	err    error
}

type archiveImportDownload struct {
	imported *archiveImportResult
	cached   bool
}

type archiveImportCacheLoad struct {
	archiveImportDownload
	retry bool
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
	imported        *archiveImportResult
	masterSequence  []p2p.DownloadedBlock
	consensusProofs map[uint32]*masterchainConsensusProof
	precheckElapsed time.Duration
	err             error
}

type archiveWindowPipeline struct {
	cancel context.CancelFunc
	done   chan archiveWindowResult
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
	done chan error
	err  error
}

type archiveImportPriority uint8

const (
	archiveImportPriorityHot archiveImportPriority = iota
	archiveImportPriorityPrefetch
)

type archiveImportQueue struct {
	downloadHot      chan archiveDownloadJob
	downloadPrefetch chan archiveDownloadJob
	prepareHot       chan archivePrepareJob
	preparePrefetch  chan archivePrepareJob
}

type archiveDownloadJob struct {
	ctx              context.Context
	masterchainSeqno uint32
	shard            archive.ShardID
	splitDepth       uint32
	priority         archiveImportPriority
	done             chan archiveImportQueueResult
}

type archivePrepareJob struct {
	ctx        context.Context
	downloaded *archive.Downloaded
	splitDepth uint32
	priority   archiveImportPriority
	done       chan archiveImportQueueResult
}

type archiveImportQueueResult struct {
	imported *archiveImportResult
	err      error
}

type archiveCheckpointResult struct {
	persisted        *storage.CurrentState
	states           appliedStateCheckpoint
	checkpointBlocks uint32
	elapsed          time.Duration
	reason           string
	err              error
}

type archiveCatchUpProgressStats struct {
	windows            uint64
	shardArchives      uint64
	checkpoints        uint64
	bytes              int64
	blocks             uint64
	entries            uint64
	pipelineWait       time.Duration
	masterPrefetchWait time.Duration
	archiveDownload    time.Duration
	archiveImport      time.Duration
	applyWall          time.Duration
	masterApply        time.Duration
	masterPrecheck     time.Duration
	masterPrepare      time.Duration
	masterConsensus    time.Duration
	masterStateUpdate  time.Duration
	shardTargetParse   time.Duration
	shardApply         time.Duration
	stateCells         uint64
	stateCellPrepare   time.Duration
	checkpointPersist  time.Duration
}

type archiveCatchUpStageTiming struct {
	name    string
	elapsed time.Duration
}

type archiveShardBlockLoader struct {
	master ton.BlockIDExt
	blocks map[string]p2p.DownloadedBlock
	mu     *sync.Mutex
}

type shardClientArchiveWindow struct {
	startSeqno     uint32
	masterStats    *archive.ImportStats
	totalStats     *archive.ImportStats
	masterStates   map[uint32]*storage.BlockState
	masterBlocks   map[uint32]p2p.DownloadedBlock
	masterSequence []p2p.DownloadedBlock
	masterProofs   map[uint32]*masterchainConsensusProof
	archiveBlocks  map[string]p2p.DownloadedBlock
	archiveImports []*archiveImportResult
	appliedStates  appliedStateSet
	shardArchives  int
	splitDepth     uint32
	masterWait     time.Duration

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

type archiveCatchUpRunner struct {
	service *Service
	ctx     context.Context
	current *storage.CurrentState
	target  ton.BlockIDExt

	importCache *archiveImportCache
	pipeline    *archiveWindowPipeline

	started                        time.Time
	startSeqno                     uint32
	lastProgress                   time.Time
	lastProgressSeqno              uint32
	lastCheckpoint                 time.Time
	lastCheckpointSeqno            uint32
	checkpointBlocksTarget         uint32
	startRemainingLagSeconds       int64
	hasStartRemainingLag           bool
	shardBlocksApplied             uint64
	shardBlocksReused              uint64
	lastProgressShardBlocksApplied uint64
	lastProgressShardBlocksReused  uint64
	progressStats                  archiveCatchUpProgressStats
	lastProgressStats              archiveCatchUpProgressStats
	pipelineWaitStarted            time.Time
	checkpointDone                 chan archiveCheckpointResult
	checkpointStates               appliedStateSet
	stateCells                     *archiveStateCellOverlay
}

func newArchiveImportCache() *archiveImportCache {
	return &archiveImportCache{
		entries: map[archiveImportCacheKey]*archiveImportResult{},
		waiters: map[archiveImportCacheKey]*archiveImportWaiter{},
	}
}

func (c *archiveImportCache) load(ctx context.Context, key archiveImportCacheKey, load func(context.Context) (*archiveImportResult, error)) (archiveImportDownload, error) {
	if c == nil {
		imported, err := load(ctx)
		return archiveImportDownload{imported: imported}, err
	}

	for {
		loaded, err := c.loadOnce(ctx, key, load)
		if !loaded.retry {
			return loaded.archiveImportDownload, err
		}
	}
}

func (c *archiveImportCache) loadOnce(ctx context.Context, key archiveImportCacheKey, load func(context.Context) (*archiveImportResult, error)) (archiveImportCacheLoad, error) {
	c.mu.Lock()
	if result := c.entries[key]; result != nil {
		c.hitCount++
		c.mu.Unlock()
		return archiveImportCacheLoad{
			archiveImportDownload: archiveImportDownload{
				imported: cloneArchiveImportResult(result),
				cached:   true,
			},
		}, nil
	}
	if waiter := c.waiters[key]; waiter != nil {
		c.mu.Unlock()
		select {
		case <-waiter.done:
			if waiter.err != nil {
				if errors.Is(waiter.err, context.Canceled) && ctx.Err() == nil {
					return archiveImportCacheLoad{retry: true}, nil
				}
				return archiveImportCacheLoad{}, waiter.err
			}
			return archiveImportCacheLoad{
				archiveImportDownload: archiveImportDownload{
					imported: cloneArchiveImportResult(waiter.result),
					cached:   true,
				},
			}, nil
		case <-ctx.Done():
			return archiveImportCacheLoad{}, ctx.Err()
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

	return archiveImportCacheLoad{archiveImportDownload: archiveImportDownload{imported: result}}, err
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

func (l archiveShardBlockLoader) load(_ context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	if block.SeqNo == 0 {
		return p2p.DownloadedBlock{}, fmt.Errorf("zerostate %s is missing", storage.FormatBlockRef(block))
	}

	key := storage.BlockKey(block)
	downloaded, ok := l.block(key)
	if !ok {
		return p2p.DownloadedBlock{}, fmt.Errorf("no archive data/proof for shard block %s", storage.FormatBlockRef(block))
	}
	downloaded, err := prepareArchiveDownloadedBlock(downloaded)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	downloaded = l.rememberPrepared(key, downloaded)
	if downloaded.Meta == nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("archive shard block %s is missing metadata", downloaded.BlockRef())
	}
	if downloaded.Meta.MasterchainRef != nil && downloaded.Meta.MasterchainRef.SeqNo > l.master.SeqNo {
		return p2p.DownloadedBlock{}, fmt.Errorf("archive shard block %s references future masterchain block %s", downloaded.BlockRef(), storage.FormatBlockRef(*downloaded.Meta.MasterchainRef))
	}
	return downloaded, nil
}

func (l archiveShardBlockLoader) block(key string) (p2p.DownloadedBlock, bool) {
	if l.mu == nil {
		downloaded, ok := l.blocks[key]
		return downloaded, ok
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	downloaded, ok := l.blocks[key]
	return downloaded, ok
}

func (l archiveShardBlockLoader) rememberPrepared(key string, downloaded p2p.DownloadedBlock) p2p.DownloadedBlock {
	if l.mu == nil {
		l.blocks[key] = downloaded
		return downloaded
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	current := l.blocks[key]
	if current.Block != nil && current.Meta != nil {
		return current
	}
	l.blocks[key] = downloaded
	return downloaded
}

func prepareArchiveDownloadedBlock(downloaded p2p.DownloadedBlock) (p2p.DownloadedBlock, error) {
	if downloaded.Block != nil {
		return prepareDownloadedBlock(downloaded)
	}
	if len(downloaded.BlockBOC) == 0 {
		return p2p.DownloadedBlock{}, fmt.Errorf("archive block %s is missing block data", downloaded.BlockRef())
	}

	prepared, err := prepareBlockData(downloaded.Kind, downloaded.ID, downloaded.BlockBOC)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	prepared.Proof = downloaded.Proof
	prepared.ProofBOC = downloaded.ProofBOC
	prepared.BroadcastSignatures = downloaded.BroadcastSignatures
	if downloaded.Meta != nil {
		prepared.Meta = downloaded.Meta
	}
	prepared.StateUpdateToCells = downloaded.StateUpdateToCells
	prepared.StateUpdateToCellsElapsed = downloaded.StateUpdateToCellsElapsed
	prepared.IsLink = downloaded.IsLink
	return prepared, nil
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
	runner := &archiveCatchUpRunner{
		service:                s,
		ctx:                    ctx,
		current:                current,
		target:                 target,
		importCache:            newArchiveImportCache(),
		started:                started,
		startSeqno:             current.ShardClientSeqno,
		lastProgress:           started,
		lastProgressSeqno:      current.ShardClientSeqno,
		lastCheckpoint:         started,
		lastCheckpointSeqno:    current.ShardClientSeqno,
		checkpointBlocksTarget: s.archiveCatchUpCheckpointBlocks,
		stateCells:             newArchiveStateCellOverlay(s.stateCellLoader()),
	}
	runner.startRemainingLagSeconds, runner.hasStartRemainingLag = runner.archiveRemainingLagSeconds()

	return runner.run()
}

func (r *archiveCatchUpRunner) run() (*storage.CurrentState, error) {
	s := r.service
	s.log.Info().
		Str("from", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(r.target)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("total_masterchain_blocks", r.target.SeqNo-r.current.ShardClientSeqno).
		Msg("starting archive shard-client catch-up")

	r.pipeline = r.startShardClientArchiveWindowPipeline()
	defer func() {
		if r.pipeline != nil {
			r.pipeline.cancel()
		}
	}()

	handoffToNext := false
	handoffCheckpointBlocks := uint32(0)
	handoffCheckpointStarted := false
	for r.current.ShardClientSeqno < r.target.SeqNo {
		before := r.current.ShardClientSeqno
		window, err := r.nextArchiveWindowWithProgress()
		if err != nil {
			if isArchiveCatchUpRetryError(err) {
				if err = r.restartPipeline(err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, r.returnWithProgress(err)
		}
		if window.startSeqno != r.current.ShardClientSeqno+1 {
			return nil, fmt.Errorf("archive pipeline returned window #%d after current seqno %d", window.startSeqno, r.current.ShardClientSeqno)
		}
		if len(window.masterStates) == 0 {
			return nil, fmt.Errorf("archive window #%d did not provide next masterchain blocks", window.startSeqno)
		}

		applyStarted := time.Now()
		next, err := r.applyShardClientArchiveWindow(r.ctx, r.current, window)
		if err != nil {
			if isArchiveCatchUpRetryError(err) {
				if err = r.restartPipeline(err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, r.returnWithProgress(err)
		}
		applyElapsed := time.Since(applyStarted)
		if err = r.storeArchiveWindow(window); err != nil {
			return nil, r.returnWithProgress(err)
		}
		s.log.Debug().
			Uint32("start_seqno", window.startSeqno).
			Int("master_blocks", len(window.masterStates)).
			Int("archive_blocks", len(window.archiveBlocks)).
			Int("shard_archives", window.shardArchives).
			Int("shard_blocks_applied", window.shardBlocksApplied).
			Int("shard_blocks_reused", window.shardBlocksReused).
			Dur("elapsed", applyElapsed).
			Dur("master_apply_elapsed", window.masterApplyElapsed).
			Dur("master_prepare_elapsed", window.masterPrepareElapsed).
			Dur("master_consensus_elapsed", window.masterConsensusElapsed).
			Dur("master_state_update_elapsed", window.masterStateUpdateElapsed).
			Dur("shard_target_parse_elapsed", window.shardTargetElapsed).
			Dur("shard_apply_elapsed", window.shardApplyElapsed).
			Msg("archive shard-client window applied")

		if next.ShardClientSeqno <= before {
			return nil, fmt.Errorf("archive window #%d did not advance shard client seqno %d", window.startSeqno, before)
		}
		r.current = next
		r.importCache.dropBefore(r.current.ShardClientSeqno + 1)
		r.shardBlocksApplied += uint64(window.shardBlocksApplied)
		r.shardBlocksReused += uint64(window.shardBlocksReused)
		r.recordArchiveWindowProgress(window, applyElapsed)
		r.checkpointStates.rememberAllEntries(window.appliedStates.takeEntries())
		window.releaseImportedData()

		if _, err = r.finishCheckpoint(false); err != nil {
			return nil, err
		}
		if lagSeconds, ok := r.archiveLiveTailLagSeconds(); ok && shouldSwitchArchiveToNextByLag(lagSeconds) {
			if r.checkpointDone == nil {
				handoffCheckpointBlocks, err = r.startArchiveToNextHandoffCheckpoint()
				if err != nil {
					return nil, err
				}
				handoffCheckpointStarted = handoffCheckpointBlocks > 0
			}
			handoffToNext = true
			s.log.Info().
				Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Str("target", storage.FormatBlockRef(r.target)).
				Int64("lag_seconds", lagSeconds).
				Int64("switch_to_next_lag_seconds", archiveToNextLagSeconds).
				Uint32("handoff_checkpoint_blocks", handoffCheckpointBlocks).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Bool("handoff_checkpoint_started", handoffCheckpointStarted).
				Msg("switching from archive catch-up to next-block pipeline")
			break
		}
		if r.checkpointDone == nil && s.shouldPersistArchiveCatchUpCheckpoint(r.current.ShardClientSeqno, r.target.SeqNo, r.lastCheckpointSeqno, r.lastCheckpoint, r.checkpointBlocksTarget) {
			if _, err = r.startCheckpoint("interval"); err != nil {
				return nil, err
			}
		}
		if r.shouldWaitArchiveCheckpointBackpressure() {
			backpressureBlocks := r.archiveCheckpointBackpressureBlocks()
			s.log.Info().
				Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Uint32("shard_client_seqno", r.current.ShardClientSeqno).
				Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
				Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
				Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
				Uint32("checkpoint_backpressure_blocks", backpressureBlocks).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Msg("waiting for archive shard-client checkpoint backpressure")
			if _, err = r.finishCheckpoint(true); err != nil {
				return nil, err
			}
		}

		if err = r.logProgress(); err != nil {
			return nil, err
		}
	}

	if r.current.ShardClientSeqno > r.lastCheckpointSeqno {
		if handoffToNext {
			s.log.Info().
				Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Uint32("shard_client_seqno", r.current.ShardClientSeqno).
				Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
				Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
				Uint32("handoff_checkpoint_blocks", handoffCheckpointBlocks).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Bool("handoff_checkpoint_started", handoffCheckpointStarted).
				Msg("archive shard-client checkpoint continues in background during next-block handoff")
		} else {
			s.log.Info().
				Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
				Uint32("shard_client_seqno", r.current.ShardClientSeqno).
				Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
				Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
				Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
				Uint32("checkpoint_backpressure_blocks", r.archiveCheckpointBackpressureBlocks()).
				Bool("checkpoint_in_flight", r.checkpointDone != nil).
				Msg("persisting final archive shard-client checkpoint")
			if _, err := r.persistCheckpoint("final"); err != nil {
				return nil, err
			}
		}
	}

	doneMsg := "archive shard-client catch-up completed"
	if handoffToNext {
		doneMsg = "archive shard-client catch-up handed off to next-block pipeline"
	}
	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Int("shards", len(r.current.Shards)).
		Msg(doneMsg)
	return r.current, nil
}

func (r *archiveCatchUpRunner) archiveLiveTailLagSeconds() (int64, bool) {
	blockUTime := blockStateUtime(r.ctx, r.service.storage, &r.current.Masterchain)
	return masterchainBlockLagSeconds(blockUTime, time.Now().Unix())
}

func (r *archiveCatchUpRunner) archiveRemainingLagSeconds() (int64, bool) {
	lagSeconds, ok := r.archiveLiveTailLagSeconds()
	if !ok {
		return 0, false
	}
	return remainingLagSeconds(lagSeconds), true
}

func (r *archiveCatchUpRunner) shouldWaitArchiveCheckpointBackpressure() bool {
	if r.checkpointDone == nil {
		return false
	}

	return r.current.ShardClientSeqno-r.lastCheckpointSeqno >= r.archiveCheckpointBackpressureBlocks()
}

func (r *archiveCatchUpRunner) archiveCheckpointBackpressureBlocks() uint32 {
	target := r.checkpointBlocksTarget
	if target == 0 {
		target = r.service.archiveCatchUpCheckpointBlocks
	}
	if target == 0 {
		target = DefaultArchiveCatchUpCheckpointBlocks
	}

	return target * 2
}

func (r *archiveCatchUpRunner) persistCheckpoint(reason string) (uint32, error) {
	var checkpointBlocks uint32
	for {
		completedBlocks, err := r.finishCheckpoint(true)
		checkpointBlocks += completedBlocks
		if err != nil {
			return checkpointBlocks, err
		}
		if r.current.ShardClientSeqno <= r.lastCheckpointSeqno {
			return checkpointBlocks, nil
		}
		if _, err = r.startCheckpoint(reason); err != nil {
			return checkpointBlocks, err
		}
	}
}

func (r *archiveCatchUpRunner) startArchiveToNextHandoffCheckpoint() (uint32, error) {
	if r.current.ShardClientSeqno <= r.lastCheckpointSeqno {
		return 0, nil
	}
	if r.checkpointDone != nil {
		return 0, nil
	}
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		return 0, err
	}

	current := storage.CloneCurrentState(r.current)
	checkpointStates := r.checkpointStates.checkpoint()
	checkpointCells := r.stateCells.beginCheckpoint()
	releaseCheckpointCells := r.service.retainStateCellLoader(checkpointCells.loader())
	checkpointBlocks := r.current.ShardClientSeqno - r.lastCheckpointSeqno
	queuedAt := time.Now()

	r.service.log.Info().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Bool("checkpoint_in_flight", r.checkpointDone != nil).
		Msg("archive shard-client checkpoint scheduled for next-block handoff")

	r.service.runAsync(func() {
		defer releaseCheckpointCells()

		lockStarted := time.Now()
		r.service.currentStatePersistMu.Lock()
		lockElapsed := time.Since(lockStarted)

		if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
			r.service.currentStatePersistMu.Unlock()
			r.recordArchiveToNextHandoffCheckpointError(current, checkpointBlocks, time.Since(queuedAt), err)
			return
		}

		started := time.Now()
		persisted, err := r.persistArchiveCurrentState(current, checkpointBlocks, lockElapsed, checkpointStates.states, checkpointCells, checkpointStates.cells)
		elapsed := time.Since(started)
		r.service.currentStatePersistMu.Unlock()
		if err != nil {
			r.recordArchiveToNextHandoffCheckpointError(current, checkpointBlocks, time.Since(queuedAt), err)
			return
		}
		r.checkpointStates.completeCheckpoint(checkpointStates)
		r.service.publishCommittedCurrentState(persisted)

		r.service.log.Info().
			Str("masterchain", storage.FormatBlockRef(persisted.Masterchain.Block)).
			Uint32("shard_client_seqno", persisted.ShardClientSeqno).
			Uint32("checkpoint_blocks", checkpointBlocks).
			Dur("queued_for", time.Since(queuedAt)).
			Dur("elapsed", elapsed).
			Msg("archive shard-client handoff checkpoint persisted")
	})
	return checkpointBlocks, nil
}

func (r *archiveCatchUpRunner) recordArchiveToNextHandoffCheckpointError(current *storage.CurrentState, checkpointBlocks uint32, queuedFor time.Duration, err error) {
	wrapped := fmt.Errorf("persist archive handoff current state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	r.service.setCurrentStatePersistError(wrapped)
	r.service.log.Error().
		Err(wrapped).
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Dur("queued_for", queuedFor).
		Msg("archive shard-client handoff checkpoint failed")
}

func (r *archiveCatchUpRunner) startCheckpoint(reason string) (uint32, error) {
	if r.checkpointDone != nil || r.current.ShardClientSeqno <= r.lastCheckpointSeqno {
		return 0, nil
	}
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		return 0, err
	}
	current := storage.CloneCurrentState(r.current)
	checkpointStates := r.checkpointStates.checkpoint()
	checkpointCells := r.stateCells.beginCheckpoint()
	releaseCheckpointCells := r.service.retainStateCellLoader(checkpointCells.loader())
	checkpointBlocks := r.current.ShardClientSeqno - r.lastCheckpointSeqno
	lockStarted := time.Now()
	r.service.currentStatePersistMu.Lock()
	lockElapsed := time.Since(lockStarted)
	if err := r.service.checkCurrentStatePersistAllowed(); err != nil {
		r.service.currentStatePersistMu.Unlock()
		releaseCheckpointCells()
		return 0, err
	}

	done := make(chan archiveCheckpointResult, 1)
	r.checkpointDone = done

	r.service.log.Debug().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Str("reason", reason).
		Msg("archive shard-client checkpoint scheduled")

	r.service.runAsync(func() {
		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(releaseCheckpointCells)
		}
		defer release()

		startedCheckpoint := time.Now()
		persisted, err := r.persistArchiveCurrentState(current, checkpointBlocks, lockElapsed, checkpointStates.states, checkpointCells, checkpointStates.cells)
		r.service.currentStatePersistMu.Unlock()
		if err != nil {
			r.service.setCurrentStatePersistError(err)
			r.service.log.Error().
				Err(err).
				Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
				Uint32("shard_client_seqno", current.ShardClientSeqno).
				Uint32("checkpoint_blocks", checkpointBlocks).
				Str("reason", reason).
				Msg("archive shard-client checkpoint failed")
		}
		if persisted != nil {
			r.service.publishCommittedCurrentState(persisted)
		}
		release()
		done <- archiveCheckpointResult{
			persisted:        persisted,
			states:           checkpointStates,
			checkpointBlocks: checkpointBlocks,
			elapsed:          time.Since(startedCheckpoint),
			reason:           reason,
			err:              err,
		}
	})
	return checkpointBlocks, nil
}

func (r *archiveCatchUpRunner) finishCheckpoint(wait bool) (uint32, error) {
	if r.checkpointDone == nil {
		return 0, nil
	}

	done := r.checkpointDone
	var result archiveCheckpointResult
	waitStarted := time.Now()
	if wait {
		result = archiveCheckpointResult{}
		ticker := time.NewTicker(archiveCheckpointWaitLogInterval)
		defer ticker.Stop()
		for {
			select {
			case result = <-done:
				goto checkpointDone
			case <-ticker.C:
				r.logCheckpointWait(waitStarted)
			case <-r.ctx.Done():
				return 0, r.ctx.Err()
			}
		}
	} else {
		select {
		case result = <-done:
		default:
			return 0, nil
		}
	}
checkpointDone:
	r.checkpointDone = nil
	if result.err != nil {
		return result.checkpointBlocks, result.err
	}
	if result.persisted == nil {
		return result.checkpointBlocks, fmt.Errorf("archive checkpoint completed without persisted state")
	}
	if result.persisted.ShardClientSeqno <= r.lastCheckpointSeqno {
		return 0, nil
	}
	if result.persisted.ShardClientSeqno > r.current.ShardClientSeqno {
		return result.checkpointBlocks, fmt.Errorf("archive checkpoint persisted future seqno %d after current seqno %d", result.persisted.ShardClientSeqno, r.current.ShardClientSeqno)
	}

	previousTarget := r.checkpointBlocksTarget
	r.checkpointBlocksTarget = r.service.adaptArchiveCheckpointBlocks(r.checkpointBlocksTarget, result.elapsed)
	if r.checkpointBlocksTarget != previousTarget {
		r.service.log.Debug().
			Uint32("previous_checkpoint_blocks", previousTarget).
			Uint32("checkpoint_blocks", r.checkpointBlocksTarget).
			Dur("checkpoint_elapsed", result.elapsed).
			Msg("adjusted archive checkpoint interval")
	}

	if result.persisted.ShardClientSeqno == r.current.ShardClientSeqno {
		r.current = result.persisted
	}
	r.checkpointStates.completeCheckpoint(result.states)
	r.lastCheckpoint = time.Now()
	r.lastCheckpointSeqno = result.persisted.ShardClientSeqno
	r.importCache.dropBefore(r.lastCheckpointSeqno + 1)
	r.recordArchiveCheckpointProgress(result.checkpointBlocks, result.elapsed)

	waitElapsed := time.Since(waitStarted)
	event := r.service.log.Debug()
	if wait && waitElapsed >= time.Second {
		event = r.service.log.Info()
	}
	event.
		Str("masterchain", storage.FormatBlockRef(result.persisted.Masterchain.Block)).
		Uint32("shard_client_seqno", result.persisted.ShardClientSeqno).
		Uint32("checkpoint_blocks", result.checkpointBlocks).
		Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
		Dur("elapsed", result.elapsed).
		Dur("wait_elapsed", waitElapsed).
		Str("reason", result.reason).
		Msg("archive shard-client checkpoint completed")
	return result.checkpointBlocks, nil
}

func (r *archiveCatchUpRunner) logCheckpointWait(started time.Time) {
	r.service.log.Info().
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
		Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
		Uint32("checkpoint_target_blocks", r.checkpointBlocksTarget).
		Uint32("checkpoint_backpressure_blocks", r.archiveCheckpointBackpressureBlocks()).
		Dur("wait_elapsed", time.Since(started)).
		Msg("still waiting for archive shard-client checkpoint")
}

func (r *archiveCatchUpRunner) persistProgressBeforeRetry(err error) error {
	if err == nil || r.current.ShardClientSeqno <= r.lastCheckpointSeqno || errors.Is(err, context.Canceled) {
		return nil
	}

	checkpointBlocks, persistErr := r.persistCheckpoint("retry")
	if persistErr != nil {
		return fmt.Errorf("persist archive retry checkpoint: %w", persistErr)
	}

	r.service.log.Info().
		Err(err).
		Str("masterchain", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Uint32("shard_client_seqno", r.current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Msg("persisted archive shard-client progress before retry")

	r.checkpointBlocksTarget = r.service.archiveCatchUpCheckpointBlocks
	r.importCache.dropBefore(r.current.ShardClientSeqno + 1)
	return nil
}

func (r *archiveCatchUpRunner) returnWithProgress(err error) error {
	if persistErr := r.persistProgressBeforeRetry(err); persistErr != nil {
		return errors.Join(err, persistErr)
	}
	return err
}

func (r *archiveCatchUpRunner) restartPipeline(err error) error {
	if persistErr := r.persistProgressBeforeRetry(err); persistErr != nil {
		return errors.Join(err, persistErr)
	}

	r.pipeline.cancel()
	entries, hits := r.importCache.stats()
	r.service.log.Debug().
		Err(err).
		Uint32("current_seqno", r.current.ShardClientSeqno).
		Int("preloaded_archives", entries).
		Uint64("preload_cache_hits", hits).
		Msg("retrying archive shard-client preload from current state")

	if err = waitRetry(r.ctx, time.Second); err != nil {
		return err
	}
	r.pipeline = r.startShardClientArchiveWindowPipeline()
	return nil
}

func (s archiveCatchUpProgressStats) since(previous archiveCatchUpProgressStats) archiveCatchUpProgressStats {
	return archiveCatchUpProgressStats{
		windows:            s.windows - previous.windows,
		shardArchives:      s.shardArchives - previous.shardArchives,
		checkpoints:        s.checkpoints - previous.checkpoints,
		bytes:              s.bytes - previous.bytes,
		blocks:             s.blocks - previous.blocks,
		entries:            s.entries - previous.entries,
		pipelineWait:       s.pipelineWait - previous.pipelineWait,
		masterPrefetchWait: s.masterPrefetchWait - previous.masterPrefetchWait,
		archiveDownload:    s.archiveDownload - previous.archiveDownload,
		archiveImport:      s.archiveImport - previous.archiveImport,
		applyWall:          s.applyWall - previous.applyWall,
		masterApply:        s.masterApply - previous.masterApply,
		masterPrecheck:     s.masterPrecheck - previous.masterPrecheck,
		masterPrepare:      s.masterPrepare - previous.masterPrepare,
		masterConsensus:    s.masterConsensus - previous.masterConsensus,
		masterStateUpdate:  s.masterStateUpdate - previous.masterStateUpdate,
		shardTargetParse:   s.shardTargetParse - previous.shardTargetParse,
		shardApply:         s.shardApply - previous.shardApply,
		stateCells:         s.stateCells - previous.stateCells,
		stateCellPrepare:   s.stateCellPrepare - previous.stateCellPrepare,
		checkpointPersist:  s.checkpointPersist - previous.checkpointPersist,
	}
}

func (r *archiveCatchUpRunner) startPipelineWaitProgress() {
	if r.pipelineWaitStarted.IsZero() {
		r.pipelineWaitStarted = time.Now()
	}
}

func (r *archiveCatchUpRunner) accountPipelineWaitProgress(now time.Time) {
	if r.pipelineWaitStarted.IsZero() {
		return
	}
	if elapsed := now.Sub(r.pipelineWaitStarted); elapsed > 0 {
		r.progressStats.pipelineWait += elapsed
	}
	r.pipelineWaitStarted = now
}

func (r *archiveCatchUpRunner) stopPipelineWaitProgress(now time.Time) {
	r.accountPipelineWaitProgress(now)
	r.pipelineWaitStarted = time.Time{}
}

func (r *archiveCatchUpRunner) recordArchiveWindowProgress(window *shardClientArchiveWindow, applyElapsed time.Duration) {
	if window == nil {
		return
	}

	stats := &r.progressStats
	stats.windows++
	stats.masterPrefetchWait += window.masterWait
	stats.masterApply += window.masterApplyElapsed
	stats.masterPrecheck += window.masterPrecheckElapsed
	stats.masterPrepare += window.masterPrepareElapsed
	stats.masterConsensus += window.masterConsensusElapsed
	stats.masterStateUpdate += window.masterStateUpdateElapsed
	stats.shardTargetParse += window.shardTargetElapsed
	stats.shardApply += window.shardApplyElapsed
	stats.applyWall += applyElapsed
	if window.totalStats == nil {
		if window.shardArchives > 0 {
			stats.shardArchives += uint64(window.shardArchives)
		}
		return
	}

	stats.bytes += window.totalStats.Bytes
	if window.totalStats.Blocks > 0 {
		stats.blocks += uint64(window.totalStats.Blocks)
	}
	if window.totalStats.Entries > 0 {
		stats.entries += uint64(window.totalStats.Entries)
	}
	stats.archiveDownload += window.totalStats.DownloadElapsed
	stats.archiveImport += window.totalStats.ImportElapsed
	stats.stateCells += window.totalStats.StateUpdateCells
	stats.stateCellPrepare += window.totalStats.StateUpdateCellPrepare

	shardArchives := window.totalStats.ShardArchives
	if shardArchives == 0 {
		shardArchives = window.shardArchives
	}
	if shardArchives > 0 {
		stats.shardArchives += uint64(shardArchives)
	}
}

func (w *shardClientArchiveWindow) releaseImportedData() {
	if w == nil {
		return
	}

	w.masterStats = nil
	w.totalStats = nil
	w.masterStates = nil
	w.masterBlocks = nil
	w.masterSequence = nil
	w.masterProofs = nil
	w.archiveBlocks = nil
	w.archiveImports = nil
	w.appliedStates = appliedStateSet{}
}

func (r *archiveCatchUpRunner) rememberImportedArchiveData(window *shardClientArchiveWindow, imported *archiveImportResult) error {
	if window == nil || imported == nil {
		return nil
	}

	if r.stateCells == nil {
		return nil
	}

	for _, block := range imported.blocks {
		if block.StateUpdateToCells == nil {
			return fmt.Errorf("archive block %s state update cells are not prepared", storage.FormatBlockRef(block.ID))
		}
		r.stateCells.rememberPreparedCells(block.StateUpdateToCells)
	}
	return nil
}

func (r *archiveCatchUpRunner) storeArchiveWindow(window *shardClientArchiveWindow) error {
	if window == nil || len(window.archiveImports) == 0 {
		return nil
	}

	stored := storage.ServedArchiveImport{}
	seenFull := map[string]struct{}{}
	seenLink := map[string]struct{}{}
	for _, imported := range window.archiveImports {
		if imported == nil {
			continue
		}
		appendArchiveImport(&stored, &imported.stored, seenFull, seenLink)
	}
	if len(stored.FullBlocks) == 0 && len(stored.BlockData) == 0 && len(stored.Proofs) == 0 && len(stored.Links) == 0 {
		return nil
	}

	started := time.Now()
	if err := r.service.storage.SaveArchiveImport(&stored); err != nil {
		return fmt.Errorf("store archive blocks: %w", err)
	}
	r.service.log.Debug().
		Int("full_blocks", len(stored.FullBlocks)).
		Int("block_data", len(stored.BlockData)).
		Int("proofs", len(stored.Proofs)).
		Int("links", len(stored.Links)).
		Dur("elapsed", time.Since(started)).
		Msg("stored archive block refs")
	return nil
}

func appendArchiveImport(dst *storage.ServedArchiveImport, src *storage.ServedArchiveImport, seenFull map[string]struct{}, seenLink map[string]struct{}) {
	if dst == nil || src == nil {
		return
	}

	for _, full := range src.FullBlocks {
		if full == nil {
			continue
		}
		key := storage.BlockKey(full.ID)
		if _, exists := seenFull[key]; exists {
			continue
		}
		seenFull[key] = struct{}{}
		cloned := *full
		cloned.ProofRef = full.ProofRef.Clone()
		cloned.BlockRef = full.BlockRef.Clone()
		cloned.Meta = full.Meta.Clone()
		dst.FullBlocks = append(dst.FullBlocks, &cloned)
	}

	for _, block := range src.BlockData {
		dst.BlockData = append(dst.BlockData, cloneServedBlockData(block))
	}

	for _, proof := range src.Proofs {
		dst.Proofs = append(dst.Proofs, cloneServedBlockProof(proof))
	}

	for _, link := range src.Links {
		prevKey := storage.BlockKey(link.Prev)
		nextKey := storage.BlockKey(link.Next)
		linkKey := prevKey + ">" + nextKey
		if _, exists := seenLink[linkKey]; exists {
			continue
		}
		seenLink[linkKey] = struct{}{}
		dst.Links = append(dst.Links, link)
	}
}

func (r *archiveCatchUpRunner) recordArchiveCheckpointProgress(checkpointBlocks uint32, elapsed time.Duration) {
	if checkpointBlocks == 0 {
		return
	}

	r.progressStats.checkpoints++
	r.progressStats.checkpointPersist += elapsed
}

func archiveCatchUpDominantStage(stages ...archiveCatchUpStageTiming) string {
	var best archiveCatchUpStageTiming
	for _, stage := range stages {
		if stage.elapsed > best.elapsed {
			best = stage
		}
	}
	if best.elapsed <= 0 {
		return "none"
	}
	return best.name
}

func (r *archiveCatchUpRunner) logProgress() error {
	if _, err := r.finishCheckpoint(false); err != nil {
		return err
	}

	now := time.Now()
	if now.Sub(r.lastProgress) < archiveCatchUpProgressInterval && r.current.ShardClientSeqno < r.target.SeqNo {
		return nil
	}

	r.accountPipelineWaitProgress(now)

	done := r.current.ShardClientSeqno - r.startSeqno
	total := r.target.SeqNo - r.startSeqno
	targetRemainingBlocks := r.target.SeqNo - r.current.ShardClientSeqno
	windowBlocks := r.current.ShardClientSeqno - r.lastProgressSeqno
	windowShardBlocksApplied := r.shardBlocksApplied - r.lastProgressShardBlocksApplied
	windowShardBlocksReused := r.shardBlocksReused - r.lastProgressShardBlocksReused
	windowElapsed := now.Sub(r.lastProgress)
	stats := r.progressStats
	windowStats := stats.since(r.lastProgressStats)
	progress := formatCatchUpProgress(done, total)
	eta := formatCatchUpETA(done, total, time.Since(r.started))

	event := r.service.log.Info().
		Str("current", storage.FormatBlockRef(r.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(r.target)).
		Str("catchup_method", "archive_shard_client").
		Bool("checkpoint_in_flight", r.checkpointDone != nil).
		Uint32("persisted_masterchain_seqno", r.lastCheckpointSeqno).
		Uint32("pending_checkpoint_blocks", r.current.ShardClientSeqno-r.lastCheckpointSeqno).
		Uint32("processed_masterchain_blocks", done).
		Uint64("applied_shard_blocks", r.shardBlocksApplied).
		Uint64("reused_shard_blocks", r.shardBlocksReused).
		Uint32("total_masterchain_blocks", total).
		Uint64("archive_windows", stats.windows).
		Uint64("window_archive_windows", windowStats.windows).
		Uint64("shard_archives", stats.shardArchives).
		Uint64("window_shard_archives", windowStats.shardArchives).
		Int64("archive_bytes", stats.bytes).
		Int64("window_archive_bytes", windowStats.bytes).
		Uint64("archive_blocks", stats.blocks).
		Uint64("window_archive_blocks", windowStats.blocks).
		Uint64("archive_entries", stats.entries).
		Uint64("window_archive_entries", windowStats.entries)
	if lagSeconds, ok := r.archiveLiveTailLagSeconds(); ok {
		remainingLag := remainingLagSeconds(lagSeconds)
		event = event.
			Int64("master_lag_seconds", lagSeconds).
			Int64("remaining", remainingLag).
			Int64("remaining_lag_seconds", remainingLag)
		if r.hasStartRemainingLag {
			progress = formatLagCatchUpProgress(r.startRemainingLagSeconds, remainingLag)
			eta = formatLagCatchUpETA(r.startRemainingLagSeconds, remainingLag, time.Since(r.started))
		}
	} else {
		event = event.Int64("remaining", int64(targetRemainingBlocks))
	}
	event = event.
		Uint64("checkpoints", stats.checkpoints).
		Uint64("window_checkpoints", windowStats.checkpoints).
		Dur("window_pipeline_wait", windowStats.pipelineWait).
		Dur("window_master_prefetch_wait", windowStats.masterPrefetchWait).
		Dur("window_archive_download", windowStats.archiveDownload).
		Dur("window_archive_import", windowStats.archiveImport).
		Dur("window_apply_wall", windowStats.applyWall).
		Dur("window_master_apply", windowStats.masterApply).
		Dur("window_master_precheck", windowStats.masterPrecheck).
		Dur("window_master_prepare", windowStats.masterPrepare).
		Dur("window_master_consensus", windowStats.masterConsensus).
		Dur("window_master_state_update", windowStats.masterStateUpdate).
		Dur("window_shard_target_parse", windowStats.shardTargetParse).
		Dur("window_shard_apply", windowStats.shardApply).
		Uint64("state_cells", stats.stateCells).
		Uint64("window_state_cells", windowStats.stateCells).
		Dur("window_state_cell_prepare", windowStats.stateCellPrepare).
		Dur("window_checkpoint_persist", windowStats.checkpointPersist).
		Str("progress", progress).
		Str("speed", formatBlockRate(done, time.Since(r.started))).
		Str("window_speed", formatBlockRate(windowBlocks, windowElapsed)).
		Str("shard_apply_speed", formatBlockRate64(windowShardBlocksApplied, windowElapsed)).
		Str("shard_seen_speed", formatBlockRate64(windowShardBlocksApplied+windowShardBlocksReused, windowElapsed)).
		Str("archive_download_speed", logutil.FormatByteRate(stats.bytes, stats.archiveDownload)).
		Str("window_archive_download_speed", logutil.FormatByteRate(windowStats.bytes, windowStats.archiveDownload)).
		Str("window_archive_ingest_speed", logutil.FormatByteRate(windowStats.bytes, windowElapsed)).
		Str("archive_import_block_speed", formatBlockRate64(stats.blocks, stats.archiveImport)).
		Str("window_archive_import_block_speed", formatBlockRate64(windowStats.blocks, windowStats.archiveImport)).
		Str("state_cell_prepare_speed", logutil.FormatCellRate(stats.stateCells, stats.stateCellPrepare)).
		Str("window_state_cell_prepare_speed", logutil.FormatCellRate(windowStats.stateCells, windowStats.stateCellPrepare)).
		Str("window_bottleneck", archiveCatchUpDominantStage(
			archiveCatchUpStageTiming{name: "pipeline_wait", elapsed: windowStats.pipelineWait},
			archiveCatchUpStageTiming{name: "apply_wall", elapsed: windowStats.applyWall},
			archiveCatchUpStageTiming{name: "checkpoint_persist", elapsed: windowStats.checkpointPersist},
		)).
		Str("window_pipeline_bottleneck", archiveCatchUpDominantStage(
			archiveCatchUpStageTiming{name: "master_prefetch_wait", elapsed: windowStats.masterPrefetchWait},
			archiveCatchUpStageTiming{name: "archive_download", elapsed: windowStats.archiveDownload},
			archiveCatchUpStageTiming{name: "archive_import", elapsed: windowStats.archiveImport},
			archiveCatchUpStageTiming{name: "master_precheck", elapsed: windowStats.masterPrecheck},
			archiveCatchUpStageTiming{name: "master_apply", elapsed: windowStats.masterApply},
		)).
		Str("window_master_apply_bottleneck", archiveCatchUpDominantStage(
			archiveCatchUpStageTiming{name: "prepare", elapsed: windowStats.masterPrepare},
			archiveCatchUpStageTiming{name: "consensus", elapsed: windowStats.masterConsensus},
			archiveCatchUpStageTiming{name: "state_update", elapsed: windowStats.masterStateUpdate},
		)).
		Str("eta", eta)
	event.Msg("archive shard-client catch-up progress")

	r.lastProgress = now
	r.lastProgressSeqno = r.current.ShardClientSeqno
	r.lastProgressShardBlocksApplied = r.shardBlocksApplied
	r.lastProgressShardBlocksReused = r.shardBlocksReused
	r.lastProgressStats = stats
	return nil
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

func (r *archiveCatchUpRunner) startShardClientArchiveWindowPipeline() *archiveWindowPipeline {
	pipelineCtx, cancel := context.WithCancel(r.ctx)
	buffer := r.service.archiveCatchUpPrefetchWindows
	if buffer < 1 {
		buffer = 1
	}

	pipeline := &archiveWindowPipeline{
		cancel: cancel,
		done:   make(chan archiveWindowResult, buffer),
	}
	targetSeqno := r.target.SeqNo
	go r.runShardClientArchiveWindowPipeline(pipelineCtx, archivePipelineCurrent(r.current), targetSeqno, pipeline.done)
	return pipeline
}

func (r *archiveCatchUpRunner) runShardClientArchiveWindowPipeline(ctx context.Context, current *storage.CurrentState, targetSeqno uint32, out chan<- archiveWindowResult) {
	defer close(out)

	taskCtx, cancelTasks := context.WithCancel(ctx)
	defer cancelTasks()

	importQueue := r.startArchiveImportQueue(taskCtx)
	planning := archivePipelineCurrent(current)
	emitted := archivePipelineCurrent(current)
	pending := make([]archivePendingWindow, 0, r.archivePendingWindowLimit())

	var masterTask *archiveMasterImportTask
	defer func() {
		if masterTask != nil {
			masterTask.cancel()
		}
	}()

	for planning.ShardClientSeqno < targetSeqno || len(pending) > 0 {
		for r.canScheduleArchiveWindow(planning, len(pending), targetSeqno) {
			prepared, err := r.prepareArchiveMasterWindow(taskCtx, importQueue, planning, masterTask, targetSeqno)
			if err != nil {
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
			if r.shouldEmitReadyArchiveWindow(pending) {
				break
			}
		}

		if len(pending) == 0 {
			return
		}

		next := pending[0]
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

		select {
		case out <- archiveWindowResult{window: next.window}:
		case <-ctx.Done():
			return
		}
		emitted = nextEmitted
		pending = pending[1:]
	}
}

func (r *archiveCatchUpRunner) archivePendingWindowLimit() int {
	return archiveMasterLookaheadWindows
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

func (r *archiveCatchUpRunner) archiveMasterBlocksForWindow(start *storage.BlockState, imported map[string]p2p.DownloadedBlock, startSeqno uint32, targetSeqno uint32) ([]p2p.DownloadedBlock, error) {
	return archiveMasterBlockSequence(start, targetSeqno, startSeqno, archiveMasterBlockMap(imported))
}

func archiveMasterBlockMap(blocks map[string]p2p.DownloadedBlock) map[uint32]p2p.DownloadedBlock {
	masterBlocks := make(map[uint32]p2p.DownloadedBlock)
	for _, block := range blocks {
		if block.ID.Workchain == -1 && block.ID.Shard == topShard {
			masterBlocks[block.ID.SeqNo] = block
		}
	}
	return masterBlocks
}

func archiveMasterBlockSequence(start *storage.BlockState, targetSeqno uint32, startSeqno uint32, masterBlocks map[uint32]p2p.DownloadedBlock) ([]p2p.DownloadedBlock, error) {
	sequence := make([]p2p.DownloadedBlock, 0, len(masterBlocks))
	for seqno := start.Block.SeqNo + 1; seqno != 0 && seqno <= targetSeqno; seqno++ {
		downloaded, ok := masterBlocks[seqno]
		if !ok {
			if seqno == start.Block.SeqNo+1 {
				return nil, fmt.Errorf("archive window #%d has no next masterchain block after %s", startSeqno, storage.FormatBlockRef(start.Block))
			}
			break
		}
		if downloaded.Meta == nil {
			var err error
			downloaded, err = prepareArchiveDownloadedBlock(downloaded)
			if err != nil {
				return nil, fmt.Errorf("prepare archive masterchain block %s: %w", storage.FormatBlockRef(downloaded.ID), err)
			}
			masterBlocks[seqno] = downloaded
		}
		if downloaded.Meta != nil && seqno != startSeqno && downloaded.Meta.Has(storage.BlockMetaIsKeyBlock) {
			break
		}
		sequence = append(sequence, downloaded)
	}
	return sequence, nil
}

func (r *archiveCatchUpRunner) precheckArchiveMasterConsensus(ctx context.Context, start *storage.BlockState, blocks []p2p.DownloadedBlock) (map[uint32]*masterchainConsensusProof, time.Duration, error) {
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
		proof, err := prepareMasterchainConsensusProof(downloaded.ID, downloaded.ProofBOC)
		if err != nil {
			return nil, time.Since(started), err
		}
		if proof.parsed.Block.BlockInfo.KeyBlock {
			return nil, time.Since(started), nil
		}
		if len(proof.parsed.Meta.PrevRefs) != 1 || !proof.parsed.Meta.PrevRefs[0].Equals(&expectedPrev) {
			return nil, time.Since(started), fmt.Errorf("%w: block=%s prev_refs=%d expected=%s", errMasterchainPrevMismatch, storage.FormatBlockRef(downloaded.ID), len(proof.parsed.Meta.PrevRefs), storage.FormatBlockRef(expectedPrev))
		}

		key := masterchainValidatorCacheKeyFromBlock(proof.parsed.Block)
		validatorSet, ok := validatorsByKey[key]
		if !ok {
			var err error
			validatorSet, err = r.service.masterchainValidatorsForConsensus(start, downloaded.ID, proof.parsed.Block)
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

func precheckMasterchainSignatures(ctx context.Context, blocks []p2p.DownloadedBlock, proofs []*masterchainConsensusProof, validators [][]*tlb.ValidatorAddr) error {
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
				err := blockproof.CheckMasterchainSignaturesWithValidators(blocks[idx].ID, proof.parsed.Block, proof.parsed.Proof.Signatures, validators[idx])
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
		downloaded, err := r.downloadArchiveFile(loadCtx, masterchainSeqno, shard)
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

func (r *archiveCatchUpRunner) downloadArchiveFile(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID) (*archive.Downloaded, error) {
	downloaded, err := r.service.node.DownloadArchive(ctx, masterchainSeqno, shard, "")
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
	if t == nil {
		return archiveMasterImportResult{}, 0, fmt.Errorf("archive master import task is nil")
	}

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
	if t == nil {
		return fmt.Errorf("archive shard import task is nil")
	}
	if t.done == nil {
		return t.err
	}

	select {
	case err := <-t.done:
		t.done = nil
		t.err = err
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
		return true
	default:
		return false
	}
}

func (r *archiveCatchUpRunner) prepareArchiveMasterWindow(ctx context.Context, queue *archiveImportQueue, current *storage.CurrentState, masterTask *archiveMasterImportTask, targetSeqno uint32) (archivePreparedMasterWindow, error) {
	startSeqno := current.ShardClientSeqno + 1
	startMaster, err := r.service.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return archivePreparedMasterWindow{}, fmt.Errorf("load archive start masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

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
		masterWait:            masterWait,
		masterPrecheckElapsed: masterResult.precheckElapsed,
	}
	if err := r.rememberImportedArchiveData(window, masterImport); err != nil {
		return archivePreparedMasterWindow{}, err
	}

	lastMaster, err := r.applyArchiveMasterBlocks(ctx, startMaster, window)
	if err != nil {
		return archivePreparedMasterWindow{}, err
	}
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

func (r *archiveCatchUpRunner) startArchiveImportQueue(ctx context.Context) *archiveImportQueue {
	downloadWorkers := archiveDownloadWorkers()
	downloadBudget := archiveDownloadWorkerBudget(downloadWorkers)
	prepareWorkers := archiveCPUWorkers()
	prepareBudget := archivePrepareWorkerBudget(prepareWorkers)
	queue := &archiveImportQueue{
		downloadHot:      make(chan archiveDownloadJob, downloadWorkers),
		downloadPrefetch: make(chan archiveDownloadJob, downloadWorkers),
		prepareHot:       make(chan archivePrepareJob, prepareWorkers),
		preparePrefetch:  make(chan archivePrepareJob, prepareWorkers),
	}
	for worker := 0; worker < downloadBudget.hotOnly; worker++ {
		go func() {
			for {
				job, ok := queue.nextHotDownload(ctx)
				if !ok {
					return
				}
				queue.runDownloadJob(r, job)
			}
		}()
	}
	for worker := 0; worker < downloadBudget.shared; worker++ {
		go func() {
			for {
				job, ok := queue.nextDownload(ctx)
				if !ok {
					return
				}
				queue.runDownloadJob(r, job)
			}
		}()
	}
	for worker := 0; worker < prepareBudget.hotOnly; worker++ {
		go func() {
			for {
				job, ok := queue.nextHotPrepare(ctx)
				if !ok {
					return
				}
				queue.runPrepareJob(r, job)
			}
		}()
	}
	for worker := 0; worker < prepareBudget.shared; worker++ {
		go func() {
			for {
				job, ok := queue.nextPrepare(ctx)
				if !ok {
					return
				}
				queue.runPrepareJob(r, job)
			}
		}()
	}
	return queue
}

func (q *archiveImportQueue) importArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority) (*archiveImportResult, error) {
	done, err := q.submitArchive(ctx, masterchainSeqno, shard, splitDepth, priority)
	if err != nil {
		return nil, err
	}

	select {
	case result := <-done:
		return result.imported, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *archiveImportQueue) submitArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority) (<-chan archiveImportQueueResult, error) {
	if q == nil {
		return nil, fmt.Errorf("archive import queue is nil")
	}
	done := make(chan archiveImportQueueResult, 1)
	job := archiveDownloadJob{
		ctx:              ctx,
		masterchainSeqno: masterchainSeqno,
		shard:            shard,
		splitDepth:       splitDepth,
		priority:         priority,
		done:             done,
	}

	select {
	case q.downloadJobs(priority) <- job:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return done, nil
}

func (q *archiveImportQueue) runDownloadJob(r *archiveCatchUpRunner, job archiveDownloadJob) {
	if err := job.ctx.Err(); err != nil {
		job.done <- archiveImportQueueResult{err: err}
		return
	}

	downloaded, err := r.downloadArchiveFile(job.ctx, job.masterchainSeqno, job.shard)
	if err != nil {
		job.done <- archiveImportQueueResult{err: err}
		return
	}

	prepare := archivePrepareJob{
		ctx:        job.ctx,
		downloaded: downloaded,
		splitDepth: job.splitDepth,
		priority:   job.priority,
		done:       job.done,
	}
	select {
	case q.prepareJobs(job.priority) <- prepare:
	case <-job.ctx.Done():
		job.done <- archiveImportQueueResult{err: job.ctx.Err()}
	}
}

func (q *archiveImportQueue) runPrepareJob(r *archiveCatchUpRunner, job archivePrepareJob) {
	if err := job.ctx.Err(); err != nil {
		job.done <- archiveImportQueueResult{err: err}
		return
	}
	imported, err := r.prepareArchiveDownload(job.ctx, job.downloaded.MasterchainSeqno, job.downloaded.Shard, job.splitDepth, job.downloaded)
	job.done <- archiveImportQueueResult{imported: imported, err: err}
}

func (q *archiveImportQueue) nextDownload(ctx context.Context) (archiveDownloadJob, bool) {
	return nextArchivePriorityJob(ctx, q.downloadHot, q.downloadPrefetch, archiveDownloadJob{})
}

func (q *archiveImportQueue) nextHotDownload(ctx context.Context) (archiveDownloadJob, bool) {
	return nextArchiveHotJob(ctx, q.downloadHot, archiveDownloadJob{})
}

func (q *archiveImportQueue) nextPrepare(ctx context.Context) (archivePrepareJob, bool) {
	return nextArchivePriorityJob(ctx, q.prepareHot, q.preparePrefetch, archivePrepareJob{})
}

func (q *archiveImportQueue) nextHotPrepare(ctx context.Context) (archivePrepareJob, bool) {
	return nextArchiveHotJob(ctx, q.prepareHot, archivePrepareJob{})
}

func nextArchivePriorityJob[T any](ctx context.Context, hot <-chan T, prefetch <-chan T, zero T) (T, bool) {
	select {
	case job := <-hot:
		return job, true
	default:
	}

	select {
	case job := <-hot:
		return job, true
	case job := <-prefetch:
		return job, true
	case <-ctx.Done():
		return zero, false
	}
}

func nextArchiveHotJob[T any](ctx context.Context, hot <-chan T, zero T) (T, bool) {
	select {
	case job := <-hot:
		return job, true
	case <-ctx.Done():
		return zero, false
	}
}

func (q *archiveImportQueue) downloadJobs(priority archiveImportPriority) chan archiveDownloadJob {
	if priority == archiveImportPriorityHot {
		return q.downloadHot
	}
	return q.downloadPrefetch
}

func (q *archiveImportQueue) prepareJobs(priority archiveImportPriority) chan archivePrepareJob {
	if priority == archiveImportPriorityHot {
		return q.prepareHot
	}
	return q.preparePrefetch
}

func archiveCPUWorkers() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}
	return workers
}

func archiveDownloadWorkers() int {
	workers := archiveCPUWorkers() * archiveDownloadWorkerMultiplier
	if workers < archiveDownloadWorkerMin {
		return archiveDownloadWorkerMin
	}
	if workers > archiveDownloadWorkerMax {
		return archiveDownloadWorkerMax
	}
	return workers
}

type archiveWorkerBudget struct {
	hotOnly int
	shared  int
}

func archiveDownloadWorkerBudget(workers int) archiveWorkerBudget {
	if workers <= 1 {
		return archiveWorkerBudget{shared: workers}
	}

	hotOnly := workers / 4
	if hotOnly < archiveDownloadHotWorkerMin {
		hotOnly = archiveDownloadHotWorkerMin
	}
	if hotOnly > archiveDownloadHotWorkerMax {
		hotOnly = archiveDownloadHotWorkerMax
	}
	if hotOnly >= workers {
		hotOnly = workers - 1
	}

	return archiveWorkerBudget{
		hotOnly: hotOnly,
		shared:  workers - hotOnly,
	}
}

func archivePrepareWorkerBudget(workers int) archiveWorkerBudget {
	if workers <= 1 {
		return archiveWorkerBudget{shared: workers}
	}

	hotOnly := workers / 8
	if hotOnly < archivePrepareHotWorkerMin {
		hotOnly = archivePrepareHotWorkerMin
	}
	if hotOnly > archivePrepareHotWorkerMax {
		hotOnly = archivePrepareHotWorkerMax
	}
	if hotOnly >= workers {
		hotOnly = workers - 1
	}

	return archiveWorkerBudget{
		hotOnly: hotOnly,
		shared:  workers - hotOnly,
	}
}

func (r *archiveCatchUpRunner) startArchiveWindowShardImport(ctx context.Context, queue *archiveImportQueue, prepared archivePreparedMasterWindow, priority archiveImportPriority) *archiveWindowShardImportTask {
	task := &archiveWindowShardImportTask{done: make(chan error, 1)}
	go func() {
		task.done <- r.importArchiveWindowShards(ctx, queue, prepared, priority)
	}()
	return task
}

func (r *archiveCatchUpRunner) importArchiveWindowShards(ctx context.Context, queue *archiveImportQueue, prepared archivePreparedMasterWindow, priority archiveImportPriority) error {
	window := prepared.window
	masterImport := prepared.masterImport
	splitDepth, err := monitorMinSplitDepth(prepared.startMaster, 0)
	if err != nil {
		return fmt.Errorf("load archive split depth for %s: %w", storage.FormatBlockRef(prepared.startMaster.Block), err)
	}
	if splitDepth > maxArchiveMonitorSplitDepth {
		return fmt.Errorf("monitor split depth %d exceeds supported archive prefix fanout %d", splitDepth, maxArchiveMonitorSplitDepth)
	}
	window.splitDepth = splitDepth

	if !masterImport.stats.ContainsShardBlocks {
		shards, splitDepth, err := r.service.archiveShardPrefixesForWindow(prepared.startMaster, prepared.lastMaster)
		if err != nil {
			return err
		}
		window.splitDepth = splitDepth
		window.shardArchives = len(shards)

		importedFiles, err := r.downloadAndImportShardArchives(ctx, queue, window.startSeqno, shards, splitDepth, priority)
		if err != nil {
			return err
		}
		for _, imported := range importedFiles {
			mergeImportStats(window.totalStats, imported.stats, true)
			window.archiveImports = append(window.archiveImports, imported)
			if err = r.rememberImportedArchiveData(window, imported); err != nil {
				return err
			}
			for key, block := range imported.blocks {
				window.archiveBlocks[key] = block
			}
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
	var applier stateUpdateApplier = r.stateCells
	if r.stateCells != nil {
		applier = r.stateCells.metered(nil)
	}
	if window.masterSequence == nil {
		sequence, err := archiveMasterBlockSequence(start, r.target.SeqNo, window.startSeqno, window.masterBlocks)
		if err != nil {
			return nil, err
		}
		window.masterSequence = sequence
	}

	for _, downloaded := range window.masterSequence {
		next, timing, err := r.service.applyMasterchainTransitionWithConsensusProof(master, downloaded, window.masterProofs[downloaded.ID.SeqNo], applier)
		window.masterApplyElapsed += timing.total
		window.masterPrepareElapsed += timing.prepare
		window.masterConsensusElapsed += timing.consensus
		window.masterStateUpdateElapsed += timing.stateUpdate
		if err != nil {
			return nil, fmt.Errorf("apply archive master block %s: %w", downloaded.BlockRef(), err)
		}
		master = next
		window.archiveBlocks[storage.BlockKey(downloaded.ID)] = downloaded
		window.masterStates[downloaded.ID.SeqNo] = master
		window.appliedStates.rememberWithCells(master, downloaded.StateUpdateToCells)
	}
	return master, nil
}

func (r *archiveCatchUpRunner) applyShardClientArchiveWindow(ctx context.Context, current *storage.CurrentState, window *shardClientArchiveWindow) (*storage.CurrentState, error) {
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

		shards, err := r.applyArchiveShardTargets(ctx, masterState.Block, prevShards, applied, window.archiveBlocks, targets, window)
		if err != nil {
			return nil, fmt.Errorf("apply archive shard blocks at masterchain seqno %d: %w", seqno, err)
		}
		for key, shard := range shards {
			next.Shards[key] = *storage.CloneBlockState(shard)
		}
	}
	return next, nil
}

func (r *archiveCatchUpRunner) applyArchiveShardTargets(ctx context.Context, master ton.BlockIDExt, current map[storage.ShardKey]storage.BlockState, applied map[string]*storage.BlockState, blocks map[string]p2p.DownloadedBlock, targets []ton.BlockIDExt, window *shardClientArchiveWindow) (map[storage.ShardKey]*storage.BlockState, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	var applier stateUpdateApplier = r.stateCells
	if r.stateCells != nil {
		applier = r.stateCells.metered(nil)
	}

	var appliedMu sync.Mutex
	var blockLoaderMu sync.Mutex
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current:   current,
		cache:     applied,
		loadState: r.service.loadBlockStateForApply,
		loadBlock: archiveShardBlockLoader{
			master: master,
			blocks: blocks,
			mu:     &blockLoaderMu,
		}.load,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*storage.BlockState, error) {
			return r.service.applyResolvedShardBlock(ctx, target, previous, downloaded, applier)
		},
		afterApplyState: func(_ context.Context, state *storage.BlockState, downloaded p2p.DownloadedBlock, _ time.Duration) error {
			setShardStateMasterchainRef(state, master)
			setDownloadedShardMasterchainRef(&downloaded, master)

			appliedMu.Lock()
			defer appliedMu.Unlock()

			window.appliedStates.rememberWithCells(state, downloaded.StateUpdateToCells)
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

func (r *archiveCatchUpRunner) persistArchiveCurrentState(current *storage.CurrentState, checkpointBlocks uint32, lockElapsed time.Duration, states []*storage.BlockState, cells *archiveStateCellCheckpoint, stateCells []storage.EncodedCellRecord) (*storage.CurrentState, error) {
	if current == nil {
		return nil, fmt.Errorf("archive current state is nil")
	}

	if len(states) == 0 {
		return nil, fmt.Errorf("archive current state has no block states")
	}

	storedCurrent := currentStateWithoutCells(current)
	started := time.Now()
	cellRecords := mergeEncodedCellRecords(cells.records(), stateCells)
	persisted, err := r.service.saveStateCheckpoint(r.ctx, storedCurrent, states, 0, cellRecords)
	if err != nil {
		return nil, fmt.Errorf("persist archive current state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	cells.complete()

	r.service.log.Debug().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("checkpoint_blocks", checkpointBlocks).
		Int("states", len(states)).
		Int("shards", len(current.Shards)).
		Dur("lock_wait", lockElapsed).
		Dur("elapsed", time.Since(started)).
		Msg("archive shard-client checkpoint persisted")
	return persisted, nil
}

func mergeEncodedCellRecords(records []storage.EncodedCellRecord, extra []storage.EncodedCellRecord) []storage.EncodedCellRecord {
	if len(records) == 0 {
		return extra
	}
	if len(extra) == 0 {
		return records
	}

	seen := make(map[cell.Hash]struct{}, len(records)+len(extra))
	merged := make([]storage.EncodedCellRecord, 0, len(records)+len(extra))
	for _, record := range records {
		if len(record.Data) == 0 {
			continue
		}
		if _, ok := seen[record.Hash]; ok {
			continue
		}
		seen[record.Hash] = struct{}{}
		merged = append(merged, record)
	}
	for _, record := range extra {
		if len(record.Data) == 0 {
			continue
		}
		if _, ok := seen[record.Hash]; ok {
			continue
		}
		seen[record.Hash] = struct{}{}
		merged = append(merged, record)
	}
	return merged
}

func (s *Service) importArchiveBlocks(ctx context.Context, downloaded *archive.Downloaded, splitDepth uint32) (*archiveImportResult, error) {
	if downloaded == nil {
		return nil, archive.ErrNotAvailable
	}

	cleanupPath := downloaded.Path
	cleanupDownloaded := cleanupPath != ""
	defer func() {
		if cleanupDownloaded {
			_ = os.Remove(cleanupPath)
		}
	}()

	imported := downloaded.Imported
	if downloaded.Path != "" {
		stored, err := s.storage.SaveArchiveFile(
			int32(downloaded.MasterchainSeqno),
			downloaded.Shard.Workchain,
			downloaded.Shard.Shard,
			downloaded.ArchiveID,
			downloaded.Path,
		)
		if err != nil {
			return nil, fmt.Errorf("store downloaded archive pack: %w", err)
		}
		cleanupDownloaded = false
		if stored.ReusedExisting {
			imported, err = importArchiveFile(ctx, downloaded, stored.Path)
			if err != nil {
				return nil, err
			}
		} else if imported == nil {
			imported, err = importArchiveFile(ctx, downloaded, stored.Path)
			if err != nil {
				return nil, err
			}
		} else {
			imported.SetArtifactPath(stored.Path)
		}
	} else if imported == nil {
		var err error
		imported, err = importArchiveFile(ctx, downloaded, downloaded.Path)
		if err != nil {
			return nil, err
		}
	}

	return s.prepareImportedArchiveBlocks(imported, splitDepth)
}

func importArchiveFile(ctx context.Context, downloaded *archive.Downloaded, path string) (*archive.Imported, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open archive pack %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	archiveRef := *downloaded
	archiveRef.Path = path
	archiveRef.Imported = nil
	imported, err := archive.ImportStream(ctx, &archiveRef, file)
	if err != nil {
		return nil, err
	}
	return imported, nil
}

func (s *Service) prepareImportedArchiveBlocks(imported *archive.Imported, splitDepth uint32) (*archiveImportResult, error) {
	if imported == nil {
		return nil, fmt.Errorf("import archive: empty imported data")
	}
	if imported.Stats == nil {
		return nil, fmt.Errorf("import archive %s: empty stats", imported.ArtifactPath)
	}

	blocks := map[string]p2p.DownloadedBlock{}
	stored := storage.ServedArchiveImport{
		FullBlocks: make([]*storage.ServedBlockFull, 0, len(imported.FullBlocks)),
		Links:      append([]storage.ServedBlockLink(nil), imported.Links...),
	}

	seenBlocks := map[string]struct{}{}
	for _, full := range imported.FullBlocks {
		key := storage.BlockKey(full.ID)
		if _, exists := seenBlocks[key]; exists {
			continue
		}
		seenBlocks[key] = struct{}{}
		prepared := imported.PreparedBlocks[key]
		if prepared.Meta == nil {
			return nil, fmt.Errorf("archive block %s was not prepared by archive import", storage.FormatBlockRef(full.ID))
		}
		if prepared.State == nil {
			return nil, fmt.Errorf("archive block %s state was not prepared by archive import", storage.FormatBlockRef(full.ID))
		}
		blocks[key] = p2p.DownloadedBlock{
			ID:                        full.ID,
			Kind:                      "archive block",
			Block:                     prepared.Block,
			BlockBOC:                  full.Block,
			ProofBOC:                  full.Proof,
			Parsed:                    prepared.Parsed,
			Meta:                      prepared.Meta.Clone(),
			StateUpdateToCells:        prepared.StateUpdateToCells,
			StateUpdateToCellsElapsed: prepared.StateUpdateToCellsElapsed,
			IsLink:                    full.IsLink,
			VerifiedRootHash:          true,
			VerifiedFileHash:          true,
		}
		stored.FullBlocks = append(stored.FullBlocks, &storage.ServedBlockFull{
			ID:                     full.ID,
			BlockRef:               full.BlockRef.Clone(),
			ProofRef:               full.ProofRef.Clone(),
			Meta:                   prepared.Meta.Clone(),
			IsLink:                 full.IsLink,
			ArchiveShardSplitDepth: splitDepth,
		})
	}

	return &archiveImportResult{stats: imported.Stats, blocks: blocks, stored: stored, splitDepth: splitDepth}, nil
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
	return &cloned
}

func cloneArchiveImportResult(result *archiveImportResult) *archiveImportResult {
	if result == nil {
		return nil
	}

	cloned := &archiveImportResult{
		stats:      cloneImportStats(result.stats),
		blocks:     make(map[string]p2p.DownloadedBlock, len(result.blocks)),
		stored:     cloneServedArchiveImport(result.stored),
		splitDepth: result.splitDepth,
	}
	for key, block := range result.blocks {
		cloned.blocks[key] = block
	}
	return cloned
}

func cloneServedArchiveImport(imported storage.ServedArchiveImport) storage.ServedArchiveImport {
	cloned := storage.ServedArchiveImport{
		FullBlocks: make([]*storage.ServedBlockFull, 0, len(imported.FullBlocks)),
		BlockData:  make([]storage.ServedBlockData, 0, len(imported.BlockData)),
		Proofs:     make([]storage.ServedBlockProof, 0, len(imported.Proofs)),
		Links:      append([]storage.ServedBlockLink(nil), imported.Links...),
	}
	for _, full := range imported.FullBlocks {
		if full == nil {
			cloned.FullBlocks = append(cloned.FullBlocks, nil)
			continue
		}
		next := *full
		next.ProofRef = full.ProofRef.Clone()
		next.BlockRef = full.BlockRef.Clone()
		next.Meta = full.Meta.Clone()
		cloned.FullBlocks = append(cloned.FullBlocks, &next)
	}
	for _, block := range imported.BlockData {
		cloned.BlockData = append(cloned.BlockData, cloneServedBlockData(block))
	}
	for _, proof := range imported.Proofs {
		cloned.Proofs = append(cloned.Proofs, cloneServedBlockProof(proof))
	}
	return cloned
}

func cloneServedBlockData(block storage.ServedBlockData) storage.ServedBlockData {
	return storage.ServedBlockData{
		ID:   block.ID,
		Data: block.Data,
		Ref:  block.Ref.Clone(),
	}
}

func cloneServedBlockProof(proof storage.ServedBlockProof) storage.ServedBlockProof {
	return storage.ServedBlockProof{
		Kind: proof.Kind,
		ID:   proof.ID,
		Data: proof.Data,
		Ref:  proof.Ref.Clone(),
	}
}
