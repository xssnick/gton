package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	topShard                           = int64(-1 << 63)
	shardStateCatchUpParallelism       = 4
	shardStateDownloadBuffer           = 16
	shardStateDownloadWorkers          = 4
	nextBlockDescriptionLookahead      = shardStateDownloadBuffer
	shardStateCatchUpRetryDelay        = 500 * time.Millisecond
	currentStateLivePollDelay          = 300 * time.Millisecond
	syncDiskSpaceRetryDelay            = 5 * time.Second
	defaultMinSyncDiskFreeBytes        = 10 << 30
	nextBlockBootstrapBlocks           = 0
	nextBlockBootstrapProbeTimeout     = 2 * time.Second
	nextBlockBootstrapLiveProbeTimeout = 5 * time.Second
	nextBlockBootstrapLiveStageDelay   = 250 * time.Millisecond
	nextBlockBootstrapProbePeers       = 3
	nextBlockBootstrapUrgentPeers      = 8
	nextBlockBootstrapWidePeers        = 16
	nextBlockBootstrapUrgentMisses     = 1
	nextBlockBootstrapWideMisses       = 2
	nextBlockBootstrapUrgentLagSeconds = 3
	nextBlockBootstrapWideLagSeconds   = 10
	nextBlockBootstrapWideGapBlocks    = 8
	nextBlockBootstrapLiveLagSeconds   = 10
	// nextBlockBootstrapDecodeGrace parks the fallback probe once per raw
	// masterchain broadcast so the already-received payload can finish its
	// local decode+verify instead of racing a peer download.
	nextBlockBootstrapDecodeGrace = 500 * time.Millisecond
	// pace bounds for the first probe of a height: the next block is not due
	// before roughly one observed block interval, so the probe stays parked
	// (wakes still cut through) instead of producing guaranteed misses. The
	// headroom keeps the pace deadline past the typical broadcast arrival and
	// decode, so the fallback probe does not race the own pipeline by a few
	// milliseconds and download the block a broadcast is about to deliver.
	nextBlockBootstrapPaceHeadroom = 100 * time.Millisecond
	nextBlockBootstrapPaceMaxDelay = 3500 * time.Millisecond
	nextBlockBootstrapPaceMinDelay = 20 * time.Millisecond
	nextBlockBootstrapPaceMaxGap   = 30 * time.Second
	exactBlockDownloadProbePeers   = 4
	chainBlockDownloadRetries      = 3
	chainBlockDownloadRetryDelay   = 500 * time.Millisecond
	nextMasterchainQueueLimit      = 64
	nextMasterchainQueueTTL        = 3 * time.Minute
	nextMasterchainQueueMaxBytes   = 512 << 20
	masterStateCacheLimit          = 2048
	syncedBlockProcessorWorkers    = 8
	syncedBlockPriorityQueueLimit  = 64
	syncedBlockMasterHotWorkers    = 2
	syncedBlockMasterHotQueueLimit = 64
)

var (
	errMasterchainPrevMismatch            = errors.New("masterchain previous state is not current")
	errSyncedBlockDeferred                = errors.New("synced block was deferred")
	errShardDescriptionTooOld             = errors.New("shard block description is older than current state")
	errShardDescriptionTooNew             = errors.New("shard block description is too far ahead of current state")
	errSyncCoordinatorArchiveAlreadyBound = errors.New("sync coordinator archive runner is already bound")
	errSyncCoordinatorArchiveNotBound     = errors.New("sync coordinator archive runner is not bound")
	errSyncCoordinatorStarted             = errors.New("sync coordinator is already started")
)
var errShardCatchUpNeedsSnapshot = errors.New("shard catch-up requires state snapshot")

// errShardApplyMasterMissing guards the shard apply callbacks: they must never
// fall back to the commit stage's current master, because the resolution they
// serve can be started by the apply-ahead stage.
var errShardApplyMasterMissing = errors.New("shard apply is missing its inclusion masterchain state")

type shardBlockLoader func(context.Context, ton.BlockIDExt) (PreparedBlock, error)

type timeoutError interface {
	Timeout() bool
}

type shardStateDownload struct {
	prev            ton.BlockIDExt
	block           PreparedBlock
	err             error
	source          SyncBlockSource
	downloadElapsed time.Duration
	prepareElapsed  time.Duration
}

type shardStateDownloadJob struct {
	prev  ton.BlockIDExt
	block ton.BlockIDExt
}

type catchUpTiming struct {
	windowStarted time.Time
	blocks        uint32
	downloadWait  time.Duration
	apply         time.Duration
	persist       time.Duration
	checkpoints   uint32
}

type nextShardClientApplyStats struct {
	wall         time.Duration
	targetParse  time.Duration
	apply        time.Duration
	obtain       time.Duration
	resolverWait time.Duration
	applied      int
	downloaded   int
	reused       int
}

func newCatchUpTiming(now time.Time) catchUpTiming {
	return catchUpTiming{windowStarted: now}
}

func (t *catchUpTiming) reset(now time.Time) {
	*t = newCatchUpTiming(now)
}

func avgDuration(total time.Duration, count uint32) time.Duration {
	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}

type SyncCoordinator struct {
	log                     zerolog.Logger
	node                    *p2p.Node
	blockSync               *blocksync.Service
	storage                 SyncStore
	stateSync               *state.Syncer
	liveState               CurrentStatePublisher
	liveBlockCache          *storage.LiveBlockCache
	liveStateUsesBlockCache bool
	sync                    SyncObserver
	blockAppliedProcessor   *blockAppliedProcessorRunner
	blockReceivedObserver   *blockReceivedObserverRunner
	shardTopObserver        ShardTopBlockDescriptionObserver
	status                  *StatusTracker
	state                   *StateLifecycle
	maintenance             *MaintenanceRunner
	archive                 *ArchiveRunner

	broadcastAdmission *BroadcastAdmission

	// currentStateWake is a close-and-replace broadcast (the rawMasterchainNotify
	// pattern): at least five sync selectors wait on it concurrently, and a
	// single-token channel would let any one of them consume a wake meant for
	// another — e.g. a shard retry eating the wake that should end the parked
	// master probe hold. Access only through currentStateWakeChan and
	// broadcastCurrentStateWake.
	currentStateWakeMu    sync.Mutex
	currentStateWake      chan struct{}
	shardDescriptionWake  chan struct{}
	shardDescriptionMu    sync.Mutex
	shardDescriptionHints map[storage.BlockRootHash]shardDescriptionHint
	shardDescriptionOrder []storage.BlockRootHash

	preparedShardBlocks preparedShardBlockCache
	shardPrepareQueue   chan shardPrepareRequest

	nextMasterchainMx               sync.Mutex
	nextMasterchainQueue            map[storage.BlockRootHash]queuedMasterchainBlock
	nextMasterchainBySeqno          map[uint32]storage.BlockRootHash
	nextMasterchainBytes            int64
	nextMasterchainCandidates       map[storage.BlockRootHash]queuedMasterchainCandidate
	nextMasterchainCandidateBySeqno map[uint32]storage.BlockRootHash
	nextMasterchainCandidateBytes   int64
	validatorCache                  masterchainValidatorCache
	broadcastValidatorCache         broadcastValidatorCache
	parsedProofs                    parsedProofCache
	masterStateCacheMu              sync.Mutex
	masterStateCache                map[storage.BlockRootHash]*storage.BlockState
	masterStateCacheKeys            []storage.BlockRootHash
	masterStateCacheHead            int
	compressedStateMu               sync.Mutex
	compressedStateCache            map[storage.BlockRootHash]compressedBlockStateEntry
	compressedStateOrder            []compressedBlockStateOrderEntry
	compressedStateOrderHead        int
	compressedStateVersion          uint64
	monitorSplitDepthMu             sync.Mutex
	monitorSplitDepth               map[monitorSplitDepthKey]uint32

	archiveFromZero   bool
	syncUntil         uint32
	shutdownContext   context.Context
	syncDiskSpacePath string
	// syncUntilReached is set once this service decides it has synced up to
	// ton.sync_until. See syncUntilFrozen.
	syncUntilReached            atomic.Bool
	minSyncDiskFreeBytes        uint64
	masterPrepare               masterPrepareFlight
	shardOverlayMu              sync.Mutex
	shardOverlayPending         *masterShardOverlayUpdate
	shardOverlayRunning         bool
	shardOverlayReconcileMu     sync.Mutex
	shardOverlayReconciledSeqno uint32
	bindMu                      sync.Mutex
	started                     bool
	startOnce                   sync.Once
	wg                          sync.WaitGroup
}

// SyncStore is the persisted chain read surface used on the live block-stream
// workflow. Durable state writes and generation ownership belong to
// StateLifecycle, while archive maintenance is owned by MaintenanceRunner.
type SyncStore interface {
	CurrentState(context.Context) (*storage.CurrentState, error)
	LookupBlockBySeqNo(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error)
	BlockFull(context.Context, ton.BlockIDExt) (*storage.ServedBlockFull, error)
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockData(context.Context, ton.BlockIDExt) ([]byte, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
}

// SyncCoordinatorDependencies are the concrete runtime components used by the
// full block-stream workflow. Concrete pointers are intentional here: these
// calls sit on the per-block path, and the composition root owns the graph.
type SyncCoordinatorDependencies struct {
	Node               *p2p.Node
	BlockSync          *blocksync.Service
	Storage            SyncStore
	StateSync          *state.Syncer
	Status             *StatusTracker
	State              *StateLifecycle
	Maintenance        *MaintenanceRunner
	BroadcastAdmission *BroadcastAdmission
}

type SyncCoordinatorOptions struct {
	CurrentStatePublisher                   CurrentStatePublisher
	LiveBlockCache                          *storage.LiveBlockCache
	CurrentStatePublisherUsesLiveBlockCache bool
	ShutdownContext                         context.Context
	ArchiveFromZero                         bool
	SyncUntil                               uint32
	StorageDir                              string
	MinSyncDiskFreeBytes                    uint64
	SyncObserver                            SyncObserver
	BlockAppliedProcessor                   BlockAppliedProcessor
	BlockReceivedObserver                   BlockReceivedObserver
	ShardTopBlockDescriptionObserver        ShardTopBlockDescriptionObserver
}

type CurrentStatePublisher interface {
	SetLiveCurrentState(*storage.CurrentState)
	// SetLiveCurrentStateSnapshot takes ownership of an already-private
	// snapshot instead of cloning the state like SetLiveCurrentState does.
	SetLiveCurrentStateSnapshot(*storage.CurrentState)
	MarkLiveCurrentStateFlushed(*storage.CurrentState)
	MarkLiveBlockStatesFlushed([]ton.BlockIDExt)
	MarkLiveBlockFlushed(ton.BlockIDExt)
	PublishLiveBlockArtifacts(storage.LiveBlockArtifacts) error
	NonfinalBlockCacheEnabled() bool
	PublishNonfinalBlockArtifacts(storage.LiveBlockArtifacts, storage.LiveBlockNonfinalKind) error
	SetNonfinalCellLoader(cell.LazyCellLoader)
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
}

func NewSyncCoordinator(logger zerolog.Logger, deps SyncCoordinatorDependencies, opts SyncCoordinatorOptions) *SyncCoordinator {
	if opts.ShutdownContext == nil {
		opts.ShutdownContext = context.Background()
	}
	if opts.MinSyncDiskFreeBytes == 0 {
		opts.MinSyncDiskFreeBytes = defaultMinSyncDiskFreeBytes
	}
	return &SyncCoordinator{
		log:                             logger,
		node:                            deps.Node,
		blockSync:                       deps.BlockSync,
		storage:                         deps.Storage,
		stateSync:                       deps.StateSync,
		liveState:                       opts.CurrentStatePublisher,
		liveBlockCache:                  opts.LiveBlockCache,
		liveStateUsesBlockCache:         opts.CurrentStatePublisherUsesLiveBlockCache,
		sync:                            opts.SyncObserver,
		blockAppliedProcessor:           newBlockAppliedProcessorRunner(logger, opts.BlockAppliedProcessor),
		blockReceivedObserver:           newBlockReceivedObserverRunner(logger, opts.BlockReceivedObserver),
		shardTopObserver:                opts.ShardTopBlockDescriptionObserver,
		status:                          deps.Status,
		state:                           deps.State,
		maintenance:                     deps.Maintenance,
		broadcastAdmission:              deps.BroadcastAdmission,
		currentStateWake:                make(chan struct{}),
		shardDescriptionWake:            make(chan struct{}, 1),
		shardDescriptionHints:           map[storage.BlockRootHash]shardDescriptionHint{},
		shardPrepareQueue:               make(chan shardPrepareRequest, preparedShardBlockQueueSize),
		nextMasterchainQueue:            make(map[storage.BlockRootHash]queuedMasterchainBlock, nextMasterchainQueueLimit),
		nextMasterchainBySeqno:          make(map[uint32]storage.BlockRootHash, nextMasterchainQueueLimit),
		nextMasterchainCandidates:       make(map[storage.BlockRootHash]queuedMasterchainCandidate, nextMasterchainQueueLimit),
		nextMasterchainCandidateBySeqno: make(map[uint32]storage.BlockRootHash, nextMasterchainQueueLimit),
		masterStateCache:                make(map[storage.BlockRootHash]*storage.BlockState, masterStateCacheLimit),
		compressedStateCache:            make(map[storage.BlockRootHash]compressedBlockStateEntry, compressedBlockStateCacheLimit),
		monitorSplitDepth:               make(map[monitorSplitDepthKey]uint32),
		archiveFromZero:                 opts.ArchiveFromZero,
		syncUntil:                       opts.SyncUntil,
		shutdownContext:                 opts.ShutdownContext,
		syncDiskSpacePath:               opts.StorageDir,
		minSyncDiskFreeBytes:            opts.MinSyncDiskFreeBytes,
	}
}

// BindArchiveRunner completes the only construction cycle in the sync graph.
// It must be called once before Start.
func (s *SyncCoordinator) BindArchiveRunner(archive *ArchiveRunner) error {
	s.bindMu.Lock()
	defer s.bindMu.Unlock()

	if s.started {
		return errSyncCoordinatorStarted
	}
	if s.archive != nil {
		return errSyncCoordinatorArchiveAlreadyBound
	}
	if archive == nil {
		return errSyncCoordinatorArchiveNotBound
	}

	s.archive = archive
	return nil
}

// Start starts only the full block-stream workflow owned by SyncCoordinator.
// Status, state lifecycle, and maintenance are independent components and are
// started by the composition root.
func (s *SyncCoordinator) Start(ctx context.Context) error {
	s.bindMu.Lock()
	if s.archive == nil {
		s.bindMu.Unlock()
		return errSyncCoordinatorArchiveNotBound
	}
	s.started = true
	s.bindMu.Unlock()

	s.startOnce.Do(func() {
		s.runAsync(func() {
			s.runInitialStateSync(ctx)
		})
		s.runAsync(func() {
			s.runBlockProcessor(ctx)
		})
		s.runAsync(func() {
			s.runShardDescriptionProcessor(ctx)
		})
		s.runAsync(func() {
			s.runShardPrepareWorkers(ctx)
		})
	})

	return nil
}

func (s *SyncCoordinator) runAsync(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

func (s *SyncCoordinator) Wait() {
	s.wg.Wait()
}

func blockStateUtime(ctx context.Context, store blockMetaStore, state *storage.BlockState) int64 {
	if state.Parsed != nil && state.Parsed.GenUTime != 0 {
		return int64(state.Parsed.GenUTime)
	}
	return blockUtimeFromMeta(ctx, store, &state.Block)
}

func blockUtimeFromMeta(ctx context.Context, store blockMetaStore, block *ton.BlockIDExt) int64 {
	meta, err := store.BlockMeta(ctx, *block)
	if err != nil || meta.GenUTime == 0 {
		return 0
	}
	return int64(meta.GenUTime)
}

func (s *SyncCoordinator) runInitialStateSync(ctx context.Context) {
	s.node.SetRebroadcastQuiet(true)
	quiet := true
	defer func() {
		if quiet {
			s.node.SetRebroadcastQuiet(false)
		}
	}()

	for {
		// Taken before the sync attempt so state published while it runs is not
		// waited out by the poll below.
		stateWake := s.currentStateWakeChan()
		err := s.ensureCurrentState(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err == nil {
			if quiet {
				s.node.SetRebroadcastQuiet(false)
				quiet = false
			}
			if s.syncUntilFrozen() {
				s.log.Info().
					Uint32("sync_until", s.syncUntil).
					Msg("current state sync stopped after sync_until")
				return
			}
			if !s.waitCurrentStatePoll(ctx, stateWake, currentStateLivePollDelay) {
				return
			}
			continue
		}

		event := s.log.Error()
		message := "failed to initialize current state, will retry"
		retryDelay := 5 * time.Second
		if errors.Is(err, p2p.ErrStateNotAvailable) {
			retryDelay = time.Second
			event = s.log.Debug()
			message = "state snapshot is not available from current peers, will retry selected state"
		} else if isExpectedRetryError(err) {
			event = s.log.Debug()
		}
		event.Err(err).Dur("retry_in", retryDelay).Msg(message)

		if waitRetry(ctx, retryDelay) != nil {
			return
		}
	}
}

func (s *SyncCoordinator) runBlockProcessor(ctx context.Context) {
	masterHotJobs := make(chan blocksync.SyncedBlock, syncedBlockMasterHotQueueLimit)
	priorityJobs := make(chan blocksync.SyncedBlock, syncedBlockPriorityQueueLimit)
	jobs := make(chan blocksync.SyncedBlock, syncedBlockProcessorWorkers*2)
	var wg sync.WaitGroup
	for worker := 0; worker < syncedBlockMasterHotWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runBlockProcessorWorker(ctx, masterHotJobs, nil)
		}()
	}
	for worker := 0; worker < syncedBlockProcessorWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runBlockProcessorWorker(ctx, priorityJobs, jobs)
		}()
	}
	defer func() {
		close(masterHotJobs)
		close(priorityJobs)
		close(jobs)
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case synced, ok := <-s.blockSync.Blocks():
			if !ok {
				return
			}
			if s.syncUntilFrozen() {
				synced.Reject()
				continue
			}
			targetJobs := jobs
			if isHotMasterchainSyncedBlock(synced) {
				targetJobs = masterHotJobs
			} else if synced.Priority {
				targetJobs = priorityJobs
			}

			select {
			case targetJobs <- synced:
			case <-ctx.Done():
				synced.Reject()
				return
			}
		}
	}
}

func isHotMasterchainSyncedBlock(synced blocksync.SyncedBlock) bool {
	if !synced.Priority || synced.CatchUp {
		return false
	}
	block := synced.Downloaded.ID
	return block.Workchain == -1 && block.Shard == topShard
}

func (s *SyncCoordinator) runBlockProcessorWorker(ctx context.Context, priorityJobs, jobs <-chan blocksync.SyncedBlock) {
	for {
		synced, ok := nextSyncedBlockProcessorJob(ctx, priorityJobs, jobs)
		if !ok {
			return
		}
		if s.syncUntilFrozen() {
			synced.Reject()
			continue
		}

		if err := s.processSyncedBlock(ctx, synced); err != nil {
			synced.Reject()
			if errors.Is(err, errSyncedBlockDeferred) {
				continue
			}
			if !errors.Is(err, context.Canceled) {
				s.logProcessError(err)
			}
			continue
		}
		synced.Accept()
	}
}

func nextSyncedBlockProcessorJob(ctx context.Context, priorityJobs, jobs <-chan blocksync.SyncedBlock) (blocksync.SyncedBlock, bool) {
	select {
	case synced, ok := <-priorityJobs:
		if ok {
			return synced, true
		}
		priorityJobs = nil
	default:
	}

	for priorityJobs != nil || jobs != nil {
		select {
		case synced, ok := <-priorityJobs:
			if !ok {
				priorityJobs = nil
				continue
			}
			return synced, true
		case synced, ok := <-jobs:
			if !ok {
				jobs = nil
				continue
			}
			return synced, true
		case <-ctx.Done():
			return blocksync.SyncedBlock{}, false
		}
	}

	return blocksync.SyncedBlock{}, false
}

func (s *SyncCoordinator) processSyncedBlock(ctx context.Context, synced blocksync.SyncedBlock) (err error) {
	source := syncedBlockSource(synced)
	observation := SyncBlockObservation{
		Pipeline:         "blocksync",
		Chain:            syncChainLabel(synced.Downloaded.ID),
		Shard:            syncShardLabel(synced.Downloaded.ID),
		Source:           source,
		Origin:           syncBlockOriginForSource(source),
		Result:           "success",
		CatchUp:          synced.CatchUp,
		DownloadDuration: synced.DownloadElapsed,
	}
	defer func() {
		if errors.Is(err, errSyncedBlockDeferred) {
			return
		}
		if err != nil {
			observation.Result = syncBlockResultForError(err)
		}
		s.observeSyncBlock(observation)
	}()

	prepareStarted := time.Now()
	downloaded := synced.Downloaded
	verified, err := s.verifyDownloadedBlock(downloaded)
	if err != nil {
		observation.PrepareDuration = time.Since(prepareStarted)
		return err
	}
	if synced.Internal {
		verified.Source = SyncBlockSourceInternal
	}

	if downloaded.ID.Workchain == -1 && downloaded.ID.Shard == topShard {
		checked, err := s.validateSyncedMasterchainBlock(ctx, verified)
		if err != nil {
			observation.PrepareDuration = time.Since(prepareStarted)
			if errors.Is(err, storage.ErrNotFound) {
				s.queueMasterchainBroadcastCandidateFromSource(verified, synced.Trigger.SourcePeerID)
				s.wakeCurrentStateSync()
				return errSyncedBlockDeferred
			}
			return err
		}
		verified.consensusChecked = checked
		// Shared with the next-block probe, which reaches the same decoded
		// broadcast through the p2p hot cache and would otherwise walk the same
		// state update a second time. Ordering is unchanged: consensus is
		// validated first, so a deferred block never pays for the cells.
		prepared, err := s.prepareVerifiedMasterchainBlockShared(ctx, verified)
		if err != nil {
			observation.PrepareDuration = time.Since(prepareStarted)
			return err
		}
		prepared.PrepareElapsed = time.Since(prepareStarted)
		observation.PrepareDuration = prepared.PrepareElapsed
		s.rememberSeenMasterchainBlock(prepared.ID)
		s.queuePreparedMasterchainBlockFromSource(prepared, synced.Trigger.SourcePeerID)
		s.wakeCurrentStateSync()
		return nil
	}

	// Shard states are advanced only by the main sync pipeline. Broadcast blocks
	// are kept as download/cache hints so they cannot compete with live tail
	// apply; preparing them ahead here turns the commit-stage resolve into a
	// take of ready state-update cells.
	if synced.Internal {
		s.publishInternalNonfinalShardBlock(verified)
	}
	s.prepareShardBlockAheadFromVerified(verified)
	observation.PrepareDuration = time.Since(prepareStarted)
	return nil
}

func syncedBlockSource(block blocksync.SyncedBlock) SyncBlockSource {
	if block.Internal {
		return SyncBlockSourceInternal
	}
	if block.CatchUp {
		return SyncBlockSourcePeerCatchUp
	}
	return SyncBlockSourceBroadcast
}

func (s *SyncCoordinator) validateSyncedMasterchainBlock(ctx context.Context, block VerifiedBlock) (*checkedMasterchainConsensus, error) {
	prev, checked, err := s.validateMasterchainBlockConsensus(ctx, block, "validate synced masterchain block")
	if errors.Is(err, storage.ErrNotFound) {
		s.log.Debug().
			Str("block", block.BlockRef()).
			Str("prev", storage.FormatBlockRef(prev)).
			Msg("skipping synced masterchain block until previous state is available for signature validation")
	}
	return checked, err
}

func (s *SyncCoordinator) validateVerifiedMasterchainBlock(ctx context.Context, block VerifiedBlock) (*checkedMasterchainConsensus, error) {
	_, checked, err := s.validateMasterchainBlockConsensus(ctx, block, "validate masterchain block")
	return checked, err
}

func (s *SyncCoordinator) validateMasterchainBlockConsensus(ctx context.Context, block VerifiedBlock, validationLabel string) (ton.BlockIDExt, *checkedMasterchainConsensus, error) {
	if len(block.Meta.PrevRefs) != 1 {
		return ton.BlockIDExt{}, nil, fmt.Errorf("masterchain block %s has no single previous ref", block.BlockRef())
	}

	prev := block.Meta.PrevRefs[0]
	current, err := s.loadMasterStateForConsensus(ctx, prev)
	if errors.Is(err, storage.ErrNotFound) {
		return prev, nil, fmt.Errorf("previous masterchain state %s: %w", storage.FormatBlockRef(prev), storage.ErrNotFound)
	}
	if err != nil {
		return prev, nil, fmt.Errorf("load previous masterchain state %s: %w", storage.FormatBlockRef(prev), err)
	}

	if block.consensus == nil {
		return prev, nil, fmt.Errorf("masterchain block %s has no prepared consensus proof", block.BlockRef())
	}
	checked, err := s.checkMasterchainBlockConsensusWithProof(current, block.consensus)
	if err != nil {
		return prev, nil, fmt.Errorf("%s %s: %w", validationLabel, block.BlockRef(), err)
	}
	return prev, checked, nil
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// waitCurrentStatePoll parks between sync attempts. The caller passes the wake
// it took before the attempt, so state published while that attempt ran is not
// waited out here.
func (s *SyncCoordinator) waitCurrentStatePoll(ctx context.Context, wake <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-timer.C:
		return true
	}
}

// currentStateWakeChan returns the channel the next wake closes. Waiters that
// loop must take a fresh channel after each release, and take it before
// re-checking the state the wake announces, so a wake between the check and the
// select is never lost.
func (s *SyncCoordinator) currentStateWakeChan() <-chan struct{} {
	s.currentStateWakeMu.Lock()
	ch := s.currentStateWake
	s.currentStateWakeMu.Unlock()
	return ch
}

func (s *SyncCoordinator) broadcastCurrentStateWake() {
	s.currentStateWakeMu.Lock()
	if s.currentStateWake != nil {
		close(s.currentStateWake)
		s.currentStateWake = make(chan struct{})
	}
	s.currentStateWakeMu.Unlock()
}

func (s *SyncCoordinator) wakeCurrentStateSync() {
	s.broadcastCurrentStateWake()
	s.maintenance.wake()
}

func (s *SyncCoordinator) logProcessError(err error) {
	event := s.log.Error()
	if isExpectedRetryError(err) {
		event = s.log.Debug()
	}
	event.Err(err).Msg("failed to process synced block")
}

func isExpectedRetryError(err error) bool {
	if errors.Is(err, p2p.ErrStateNotAvailable) ||
		errors.Is(err, p2p.ErrBlockNotAvailable) ||
		errors.Is(err, errCellGenerationMigrationRunning) ||
		errors.Is(err, errCellGenerationMigrationStopping) ||
		errors.Is(err, errCellGenerationMigrationStopped) ||
		errors.Is(err, errPendingCellGenerationCompaction) ||
		errors.Is(err, errPersistentStateGCActive) ||
		errors.Is(err, errArchiveTTLGCActive) ||
		errors.Is(err, errExclusiveServiceTaskHighReadAmp) ||
		errors.Is(err, errExclusiveServiceTaskHighLag) ||
		errors.Is(err, errStateSerializationDelayed) ||
		errors.Is(err, errStateSerializationLowDiskSpace) ||
		errors.Is(err, errStateSerializationCanceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return isTimeoutError(err)
}

func isTimeoutError(err error) bool {
	var timeout timeoutError
	return errors.As(err, &timeout) && timeout.Timeout()
}

func formatCatchUpProgress(done, total uint32) string {
	if total == 0 || done >= total {
		return "100.0%"
	}

	return formatCatchUpProgressPercent(float64(done) * 100 / float64(total))
}

func formatLagCatchUpProgress(startRemaining int64, remaining int64) string {
	if startRemaining <= 0 || remaining <= 0 {
		return "100.0%"
	}
	if remaining >= startRemaining {
		return "0.0%"
	}

	progress := float64(startRemaining-remaining) * 100 / float64(startRemaining)
	return formatCatchUpProgressPercent(progress)
}

func formatCatchUpProgressPercent(progress float64) string {
	if progress > 0 && progress < 0.1 {
		return fmt.Sprintf("%.3f%%", progress)
	}
	if progress > 100 {
		progress = 100
	}
	return fmt.Sprintf("%.1f%%", progress)
}

func formatBlockRate(done uint32, elapsed time.Duration) string {
	return formatBlockRate64(uint64(done), elapsed)
}

func formatBlockRate64(done uint64, elapsed time.Duration) string {
	if done == 0 || elapsed <= 0 {
		return "0 blocks/s"
	}

	rate := float64(done) / elapsed.Seconds()
	switch {
	case rate >= 100:
		return fmt.Sprintf("%.0f blocks/s", rate)
	case rate >= 10:
		return fmt.Sprintf("%.1f blocks/s", rate)
	default:
		return fmt.Sprintf("%.2f blocks/s", rate)
	}
}

func formatCatchUpETA(done, total uint32, elapsed time.Duration) string {
	if done == 0 || done >= total || elapsed <= 0 {
		return "unknown"
	}

	rate := float64(done) / elapsed.Seconds()
	if rate <= 0 {
		return "unknown"
	}

	remaining := total - done
	eta := time.Duration(float64(remaining) / rate * float64(time.Second)).Round(time.Second)
	return eta.String()
}

func formatLagCatchUpETA(startRemaining int64, remaining int64, elapsed time.Duration) string {
	if startRemaining <= 0 || remaining <= 0 || remaining >= startRemaining || elapsed <= 0 {
		return "unknown"
	}

	done := startRemaining - remaining
	rate := float64(done) / elapsed.Seconds()
	if rate <= 0 {
		return "unknown"
	}

	eta := time.Duration(float64(remaining) / rate * float64(time.Second)).Round(time.Second)
	return eta.String()
}
