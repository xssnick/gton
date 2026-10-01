package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var (
	errStateLifecycleAlreadyBound = errors.New("state lifecycle transitions are already bound")
	errStateLifecycleNotBound     = errors.New("state lifecycle transitions are not bound")
	errStateLifecycleStarted      = errors.New("state lifecycle is already started")
	errStatePersistBusy           = errors.New("current state persistence is busy")
)

const stateSerializationMinDiskFreeBytes = 30 << 30

// StateLifecycleOptions configures state persistence, serialization, and cell
// generation maintenance.
type StateLifecycleOptions struct {
	ShutdownContext                    context.Context
	StateFilesDir                      string
	StateTTL                           time.Duration
	StorageDir                         string
	MinStateSerializationDiskFreeBytes uint64
	DisableStateSerialization          bool
	PersistentStateLargeBOCBatchSize   int
	StateSerializeOnePass              bool
	NextBlockCheckpointBlocks          uint32
	CheckpointBytes                    uint64
	SyncBackpressureWindows            uint32
}

// stateBlockTransitions is owned by StateLifecycle and exposes only the chain
// operations needed by the cold cell-generation migration path.
type stateBlockTransitions interface {
	applyMasterchainTransition(context.Context, *storage.BlockState, PreparedBlock, *checkedMasterchainConsensus, stateUpdateApplier, *blockAppliedObserverMeta) (*storage.BlockState, masterchainApplyTiming, error)
	currentStateForNextMasterState(context.Context, *storage.CurrentState, *storage.BlockState, []ton.BlockIDExt, *shardStateResolver) (*storage.CurrentState, nextShardClientApplyStats, error)
	applyResolvedShardBlock(context.Context, ton.BlockIDExt, []*storage.BlockState, PreparedBlock, stateUpdateApplier, *blockAppliedObserverMeta) (*storage.BlockState, error)
}

// stateCheckpointTransitions publishes the result of an atomic cell generation
// switch and wakes chain consumers. Normal per-block checkpoints do not dispatch
// through this interface.
type stateCheckpointTransitions interface {
	publishCommittedCurrentState(*storage.CurrentState)
	broadcastCurrentStateWake()
}

// stateMigrationBlockLoader keeps stored-block loading separate from chain
// transitions. Migration is a cold path, so this narrow interface does not sit
// in normal block application.
type stateMigrationBlockLoader interface {
	loadNext(context.Context, ton.BlockIDExt) (PreparedBlock, error)
	load(context.Context, ton.BlockIDExt) (PreparedBlock, error)
}

type statePersistentArtifactLoader interface {
	loadPersistentStateArtifact(context.Context, persistentStateArtifactStore, ton.BlockIDExt, ton.BlockIDExt, uint32, []byte) (storage.DownloadedState, error)
}

// stateSyncGate is owned by StateLifecycle. Serialization reads the terminal
// sync boundary directly instead of forwarding through MaintenanceRunner.
type stateSyncGate interface {
	syncUntilFrozen() bool
	syncUntilTarget() uint32
}

type storedStateMigrationBlockLoader struct {
	store stateMigrationBlockStore
}

func (l storedStateMigrationBlockLoader) loadNext(ctx context.Context, previous ton.BlockIDExt) (PreparedBlock, error) {
	next, err := l.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: -1, Shard: topShard, SeqNo: previous.SeqNo + 1})
	if err != nil {
		return PreparedBlock{}, err
	}

	return l.load(ctx, next)
}

func (l storedStateMigrationBlockLoader) load(ctx context.Context, block ton.BlockIDExt) (PreparedBlock, error) {
	full, err := l.store.BlockFull(ctx, block)
	if err != nil {
		return PreparedBlock{}, err
	}

	return prepareStoredBlockForApply(block, full)
}

// StateLifecycle owns durable-state checkpoints, state-cell loaders,
// serialization, and cell-generation rotation. It is deliberately referenced
// by SyncCoordinator as a named field so the normal apply path remains a direct concrete
// pointer access.
type StateLifecycle struct {
	log             zerolog.Logger
	checkpointStore stateCheckpointStore
	generationStore cellGenerationStore
	status          *StatusTracker

	stateCellPrewrite *stateCellPrewriter
	artifactPrewrite  *artifactPrewriter

	stateMu sync.Mutex

	currentStatePersistMu    sync.Mutex
	currentStatePersistErrMu sync.Mutex
	currentStatePersistErr   error

	stateCellLoaderMu       sync.RWMutex
	stateCellLoaders        map[uint64]cell.LazyCellLoader
	stateCellLoaderSnapshot atomic.Value
	nextStateCellLoaderID   uint64

	stateSerializer *stateSerializer

	stateTTL                           time.Duration
	syncDiskSpacePath                  string
	minStateSerializationDiskFreeBytes uint64
	nextBlockCheckpointBlocks          uint32
	checkpointBytes                    uint64
	syncBackpressureWindows            uint32
	shutdownContext                    context.Context

	cellMigrationMu                 sync.Mutex
	cellGenerationMigrationRun      *cellGenerationMigrationRun
	cellGenerationMigrationStopping bool
	cellGenerationSwitchRequested   bool
	cellGenerationNextBlockYieldAt  time.Time
	cellGenerationSwitching         bool

	bindMu                sync.Mutex
	bound                 bool
	started               bool
	blockTransitions      stateBlockTransitions
	checkpointTransitions stateCheckpointTransitions
	blockLoader           stateMigrationBlockLoader
	artifactLoader        statePersistentArtifactLoader
	syncGate              stateSyncGate
	maintenance           *MaintenanceRunner

	startOnce sync.Once
	wg        sync.WaitGroup
}

func NewStateLifecycle(logger zerolog.Logger, store stateLifecycleStore, status *StatusTracker, opts StateLifecycleOptions) *StateLifecycle {
	if opts.ShutdownContext == nil {
		opts.ShutdownContext = context.Background()
	}
	if opts.MinStateSerializationDiskFreeBytes == 0 {
		opts.MinStateSerializationDiskFreeBytes = stateSerializationMinDiskFreeBytes
	}
	if opts.NextBlockCheckpointBlocks == 0 {
		opts.NextBlockCheckpointBlocks = DefaultNextBlockCheckpointBlocks
	}
	if opts.CheckpointBytes == 0 {
		opts.CheckpointBytes = DefaultCheckpointBytes
	}
	if opts.SyncBackpressureWindows == 0 {
		opts.SyncBackpressureWindows = DefaultSyncBackpressureWindows
	}

	checkpointBackpressure := checkpointBackpressureBytes(opts.CheckpointBytes, opts.SyncBackpressureWindows)

	var artifactPrewrite *artifactPrewriter
	if prewriteStore, ok := store.(checkpointArtifactPrewriteStore); ok {
		artifactPrewrite = newArtifactPrewriterWithStore(logger, prewriteStore, checkpointBackpressure)
	}

	lifecycle := &StateLifecycle{
		log:                                logger,
		checkpointStore:                    store,
		generationStore:                    store,
		status:                             status,
		stateCellPrewrite:                  newStateCellPrewriter(logger, store, checkpointBackpressure),
		artifactPrewrite:                   artifactPrewrite,
		stateCellLoaders:                   make(map[uint64]cell.LazyCellLoader),
		stateTTL:                           opts.StateTTL,
		syncDiskSpacePath:                  opts.StorageDir,
		minStateSerializationDiskFreeBytes: opts.MinStateSerializationDiskFreeBytes,
		nextBlockCheckpointBlocks:          opts.NextBlockCheckpointBlocks,
		checkpointBytes:                    opts.CheckpointBytes,
		syncBackpressureWindows:            opts.SyncBackpressureWindows,
		shutdownContext:                    opts.ShutdownContext,
		blockLoader:                        storedStateMigrationBlockLoader{store: store},
	}
	lifecycle.stateSerializer = newStateSerializer(logger, store, opts.StateFilesDir, opts.DisableStateSerialization, opts.PersistentStateLargeBOCBatchSize, opts.StateSerializeOnePass)
	lifecycle.stateSerializer.runAsync = lifecycle.runAsync
	lifecycle.storeStateCellLoadersSnapshotLocked()

	return lifecycle
}

type stateCheckpointPolicy struct {
	blocks              uint32
	bytes               uint64
	backpressureWindows uint32
	backpressureBlocks  uint32
	backpressureBytes   uint64
}

func (s *StateLifecycle) nextCheckpointPolicy() stateCheckpointPolicy {
	return stateCheckpointPolicy{
		blocks:              s.nextBlockCheckpointBlocks,
		bytes:               s.checkpointBytes,
		backpressureWindows: s.syncBackpressureWindows,
		backpressureBlocks:  checkpointBackpressureBlocks(s.nextBlockCheckpointBlocks, s.syncBackpressureWindows),
		backpressureBytes:   checkpointBackpressureBytes(s.checkpointBytes, s.syncBackpressureWindows),
	}
}

func (s *StateLifecycle) enqueueCheckpointArtifact(
	state *storage.BlockState,
	artifact *storage.ServedBlockFull,
) (uint64, func() error, error) {
	return s.artifactPrewrite.enqueueDetached(state, artifact)
}

// BindTransitions completes the cyclic SyncCoordinator/StateLifecycle construction.
// Binding is a one-shot operation and must happen before Start.
func (s *StateLifecycle) BindTransitions(
	block stateBlockTransitions,
	checkpoint stateCheckpointTransitions,
	artifacts statePersistentArtifactLoader,
	syncGate stateSyncGate,
	maintenance *MaintenanceRunner,
) error {
	s.bindMu.Lock()
	defer s.bindMu.Unlock()

	if s.started {
		return errStateLifecycleStarted
	}
	if s.bound {
		return errStateLifecycleAlreadyBound
	}
	if block == nil || checkpoint == nil || artifacts == nil || syncGate == nil || maintenance == nil {
		return errStateLifecycleNotBound
	}

	s.blockTransitions = block
	s.checkpointTransitions = checkpoint
	s.artifactLoader = artifacts
	s.syncGate = syncGate
	s.maintenance = maintenance
	s.bound = true
	return nil
}

func (s *StateLifecycle) Start(ctx context.Context) error {
	s.bindMu.Lock()
	if !s.bound {
		s.bindMu.Unlock()
		return errStateLifecycleNotBound
	}
	s.started = true
	s.bindMu.Unlock()

	s.startOnce.Do(func() {
		s.stateCellPrewrite.start(ctx, s.shutdownContext, s.runAsync)
		s.artifactPrewrite.start(ctx, s.runAsync)
	})

	return nil
}

func (s *StateLifecycle) runAsync(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

func (s *StateLifecycle) Wait() {
	s.wg.Wait()
}

// lockCurrentStateTransition serializes durable-current catch-up with the
// atomic cell-generation switch. Both workflows change the same persisted
// current-state boundary, so this lock belongs to StateLifecycle.
func (s *StateLifecycle) lockCurrentStateTransition() {
	s.stateMu.Lock()
}

func (s *StateLifecycle) unlockCurrentStateTransition() {
	s.stateMu.Unlock()
}

func (s *StateLifecycle) beginCurrentStatePersist() error {
	s.currentStatePersistMu.Lock()
	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		return err
	}

	return nil
}

func (s *StateLifecycle) tryBeginCurrentStatePersist() error {
	if !s.currentStatePersistMu.TryLock() {
		return errStatePersistBusy
	}
	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		return err
	}

	return nil
}

func (s *StateLifecycle) finishCurrentStatePersist() {
	s.currentStatePersistMu.Unlock()
}

// newArchiveStateCellWindow creates an archive-owned cell window while keeping
// loader and prewriter policy inside StateLifecycle.
func (s *StateLifecycle) newArchiveStateCellWindow(base cell.LazyCellLoader) *stateCellWindowCache {
	window := newStateCellWindowCache(base, &s.status.lazyCellLoads)
	window.setPrewriter(s.stateCellPrewrite)
	return window
}

// CellLoader returns the lifecycle-owned lazy loader used by live, nonfinal
// state readers. It is safe to retain for the lifetime of StateLifecycle.
func (s *StateLifecycle) CellLoader() cell.LazyCellLoader {
	return s.stateCellLoader()
}

// newNextStateCellWindow creates and retains the loader used by one live
// next-block run. The returned release must be called when the run stops.
func (s *StateLifecycle) newNextStateCellWindow() (*stateCellWindowCache, func()) {
	base := s.stateCellLoader()
	window := newStateCellWindowCache(base, &s.status.lazyCellLoads)
	window.setPrewriter(s.stateCellPrewrite)

	return window, s.retainStateCellLoader(window.retainedLoader(base))
}

func (s *StateLifecycle) checkpointTargetBytes() uint64 {
	return s.checkpointBytes
}
