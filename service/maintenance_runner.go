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
)

var (
	errMaintenanceRunnerAlreadyBound = errors.New("maintenance runner dependencies are already bound")
	errMaintenanceRunnerNotBound     = errors.New("maintenance runner dependencies are not bound")
	errMaintenanceRunnerStarted      = errors.New("maintenance runner is already started")
)

type maintenanceSyncGate interface {
	syncUntilFrozen() bool
	syncUntilTarget() uint32
}

// MaintenanceStore is the storage policy surface used by background cleanup
// and its admission checks. Generation reads here only select maintenance
// work; cell data stays owned by StateLifecycle's generation handle.
type MaintenanceStore interface {
	MaxReadAmp(context.Context) (int64, error)
	BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error)
	ActiveCellGeneration(context.Context) (storage.CellGenerationInfo, error)
	PendingCellGenerationMigration(context.Context) (storage.CellGenerationInfo, error)
	PruneArchivePackages(context.Context, uint32, int) (storage.ArchivePruneStats, error)
	PruneExpiredPersistentStateFiles(context.Context, uint64, int, int) (storage.PersistentStatePruneStats, error)
}

// MaintenanceRunnerOptions configures background state and archive cleanup.
type MaintenanceRunnerOptions struct {
	ArchiveTTL                time.Duration
	PersistentStateKeepRecent int
	ShutdownContext           context.Context
}

// MaintenanceRunner owns the maintenance scheduler, its wake/readiness state,
// and the policy that serializes expensive background storage tasks.
type MaintenanceRunner struct {
	log     zerolog.Logger
	storage MaintenanceStore
	status  *StatusTracker

	archiveTTL                time.Duration
	persistentStateKeepRecent int
	shutdownContext           context.Context

	automaticStateSerializationReady atomic.Bool
	maintenanceWake                  chan struct{}

	exclusiveTaskMu sync.Mutex
	exclusiveTask   exclusiveServiceTask

	bindMu  sync.Mutex
	bound   bool
	started bool
	state   *StateLifecycle
	sync    maintenanceSyncGate

	startOnce sync.Once
	wg        sync.WaitGroup
}

func NewMaintenanceRunner(logger zerolog.Logger, store MaintenanceStore, status *StatusTracker, opts MaintenanceRunnerOptions) *MaintenanceRunner {
	if opts.PersistentStateKeepRecent == 0 {
		opts.PersistentStateKeepRecent = DefaultPersistentStateKeepRecent
	}
	if opts.ShutdownContext == nil {
		opts.ShutdownContext = context.Background()
	}

	return &MaintenanceRunner{
		log:                       logger,
		storage:                   store,
		status:                    status,
		archiveTTL:                opts.ArchiveTTL,
		persistentStateKeepRecent: opts.PersistentStateKeepRecent,
		shutdownContext:           opts.ShutdownContext,
		maintenanceWake:           make(chan struct{}, 1),
	}
}

// Bind completes the cyclic maintenance/state construction. It may be called
// exactly once and only before Start.
func (m *MaintenanceRunner) Bind(state *StateLifecycle, syncGate maintenanceSyncGate) error {
	m.bindMu.Lock()
	defer m.bindMu.Unlock()

	if m.started {
		return errMaintenanceRunnerStarted
	}
	if m.bound {
		return errMaintenanceRunnerAlreadyBound
	}
	if state == nil || syncGate == nil {
		return errMaintenanceRunnerNotBound
	}

	m.state = state
	m.sync = syncGate
	m.bound = true
	return nil
}

func (m *MaintenanceRunner) Start(ctx context.Context) error {
	m.bindMu.Lock()
	if !m.bound {
		m.bindMu.Unlock()
		return errMaintenanceRunnerNotBound
	}
	m.started = true
	m.bindMu.Unlock()

	m.startOnce.Do(func() {
		m.runAsync(func() {
			m.runDelayedServiceMaintenance(ctx)
		})
	})

	return nil
}

func (m *MaintenanceRunner) runAsync(fn func()) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		fn()
	}()
}

func (m *MaintenanceRunner) Wait() {
	m.wg.Wait()
}

func (m *MaintenanceRunner) wake() {
	select {
	case m.maintenanceWake <- struct{}{}:
	default:
	}
}
