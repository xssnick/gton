package gton

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/xssnick/gton/api/httpapi"
	"github.com/xssnick/gton/api/liteserver"
	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/internal/metrics"
	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxNodeBOCCells           = 4_000_000_000
	topShard                  = int64(-1 << 63)
	liteserverShutdownTimeout = 10 * time.Second
)

// ErrShutdownIncomplete reports that the second shutdown signal interrupted
// durable extension cleanup. Composition roots must not close extension-owned
// stores after this error: the process is exiting and the operating system is
// the only safe final owner while an accepted write may still be in flight.
var ErrShutdownIncomplete = errors.New("node shutdown incomplete")

func waitForLiteserverShutdown(ctx context.Context, wait func()) error {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type MetricsOptions struct {
	Enabled    bool
	ListenAddr string
	Namespace  string
}

type StorageOptions struct {
	Dir                string
	CellTotalCacheSize int64
	DecodedCellCache   DecodedCellCacheOptions
	// CellRecordCacheBytes budgets the encoded cell record cache in BYTES: the
	// tier between the decoded cell cache and pebble, holding raw celldb
	// records pre-decode in region rings allocated outside the GC under cgo.
	// Bytes rather than entries because this tier has no per-object GC cost;
	// the derived index adds ~22-25% on top. Zero disables the tier — callers
	// that want the default must pass it explicitly (the config layer does).
	CellRecordCacheBytes             int64
	CellShardMemTableSize            int
	CellMemTableStopWritesThreshold  int
	LargeBOCShardReadWorkers         int
	PersistentStateLargeBOCBatchSize int
	PersistentStateKeepRecent        int
	StateSerializeOnePass            bool
	ArtifactFileMaxOpen              int
}

// DecodedCellCacheOptions sizes the decoded cell cache, in ENTRIES.
//
// There is ONE such cache, shared by every consumer that decodes a cell out of
// celldb: the lightserver, proof building, the archive importer, sync and
// download, collation and validation. It is deliberately not per-consumer.
// Collation and validation must receive the same *cell.Cell for a given parent —
// the validator's live-successor carry-back compares tip states by pointer — and
// two caches cannot both supply one object.
//
// Entries, not bytes, because each entry is a live Go object graph (~9.9 live
// objects, ~820 B measured) that every GC mark cycle has to scan: mark cost
// tracks object count, so an entry cap bounds the cost directly and a byte
// budget does not. This is also why the default is small. Bulk capacity belongs
// in the off-heap tiers — StorageOptions.CellTotalCacheSize is pebble's block
// cache, and below it the OS page cache — where a resident byte costs nothing to
// mark. The two knobs are independent and have different cost models; raising
// CellTotalCacheSize does not and must not move this one.
type DecodedCellCacheOptions struct {
	Enabled bool
	Shards  int
	// Entries sizes the cache. Zero takes the default.
	Entries int
}

// NodeOptions configures RunNode.
type NodeOptions struct {
	GlobalConfig *liteclient.GlobalConfig
	Logger       zerolog.Logger
	P2P          p2p.Options
	Metrics      MetricsOptions
	Storage      StorageOptions
	Extension    hooks.ExtensionFactory
	HTTPAPI      *HTTPAPIOptions
	Liteserver   *LiteserverOptions

	SyncBefore                time.Duration
	SyncUntil                 uint32
	ArchiveFromZero           bool
	StateTTL                  time.Duration
	ArchiveTTL                time.Duration
	NextCheckpointBlocks      uint32
	ArchiveCheckpointBlocks   uint32
	CheckpointBytes           uint64
	SyncBackpressureWindows   uint32
	DisableStateSerialization bool

	LiveView *liveview.Options

	ArchiveCheckpointPeriod time.Duration
	ArchivePrefetchWindows  int

	ConsoleInput  io.Reader
	ConsoleOutput io.Writer
}

func DefaultNodeOptions() NodeOptions {
	return NodeOptions{
		Logger:                  zerolog.Nop(),
		ArchiveCheckpointPeriod: service.DefaultArchiveCatchUpCheckpointPeriod,
		ArchivePrefetchWindows:  service.DefaultArchiveCatchUpPrefetchWindows,
	}
}

func applyNodeOptionDefaults(opts NodeOptions) NodeOptions {
	if opts.NextCheckpointBlocks == 0 {
		opts.NextCheckpointBlocks = service.DefaultNextBlockCheckpointBlocks
	}
	if opts.ArchiveCheckpointBlocks == 0 {
		opts.ArchiveCheckpointBlocks = service.DefaultArchiveCatchUpCheckpointBlocks
	}
	if opts.CheckpointBytes == 0 {
		opts.CheckpointBytes = service.DefaultCheckpointBytes
	}
	if opts.SyncBackpressureWindows == 0 {
		opts.SyncBackpressureWindows = service.DefaultSyncBackpressureWindows
	}
	if opts.ArchiveCheckpointPeriod == 0 {
		opts.ArchiveCheckpointPeriod = service.DefaultArchiveCatchUpCheckpointPeriod
	}
	if opts.ArchivePrefetchWindows == 0 {
		opts.ArchivePrefetchWindows = service.DefaultArchiveCatchUpPrefetchWindows
	}
	if opts.Storage.PersistentStateKeepRecent == 0 {
		opts.Storage.PersistentStateKeepRecent = service.DefaultPersistentStateKeepRecent
	}
	return opts
}

// RunNode runs the gton node from already resolved startup options.
func RunNode(parentCtx context.Context, runOpts NodeOptions) (returnErr error) {
	if runOpts.ArchivePrefetchWindows < 0 {
		return fmt.Errorf("archive prefetch windows cannot be negative: %d", runOpts.ArchivePrefetchWindows)
	}
	runOpts = applyNodeOptionDefaults(runOpts)

	baseLogger := runOpts.Logger
	logger := baseLogger.With().Str("component", "main").Logger()
	cell.MaxBOCCells = maxNodeBOCCells
	logger.Debug().Int("max_boc_cells", cell.MaxBOCCells).Msg("configured BOC parser limits")

	globalConfig := runOpts.GlobalConfig
	if globalConfig == nil {
		return fmt.Errorf("global config is required")
	}

	ctx, shutdownCtx, cancelRun, stop := signalContexts(parentCtx)
	defer stop()
	networkCtx, cancelNetwork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelNetwork()

	globalConfigZeroState, err := zeroStateBlockFromGlobalConfig(globalConfig)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load global config zerostate")
		return fmt.Errorf("load global config zerostate: %w", err)
	}

	opts := runOpts.P2P
	opts.GlobalConfig = globalConfig
	opts.Logger = &baseLogger

	metricsOpts := runOpts.Metrics
	var runtimeMetrics *metrics.Metrics
	var syncObserver service.SyncObserver
	if metricsOpts.Enabled {
		runtimeMetrics = metrics.New(metricsOpts.Namespace)
		if err = startMetricsServer(ctx, logger, metricsOpts.ListenAddr, runtimeMetrics.Handler()); err != nil {
			logger.Error().Err(err).Str("metrics_addr", metricsOpts.ListenAddr).Msg("failed to start metrics server")
			return fmt.Errorf("start metrics server %s: %w", metricsOpts.ListenAddr, err)
		}
		syncObserver = runtimeMetrics
	}
	syncBefore := runOpts.SyncBefore
	syncUntil := runOpts.SyncUntil
	archiveFromZero := runOpts.ArchiveFromZero
	stateTTL := runOpts.StateTTL
	archiveTTL := runOpts.ArchiveTTL
	if archiveFromZero && stateTTL != 0 {
		logger.Error().
			Dur("state_ttl", stateTTL).
			Msg("ton.sync_before=-1 requires ton.state_ttl=0")
		return fmt.Errorf("ton.sync_before=-1 requires ton.state_ttl=0")
	}
	if archiveFromZero && archiveTTL != 0 {
		logger.Error().
			Dur("archive_ttl", archiveTTL).
			Msg("ton.sync_before=-1 requires ton.archive_ttl=0")
		return fmt.Errorf("ton.sync_before=-1 requires ton.archive_ttl=0")
	}
	nextCheckpointBlocks := runOpts.NextCheckpointBlocks
	archiveCheckpointBlocks := runOpts.ArchiveCheckpointBlocks
	checkpointBytes := runOpts.CheckpointBytes
	syncBackpressureWindows := runOpts.SyncBackpressureWindows
	broadcastAdmissionLogger := baseLogger.With().Str("component", "broadcast_admission").Logger()
	broadcastAdmission := service.NewBroadcastAdmission(broadcastAdmissionLogger, nextCheckpointBlocks, syncBackpressureWindows)

	storageOpts := runOpts.Storage
	storageDir := storageOpts.Dir
	if storageDir == "" {
		logger.Error().Msg("storage.dir is required")
		return fmt.Errorf("storage.dir is required")
	}
	cellTotalCacheSize := storageOpts.CellTotalCacheSize
	decodedCellCacheOpts := storageOpts.DecodedCellCache
	cellShardMemTableSize := storageOpts.CellShardMemTableSize
	cellMemTableStopWritesThreshold := storageOpts.CellMemTableStopWritesThreshold
	largeBOCShardReadWorkers := storageOpts.LargeBOCShardReadWorkers
	persistentStateLargeBOCBatchSize := storageOpts.PersistentStateLargeBOCBatchSize
	artifactFileMaxOpen := storageOpts.ArtifactFileMaxOpen
	storageOpenStarted := time.Now()
	logger.Info().
		Str("storage", "pebble").
		Str("dir", storageDir).
		Int64("cell_total_cache_size", cellTotalCacheSize).
		Bool("decoded_cell_cache_enabled", decodedCellCacheOpts.Enabled).
		Int("decoded_cell_cache_shards", decodedCellCacheOpts.Shards).
		Int("decoded_cell_cache_entries_requested", decodedCellCacheOpts.Entries).
		Int64("cell_record_cache_bytes", storageOpts.CellRecordCacheBytes).
		Int("cell_shard_memtable_size", cellShardMemTableSize).
		Int("cell_memtable_stop_writes_threshold", cellMemTableStopWritesThreshold).
		Int("large_boc_shard_read_workers", largeBOCShardReadWorkers).
		Int("persistent_state_large_boc_batch_size", persistentStateLargeBOCBatchSize).
		Int("artifact_file_max_open", artifactFileMaxOpen).
		Msg("opening storage")
	store, err := pebblestore.Open(pebblestore.Options{
		Dir:                     storageDir,
		Logger:                  &baseLogger,
		CellCacheSize:           cellTotalCacheSize,
		DisableDecodedCellCache: !decodedCellCacheOpts.Enabled,
		DecodedCellCacheShards:  decodedCellCacheOpts.Shards,

		DecodedCellCacheEntries: decodedCellCacheOpts.Entries,
		CellRecordCacheBytes:    storageOpts.CellRecordCacheBytes,

		CellShardMemTableSize:           cellShardMemTableSize,
		CellMemTableStopWritesThreshold: cellMemTableStopWritesThreshold,
		LargeBOCShardReadWorkers:        largeBOCShardReadWorkers,
		ArtifactFileMaxOpen:             artifactFileMaxOpen,
	})
	if err != nil {
		logger.Error().Err(err).Str("dir", storageDir).Msg("failed to open pebble storage")
		return fmt.Errorf("open pebble storage %s: %w", storageDir, err)
	}
	stateFilesDir := store.StateFilesDir()
	if opts.PeerStorage == nil {
		opts.PeerStorage = store
	}
	if opts.StateArtifactStorage == nil {
		opts.StateArtifactStorage = store
	}
	if opts.PeerCache == nil {
		opts.PeerCache = store
	}
	if opts.FastSyncCertificateStorage == nil {
		opts.FastSyncCertificateStorage = store
	}
	logger.Info().
		Str("storage", "pebble").
		Str("dir", storageDir).
		Dur("elapsed", time.Since(storageOpenStarted)).
		Msg("configured storage")
	opts.StateFilesDir = stateFilesDir
	liveBlockCache := storage.NewLiveBlockCache(storage.DefaultLiveBlockCacheMaxBlocks)
	opts.LiveBlockCache = liveBlockCache
	storageClosed := false
	shutdownAbandoned := false
	closeStore := func() {
		if storageClosed {
			return
		}
		closeStorage(logger, store)
		storageClosed = true
	}
	defer func() {
		if !shutdownAbandoned {
			closeStore()
		}
	}()
	if err = ensureStoredZeroStateMatchesGlobalConfig(ctx, store, globalConfigZeroState); err != nil {
		logger.Error().
			Err(err).
			Str("configured_zerostate", formatBlockID(globalConfigZeroState)).
			Msg("stored zerostate does not match global config")
		return err
	}

	node, err := p2p.New(opts)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize p2p node")
		return fmt.Errorf("initialize p2p node: %w", err)
	}
	if runtimeMetrics != nil {
		node.SetBroadcastPipelineObserver(runtimeMetrics)
	}

	blockSync := blocksync.New(&baseLogger, node)

	stateLogger := &baseLogger
	stateSource := service.NewP2PStateSource(node, stateLogger)

	stateSync := state.NewSyncer(stateSource, store, state.SyncerOptions{
		SyncBefore: syncBefore,
		SyncUntil:  syncUntil,
	}, stateLogger)

	liveViewOptions := liveview.Options{
		MasterBlockCache: liveview.DefaultMasterBlockCache,
		ShardBlockCache:  liveview.DefaultShardBlockCache,
		LiveBlockCache:   liveBlockCache,
	}
	if runOpts.LiveView != nil {
		liveViewOptions = *runOpts.LiveView
		liveViewOptions.LiveBlockCache = liveBlockCache
	}
	// Publishing happens on the block apply path, so the per-block view prewarm
	// runs on build workers instead of the apply goroutines. Configured here
	// rather than in liteserverOptions because it is a property of the node's
	// apply pipeline, not of the liteserver.
	if liveViewOptions.FragmentBuildWorkers == 0 {
		liveViewOptions.FragmentBuildWorkers = liveview.DefaultFragmentBuildWorkers
	}
	if liveViewOptions.OnFragmentBuildError == nil {
		fragmentLogger := baseLogger.With().Str("component", "liveview").Logger()
		liveViewOptions.OnFragmentBuildError = func(block ton.BlockIDExt, err error) {
			fragmentLogger.Debug().
				Err(err).
				Stringer("block", storage.BlockRef(block)).
				Msg("failed to prewarm live block view")
		}
	}
	liveStore := liveview.New(store, liveViewOptions)
	tvmInstance := tvm.NewTVM()

	externalMessageLogger := baseLogger.With().Str("component", "external_message").Logger()
	externalMessageChecker, err := externalmsg.NewChecker(externalmsg.Options{
		Logger: &externalMessageLogger,
		Store:  liveStore,
		TVM:    tvmInstance,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize external message checker")
		return fmt.Errorf("initialize external message checker: %w", err)
	}
	externalMessages := externalMessageNetwork{
		node:      node,
		checker:   externalMessageChecker,
		blockSync: blockSync,
	}

	serviceLogger := baseLogger.With().Str("component", "service").Logger()
	statusTracker := service.NewStatusTracker(serviceLogger, store, liveBlockCache)
	stateLifecycle := service.NewStateLifecycle(serviceLogger, store, statusTracker, service.StateLifecycleOptions{
		ShutdownContext:                  shutdownCtx,
		StateFilesDir:                    stateFilesDir,
		StateTTL:                         stateTTL,
		StorageDir:                       storageDir,
		DisableStateSerialization:        runOpts.DisableStateSerialization,
		PersistentStateLargeBOCBatchSize: persistentStateLargeBOCBatchSize,
		StateSerializeOnePass:            storageOpts.StateSerializeOnePass,
		NextBlockCheckpointBlocks:        nextCheckpointBlocks,
		CheckpointBytes:                  checkpointBytes,
		SyncBackpressureWindows:          syncBackpressureWindows,
	})
	maintenance := service.NewMaintenanceRunner(serviceLogger, store, statusTracker, service.MaintenanceRunnerOptions{
		ArchiveTTL:                archiveTTL,
		PersistentStateKeepRecent: storageOpts.PersistentStateKeepRecent,
		ShutdownContext:           shutdownCtx,
	})
	readStatus := func(ctx context.Context) service.StatusSnapshot {
		snapshot := statusTracker.Snapshot(ctx, node.StatusSnapshot())
		snapshot.BlockSync = blockSync.StatusSnapshot()
		snapshot.BackgroundTask = maintenance.BackgroundTaskStatus()

		return snapshot
	}
	commandRegistry := &console.Registry{}

	extensionFactory := runOpts.Extension
	extensionLogger := baseLogger.With().Str("source", "extension").Logger()
	var metricsCapability any
	if runtimeMetrics != nil {
		metricsCapability = runtimeMetrics
	}
	extensionNode := hooks.Node{
		Network:         externalMessages,
		PrivateOverlays: node.PrivateOverlays(),
		BlockBroadcasts: node.BlockBroadcasts(),
		Store:           liveStore,
		TVM:             tvmInstance,
		Logger:          extensionLogger,
		Metrics:         metricsCapability,
		Commands:        commandRegistry,
	}
	extension, err := extensionFromFactory(extensionFactory, extensionNode)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize static extension")
		return fmt.Errorf("initialize static extension: %w", err)
	}
	extensionClosed := false
	closeExtension := func(ctx context.Context) error {
		if extensionClosed || extension == nil {
			return nil
		}
		if closeErr := extension.Close(ctx); closeErr != nil {
			return closeErr
		}
		extensionClosed = true

		return nil
	}
	defer func() {
		if shutdownAbandoned {
			return
		}
		if closeErr := closeExtension(shutdownCtx); closeErr != nil {
			shutdownAbandoned = true
			returnErr = errors.Join(
				returnErr,
				ErrShutdownIncomplete,
				fmt.Errorf("stop static extension: %w", closeErr),
			)
			logger.Error().Err(closeErr).Msg("static extension cleanup is incomplete")
		}
	}()
	eventHandlers := eventHandlersFromExtension(extension)

	coordinator := service.NewSyncCoordinator(serviceLogger, service.SyncCoordinatorDependencies{
		Node:               node,
		BlockSync:          blockSync,
		Storage:            store,
		StateSync:          stateSync,
		Status:             statusTracker,
		State:              stateLifecycle,
		Maintenance:        maintenance,
		BroadcastAdmission: broadcastAdmission,
	}, service.SyncCoordinatorOptions{
		CurrentStatePublisher:                   liveStore,
		LiveBlockCache:                          liveBlockCache,
		CurrentStatePublisherUsesLiveBlockCache: true,
		ShutdownContext:                         shutdownCtx,
		ArchiveFromZero:                         archiveFromZero,
		SyncUntil:                               syncUntil,
		StorageDir:                              storageDir,
		SyncObserver:                            syncObserver,
		BlockAppliedProcessor:                   eventHandlers.BlockApplied,
		BlockReceivedObserver:                   eventHandlers.BlockReceived,
		ShardTopBlockDescriptionObserver:        eventHandlers.ShardTopBlockDescription,
	})
	archiveRunner := service.NewArchiveRunner(
		serviceLogger,
		node,
		store,
		stateSync,
		stateLifecycle,
		eventHandlers.BlockReceived,
		maintenance,
		service.ArchiveRunnerOptions{
			CheckpointBlocks: archiveCheckpointBlocks,
			CheckpointPeriod: runOpts.ArchiveCheckpointPeriod,
			PrefetchWindows:  runOpts.ArchivePrefetchWindows,
			SyncBackpressure: syncBackpressureWindows,
			SyncUntil:        syncUntil,
			StorageDir:       storageDir,
		},
	)
	externalAdmission := service.NewExternalMessageAdmission(
		externalMessageLogger,
		externalMessageChecker,
		eventHandlers.ExternalMessage,
	)

	if err = stateLifecycle.BindTransitions(coordinator, coordinator, coordinator, coordinator, maintenance); err != nil {
		return fmt.Errorf("bind state lifecycle transitions: %w", err)
	}
	if err = maintenance.Bind(stateLifecycle, coordinator); err != nil {
		return fmt.Errorf("bind maintenance runner: %w", err)
	}
	if err = archiveRunner.Bind(coordinator, coordinator, coordinator, coordinator); err != nil {
		return fmt.Errorf("bind archive runner transitions: %w", err)
	}
	if err = coordinator.BindArchiveRunner(archiveRunner); err != nil {
		return fmt.Errorf("bind archive runner to sync coordinator: %w", err)
	}
	if err = registerConsoleCommands(commandRegistry, readStatus, stateLifecycle, store.DBStatus); err != nil {
		return fmt.Errorf("register console commands: %w", err)
	}

	liveStore.SetNonfinalCellLoader(stateLifecycle.CellLoader())
	if err = node.BindRuntimeCallbacks(p2p.RuntimeCallbacks{
		CompressedState:          coordinator,
		SyncLag:                  statusTracker,
		SignatureVerifier:        coordinator,
		BroadcastAdmission:       broadcastAdmission,
		ExternalMessageAdmission: externalAdmission,
		BlockReceivedObserver:    coordinator,
	}); err != nil {
		return fmt.Errorf("bind p2p runtime callbacks: %w", err)
	}
	node.SetBlockCacheObserver(liveStore)
	if runtimeMetrics != nil {
		if err = runtimeMetrics.RegisterRuntimeCollectors(metrics.RuntimeReaders{
			ServiceStatusReader: func() service.StatusSnapshot {
				return readStatus(context.Background())
			},
			DBStatusReader:     store.DBStatus,
			LazyCellLoadReader: statusTracker.LazyCellLoadMetrics,
			ArchivePackagesDir: filepath.Join(storageDir, "archive", "packages"),
			StateFilesDir:      stateFilesDir,
		}); err != nil {
			logger.Error().Err(err).Msg("failed to initialize runtime metrics collectors")
			return fmt.Errorf("initialize runtime metrics collectors: %w", err)
		}
		store.SetArtifactMetricsObserver(runtimeMetrics)
	}

	var httpAPIServer *httpapi.Server
	var liteserverServer *liteserver.Server
	var consoleDone <-chan struct{}
	apiServersClosed := false
	closeAPIServers := func() {
		if apiServersClosed {
			return
		}
		apiServersClosed = true

		if httpAPIServer != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if closeErr := httpAPIServer.Close(closeCtx); closeErr != nil {
				logger.Warn().Err(closeErr).Msg("failed to stop http api")
			}
			cancel()
			httpAPIServer.Wait()
		}
		if liteserverServer != nil {
			if closeErr := liteserverServer.Close(); closeErr != nil {
				logger.Warn().Err(closeErr).Msg("failed to stop liteserver")
			}

			closeCtx, cancel := context.WithTimeout(context.Background(), liteserverShutdownTimeout)
			if waitErr := waitForLiteserverShutdown(closeCtx, liteserverServer.Wait); waitErr != nil {
				logger.Warn().
					Err(waitErr).
					Dur("timeout", liteserverShutdownTimeout).
					Msg("timed out waiting for liteserver shutdown")
			}
			cancel()
		}
	}
	defer closeAPIServers()

	if apiOpts := runOpts.HTTPAPI; apiOpts != nil {
		httpAPIServer, err = httpapi.New(httpapi.Options{
			Logger:         &baseLogger,
			Store:          liveStore,
			Network:        externalMessages,
			TVM:            tvmInstance,
			ListenAddr:     apiOpts.ListenAddr,
			RequestTimeout: apiOpts.RequestTimeout,
			ZeroState:      apiOpts.ZeroState,
		})
		if err != nil {
			logger.Error().Err(err).Msg("failed to initialize http api")
			return fmt.Errorf("initialize http api: %w", err)
		}
	}

	if liteOpts := runOpts.Liteserver; liteOpts != nil {
		var queryObserver liteserver.QueryObserver
		if runtimeMetrics != nil {
			queryObserver, err = liteserver.NewQueryObserver(runtimeMetrics)
			if err != nil {
				logger.Error().Err(err).Msg("failed to initialize liteserver metrics")
				return fmt.Errorf("initialize liteserver metrics: %w", err)
			}
		}

		liteserverServer, err = liteserver.New(liteserver.Options{
			Logger:                  &baseLogger,
			Store:                   liveStore,
			MessageSender:           externalMessages,
			TVM:                     tvmInstance,
			CheckExternalMessage:    externalMessageChecker.Check,
			QueryObserver:           queryObserver,
			PrivateKey:              liteOpts.PrivateKey,
			ListenAddr:              liteOpts.ListenAddr,
			NonFinal:                liteOpts.NonFinal,
			AllowDuplicateExternals: liteOpts.AllowDuplicateExternals,
			ZeroState:               liteOpts.ZeroState,
			RequestLimits:           liteOpts.RequestLimits,
			QueryConcurrency:        liteOpts.QueryConcurrency,
		})
		if err != nil {
			logger.Error().Err(err).Msg("failed to initialize liteserver")
			return fmt.Errorf("initialize liteserver: %w", err)
		}
	}

	if err = node.Start(networkCtx); err != nil {
		logger.Error().Err(err).Msg("failed to start p2p node")
		return fmt.Errorf("start p2p node: %w", err)
	}
	runtimeStopped := false
	shutdownRuntime := func() error {
		if runtimeStopped {
			return nil
		}

		cancelRun()
		if consoleDone != nil {
			<-consoleDone
		}
		closeAPIServers()
		coordinator.Wait()
		blockSync.Wait()
		maintenance.Wait()
		stateLifecycle.Wait()
		statusTracker.Wait()
		// Stop ordinary extension hook delivery first. Extensions own dynamic
		// overlays, so retire them while P2P is still alive and their private
		// callbacks and overlay workers can drain deterministically.
		eventHandlers.stop()
		if closeErr := closeExtension(shutdownCtx); closeErr != nil {
			return closeErr
		}
		cancelNetwork()
		node.Wait()
		stop()
		runtimeStopped = true

		return nil
	}
	shutdownUntilComplete := func() error {
		for {
			shutdownErr := shutdownRuntime()
			if shutdownErr == nil {
				return nil
			}
			if err := shutdownCtx.Err(); err != nil {
				return errors.Join(ErrShutdownIncomplete, shutdownErr, err)
			}

			logger.Warn().Err(shutdownErr).Msg("node runtime cleanup failed; retrying")
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-shutdownCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}

				return errors.Join(ErrShutdownIncomplete, shutdownErr, shutdownCtx.Err())
			}
		}
	}
	defer func() {
		if runtimeStopped || shutdownAbandoned {
			return
		}
		if shutdownErr := shutdownUntilComplete(); shutdownErr != nil {
			shutdownAbandoned = true
			returnErr = errors.Join(returnErr, shutdownErr)
			logger.Error().Err(shutdownErr).Msg("node runtime cleanup is incomplete")
		}
	}()

	if extension != nil {
		if err = extension.Start(ctx); err != nil {
			logger.Error().Err(err).Msg("failed to start static extension")
			return fmt.Errorf("start static extension: %w", err)
		}
	}

	statusTracker.Start(ctx)
	if err = stateLifecycle.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start state lifecycle")
		return fmt.Errorf("start state lifecycle: %w", err)
	}
	if err = maintenance.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start maintenance runner")
		return fmt.Errorf("start maintenance runner: %w", err)
	}
	blockSync.Start(ctx)
	if err = coordinator.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start sync coordinator")
		return fmt.Errorf("start sync coordinator: %w", err)
	}

	if liteserverServer != nil {
		if err = liteserverServer.Start(ctx); err != nil {
			logger.Error().Err(err).Msg("failed to start liteserver")
			return fmt.Errorf("start liteserver: %w", err)
		}
	}
	if httpAPIServer != nil {
		if err = httpAPIServer.Start(ctx); err != nil {
			logger.Error().Err(err).Msg("failed to start http api")
			return fmt.Errorf("start http api: %w", err)
		}
	}

	logger.Info().
		Str("listen_addr", fallbackString(opts.ListenAddr, "<client-mode>")).
		Str("adnl_id", node.LocalID().Base64()).
		Bool("dht_server", opts.DHTListenAddr != "").
		Str("dht_listen_addr", fallbackString(opts.DHTListenAddr, "<client-mode>")).
		Int64("external_message_broadcast_bytes_per_second", opts.ExternalBroadcastCapacity.BytesPerSecond).
		Dur("external_message_broadcast_max_delay", opts.ExternalBroadcastCapacity.MaxDelay).
		Bool("metrics", metricsOpts.Enabled).
		Str("metrics_listen_addr", fallbackString(metricsOpts.ListenAddr, "<disabled>")).
		Bool("archive_from_zero", archiveFromZero).
		Dur("sync_before", syncBefore).
		Uint32("sync_until", syncUntil).
		Dur("state_ttl", stateTTL).
		Dur("archive_ttl", archiveTTL).
		Uint32("next_checkpoint_blocks", nextCheckpointBlocks).
		Uint32("archive_checkpoint_blocks", archiveCheckpointBlocks).
		Uint64("checkpoint_bytes", checkpointBytes).
		Uint32("sync_backpressure_windows", syncBackpressureWindows).
		Dur("archive_checkpoint_period", runOpts.ArchiveCheckpointPeriod).
		Int("archive_prefetch_windows", runOpts.ArchivePrefetchWindows).
		Int("large_boc_shard_read_workers", largeBOCShardReadWorkers).
		Int("persistent_state_large_boc_batch_size", persistentStateLargeBOCBatchSize).
		Int("persistent_state_keep_recent", storageOpts.PersistentStateKeepRecent).
		Bool("state_serialize_one_pass", storageOpts.StateSerializeOnePass).
		Bool("disable_state_serialization", runOpts.DisableStateSerialization).
		Msg("service started")

	if runOpts.ConsoleInput != nil && runOpts.ConsoleOutput != nil {
		done := make(chan struct{})
		consoleDone = done
		go func() {
			defer close(done)
			runConsole(ctx, logger, runOpts.ConsoleInput, runOpts.ConsoleOutput, commandRegistry)
		}()
	}

	// A node that stopped on its own because a subsystem died must take the
	// process down with it: it can no longer sync, and leaving it up would keep
	// the liteserver answering from a state that stopped advancing while the
	// supervisor sees a healthy process. The deliberate ton.sync_until stop does
	// not close Failed, so it still parks here until a signal arrives.
	var nodeFailure string
	select {
	case <-ctx.Done():
	case <-node.Failed():
		nodeFailure = node.OfflineReason()
		logger.Error().
			Str("reason", nodeFailure).
			Msg("p2p node stopped unexpectedly, shutting the node down")
		cancelRun()
	}

	logger.Info().Msg("shutting down")
	shutdownErr := shutdownUntilComplete()
	if shutdownErr == nil {
		closeStore()
		logger.Info().Msg("shutdown complete")
	} else {
		shutdownAbandoned = true
		logger.Error().Err(shutdownErr).Msg("forced shutdown left durable cleanup incomplete")
	}
	if nodeFailure != "" {
		return errors.Join(
			fmt.Errorf("p2p node stopped unexpectedly: %s", nodeFailure),
			shutdownErr,
		)
	}
	return shutdownErr
}
