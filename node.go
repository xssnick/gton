package gton

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

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
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxNodeBOCCells = 4_000_000_000
	topShard        = int64(-1 << 63)
)

type MetricsOptions struct {
	Enabled    bool
	ListenAddr string
	Namespace  string
}

type StorageOptions struct {
	Dir                              string
	CellTotalCacheSize               int64
	DecodedCellCache                 DecodedCellCacheOptions
	CellShardMemTableSize            int
	CellMemTableStopWritesThreshold  int
	LargeBOCShardReadWorkers         int
	PersistentStateLargeBOCBatchSize int
	StateSerializeOnePass            bool
	ArtifactFileMaxOpen              int
}

type DecodedCellCacheOptions struct {
	Enabled       bool
	Shards        int
	BytesPerEntry int64
	MinEntries    int
	MaxEntries    int
}

// NodeOptions configures RunNode.
type NodeOptions struct {
	GlobalConfig *liteclient.GlobalConfig
	Logger       zerolog.Logger
	P2P          p2p.Options
	Metrics      MetricsOptions
	Storage      StorageOptions
	Extension    hooks.ExtensionFactory

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
	if opts.ArchiveCheckpointPeriod == 0 {
		opts.ArchiveCheckpointPeriod = service.DefaultArchiveCatchUpCheckpointPeriod
	}
	if opts.ArchivePrefetchWindows == 0 {
		opts.ArchivePrefetchWindows = service.DefaultArchiveCatchUpPrefetchWindows
	}
	return opts
}

func componentLogger(base zerolog.Logger, component string) zerolog.Logger {
	if component == "" {
		return base
	}
	return base.With().Str("component", component).Logger()
}

// RunNode runs the gton node from already resolved startup options.
func RunNode(parentCtx context.Context, runOpts NodeOptions) error {
	runOpts = applyNodeOptionDefaults(runOpts)

	baseLogger := runOpts.Logger
	logger := componentLogger(baseLogger, "main")
	cell.MaxBOCCells = maxNodeBOCCells
	logger.Debug().Int("max_boc_cells", cell.MaxBOCCells).Msg("configured BOC parser limits")

	globalConfig := runOpts.GlobalConfig
	if globalConfig == nil {
		return fmt.Errorf("global config is required")
	}

	ctx, shutdownCtx, stop := signalContexts(parentCtx)
	defer stop()

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
		Int64("decoded_cell_cache_bytes_per_entry", decodedCellCacheOpts.BytesPerEntry).
		Int("decoded_cell_cache_min_entries", decodedCellCacheOpts.MinEntries).
		Int("decoded_cell_cache_max_entries", decodedCellCacheOpts.MaxEntries).
		Int("cell_shard_memtable_size", cellShardMemTableSize).
		Int("cell_memtable_stop_writes_threshold", cellMemTableStopWritesThreshold).
		Int("large_boc_shard_read_workers", largeBOCShardReadWorkers).
		Int("persistent_state_large_boc_batch_size", persistentStateLargeBOCBatchSize).
		Int("artifact_file_max_open", artifactFileMaxOpen).
		Msg("opening storage")
	store, err := pebblestore.Open(pebblestore.Options{
		Dir:                             storageDir,
		Logger:                          &baseLogger,
		CellCacheSize:                   cellTotalCacheSize,
		DisableDecodedCellCache:         !decodedCellCacheOpts.Enabled,
		DecodedCellCacheShards:          decodedCellCacheOpts.Shards,
		DecodedCellCacheBytesPerEntry:   decodedCellCacheOpts.BytesPerEntry,
		DecodedCellCacheMinEntries:      decodedCellCacheOpts.MinEntries,
		DecodedCellCacheMaxEntries:      decodedCellCacheOpts.MaxEntries,
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
	opts.Storage = store
	logger.Info().
		Str("storage", "pebble").
		Str("dir", storageDir).
		Dur("elapsed", time.Since(storageOpenStarted)).
		Msg("configured storage")
	opts.StateFilesDir = stateFilesDir
	liveBlockCache := storage.NewLiveBlockCache(storage.DefaultLiveBlockCacheMaxBlocks)
	opts.LiveBlockCache = liveBlockCache
	storageClosed := false
	closeStore := func() {
		if storageClosed {
			return
		}
		closeStorage(logger, store)
		storageClosed = true
	}
	defer func() {
		closeStore()
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
	liveStore := liveview.New(store, liveViewOptions)
	tvmInstance := tvm.NewTVM()

	currentStatePublisher := service.CurrentStatePublisher(liveStore)
	externalMessageLogger := componentLogger(baseLogger, "external_message")
	externalMessageChecker, err := externalmsg.NewChecker(externalmsg.Options{
		Logger: &externalMessageLogger,
		Store:  liveStore,
		TVM:    tvmInstance,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize external message checker")
		return fmt.Errorf("initialize external message checker: %w", err)
	}

	extensionFactory := runOpts.Extension
	extensionLogger := baseLogger.With().Str("source", "extension").Logger()
	var metricsCapability any
	if runtimeMetrics != nil {
		metricsCapability = runtimeMetrics
	}
	extensionNode := hooks.Node{
		Network: extensionNetwork{node: node, checker: externalMessageChecker},
		Store:   liveStore,
		TVM:     tvmInstance,
		Logger:  extensionLogger,
		Metrics: metricsCapability,
	}
	extension, err := extensionFromFactory(extensionFactory, extensionNode)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize static extension")
		return fmt.Errorf("initialize static extension: %w", err)
	}

	serviceLogger := componentLogger(baseLogger, "service")
	svc := service.New(serviceLogger, node, blockSync, store, stateSync, service.Options{
		ArchiveCatchUpCheckpointBlocks:          archiveCheckpointBlocks,
		ArchiveCatchUpCheckpointPeriod:          runOpts.ArchiveCheckpointPeriod,
		ArchiveCatchUpPrefetchWindows:           runOpts.ArchivePrefetchWindows,
		NextBlockCheckpointBlocks:               nextCheckpointBlocks,
		CheckpointBytes:                         checkpointBytes,
		SyncBackpressureWindows:                 syncBackpressureWindows,
		CurrentStatePublisher:                   currentStatePublisher,
		LiveBlockCache:                          liveBlockCache,
		CurrentStatePublisherUsesLiveBlockCache: liveStore != nil,
		ShutdownContext:                         shutdownCtx,
		StateFilesDir:                           stateFilesDir,
		StateTTL:                                stateTTL,
		ArchiveTTL:                              archiveTTL,
		ArchiveFromZero:                         archiveFromZero,
		SyncUntil:                               syncUntil,
		StorageDir:                              storageDir,
		DisableStateSerialization:               runOpts.DisableStateSerialization,
		PersistentStateLargeBOCBatchSize:        persistentStateLargeBOCBatchSize,
		StateSerializeOnePass:                   storageOpts.StateSerializeOnePass,
		SyncObserver:                            syncObserver,
		Extension:                               extension,
		ExternalMessageChecker:                  externalMessageChecker,
	})
	node.SetRuntimeCallbacks(svc)
	if currentStatePublisher != nil {
		node.SetBlockCacheObserver(currentStatePublisher)
	}
	if runtimeMetrics != nil {
		if err = runtimeMetrics.RegisterRuntimeCollectors(metrics.RuntimeReaders{
			ServiceStatusReader: svc.StatusSnapshot,
			DBStatusReader:      store.DBStatus,
			LazyCellLoadReader:  svc.LazyCellLoadMetrics,
			ArchivePackagesDir:  filepath.Join(storageDir, "archive", "packages"),
			StateFilesDir:       stateFilesDir,
		}); err != nil {
			logger.Error().Err(err).Msg("failed to initialize runtime metrics collectors")
			return fmt.Errorf("initialize runtime metrics collectors: %w", err)
		}
		store.SetArtifactMetricsObserver(runtimeMetrics)
	}

	if err = node.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start p2p node")
		return fmt.Errorf("start p2p node: %w", err)
	}

	var blockSyncWG sync.WaitGroup
	blockSyncWG.Add(1)
	go func() {
		defer blockSyncWG.Done()
		blockSync.Run(ctx)
	}()

	svc.Start(ctx)

	if extension != nil {
		if err = extension.Start(ctx); err != nil {
			logger.Error().Err(err).Msg("failed to start static extension")
			stop()
			svc.Wait()
			blockSyncWG.Wait()
			node.Wait()
			return fmt.Errorf("start static extension: %w", err)
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
		Bool("state_serialize_one_pass", storageOpts.StateSerializeOnePass).
		Bool("disable_state_serialization", runOpts.DisableStateSerialization).
		Msg("service started")

	if runOpts.ConsoleInput != nil && runOpts.ConsoleOutput != nil {
		go runConsole(ctx, logger, runOpts.ConsoleInput, runOpts.ConsoleOutput, svc, store.DBStatus)
	}

	<-ctx.Done()
	logger.Info().Msg("shutting down")
	if extension != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := extension.Close(closeCtx); err != nil {
			logger.Warn().Err(err).Msg("failed to stop static extension")
		}
		cancel()
	}
	svc.Wait()
	blockSyncWG.Wait()
	node.Wait()
	closeStore()
	logger.Info().Msg("shutdown complete")
	return nil
}
