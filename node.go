package gton

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/xssnick/gton/api/liteserver"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/internal/metrics"
	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxNodeBOCCells = 4_000_000_000
	topShard        = int64(-1 << 63)
)

// ExtensionFactory initializes a statically linked hook extension.
type ExtensionFactory func(hooks.Node) (hooks.Extension, error)

// NodeOptions configures RunNode.
type NodeOptions struct {
	Config       nodeconfig.Config
	GlobalConfig *liteclient.GlobalConfig
	Logger       zerolog.Logger
	Extension    ExtensionFactory

	ArchiveCheckpointPeriod time.Duration
	ArchivePrefetchWindows  int

	ConsoleInput  io.Reader
	ConsoleOutput io.Writer
}

func DefaultNodeOptions() NodeOptions {
	return NodeOptions{
		Logger:                  zerolog.Nop(),
		ArchiveCheckpointPeriod: service2.DefaultArchiveCatchUpCheckpointPeriod,
		ArchivePrefetchWindows:  service2.DefaultArchiveCatchUpPrefetchWindows,
	}
}

func applyNodeOptionDefaults(opts NodeOptions) NodeOptions {
	if opts.ArchiveCheckpointPeriod == 0 {
		opts.ArchiveCheckpointPeriod = service2.DefaultArchiveCatchUpCheckpointPeriod
	}
	if opts.ArchivePrefetchWindows == 0 {
		opts.ArchivePrefetchWindows = service2.DefaultArchiveCatchUpPrefetchWindows
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

	cfg := runOpts.Config
	globalConfig := runOpts.GlobalConfig
	if globalConfig == nil {
		return fmt.Errorf("global config is required")
	}

	ctx, shutdownCtx, stop := signalContexts(parentCtx)
	defer stop()

	globalConfigPath := cfg.GlobalConfigPath()
	globalConfigZeroState, err := zeroStateBlockFromGlobalConfig(globalConfig)
	if err != nil {
		logger.Error().Err(err).Str("global_config", globalConfigPath).Msg("failed to load global config zerostate")
		return fmt.Errorf("load global config zerostate %s: %w", globalConfigPath, err)
	}

	opts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load p2p options")
		return fmt.Errorf("load p2p options: %w", err)
	}
	opts.GlobalConfig = globalConfig
	opts.Logger = &baseLogger

	liteOpts, err := liteserverOptionsFromConfig(cfg)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load liteserver options")
		return fmt.Errorf("load liteserver options: %w", err)
	}
	metricsOpts, err := metricsOptionsFromConfig(cfg)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load metrics options")
		return fmt.Errorf("load metrics options: %w", err)
	}
	var runtimeMetrics *metrics.Metrics
	var syncObserver service2.SyncObserver
	var queryObserver liteserver.QueryObserver
	if metricsOpts.Enabled {
		runtimeMetrics = metrics.New(metricsOpts.Namespace)
		if err = startMetricsServer(ctx, logger, metricsOpts.ListenAddr, runtimeMetrics.Handler()); err != nil {
			logger.Error().Err(err).Str("metrics_addr", metricsOpts.ListenAddr).Msg("failed to start metrics server")
			return fmt.Errorf("start metrics server %s: %w", metricsOpts.ListenAddr, err)
		}
		syncObserver = runtimeMetrics
		queryObserver = runtimeMetrics
	}
	syncBefore, err := cfg.SyncBefore()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load sync options")
		return fmt.Errorf("load sync options: %w", err)
	}
	archiveFromZero := cfg.ArchiveFromZero()
	stateTTL, err := cfg.StateTTL()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load state ttl option")
		return fmt.Errorf("load state ttl option: %w", err)
	}
	archiveTTL, err := cfg.ArchiveTTL()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load archive ttl option")
		return fmt.Errorf("load archive ttl option: %w", err)
	}
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
	nextCheckpointBlocks, err := cfg.NextCheckpointBlocks()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load next checkpoint blocks option")
		return fmt.Errorf("load next checkpoint blocks option: %w", err)
	}
	archiveCheckpointBlocks, err := cfg.ArchiveCheckpointBlocks()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load archive checkpoint blocks option")
		return fmt.Errorf("load archive checkpoint blocks option: %w", err)
	}
	checkpointBytes, err := cfg.CheckpointBytes()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load checkpoint bytes option")
		return fmt.Errorf("load checkpoint bytes option: %w", err)
	}
	syncBackpressureWindows, err := cfg.SyncBackpressureWindows()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load sync backpressure windows option")
		return fmt.Errorf("load sync backpressure windows option: %w", err)
	}

	storageDir := cfg.StorageDir()
	if storageDir == "" {
		logger.Error().Msg("storage.dir is required")
		return fmt.Errorf("storage.dir is required")
	}
	cellTotalCacheSize, err := cfg.CellTotalCacheSize()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage cell cache option")
		return fmt.Errorf("load storage cell cache option: %w", err)
	}
	decodedCellCacheOpts, err := cfg.DecodedCellCacheOptions()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage decoded cell cache option")
		return fmt.Errorf("load storage decoded cell cache option: %w", err)
	}
	cellShardMemTableSize, err := cfg.CellShardMemTableSize()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage cell memtable option")
		return fmt.Errorf("load storage cell memtable option: %w", err)
	}
	cellMemTableStopWritesThreshold, err := cfg.CellMemTableStopWritesThreshold()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage cell memtable stop writes threshold option")
		return fmt.Errorf("load storage cell memtable stop writes threshold option: %w", err)
	}
	largeBOCShardReadWorkers, err := cfg.LargeBOCShardReadWorkers()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage large boc shard read workers option")
		return fmt.Errorf("load storage large boc shard read workers option: %w", err)
	}
	persistentStateLargeBOCBatchSize, err := cfg.PersistentStateLargeBOCBatchSize()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage persistent state large boc batch size option")
		return fmt.Errorf("load storage persistent state large boc batch size option: %w", err)
	}
	artifactFileMaxOpen, err := cfg.ArtifactFileMaxOpen()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage artifact file max open option")
		return fmt.Errorf("load storage artifact file max open option: %w", err)
	}
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
			Str("global_config", globalConfigPath).
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
	stateSource := service2.NewP2PStateSource(node, stateLogger)

	stateSync := state.NewSyncer(stateSource, store, state.SyncerOptions{
		SyncBefore: syncBefore,
	}, stateLogger)

	liveLiteStore := liteserver.NewLiveStore(store, liteserver.LiveStoreOptions{
		MasterBlockCache: liteOpts.MasterBlockCache,
		ShardBlockCache:  liteOpts.ShardBlockCache,
		NonFinalEnabled:  liteOpts.NonFinalEnabled,
		LiveBlockCache:   liveBlockCache,
	})
	extensionLogger := baseLogger.With().Str("source", "extension").Logger()
	extensionNode := hooks.Node{
		Network: extensionNetwork{node: node},
		Store:   liveLiteStore,
		Logger:  extensionLogger,
	}
	extension, err := extensionFromFactory(runOpts.Extension, extensionNode)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize static extension")
		return fmt.Errorf("initialize static extension: %w", err)
	}

	currentStatePublisher := service2.CurrentStatePublisher(liveLiteStore)
	externalMessageLogger := componentLogger(baseLogger, "external_message")
	externalMessageChecker, err := liteserver.NewExternalMessageChecker(liteserver.ExternalMessageCheckOptions{
		Logger: &externalMessageLogger,
		Store:  liveLiteStore,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize external message checker")
		return fmt.Errorf("initialize external message checker: %w", err)
	}

	serviceLogger := componentLogger(baseLogger, "service")
	svc := service2.New(serviceLogger, node, blockSync, store, stateSync, service2.Options{
		ArchiveCatchUpCheckpointBlocks:          archiveCheckpointBlocks,
		ArchiveCatchUpCheckpointPeriod:          runOpts.ArchiveCheckpointPeriod,
		ArchiveCatchUpPrefetchWindows:           runOpts.ArchivePrefetchWindows,
		NextBlockCheckpointBlocks:               nextCheckpointBlocks,
		CheckpointBytes:                         checkpointBytes,
		SyncBackpressureWindows:                 syncBackpressureWindows,
		CurrentStatePublisher:                   currentStatePublisher,
		LiveBlockCache:                          liveBlockCache,
		CurrentStatePublisherUsesLiveBlockCache: liveLiteStore != nil,
		ShutdownContext:                         shutdownCtx,
		StateFilesDir:                           stateFilesDir,
		StateTTL:                                stateTTL,
		ArchiveTTL:                              archiveTTL,
		ArchiveFromZero:                         archiveFromZero,
		StorageDir:                              storageDir,
		DisableStateSerialization:               cfg.DisableStateSerialization,
		PersistentStateLargeBOCBatchSize:        persistentStateLargeBOCBatchSize,
		StateSerializeOnePass:                   cfg.Storage.StateSerializeOnePass,
		SyncObserver:                            syncObserver,
		Extension:                               extension,
		ExternalMessageChecker:                  externalMessageChecker,
	})
	node.SetRuntimeCallbacks(svc)
	if currentStatePublisher != nil {
		currentStatePublisher.SetNonfinalCellLoader(svc.NonfinalCellLoader())
		node.SetBlockCacheObserver(currentStatePublisher)
	}
	if runtimeMetrics != nil {
		if err = runtimeMetrics.RegisterRuntimeCollectors(metrics.RuntimeReaders{
			ServiceStatusReader: svc.StatusSnapshot,
			DBStatusReader:      store.DBStatus,
			ArchivePackagesDir:  filepath.Join(storageDir, "archive", "packages"),
			StateFilesDir:       stateFilesDir,
		}); err != nil {
			logger.Error().Err(err).Msg("failed to initialize runtime metrics collectors")
			return fmt.Errorf("initialize runtime metrics collectors: %w", err)
		}
	}

	var liteSrv *liteserver.Server
	if liteOpts.Enabled {
		liteSrv, err = liteserver.New(liteserver.Options{
			Logger:        &baseLogger,
			Store:         liveLiteStore,
			MessageSender: node,
			QueryObserver: queryObserver,
			PrivateKey:    liteOpts.PrivateKey,
			ListenAddr:    liteOpts.ListenAddr,
			NonFinal:      liteOpts.NonFinalEnabled,
			ZeroState:     zeroStateIDFromBlock(globalConfigZeroState),
			RequestLimits: liteOpts.Limits,
		})
		if err != nil {
			logger.Error().Err(err).Msg("failed to initialize liteserver")
			return fmt.Errorf("initialize liteserver: %w", err)
		}
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

	if liteSrv != nil {
		if err = liteSrv.Start(ctx); err != nil {
			logger.Error().Err(err).Str("listen_addr", liteOpts.ListenAddr).Msg("failed to start liteserver")
			stop()
			svc.Wait()
			blockSyncWG.Wait()
			node.Wait()
			return fmt.Errorf("start liteserver %s: %w", liteOpts.ListenAddr, err)
		}
	}

	logger.Info().
		Str("global_config", globalConfigPath).
		Str("listen_addr", fallbackString(opts.ListenAddr, "<client-mode>")).
		Str("adnl_id", node.LocalID().Base64()).
		Bool("dht_server", opts.DHTListenAddr != "").
		Str("dht_listen_addr", fallbackString(opts.DHTListenAddr, "<client-mode>")).
		Bool("liteserver", liteOpts.Enabled).
		Str("liteserver_listen_addr", fallbackString(liteOpts.ListenAddr, "<disabled>")).
		Int("liteserver_query_workers", liteclient.ServerQueryWorkers).
		Int64("liteserver_send_message_broadcast_bytes_per_second", opts.ExternalBroadcastCapacity.BytesPerSecond).
		Dur("liteserver_send_message_broadcast_max_delay", opts.ExternalBroadcastCapacity.MaxDelay).
		Int("liteserver_capacity_per_ip", liteOpts.Limits.CapacityPerIP).
		Float64("liteserver_cooling_per_sec", liteOpts.Limits.CoolingPerSec).
		Int("liteserver_max_connections_per_ip", liteOpts.Limits.MaxConnectionsPerIP).
		Dur("liteserver_max_keep_alive", liteOpts.Limits.MaxKeepAlive).
		Bool("metrics", metricsOpts.Enabled).
		Str("metrics_listen_addr", fallbackString(metricsOpts.ListenAddr, "<disabled>")).
		Bool("archive_from_zero", archiveFromZero).
		Dur("sync_before", syncBefore).
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
		Bool("state_serialize_one_pass", cfg.Storage.StateSerializeOnePass).
		Bool("disable_state_serialization", cfg.DisableStateSerialization).
		Msg("service started")

	if runOpts.ConsoleInput != nil && runOpts.ConsoleOutput != nil {
		go runConsole(ctx, logger, runOpts.ConsoleInput, runOpts.ConsoleOutput, svc, store.DBStatus)
	}

	<-ctx.Done()
	logger.Info().Msg("shutting down")
	if liteSrv != nil {
		if err := liteSrv.Close(); err != nil {
			logger.Warn().Err(err).Msg("failed to stop liteserver")
		}
	}
	svc.Wait()
	blockSyncWG.Wait()
	node.Wait()
	if liteSrv != nil {
		liteSrv.Wait()
	}
	closeStore()
	logger.Info().Msg("shutdown complete")
	return nil
}
