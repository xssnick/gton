package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/internal/metrics"
	"github.com/xssnick/gton/liteserver"
	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxNodeBOCCells = 4_000_000_000
	topShard        = int64(-1 << 63)

	startupZeroStateRetry          = 5 * time.Second
	startupZeroStateAttemptTimeout = 45 * time.Second
)

func main() {
	configPath := flag.String("config", nodeconfig.DefaultPath, "path to node config JSON")
	configPathShort := flag.String("C", "", "path to node config JSON (alias for --config)")
	logLevelFlag := flag.String("log-level", "info", "log level: trace, debug, info, warn, error")
	logLevelsFlag := flag.String("log-levels", "", "category log level overrides, comma-separated: liteserver=debug,p2p=warn")
	logJSONFlag := flag.Bool("log-json", false, "write logs as JSON instead of pretty console")
	globalConfigURLFlag := flag.String("global-config", "", "download TON global config from URL and replace the configured file before start")
	pprofAddrFlag := flag.String("pprof-addr", "", "listen address for net/http/pprof, disabled by default")
	fromZeroFlag := flag.Bool("from-zero", false, "verify initial key block chain from zerostate instead of global config init_block")
	archiveCheckpointPeriodFlag := flag.Duration("archive-checkpoint-period", service2.DefaultArchiveCatchUpCheckpointPeriod, "archive catch-up current-state checkpoint max interval")
	archivePrefetchWindowsFlag := flag.Int("archive-prefetch-windows", service2.DefaultArchiveCatchUpPrefetchWindows, "archive catch-up imported window prefetch depth")
	flag.Parse()

	level, err := logutil.ParseLevel(*logLevelFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevelFlag, err)
		os.Exit(1)
	}
	logLevelOverrides, err := logutil.ParseLevelOverrides(*logLevelsFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level overrides %q: %v\n", *logLevelsFlag, err)
		os.Exit(1)
	}
	logs := logutil.NewFactory(os.Stdout, logutil.Config{
		Level:     level,
		Overrides: logLevelOverrides,
		JSON:      *logJSONFlag,
	})
	logger := logs.Component("main")
	cell.MaxBOCCells = maxNodeBOCCells
	logger.Debug().Int("max_boc_cells", cell.MaxBOCCells).Msg("configured BOC parser limits")

	ctx, shutdownCtx, stop := signalContexts()
	defer stop()
	startPprof(ctx, logger, strings.TrimSpace(*pprofAddrFlag))

	selectedConfigPath := resolveConfigPath(*configPath, *configPathShort)
	configResult, err := nodeconfig.LoadOrCreate(ctx, selectedConfigPath, nodeconfig.DetectExternalIP)
	if err != nil {
		logger.Error().Err(err).Str("config", selectedConfigPath).Msg("failed to load config")
		os.Exit(1)
	}
	cfg := configResult.Config
	if configResult.Created {
		logger.Info().
			Str("config", displayConfigPath(selectedConfigPath)).
			Msg("created default config; review and approve config.json settings, then start the node again")
		return
	}

	globalConfigURL := nodeconfig.DefaultGlobalConfigURL
	replaceGlobalConfig := false
	if strings.TrimSpace(*globalConfigURLFlag) != "" {
		globalConfigURL = strings.TrimSpace(*globalConfigURLFlag)
		replaceGlobalConfig = true
	}
	globalConfigPath := cfg.GlobalConfigPath()
	globalConfigResult, err := nodeconfig.EnsureGlobalConfig(ctx, globalConfigPath, globalConfigURL, replaceGlobalConfig)
	if err != nil {
		logger.Error().
			Err(err).
			Str("path", globalConfigPath).
			Str("url", globalConfigURL).
			Msg("failed to prepare global config")
		os.Exit(1)
	}
	if globalConfigResult.Downloaded {
		logger.Info().
			Str("path", globalConfigPath).
			Str("url", globalConfigURL).
			Bool("replace", replaceGlobalConfig).
			Msg("downloaded global config")
	}
	globalConfigZeroState, err := zeroStateBlockFromGlobalConfig(globalConfigPath)
	if err != nil {
		logger.Error().Err(err).Str("global_config", globalConfigPath).Msg("failed to load global config zerostate")
		os.Exit(1)
	}

	opts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load p2p options")
		os.Exit(1)
	}
	opts.Logger = logs.CategoryPtr("p2p")

	liteOpts, err := liteserverOptionsFromConfig(cfg)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load liteserver options")
		os.Exit(1)
	}
	metricsOpts, err := metricsOptionsFromConfig(cfg)
	if err != nil {
		logger.Error().Err(err).Msg("failed to load metrics options")
		os.Exit(1)
	}
	var runtimeMetrics *metrics.Metrics
	var syncObserver service2.SyncObserver
	var queryObserver liteserver.QueryObserver
	if metricsOpts.Enabled {
		runtimeMetrics = metrics.New()
		if err = startMetricsServer(ctx, logger, metricsOpts.ListenAddr, runtimeMetrics.Handler()); err != nil {
			logger.Error().Err(err).Str("metrics_addr", metricsOpts.ListenAddr).Msg("failed to start metrics server")
			os.Exit(1)
		}
		syncObserver = runtimeMetrics
		queryObserver = runtimeMetrics
	}
	syncBefore, err := cfg.SyncBefore()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load sync options")
		os.Exit(1)
	}
	stateTTL, err := cfg.StateTTL()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load state ttl option")
		os.Exit(1)
	}
	archiveTTL, err := cfg.ArchiveTTL()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load archive ttl option")
		os.Exit(1)
	}
	nextCheckpointBlocks, err := cfg.NextCheckpointBlocks()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load next checkpoint blocks option")
		os.Exit(1)
	}
	archiveCheckpointBlocks, err := cfg.ArchiveCheckpointBlocks()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load archive checkpoint blocks option")
		os.Exit(1)
	}
	checkpointBytes, err := cfg.CheckpointBytes()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load checkpoint bytes option")
		os.Exit(1)
	}

	storageDir := cfg.StorageDir()
	if storageDir == "" {
		logger.Error().Msg("storage.dir is required")
		os.Exit(1)
	}
	cellTotalCacheSize, err := cfg.CellTotalCacheSize()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage cell cache option")
		os.Exit(1)
	}
	cellShardMemTableSize, err := cfg.CellShardMemTableSize()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage cell memtable option")
		os.Exit(1)
	}
	cellMemTableStopWritesThreshold, err := cfg.CellMemTableStopWritesThreshold()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage cell memtable stop writes threshold option")
		os.Exit(1)
	}
	artifactFileMaxOpen, err := cfg.ArtifactFileMaxOpen()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage artifact file max open option")
		os.Exit(1)
	}
	storageOpenStarted := time.Now()
	logger.Info().
		Str("storage", "pebble").
		Str("dir", storageDir).
		Int64("cell_total_cache_size", cellTotalCacheSize).
		Int("cell_shard_memtable_size", cellShardMemTableSize).
		Int("cell_memtable_stop_writes_threshold", cellMemTableStopWritesThreshold).
		Int("artifact_file_max_open", artifactFileMaxOpen).
		Msg("opening storage")
	store, err := pebblestore.Open(pebblestore.Options{
		Dir:                             storageDir,
		Logger:                          logs.CategoryPtr("pebblestore"),
		CellCacheSize:                   cellTotalCacheSize,
		CellShardMemTableSize:           cellShardMemTableSize,
		CellMemTableStopWritesThreshold: cellMemTableStopWritesThreshold,
		ArtifactFileMaxOpen:             artifactFileMaxOpen,
	})
	if err != nil {
		logger.Error().Err(err).Str("dir", storageDir).Msg("failed to open pebble storage")
		os.Exit(1)
	}
	stateFilesDir := store.StateFilesDir()
	opts.Storage = store
	opts.PeerServingStorage = store
	if runtimeMetrics != nil {
		runtimeMetrics.SetDBStatusReader(store.DBStatus)
	}
	logger.Info().
		Str("storage", "pebble").
		Str("dir", storageDir).
		Dur("elapsed", time.Since(storageOpenStarted)).
		Msg("configured storage")
	opts.StateFilesDir = stateFilesDir
	defer func() {
		if opts.Storage != nil {
			closeStorage(logger, opts.Storage)
		}
	}()
	if err = ensureStoredZeroStateMatchesGlobalConfig(ctx, store, globalConfigZeroState); err != nil {
		logger.Error().
			Err(err).
			Str("global_config", globalConfigPath).
			Str("configured_zerostate", formatBlockID(globalConfigZeroState)).
			Msg("stored zerostate does not match global config")
		os.Exit(1)
	}

	var svc *service2.Service
	opts.CompressedState = p2p.CompressedBlockStateProviderFunc(func(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
		if svc == nil {
			return nil, storage.ErrNotFound
		}
		return svc.StateRootForCompressedBlock(ctx, block)
	})

	node, err := p2p.New(opts)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize p2p node")
		os.Exit(1)
	}

	blockSync := blocksync.New(logs.CategoryPtr("blocksync"), node, blocksync.NewNodeFetcher(node))

	stateLogger := logs.CategoryPtr("state")
	stateSource := service2.NewP2PStateSource(node, stateLogger)

	stateSync := state.NewSyncer(stateSource, opts.Storage, state.SyncerOptions{
		FromZero:   *fromZeroFlag,
		SyncBefore: syncBefore,
	}, stateLogger)

	var liveLiteStore *liteserver.LiveStore
	var currentStatePublisher service2.CurrentStatePublisher
	if liteOpts.Enabled {
		liveLiteStore = liteserver.NewLiveStore(opts.Storage, liteserver.LiveStoreOptions{
			MasterBlockCache: liteOpts.MasterBlockCache,
			ShardBlockCache:  liteOpts.ShardBlockCache,
		})
		currentStatePublisher = liveLiteStore
	}

	serviceLogger := logs.Component("service")
	svc = service2.New(serviceLogger, node, blockSync, opts.Storage, stateSync, service2.Options{
		ArchiveCatchUpCheckpointBlocks: archiveCheckpointBlocks,
		ArchiveCatchUpCheckpointPeriod: *archiveCheckpointPeriodFlag,
		ArchiveCatchUpPrefetchWindows:  *archivePrefetchWindowsFlag,
		NextBlockCheckpointBlocks:      nextCheckpointBlocks,
		CheckpointBytes:                checkpointBytes,
		CurrentStatePublisher:          currentStatePublisher,
		ShutdownContext:                shutdownCtx,
		StateFilesDir:                  stateFilesDir,
		StateTTL:                       stateTTL,
		ArchiveTTL:                     archiveTTL,
		StorageDir:                     storageDir,
		DisableStateSerialization:      cfg.DisableStateSerialization,
		SyncObserver:                   syncObserver,
	})
	if runtimeMetrics != nil {
		runtimeMetrics.SetServiceStatusReader(svc.StatusSnapshot)
	}

	var liteSrv *liteserver.Server
	if liteOpts.Enabled {
		liteSrv, err = liteserver.New(liteserver.Options{
			Logger:        logs.CategoryPtr("liteserver"),
			Store:         liveLiteStore,
			MessageSender: node,
			QueryObserver: queryObserver,
			PrivateKey:    liteOpts.PrivateKey,
			ListenAddr:    liteOpts.ListenAddr,
			ZeroState:     zeroStateIDFromBlock(globalConfigZeroState),
		})
		if err != nil {
			logger.Error().Err(err).Msg("failed to initialize liteserver")
			os.Exit(1)
		}
	}

	if err = node.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start p2p node")
		os.Exit(1)
	}
	initBlock, err := node.InitBlock()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load configured init block")
		stop()
		node.Wait()
		os.Exit(1)
	}
	if startupZeroStateRequired(*fromZeroFlag, initBlock) {
		if err = ensureZeroStateBeforeInitialSync(ctx, logger, node); err != nil {
			if errors.Is(err, context.Canceled) {
				node.Wait()
				return
			}
			logger.Error().Err(err).Msg("failed to prepare zero state before initial sync")
			stop()
			node.Wait()
			os.Exit(1)
		}
	}

	var blockSyncWG sync.WaitGroup
	blockSyncWG.Add(1)
	go func() {
		defer blockSyncWG.Done()
		blockSync.Run(ctx)
	}()

	if err = svc.Start(ctx); err != nil {
		logger.Error().Err(err).Msg("failed to start service")
		stop()
		blockSyncWG.Wait()
		node.Wait()
		os.Exit(1)
	}

	if liteSrv != nil {
		if err = liteSrv.Start(ctx); err != nil {
			logger.Error().Err(err).Str("listen_addr", liteOpts.ListenAddr).Msg("failed to start liteserver")
			stop()
			svc.Wait()
			blockSyncWG.Wait()
			node.Wait()
			os.Exit(1)
		}
	}

	logger.Info().
		Str("config", selectedConfigPath).
		Str("log_level", level.String()).
		Str("log_levels", fallbackString(logutil.FormatLevelOverrides(logLevelOverrides), "<none>")).
		Str("log_format", logFormat(*logJSONFlag)).
		Str("global_config", globalConfigPath).
		Str("listen_addr", fallbackString(opts.ListenAddr, "<client-mode>")).
		Bool("dht_server", opts.DHTListenAddr != "").
		Str("dht_listen_addr", fallbackString(opts.DHTListenAddr, "<client-mode>")).
		Bool("liteserver", liteOpts.Enabled).
		Str("liteserver_listen_addr", fallbackString(liteOpts.ListenAddr, "<disabled>")).
		Bool("metrics", metricsOpts.Enabled).
		Str("metrics_listen_addr", fallbackString(metricsOpts.ListenAddr, "<disabled>")).
		Str("pprof_addr", fallbackString(strings.TrimSpace(*pprofAddrFlag), "<disabled>")).
		Bool("from_zero", *fromZeroFlag).
		Dur("sync_before", syncBefore).
		Dur("state_ttl", stateTTL).
		Dur("archive_ttl", archiveTTL).
		Uint32("next_checkpoint_blocks", nextCheckpointBlocks).
		Uint32("archive_checkpoint_blocks", archiveCheckpointBlocks).
		Uint64("checkpoint_bytes", checkpointBytes).
		Dur("archive_checkpoint_period", *archiveCheckpointPeriodFlag).
		Int("archive_prefetch_windows", *archivePrefetchWindowsFlag).
		Bool("disable_state_serialization", cfg.DisableStateSerialization).
		Msg("service started")

	go runConsole(ctx, logger, svc, store)

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
	closeStorage(logger, opts.Storage)
	opts.Storage = nil
	logger.Info().Msg("shutdown complete")
}
