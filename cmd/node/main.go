package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxNodeBOCCells = 4_000_000_000
	topShard        = int64(-1 << 63)
)

var GitCommit = "unknown"

func main() {
	configPath := flag.String("config", nodeconfig.DefaultPath, "path to node config JSON")
	lsPubkeyFlag := flag.Bool("ls-pubkey", false, "print liteserver public key in base64 and exit")
	adnlIDFlag := flag.Bool("adnl-id", false, "print ADNL id derived from adnl.key in base64 and exit")
	versionFlag := flag.Bool("version", false, "print build version and exit")
	skipConfigCheckFlag := flag.Bool("skip-cfg-check", false, "continue startup after creating a missing config file")
	verbosityFlag := flag.String("verbosity", "info", "log verbosity: trace, debug, info, warn, error")
	logTypesFlag := flag.String("log-types", "", "category log verbosity overrides, comma-separated: liteserver=debug,p2p=warn")
	logJSONFlag := flag.Bool("log-json", false, "write logs as JSON instead of pretty console")
	logFileFlag := flag.String("log-file", "", "path to rotating log file, disabled by default")
	logFileMaxSizeFlag := flag.Int("log-file-max-size", defaultLogFileMaxSizeMB, "maximum log file size in megabytes before rotation")
	logFileMaxBackupsFlag := flag.Int("log-file-max-backups", defaultLogFileMaxBackups, "maximum rotated log files to keep, 0 keeps all")
	logFileMaxAgeFlag := flag.Int("log-file-max-age", defaultLogFileMaxAgeDays, "maximum days to keep rotated log files, 0 keeps all")
	logFileCompressFlag := flag.Bool("log-file-compress", false, "compress rotated log files")
	globalConfigURLFlag := flag.String("global-config", "", "download TON global config from URL and replace the configured file before start")
	pprofAddrFlag := flag.String("pprof-addr", "", "listen address for net/http/pprof, disabled by default")
	fromZeroFlag := flag.Bool("from-zero", false, "verify initial key block chain from zerostate instead of global config init_block")
	liteSendMessageTVMTraceFlag := flag.Bool("liteserver-send-message-tvm-trace", false, "dump TVM opcode trace when liteserver sendMessage execution rejects a message")
	liteQueryWorkersFlag := flag.Int("liteserver-query-workers", 0, "liteserver query worker goroutines, 0 uses tonutils default")
	archiveCheckpointPeriodFlag := flag.Duration("archive-checkpoint-period", service2.DefaultArchiveCatchUpCheckpointPeriod, "archive catch-up current-state checkpoint max interval")
	archivePrefetchWindowsFlag := flag.Int("archive-prefetch-windows", service2.DefaultArchiveCatchUpPrefetchWindows, "archive catch-up imported window prefetch depth")
	flag.Parse()

	if *versionFlag {
		if _, err := fmt.Fprintln(os.Stdout, GitCommit); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write version: %v\n", err)
			os.Exit(1)
		}
		return
	}

	level, err := logutil.ParseLevel(*verbosityFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid verbosity %q: %v\n", *verbosityFlag, err)
		os.Exit(1)
	}
	logTypeOverrides, err := logutil.ParseLevelOverrides(*logTypesFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log type overrides %q: %v\n", *logTypesFlag, err)
		os.Exit(1)
	}
	logRotation := logRotationOptions{
		MaxSizeMB:  *logFileMaxSizeFlag,
		MaxBackups: *logFileMaxBackupsFlag,
		MaxAgeDays: *logFileMaxAgeFlag,
		Compress:   *logFileCompressFlag,
	}
	logOutput, logFile, err := newLogOutput(os.Stdout, *logFileFlag, logRotation)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log file config: %v\n", err)
		os.Exit(1)
	}
	if logFile != nil {
		defer func() {
			if err := logFile.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "failed to close log file %q: %v\n", logFile.Filename, err)
			}
		}()
	}
	logs := logutil.NewFactory(logOutput, logutil.Config{
		Level:     level,
		Overrides: logTypeOverrides,
		JSON:      *logJSONFlag,
	})
	logger := logs.Component("main")
	cell.MaxBOCCells = maxNodeBOCCells
	logger.Debug().Int("max_boc_cells", cell.MaxBOCCells).Msg("configured BOC parser limits")
	if *liteQueryWorkersFlag < 0 {
		logger.Error().Int("liteserver_query_workers", *liteQueryWorkersFlag).Msg("liteserver query workers cannot be negative")
		os.Exit(1)
	}
	if *liteQueryWorkersFlag > 0 {
		liteclient.ServerQueryWorkers = *liteQueryWorkersFlag
	}

	selectedConfigPath := resolveConfigPath(*configPath)
	cfg, err := nodeconfig.Load(selectedConfigPath)
	if err != nil {
		if *lsPubkeyFlag || *adnlIDFlag {
			logger.Error().Err(err).Str("config", selectedConfigPath).Msg("failed to load config")
			os.Exit(1)
		}
		configResult, err := nodeconfig.LoadOrCreate(context.Background(), selectedConfigPath, nodeconfig.DetectExternalIP)
		if err != nil {
			logger.Error().Err(err).Str("config", selectedConfigPath).Msg("failed to load config")
			os.Exit(1)
		}
		cfg = configResult.Config
		if configResult.Created {
			if *skipConfigCheckFlag {
				logger.Info().
					Str("config", displayConfigPath(selectedConfigPath)).
					Msg("created default config and continuing startup due --skip-cfg-check")
			} else {
				logger.Info().
					Str("config", displayConfigPath(selectedConfigPath)).
					Msg("created default config; review and approve config.json settings, then start the node again")
				return
			}
		}
	}
	if *lsPubkeyFlag {
		liteSeed := cfg.Lite.Key
		if len(liteSeed) == 0 {
			logger.Error().Str("config", selectedConfigPath).Msg("liteserver key is missing")
			os.Exit(1)
		}
		if len(liteSeed) != ed25519.SeedSize {
			logger.Error().
				Int("key_bytes", len(liteSeed)).
				Int("expected_bytes", ed25519.SeedSize).
				Str("config", selectedConfigPath).
				Msg("invalid liteserver key size")
			os.Exit(1)
		}
		litePriv := ed25519.NewKeyFromSeed(liteSeed)
		if _, err = fmt.Fprintln(os.Stdout, base64.StdEncoding.EncodeToString(litePriv.Public().(ed25519.PublicKey))); err != nil {
			logger.Error().Err(err).Msg("failed to write liteserver public key")
			os.Exit(1)
		}
		return
	}
	if *adnlIDFlag {
		adnlSeed := cfg.ADNL.Key
		if len(adnlSeed) == 0 {
			logger.Error().Str("config", selectedConfigPath).Msg("ADNL key is missing")
			os.Exit(1)
		}
		if len(adnlSeed) != ed25519.SeedSize {
			logger.Error().
				Int("key_bytes", len(adnlSeed)).
				Int("expected_bytes", ed25519.SeedSize).
				Str("config", selectedConfigPath).
				Msg("invalid ADNL key size")
			os.Exit(1)
		}
		adnlPriv := ed25519.NewKeyFromSeed(adnlSeed)
		adnlID, err := tl.Hash(keys.PublicKeyED25519{Key: adnlPriv.Public().(ed25519.PublicKey)})
		if err != nil {
			logger.Error().Err(err).Msg("failed to compute ADNL id")
			os.Exit(1)
		}
		if _, err = fmt.Fprintln(os.Stdout, base64.StdEncoding.EncodeToString(adnlID)); err != nil {
			logger.Error().Err(err).Msg("failed to write ADNL id")
			os.Exit(1)
		}
		return
	}

	ctx, shutdownCtx, stop := signalContexts()
	defer stop()
	startPprof(ctx, logger, strings.TrimSpace(*pprofAddrFlag))

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
		runtimeMetrics = metrics.New(metricsOpts.Namespace)
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
	archiveFromZero := cfg.ArchiveFromZero()
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
	if archiveFromZero && stateTTL != 0 {
		logger.Error().
			Dur("state_ttl", stateTTL).
			Msg("ton.sync_before=-1 requires ton.state_ttl=0")
		os.Exit(1)
	}
	if archiveFromZero && archiveTTL != 0 {
		logger.Error().
			Dur("archive_ttl", archiveTTL).
			Msg("ton.sync_before=-1 requires ton.archive_ttl=0")
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
	syncBackpressureWindows, err := cfg.SyncBackpressureWindows()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load sync backpressure windows option")
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
	decodedCellCacheOpts, err := cfg.DecodedCellCacheOptions()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load storage decoded cell cache option")
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
		Bool("decoded_cell_cache_enabled", decodedCellCacheOpts.Enabled).
		Int("decoded_cell_cache_shards", decodedCellCacheOpts.Shards).
		Int64("decoded_cell_cache_bytes_per_entry", decodedCellCacheOpts.BytesPerEntry).
		Int("decoded_cell_cache_min_entries", decodedCellCacheOpts.MinEntries).
		Int("decoded_cell_cache_max_entries", decodedCellCacheOpts.MaxEntries).
		Int("cell_shard_memtable_size", cellShardMemTableSize).
		Int("cell_memtable_stop_writes_threshold", cellMemTableStopWritesThreshold).
		Int("artifact_file_max_open", artifactFileMaxOpen).
		Msg("opening storage")
	store, err := pebblestore.Open(pebblestore.Options{
		Dir:                             storageDir,
		Logger:                          logs.CategoryPtr("pebblestore"),
		CellCacheSize:                   cellTotalCacheSize,
		DisableDecodedCellCache:         !decodedCellCacheOpts.Enabled,
		DecodedCellCacheShards:          decodedCellCacheOpts.Shards,
		DecodedCellCacheBytesPerEntry:   decodedCellCacheOpts.BytesPerEntry,
		DecodedCellCacheMinEntries:      decodedCellCacheOpts.MinEntries,
		DecodedCellCacheMaxEntries:      decodedCellCacheOpts.MaxEntries,
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
		os.Exit(1)
	}

	node, err := p2p.New(opts)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize p2p node")
		os.Exit(1)
	}
	if runtimeMetrics != nil {
		node.SetBroadcastPipelineObserver(runtimeMetrics)
	}

	blockSync := blocksync.New(logs.CategoryPtr("blocksync"), node)

	stateLogger := logs.CategoryPtr("state")
	stateSource := service2.NewP2PStateSource(node, stateLogger)

	stateSync := state.NewSyncer(stateSource, store, state.SyncerOptions{
		FromZero:   *fromZeroFlag,
		SyncBefore: syncBefore,
	}, stateLogger)

	var liveLiteStore *liteserver.LiveStore
	var currentStatePublisher service2.CurrentStatePublisher
	if liteOpts.Enabled {
		liveLiteStore = liteserver.NewLiveStore(store, liteserver.LiveStoreOptions{
			MasterBlockCache: liteOpts.MasterBlockCache,
			ShardBlockCache:  liteOpts.ShardBlockCache,
			NonFinalEnabled:  liteOpts.NonFinalEnabled,
			LiveBlockCache:   liveBlockCache,
		})
		currentStatePublisher = liveLiteStore
	}

	serviceLogger := logs.Component("service")
	svc := service2.New(serviceLogger, node, blockSync, store, stateSync, service2.Options{
		ArchiveCatchUpCheckpointBlocks:          archiveCheckpointBlocks,
		ArchiveCatchUpCheckpointPeriod:          *archiveCheckpointPeriodFlag,
		ArchiveCatchUpPrefetchWindows:           *archivePrefetchWindowsFlag,
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
		SyncObserver:                            syncObserver,
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
			os.Exit(1)
		}
	}

	var liteSrv *liteserver.Server
	if liteOpts.Enabled {
		liteSrv, err = liteserver.New(liteserver.Options{
			Logger:              logs.CategoryPtr("liteserver"),
			Store:               liveLiteStore,
			MessageSender:       node,
			QueryObserver:       queryObserver,
			PrivateKey:          liteOpts.PrivateKey,
			ListenAddr:          liteOpts.ListenAddr,
			NonFinal:            liteOpts.NonFinalEnabled,
			SendMessageTVMTrace: *liteSendMessageTVMTraceFlag,
			ZeroState:           zeroStateIDFromBlock(globalConfigZeroState),
			RequestLimits:       liteOpts.Limits,
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
			os.Exit(1)
		}
	}

	logger.Info().
		Str("config", selectedConfigPath).
		Str("git_commit", GitCommit).
		Str("log_level", level.String()).
		Str("log_levels", fallbackString(logutil.FormatLevelOverrides(logTypeOverrides), "<none>")).
		Str("log_format", logFormat(*logJSONFlag)).
		Str("log_file", fallbackString(strings.TrimSpace(*logFileFlag), "<disabled>")).
		Int("log_file_max_size_mb", logRotation.MaxSizeMB).
		Int("log_file_max_backups", logRotation.MaxBackups).
		Int("log_file_max_age_days", logRotation.MaxAgeDays).
		Bool("log_file_compress", logRotation.Compress).
		Str("global_config", globalConfigPath).
		Str("listen_addr", fallbackString(opts.ListenAddr, "<client-mode>")).
		Str("adnl_id", node.LocalID().Base64()).
		Bool("dht_server", opts.DHTListenAddr != "").
		Str("dht_listen_addr", fallbackString(opts.DHTListenAddr, "<client-mode>")).
		Bool("liteserver", liteOpts.Enabled).
		Str("liteserver_listen_addr", fallbackString(liteOpts.ListenAddr, "<disabled>")).
		Int("liteserver_query_workers", liteclient.ServerQueryWorkers).
		Bool("liteserver_send_message_tvm_trace", *liteSendMessageTVMTraceFlag).
		Int64("liteserver_send_message_broadcast_bytes_per_second", opts.ExternalBroadcastCapacity.BytesPerSecond).
		Dur("liteserver_send_message_broadcast_max_delay", opts.ExternalBroadcastCapacity.MaxDelay).
		Int("liteserver_capacity_per_ip", liteOpts.Limits.CapacityPerIP).
		Float64("liteserver_cooling_per_sec", liteOpts.Limits.CoolingPerSec).
		Int("liteserver_max_connections_per_ip", liteOpts.Limits.MaxConnectionsPerIP).
		Dur("liteserver_max_keep_alive", liteOpts.Limits.MaxKeepAlive).
		Bool("metrics", metricsOpts.Enabled).
		Str("metrics_listen_addr", fallbackString(metricsOpts.ListenAddr, "<disabled>")).
		Str("pprof_addr", fallbackString(strings.TrimSpace(*pprofAddrFlag), "<disabled>")).
		Bool("from_zero", *fromZeroFlag).
		Bool("archive_from_zero", archiveFromZero).
		Dur("sync_before", syncBefore).
		Dur("state_ttl", stateTTL).
		Dur("archive_ttl", archiveTTL).
		Uint32("next_checkpoint_blocks", nextCheckpointBlocks).
		Uint32("archive_checkpoint_blocks", archiveCheckpointBlocks).
		Uint64("checkpoint_bytes", checkpointBytes).
		Uint32("sync_backpressure_windows", syncBackpressureWindows).
		Dur("archive_checkpoint_period", *archiveCheckpointPeriodFlag).
		Int("archive_prefetch_windows", *archivePrefetchWindowsFlag).
		Bool("disable_state_serialization", cfg.DisableStateSerialization).
		Msg("service started")

	go runConsole(ctx, logger, svc, store.DBStatus)

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
}
