package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
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
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/liteclient"
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

	opts, err := cfg.P2POptions()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load p2p options")
		os.Exit(1)
	}
	opts.Logger = logs.CategoryPtr("p2p")

	liteOpts, err := cfg.LiteserverOptions()
	if err != nil {
		logger.Error().Err(err).Msg("failed to load liteserver options")
		os.Exit(1)
	}
	metricsOpts, err := cfg.MetricsOptions()
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

	node, err := p2p.New(opts)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize p2p node")
		os.Exit(1)
	}

	blockSync := blocksync.New(logs.CategoryPtr("blocksync"), node, blocksync.NewNodeFetcher(node))

	stateLogger := logs.CategoryPtr("state")
	stateSource := state.NewP2PSource(node, stateLogger)

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
	svc := service2.New(serviceLogger, node, blockSync, opts.Storage, stateSync, service2.Options{
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
	node.SetCompressedBlockStateProvider(svc)

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

func signalContexts() (context.Context, context.Context, func()) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			signal.Stop(signals)
			cancelRun()
			cancelShutdown()
		})
	}

	go func() {
		select {
		case <-signals:
			cancelRun()
		case <-runCtx.Done():
			return
		}

		select {
		case <-signals:
			cancelShutdown()
			signal.Stop(signals)
		case <-shutdownCtx.Done():
		}
	}()

	return runCtx, shutdownCtx, stop
}

type storedZeroStateReader interface {
	StoredZeroStateBlocks(ctx context.Context) ([]ton.BlockIDExt, error)
}

func zeroStateBlockFromGlobalConfig(path string) (ton.BlockIDExt, error) {
	cfg, err := liteclient.GetConfigFromFile(path)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	block := ton.BlockIDExt{
		Workchain: cfg.Validator.ZeroState.Workchain,
		Shard:     topShard,
		SeqNo:     0,
		RootHash:  append([]byte(nil), cfg.Validator.ZeroState.RootHash...),
		FileHash:  append([]byte(nil), cfg.Validator.ZeroState.FileHash...),
	}
	if block.Workchain != -1 || !validBlockID(block) {
		return ton.BlockIDExt{}, fmt.Errorf("global config contains invalid zero_state")
	}
	return block, nil
}

func zeroStateIDFromBlock(block ton.BlockIDExt) ton.ZeroStateIDExt {
	return ton.ZeroStateIDExt{
		Workchain: block.Workchain,
		RootHash:  append([]byte(nil), block.RootHash...),
		FileHash:  append([]byte(nil), block.FileHash...),
	}
}

func ensureStoredZeroStateMatchesGlobalConfig(ctx context.Context, store storedZeroStateReader, configured ton.BlockIDExt) error {
	stored, err := store.StoredZeroStateBlocks(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load stored zerostate: %w", err)
	}

	for _, block := range stored {
		if !block.Equals(&configured) {
			return fmt.Errorf("stored zerostate %s does not match global config zerostate %s",
				formatBlockID(block), formatBlockID(configured))
		}
	}
	return nil
}

func startupZeroStateRequired(fromZero bool, initBlock ton.BlockIDExt) bool {
	return fromZero || initBlock.SeqNo == 0
}

func ensureZeroStateBeforeInitialSync(ctx context.Context, logger zerolog.Logger, node *p2p.Node) error {
	logger.Info().Msg("preparing zero state before initial sync")

	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, startupZeroStateAttemptTimeout)
		err := node.EnsureZeroState(attemptCtx)
		cancel()
		if err == nil {
			logger.Info().Msg("zero state is ready")
			return nil
		}
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if errors.Is(err, context.Canceled) {
			return err
		}

		event := logger.Warn()
		event.
			Err(err).
			Int("attempt", attempt).
			Dur("attempt_timeout", startupZeroStateAttemptTimeout).
			Dur("retry_in", startupZeroStateRetry).
			Msg("zero state is not ready, will retry before initial sync")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(startupZeroStateRetry):
		}
	}
}

func validBlockID(block ton.BlockIDExt) bool {
	return len(block.RootHash) == 32 && len(block.FileHash) == 32
}

func formatBlockID(block ton.BlockIDExt) string {
	return fmt.Sprintf(
		"wc=%d shard=%016x seqno=%d root=%x file=%x",
		block.Workchain,
		uint64(block.Shard),
		block.SeqNo,
		block.RootHash,
		block.FileHash,
	)
}

type closeableStorage interface {
	Close() error
}

func closeStorage(logger zerolog.Logger, store closeableStorage) {
	started := time.Now()
	logger.Info().Msg("closing storage")
	if err := store.Close(); err != nil {
		logger.Error().Err(err).Dur("elapsed", time.Since(started)).Msg("failed to close storage")
		return
	}
	logger.Info().Dur("elapsed", time.Since(started)).Msg("storage closed")
}

func startPprof(ctx context.Context, logger zerolog.Logger, addr string) {
	if addr == "" {
		return
	}

	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn().Err(err).Str("pprof_addr", addr).Msg("failed to stop pprof server")
		}
	}()

	go func() {
		logger.Info().
			Str("pprof_addr", addr).
			Str("heap_url", "http://"+addr+"/debug/pprof/heap").
			Str("profile_url", "http://"+addr+"/debug/pprof/profile").
			Msg("started pprof server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Str("pprof_addr", addr).Msg("pprof server stopped")
		}
	}()
}

func startMetricsServer(ctx context.Context, logger zerolog.Logger, addr string, handler http.Handler) error {
	if addr == "" || handler == nil {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn().Err(err).Str("metrics_addr", addr).Msg("failed to stop metrics server")
		}
	}()

	go func() {
		logger.Info().
			Str("metrics_addr", addr).
			Str("metrics_url", "http://"+addr+"/metrics").
			Msg("started prometheus metrics server")
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Str("metrics_addr", addr).Msg("metrics server stopped")
		}
	}()

	return nil
}

type pebbleDBStatusReader interface {
	DBStatus(ctx context.Context) (pebblestore.DBStatus, error)
}

func runConsole(ctx context.Context, logger zerolog.Logger, svc *service2.Service, dbStatus pebbleDBStatusReader) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		if !scanner.Scan() {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		cmd := strings.Fields(strings.ToLower(strings.TrimSpace(scanner.Text())))
		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {
		case "status":
			if len(cmd) == 2 && cmd[1] == "db" {
				if dbStatus == nil {
					fmt.Fprintln(os.Stdout, formatDBStatus(pebblestore.DBStatus{}))
					continue
				}
				status, err := dbStatus.DBStatus(ctx)
				if err != nil {
					logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("failed to load db status")
					continue
				}
				fmt.Fprintln(os.Stdout, formatDBStatus(status))
				continue
			}

			showPeers := len(cmd) > 1 && cmd[1] == "full"
			if len(cmd) > 2 {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			if len(cmd) == 2 && cmd[1] != "full" {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			fmt.Fprintln(os.Stdout, formatStatus(svc.StatusSnapshot(), showPeers))
		case "serialize":
			if len(cmd) != 2 && len(cmd) != 3 {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			if len(cmd) == 2 && cmd[1] == "cancel" {
				if err := svc.CancelPersistentStateSerialization(ctx); err != nil {
					logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("failed to cancel persistent state serialization")
					continue
				}
				fmt.Fprintln(os.Stdout, "persistent state serialization canceled")
				continue
			}

			seqno, err := parseMasterchainSeqno(cmd[1])
			if err != nil {
				logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("invalid serialize command")
				continue
			}

			scope := service2.PersistentStateSerializationAll
			if len(cmd) == 3 {
				if cmd[2] != "basechain" {
					logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown serialize scope")
					continue
				}
				scope = service2.PersistentStateSerializationBasechain
			}

			if err = svc.StartPersistentStateSerialization(ctx, seqno, scope); err != nil {
				logger.Warn().Err(err).Uint32("masterchain_seqno", seqno).Msg("failed to start persistent state serialization")
				continue
			}
			if scope == service2.PersistentStateSerializationBasechain {
				fmt.Fprintf(os.Stdout, "persistent basechain state serialization started for masterchain seqno %d\n", seqno)
			} else {
				fmt.Fprintf(os.Stdout, "persistent state serialization started for masterchain seqno %d\n", seqno)
			}
		case "migrate":
			if len(cmd) != 2 {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			seqno, err := parseMasterchainSeqno(cmd[1])
			if err != nil {
				logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("invalid migrate command")
				continue
			}

			if err = svc.StartCellGenerationMigration(ctx, seqno); err != nil {
				logger.Warn().Err(err).Uint32("masterchain_seqno", seqno).Msg("failed to start cell generation migration")
				continue
			}
			fmt.Fprintf(os.Stdout, "cell generation migration started for masterchain seqno %d\n", seqno)
		default:
			logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
		}
	}
}

func parseMasterchainSeqno(value string) (uint32, error) {
	seqno, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(seqno), nil
}

func formatStatus(snapshot service2.StatusSnapshot, showPeers bool) string {
	return formatStatusWithNow(snapshot, showPeers, time.Now())
}

func formatDBStatus(status pebblestore.DBStatus) string {
	var b strings.Builder

	fmt.Fprintf(&b, "DB Status\n")
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "Cell DB\n")
	if len(status.CellGenerations) == 0 {
		fmt.Fprintf(&b, "  unavailable\n")
		return b.String()
	}

	for _, generation := range status.CellGenerations {
		role := fallbackString(generation.Role, "open")
		fmt.Fprintf(&b, "  generation %d role=%s", generation.ID, role)
		if validBlockID(generation.Origin) {
			fmt.Fprintf(&b, " origin=%s", formatBlock(&generation.Origin))
		}
		fmt.Fprintf(&b, "\n")
		fmt.Fprintf(
			&b,
			"    cache block=%s file=%s file_tables=%d block_hit=%s\n",
			formatDBBytes(uint64(generation.Cache.BlockCacheSize)),
			formatDBBytes(uint64(generation.Cache.FileCacheSize)),
			generation.Cache.FileCacheTableCount,
			formatDBCacheHitRate(generation.Cache.BlockCacheHits, generation.Cache.BlockCacheMisses),
		)
		fmt.Fprintf(&b, "    %-5s %9s %9s %7s %5s %9s %9s %9s %10s %10s %8s\n",
			"shard", "disk", "live", "tables", "amp", "l0 f/s", "l0 size", "debt", "comp", "mem", "fl/ing")
		for _, shard := range generation.Shards {
			formatCellDBShardStatus(&b, fmt.Sprintf("%d", shard.Shard), shard)
		}
		formatCellDBShardStatus(&b, "total", generation.Total)
	}

	return b.String()
}

func formatCellDBShardStatus(b *strings.Builder, label string, shard pebblestore.CellDBShardStatus) {
	fmt.Fprintf(b, "    %-5s %9s %9s %7d %5d %9s %9s %9s %10s %10s %8s\n",
		label,
		formatDBBytes(shard.DiskSize),
		formatDBBytes(shard.LiveSize),
		shard.LiveTables,
		shard.ReadAmp,
		fmt.Sprintf("%d/%d", shard.L0Files, shard.L0Sublevels),
		formatDBBytesInt(shard.L0Size),
		formatDBBytes(shard.CompactionDebt),
		fmt.Sprintf("%d/%s", shard.CompactionsInProgress, formatDBBytesInt(shard.CompactionInProgressSize)),
		fmt.Sprintf("%s/%d", formatDBBytes(shard.MemTableSize), shard.MemTableCount),
		fmt.Sprintf("%d/%d", shard.Flushes, shard.Ingests),
	)
}

func formatDBCacheHitRate(hits int64, misses int64) string {
	total := hits + misses
	if total <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", float64(hits)*100/float64(total))
}

func formatDBBytesInt(value int64) string {
	if value <= 0 {
		return "0B"
	}
	return formatDBBytes(uint64(value))
}

func formatDBBytes(value uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	size := float64(value)
	unit := 0
	for size >= 1024 && unit+1 < len(units) {
		size /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%dB", value)
	}
	if size >= 100 {
		return fmt.Sprintf("%.0f%s", size, units[unit])
	}
	return fmt.Sprintf("%.1f%s", size, units[unit])
}

func formatStatusWithNow(snapshot service2.StatusSnapshot, showPeers bool, now time.Time) string {
	var b strings.Builder

	totalNeighbours := 0
	totalAliveNeighbours := 0
	totalKnownPeers := 0
	totalAliveKnownPeers := 0
	for _, overlay := range snapshot.Overlays {
		totalNeighbours += overlay.ActiveNeighbours
		totalAliveNeighbours += overlay.AliveNeighbours
		totalKnownPeers += overlay.KnownPeers
		totalAliveKnownPeers += overlay.AliveKnownPeers
	}

	fmt.Fprintf(&b, "Status\n")
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "Node\n")
	fmt.Fprintf(&b, "  %-20s %s\n", "listen", fallbackString(snapshot.ListenAddr, "<client-mode>"))
	fmt.Fprintf(&b, "  %-20s %s\n", "latest masterchain", formatBlock(snapshot.LatestMasterchain))
	fmt.Fprintf(&b, "  %-20s %s\n", "latest basechain", formatBlock(snapshot.LatestBasechain))
	fmt.Fprintf(&b, "  %-20s %d\n", "overlays", len(snapshot.Overlays))
	fmt.Fprintf(&b, "  %-20s %d / %d alive\n", "known peers", totalAliveKnownPeers, totalKnownPeers)
	fmt.Fprintf(&b, "  %-20s %d / %d alive\n", "neighbours", totalAliveNeighbours, totalNeighbours)

	formatChainLagStatus(&b, snapshot, now)

	if showPeers {
		fmt.Fprintf(&b, "\n")
		fmt.Fprintf(&b, "Overlays\n")
		if len(snapshot.Overlays) == 0 {
			fmt.Fprintf(&b, "  none\n")
			return b.String()
		}
		for _, overlay := range snapshot.Overlays {
			fmt.Fprintf(&b, "  %s\n", overlay.Name)
			fmt.Fprintf(&b, "    %-18s %d / %d alive\n", "known peers", overlay.AliveKnownPeers, overlay.KnownPeers)
			fmt.Fprintf(&b, "    %-18s %d / %d alive\n", "neighbours", overlay.AliveNeighbours, overlay.ActiveNeighbours)
			if len(overlay.Neighbours) == 0 {
				fmt.Fprintf(&b, "    no active neighbours\n")
				continue
			}
			fmt.Fprintf(&b, "    %-5s %-12s %6s %8s  %s\n", "alive", "last ok", "fail", "score", "addr")
			for _, peer := range overlay.Neighbours {
				fmt.Fprintf(
					&b,
					"    %-5s %-12s %6d %8.1f  %s\n",
					formatBool(peer.Alive),
					formatSince(peer.LastSuccessAt),
					peer.FailedQueries,
					peer.Unreliability,
					peer.Addr,
				)
			}
		}
	}

	return b.String()
}

func formatChainLagStatus(b *strings.Builder, snapshot service2.StatusSnapshot, now time.Time) {
	fmt.Fprintf(b, "\n")
	fmt.Fprintf(b, "Chain Lag\n")
	formatChainLag(
		b,
		"masterchain",
		snapshot.LatestMasterchain,
		snapshot.LocalMasterchain,
		localMasterchainStatus(snapshot),
		snapshot.LocalMasterchainUtime,
		snapshot.LocalMasterchainTx,
		snapshot.LocalMasterchainHasTx,
		now,
	)
	formatBasechainLagStatus(b, snapshot, now)
	formatRecentTPSStatus(b, snapshot.RecentTPS)
}

func formatBasechainLagStatus(b *strings.Builder, snapshot service2.StatusSnapshot, now time.Time) {
	if len(snapshot.LocalBasechainShards) == 0 {
		formatChainLag(
			b,
			"basechain",
			snapshot.LatestBasechain,
			snapshot.LocalBasechain,
			localBasechainStatus(snapshot),
			snapshot.LocalBasechainUtime,
			snapshot.LocalBasechainTx,
			snapshot.LocalBasechainHasTx,
			now,
		)
		return
	}

	latest := latestBasechainShardsByKey(snapshot)
	for _, shard := range snapshot.LocalBasechainShards {
		var network *ton.BlockIDExt
		if block, ok := latest[storage.ShardKeyFromBlock(shard.Block)]; ok {
			network = &block
		}
		formatChainLag(
			b,
			formatBasechainShardName(shard.Block),
			network,
			&shard.Block,
			localBasechainStatus(snapshot),
			shard.Utime,
			shard.Transactions,
			shard.HasTransactions,
			now,
		)
	}
}

func formatRecentTPSStatus(b *strings.Builder, snapshot service2.StatusTPSSnapshot) {
	if snapshot.WindowMasters == 0 {
		return
	}
	if !snapshot.Complete {
		fmt.Fprintf(b, "  %-12s window_masters=%d tx=unknown duration=unknown tps=unknown\n", "tps", snapshot.WindowMasters)
		return
	}

	fmt.Fprintf(
		b,
		"  %-12s window_masters=%d tx=%d duration=%s tps=%.2f\n",
		"tps",
		snapshot.WindowMasters,
		snapshot.Transactions,
		formatLagSeconds(snapshot.DurationSeconds),
		snapshot.TPS,
	)
}

func latestBasechainShardsByKey(snapshot service2.StatusSnapshot) map[storage.ShardKey]ton.BlockIDExt {
	latest := make(map[storage.ShardKey]ton.BlockIDExt, len(snapshot.LatestBasechainShards))
	for _, block := range snapshot.LatestBasechainShards {
		latest[storage.ShardKeyFromBlock(block)] = block
	}
	if len(latest) == 0 && snapshot.LatestBasechain != nil {
		latest[storage.ShardKeyFromBlock(*snapshot.LatestBasechain)] = *snapshot.LatestBasechain
	}
	return latest
}

func formatBasechainShardName(block ton.BlockIDExt) string {
	if block.Shard == topShard {
		return "basechain"
	}
	return fmt.Sprintf("basechain/%016x", uint64(block.Shard))
}

func formatChainLag(
	b *strings.Builder,
	name string,
	network *ton.BlockIDExt,
	local *ton.BlockIDExt,
	localMissing string,
	localUtime int64,
	localTransactions uint32,
	hasLocalTransactions bool,
	now time.Time,
) {
	if local == nil {
		fmt.Fprintf(b, "  %-12s %s latest=%s\n", name, localMissing, formatBlockSeq(network))
		return
	}

	fmt.Fprintf(
		b,
		"  %-12s local=%s latest=%s lag_seconds=%s block_time=%s tx=%s\n",
		name,
		formatBlockSeq(local),
		formatBlockSeq(network),
		formatLocalLagSeconds(now, localUtime),
		formatBlockUtime(localUtime),
		formatTransactionCount(localTransactions, hasLocalTransactions),
	)
}

func formatTransactionCount(count uint32, known bool) string {
	if !known {
		return "unknown"
	}
	return strconv.FormatUint(uint64(count), 10)
}

func formatLocalLagSeconds(now time.Time, blockUtime int64) string {
	if blockUtime <= 0 {
		return "unknown"
	}
	return formatLagSeconds(now.Unix() - blockUtime)
}

func formatLagSeconds(delta int64) string {
	return fmt.Sprintf("%ds", delta)
}

func formatBlockUtime(blockUtime int64) string {
	if blockUtime <= 0 {
		return "unknown"
	}
	return time.Unix(blockUtime, 0).UTC().Format(time.RFC3339)
}

func localMasterchainStatus(snapshot service2.StatusSnapshot) string {
	if snapshot.LocalStateError != "" {
		return "local state error: " + snapshot.LocalStateError
	}
	return "no local current state"
}

func localBasechainStatus(snapshot service2.StatusSnapshot) string {
	if snapshot.LocalStateError != "" {
		return "local state error: " + snapshot.LocalStateError
	}
	if snapshot.LocalStateLoaded {
		return "no local basechain state"
	}
	return "no local current state"
}

func formatBool(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func formatBlock(block *ton.BlockIDExt) string {
	if block == nil {
		return "<none>"
	}
	return fmt.Sprintf("wc=%d shard=%016x seqno=%d", block.Workchain, uint64(block.Shard), block.SeqNo)
}

func formatBlockSeq(block *ton.BlockIDExt) string {
	if block == nil {
		return "<none>"
	}
	return fmt.Sprintf("%d", block.SeqNo)
}

func formatSince(ts time.Time) string {
	if ts.IsZero() {
		return "never"
	}
	return time.Since(ts).Round(time.Second).String() + " ago"
}

func fallbackString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func resolveConfigPath(longPath string, shortPath string) string {
	shortPath = strings.TrimSpace(shortPath)
	if shortPath != "" {
		return shortPath
	}
	longPath = strings.TrimSpace(longPath)
	if longPath == "" {
		return nodeconfig.DefaultPath
	}
	return longPath
}

func displayConfigPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		path = nodeconfig.DefaultPath
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func logFormat(useJSON bool) string {
	if useJSON {
		return "json"
	}
	return "console"
}
