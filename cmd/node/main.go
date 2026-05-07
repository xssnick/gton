package main

import (
	"bufio"
	"context"
	"flag"
	nodeconfig "flexserver/cmd/node/config"
	"flexserver/internal/logutil"
	"flexserver/liteserver"
	service2 "flexserver/service"
	"flexserver/service/blocksync"
	"flexserver/service/p2p"
	"flexserver/service/state"
	"flexserver/service/storage"
	"flexserver/service/storage/pebblestore"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
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
)

func main() {
	configPath := flag.String("config", nodeconfig.DefaultPath, "path to node config JSON")
	logLevelFlag := flag.String("log-level", "info", "log level: trace, debug, info, warn, error")
	logLevelsFlag := flag.String("log-levels", "", "category log level overrides, comma-separated: liteserver=debug,p2p=warn")
	logJSONFlag := flag.Bool("log-json", false, "write logs as JSON instead of pretty console")
	globalConfigURLFlag := flag.String("global-config", "", "download TON global config from URL and replace the configured file before start")
	pprofAddrFlag := flag.String("pprof-addr", "", "listen address for net/http/pprof, disabled by default")
	fromZeroFlag := flag.Bool("from-zero", false, "verify initial key block chain from zerostate instead of global config init_block")
	rollbackFlag := flag.Uint("rollback", 0, "rollback local current state to this masterchain seqno before starting")
	archiveCheckpointBlocksFlag := flag.Uint("archive-checkpoint-blocks", service2.DefaultArchiveCatchUpCheckpointBlocks, "archive catch-up current-state checkpoint interval in masterchain blocks")
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
	archiveCheckpointBlocks := uint32(*archiveCheckpointBlocksFlag)
	if uint(archiveCheckpointBlocks) != *archiveCheckpointBlocksFlag {
		fmt.Fprintf(os.Stderr, "invalid archive checkpoint blocks %d: exceeds uint32\n", *archiveCheckpointBlocksFlag)
		os.Exit(1)
	}
	rollbackSeqno := uint32(*rollbackFlag)
	if uint(rollbackSeqno) != *rollbackFlag {
		fmt.Fprintf(os.Stderr, "invalid rollback seqno %d: exceeds uint32\n", *rollbackFlag)
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

	cfg, createdConfig, err := nodeconfig.LoadOrCreate(ctx, *configPath, nodeconfig.DetectExternalIP)
	if err != nil {
		logger.Error().Err(err).Str("config", *configPath).Msg("failed to load config")
		os.Exit(1)
	}
	if createdConfig {
		logger.Info().Str("config", *configPath).Msg("created default config")
	}

	globalConfigURL := nodeconfig.DefaultGlobalConfigURL
	replaceGlobalConfig := false
	if strings.TrimSpace(*globalConfigURLFlag) != "" {
		globalConfigURL = strings.TrimSpace(*globalConfigURLFlag)
		replaceGlobalConfig = true
	}
	globalConfigPath := cfg.GlobalConfigPath()
	downloadedGlobalConfig, err := nodeconfig.EnsureGlobalConfig(ctx, globalConfigPath, globalConfigURL, replaceGlobalConfig)
	if err != nil {
		logger.Error().
			Err(err).
			Str("path", globalConfigPath).
			Str("url", globalConfigURL).
			Msg("failed to prepare global config")
		os.Exit(1)
	}
	if downloadedGlobalConfig {
		logger.Info().
			Str("path", globalConfigPath).
			Str("url", globalConfigURL).
			Bool("replace", replaceGlobalConfig).
			Msg("downloaded global config")
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

	storageDir := cfg.StorageDir()
	stateDownloadDir := ""
	if storageDir == "" {
		logger.Error().Msg("storage.dir is required")
		os.Exit(1)
	}

	store, err := pebblestore.Open(pebblestore.Options{
		Dir:    storageDir,
		Logger: logs.CategoryPtr("pebblestore"),
	})
	if err != nil {
		logger.Error().Err(err).Str("dir", storageDir).Msg("failed to open pebble storage")
		os.Exit(1)
	}
	stateDownloadDir = filepath.Join(storageDir, "runtime", "state-downloads")
	opts.Storage = store
	opts.PeerServingStorage = store
	logger.Info().
		Str("storage", "pebble").
		Str("dir", storageDir).
		Msg("configured storage")
	opts.StateDownloadDir = stateDownloadDir
	defer func() {
		if opts.Storage != nil {
			closeStorage(logger, opts.Storage)
		}
	}()

	if rollbackSeqno != 0 {
		current, err := rollbackCurrentState(ctx, store, rollbackSeqno)
		if err != nil {
			logger.Error().Err(err).Uint32("rollback_seqno", rollbackSeqno).Msg("failed to prepare rollback")
			os.Exit(1)
		}
		stats, err := store.Rollback(ctx, current)
		if err != nil {
			logger.Error().Err(err).Uint32("rollback_seqno", rollbackSeqno).Msg("failed to rollback storage")
			os.Exit(1)
		}
		logger.Info().
			Uint32("rollback_seqno", rollbackSeqno).
			Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Int("shards", len(current.Shards)).
			Int("deleted_metadata_keys", stats.DeletedKeys).
			Msg("storage rolled back")
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
		FromZero: *fromZeroFlag,
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
		CurrentStatePublisher:          currentStatePublisher,
		ShutdownContext:                shutdownCtx,
	})
	node.SetCompressedBlockStateProvider(svc)

	var liteSrv *liteserver.Server
	if liteOpts.Enabled {
		zeroState, err := zeroStateFromGlobalConfig(globalConfigPath)
		if err != nil {
			logger.Error().Err(err).Str("global_config", globalConfigPath).Msg("failed to load liteserver zerostate")
			os.Exit(1)
		}

		liteSrv, err = liteserver.New(liteserver.Options{
			Logger:        logs.CategoryPtr("liteserver"),
			Store:         liveLiteStore,
			MessageSender: node,
			PrivateKey:    liteOpts.PrivateKey,
			ListenAddr:    liteOpts.ListenAddr,
			ZeroState:     zeroState,
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
		Str("config", *configPath).
		Str("log_level", level.String()).
		Str("log_levels", fallbackString(logutil.FormatLevelOverrides(logLevelOverrides), "<none>")).
		Str("log_format", logFormat(*logJSONFlag)).
		Str("global_config", globalConfigPath).
		Str("listen_addr", fallbackString(opts.ListenAddr, "<client-mode>")).
		Bool("dht_server", opts.DHTListenAddr != "").
		Str("dht_listen_addr", fallbackString(opts.DHTListenAddr, "<client-mode>")).
		Bool("liteserver", liteOpts.Enabled).
		Str("liteserver_listen_addr", fallbackString(liteOpts.ListenAddr, "<disabled>")).
		Str("pprof_addr", fallbackString(strings.TrimSpace(*pprofAddrFlag), "<disabled>")).
		Bool("from_zero", *fromZeroFlag).
		Uint32("archive_checkpoint_blocks", archiveCheckpointBlocks).
		Dur("archive_checkpoint_period", *archiveCheckpointPeriodFlag).
		Int("archive_prefetch_windows", *archivePrefetchWindowsFlag).
		Msg("service started")

	go runConsole(ctx, logger, svc)

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

func zeroStateFromGlobalConfig(path string) (ton.ZeroStateIDExt, error) {
	cfg, err := liteclient.GetConfigFromFile(path)
	if err != nil {
		return ton.ZeroStateIDExt{}, err
	}

	return ton.ZeroStateIDExt{
		Workchain: cfg.Validator.ZeroState.Workchain,
		RootHash:  append([]byte(nil), cfg.Validator.ZeroState.RootHash...),
		FileHash:  append([]byte(nil), cfg.Validator.ZeroState.FileHash...),
	}, nil
}

func rollbackCurrentState(ctx context.Context, store *pebblestore.Store, seqno uint32) (*storage.CurrentState, error) {
	master, err := store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, seqno)
	if err != nil {
		return nil, fmt.Errorf("lookup rollback masterchain block #%d: %w", seqno, err)
	}

	masterState, err := store.BlockState(ctx, master)
	if err != nil {
		return nil, fmt.Errorf("load rollback masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}

	shardBlocks, err := state.ShardBlocksFromMasterState(masterState)
	if err != nil {
		return nil, fmt.Errorf("load rollback shard blocks from %s: %w", storage.FormatBlockRef(master), err)
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(masterState),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(shardBlocks)),
	}
	for _, shardBlock := range shardBlocks {
		shardState, err := store.BlockState(ctx, shardBlock)
		if err != nil {
			return nil, fmt.Errorf("load rollback shard state %s: %w", storage.FormatBlockRef(shardBlock), err)
		}
		current.Shards[storage.ShardKeyFromBlock(shardBlock)] = storage.BlockStateWithoutCells(shardState)
	}
	return current, nil
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

func runConsole(ctx context.Context, logger zerolog.Logger, svc *service2.Service) {
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
		default:
			logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
		}
	}
}

func formatStatus(snapshot service2.StatusSnapshot, showPeers bool) string {
	return formatStatusWithNow(snapshot, showPeers, time.Now())
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
		now,
	)
	formatChainLag(
		b,
		"basechain",
		snapshot.LatestBasechain,
		snapshot.LocalBasechain,
		localBasechainStatus(snapshot),
		snapshot.LocalBasechainUtime,
		now,
	)
}

func formatChainLag(
	b *strings.Builder,
	name string,
	network *ton.BlockIDExt,
	local *ton.BlockIDExt,
	localMissing string,
	localUtime int64,
	now time.Time,
) {
	if local == nil {
		fmt.Fprintf(b, "  %-12s %s latest=%s\n", name, localMissing, formatBlockSeq(network))
		return
	}

	fmt.Fprintf(
		b,
		"  %-12s local=%s latest=%s lag_seconds=%s block_time=%s\n",
		name,
		formatBlockSeq(local),
		formatBlockSeq(network),
		formatLocalLagSeconds(now, localUtime),
		formatBlockUtime(localUtime),
	)
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

func logFormat(useJSON bool) string {
	if useJSON {
		return "json"
	}
	return "console"
}
