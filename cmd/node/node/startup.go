package node

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/gton"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/keyring"
	"github.com/xssnick/gton/service/validator/msgpool"
	validatorpebble "github.com/xssnick/gton/service/validator/pebblestore"

	"github.com/rs/zerolog"
	adnladdress "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
)

var GitCommit = "unknown"

// liteserverSendQueueSize overrides the per-connection answer queue size:
// responses are produced concurrently, so pipelined backend clients need
// enough headroom to absorb answer bursts without drops.
const liteserverSendQueueSize = 2048

type cliCommands struct {
	version                bool
	lsPubkey               bool
	adnlID                 bool
	validatorControlPubkey bool
	dhtDescriptor          bool
	skipConfigCheck        bool
}

type startupOptions struct {
	Node             gton.NodeOptions
	ConfigFile       string
	DataDir          string
	GlobalConfigFile string

	LogConfig   logutil.Config
	LogFilePath string
	LogFile     logFileOptions

	GlobalConfigURL     string
	ReplaceGlobalConfig bool
	PprofAddr           string
	LiteQueryWorkers    int
}

func Run(extensions ...hooks.ExtensionFactory) {
	startOpts, commands, err := parseNodeFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if commands.version {
		fmt.Fprintln(os.Stdout, GitCommit)
		return
	}

	cfg, created, err := loadNodeConfig(
		context.Background(),
		startOpts.ConfigFile,
		commands.lsPubkey || commands.adnlID || commands.validatorControlPubkey || commands.dhtDescriptor,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	if created {
		if commands.skipConfigCheck {
			fmt.Fprintf(os.Stdout, "created default config %s and continuing startup due --skip-cfg-check\n", displayConfigPath(startOpts.ConfigFile))
		} else {
			fmt.Fprintf(os.Stdout, "created default config %s; review and approve config.json settings, then start the node again\n", displayConfigPath(startOpts.ConfigFile))
			return
		}
	}

	if commands.lsPubkey {
		if err = writeLiteServerPublicKey(os.Stdout, cfg, startOpts.ConfigFile); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		return
	}
	if commands.adnlID {
		if err = writeADNLID(os.Stdout, cfg, startOpts.ConfigFile); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		return
	}
	if commands.validatorControlPubkey {
		if err = writeValidatorControlPublicKey(os.Stdout, cfg, startOpts.ConfigFile); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		return
	}
	if commands.dhtDescriptor {
		if err = writeDHTDescriptor(os.Stdout, cfg, startOpts.ConfigFile, time.Now()); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		return
	}

	exitCode := runConfiguredNode(startOpts, cfg, extensions)
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func runConfiguredNode(startOpts startupOptions, cfg nodeconfig.Config, extensions []hooks.ExtensionFactory) int {
	logOutput, logFile, err := newLogOutput(os.Stdout, startOpts.LogFilePath, startOpts.LogFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log file config: %v\n", err)
		return 1
	}
	if logFile != nil {
		defer func() {
			if err := logFile.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "failed to close log file %q: %v\n", logFile.Filename, err)
			}
		}()
	}

	logs := logutil.NewFactory(logOutput, startOpts.LogConfig)
	logger := logs.Component("main")
	logger.Info().
		Str("git_commit", GitCommit).
		Str("log_level", startOpts.LogConfig.Level.String()).
		Str("log_levels", fallbackString(logutil.FormatLevelOverrides(startOpts.LogConfig.Overrides), "<none>")).
		Str("log_format", logFormat(startOpts.LogConfig.JSON)).
		Str("log_file", fallbackString(strings.TrimSpace(startOpts.LogFilePath), "<disabled>")).
		Int("log_file_max_size_mb", startOpts.LogFile.MaxSizeMB).
		Int("log_file_max_backups", startOpts.LogFile.MaxBackups).
		Int("log_file_max_age_days", startOpts.LogFile.MaxAgeDays).
		Bool("log_file_compress", startOpts.LogFile.Compress).
		Msg("configured logging")

	if startOpts.LiteQueryWorkers < 0 {
		logger.Error().Int("workers", startOpts.LiteQueryWorkers).Msg("invalid liteserver query workers")
		fmt.Fprintf(os.Stderr, "liteserver query workers cannot be negative: %d\n", startOpts.LiteQueryWorkers)
		return 1
	}
	liteQueryConcurrency := startOpts.LiteQueryWorkers
	if liteQueryConcurrency == 0 {
		liteQueryConcurrency = runtime.GOMAXPROCS(0) * 4
	}

	pprofCtx, stopPprof := context.WithCancel(context.Background())
	defer stopPprof()
	if err = startPprof(pprofCtx, logger, strings.TrimSpace(startOpts.PprofAddr)); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}

	runtimeOpts, err := cfg.RuntimeOptions(startOpts.Node)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	if startOpts.DataDir != "" {
		runtimeOpts.Node.Storage.Dir = startOpts.DataDir
	}
	if startOpts.GlobalConfigFile != "" {
		runtimeOpts.GlobalConfigPath = startOpts.GlobalConfigFile
	}

	globalConfig, err := prepareGlobalConfig(context.Background(), logger, runtimeOpts.GlobalConfigPath, startOpts.GlobalConfigURL, startOpts.ReplaceGlobalConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}

	runOpts := runtimeOpts.Node
	runOpts.GlobalConfig = globalConfig
	runOpts.Logger = logs.Base()
	runOpts.ConsoleInput = os.Stdin
	runOpts.ConsoleOutput = os.Stdout
	liteOpts, err := configureLiteserver(
		&runOpts,
		cfg,
		runtimeOpts,
		globalConfig,
		liteQueryConcurrency,
		cfg.Validator.Enabled || cfg.Collator.Enabled,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	if liteOpts.Enabled {
		// Query answers are produced concurrently now: give pipelined backend
		// clients enough per-connection send buffer to absorb response bursts.
		liteclient.ServerClientSendQueueSize = liteserverSendQueueSize
	}
	logger.Info().
		Bool("liteserver", liteOpts.Enabled).
		Str("liteserver_listen_addr", fallbackString(liteOpts.ListenAddr, "<disabled>")).
		Int("liteserver_query_concurrency", liteOpts.QueryConcurrency).
		Int64("liteserver_send_message_broadcast_bytes_per_second", liteOpts.ExternalBroadcastCapacity.BytesPerSecond).
		Dur("liteserver_send_message_broadcast_max_delay", liteOpts.ExternalBroadcastCapacity.MaxDelay).
		Int("liteserver_send_message_broadcast_fanout", liteOpts.ExternalBroadcastFanout).
		Int("liteserver_capacity_per_ip", liteOpts.Limits.CapacityPerIP).
		Float64("liteserver_cooling_per_sec", liteOpts.Limits.CoolingPerSec).
		Int("liteserver_max_connections_per_ip", liteOpts.Limits.MaxConnectionsPerIP).
		Dur("liteserver_max_keep_alive", liteOpts.Limits.MaxKeepAlive).
		Int("liteserver_max_waits_per_ip", liteOpts.Limits.MaxWaitsPerIP).
		Msg("configured liteserver")

	httpOpts, err := configureHTTPAPI(&runOpts, cfg, runtimeOpts, globalConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	logger.Info().
		Bool("http_api", httpOpts.Enabled).
		Str("http_api_listen_addr", fallbackString(httpOpts.ListenAddr, "<disabled>")).
		Dur("http_api_request_timeout", httpOpts.RequestTimeout).
		Msg("configured http api")

	validatorOpts, err := configureValidator(cfg.Validator)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	extensionFactories := append([]hooks.ExtensionFactory(nil), extensions...)
	skipPersistentCleanup := false
	var collationIdentity collatorIdentity
	var localValidator *localValidatorComposition
	var standaloneCollator *standaloneCollatorComposition
	if validatorOpts.Enabled || cfg.Collator.Enabled {
		if runOpts.Storage.Dir == "" {
			fmt.Fprintln(os.Stderr, "storage.dir is required for validator and collator storage")
			return 1
		}

		collationIdentity, err = configureCollatorIdentity(cfg.ADNL.Key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 1
		}
	}
	if validatorOpts.Enabled {
		validatorDir := filepath.Join(runOpts.Storage.Dir, "validator")
		openStarted := time.Now()
		logger.Info().Str("dir", validatorDir).Msg("opening validator storage")
		validatorStore, openErr := validatorpebble.Open(validatorpebble.Options{Dir: validatorDir})
		if openErr != nil {
			logger.Error().Err(openErr).Str("dir", validatorDir).Msg("failed to open validator storage")
			fmt.Fprintf(os.Stderr, "open validator storage %s: %v\n", validatorDir, openErr)

			return 1
		}
		defer func() {
			if skipPersistentCleanup {
				logger.Warn().Str("dir", validatorDir).
					Msg("leaving validator storage to process exit after incomplete node shutdown")

				return
			}
			closeStarted := time.Now()
			logger.Info().Str("dir", validatorDir).Msg("closing validator storage")
			if closeErr := validatorStore.Close(); closeErr != nil {
				logger.Error().Err(closeErr).Str("dir", validatorDir).Msg("failed to close validator storage")

				return
			}
			logger.Info().Str("dir", validatorDir).Dur("elapsed", time.Since(closeStarted)).Msg("validator storage closed")
		}()
		validatorOpts.Extension.Storage = validatorStore.Validator()
		validatorKeys, keyErr := keyring.Open(context.Background(), validatorStore.Validator())
		if keyErr != nil {
			logger.Error().Err(keyErr).Msg("failed to load validator signing keys")
			fmt.Fprintf(os.Stderr, "load validator signing keys: %v\n", keyErr)

			return 1
		}
		validatorOpts.Extension.Keys = validatorKeys
		poolLog := logger.With().Str("component", "validator").Str("subcomponent", "msgpool").Logger()
		validatorOpts.Runtime.Messages.Logger = &poolLog
		validatorRuntime, runtimeErr := validator.NewRuntime(validatorOpts.Runtime)
		if runtimeErr != nil {
			logger.Error().Err(runtimeErr).Msg("failed to initialize validator runtime")
			fmt.Fprintf(os.Stderr, "initialize validator runtime: %v\n", runtimeErr)

			return 1
		}
		defer func() {
			if !skipPersistentCleanup {
				validatorRuntime.Close()
			}
		}()
		validatorOpts.Extension.Runtime = validatorRuntime

		activeKeyIDs := validatorKeys.KeyIDs()
		validatorKeyIDs := make([]string, len(activeKeyIDs))
		for i := range activeKeyIDs {
			validatorKeyIDs[i] = fmt.Sprintf("%x", activeKeyIDs[i])
		}
		logger.Info().
			Bool("validator", true).
			Strs("validator_signing_key_ids", validatorKeyIDs).
			Str("validator_storage", validatorDir).
			Dur("validator_storage_open_elapsed", time.Since(openStarted)).
			Msg("configured validator")

		localValidator = &localValidatorComposition{
			options:         validatorOpts.Extension,
			runtime:         validatorRuntime,
			keys:            validatorKeys,
			control:         validatorOpts.Control,
			delegations:     validatorStore.Validator(),
			collatorStorage: validatorStore.Collator(),
			collatorKeys:    collationIdentity.keys,
			collatorKeyID:   collationIdentity.keyID,
		}
	}

	if cfg.Collator.Enabled {
		policy, policyErr := configureStandaloneValidatorPolicy(cfg.Collator.ValidatorAllowlist)
		if policyErr != nil {
			fmt.Fprintf(os.Stderr, "%v\n", policyErr)
			return 1
		}

		collatorDir := filepath.Join(runOpts.Storage.Dir, "collator")
		openStarted := time.Now()
		logger.Info().Str("dir", collatorDir).Msg("opening standalone collator storage")
		collatorStore, openErr := validatorpebble.Open(validatorpebble.Options{Dir: collatorDir})
		if openErr != nil {
			logger.Error().Err(openErr).Str("dir", collatorDir).Msg("failed to open standalone collator storage")
			fmt.Fprintf(os.Stderr, "open standalone collator storage %s: %v\n", collatorDir, openErr)

			return 1
		}
		defer func() {
			if skipPersistentCleanup {
				logger.Warn().Str("dir", collatorDir).
					Msg("leaving standalone collator storage to process exit after incomplete node shutdown")

				return
			}
			closeStarted := time.Now()
			logger.Info().Str("dir", collatorDir).Msg("closing standalone collator storage")
			if closeErr := collatorStore.Close(); closeErr != nil {
				logger.Error().Err(closeErr).Str("dir", collatorDir).
					Msg("failed to close standalone collator storage")

				return
			}
			logger.Info().Str("dir", collatorDir).Dur("elapsed", time.Since(closeStarted)).
				Msg("standalone collator storage closed")
		}()

		poolLog := logger.With().Str("component", "collator").Str("subcomponent", "msgpool").Logger()
		collatorRuntime, runtimeErr := validator.NewRuntime(validator.SharedRuntimeOptions{
			Messages: msgpool.Config{Logger: &poolLog},
		})
		if runtimeErr != nil {
			logger.Error().Err(runtimeErr).Msg("failed to initialize standalone collator runtime")
			fmt.Fprintf(os.Stderr, "initialize standalone collator runtime: %v\n", runtimeErr)

			return 1
		}
		defer func() {
			if !skipPersistentCleanup {
				collatorRuntime.Close()
			}
		}()

		logger.Info().
			Bool("collator", true).
			Hex("collator_key_id", collationIdentity.keyID[:]).
			Bool("validator_allowlist", cfg.Collator.ValidatorAllowlist.Enabled).
			Int("validator_allowlist_entries", len(policy.allowed)).
			Str("collator_storage", collatorDir).
			Dur("collator_storage_open_elapsed", time.Since(openStarted)).
			Msg("configured standalone collator")

		standaloneCollator = &standaloneCollatorComposition{
			runtime:            collatorRuntime,
			validatorStorage:   collatorStore.Validator(),
			collatorStorage:    collatorStore.Collator(),
			keys:               collationIdentity.keys,
			keyID:              collationIdentity.keyID,
			allowedValidators:  policy.allowed,
			allowAllValidators: policy.allowAll,
		}
	}

	if localValidator != nil || standaloneCollator != nil {
		extensionFactories = append(extensionFactories, newValidatorStackFactory(validatorStackComposition{
			localValidator:     localValidator,
			standaloneCollator: standaloneCollator,
			localADNLID:        collationIdentity.keyID,
		}))
	}

	composeExtensions(&runOpts, extensionFactories)

	err = gton.RunNode(context.Background(), runOpts)
	if errors.Is(err, gton.ErrShutdownIncomplete) {
		skipPersistentCleanup = true
	}
	if err == nil {
		return 0
	}
	fmt.Fprintf(os.Stderr, "%v\n", err)
	return 1
}

// composeExtensions combines configured extension factories. Built-in APIs
// are configured directly on NodeOptions, while the built-in validator uses
// the extension lifecycle and arrives through this list.
func composeExtensions(runOpts *gton.NodeOptions, extensions []hooks.ExtensionFactory) {
	factories := make(hooks.ExtensionComposer, 0, len(extensions)+1)
	factories = append(factories, extensions...)
	if runOpts.Extension != nil {
		factories = append(factories, runOpts.Extension)
	}
	switch {
	case len(factories) == 1 && factories[0] != nil:
		runOpts.Extension = factories[0]
	case len(factories) > 0:
		runOpts.Extension = factories.New
	}
}

func parseNodeFlags(args []string, stderr io.Writer) (startupOptions, cliCommands, error) {
	runOpts := gton.DefaultNodeOptions()
	startOpts := startupOptions{
		Node:      runOpts,
		LogConfig: logutil.Config{Level: zerolog.InfoLevel},
		LogFile:   defaultLogFileOptions(),
	}

	flags := flag.NewFlagSet("gton-node", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", nodeconfig.DefaultPath, "path to node config JSON")
	dataDirFlag := flags.String("data-dir", "", "override storage.dir from node config")
	globalConfigFileFlag := flags.String("global-config-file", "", "override ton.global_config_path from node config")
	lsPubkeyFlag := flags.Bool("ls-pubkey", false, "print liteserver public key in base64 and exit")
	adnlIDFlag := flags.Bool("adnl-id", false, "print ADNL id derived from adnl.key in base64 and exit")
	validatorControlPubkeyFlag := flags.Bool(
		"validator-control-pubkey",
		false,
		"print the boxed validator control server public key in base64 and exit",
	)
	dhtDescriptorFlag := flags.Bool("dht-descriptor", false, "print the signed public DHT descriptor as JSON and exit")
	versionFlag := flags.Bool("version", false, "print build version and exit")
	skipConfigCheckFlag := flags.Bool("skip-cfg-check", false, "continue startup after creating a missing config file")
	verbosityFlag := flags.String("verbosity", "info", "log verbosity: trace, debug, info, warn, error")
	logTypesFlag := flags.String("log-types", "", "category log verbosity overrides, comma-separated: liteserver=debug,p2p=warn")
	logJSONFlag := flags.Bool("log-json", false, "write logs as JSON instead of pretty console")
	logFileFlag := flags.String("log-file", "", "path to rotating log file, disabled by default")
	logFileDefaults := defaultLogFileOptions()
	logFileMaxSizeFlag := flags.Int("log-file-max-size", logFileDefaults.MaxSizeMB, "maximum log file size in megabytes before rotation")
	logFileMaxBackupsFlag := flags.Int("log-file-max-backups", logFileDefaults.MaxBackups, "maximum rotated log files to keep, 0 keeps all")
	logFileMaxAgeFlag := flags.Int("log-file-max-age", logFileDefaults.MaxAgeDays, "maximum days to keep rotated log files, 0 keeps all")
	logFileCompressFlag := flags.Bool("log-file-compress", false, "compress rotated log files")
	globalConfigURLFlag := flags.String("global-config", "", "download TON global config from URL and replace the configured file before start")
	pprofAddrFlag := flags.String("pprof-addr", "", "listen address for net/http/pprof, disabled by default")
	liteQueryWorkersFlag := flags.Int("liteserver-query-workers", 0, "liteserver query worker goroutines, 0 uses tonutils default")
	archiveCheckpointPeriodFlag := flags.Duration("archive-checkpoint-period", runOpts.ArchiveCheckpointPeriod, "archive catch-up current-state checkpoint max interval")
	archivePrefetchWindowsFlag := flags.Int("archive-prefetch-windows", runOpts.ArchivePrefetchWindows, "archive catch-up imported window prefetch depth, 0 uses the default")
	if err := flags.Parse(args); err != nil {
		return startupOptions{}, cliCommands{}, err
	}

	startOpts.ConfigFile = resolveConfigPath(*configPath)
	startOpts.DataDir = strings.TrimSpace(*dataDirFlag)
	startOpts.GlobalConfigFile = strings.TrimSpace(*globalConfigFileFlag)
	startOpts.Node = runOpts
	commands := cliCommands{
		version:                *versionFlag,
		lsPubkey:               *lsPubkeyFlag,
		adnlID:                 *adnlIDFlag,
		validatorControlPubkey: *validatorControlPubkeyFlag,
		dhtDescriptor:          *dhtDescriptorFlag,
		skipConfigCheck:        *skipConfigCheckFlag,
	}
	if commands.version || commands.lsPubkey || commands.adnlID || commands.validatorControlPubkey || commands.dhtDescriptor {
		return startOpts, commands, nil
	}
	if *archivePrefetchWindowsFlag < 0 {
		return startupOptions{}, cliCommands{}, fmt.Errorf("archive prefetch windows cannot be negative: %d", *archivePrefetchWindowsFlag)
	}

	level, err := logutil.ParseLevel(*verbosityFlag)
	if err != nil {
		return startupOptions{}, cliCommands{}, fmt.Errorf("invalid verbosity %q: %w", *verbosityFlag, err)
	}
	logTypeOverrides, err := logutil.ParseLevelOverrides(*logTypesFlag)
	if err != nil {
		return startupOptions{}, cliCommands{}, fmt.Errorf("invalid log type overrides %q: %w", *logTypesFlag, err)
	}

	startOpts.LogConfig = logutil.Config{
		Level:     level,
		Overrides: logTypeOverrides,
		JSON:      *logJSONFlag,
	}
	startOpts.LogFilePath = *logFileFlag
	startOpts.LogFile = logFileOptions{
		MaxSizeMB:  *logFileMaxSizeFlag,
		MaxBackups: *logFileMaxBackupsFlag,
		MaxAgeDays: *logFileMaxAgeFlag,
		Compress:   *logFileCompressFlag,
	}
	if raw := strings.TrimSpace(*globalConfigURLFlag); raw != "" {
		startOpts.GlobalConfigURL = raw
		startOpts.ReplaceGlobalConfig = true
	}
	startOpts.PprofAddr = strings.TrimSpace(*pprofAddrFlag)
	startOpts.LiteQueryWorkers = *liteQueryWorkersFlag
	runOpts.ArchiveCheckpointPeriod = *archiveCheckpointPeriodFlag
	runOpts.ArchivePrefetchWindows = *archivePrefetchWindowsFlag
	startOpts.Node = runOpts

	return startOpts, commands, nil
}

func loadNodeConfig(ctx context.Context, path string, keyOnly bool) (nodeconfig.Config, bool, error) {
	path = resolveConfigPath(path)
	if keyOnly {
		cfg, err := nodeconfig.Load(path)
		if err != nil {
			return nodeconfig.Config{}, false, fmt.Errorf("load config %s: %w", path, err)
		}
		return cfg, false, nil
	}

	result, err := nodeconfig.LoadOrCreate(ctx, path, nodeconfig.DetectExternalIP)
	if err != nil {
		return nodeconfig.Config{}, false, fmt.Errorf("load config %s: %w", path, err)
	}
	return result.Config, result.Created, nil
}

func prepareGlobalConfig(ctx context.Context, logger zerolog.Logger, path string, url string, replace bool) (*liteclient.GlobalConfig, error) {
	result, err := nodeconfig.EnsureGlobalConfig(ctx, path, url, replace)
	if err != nil {
		logger.Error().
			Err(err).
			Str("path", path).
			Str("url", globalConfigURLLabel(url)).
			Msg("failed to prepare global config")
		return nil, fmt.Errorf("prepare global config %s: %w", path, err)
	}
	if result.Downloaded {
		logger.Info().
			Str("path", path).
			Str("url", globalConfigURLLabel(url)).
			Bool("replace", replace).
			Msg("downloaded global config")
	}

	globalConfig, err := liteclient.GetConfigFromFile(path)
	if err != nil {
		logger.Error().Err(err).Str("global_config", path).Msg("failed to load global config")
		return nil, fmt.Errorf("load global config %s: %w", path, err)
	}
	return globalConfig, nil
}

func globalConfigURLLabel(url string) string {
	url = strings.TrimSpace(url)
	if url == "" {
		return nodeconfig.DefaultGlobalConfigURL
	}
	return url
}

const (
	// pprofMutexProfileFraction samples one in N mutex contention events. Off
	// by default in Go, which left /debug/pprof/mutex empty exactly when a
	// stalled node needed it; 1/100 keeps the cost negligible while still
	// showing which lock is contended.
	pprofMutexProfileFraction = 100
	// pprofBlockProfileRateNs samples, on average, one blocking event per this
	// many nanoseconds spent blocked. At 1ms it ignores ordinary channel
	// traffic and records only the stalls worth explaining.
	pprofBlockProfileRateNs = 1_000_000
)

func startPprof(ctx context.Context, logger zerolog.Logger, addr string) error {
	if addr == "" {
		return nil
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen pprof on %s: %w", addr, err)
	}

	// Both profiles are inert unless enabled, so they are armed together with
	// the endpoint that serves them.
	runtime.SetMutexProfileFraction(pprofMutexProfileFraction)
	runtime.SetBlockProfileRate(pprofBlockProfileRateNs)

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
			Str("mutex_url", "http://"+addr+"/debug/pprof/mutex").
			Str("block_url", "http://"+addr+"/debug/pprof/block").
			Msg("started pprof server")
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Str("pprof_addr", addr).Msg("pprof server stopped")
		}
	}()

	return nil
}

func writeLiteServerPublicKey(out io.Writer, cfg nodeconfig.Config, path string) error {
	liteSeed := cfg.Lite.Key
	if len(liteSeed) == 0 {
		return fmt.Errorf("liteserver key is missing in %s", path)
	}
	if len(liteSeed) != ed25519.SeedSize {
		return fmt.Errorf("invalid liteserver key size: expected %d bytes, got %d", ed25519.SeedSize, len(liteSeed))
	}

	litePriv := ed25519.NewKeyFromSeed(liteSeed)
	if _, err := fmt.Fprintln(out, base64.StdEncoding.EncodeToString(litePriv.Public().(ed25519.PublicKey))); err != nil {
		return fmt.Errorf("write liteserver public key: %w", err)
	}
	return nil
}

func writeADNLID(out io.Writer, cfg nodeconfig.Config, path string) error {
	adnlSeed := cfg.ADNL.Key
	if len(adnlSeed) == 0 {
		return fmt.Errorf("ADNL key is missing in %s", path)
	}
	if len(adnlSeed) != ed25519.SeedSize {
		return fmt.Errorf("invalid ADNL key size: expected %d bytes, got %d", ed25519.SeedSize, len(adnlSeed))
	}

	adnlPriv := ed25519.NewKeyFromSeed(adnlSeed)
	adnlID, err := tl.Hash(keys.PublicKeyED25519{Key: adnlPriv.Public().(ed25519.PublicKey)})
	if err != nil {
		return fmt.Errorf("compute ADNL id: %w", err)
	}
	if _, err = fmt.Fprintln(out, base64.StdEncoding.EncodeToString(adnlID)); err != nil {
		return fmt.Errorf("write ADNL id: %w", err)
	}
	return nil
}

func writeDHTDescriptor(out io.Writer, cfg nodeconfig.Config, path string, now time.Time) error {
	seed := cfg.DHT.Key
	if len(seed) == 0 {
		return fmt.Errorf("DHT key is missing in %s", path)
	}
	if len(seed) != ed25519.SeedSize {
		return fmt.Errorf("invalid DHT key size: expected %d bytes, got %d", ed25519.SeedSize, len(seed))
	}

	externalHost, _, err := net.SplitHostPort(strings.TrimSpace(cfg.ADNL.ExternalAddr))
	if err != nil {
		return fmt.Errorf("parse adnl.external_addr: %w", err)
	}
	externalIP := net.ParseIP(externalHost).To4()
	if externalIP == nil || externalIP.IsUnspecified() {
		return fmt.Errorf("adnl.external_addr must contain a public IPv4 address")
	}
	_, dhtPortText, err := net.SplitHostPort(strings.TrimSpace(cfg.DHT.ListenAddr))
	if err != nil {
		return fmt.Errorf("parse dht.listen_addr: %w", err)
	}
	dhtPort, err := strconv.ParseUint(dhtPortText, 10, 16)
	if err != nil || dhtPort == 0 {
		return fmt.Errorf("dht.listen_addr must contain a non-zero port")
	}
	version := now.Unix()
	if version <= 0 || version > int64(^uint32(0)>>1) {
		return fmt.Errorf("current unix time %d does not fit a DHT descriptor version", version)
	}

	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	addressList := &adnladdress.List{
		Addresses: []adnladdress.Address{&adnladdress.UDP{IP: externalIP, Port: int32(dhtPort)}},
		Version:   int32(version),
	}
	node, err := dht.BuildSignedNode(
		keys.PublicKeyED25519{Key: publicKey},
		addressList,
		int32(version),
		-1,
		privateKey,
	)
	if err != nil {
		return fmt.Errorf("sign DHT descriptor: %w", err)
	}

	descriptor := liteclient.DHTNode{
		Type: "dht.node",
		ID: liteclient.ServerID{
			Type: "pub.ed25519",
			Key:  base64.StdEncoding.EncodeToString(publicKey),
		},
		AddrList: liteclient.DHTAddressList{
			Type: "adnl.addressList",
			Addrs: []liteclient.DHTAddress{{
				Type: "adnl.address.udp",
				IP:   int(int32(binary.BigEndian.Uint32(externalIP))),
				Port: int(dhtPort),
			}},
			Version: int(version),
		},
		Version:   int(version),
		Signature: base64.StdEncoding.EncodeToString(node.Signature),
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(descriptor); err != nil {
		return fmt.Errorf("write DHT descriptor: %w", err)
	}
	return nil
}

func writeValidatorControlPublicKey(out io.Writer, cfg nodeconfig.Config, path string) error {
	seed := cfg.Validator.Control.Key
	if len(seed) == 0 {
		return fmt.Errorf("validator control key is missing in %s", path)
	}
	if len(seed) != ed25519.SeedSize {
		return fmt.Errorf(
			"invalid validator control key size: expected %d bytes, got %d",
			ed25519.SeedSize,
			len(seed),
		)
	}

	privateKey := ed25519.NewKeyFromSeed(seed)
	serialized, err := tl.Serialize(keys.PublicKeyED25519{
		Key: privateKey.Public().(ed25519.PublicKey),
	}, true)
	clear(privateKey)
	if err != nil {
		return fmt.Errorf("serialize validator control public key: %w", err)
	}
	if _, err = fmt.Fprintln(out, base64.StdEncoding.EncodeToString(serialized)); err != nil {
		return fmt.Errorf("write validator control public key: %w", err)
	}

	return nil
}

func resolveConfigPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return nodeconfig.DefaultPath
	}
	return path
}

func displayConfigPath(path string) string {
	path = resolveConfigPath(path)
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

func fallbackString(value string, fallback string) string {
	value = strings.TrimSpace(value)
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
