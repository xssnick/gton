package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xssnick/gton"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/internal/logutil"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
)

var GitCommit = "unknown"

type cliCommands struct {
	version         bool
	lsPubkey        bool
	adnlID          bool
	skipConfigCheck bool
}

type startupOptions struct {
	Node       gton.NodeOptions
	ConfigFile string

	LogConfig   logutil.Config
	LogFilePath string
	LogFile     logFileOptions

	GlobalConfigURL     string
	ReplaceGlobalConfig bool
	PprofAddr           string
	LiteQueryWorkers    int
}

func main() {
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

	cfg, created, err := loadNodeConfig(context.Background(), startOpts.ConfigFile, commands.lsPubkey || commands.adnlID)
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

	logOutput, logFile, err := newLogOutput(os.Stdout, startOpts.LogFilePath, startOpts.LogFile)
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
		os.Exit(1)
	}
	if startOpts.LiteQueryWorkers > 0 {
		liteclient.ServerQueryWorkers = startOpts.LiteQueryWorkers
	}

	pprofCtx, stopPprof := context.WithCancel(context.Background())
	defer stopPprof()
	startPprof(pprofCtx, logger, strings.TrimSpace(startOpts.PprofAddr))

	globalConfig, err := prepareGlobalConfig(context.Background(), logger, cfg, startOpts.GlobalConfigURL, startOpts.ReplaceGlobalConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	runOpts := startOpts.Node
	runOpts.Config = cfg
	runOpts.GlobalConfig = globalConfig
	runOpts.Logger = logs.Base()
	runOpts.ConsoleInput = os.Stdin
	runOpts.ConsoleOutput = os.Stdout
	err = gton.RunNode(context.Background(), runOpts)
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
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
	lsPubkeyFlag := flags.Bool("ls-pubkey", false, "print liteserver public key in base64 and exit")
	adnlIDFlag := flags.Bool("adnl-id", false, "print ADNL id derived from adnl.key in base64 and exit")
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
	archivePrefetchWindowsFlag := flags.Int("archive-prefetch-windows", runOpts.ArchivePrefetchWindows, "archive catch-up imported window prefetch depth")
	if err := flags.Parse(args); err != nil {
		return startupOptions{}, cliCommands{}, err
	}

	startOpts.ConfigFile = resolveConfigPath(*configPath)
	startOpts.Node = runOpts
	commands := cliCommands{
		version:         *versionFlag,
		lsPubkey:        *lsPubkeyFlag,
		adnlID:          *adnlIDFlag,
		skipConfigCheck: *skipConfigCheckFlag,
	}
	if commands.version || commands.lsPubkey || commands.adnlID {
		return startOpts, commands, nil
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

func prepareGlobalConfig(ctx context.Context, logger zerolog.Logger, cfg nodeconfig.Config, url string, replace bool) (*liteclient.GlobalConfig, error) {
	path := cfg.GlobalConfigPath()
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
