package node

import (
	"crypto/ed25519"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/xssnick/gton"
	"github.com/xssnick/gton/api/liteserver"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

type liteserverOptions struct {
	Enabled                   bool
	NonFinalEnabled           bool
	AllowDuplicateExternals   bool
	ListenAddr                string
	PrivateKey                ed25519.PrivateKey
	MasterBlockCache          int
	ShardBlockCache           int
	QueryConcurrency          int
	Limits                    liteserver.RequestLimitOptions
	ExternalBroadcastCapacity p2p.ExternalBroadcastCapacityOptions
	ExternalBroadcastFanout   int
}

func configureLiteserver(runOpts *gton.NodeOptions, cfg nodeconfig.Config, globalConfig *liteclient.GlobalConfig, queryConcurrency int) (liteserverOptions, error) {
	opts, err := liteserverOptionsFromConfig(cfg)
	if err != nil {
		return liteserverOptions{}, err
	}
	opts.QueryConcurrency = queryConcurrency

	runOpts.LiveView = &liveview.Options{
		MasterBlockCache: opts.MasterBlockCache,
		ShardBlockCache:  opts.ShardBlockCache,
		NonFinalEnabled:  opts.NonFinalEnabled,
	}
	runOpts.P2P.ExternalBroadcastCapacity = opts.ExternalBroadcastCapacity
	runOpts.P2P.LocalExternalFanout = opts.ExternalBroadcastFanout
	runOpts.P2P.AllowDuplicateExternals = opts.AllowDuplicateExternals

	if opts.Enabled {
		if runOpts.Extension != nil {
			return liteserverOptions{}, fmt.Errorf("liteserver cannot be enabled together with a custom static extension")
		}

		zeroState, err := liteserverZeroStateFromGlobalConfig(globalConfig)
		if err != nil {
			return liteserverOptions{}, err
		}
		runOpts.Extension = liteserverExtensionFactory(opts, zeroState)
	}

	return opts, nil
}

func liteserverExtensionFactory(opts liteserverOptions, zeroState ton.ZeroStateIDExt) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		return liteserver.NewExtension(node, liteserver.ExtensionConfig{
			PrivateKey:              opts.PrivateKey,
			ListenAddr:              opts.ListenAddr,
			NonFinal:                opts.NonFinalEnabled,
			AllowDuplicateExternals: opts.AllowDuplicateExternals,
			ZeroState:               zeroState,
			RequestLimits:           opts.Limits,
			QueryConcurrency:        opts.QueryConcurrency,
		})
	}
}

func liteserverOptionsFromConfig(cfg nodeconfig.Config) (liteserverOptions, error) {
	opts := liteserverOptions{
		Enabled:                 cfg.Lite.Enabled,
		NonFinalEnabled:         cfg.Lite.NonFinalEnabled,
		AllowDuplicateExternals: cfg.Lite.AllowDuplicateExternals,
		ListenAddr:              strings.TrimSpace(cfg.Lite.ListenAddr),
		MasterBlockCache:        cfg.Lite.MasterBlockCache,
		ShardBlockCache:         cfg.Lite.ShardBlockCache,
	}
	if opts.ListenAddr == "" {
		opts.ListenAddr = nodeconfig.DefaultLiteListen
	}

	var err error
	opts.PrivateKey, err = liteserverPrivateKeyFromSeed(cfg.Lite.Key, "liteserver.key")
	if err != nil {
		return liteserverOptions{}, err
	}
	if opts.Enabled && len(opts.PrivateKey) == 0 {
		return liteserverOptions{}, fmt.Errorf("liteserver.key is required when liteserver.enabled is true")
	}
	if opts.MasterBlockCache < 0 {
		return liteserverOptions{}, fmt.Errorf("liteserver.master_block_cache cannot be negative")
	}
	if opts.ShardBlockCache < 0 {
		return liteserverOptions{}, fmt.Errorf("liteserver.shard_block_cache cannot be negative")
	}

	limits, err := liteserverLimitOptionsFromConfig(cfg.Lite.Limits)
	if err != nil {
		return liteserverOptions{}, err
	}
	opts.Limits = limits

	capacity, err := cfg.LiteSendMessageBroadcastCapacity()
	if err != nil {
		return liteserverOptions{}, err
	}
	opts.ExternalBroadcastCapacity = p2p.ExternalBroadcastCapacityOptions{
		BytesPerSecond: capacity.BytesPerSecond,
		MaxDelay:       capacity.MaxDelay,
	}

	fanout, err := cfg.LiteSendMessageBroadcastFanout()
	if err != nil {
		return liteserverOptions{}, err
	}
	opts.ExternalBroadcastFanout = fanout

	return opts, nil
}

func liteserverLimitOptionsFromConfig(cfg nodeconfig.LiteLimits) (liteserver.RequestLimitOptions, error) {
	if cfg.CapacityPerIP < 0 {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.capacity_per_ip cannot be negative")
	}
	if cfg.CoolingPerSec < 0 || math.IsNaN(cfg.CoolingPerSec) || math.IsInf(cfg.CoolingPerSec, 0) {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.cooling_per_sec must be finite and non-negative")
	}
	if cfg.MaxConnectionsPerIP < 0 {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.max_connections_per_ip cannot be negative")
	}
	if cfg.MaxKeepAliveSeconds < 0 {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.max_keep_alive_seconds cannot be negative")
	}
	if cfg.MaxWaitsPerIP < 0 {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.max_waits_per_ip cannot be negative")
	}
	if cfg.MaxWaitsPerIP > int64(int(^uint(0)>>1)) {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.max_waits_per_ip is too large")
	}
	if (cfg.CapacityPerIP == 0) != (cfg.CoolingPerSec == 0) {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.capacity_per_ip and liteserver.limits.cooling_per_sec must be configured together")
	}
	if cfg.CapacityPerIP > int64(int(^uint(0)>>1)) {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.capacity_per_ip is too large")
	}
	if cfg.MaxConnectionsPerIP > int64(int(^uint(0)>>1)) {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.max_connections_per_ip is too large")
	}

	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if cfg.MaxKeepAliveSeconds > maxDurationSeconds {
		return liteserver.RequestLimitOptions{}, fmt.Errorf("liteserver.limits.max_keep_alive_seconds is too large")
	}

	maxWaitsPerIP := int(cfg.MaxWaitsPerIP)
	if maxWaitsPerIP == 0 {
		maxWaitsPerIP = liteserver.DefaultMaxWaitsPerIP
	}

	return liteserver.RequestLimitOptions{
		CapacityPerIP:       int(cfg.CapacityPerIP),
		CoolingPerSec:       cfg.CoolingPerSec,
		MaxConnectionsPerIP: int(cfg.MaxConnectionsPerIP),
		MaxKeepAlive:        time.Duration(cfg.MaxKeepAliveSeconds) * time.Second,
		MaxWaitsPerIP:       maxWaitsPerIP,
	}, nil
}

func liteserverPrivateKeyFromSeed(seed []byte, field string) (ed25519.PrivateKey, error) {
	if len(seed) == 0 {
		return nil, nil
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("invalid %s: expected %d-byte seed, got %d bytes", field, ed25519.SeedSize, len(seed))
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func liteserverZeroStateFromGlobalConfig(cfg *liteclient.GlobalConfig) (ton.ZeroStateIDExt, error) {
	if cfg == nil {
		return ton.ZeroStateIDExt{}, fmt.Errorf("global config is required")
	}

	zeroState := cfg.Validator.ZeroState
	id := ton.ZeroStateIDExt{
		Workchain: zeroState.Workchain,
		RootHash:  append([]byte(nil), zeroState.RootHash...),
		FileHash:  append([]byte(nil), zeroState.FileHash...),
	}
	if id.Workchain != -1 || len(id.RootHash) != 32 || len(id.FileHash) != 32 {
		return ton.ZeroStateIDExt{}, fmt.Errorf("global config contains invalid zero_state")
	}
	return id, nil
}
