package main

import (
	"crypto/ed25519"
	"fmt"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/liteserver"
	"github.com/xssnick/gton/service/p2p"
)

type liteserverOptions struct {
	Enabled          bool
	NonFinalEnabled  bool
	ListenAddr       string
	PrivateKey       ed25519.PrivateKey
	MasterBlockCache int
	ShardBlockCache  int
	Limits           liteserver.RequestLimitOptions
}

type metricsOptions struct {
	Enabled    bool
	ListenAddr string
	Namespace  string
}

var metricsNamespacePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func p2pOptionsFromConfig(cfg nodeconfig.Config) (p2p.Options, error) {
	opts := p2p.Options{
		GlobalConfigPath: strings.TrimSpace(cfg.TON.GlobalConfigPath),
		ListenAddr:       strings.TrimSpace(cfg.ADNL.ListenAddr),
		DHTListenAddr:    strings.TrimSpace(cfg.DHT.ListenAddr),
	}

	var err error
	opts.PrivateKey, err = privateKeyFromSeed(cfg.ADNL.Key, "adnl.key")
	if err != nil {
		return p2p.Options{}, err
	}

	opts.DHTPrivateKey, err = privateKeyFromSeed(cfg.DHT.Key, "dht.key")
	if err != nil {
		return p2p.Options{}, err
	}

	if rawAddr := strings.TrimSpace(cfg.ADNL.ExternalAddr); rawAddr != "" {
		ip, port, err := parseExternalAddr(rawAddr)
		if err != nil {
			return p2p.Options{}, err
		}
		opts.ExternalIP = ip
		opts.ExternalPort = port
	}

	opts.CustomOverlays, err = customOverlaysFromConfig(cfg.CustomOverlays)
	if err != nil {
		return p2p.Options{}, err
	}

	capacity, err := cfg.LiteSendMessageBroadcastCapacity()
	if err != nil {
		return p2p.Options{}, err
	}
	opts.ExternalBroadcastCapacity = p2p.ExternalBroadcastCapacityOptions{
		BytesPerSecond: capacity.BytesPerSecond,
		MaxDelay:       capacity.MaxDelay,
	}

	return opts, nil
}

func customOverlaysFromConfig(raw []nodeconfig.CustomOverlay) ([]p2p.CustomOverlayConfig, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	seen := map[string]struct{}{}
	overlays := make([]p2p.CustomOverlayConfig, 0, len(raw))
	for idx, overlay := range raw {
		name := strings.TrimSpace(overlay.Name)
		if name == "" {
			return nil, fmt.Errorf("custom_overlays[%d].name is empty", idx)
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate custom overlay name %q", name)
		}
		seen[name] = struct{}{}

		if len(overlay.Nodes) == 0 {
			return nil, fmt.Errorf("custom_overlays[%d].nodes is empty", idx)
		}

		nodes := make([]p2p.CustomOverlayNodeConfig, 0, len(overlay.Nodes))
		for nodeIdx, node := range overlay.Nodes {
			adnlID, err := p2p.NewPeerID(node.ADNLID)
			if err != nil {
				return nil, fmt.Errorf("custom_overlays[%d].nodes[%d].adnl_id: %w", idx, nodeIdx, err)
			}
			nodes = append(nodes, p2p.CustomOverlayNodeConfig{
				ADNLID:            adnlID,
				MsgSender:         node.MsgSender,
				MsgSenderPriority: node.MsgSenderPriority,
				BlockSender:       node.BlockSender,
			})
		}

		shards := make([]p2p.CustomOverlayShard, 0, len(overlay.SenderShards))
		for _, shard := range overlay.SenderShards {
			shards = append(shards, p2p.CustomOverlayShard{
				Workchain: shard.Workchain,
				Shard:     shard.Shard,
			})
		}

		overlays = append(overlays, p2p.CustomOverlayConfig{
			Name:              name,
			Nodes:             nodes,
			SenderShards:      shards,
			SkipPublicMsgSend: overlay.SkipPublicMsgSend,
		})
	}
	return overlays, nil
}

func liteserverOptionsFromConfig(cfg nodeconfig.Config) (liteserverOptions, error) {
	opts := liteserverOptions{
		Enabled:          cfg.Lite.Enabled,
		NonFinalEnabled:  cfg.Lite.NonFinalEnabled,
		ListenAddr:       strings.TrimSpace(cfg.Lite.ListenAddr),
		MasterBlockCache: cfg.Lite.MasterBlockCache,
		ShardBlockCache:  cfg.Lite.ShardBlockCache,
	}
	if opts.ListenAddr == "" {
		opts.ListenAddr = nodeconfig.DefaultLiteListen
	}

	var err error
	opts.PrivateKey, err = privateKeyFromSeed(cfg.Lite.Key, "liteserver.key")
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

	return liteserver.RequestLimitOptions{
		CapacityPerIP:       int(cfg.CapacityPerIP),
		CoolingPerSec:       cfg.CoolingPerSec,
		MaxConnectionsPerIP: int(cfg.MaxConnectionsPerIP),
		MaxKeepAlive:        time.Duration(cfg.MaxKeepAliveSeconds) * time.Second,
	}, nil
}

func metricsOptionsFromConfig(cfg nodeconfig.Config) (metricsOptions, error) {
	namespace := strings.TrimSpace(cfg.Metrics.Namespace)
	if namespace == "" {
		// Config loading applies this default too; keep direct Config callers consistent.
		namespace = nodeconfig.DefaultMetricsNamespace
	}
	if !metricsNamespacePattern.MatchString(namespace) {
		return metricsOptions{}, fmt.Errorf("metrics.namespace must match [A-Za-z_][A-Za-z0-9_]*")
	}

	opts := metricsOptions{
		Enabled:    cfg.Metrics.Enabled,
		ListenAddr: strings.TrimSpace(cfg.Metrics.ListenAddr),
		Namespace:  namespace,
	}
	if opts.Enabled && opts.ListenAddr == "" {
		return metricsOptions{}, fmt.Errorf("metrics.listen_addr is required when metrics.enabled is true")
	}
	return opts, nil
}

func privateKeyFromSeed(seed []byte, field string) (ed25519.PrivateKey, error) {
	if len(seed) == 0 {
		return nil, nil
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("invalid %s: expected %d-byte seed, got %d bytes", field, ed25519.SeedSize, len(seed))
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func parseExternalAddr(raw string) (net.IP, uint16, error) {
	host, portStr, err := net.SplitHostPort(raw)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid adnl.external_addr %q: %w", raw, err)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return nil, 0, fmt.Errorf("invalid adnl.external_addr %q: invalid ip %q", raw, host)
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return nil, 0, fmt.Errorf("invalid adnl.external_addr %q: invalid port %q", raw, portStr)
	}

	return ip, uint16(port), nil
}
