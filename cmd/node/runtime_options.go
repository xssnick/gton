package main

import (
	"crypto/ed25519"
	"fmt"
	"net"
	"strconv"
	"strings"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/p2p"
)

type liteserverOptions struct {
	Enabled          bool
	ListenAddr       string
	PrivateKey       ed25519.PrivateKey
	MasterBlockCache int
	ShardBlockCache  int
}

type metricsOptions struct {
	Enabled    bool
	ListenAddr string
}

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

	return opts, nil
}

func liteserverOptionsFromConfig(cfg nodeconfig.Config) (liteserverOptions, error) {
	opts := liteserverOptions{
		Enabled:          cfg.Lite.Enabled,
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

	return opts, nil
}

func metricsOptionsFromConfig(cfg nodeconfig.Config) (metricsOptions, error) {
	opts := metricsOptions{
		Enabled:    cfg.Metrics.Enabled,
		ListenAddr: strings.TrimSpace(cfg.Metrics.ListenAddr),
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
