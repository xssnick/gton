package main

import (
	"bytes"
	"crypto/ed25519"
	"net"
	"testing"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
)

func TestRuntimeOptionsFromConfig(t *testing.T) {
	cfg := nodeconfig.Config{
		TON: nodeconfig.TON{
			GlobalConfigPath: "configs/global.config.json",
		},
		ADNL: nodeconfig.ADNL{
			Key:          testSeed(1),
			ListenAddr:   "0.0.0.0:30303",
			ExternalAddr: "203.0.113.10:30303",
		},
		DHT: nodeconfig.DHT{
			Key:        testSeed(2),
			ListenAddr: "0.0.0.0:30304",
		},
		Lite: nodeconfig.Lite{
			Enabled:          true,
			Key:              testSeed(3),
			ListenAddr:       "0.0.0.0:7445",
			MasterBlockCache: 11,
			ShardBlockCache:  22,
		},
		Storage: nodeconfig.Storage{
			Dir: "data/node",
		},
		Metrics: nodeconfig.Metrics{
			Enabled:    true,
			ListenAddr: "127.0.0.1:9090",
		},
	}

	opts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("p2p options: %v", err)
	}
	if opts.GlobalConfigPath != "configs/global.config.json" {
		t.Fatalf("unexpected TON config path %q", opts.GlobalConfigPath)
	}
	if !bytes.Equal(opts.PrivateKey, testPrivateKey(1)) {
		t.Fatal("unexpected ADNL private key")
	}
	if opts.ListenAddr != "0.0.0.0:30303" {
		t.Fatalf("unexpected listen addr %q", opts.ListenAddr)
	}
	if !opts.ExternalIP.Equal(net.ParseIP("203.0.113.10")) {
		t.Fatalf("unexpected external ip %s", opts.ExternalIP)
	}
	if opts.ExternalPort != 30303 {
		t.Fatalf("unexpected external port %d", opts.ExternalPort)
	}
	if !bytes.Equal(opts.DHTPrivateKey, testPrivateKey(2)) {
		t.Fatal("unexpected DHT private key")
	}
	if opts.DHTListenAddr != "0.0.0.0:30304" {
		t.Fatalf("unexpected DHT listen addr %q", opts.DHTListenAddr)
	}

	liteOpts, err := liteserverOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("liteserver options: %v", err)
	}
	if !liteOpts.Enabled {
		t.Fatal("expected liteserver to be enabled")
	}
	if liteOpts.ListenAddr != "0.0.0.0:7445" {
		t.Fatalf("unexpected liteserver listen addr %q", liteOpts.ListenAddr)
	}
	if !bytes.Equal(liteOpts.PrivateKey, testPrivateKey(3)) {
		t.Fatal("unexpected liteserver private key")
	}
	if liteOpts.MasterBlockCache != 11 {
		t.Fatalf("unexpected liteserver master cache %d", liteOpts.MasterBlockCache)
	}
	if liteOpts.ShardBlockCache != 22 {
		t.Fatalf("unexpected liteserver shard cache %d", liteOpts.ShardBlockCache)
	}

	metricsOpts, err := metricsOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("metrics options: %v", err)
	}
	if !metricsOpts.Enabled {
		t.Fatal("expected metrics to be enabled")
	}
	if metricsOpts.ListenAddr != "127.0.0.1:9090" {
		t.Fatalf("unexpected metrics listen addr %q", metricsOpts.ListenAddr)
	}
}

func TestMetricsOptionsRequireListenAddrWhenEnabled(t *testing.T) {
	cfg := nodeconfig.Config{
		Metrics: nodeconfig.Metrics{
			Enabled: true,
		},
	}

	if _, err := metricsOptionsFromConfig(cfg); err == nil {
		t.Fatal("expected enabled metrics without listen addr to fail")
	}
}

func TestLiteserverOptionsRequireKeyWhenEnabled(t *testing.T) {
	cfg := nodeconfig.Config{
		Lite: nodeconfig.Lite{
			Enabled: true,
		},
	}

	if _, err := liteserverOptionsFromConfig(cfg); err == nil {
		t.Fatal("expected enabled liteserver without key to fail")
	}
}

func testPrivateKey(seedByte byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(testSeed(seedByte))
}

func testSeed(seedByte byte) []byte {
	return bytes.Repeat([]byte{seedByte}, ed25519.SeedSize)
}
