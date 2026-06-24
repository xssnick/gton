package gton

import (
	"bytes"
	"crypto/ed25519"
	"net"
	"testing"
	"time"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/p2p"
)

func TestRuntimeOptionsFromConfig(t *testing.T) {
	customNodeID := bytes.Repeat([]byte{4}, p2p.PeerIDSize)
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
			Enabled:                            true,
			NonFinalEnabled:                    true,
			Key:                                testSeed(3),
			ListenAddr:                         "0.0.0.0:7445",
			MasterBlockCache:                   11,
			ShardBlockCache:                    22,
			SendMessageBroadcastBytesPerSecond: 123456,
			SendMessageBroadcastMaxDelayMS:     75,
			Limits: nodeconfig.LiteLimits{
				CapacityPerIP:       100,
				CoolingPerSec:       20,
				MaxConnectionsPerIP: 50,
				MaxKeepAliveSeconds: 60,
			},
		},
		Storage: nodeconfig.Storage{
			Dir: "data/node",
		},
		Metrics: nodeconfig.Metrics{
			Enabled:    true,
			ListenAddr: "127.0.0.1:9090",
			Namespace:  "custom",
		},
		CustomOverlays: []nodeconfig.CustomOverlay{{
			Name: "private-a",
			Nodes: []nodeconfig.CustomOverlayNode{{
				ADNLID:            customNodeID,
				MsgSender:         true,
				MsgSenderPriority: 7,
				BlockSender:       true,
			}},
			SenderShards: []nodeconfig.CustomOverlayShard{{
				Workchain: 0,
				Shard:     topShard,
			}},
			SkipPublicMsgSend: true,
		}},
	}

	opts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("p2p options: %v", err)
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
	if len(opts.CustomOverlays) != 1 {
		t.Fatalf("unexpected custom overlay count %d", len(opts.CustomOverlays))
	}
	customOverlay := opts.CustomOverlays[0]
	if customOverlay.Name != "private-a" || !customOverlay.SkipPublicMsgSend {
		t.Fatalf("unexpected custom overlay metadata: %+v", customOverlay)
	}
	if len(customOverlay.Nodes) != 1 {
		t.Fatalf("unexpected custom overlay node count %d", len(customOverlay.Nodes))
	}
	customNode := customOverlay.Nodes[0]
	if !bytes.Equal(customNode.ADNLID.Bytes(), customNodeID) {
		t.Fatal("unexpected custom overlay ADNL id")
	}
	if !customNode.MsgSender || customNode.MsgSenderPriority != 7 || !customNode.BlockSender {
		t.Fatalf("unexpected custom overlay node roles: %+v", customNode)
	}
	if len(customOverlay.SenderShards) != 1 || customOverlay.SenderShards[0].Shard != topShard {
		t.Fatalf("unexpected custom overlay sender shards: %+v", customOverlay.SenderShards)
	}
	if opts.ExternalBroadcastCapacity.BytesPerSecond != 123456 {
		t.Fatalf("unexpected external broadcast capacity %d", opts.ExternalBroadcastCapacity.BytesPerSecond)
	}
	if opts.ExternalBroadcastCapacity.MaxDelay != 75*time.Millisecond {
		t.Fatalf("unexpected external broadcast max delay %s", opts.ExternalBroadcastCapacity.MaxDelay)
	}

	liteOpts, err := liteserverOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("liteserver options: %v", err)
	}
	if !liteOpts.Enabled {
		t.Fatal("expected liteserver to be enabled")
	}
	if !liteOpts.NonFinalEnabled {
		t.Fatal("expected liteserver non-final mode to be enabled")
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
	if liteOpts.Limits.CapacityPerIP != 100 {
		t.Fatalf("unexpected liteserver capacity per IP %d", liteOpts.Limits.CapacityPerIP)
	}
	if liteOpts.Limits.CoolingPerSec != 20 {
		t.Fatalf("unexpected liteserver cooling per second %f", liteOpts.Limits.CoolingPerSec)
	}
	if liteOpts.Limits.MaxConnectionsPerIP != 50 {
		t.Fatalf("unexpected liteserver max connections per IP %d", liteOpts.Limits.MaxConnectionsPerIP)
	}
	if liteOpts.Limits.MaxKeepAlive != time.Minute {
		t.Fatalf("unexpected liteserver max keep alive %s", liteOpts.Limits.MaxKeepAlive)
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
	if metricsOpts.Namespace != "custom" {
		t.Fatalf("unexpected metrics namespace %q", metricsOpts.Namespace)
	}
}

func TestP2POptionsRejectsInvalidCustomOverlays(t *testing.T) {
	_, err := p2pOptionsFromConfig(nodeconfig.Config{
		CustomOverlays: []nodeconfig.CustomOverlay{{
			Name: "private-a",
			Nodes: []nodeconfig.CustomOverlayNode{{
				ADNLID: []byte{1},
			}},
		}},
	})
	if err == nil {
		t.Fatal("expected invalid custom overlay config to fail")
	}
}

func TestMetricsOptionsDefaultNamespace(t *testing.T) {
	cfg := nodeconfig.Config{
		Metrics: nodeconfig.Metrics{
			Enabled:    true,
			ListenAddr: "127.0.0.1:9090",
		},
	}

	metricsOpts, err := metricsOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("metrics options: %v", err)
	}
	if metricsOpts.Namespace != nodeconfig.DefaultMetricsNamespace {
		t.Fatalf("unexpected default metrics namespace %q", metricsOpts.Namespace)
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

func TestMetricsOptionsRejectInvalidNamespace(t *testing.T) {
	cfg := nodeconfig.Config{
		Metrics: nodeconfig.Metrics{
			Enabled:    true,
			ListenAddr: "127.0.0.1:9090",
			Namespace:  "bad-name",
		},
	}

	if _, err := metricsOptionsFromConfig(cfg); err == nil {
		t.Fatal("expected invalid metrics namespace to fail")
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

func TestLiteserverOptionsDefaultLimitsDisabled(t *testing.T) {
	opts, err := liteserverOptionsFromConfig(nodeconfig.Config{
		Lite: nodeconfig.Lite{
			Key: testSeed(3),
		},
	})
	if err != nil {
		t.Fatalf("liteserver options: %v", err)
	}
	if opts.Limits.CapacityPerIP != 0 || opts.Limits.CoolingPerSec != 0 ||
		opts.Limits.MaxConnectionsPerIP != 0 || opts.Limits.MaxKeepAlive != 0 {
		t.Fatalf("expected default limits to be disabled: %+v", opts.Limits)
	}
}

func TestLiteserverOptionsRejectInvalidLimits(t *testing.T) {
	tests := []struct {
		name   string
		limits nodeconfig.LiteLimits
	}{
		{
			name:   "negative capacity",
			limits: nodeconfig.LiteLimits{CapacityPerIP: -1},
		},
		{
			name:   "negative cooling",
			limits: nodeconfig.LiteLimits{CoolingPerSec: -1},
		},
		{
			name:   "capacity without cooling",
			limits: nodeconfig.LiteLimits{CapacityPerIP: 100},
		},
		{
			name:   "cooling without capacity",
			limits: nodeconfig.LiteLimits{CoolingPerSec: 20},
		},
		{
			name:   "negative connections",
			limits: nodeconfig.LiteLimits{MaxConnectionsPerIP: -1},
		},
		{
			name:   "negative keep alive",
			limits: nodeconfig.LiteLimits{MaxKeepAliveSeconds: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := liteserverOptionsFromConfig(nodeconfig.Config{
				Lite: nodeconfig.Lite{
					Key:    testSeed(3),
					Limits: tt.limits,
				},
			})
			if err == nil {
				t.Fatal("expected invalid liteserver limits to fail")
			}
		})
	}
}

func testPrivateKey(seedByte byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(testSeed(seedByte))
}

func testSeed(seedByte byte) []byte {
	return bytes.Repeat([]byte{seedByte}, ed25519.SeedSize)
}
