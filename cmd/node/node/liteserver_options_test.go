package node

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/xssnick/gton"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/tonutils-go/liteclient"
)

func TestLiteserverOptionsFromConfig(t *testing.T) {
	cfg := nodeconfig.Config{
		Lite: nodeconfig.Lite{
			Enabled:                            true,
			NonFinalEnabled:                    true,
			Key:                                liteserverTestSeed(3),
			ListenAddr:                         "0.0.0.0:7445",
			MasterBlockCache:                   11,
			ShardBlockCache:                    22,
			SendMessageBroadcastBytesPerSecond: 123456,
			SendMessageBroadcastMaxDelayMS:     75,
			SendMessageBroadcastFanout:         15,
			Limits: nodeconfig.LiteLimits{
				CapacityPerIP:       100,
				CoolingPerSec:       20,
				MaxConnectionsPerIP: 50,
				MaxKeepAliveSeconds: 60,
			},
		},
	}

	opts, err := liteserverOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("liteserver options: %v", err)
	}
	if !opts.Enabled {
		t.Fatal("expected liteserver to be enabled")
	}
	if !opts.NonFinalEnabled {
		t.Fatal("expected liteserver non-final mode to be enabled")
	}
	if opts.ListenAddr != "0.0.0.0:7445" {
		t.Fatalf("unexpected liteserver listen addr %q", opts.ListenAddr)
	}
	if !bytes.Equal(opts.PrivateKey, liteserverTestPrivateKey(3)) {
		t.Fatal("unexpected liteserver private key")
	}
	if opts.MasterBlockCache != 11 {
		t.Fatalf("unexpected liteserver master cache %d", opts.MasterBlockCache)
	}
	if opts.ShardBlockCache != 22 {
		t.Fatalf("unexpected liteserver shard cache %d", opts.ShardBlockCache)
	}
	if opts.Limits.CapacityPerIP != 100 {
		t.Fatalf("unexpected liteserver capacity per IP %d", opts.Limits.CapacityPerIP)
	}
	if opts.Limits.CoolingPerSec != 20 {
		t.Fatalf("unexpected liteserver cooling per second %f", opts.Limits.CoolingPerSec)
	}
	if opts.Limits.MaxConnectionsPerIP != 50 {
		t.Fatalf("unexpected liteserver max connections per IP %d", opts.Limits.MaxConnectionsPerIP)
	}
	if opts.Limits.MaxKeepAlive != time.Minute {
		t.Fatalf("unexpected liteserver max keep alive %s", opts.Limits.MaxKeepAlive)
	}
	if opts.ExternalBroadcastCapacity.BytesPerSecond != 123456 {
		t.Fatalf("unexpected external broadcast capacity %d", opts.ExternalBroadcastCapacity.BytesPerSecond)
	}
	if opts.ExternalBroadcastCapacity.MaxDelay != 75*time.Millisecond {
		t.Fatalf("unexpected external broadcast max delay %s", opts.ExternalBroadcastCapacity.MaxDelay)
	}
	if opts.ExternalBroadcastFanout != 15 {
		t.Fatalf("unexpected external broadcast fanout %d", opts.ExternalBroadcastFanout)
	}
}

func TestConfigureLiteserverSetsNodeOptions(t *testing.T) {
	cfg := nodeconfig.Config{
		Lite: nodeconfig.Lite{
			Enabled:                            true,
			NonFinalEnabled:                    true,
			Key:                                liteserverTestSeed(3),
			ListenAddr:                         "0.0.0.0:7445",
			MasterBlockCache:                   11,
			ShardBlockCache:                    22,
			SendMessageBroadcastBytesPerSecond: 123456,
			SendMessageBroadcastMaxDelayMS:     75,
			SendMessageBroadcastFanout:         15,
		},
	}
	var runOpts gton.NodeOptions

	_, err := configureLiteserver(&runOpts, cfg, liteserverTestGlobalConfig())
	if err != nil {
		t.Fatalf("configure liteserver: %v", err)
	}
	if runOpts.LiveView == nil {
		t.Fatal("expected liveview options")
	}
	if runOpts.LiveView.MasterBlockCache != 11 || runOpts.LiveView.ShardBlockCache != 22 || !runOpts.LiveView.NonFinalEnabled {
		t.Fatalf("unexpected liveview options: %+v", *runOpts.LiveView)
	}
	if runOpts.P2P.ExternalBroadcastCapacity.BytesPerSecond != 123456 {
		t.Fatalf("unexpected external broadcast capacity %d", runOpts.P2P.ExternalBroadcastCapacity.BytesPerSecond)
	}
	if runOpts.P2P.LocalExternalFanout != 15 {
		t.Fatalf("unexpected external broadcast fanout %d", runOpts.P2P.LocalExternalFanout)
	}
	if runOpts.Extension == nil {
		t.Fatal("expected liteserver extension factory")
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
			Key: liteserverTestSeed(3),
		},
	})
	if err != nil {
		t.Fatalf("liteserver options: %v", err)
	}
	if opts.Limits.CapacityPerIP != 0 || opts.Limits.CoolingPerSec != 0 ||
		opts.Limits.MaxConnectionsPerIP != 0 || opts.Limits.MaxKeepAlive != 0 {
		t.Fatalf("expected default limits to be disabled: %+v", opts.Limits)
	}
	if opts.ExternalBroadcastFanout != nodeconfig.DefaultLiteSendMessageBroadcastFanout {
		t.Fatalf("unexpected default external broadcast fanout %d", opts.ExternalBroadcastFanout)
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
					Key:    liteserverTestSeed(3),
					Limits: tt.limits,
				},
			})
			if err == nil {
				t.Fatal("expected invalid liteserver limits to fail")
			}
		})
	}
}

func liteserverTestGlobalConfig() *liteclient.GlobalConfig {
	return &liteclient.GlobalConfig{
		Validator: liteclient.ValidatorConfig{
			ZeroState: liteclient.ConfigBlock{
				Workchain: -1,
				RootHash:  bytes.Repeat([]byte{0x11}, 32),
				FileHash:  bytes.Repeat([]byte{0x22}, 32),
			},
		},
	}
}

func liteserverTestPrivateKey(seedByte byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(liteserverTestSeed(seedByte))
}

func liteserverTestSeed(seedByte byte) []byte {
	return bytes.Repeat([]byte{seedByte}, ed25519.SeedSize)
}
