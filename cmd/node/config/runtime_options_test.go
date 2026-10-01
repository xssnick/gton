package config

import (
	"bytes"
	"crypto/ed25519"
	"net"
	"strings"
	"testing"

	"github.com/xssnick/gton"
	"github.com/xssnick/gton/service/p2p"
)

const testTopShard = int64(-1 << 63)

func TestRuntimeOptionsFromConfig(t *testing.T) {
	customNodeID := bytes.Repeat([]byte{4}, p2p.PeerIDSize)
	cfg := defaultConfig()
	cfg.ADNL = ADNL{
		Key:          testSeed(1),
		ListenAddr:   "0.0.0.0:30303",
		ExternalAddr: "203.0.113.10:30303",
	}
	cfg.DHT = DHT{
		Key:        testSeed(2),
		ListenAddr: "0.0.0.0:30304",
	}
	cfg.Storage.Dir = "data/node"
	cfg.TON.SyncUntil = 1719763200
	cfg.Metrics = Metrics{
		Enabled:    true,
		ListenAddr: "127.0.0.1:9090",
		Namespace:  "custom",
	}
	cfg.CustomOverlays = []CustomOverlay{{
		Name: "private-a",
		Nodes: []CustomOverlayNode{{
			ADNLID:            customNodeID,
			MsgSender:         true,
			MsgSenderPriority: 7,
			BlockSender:       true,
			AcceptQueries:     true,
		}},
		SenderShards: []CustomOverlayShard{{
			Workchain: 0,
			Shard:     testTopShard,
		}},
		SkipPublicMsgSend: true,
		UseQUIC:           true,
		SendQueries:       true,
	}}

	runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatalf("runtime options: %v", err)
	}
	nodeOpts := runtimeOpts.Node

	opts := nodeOpts.P2P
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
	if customOverlay.Name != "private-a" || !customOverlay.SkipPublicMsgSend ||
		!customOverlay.UseQUIC || !customOverlay.SendQueries {
		t.Fatalf("unexpected custom overlay metadata: %+v", customOverlay)
	}
	if len(customOverlay.Nodes) != 1 {
		t.Fatalf("unexpected custom overlay node count %d", len(customOverlay.Nodes))
	}
	customNode := customOverlay.Nodes[0]
	if !bytes.Equal(customNode.ADNLID.Bytes(), customNodeID) {
		t.Fatal("unexpected custom overlay ADNL id")
	}
	if !customNode.MsgSender || customNode.MsgSenderPriority != 7 ||
		!customNode.BlockSender || !customNode.AcceptQueries {
		t.Fatalf("unexpected custom overlay node roles: %+v", customNode)
	}
	if len(customOverlay.SenderShards) != 1 || customOverlay.SenderShards[0].Shard != testTopShard {
		t.Fatalf("unexpected custom overlay sender shards: %+v", customOverlay.SenderShards)
	}
	metricsOpts := nodeOpts.Metrics
	if !metricsOpts.Enabled {
		t.Fatal("expected metrics to be enabled")
	}
	if metricsOpts.ListenAddr != "127.0.0.1:9090" {
		t.Fatalf("unexpected metrics listen addr %q", metricsOpts.ListenAddr)
	}
	if metricsOpts.Namespace != "custom" {
		t.Fatalf("unexpected metrics namespace %q", metricsOpts.Namespace)
	}
	if nodeOpts.Storage.Dir != "data/node" {
		t.Fatalf("unexpected storage dir %q", nodeOpts.Storage.Dir)
	}
	if nodeOpts.SyncUntil != 1719763200 {
		t.Fatalf("unexpected sync until %d", nodeOpts.SyncUntil)
	}
}

func TestP2POptionsFromConfig(t *testing.T) {
	cfg := Config{
		ADNL: ADNL{
			Key:        testSeed(1),
			ListenAddr: "0.0.0.0:30303",
		},
		DHT: DHT{
			Key:        testSeed(2),
			ListenAddr: "0.0.0.0:30304",
		},
	}

	opts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("p2p options: %v", err)
	}
	if !bytes.Equal(opts.PrivateKey, testPrivateKey(1)) {
		t.Fatal("unexpected ADNL private key")
	}
	if !bytes.Equal(opts.DHTPrivateKey, testPrivateKey(2)) {
		t.Fatal("unexpected DHT private key")
	}
}

func TestP2POptionsRejectsInvalidCustomOverlays(t *testing.T) {
	_, err := p2pOptionsFromConfig(Config{
		CustomOverlays: []CustomOverlay{{
			Name: "private-a",
			Nodes: []CustomOverlayNode{{
				ADNLID: []byte{1},
			}},
		}},
	})
	if err == nil {
		t.Fatal("expected invalid custom overlay config to fail")
	}
}

func TestRuntimeOptionsConsensusADNL(t *testing.T) {
	cfg := consensusTestConfig()
	cfg.ConsensusADNL.ListenAddr = " 0.0.0.0:30305 "
	cfg.ConsensusADNL.ExternalAddr = " 203.0.113.11:40305 "

	runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatal(err)
	}
	opts := runtimeOpts.Node.P2P
	if !bytes.Equal(opts.PrivateKey, testPrivateKey(1)) || opts.ListenAddr != "0.0.0.0:30303" {
		t.Fatal("dedicated consensus ADNL changed the primary transport")
	}
	if !bytes.Equal(opts.DHTPrivateKey, testPrivateKey(2)) || opts.DHTListenAddr != "0.0.0.0:30304" {
		t.Fatal("dedicated consensus ADNL changed the DHT transport")
	}
	private := opts.PrivateNetwork
	if private == nil {
		t.Fatal("dedicated consensus network is missing")
	}
	if !bytes.Equal(private.PrivateKey, testPrivateKey(3)) || private.ListenAddr != "0.0.0.0:30305" {
		t.Fatal("dedicated consensus ADNL identity or listen address is incorrect")
	}
	if !private.ExternalIP.Equal(net.ParseIP("203.0.113.11")) || private.ExternalPort != 40305 {
		t.Fatal("dedicated consensus ADNL advertised endpoint is incorrect")
	}
}

func TestP2POptionsConsensusADNLOmitted(t *testing.T) {
	cfg := consensusTestConfig()
	cfg.ConsensusADNL = nil

	opts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if opts.PrivateNetwork != nil {
		t.Fatal("omitted consensus_adnl must preserve the shared primary transport")
	}
}

func TestP2POptionsConsensusADNLRejectsInvalidConfig(t *testing.T) {
	type testCase struct {
		name   string
		change func(*Config)
		want   string
	}

	cases := []testCase{
		{
			name:   "missing seed",
			change: func(cfg *Config) { cfg.ConsensusADNL.Key = nil },
			want:   "consensus_adnl.key",
		},
		{
			name:   "private key instead of seed",
			change: func(cfg *Config) { cfg.ConsensusADNL.Key = testPrivateKey(3) },
			want:   "32-byte seed",
		},
		{
			name:   "primary identity reused",
			change: func(cfg *Config) { cfg.ConsensusADNL.Key = cfg.ADNL.Key },
			want:   "must differ from adnl.key",
		},
		{
			name:   "DHT identity reused",
			change: func(cfg *Config) { cfg.ConsensusADNL.Key = cfg.DHT.Key },
			want:   "must differ from dht.key",
		},
		{
			name:   "missing bind address",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "" },
			want:   "consensus_adnl.listen_addr",
		},
		{
			name:   "bind hostname",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "localhost:30305" },
			want:   "consensus_adnl.listen_addr",
		},
		{
			name:   "ephemeral ADNL bind",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "0.0.0.0:0" },
			want:   "port must not be zero",
		},
		{
			name:   "ephemeral derived QUIC bind",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "0.0.0.0:64536" },
			want:   "derives a zero QUIC port",
		},
		{
			name:   "missing advertised address",
			change: func(cfg *Config) { cfg.ConsensusADNL.ExternalAddr = "" },
			want:   "consensus_adnl.external_addr",
		},
		{
			name:   "advertised hostname",
			change: func(cfg *Config) { cfg.ConsensusADNL.ExternalAddr = "node.example:30305" },
			want:   "consensus_adnl.external_addr",
		},
		{
			name:   "advertised IPv6",
			change: func(cfg *Config) { cfg.ConsensusADNL.ExternalAddr = "[2001:db8::1]:30305" },
			want:   "concrete IPv4 address",
		},
		{
			name:   "advertised wildcard",
			change: func(cfg *Config) { cfg.ConsensusADNL.ExternalAddr = "0.0.0.0:30305" },
			want:   "concrete IPv4 address",
		},
		{
			name:   "advertised multicast",
			change: func(cfg *Config) { cfg.ConsensusADNL.ExternalAddr = "224.0.0.1:30305" },
			want:   "concrete IPv4 address",
		},
		{
			name:   "zero advertised port",
			change: func(cfg *Config) { cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:0" },
			want:   "invalid port",
		},
		{
			name:   "zero derived advertised QUIC port",
			change: func(cfg *Config) { cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:64536" },
			want:   "derives a zero QUIC port",
		},
		{
			name:   "primary ADNL collision",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "0.0.0.0:30303" },
			want:   "conflicts with",
		},
		{
			name:   "primary wildcard collision",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "127.0.0.1:30303" },
			want:   "conflicts with",
		},
		{
			name:   "DHT collision",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "0.0.0.0:30304" },
			want:   "dht.listen_addr",
		},
		{
			name:   "primary QUIC collision",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "127.0.0.1:31303" },
			want:   "adnl.listen_addr QUIC",
		},
		{
			name:   "consensus QUIC collides with primary ADNL",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "0.0.0.0:29303" },
			want:   "consensus_adnl.listen_addr QUIC",
		},
		{
			name:   "consensus QUIC collides with DHT",
			change: func(cfg *Config) { cfg.ConsensusADNL.ListenAddr = "0.0.0.0:29304" },
			want:   "consensus_adnl.listen_addr QUIC",
		},
		{
			name: "QUIC wildcard collision on different ADNL interfaces",
			change: func(cfg *Config) {
				cfg.ADNL.ListenAddr = "127.0.0.1:30303"
				cfg.ConsensusADNL.ListenAddr = "127.0.0.2:30303"
			},
			want: "QUIC",
		},
		{
			name:   "IPv6 wildcard collision",
			change: func(cfg *Config) { cfg.DHT.ListenAddr = "[::]:30305" },
			want:   "conflicts with",
		},
		{
			name:   "IPv4-mapped bind collision",
			change: func(cfg *Config) { cfg.DHT.ListenAddr = "[::ffff:127.0.0.1]:30305" },
			want:   "conflicts with",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := consensusTestConfig()
			tc.change(&cfg)

			_, err := p2pOptionsFromConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestP2POptionsConsensusADNLAllowsWrappedQUICPorts(t *testing.T) {
	cfg := consensusTestConfig()
	cfg.ConsensusADNL.ListenAddr = "0.0.0.0:65000"
	cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:65000"

	if _, err := p2pOptionsFromConfig(cfg); err != nil {
		t.Fatalf("C++-compatible QUIC port wraparound was rejected: %v", err)
	}
}

func TestP2POptionsConsensusADNLAdvertisedEndpoints(t *testing.T) {
	type testCase struct {
		name   string
		change func(*Config)
		want   string
	}
	cases := []testCase{
		{
			name: "same ADNL endpoint behind NAT",
			change: func(cfg *Config) {
				cfg.ADNL.ExternalAddr = "203.0.113.10:40303"
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:40303"
			},
			want: "conflicts with adnl.external_addr",
		},
		{
			name: "dedicated ADNL overlaps primary QUIC",
			change: func(cfg *Config) {
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:31303"
			},
			want: "conflicts with adnl.external_addr QUIC",
		},
		{
			name: "dedicated QUIC overlaps primary ADNL",
			change: func(cfg *Config) {
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:29303"
			},
			want: "consensus_adnl.external_addr QUIC",
		},
		{
			name: "dedicated ADNL overlaps DHT",
			change: func(cfg *Config) {
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:30304"
			},
			want: "conflicts with dht advertised endpoint",
		},
		{
			name: "dedicated QUIC overlaps DHT",
			change: func(cfg *Config) {
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:29304"
			},
			want: "conflicts with dht advertised endpoint",
		},
		{
			name: "primary wrapped QUIC port",
			change: func(cfg *Config) {
				cfg.ADNL.ExternalAddr = "203.0.113.10:65000"
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:464"
			},
			want: "conflicts with adnl.external_addr QUIC",
		},
		{
			name: "dedicated wrapped QUIC port",
			change: func(cfg *Config) {
				cfg.ADNL.ExternalAddr = "203.0.113.10:464"
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:65000"
			},
			want: "consensus_adnl.external_addr QUIC",
		},
		{
			name: "primary public endpoint derived from concrete listen address",
			change: func(cfg *Config) {
				cfg.ADNL.ListenAddr = "127.0.0.1:30303"
				cfg.ADNL.ExternalAddr = ""
				cfg.DHT.ListenAddr = ""
				cfg.ConsensusADNL.ExternalAddr = "127.0.0.1:30303"
			},
			want: "conflicts with adnl.external_addr",
		},
		{
			name: "same advertised port on different IPs",
			change: func(cfg *Config) {
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.11:30303"
			},
		},
		{
			name: "DHT port on different public IP",
			change: func(cfg *Config) {
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.11:30304"
			},
		},
		{
			name: "client primary has no public listener",
			change: func(cfg *Config) {
				cfg.ADNL.ListenAddr = ""
				cfg.ConsensusADNL.ExternalAddr = cfg.ADNL.ExternalAddr
			},
		},
		{
			name: "DHT publication remains with client primary",
			change: func(cfg *Config) {
				cfg.ADNL.ListenAddr = ""
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:30304"
			},
			want: "conflicts with dht advertised endpoint",
		},
		{
			name: "wildcard main bind does not imply a public IP",
			change: func(cfg *Config) {
				cfg.ADNL.ExternalAddr = ""
				cfg.DHT.ListenAddr = ""
				cfg.ConsensusADNL.ExternalAddr = "203.0.113.10:30303"
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := consensusTestConfig()
			tc.change(&cfg)

			_, err := p2pOptionsFromConfig(cfg)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("non-overlapping published endpoints were rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestP2POptionsConsensusADNLAllowsClientPrimary(t *testing.T) {
	cfg := consensusTestConfig()
	cfg.ADNL = ADNL{}
	cfg.DHT = DHT{}

	if _, err := p2pOptionsFromConfig(cfg); err != nil {
		t.Fatalf("consensus network with client-only primary transport was rejected: %v", err)
	}
}

func TestP2POptionsConsensusADNLAllowsDHTOnDifferentInterface(t *testing.T) {
	cfg := consensusTestConfig()
	cfg.ConsensusADNL.ListenAddr = "127.0.0.1:30305"
	cfg.ConsensusADNL.ExternalAddr = "203.0.113.11:30305"
	cfg.DHT.ListenAddr = "127.0.0.2:30305"

	if _, err := p2pOptionsFromConfig(cfg); err != nil {
		t.Fatalf("non-overlapping UDP endpoints were rejected: %v", err)
	}
}

func consensusTestConfig() Config {
	cfg := defaultConfig()
	cfg.ADNL = ADNL{
		Key:          testSeed(1),
		ListenAddr:   "0.0.0.0:30303",
		ExternalAddr: "203.0.113.10:30303",
	}
	cfg.DHT = DHT{
		Key:        testSeed(2),
		ListenAddr: "0.0.0.0:30304",
	}
	cfg.ConsensusADNL = &ConsensusADNL{
		Enabled: true,
		ADNL: ADNL{
			Key:          testSeed(3),
			ListenAddr:   "0.0.0.0:30305",
			ExternalAddr: "203.0.113.10:30305",
		},
	}
	return cfg
}

func TestMetricsOptionsDefaultNamespace(t *testing.T) {
	cfg := Config{
		Metrics: Metrics{
			Enabled:    true,
			ListenAddr: "127.0.0.1:9090",
		},
	}

	metricsOpts, err := metricsOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("metrics options: %v", err)
	}
	if metricsOpts.Namespace != DefaultMetricsNamespace {
		t.Fatalf("unexpected default metrics namespace %q", metricsOpts.Namespace)
	}
}

func TestMetricsOptionsRequireListenAddrWhenEnabled(t *testing.T) {
	cfg := Config{
		Metrics: Metrics{
			Enabled: true,
		},
	}

	if _, err := metricsOptionsFromConfig(cfg); err == nil {
		t.Fatal("expected enabled metrics without listen addr to fail")
	}
}

func TestMetricsOptionsRejectInvalidNamespace(t *testing.T) {
	cfg := Config{
		Metrics: Metrics{
			Enabled:    true,
			ListenAddr: "127.0.0.1:9090",
			Namespace:  "bad-name",
		},
	}

	if _, err := metricsOptionsFromConfig(cfg); err == nil {
		t.Fatal("expected invalid metrics namespace to fail")
	}
}

func testPrivateKey(seedByte byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(testSeed(seedByte))
}

func testSeed(seedByte byte) []byte {
	return bytes.Repeat([]byte{seedByte}, ed25519.SeedSize)
}
