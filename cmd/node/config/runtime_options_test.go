package config

import (
	"bytes"
	"crypto/ed25519"
	"math"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/xssnick/gton"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
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
		FastSyncBroadcastSpeedMultiplier: DefaultFastSyncBroadcastSpeedMultiplier,
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

func TestP2POptionsParseFastSyncMemberCertificates(t *testing.T) {
	certificate := overlay.MemberCertificate{
		IssuedBy: keys.PublicKeyED25519{
			Key: bytes.Repeat([]byte{0x11}, ed25519.PublicKeySize),
		},
		Flags:     7,
		Slot:      2,
		ExpireAt:  2_000_000_000,
		Signature: bytes.Repeat([]byte{0x22}, ed25519.SignatureSize),
	}
	data, err := tl.Serialize(certificate, true)
	if err != nil {
		t.Fatalf("serialize fast sync member certificate: %v", err)
	}

	opts, err := p2pOptionsFromConfig(Config{
		FastSyncMemberCertificates:       [][]byte{data},
		FastSyncBroadcastSpeedMultiplier: DefaultFastSyncBroadcastSpeedMultiplier,
	})
	if err != nil {
		t.Fatalf("p2p options: %v", err)
	}
	if len(opts.FastSyncCertificates) != 1 {
		t.Fatalf(
			"unexpected fast sync member certificate count %d",
			len(opts.FastSyncCertificates),
		)
	}
	if !reflect.DeepEqual(opts.FastSyncCertificates[0], certificate) {
		t.Fatalf(
			"unexpected fast sync member certificate: %+v",
			opts.FastSyncCertificates[0],
		)
	}
}

func TestP2POptionsRejectInvalidFastSyncMemberCertificates(t *testing.T) {
	certificate := overlay.MemberCertificate{
		IssuedBy: keys.PublicKeyED25519{
			Key: bytes.Repeat([]byte{0x11}, ed25519.PublicKeySize),
		},
		Flags:     7,
		Slot:      2,
		ExpireAt:  2_000_000_000,
		Signature: bytes.Repeat([]byte{0x22}, ed25519.SignatureSize),
	}
	boxed, err := tl.Serialize(certificate, true)
	if err != nil {
		t.Fatalf("serialize boxed fast sync member certificate: %v", err)
	}
	unboxed, err := tl.Serialize(certificate, false)
	if err != nil {
		t.Fatalf("serialize unboxed fast sync member certificate: %v", err)
	}
	emptyCertificate, err := tl.Serialize(overlay.EmptyMemberCertificate{}, true)
	if err != nil {
		t.Fatalf("serialize empty fast sync member certificate: %v", err)
	}

	tests := []struct {
		name      string
		data      []byte
		errorPart string
	}{
		{
			name:      "empty bytes",
			data:      []byte{},
			errorPart: "is empty",
		},
		{
			name:      "empty certificate constructor",
			data:      emptyCertificate,
			errorPart: "invalid TL type id",
		},
		{
			name:      "unboxed",
			data:      unboxed,
			errorPart: "invalid TL type id",
		},
		{
			name:      "truncated",
			data:      boxed[:len(boxed)-4],
			errorPart: "failed to parse",
		},
		{
			name:      "trailing bytes",
			data:      append(boxed, 0xff),
			errorPart: "trailing bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p2pOptionsFromConfig(Config{
				FastSyncMemberCertificates:       [][]byte{tt.data},
				FastSyncBroadcastSpeedMultiplier: DefaultFastSyncBroadcastSpeedMultiplier,
			})
			if err == nil {
				t.Fatal("expected invalid fast sync member certificate to fail")
			}
			if !strings.Contains(err.Error(), tt.errorPart) {
				t.Fatalf("unexpected error %q, want containing %q", err, tt.errorPart)
			}
		})
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
		FastSyncBroadcastSpeedMultiplier: DefaultFastSyncBroadcastSpeedMultiplier,
	})
	if err == nil {
		t.Fatal("expected invalid custom overlay config to fail")
	}
}

func TestP2POptionsFastSyncBroadcastSpeedMultiplier(t *testing.T) {
	tests := []struct {
		name       string
		multiplier float64
		wantError  bool
	}{
		{name: "positive", multiplier: 2.5},
		{name: "zero", multiplier: 0, wantError: true},
		{name: "negative", multiplier: -1, wantError: true},
		{name: "nan", multiplier: math.NaN(), wantError: true},
		{name: "infinity", multiplier: math.Inf(1), wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := p2pOptionsFromConfig(Config{
				FastSyncBroadcastSpeedMultiplier: tt.multiplier,
			})
			if tt.wantError {
				if err == nil {
					t.Fatal("expected multiplier rejection")
				}
				return
			}
			if err != nil {
				t.Fatalf("p2p options: %v", err)
			}
			if opts.FastSyncBroadcastSpeedMultiplier != tt.multiplier {
				t.Fatalf(
					"multiplier = %v, want %v",
					opts.FastSyncBroadcastSpeedMultiplier,
					tt.multiplier,
				)
			}
		})
	}
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
