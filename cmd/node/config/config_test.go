package config

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"flexserver/liteserver"
	"flexserver/service/p2p"
)

func TestLoadDefaults(t *testing.T) {
	path := writeTestConfig(t, `{}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.TON.GlobalConfigPath != p2p.DefaultGlobalConfigPath {
		t.Fatalf("unexpected TON config path %q", cfg.TON.GlobalConfigPath)
	}
}

func TestP2POptions(t *testing.T) {
	adnlSeed := testSeedBase64(1)
	dhtSeed := testSeedBase64(2)
	liteSeed := testSeedBase64(3)
	path := writeTestConfig(t, fmt.Sprintf(`{
		"ton": {
			"global_config_path": "configs/global.config.json"
		},
		"adnl": {
			"key": %q,
			"listen_addr": "0.0.0.0:30303",
			"external_addr": "203.0.113.10:30303"
		},
		"dht": {
			"key": %q,
			"listen_addr": "0.0.0.0:30304"
		},
			"liteserver": {
				"enabled": true,
				"key": %q,
				"listen_addr": "0.0.0.0:7445",
				"master_block_cache": 11,
				"shard_block_cache": 22
			},
		"storage": {
			"dir": "data/node"
		}
	}`, adnlSeed, dhtSeed, liteSeed))

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	opts, err := cfg.P2POptions()
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
	if cfg.StorageDir() != "data/node" {
		t.Fatalf("unexpected storage dir %q", cfg.StorageDir())
	}

	liteOpts, err := cfg.LiteserverOptions()
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
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeTestConfig(t, `{"logging":{"level":"debug"}}`)

	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown config field to fail")
	}
}

func TestLoadOrCreateWritesGeneratedConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "config.json")

	cfg, created, err := LoadOrCreate(context.Background(), path, func(context.Context) (string, error) {
		return "203.0.113.20", nil
	})
	if err != nil {
		t.Fatalf("load or create config: %v", err)
	}
	if !created {
		t.Fatal("expected config to be created")
	}
	if len(cfg.ADNL.Key) != ed25519.SeedSize {
		t.Fatal("expected generated ADNL key")
	}
	if len(cfg.DHT.Key) != ed25519.SeedSize {
		t.Fatal("expected generated DHT key")
	}
	if len(cfg.Lite.Key) != ed25519.SeedSize {
		t.Fatal("expected generated liteserver key")
	}
	if cfg.ADNL.ListenAddr != defaultADNLListen {
		t.Fatalf("unexpected ADNL listen addr %q", cfg.ADNL.ListenAddr)
	}
	if cfg.ADNL.ExternalAddr != "203.0.113.20:30303" {
		t.Fatalf("unexpected external addr %q", cfg.ADNL.ExternalAddr)
	}
	if cfg.DHT.ListenAddr != defaultDHTListen {
		t.Fatalf("unexpected DHT listen addr %q", cfg.DHT.ListenAddr)
	}
	if cfg.Lite.Enabled {
		t.Fatal("expected generated liteserver to be disabled")
	}
	if cfg.Lite.ListenAddr != defaultLiteListen {
		t.Fatalf("unexpected liteserver listen addr %q", cfg.Lite.ListenAddr)
	}
	if cfg.Lite.MasterBlockCache != liteserver.DefaultMasterBlockCache {
		t.Fatalf("unexpected liteserver master cache %d", cfg.Lite.MasterBlockCache)
	}
	if cfg.Lite.ShardBlockCache != liteserver.DefaultShardBlockCache {
		t.Fatalf("unexpected liteserver shard cache %d", cfg.Lite.ShardBlockCache)
	}
	wantStorageDir, err := filepath.Abs(defaultStorageDir)
	if err != nil {
		t.Fatalf("resolve storage dir: %v", err)
	}
	if cfg.Storage.Dir != wantStorageDir {
		t.Fatalf("unexpected storage dir %q", cfg.Storage.Dir)
	}
	wantGlobalConfigPath, err := filepath.Abs(p2p.DefaultGlobalConfigPath)
	if err != nil {
		t.Fatalf("resolve global config path: %v", err)
	}
	if cfg.TON.GlobalConfigPath != wantGlobalConfigPath {
		t.Fatalf("unexpected global config path %q", cfg.TON.GlobalConfigPath)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("unexpected config permissions %s", got)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load generated config: %v", err)
	}
	if !bytes.Equal(loaded.ADNL.Key, cfg.ADNL.Key) {
		t.Fatal("generated config was not persisted")
	}
	if loaded.TON.GlobalConfigPath != wantGlobalConfigPath {
		t.Fatalf("unexpected persisted global config path %q", loaded.TON.GlobalConfigPath)
	}
}

func TestDownloadGlobalConfigWritesAndReplaces(t *testing.T) {
	body := []byte(`{"dht":{"nodes":[]}}`)
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewReader(body)),
			}, nil
		}),
	}

	path := filepath.Join(t.TempDir(), "global.config.json")
	if err := downloadFileWithClient(context.Background(), client, path, "http://example.com/global.config.json"); err != nil {
		t.Fatalf("download global config: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read global config: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("unexpected global config body %q", got)
	}

	body = []byte(`{"updated":true}`)
	if err = downloadFileWithClient(context.Background(), client, path, "http://example.com/global.config.json"); err != nil {
		t.Fatalf("replace global config: %v", err)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read replaced global config: %v", err)
	}
	if string(got) != `{"updated":true}` {
		t.Fatalf("unexpected replaced global config body %q", got)
	}
}

func TestEnsureGlobalConfigSkipsExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "global.config.json")
	if err := os.WriteFile(path, []byte(`{"ready":true}`), 0o644); err != nil {
		t.Fatalf("write global config: %v", err)
	}

	downloaded, err := EnsureGlobalConfig(context.Background(), path, "http://127.0.0.1:1/global.config.json", false)
	if err != nil {
		t.Fatalf("ensure global config: %v", err)
	}
	if downloaded {
		t.Fatal("expected existing global config to be reused")
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func writeTestConfig(tb testing.TB, body string) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		tb.Fatalf("write config: %v", err)
	}
	return path
}

func testSeedBase64(seedByte byte) string {
	return base64.StdEncoding.EncodeToString(testSeed(seedByte))
}

func testPrivateKey(seedByte byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(testSeed(seedByte))
}

func testSeed(seedByte byte) []byte {
	return bytes.Repeat([]byte{seedByte}, ed25519.SeedSize)
}
