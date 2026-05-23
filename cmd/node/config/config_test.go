package config

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	path := writeTestConfig(t, `{}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.TON.GlobalConfigPath != DefaultGlobalConfigPath {
		t.Fatalf("unexpected TON config path %q", cfg.TON.GlobalConfigPath)
	}
	if cfg.TON.SyncBefore != int64(DefaultSyncBefore/time.Second) {
		t.Fatalf("unexpected sync_before %d", cfg.TON.SyncBefore)
	}
	syncBefore, err := cfg.SyncBefore()
	if err != nil {
		t.Fatalf("sync before: %v", err)
	}
	if syncBefore != DefaultSyncBefore {
		t.Fatalf("unexpected sync before %s", syncBefore)
	}
	if cfg.TON.StateTTL != int64(DefaultStateTTL/time.Second) {
		t.Fatalf("unexpected state_ttl %d", cfg.TON.StateTTL)
	}
	stateTTL, err := cfg.StateTTL()
	if err != nil {
		t.Fatalf("state ttl: %v", err)
	}
	if stateTTL != DefaultStateTTL {
		t.Fatalf("unexpected state ttl %s", stateTTL)
	}
	if cfg.TON.ArchiveTTL != int64(DefaultArchiveTTL/time.Second) {
		t.Fatalf("unexpected archive_ttl %d", cfg.TON.ArchiveTTL)
	}
	archiveTTL, err := cfg.ArchiveTTL()
	if err != nil {
		t.Fatalf("archive ttl: %v", err)
	}
	if archiveTTL != DefaultArchiveTTL {
		t.Fatalf("unexpected archive ttl %s", archiveTTL)
	}
	nextCheckpointBlocks, err := cfg.NextCheckpointBlocks()
	if err != nil {
		t.Fatalf("next checkpoint blocks: %v", err)
	}
	if int64(nextCheckpointBlocks) != DefaultNextCheckpointBlocks {
		t.Fatalf("unexpected next checkpoint blocks %d", nextCheckpointBlocks)
	}
	archiveCheckpointBlocks, err := cfg.ArchiveCheckpointBlocks()
	if err != nil {
		t.Fatalf("archive checkpoint blocks: %v", err)
	}
	if int64(archiveCheckpointBlocks) != DefaultArchiveCheckpointBlocks {
		t.Fatalf("unexpected archive checkpoint blocks %d", archiveCheckpointBlocks)
	}
	checkpointBytes, err := cfg.CheckpointBytes()
	if err != nil {
		t.Fatalf("checkpoint bytes: %v", err)
	}
	if int64(checkpointBytes) != DefaultCheckpointBytes {
		t.Fatalf("unexpected checkpoint bytes %d", checkpointBytes)
	}
	if cfg.DisableStateSerialization {
		t.Fatal("state serialization should be enabled by default")
	}
	if cfg.Metrics.Enabled {
		t.Fatal("metrics should be disabled by default")
	}
	if cfg.Metrics.ListenAddr != "" {
		t.Fatalf("unexpected metrics listen addr %q", cfg.Metrics.ListenAddr)
	}
	if cfg.Metrics.Namespace != DefaultMetricsNamespace {
		t.Fatalf("unexpected metrics namespace %q", cfg.Metrics.Namespace)
	}
	cellTotalCacheSize, err := cfg.CellTotalCacheSize()
	if err != nil {
		t.Fatalf("cell total cache size: %v", err)
	}
	if cellTotalCacheSize != DefaultCellTotalCache {
		t.Fatalf("unexpected cell total cache size %d", cellTotalCacheSize)
	}
	cellShardMemTableSize, err := cfg.CellShardMemTableSize()
	if err != nil {
		t.Fatalf("cell shard memtable size: %v", err)
	}
	if int64(cellShardMemTableSize) != DefaultCellShardMemTable {
		t.Fatalf("unexpected cell shard memtable size %d", cellShardMemTableSize)
	}
	cellMemTableStopWritesThreshold, err := cfg.CellMemTableStopWritesThreshold()
	if err != nil {
		t.Fatalf("cell memtable stop writes threshold: %v", err)
	}
	if int64(cellMemTableStopWritesThreshold) != DefaultCellMemTableStopWritesThreshold {
		t.Fatalf("unexpected cell memtable stop writes threshold %d", cellMemTableStopWritesThreshold)
	}
	artifactFileMaxOpen, err := cfg.ArtifactFileMaxOpen()
	if err != nil {
		t.Fatalf("artifact file max open: %v", err)
	}
	if int64(artifactFileMaxOpen) != DefaultArtifactFileMaxOpen {
		t.Fatalf("unexpected artifact file max open %d", artifactFileMaxOpen)
	}
}

func TestLoadSyncOptions(t *testing.T) {
	path := writeTestConfig(t, `{"ton":{"sync_before":7200,"state_ttl":86400,"archive_ttl":172800,"next_checkpoint_blocks":700,"archive_checkpoint_blocks":2100,"checkpoint_bytes":123456789}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	syncBefore, err := cfg.SyncBefore()
	if err != nil {
		t.Fatalf("sync before: %v", err)
	}
	if syncBefore != 2*time.Hour {
		t.Fatalf("unexpected sync before %s", syncBefore)
	}
	stateTTL, err := cfg.StateTTL()
	if err != nil {
		t.Fatalf("state ttl: %v", err)
	}
	if stateTTL != 24*time.Hour {
		t.Fatalf("unexpected state ttl %s", stateTTL)
	}
	archiveTTL, err := cfg.ArchiveTTL()
	if err != nil {
		t.Fatalf("archive ttl: %v", err)
	}
	if archiveTTL != 48*time.Hour {
		t.Fatalf("unexpected archive ttl %s", archiveTTL)
	}
	nextCheckpointBlocks, err := cfg.NextCheckpointBlocks()
	if err != nil {
		t.Fatalf("next checkpoint blocks: %v", err)
	}
	if nextCheckpointBlocks != 700 {
		t.Fatalf("unexpected next checkpoint blocks %d", nextCheckpointBlocks)
	}
	archiveCheckpointBlocks, err := cfg.ArchiveCheckpointBlocks()
	if err != nil {
		t.Fatalf("archive checkpoint blocks: %v", err)
	}
	if archiveCheckpointBlocks != 2100 {
		t.Fatalf("unexpected archive checkpoint blocks %d", archiveCheckpointBlocks)
	}
	checkpointBytes, err := cfg.CheckpointBytes()
	if err != nil {
		t.Fatalf("checkpoint bytes: %v", err)
	}
	if checkpointBytes != 123456789 {
		t.Fatalf("unexpected checkpoint bytes %d", checkpointBytes)
	}
}

func TestStorageOptions(t *testing.T) {
	path := writeTestConfig(t, `{
		"storage": {
			"dir": "data/node",
			"cell_total_cache_size": 8589934592,
			"cell_shard_memtable_size": 1073741824,
			"cell_memtable_stop_writes_threshold": 3,
			"artifact_file_max_open": 123
		}
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.StorageDir() != "data/node" {
		t.Fatalf("unexpected storage dir %q", cfg.StorageDir())
	}
	cellTotalCacheSize, err := cfg.CellTotalCacheSize()
	if err != nil {
		t.Fatalf("cell total cache size: %v", err)
	}
	if cellTotalCacheSize != 8<<30 {
		t.Fatalf("unexpected cell total cache size %d", cellTotalCacheSize)
	}
	cellShardMemTableSize, err := cfg.CellShardMemTableSize()
	if err != nil {
		t.Fatalf("cell shard memtable size: %v", err)
	}
	if cellShardMemTableSize != 1<<30 {
		t.Fatalf("unexpected cell shard memtable size %d", cellShardMemTableSize)
	}
	cellMemTableStopWritesThreshold, err := cfg.CellMemTableStopWritesThreshold()
	if err != nil {
		t.Fatalf("cell memtable stop writes threshold: %v", err)
	}
	if cellMemTableStopWritesThreshold != 3 {
		t.Fatalf("unexpected cell memtable stop writes threshold %d", cellMemTableStopWritesThreshold)
	}
	artifactFileMaxOpen, err := cfg.ArtifactFileMaxOpen()
	if err != nil {
		t.Fatalf("artifact file max open: %v", err)
	}
	if artifactFileMaxOpen != 123 {
		t.Fatalf("unexpected artifact file max open %d", artifactFileMaxOpen)
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

	result, err := LoadOrCreate(context.Background(), path, func(context.Context) (string, error) {
		return "203.0.113.20", nil
	})
	if err != nil {
		t.Fatalf("load or create config: %v", err)
	}
	if !result.Created {
		t.Fatal("expected config to be created")
	}
	cfg := result.Config
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
	if cfg.Lite.ListenAddr != DefaultLiteListen {
		t.Fatalf("unexpected liteserver listen addr %q", cfg.Lite.ListenAddr)
	}
	if cfg.Lite.MasterBlockCache != DefaultLiteMasterBlockCache {
		t.Fatalf("unexpected liteserver master cache %d", cfg.Lite.MasterBlockCache)
	}
	if cfg.Lite.ShardBlockCache != DefaultLiteShardBlockCache {
		t.Fatalf("unexpected liteserver shard cache %d", cfg.Lite.ShardBlockCache)
	}
	wantStorageDir, err := filepath.Abs(defaultStorageDir)
	if err != nil {
		t.Fatalf("resolve storage dir: %v", err)
	}
	if cfg.Storage.Dir != wantStorageDir {
		t.Fatalf("unexpected storage dir %q", cfg.Storage.Dir)
	}
	if cfg.Storage.ArtifactFileMaxOpen != DefaultArtifactFileMaxOpen {
		t.Fatalf("unexpected artifact file max open %d", cfg.Storage.ArtifactFileMaxOpen)
	}
	wantGlobalConfigPath, err := filepath.Abs(DefaultGlobalConfigPath)
	if err != nil {
		t.Fatalf("resolve global config path: %v", err)
	}
	if cfg.TON.GlobalConfigPath != wantGlobalConfigPath {
		t.Fatalf("unexpected global config path %q", cfg.TON.GlobalConfigPath)
	}
	if cfg.TON.SyncBefore != int64(DefaultSyncBefore/time.Second) {
		t.Fatalf("unexpected sync_before %d", cfg.TON.SyncBefore)
	}
	if cfg.TON.StateTTL != int64(DefaultStateTTL/time.Second) {
		t.Fatalf("unexpected state_ttl %d", cfg.TON.StateTTL)
	}
	if cfg.TON.ArchiveTTL != int64(DefaultArchiveTTL/time.Second) {
		t.Fatalf("unexpected archive_ttl %d", cfg.TON.ArchiveTTL)
	}
	if cfg.TON.NextCheckpointBlocks != DefaultNextCheckpointBlocks {
		t.Fatalf("unexpected next checkpoint blocks %d", cfg.TON.NextCheckpointBlocks)
	}
	if cfg.TON.ArchiveCheckpointBlocks != DefaultArchiveCheckpointBlocks {
		t.Fatalf("unexpected archive checkpoint blocks %d", cfg.TON.ArchiveCheckpointBlocks)
	}
	if cfg.TON.CheckpointBytes != DefaultCheckpointBytes {
		t.Fatalf("unexpected checkpoint bytes %d", cfg.TON.CheckpointBytes)
	}
	if cfg.Metrics.Enabled {
		t.Fatal("expected generated metrics to be disabled")
	}
	if cfg.Metrics.Namespace != DefaultMetricsNamespace {
		t.Fatalf("unexpected generated metrics namespace %q", cfg.Metrics.Namespace)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("unexpected config permissions %s", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !bytes.Contains(data, []byte(`"sync_before"`)) {
		t.Fatal("generated config should use sync_before key")
	}
	if !bytes.Contains(data, []byte(`"state_ttl"`)) {
		t.Fatal("generated config should use state_ttl key")
	}
	if !bytes.Contains(data, []byte(`"archive_ttl"`)) {
		t.Fatal("generated config should use archive_ttl key")
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
	if loaded.TON.SyncBefore != int64(DefaultSyncBefore/time.Second) {
		t.Fatalf("unexpected persisted sync_before %d", loaded.TON.SyncBefore)
	}
	if loaded.TON.StateTTL != int64(DefaultStateTTL/time.Second) {
		t.Fatalf("unexpected persisted state_ttl %d", loaded.TON.StateTTL)
	}
	if loaded.TON.ArchiveTTL != int64(DefaultArchiveTTL/time.Second) {
		t.Fatalf("unexpected persisted archive_ttl %d", loaded.TON.ArchiveTTL)
	}
}

func TestLoadOrCreateRefusesExistingDefaultMetadata(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll(filepath.Join("data", "metadb"), 0o755); err != nil {
		t.Fatalf("create metadb: %v", err)
	}

	_, err := LoadOrCreate(context.Background(), "config.json", func(context.Context) (string, error) {
		t.Fatal("external ip lookup should not be called")
		return "", nil
	})
	if !errors.Is(err, ErrConfigMissingWithExistingStorage) {
		t.Fatalf("expected existing storage error, got %v", err)
	}
	if _, statErr := os.Stat("config.json"); !os.IsNotExist(statErr) {
		t.Fatalf("config should not be created, stat err=%v", statErr)
	}
}

func TestSyncBeforeValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.SyncBefore = 0

	if _, err := cfg.SyncBefore(); err == nil {
		t.Fatal("expected zero sync_before to fail")
	}
}

func TestStateTTLValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.StateTTL = 0

	if _, err := cfg.StateTTL(); err == nil {
		t.Fatal("expected zero state_ttl to fail")
	}
}

func TestArchiveTTLValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.ArchiveTTL = 0

	if _, err := cfg.ArchiveTTL(); err == nil {
		t.Fatal("expected zero archive_ttl to fail")
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

	result, err := EnsureGlobalConfig(context.Background(), path, "http://127.0.0.1:1/global.config.json", false)
	if err != nil {
		t.Fatalf("ensure global config: %v", err)
	}
	if result.Downloaded {
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
