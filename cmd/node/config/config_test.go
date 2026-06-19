package config

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
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
	syncBackpressureWindows, err := cfg.SyncBackpressureWindows()
	if err != nil {
		t.Fatalf("sync backpressure windows: %v", err)
	}
	if int64(syncBackpressureWindows) != DefaultSyncBackpressureWindows {
		t.Fatalf("unexpected sync backpressure windows %d", syncBackpressureWindows)
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
	if cfg.CustomOverlays == nil {
		t.Fatal("custom overlays should default to an empty list")
	}
	if len(cfg.CustomOverlays) != 0 {
		t.Fatalf("unexpected custom overlays %d", len(cfg.CustomOverlays))
	}
	capacity, err := cfg.LiteSendMessageBroadcastCapacity()
	if err != nil {
		t.Fatalf("liteserver send message broadcast capacity: %v", err)
	}
	if capacity.BytesPerSecond != 0 {
		t.Fatalf("unexpected default liteserver send message broadcast capacity %d", capacity.BytesPerSecond)
	}
	if capacity.MaxDelay != DefaultLiteSendMessageBroadcastMaxDelay {
		t.Fatalf("unexpected default liteserver send message broadcast max delay %s", capacity.MaxDelay)
	}
	if cfg.Lite.Limits.CapacityPerIP != 0 || cfg.Lite.Limits.CoolingPerSec != 0 ||
		cfg.Lite.Limits.MaxConnectionsPerIP != 0 || cfg.Lite.Limits.MaxKeepAliveSeconds != 0 {
		t.Fatalf("unexpected default liteserver limits: %+v", cfg.Lite.Limits)
	}
	cellTotalCacheSize, err := cfg.CellTotalCacheSize()
	if err != nil {
		t.Fatalf("cell total cache size: %v", err)
	}
	if cellTotalCacheSize != DefaultCellTotalCache {
		t.Fatalf("unexpected cell total cache size %d", cellTotalCacheSize)
	}
	decodedCellCache, err := cfg.DecodedCellCacheOptions()
	if err != nil {
		t.Fatalf("decoded cell cache options: %v", err)
	}
	if decodedCellCache.Enabled != DefaultDecodedCellCacheEnabled {
		t.Fatalf("unexpected decoded cell cache enabled %v", decodedCellCache.Enabled)
	}
	if int64(decodedCellCache.Shards) != DefaultDecodedCellCacheShards {
		t.Fatalf("unexpected decoded cell cache shards %d", decodedCellCache.Shards)
	}
	if decodedCellCache.BytesPerEntry != DefaultDecodedCellCacheBytesPerEntry {
		t.Fatalf("unexpected decoded cell cache bytes per entry %d", decodedCellCache.BytesPerEntry)
	}
	if int64(decodedCellCache.MinEntries) != DefaultDecodedCellCacheMinEntries {
		t.Fatalf("unexpected decoded cell cache min entries %d", decodedCellCache.MinEntries)
	}
	if int64(decodedCellCache.MaxEntries) != DefaultDecodedCellCacheMaxEntries {
		t.Fatalf("unexpected decoded cell cache max entries %d", decodedCellCache.MaxEntries)
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

func TestLoadCustomOverlays(t *testing.T) {
	nodeID := bytes.Repeat([]byte{0x11}, 32)
	path := writeTestConfig(t, `{"custom_overlays":[{
		"name":"private-a",
		"nodes":[{
			"adnl_id":"`+base64.StdEncoding.EncodeToString(nodeID)+`",
			"msg_sender":true,
			"msg_sender_priority":7,
			"block_sender":true
		}],
		"sender_shards":[{
			"workchain":0,
			"shard":-9223372036854775808
		}],
		"skip_public_msg_send":true
	}]}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.CustomOverlays) != 1 {
		t.Fatalf("unexpected custom overlay count %d", len(cfg.CustomOverlays))
	}
	overlay := cfg.CustomOverlays[0]
	if overlay.Name != "private-a" || !overlay.SkipPublicMsgSend {
		t.Fatalf("unexpected custom overlay metadata: %+v", overlay)
	}
	if len(overlay.Nodes) != 1 {
		t.Fatalf("unexpected custom overlay node count %d", len(overlay.Nodes))
	}
	node := overlay.Nodes[0]
	if !bytes.Equal(node.ADNLID, nodeID) || !node.MsgSender || node.MsgSenderPriority != 7 || !node.BlockSender {
		t.Fatalf("unexpected custom overlay node: %+v", node)
	}
	if len(overlay.SenderShards) != 1 {
		t.Fatalf("unexpected sender shard count %d", len(overlay.SenderShards))
	}
	if overlay.SenderShards[0].Workchain != 0 || overlay.SenderShards[0].Shard != int64(-1<<63) {
		t.Fatalf("unexpected sender shard: %+v", overlay.SenderShards[0])
	}
}

func TestLoadLiteSendMessageBroadcastCapacity(t *testing.T) {
	path := writeTestConfig(t, `{"liteserver":{"send_message_broadcast_bytes_per_second":123456,"send_message_broadcast_max_delay_ms":75}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	capacity, err := cfg.LiteSendMessageBroadcastCapacity()
	if err != nil {
		t.Fatalf("liteserver send message broadcast capacity: %v", err)
	}
	if capacity.BytesPerSecond != 123456 {
		t.Fatalf("unexpected capacity %d", capacity.BytesPerSecond)
	}
	if capacity.MaxDelay != 75*time.Millisecond {
		t.Fatalf("unexpected max delay %s", capacity.MaxDelay)
	}
}

func TestLoadLiteSendMessageBroadcastCapacityRejectsNegative(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "bytes per second",
			body: `{"liteserver":{"send_message_broadcast_bytes_per_second":-1}}`,
		},
		{
			name: "max delay",
			body: `{"liteserver":{"send_message_broadcast_max_delay_ms":-1}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestConfig(t, tt.body)
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			if _, err = cfg.LiteSendMessageBroadcastCapacity(); err == nil {
				t.Fatal("expected negative capacity config to fail")
			}
		})
	}
}

func TestLoadLiteLimits(t *testing.T) {
	path := writeTestConfig(t, `{"liteserver":{"limits":{"capacity_per_ip":100,"cooling_per_sec":20,"max_connections_per_ip":50,"max_keep_alive_seconds":60}}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Lite.Limits.CapacityPerIP != 100 {
		t.Fatalf("unexpected capacity per IP %d", cfg.Lite.Limits.CapacityPerIP)
	}
	if cfg.Lite.Limits.CoolingPerSec != 20 {
		t.Fatalf("unexpected cooling per second %f", cfg.Lite.Limits.CoolingPerSec)
	}
	if cfg.Lite.Limits.MaxConnectionsPerIP != 50 {
		t.Fatalf("unexpected max connections per IP %d", cfg.Lite.Limits.MaxConnectionsPerIP)
	}
	if cfg.Lite.Limits.MaxKeepAliveSeconds != 60 {
		t.Fatalf("unexpected max keep alive seconds %d", cfg.Lite.Limits.MaxKeepAliveSeconds)
	}
}

func TestLoadSyncOptions(t *testing.T) {
	path := writeTestConfig(t, `{"ton":{"sync_before":7200,"state_ttl":86400,"archive_ttl":172800,"next_checkpoint_blocks":700,"archive_checkpoint_blocks":2100,"checkpoint_bytes":123456789,"sync_backpressure_windows":6}}`)

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
	syncBackpressureWindows, err := cfg.SyncBackpressureWindows()
	if err != nil {
		t.Fatalf("sync backpressure windows: %v", err)
	}
	if syncBackpressureWindows != 6 {
		t.Fatalf("unexpected sync backpressure windows %d", syncBackpressureWindows)
	}
}

func TestLoadOldSyncOptionsUsesBackpressureDefault(t *testing.T) {
	path := writeTestConfig(t, `{"ton":{"sync_before":7200,"next_checkpoint_blocks":700,"checkpoint_bytes":123456789}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	syncBackpressureWindows, err := cfg.SyncBackpressureWindows()
	if err != nil {
		t.Fatalf("sync backpressure windows: %v", err)
	}
	if int64(syncBackpressureWindows) != DefaultSyncBackpressureWindows {
		t.Fatalf("unexpected sync backpressure windows %d", syncBackpressureWindows)
	}
}

func TestLoadZeroTTLs(t *testing.T) {
	path := writeTestConfig(t, `{"ton":{"state_ttl":0,"archive_ttl":0}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	stateTTL, err := cfg.StateTTL()
	if err != nil {
		t.Fatalf("state ttl: %v", err)
	}
	if stateTTL != 0 {
		t.Fatalf("unexpected state ttl %s", stateTTL)
	}

	archiveTTL, err := cfg.ArchiveTTL()
	if err != nil {
		t.Fatalf("archive ttl: %v", err)
	}
	if archiveTTL != 0 {
		t.Fatalf("unexpected archive ttl %s", archiveTTL)
	}
}

func TestStorageOptions(t *testing.T) {
	path := writeTestConfig(t, `{
		"storage": {
			"dir": "data/node",
			"cell_total_cache_size": 8589934592,
			"decoded_cell_cache_enabled": false,
			"decoded_cell_cache_shards": 16,
			"decoded_cell_cache_bytes_per_entry": 8192,
			"decoded_cell_cache_min_entries": 1000,
			"decoded_cell_cache_max_entries": 2000,
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
	decodedCellCache, err := cfg.DecodedCellCacheOptions()
	if err != nil {
		t.Fatalf("decoded cell cache options: %v", err)
	}
	if decodedCellCache.Enabled {
		t.Fatal("decoded cell cache should be disabled")
	}
	if decodedCellCache.Shards != 16 {
		t.Fatalf("unexpected decoded cell cache shards %d", decodedCellCache.Shards)
	}
	if decodedCellCache.BytesPerEntry != 8192 {
		t.Fatalf("unexpected decoded cell cache bytes per entry %d", decodedCellCache.BytesPerEntry)
	}
	if decodedCellCache.MinEntries != 1000 {
		t.Fatalf("unexpected decoded cell cache min entries %d", decodedCellCache.MinEntries)
	}
	if decodedCellCache.MaxEntries != 2000 {
		t.Fatalf("unexpected decoded cell cache max entries %d", decodedCellCache.MaxEntries)
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

func TestDecodedCellCacheOptionsRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "negative shards",
			cfg:  Config{Storage: Storage{DecodedCellCacheShards: -1}},
		},
		{
			name: "negative bytes per entry",
			cfg:  Config{Storage: Storage{DecodedCellCacheBytesPerEntry: -1}},
		},
		{
			name: "negative min entries",
			cfg:  Config{Storage: Storage{DecodedCellCacheMinEntries: -1}},
		},
		{
			name: "negative max entries",
			cfg:  Config{Storage: Storage{DecodedCellCacheMaxEntries: -1}},
		},
		{
			name: "min over max",
			cfg:  Config{Storage: Storage{DecodedCellCacheMinEntries: 20, DecodedCellCacheMaxEntries: 10}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.cfg.DecodedCellCacheOptions(); err == nil {
				t.Fatal("expected invalid decoded cell cache options to fail")
			}
		})
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
	if cfg.Lite.NonFinalEnabled {
		t.Fatal("expected generated liteserver non-final mode to be disabled")
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
	if cfg.Lite.SendMessageBroadcastBytesPerSecond != 0 {
		t.Fatalf("unexpected liteserver send message broadcast capacity %d", cfg.Lite.SendMessageBroadcastBytesPerSecond)
	}
	if cfg.Lite.SendMessageBroadcastMaxDelayMS != int64(DefaultLiteSendMessageBroadcastMaxDelay/time.Millisecond) {
		t.Fatalf("unexpected liteserver send message broadcast max delay %d", cfg.Lite.SendMessageBroadcastMaxDelayMS)
	}
	if cfg.Lite.Limits.CapacityPerIP != 0 || cfg.Lite.Limits.CoolingPerSec != 0 ||
		cfg.Lite.Limits.MaxConnectionsPerIP != 0 || cfg.Lite.Limits.MaxKeepAliveSeconds != 0 {
		t.Fatalf("unexpected generated liteserver limits: %+v", cfg.Lite.Limits)
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
	if cfg.Storage.DecodedCellCacheEnabled != DefaultDecodedCellCacheEnabled {
		t.Fatalf("unexpected decoded cell cache enabled %v", cfg.Storage.DecodedCellCacheEnabled)
	}
	if cfg.Storage.DecodedCellCacheShards != DefaultDecodedCellCacheShards {
		t.Fatalf("unexpected decoded cell cache shards %d", cfg.Storage.DecodedCellCacheShards)
	}
	if cfg.Storage.DecodedCellCacheBytesPerEntry != DefaultDecodedCellCacheBytesPerEntry {
		t.Fatalf("unexpected decoded cell cache bytes per entry %d", cfg.Storage.DecodedCellCacheBytesPerEntry)
	}
	if cfg.Storage.DecodedCellCacheMinEntries != DefaultDecodedCellCacheMinEntries {
		t.Fatalf("unexpected decoded cell cache min entries %d", cfg.Storage.DecodedCellCacheMinEntries)
	}
	if cfg.Storage.DecodedCellCacheMaxEntries != DefaultDecodedCellCacheMaxEntries {
		t.Fatalf("unexpected decoded cell cache max entries %d", cfg.Storage.DecodedCellCacheMaxEntries)
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
	if cfg.TON.SyncBackpressureWindows != DefaultSyncBackpressureWindows {
		t.Fatalf("unexpected sync backpressure windows %d", cfg.TON.SyncBackpressureWindows)
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
	if !bytes.Contains(data, []byte(`"sync_backpressure_windows"`)) {
		t.Fatal("generated config should use sync_backpressure_windows key")
	}
	if !bytes.Contains(data, []byte(`"send_message_broadcast_bytes_per_second"`)) {
		t.Fatal("generated config should use send_message_broadcast_bytes_per_second key")
	}
	if !bytes.Contains(data, []byte(`"send_message_broadcast_max_delay_ms"`)) {
		t.Fatal("generated config should use send_message_broadcast_max_delay_ms key")
	}
	if !bytes.Contains(data, []byte(`"limits"`)) {
		t.Fatal("generated config should use liteserver limits key")
	}
	if !bytes.Contains(data, []byte(`"custom_overlays": []`)) {
		t.Fatal("generated config should use an empty custom_overlays list")
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
	if loaded.TON.SyncBackpressureWindows != DefaultSyncBackpressureWindows {
		t.Fatalf("unexpected persisted sync_backpressure_windows %d", loaded.TON.SyncBackpressureWindows)
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
	cfg.TON.SyncBefore = ArchiveFromZeroSyncBefore

	syncBefore, err := cfg.SyncBefore()
	if err != nil {
		t.Fatalf("archive-from-zero sync_before should be allowed: %v", err)
	}
	if syncBefore != 0 {
		t.Fatalf("unexpected archive-from-zero sync before %s", syncBefore)
	}
	if !cfg.ArchiveFromZero() {
		t.Fatal("archive-from-zero mode should be enabled")
	}

	cfg.TON.SyncBefore = 0

	if _, err := cfg.SyncBefore(); err == nil {
		t.Fatal("expected zero sync_before to fail")
	}

	cfg.TON.SyncBefore = -2
	if _, err := cfg.SyncBefore(); err == nil {
		t.Fatal("expected negative sync_before other than -1 to fail")
	}
}

func TestSyncBackpressureWindowsValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.SyncBackpressureWindows = 0

	windows, err := cfg.SyncBackpressureWindows()
	if err != nil {
		t.Fatalf("zero sync_backpressure_windows should use default: %v", err)
	}
	if int64(windows) != DefaultSyncBackpressureWindows {
		t.Fatalf("unexpected sync backpressure windows %d", windows)
	}

	cfg.TON.SyncBackpressureWindows = -1
	if _, err = cfg.SyncBackpressureWindows(); err == nil {
		t.Fatal("expected negative sync_backpressure_windows to fail")
	}
}

func TestStateTTLValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.StateTTL = 0

	stateTTL, err := cfg.StateTTL()
	if err != nil {
		t.Fatalf("zero state_ttl should be allowed: %v", err)
	}
	if stateTTL != 0 {
		t.Fatalf("unexpected zero state ttl %s", stateTTL)
	}

	cfg.TON.StateTTL = -1
	if _, err = cfg.StateTTL(); err == nil {
		t.Fatal("expected negative state_ttl to fail")
	}
}

func TestArchiveTTLValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.ArchiveTTL = 0

	archiveTTL, err := cfg.ArchiveTTL()
	if err != nil {
		t.Fatalf("zero archive_ttl should be allowed: %v", err)
	}
	if archiveTTL != 0 {
		t.Fatalf("unexpected zero archive ttl %s", archiveTTL)
	}

	cfg.TON.ArchiveTTL = -1
	if _, err = cfg.ArchiveTTL(); err == nil {
		t.Fatal("expected negative archive_ttl to fail")
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
