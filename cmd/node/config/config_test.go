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

	"github.com/xssnick/gton"
	"github.com/xssnick/gton/service"
)

func TestLoadDefaults(t *testing.T) {
	path := writeTestConfig(t, `{}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatalf("runtime options: %v", err)
	}
	nodeOpts := runtimeOpts.Node

	if cfg.TON.GlobalConfigPath != DefaultGlobalConfigPath {
		t.Fatalf("unexpected TON config path %q", cfg.TON.GlobalConfigPath)
	}
	if cfg.TON.SyncBefore != int64(DefaultSyncBefore/time.Second) {
		t.Fatalf("unexpected sync_before %d", cfg.TON.SyncBefore)
	}
	if nodeOpts.SyncBefore != DefaultSyncBefore {
		t.Fatalf("unexpected sync before %s", nodeOpts.SyncBefore)
	}
	if nodeOpts.SyncUntil != 0 {
		t.Fatalf("unexpected sync_until %d", nodeOpts.SyncUntil)
	}
	if cfg.TON.StateTTL != int64(DefaultStateTTL/time.Second) {
		t.Fatalf("unexpected state_ttl %d", cfg.TON.StateTTL)
	}
	if nodeOpts.StateTTL != DefaultStateTTL {
		t.Fatalf("unexpected state ttl %s", nodeOpts.StateTTL)
	}
	if cfg.TON.ArchiveTTL != int64(DefaultArchiveTTL/time.Second) {
		t.Fatalf("unexpected archive_ttl %d", cfg.TON.ArchiveTTL)
	}
	if nodeOpts.ArchiveTTL != DefaultArchiveTTL {
		t.Fatalf("unexpected archive ttl %s", nodeOpts.ArchiveTTL)
	}
	if int64(nodeOpts.NextCheckpointBlocks) != DefaultNextCheckpointBlocks {
		t.Fatalf("unexpected next checkpoint blocks %d", nodeOpts.NextCheckpointBlocks)
	}
	if int64(nodeOpts.ArchiveCheckpointBlocks) != DefaultArchiveCheckpointBlocks {
		t.Fatalf("unexpected archive checkpoint blocks %d", nodeOpts.ArchiveCheckpointBlocks)
	}
	if int64(nodeOpts.CheckpointBytes) != DefaultCheckpointBytes {
		t.Fatalf("unexpected checkpoint bytes %d", nodeOpts.CheckpointBytes)
	}
	if int64(nodeOpts.SyncBackpressureWindows) != DefaultSyncBackpressureWindows {
		t.Fatalf("unexpected sync backpressure windows %d", nodeOpts.SyncBackpressureWindows)
	}
	if cfg.DisableStateSerialization {
		t.Fatal("state serialization should be enabled by default")
	}
	if cfg.Validator.Enabled {
		t.Fatal("validator should be disabled by default")
	}
	if cfg.Collator.Enabled {
		t.Fatal("standalone collator should be disabled by default")
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
	capacity := runtimeOpts.LiteSendMessageBroadcastCapacity
	if capacity.BytesPerSecond != 0 {
		t.Fatalf("unexpected default liteserver send message broadcast capacity %d", capacity.BytesPerSecond)
	}
	if capacity.MaxDelay != DefaultLiteSendMessageBroadcastMaxDelay {
		t.Fatalf("unexpected default liteserver send message broadcast max delay %s", capacity.MaxDelay)
	}
	fanout := runtimeOpts.LiteSendMessageBroadcastFanout
	if fanout != DefaultLiteSendMessageBroadcastFanout {
		t.Fatalf("unexpected default liteserver send message broadcast fanout %d", fanout)
	}
	if cfg.Lite.Limits.CapacityPerIP != 0 || cfg.Lite.Limits.CoolingPerSec != 0 ||
		cfg.Lite.Limits.MaxConnectionsPerIP != 0 || cfg.Lite.Limits.MaxKeepAliveSeconds != 0 {
		t.Fatalf("unexpected default liteserver limits: %+v", cfg.Lite.Limits)
	}
	if cfg.Lite.AllowDuplicateExternals {
		t.Fatal("duplicate liteserver externals should be disabled by default")
	}
	storageOpts := nodeOpts.Storage
	cellTotalCacheSize := storageOpts.CellTotalCacheSize
	if cellTotalCacheSize != DefaultCellTotalCache {
		t.Fatalf("unexpected cell total cache size %d", cellTotalCacheSize)
	}
	decodedCellCache := storageOpts.DecodedCellCache
	if decodedCellCache.Enabled != DefaultDecodedCellCacheEnabled {
		t.Fatalf("unexpected decoded cell cache enabled %v", decodedCellCache.Enabled)
	}
	if int64(decodedCellCache.Shards) != DefaultDecodedCellCacheShards {
		t.Fatalf("unexpected decoded cell cache shards %d", decodedCellCache.Shards)
	}
	if int64(decodedCellCache.Entries) != DefaultDecodedCellCacheEntries {
		t.Fatalf("unexpected decoded cell cache entries %d", decodedCellCache.Entries)
	}
	cellShardMemTableSize := storageOpts.CellShardMemTableSize
	if int64(cellShardMemTableSize) != DefaultCellShardMemTable {
		t.Fatalf("unexpected cell shard memtable size %d", cellShardMemTableSize)
	}
	cellMemTableStopWritesThreshold := storageOpts.CellMemTableStopWritesThreshold
	if int64(cellMemTableStopWritesThreshold) != DefaultCellMemTableStopWritesThreshold {
		t.Fatalf("unexpected cell memtable stop writes threshold %d", cellMemTableStopWritesThreshold)
	}
	largeBOCShardReadWorkers := storageOpts.LargeBOCShardReadWorkers
	if int64(largeBOCShardReadWorkers) != DefaultLargeBOCShardReadWorkers {
		t.Fatalf("unexpected large boc shard read workers %d", largeBOCShardReadWorkers)
	}
	persistentStateLargeBOCBatchSize := storageOpts.PersistentStateLargeBOCBatchSize
	if int64(persistentStateLargeBOCBatchSize) != DefaultPersistentStateLargeBOCBatchSize {
		t.Fatalf("unexpected persistent state large boc batch size %d", persistentStateLargeBOCBatchSize)
	}
	if storageOpts.PersistentStateKeepRecent != service.DefaultPersistentStateKeepRecent {
		t.Fatalf("unexpected persistent state keep recent %d", storageOpts.PersistentStateKeepRecent)
	}
	if cfg.Storage.StateSerializeOnePass {
		t.Fatal("state serialize one-pass should be disabled by default")
	}
	artifactFileMaxOpen := storageOpts.ArtifactFileMaxOpen
	if int64(artifactFileMaxOpen) != DefaultArtifactFileMaxOpen {
		t.Fatalf("unexpected artifact file max open %d", artifactFileMaxOpen)
	}
}

// Load parses with DisallowUnknownFields, so a config.json written by any
// earlier build — every one of which carried all three byte knobs — must still
// open. This is the whole reason the fields are kept as no-ops rather than
// deleted: deleting them would have made existing nodes refuse to start.
func TestLoadAcceptsDeprecatedDecodedCellCacheByteKnobs(t *testing.T) {
	path := writeTestConfig(t, `{
	  "storage": {
	    "cell_total_cache_size": 17179869184,
	    "decoded_cell_cache_enabled": true,
	    "decoded_cell_cache_shards": 64,
	    "decoded_cell_cache_bytes_per_entry": 16384,
	    "decoded_cell_cache_min_entries": 65536,
	    "decoded_cell_cache_max_entries": 1048576
	  }
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config with deprecated knobs: %v", err)
	}
	runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatalf("runtime options: %v", err)
	}

	// cell_total_cache_size keeps its meaning: it is the pebble block cache.
	if runtimeOpts.Node.Storage.CellTotalCacheSize != 16<<30 {
		t.Fatalf("pebble cell cache size = %d, want %d",
			runtimeOpts.Node.Storage.CellTotalCacheSize, int64(16<<30))
	}

	// The dead knobs size nothing. In particular min_entries = 65536 must NOT
	// clamp anything: it is a floor from a derivation that no longer happens,
	// and honouring it would override any smaller value an operator sets today.
	decoded := runtimeOpts.Node.Storage.DecodedCellCache
	if int64(decoded.Entries) != DefaultDecodedCellCacheEntries {
		t.Fatalf("entries = %d, want %d", decoded.Entries, DefaultDecodedCellCacheEntries)
	}

	// And nothing is warned about, because these are exactly the values the node
	// itself wrote. Every deployment on earth carries them, so a warning here
	// would fire for all of them and would say something untrue: that a setting
	// somebody chose has stopped working. The warning is for a choice, and this
	// config records no choice.
	if dead := DeprecatedDecodedCellCacheFields(cfg.Storage); len(dead) != 0 {
		t.Fatalf("the node's own stock values are reported as deprecated: %v", dead)
	}
}

// The same three knobs, at values no released build ever wrote. Somebody typed
// these, they no longer do anything, and that is the case the warning exists
// for.
func TestDeprecatedDecodedCellCacheKnobsWarnWhenTunedByHand(t *testing.T) {
	path := writeTestConfig(t, `{
	  "storage": {
	    "decoded_cell_cache_bytes_per_entry": 4096,
	    "decoded_cell_cache_min_entries": 131072,
	    "decoded_cell_cache_max_entries": 4194304
	  }
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config with hand-tuned dead knobs: %v", err)
	}
	dead := DeprecatedDecodedCellCacheFields(cfg.Storage)
	if len(dead) != 3 {
		t.Fatalf("deprecated fields reported = %v, want all three byte knobs", dead)
	}
}

// The decoded cache is bounded in entries because its cost is GC mark work over
// roughly ten live objects per entry, paid on every collection. When the
// derivation that used to size it was removed, the clamp that bounded the
// derivation went with it and the knob was left open at the top — so a config
// could put back exactly the object count the resize removed.
func TestDecodedCellCacheEntriesAreBoundedAbove(t *testing.T) {
	for _, field := range []string{"decoded_cell_cache_entries", "service_decoded_cell_cache_entries"} {
		path := writeTestConfig(t, `{"storage": {"`+field+`": 8388608}}`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("load config with an oversized %s: %v", field, err)
		}
		if _, err = cfg.RuntimeOptions(gton.DefaultNodeOptions()); err == nil {
			t.Fatalf("storage.%s accepted 8 Mi entries, %d times the ceiling",
				field, 8<<20/MaxDecodedCellCacheEntries)
		}
	}

	// The ceiling itself is usable.
	path := writeTestConfig(t, `{"storage": {"decoded_cell_cache_entries": 1048576}}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatalf("the ceiling itself was rejected: %v", err)
	}
	if int64(opts.Node.Storage.DecodedCellCache.Entries) != MaxDecodedCellCacheEntries {
		t.Fatalf("entries at the ceiling = %d, want %d",
			opts.Node.Storage.DecodedCellCache.Entries, MaxDecodedCellCacheEntries)
	}
}

// A config written by the two-cache build carries the service/operation pair.
// DisallowUnknownFields means both names must still parse, and — the part that
// matters to an operator who tuned it — the service value must still be applied
// rather than silently reverting to the default.
func TestLoadHonoursTheRenamedServiceEntriesKnob(t *testing.T) {
	path := writeTestConfig(t, `{
	  "storage": {
	    "decoded_cell_cache_enabled": true,
	    "decoded_cell_cache_shards": 64,
	    "service_decoded_cell_cache_entries": 4096,
	    "operation_decoded_cell_cache_entries": 1024
	  }
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config written by the two-cache build: %v", err)
	}
	runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatalf("runtime options: %v", err)
	}

	if got := runtimeOpts.Node.Storage.DecodedCellCache.Entries; got != 4096 {
		t.Fatalf("entries = %d, want the operator's 4096 carried over from the old name", got)
	}

	// The old name is reported as renamed-but-honoured, not as dead.
	renamed := RenamedDecodedCellCacheFields(cfg.Storage)
	if renamed["storage.service_decoded_cell_cache_entries"] != "storage.decoded_cell_cache_entries" {
		t.Fatalf("renamed fields = %v, want the service knob mapped to the new name", renamed)
	}
	// The operation knob really is dead: there is no second cache to size, and
	// folding its value into the single one would misstate the operator's intent.
	dead := DeprecatedDecodedCellCacheFields(cfg.Storage)
	if len(dead) != 1 || dead[0] != "storage.operation_decoded_cell_cache_entries" {
		t.Fatalf("deprecated fields = %v, want only the operation knob", dead)
	}
}

// When both names are present the current one wins and the old one is reported
// as ignored rather than honoured, so the two messages can never both be true.
func TestLoadPrefersTheCurrentEntriesKnobOverTheAlias(t *testing.T) {
	path := writeTestConfig(t, `{
	  "storage": {
	    "decoded_cell_cache_entries": 8192,
	    "service_decoded_cell_cache_entries": 4096
	  }
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatalf("runtime options: %v", err)
	}

	if got := runtimeOpts.Node.Storage.DecodedCellCache.Entries; got != 8192 {
		t.Fatalf("entries = %d, want the current knob's 8192", got)
	}
	if renamed := RenamedDecodedCellCacheFields(cfg.Storage); len(renamed) != 0 {
		t.Fatalf("renamed fields = %v, want none: the alias is ignored, not honoured", renamed)
	}
	dead := DeprecatedDecodedCellCacheFields(cfg.Storage)
	if len(dead) != 1 || dead[0] != "storage.service_decoded_cell_cache_entries" {
		t.Fatalf("deprecated fields = %v, want the shadowed alias", dead)
	}
}

// An operator who sets the current knob gets exactly what they asked for, and
// nothing is reported.
func TestLoadExplicitDecodedCellCacheEntries(t *testing.T) {
	path := writeTestConfig(t, `{
	  "storage": {
	    "decoded_cell_cache_entries": 4096
	  }
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
	if err != nil {
		t.Fatalf("runtime options: %v", err)
	}

	if got := runtimeOpts.Node.Storage.DecodedCellCache.Entries; got != 4096 {
		t.Fatalf("entries = %d, want 4096", got)
	}
	if len(DeprecatedDecodedCellCacheFields(cfg.Storage)) != 0 {
		t.Fatal("a config using only the current knob should report nothing deprecated")
	}
	if len(RenamedDecodedCellCacheFields(cfg.Storage)) != 0 {
		t.Fatal("a config using only the current knob should report nothing renamed")
	}
}

func TestLoadEnablesValidator(t *testing.T) {
	serverSeed := bytes.Repeat([]byte{0x11}, ed25519.SeedSize)
	clientID := bytes.Repeat([]byte{0x22}, 32)
	path := writeTestConfig(t, `{"validator":{"enabled":true,"control":{"listen_addr":"127.0.0.1:3030","key":"`+
		base64.StdEncoding.EncodeToString(serverSeed)+`","clients":[{"id":"`+
		base64.StdEncoding.EncodeToString(clientID)+`","permissions":15}]}}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Validator.Enabled ||
		!bytes.Equal(cfg.Validator.Control.Key, serverSeed) ||
		len(cfg.Validator.Control.Clients) != 1 ||
		!bytes.Equal(cfg.Validator.Control.Clients[0].ID, clientID) ||
		cfg.Validator.Control.Clients[0].Permissions != 15 {
		t.Fatal("validator was not enabled")
	}
}

func TestLoadRejectsValidatorKeysInConfig(t *testing.T) {
	path := writeTestConfig(t, `{"validator":{"enabled":true,"keys":[]}}`)

	if _, err := Load(path); err == nil {
		t.Fatal("validator signing keys in config were accepted")
	}
}

func TestLoadStandaloneCollatorAllowlist(t *testing.T) {
	id := bytes.Repeat([]byte{0x44}, 32)
	path := writeTestConfig(t, `{"collator":{"enabled":true,"validator_allowlist":{"enabled":true,"adnl_ids":["`+
		base64.StdEncoding.EncodeToString(id)+`"]}}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Collator.Enabled || !cfg.Collator.ValidatorAllowlist.Enabled ||
		len(cfg.Collator.ValidatorAllowlist.ADNLIDs) != 1 ||
		!bytes.Equal(cfg.Collator.ValidatorAllowlist.ADNLIDs[0], id) {
		t.Fatalf("unexpected collator config: %+v", cfg.Collator)
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
			"block_sender":true,
			"accept_queries":true
		}],
		"sender_shards":[{
			"workchain":0,
			"shard":-9223372036854775808
		}],
		"skip_public_msg_send":true,
		"use_quic":true,
		"send_queries":true
	}]}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.CustomOverlays) != 1 {
		t.Fatalf("unexpected custom overlay count %d", len(cfg.CustomOverlays))
	}
	overlay := cfg.CustomOverlays[0]
	if overlay.Name != "private-a" || !overlay.SkipPublicMsgSend || !overlay.UseQUIC || !overlay.SendQueries {
		t.Fatalf("unexpected custom overlay metadata: %+v", overlay)
	}
	if len(overlay.Nodes) != 1 {
		t.Fatalf("unexpected custom overlay node count %d", len(overlay.Nodes))
	}
	node := overlay.Nodes[0]
	if !bytes.Equal(node.ADNLID, nodeID) || !node.MsgSender || node.MsgSenderPriority != 7 ||
		!node.BlockSender || !node.AcceptQueries {
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
	path := writeTestConfig(t, `{"liteserver":{"send_message_broadcast_bytes_per_second":123456,"send_message_broadcast_max_delay_ms":75,"send_message_broadcast_fanout":15}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	capacity, err := liteSendMessageBroadcastCapacityFromConfig(cfg.Lite)
	if err != nil {
		t.Fatalf("liteserver send message broadcast capacity: %v", err)
	}
	if capacity.BytesPerSecond != 123456 {
		t.Fatalf("unexpected capacity %d", capacity.BytesPerSecond)
	}
	if capacity.MaxDelay != 75*time.Millisecond {
		t.Fatalf("unexpected max delay %s", capacity.MaxDelay)
	}
	fanout, err := liteSendMessageBroadcastFanoutFromConfig(cfg.Lite)
	if err != nil {
		t.Fatalf("liteserver send message broadcast fanout: %v", err)
	}
	if fanout != 15 {
		t.Fatalf("unexpected fanout %d", fanout)
	}
}

func TestLoadLiteAllowDuplicateExternals(t *testing.T) {
	path := writeTestConfig(t, `{"liteserver":{"allow_duplicate_externals":true}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Lite.AllowDuplicateExternals {
		t.Fatal("expected liteserver duplicate externals to be allowed")
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
			if _, err = liteSendMessageBroadcastCapacityFromConfig(cfg.Lite); err == nil {
				t.Fatal("expected negative capacity config to fail")
			}
		})
	}
}

func TestLoadLiteSendMessageBroadcastFanoutRejectsOutOfRange(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "negative",
			body: `{"liteserver":{"send_message_broadcast_fanout":-1}}`,
		},
		{
			name: "below minimum",
			body: `{"liteserver":{"send_message_broadcast_fanout":2}}`,
		},
		{
			name: "above maximum",
			body: `{"liteserver":{"send_message_broadcast_fanout":21}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestConfig(t, tt.body)

			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			if _, err = liteSendMessageBroadcastFanoutFromConfig(cfg.Lite); err == nil {
				t.Fatal("expected invalid liteserver send message broadcast fanout to fail")
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
	path := writeTestConfig(t, `{"ton":{"sync_before":7200,"sync_until":1719763200,"state_ttl":86400,"archive_ttl":172800,"next_checkpoint_blocks":700,"archive_checkpoint_blocks":2100,"checkpoint_bytes":123456789,"sync_backpressure_windows":6}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	nodeOpts := gton.DefaultNodeOptions()
	if err = applyTONOptionsFromConfig(&nodeOpts, cfg); err != nil {
		t.Fatalf("TON options: %v", err)
	}

	if nodeOpts.SyncBefore != 2*time.Hour {
		t.Fatalf("unexpected sync before %s", nodeOpts.SyncBefore)
	}
	if nodeOpts.SyncUntil != 1719763200 {
		t.Fatalf("unexpected sync until %d", nodeOpts.SyncUntil)
	}
	if nodeOpts.StateTTL != 24*time.Hour {
		t.Fatalf("unexpected state ttl %s", nodeOpts.StateTTL)
	}
	if nodeOpts.ArchiveTTL != 48*time.Hour {
		t.Fatalf("unexpected archive ttl %s", nodeOpts.ArchiveTTL)
	}
	if nodeOpts.NextCheckpointBlocks != 700 {
		t.Fatalf("unexpected next checkpoint blocks %d", nodeOpts.NextCheckpointBlocks)
	}
	if nodeOpts.ArchiveCheckpointBlocks != 2100 {
		t.Fatalf("unexpected archive checkpoint blocks %d", nodeOpts.ArchiveCheckpointBlocks)
	}
	if nodeOpts.CheckpointBytes != 123456789 {
		t.Fatalf("unexpected checkpoint bytes %d", nodeOpts.CheckpointBytes)
	}
	if nodeOpts.SyncBackpressureWindows != 6 {
		t.Fatalf("unexpected sync backpressure windows %d", nodeOpts.SyncBackpressureWindows)
	}
}

func TestLoadOldSyncOptionsUsesBackpressureDefault(t *testing.T) {
	path := writeTestConfig(t, `{"ton":{"sync_before":7200,"next_checkpoint_blocks":700,"checkpoint_bytes":123456789}}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	syncBackpressureWindows, err := uint32ConfigValue(
		"ton.sync_backpressure_windows",
		cfg.TON.SyncBackpressureWindows,
		uint32(DefaultSyncBackpressureWindows),
	)
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

	stateTTL, err := durationSeconds("ton.state_ttl", cfg.TON.StateTTL, true)
	if err != nil {
		t.Fatalf("state ttl: %v", err)
	}
	if stateTTL != 0 {
		t.Fatalf("unexpected state ttl %s", stateTTL)
	}

	archiveTTL, err := durationSeconds("ton.archive_ttl", cfg.TON.ArchiveTTL, true)
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
			"decoded_cell_cache_entries": 2000,
			"cell_shard_memtable_size": 1073741824,
			"cell_memtable_stop_writes_threshold": 3,
			"large_boc_shard_read_workers": 8,
			"persistent_state_large_boc_batch_size": 2097152,
			"persistent_state_keep_recent": 7,
			"state_serialize_one_pass": true,
			"artifact_file_max_open": 123
		}
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	storageOpts, err := storageOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("storage options: %v", err)
	}
	if storageOpts.Dir != "data/node" {
		t.Fatalf("unexpected storage dir %q", storageOpts.Dir)
	}
	if storageOpts.CellTotalCacheSize != 8<<30 {
		t.Fatalf("unexpected cell total cache size %d", storageOpts.CellTotalCacheSize)
	}
	decodedCellCache := storageOpts.DecodedCellCache
	if decodedCellCache.Enabled {
		t.Fatal("decoded cell cache should be disabled")
	}
	if decodedCellCache.Shards != 16 {
		t.Fatalf("unexpected decoded cell cache shards %d", decodedCellCache.Shards)
	}
	if decodedCellCache.Entries != 2000 {
		t.Fatalf("unexpected decoded cell cache entries %d", decodedCellCache.Entries)
	}
	if storageOpts.CellShardMemTableSize != 1<<30 {
		t.Fatalf("unexpected cell shard memtable size %d", storageOpts.CellShardMemTableSize)
	}
	if storageOpts.CellMemTableStopWritesThreshold != 3 {
		t.Fatalf("unexpected cell memtable stop writes threshold %d", storageOpts.CellMemTableStopWritesThreshold)
	}
	if storageOpts.LargeBOCShardReadWorkers != 8 {
		t.Fatalf("unexpected large boc shard read workers %d", storageOpts.LargeBOCShardReadWorkers)
	}
	if storageOpts.PersistentStateLargeBOCBatchSize != 2<<20 {
		t.Fatalf("unexpected persistent state large boc batch size %d", storageOpts.PersistentStateLargeBOCBatchSize)
	}
	if storageOpts.PersistentStateKeepRecent != 7 {
		t.Fatalf("unexpected persistent state keep recent %d", storageOpts.PersistentStateKeepRecent)
	}
	if !storageOpts.StateSerializeOnePass {
		t.Fatal("state serialize one-pass should be enabled")
	}
	if storageOpts.ArtifactFileMaxOpen != 123 {
		t.Fatalf("unexpected artifact file max open %d", storageOpts.ArtifactFileMaxOpen)
	}
}

// The record cache knob's three meanings, pinned separately because a JSON
// int64 usually cannot carry all of them: ABSENT takes the 4 GiB default (an
// existing config from before the knob gets the tier on upgrade), an explicit
// value is honoured, and an explicit ZERO is the off switch rather than "give
// me the default".
func TestCellRecordCacheBytesKnob(t *testing.T) {
	absent := writeTestConfig(t, `{"storage": {"dir": "data/node"}}`)
	cfg, err := Load(absent)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	storageOpts, err := storageOptionsFromConfig(cfg)
	if err != nil {
		t.Fatalf("storage options: %v", err)
	}
	if storageOpts.CellRecordCacheBytes != DefaultCellRecordCacheBytes {
		t.Fatalf("absent knob = %d, want the %d default", storageOpts.CellRecordCacheBytes, DefaultCellRecordCacheBytes)
	}

	explicit := writeTestConfig(t, `{"storage": {"dir": "data/node", "cell_record_cache_bytes": 1073741824}}`)
	if cfg, err = Load(explicit); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if storageOpts, err = storageOptionsFromConfig(cfg); err != nil {
		t.Fatalf("storage options: %v", err)
	}
	if storageOpts.CellRecordCacheBytes != 1<<30 {
		t.Fatalf("explicit knob = %d, want 1 GiB", storageOpts.CellRecordCacheBytes)
	}

	disabled := writeTestConfig(t, `{"storage": {"dir": "data/node", "cell_record_cache_bytes": 0}}`)
	if cfg, err = Load(disabled); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if storageOpts, err = storageOptionsFromConfig(cfg); err != nil {
		t.Fatalf("storage options: %v", err)
	}
	if storageOpts.CellRecordCacheBytes != 0 {
		t.Fatalf("explicit zero = %d, want 0 (disabled)", storageOpts.CellRecordCacheBytes)
	}

	negative := writeTestConfig(t, `{"storage": {"dir": "data/node", "cell_record_cache_bytes": -1}}`)
	if cfg, err = Load(negative); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if _, err = storageOptionsFromConfig(cfg); err == nil {
		t.Fatal("negative record cache bytes should be rejected")
	}

	absurd := writeTestConfig(t, `{"storage": {"dir": "data/node", "cell_record_cache_bytes": 1099511627777}}`)
	if cfg, err = Load(absurd); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if _, err = storageOptionsFromConfig(cfg); err == nil {
		t.Fatal("a >1 TiB record cache budget should be rejected as a typo")
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
			name: "negative entries",
			cfg:  Config{Storage: Storage{DecodedCellCacheEntries: -1}},
		},
		// The renamed alias and the dead operation knob are range-checked too,
		// so a negative left in an old config is reported rather than skipped.
		{
			name: "negative service entries alias",
			cfg:  Config{Storage: Storage{ServiceDecodedCellCacheEntries: -1}},
		},
		{
			name: "negative operation entries",
			cfg:  Config{Storage: Storage{OperationDecodedCellCacheEntries: -1}},
		},
		// The deprecated knobs no longer size anything, but a negative value is
		// still reported rather than quietly ignored.
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodedCellCacheOptionsFromConfig(tt.cfg.Storage); err == nil {
				t.Fatal("expected invalid decoded cell cache options to fail")
			}
		})
	}
}

func TestStorageLargeBOCOptionsRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		run  func(Config) error
		cfg  Config
	}{
		{
			name: "negative large boc shard read workers",
			run: func(cfg Config) error {
				_, err := intConfigValue(
					"storage.large_boc_shard_read_workers",
					cfg.Storage.LargeBOCShardReadWorkers,
					DefaultLargeBOCShardReadWorkers,
				)
				return err
			},
			cfg: Config{Storage: Storage{LargeBOCShardReadWorkers: -1}},
		},
		{
			name: "negative persistent state large boc batch size",
			run: func(cfg Config) error {
				_, err := intConfigValue(
					"storage.persistent_state_large_boc_batch_size",
					cfg.Storage.PersistentStateLargeBOCBatchSize,
					DefaultPersistentStateLargeBOCBatchSize,
				)
				return err
			},
			cfg: Config{Storage: Storage{PersistentStateLargeBOCBatchSize: -1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(tt.cfg); err == nil {
				t.Fatal("expected invalid storage large boc option to fail")
			}
		})
	}
}

func TestStorageLargeBOCOptionsUseDefaultsForZero(t *testing.T) {
	cfg := Config{Storage: Storage{
		LargeBOCShardReadWorkers:         0,
		PersistentStateLargeBOCBatchSize: 0,
	}}

	workers, err := intConfigValue(
		"storage.large_boc_shard_read_workers",
		cfg.Storage.LargeBOCShardReadWorkers,
		DefaultLargeBOCShardReadWorkers,
	)
	if err != nil {
		t.Fatalf("large boc shard read workers: %v", err)
	}
	if int64(workers) != DefaultLargeBOCShardReadWorkers {
		t.Fatalf("large boc shard read workers = %d, want %d", workers, DefaultLargeBOCShardReadWorkers)
	}

	batchSize, err := intConfigValue(
		"storage.persistent_state_large_boc_batch_size",
		cfg.Storage.PersistentStateLargeBOCBatchSize,
		DefaultPersistentStateLargeBOCBatchSize,
	)
	if err != nil {
		t.Fatalf("persistent state large boc batch size: %v", err)
	}
	if int64(batchSize) != DefaultPersistentStateLargeBOCBatchSize {
		t.Fatalf("persistent state large boc batch size = %d, want %d", batchSize, DefaultPersistentStateLargeBOCBatchSize)
	}
}

func TestStorageOptionsPersistentStateKeepRecent(t *testing.T) {
	tests := []struct {
		name    string
		value   int64
		want    int
		wantErr bool
	}{
		{name: "zero uses default", value: 0, want: service.DefaultPersistentStateKeepRecent},
		{name: "keep all", value: -1, want: service.PersistentStateKeepAll},
		{name: "positive", value: 7, want: 7},
		{name: "less than keep all", value: -2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := storageOptionsFromConfig(Config{Storage: Storage{PersistentStateKeepRecent: tt.value}})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected invalid persistent state keep recent to fail")
				}
				return
			}
			if err != nil {
				t.Fatalf("persistent state keep recent: %v", err)
			}
			if opts.PersistentStateKeepRecent != tt.want {
				t.Fatalf("persistent state keep recent = %d, want %d", opts.PersistentStateKeepRecent, tt.want)
			}
		})
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "logging",
			body: `{"logging":{"level":"debug"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestConfig(t, tt.body)

			if _, err := Load(path); err == nil {
				t.Fatal("expected unknown config field to fail")
			}
		})
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
	if len(cfg.Validator.Control.Key) != ed25519.SeedSize {
		t.Fatalf("generated validator control key has length %d", len(cfg.Validator.Control.Key))
	}
	if cfg.Validator.Control.ListenAddr != DefaultValidatorControlListen {
		t.Fatalf("generated validator control listen addr = %q", cfg.Validator.Control.ListenAddr)
	}
	if cfg.Validator.Control.Clients == nil || len(cfg.Validator.Control.Clients) != 0 {
		t.Fatalf("generated validator control clients = %#v, want empty list", cfg.Validator.Control.Clients)
	}
	if cfg.Validator.Enabled || cfg.Collator.Enabled {
		t.Fatal("generated validator and collator should be disabled")
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
	if cfg.Lite.SendMessageBroadcastFanout != DefaultLiteSendMessageBroadcastFanout {
		t.Fatalf("unexpected liteserver send message broadcast fanout %d", cfg.Lite.SendMessageBroadcastFanout)
	}
	if cfg.Lite.AllowDuplicateExternals {
		t.Fatal("expected generated liteserver duplicate externals to be disabled")
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
	if cfg.Storage.DecodedCellCacheEntries != DefaultDecodedCellCacheEntries {
		t.Fatalf("unexpected decoded cell cache entries %d", cfg.Storage.DecodedCellCacheEntries)
	}
	// A generated config must not carry the old names at all, so a fresh node
	// never emits a knob it would then have to warn about.
	if cfg.Storage.ServiceDecodedCellCacheEntries != 0 {
		t.Fatalf("generated config carries the renamed service knob: %d", cfg.Storage.ServiceDecodedCellCacheEntries)
	}
	if cfg.Storage.OperationDecodedCellCacheEntries != 0 {
		t.Fatalf("generated config carries the dead operation knob: %d", cfg.Storage.OperationDecodedCellCacheEntries)
	}
	if len(RenamedDecodedCellCacheFields(cfg.Storage)) != 0 {
		t.Fatalf("generated config carries renamed knobs: %v", RenamedDecodedCellCacheFields(cfg.Storage))
	}
	// A freshly generated config carries none of the dead knobs.
	if len(DeprecatedDecodedCellCacheFields(cfg.Storage)) != 0 {
		t.Fatalf("generated config carries deprecated knobs: %v", DeprecatedDecodedCellCacheFields(cfg.Storage))
	}
	if cfg.Storage.LargeBOCShardReadWorkers != DefaultLargeBOCShardReadWorkers {
		t.Fatalf("unexpected large boc shard read workers %d", cfg.Storage.LargeBOCShardReadWorkers)
	}
	if cfg.Storage.PersistentStateLargeBOCBatchSize != DefaultPersistentStateLargeBOCBatchSize {
		t.Fatalf("unexpected persistent state large boc batch size %d", cfg.Storage.PersistentStateLargeBOCBatchSize)
	}
	if cfg.Storage.PersistentStateKeepRecent != service.DefaultPersistentStateKeepRecent {
		t.Fatalf("unexpected persistent state keep recent %d", cfg.Storage.PersistentStateKeepRecent)
	}
	if cfg.Storage.StateSerializeOnePass {
		t.Fatal("state serialize one-pass should be disabled by default")
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
	if !bytes.Contains(data, []byte(`"sync_until"`)) {
		t.Fatal("generated config should use sync_until key")
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
	if !bytes.Contains(data, []byte(`"send_message_broadcast_fanout"`)) {
		t.Fatal("generated config should use send_message_broadcast_fanout key")
	}
	if !bytes.Contains(data, []byte(`"allow_duplicate_externals"`)) {
		t.Fatal("generated config should use allow_duplicate_externals key")
	}
	if !bytes.Contains(data, []byte(`"limits"`)) {
		t.Fatal("generated config should use liteserver limits key")
	}
	if !bytes.Contains(data, []byte(`"custom_overlays": []`)) {
		t.Fatal("generated config should use an empty custom_overlays list")
	}
	if !bytes.Contains(data, []byte(`"validator": {`)) ||
		!bytes.Contains(data, []byte(`"collator": {`)) {
		t.Fatal("generated config should contain validator and collator sections")
	}
	if bytes.Contains(data, []byte(`"enable_validator"`)) {
		t.Fatal("generated config should not contain the legacy enable_validator field")
	}
	if bytes.Contains(data, []byte(`"fast_sync_member_certificates"`)) {
		t.Fatal("generated config should not contain fast_sync_member_certificates")
	}
	if bytes.Contains(data, []byte(`"fast_sync_broadcast_speed_multiplier"`)) {
		t.Fatal("generated config should not contain fast_sync_broadcast_speed_multiplier")
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

	syncBefore, archiveFromZero, err := syncBeforeFromConfig(cfg.TON)
	if err != nil {
		t.Fatalf("archive-from-zero sync_before should be allowed: %v", err)
	}
	if syncBefore != 0 {
		t.Fatalf("unexpected archive-from-zero sync before %s", syncBefore)
	}
	if !archiveFromZero {
		t.Fatal("archive-from-zero mode should be enabled")
	}

	cfg.TON.SyncBefore = 0

	if _, _, err := syncBeforeFromConfig(cfg.TON); err == nil {
		t.Fatal("expected zero sync_before to fail")
	}

	cfg.TON.SyncBefore = -2
	if _, _, err := syncBeforeFromConfig(cfg.TON); err == nil {
		t.Fatal("expected negative sync_before other than -1 to fail")
	}
}

func TestSyncUntilValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.SyncUntil = 0

	syncUntil, err := syncUntilConfigValue(cfg.TON.SyncUntil)
	if err != nil {
		t.Fatalf("zero sync_until should be allowed: %v", err)
	}
	if syncUntil != 0 {
		t.Fatalf("unexpected sync_until %d", syncUntil)
	}

	cfg.TON.SyncUntil = -1
	if _, err = syncUntilConfigValue(cfg.TON.SyncUntil); err == nil {
		t.Fatal("expected negative sync_until to fail")
	}

	cfg.TON.SyncUntil = int64(^uint32(0)) + 1
	if _, err = syncUntilConfigValue(cfg.TON.SyncUntil); err == nil {
		t.Fatal("expected too large sync_until to fail")
	}
}

func TestSyncBackpressureWindowsValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.SyncBackpressureWindows = 0

	windows, err := uint32ConfigValue(
		"ton.sync_backpressure_windows",
		cfg.TON.SyncBackpressureWindows,
		uint32(DefaultSyncBackpressureWindows),
	)
	if err != nil {
		t.Fatalf("zero sync_backpressure_windows should use default: %v", err)
	}
	if int64(windows) != DefaultSyncBackpressureWindows {
		t.Fatalf("unexpected sync backpressure windows %d", windows)
	}

	cfg.TON.SyncBackpressureWindows = -1
	if _, err = uint32ConfigValue(
		"ton.sync_backpressure_windows",
		cfg.TON.SyncBackpressureWindows,
		uint32(DefaultSyncBackpressureWindows),
	); err == nil {
		t.Fatal("expected negative sync_backpressure_windows to fail")
	}
}

func TestStateTTLValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.StateTTL = 0

	stateTTL, err := durationSeconds("ton.state_ttl", cfg.TON.StateTTL, true)
	if err != nil {
		t.Fatalf("zero state_ttl should be allowed: %v", err)
	}
	if stateTTL != 0 {
		t.Fatalf("unexpected zero state ttl %s", stateTTL)
	}

	cfg.TON.StateTTL = -1
	if _, err = durationSeconds("ton.state_ttl", cfg.TON.StateTTL, true); err == nil {
		t.Fatal("expected negative state_ttl to fail")
	}
}

func TestArchiveTTLValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.TON.ArchiveTTL = 0

	archiveTTL, err := durationSeconds("ton.archive_ttl", cfg.TON.ArchiveTTL, true)
	if err != nil {
		t.Fatalf("zero archive_ttl should be allowed: %v", err)
	}
	if archiveTTL != 0 {
		t.Fatalf("unexpected zero archive ttl %s", archiveTTL)
	}

	cfg.TON.ArchiveTTL = -1
	if _, err = durationSeconds("ton.archive_ttl", cfg.TON.ArchiveTTL, true); err == nil {
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
	if err := downloadFile(context.Background(), client, path, "http://example.com/global.config.json"); err != nil {
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
	if err = downloadFile(context.Background(), client, path, "http://example.com/global.config.json"); err != nil {
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
