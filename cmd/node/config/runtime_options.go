package config

import (
	"crypto/ed25519"
	"fmt"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/gton"
	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/p2p"
)

var metricsNamespacePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type RuntimeOptions struct {
	Node                             gton.NodeOptions
	GlobalConfigPath                 string
	LiteSendMessageBroadcastCapacity LiteSendMessageBroadcastCapacity
	LiteSendMessageBroadcastFanout   int
	HTTPAPI                          HTTPAPIOptions
}

func (cfg Config) RuntimeOptions(nodeOpts gton.NodeOptions) (RuntimeOptions, error) {
	p2pOpts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		return RuntimeOptions{}, err
	}
	nodeOpts.P2P = p2pOpts

	metricsOpts, err := metricsOptionsFromConfig(cfg)
	if err != nil {
		return RuntimeOptions{}, err
	}
	nodeOpts.Metrics = metricsOpts

	storageOpts, err := storageOptionsFromConfig(cfg)
	if err != nil {
		return RuntimeOptions{}, err
	}
	nodeOpts.Storage = storageOpts

	if err = applyTONOptionsFromConfig(&nodeOpts, cfg); err != nil {
		return RuntimeOptions{}, err
	}

	liteSendCapacity, err := liteSendMessageBroadcastCapacityFromConfig(cfg.Lite)
	if err != nil {
		return RuntimeOptions{}, err
	}
	liteSendFanout, err := liteSendMessageBroadcastFanoutFromConfig(cfg.Lite)
	if err != nil {
		return RuntimeOptions{}, err
	}

	httpAPIOpts, err := httpapiOptionsFromConfig(cfg.HTTPAPI)
	if err != nil {
		return RuntimeOptions{}, err
	}

	nodeOpts.DisableStateSerialization = cfg.DisableStateSerialization

	return RuntimeOptions{
		Node:                             nodeOpts,
		GlobalConfigPath:                 globalConfigPath(cfg.TON),
		LiteSendMessageBroadcastCapacity: liteSendCapacity,
		LiteSendMessageBroadcastFanout:   liteSendFanout,
		HTTPAPI:                          httpAPIOpts,
	}, nil
}

func applyTONOptionsFromConfig(nodeOpts *gton.NodeOptions, cfg Config) error {
	syncBefore, archiveFromZero, err := syncBeforeFromConfig(cfg.TON)
	if err != nil {
		return err
	}
	syncUntil, err := syncUntilConfigValue(cfg.TON.SyncUntil)
	if err != nil {
		return err
	}
	stateTTL, err := durationSeconds("ton.state_ttl", cfg.TON.StateTTL, true)
	if err != nil {
		return err
	}
	archiveTTL, err := durationSeconds("ton.archive_ttl", cfg.TON.ArchiveTTL, true)
	if err != nil {
		return err
	}
	nextCheckpointBlocks, err := uint32ConfigValue("ton.next_checkpoint_blocks", cfg.TON.NextCheckpointBlocks, uint32(DefaultNextCheckpointBlocks))
	if err != nil {
		return err
	}
	archiveCheckpointBlocks, err := uint32ConfigValue("ton.archive_checkpoint_blocks", cfg.TON.ArchiveCheckpointBlocks, uint32(DefaultArchiveCheckpointBlocks))
	if err != nil {
		return err
	}
	checkpointBytes, err := checkpointBytesFromConfig(cfg.TON)
	if err != nil {
		return err
	}
	syncBackpressureWindows, err := uint32ConfigValue("ton.sync_backpressure_windows", cfg.TON.SyncBackpressureWindows, uint32(DefaultSyncBackpressureWindows))
	if err != nil {
		return err
	}

	nodeOpts.SyncBefore = syncBefore
	nodeOpts.SyncUntil = syncUntil
	nodeOpts.ArchiveFromZero = archiveFromZero
	nodeOpts.StateTTL = stateTTL
	nodeOpts.ArchiveTTL = archiveTTL
	nodeOpts.NextCheckpointBlocks = nextCheckpointBlocks
	nodeOpts.ArchiveCheckpointBlocks = archiveCheckpointBlocks
	nodeOpts.CheckpointBytes = checkpointBytes
	nodeOpts.SyncBackpressureWindows = syncBackpressureWindows
	return nil
}

func storageOptionsFromConfig(cfg Config) (gton.StorageOptions, error) {
	cellTotalCacheSize, err := cellTotalCacheSizeFromConfig(cfg.Storage)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	decodedCellCacheOpts, err := decodedCellCacheOptionsFromConfig(cfg.Storage)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	cellRecordCacheBytes, err := cellRecordCacheBytesFromConfig(cfg.Storage)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	cellShardMemTableSize, err := intConfigValue("storage.cell_shard_memtable_size", cfg.Storage.CellShardMemTableSize, DefaultCellShardMemTable)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	cellMemTableStopWritesThreshold, err := intConfigValue("storage.cell_memtable_stop_writes_threshold", cfg.Storage.CellMemTableStopWritesThreshold, DefaultCellMemTableStopWritesThreshold)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	largeBOCShardReadWorkers, err := intConfigValue("storage.large_boc_shard_read_workers", cfg.Storage.LargeBOCShardReadWorkers, DefaultLargeBOCShardReadWorkers)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	persistentStateLargeBOCBatchSize, err := intConfigValue("storage.persistent_state_large_boc_batch_size", cfg.Storage.PersistentStateLargeBOCBatchSize, DefaultPersistentStateLargeBOCBatchSize)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	persistentStateKeepRecent, err := persistentStateKeepRecentFromConfig(cfg.Storage.PersistentStateKeepRecent)
	if err != nil {
		return gton.StorageOptions{}, err
	}
	artifactFileMaxOpen, err := intConfigValue("storage.artifact_file_max_open", cfg.Storage.ArtifactFileMaxOpen, DefaultArtifactFileMaxOpen)
	if err != nil {
		return gton.StorageOptions{}, err
	}

	return gton.StorageOptions{
		Dir:                              strings.TrimSpace(cfg.Storage.Dir),
		CellTotalCacheSize:               cellTotalCacheSize,
		DecodedCellCache:                 decodedCellCacheOpts,
		CellRecordCacheBytes:             cellRecordCacheBytes,
		CellShardMemTableSize:            cellShardMemTableSize,
		CellMemTableStopWritesThreshold:  cellMemTableStopWritesThreshold,
		LargeBOCShardReadWorkers:         largeBOCShardReadWorkers,
		PersistentStateLargeBOCBatchSize: persistentStateLargeBOCBatchSize,
		PersistentStateKeepRecent:        persistentStateKeepRecent,
		StateSerializeOnePass:            cfg.Storage.StateSerializeOnePass,
		ArtifactFileMaxOpen:              artifactFileMaxOpen,
	}, nil
}

// cellRecordCacheBytesFromConfig validates the record cache budget. It is NOT
// run through intConfigValue on purpose: there zero means "take the default",
// here zero is the OFF switch (the default rides in via defaultConfig's
// prefill), so zero must pass through untouched. A positive dust value is
// clamped up to a workable ring by pebblestore, not here — the store logs the
// effective figure it built.
func cellRecordCacheBytesFromConfig(cfg Storage) (int64, error) {
	if cfg.CellRecordCacheBytes < 0 {
		return 0, fmt.Errorf("storage.cell_record_cache_bytes cannot be negative; use 0 to disable the record cache")
	}
	if cfg.CellRecordCacheBytes > MaxCellRecordCacheBytes {
		return 0, fmt.Errorf("storage.cell_record_cache_bytes cannot exceed %d bytes (1 TiB): the record cache arena is off-GC memory with nothing else pushing back on a typo", MaxCellRecordCacheBytes)
	}
	return cfg.CellRecordCacheBytes, nil
}

func persistentStateKeepRecentFromConfig(value int64) (int, error) {
	if value == service.PersistentStateKeepAll {
		return service.PersistentStateKeepAll, nil
	}

	return intConfigValue("storage.persistent_state_keep_recent", value, service.DefaultPersistentStateKeepRecent)
}

func globalConfigPath(cfg TON) string {
	path := strings.TrimSpace(cfg.GlobalConfigPath)
	if path == "" {
		return DefaultGlobalConfigPath
	}
	return path
}

func syncBeforeFromConfig(cfg TON) (time.Duration, bool, error) {
	if cfg.SyncBefore == ArchiveFromZeroSyncBefore {
		return 0, true, nil
	}
	if cfg.SyncBefore <= 0 {
		return 0, false, fmt.Errorf("ton.sync_before should be positive seconds")
	}
	d, err := durationSeconds("ton.sync_before", cfg.SyncBefore, false)
	if err != nil {
		return 0, false, err
	}
	return d, false, nil
}

func checkpointBytesFromConfig(cfg TON) (uint64, error) {
	if cfg.CheckpointBytes < 0 {
		return 0, fmt.Errorf("ton.checkpoint_bytes cannot be negative")
	}
	if cfg.CheckpointBytes == 0 {
		return uint64(DefaultCheckpointBytes), nil
	}
	return uint64(cfg.CheckpointBytes), nil
}

func durationSeconds(field string, seconds int64, allowZero bool) (time.Duration, error) {
	if seconds < 0 {
		return 0, fmt.Errorf("%s cannot be negative", field)
	}
	if seconds == 0 && !allowZero {
		return 0, fmt.Errorf("%s should be positive seconds", field)
	}

	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if seconds > maxDurationSeconds {
		return 0, fmt.Errorf("%s is too large", field)
	}
	return time.Duration(seconds) * time.Second, nil
}

func cellTotalCacheSizeFromConfig(cfg Storage) (int64, error) {
	if cfg.CellTotalCacheSize < 0 {
		return 0, fmt.Errorf("storage.cell_total_cache_size cannot be negative")
	}
	if cfg.CellTotalCacheSize == 0 {
		return DefaultCellTotalCache, nil
	}
	return cfg.CellTotalCacheSize, nil
}

// DeprecatedDecodedCellCacheFields lists the storage knobs that are still
// accepted on disk but no longer affect anything. Callers report them so an
// operator who tuned one is told it is dead rather than left guessing.
//
// storage.service_decoded_cell_cache_entries is NOT in this list when it is the
// only entry count set: it is honoured as the former name of
// storage.decoded_cell_cache_entries. It appears here only when the current name
// is also set, in which case the current name wins and the old one really is
// ignored. See RenamedDecodedCellCacheFields for the honoured-but-renamed case.
//
// A field holding exactly the value the node's own earlier releases wrote is
// also not in this list. Those three keys were emitted into every generated
// config.json with the defaults below, so nobody chose them; warning about them
// would fire on every existing deployment on upgrade and would mean the
// opposite of what the message says — it would tell an operator that something
// they never set has stopped doing something they never asked for, and it would
// bury the case the warning exists for. Any other value at those keys was typed
// by a person and is still reported.
func DeprecatedDecodedCellCacheFields(cfg Storage) []string {
	var dead []string
	if cfg.ServiceDecodedCellCacheEntries != 0 && cfg.DecodedCellCacheEntries != 0 {
		dead = append(dead, "storage.service_decoded_cell_cache_entries")
	}
	if cfg.OperationDecodedCellCacheEntries != 0 {
		dead = append(dead, "storage.operation_decoded_cell_cache_entries")
	}
	if tunedByHand(cfg.DecodedCellCacheBytesPerEntry, formerDecodedCellCacheBytesPerEntry) {
		dead = append(dead, "storage.decoded_cell_cache_bytes_per_entry")
	}
	if tunedByHand(cfg.DecodedCellCacheMinEntries, formerDecodedCellCacheMinEntries) {
		dead = append(dead, "storage.decoded_cell_cache_min_entries")
	}
	if tunedByHand(cfg.DecodedCellCacheMaxEntries, formerDecodedCellCacheMaxEntries) {
		dead = append(dead, "storage.decoded_cell_cache_max_entries")
	}
	return dead
}

// The stock values every released version of this node wrote into a generated
// config.json for the three knobs that sized the derivation. They are recorded
// here and nowhere else: nothing reads them, and their only purpose is to tell
// a value the node emitted apart from one an operator chose.
const (
	formerDecodedCellCacheBytesPerEntry = int64(16 << 10)
	formerDecodedCellCacheMinEntries    = int64(64 << 10)
	formerDecodedCellCacheMaxEntries    = int64(1 << 20)
)

func tunedByHand(value, stock int64) bool { return value != 0 && value != stock }

// RenamedDecodedCellCacheFields lists knobs whose value IS being used but under
// an old name, so startup can tell the operator to rename them rather than
// warning that they do nothing. A field is never in both this list and
// DeprecatedDecodedCellCacheFields.
func RenamedDecodedCellCacheFields(cfg Storage) map[string]string {
	renamed := map[string]string{}
	if cfg.ServiceDecodedCellCacheEntries != 0 && cfg.DecodedCellCacheEntries == 0 {
		renamed["storage.service_decoded_cell_cache_entries"] = "storage.decoded_cell_cache_entries"
	}
	if len(renamed) == 0 {
		return nil
	}
	return renamed
}

func decodedCellCacheOptionsFromConfig(cfg Storage) (gton.DecodedCellCacheOptions, error) {
	opts := gton.DecodedCellCacheOptions{Enabled: cfg.DecodedCellCacheEnabled}

	shards, err := intConfigValue("storage.decoded_cell_cache_shards", cfg.DecodedCellCacheShards, DefaultDecodedCellCacheShards)
	if err != nil {
		return gton.DecodedCellCacheOptions{}, err
	}

	// The alias is range-checked whether or not it is the value that wins, so a
	// negative left over from an old config is still reported rather than
	// silently skipped.
	if cfg.ServiceDecodedCellCacheEntries < 0 {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.service_decoded_cell_cache_entries cannot be negative")
	}
	entriesField, entriesValue := "storage.decoded_cell_cache_entries", cfg.DecodedCellCacheEntries
	if entriesValue == 0 && cfg.ServiceDecodedCellCacheEntries != 0 {
		entriesField, entriesValue = "storage.service_decoded_cell_cache_entries", cfg.ServiceDecodedCellCacheEntries
	}
	if entriesValue > MaxDecodedCellCacheEntries {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf(
			"%s cannot exceed %d entries: the decoded cache is bounded in entries because its cost "+
				"is GC mark work over roughly ten live objects each, paid on every collection",
			entriesField, MaxDecodedCellCacheEntries)
	}
	entries, err := intConfigValue(entriesField, entriesValue, DefaultDecodedCellCacheEntries)
	if err != nil {
		return gton.DecodedCellCacheOptions{}, err
	}

	// The deprecated knobs no longer size anything, but a negative value is
	// still a mistake worth reporting rather than silently ignoring.
	if cfg.OperationDecodedCellCacheEntries < 0 {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.operation_decoded_cell_cache_entries cannot be negative")
	}
	if cfg.DecodedCellCacheBytesPerEntry < 0 {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_bytes_per_entry cannot be negative")
	}
	if cfg.DecodedCellCacheMinEntries < 0 {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_min_entries cannot be negative")
	}
	if cfg.DecodedCellCacheMaxEntries < 0 {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_max_entries cannot be negative")
	}

	opts.Shards = shards
	opts.Entries = entries
	return opts, nil
}

func p2pOptionsFromConfig(cfg Config) (p2p.Options, error) {
	opts := p2p.Options{
		ListenAddr:    strings.TrimSpace(cfg.ADNL.ListenAddr),
		DHTListenAddr: strings.TrimSpace(cfg.DHT.ListenAddr),
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

	opts.CustomOverlays, err = customOverlaysFromConfig(cfg.CustomOverlays)
	if err != nil {
		return p2p.Options{}, err
	}

	return opts, nil
}

func liteSendMessageBroadcastCapacityFromConfig(cfg Lite) (LiteSendMessageBroadcastCapacity, error) {
	if cfg.SendMessageBroadcastBytesPerSecond < 0 {
		return LiteSendMessageBroadcastCapacity{}, fmt.Errorf("liteserver.send_message_broadcast_bytes_per_second cannot be negative")
	}
	if cfg.SendMessageBroadcastMaxDelayMS < 0 {
		return LiteSendMessageBroadcastCapacity{}, fmt.Errorf("liteserver.send_message_broadcast_max_delay_ms cannot be negative")
	}

	delayMS := cfg.SendMessageBroadcastMaxDelayMS
	const maxDurationMilliseconds = int64(time.Duration(1<<63-1) / time.Millisecond)
	if delayMS > maxDurationMilliseconds {
		return LiteSendMessageBroadcastCapacity{}, fmt.Errorf("liteserver.send_message_broadcast_max_delay_ms is too large")
	}

	return LiteSendMessageBroadcastCapacity{
		BytesPerSecond: cfg.SendMessageBroadcastBytesPerSecond,
		MaxDelay:       time.Duration(delayMS) * time.Millisecond,
	}, nil
}

func liteSendMessageBroadcastFanoutFromConfig(cfg Lite) (int, error) {
	fanout := cfg.SendMessageBroadcastFanout
	if fanout == 0 {
		return DefaultLiteSendMessageBroadcastFanout, nil
	}
	if fanout < MinLiteSendMessageBroadcastFanout {
		return 0, fmt.Errorf("liteserver.send_message_broadcast_fanout cannot be less than %d", MinLiteSendMessageBroadcastFanout)
	}
	if fanout > MaxLiteSendMessageBroadcastFanout {
		return 0, fmt.Errorf("liteserver.send_message_broadcast_fanout cannot exceed %d", MaxLiteSendMessageBroadcastFanout)
	}
	return fanout, nil
}

func httpapiOptionsFromConfig(cfg HTTPAPI) (HTTPAPIOptions, error) {
	timeoutSeconds := cfg.RequestTimeoutSeconds
	if timeoutSeconds == 0 {
		timeoutSeconds = int64(DefaultHTTPAPIRequestTimeout / time.Second)
	}
	if timeoutSeconds < 0 {
		return HTTPAPIOptions{}, fmt.Errorf("http_api.request_timeout_seconds cannot be negative")
	}
	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if timeoutSeconds > maxDurationSeconds {
		return HTTPAPIOptions{}, fmt.Errorf("http_api.request_timeout_seconds is too large")
	}

	return HTTPAPIOptions{
		RequestTimeout: time.Duration(timeoutSeconds) * time.Second,
	}, nil
}

func intConfigValue(field string, value int64, defaultValue int64) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s cannot be negative", field)
	}
	if value == 0 {
		value = defaultValue
	}
	if value > math.MaxInt {
		return 0, fmt.Errorf("%s is too large", field)
	}
	return int(value), nil
}

func uint32ConfigValue(field string, value int64, defaultValue uint32) (uint32, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s cannot be negative", field)
	}
	if value == 0 {
		return defaultValue, nil
	}
	if value > int64(^uint32(0)) {
		return 0, fmt.Errorf("%s is too large", field)
	}
	return uint32(value), nil
}

func syncUntilConfigValue(value int64) (uint32, error) {
	if value < 0 {
		return 0, fmt.Errorf("ton.sync_until cannot be negative")
	}
	if value > int64(^uint32(0)) {
		return 0, fmt.Errorf("ton.sync_until is too large")
	}
	return uint32(value), nil
}

func customOverlaysFromConfig(raw []CustomOverlay) ([]p2p.CustomOverlayConfig, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	seen := map[string]struct{}{}
	overlays := make([]p2p.CustomOverlayConfig, 0, len(raw))
	for idx, overlay := range raw {
		name := strings.TrimSpace(overlay.Name)
		if name == "" {
			return nil, fmt.Errorf("custom_overlays[%d].name is empty", idx)
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate custom overlay name %q", name)
		}
		seen[name] = struct{}{}

		if len(overlay.Nodes) == 0 {
			return nil, fmt.Errorf("custom_overlays[%d].nodes is empty", idx)
		}

		nodes := make([]p2p.CustomOverlayNodeConfig, 0, len(overlay.Nodes))
		for nodeIdx, node := range overlay.Nodes {
			adnlID, err := p2p.NewPeerID(node.ADNLID)
			if err != nil {
				return nil, fmt.Errorf("custom_overlays[%d].nodes[%d].adnl_id: %w", idx, nodeIdx, err)
			}
			nodes = append(nodes, p2p.CustomOverlayNodeConfig{
				ADNLID:            adnlID,
				MsgSender:         node.MsgSender,
				MsgSenderPriority: node.MsgSenderPriority,
				BlockSender:       node.BlockSender,
				AcceptQueries:     node.AcceptQueries,
			})
		}

		shards := make([]p2p.CustomOverlayShard, 0, len(overlay.SenderShards))
		for _, shard := range overlay.SenderShards {
			shards = append(shards, p2p.CustomOverlayShard{
				Workchain: shard.Workchain,
				Shard:     shard.Shard,
			})
		}

		overlays = append(overlays, p2p.CustomOverlayConfig{
			Name:              name,
			Nodes:             nodes,
			SenderShards:      shards,
			SkipPublicMsgSend: overlay.SkipPublicMsgSend,
			UseQUIC:           overlay.UseQUIC,
			SendQueries:       overlay.SendQueries,
		})
	}
	return overlays, nil
}

func metricsOptionsFromConfig(cfg Config) (gton.MetricsOptions, error) {
	namespace := strings.TrimSpace(cfg.Metrics.Namespace)
	if namespace == "" {
		namespace = DefaultMetricsNamespace
	}
	if !metricsNamespacePattern.MatchString(namespace) {
		return gton.MetricsOptions{}, fmt.Errorf("metrics.namespace must match [A-Za-z_][A-Za-z0-9_]*")
	}

	opts := gton.MetricsOptions{
		Enabled:    cfg.Metrics.Enabled,
		ListenAddr: strings.TrimSpace(cfg.Metrics.ListenAddr),
		Namespace:  namespace,
	}
	if opts.Enabled && opts.ListenAddr == "" {
		return gton.MetricsOptions{}, fmt.Errorf("metrics.listen_addr is required when metrics.enabled is true")
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
