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
	syncUntil, err := uint32ConfigValueAllowZero("ton.sync_until", cfg.TON.SyncUntil)
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
		CellShardMemTableSize:            cellShardMemTableSize,
		CellMemTableStopWritesThreshold:  cellMemTableStopWritesThreshold,
		LargeBOCShardReadWorkers:         largeBOCShardReadWorkers,
		PersistentStateLargeBOCBatchSize: persistentStateLargeBOCBatchSize,
		PersistentStateKeepRecent:        persistentStateKeepRecent,
		StateSerializeOnePass:            cfg.Storage.StateSerializeOnePass,
		ArtifactFileMaxOpen:              artifactFileMaxOpen,
	}, nil
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

func decodedCellCacheOptionsFromConfig(cfg Storage) (gton.DecodedCellCacheOptions, error) {
	opts := gton.DecodedCellCacheOptions{
		Enabled:       cfg.DecodedCellCacheEnabled,
		BytesPerEntry: cfg.DecodedCellCacheBytesPerEntry,
	}
	if cfg.DecodedCellCacheShards < 0 {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_shards cannot be negative")
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
	if cfg.DecodedCellCacheMinEntries > int64(int(^uint(0)>>1)) {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_min_entries is too large")
	}
	if cfg.DecodedCellCacheMaxEntries > int64(int(^uint(0)>>1)) {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_max_entries is too large")
	}
	if cfg.DecodedCellCacheShards > int64(int(^uint(0)>>1)) {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_shards is too large")
	}

	opts.Shards = int(cfg.DecodedCellCacheShards)
	opts.MinEntries = int(cfg.DecodedCellCacheMinEntries)
	opts.MaxEntries = int(cfg.DecodedCellCacheMaxEntries)
	if opts.Shards == 0 {
		opts.Shards = int(DefaultDecodedCellCacheShards)
	}
	if opts.BytesPerEntry == 0 {
		opts.BytesPerEntry = DefaultDecodedCellCacheBytesPerEntry
	}
	if opts.MinEntries == 0 {
		opts.MinEntries = int(DefaultDecodedCellCacheMinEntries)
	}
	if opts.MaxEntries == 0 {
		opts.MaxEntries = int(DefaultDecodedCellCacheMaxEntries)
	}
	if opts.MinEntries > opts.MaxEntries {
		return gton.DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_min_entries cannot exceed storage.decoded_cell_cache_max_entries")
	}
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

func uint32ConfigValueAllowZero(field string, value int64) (uint32, error) {
	if value < 0 {
		return 0, fmt.Errorf("%s cannot be negative", field)
	}
	if value > int64(^uint32(0)) {
		return 0, fmt.Errorf("%s is too large", field)
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
