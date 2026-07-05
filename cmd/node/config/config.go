package config

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultPath                             = "config.json"
	DefaultGlobalConfigPath                 = "global.config.json"
	DefaultGlobalConfigURL                  = "https://ton-blockchain.github.io/global.config.json"
	DefaultSyncBefore                       = 6 * time.Hour
	ArchiveFromZeroSyncBefore               = int64(-1)
	DefaultStateTTL                         = 2 * 24 * time.Hour
	DefaultArchiveTTL                       = 7 * 24 * time.Hour
	DefaultNextCheckpointBlocks             = int64(400)
	DefaultArchiveCheckpointBlocks          = int64(2000)
	DefaultCheckpointBytes                  = int64(512 << 20)
	DefaultSyncBackpressureWindows          = int64(4)
	DefaultCellTotalCache                   = int64(8 << 30)
	DefaultDecodedCellCacheEnabled          = true
	DefaultDecodedCellCacheShards           = int64(64)
	DefaultDecodedCellCacheBytesPerEntry    = int64(16 << 10)
	DefaultDecodedCellCacheMinEntries       = int64(64 << 10)
	DefaultDecodedCellCacheMaxEntries       = int64(1 << 20)
	DefaultCellShardMemTable                = int64(256 << 20)
	DefaultCellMemTableStopWritesThreshold  = int64(4)
	DefaultLargeBOCShardReadWorkers         = int64(2)
	DefaultPersistentStateLargeBOCBatchSize = int64(512 << 10)
	DefaultArtifactFileMaxOpen              = int64(512)
	DefaultLiteMasterBlockCache             = 32
	DefaultLiteShardBlockCache              = 128
	DefaultLiteListen                       = "0.0.0.0:7445"
	DefaultLiteSendMessageBroadcastMaxDelay = 100 * time.Millisecond
	DefaultLiteSendMessageBroadcastFanout   = 5
	MinLiteSendMessageBroadcastFanout       = 3
	MaxLiteSendMessageBroadcastFanout       = 20
	DefaultMetricsNamespace                 = "gton"
	defaultStorageDir                       = "data"
	defaultADNLPort                         = 30303
	defaultADNLListen                       = "0.0.0.0:30303"
	defaultDHTListen                        = "0.0.0.0:30304"
	privateKeySeedSize                      = 32
	externalIPHTTPClient                    = 5 * time.Second
	globalConfigHTTPClient                  = 30 * time.Second
	ipAPILookupURL                          = "http://ip-api.com/json/?fields=status,message,query"
)

var ErrConfigMissingWithExistingStorage = errors.New("config file is missing while storage metadata exists")

type Config struct {
	TON                       TON             `json:"ton"`
	ADNL                      ADNL            `json:"adnl"`
	DHT                       DHT             `json:"dht"`
	Lite                      Lite            `json:"liteserver"`
	Storage                   Storage         `json:"storage"`
	Metrics                   Metrics         `json:"metrics"`
	CustomOverlays            []CustomOverlay `json:"custom_overlays"`
	DisableStateSerialization bool            `json:"disable_state_serialization"`
}

type TON struct {
	GlobalConfigPath string `json:"global_config_path"`
	SyncBefore       int64  `json:"sync_before"`
	SyncUntil        int64  `json:"sync_until"`
	StateTTL         int64  `json:"state_ttl"`
	ArchiveTTL       int64  `json:"archive_ttl"`

	NextCheckpointBlocks    int64 `json:"next_checkpoint_blocks"`
	ArchiveCheckpointBlocks int64 `json:"archive_checkpoint_blocks"`
	CheckpointBytes         int64 `json:"checkpoint_bytes"`
	SyncBackpressureWindows int64 `json:"sync_backpressure_windows"`
}

type ADNL struct {
	Key          []byte `json:"key"`
	ListenAddr   string `json:"listen_addr"`
	ExternalAddr string `json:"external_addr"`
}

type DHT struct {
	Key        []byte `json:"key"`
	ListenAddr string `json:"listen_addr"`
}

type Lite struct {
	Enabled                            bool       `json:"enabled"`
	NonFinalEnabled                    bool       `json:"non_final_enabled"`
	AllowDuplicateExternals            bool       `json:"allow_duplicate_externals"`
	Key                                []byte     `json:"key"`
	ListenAddr                         string     `json:"listen_addr"`
	MasterBlockCache                   int        `json:"master_block_cache"`
	ShardBlockCache                    int        `json:"shard_block_cache"`
	SendMessageBroadcastBytesPerSecond int64      `json:"send_message_broadcast_bytes_per_second"`
	SendMessageBroadcastMaxDelayMS     int64      `json:"send_message_broadcast_max_delay_ms"`
	SendMessageBroadcastFanout         int        `json:"send_message_broadcast_fanout"`
	Limits                             LiteLimits `json:"limits"`
}

type LiteLimits struct {
	CapacityPerIP       int64   `json:"capacity_per_ip"`
	CoolingPerSec       float64 `json:"cooling_per_sec"`
	MaxConnectionsPerIP int64   `json:"max_connections_per_ip"`
	MaxKeepAliveSeconds int64   `json:"max_keep_alive_seconds"`
	// MaxWaitsPerIP caps parked waitMasterchainSeqno requests per client
	// address; 0 applies the default of 256.
	MaxWaitsPerIP int64 `json:"max_waits_per_ip"`
}

type Storage struct {
	Dir                              string `json:"dir"`
	CellTotalCacheSize               int64  `json:"cell_total_cache_size"`
	DecodedCellCacheEnabled          bool   `json:"decoded_cell_cache_enabled"`
	DecodedCellCacheShards           int64  `json:"decoded_cell_cache_shards"`
	DecodedCellCacheBytesPerEntry    int64  `json:"decoded_cell_cache_bytes_per_entry"`
	DecodedCellCacheMinEntries       int64  `json:"decoded_cell_cache_min_entries"`
	DecodedCellCacheMaxEntries       int64  `json:"decoded_cell_cache_max_entries"`
	CellShardMemTableSize            int64  `json:"cell_shard_memtable_size"`
	CellMemTableStopWritesThreshold  int64  `json:"cell_memtable_stop_writes_threshold"`
	LargeBOCShardReadWorkers         int64  `json:"large_boc_shard_read_workers"`
	PersistentStateLargeBOCBatchSize int64  `json:"persistent_state_large_boc_batch_size"`
	StateSerializeOnePass            bool   `json:"state_serialize_one_pass"`
	ArtifactFileMaxOpen              int64  `json:"artifact_file_max_open"`
}

type DecodedCellCacheOptions struct {
	Enabled       bool
	Shards        int
	BytesPerEntry int64
	MinEntries    int
	MaxEntries    int
}

type LiteSendMessageBroadcastCapacity struct {
	BytesPerSecond int64
	MaxDelay       time.Duration
}

type Metrics struct {
	Enabled    bool   `json:"enabled"`
	ListenAddr string `json:"listen_addr"`
	Namespace  string `json:"namespace"`
}

type CustomOverlay struct {
	Name              string               `json:"name"`
	Nodes             []CustomOverlayNode  `json:"nodes"`
	SenderShards      []CustomOverlayShard `json:"sender_shards"`
	SkipPublicMsgSend bool                 `json:"skip_public_msg_send"`
}

type CustomOverlayNode struct {
	ADNLID            []byte `json:"adnl_id"`
	MsgSender         bool   `json:"msg_sender"`
	MsgSenderPriority int    `json:"msg_sender_priority"`
	BlockSender       bool   `json:"block_sender"`
}

type CustomOverlayShard struct {
	Workchain int32 `json:"workchain"`
	Shard     int64 `json:"shard"`
}

func defaultConfig() Config {
	return Config{
		TON: TON{
			GlobalConfigPath:        DefaultGlobalConfigPath,
			SyncBefore:              int64(DefaultSyncBefore / time.Second),
			StateTTL:                int64(DefaultStateTTL / time.Second),
			ArchiveTTL:              int64(DefaultArchiveTTL / time.Second),
			NextCheckpointBlocks:    DefaultNextCheckpointBlocks,
			ArchiveCheckpointBlocks: DefaultArchiveCheckpointBlocks,
			CheckpointBytes:         DefaultCheckpointBytes,
			SyncBackpressureWindows: DefaultSyncBackpressureWindows,
		},
		Lite: Lite{
			MasterBlockCache:               DefaultLiteMasterBlockCache,
			ShardBlockCache:                DefaultLiteShardBlockCache,
			SendMessageBroadcastMaxDelayMS: int64(DefaultLiteSendMessageBroadcastMaxDelay / time.Millisecond),
			SendMessageBroadcastFanout:     DefaultLiteSendMessageBroadcastFanout,
		},
		Storage: Storage{
			CellTotalCacheSize:               DefaultCellTotalCache,
			DecodedCellCacheEnabled:          DefaultDecodedCellCacheEnabled,
			DecodedCellCacheShards:           DefaultDecodedCellCacheShards,
			DecodedCellCacheBytesPerEntry:    DefaultDecodedCellCacheBytesPerEntry,
			DecodedCellCacheMinEntries:       DefaultDecodedCellCacheMinEntries,
			DecodedCellCacheMaxEntries:       DefaultDecodedCellCacheMaxEntries,
			CellShardMemTableSize:            DefaultCellShardMemTable,
			CellMemTableStopWritesThreshold:  DefaultCellMemTableStopWritesThreshold,
			LargeBOCShardReadWorkers:         DefaultLargeBOCShardReadWorkers,
			PersistentStateLargeBOCBatchSize: DefaultPersistentStateLargeBOCBatchSize,
			StateSerializeOnePass:            false,
			ArtifactFileMaxOpen:              DefaultArtifactFileMaxOpen,
		},
		Metrics: Metrics{
			Namespace: DefaultMetricsNamespace,
		},
		CustomOverlays: []CustomOverlay{},
	}
}

func generate(ctx context.Context, externalIPLookup func(context.Context) (string, error)) (Config, error) {
	adnlSeed, err := generateSeed()
	if err != nil {
		return Config{}, fmt.Errorf("generate ADNL key: %w", err)
	}

	dhtSeed, err := generateSeed()
	if err != nil {
		return Config{}, fmt.Errorf("generate DHT key: %w", err)
	}

	liteSeed, err := generateSeed()
	if err != nil {
		return Config{}, fmt.Errorf("generate liteserver key: %w", err)
	}

	externalIP, err := externalIPLookup(ctx)
	if err != nil {
		return Config{}, err
	}

	globalConfigPath, err := filepath.Abs(DefaultGlobalConfigPath)
	if err != nil {
		return Config{}, fmt.Errorf("resolve global config path: %w", err)
	}

	storageDir, err := filepath.Abs(defaultStorageDir)
	if err != nil {
		return Config{}, fmt.Errorf("resolve storage dir: %w", err)
	}

	cfg := defaultConfig()
	cfg.TON.GlobalConfigPath = globalConfigPath
	cfg.TON.NextCheckpointBlocks = DefaultNextCheckpointBlocks
	cfg.TON.ArchiveCheckpointBlocks = DefaultArchiveCheckpointBlocks
	cfg.TON.CheckpointBytes = DefaultCheckpointBytes
	cfg.TON.SyncBackpressureWindows = DefaultSyncBackpressureWindows
	cfg.ADNL = ADNL{
		Key:          adnlSeed,
		ListenAddr:   defaultADNLListen,
		ExternalAddr: net.JoinHostPort(externalIP, strconv.Itoa(defaultADNLPort)),
	}
	cfg.DHT = DHT{
		Key:        dhtSeed,
		ListenAddr: defaultDHTListen,
	}
	cfg.Lite = Lite{
		Enabled:                        false,
		Key:                            liteSeed,
		ListenAddr:                     DefaultLiteListen,
		MasterBlockCache:               DefaultLiteMasterBlockCache,
		ShardBlockCache:                DefaultLiteShardBlockCache,
		SendMessageBroadcastMaxDelayMS: int64(DefaultLiteSendMessageBroadcastMaxDelay / time.Millisecond),
		SendMessageBroadcastFanout:     DefaultLiteSendMessageBroadcastFanout,
	}
	cfg.Storage = Storage{
		Dir:                              storageDir,
		CellTotalCacheSize:               DefaultCellTotalCache,
		DecodedCellCacheEnabled:          DefaultDecodedCellCacheEnabled,
		DecodedCellCacheShards:           DefaultDecodedCellCacheShards,
		DecodedCellCacheBytesPerEntry:    DefaultDecodedCellCacheBytesPerEntry,
		DecodedCellCacheMinEntries:       DefaultDecodedCellCacheMinEntries,
		DecodedCellCacheMaxEntries:       DefaultDecodedCellCacheMaxEntries,
		CellShardMemTableSize:            DefaultCellShardMemTable,
		CellMemTableStopWritesThreshold:  DefaultCellMemTableStopWritesThreshold,
		LargeBOCShardReadWorkers:         DefaultLargeBOCShardReadWorkers,
		PersistentStateLargeBOCBatchSize: DefaultPersistentStateLargeBOCBatchSize,
		StateSerializeOnePass:            false,
		ArtifactFileMaxOpen:              DefaultArtifactFileMaxOpen,
	}

	return cfg, nil
}

func LoadOrCreate(
	ctx context.Context,
	path string,
	externalIPLookup func(context.Context) (string, error),
) (LoadOrCreateResult, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultPath
	}

	cfg, err := Load(path)
	if err == nil {
		return LoadOrCreateResult{Config: cfg}, nil
	}
	if !os.IsNotExist(err) {
		return LoadOrCreateResult{}, err
	}

	metadata, err := defaultStorageMetadata()
	if err != nil {
		return LoadOrCreateResult{}, err
	}
	if metadata.Exists {
		return LoadOrCreateResult{}, fmt.Errorf("%w: config %s was not found, storage metadata exists at %s",
			ErrConfigMissingWithExistingStorage, path, metadata.Path)
	}

	cfg, err = generate(ctx, externalIPLookup)
	if err != nil {
		return LoadOrCreateResult{}, fmt.Errorf("generate default config: %w", err)
	}

	if err = write(path, cfg); err != nil {
		return LoadOrCreateResult{}, err
	}

	return LoadOrCreateResult{Config: cfg, Created: true}, nil
}

type LoadOrCreateResult struct {
	Config  Config
	Created bool
}

func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultPath
	}

	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer func() {
		_ = file.Close()
	}()

	cfg := defaultConfig()
	dec := json.NewDecoder(file)
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}

	if err = dec.Decode(&struct{}{}); err != io.EOF {
		return Config{}, fmt.Errorf("parse %s: multiple JSON values", path)
	}

	return cfg, nil
}

func (cfg Config) StorageDir() string {
	return strings.TrimSpace(cfg.Storage.Dir)
}

func (cfg Config) CellTotalCacheSize() (int64, error) {
	if cfg.Storage.CellTotalCacheSize < 0 {
		return 0, fmt.Errorf("storage.cell_total_cache_size cannot be negative")
	}
	if cfg.Storage.CellTotalCacheSize == 0 {
		return DefaultCellTotalCache, nil
	}
	return cfg.Storage.CellTotalCacheSize, nil
}

func (cfg Config) DecodedCellCacheOptions() (DecodedCellCacheOptions, error) {
	opts := DecodedCellCacheOptions{
		Enabled:       cfg.Storage.DecodedCellCacheEnabled,
		BytesPerEntry: cfg.Storage.DecodedCellCacheBytesPerEntry,
	}
	if cfg.Storage.DecodedCellCacheShards < 0 {
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_shards cannot be negative")
	}
	if cfg.Storage.DecodedCellCacheBytesPerEntry < 0 {
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_bytes_per_entry cannot be negative")
	}
	if cfg.Storage.DecodedCellCacheMinEntries < 0 {
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_min_entries cannot be negative")
	}
	if cfg.Storage.DecodedCellCacheMaxEntries < 0 {
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_max_entries cannot be negative")
	}
	if cfg.Storage.DecodedCellCacheMinEntries > int64(int(^uint(0)>>1)) {
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_min_entries is too large")
	}
	if cfg.Storage.DecodedCellCacheMaxEntries > int64(int(^uint(0)>>1)) {
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_max_entries is too large")
	}
	if cfg.Storage.DecodedCellCacheShards > int64(int(^uint(0)>>1)) {
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_shards is too large")
	}
	opts.Shards = int(cfg.Storage.DecodedCellCacheShards)
	opts.MinEntries = int(cfg.Storage.DecodedCellCacheMinEntries)
	opts.MaxEntries = int(cfg.Storage.DecodedCellCacheMaxEntries)
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
		return DecodedCellCacheOptions{}, fmt.Errorf("storage.decoded_cell_cache_min_entries cannot exceed storage.decoded_cell_cache_max_entries")
	}
	return opts, nil
}

func (cfg Config) CellShardMemTableSize() (int, error) {
	if cfg.Storage.CellShardMemTableSize < 0 {
		return 0, fmt.Errorf("storage.cell_shard_memtable_size cannot be negative")
	}
	value := cfg.Storage.CellShardMemTableSize
	if value == 0 {
		value = DefaultCellShardMemTable
	}
	maxInt := int64(int(^uint(0) >> 1))
	if value > maxInt {
		return 0, fmt.Errorf("storage.cell_shard_memtable_size is too large")
	}
	return int(value), nil
}

func (cfg Config) CellMemTableStopWritesThreshold() (int, error) {
	if cfg.Storage.CellMemTableStopWritesThreshold < 0 {
		return 0, fmt.Errorf("storage.cell_memtable_stop_writes_threshold cannot be negative")
	}
	value := cfg.Storage.CellMemTableStopWritesThreshold
	if value == 0 {
		value = DefaultCellMemTableStopWritesThreshold
	}
	maxInt := int64(int(^uint(0) >> 1))
	if value > maxInt {
		return 0, fmt.Errorf("storage.cell_memtable_stop_writes_threshold is too large")
	}
	return int(value), nil
}

func (cfg Config) LargeBOCShardReadWorkers() (int, error) {
	if cfg.Storage.LargeBOCShardReadWorkers < 0 {
		return 0, fmt.Errorf("storage.large_boc_shard_read_workers cannot be negative")
	}
	value := cfg.Storage.LargeBOCShardReadWorkers
	if value == 0 {
		value = DefaultLargeBOCShardReadWorkers
	}
	maxInt := int64(int(^uint(0) >> 1))
	if value > maxInt {
		return 0, fmt.Errorf("storage.large_boc_shard_read_workers is too large")
	}
	return int(value), nil
}

func (cfg Config) PersistentStateLargeBOCBatchSize() (int, error) {
	if cfg.Storage.PersistentStateLargeBOCBatchSize < 0 {
		return 0, fmt.Errorf("storage.persistent_state_large_boc_batch_size cannot be negative")
	}
	value := cfg.Storage.PersistentStateLargeBOCBatchSize
	if value == 0 {
		value = DefaultPersistentStateLargeBOCBatchSize
	}
	maxInt := int64(int(^uint(0) >> 1))
	if value > maxInt {
		return 0, fmt.Errorf("storage.persistent_state_large_boc_batch_size is too large")
	}
	return int(value), nil
}

func (cfg Config) ArtifactFileMaxOpen() (int, error) {
	if cfg.Storage.ArtifactFileMaxOpen < 0 {
		return 0, fmt.Errorf("storage.artifact_file_max_open cannot be negative")
	}
	value := cfg.Storage.ArtifactFileMaxOpen
	if value == 0 {
		value = DefaultArtifactFileMaxOpen
	}
	maxInt := int64(int(^uint(0) >> 1))
	if value > maxInt {
		return 0, fmt.Errorf("storage.artifact_file_max_open is too large")
	}
	return int(value), nil
}

func (cfg Config) GlobalConfigPath() string {
	path := strings.TrimSpace(cfg.TON.GlobalConfigPath)
	if path == "" {
		return DefaultGlobalConfigPath
	}
	return path
}

func (cfg Config) SyncBefore() (time.Duration, error) {
	if cfg.ArchiveFromZero() {
		return 0, nil
	}
	if cfg.TON.SyncBefore <= 0 {
		return 0, fmt.Errorf("ton.sync_before should be positive seconds")
	}
	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if cfg.TON.SyncBefore > maxDurationSeconds {
		return 0, fmt.Errorf("ton.sync_before is too large")
	}
	return time.Duration(cfg.TON.SyncBefore) * time.Second, nil
}

func (cfg Config) SyncUntil() (uint32, error) {
	if cfg.TON.SyncUntil < 0 {
		return 0, fmt.Errorf("ton.sync_until cannot be negative")
	}
	if cfg.TON.SyncUntil == 0 {
		return 0, nil
	}
	if cfg.TON.SyncUntil > int64(^uint32(0)) {
		return 0, fmt.Errorf("ton.sync_until is too large")
	}
	return uint32(cfg.TON.SyncUntil), nil
}

func (cfg Config) ArchiveFromZero() bool {
	return cfg.TON.SyncBefore == ArchiveFromZeroSyncBefore
}

func (cfg Config) StateTTL() (time.Duration, error) {
	if cfg.TON.StateTTL < 0 {
		return 0, fmt.Errorf("ton.state_ttl cannot be negative")
	}
	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if cfg.TON.StateTTL > maxDurationSeconds {
		return 0, fmt.Errorf("ton.state_ttl is too large")
	}
	return time.Duration(cfg.TON.StateTTL) * time.Second, nil
}

func (cfg Config) ArchiveTTL() (time.Duration, error) {
	if cfg.TON.ArchiveTTL < 0 {
		return 0, fmt.Errorf("ton.archive_ttl cannot be negative")
	}
	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if cfg.TON.ArchiveTTL > maxDurationSeconds {
		return 0, fmt.Errorf("ton.archive_ttl is too large")
	}
	return time.Duration(cfg.TON.ArchiveTTL) * time.Second, nil
}

func (cfg Config) NextCheckpointBlocks() (uint32, error) {
	return uint32ConfigValue("ton.next_checkpoint_blocks", cfg.TON.NextCheckpointBlocks, uint32(DefaultNextCheckpointBlocks))
}

func (cfg Config) ArchiveCheckpointBlocks() (uint32, error) {
	return uint32ConfigValue("ton.archive_checkpoint_blocks", cfg.TON.ArchiveCheckpointBlocks, uint32(DefaultArchiveCheckpointBlocks))
}

func (cfg Config) CheckpointBytes() (uint64, error) {
	if cfg.TON.CheckpointBytes < 0 {
		return 0, fmt.Errorf("ton.checkpoint_bytes cannot be negative")
	}
	if cfg.TON.CheckpointBytes == 0 {
		return uint64(DefaultCheckpointBytes), nil
	}
	return uint64(cfg.TON.CheckpointBytes), nil
}

func (cfg Config) SyncBackpressureWindows() (uint32, error) {
	return uint32ConfigValue("ton.sync_backpressure_windows", cfg.TON.SyncBackpressureWindows, uint32(DefaultSyncBackpressureWindows))
}

func (cfg Config) LiteSendMessageBroadcastCapacity() (LiteSendMessageBroadcastCapacity, error) {
	if cfg.Lite.SendMessageBroadcastBytesPerSecond < 0 {
		return LiteSendMessageBroadcastCapacity{}, fmt.Errorf("liteserver.send_message_broadcast_bytes_per_second cannot be negative")
	}
	if cfg.Lite.SendMessageBroadcastMaxDelayMS < 0 {
		return LiteSendMessageBroadcastCapacity{}, fmt.Errorf("liteserver.send_message_broadcast_max_delay_ms cannot be negative")
	}

	delayMS := cfg.Lite.SendMessageBroadcastMaxDelayMS
	const maxDurationMilliseconds = int64(time.Duration(1<<63-1) / time.Millisecond)
	if delayMS > maxDurationMilliseconds {
		return LiteSendMessageBroadcastCapacity{}, fmt.Errorf("liteserver.send_message_broadcast_max_delay_ms is too large")
	}

	return LiteSendMessageBroadcastCapacity{
		BytesPerSecond: cfg.Lite.SendMessageBroadcastBytesPerSecond,
		MaxDelay:       time.Duration(delayMS) * time.Millisecond,
	}, nil
}

func (cfg Config) LiteSendMessageBroadcastFanout() (int, error) {
	fanout := cfg.Lite.SendMessageBroadcastFanout
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

func EnsureGlobalConfig(ctx context.Context, path string, url string, replace bool) (EnsureGlobalConfigResult, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = DefaultGlobalConfigPath
	}
	url = strings.TrimSpace(url)
	if url == "" {
		url = DefaultGlobalConfigURL
	}

	if !replace {
		if _, err := os.Stat(path); err == nil {
			return EnsureGlobalConfigResult{}, nil
		} else if !os.IsNotExist(err) {
			return EnsureGlobalConfigResult{}, err
		}
	}

	if err := downloadFile(ctx, path, url); err != nil {
		return EnsureGlobalConfigResult{}, err
	}
	return EnsureGlobalConfigResult{Downloaded: true}, nil
}

type EnsureGlobalConfigResult struct {
	Downloaded bool
}

type defaultStorageMetadataStatus struct {
	Exists bool
	Path   string
}

func defaultStorageMetadata() (defaultStorageMetadataStatus, error) {
	storageDir, err := filepath.Abs(defaultStorageDir)
	if err != nil {
		return defaultStorageMetadataStatus{}, fmt.Errorf("resolve storage dir: %w", err)
	}
	metadataPath := filepath.Join(storageDir, "metadb")
	_, err = os.Stat(metadataPath)
	if err == nil {
		return defaultStorageMetadataStatus{Exists: true, Path: metadataPath}, nil
	}
	if os.IsNotExist(err) {
		return defaultStorageMetadataStatus{Path: metadataPath}, nil
	}
	return defaultStorageMetadataStatus{Path: metadataPath}, fmt.Errorf("stat storage metadata %s: %w", metadataPath, err)
}

func write(path string, cfg Config) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create config dir: %w", err)
		}
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	data = append(data, '\n')

	if err = os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write config %s: %w", path, err)
	}
	return nil
}

func generateSeed() ([]byte, error) {
	seed := make([]byte, privateKeySeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	return seed, nil
}

func DetectExternalIP(ctx context.Context) (string, error) {
	client := http.Client{Timeout: externalIPHTTPClient}
	ip, err := lookupExternalIP(ctx, &client, ipAPILookupURL)
	if err != nil {
		return "", fmt.Errorf("lookup external ip: %w", err)
	}
	return ip, nil
}

func downloadFile(ctx context.Context, path string, url string) error {
	client := http.Client{Timeout: globalConfigHTTPClient}
	return downloadFileWithClient(ctx, &client, path, url)
}

func downloadFileWithClient(ctx context.Context, client *http.Client, path string, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %s", url, resp.Status)
	}

	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err = os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create global config dir: %w", err)
		}
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".global-config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp global config: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if _, err = io.Copy(tmp, resp.Body); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp global config: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temp global config: %w", err)
	}
	if err = os.Chmod(tmpPath, 0o644); err != nil {
		return fmt.Errorf("chmod temp global config: %w", err)
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace global config %s: %w", path, err)
	}
	return nil
}

func lookupExternalIP(ctx context.Context, client *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s returned %s", url, resp.Status)
	}

	var res struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Query   string `json:"query"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", fmt.Errorf("decode %s response: %w", url, err)
	}

	if res.Status != "success" {
		if res.Message == "" {
			res.Message = "unknown error"
		}
		return "", fmt.Errorf("%s failed: %s", url, res.Message)
	}

	ip := net.ParseIP(res.Query)
	if ip == nil {
		return "", fmt.Errorf("%s returned invalid ip %q", url, res.Query)
	}

	return ip.String(), nil
}
