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

	"github.com/xssnick/gton/service"
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
	DefaultHTTPAPIListen                    = "0.0.0.0:8081"
	DefaultHTTPAPIRequestTimeout            = 10 * time.Second
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
	HTTPAPI                   HTTPAPI         `json:"http_api"`
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

type HTTPAPI struct {
	Enabled               bool   `json:"enabled"`
	ListenAddr            string `json:"listen_addr"`
	RequestTimeoutSeconds int64  `json:"request_timeout_seconds"`
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
	PersistentStateKeepRecent        int64  `json:"persistent_state_keep_recent"`
	StateSerializeOnePass            bool   `json:"state_serialize_one_pass"`
	ArtifactFileMaxOpen              int64  `json:"artifact_file_max_open"`
}

type LiteSendMessageBroadcastCapacity struct {
	BytesPerSecond int64
	MaxDelay       time.Duration
}

type HTTPAPIOptions struct {
	RequestTimeout time.Duration
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
		HTTPAPI: HTTPAPI{
			ListenAddr:            DefaultHTTPAPIListen,
			RequestTimeoutSeconds: int64(DefaultHTTPAPIRequestTimeout / time.Second),
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
			PersistentStateKeepRecent:        service.DefaultPersistentStateKeepRecent,
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

	// Everything except the generated keys, listen/external addresses, and the
	// resolved paths matches defaultConfig() exactly.
	cfg := defaultConfig()
	cfg.TON.GlobalConfigPath = globalConfigPath
	cfg.ADNL = ADNL{
		Key:          adnlSeed,
		ListenAddr:   defaultADNLListen,
		ExternalAddr: net.JoinHostPort(externalIP, strconv.Itoa(defaultADNLPort)),
	}
	cfg.DHT = DHT{
		Key:        dhtSeed,
		ListenAddr: defaultDHTListen,
	}
	cfg.Lite.Key = liteSeed
	cfg.Lite.ListenAddr = DefaultLiteListen
	cfg.Storage.Dir = storageDir

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

	client := http.Client{Timeout: globalConfigHTTPClient}
	if err := downloadFile(ctx, &client, path, url); err != nil {
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

func downloadFile(ctx context.Context, client *http.Client, path string, url string) error {
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

type externalIPResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Query   string `json:"query"`
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

	var res externalIPResponse
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
