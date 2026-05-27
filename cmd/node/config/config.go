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
	DefaultPath                            = "config.json"
	DefaultGlobalConfigPath                = "global.config.json"
	DefaultGlobalConfigURL                 = "https://ton-blockchain.github.io/global.config.json"
	DefaultSyncBefore                      = time.Hour
	DefaultStateTTL                        = 3 * 24 * time.Hour
	DefaultArchiveTTL                      = 7 * 24 * time.Hour
	DefaultNextCheckpointBlocks            = int64(300)
	DefaultArchiveCheckpointBlocks         = int64(2000)
	DefaultCheckpointBytes                 = int64(512 << 20)
	DefaultCellTotalCache                  = int64(8 << 30)
	DefaultCellShardMemTable               = int64(256 << 20)
	DefaultCellMemTableStopWritesThreshold = int64(4)
	DefaultArtifactFileMaxOpen             = int64(512)
	DefaultLiteMasterBlockCache            = 128
	DefaultLiteShardBlockCache             = 4096
	DefaultLiteListen                      = "0.0.0.0:7445"
	DefaultMetricsNamespace                = "gton"
	defaultStorageDir                      = "data"
	defaultADNLPort                        = 30303
	defaultADNLListen                      = "0.0.0.0:30303"
	defaultDHTListen                       = "0.0.0.0:30304"
	privateKeySeedSize                     = 32
	externalIPHTTPClient                   = 5 * time.Second
	globalConfigHTTPClient                 = 30 * time.Second
	ipAPILookupURL                         = "http://ip-api.com/json/?fields=status,message,query"
)

var ErrConfigMissingWithExistingStorage = errors.New("config file is missing while storage metadata exists")

type Config struct {
	TON                       TON     `json:"ton"`
	ADNL                      ADNL    `json:"adnl"`
	DHT                       DHT     `json:"dht"`
	Lite                      Lite    `json:"liteserver"`
	Storage                   Storage `json:"storage"`
	Metrics                   Metrics `json:"metrics"`
	DisableStateSerialization bool    `json:"disable_state_serialization"`
}

type TON struct {
	GlobalConfigPath        string `json:"global_config_path"`
	SyncBefore              int64  `json:"sync_before"`
	StateTTL                int64  `json:"state_ttl"`
	ArchiveTTL              int64  `json:"archive_ttl"`
	NextCheckpointBlocks    int64  `json:"next_checkpoint_blocks"`
	ArchiveCheckpointBlocks int64  `json:"archive_checkpoint_blocks"`
	CheckpointBytes         int64  `json:"checkpoint_bytes"`
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
	Enabled          bool   `json:"enabled"`
	Key              []byte `json:"key"`
	ListenAddr       string `json:"listen_addr"`
	MasterBlockCache int    `json:"master_block_cache"`
	ShardBlockCache  int    `json:"shard_block_cache"`
}

type Storage struct {
	Dir                             string `json:"dir"`
	CellTotalCacheSize              int64  `json:"cell_total_cache_size"`
	CellShardMemTableSize           int64  `json:"cell_shard_memtable_size"`
	CellMemTableStopWritesThreshold int64  `json:"cell_memtable_stop_writes_threshold"`
	ArtifactFileMaxOpen             int64  `json:"artifact_file_max_open"`
}

type Metrics struct {
	Enabled    bool   `json:"enabled"`
	ListenAddr string `json:"listen_addr"`
	Namespace  string `json:"namespace"`
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
		},
		Lite: Lite{
			MasterBlockCache: DefaultLiteMasterBlockCache,
			ShardBlockCache:  DefaultLiteShardBlockCache,
		},
		Storage: Storage{
			CellTotalCacheSize:              DefaultCellTotalCache,
			CellShardMemTableSize:           DefaultCellShardMemTable,
			CellMemTableStopWritesThreshold: DefaultCellMemTableStopWritesThreshold,
			ArtifactFileMaxOpen:             DefaultArtifactFileMaxOpen,
		},
		Metrics: Metrics{
			Namespace: DefaultMetricsNamespace,
		},
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
		Enabled:          false,
		Key:              liteSeed,
		ListenAddr:       DefaultLiteListen,
		MasterBlockCache: DefaultLiteMasterBlockCache,
		ShardBlockCache:  DefaultLiteShardBlockCache,
	}
	cfg.Storage = Storage{
		Dir:                             storageDir,
		CellTotalCacheSize:              DefaultCellTotalCache,
		CellShardMemTableSize:           DefaultCellShardMemTable,
		CellMemTableStopWritesThreshold: DefaultCellMemTableStopWritesThreshold,
		ArtifactFileMaxOpen:             DefaultArtifactFileMaxOpen,
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
	defer file.Close()

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
	if cfg.TON.SyncBefore <= 0 {
		return 0, fmt.Errorf("ton.sync_before should be positive seconds")
	}
	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if cfg.TON.SyncBefore > maxDurationSeconds {
		return 0, fmt.Errorf("ton.sync_before is too large")
	}
	return time.Duration(cfg.TON.SyncBefore) * time.Second, nil
}

func (cfg Config) StateTTL() (time.Duration, error) {
	if cfg.TON.StateTTL <= 0 {
		return 0, fmt.Errorf("ton.state_ttl should be positive seconds")
	}
	const maxDurationSeconds = int64(time.Duration(1<<63-1) / time.Second)
	if cfg.TON.StateTTL > maxDurationSeconds {
		return 0, fmt.Errorf("ton.state_ttl is too large")
	}
	return time.Duration(cfg.TON.StateTTL) * time.Second, nil
}

func (cfg Config) ArchiveTTL() (time.Duration, error) {
	if cfg.TON.ArchiveTTL <= 0 {
		return 0, fmt.Errorf("ton.archive_ttl should be positive seconds")
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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
