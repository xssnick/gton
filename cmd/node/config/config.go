package config

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"flexserver/liteserver"
	"flexserver/service/p2p"
)

const (
	DefaultPath            = "config.json"
	DefaultGlobalConfigURL = "https://ton-blockchain.github.io/global.config.json"
	defaultStorageDir      = "data"
	defaultADNLPort        = 30303
	defaultADNLListen      = "0.0.0.0:30303"
	defaultDHTListen       = "0.0.0.0:30304"
	defaultLiteListen      = "0.0.0.0:7445"
	externalIPHTTPClient   = 5 * time.Second
	globalConfigHTTPClient = 30 * time.Second
	ipAPILookupURL         = "http://ip-api.com/json/?fields=status,message,query"
)

type Config struct {
	TON     TON     `json:"ton"`
	ADNL    ADNL    `json:"adnl"`
	DHT     DHT     `json:"dht"`
	Lite    Lite    `json:"liteserver"`
	Storage Storage `json:"storage"`
}

type TON struct {
	GlobalConfigPath string `json:"global_config_path"`
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
	Dir string `json:"dir"`
}

func defaultConfig() Config {
	return Config{
		TON: TON{
			GlobalConfigPath: p2p.DefaultGlobalConfigPath,
		},
		Lite: Lite{
			MasterBlockCache: liteserver.DefaultMasterBlockCache,
			ShardBlockCache:  liteserver.DefaultShardBlockCache,
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

	globalConfigPath, err := filepath.Abs(p2p.DefaultGlobalConfigPath)
	if err != nil {
		return Config{}, fmt.Errorf("resolve global config path: %w", err)
	}

	storageDir, err := filepath.Abs(defaultStorageDir)
	if err != nil {
		return Config{}, fmt.Errorf("resolve storage dir: %w", err)
	}

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
	cfg.Lite = Lite{
		Enabled:          false,
		Key:              liteSeed,
		ListenAddr:       defaultLiteListen,
		MasterBlockCache: liteserver.DefaultMasterBlockCache,
		ShardBlockCache:  liteserver.DefaultShardBlockCache,
	}
	cfg.Storage = Storage{
		Dir: storageDir,
	}

	return cfg, nil
}

func LoadOrCreate(
	ctx context.Context,
	path string,
	externalIPLookup func(context.Context) (string, error),
) (Config, bool, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultPath
	}

	cfg, err := Load(path)
	if err == nil {
		return cfg, false, nil
	}
	if !os.IsNotExist(err) {
		return Config{}, false, err
	}

	cfg, err = generate(ctx, externalIPLookup)
	if err != nil {
		return Config{}, false, fmt.Errorf("generate default config: %w", err)
	}

	if err = write(path, cfg); err != nil {
		return Config{}, false, err
	}

	return cfg, true, nil
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

func (cfg Config) P2POptions() (p2p.Options, error) {
	opts := p2p.Options{
		GlobalConfigPath: strings.TrimSpace(cfg.TON.GlobalConfigPath),
		ListenAddr:       strings.TrimSpace(cfg.ADNL.ListenAddr),
		DHTListenAddr:    strings.TrimSpace(cfg.DHT.ListenAddr),
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

	return opts, nil
}

type LiteserverOptions struct {
	Enabled          bool
	ListenAddr       string
	PrivateKey       ed25519.PrivateKey
	MasterBlockCache int
	ShardBlockCache  int
}

func (cfg Config) LiteserverOptions() (LiteserverOptions, error) {
	opts := LiteserverOptions{
		Enabled:          cfg.Lite.Enabled,
		ListenAddr:       strings.TrimSpace(cfg.Lite.ListenAddr),
		MasterBlockCache: cfg.Lite.MasterBlockCache,
		ShardBlockCache:  cfg.Lite.ShardBlockCache,
	}
	if opts.ListenAddr == "" {
		opts.ListenAddr = defaultLiteListen
	}

	var err error
	opts.PrivateKey, err = privateKeyFromSeed(cfg.Lite.Key, "liteserver.key")
	if err != nil {
		return LiteserverOptions{}, err
	}
	if opts.Enabled && len(opts.PrivateKey) == 0 {
		return LiteserverOptions{}, fmt.Errorf("liteserver.key is required when liteserver.enabled is true")
	}
	if opts.MasterBlockCache < 0 {
		return LiteserverOptions{}, fmt.Errorf("liteserver.master_block_cache cannot be negative")
	}
	if opts.ShardBlockCache < 0 {
		return LiteserverOptions{}, fmt.Errorf("liteserver.shard_block_cache cannot be negative")
	}

	return opts, nil
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

func (cfg Config) StorageDir() string {
	return strings.TrimSpace(cfg.Storage.Dir)
}

func (cfg Config) GlobalConfigPath() string {
	path := strings.TrimSpace(cfg.TON.GlobalConfigPath)
	if path == "" {
		return p2p.DefaultGlobalConfigPath
	}
	return path
}

func EnsureGlobalConfig(ctx context.Context, path string, url string, replace bool) (bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = p2p.DefaultGlobalConfigPath
	}
	url = strings.TrimSpace(url)
	if url == "" {
		url = DefaultGlobalConfigURL
	}

	if !replace {
		if _, err := os.Stat(path); err == nil {
			return false, nil
		} else if !os.IsNotExist(err) {
			return false, err
		}
	}

	if err := downloadFile(ctx, path, url); err != nil {
		return false, err
	}
	return true, nil
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
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	return seed, nil
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
