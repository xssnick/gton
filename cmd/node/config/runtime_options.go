package config

import (
	"crypto/ed25519"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/xssnick/gton"
	"github.com/xssnick/gton/service/p2p"
)

var metricsNamespacePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (cfg Config) NodeOptions(runOpts gton.NodeOptions) (gton.NodeOptions, error) {
	p2pOpts, err := p2pOptionsFromConfig(cfg)
	if err != nil {
		return gton.NodeOptions{}, err
	}
	runOpts.P2P = p2pOpts

	metricsOpts, err := metricsOptionsFromConfig(cfg)
	if err != nil {
		return gton.NodeOptions{}, err
	}
	runOpts.Metrics = metricsOpts

	storageOpts, err := storageOptionsFromConfig(cfg)
	if err != nil {
		return gton.NodeOptions{}, err
	}
	runOpts.Storage = storageOpts

	syncBefore, err := cfg.SyncBefore()
	if err != nil {
		return gton.NodeOptions{}, err
	}
	syncUntil, err := cfg.SyncUntil()
	if err != nil {
		return gton.NodeOptions{}, err
	}
	stateTTL, err := cfg.StateTTL()
	if err != nil {
		return gton.NodeOptions{}, err
	}
	archiveTTL, err := cfg.ArchiveTTL()
	if err != nil {
		return gton.NodeOptions{}, err
	}
	nextCheckpointBlocks, err := cfg.NextCheckpointBlocks()
	if err != nil {
		return gton.NodeOptions{}, err
	}
	archiveCheckpointBlocks, err := cfg.ArchiveCheckpointBlocks()
	if err != nil {
		return gton.NodeOptions{}, err
	}
	checkpointBytes, err := cfg.CheckpointBytes()
	if err != nil {
		return gton.NodeOptions{}, err
	}
	syncBackpressureWindows, err := cfg.SyncBackpressureWindows()
	if err != nil {
		return gton.NodeOptions{}, err
	}

	runOpts.SyncBefore = syncBefore
	runOpts.SyncUntil = syncUntil
	runOpts.ArchiveFromZero = cfg.ArchiveFromZero()
	runOpts.StateTTL = stateTTL
	runOpts.ArchiveTTL = archiveTTL
	runOpts.NextCheckpointBlocks = nextCheckpointBlocks
	runOpts.ArchiveCheckpointBlocks = archiveCheckpointBlocks
	runOpts.CheckpointBytes = checkpointBytes
	runOpts.SyncBackpressureWindows = syncBackpressureWindows
	runOpts.DisableStateSerialization = cfg.DisableStateSerialization

	return runOpts, nil
}

func storageOptionsFromConfig(cfg Config) (gton.StorageOptions, error) {
	cellTotalCacheSize, err := cfg.CellTotalCacheSize()
	if err != nil {
		return gton.StorageOptions{}, err
	}
	decodedCellCacheOpts, err := cfg.DecodedCellCacheOptions()
	if err != nil {
		return gton.StorageOptions{}, err
	}
	cellShardMemTableSize, err := cfg.CellShardMemTableSize()
	if err != nil {
		return gton.StorageOptions{}, err
	}
	cellMemTableStopWritesThreshold, err := cfg.CellMemTableStopWritesThreshold()
	if err != nil {
		return gton.StorageOptions{}, err
	}
	largeBOCShardReadWorkers, err := cfg.LargeBOCShardReadWorkers()
	if err != nil {
		return gton.StorageOptions{}, err
	}
	persistentStateLargeBOCBatchSize, err := cfg.PersistentStateLargeBOCBatchSize()
	if err != nil {
		return gton.StorageOptions{}, err
	}
	artifactFileMaxOpen, err := cfg.ArtifactFileMaxOpen()
	if err != nil {
		return gton.StorageOptions{}, err
	}

	return gton.StorageOptions{
		Dir:                              cfg.StorageDir(),
		CellTotalCacheSize:               cellTotalCacheSize,
		DecodedCellCache:                 decodedCellCacheOptions(decodedCellCacheOpts),
		CellShardMemTableSize:            cellShardMemTableSize,
		CellMemTableStopWritesThreshold:  cellMemTableStopWritesThreshold,
		LargeBOCShardReadWorkers:         largeBOCShardReadWorkers,
		PersistentStateLargeBOCBatchSize: persistentStateLargeBOCBatchSize,
		StateSerializeOnePass:            cfg.Storage.StateSerializeOnePass,
		ArtifactFileMaxOpen:              artifactFileMaxOpen,
	}, nil
}

func decodedCellCacheOptions(opts DecodedCellCacheOptions) gton.DecodedCellCacheOptions {
	return gton.DecodedCellCacheOptions{
		Enabled:       opts.Enabled,
		Shards:        opts.Shards,
		BytesPerEntry: opts.BytesPerEntry,
		MinEntries:    opts.MinEntries,
		MaxEntries:    opts.MaxEntries,
	}
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
