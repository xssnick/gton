package config

import (
	"strings"
	"time"

	"github.com/xssnick/gton"
)

func (cfg Config) NodeOptions(nodeOpts gton.NodeOptions) (gton.NodeOptions, error) {
	opts, err := cfg.RuntimeOptions(nodeOpts)
	if err != nil {
		return gton.NodeOptions{}, err
	}
	return opts.Node, nil
}

func (cfg Config) StorageDir() string {
	return strings.TrimSpace(cfg.Storage.Dir)
}

func (cfg Config) CellTotalCacheSize() (int64, error) {
	return cellTotalCacheSizeFromConfig(cfg.Storage)
}

func (cfg Config) DecodedCellCacheOptions() (DecodedCellCacheOptions, error) {
	return decodedCellCacheOptionsFromConfig(cfg.Storage)
}

func (cfg Config) CellShardMemTableSize() (int, error) {
	return intConfigValue("storage.cell_shard_memtable_size", cfg.Storage.CellShardMemTableSize, DefaultCellShardMemTable)
}

func (cfg Config) CellMemTableStopWritesThreshold() (int, error) {
	return intConfigValue("storage.cell_memtable_stop_writes_threshold", cfg.Storage.CellMemTableStopWritesThreshold, DefaultCellMemTableStopWritesThreshold)
}

func (cfg Config) LargeBOCShardReadWorkers() (int, error) {
	return intConfigValue("storage.large_boc_shard_read_workers", cfg.Storage.LargeBOCShardReadWorkers, DefaultLargeBOCShardReadWorkers)
}

func (cfg Config) PersistentStateLargeBOCBatchSize() (int, error) {
	return intConfigValue("storage.persistent_state_large_boc_batch_size", cfg.Storage.PersistentStateLargeBOCBatchSize, DefaultPersistentStateLargeBOCBatchSize)
}

func (cfg Config) ArtifactFileMaxOpen() (int, error) {
	return intConfigValue("storage.artifact_file_max_open", cfg.Storage.ArtifactFileMaxOpen, DefaultArtifactFileMaxOpen)
}

func (cfg Config) SyncBefore() (time.Duration, error) {
	syncBefore, _, err := syncBeforeFromConfig(cfg.TON)
	return syncBefore, err
}

func (cfg Config) SyncUntil() (uint32, error) {
	return uint32ConfigValueAllowZero("ton.sync_until", cfg.TON.SyncUntil)
}

func (cfg Config) ArchiveFromZero() bool {
	return cfg.TON.SyncBefore == ArchiveFromZeroSyncBefore
}

func (cfg Config) StateTTL() (time.Duration, error) {
	return durationSeconds("ton.state_ttl", cfg.TON.StateTTL, true)
}

func (cfg Config) ArchiveTTL() (time.Duration, error) {
	return durationSeconds("ton.archive_ttl", cfg.TON.ArchiveTTL, true)
}

func (cfg Config) NextCheckpointBlocks() (uint32, error) {
	return uint32ConfigValue("ton.next_checkpoint_blocks", cfg.TON.NextCheckpointBlocks, uint32(DefaultNextCheckpointBlocks))
}

func (cfg Config) ArchiveCheckpointBlocks() (uint32, error) {
	return uint32ConfigValue("ton.archive_checkpoint_blocks", cfg.TON.ArchiveCheckpointBlocks, uint32(DefaultArchiveCheckpointBlocks))
}

func (cfg Config) CheckpointBytes() (uint64, error) {
	return checkpointBytesFromConfig(cfg.TON)
}

func (cfg Config) SyncBackpressureWindows() (uint32, error) {
	return uint32ConfigValue("ton.sync_backpressure_windows", cfg.TON.SyncBackpressureWindows, uint32(DefaultSyncBackpressureWindows))
}

func (cfg Config) LiteSendMessageBroadcastCapacity() (LiteSendMessageBroadcastCapacity, error) {
	return liteSendMessageBroadcastCapacityFromConfig(cfg.Lite)
}

func (cfg Config) LiteSendMessageBroadcastFanout() (int, error) {
	return liteSendMessageBroadcastFanoutFromConfig(cfg.Lite)
}

func (cfg Config) HTTPAPIOptions() (HTTPAPIOptions, error) {
	return httpapiOptionsFromConfig(cfg.HTTPAPI)
}
