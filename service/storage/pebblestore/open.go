package pebblestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage"
)

func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("storage dir is empty")
	}
	logger := logutil.WithComponent(opts.Logger, "pebblestore")
	started := time.Now()
	if opts.MetaCacheSize <= 0 {
		opts.MetaCacheSize = defaultPebbleMetaCacheSize
	}
	if opts.CellCacheSize <= 0 {
		opts.CellCacheSize = defaultPebbleCellTotalCacheSize
	}
	if opts.BytesPerSync <= 0 {
		opts.BytesPerSync = defaultPebbleBytesPerSync
	}
	if opts.WALBytesPerSync <= 0 {
		opts.WALBytesPerSync = defaultPebbleWALBytesSync
	}
	if opts.CellMemTableStopWritesThreshold < 0 {
		return nil, fmt.Errorf("cell memtable stop writes threshold cannot be negative")
	}
	if opts.ArtifactFileMaxOpen < 0 {
		return nil, fmt.Errorf("artifact file max open cannot be negative")
	}
	if opts.CellMemTableStopWritesThreshold == 0 {
		opts.CellMemTableStopWritesThreshold = defaultPebbleCellMemTableStopThreshold
	}
	if opts.ArtifactFileMaxOpen == 0 {
		opts.ArtifactFileMaxOpen = DefaultArtifactFileMaxOpen
	}
	decodedCellCache, err := decodedCellCacheConfigFromOptions(opts)
	if err != nil {
		return nil, err
	}

	metaMemTableSize := opts.MetaMemTableSize
	if metaMemTableSize <= 0 {
		metaMemTableSize = defaultPebbleMetaMemTableSize
	}
	cellMemTableSize := opts.CellMemTableSize
	cellShardMemTable := opts.CellShardMemTableSize
	if cellMemTableSize < 0 {
		return nil, fmt.Errorf("cell total memtable size cannot be negative")
	}
	if cellShardMemTable < 0 {
		return nil, fmt.Errorf("cell shard memtable size cannot be negative")
	}
	if cellShardMemTable > 0 {
		maxInt := int(^uint(0) >> 1)
		if cellShardMemTable > maxInt/cellDBShardCount {
			return nil, fmt.Errorf("cell shard memtable size is too large")
		}
		total := cellShardMemTable * cellDBShardCount
		if cellMemTableSize > 0 && cellMemTableSize != total {
			return nil, fmt.Errorf("cell total memtable size %d does not match shard memtable size %d", cellMemTableSize, cellShardMemTable)
		}
		cellMemTableSize = total
	} else {
		if cellMemTableSize <= 0 {
			cellMemTableSize = defaultPebbleCellTotalMemTableSize
		}
		cellShardMemTable = cellShardMemTableSize(cellMemTableSize)
	}
	logger.Info().
		Str("dir", opts.Dir).
		Int64("meta_cache_size", opts.MetaCacheSize).
		Int64("cell_total_cache_size", opts.CellCacheSize).
		Bool("decoded_cell_cache_enabled", decodedCellCache.enabled).
		Int("decoded_cell_cache_shards", decodedCellCache.shards).
		Int64("decoded_cell_cache_bytes_per_entry", decodedCellCache.bytesPerEntry).
		Int("decoded_cell_cache_min_entries", decodedCellCache.minEntries).
		Int("decoded_cell_cache_max_entries", decodedCellCache.maxEntries).
		Int("meta_memtable_size", metaMemTableSize).
		Int("cell_total_memtable_size", cellMemTableSize).
		Int("cell_shard_memtable_size", cellShardMemTable).
		Int("cell_memtable_stop_writes_threshold", opts.CellMemTableStopWritesThreshold).
		Int("artifact_file_max_open", opts.ArtifactFileMaxOpen).
		Int("bytes_per_sync", opts.BytesPerSync).
		Int("wal_bytes_per_sync", opts.WALBytesPerSync).
		Bool("read_only", opts.ReadOnly).
		Msg("opening pebble storage")

	hotDir := filepath.Join(opts.Dir, "metadb")
	if !opts.ReadOnly {
		if err := os.MkdirAll(hotDir, 0o755); err != nil {
			return nil, fmt.Errorf("create metadb dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Join(opts.Dir, "archive", "packages"), 0o755); err != nil {
			return nil, fmt.Errorf("create archive packages dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Join(opts.Dir, "archive", "states"), 0o755); err != nil {
			return nil, fmt.Errorf("create archive states dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Join(opts.Dir, "archive", "tmp"), 0o755); err != nil {
			return nil, fmt.Errorf("create archive temp dir: %w", err)
		}
		if err := os.MkdirAll(filepath.Join(opts.Dir, cellDBRootDir), 0o755); err != nil {
			return nil, fmt.Errorf("create celldb root dir: %w", err)
		}
	}
	logger.Info().
		Str("dir", opts.Dir).
		Dur("elapsed", time.Since(started)).
		Bool("read_only", opts.ReadOnly).
		Msg("prepared pebble storage directories")

	fs := newPebbleDiskFullRetryFS(opts.Dir, logger)
	hotCache := pebble.NewCache(opts.MetaCacheSize)
	hotLogger := logger.With().Str("db", "metadb").Logger()
	hotCompactions := newPebbleCompactionController(pebbleMetaMaxConcurrentCompactions())

	hotOpts := newMetaPebbleOptions(hotCache, fs, metaMemTableSize, opts.BytesPerSync, opts.WALBytesPerSync, hotCompactions.newScheduler(), hotLogger)
	hotOpts.ReadOnly = opts.ReadOnly
	stageStarted := time.Now()
	logger.Info().Str("dir", hotDir).Msg("opening pebble metadb")
	hot, err := pebble.Open(hotDir, hotOpts)
	if err != nil {
		hotCache.Unref()
		return nil, fmt.Errorf("open metadb: %w", err)
	}
	hotCompactions.start()
	logger.Info().Str("dir", hotDir).Dur("elapsed", time.Since(stageStarted)).Msg("opened pebble metadb")

	stageStarted = time.Now()
	logger.Info().Uint32("version", metaDBVersion).Msg("checking metadb version")
	if err = ensureMetaDBVersion(hot, opts.ReadOnly); err != nil {
		_ = hot.Close()
		hotCache.Unref()
		return nil, fmt.Errorf("check metadb version: %w", err)
	}
	logger.Info().Uint32("version", metaDBVersion).Dur("elapsed", time.Since(stageStarted)).Msg("checked metadb version")

	stageStarted = time.Now()
	logger.Info().Msg("loading cell generation manifest")
	var manifest cellGenerationManifest
	if opts.ReadOnly {
		manifest, err = loadCellGenerationManifest(hot)
	} else {
		manifest, err = loadOrInitCellGenerationManifest(hot)
	}
	if err != nil {
		_ = hot.Close()
		hotCache.Unref()
		return nil, fmt.Errorf("load cell generation manifest: %w", err)
	}
	var migrations startupMigrations
	if !opts.ReadOnly {
		manifest, migrations, err = runStartupMigrations(hot, manifest)
		if err != nil {
			_ = hot.Close()
			hotCache.Unref()
			return nil, fmt.Errorf("run storage startup migrations: %w", err)
		}
		if !isEmptyBlockID(migrations.activeOrigin) {
			logger.Info().
				Str("origin_persistent_state", storage.FormatBlockRef(migrations.activeOrigin)).
				Msg("migrated active cell generation origin from current state")
		}
	}
	logger.Info().
		Uint64("active_cell_generation", manifest.active).
		Uint64("next_cell_generation", manifest.next).
		Str("origin_persistent_state", storage.FormatBlockRef(manifest.activeOrigin)).
		Bool("pending_cell_generation_migration", manifest.pending != nil).
		Int("retired_cell_generations", len(manifest.retired)).
		Dur("elapsed", time.Since(stageStarted)).
		Msg("loaded cell generation manifest")

	stageStarted = time.Now()
	logger.Info().
		Uint64("cell_generation", manifest.active).
		Msg("opening active celldb generation")
	cells, err := openCellStore(opts.Dir, manifest.active, fs, opts.CellCacheSize, cellShardMemTable, opts.CellMemTableStopWritesThreshold, opts.BytesPerSync, opts.ReadOnly, logger)
	if err != nil {
		_ = hot.Close()
		hotCache.Unref()
		return nil, err
	}
	logger.Info().
		Uint64("cell_generation", manifest.active).
		Dur("elapsed", time.Since(stageStarted)).
		Msg("opened active celldb generation")
	cellGenerations := map[uint64]*cellStore{manifest.active: cells}
	if manifest.pending != nil && manifest.pending.generation != manifest.active {
		stageStarted = time.Now()
		logger.Info().
			Uint64("cell_generation", manifest.pending.generation).
			Msg("opening pending celldb generation")
		pendingCells, err := openCellStore(opts.Dir, manifest.pending.generation, fs, opts.CellCacheSize, cellShardMemTable, opts.CellMemTableStopWritesThreshold, opts.BytesPerSync, opts.ReadOnly, logger)
		if err != nil {
			_ = cells.close()
			_ = hot.Close()
			hotCache.Unref()
			return nil, err
		}
		logger.Info().
			Uint64("cell_generation", manifest.pending.generation).
			Dur("elapsed", time.Since(stageStarted)).
			Msg("opened pending celldb generation")
		cellGenerations[manifest.pending.generation] = pendingCells
	}

	store := &Store{
		log:                             logger,
		hot:                             hot,
		cells:                           cells,
		cellGenerations:                 cellGenerations,
		activeCellGeneration:            manifest.active,
		activeCellOrigin:                manifest.activeOrigin,
		pendingCellMigration:            cloneCellGenerationPendingMigration(manifest.pending),
		retiredGenerations:              cloneUint64Slice(manifest.retired),
		nextCellGeneration:              manifest.next,
		cellCache:                       newDecodedCellCache(decodedCellCache),
		dir:                             opts.Dir,
		cellCacheSize:                   opts.CellCacheSize,
		cellShardMemTable:               cellShardMemTable,
		cellMemTableStopWritesThreshold: opts.CellMemTableStopWritesThreshold,
		artifactFiles:                   newArtifactFileCache(opts.ArtifactFileMaxOpen),
		archivePackages:                 map[int64]archivePackageMeta{},
		bytesPerSync:                    opts.BytesPerSync,
		fs:                              fs,
		hotOpts:                         hotOpts,
		hotCache:                        hotCache,
		readOnly:                        opts.ReadOnly,
		hotDrained:                      make(chan struct{}),
		pendingArchiveSync:              map[string]pendingPackWrite{},
		pendingKeyProofSync:             map[string]pendingPackWrite{},
	}
	if !opts.ReadOnly {
		stageStarted = time.Now()
		logger.Info().Msg("recovering artifact pack journals")
		if err = store.recoverPackJournals(context.Background()); err != nil {
			_ = store.closeCellGenerations()
			_ = hot.Close()
			_ = store.artifactFiles.close()
			hotCache.Unref()
			return nil, err
		}
		logger.Info().Dur("elapsed", time.Since(stageStarted)).Msg("recovered artifact pack journals")
		stageStarted = time.Now()
		logger.Info().Msg("cleaning retired cell generations")
		if err = store.CleanupRetiredCellGenerations(context.Background()); err != nil {
			_ = store.closeCellGenerations()
			_ = hot.Close()
			_ = store.artifactFiles.close()
			hotCache.Unref()
			return nil, fmt.Errorf("cleanup retired cell generations: %w", err)
		}
		logger.Info().Dur("elapsed", time.Since(stageStarted)).Msg("cleaned retired cell generations")
	} else {
		logger.Info().Msg("skipped artifact repair and retired cell cleanup in read-only mode")
	}
	logger.Info().
		Int64("meta_cache_size", opts.MetaCacheSize).
		Int64("cell_total_cache_size", opts.CellCacheSize).
		Bool("decoded_cell_cache_enabled", decodedCellCache.enabled).
		Int("decoded_cell_cache_shards", decodedCellCache.shards).
		Int64("decoded_cell_cache_bytes_per_entry", decodedCellCache.bytesPerEntry).
		Int("decoded_cell_cache_min_entries", decodedCellCache.minEntries).
		Int("decoded_cell_cache_max_entries", decodedCellCache.maxEntries).
		Int("meta_memtable_size", metaMemTableSize).
		Int("cell_total_memtable_size", cellMemTableSize).
		Int("cell_shard_memtable_size", cellShardMemTable).
		Int("cell_shards", cellDBShardCount).
		Uint64("cell_generation", manifest.active).
		Uint64("next_cell_generation", manifest.next).
		Str("cell_generation_origin", storage.FormatBlockRef(manifest.activeOrigin)).
		Int("retired_cell_generations", len(store.retiredGenerations)).
		Bool("pending_cell_generation_migration", store.pendingCellMigration != nil).
		Bool("read_only", opts.ReadOnly).
		Bool("cell_disable_wal", true).
		Int("meta_target_file_size", defaultPebbleMetaTargetFileSize).
		Int("cell_target_file_size", defaultPebbleCellTargetFileSize).
		Int("meta_max_open_files", defaultPebbleMetaMaxOpenFiles).
		Int("cell_max_open_files", defaultPebbleCellMaxOpenFiles).
		Int("pebble_format_major_version", int(defaultPebbleFormatMajorVersion)).
		Bool("pebble_columnar_blocks", false).
		Bool("pebble_value_blocks", false).
		Int64("cell_lbase_max_bytes", defaultPebbleCellLBaseMaxBytes).
		Int("state_cell_import_batch_target_bytes", stateCellImportBatchTargetBytes).
		Int("artifact_file_max_open", opts.ArtifactFileMaxOpen).
		Int("cell_memtable_stop_writes_threshold", opts.CellMemTableStopWritesThreshold).
		Int("cell_l0_compaction_threshold", defaultPebbleCellL0CompactionThreshold).
		Int("cell_l0_file_threshold", defaultPebbleCellL0FileThreshold).
		Int("cell_l0_stop_writes_threshold", defaultPebbleCellL0StopWritesThreshold).
		Int("meta_max_concurrent_compactions", pebbleMetaMaxConcurrentCompactions()).
		Int("cell_compaction_parallelism", pebbleCellCompactionParallelism()).
		Int("cell_max_concurrent_compactions", pebbleCellMaxConcurrentCompactions()).
		Int("max_concurrent_compactions", pebbleMaxConcurrentCompactions()).
		Dur("open_elapsed", time.Since(started)).
		Msg("configured pebble storage tuning")
	// Do not scan the full cell DB on startup just to populate console stats.
	return store, nil
}

func decodedCellCacheConfigFromOptions(opts Options) (decodedCellCacheConfig, error) {
	cfg := decodedCellCacheConfig{
		enabled:       !opts.DisableDecodedCellCache,
		shards:        opts.DecodedCellCacheShards,
		cacheBytes:    opts.CellCacheSize,
		bytesPerEntry: opts.DecodedCellCacheBytesPerEntry,
		minEntries:    opts.DecodedCellCacheMinEntries,
		maxEntries:    opts.DecodedCellCacheMaxEntries,
	}
	if cfg.bytesPerEntry < 0 {
		return decodedCellCacheConfig{}, fmt.Errorf("decoded cell cache bytes per entry cannot be negative")
	}
	if cfg.shards < 0 {
		return decodedCellCacheConfig{}, fmt.Errorf("decoded cell cache shards cannot be negative")
	}
	if cfg.minEntries < 0 {
		return decodedCellCacheConfig{}, fmt.Errorf("decoded cell cache min entries cannot be negative")
	}
	if cfg.maxEntries < 0 {
		return decodedCellCacheConfig{}, fmt.Errorf("decoded cell cache max entries cannot be negative")
	}
	if cfg.bytesPerEntry == 0 {
		cfg.bytesPerEntry = DefaultDecodedCellCacheBytesPerEntry
	}
	if cfg.shards == 0 {
		cfg.shards = DefaultDecodedCellCacheShards
	}
	if cfg.minEntries == 0 {
		cfg.minEntries = DefaultDecodedCellCacheMinEntries
	}
	if cfg.maxEntries == 0 {
		cfg.maxEntries = DefaultDecodedCellCacheMaxEntries
	}
	if cfg.minEntries > cfg.maxEntries {
		return decodedCellCacheConfig{}, fmt.Errorf("decoded cell cache min entries cannot exceed max entries")
	}
	return cfg, nil
}
