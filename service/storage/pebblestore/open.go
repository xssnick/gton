package pebblestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/internal/manualmem"
	"github.com/xssnick/gton/service/storage"
)

func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("storage dir is empty")
	}
	// All artifact pack paths are either relative to this directory or
	// absolute. Keep the store root absolute so an otherwise valid relative
	// --data-dir cannot leak its prefix into durable archive references.
	// In particular, pack journaling receives the physical pack path and
	// derives its archive-relative key from this root.
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("resolve storage dir: %w", err)
	}
	opts.Dir = dir
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
	if opts.LargeBOCShardReadWorkers < 0 {
		return nil, fmt.Errorf("large boc shard read workers cannot be negative")
	}
	if opts.ArtifactFileMaxOpen < 0 {
		return nil, fmt.Errorf("artifact file max open cannot be negative")
	}
	if opts.CellMemTableStopWritesThreshold == 0 {
		opts.CellMemTableStopWritesThreshold = defaultPebbleCellMemTableStopThreshold
	}
	if opts.LargeBOCShardReadWorkers == 0 {
		opts.LargeBOCShardReadWorkers = defaultLargeBOCShardReadWorkers
	}
	if opts.ArtifactFileMaxOpen == 0 {
		opts.ArtifactFileMaxOpen = DefaultArtifactFileMaxOpen
	}
	decodedCellCacheCfg, err := decodedCellCacheConfigFromOptions(opts)
	if err != nil {
		return nil, err
	}
	if opts.CellRecordCacheBytes < 0 {
		return nil, fmt.Errorf("cell record cache bytes cannot be negative")
	}
	var recordCacheCfg cellRecordCacheConfig
	if opts.CellRecordCacheBytes > 0 {
		recordCacheCfg = cellRecordCacheConfigFromBytes(opts.CellRecordCacheBytes)
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
		Bool("decoded_cell_cache_enabled", decodedCellCacheCfg.enabled).
		// Requested, not effective, for both shards and entries. The cache is
		// sharded, so each shard rounds the entry budget down, and the shard
		// count itself is clamped to the entry count. The pair of log lines is
		// deliberate: this one says what was asked for, the one after Open says
		// what exists.
		Int("decoded_cell_cache_shards_requested", decodedCellCacheCfg.shards).
		Int("decoded_cell_cache_entries_requested", decodedCellCacheCfg.entries).
		Int64("cell_record_cache_bytes_requested", opts.CellRecordCacheBytes).
		Int("meta_memtable_size", metaMemTableSize).
		Int("cell_total_memtable_size", cellMemTableSize).
		Int("cell_shard_memtable_size", cellShardMemTable).
		Int("cell_memtable_stop_writes_threshold", opts.CellMemTableStopWritesThreshold).
		Int("large_boc_shard_read_workers", opts.LargeBOCShardReadWorkers).
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
	if err = ensureMetaDBVersion(hot, opts.ReadOnly, logger); err != nil {
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
		decodedCells:                    newDecodedCellCache(decodedCellCacheCfg),
		recordCache:                     newCellRecordCache(recordCacheCfg),
		dir:                             opts.Dir,
		cellCacheSize:                   opts.CellCacheSize,
		cellShardMemTable:               cellShardMemTable,
		cellMemTableStopWritesThreshold: opts.CellMemTableStopWritesThreshold,
		largeBOCShardReadWorkers:        opts.LargeBOCShardReadWorkers,
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
	store.activeCellLoader = store.newActiveCellLoader()
	if !opts.ReadOnly {
		stageStarted = time.Now()
		logger.Info().Msg("recovering artifact pack journals")
		if err = store.recoverPackJournals(context.Background()); err != nil {
			_ = store.closeCellGenerations()
			store.recordCache.free()
			_ = hot.Close()
			_ = store.artifactFiles.close()
			hotCache.Unref()
			return nil, err
		}
		logger.Info().Dur("elapsed", time.Since(stageStarted)).Msg("recovered artifact pack journals")
		stageStarted = time.Now()
		logger.Info().Msg("cleaning retired cell generations")
		if err = store.cleanupRetiredCellGenerations(context.Background()); err != nil {
			_ = store.closeCellGenerations()
			store.recordCache.free()
			_ = hot.Close()
			_ = store.artifactFiles.close()
			hotCache.Unref()
			return nil, fmt.Errorf("cleanup retired cell generations: %w", err)
		}
		logger.Info().Dur("elapsed", time.Since(stageStarted)).Msg("cleaned retired cell generations")
		stageStarted = time.Now()
		logger.Info().Msg("cleaning unreferenced cell generations")
		if err = store.cleanupUnreferencedCellGenerationDirs(); err != nil {
			_ = store.closeCellGenerations()
			store.recordCache.free()
			_ = hot.Close()
			_ = store.artifactFiles.close()
			hotCache.Unref()
			return nil, fmt.Errorf("cleanup unreferenced cell generations: %w", err)
		}
		logger.Info().Dur("elapsed", time.Since(stageStarted)).Msg("cleaned unreferenced cell generations")
	} else {
		logger.Info().Msg("skipped artifact repair and cell generation cleanup in read-only mode")
	}
	logger.Info().
		Int64("meta_cache_size", opts.MetaCacheSize).
		Int64("cell_total_cache_size", opts.CellCacheSize).
		Bool("decoded_cell_cache_enabled", decodedCellCacheCfg.enabled).
		// Effective, meaning read off the cache that exists rather than off the
		// request. Both numbers can be lower than the *_requested values logged
		// before Open: the shard count is clamped down to the entry count, the
		// entry budget is divided across the shards rounding down, and each
		// shard then rounds its budget down to its set-associative bucket
		// geometry (a power of two of buckets times up to 8 ways). So shards=64
		// with entries=100 yields 64 shards of 1, shards=64 with entries=32
		// yields 32 shards, and shards=4 with entries=100 yields 4 shards of 16
		// rather than 25. Never higher than the request.
		Int("decoded_cell_cache_shards_effective", store.decodedCells.shardCount()).
		Int("decoded_cell_cache_entries_effective", store.decodedCells.capacity()).
		// The record cache pair follows the same requested/effective split: the
		// effective arena rounds the request down to whole regions (or up to
		// the smallest workable ring), and the index is derived on top of it —
		// see record_cache.go for the measured sizing.
		Bool("cell_record_cache_enabled", store.recordCache != nil).
		Int64("cell_record_cache_bytes_effective", store.recordCache.capacityBytes()).
		Int64("cell_record_cache_index_bytes", store.recordCache.indexBytes()).
		Bool("cell_record_cache_gc_managed", manualmem.ManagedByGC).
		Int("meta_memtable_size", metaMemTableSize).
		Int("cell_total_memtable_size", cellMemTableSize).
		Int("cell_shard_memtable_size", cellShardMemTable).
		Int("cell_shards", cellDBShardCount).
		Int("large_boc_shard_read_workers", opts.LargeBOCShardReadWorkers).
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
		Int("state_boc_import_max_batch_bytes", stateBOCImportMaxBatchBytes).
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

// decodedCellCacheConfigFromOptions derives the decoded cell cache
// configuration. Note what it does NOT read: opts.CellCacheSize. That knob is
// the pebble block cache budget and nothing else. Deriving a Go-object entry
// count from it coupled a GC-visible cache to an opaque byte budget, so raising
// the pebble cache — the normal thing to do on a large machine — silently
// multiplied the number of live Go objects every mark cycle has to scan.
func decodedCellCacheConfigFromOptions(opts Options) (decodedCellCacheConfig, error) {
	if opts.DecodedCellCacheShards < 0 {
		return decodedCellCacheConfig{}, fmt.Errorf("decoded cell cache shards cannot be negative")
	}
	if opts.DecodedCellCacheEntries < 0 {
		return decodedCellCacheConfig{}, fmt.Errorf("decoded cell cache entries cannot be negative")
	}

	shards := opts.DecodedCellCacheShards
	if shards == 0 {
		shards = DefaultDecodedCellCacheShards
	}
	entries := opts.DecodedCellCacheEntries
	if entries == 0 {
		entries = DefaultDecodedCellCacheEntries
	}

	return decodedCellCacheConfig{
		enabled: !opts.DisableDecodedCellCache,
		shards:  shards,
		entries: entries,
	}, nil
}
