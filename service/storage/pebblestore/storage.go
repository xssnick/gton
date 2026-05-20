package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/rs/zerolog"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	defaultPebbleMetaCacheSize      = 64 << 20
	defaultPebbleCellTotalCacheSize = 8 << 30

	defaultPebbleMetaMemTableSize      = 16 << 20
	defaultPebbleCellShardMemTableSize = 512 << 20
	defaultPebbleCellTotalMemTableSize = defaultPebbleCellShardMemTableSize * cellDBShardCount

	defaultPebbleBytesPerSync = 8 << 20
	defaultPebbleWALBytesSync = 8 << 20

	defaultPebbleMetaTargetFileSize = 16 << 20
	defaultPebbleCellTargetFileSize = 256 << 20
	defaultPebbleMetaMaxOpenFiles   = 4096
	defaultPebbleCellMaxOpenFiles   = 16384
	defaultPebbleFormatMajorVersion = pebble.FormatFlushableIngest

	defaultPebbleMetaMemTableStopThreshold = 4
	defaultPebbleCellMemTableStopThreshold = 8

	defaultPebbleMetaL0CompactionThreshold = 4
	defaultPebbleCellL0CompactionThreshold = 4

	defaultPebbleMetaL0FileThreshold = 8
	defaultPebbleCellL0FileThreshold = 16

	defaultPebbleMetaL0StopWritesThreshold = 32
	defaultPebbleCellL0StopWritesThreshold = 64

	defaultPebbleCellLBaseMaxBytes = 1 << 30

	stateCellImportBatchTargetBytes = 128 << 20
	stateCellSaveProgressInterval   = 5 * time.Second
	archivePackageMasterchainBlocks = 20000
	archiveSliceMasterchainBlocks   = 100
	keyArchiveMasterchainBlocks     = 200000

	blockMetaVersion       = 1
	currentStateVersion    = 1
	blockStateMetaVersion  = 1
	artifactRefVersion     = 1
	persistentStateVersion = 1
	archivePackageVersion  = 1

	cellRecordCompactRefsFlag = 0x10
	cellRecordHashSize        = 32
	cellRecordDepthSize       = 2
)

var (
	errPebbleClosed          = errors.New("pebble storage is closed")
	errCellGenerationNotOpen = errors.New("cell generation is not open")
)

type Options struct {
	Dir                   string
	Logger                *zerolog.Logger
	ReadOnly              bool
	MetaCacheSize         int64
	CellCacheSize         int64
	MetaMemTableSize      int
	CellMemTableSize      int
	CellShardMemTableSize int
	BytesPerSync          int
	WALBytesPerSync       int
}

type Store struct {
	log zerolog.Logger

	hot                  *pebble.DB
	cells                *cellStore
	cellGenerations      map[uint64]*cellStore
	activeCellGeneration uint64
	activeCellOrigin     ton.BlockIDExt
	pendingCellMigration *cellGenerationPendingMigration
	retiredGenerations   []uint64
	nextCellGeneration   uint64
	cellCache            *decodedCellCache
	dir                  string
	cellCacheSize        int64
	cellShardMemTable    int
	bytesPerSync         int
	hotOpts              *pebble.Options
	hotCache             *pebble.Cache
	readOnly             bool
	hotWriteMu           sync.Mutex
	hotClosing           atomic.Bool
	hotRefs              atomic.Int64
	hotDrained           chan struct{}
	hotDrainOnce         sync.Once

	mu                  sync.RWMutex
	artifactMu          sync.Mutex
	artifactSyncSeq     uint64
	pendingArchiveSync  map[string]uint64
	pendingKeyProofSync map[string]uint64
	dirtyArchivePacks   map[string]struct{}
	dirtyKeyProofPacks  map[string]struct{}
	closed              bool
}

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
		Int("meta_memtable_size", metaMemTableSize).
		Int("cell_total_memtable_size", cellMemTableSize).
		Int("cell_shard_memtable_size", cellShardMemTable).
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

	hotCache := pebble.NewCache(opts.MetaCacheSize)
	hotLogger := logger.With().Str("db", "metadb").Logger()

	hotOpts := newMetaPebbleOptions(hotCache, metaMemTableSize, opts.BytesPerSync, opts.WALBytesPerSync, hotLogger)
	hotOpts.ReadOnly = opts.ReadOnly
	stageStarted := time.Now()
	logger.Info().Str("dir", hotDir).Msg("opening pebble metadb")
	hot, err := pebble.Open(hotDir, hotOpts)
	if err != nil {
		hotCache.Unref()
		return nil, fmt.Errorf("open metadb: %w", err)
	}
	logger.Info().Str("dir", hotDir).Dur("elapsed", time.Since(stageStarted)).Msg("opened pebble metadb")

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
		Bool("pending_cell_generation_migration", manifest.pending != nil).
		Int("retired_cell_generations", len(manifest.retired)).
		Dur("elapsed", time.Since(stageStarted)).
		Msg("loaded cell generation manifest")

	stageStarted = time.Now()
	logger.Info().
		Uint64("cell_generation", manifest.active).
		Msg("opening active celldb generation")
	cells, err := openCellStore(opts.Dir, manifest.active, opts.CellCacheSize, cellShardMemTable, opts.BytesPerSync, opts.ReadOnly, logger)
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
		pendingCells, err := openCellStore(opts.Dir, manifest.pending.generation, opts.CellCacheSize, cellShardMemTable, opts.BytesPerSync, opts.ReadOnly, logger)
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
		log:                  logger,
		hot:                  hot,
		cells:                cells,
		cellGenerations:      cellGenerations,
		activeCellGeneration: manifest.active,
		activeCellOrigin:     manifest.activeOrigin,
		pendingCellMigration: cloneCellGenerationPendingMigration(manifest.pending),
		retiredGenerations:   cloneUint64Slice(manifest.retired),
		nextCellGeneration:   manifest.next,
		cellCache:            newDecodedCellCache(opts.CellCacheSize),
		dir:                  opts.Dir,
		cellCacheSize:        opts.CellCacheSize,
		cellShardMemTable:    cellShardMemTable,
		bytesPerSync:         opts.BytesPerSync,
		hotOpts:              hotOpts,
		hotCache:             hotCache,
		readOnly:             opts.ReadOnly,
		hotDrained:           make(chan struct{}),
		pendingArchiveSync:   map[string]uint64{},
		pendingKeyProofSync:  map[string]uint64{},
		dirtyArchivePacks:    map[string]struct{}{},
		dirtyKeyProofPacks:   map[string]struct{}{},
	}
	if !opts.ReadOnly {
		stageStarted = time.Now()
		logger.Info().Msg("reconciling committed artifact files")
		if err = store.reconcileCommittedArtifactFiles(); err != nil {
			_ = store.closeCellGenerations()
			_ = hot.Close()
			hotCache.Unref()
			return nil, err
		}
		logger.Info().Dur("elapsed", time.Since(stageStarted)).Msg("reconciled committed artifact files")
		stageStarted = time.Now()
		logger.Info().Msg("cleaning retired cell generations")
		if err = store.CleanupRetiredCellGenerations(context.Background()); err != nil {
			_ = store.closeCellGenerations()
			_ = hot.Close()
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
		Int("cell_memtable_stop_writes_threshold", defaultPebbleCellMemTableStopThreshold).
		Int("cell_l0_compaction_threshold", defaultPebbleCellL0CompactionThreshold).
		Int("cell_l0_file_threshold", defaultPebbleCellL0FileThreshold).
		Int("cell_l0_stop_writes_threshold", defaultPebbleCellL0StopWritesThreshold).
		Int("cell_compaction_parallelism", pebbleCellCompactionParallelism()).
		Int("cell_max_concurrent_compactions", pebbleCellMaxConcurrentCompactions()).
		Int("max_concurrent_compactions", pebbleMaxConcurrentCompactions()).
		Dur("open_elapsed", time.Since(started)).
		Msg("configured pebble storage tuning")
	// Do not scan the full cell DB on startup just to populate console stats.
	return store, nil
}

type pebbleOptionsTuning struct {
	blockSize                   int
	compression                 func() *sstable.CompressionProfile
	filterPolicy                pebble.FilterPolicy
	targetFileSize              int
	maxOpenFiles                int
	fileCache                   *pebble.FileCache
	maxConcurrentCompactions    func() int
	memTableStopWritesThreshold int
	l0CompactionThreshold       int
	l0CompactionFileThreshold   int
	l0StopWritesThreshold       int
	lBaseMaxBytes               int64
	disableWAL                  bool
	compactionScheduler         pebble.CompactionScheduler
}

func newMetaPebbleOptions(cache *pebble.Cache, memTableSize, bytesPerSync, walBytesPerSync int, logger zerolog.Logger) *pebble.Options {
	return newPebbleOptions(cache, memTableSize, bytesPerSync, walBytesPerSync, pebbleOptionsTuning{
		blockSize:                   4 << 10,
		compression:                 pebbleNoCompression,
		filterPolicy:                bloom.FilterPolicy(10),
		targetFileSize:              defaultPebbleMetaTargetFileSize,
		maxOpenFiles:                defaultPebbleMetaMaxOpenFiles,
		maxConcurrentCompactions:    pebbleMaxConcurrentCompactions,
		memTableStopWritesThreshold: defaultPebbleMetaMemTableStopThreshold,
		l0CompactionThreshold:       defaultPebbleMetaL0CompactionThreshold,
		l0CompactionFileThreshold:   defaultPebbleMetaL0FileThreshold,
		l0StopWritesThreshold:       defaultPebbleMetaL0StopWritesThreshold,
	}, logger)
}

func newCellPebbleOptions(cache *pebble.Cache, fileCache *pebble.FileCache, memTableSize, bytesPerSync int, compactionScheduler pebble.CompactionScheduler, logger zerolog.Logger) *pebble.Options {
	return newPebbleOptions(cache, memTableSize, bytesPerSync, 0, pebbleOptionsTuning{
		blockSize:                   4 << 10,
		compression:                 pebbleNoCompression,
		filterPolicy:                bloom.FilterPolicy(10),
		targetFileSize:              defaultPebbleCellTargetFileSize,
		maxOpenFiles:                defaultPebbleCellMaxOpenFiles,
		fileCache:                   fileCache,
		maxConcurrentCompactions:    pebbleCellMaxConcurrentCompactions,
		memTableStopWritesThreshold: defaultPebbleCellMemTableStopThreshold,
		l0CompactionThreshold:       defaultPebbleCellL0CompactionThreshold,
		l0CompactionFileThreshold:   defaultPebbleCellL0FileThreshold,
		l0StopWritesThreshold:       defaultPebbleCellL0StopWritesThreshold,
		lBaseMaxBytes:               defaultPebbleCellLBaseMaxBytes,
		disableWAL:                  true,
		compactionScheduler:         compactionScheduler,
	}, logger)
}

func newPebbleOptions(cache *pebble.Cache, memTableSize, bytesPerSync, walBytesPerSync int, tuning pebbleOptionsTuning, logger zerolog.Logger) *pebble.Options {
	filterPolicy := tuning.filterPolicy
	if filterPolicy == nil {
		filterPolicy = bloom.FilterPolicy(10)
	}

	var levels [7]pebble.LevelOptions
	for i := range levels {
		levels[i] = pebble.LevelOptions{
			BlockSize:      tuning.blockSize,
			IndexBlockSize: tuning.blockSize,
			FilterPolicy:   filterPolicy,
			FilterType:     pebble.TableFilter,
			Compression:    tuning.compression,
		}
	}
	var targetFileSizes [7]int64
	for i := range targetFileSizes {
		targetFileSizes[i] = int64(tuning.targetFileSize)
	}
	maxConcurrentCompactions := tuning.maxConcurrentCompactions
	if maxConcurrentCompactions == nil {
		maxConcurrentCompactions = pebbleMaxConcurrentCompactions
	}
	opts := &pebble.Options{
		Cache:                       cache,
		FileCache:                   tuning.fileCache,
		FormatMajorVersion:          defaultPebbleFormatMajorVersion,
		MaxOpenFiles:                tuning.maxOpenFiles,
		MemTableSize:                uint64(memTableSize),
		MemTableStopWritesThreshold: tuning.memTableStopWritesThreshold,
		BytesPerSync:                bytesPerSync,
		WALBytesPerSync:             walBytesPerSync,
		FlushSplitBytes:             int64(tuning.targetFileSize),
		TargetFileSizes:             targetFileSizes,
		L0CompactionThreshold:       tuning.l0CompactionThreshold,
		L0CompactionFileThreshold:   tuning.l0CompactionFileThreshold,
		L0StopWritesThreshold:       tuning.l0StopWritesThreshold,
		LBaseMaxBytes:               tuning.lBaseMaxBytes,
		DisableWAL:                  tuning.disableWAL,
		Logger:                      pebbleDebugLogger{log: logger},
		CompactionConcurrencyRange: func() (int, int) {
			maxCompactions := maxConcurrentCompactions()
			if maxCompactions < 1 {
				maxCompactions = 1
			}
			return 1, maxCompactions
		},
		Levels: levels,
	}
	opts.Experimental.EnableColumnarBlocks = func() bool { return false }
	opts.Experimental.EnableValueBlocks = func() bool { return false }
	if tuning.compactionScheduler != nil {
		opts.Experimental.CompactionScheduler = tuning.compactionScheduler
	}
	return opts
}

func pebbleNoCompression() *sstable.CompressionProfile {
	return sstable.NoCompression
}

func newPebbleFileCache(maxOpenFiles int) *pebble.FileCache {
	shards := runtime.GOMAXPROCS(0)
	if shards < 1 {
		shards = 1
	}
	return pebble.NewFileCache(shards, pebble.FileCacheSize(maxOpenFiles))
}

type pebbleDebugLogger struct {
	log zerolog.Logger
}

func (l pebbleDebugLogger) Infof(format string, args ...interface{}) {
	event := l.log.Debug()
	if !event.Enabled() {
		return
	}
	event.Msgf(format, args...)
}

func (l pebbleDebugLogger) Errorf(format string, args ...interface{}) {
	event := l.log.Error()
	if !event.Enabled() {
		return
	}
	event.Msgf(format, args...)
}

func (l pebbleDebugLogger) Fatalf(format string, args ...interface{}) {
	l.log.Fatal().Msgf(format, args...)
}

func pebbleMaxConcurrentCompactions() int {
	n := runtime.GOMAXPROCS(0) / 2
	if n < 1 {
		return 1
	}
	return n
}

func pebbleCellMaxConcurrentCompactions() int {
	return 2
}

func pebbleCellCompactionParallelism() int {
	n := runtime.GOMAXPROCS(0) * 2 / 3
	if n < 1 {
		return 1
	}
	max := cellDBShardCount * pebbleCellMaxConcurrentCompactions()
	if n > max {
		return max
	}
	return n
}

func (s *Store) Close() error {
	var firstErr error
	if err := s.syncPendingArtifactFiles(); err != nil && firstErr == nil {
		firstErr = err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return firstErr
	}
	s.closed = true
	s.hotClosing.Store(true)
	if s.hotRefs.Load() == 0 {
		s.signalHotDrained()
	}
	s.mu.Unlock()

	<-s.hotDrained
	if err := s.hot.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.closeCellGenerations(); err != nil && firstErr == nil {
		firstErr = err
	}
	s.mu.Lock()
	s.cells = nil
	s.cellGenerations = nil
	s.mu.Unlock()
	if s.hotCache != nil {
		s.hotCache.Unref()
	}
	return firstErr
}

func (s *Store) acquireHotDB(ctx context.Context) (*pebble.DB, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.hotClosing.Load() || s.hot == nil {
		return nil, errPebbleClosed
	}
	s.hotRefs.Add(1)
	return s.hot, nil
}

func (s *Store) releaseHotDB() {
	if s.hotRefs.Add(-1) == 0 && s.hotClosing.Load() {
		s.signalHotDrained()
	}
}

func (s *Store) signalHotDrained() {
	s.hotDrainOnce.Do(func() {
		close(s.hotDrained)
	})
}

func (s *Store) ensureWritable() error {
	if s.readOnly {
		return pebble.ErrReadOnly
	}
	return nil
}

func (s *Store) closeCellGenerations() error {
	var err error
	for id, cells := range s.cellGenerations {
		if cells == nil {
			continue
		}
		if closeErr := cells.close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close celldb generation %d: %w", id, closeErr))
		}
	}
	return err
}

func (s *Store) StateFilesDir() string {
	return filepath.Join(s.dir, "archive", "states")
}

func (s *Store) withHotBatch(fn func(batch *pebble.Batch) error) error {
	if err := s.ensureWritable(); err != nil {
		return err
	}

	db, err := s.acquireHotDB(context.Background())
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) setHotRecord(ctx context.Context, key, value []byte, writeOptions *pebble.WriteOptions) error {
	if err := s.ensureWritable(); err != nil {
		return err
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(key, value, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) deleteHotRecord(ctx context.Context, key []byte, writeOptions *pebble.WriteOptions) error {
	if err := s.ensureWritable(); err != nil {
		return err
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Delete(key, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) getHotCopy(ctx context.Context, key []byte) ([]byte, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()
	return pebbleReaderGetCopy(db, key)
}

func (s *Store) getCellCopy(ctx context.Context, hash []byte) ([]byte, error) {
	return s.getCellCopyFromGeneration(ctx, 0, hash)
}

func (s *Store) getCellCopyFromGeneration(ctx context.Context, generation uint64, hash []byte) ([]byte, error) {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return nil, err
	}
	defer cells.release()
	return cells.getCopy(hash)
}

func (s *Store) acquireCellStore(ctx context.Context, generation uint64) (*cellStore, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errPebbleClosed
	}
	cells, err := s.cellStoreForGenerationLocked(generation)
	if err != nil {
		return nil, err
	}
	if err = cells.acquire(); err != nil {
		return nil, err
	}
	return cells, nil
}

func (s *Store) ThrottleCellCompactions() func() {
	s.mu.RLock()
	releases := make([]func(), 0, len(s.cellGenerations))
	for _, cells := range s.cellGenerations {
		releases = append(releases, cells.throttleCompactions())
	}
	s.mu.RUnlock()

	if len(releases) == 0 {
		return func() {}
	}

	s.log.Info().
		Int("cell_generations", len(releases)).
		Msg("throttled cell compactions for foreground read")

	var once sync.Once
	return func() {
		once.Do(func() {
			for _, release := range releases {
				release()
			}
			s.log.Info().
				Int("cell_generations", len(releases)).
				Msg("resumed cell compactions after foreground read")
		})
	}
}

func (s *Store) flushCellDBs(generation uint64) error {
	cells, err := s.acquireCellStore(context.Background(), generation)
	if err != nil {
		return err
	}
	defer cells.release()
	return cells.flush()
}

func (s *Store) activeCellStoreLocked() (*cellStore, error) {
	return s.cellStoreForGenerationLocked(0)
}

func (s *Store) cellStoreForGenerationLocked(generation uint64) (*cellStore, error) {
	if generation == 0 {
		generation = s.activeCellGeneration
	}
	cells := s.cellGenerations[generation]
	if cells == nil {
		return nil, fmt.Errorf("%w: %d", errCellGenerationNotOpen, generation)
	}
	return cells, nil
}

func (s *Store) activeCellGenerationID() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, errPebbleClosed
	}
	if s.activeCellGeneration == 0 {
		return 0, fmt.Errorf("active cell generation is zero")
	}
	return s.activeCellGeneration, nil
}

func (s *Store) ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error) {
	select {
	case <-ctx.Done():
		return storage.CellGenerationInfo{}, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return storage.CellGenerationInfo{}, errPebbleClosed
	}
	if s.activeCellGeneration == 0 {
		return storage.CellGenerationInfo{}, fmt.Errorf("active cell generation is zero")
	}
	return storage.CellGenerationInfo{
		ID:                    s.activeCellGeneration,
		OriginPersistentState: s.activeCellOrigin,
	}, nil
}

func (s *Store) PendingCellGenerationMigration(ctx context.Context) (storage.CellGenerationInfo, error) {
	select {
	case <-ctx.Done():
		return storage.CellGenerationInfo{}, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return storage.CellGenerationInfo{}, errPebbleClosed
	}
	if s.pendingCellMigration == nil {
		return storage.CellGenerationInfo{}, storage.ErrNotFound
	}
	return storage.CellGenerationInfo{
		ID:                    s.pendingCellMigration.generation,
		OriginPersistentState: s.pendingCellMigration.origin,
	}, nil
}

func (s *Store) BeginCellGeneration(ctx context.Context, origin ton.BlockIDExt) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if err := s.ensureWritable(); err != nil {
		return 0, err
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return 0, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.hotWriteMu.Unlock()
		return 0, errPebbleClosed
	}
	if s.pendingCellMigration != nil {
		pending := *s.pendingCellMigration
		alreadyOpen := s.cellGenerations[pending.generation] != nil
		s.mu.Unlock()
		s.hotWriteMu.Unlock()

		if !pending.origin.Equals(&origin) {
			return 0, fmt.Errorf("pending cell generation migration uses origin %s, requested %s", storage.FormatBlockRef(pending.origin), storage.FormatBlockRef(origin))
		}
		if alreadyOpen {
			return pending.generation, nil
		}
		if err := s.openCellGeneration(ctx, pending.generation); err != nil {
			return 0, err
		}
		return pending.generation, nil
	}

	generation := s.nextCellGeneration
	if generation <= s.activeCellGeneration {
		s.mu.Unlock()
		s.hotWriteMu.Unlock()
		return 0, fmt.Errorf("next cell generation %d is not above active %d", generation, s.activeCellGeneration)
	}
	s.nextCellGeneration++
	pending := &cellGenerationPendingMigration{
		generation: generation,
		origin:     origin,
	}
	manifest := cellGenerationManifest{
		active:       s.activeCellGeneration,
		next:         s.nextCellGeneration,
		activeOrigin: s.activeCellOrigin,
		pending:      pending,
		retired:      cloneUint64Slice(s.retiredGenerations),
	}
	s.mu.Unlock()

	if err := db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		s.mu.Lock()
		s.nextCellGeneration--
		s.mu.Unlock()
		s.hotWriteMu.Unlock()
		return 0, err
	}

	s.mu.Lock()
	s.pendingCellMigration = cloneCellGenerationPendingMigration(pending)
	s.mu.Unlock()
	s.hotWriteMu.Unlock()

	if err := s.openCellGeneration(ctx, generation); err != nil {
		return 0, err
	}
	return generation, nil
}

func (s *Store) openCellGeneration(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	cells, err := openCellStore(s.dir, generation, s.cellCacheSize, s.cellShardMemTable, s.bytesPerSync, s.readOnly, s.log)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = cells.close()
		return errPebbleClosed
	}
	if s.cellGenerations[generation] != nil {
		_ = cells.close()
		return nil
	}
	s.cellGenerations[generation] = cells

	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("opened cell generation")
	return nil
}

func (s *Store) AbortCellGeneration(ctx context.Context, generation uint64) error {
	if err := s.closeAndRemoveCellGeneration(ctx, generation, false); err != nil {
		return err
	}
	return s.clearPendingCellGeneration(ctx, generation)
}

func (s *Store) DeleteCellGeneration(ctx context.Context, generation uint64) error {
	if err := s.closeAndRemoveCellGeneration(ctx, generation, false); err != nil {
		return err
	}
	return s.removeRetiredCellGeneration(ctx, generation)
}

func (s *Store) CleanupCellGeneration(ctx context.Context, generation uint64) error {
	if err := s.DeleteCellGeneration(ctx, generation); err != nil {
		return err
	}
	return nil
}

func (s *Store) CleanupRetiredCellGenerations(ctx context.Context) error {
	retired := s.retiredCellGenerationSnapshot()
	for _, generation := range retired {
		if err := s.CleanupCellGeneration(ctx, generation); err != nil {
			return fmt.Errorf("cleanup retired cell generation %d: %w", generation, err)
		}
	}
	return nil
}

func (s *Store) closeAndRemoveCellGeneration(ctx context.Context, generation uint64, allowActive bool) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.ensureWritable(); err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}
	if !allowActive && generation == s.activeCellGeneration {
		s.mu.Unlock()
		return fmt.Errorf("cannot remove active cell generation %d", generation)
	}
	cells := s.cellGenerations[generation]
	if cells != nil {
		delete(s.cellGenerations, generation)
	}
	s.mu.Unlock()

	var errs []error
	if err := cells.close(); err != nil {
		errs = append(errs, err)
	}
	for shard := 0; shard < cellDBShardCount; shard++ {
		if err := os.RemoveAll(cellGenerationShardDir(s.dir, generation, shard)); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("removed cell generation")
	return nil
}

func (s *Store) retiredCellGenerationSnapshot() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneUint64Slice(s.retiredGenerations)
}

func (s *Store) clearPendingCellGeneration(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}
	if s.pendingCellMigration == nil || s.pendingCellMigration.generation != generation {
		s.mu.Unlock()
		return nil
	}

	manifest := s.manifestLocked()
	manifest.pending = nil
	s.mu.Unlock()

	if err := db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		return err
	}

	s.mu.Lock()
	if s.pendingCellMigration != nil && s.pendingCellMigration.generation == generation {
		s.pendingCellMigration = nil
	}
	s.mu.Unlock()
	return nil
}

func (s *Store) removeRetiredCellGeneration(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}
	if !containsUint64(s.retiredGenerations, generation) {
		s.mu.Unlock()
		return nil
	}

	manifest := s.manifestLocked()
	manifest.retired = removeUint64(manifest.retired, generation)
	s.mu.Unlock()

	if err := db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		return err
	}

	s.mu.Lock()
	s.retiredGenerations = removeUint64(s.retiredGenerations, generation)
	s.mu.Unlock()
	return nil
}

func (s *Store) manifestLocked() cellGenerationManifest {
	return cellGenerationManifest{
		active:       s.activeCellGeneration,
		next:         s.nextCellGeneration,
		activeOrigin: s.activeCellOrigin,
		pending:      cloneCellGenerationPendingMigration(s.pendingCellMigration),
		retired:      cloneUint64Slice(s.retiredGenerations),
	}
}

func (s *Store) setHotUnique(batch *pebble.Batch, key, value []byte) error {
	current, err := pebbleReaderGetCopy(s.hot, key)
	if err == nil {
		if !bytes.Equal(current, value) {
			return fmt.Errorf("hot unique record %x already has different value", key)
		}
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, value, pebble.NoSync)
}

func pebbleReaderGetCopy(reader interface {
	Get(key []byte) ([]byte, io.Closer, error)
}, key []byte) ([]byte, error) {
	value, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return bytes.Clone(value), nil
}

func pebbleReaderHas(reader interface {
	Get(key []byte) ([]byte, io.Closer, error)
}, key []byte) (bool, error) {
	_, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if closer != nil {
		_ = closer.Close()
	}
	return true, nil
}

var (
	hotPrefixBlockMeta             = []byte{0x01}
	hotPrefixNextBlock             = []byte{0x02}
	hotPrefixBlockSeq              = []byte{0x03}
	hotPrefixBlockLT               = []byte{0x04}
	hotPrefixBlockUTime            = []byte{0x05}
	hotPrefixCurrentState          = []byte{0x06}
	hotPrefixStateMeta             = []byte{0x07}
	hotPrefixArchiveInfo           = []byte{0x0B}
	hotPrefixStateSync             = []byte{0x0C}
	hotPrefixBlockDataRef          = []byte{0x0D}
	hotPrefixProofRef              = []byte{0x0E}
	hotPrefixArchiveFile           = []byte{0x0F}
	hotPrefixZeroStateRef          = []byte{0x11}
	hotPrefixKeyProofRef           = []byte{0x12}
	hotPrefixStateFileRef          = []byte{0x13}
	hotPrefixVerifiedKey           = []byte{0x14}
	hotPrefixPackCommitted         = []byte{0x15}
	hotPrefixPackStart             = []byte{0x16}
	hotPrefixStateSerializer       = []byte{0x17}
	hotPrefixStateDescription      = []byte{0x18}
	hotPrefixCellGeneration        = []byte{0x19}
	hotPrefixArchivePackage        = []byte{0x1A}
	hotPrefixStateSerializerActive = []byte{0x1B}
	hotPrefixKeyBlockSeq           = []byte{0x1C}
)

func hotKeyCellGenerationManifest() []byte {
	return bytes.Clone(hotPrefixCellGeneration)
}

func hotKeyBlockMeta(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixBlockMeta, id)
}

func hotKeyNextBlock(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixNextBlock, id)
}

func hotKeyBlockSeqIndex(key storage.BlockHistoryKey, seqno uint32) []byte {
	buf := appendHistoryPrefix(hotPrefixBlockSeq, key)
	return binary.BigEndian.AppendUint32(buf, seqno)
}

func hotKeyKeyBlockSeqIndex(seqno uint32) []byte {
	return binary.BigEndian.AppendUint32(bytes.Clone(hotPrefixKeyBlockSeq), seqno)
}

func hotKeyBlockLTPrefix(key storage.BlockHistoryKey) []byte {
	return appendHistoryPrefix(hotPrefixBlockLT, key)
}

func hotKeyBlockLTIndex(meta *storage.BlockMeta) []byte {
	buf := hotKeyBlockLTPrefix(storage.BlockHistoryKey{Workchain: meta.ID.Workchain, Shard: meta.ID.Shard})
	buf = binary.BigEndian.AppendUint64(buf, meta.EndLT)
	return binary.BigEndian.AppendUint32(buf, meta.ID.SeqNo)
}

func hotKeyBlockLTSeek(key storage.BlockHistoryKey, lt uint64) []byte {
	buf := hotKeyBlockLTPrefix(key)
	buf = binary.BigEndian.AppendUint64(buf, lt)
	return binary.BigEndian.AppendUint32(buf, math.MaxUint32)
}

func hotKeyBlockLTSeekGE(key storage.BlockHistoryKey, lt uint64) []byte {
	buf := hotKeyBlockLTPrefix(key)
	buf = binary.BigEndian.AppendUint64(buf, lt)
	return binary.BigEndian.AppendUint32(buf, 0)
}

func hotKeyBlockUTimePrefix(key storage.BlockHistoryKey) []byte {
	return appendHistoryPrefix(hotPrefixBlockUTime, key)
}

func hotKeyBlockUTimeIndex(meta *storage.BlockMeta) []byte {
	buf := hotKeyBlockUTimePrefix(storage.BlockHistoryKey{Workchain: meta.ID.Workchain, Shard: meta.ID.Shard})
	buf = binary.BigEndian.AppendUint32(buf, meta.GenUTime)
	return binary.BigEndian.AppendUint32(buf, meta.ID.SeqNo)
}

func hotKeyBlockUTimeSeek(key storage.BlockHistoryKey, utime uint32) []byte {
	buf := hotKeyBlockUTimePrefix(key)
	buf = binary.BigEndian.AppendUint32(buf, utime)
	return binary.BigEndian.AppendUint32(buf, math.MaxUint32)
}

func hotKeyCurrentState() []byte {
	return bytes.Clone(hotPrefixCurrentState)
}

func hotKeyStateSyncProgress() []byte {
	return bytes.Clone(hotPrefixStateSync)
}

func hotKeyVerifiedKeyBlockProgress() []byte {
	return bytes.Clone(hotPrefixVerifiedKey)
}

func hotKeyPersistentStateSerializer() []byte {
	return bytes.Clone(hotPrefixStateSerializer)
}

func hotKeyPersistentStateSerializerActive() []byte {
	return bytes.Clone(hotPrefixStateSerializerActive)
}

func hotKeyPersistentStateDescription(masterchainSeqno uint32) []byte {
	buf := append([]byte(nil), hotPrefixStateDescription...)
	return binary.BigEndian.AppendUint32(buf, masterchainSeqno)
}

func hotKeyPersistentStateDescriptionPrefix() []byte {
	return bytes.Clone(hotPrefixStateDescription)
}

func hotKeyStateMeta(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixStateMeta, id)
}

func hotKeyStateMetaMasterchainPrefix() []byte {
	buf := append([]byte(nil), hotPrefixStateMeta...)
	buf = binary.BigEndian.AppendUint32(buf, ^uint32(0))
	return binary.BigEndian.AppendUint64(buf, uint64(1)<<63)
}

func hotKeyArchiveInfo(masterchainSeqno int32, workchain int32, shard int64) []byte {
	buf := append([]byte(nil), hotPrefixArchiveInfo...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(masterchainSeqno))
	buf = binary.BigEndian.AppendUint32(buf, uint32(workchain))
	return binary.BigEndian.AppendUint64(buf, uint64(shard))
}

func hotKeyBlockDataRef(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixBlockDataRef, id)
}

func hotKeyProofRef(kind storage.ServedProofKind, id ton.BlockIDExt) []byte {
	buf := append([]byte(nil), hotPrefixProofRef...)
	buf = append(buf, byte(proofKindOrder(kind)))
	return append(buf, encodeBlockID(id)...)
}

func hotKeyStoredProofRef(kind storage.ServedProofKind, id ton.BlockIDExt) []byte {
	if isKeyProofKind(kind) {
		return hotKeyKeyProofRef(kind, id)
	}
	return hotKeyProofRef(kind, id)
}

func hotKeyKeyProofRef(kind storage.ServedProofKind, id ton.BlockIDExt) []byte {
	buf := append([]byte(nil), hotPrefixKeyProofRef...)
	buf = append(buf, byte(proofKindOrder(kind)))
	return append(buf, encodeBlockID(id)...)
}

func hotKeyZeroStateRef(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixZeroStateRef, id)
}

func hotKeyPersistentStateFile(block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) []byte {
	buf := appendPrefixAndBlockID(hotPrefixStateFileRef, block)
	buf = append(buf, encodeBlockID(masterchainBlock)...)
	return binary.BigEndian.AppendUint64(buf, uint64(effectiveShard))
}

func hotKeyArchiveFile(archiveID int64) []byte {
	buf := append([]byte(nil), hotPrefixArchiveFile...)
	return binary.BigEndian.AppendUint64(buf, uint64(archiveID))
}

func hotKeyPackCommitted(path string) []byte {
	buf := append([]byte(nil), hotPrefixPackCommitted...)
	return append(buf, path...)
}

func hotKeyPackCommittedPrefix() []byte {
	return bytes.Clone(hotPrefixPackCommitted)
}

func hotKeyArchivePackageStart(seqno uint32) []byte {
	buf := append([]byte(nil), hotPrefixPackStart...)
	return binary.BigEndian.AppendUint32(buf, seqno)
}

func hotKeyArchivePackageStartPrefix() []byte {
	return bytes.Clone(hotPrefixPackStart)
}

func hotKeyArchivePackage(archiveID int64) []byte {
	buf := append([]byte(nil), hotPrefixArchivePackage...)
	return binary.BigEndian.AppendUint64(buf, uint64(archiveID))
}

func hotKeyArchivePackagePrefix() []byte {
	return bytes.Clone(hotPrefixArchivePackage)
}

func appendHistoryPrefix(prefix []byte, key storage.BlockHistoryKey) []byte {
	buf := append([]byte(nil), prefix...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(key.Workchain))
	return binary.BigEndian.AppendUint64(buf, uint64(key.Shard))
}

func appendPrefixAndBlockID(prefix []byte, id ton.BlockIDExt) []byte {
	buf := append([]byte(nil), prefix...)
	return append(buf, encodeBlockID(id)...)
}

func encodeBlockID(id ton.BlockIDExt) []byte {
	buf := make([]byte, 0, 4+8+4+32+32)
	buf = binary.BigEndian.AppendUint32(buf, uint32(id.Workchain))
	buf = binary.BigEndian.AppendUint64(buf, uint64(id.Shard))
	buf = binary.BigEndian.AppendUint32(buf, id.SeqNo)
	buf = append(buf, id.RootHash...)
	buf = append(buf, id.FileHash...)
	return buf
}

func decodeBlockID(data []byte) (ton.BlockIDExt, error) {
	if len(data) != 80 {
		return ton.BlockIDExt{}, fmt.Errorf("invalid block id size %d", len(data))
	}
	return ton.BlockIDExt{
		Workchain: int32(binary.BigEndian.Uint32(data[:4])),
		Shard:     int64(binary.BigEndian.Uint64(data[4:12])),
		SeqNo:     binary.BigEndian.Uint32(data[12:16]),
		RootHash:  bytes.Clone(data[16:48]),
		FileHash:  bytes.Clone(data[48:80]),
	}, nil
}

func encodeBlockMeta(meta *storage.BlockMeta) []byte {
	if meta == nil {
		return nil
	}
	buf := make([]byte, 0, 256)
	buf = append(buf, blockMetaVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(meta.Flags))
	buf = binary.BigEndian.AppendUint32(buf, meta.GenUTime)
	buf = binary.BigEndian.AppendUint64(buf, meta.StartLT)
	buf = binary.BigEndian.AppendUint64(buf, meta.EndLT)
	buf = appendLenBytes(buf, meta.StateRootHash)
	buf = appendLenBytes(buf, meta.StateFileHash)
	if meta.MasterchainRef == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = append(buf, encodeBlockID(*meta.MasterchainRef)...)
	}
	buf = append(buf, byte(len(meta.PrevRefs)))
	for _, ref := range meta.PrevRefs {
		buf = append(buf, encodeBlockID(ref)...)
	}
	return buf
}

type cellGenerationManifest struct {
	active       uint64
	next         uint64
	activeOrigin ton.BlockIDExt
	pending      *cellGenerationPendingMigration
	retired      []uint64
}

type cellGenerationPendingMigration struct {
	generation uint64
	origin     ton.BlockIDExt
}

func loadOrInitCellGenerationManifest(db *pebble.DB) (cellGenerationManifest, error) {
	key := hotKeyCellGenerationManifest()
	raw, err := pebbleReaderGetCopy(db, key)
	if err == nil {
		return decodeCellGenerationManifest(raw)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return cellGenerationManifest{}, err
	}
	hasRecords, err := pebbleHasAnyUserRecord(db)
	if err != nil {
		return cellGenerationManifest{}, err
	}
	if hasRecords {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest is missing in non-empty metadb")
	}

	manifest := cellGenerationManifest{
		active: initialCellGenerationID,
		next:   initialCellGenerationID + 1,
	}
	if err = db.Set(key, encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		return cellGenerationManifest{}, err
	}
	return manifest, nil
}

func loadCellGenerationManifest(db *pebble.DB) (cellGenerationManifest, error) {
	raw, err := pebbleReaderGetCopy(db, hotKeyCellGenerationManifest())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest is missing")
		}
		return cellGenerationManifest{}, err
	}
	return decodeCellGenerationManifest(raw)
}

func encodeCellGenerationManifest(manifest cellGenerationManifest) []byte {
	buf := make([]byte, 0, 1+8+8+80+1+8+80+4+len(manifest.retired)*8)
	buf = append(buf, cellGenerationManifestVersion)
	buf = binary.BigEndian.AppendUint64(buf, manifest.active)
	buf = binary.BigEndian.AppendUint64(buf, manifest.next)
	buf = appendManifestBlockID(buf, manifest.activeOrigin)
	if manifest.pending == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = binary.BigEndian.AppendUint64(buf, manifest.pending.generation)
		buf = appendManifestBlockID(buf, manifest.pending.origin)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(manifest.retired)))
	for _, generation := range manifest.retired {
		buf = binary.BigEndian.AppendUint64(buf, generation)
	}
	return buf
}

func appendManifestBlockID(buf []byte, id ton.BlockIDExt) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(id.Workchain))
	buf = binary.BigEndian.AppendUint64(buf, uint64(id.Shard))
	buf = binary.BigEndian.AppendUint32(buf, id.SeqNo)
	buf = appendFixedHash(buf, id.RootHash)
	return appendFixedHash(buf, id.FileHash)
}

func appendFixedHash(buf []byte, hash []byte) []byte {
	if len(hash) == 32 {
		return append(buf, hash...)
	}
	return append(buf, make([]byte, 32)...)
}

func decodeCellGenerationManifest(data []byte) (cellGenerationManifest, error) {
	if len(data) < 1+8+8+80+1+4 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest too small: %d", len(data))
	}
	if data[0] != cellGenerationManifestVersion {
		return cellGenerationManifest{}, fmt.Errorf("unsupported cell generation manifest version %d", data[0])
	}
	active := binary.BigEndian.Uint64(data[1:])
	next := binary.BigEndian.Uint64(data[9:])
	if active == 0 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest active generation is zero")
	}
	if next <= active {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest next generation %d is not above active %d", next, active)
	}
	origin, err := decodeBlockID(data[17 : 17+80])
	if err != nil {
		return cellGenerationManifest{}, err
	}
	pos := 17 + 80
	var pending *cellGenerationPendingMigration
	switch data[pos] {
	case 0:
		pos++
	case 1:
		if len(data) < pos+1+8+80+4 {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending payload too small: %d", len(data))
		}
		pos++
		pendingGeneration := binary.BigEndian.Uint64(data[pos:])
		pos += 8
		pendingOrigin, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return cellGenerationManifest{}, err
		}
		pos += 80
		if pendingGeneration == 0 {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending generation is zero")
		}
		if pendingGeneration == active {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending generation equals active generation %d", active)
		}
		if pendingGeneration >= next {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending generation %d is not below next generation %d", pendingGeneration, next)
		}
		pending = &cellGenerationPendingMigration{
			generation: pendingGeneration,
			origin:     pendingOrigin,
		}
	default:
		return cellGenerationManifest{}, fmt.Errorf("unsupported cell generation manifest pending flag %d", data[pos])
	}
	if len(data) < pos+4 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired payload too small: %d", len(data))
	}
	retiredCount := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4
	if len(data) != pos+retiredCount*8 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest size mismatch: %d", len(data))
	}
	retired := make([]uint64, 0, retiredCount)
	for i := 0; i < retiredCount; i++ {
		generation := binary.BigEndian.Uint64(data[pos:])
		pos += 8
		if generation == 0 {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired generation is zero")
		}
		if generation == active {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired generation equals active generation %d", active)
		}
		if pending != nil && generation == pending.generation {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired generation equals pending generation %d", generation)
		}
		retired = appendRetiredCellGeneration(retired, generation)
	}
	return cellGenerationManifest{
		active:       active,
		next:         next,
		activeOrigin: origin,
		pending:      pending,
		retired:      retired,
	}, nil
}

func pebbleHasAnyUserRecord(db *pebble.DB) (bool, error) {
	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = iter.Close() }()

	hasRecords := iter.First()
	if err = iter.Error(); err != nil {
		return false, err
	}
	return hasRecords, nil
}

func cloneCellGenerationPendingMigration(pending *cellGenerationPendingMigration) *cellGenerationPendingMigration {
	if pending == nil {
		return nil
	}
	cloned := *pending
	return &cloned
}

func cloneUint64Slice(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	return append([]uint64(nil), values...)
}

func appendRetiredCellGeneration(retired []uint64, generation uint64) []uint64 {
	if generation == 0 || containsUint64(retired, generation) {
		return retired
	}
	return append(retired, generation)
}

func removeUint64(values []uint64, value uint64) []uint64 {
	next := values[:0]
	for _, current := range values {
		if current == value {
			continue
		}
		next = append(next, current)
	}
	clear(values[len(next):])
	return next
}

func containsUint64(values []uint64, value uint64) bool {
	for _, current := range values {
		if current == value {
			return true
		}
	}
	return false
}

func decodeBlockMeta(id ton.BlockIDExt, data []byte) (*storage.BlockMeta, error) {
	if len(data) < 1+4+4+8+8+1+1+1+1 {
		return nil, fmt.Errorf("block meta payload too small")
	}
	if data[0] != blockMetaVersion {
		return nil, fmt.Errorf("unsupported block meta version %d", data[0])
	}
	pos := 1
	meta := &storage.BlockMeta{
		ID:       id,
		Flags:    storage.BlockMetaFlags(binary.BigEndian.Uint32(data[pos : pos+4])),
		GenUTime: binary.BigEndian.Uint32(data[pos+4 : pos+8]),
		StartLT:  binary.BigEndian.Uint64(data[pos+8 : pos+16]),
		EndLT:    binary.BigEndian.Uint64(data[pos+16 : pos+24]),
	}
	pos += 24
	var err error
	meta.StateRootHash, pos, err = readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	meta.StateFileHash, pos, err = readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	if pos >= len(data) {
		return nil, fmt.Errorf("block meta truncated")
	}
	switch data[pos] {
	case 0:
		pos++
	case 1:
		pos++
		if pos+80 > len(data) {
			return nil, fmt.Errorf("block meta masterchain ref truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		meta.MasterchainRef = &ref
		pos += 80
	default:
		return nil, fmt.Errorf("invalid block meta masterchain ref flag %d", data[pos])
	}
	if pos >= len(data) {
		return nil, fmt.Errorf("block meta prev refs count missing")
	}
	prevCount := int(data[pos])
	pos++
	meta.PrevRefs = make([]ton.BlockIDExt, 0, prevCount)
	for i := 0; i < prevCount; i++ {
		if pos+80 > len(data) {
			return nil, fmt.Errorf("block meta prev refs truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		meta.PrevRefs = append(meta.PrevRefs, ref)
		pos += 80
	}
	if pos != len(data) {
		return nil, fmt.Errorf("block meta has %d trailing bytes", len(data)-pos)
	}
	return meta, nil
}

func encodeBlockStateMeta(state *storage.BlockState) []byte {
	buf := make([]byte, 0, 169)
	buf = append(buf, blockStateMetaVersion)
	buf = appendLenBytes(buf, state.StateRootHash)
	buf = appendLenBytes(buf, state.StateCellHash)
	buf = appendLenBytes(buf, state.StateFileHash)
	if state.MasterchainRef == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = append(buf, encodeBlockID(*state.MasterchainRef)...)
	}
	return buf
}

func decodeBlockStateMeta(data []byte) ([]byte, []byte, []byte, *ton.BlockIDExt, error) {
	if len(data) < 1+1+1+1 {
		return nil, nil, nil, nil, fmt.Errorf("block state meta payload too small")
	}
	if data[0] != blockStateMetaVersion {
		return nil, nil, nil, nil, fmt.Errorf("unsupported block state meta version %d", data[0])
	}
	pos := 1

	root, pos, err := readLenBytes(data, pos)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cellHash, pos, err := readLenBytes(data, pos)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	file, pos, err := readLenBytes(data, pos)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if pos >= len(data) {
		return nil, nil, nil, nil, fmt.Errorf("block state meta masterchain ref flag missing")
	}
	var masterRef *ton.BlockIDExt
	switch data[pos] {
	case 0:
		pos++
	case 1:
		pos++
		if pos+80 > len(data) {
			return nil, nil, nil, nil, fmt.Errorf("block state meta masterchain ref truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, nil, nil, nil, err
		}
		masterRef = &ref
		pos += 80
	default:
		return nil, nil, nil, nil, fmt.Errorf("invalid block state meta masterchain ref flag %d", data[pos])
	}
	if pos != len(data) {
		return nil, nil, nil, nil, fmt.Errorf("block state meta has %d trailing bytes", len(data)-pos)
	}
	return root, cellHash, file, masterRef, nil
}

func encodeCurrentState(state *storage.CurrentState) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, currentStateVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(state.SyncedAt.UnixNano()))
	buf = binary.BigEndian.AppendUint32(buf, state.ShardClientSeqno)
	buf = append(buf, encodeBlockID(state.Masterchain.Block)...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(state.Shards)))
	for _, key := range storage.SortedShardKeys(state.Shards) {
		buf = append(buf, encodeBlockID(state.Shards[key].Block)...)
	}
	return buf
}

func decodeCurrentState(data []byte) (*storage.CurrentState, error) {
	if len(data) < 1+8+4+80+4 {
		return nil, fmt.Errorf("current state payload too small")
	}
	if data[0] != currentStateVersion {
		return nil, fmt.Errorf("unsupported current state version %d", data[0])
	}
	pos := 1
	state := &storage.CurrentState{
		SyncedAt: time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8]))),
		Shards:   map[storage.ShardKey]storage.BlockState{},
	}
	pos += 8
	state.ShardClientSeqno = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	master, err := decodeBlockID(data[pos : pos+80])
	if err != nil {
		return nil, err
	}
	state.Masterchain = storage.BlockState{Block: master}
	pos += 80
	shardCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	for i := 0; i < shardCount; i++ {
		if pos+80 > len(data) {
			return nil, fmt.Errorf("current state shards truncated")
		}
		block, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		pos += 80
		state.Shards[storage.ShardKeyFromBlock(block)] = storage.BlockState{Block: block}
	}
	if pos != len(data) {
		return nil, fmt.Errorf("current state has %d trailing bytes", len(data)-pos)
	}
	return state, nil
}

func encodeCellRecord(record *storage.CellRecord) []byte {
	buf := make([]byte, cellRecordEncodedLen(record.D2, record.Refs))
	encodeCellRecordTo(buf, record)
	return buf
}

func encodeCellRecordTo(buf []byte, record *storage.CellRecord) {
	slowRefs, compactRefs := cellRecordCompactRefLayout(record.Refs)
	pos := 0
	d1 := record.D1
	if compactRefs {
		d1 |= cellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = record.D2
	pos += 2

	copy(buf[pos:], record.Data)
	pos += len(record.Data)
	if compactRefs {
		buf[pos] = slowRefs
		pos++
	}
	for i, ref := range record.Refs {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			copy(buf[pos:pos+cellRecordHashSize], ref.Hashes)
			pos += cellRecordHashSize
			copy(buf[pos:pos+cellRecordDepthSize], ref.Depths)
			pos += cellRecordDepthSize
			continue
		}

		buf[pos] = ref.LevelMask
		pos++
		copy(buf[pos:], ref.Hashes)
		pos += len(ref.Hashes)
		copy(buf[pos:], ref.Depths)
		pos += len(ref.Depths)
	}
}

func cellRecordEncodedLen(d2 byte, refs []storage.CellRefRecord) int {
	size := 2 + int(d2/2+d2%2)
	slowRefs, compactRefs := cellRecordCompactRefLayout(refs)
	if compactRefs {
		size++
	}
	for i, ref := range refs {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			size += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		size += 1 + len(ref.Hashes) + len(ref.Depths)
	}
	return size
}

func cellRecordRefCommon(ref storage.CellRefRecord) bool {
	return ref.LevelMask == 0 && len(ref.Hashes) == cellRecordHashSize && len(ref.Depths) == cellRecordDepthSize
}

func cellRecordCompactRefLayout(refs []storage.CellRefRecord) (byte, bool) {
	if len(refs) == 0 {
		return 0, false
	}

	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	var slowRefs byte
	for i, ref := range refs {
		refSize := 1 + len(ref.Hashes) + len(ref.Depths)
		refsSize += refSize
		if cellRecordRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	return slowRefs, hasCommonRef && compactRefsSize <= refsSize
}

func decodeCellRecord(hash []byte, data []byte) (*storage.CellRecord, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("cell record payload too small")
	}
	return decodeCellRecordBytes(hash, data, true)
}

func decodeCellRecordBytes(hash []byte, data []byte, clone bool) (*storage.CellRecord, error) {
	if len(hash) != 32 {
		return nil, fmt.Errorf("cell hash size mismatch: %d", len(hash))
	}

	pos := 0
	if len(data)-pos < 2 {
		return nil, fmt.Errorf("cell record payload too small")
	}

	storedD1 := data[pos]
	compactRefs := storedD1&cellRecordCompactRefsFlag != 0
	record := &storage.CellRecord{
		Hash: hash,
		D1:   storedD1 &^ cellRecordCompactRefsFlag,
		D2:   data[pos+1],
	}
	if clone {
		record.Hash = bytes.Clone(hash)
	}
	pos += 2

	refsCount := int(record.D1 & 7)
	if refsCount > 4 {
		return nil, fmt.Errorf("invalid cell refs count %d", refsCount)
	}
	dataLen := int(record.D2/2 + record.D2%2)
	if len(data)-pos < dataLen {
		return nil, fmt.Errorf("cell record payload truncated")
	}
	record.Data = data[pos : pos+dataLen]
	if clone {
		record.Data = bytes.Clone(record.Data)
	}
	pos += dataLen

	record.Refs = make([]storage.CellRefRecord, 0, refsCount)
	var slowRefs byte
	if compactRefs && refsCount > 0 {
		if pos >= len(data) {
			return nil, fmt.Errorf("cell record compact ref layout truncated")
		}
		slowRefs = data[pos]
		pos++
		if slowRefs&^byte((1<<uint(refsCount))-1) != 0 {
			return nil, fmt.Errorf("cell record compact ref layout has invalid slow refs mask %d", slowRefs)
		}
	}
	for i := 0; i < refsCount; i++ {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			if len(data)-pos < cellRecordHashSize+cellRecordDepthSize {
				return nil, fmt.Errorf("cell record compact ref metadata truncated")
			}
			hashes := data[pos : pos+cellRecordHashSize]
			pos += cellRecordHashSize
			depths := data[pos : pos+cellRecordDepthSize]
			pos += cellRecordDepthSize
			if clone {
				hashes = bytes.Clone(hashes)
				depths = bytes.Clone(depths)
			}
			record.Refs = append(record.Refs, storage.CellRefRecord{
				LevelMask: 0,
				Hashes:    hashes,
				Depths:    depths,
			})
			continue
		}

		if pos >= len(data) {
			return nil, fmt.Errorf("cell record ref metadata truncated")
		}
		levelMask := data[pos]
		pos++
		hashesCount := storage.CellRefHashesCount(levelMask)
		hashesLen := hashesCount * 32
		depthsLen := hashesCount * 2
		if len(data)-pos < hashesLen+depthsLen {
			return nil, fmt.Errorf("cell record ref metadata truncated")
		}
		hashes := data[pos : pos+hashesLen]
		pos += hashesLen
		depths := data[pos : pos+depthsLen]
		pos += depthsLen
		if clone {
			hashes = bytes.Clone(hashes)
			depths = bytes.Clone(depths)
		}
		record.Refs = append(record.Refs, storage.CellRefRecord{
			LevelMask: levelMask,
			Hashes:    hashes,
			Depths:    depths,
		})
	}
	if pos != len(data) {
		return nil, fmt.Errorf("cell record payload has trailing bytes")
	}
	return record, nil
}

func encodeInt64(v int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return buf[:]
}

func encodeArtifactRef(ref *storage.ArtifactRef) []byte {
	path := []byte(ref.Path)
	buf := make([]byte, 0, 1+8+8+4+len(path))
	buf = append(buf, artifactRefVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(ref.Offset))
	buf = binary.BigEndian.AppendUint64(buf, uint64(ref.Size))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(path)))
	return append(buf, path...)
}

func decodeArtifactRef(data []byte) (*storage.ArtifactRef, error) {
	const fixed = 1 + 8 + 8 + 4
	if len(data) < fixed {
		return nil, fmt.Errorf("artifact ref payload truncated")
	}
	if data[0] != artifactRefVersion {
		return nil, fmt.Errorf("artifact ref version mismatch")
	}
	offset := int64(binary.BigEndian.Uint64(data[1:9]))
	size := int64(binary.BigEndian.Uint64(data[9:17]))
	pathLen := int(binary.BigEndian.Uint32(data[17:21]))
	if len(data) != fixed+pathLen {
		return nil, fmt.Errorf("artifact ref payload size mismatch")
	}
	if offset < 0 || size < 0 {
		return nil, fmt.Errorf("artifact ref has invalid range")
	}
	return &storage.ArtifactRef{
		Path:   string(data[fixed:]),
		Offset: offset,
		Size:   size,
	}, nil
}

func encodeArchivePackageMeta(meta archivePackageMeta) []byte {
	path := []byte(meta.path)
	buf := make([]byte, 0, 1+8+4+4+4+8+8+4+4+8+4+len(path))
	buf = append(buf, archivePackageVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(meta.archiveID))
	buf = binary.BigEndian.AppendUint32(buf, meta.baseSeq)
	buf = binary.BigEndian.AppendUint32(buf, meta.startSeq)
	buf = binary.BigEndian.AppendUint32(buf, uint32(meta.workchain))
	buf = binary.BigEndian.AppendUint64(buf, uint64(meta.shard))
	buf = binary.BigEndian.AppendUint64(buf, uint64(meta.size))
	buf = binary.BigEndian.AppendUint32(buf, meta.firstMasterSeq)
	buf = binary.BigEndian.AppendUint32(buf, meta.firstMasterUTime)
	buf = binary.BigEndian.AppendUint64(buf, meta.firstMasterLT)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(path)))
	return append(buf, path...)
}

func decodeArchivePackageMeta(data []byte) (archivePackageMeta, error) {
	const fixed = 1 + 8 + 4 + 4 + 4 + 8 + 8 + 4 + 4 + 8 + 4
	if len(data) < fixed {
		return archivePackageMeta{}, fmt.Errorf("archive package payload truncated")
	}
	if data[0] != archivePackageVersion {
		return archivePackageMeta{}, fmt.Errorf("archive package version mismatch")
	}
	pos := 1
	meta := archivePackageMeta{
		archiveID: int64(binary.BigEndian.Uint64(data[pos : pos+8])),
	}
	pos += 8
	meta.baseSeq = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.startSeq = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.workchain = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	meta.shard = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	meta.size = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	meta.firstMasterSeq = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.firstMasterUTime = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.firstMasterLT = binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8
	pathLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if len(data) != fixed+pathLen {
		return archivePackageMeta{}, fmt.Errorf("archive package payload size mismatch")
	}
	if meta.size < 0 {
		return archivePackageMeta{}, fmt.Errorf("archive package has invalid signed fields")
	}
	meta.path = string(data[pos:])
	return meta, nil
}

type persistentStateFileRecord struct {
	ref           *storage.ArtifactRef
	fileHash      []byte
	stateRootHash []byte
}

func encodePersistentStateFileRecord(file *storage.PersistentStateFile) []byte {
	ref := encodeArtifactRef(file.Ref)
	buf := make([]byte, 0, 1+1+len(file.FileHash)+1+len(file.StateRootHash)+4+len(ref))
	buf = append(buf, persistentStateVersion)
	buf = appendLenBytes(buf, file.FileHash)
	buf = appendLenBytes(buf, file.StateRootHash)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ref)))
	return append(buf, ref...)
}

func decodePersistentStateFileRecord(data []byte) (*persistentStateFileRecord, error) {
	if len(data) < 1+1+1+4 {
		return nil, fmt.Errorf("persistent state file payload truncated")
	}
	if data[0] != persistentStateVersion {
		return nil, fmt.Errorf("persistent state file version mismatch")
	}
	pos := 1

	fileHash, next, err := readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	pos = next

	stateRootHash, next, err := readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	pos = next

	if len(data)-pos < 4 {
		return nil, fmt.Errorf("persistent state file payload truncated")
	}
	refLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if refLen <= 0 || len(data)-pos != refLen {
		return nil, fmt.Errorf("persistent state file payload size mismatch")
	}
	ref, err := decodeArtifactRef(data[pos:])
	if err != nil {
		return nil, err
	}
	return &persistentStateFileRecord{
		ref:           ref,
		fileHash:      fileHash,
		stateRootHash: stateRootHash,
	}, nil
}

func appendLenBytes(dst []byte, data []byte) []byte {
	dst = append(dst, byte(len(data)))
	return append(dst, data...)
}

func readLenBytes(src []byte, pos int) ([]byte, int, error) {
	if pos >= len(src) {
		return nil, pos, fmt.Errorf("payload truncated")
	}
	ln := int(src[pos])
	pos++
	if pos+ln > len(src) {
		return nil, pos, fmt.Errorf("payload truncated")
	}
	return bytes.Clone(src[pos : pos+ln]), pos + ln, nil
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func proofKindOrder(kind storage.ServedProofKind) int {
	switch kind {
	case storage.ServedProofBlock:
		return 1
	case storage.ServedProofBlockLink:
		return 2
	case storage.ServedProofKeyBlock:
		return 3
	case storage.ServedProofKeyBlockLink:
		return 4
	default:
		return 0
	}
}

func blockMetaServedFlags(isLink bool) storage.BlockMetaFlags {
	flags := storage.BlockMetaHasServedFull
	if isLink {
		flags |= storage.BlockMetaServedFullIsLink
	}
	return flags
}
