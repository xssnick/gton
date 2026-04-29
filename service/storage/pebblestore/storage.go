package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flexserver/internal/logutil"
	"flexserver/service/archive/packfile"
	"flexserver/service/storage"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	defaultPebbleMetaCacheSize      = 64 << 20
	defaultPebbleCellTotalCacheSize = 4 << 30

	defaultPebbleMetaMemTableSize      = 16 << 20
	defaultPebbleCellTotalMemTableSize = 2 << 30

	defaultPebbleBytesPerSync = 8 << 20
	defaultPebbleWALBytesSync = 8 << 20

	defaultPebbleMetaTargetFileSize = 16 << 20
	defaultPebbleCellTargetFileSize = 256 << 20

	defaultPebbleMetaMemTableStopThreshold = 4
	defaultPebbleCellMemTableStopThreshold = 8

	defaultPebbleMetaL0CompactionThreshold = 4
	defaultPebbleCellL0CompactionThreshold = 16

	defaultPebbleMetaL0FileThreshold = 8
	defaultPebbleCellL0FileThreshold = 64

	defaultPebbleMetaL0StopWritesThreshold = 32
	defaultPebbleCellL0StopWritesThreshold = 256

	defaultPebbleCellLBaseMaxBytes = 1 << 30

	stateCellImportBatchTargetBytes = 128 << 20
	stateCellSaveProgressInterval   = 5 * time.Second
	archivePackageMasterchainBlocks = 100

	blockStateMetaVersion = 1
	artifactRefVersion    = 1

	cellRecordCompactRefsFlag = 0x10
	cellRecordHashSize        = 32
	cellRecordDepthSize       = 2
)

var errPebbleClosed = errors.New("pebble storage is closed")

type Options struct {
	Dir              string
	Logger           *zerolog.Logger
	MetaCacheSize    int64
	CellCacheSize    int64
	MetaMemTableSize int
	CellMemTableSize int
	BytesPerSync     int
	WALBytesPerSync  int
}

type Store struct {
	log zerolog.Logger

	hot      *pebble.DB
	cells    *cellStore
	dir      string
	hotOpts  *pebble.Options
	hotCache *pebble.Cache

	mu                 sync.RWMutex
	artifactMu         sync.Mutex
	pendingArchiveSync map[string]struct{}
	closed             bool
}

func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("storage dir is empty")
	}
	logger := logutil.WithComponent(opts.Logger, "pebblestore")
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
	if cellMemTableSize <= 0 {
		cellMemTableSize = defaultPebbleCellTotalMemTableSize
	}

	hotDir := filepath.Join(opts.Dir, "metadb")
	if err := os.MkdirAll(hotDir, 0o755); err != nil {
		return nil, fmt.Errorf("create metadb dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(opts.Dir, "packs", "archive"), 0o755); err != nil {
		return nil, fmt.Errorf("create archive pack dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(opts.Dir, "packs", "loose"), 0o755); err != nil {
		return nil, fmt.Errorf("create loose pack dir: %w", err)
	}

	hotCache := pebble.NewCache(opts.MetaCacheSize)
	hotLogger := logger.With().Str("db", "metadb").Logger()

	hotOpts := newMetaPebbleOptions(hotCache, metaMemTableSize, opts.BytesPerSync, opts.WALBytesPerSync, hotLogger)
	hot, err := pebble.Open(hotDir, hotOpts)
	if err != nil {
		hotCache.Unref()
		return nil, fmt.Errorf("open metadb: %w", err)
	}

	cellShardMemTable := cellShardMemTableSize(cellMemTableSize)
	cells, err := openCellStore(opts.Dir, opts.CellCacheSize, cellShardMemTable, opts.BytesPerSync, logger)
	if err != nil {
		_ = hot.Close()
		hotCache.Unref()
		return nil, err
	}

	store := &Store{
		log:                logger,
		hot:                hot,
		cells:              cells,
		dir:                opts.Dir,
		hotOpts:            hotOpts,
		hotCache:           hotCache,
		pendingArchiveSync: map[string]struct{}{},
	}
	logger.Info().
		Int64("meta_cache_size", opts.MetaCacheSize).
		Int64("cell_total_cache_size", opts.CellCacheSize).
		Int("meta_memtable_size", metaMemTableSize).
		Int("cell_total_memtable_size", cellMemTableSize).
		Int("cell_shard_memtable_size", cellShardMemTable).
		Int("cell_shards", cellDBShardCount).
		Bool("cell_disable_wal", true).
		Int("meta_target_file_size", defaultPebbleMetaTargetFileSize).
		Int("cell_target_file_size", defaultPebbleCellTargetFileSize).
		Int64("cell_lbase_max_bytes", defaultPebbleCellLBaseMaxBytes).
		Int("state_cell_import_batch_target_bytes", stateCellImportBatchTargetBytes).
		Int("cell_memtable_stop_writes_threshold", defaultPebbleCellMemTableStopThreshold).
		Int("cell_l0_compaction_threshold", defaultPebbleCellL0CompactionThreshold).
		Int("cell_l0_file_threshold", defaultPebbleCellL0FileThreshold).
		Int("cell_l0_stop_writes_threshold", defaultPebbleCellL0StopWritesThreshold).
		Int("cell_max_concurrent_compactions", pebbleCellMaxConcurrentCompactions()).
		Int("max_concurrent_compactions", pebbleMaxConcurrentCompactions()).
		Msg("configured pebble storage tuning")
	// Do not scan the full cell DB on startup just to populate console stats.
	return store, nil
}

type pebbleOptionsTuning struct {
	blockSize                   int
	compression                 pebble.Compression
	targetFileSize              int
	memTableStopWritesThreshold int
	l0CompactionThreshold       int
	l0CompactionFileThreshold   int
	l0StopWritesThreshold       int
	lBaseMaxBytes               int64
	disableWAL                  bool
}

func newMetaPebbleOptions(cache *pebble.Cache, memTableSize, bytesPerSync, walBytesPerSync int, logger zerolog.Logger) *pebble.Options {
	return newPebbleOptions(cache, memTableSize, bytesPerSync, walBytesPerSync, pebbleOptionsTuning{
		blockSize:                   4 << 10,
		compression:                 pebble.NoCompression,
		targetFileSize:              defaultPebbleMetaTargetFileSize,
		memTableStopWritesThreshold: defaultPebbleMetaMemTableStopThreshold,
		l0CompactionThreshold:       defaultPebbleMetaL0CompactionThreshold,
		l0CompactionFileThreshold:   defaultPebbleMetaL0FileThreshold,
		l0StopWritesThreshold:       defaultPebbleMetaL0StopWritesThreshold,
	}, logger)
}

func newCellPebbleOptions(cache *pebble.Cache, memTableSize, bytesPerSync int, logger zerolog.Logger) *pebble.Options {
	opts := newPebbleOptions(cache, memTableSize, bytesPerSync, 0, pebbleOptionsTuning{
		blockSize:                   4 << 10,
		compression:                 pebble.NoCompression,
		targetFileSize:              defaultPebbleCellTargetFileSize,
		memTableStopWritesThreshold: defaultPebbleCellMemTableStopThreshold,
		l0CompactionThreshold:       defaultPebbleCellL0CompactionThreshold,
		l0CompactionFileThreshold:   defaultPebbleCellL0FileThreshold,
		l0StopWritesThreshold:       defaultPebbleCellL0StopWritesThreshold,
		lBaseMaxBytes:               defaultPebbleCellLBaseMaxBytes,
		disableWAL:                  true,
	}, logger)
	opts.MaxConcurrentCompactions = pebbleCellMaxConcurrentCompactions
	return opts
}

func newPebbleOptions(cache *pebble.Cache, memTableSize, bytesPerSync, walBytesPerSync int, tuning pebbleOptionsTuning, logger zerolog.Logger) *pebble.Options {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		levels[i] = pebble.LevelOptions{
			BlockSize:      tuning.blockSize,
			IndexBlockSize: tuning.blockSize,
			FilterPolicy:   bloom.FilterPolicy(10),
			FilterType:     pebble.TableFilter,
			TargetFileSize: int64(tuning.targetFileSize),
			Compression:    tuning.compression,
		}
	}
	opts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                uint64(memTableSize),
		MemTableStopWritesThreshold: tuning.memTableStopWritesThreshold,
		BytesPerSync:                bytesPerSync,
		WALBytesPerSync:             walBytesPerSync,
		FlushSplitBytes:             int64(tuning.targetFileSize),
		L0CompactionThreshold:       tuning.l0CompactionThreshold,
		L0CompactionFileThreshold:   tuning.l0CompactionFileThreshold,
		L0StopWritesThreshold:       tuning.l0StopWritesThreshold,
		LBaseMaxBytes:               tuning.lBaseMaxBytes,
		DisableWAL:                  tuning.disableWAL,
		Logger:                      pebbleDebugLogger{log: logger},
		MaxConcurrentCompactions:    pebbleMaxConcurrentCompactions,
		Levels:                      levels,
	}
	return opts
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
	return 1
}

func (s *Store) Close() error {
	var firstErr error
	if err := s.syncPendingArchiveFiles(); err != nil && firstErr == nil {
		firstErr = err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return firstErr
	}
	s.closed = true

	if err := s.hot.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.cells.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if s.hotCache != nil {
		s.hotCache.Unref()
	}
	return firstErr
}

func (s *Store) SaveBlockFull(block *storage.ServedBlockFull) error {
	if block == nil {
		return fmt.Errorf("served block is nil")
	}

	meta, proofKinds := servedBlockFullMeta(block)
	blockRef, proofRef, err := s.servedBlockFullArtifactRefs(block, proofKinds)
	if err != nil {
		return err
	}
	return s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.setServedBlockFullArtifactRefs(batch, block, proofKinds, blockRef, proofRef); err != nil {
			return err
		}
		return s.setMergedBlockMeta(batch, meta)
	})
}

func servedBlockFullMeta(block *storage.ServedBlockFull) (*storage.BlockMeta, []storage.ServedProofKind) {
	meta := &storage.BlockMeta{
		ID:        block.ID,
		Flags:     blockMetaServedFlags(block.IsLink),
		UpdatedAt: time.Now(),
	}
	if block.Meta != nil {
		meta = storage.MergeBlockMeta(meta, block.Meta)
		meta.ID = block.ID
	}
	if len(block.Block) > 0 {
		meta.Mark(storage.BlockMetaHasBlockData)
		if block.Meta == nil {
			meta = storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Block)
		}
	}

	var proofKinds []storage.ServedProofKind
	if len(block.Proof) > 0 {
		proofKinds = storage.StoredProofKindsForBlock(block.IsLink, meta.Has(storage.BlockMetaIsKeyBlock))
		for _, kind := range proofKinds {
			meta.Mark(storage.BlockMetaFlagForProof(kind))
		}
	}
	return meta, proofKinds
}

func (s *Store) SaveArchiveImport(imported *storage.ServedArchiveImport) error {
	if imported == nil {
		return fmt.Errorf("served archive import is nil")
	}

	type fullWrite struct {
		block      *storage.ServedBlockFull
		proofKinds []storage.ServedProofKind
		blockRef   *storage.ArtifactRef
		proofRef   *storage.ArtifactRef
	}
	type blockDataWrite struct {
		block ton.BlockIDExt
		ref   *storage.ArtifactRef
	}
	type proofWrite struct {
		kind  storage.ServedProofKind
		block ton.BlockIDExt
		ref   *storage.ArtifactRef
	}

	metas := make(map[string]*storage.BlockMeta, len(imported.FullBlocks)+len(imported.BlockData)+len(imported.Proofs))
	fullWrites := make([]fullWrite, 0, len(imported.FullBlocks))
	for _, full := range imported.FullBlocks {
		meta, proofKinds := servedBlockFullMeta(full)
		blockRef, proofRef, err := s.servedBlockFullArtifactRefs(full, proofKinds)
		if err != nil {
			return err
		}
		mergeArchiveImportMeta(metas, meta)
		fullWrites = append(fullWrites, fullWrite{
			block:      full,
			proofKinds: proofKinds,
			blockRef:   blockRef,
			proofRef:   proofRef,
		})
	}

	blockDataWrites := make([]blockDataWrite, 0, len(imported.BlockData))
	for _, block := range imported.BlockData {
		if len(block.Data) == 0 {
			continue
		}
		ref := block.Ref
		if ref == nil {
			var err error
			ref, err = s.appendArtifactEntry(packfile.KindBlock, block.ID, block.Data)
			if err != nil {
				return err
			}
		}
		meta := &storage.BlockMeta{
			ID:        block.ID,
			Flags:     storage.BlockMetaHasBlockData,
			UpdatedAt: time.Now(),
		}
		mergeArchiveImportMeta(metas, storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Data))
		blockDataWrites = append(blockDataWrites, blockDataWrite{block: block.ID, ref: ref})
	}

	proofWrites := make([]proofWrite, 0, len(imported.Proofs))
	for _, proof := range imported.Proofs {
		if len(proof.Data) == 0 {
			continue
		}
		ref := proof.Ref
		if ref == nil {
			var err error
			ref, err = s.appendArtifactEntry(packEntryKindForProofKind(proof.Kind), proof.ID, proof.Data)
			if err != nil {
				return err
			}
		}
		mergeArchiveImportMeta(metas, &storage.BlockMeta{
			ID:        proof.ID,
			Flags:     storage.BlockMetaFlagForProof(proof.Kind),
			UpdatedAt: time.Now(),
		})
		proofWrites = append(proofWrites, proofWrite{kind: proof.Kind, block: proof.ID, ref: ref})
	}

	metaKeys := make([]string, 0, len(metas))
	for key := range metas {
		metaKeys = append(metaKeys, key)
	}
	sort.Strings(metaKeys)

	return s.withHotBatch(func(batch *pebble.Batch) error {
		for _, write := range fullWrites {
			if err := s.setServedBlockFullArtifactRefs(batch, write.block, write.proofKinds, write.blockRef, write.proofRef); err != nil {
				return err
			}
		}
		for _, write := range blockDataWrites {
			if err := s.setHotUnique(batch, hotKeyBlockDataRef(write.block), encodeArtifactRef(write.ref)); err != nil {
				return err
			}
		}
		for _, write := range proofWrites {
			if err := s.setHotUnique(batch, hotKeyProofRef(write.kind, write.block), encodeArtifactRef(write.ref)); err != nil {
				return err
			}
		}
		for _, link := range imported.Links {
			if err := s.setHotUnique(batch, hotKeyNextBlock(link.Prev), encodeBlockID(link.Next)); err != nil {
				return err
			}
		}
		for _, key := range metaKeys {
			if err := s.setMergedBlockMeta(batch, metas[key]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) servedBlockFullArtifactRefs(block *storage.ServedBlockFull, proofKinds []storage.ServedProofKind) (*storage.ArtifactRef, *storage.ArtifactRef, error) {
	if len(block.Block) == 0 && len(block.Proof) == 0 {
		return nil, nil, nil
	}

	blockRef := block.BlockRef
	if len(block.Block) > 0 && blockRef == nil {
		ref, err := s.appendArtifactEntry(packfile.KindBlock, block.ID, block.Block)
		if err != nil {
			return nil, nil, err
		}
		blockRef = ref
	}

	proofRef := block.ProofRef
	if len(block.Proof) > 0 && proofRef == nil {
		ref, err := s.appendArtifactEntry(packEntryKindForProof(block.IsLink), block.ID, block.Proof)
		if err != nil {
			return nil, nil, err
		}
		proofRef = ref
	}

	return blockRef, proofRef, nil
}

func (s *Store) setServedBlockFullArtifactRefs(batch *pebble.Batch, block *storage.ServedBlockFull, proofKinds []storage.ServedProofKind, blockRef *storage.ArtifactRef, proofRef *storage.ArtifactRef) error {
	if blockRef != nil {
		if err := s.setHotUnique(batch, hotKeyBlockDataRef(block.ID), encodeArtifactRef(blockRef)); err != nil {
			return err
		}
	}
	if proofRef != nil {
		for _, kind := range proofKinds {
			if err := s.setHotUnique(batch, hotKeyProofRef(kind, block.ID), encodeArtifactRef(proofRef)); err != nil {
				return err
			}
		}
	}
	return nil
}

func mergeArchiveImportMeta(metas map[string]*storage.BlockMeta, meta *storage.BlockMeta) {
	key := storage.BlockKey(meta.ID)
	merged := storage.MergeBlockMeta(metas[key], meta)
	merged.ID = meta.ID
	metas[key] = merged
}

func (s *Store) LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) {
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		key := hotKeyNextBlock(prev)
		val := encodeBlockID(next)
		return s.setHotUnique(batch, key, val)
	}); err != nil {
		s.log.Warn().
			Err(err).
			Str("prev", storage.FormatBlockRef(prev)).
			Str("next", storage.FormatBlockRef(next)).
			Msg("failed to persist next block link")
	}
}

func (s *Store) SaveBlockData(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) {
	if err := s.persistBlockData(block, data, ref); err != nil {
		s.log.Warn().
			Err(err).
			Str("block", storage.FormatBlockRef(block)).
			Msg("failed to persist block data")
	}
}

func (s *Store) persistBlockData(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	if len(data) == 0 {
		return nil
	}
	if ref == nil {
		var err error
		ref, err = s.appendArtifactEntry(packfile.KindBlock, block, data)
		if err != nil {
			return err
		}
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		return s.setHotUnique(batch, hotKeyBlockDataRef(block), encodeArtifactRef(ref))
	}); err != nil {
		return err
	}

	meta := &storage.BlockMeta{
		ID:        block,
		Flags:     storage.BlockMetaHasBlockData,
		UpdatedAt: time.Now(),
	}
	meta = storage.MergeBlockMetaFromBlockData(meta, block, data)
	return s.mergeAndStoreBlockMeta(meta)
}

func (s *Store) SaveBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) {
	if err := s.persistBlockProof(kind, block, data, ref); err != nil {
		s.log.Warn().
			Err(err).
			Str("kind", string(kind)).
			Str("block", storage.FormatBlockRef(block)).
			Msg("failed to persist block proof")
	}
}

func (s *Store) persistBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	if len(data) == 0 {
		return nil
	}
	if ref == nil {
		var err error
		ref, err = s.appendArtifactEntry(packEntryKindForProofKind(kind), block, data)
		if err != nil {
			return err
		}
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		return s.setHotUnique(batch, hotKeyProofRef(kind, block), encodeArtifactRef(ref))
	}); err != nil {
		return err
	}
	return s.mergeAndStoreBlockMeta(&storage.BlockMeta{
		ID:        block,
		Flags:     storage.BlockMetaFlagForProof(kind),
		UpdatedAt: time.Now(),
	})
}

func (s *Store) SaveArchiveFile(masterchainSeqno int32, workchain int32, shard int64, archiveID int64, path string) (string, error) {
	storedPath := s.archivePackPath(archiveID)
	s.artifactMu.Lock()
	stored, err := storeArchivePack(path, storedPath)
	if err != nil {
		s.artifactMu.Unlock()
		return "", err
	}
	if stored {
		s.pendingArchiveSync[storedPath] = struct{}{}
	}
	stat, err := os.Stat(storedPath)
	s.artifactMu.Unlock()
	if err != nil {
		return "", err
	}
	ref := &storage.ArtifactRef{
		Path: s.relativeArtifactPath(storedPath),
		Size: stat.Size(),
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.setHotMaybeReplace(batch, hotKeyArchiveInfo(masterchainSeqno, workchain, shard), encodeInt64(archiveID)); err != nil {
			return err
		}
		return s.setHotMaybeReplace(batch, hotKeyArchiveFile(archiveID), encodeArtifactRef(ref))
	}); err != nil {
		return "", err
	}
	return storedPath, nil
}

func (s *Store) syncPendingArchiveFiles() error {
	s.artifactMu.Lock()
	if len(s.pendingArchiveSync) == 0 {
		s.artifactMu.Unlock()
		return nil
	}

	paths := make([]string, 0, len(s.pendingArchiveSync))
	for path := range s.pendingArchiveSync {
		paths = append(paths, path)
	}
	s.artifactMu.Unlock()

	sort.Strings(paths)
	dirs := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if err := syncFile(path); err != nil {
			return fmt.Errorf("sync archive pack %s: %w", path, err)
		}
		dirs[filepath.Dir(path)] = struct{}{}
	}

	dirPaths := make([]string, 0, len(dirs))
	for dir := range dirs {
		dirPaths = append(dirPaths, dir)
	}
	sort.Strings(dirPaths)
	for _, dir := range dirPaths {
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("sync archive pack dir %s: %w", dir, err)
		}
	}

	s.artifactMu.Lock()
	for _, path := range paths {
		delete(s.pendingArchiveSync, path)
	}
	s.artifactMu.Unlock()
	return nil
}

func syncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

func syncDir(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err = file.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}

func (s *Store) BlockFull(ctx context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	meta, err := s.BlockMeta(ctx, block)
	if err != nil {
		return nil, err
	}
	if !meta.Has(storage.BlockMetaHasServedFull) {
		return nil, storage.ErrNotFound
	}

	data, err := s.BlockData(ctx, block)
	if err != nil {
		return nil, err
	}

	var proof []byte
	for _, kind := range storage.ProofCandidates(meta) {
		proof, err = s.BlockProof(ctx, kind, block)
		if err == nil {
			break
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	return &storage.ServedBlockFull{
		ID:     block,
		Proof:  proof,
		Block:  data,
		IsLink: meta.Has(storage.BlockMetaServedFullIsLink),
	}, nil
}

func (s *Store) NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	nextRaw, err := s.getHotCopy(ctx, hotKeyNextBlock(prev))
	if err != nil {
		return nil, err
	}
	next, err := decodeBlockID(nextRaw)
	if err != nil {
		return nil, err
	}
	return s.BlockFull(ctx, next)
}

func (s *Store) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	return s.readArtifact(ctx, hotKeyBlockDataRef(block))
}

func (s *Store) BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	return s.readArtifact(ctx, hotKeyProofRef(kind, block))
}

func (s *Store) ArchiveInfo(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) (int64, error) {
	raw, err := s.getHotCopy(ctx, hotKeyArchiveInfo(masterchainSeqno, workchain, shard))
	if errors.Is(err, storage.ErrNotFound) {
		rounded := roundArchiveMasterchainSeqno(masterchainSeqno)
		if rounded != masterchainSeqno {
			raw, err = s.getHotCopy(ctx, hotKeyArchiveInfo(rounded, workchain, shard))
		}
	}
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid archive info payload")
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func roundArchiveMasterchainSeqno(seqno int32) int32 {
	if seqno <= 0 {
		return seqno
	}
	return seqno - seqno%archivePackageMasterchainBlocks
}

func (s *Store) ArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) ([]byte, error) {
	raw, err := s.getHotCopy(ctx, hotKeyArchiveFile(archiveID))
	if err != nil {
		return nil, err
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		return nil, err
	}
	if offset < 0 || maxSize < 0 {
		return nil, fmt.Errorf("invalid archive range offset=%d max_size=%d", offset, maxSize)
	}
	if maxSize == 0 {
		return []byte{}, nil
	}
	if offset >= ref.Size {
		return nil, nil
	}
	size := ref.Size - offset
	if size > int64(maxSize) {
		size = int64(maxSize)
	}
	return packfile.ReadRange(s.artifactPath(ref.Path), offset, size)
}

func (s *Store) appendArtifactEntry(kind string, block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	path := s.loosePackPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	ptr, err := packfile.Append(file, packfile.EntryName(kind, block), data, true)
	if err != nil {
		return nil, err
	}
	return &storage.ArtifactRef{
		Path:   s.relativeArtifactPath(path),
		Offset: ptr.Offset,
		Size:   ptr.Size,
	}, nil
}

func (s *Store) readArtifact(ctx context.Context, key []byte) ([]byte, error) {
	raw, err := s.getHotCopy(ctx, key)
	if err != nil {
		return nil, err
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		return nil, err
	}
	data, err := packfile.ReadRange(s.artifactPath(ref.Path), ref.Offset, ref.Size)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != ref.Size {
		return nil, fmt.Errorf("artifact %s range offset=%d size=%d is truncated", ref.Path, ref.Offset, ref.Size)
	}
	return data, nil
}

func (s *Store) artifactPath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(s.dir, path)
}

func (s *Store) relativeArtifactPath(path string) string {
	rel, err := filepath.Rel(s.dir, path)
	if err != nil {
		return path
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return path
	}
	return rel
}

func (s *Store) loosePackPath() string {
	return filepath.Join(s.dir, "packs", "loose", "blocks.pack")
}

func (s *Store) archivePackPath(archiveID int64) string {
	return filepath.Join(s.dir, "packs", "archive", fmt.Sprintf("%d.pack", archiveID))
}

func packEntryKindForProof(isLink bool) string {
	if isLink {
		return packfile.KindProofLink
	}
	return packfile.KindProof
}

func packEntryKindForProofKind(kind storage.ServedProofKind) string {
	switch kind {
	case storage.ServedProofBlockLink, storage.ServedProofKeyBlockLink:
		return packfile.KindProofLink
	default:
		return packfile.KindProof
	}
}

func storeArchivePack(src string, dst string) (bool, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return false, err
	}
	if filepath.Clean(src) == filepath.Clean(dst) {
		return false, validateArchivePack(dst)
	}
	if _, err := os.Stat(dst); err == nil {
		if err = validateArchivePack(dst); err != nil {
			return false, err
		}
		_ = os.Remove(src)
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.Rename(src, dst); err == nil {
		return true, validateArchivePack(dst)
	}
	if err := copyFileSync(src, dst); err != nil {
		return false, err
	}
	if err := validateArchivePack(dst); err != nil {
		return false, err
	}
	return true, os.Remove(src)
}

func validateArchivePack(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	var magic [packfile.HeaderSize]byte
	if _, err = io.ReadFull(file, magic[:]); err != nil {
		return fmt.Errorf("read archive package magic: %w", err)
	}
	if binary.LittleEndian.Uint32(magic[:]) != packfile.PackageMagic {
		return fmt.Errorf("archive package magic mismatch")
	}
	return nil
}

func copyFileSync(src string, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func (s *Store) SaveCurrentState(ctx context.Context, state *storage.CurrentState) error {
	if state == nil {
		return fmt.Errorf("current state is nil")
	}
	return s.saveCurrentStateRecord(ctx, hotKeyCurrentState(), state, pebble.NoSync)
}

func (s *Store) SaveStateSyncProgress(ctx context.Context, state *storage.CurrentState) error {
	if state == nil {
		return fmt.Errorf("state sync progress is nil")
	}
	return s.saveCurrentStateRecord(ctx, hotKeyStateSyncProgress(), state, pebble.Sync)
}

func (s *Store) saveCurrentStateRecord(ctx context.Context, key []byte, state *storage.CurrentState, writeOptions *pebble.WriteOptions) error {
	if err := s.saveBlockStateIfMissing(ctx, &state.Masterchain); err != nil {
		return err
	}
	for _, key := range storage.SortedShardKeys(state.Shards) {
		shard := state.Shards[key]
		if err := s.saveBlockStateIfMissing(ctx, &shard); err != nil {
			return err
		}
	}

	payload := encodeCurrentState(state)
	return s.setHotRecord(ctx, key, payload, writeOptions)
}

func (s *Store) saveBlockStateIfMissing(ctx context.Context, state *storage.BlockState) error {
	if state.Cell == nil && len(state.StateRootHash) == 0 {
		return nil
	}
	exists, err := s.blockStateExists(ctx, state.Block)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.SaveBlockState(ctx, state)
}

func (s *Store) blockStateExists(ctx context.Context, block ton.BlockIDExt) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, errPebbleClosed
	}
	return pebbleReaderHas(s.hot, hotKeyStateMeta(block))
}

func (s *Store) CurrentState(ctx context.Context) (*storage.CurrentState, error) {
	return s.currentStateRecord(ctx, hotKeyCurrentState(), "current", true)
}

func (s *Store) StateSyncProgress(ctx context.Context) (*storage.CurrentState, error) {
	return s.currentStateRecord(ctx, hotKeyStateSyncProgress(), "state sync progress", false)
}

func (s *Store) ClearStateSyncProgress(ctx context.Context) error {
	return s.deleteHotRecord(ctx, hotKeyStateSyncProgress(), pebble.Sync)
}

func (s *Store) currentStateRecord(ctx context.Context, key []byte, label string, missingMetaIsAbsent bool) (*storage.CurrentState, error) {
	raw, err := s.getHotCopy(ctx, key)
	if err != nil {
		return nil, err
	}
	state, err := decodeCurrentState(raw)
	if err != nil {
		return nil, err
	}
	master, err := s.blockStateMeta(ctx, state.Masterchain.Block)
	if err == nil {
		state.Masterchain = master
	} else if errors.Is(err, storage.ErrNotFound) {
		if missingMetaIsAbsent {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("load %s masterchain state %s: block state metadata is missing", label, storage.FormatBlockRef(state.Masterchain.Block))
	} else {
		return nil, fmt.Errorf("load %s masterchain state %s: %w", label, storage.FormatBlockRef(state.Masterchain.Block), err)
	}

	for key, shard := range state.Shards {
		loaded, err := s.blockStateMeta(ctx, shard.Block)
		if err == nil {
			state.Shards[key] = loaded
			continue
		}
		if errors.Is(err, storage.ErrNotFound) {
			if missingMetaIsAbsent {
				return nil, storage.ErrNotFound
			}
			return nil, fmt.Errorf("load %s shard state %s: block state metadata is missing", label, storage.FormatBlockRef(shard.Block))
		}
		return nil, fmt.Errorf("load %s shard state %s: %w", label, storage.FormatBlockRef(shard.Block), err)
	}
	return state, nil
}

type preparedBlockStateSave struct {
	original     *storage.BlockState
	saved        storage.BlockState
	lazyRoot     *cell.Cell
	cellSyncHash cell.Hash
	syncCells    bool
	flushCells   bool
}

func (s *Store) SaveBlockState(ctx context.Context, state *storage.BlockState) error {
	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, []*storage.BlockState{state})
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, nil, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func (s *Store) SaveBlockStateAndCurrentState(ctx context.Context, block *storage.BlockState, current *storage.CurrentState) error {
	var blocks []*storage.BlockState
	if block != nil {
		blocks = []*storage.BlockState{block}
	}
	return s.SaveBlockStatesAndCurrentState(ctx, blocks, current)
}

func (s *Store) SaveBlockStatesAndCurrentState(ctx context.Context, blocks []*storage.BlockState, current *storage.CurrentState) error {
	if current == nil {
		return fmt.Errorf("current state is nil")
	}

	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, blocks)
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, current, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func (s *Store) prepareBlockStatesForSave(ctx context.Context, states []*storage.BlockState) ([]preparedBlockStateSave, time.Duration, error) {
	started := time.Now()
	prepared := make([]preparedBlockStateSave, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	exists := newStateCellExistenceCache()
	for _, state := range states {
		if state == nil {
			continue
		}
		key := storage.BlockKey(state.Block)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		next, err := s.prepareBlockStateForSave(ctx, state, exists)
		if err != nil {
			return nil, 0, err
		}
		prepared = append(prepared, next)
	}
	return prepared, time.Since(started), nil
}

func (s *Store) prepareBlockStateForSave(ctx context.Context, state *storage.BlockState, exists *stateCellExistenceCache) (preparedBlockStateSave, error) {
	var zero cell.Hash
	var prepared preparedBlockStateSave
	if state == nil {
		return prepared, fmt.Errorf("block state is nil")
	}
	saved := *state
	if len(saved.StateRootHash) == 0 && saved.Cell != nil {
		hash := saved.Cell.HashKey(0)
		saved.StateRootHash = hash[:]
	}
	if len(saved.StateCellHash) == 0 && saved.Cell != nil {
		hash := saved.Cell.HashKey()
		saved.StateCellHash = hash[:]
	}
	if len(saved.StateRootHash) == 0 {
		return prepared, fmt.Errorf("block state root hash is empty")
	}
	if len(saved.StateCellHash) == 0 {
		return prepared, fmt.Errorf("block state cell hash is empty")
	}
	if len(saved.StateCellHash) != len(zero) {
		return prepared, fmt.Errorf("block state cell hash size mismatch: got %d", len(saved.StateCellHash))
	}

	var cellSyncHash cell.Hash
	copy(cellSyncHash[:], saved.StateCellHash)
	var lazyRoot *cell.Cell
	syncCells := false
	flushCells := false

	if saved.Cell != nil {
		syncCells = true
		if saved.Cell.IsLazy() {
			lazyRoot = saved.Cell
		} else {
			cellSyncHash = saved.Cell.HashKey()
			var err error
			flushCells, err = s.saveStateCellTreeWithCache(ctx, saved.Block, saved.Cell, nil, saved.CellsCount, saved.ReusedStateCells, saved.ReusedStateRefs, exists)
			if err != nil {
				return prepared, err
			}
			lazyRoot, err = s.loadLazyCell(ctx, cellSyncHash[:])
			if err != nil {
				return prepared, fmt.Errorf("load persisted lazy state root: %w", err)
			}
		}
	}

	return preparedBlockStateSave{
		original:     state,
		saved:        saved,
		lazyRoot:     lazyRoot,
		cellSyncHash: cellSyncHash,
		syncCells:    syncCells,
		flushCells:   flushCells,
	}, nil
}

func (s *Store) savePreparedBlockStateRecords(prepared []preparedBlockStateSave, current *storage.CurrentState, cellsElapsed time.Duration) error {
	flushCells := false
	for _, state := range prepared {
		if state.flushCells {
			flushCells = true
			break
		}
	}

	flushStarted := time.Now()
	if flushCells {
		if err := s.flushCellDBs(); err != nil {
			return fmt.Errorf("flush state cells before metadata marker: %w", err)
		}
	}
	flushElapsed := time.Since(flushStarted)

	if err := s.syncPendingArchiveFiles(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errPebbleClosed
	}

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, state := range prepared {
		if state.syncCells {
			if err := batch.Set(hotKeyStateCellSync(state.saved.Block), encodeStateCellSync(state.cellSyncHash, state.saved.CellsCount), pebble.NoSync); err != nil {
				return err
			}
		}
		if err := s.setHotMaybeReplace(batch, hotKeyStateMeta(state.saved.Block), encodeBlockStateMeta(&state.saved)); err != nil {
			return err
		}
		if err := s.setMergedBlockMeta(batch, storage.BuildBlockMetaFromState(state.saved)); err != nil {
			return err
		}
	}
	if current != nil {
		if err := s.setHotMaybeReplace(batch, hotKeyCurrentState(), encodeCurrentState(current)); err != nil {
			return err
		}
	}

	hotSyncStarted := time.Now()
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	hotSyncElapsed := time.Since(hotSyncStarted)

	s.logBlockStateCheckpoint(prepared, current, flushCells, cellsElapsed, flushElapsed, hotSyncElapsed)
	return nil
}

func (s *Store) replacePreparedBlockStatesWithLazyRoots(prepared []preparedBlockStateSave) error {
	for _, state := range prepared {
		if state.saved.Cell == nil || state.saved.Parsed == nil {
			continue
		}
		if err := s.replaceBlockStateWithLazyRoot(state.original, state.saved, state.lazyRoot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) logBlockStateCheckpoint(prepared []preparedBlockStateSave, current *storage.CurrentState, flushCells bool, cellsElapsed time.Duration, flushElapsed time.Duration, hotSyncElapsed time.Duration) {
	metrics := s.cells.metrics()
	event := s.log.Debug().
		Int("states", len(prepared)).
		Bool("flush_cells", flushCells).
		Dur("save_cells_batch_elapsed", cellsElapsed).
		Dur("flush_cell_dbs_elapsed", flushElapsed).
		Dur("hot_metadata_sync_elapsed", hotSyncElapsed).
		Int64("cell_flush_count", metrics.flushCount).
		Uint64("cell_ingest_count", metrics.ingestCount).
		Int64("cell_l0_files", metrics.l0Files).
		Int64("cell_l0_size", metrics.l0Size).
		Uint64("cell_compaction_debt", metrics.compactionDebt).
		Int64("cell_compactions_in_progress", metrics.compactionsInProgress).
		Int64("cell_compaction_bytes_pending", metrics.compactionBytesPending).
		Uint64("cell_memtable_size", metrics.memTableSize).
		Int64("cell_memtable_count", metrics.memTableCount)

	if current != nil {
		event.
			Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Uint32("shard_client_seqno", current.ShardClientSeqno).
			Int("shards", len(current.Shards))
	}

	event.Msg("block state checkpoint persisted")
}

func (s *Store) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64) (*cell.Cell, error) {
	roots, err := s.importStateCellTrees(ctx, []stateCellTreeImport{{
		block:       block,
		root:        root,
		parsedCells: parsedCells,
		totalCells:  totalCells,
	}})
	if err != nil {
		return nil, err
	}
	return roots[0], nil
}

func (s *Store) ImportStateCellTrees(ctx context.Context, trees []storage.StateCellTreeImport) ([]*cell.Cell, error) {
	imports := make([]stateCellTreeImport, len(trees))
	for i, tree := range trees {
		imports[i] = stateCellTreeImport{
			block:       tree.Block,
			root:        tree.Root,
			parsedCells: tree.ParsedCells,
			totalCells:  tree.TotalCells,
		}
	}
	return s.importStateCellTrees(ctx, imports)
}

type stateCellTreeImport struct {
	block            ton.BlockIDExt
	root             *cell.Cell
	parsedCells      []cell.Cell
	totalCells       uint64
	reusedStateCells []cell.MerkleUpdateReusedCell
	reusedStateRefs  []cell.MerkleUpdateReusedRef
}

type stateCellTreeSync struct {
	block     ton.BlockIDExt
	rootHash  cell.Hash
	cellCount uint64
}

func (s *Store) importStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64, reusedStateCells []cell.MerkleUpdateReusedCell, reusedStateRefs []cell.MerkleUpdateReusedRef) (*cell.Cell, error) {
	roots, err := s.importStateCellTrees(ctx, []stateCellTreeImport{{
		block:            block,
		root:             root,
		parsedCells:      parsedCells,
		totalCells:       totalCells,
		reusedStateCells: reusedStateCells,
		reusedStateRefs:  reusedStateRefs,
	}})
	if err != nil {
		return nil, err
	}
	return roots[0], nil
}

func (s *Store) importStateCellTrees(ctx context.Context, trees []stateCellTreeImport) ([]*cell.Cell, error) {
	if len(trees) == 0 {
		return nil, nil
	}

	roots := make([]*cell.Cell, len(trees))
	syncs := make([]stateCellTreeSync, len(trees))
	for i, tree := range trees {
		if tree.root == nil {
			return nil, fmt.Errorf("state cell tree root is nil")
		}

		rootCellHash := tree.root.HashKey()
		if _, err := s.saveStateCellTree(ctx, tree.block, tree.root, tree.parsedCells, tree.totalCells, tree.reusedStateCells, tree.reusedStateRefs); err != nil {
			return nil, err
		}
		syncs[i] = stateCellTreeSync{
			block:     tree.block,
			rootHash:  rootCellHash,
			cellCount: tree.totalCells,
		}
	}

	if err := s.syncStateCellTrees(syncs); err != nil {
		return nil, fmt.Errorf("sync state cells: %w", err)
	}

	for i, sync := range syncs {
		lazyRoot, err := s.loadLazyCell(ctx, sync.rootHash[:])
		if err != nil {
			return nil, fmt.Errorf("load persisted lazy state root: %w", err)
		}
		roots[i] = lazyRoot

		s.log.Debug().
			Str("block", storage.FormatBlockRef(sync.block)).
			Uint64("cells", sync.cellCount).
			Msg("state cell tree imported and switched to lazy celldb root")
	}
	return roots, nil
}

func (s *Store) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, uint64, error) {
	raw, err := s.getHotCopy(ctx, hotKeyStateCellSync(block))
	if err != nil {
		return nil, 0, err
	}

	cellHash, cellsCount, err := decodeStateCellSync(raw)
	if err != nil {
		return nil, 0, err
	}

	root, err := s.loadLazyCell(ctx, cellHash[:])
	if err != nil {
		return nil, 0, err
	}
	if len(rootHash) > 0 {
		hash := root.HashKey(0)
		if !bytes.Equal(hash[:], rootHash) {
			return nil, 0, storage.ErrNotFound
		}
	}
	return root, cellsCount, nil
}

func (s *Store) syncStateCellTree(block ton.BlockIDExt, rootHash cell.Hash, totalCells uint64) error {
	return s.syncStateCellTrees([]stateCellTreeSync{{
		block:     block,
		rootHash:  rootHash,
		cellCount: totalCells,
	}})
}

func (s *Store) syncStateCellTrees(syncs []stateCellTreeSync) error {
	if err := s.flushCellDBs(); err != nil {
		return fmt.Errorf("flush state cells before sync marker: %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}

	s.log.Debug().
		Int("trees", len(syncs)).
		Msg("syncing persisted state cells before returning lazy roots")

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, sync := range syncs {
		if err := batch.Set(hotKeyStateCellSync(sync.block), encodeStateCellSync(sync.rootHash, sync.cellCount), pebble.NoSync); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) replaceBlockStateWithLazyRoot(state *storage.BlockState, saved storage.BlockState, root *cell.Cell) error {
	var err error
	if root == nil {
		root, err = s.loadLazyCell(context.Background(), saved.StateCellHash)
		if err != nil {
			return fmt.Errorf("load persisted lazy state root: %w", err)
		}
	}

	state.Block = saved.Block
	state.StateRootHash = bytes.Clone(saved.StateRootHash)
	state.StateCellHash = bytes.Clone(saved.StateCellHash)
	state.StateFileHash = bytes.Clone(saved.StateFileHash)
	state.CellsCount = saved.CellsCount
	state.Cell = root
	state.Parsed = saved.Parsed
	state.DownloadedAt = saved.DownloadedAt
	state.ReusedStateCells = nil
	state.ReusedStateRefs = nil

	s.log.Debug().
		Str("block", storage.FormatBlockRef(saved.Block)).
		Uint64("cells", saved.CellsCount).
		Msg("block state switched to lazy celldb root")
	return nil
}

func (s *Store) saveStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64, reusedStateCells []cell.MerkleUpdateReusedCell, reusedStateRefs []cell.MerkleUpdateReusedRef) (bool, error) {
	return s.saveStateCellTreeWithCache(ctx, block, root, parsedCells, totalCells, reusedStateCells, reusedStateRefs, nil)
}

func (s *Store) saveStateCellTreeWithCache(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64, reusedStateCells []cell.MerkleUpdateReusedCell, reusedStateRefs []cell.MerkleUpdateReusedRef, exists *stateCellExistenceCache) (bool, error) {
	return s.saveStateCellTreeWithCacheLogging(ctx, block, root, parsedCells, totalCells, reusedStateCells, reusedStateRefs, exists, "dfs", false)
}

func (s *Store) saveStateCellTreeWithCacheLogging(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64, reusedStateCells []cell.MerkleUpdateReusedCell, reusedStateRefs []cell.MerkleUpdateReusedRef, exists *stateCellExistenceCache, source string, quiet bool) (bool, error) {
	if len(parsedCells) > 0 {
		return s.saveParsedStateCellsBatch(ctx, block, parsedCells, totalCells)
	}

	started := time.Now()
	lastLog := started

	if !quiet {
		event := s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Str("source", source).
			Uint64("total_cells", totalCells).
			Int("reused_state_cells", len(reusedStateCells))
		addCellProgress(event, 0, totalCells)
		event.Msg("persisting state cells")
	}

	writer, err := s.newStateCellBatchWriter(exists)
	if err != nil {
		return false, err
	}
	defer writer.close()

	var processed int64
	var applied int64
	var bytesWritten int64
	var skippedReusedState int64
	var skippedExisting int64

	logProgress := func(done bool) {
		if quiet {
			return
		}
		event := s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Str("source", source).
			Int64("processed_cells", processed).
			Int64("applied_cells", applied).
			Int64("skipped_reused_state_cells", skippedReusedState).
			Int64("skipped_existing_cells", skippedExisting).
			Int64("bytes", bytesWritten).
			Uint64("total_cells", totalCells).
			Dur("elapsed", time.Since(started)).
			Str("speed", formatCellRate(processed, time.Since(started)))
		addCellProgress(event, processed, totalCells)
		if done {
			event.Msg("state cells persisted")
			return
		}
		event.Int("pending_batch_cells", writer.pendingCells()).Msg("state cell persistence progress")
	}

	flush := func() error {
		stats, err := writer.flush()
		if err != nil {
			return err
		}
		applied += stats.cells
		bytesWritten += stats.bytes

		if time.Since(lastLog) >= stateCellSaveProgressInterval {
			logProgress(false)
			lastLog = time.Now()
		}
		return nil
	}

	reusedStateCellRefs := make(map[cell.Hash]*cell.Cell, len(reusedStateCells))
	for _, reused := range reusedStateCells {
		if reused.Cell == nil {
			continue
		}
		reusedStateCellRefs[reused.Hash] = reused.Cell
	}
	reusedStateRefEdges := make(map[stateCellReusedRefKey]cell.MerkleUpdateReusedRef, len(reusedStateRefs))
	for _, reusedRef := range reusedStateRefs {
		if reusedRef.RawCell == nil || reusedRef.RefIndex < 0 || reusedRef.RefIndex >= 4 {
			continue
		}
		reusedStateRefEdges[stateCellReusedRefKey{
			parentHash: reusedRef.ParentHash,
			refIndex:   reusedRef.RefIndex,
		}] = reusedRef
	}
	reusedCellExists := func(hash cell.Hash) (bool, error) {
		raw, ok := reusedStateCellRefs[hash]
		if !ok {
			return false, nil
		}
		if raw.IsLazy() {
			return true, nil
		}
		exists, err := writer.has(hash)
		if err != nil {
			return false, err
		}
		return exists, nil
	}

	stack := make([]*cell.Cell, 0, 1024)
	stack = append(stack, root)
	visited := make(map[cell.Hash]struct{}, cellVisitSetCapacity(totalCells))
	rootHash := stateCellStorageHash(root)
	visited[rootHash] = struct{}{}

	for len(stack) > 0 {
		if processed&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
			}
		}

		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]
		currentMeta := current.GetMetadata()
		currentHash := currentMeta.Hash

		if exists, err := reusedCellExists(currentHash); err != nil {
			return false, err
		} else if exists {
			skippedReusedState++
			processed++
			continue
		}
		if current.IsLazy() {
			skippedExisting++
			processed++
			continue
		}

		refs := currentMeta.Refs
		for i := 0; i < len(refs); i++ {
			refFromReused := false
			var refCell *cell.Cell
			if reusedRef, ok := reusedStateRefEdges[stateCellReusedRefKey{parentHash: currentHash, refIndex: i}]; ok {
				refCell = reusedRef.RawCell
				refs[i] = stateCellRefMetadata(reusedRef.RawCell)
				refFromReused = true
			} else {
				refHash := refs[i].Hash
				if raw, ok := reusedStateCellRefs[refHash]; ok {
					refCell = raw
					refs[i] = stateCellRefMetadata(raw)
					refFromReused = true
				}
			}

			ref := refs[i]
			refHash := ref.Hash
			if refFromReused {
				if ref.Lazy {
					skippedReusedState++
					continue
				}
				exists, err := writer.has(refHash)
				if err != nil {
					return false, err
				}
				if exists {
					skippedReusedState++
					continue
				}
			} else if exists, err := reusedCellExists(refHash); err != nil {
				return false, err
			} else if exists {
				skippedReusedState++
				continue
			}
			if _, ok := visited[refHash]; ok {
				continue
			}
			if ref.Lazy {
				skippedExisting++
				continue
			}
			if refCell == nil {
				refCell, err = current.PeekRef(i)
				if err != nil {
					return false, fmt.Errorf("load state ref %x from parent %x ref=%d: %w", refHash[:], currentHash[:], i, err)
				}
			}
			if refCell == nil || refCell.IsLazy() {
				return false, fmt.Errorf("state ref %x from parent %x ref=%d has no body", refHash[:], currentHash[:], i)
			}

			visited[refHash] = struct{}{}
			stack = append(stack, refCell)
		}

		if err := writer.add(current, currentMeta, refs); err != nil {
			return false, err
		}
		processed++

		if writer.pendingBytes() >= stateCellImportBatchTargetBytes {
			if err := flush(); err != nil {
				return false, err
			}
		}
	}

	if err := flush(); err != nil {
		return false, err
	}
	logProgress(true)
	return applied > 0, nil
}

func (s *Store) saveParsedStateCellsBatch(ctx context.Context, block ton.BlockIDExt, parsedCells []cell.Cell, totalCells uint64) (bool, error) {
	if totalCells == 0 {
		totalCells = uint64(len(parsedCells))
	}

	started := time.Now()
	lastLog := started
	source := "parsed_batch"

	event := s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("source", source).
		Uint64("total_cells", totalCells)
	addCellProgress(event, 0, totalCells)
	event.Msg("persisting state cells")

	writer, err := s.newStateCellBatchWriter(nil)
	if err != nil {
		return false, err
	}
	defer writer.close()

	var processed int64
	var applied int64
	var bytesWritten int64

	logProgress := func(done bool) {
		event := s.log.Info().
			Str("block", storage.FormatBlockRef(block)).
			Str("source", source).
			Int64("processed_cells", processed).
			Int64("applied_cells", applied).
			Int64("bytes", bytesWritten).
			Uint64("total_cells", totalCells).
			Dur("elapsed", time.Since(started)).
			Str("speed", formatCellRate(processed, time.Since(started)))
		addCellProgress(event, processed, totalCells)
		if done {
			event.Msg("state cells persisted")
			return
		}
		event.Int("pending_batch_cells", writer.pendingCells()).Msg("state cell persistence progress")
	}

	flush := func() error {
		stats, err := writer.flush()
		if err != nil {
			return err
		}
		applied += stats.cells
		bytesWritten += stats.bytes

		if time.Since(lastLog) >= stateCellSaveProgressInterval {
			logProgress(false)
			lastLog = time.Now()
		}
		return nil
	}

	for i := range parsedCells {
		if processed&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
			}
		}

		current := &parsedCells[i]
		refsSlice, err := stateCellRefs(current)
		if err != nil {
			hash := stateCellStorageHash(current)
			return false, fmt.Errorf("load parsed state cell refs hash=%x lazy=%t virtual=%t type=%d: %w", hash[:], current.IsLazy(), current.IsVirtualized(), current.GetType(), err)
		}

		if err := writer.add(current, current.GetMetadata(), refsSlice); err != nil {
			return false, err
		}
		processed++

		if writer.pendingBytes() >= stateCellImportBatchTargetBytes {
			if err := flush(); err != nil {
				return false, err
			}
		}
	}

	if err := flush(); err != nil {
		return false, err
	}
	logProgress(true)
	return applied > 0, nil
}

func stateCellRefs(cl *cell.Cell) ([]cell.RefMetadata, error) {
	meta := cl.GetMetadata()
	if len(meta.Refs) > 4 {
		return nil, fmt.Errorf("cell refs count is too large: %d", len(meta.Refs))
	}
	return meta.Refs, nil
}

func stateCellStorageHash(cl *cell.Cell) cell.Hash {
	return cl.GetMetadata().Hash
}

func stateCellRefMetadata(cl *cell.Cell) cell.RefMetadata {
	meta := cl.GetMetadata()
	return cell.RefMetadata{
		Hash:      meta.Hash,
		LevelMask: meta.LevelMask,
		Hashes:    meta.Hashes,
		Depths:    meta.Depths,
		Lazy:      cl.IsLazy(),
	}
}

type stateCellReusedRefKey struct {
	parentHash cell.Hash
	refIndex   int
}

func cellVisitSetCapacity(totalCells uint64) int {
	if totalCells == 0 {
		return 1024
	}
	const maxInitialVisitSetCapacity = 1 << 20
	if totalCells > maxInitialVisitSetCapacity {
		return maxInitialVisitSetCapacity
	}
	maxInt := int(^uint(0) >> 1)
	if totalCells > uint64(maxInt) {
		return maxInt
	}
	return int(totalCells)
}

type stateCellWriteStats struct {
	cells int64
	bytes int64
}

type stateCellExistenceCache struct {
	values map[cell.Hash]bool
}

func newStateCellExistenceCache() *stateCellExistenceCache {
	return &stateCellExistenceCache{values: map[cell.Hash]bool{}}
}

func (c *stateCellExistenceCache) get(hash cell.Hash) (bool, bool) {
	if c == nil {
		return false, false
	}
	exists, ok := c.values[hash]
	return exists, ok
}

func (c *stateCellExistenceCache) set(hash cell.Hash, exists bool) {
	if c == nil {
		return
	}
	c.values[hash] = exists
}

func (c *stateCellExistenceCache) clear() {
	if c == nil {
		return
	}
	clear(c.values)
}

type stateCellBatchWriter struct {
	cells  *cellBatchWriter
	exists *stateCellExistenceCache
}

func (s *Store) newStateCellBatchWriter(exists *stateCellExistenceCache) (*stateCellBatchWriter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errPebbleClosed
	}

	return &stateCellBatchWriter{
		cells:  s.cells.newBatchWriter(),
		exists: exists,
	}, nil
}

func (w *stateCellBatchWriter) add(cl *cell.Cell, meta cell.Metadata, refs []cell.RefMetadata) error {
	valueLen, d1, d2, err := stateCellEncodedLen(cl, refs)
	if err != nil {
		return err
	}
	hash := meta.Hash
	if err = w.cells.setDeferred(hash[:], valueLen, func(value []byte) {
		encodeStateCellRecordTo(value, cl, refs, d1, d2)
	}); err != nil {
		return err
	}
	w.exists.set(hash, true)
	return nil
}

func stateCellEncodedLen(cl *cell.Cell, refs []cell.RefMetadata) (int, byte, byte, error) {
	cellBits := cl.BitsSize()
	if cellBits > 1023 {
		return 0, 0, 0, fmt.Errorf("cell bits length is too large: %d", cellBits)
	}
	if len(refs) > 4 {
		return 0, 0, 0, fmt.Errorf("cell refs count is too large: %d", len(refs))
	}

	d1, d2 := stateCellRecordDescriptors(cl, len(refs), cellBits)
	size := 2 + cl.SerializedBOCBodySize()
	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	for _, ref := range refs {
		hashesCount := storage.CellRefHashesCount(ref.LevelMask.Mask)
		if len(ref.Hashes) != hashesCount || len(ref.Depths) != hashesCount {
			return 0, 0, 0, fmt.Errorf("invalid ref metadata for %x: hashes=%d depths=%d want=%d", ref.Hash[:], len(ref.Hashes), len(ref.Depths), hashesCount)
		}
		refSize := 1 + hashesCount*(cellRecordHashSize+cellRecordDepthSize)
		refsSize += refSize
		if stateCellRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		compactRefsSize += refSize
	}
	if hasCommonRef && compactRefsSize <= refsSize {
		size += compactRefsSize
	} else {
		size += refsSize
	}
	return size, d1, d2, nil
}

func stateCellRefCommon(ref cell.RefMetadata) bool {
	return ref.LevelMask.Mask == 0 && len(ref.Hashes) == 1 && len(ref.Depths) == 1
}

func stateCellCompactRefLayout(refs []cell.RefMetadata) (byte, bool) {
	if len(refs) == 0 {
		return 0, false
	}

	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	var slowRefs byte
	for i, ref := range refs {
		refSize := 1 + len(ref.Hashes)*(cellRecordHashSize+cellRecordDepthSize)
		refsSize += refSize
		if stateCellRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	return slowRefs, hasCommonRef && compactRefsSize <= refsSize
}

func stateCellRecordDescriptors(cl *cell.Cell, refsCount int, bitLen uint) (byte, byte) {
	d1 := byte(refsCount)
	if cl.IsSpecial() {
		d1 += 8
	}
	d1 += cl.LevelMask().Mask * 32

	d2 := byte((bitLen / 8) * 2)
	if bitLen%8 != 0 {
		d2++
	}
	return d1, d2
}

func encodeStateCellRecordTo(buf []byte, cl *cell.Cell, refs []cell.RefMetadata, d1 byte, d2 byte) {
	slowRefs, compactRefs := stateCellCompactRefLayout(refs)
	pos := 0
	if compactRefs {
		d1 |= cellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = d2
	pos += 2

	pos += cl.SerializeBOCBodyTo(buf[pos:])
	if compactRefs {
		buf[pos] = slowRefs
		pos++
	}
	for i, ref := range refs {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			copy(buf[pos:pos+cellRecordHashSize], ref.Hashes[0][:])
			pos += cellRecordHashSize
			binary.BigEndian.PutUint16(buf[pos:pos+cellRecordDepthSize], ref.Depths[0])
			pos += cellRecordDepthSize
			continue
		}

		buf[pos] = ref.LevelMask.Mask
		pos++

		for _, hash := range ref.Hashes {
			copy(buf[pos:pos+32], hash[:])
			pos += 32
		}
		for _, depth := range ref.Depths {
			binary.BigEndian.PutUint16(buf[pos:pos+2], depth)
			pos += 2
		}
	}
}

func (w *stateCellBatchWriter) flush() (stateCellWriteStats, error) {
	stats, err := w.cells.flush()
	if err != nil {
		return stateCellWriteStats{}, err
	}
	w.exists.clear()
	return stats, nil
}

func (w *stateCellBatchWriter) close() {
	w.cells.close()
}

func (w *stateCellBatchWriter) has(hash cell.Hash) (bool, error) {
	if exists, ok := w.exists.get(hash); ok {
		return exists, nil
	}
	exists, err := w.cells.store.has(hash[:])
	if err != nil {
		return false, err
	}
	w.exists.set(hash, exists)
	return exists, nil
}

func (w *stateCellBatchWriter) pendingCells() int {
	return w.cells.cellsInBatch
}

func (w *stateCellBatchWriter) pendingBytes() int {
	return w.cells.bytesInBatch
}

func addCellProgress(event *zerolog.Event, processed int64, total uint64) {
	if total == 0 {
		return
	}
	event.Str("progress", formatPercent(processed, total))
}

func formatPercent(processed int64, total uint64) string {
	if total == 0 {
		return "0.0%"
	}
	progress := float64(processed) / float64(total) * 100
	if progress > 100 {
		progress = 100
	}
	return fmt.Sprintf("%.1f%%", progress)
}

func formatCellRate(cells int64, elapsed time.Duration) string {
	if cells <= 0 || elapsed <= 0 {
		return "0 cells/s"
	}
	rate := float64(cells) / elapsed.Seconds()
	switch {
	case rate >= 1_000_000:
		return fmt.Sprintf("%.2f Mcells/s", rate/1_000_000)
	case rate >= 1_000:
		return fmt.Sprintf("%.2f Kcells/s", rate/1_000)
	default:
		return fmt.Sprintf("%.0f cells/s", rate)
	}
}

func (s *Store) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	started := time.Now()
	state, err := s.blockStateMeta(ctx, block)
	if err != nil {
		return nil, err
	}

	event := s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cells", state.CellsCount)
	event.Msg("loading block state lazy root from storage")

	rootLoadStarted := time.Now()
	root, err := s.loadLazyCell(ctx, state.StateCellHash)
	if err != nil {
		return nil, fmt.Errorf("load state root cell: %w", err)
	}
	s.log.Info().
		Str("block", storage.FormatBlockRef(state.Block)).
		Uint64("cells", state.CellsCount).
		Dur("elapsed", time.Since(rootLoadStarted)).
		Msg("block state lazy root loaded")

	parsed, err := storage.ParseStateProof(&block, root, nil, state.StateRootHash, nil)
	if err != nil {
		return nil, fmt.Errorf("parse block state: %w", err)
	}
	parsed.StateFileHash = state.StateFileHash
	parsed.CellsCount = state.CellsCount
	parsed.DownloadedAt = state.DownloadedAt

	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cells", parsed.CellsCount).
		Dur("elapsed", time.Since(started)).
		Msg("block state loaded")
	return parsed, nil
}

func (s *Store) blockStateMeta(ctx context.Context, block ton.BlockIDExt) (storage.BlockState, error) {
	metaRaw, err := s.getHotCopy(ctx, hotKeyStateMeta(block))
	if err != nil {
		return storage.BlockState{}, err
	}
	downloadedAt, cellsCount, rootHash, cellHash, fileHash, err := decodeBlockStateMeta(metaRaw)
	if err != nil {
		return storage.BlockState{}, err
	}
	if len(rootHash) == 0 {
		return storage.BlockState{}, storage.ErrNotFound
	}

	return storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Clone(rootHash),
		StateCellHash: bytes.Clone(cellHash),
		StateFileHash: bytes.Clone(fileHash),
		CellsCount:    cellsCount,
		DownloadedAt:  downloadedAt,
	}, nil
}

func (s *Store) SaveBlockMeta(meta *storage.BlockMeta) error {
	return s.mergeAndStoreBlockMeta(meta)
}

func (s *Store) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	raw, err := s.getHotCopy(ctx, hotKeyBlockMeta(block))
	if err != nil {
		return nil, err
	}
	meta, err := decodeBlockMeta(raw)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Store) LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	raw, err := s.getHotCopy(ctx, hotKeyBlockSeqIndex(key, seqno))
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	block, err := decodeBlockID(raw)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return block, nil
}

func (s *Store) LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	prefix := hotKeyBlockLTPrefix(key)
	var geBlock ton.BlockIDExt
	var geFound bool
	var ltBlock ton.BlockIDExt
	var ltFound bool

	if err := func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()

		if s.closed {
			return errPebbleClosed
		}

		snap := s.hot.NewSnapshot()
		defer func() { _ = snap.Close() }()

		iter, err := snap.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		})
		if err != nil {
			return err
		}
		defer func() { _ = iter.Close() }()

		seekGE := hotKeyBlockLTSeekGE(key, lt)
		if iter.SeekGE(seekGE) && bytes.HasPrefix(iter.Key(), prefix) {
			block, err := decodeBlockID(iter.Value())
			if err != nil {
				return err
			}
			geBlock = block
			geFound = true
		}

		seekLT := hotKeyBlockLTSeek(key, lt)
		if iter.SeekLT(seekLT) && bytes.HasPrefix(iter.Key(), prefix) {
			block, err := decodeBlockID(iter.Value())
			if err != nil {
				return err
			}
			ltBlock = block
			ltFound = true
		}
		return nil
	}(); err != nil {
		return ton.BlockIDExt{}, err
	}

	if geFound {
		meta, err := s.BlockMeta(ctx, geBlock)
		switch {
		case err == nil && meta.StartLT <= lt && (meta.EndLT == 0 || lt <= meta.EndLT):
			return geBlock, nil
		case err == nil:
		case errors.Is(err, storage.ErrNotFound):
		default:
			return ton.BlockIDExt{}, err
		}
	}

	if !ltFound {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return ltBlock, nil
}

func (s *Store) LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	return s.lookupBlockByBoundedIndex(ctx, hotKeyBlockUTimePrefix(key), hotKeyBlockUTimeSeek(key, utime))
}

func (s *Store) SaveCells(records []*storage.CellRecord) error {
	_, err := s.saveCellRecordBatch(records)
	return err
}

type cellRecordBatchStats struct {
	written int
	skipped int
	bytes   int64
}

func (s *Store) saveCellRecordBatch(records []*storage.CellRecord) (cellRecordBatchStats, error) {
	var stats cellRecordBatchStats
	if len(records) == 0 {
		return stats, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return stats, errPebbleClosed
	}

	writer := s.cells.newBatchWriter()
	defer writer.close()

	written := make(map[cell.Hash]struct{}, len(records))
	for _, record := range records {
		switch {
		case record == nil:
			return stats, fmt.Errorf("cell record is nil")
		case len(record.Hash) != 32:
			return stats, fmt.Errorf("cell record hash size mismatch: %d", len(record.Hash))
		}

		var hashKey cell.Hash
		copy(hashKey[:], record.Hash)
		if _, ok := written[hashKey]; ok {
			stats.skipped++
			continue
		}

		val := encodeCellRecord(record)
		if err := writer.set(record.Hash, val); err != nil {
			return stats, err
		}
		stats.written++
		stats.bytes += int64(len(val))
		written[hashKey] = struct{}{}
	}

	if _, err := writer.flush(); err != nil {
		return stats, err
	}
	if err := s.cells.flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Store) CellRecord(ctx context.Context, hash []byte) (*storage.CellRecord, error) {
	raw, err := s.getCellCopy(ctx, hash)
	if err != nil {
		return nil, err
	}
	record, err := decodeCellRecord(hash, raw)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return s.loadLazyCell(ctx, hash)
}

type lazyCellLoader struct {
	store *Store
}

func (s *Store) LazyCellLoader() cell.LazyCellLoader {
	return lazyCellLoader{store: s}
}

func (l lazyCellLoader) LoadCell(hash cell.Hash) (*cell.Cell, error) {
	loaded, err := l.store.loadLazyCell(context.Background(), hash[:])
	if err != nil {
		return nil, fmt.Errorf("load lazy cell %x: %w", hash[:], err)
	}
	return loaded, nil
}

func (s *Store) loadLazyCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	raw, err := s.getCellCopy(ctx, hash)
	if err != nil {
		return nil, err
	}
	loaded, err := storage.LazyCellRecord(decodeCellRecordTrusted(hash, raw))
	if err != nil {
		return nil, fmt.Errorf("create lazy cell %x: %w", hash, err)
	}
	return loaded, nil
}

func (s *Store) SaveAccountTxIndex(entry storage.AccountTxIndexEntry) error {
	if len(entry.AccountKey) == 0 {
		return fmt.Errorf("account key is empty")
	}
	return s.withHotBatch(func(batch *pebble.Batch) error {
		key := hotKeyAccountTx(entry.AccountKey, entry.LT, entry.Hash)
		val := encodeBlockID(entry.Block)
		return s.setHotUnique(batch, key, val)
	})
}

func (s *Store) ListAccountTx(ctx context.Context, accountKey []byte, beforeLT uint64, limit int) ([]storage.AccountTxIndexEntry, error) {
	if limit <= 0 {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errPebbleClosed
	}

	snap := s.hot.NewSnapshot()
	defer func() { _ = snap.Close() }()

	prefix := hotKeyAccountTxPrefix(accountKey)
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	entries := make([]storage.AccountTxIndexEntry, 0, limit)
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		key := iter.Key()
		lt, hash, err := decodeAccountTxKey(key, len(prefix))
		if err != nil {
			return nil, err
		}
		if beforeLT != 0 && lt >= beforeLT {
			continue
		}
		block, err := decodeBlockID(iter.Value())
		if err != nil {
			return nil, err
		}
		entries = append(entries, storage.AccountTxIndexEntry{
			AccountKey: bytes.Clone(accountKey),
			LT:         lt,
			Hash:       hash,
			Block:      block,
		})
		if len(entries) >= limit {
			break
		}
	}
	return entries, nil
}

func (s *Store) mergeAndStoreBlockMeta(next *storage.BlockMeta) error {
	if next == nil {
		return fmt.Errorf("block meta is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errPebbleClosed
	}

	hotBatch := s.hot.NewBatch()
	defer func() { _ = hotBatch.Close() }()

	if err := s.setMergedBlockMeta(hotBatch, next); err != nil {
		return err
	}
	return hotBatch.Commit(pebble.NoSync)
}

func (s *Store) setMergedBlockMeta(batch *pebble.Batch, next *storage.BlockMeta) error {
	if next == nil {
		return fmt.Errorf("block meta is nil")
	}

	key := hotKeyBlockMeta(next.ID)
	existingRaw, err := pebbleReaderGetCopy(s.hot, key)
	existed := false
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		existingRaw = nil
	} else {
		existed = true
	}

	var existing *storage.BlockMeta
	if existed {
		existing, err = decodeBlockMeta(existingRaw)
		if err != nil {
			return err
		}
	}
	merged := storage.MergeBlockMeta(existing, next)
	merged.UpdatedAt = time.Now()
	encoded := encodeBlockMeta(merged)

	if err = batch.Set(key, encoded, pebble.NoSync); err != nil {
		return err
	}
	if err = batch.Set(hotKeyBlockSeqIndex(storage.BlockHistoryKey{Workchain: merged.ID.Workchain, Shard: merged.ID.Shard}, merged.ID.SeqNo), encodeBlockID(merged.ID), pebble.NoSync); err != nil {
		return err
	}
	if merged.EndLT != 0 {
		if err = batch.Set(hotKeyBlockLTIndex(merged), encodeBlockID(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	if merged.GenUTime != 0 {
		if err = batch.Set(hotKeyBlockUTimeIndex(merged), encodeBlockID(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) lookupBlockByBoundedIndex(ctx context.Context, prefix []byte, seek []byte) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return ton.BlockIDExt{}, errPebbleClosed
	}

	snap := s.hot.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer func() { _ = iter.Close() }()

	select {
	case <-ctx.Done():
		return ton.BlockIDExt{}, ctx.Err()
	default:
	}

	if !iter.SeekLT(seek) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	if !bytes.HasPrefix(iter.Key(), prefix) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := decodeBlockID(iter.Value())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return block, nil
}

func (s *Store) withHotBatch(fn func(batch *pebble.Batch) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}
	db := s.hot

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) setHotRecord(ctx context.Context, key, value []byte, writeOptions *pebble.WriteOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := s.setHotMaybeReplace(batch, key, value); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) deleteHotRecord(ctx context.Context, key []byte, writeOptions *pebble.WriteOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Delete(key, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) getHotCopy(ctx context.Context, key []byte) ([]byte, error) {
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
	return pebbleReaderGetCopy(s.hot, key)
}

func (s *Store) getCellCopy(ctx context.Context, hash []byte) ([]byte, error) {
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
	return s.cells.getCopy(hash)
}

func (s *Store) flushCellDBs() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}
	return s.cells.flush()
}

func (s *Store) setHotUnique(batch *pebble.Batch, key, value []byte) error {
	exists, err := pebbleReaderHas(s.hot, key)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err = batch.Set(key, value, pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (s *Store) setHotMaybeReplace(batch *pebble.Batch, key, value []byte) error {
	old, err := pebbleReaderGetCopy(s.hot, key)
	created := false
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		old = nil
		created = true
	}
	if !created && bytes.Equal(old, value) {
		return nil
	}
	if err = batch.Set(key, value, pebble.NoSync); err != nil {
		return err
	}
	return nil
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
	hotPrefixBlockMeta     = []byte{0x01}
	hotPrefixNextBlock     = []byte{0x02}
	hotPrefixBlockSeq      = []byte{0x03}
	hotPrefixBlockLT       = []byte{0x04}
	hotPrefixBlockUTime    = []byte{0x05}
	hotPrefixCurrentState  = []byte{0x06}
	hotPrefixStateMeta     = []byte{0x07}
	hotPrefixAccountTx     = []byte{0x09}
	hotPrefixStateCellSync = []byte{0x0A}
	hotPrefixArchiveInfo   = []byte{0x0B}
	hotPrefixStateSync     = []byte{0x0C}
	hotPrefixBlockDataRef  = []byte{0x0D}
	hotPrefixProofRef      = []byte{0x0E}
	hotPrefixArchiveFile   = []byte{0x0F}
)

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

func hotKeyStateMeta(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixStateMeta, id)
}

func hotKeyStateMetaMasterchainPrefix() []byte {
	buf := append([]byte(nil), hotPrefixStateMeta...)
	buf = binary.BigEndian.AppendUint32(buf, ^uint32(0))
	return binary.BigEndian.AppendUint64(buf, uint64(1)<<63)
}

func hotKeyStateCellSync(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixStateCellSync, id)
}

func hotKeyAccountTxPrefix(accountKey []byte) []byte {
	buf := append([]byte(nil), hotPrefixAccountTx...)
	buf = append(buf, byte(len(accountKey)))
	return append(buf, accountKey...)
}

func hotKeyAccountTx(accountKey []byte, lt uint64, hash []byte) []byte {
	buf := hotKeyAccountTxPrefix(accountKey)
	buf = binary.BigEndian.AppendUint64(buf, math.MaxUint64-lt)
	return append(buf, hash...)
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

func hotKeyArchiveFile(archiveID int64) []byte {
	buf := append([]byte(nil), hotPrefixArchiveFile...)
	return binary.BigEndian.AppendUint64(buf, uint64(archiveID))
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
	buf = append(buf, 1)
	buf = append(buf, encodeBlockID(meta.ID)...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(meta.Flags))
	buf = binary.BigEndian.AppendUint32(buf, meta.GenUTime)
	buf = binary.BigEndian.AppendUint64(buf, meta.StartLT)
	buf = binary.BigEndian.AppendUint64(buf, meta.EndLT)
	buf = binary.BigEndian.AppendUint64(buf, uint64(meta.UpdatedAt.UnixNano()))
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

func decodeBlockMeta(data []byte) (*storage.BlockMeta, error) {
	if len(data) < 1+80+4+4+8+8+8+1+1 {
		return nil, fmt.Errorf("block meta payload too small")
	}
	if data[0] != 1 {
		return nil, fmt.Errorf("unsupported block meta version %d", data[0])
	}
	pos := 1
	id, err := decodeBlockID(data[pos : pos+80])
	if err != nil {
		return nil, err
	}
	pos += 80
	meta := &storage.BlockMeta{
		ID:        id,
		Flags:     storage.BlockMetaFlags(binary.BigEndian.Uint32(data[pos : pos+4])),
		GenUTime:  binary.BigEndian.Uint32(data[pos+4 : pos+8]),
		StartLT:   binary.BigEndian.Uint64(data[pos+8 : pos+16]),
		EndLT:     binary.BigEndian.Uint64(data[pos+16 : pos+24]),
		UpdatedAt: time.Unix(0, int64(binary.BigEndian.Uint64(data[pos+24:pos+32]))),
	}
	pos += 32
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
	if data[pos] == 1 {
		pos++
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		meta.MasterchainRef = &ref
		pos += 80
	} else {
		pos++
	}
	if pos >= len(data) {
		return meta, nil
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
	return meta, nil
}

func encodeBlockStateMeta(state *storage.BlockState) []byte {
	buf := make([]byte, 0, 104)
	buf = append(buf, blockStateMetaVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(state.DownloadedAt.UnixNano()))
	buf = binary.BigEndian.AppendUint64(buf, state.CellsCount)
	buf = appendLenBytes(buf, state.StateRootHash)
	buf = appendLenBytes(buf, state.StateCellHash)
	buf = appendLenBytes(buf, state.StateFileHash)
	return buf
}

func encodeStateCellSync(rootHash cell.Hash, cellsCount uint64) []byte {
	buf := make([]byte, 0, 1+8+32)
	buf = append(buf, 1)
	buf = binary.BigEndian.AppendUint64(buf, cellsCount)
	return append(buf, rootHash[:]...)
}

func decodeStateCellSync(data []byte) (cell.Hash, uint64, error) {
	var rootHash cell.Hash
	if len(data) != 1+8+len(rootHash) {
		return rootHash, 0, fmt.Errorf("state cell sync payload size mismatch: got %d", len(data))
	}
	if data[0] != 1 {
		return rootHash, 0, fmt.Errorf("unsupported state cell sync version %d", data[0])
	}
	cellsCount := binary.BigEndian.Uint64(data[1:9])
	copy(rootHash[:], data[9:])
	return rootHash, cellsCount, nil
}

func decodeBlockStateMeta(data []byte) (time.Time, uint64, []byte, []byte, []byte, error) {
	if len(data) < 1+8+1+1+1 {
		return time.Time{}, 0, nil, nil, nil, fmt.Errorf("block state meta payload too small")
	}
	if data[0] != blockStateMetaVersion {
		return time.Time{}, 0, nil, nil, nil, fmt.Errorf("unsupported block state meta version %d", data[0])
	}
	pos := 1
	ts := time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8])))
	pos += 8

	if len(data) < pos+8+1+1+1 {
		return time.Time{}, 0, nil, nil, nil, fmt.Errorf("block state meta payload too small")
	}
	cellsCount := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	root, pos, err := readLenBytes(data, pos)
	if err != nil {
		return time.Time{}, 0, nil, nil, nil, err
	}
	cellHash, pos, err := readLenBytes(data, pos)
	if err != nil {
		return time.Time{}, 0, nil, nil, nil, err
	}
	file, _, err := readLenBytes(data, pos)
	if err != nil {
		return time.Time{}, 0, nil, nil, nil, err
	}
	return ts, cellsCount, root, cellHash, file, nil
}

func encodeCurrentState(state *storage.CurrentState) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, 1)
	buf = binary.BigEndian.AppendUint64(buf, uint64(state.SyncedAt.UnixNano()))
	buf = binary.BigEndian.AppendUint32(buf, state.ShardClientSeqno)
	buf = append(buf, encodeBlockID(state.Masterchain.Block)...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(state.Shards)))
	for _, key := range storage.SortedShardKeys(state.Shards) {
		buf = binary.BigEndian.AppendUint32(buf, uint32(key.Workchain))
		buf = binary.BigEndian.AppendUint64(buf, uint64(key.Shard))
		buf = append(buf, encodeBlockID(state.Shards[key].Block)...)
	}
	return buf
}

func decodeCurrentState(data []byte) (*storage.CurrentState, error) {
	if len(data) < 1+8+4+80+4 {
		return nil, fmt.Errorf("current state payload too small")
	}
	if data[0] != 1 {
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
		if pos+12+80 > len(data) {
			return nil, fmt.Errorf("current state shards truncated")
		}
		key := storage.ShardKey{
			Workchain: int32(binary.BigEndian.Uint32(data[pos : pos+4])),
			Shard:     int64(binary.BigEndian.Uint64(data[pos+4 : pos+12])),
		}
		pos += 12
		block, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		pos += 80
		state.Shards[key] = storage.BlockState{Block: block}
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

func decodeCellRecordTrusted(hash []byte, data []byte) *storage.CellRecord {
	pos := 0
	storedD1 := data[pos]
	compactRefs := storedD1&cellRecordCompactRefsFlag != 0
	record := &storage.CellRecord{
		Hash: hash,
		D1:   storedD1 &^ cellRecordCompactRefsFlag,
		D2:   data[pos+1],
	}
	pos += 2

	dataLen := int(record.D2/2 + record.D2%2)
	record.Data = data[pos : pos+dataLen]
	pos += dataLen

	refsCount := int(record.D1 & 7)
	record.Refs = make([]storage.CellRefRecord, refsCount)
	var slowRefs byte
	if compactRefs && refsCount > 0 {
		slowRefs = data[pos]
		pos++
	}
	for i := 0; i < refsCount; i++ {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			hashes := data[pos : pos+cellRecordHashSize]
			pos += cellRecordHashSize
			depths := data[pos : pos+cellRecordDepthSize]
			pos += cellRecordDepthSize

			record.Refs[i] = storage.CellRefRecord{
				LevelMask: 0,
				Hashes:    hashes,
				Depths:    depths,
			}
			continue
		}

		levelMask := data[pos]
		pos++

		hashesCount := storage.CellRefHashesCount(levelMask)
		hashesLen := hashesCount * 32
		depthsLen := hashesCount * 2
		hashes := data[pos : pos+hashesLen]
		pos += hashesLen
		depths := data[pos : pos+depthsLen]
		pos += depthsLen

		record.Refs[i] = storage.CellRefRecord{
			LevelMask: levelMask,
			Hashes:    hashes,
			Depths:    depths,
		}
	}
	return record
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

func decodeAccountTxKey(key []byte, prefixLen int) (uint64, []byte, error) {
	if len(key) < prefixLen+8 {
		return 0, nil, fmt.Errorf("account tx key too small")
	}
	lt := math.MaxUint64 - binary.BigEndian.Uint64(key[prefixLen:prefixLen+8])
	return lt, bytes.Clone(key[prefixLen+8:]), nil
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
