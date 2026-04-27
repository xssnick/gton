package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flexserver/internal/logutil"
	"flexserver/service/storage"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	defaultPebbleHotCacheSize     = 1 << 30
	defaultPebbleBlobCacheSize    = 64 << 20
	defaultPebbleHotMemTableSize  = 256 << 20
	defaultPebbleBlobMemTableSize = 64 << 20
	defaultPebbleBytesPerSync     = 8 << 20
	defaultPebbleWALBytesSync     = 8 << 20

	defaultPebbleTargetFileSize        = 64 << 20
	defaultPebbleMemTableStopThreshold = 8
	defaultPebbleL0CompactionThreshold = 8
	defaultPebbleL0FileThreshold       = 16
	defaultPebbleL0StopWritesThreshold = 64
	stateCellImportBatchTargetBytes    = 128 << 20
	stateCellSaveProgressInterval      = 5 * time.Second

	blockStateMetaVersion = 1
)

var errPebbleClosed = errors.New("pebble storage is closed")

type Options struct {
	Dir             string
	Logger          *zerolog.Logger
	HotCacheSize    int64
	BlobCacheSize   int64
	MemTableSize    int
	BytesPerSync    int
	WALBytesPerSync int
}

type Store struct {
	log zerolog.Logger

	hot       *pebble.DB
	blob      *pebble.DB
	dir       string
	hotOpts   *pebble.Options
	hotCache  *pebble.Cache
	blobCache *pebble.Cache

	mu     sync.RWMutex
	closed bool
}

func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("storage dir is empty")
	}
	logger := logutil.WithComponent(opts.Logger, "pebblestore")
	if opts.HotCacheSize <= 0 {
		opts.HotCacheSize = defaultPebbleHotCacheSize
	}
	if opts.BlobCacheSize <= 0 {
		opts.BlobCacheSize = defaultPebbleBlobCacheSize
	}
	if opts.BytesPerSync <= 0 {
		opts.BytesPerSync = defaultPebbleBytesPerSync
	}
	if opts.WALBytesPerSync <= 0 {
		opts.WALBytesPerSync = defaultPebbleWALBytesSync
	}
	hotMemTableSize := opts.MemTableSize
	if hotMemTableSize <= 0 {
		hotMemTableSize = defaultPebbleHotMemTableSize
	}
	blobMemTableSize := opts.MemTableSize
	if blobMemTableSize <= 0 {
		blobMemTableSize = defaultPebbleBlobMemTableSize
	}

	hotDir := filepath.Join(opts.Dir, "hotdb")
	blobDir := filepath.Join(opts.Dir, "blobdb")
	if err := os.MkdirAll(hotDir, 0o755); err != nil {
		return nil, fmt.Errorf("create hotdb dir: %w", err)
	}
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return nil, fmt.Errorf("create blobdb dir: %w", err)
	}

	hotCache := pebble.NewCache(opts.HotCacheSize)
	blobCache := pebble.NewCache(opts.BlobCacheSize)
	hotLogger := logger.With().Str("db", "hotdb").Logger()
	blobLogger := logger.With().Str("db", "blobdb").Logger()

	hotOpts := newPebbleOptions(hotCache, hotMemTableSize, opts.BytesPerSync, opts.WALBytesPerSync, 4<<10, pebble.NoCompression, hotLogger)
	hot, err := pebble.Open(hotDir, hotOpts)
	if err != nil {
		hotCache.Unref()
		blobCache.Unref()
		return nil, fmt.Errorf("open hotdb: %w", err)
	}
	blob, err := pebble.Open(blobDir, newPebbleOptions(blobCache, blobMemTableSize, opts.BytesPerSync, opts.WALBytesPerSync, 32<<10, pebble.SnappyCompression, blobLogger))
	if err != nil {
		_ = hot.Close()
		hotCache.Unref()
		blobCache.Unref()
		return nil, fmt.Errorf("open blobdb: %w", err)
	}

	store := &Store{
		log:       logger,
		hot:       hot,
		blob:      blob,
		dir:       opts.Dir,
		hotOpts:   hotOpts,
		hotCache:  hotCache,
		blobCache: blobCache,
	}
	logger.Info().
		Int64("hot_cache_size", opts.HotCacheSize).
		Int64("blob_cache_size", opts.BlobCacheSize).
		Int("hot_memtable_size", hotMemTableSize).
		Int("blob_memtable_size", blobMemTableSize).
		Int("target_file_size", defaultPebbleTargetFileSize).
		Int("state_cell_import_batch_target_bytes", stateCellImportBatchTargetBytes).
		Int("memtable_stop_writes_threshold", defaultPebbleMemTableStopThreshold).
		Int("l0_compaction_threshold", defaultPebbleL0CompactionThreshold).
		Int("l0_file_threshold", defaultPebbleL0FileThreshold).
		Int("l0_stop_writes_threshold", defaultPebbleL0StopWritesThreshold).
		Int("max_concurrent_compactions", pebbleMaxConcurrentCompactions()).
		Msg("configured pebble storage tuning")
	// Do not scan the full cell DB on startup just to populate console stats.
	return store, nil
}

func newPebbleOptions(cache *pebble.Cache, memTableSize, bytesPerSync, walBytesPerSync, blockSize int, compression pebble.Compression, logger zerolog.Logger) *pebble.Options {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		levels[i] = pebble.LevelOptions{
			BlockSize:      blockSize,
			IndexBlockSize: blockSize,
			FilterPolicy:   bloom.FilterPolicy(10),
			FilterType:     pebble.TableFilter,
			TargetFileSize: defaultPebbleTargetFileSize,
			Compression:    compression,
		}
	}
	opts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                uint64(memTableSize),
		MemTableStopWritesThreshold: defaultPebbleMemTableStopThreshold,
		BytesPerSync:                bytesPerSync,
		WALBytesPerSync:             walBytesPerSync,
		FlushSplitBytes:             defaultPebbleTargetFileSize,
		L0CompactionThreshold:       defaultPebbleL0CompactionThreshold,
		L0CompactionFileThreshold:   defaultPebbleL0FileThreshold,
		L0StopWritesThreshold:       defaultPebbleL0StopWritesThreshold,
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

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var firstErr error
	if err := s.hot.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.blob.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if s.hotCache != nil {
		s.hotCache.Unref()
	}
	if s.blobCache != nil {
		s.blobCache.Unref()
	}
	return firstErr
}

func (s *Store) SaveBlockFull(block *storage.ServedBlockFull) error {
	if block == nil {
		return fmt.Errorf("served block is nil")
	}

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

	if err := s.persistServedBlockFullBlobs(block, proofKinds); err != nil {
		return err
	}
	return s.mergeAndStoreBlockMeta(meta)
}

func (s *Store) persistServedBlockFullBlobs(block *storage.ServedBlockFull, proofKinds []storage.ServedProofKind) error {
	if len(block.Block) == 0 && len(block.Proof) == 0 {
		return nil
	}

	return s.withBlobBatch(func(batch *pebble.Batch) error {
		if len(block.Block) > 0 {
			if err := s.setBlobUnique(batch, blobKeyBlockData(block.ID), block.Block); err != nil {
				return err
			}
		}
		if len(block.Proof) > 0 {
			for _, kind := range proofKinds {
				if err := s.setBlobUnique(batch, blobKeyProof(kind, block.ID), block.Proof); err != nil {
					return err
				}
			}
		}
		return nil
	})
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

func (s *Store) SaveBlockData(block ton.BlockIDExt, data []byte) {
	if err := s.persistBlockData(block, data); err != nil {
		s.log.Warn().
			Err(err).
			Str("block", storage.FormatBlockRef(block)).
			Msg("failed to persist block data")
	}
}

func (s *Store) persistBlockData(block ton.BlockIDExt, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := s.withBlobBatch(func(batch *pebble.Batch) error {
		return s.setBlobUnique(batch, blobKeyBlockData(block), data)
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

func (s *Store) SaveBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte) {
	if err := s.persistBlockProof(kind, block, data); err != nil {
		s.log.Warn().
			Err(err).
			Str("kind", string(kind)).
			Str("block", storage.FormatBlockRef(block)).
			Msg("failed to persist block proof")
	}
}

func (s *Store) persistBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := s.withBlobBatch(func(batch *pebble.Batch) error {
		return s.setBlobUnique(batch, blobKeyProof(kind, block), data)
	}); err != nil {
		return err
	}
	return s.mergeAndStoreBlockMeta(&storage.BlockMeta{
		ID:        block,
		Flags:     storage.BlockMetaFlagForProof(kind),
		UpdatedAt: time.Now(),
	})
}

func (s *Store) SaveArchiveInfo(masterchainSeqno int32, archiveID int64) {
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		key := hotKeyArchiveInfo(masterchainSeqno)
		val := encodeInt64(archiveID)
		return s.setHotMaybeReplace(batch, key, val)
	}); err != nil {
		s.log.Warn().
			Err(err).
			Int32("masterchain_seqno", masterchainSeqno).
			Int64("archive_id", archiveID).
			Msg("failed to persist archive info")
	}
}

func (s *Store) SaveArchiveSlice(archiveID, offset int64, data []byte) {
	if len(data) == 0 {
		return
	}
	if err := s.withBlobBatch(func(batch *pebble.Batch) error {
		return s.setBlobUnique(batch, blobKeyArchiveChunk(archiveID, offset), data)
	}); err != nil {
		s.log.Warn().
			Err(err).
			Int64("archive_id", archiveID).
			Int64("offset", offset).
			Msg("failed to persist archive chunk")
	}
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
	return s.getBlobCopy(ctx, blobKeyBlockData(block))
}

func (s *Store) BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	return s.getBlobCopy(ctx, blobKeyProof(kind, block))
}

func (s *Store) ArchiveInfo(ctx context.Context, masterchainSeqno int32) (int64, error) {
	raw, err := s.getHotCopy(ctx, hotKeyArchiveInfo(masterchainSeqno))
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid archive info payload")
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func (s *Store) ArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) ([]byte, error) {
	data, err := s.getBlobCopy(ctx, blobKeyArchiveChunk(archiveID, offset))
	if err != nil {
		return nil, err
	}
	if maxSize > 0 && len(data) > int(maxSize) {
		data = data[:maxSize]
	}
	return data, nil
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

func (s *Store) SaveBlockState(ctx context.Context, state *storage.BlockState) error {
	saved, lazyRoot, cellSyncHash, syncCells, err := s.prepareBlockStateForSave(ctx, state)
	if err != nil {
		return err
	}
	if err = s.saveBlockStateRecords(saved, cellSyncHash, syncCells, nil); err != nil {
		return err
	}
	if saved.Cell != nil && saved.Parsed != nil {
		if err := s.replaceBlockStateWithLazyRoot(state, saved, lazyRoot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SaveBlockStateAndCurrentState(ctx context.Context, block *storage.BlockState, current *storage.CurrentState) error {
	if current == nil {
		return fmt.Errorf("current state is nil")
	}

	saved, lazyRoot, cellSyncHash, syncCells, err := s.prepareBlockStateForSave(ctx, block)
	if err != nil {
		return err
	}
	if err = s.saveBlockStateRecords(saved, cellSyncHash, syncCells, current); err != nil {
		return err
	}
	if saved.Cell != nil && saved.Parsed != nil {
		if err := s.replaceBlockStateWithLazyRoot(block, saved, lazyRoot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) prepareBlockStateForSave(ctx context.Context, state *storage.BlockState) (storage.BlockState, *cell.Cell, cell.Hash, bool, error) {
	var zero cell.Hash
	if state == nil {
		return storage.BlockState{}, nil, zero, false, fmt.Errorf("block state is nil")
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
		return storage.BlockState{}, nil, zero, false, fmt.Errorf("block state root hash is empty")
	}
	if len(saved.StateCellHash) == 0 {
		return storage.BlockState{}, nil, zero, false, fmt.Errorf("block state cell hash is empty")
	}
	if len(saved.StateCellHash) != len(zero) {
		return storage.BlockState{}, nil, zero, false, fmt.Errorf("block state cell hash size mismatch: got %d", len(saved.StateCellHash))
	}

	var cellSyncHash cell.Hash
	copy(cellSyncHash[:], saved.StateCellHash)
	var lazyRoot *cell.Cell
	syncCells := false

	if saved.Cell != nil {
		syncCells = true
		if saved.Cell.IsLazy() {
			lazyRoot = saved.Cell
		} else {
			cellSyncHash = saved.Cell.HashKey()
			if err := s.saveStateCellTree(ctx, saved.Block, saved.Cell, nil, saved.CellsCount, saved.ReusedStateCells, saved.ReusedStateRefs); err != nil {
				return storage.BlockState{}, nil, zero, false, err
			}
			var err error
			lazyRoot, err = s.loadLazyCell(ctx, cellSyncHash[:])
			if err != nil {
				return storage.BlockState{}, nil, zero, false, fmt.Errorf("load persisted lazy state root: %w", err)
			}
		}
	}

	return saved, lazyRoot, cellSyncHash, syncCells, nil
}

func (s *Store) saveBlockStateRecords(saved storage.BlockState, cellSyncHash cell.Hash, syncCells bool, current *storage.CurrentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errPebbleClosed
	}

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()

	if syncCells {
		if err := batch.Set(hotKeyStateCellSync(saved.Block), encodeStateCellSync(cellSyncHash, saved.CellsCount), pebble.NoSync); err != nil {
			return err
		}
	}
	if err := s.setHotMaybeReplace(batch, hotKeyStateMeta(saved.Block), encodeBlockStateMeta(&saved)); err != nil {
		return err
	}
	if err := s.setMergedBlockMeta(batch, storage.BuildBlockMetaFromState(saved)); err != nil {
		return err
	}
	if current != nil {
		if err := s.setHotMaybeReplace(batch, hotKeyCurrentState(), encodeCurrentState(current)); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64) (*cell.Cell, error) {
	return s.importStateCellTree(ctx, block, root, parsedCells, totalCells, nil, nil)
}

func (s *Store) importStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64, reusedStateCells []cell.MerkleUpdateReusedCell, reusedStateRefs []cell.MerkleUpdateReusedRef) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("state cell tree root is nil")
	}

	rootCellHash := root.HashKey()
	if err := s.saveStateCellTree(ctx, block, root, parsedCells, totalCells, reusedStateCells, reusedStateRefs); err != nil {
		return nil, err
	}

	if err := s.syncStateCellTree(block, rootCellHash, totalCells); err != nil {
		return nil, fmt.Errorf("sync state cells: %w", err)
	}

	lazyRoot, err := s.loadLazyCell(ctx, rootCellHash[:])
	if err != nil {
		return nil, fmt.Errorf("load persisted lazy state root: %w", err)
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cells", totalCells).
		Msg("state cell tree imported and switched to lazy celldb root")
	return lazyRoot, nil
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cells", totalCells).
		Msg("syncing persisted state cells before returning lazy root")

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Set(hotKeyStateCellSync(block), encodeStateCellSync(rootHash, totalCells), pebble.NoSync); err != nil {
		return err
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

func (s *Store) saveStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64, reusedStateCells []cell.MerkleUpdateReusedCell, reusedStateRefs []cell.MerkleUpdateReusedRef) error {
	if len(parsedCells) > 0 {
		return s.saveParsedStateCellsBatch(ctx, block, parsedCells, totalCells)
	}

	started := time.Now()
	lastLog := started
	source := "dfs"

	event := s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("source", source).
		Uint64("total_cells", totalCells).
		Int("reused_state_cells", len(reusedStateCells))
	addCellProgress(event, 0, totalCells)
	event.Msg("persisting state cells")

	writer, err := s.newStateCellBatchWriter()
	if err != nil {
		return err
	}
	defer writer.close()

	var processed int64
	var applied int64
	var bytesWritten int64
	var skippedReusedState int64
	var skippedExisting int64

	logProgress := func(done bool) {
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
		event.Int("pending_batch_cells", writer.cellsInBatch).Msg("state cell persistence progress")
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
		exists, err := pebbleReaderHas(writer.db, hotKeyCell(hash[:]))
		if err != nil {
			return false, err
		}
		if !exists && raw.IsLazy() {
			return false, fmt.Errorf("reused lazy state cell %x is missing from storage", hash[:])
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
				return ctx.Err()
			default:
			}
		}

		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]
		currentHash := stateCellStorageHash(current)

		if exists, err := reusedCellExists(currentHash); err != nil {
			return err
		} else if exists {
			skippedReusedState++
			processed++
			continue
		}
		if current.IsLazy() {
			exists, err := pebbleReaderHas(writer.db, hotKeyCell(currentHash[:]))
			if err != nil {
				return err
			}
			if exists {
				skippedExisting++
				processed++
				continue
			}
			return fmt.Errorf("lazy state cell %x is missing from storage", currentHash[:])
		}

		var refs [4]*cell.Cell
		refsCount := int(current.RefsNum())
		if refsCount > len(refs) {
			return fmt.Errorf("cell refs count is too large: %d", refsCount)
		}
		for i := 0; i < refsCount; i++ {
			if reusedRef, ok := reusedStateRefEdges[stateCellReusedRefKey{parentHash: currentHash, refIndex: i}]; ok {
				refs[i] = reusedRef.RawCell
			} else {
				ref, err := current.PeekRef(i)
				if err != nil {
					return fmt.Errorf("load state cell ref hash=%x ref=%d: %w", currentHash[:], i, err)
				}
				refs[i] = ref
				refHash := stateCellStorageHash(ref)
				if raw, ok := reusedStateCellRefs[refHash]; ok {
					refs[i] = raw
				}
			}

			ref := refs[i]
			refHash := stateCellStorageHash(ref)
			if exists, err := reusedCellExists(refHash); err != nil {
				return err
			} else if exists {
				skippedReusedState++
				continue
			}
			if _, ok := visited[refHash]; ok {
				continue
			}
			if ref.IsLazy() {
				exists, err := pebbleReaderHas(writer.db, hotKeyCell(refHash[:]))
				if err != nil {
					return err
				}
				if exists {
					skippedExisting++
					continue
				}
				return fmt.Errorf("lazy state ref %x from parent %x ref=%d is missing from storage", refHash[:], currentHash[:], i)
			}

			visited[refHash] = struct{}{}
			stack = append(stack, ref)
		}

		if err := writer.add(current, refs[:refsCount]); err != nil {
			return err
		}
		processed++

		if writer.bytesInBatch >= stateCellImportBatchTargetBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if err := flush(); err != nil {
		return err
	}
	logProgress(true)
	return nil
}

func (s *Store) saveParsedStateCellsBatch(ctx context.Context, block ton.BlockIDExt, parsedCells []cell.Cell, totalCells uint64) error {
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

	writer, err := s.newStateCellBatchWriter()
	if err != nil {
		return err
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
		event.Int("pending_batch_cells", writer.cellsInBatch).Msg("state cell persistence progress")
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
				return ctx.Err()
			default:
			}
		}

		current := &parsedCells[i]
		var refs [4]*cell.Cell
		refsSlice, err := stateCellRefs(current, &refs)
		if err != nil {
			hash := stateCellStorageHash(current)
			return fmt.Errorf("load parsed state cell refs hash=%x lazy=%t virtual=%t type=%d: %w", hash[:], current.IsLazy(), current.IsVirtualized(), current.GetType(), err)
		}

		if err := writer.add(current, refsSlice); err != nil {
			return err
		}
		processed++

		if writer.bytesInBatch >= stateCellImportBatchTargetBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if err := flush(); err != nil {
		return err
	}
	logProgress(true)
	return nil
}

func stateCellRefs(cl *cell.Cell, refs *[4]*cell.Cell) ([]*cell.Cell, error) {
	refsCount := int(cl.RefsNum())
	if refsCount > len(refs) {
		return nil, fmt.Errorf("cell refs count is too large: %d", refsCount)
	}
	for i := 0; i < refsCount; i++ {
		ref, err := cl.PeekRef(i)
		if err != nil {
			return nil, err
		}
		refs[i] = ref
	}
	return refs[:refsCount], nil
}

func stateCellStorageHash(cl *cell.Cell) cell.Hash {
	return cl.HashKey()
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

type stateCellBatchWriter struct {
	db    *pebble.DB
	batch *pebble.Batch

	cellsInBatch int
	bytesInBatch int
}

func (s *Store) newStateCellBatchWriter() (*stateCellBatchWriter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errPebbleClosed
	}
	db := s.hot

	return &stateCellBatchWriter{
		db:    db,
		batch: db.NewBatch(),
	}, nil
}

func (w *stateCellBatchWriter) add(cl *cell.Cell, refs []*cell.Cell) error {
	valueLen, d1, d2, err := stateCellEncodedLen(cl, refs)
	if err != nil {
		return err
	}
	hash := cl.HashKey()

	op := w.batch.SetDeferred(len(hotPrefixCell)+len(hash), valueLen)
	copy(op.Key, hotPrefixCell)
	copy(op.Key[len(hotPrefixCell):], hash[:])

	encodeStateCellRecordTo(op.Value, cl, refs, d1, d2)
	if err = op.Finish(); err != nil {
		return err
	}

	w.cellsInBatch++
	w.bytesInBatch += len(op.Value)
	return nil
}

func stateCellEncodedLen(cl *cell.Cell, refs []*cell.Cell) (int, byte, byte, error) {
	cellBits := cl.BitsSize()
	if cellBits > 1023 {
		return 0, 0, 0, fmt.Errorf("cell bits length is too large: %d", cellBits)
	}
	if len(refs) > 4 {
		return 0, 0, 0, fmt.Errorf("cell refs count is too large: %d", len(refs))
	}

	d1, d2 := stateCellRecordDescriptors(cl, len(refs), cellBits)
	size := uvarintLen(1) + 2 + cl.SerializedBOCBodySize()
	for _, ref := range refs {
		size += 1 + storage.CellRefHashesCount(ref.LevelMask().Mask)*(32+2)
	}
	return size, d1, d2, nil
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

func encodeStateCellRecordTo(buf []byte, cl *cell.Cell, refs []*cell.Cell, d1 byte, d2 byte) {
	pos := binary.PutUvarint(buf, 1)
	buf[pos] = d1
	buf[pos+1] = d2
	pos += 2

	pos += cl.SerializeBOCBodyTo(buf[pos:])
	for _, ref := range refs {
		levelMask := ref.LevelMask()
		buf[pos] = levelMask.Mask
		pos++

		for level := 0; level <= levelMask.GetLevel(); level++ {
			if !levelMask.IsSignificant(level) {
				continue
			}
			hash := ref.HashKey(level)
			copy(buf[pos:pos+32], hash[:])
			pos += 32
		}
		for level := 0; level <= levelMask.GetLevel(); level++ {
			if !levelMask.IsSignificant(level) {
				continue
			}
			binary.BigEndian.PutUint16(buf[pos:pos+2], ref.Depth(level))
			pos += 2
		}
	}
}

func (w *stateCellBatchWriter) flush() (stateCellWriteStats, error) {
	stats := stateCellWriteStats{
		cells: int64(w.cellsInBatch),
		bytes: int64(w.bytesInBatch),
	}
	if w.cellsInBatch == 0 {
		return stats, nil
	}

	batch := w.batch
	if err := batch.Commit(pebble.NoSync); err != nil {
		return stateCellWriteStats{}, err
	}
	_ = batch.Close()
	w.batch = w.db.NewBatch()
	w.cellsInBatch = 0
	w.bytesInBatch = 0
	return stats, nil
}

func (w *stateCellBatchWriter) close() {
	if w.batch != nil {
		_ = w.batch.Close()
	}
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
	db := s.hot

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

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
		if err := batch.Set(hotKeyCell(record.Hash), val, pebble.NoSync); err != nil {
			return stats, err
		}
		stats.written++
		stats.bytes += int64(len(val))
		written[hashKey] = struct{}{}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Store) CellRecord(ctx context.Context, hash []byte) (*storage.CellRecord, error) {
	raw, err := s.getHotCopy(ctx, hotKeyCell(hash))
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
	raw, err := s.getHotCopy(ctx, hotKeyCell(hash))
	if err != nil {
		return nil, err
	}
	return storage.LazyCellRecord(decodeCellRecordTrusted(hash, raw)), nil
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

func (s *Store) withBlobBatch(fn func(batch *pebble.Batch) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}
	db := s.blob

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
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

func (s *Store) getBlobCopy(ctx context.Context, key []byte) ([]byte, error) {
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
	return pebbleReaderGetCopy(s.blob, key)
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

func (s *Store) setBlobUnique(batch *pebble.Batch, key, value []byte) error {
	exists, err := pebbleReaderHas(s.blob, key)
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
	hotPrefixCell          = []byte{0x08}
	hotPrefixAccountTx     = []byte{0x09}
	hotPrefixStateCellSync = []byte{0x0A}
	hotPrefixArchiveInfo   = []byte{0x0B}
	hotPrefixStateSync     = []byte{0x0C}
	blobPrefixBlockData    = []byte{0x21}
	blobPrefixProof        = []byte{0x22}
	blobPrefixArchive      = []byte{0x25}
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

func hotKeyCell(hash []byte) []byte {
	buf := append([]byte(nil), hotPrefixCell...)
	return append(buf, hash...)
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

func hotKeyArchiveInfo(masterchainSeqno int32) []byte {
	buf := append([]byte(nil), hotPrefixArchiveInfo...)
	return binary.BigEndian.AppendUint32(buf, uint32(masterchainSeqno))
}

func blobKeyBlockData(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(blobPrefixBlockData, id)
}

func blobKeyProof(kind storage.ServedProofKind, id ton.BlockIDExt) []byte {
	buf := append([]byte(nil), blobPrefixProof...)
	buf = append(buf, byte(proofKindOrder(kind)))
	return append(buf, encodeBlockID(id)...)
}

func blobKeyArchiveChunk(archiveID, offset int64) []byte {
	buf := append([]byte(nil), blobPrefixArchive...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(archiveID))
	return binary.BigEndian.AppendUint64(buf, uint64(offset))
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
	refCount := cellRecordRefCount(record)
	buf := make([]byte, cellRecordEncodedLen(refCount, record.D2, record.Refs))
	encodeCellRecordTo(buf, record, refCount)
	return buf
}

func cellRecordRefCount(record *storage.CellRecord) int32 {
	refCount := record.RefCount
	if refCount <= 0 {
		refCount = 1
	}
	return refCount
}

func encodeCellRecordTo(buf []byte, record *storage.CellRecord, refCount int32) {
	pos := binary.PutUvarint(buf, uint64(refCount))
	buf[pos] = record.D1
	buf[pos+1] = record.D2
	pos += 2

	copy(buf[pos:], record.Data)
	pos += len(record.Data)
	for _, ref := range record.Refs {
		buf[pos] = ref.LevelMask
		pos++
		copy(buf[pos:], ref.Hashes)
		pos += len(ref.Hashes)
		copy(buf[pos:], ref.Depths)
		pos += len(ref.Depths)
	}
}

func cellRecordEncodedLen(refCount int32, d2 byte, refs []storage.CellRefRecord) int {
	size := uvarintLen(uint64(refCount)) + 2 + int(d2/2+d2%2)
	for _, ref := range refs {
		size += 1 + len(ref.Hashes) + len(ref.Depths)
	}
	return size
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func decodeCellRecord(hash []byte, data []byte) (*storage.CellRecord, error) {
	if len(data) < 3 {
		return nil, fmt.Errorf("cell record payload too small")
	}
	return decodeCellRecordBytes(hash, data, true)
}

func decodeCellRecordTrusted(hash []byte, data []byte) *storage.CellRecord {
	refCount, pos := binary.Uvarint(data)
	record := &storage.CellRecord{
		Hash:     hash,
		RefCount: int32(refCount),
		D1:       data[pos],
		D2:       data[pos+1],
	}
	pos += 2

	dataLen := int(record.D2/2 + record.D2%2)
	record.Data = data[pos : pos+dataLen]
	pos += dataLen

	refsCount := int(record.D1 & 7)
	record.Refs = make([]storage.CellRefRecord, refsCount)
	for i := 0; i < refsCount; i++ {
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

	refCount, pos := binary.Uvarint(data)
	if pos <= 0 {
		return nil, fmt.Errorf("invalid cell refcount")
	}
	if refCount == 0 || refCount > 1<<31-1 {
		return nil, fmt.Errorf("invalid cell refcount %d", refCount)
	}
	if len(data)-pos < 2 {
		return nil, fmt.Errorf("cell record payload too small")
	}

	record := &storage.CellRecord{
		Hash:     hash,
		RefCount: int32(refCount),
		D1:       data[pos],
		D2:       data[pos+1],
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
	for i := 0; i < refsCount; i++ {
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
