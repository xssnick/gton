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
	keyArchiveMasterchainBlocks     = 200000

	blockStateMetaVersion  = 1
	artifactRefVersion     = 1
	persistentStateVersion = 1

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

	hot       *pebble.DB
	cells     *cellStore
	cellCache *decodedCellCache
	dir       string
	hotOpts   *pebble.Options
	hotCache  *pebble.Cache

	mu                  sync.RWMutex
	artifactMu          sync.Mutex
	pendingLooseSync    map[string]struct{}
	pendingArchiveSync  map[string]struct{}
	pendingKeyProofSync map[string]struct{}
	closed              bool
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
	if err := os.MkdirAll(filepath.Join(opts.Dir, "packs", "key"), 0o755); err != nil {
		return nil, fmt.Errorf("create key pack dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(opts.Dir, "states"), 0o755); err != nil {
		return nil, fmt.Errorf("create states dir: %w", err)
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
		log:                 logger,
		hot:                 hot,
		cells:               cells,
		cellCache:           newDecodedCellCache(opts.CellCacheSize),
		dir:                 opts.Dir,
		hotOpts:             hotOpts,
		hotCache:            hotCache,
		pendingLooseSync:    map[string]struct{}{},
		pendingArchiveSync:  map[string]struct{}{},
		pendingKeyProofSync: map[string]struct{}{},
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
	if err := s.syncPendingArtifactFiles(); err != nil && firstErr == nil {
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

func (s *Store) StateFilesDir() string {
	return filepath.Join(s.dir, "states")
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
	hotPrefixStateCellSync = []byte{0x0A}
	hotPrefixArchiveInfo   = []byte{0x0B}
	hotPrefixStateSync     = []byte{0x0C}
	hotPrefixBlockDataRef  = []byte{0x0D}
	hotPrefixProofRef      = []byte{0x0E}
	hotPrefixArchiveFile   = []byte{0x0F}
	hotPrefixSeenMaster    = []byte{0x10}
	hotPrefixZeroStateRef  = []byte{0x11}
	hotPrefixKeyProofRef   = []byte{0x12}
	hotPrefixStateFileRef  = []byte{0x13}
	hotPrefixVerifiedKey   = []byte{0x14}
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

func hotKeySeenMasterchainBlock() []byte {
	return bytes.Clone(hotPrefixSeenMaster)
}

func hotKeyVerifiedKeyBlockProgress() []byte {
	return bytes.Clone(hotPrefixVerifiedKey)
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

type persistentStateFileRecord struct {
	ref           *storage.ArtifactRef
	fileHash      []byte
	stateRootHash []byte
	cellsCount    uint64
}

func encodePersistentStateFileRecord(file *storage.PersistentStateFile) []byte {
	ref := encodeArtifactRef(file.Ref)
	buf := make([]byte, 0, 1+8+1+len(file.FileHash)+1+len(file.StateRootHash)+4+len(ref))
	buf = append(buf, persistentStateVersion)
	buf = binary.BigEndian.AppendUint64(buf, file.CellsCount)
	buf = appendLenBytes(buf, file.FileHash)
	buf = appendLenBytes(buf, file.StateRootHash)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ref)))
	return append(buf, ref...)
}

func decodePersistentStateFileRecord(data []byte) (*persistentStateFileRecord, error) {
	if len(data) < 1+8+1+1+4 {
		return nil, fmt.Errorf("persistent state file payload truncated")
	}
	if data[0] != persistentStateVersion {
		return nil, fmt.Errorf("persistent state file version mismatch")
	}
	pos := 1
	cellsCount := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

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
		cellsCount:    cellsCount,
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
