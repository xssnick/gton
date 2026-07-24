package pebblestore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	cellDBShardCount              = 8
	cellDBRootDir                 = "celldb"
	initialCellGenerationID       = 1
	cellGenerationManifestVersion = 1
	cellGenerationDirTemplate     = "gen%d-shard%d"
	cellStoreAggressiveCloseGrace = 500 * time.Millisecond
)

type cellStore struct {
	generation  uint64
	shards      [cellDBShardCount]*cellDBShard
	cache       *pebble.Cache
	fileCache   *pebble.FileCache
	compactions *pebbleCompactionController
	closeMu     sync.Mutex
	closing     atomic.Bool
	refs        atomic.Int64
	io          cellStoreIOCounters
	drained     chan struct{}
	drainOnce   sync.Once
	closed      bool
	mu          sync.Mutex
	dirty       [cellDBShardCount]bool
}

type cellDBShard struct {
	db *pebble.DB
}

type cellStoreIOCounters struct {
	readCells    [cellDBShardCount]atomic.Uint64
	writtenCells [cellDBShardCount]atomic.Uint64
	rates        cellStoreIORateSampler
}

type cellStoreIOStatus struct {
	readCells             uint64
	writtenCells          uint64
	readCellsPerSecond    float64
	writtenCellsPerSecond float64
}

type cellStoreIORateSampler struct {
	mu                    sync.Mutex
	lastAt                time.Time
	lastReadCells         [cellDBShardCount]uint64
	lastWrittenCells      [cellDBShardCount]uint64
	readCellsPerSecond    [cellDBShardCount]float64
	writtenCellsPerSecond [cellDBShardCount]float64
}

type cellStoreMetrics struct {
	flushCount             int64
	ingestCount            uint64
	l0Files                int64
	l0Size                 int64
	compactionDebt         uint64
	compactionsInProgress  int64
	compactionBytesPending int64
	memTableSize           uint64
	memTableCount          int64
}

func openCellStore(dir string, generation uint64, fs vfs.FS, cacheSize int64, shardMemTableSize, memTableStopWritesThreshold, bytesPerSync int, readOnly bool, logger zerolog.Logger) (*cellStore, error) {
	if generation == 0 {
		return nil, fmt.Errorf("cell generation is zero")
	}

	started := time.Now()
	logger.Info().
		Uint64("generation", generation).
		Int64("cache_size", cacheSize).
		Int("shard_memtable_size", shardMemTableSize).
		Int("memtable_stop_writes_threshold", memTableStopWritesThreshold).
		Int("bytes_per_sync", bytesPerSync).
		Int("compaction_parallelism", pebbleCellCompactionParallelism()).
		Int("max_concurrent_compactions", pebbleCellMaxConcurrentCompactions()).
		Int("shards", cellDBShardCount).
		Int("open_parallelism", cellDBShardCount).
		Bool("read_only", readOnly).
		Msg("opening celldb generation")
	cells := &cellStore{
		generation: generation,
		cache:      pebble.NewCache(cacheSize),
		fileCache:  newPebbleFileCache(defaultPebbleCellMaxOpenFiles),
		drained:    make(chan struct{}),
	}
	compactions := newPebbleCompactionController(pebbleCellCompactionParallelism())
	cells.compactions = compactions

	var wg sync.WaitGroup
	errs := make([]error, cellDBShardCount)
	for i := range cells.shards {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			shardDir := cellGenerationShardDir(dir, generation, i)
			if !readOnly {
				if err := os.MkdirAll(shardDir, 0o755); err != nil {
					errs[i] = fmt.Errorf("create celldb generation %d shard %d dir: %w", generation, i, err)
					return
				}
			}

			shardLogger := logger.With().Str("db", "celldb").Uint64("generation", generation).Int("shard", i).Logger()
			compactionScheduler := compactions.newScheduler()
			shardOpts := newCellPebbleOptions(cells.cache, cells.fileCache, fs, shardMemTableSize, memTableStopWritesThreshold, bytesPerSync, compactionScheduler, shardLogger)
			shardOpts.ReadOnly = readOnly
			shardStarted := time.Now()
			logger.Info().
				Uint64("generation", generation).
				Int("shard", i).
				Str("dir", shardDir).
				Msg("opening celldb shard")
			db, err := pebble.Open(shardDir, shardOpts)
			if err != nil {
				errs[i] = fmt.Errorf("open celldb generation %d shard %d: %w", generation, i, err)
				return
			}
			logger.Info().
				Uint64("generation", generation).
				Int("shard", i).
				Str("dir", shardDir).
				Dur("elapsed", time.Since(shardStarted)).
				Msg("opened celldb shard")

			cells.shards[i] = &cellDBShard{
				db: db,
			}
		}(i)
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		_ = cells.close()
		return nil, err
	}
	compactions.start()

	logger.Info().
		Uint64("generation", generation).
		Int("shards", cellDBShardCount).
		Dur("elapsed", time.Since(started)).
		Msg("opened celldb generation")
	return cells, nil
}

func cellGenerationShardDir(dir string, generation uint64, shard int) string {
	return filepath.Join(dir, cellDBRootDir, fmt.Sprintf(cellGenerationDirTemplate, generation, shard))
}

func (c *cellStore) close() error {
	return c.closeWithLogger(zerolog.Nop())
}

func (c *cellStore) closeWithLogger(logger zerolog.Logger) error {
	started := time.Now()
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closing.Store(true)
	if c.refs.Load() == 0 {
		c.signalDrained()
	}
	<-c.drained
	c.closed = true

	err := c.closeShards(logger)
	stageStarted := time.Now()
	logger.Info().Msg("closing celldb file cache")
	c.fileCache.Unref()
	c.fileCache = nil
	logger.Info().
		Dur("elapsed", time.Since(stageStarted)).
		Msg("closed celldb file cache")

	stageStarted = time.Now()
	logger.Info().Msg("closing celldb cache")
	c.cache.Unref()
	c.cache = nil
	logger.Info().
		Dur("elapsed", time.Since(stageStarted)).
		Msg("closed celldb cache")
	logger.Debug().
		Dur("elapsed", time.Since(started)).
		Msg("closed celldb store")
	return err
}

func (c *cellStore) closeAggressively() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closing.Store(true)
	if c.refs.Load() == 0 {
		c.signalDrained()
	}
	timer := time.NewTimer(cellStoreAggressiveCloseGrace)
	select {
	case <-c.drained:
	case <-timer.C:
	}
	timer.Stop()
	c.closed = true

	err := c.closeShards(zerolog.Nop())
	c.fileCache.Unref()
	c.fileCache = nil
	c.cache.Unref()
	c.cache = nil
	return err
}

func (c *cellStore) closeShards(logger zerolog.Logger) error {
	var wg sync.WaitGroup
	var errs [cellDBShardCount]error
	for i, shard := range c.shards {
		if shard == nil {
			continue
		}

		wg.Go(func() {
			shardStarted := time.Now()
			logger.Info().Int("shard", i).Msg("closing celldb shard")
			errs[i] = shard.db.Close()
			logger.Info().
				Int("shard", i).
				Dur("elapsed", time.Since(shardStarted)).
				Msg("closed celldb shard")
		})
	}
	wg.Wait()

	for i := range c.shards {
		c.shards[i] = nil
	}
	return errors.Join(errs[:]...)
}

func (c *cellStore) acquire() error {
	if c.closing.Load() {
		return errPebbleClosed
	}
	c.refs.Add(1)
	if c.closing.Load() {
		c.release()
		return errPebbleClosed
	}
	return nil
}

func (c *cellStore) release() {
	if c.refs.Add(-1) == 0 && c.closing.Load() {
		c.signalDrained()
	}
}

func (c *cellStore) signalDrained() {
	c.drainOnce.Do(func() {
		close(c.drained)
	})
}

func (c *cellStore) throttleCompactions() func() {
	return c.compactions.beginForegroundRead()
}

// cellShardIndex maps a cell hash to its celldb shard.
func cellShardIndex(hash cell.Hash) int {
	return int(hash[0] >> 5)
}

func (c *cellStore) shardForHash(hash []byte) (int, *cellDBShard, error) {
	if len(hash) != 32 {
		return 0, nil, fmt.Errorf("cell hash size mismatch: %d", len(hash))
	}
	idx := int(hash[0] >> 5)
	shard := c.shards[idx]
	if shard == nil {
		return 0, nil, errPebbleClosed
	}
	return idx, shard, nil
}

func (c *cellStore) getCopy(hash []byte) ([]byte, error) {
	idx, shard, err := c.shardForHash(hash)
	if err != nil {
		return nil, err
	}
	value, err := pebbleReaderGetCopy(shard.db, hash, nil)
	if err != nil {
		return nil, err
	}
	c.addReadCells(idx, 1)
	return value, nil
}

func (c *cellStore) has(hash []byte) (bool, error) {
	_, shard, err := c.shardForHash(hash)
	if err != nil {
		return false, err
	}
	return pebbleReaderHas(shard.db, hash)
}

func (c *cellStore) newBatchWriter(shardBatchInitialSize int) *cellBatchWriter {
	return &cellBatchWriter{
		store:                 c,
		shardBatchInitialSize: shardBatchInitialSize,
	}
}

func (c *cellStore) flush() error {
	c.mu.Lock()
	dirty := c.dirty
	for i := range c.dirty {
		c.dirty[i] = false
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, len(c.shards))
	for i, shard := range c.shards {
		if !dirty[i] {
			continue
		}
		wg.Add(1)
		go func(i int, db *pebble.DB) {
			defer wg.Done()
			if err := db.Flush(); err != nil {
				errs[i] = fmt.Errorf("flush celldb shard %d: %w", i, err)
			}
		}(i, shard.db)
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			continue
		}
		c.markDirty(i)
	}
	return errors.Join(errs...)
}

func (c *cellStore) dirtyShardsSnapshot() []int {
	c.mu.Lock()
	defer c.mu.Unlock()

	var dirty []int
	for i, isDirty := range c.dirty {
		if isDirty {
			dirty = append(dirty, i)
		}
	}
	return dirty
}

func (c *cellStore) markDirty(idx int) {
	if idx < 0 || idx >= len(c.dirty) {
		return
	}
	c.mu.Lock()
	c.dirty[idx] = true
	c.mu.Unlock()
}

func (c *cellStore) metrics() cellStoreMetrics {
	var metrics cellStoreMetrics
	for _, shard := range c.shards {
		if shard == nil {
			continue
		}
		dbMetrics := shard.db.Metrics()
		metrics.flushCount += dbMetrics.Flush.Count
		metrics.ingestCount += dbMetrics.Ingest.Count
		metrics.l0Files += dbMetrics.Levels[0].TablesCount
		metrics.l0Size += dbMetrics.Levels[0].TablesSize
		metrics.compactionDebt += dbMetrics.Compact.EstimatedDebt
		metrics.compactionsInProgress += dbMetrics.Compact.NumInProgress
		metrics.compactionBytesPending += dbMetrics.Compact.InProgressBytes
		metrics.memTableSize += dbMetrics.MemTable.Size
		metrics.memTableCount += dbMetrics.MemTable.Count
	}
	return metrics
}

func (c *cellStore) addReadCells(shard int, count uint64) {
	if count == 0 || shard < 0 || shard >= cellDBShardCount {
		return
	}
	c.io.readCells[shard].Add(count)
}

func (c *cellStore) addWrittenCells(shard int, count uint64) {
	if count == 0 || shard < 0 || shard >= cellDBShardCount {
		return
	}
	c.io.writtenCells[shard].Add(count)
}

func (c *cellStore) ioStatus(now time.Time) [cellDBShardCount]cellStoreIOStatus {
	var status [cellDBShardCount]cellStoreIOStatus
	for i := range status {
		status[i].readCells = c.io.readCells[i].Load()
		status[i].writtenCells = c.io.writtenCells[i].Load()
	}

	c.io.rates.mu.Lock()
	defer c.io.rates.mu.Unlock()

	if !c.io.rates.lastAt.IsZero() {
		elapsed := now.Sub(c.io.rates.lastAt).Seconds()
		if elapsed > 0 {
			for i := range status {
				c.io.rates.readCellsPerSecond[i] = float64(status[i].readCells-c.io.rates.lastReadCells[i]) / elapsed
				c.io.rates.writtenCellsPerSecond[i] = float64(status[i].writtenCells-c.io.rates.lastWrittenCells[i]) / elapsed
			}
		}
	}

	for i := range status {
		c.io.rates.lastReadCells[i] = status[i].readCells
		c.io.rates.lastWrittenCells[i] = status[i].writtenCells
		status[i].readCellsPerSecond = c.io.rates.readCellsPerSecond[i]
		status[i].writtenCellsPerSecond = c.io.rates.writtenCellsPerSecond[i]
	}
	c.io.rates.lastAt = now
	return status
}

type cellBatchWriter struct {
	store                 *cellStore
	shardBatchInitialSize int

	batches [cellDBShardCount]*pebble.Batch

	cellsByShard [cellDBShardCount]int
	bytesByShard [cellDBShardCount]int

	cellsInBatch int
	bytesInBatch int
}

func (w *cellBatchWriter) set(hash []byte, value []byte) error {
	idx, err := w.ensureBatch(hash)
	if err != nil {
		return err
	}

	if err = w.batches[idx].Set(hash, value, pebble.NoSync); err != nil {
		return err
	}

	w.cellsInBatch++
	w.bytesInBatch += len(value)
	w.cellsByShard[idx]++
	w.bytesByShard[idx] += len(value)
	return nil
}

func (w *cellBatchWriter) setDeferred(hash []byte, valueLen int, encode func([]byte)) error {
	idx, err := w.ensureBatch(hash)
	if err != nil {
		return err
	}

	op := w.batches[idx].SetDeferred(len(hash), valueLen)
	copy(op.Key, hash)
	encode(op.Value)
	if err = op.Finish(); err != nil {
		return err
	}

	w.cellsInBatch++
	w.bytesInBatch += valueLen
	w.cellsByShard[idx]++
	w.bytesByShard[idx] += valueLen
	return nil
}

func (w *cellBatchWriter) flush() (stateCellWriteStats, error) {
	stats := stateCellWriteStats{
		cells: int64(w.cellsInBatch),
		bytes: int64(w.bytesInBatch),
	}
	if w.cellsInBatch == 0 {
		return stats, nil
	}

	// Shard batches target independent pebble DBs, so their memtable commits can
	// run concurrently. Keep the single-batch case inline to avoid goroutine
	// overhead for per-shard writers.
	var commitErrs [cellDBShardCount]error
	pendingShards := 0
	lastPending := -1
	for i, batch := range w.batches {
		if batch == nil || w.cellsByShard[i] == 0 {
			continue
		}
		pendingShards++
		lastPending = i
	}
	if pendingShards == 1 {
		commitErrs[lastPending] = w.batches[lastPending].Commit(pebble.NoSync)
	} else if pendingShards > 1 {
		var wg sync.WaitGroup
		for i, batch := range w.batches {
			if batch == nil || w.cellsByShard[i] == 0 {
				continue
			}
			wg.Add(1)
			go func(i int, batch *pebble.Batch) {
				defer wg.Done()
				commitErrs[i] = batch.Commit(pebble.NoSync)
			}(i, batch)
		}
		wg.Wait()
	}

	var firstErr error
	for i, batch := range w.batches {
		if batch == nil {
			continue
		}
		if err := commitErrs[i]; err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("commit celldb shard %d batch: %w", i, err)
			}
			_ = batch.Close()
			w.batches[i] = nil
		} else {
			w.store.addWrittenCells(i, uint64(w.cellsByShard[i]))
			w.store.markDirty(i)
			batch.Reset()
		}
		w.cellsByShard[i] = 0
		w.bytesByShard[i] = 0
	}

	if firstErr != nil {
		return stateCellWriteStats{}, firstErr
	}

	w.cellsInBatch = 0
	w.bytesInBatch = 0
	return stats, nil
}

func (w *cellBatchWriter) close() {
	for i, batch := range w.batches {
		if batch == nil {
			continue
		}
		_ = batch.Close()
		w.batches[i] = nil
	}
}

func (w *cellBatchWriter) ensureBatch(hash []byte) (int, error) {
	idx, shard, err := w.store.shardForHash(hash)
	if err != nil {
		return 0, err
	}
	if w.batches[idx] == nil {
		w.batches[idx] = newCellShardBatch(shard.db, w.shardBatchInitialSize)
	}
	return idx, nil
}

func newCellShardBatch(db *pebble.DB, size int) *pebble.Batch {
	return db.NewBatchWithSize(size, pebble.WithMaxRetainedSizeBytes(size))
}

func cellShardMemTableSize(total int) int {
	shard := total / cellDBShardCount
	if shard < 16<<20 {
		return 16 << 20
	}
	return shard
}

func cellShardBatchInitialSize(aggregateBytes int) int {
	size := aggregateBytes / cellDBShardCount
	if size < 1<<20 {
		return 1 << 20
	}
	return size
}
