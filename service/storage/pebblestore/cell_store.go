package pebblestore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
)

const (
	cellDBShardCount = 8
	cellDBDirPrefix  = "celldb"
)

type cellStore struct {
	shards [cellDBShardCount]*cellDBShard
	cache  *pebble.Cache
	mu     sync.Mutex
	dirty  [cellDBShardCount]bool
}

type cellDBShard struct {
	db *pebble.DB
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

func openCellStore(dir string, cacheSize int64, shardMemTableSize, bytesPerSync int, logger zerolog.Logger) (*cellStore, error) {
	cells := &cellStore{cache: pebble.NewCache(cacheSize)}
	for i := range cells.shards {
		shardDir := filepath.Join(dir, fmt.Sprintf("%s-%d", cellDBDirPrefix, i))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			_ = cells.close()
			return nil, fmt.Errorf("create celldb shard %d dir: %w", i, err)
		}

		shardLogger := logger.With().Str("db", "celldb").Int("shard", i).Logger()
		db, err := pebble.Open(shardDir, newCellPebbleOptions(cells.cache, shardMemTableSize, bytesPerSync, shardLogger))
		if err != nil {
			_ = cells.close()
			return nil, fmt.Errorf("open celldb shard %d: %w", i, err)
		}

		cells.shards[i] = &cellDBShard{
			db: db,
		}
	}
	return cells, nil
}

func (c *cellStore) close() error {
	if c == nil {
		return nil
	}

	var err error
	for i, shard := range c.shards {
		if shard == nil {
			continue
		}
		if shard.db != nil {
			err = errors.Join(err, shard.db.Close())
		}
		c.shards[i] = nil
	}
	if c.cache != nil {
		c.cache.Unref()
		c.cache = nil
	}
	return err
}

func (c *cellStore) shardForHash(hash []byte) (int, *cellDBShard, error) {
	if len(hash) != 32 {
		return 0, nil, fmt.Errorf("cell hash size mismatch: %d", len(hash))
	}
	idx := int(hash[0] >> 5)
	shard := c.shards[idx]
	if shard == nil || shard.db == nil {
		return 0, nil, errPebbleClosed
	}
	return idx, shard, nil
}

func (c *cellStore) getCopy(hash []byte) ([]byte, error) {
	_, shard, err := c.shardForHash(hash)
	if err != nil {
		return nil, err
	}
	return pebbleReaderGetCopy(shard.db, cellKey(hash))
}

func (c *cellStore) has(hash []byte) (bool, error) {
	_, shard, err := c.shardForHash(hash)
	if err != nil {
		return false, err
	}
	return pebbleReaderHas(shard.db, cellKey(hash))
}

func (c *cellStore) newBatchWriter() *cellBatchWriter {
	return &cellBatchWriter{store: c}
}

func (c *cellStore) flush() error {
	if c == nil {
		return errPebbleClosed
	}

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
		if shard == nil || shard.db == nil {
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

func (c *cellStore) markDirty(idx int) {
	if c == nil || idx < 0 || idx >= len(c.dirty) {
		return
	}
	c.mu.Lock()
	c.dirty[idx] = true
	c.mu.Unlock()
}

func (c *cellStore) metrics() cellStoreMetrics {
	var metrics cellStoreMetrics
	if c == nil {
		return metrics
	}

	for _, shard := range c.shards {
		if shard == nil || shard.db == nil {
			continue
		}
		dbMetrics := shard.db.Metrics()
		metrics.flushCount += dbMetrics.Flush.Count
		metrics.ingestCount += dbMetrics.Ingest.Count
		metrics.l0Files += dbMetrics.Levels[0].NumFiles
		metrics.l0Size += dbMetrics.Levels[0].Size
		metrics.compactionDebt += dbMetrics.Compact.EstimatedDebt
		metrics.compactionsInProgress += dbMetrics.Compact.NumInProgress
		metrics.compactionBytesPending += dbMetrics.Compact.InProgressBytes
		metrics.memTableSize += dbMetrics.MemTable.Size
		metrics.memTableCount += dbMetrics.MemTable.Count
	}
	return metrics
}

type cellBatchWriter struct {
	store *cellStore

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

	if err = w.batches[idx].Set(cellKey(hash), value, pebble.NoSync); err != nil {
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

	var firstErr error
	for i, batch := range w.batches {
		if batch == nil {
			continue
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("commit celldb shard %d batch: %w", i, err)
			}
		} else {
			w.store.markDirty(i)
		}
		_ = batch.Close()
		w.batches[i] = nil
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
		w.batches[idx] = shard.db.NewBatchWithSize(cellShardBatchInitialSize())
	}
	return idx, nil
}

func cellKey(hash []byte) []byte {
	return hash
}

func cellShardMemTableSize(total int) int {
	shard := total / cellDBShardCount
	if shard < 16<<20 {
		return 16 << 20
	}
	return shard
}

func cellShardBatchInitialSize() int {
	size := stateCellImportBatchTargetBytes / cellDBShardCount
	if size < 1<<20 {
		return 1 << 20
	}
	return size
}
