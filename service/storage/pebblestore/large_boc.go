package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	largeBOCSlowBatchLogThreshold   = 2 * time.Second
	defaultLargeBOCShardReadWorkers = 2
	largeBOCShardReadWorkerMinCell  = 4096
)

type largeBOCRecordVisitor func(shardIdx int, workerIdx int, index int, hash []byte, value []byte) error

// LargeBOCLoadMeta loads compact cell metadata for tonutils-go large-BOC
// serialization. The method groups requests by celldb shard and loads shards in
// parallel while preserving the input order.
func (s *Store) LargeBOCLoadMeta(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error) {
	return s.LargeBOCLoadMetaInGeneration(ctx, 0, hashes, dst)
}

func (s *Store) LargeBOCLoadMetaInGeneration(ctx context.Context, generation uint64, hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error) {
	base := len(dst)
	dst = append(dst, make([]cell.LargeBOCMetaRecord, len(hashes))...)

	stats, err := s.largeBOCLoadRecords(ctx, generation, hashes, s.largeBOCShardReadWorkers, func(_ int, _ int, index int, hash []byte, value []byte) error {
		meta, err := storage.LargeBOCMetaRecordFromEncodedCellRecord(hash, value)
		if err != nil {
			return err
		}
		dst[base+index] = meta
		return nil
	})
	if err != nil {
		return dst[:base], err
	}
	s.logLargeBOCLoadBatch("meta", generation, len(hashes), stats)
	return dst, nil
}

// LargeBOCLoadPayload loads raw cell payloads for tonutils-go large-BOC
// serialization. Payload bytes are copied out of Pebble values because Pebble
// invalidates Get buffers after the closer is closed.
func (s *Store) LargeBOCLoadPayload(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error) {
	return s.LargeBOCLoadPayloadInGeneration(ctx, 0, hashes, dst)
}

func (s *Store) LargeBOCLoadPayloadInGeneration(ctx context.Context, generation uint64, hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error) {
	base := len(dst)
	dst = append(dst, make([]cell.LargeBOCPayloadRecord, len(hashes))...)

	var shardCounts [cellDBShardCount]int
	for i := range hashes {
		shardCounts[int(hashes[i][0]>>5)]++
	}

	arenas := largeBOCPayloadWorkerArenas(shardCounts, s.largeBOCShardReadWorkers)

	stats, err := s.largeBOCLoadRecords(ctx, generation, hashes, s.largeBOCShardReadWorkers, func(shardIdx int, workerIdx int, index int, _ []byte, value []byte) error {
		payload, arena, err := storage.AppendLargeBOCPayloadRecordFromEncodedCellRecord(value, arenas[shardIdx][workerIdx])
		if err != nil {
			return err
		}
		arenas[shardIdx][workerIdx] = arena
		dst[base+index] = payload
		return nil
	})
	if err != nil {
		return dst[:base], err
	}
	s.logLargeBOCLoadBatch("payload", generation, len(hashes), stats)
	return dst, nil
}

// LargeBOCLoadCells loads compact cell metadata and payloads together for
// tonutils-go one-pass large-BOC serialization. Payload bytes are copied out of
// Pebble values and retained in worker-local arenas owned by the returned
// records.
func (s *Store) LargeBOCLoadCells(ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error) {
	return s.LargeBOCLoadCellsInGeneration(ctx, 0, hashes, dst)
}

func (s *Store) LargeBOCLoadCellsInGeneration(ctx context.Context, generation uint64, hashes []cell.Hash, dst []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error) {
	base := len(dst)
	dst = append(dst, make([]cell.LargeBOCRecord, len(hashes))...)

	var shardCounts [cellDBShardCount]int
	for i := range hashes {
		shardCounts[int(hashes[i][0]>>5)]++
	}

	arenas := largeBOCPayloadWorkerArenas(shardCounts, s.largeBOCShardReadWorkers)

	stats, err := s.largeBOCLoadRecords(ctx, generation, hashes, s.largeBOCShardReadWorkers, func(shardIdx int, workerIdx int, index int, hash []byte, value []byte) error {
		record, arena, err := storage.AppendLargeBOCRecordFromEncodedCellRecord(hash, value, arenas[shardIdx][workerIdx])
		if err != nil {
			return err
		}
		arenas[shardIdx][workerIdx] = arena
		dst[base+index] = record
		return nil
	})
	if err != nil {
		return dst[:base], err
	}
	s.logLargeBOCLoadBatch("cells", generation, len(hashes), stats)
	return dst, nil
}

type largeBOCBatchStats struct {
	elapsed time.Duration
	shards  [cellDBShardCount]largeBOCShardBatchStats
}

type largeBOCShardBatchStats struct {
	cells   int
	elapsed time.Duration
}

func (s *Store) largeBOCLoadRecords(ctx context.Context, generation uint64, hashes []cell.Hash, shardReadWorkers int, visit largeBOCRecordVisitor) (largeBOCBatchStats, error) {
	var stats largeBOCBatchStats
	started := time.Now()

	select {
	case <-ctx.Done():
		return stats, ctx.Err()
	default:
	}

	var byShard [cellDBShardCount][]int
	for i := range hashes {
		byShard[int(hashes[i][0]>>5)] = append(byShard[int(hashes[i][0]>>5)], i)
	}
	for _, indexes := range byShard {
		largeBOCSortRecordIndexes(indexes, hashes)
	}

	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return stats, err
	}
	defer cells.release()

	var wg sync.WaitGroup
	errs := make(chan error, cellDBShardCount)
	for shardIdx, indexes := range byShard {
		if len(indexes) == 0 {
			continue
		}
		db := cells.shards[shardIdx].db

		wg.Add(1)
		go func(shardIdx int, db *pebble.DB, indexes []int) {
			defer wg.Done()
			shardStarted := time.Now()
			defer func() {
				stats.shards[shardIdx] = largeBOCShardBatchStats{
					cells:   len(indexes),
					elapsed: time.Since(shardStarted),
				}
			}()

			if err := largeBOCLoadShardRecordIndexes(ctx, cells, db, shardIdx, indexes, hashes, shardReadWorkers, visit); err != nil {
				errs <- err
			}
		}(shardIdx, db, indexes)
	}
	wg.Wait()
	close(errs)
	stats.elapsed = time.Since(started)

	var joinedErr error
	for workerErr := range errs {
		joinedErr = errors.Join(joinedErr, workerErr)
	}
	return stats, joinedErr
}

func largeBOCLoadShardRecordIndexes(ctx context.Context, cells *cellStore, db *pebble.DB, shardIdx int, indexes []int, hashes []cell.Hash, shardReadWorkers int, visit largeBOCRecordVisitor) error {
	shardReadWorkers = largeBOCShardReadWorkerCount(len(indexes), shardReadWorkers)
	if shardReadWorkers <= 1 {
		return largeBOCLoadRecordIndexes(ctx, cells, db, shardIdx, 0, indexes, hashes, visit)
	}

	var wg sync.WaitGroup
	errs := make(chan error, shardReadWorkers)
	for worker := 0; worker < shardReadWorkers; worker++ {
		from := worker * len(indexes) / shardReadWorkers
		to := (worker + 1) * len(indexes) / shardReadWorkers
		chunk := indexes[from:to]
		if len(chunk) == 0 {
			continue
		}

		wg.Add(1)
		go func(workerIdx int, chunk []int) {
			defer wg.Done()
			if err := largeBOCLoadRecordIndexes(ctx, cells, db, shardIdx, workerIdx, chunk, hashes, visit); err != nil {
				errs <- err
			}
		}(worker, chunk)
	}
	wg.Wait()
	close(errs)

	var joinedErr error
	for workerErr := range errs {
		joinedErr = errors.Join(joinedErr, workerErr)
	}
	return joinedErr
}

func largeBOCLoadRecordIndexes(ctx context.Context, cells *cellStore, db *pebble.DB, shardIdx int, workerIdx int, indexes []int, hashes []cell.Hash, visit largeBOCRecordVisitor) (retErr error) {
	var loaded uint64
	defer func() {
		cells.addReadCells(shardIdx, loaded)
	}()

	firstHash := hashes[indexes[0]]
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: firstHash[:]})
	if err != nil {
		return fmt.Errorf("open celldb shard %d iterator: %w", shardIdx, err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close celldb shard %d iterator: %w", shardIdx, err))
		}
	}()

	var previousHash cell.Hash
	valid := false
	for n, index := range indexes {
		if n&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		hash := hashes[index]
		if n == 0 || hash != previousHash {
			valid = iter.SeekGE(hash[:])
			previousHash = hash
		}
		if !valid || !bytes.Equal(iter.Key(), hash[:]) {
			if err := iter.Error(); err != nil {
				return fmt.Errorf("load cell %x: %w", hash[:], err)
			}
			return fmt.Errorf("load cell %x: %w", hash[:], storage.ErrNotFound)
		}
		if err := visit(shardIdx, workerIdx, index, hash[:], iter.Value()); err != nil {
			return fmt.Errorf("decode cell %x: %w", hash[:], err)
		}
		loaded++
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterate celldb shard %d: %w", shardIdx, err)
	}
	return nil
}

func largeBOCSortRecordIndexes(indexes []int, hashes []cell.Hash) {
	sort.Slice(indexes, func(i, j int) bool {
		return bytes.Compare(hashes[indexes[i]][:], hashes[indexes[j]][:]) < 0
	})
}

func largeBOCPayloadWorkerArenas(shardCounts [cellDBShardCount]int, shardReadWorkers int) [cellDBShardCount][][]byte {
	var arenas [cellDBShardCount][][]byte
	for shardIdx, count := range shardCounts {
		workers := largeBOCShardReadWorkerCount(count, shardReadWorkers)
		arenas[shardIdx] = make([][]byte, workers)
		for worker := 0; worker < workers; worker++ {
			from := worker * count / workers
			to := (worker + 1) * count / workers
			arenas[shardIdx][worker] = make([]byte, 0, (to-from)*64)
		}
	}
	return arenas
}

func largeBOCShardReadWorkerCount(cells int, configured int) int {
	if cells <= 0 {
		return 0
	}
	if configured <= 1 || cells < largeBOCShardReadWorkerMinCell {
		return 1
	}
	if configured > cells {
		return cells
	}
	return configured
}

func (s *Store) logLargeBOCLoadBatch(phase string, generation uint64, cells int, stats largeBOCBatchStats) {
	if cells == 0 || stats.elapsed < largeBOCSlowBatchLogThreshold {
		return
	}

	event := s.log.Debug()
	if !event.Enabled() {
		return
	}

	shardIdx, shardCells, shardElapsed := stats.slowestShard()
	event.
		Str("phase", phase).
		Uint64("generation", generation).
		Int("cells", cells).
		Dur("elapsed", stats.elapsed).
		Str("speed", logutil.FormatCellRate(uint64(cells), stats.elapsed)).
		Int("slowest_shard", shardIdx).
		Int("slowest_shard_cells", shardCells).
		Dur("slowest_shard_elapsed", shardElapsed).
		Str("slowest_shard_speed", logutil.FormatCellRate(uint64(shardCells), shardElapsed)).
		Str("shard_cells", stats.shardCellsString()).
		Str("shard_elapsed", stats.shardElapsedString()).
		Msg("large boc cell batch loaded")
}

func (s largeBOCBatchStats) slowestShard() (int, int, time.Duration) {
	shardIdx := -1
	var cells int
	var elapsed time.Duration
	for i, shard := range s.shards {
		if shard.cells == 0 {
			continue
		}
		if shard.elapsed > elapsed {
			shardIdx = i
			cells = shard.cells
			elapsed = shard.elapsed
		}
	}
	return shardIdx, cells, elapsed
}

func (s largeBOCBatchStats) shardCellsString() string {
	var b strings.Builder
	for i, shard := range s.shards {
		if shard.cells == 0 {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&b, "%d:%d", i, shard.cells)
	}
	return b.String()
}

func (s largeBOCBatchStats) shardElapsedString() string {
	var b strings.Builder
	for i, shard := range s.shards {
		if shard.cells == 0 {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&b, "%d:%s", i, shard.elapsed.Truncate(time.Millisecond))
	}
	return b.String()
}
