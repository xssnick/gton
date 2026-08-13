package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Store) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error) {
	generation, err := s.activeCellGenerationID()
	if err != nil {
		return nil, err
	}
	rootHash, err := s.persistStateCellTreeInGeneration(ctx, generation, block, root, totalCells)
	if err != nil {
		return nil, err
	}
	return s.loadActiveLazyCell(ctx, rootHash[:])
}

func (s *Store) ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error) {
	generation, err := s.activeCellGenerationID()
	if err != nil {
		return nil, err
	}
	rootHash, err := s.persistStateBOCViewInGeneration(ctx, generation, block, view)
	if err != nil {
		return nil, err
	}
	return s.loadActiveLazyCell(ctx, rootHash[:])
}

func (s *Store) TrustImportedStateCellHashes() bool {
	return false
}

func (s *Store) ReuseImportedSplitStatePartCells() bool {
	return true
}

func (s *Store) SaveStateCellRecords(ctx context.Context, records storage.StateCellRecords) error {
	generation, err := s.activeCellGenerationID()
	if err != nil {
		return err
	}
	_, err = s.saveCellRecordSet(ctx, records, false, generation, false)
	return err
}

func (s *Store) FlushStateCells(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	generation, err := s.activeCellGenerationID()
	if err != nil {
		return err
	}
	return s.flushCellDBs(generation)
}

func (s *Store) persistStateCellTreeInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (cell.Hash, error) {
	rootHash := root.HashKey()
	if _, err := s.saveStateCellTree(ctx, stateCellTreeSave{
		block:          block,
		root:           root,
		totalCells:     totalCells,
		cellGeneration: generation,
	}); err != nil {
		return cell.Hash{}, err
	}
	if err := s.flushCellDBs(generation); err != nil {
		return cell.Hash{}, fmt.Errorf("flush generation %d state cells before returning lazy root: %w", generation, err)
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cell_generation", generation).
		Uint64("cells", totalCells).
		Msg("state cell tree imported")
	return rootHash, nil
}

func (s *Store) persistStateBOCViewInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, view *cell.BOCView) (cell.Hash, error) {
	roots := view.Roots()
	if len(roots) != 1 {
		return cell.Hash{}, fmt.Errorf("state boc should contain exactly one root, got %d", len(roots))
	}

	rootCell, _, err := view.ReadCell(roots[0], nil)
	if err != nil {
		return cell.Hash{}, fmt.Errorf("load state boc root cell: %w", err)
	}
	if rootCell.D1&0b1000 != 0 && len(rootCell.Body) > 0 && cell.Type(rootCell.Body[0]) == cell.PrunedCellType {
		return cell.Hash{}, fmt.Errorf("state cell tree root is pruned")
	}

	rootHash := rootCell.Meta.Hash
	if err = s.saveStateBOCView(ctx, generation, block, view); err != nil {
		return cell.Hash{}, err
	}
	if err = s.flushCellDBs(generation); err != nil {
		return cell.Hash{}, fmt.Errorf("flush generation %d boc state cells before returning lazy root: %w", generation, err)
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cell_generation", generation).
		Uint64("cells", uint64(view.Cells())).
		Msg("boc state cells imported")
	return rootHash, nil
}

func (s *Store) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error) {
	state, err := s.blockStateMeta(ctx, block)
	if err != nil {
		return nil, err
	}

	if len(rootHash) > 0 && !bytes.Equal(state.StateRootHash, rootHash) {
		return nil, storage.ErrNotFound
	}

	root, err := s.loadActiveLazyCell(ctx, state.StateRootHash)
	if err != nil {
		return nil, err
	}
	hash := root.HashKeyAt(0)
	if !bytes.Equal(hash[:], state.StateRootHash) {
		return nil, storage.ErrNotFound
	}
	return root, nil
}

func (s *Store) replaceBlockStateWithLazyRoot(state *storage.BlockState, saved storage.BlockState, parsed *storage.BlockState, root *cell.Cell) {
	state.Block = saved.Block
	state.StateRootHash = bytes.Clone(saved.StateRootHash)
	state.StateFileHash = bytes.Clone(saved.StateFileHash)
	if saved.MasterchainRef == nil {
		state.MasterchainRef = nil
	} else {
		ref := *saved.MasterchainRef
		state.MasterchainRef = &ref
	}
	state.Cell = root
	state.Parsed = nil
	if parsed != nil {
		state.Parsed = parsed.Parsed
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(saved.Block)).
		Msg("block state switched to lazy celldb root")
}

func (s *Store) saveCellsInGeneration(ctx context.Context, generation uint64, records []*storage.CellRecord) error {
	encoded := make([]storage.EncodedCellRecord, 0, len(records))
	for _, record := range records {
		if len(record.Hash) != 32 {
			return fmt.Errorf("cell record hash size mismatch: %d", len(record.Hash))
		}

		var hash cell.Hash
		copy(hash[:], record.Hash)
		encoded = append(encoded, storage.EncodedCellRecord{
			Hash: hash,
			Data: storage.EncodeCellRecord(record),
		})
	}

	_, err := s.saveCellRecordBatch(ctx, encoded, true, generation, true)
	return err
}

func (s *Store) saveEncodedCellsInGeneration(ctx context.Context, generation uint64, records []storage.EncodedCellRecord, sync bool) error {
	_, err := s.saveCellRecordBatch(ctx, records, sync, generation, true)
	return err
}

type cellRecordBatchStats struct {
	written int
	skipped int
	bytes   int64
}

func (s *Store) savePreparedStateCellRecords(ctx context.Context, records storage.StateCellRecords, generation uint64) (stateCellSaveStats, error) {
	stats, err := s.saveCellRecordSet(ctx, records, false, generation, false)
	if err != nil {
		return stateCellSaveStats{}, err
	}
	return stateCellSaveStats{applied: stats.written > 0}, nil
}

func (s *Store) saveCellRecordBatch(ctx context.Context, records []storage.EncodedCellRecord, sync bool, generation uint64, dedupe bool) (cellRecordBatchStats, error) {
	return s.saveCellRecordSet(ctx, storage.NewStateCellRecords(records), sync, generation, dedupe)
}

// stateCellSaveShardedMinRecords is the record count at which saveCellRecordSet
// routes work to one writer per celldb shard. Small live-tail prewrites stay on
// the single-writer path to avoid goroutine and routing-table overhead.
const stateCellSaveShardedMinRecords = 4096

const (
	// liveStateCellBatchMinInitialBytes keeps a floor under the per-shard batch
	// so tiny prewrites still avoid the first few grow-and-copy rounds.
	liveStateCellBatchMinInitialBytes = 64 << 10
	// liveStateCellBatchRecordOverhead covers the Pebble batch record framing
	// around each value: kind byte, key and value varint lengths and the
	// 32-byte cell hash. Real framing is ~36 B; the margin absorbs per-shard
	// record count skew without growing the batch.
	liveStateCellBatchRecordOverhead = 64
)

// liveStateCellBatchInitialSize sizes a live celldb batch from the payload it
// is about to hold instead of always reserving the bulk-import target. Writing
// one applied block used to reserve stateCellImportBatchTargetBytes across the
// shards regardless of set size, so a few hundred kilobytes of state update
// materialized 128 MiB of batch buffer.
//
// Bulk persistent-state import deliberately keeps the large batches: there the
// reservation is a throughput parameter, not an overshoot. The flush thresholds
// are separate checks and stay unchanged, so commit cadence is unaffected.
func liveStateCellBatchInitialSize(records storage.StateCellRecords) int {
	maxAggregateBytes := uint64(stateCellImportBatchTargetBytes)
	estimatedBytes := records.ByteSize()
	if estimatedBytes >= maxAggregateBytes {
		return cellShardBatchInitialSize(stateCellImportBatchTargetBytes)
	}

	remaining := maxAggregateBytes - estimatedBytes
	recordCount := uint64(records.Len())
	if recordCount > remaining/liveStateCellBatchRecordOverhead {
		return cellShardBatchInitialSize(stateCellImportBatchTargetBytes)
	}
	estimatedBytes += recordCount * liveStateCellBatchRecordOverhead

	perShard := (estimatedBytes + cellDBShardCount - 1) / cellDBShardCount
	if perShard < liveStateCellBatchMinInitialBytes {
		return liveStateCellBatchMinInitialBytes
	}
	return int(perShard)
}

func (s *Store) saveCellRecordSet(ctx context.Context, records storage.StateCellRecords, sync bool, generation uint64, dedupe bool) (cellRecordBatchStats, error) {
	var stats cellRecordBatchStats
	if records.Empty() {
		return stats, nil
	}
	if err := s.ensureWritable(); err != nil {
		return stats, err
	}

	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return stats, err
	}
	defer cells.release()

	if records.Len() >= stateCellSaveShardedMinRecords {
		stats, err = saveCellRecordSetSharded(ctx, cells, records, dedupe)
	} else {
		stats, err = saveCellRecords(ctx, cells, records, dedupe)
	}
	if err != nil {
		return stats, err
	}

	if sync {
		if err := cells.flush(); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

// saveCellRecordSetSharded routes every record once into compact per-shard
// bitsets. Workers visit only their set bits and read records from the original
// immutable chunks, preserving parallel Pebble batch construction without
// copying record descriptors or cell payloads.
func saveCellRecordSetSharded(ctx context.Context, cells *cellStore, records storage.StateCellRecords, dedupe bool) (cellRecordBatchStats, error) {
	if err := ctx.Err(); err != nil {
		return cellRecordBatchStats{}, err
	}

	chunks := records.AppendChunks(nil)
	routes, err := buildCellShardRoutes(ctx, chunks)
	if err != nil {
		return cellRecordBatchStats{}, err
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	batchInitialSize := liveStateCellBatchInitialSize(records)
	var shardStats [cellDBShardCount]cellRecordBatchStats
	var shardErrs [cellDBShardCount]error
	var wg sync.WaitGroup
	for shard := 0; shard < cellDBShardCount; shard++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			shardStats[shard], shardErrs[shard] = saveCellRecordShard(
				workerCtx,
				cells,
				chunks,
				routes,
				records.Len(),
				dedupe,
				shard,
				batchInitialSize,
			)
			if shardErrs[shard] != nil {
				cancel()
			}
		}(shard)
	}
	wg.Wait()

	var stats cellRecordBatchStats
	var firstErr error
	for shard := 0; shard < cellDBShardCount; shard++ {
		stats.written += shardStats[shard].written
		stats.skipped += shardStats[shard].skipped
		stats.bytes += shardStats[shard].bytes
		if shardErrs[shard] != nil && firstErr == nil {
			firstErr = shardErrs[shard]
		}
	}
	// A worker failure cancels its siblings. Prefer that root cause over a
	// sibling's derived context.Canceled error.
	if firstErr != nil && errors.Is(firstErr, context.Canceled) && ctx.Err() == nil {
		for shard := 0; shard < cellDBShardCount; shard++ {
			if shardErrs[shard] != nil && !errors.Is(shardErrs[shard], context.Canceled) {
				firstErr = shardErrs[shard]
				break
			}
		}
	}
	return stats, firstErr
}

func buildCellShardRoutes(ctx context.Context, chunks [][]storage.EncodedCellRecord) ([][]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	routes := make([][]uint64, len(chunks))
	totalWords := 0
	for _, chunk := range chunks {
		totalWords += ((len(chunk) + 63) / 64) * cellDBShardCount
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	routeWords := make([]uint64, totalWords)
	offset := 0
	position := 0
	for chunkIndex, chunk := range chunks {
		wordsPerShard := (len(chunk) + 63) / 64
		chunkWords := wordsPerShard * cellDBShardCount
		routes[chunkIndex] = routeWords[offset : offset+chunkWords]
		offset += chunkWords

		for i := range chunk {
			if position&0x3fff == 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
				}
			}
			shard := cellShardIndex(chunk[i].Hash)
			routes[chunkIndex][shard*wordsPerShard+(i>>6)] |= uint64(1) << uint(i&63)
			position++
		}
	}
	return routes, nil
}

func saveCellRecordShard(ctx context.Context, cells *cellStore, chunks [][]storage.EncodedCellRecord, routes [][]uint64, recordCount int, dedupe bool, shard int, batchInitialSize int) (cellRecordBatchStats, error) {
	var stats cellRecordBatchStats
	writer := cells.newBatchWriter(batchInitialSize)
	defer writer.close()

	var written map[cell.Hash]struct{}
	if dedupe {
		written = make(map[cell.Hash]struct{}, recordCount/cellDBShardCount)
	}
	wordsVisited := 0
	for chunkIndex, chunk := range chunks {
		wordsPerShard := len(routes[chunkIndex]) / cellDBShardCount
		routeBits := routes[chunkIndex][shard*wordsPerShard : (shard+1)*wordsPerShard]
		for wordIndex, word := range routeBits {
			if wordsVisited&0xff == 0 {
				select {
				case <-ctx.Done():
					return stats, ctx.Err()
				default:
				}
			}
			wordsVisited++

			for word != 0 {
				recordIndex := wordIndex*64 + bits.TrailingZeros64(word)
				word &= word - 1
				record := &chunk[recordIndex]
				if len(record.Data) == 0 {
					return stats, fmt.Errorf("encoded cell record is empty")
				}

				if dedupe {
					if _, ok := written[record.Hash]; ok {
						stats.skipped++
						continue
					}
					written[record.Hash] = struct{}{}
				}

				if err := writer.set(record.Hash[:], record.Data); err != nil {
					return stats, err
				}
				stats.written++
				stats.bytes += int64(len(record.Data))

				if writer.bytesInBatch >= stateCellImportBatchTargetBytes/cellDBShardCount {
					if _, err := writer.flush(); err != nil {
						return stats, err
					}
				}
			}
		}
	}

	if _, err := writer.flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

func saveCellRecords(ctx context.Context, cells *cellStore, records storage.StateCellRecords, dedupe bool) (cellRecordBatchStats, error) {
	var stats cellRecordBatchStats
	writer := cells.newBatchWriter(liveStateCellBatchInitialSize(records))
	defer writer.close()

	var written map[cell.Hash]struct{}
	if dedupe {
		written = make(map[cell.Hash]struct{}, records.Len())
	}
	i := 0
	if err := records.ForEach(func(record storage.EncodedCellRecord) error {
		if i&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		i++

		if len(record.Data) == 0 {
			return fmt.Errorf("encoded cell record is empty")
		}

		if dedupe {
			if _, ok := written[record.Hash]; ok {
				stats.skipped++
				return nil
			}
			written[record.Hash] = struct{}{}
		}

		if err := writer.set(record.Hash[:], record.Data); err != nil {
			return err
		}
		stats.written++
		stats.bytes += int64(len(record.Data))

		shard := cellShardIndex(record.Hash)
		if writer.bytesByShard[shard] >= stateCellImportBatchTargetBytes/cellDBShardCount {
			if _, err := writer.flush(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return stats, err
	}

	if _, err := writer.flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Store) CellRecord(ctx context.Context, hash []byte) (*storage.CellRecord, error) {
	cells, err := s.acquireActiveCellStore(ctx)
	if err != nil {
		return nil, err
	}
	defer cells.release()

	return cellRecordFromStore(cells, hash)
}

func (s *Store) cellRecordInGeneration(ctx context.Context, generation uint64, hash []byte) (*storage.CellRecord, error) {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return nil, err
	}
	defer cells.release()

	return cellRecordFromStore(cells, hash)
}

func cellRecordFromStore(cells *cellStore, hash []byte) (*storage.CellRecord, error) {
	raw, err := cells.getCopy(hash)
	if err != nil {
		return nil, err
	}

	record, err := storage.DecodeCellRecord(hash, raw)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (s *Store) LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return s.loadActiveLazyCell(ctx, hash)
}

func (s *Store) LazyCellLoader() cell.LazyCellLoader {
	return s.activeCellLoader
}

func (s *Store) newLazyCellLoaderForGeneration(generation uint64) cell.LazyCellLoader {
	var loader cell.LazyCellLoader
	loadMiss := func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := s.loadLazyCellMissFromGeneration(context.Background(), generation, hash[:], loader)
		if err != nil {
			return nil, fmt.Errorf("load lazy cell %x: %w", hash[:], err)
		}
		return loaded, nil
	}
	loader = s.cachedCellLoader(generation, loadMiss)
	return loader
}

const activeCellCacheNamespace uint64 = 0

func (s *Store) newActiveCellLoader() cell.LazyCellLoader {
	loadMiss := func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := s.loadActiveLazyCellMiss(context.Background(), hash[:])
		if err != nil {
			return nil, fmt.Errorf("load lazy cell %x: %w", hash[:], err)
		}
		return loaded, nil
	}

	return s.cachedCellLoader(activeCellCacheNamespace, loadMiss)
}

func (s *Store) cachedCellLoader(cacheNamespace uint64, loadMiss cell.LazyCellLoader) cell.LazyCellLoader {
	// Resolve the cache branch once rather than per lookup.
	if s.cellCache == nil {
		return loadMiss
	}

	return func(hash cell.Hash) (*cell.Cell, error) {
		// getHash reports storage.ErrNotFound and nothing else, so any error here
		// is a plain miss.
		if loaded, err := s.cellCache.getHash(cacheNamespace, hash); err == nil {
			s.lazyCellLoads.observeDecodedCache()
			return loaded, nil
		}
		return loadMiss(hash)
	}
}

func (s *Store) loadLazyCellFromGeneration(
	ctx context.Context,
	generation uint64,
	hash []byte,
	loader cell.LazyCellLoader,
) (*cell.Cell, error) {
	if generation == 0 {
		return nil, fmt.Errorf("cell generation is zero")
	}

	loaded, err := s.cellCache.get(generation, hash)
	if err == nil {
		s.lazyCellLoads.observeDecodedCache()
		return loaded, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	return s.loadLazyCellMissFromGeneration(ctx, generation, hash, loader)
}

func (s *Store) loadLazyCellMissFromGeneration(ctx context.Context, generation uint64, hash []byte, loader cell.LazyCellLoader) (*cell.Cell, error) {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return nil, err
	}
	defer cells.release()

	raw, closer, err := cells.get(hash)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()

	s.lazyCellLoads.observePebble()
	loaded, err := storage.DecodeLazyCellRecordTrusted(hash, raw, loader)
	if err != nil {
		return nil, fmt.Errorf("create lazy cell %x: %w", hash, err)
	}
	s.cellCache.set(generation, hash, loaded)
	return loaded, nil
}

func (s *Store) loadActiveLazyCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	loaded, err := s.cellCache.get(activeCellCacheNamespace, hash)
	if err == nil {
		s.lazyCellLoads.observeDecodedCache()
		return loaded, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	return s.loadActiveLazyCellMiss(ctx, hash)
}

func (s *Store) loadActiveLazyCellMiss(ctx context.Context, hash []byte) (*cell.Cell, error) {
	cells, err := s.acquireActiveCellStore(ctx)
	if err != nil {
		return nil, err
	}
	defer cells.release()

	raw, closer, err := cells.get(hash)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()

	s.lazyCellLoads.observePebble()
	loaded, err := storage.DecodeLazyCellRecordTrusted(hash, raw, s.activeCellLoader)
	if err != nil {
		return nil, fmt.Errorf("create lazy cell %x: %w", hash, err)
	}
	s.cellCache.set(activeCellCacheNamespace, hash, loaded)
	return loaded, nil
}
