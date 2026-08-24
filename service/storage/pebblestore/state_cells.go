package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"time"

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

// LoadStateCellTree is the ONE state entry point. The lightserver, proof
// building, the archive importer, sync, the operator tools, collation and
// validation all read state through here and share one decoded cell cache.
//
// There was briefly a second, "operation" entry point that answered the same
// question out of a second cache. It is gone, and reintroducing it needs more
// than a second cache: the collator and the validator must receive the SAME
// *cell.Cell for a given parent, because ChainState.validatedCandidateState
// compares tip states by POINTER and silently degrades every candidate to a full
// re-apply otherwise. Two caches cannot both supply one object. Separately, a
// resident tree's lazy tips carry the loader they were decoded with, so a second
// entry point would not reroute the tree the node actually collates on without
// rebuilding it — the exact work residency exists to avoid.
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

const cellRecordWarmScratchInitialCells = 1024

type cellRecordWarmScratch struct {
	stack []cell.Hash
	seen  map[cell.Hash]struct{}
	raw   []byte
	refs  [4]cell.Hash
}

var cellRecordWarmScratchPool = sync.Pool{
	New: func() any {
		return &cellRecordWarmScratch{
			stack: make([]cell.Hash, 0, cellRecordWarmScratchInitialCells),
			seen:  make(map[cell.Hash]struct{}, cellRecordWarmScratchInitialCells),
			raw:   make([]byte, 0, 512),
		}
	},
}

// WarmCellRecords recursively brings the encoded records reachable from root
// into the record, Pebble block and OS caches without decoding or inserting a
// cell into decodedCells. A decoded-cache hit still walks its reference hashes:
// the hit refreshes the entry's CLOCK bit, but a resident parent does not imply
// that its independently cached children are resident too.
func (s *Store) WarmCellRecords(ctx context.Context, root cell.Hash) error {
	cells, err := s.acquireActiveCellStore(ctx)
	if err != nil {
		return err
	}
	defer cells.release()

	scratch := cellRecordWarmScratchPool.Get().(*cellRecordWarmScratch)
	defer func() {
		scratch.stack = scratch.stack[:0]
		clear(scratch.seen)
		scratch.raw = scratch.raw[:0]
		cellRecordWarmScratchPool.Put(scratch)
	}()

	scratch.stack = append(scratch.stack, root)
	iterations := 0
	for len(scratch.stack) > 0 {
		if iterations&0xff == 0 {
			if err = ctx.Err(); err != nil {
				return err
			}
		}
		iterations++

		last := len(scratch.stack) - 1
		hash := scratch.stack[last]
		scratch.stack = scratch.stack[:last]
		if _, ok := scratch.seen[hash]; ok {
			continue
		}
		scratch.seen[hash] = struct{}{}

		loaded, cacheErr := s.decodedCells.getHash(activeCellCacheNamespace, hash)
		if cacheErr == nil {
			for i := int(loaded.RefsNum()) - 1; i >= 0; i-- {
				scratch.stack = append(scratch.stack, loaded.MustRefHashAt(i))
			}
			continue
		}
		if !errors.Is(cacheErr, storage.ErrNotFound) {
			return cacheErr
		}

		raw := s.recordCache.get(hash[:], scratch.raw)
		if raw == nil {
			storeRaw, closer, readErr := cells.get(hash[:])
			if readErr != nil {
				return fmt.Errorf("warm cell record %x: %w", hash, readErr)
			}

			refsCount, decodeErr := storage.DecodeCellRecordRefHashes(storeRaw, &scratch.refs)
			if decodeErr != nil {
				_ = closer.Close()
				return fmt.Errorf("decode warm cell record %x refs: %w", hash, decodeErr)
			}
			s.recordCache.put(hash[:], storeRaw)
			_ = closer.Close()
			for i := refsCount - 1; i >= 0; i-- {
				scratch.stack = append(scratch.stack, scratch.refs[i])
			}
			continue
		}

		scratch.raw = raw
		refsCount, err := storage.DecodeCellRecordRefHashes(raw, &scratch.refs)
		if err != nil {
			return fmt.Errorf("decode cached warm cell record %x refs: %w", hash, err)
		}
		for i := refsCount - 1; i >= 0; i-- {
			scratch.stack = append(scratch.stack, scratch.refs[i])
		}
	}
	return nil
}

// newLazyCellLoaderForGeneration builds a loader for a NON-ACTIVE generation.
// Its single caller is generation_cells.go, the retired- and
// migrating-generation read surface used by cleanup, migration and the operator
// tools. It reads through the same cache as everything else, under the
// generation's own namespace rather than activeCellCacheNamespace, so a retired
// generation's cells can never be answered as if they were the active one.
func (s *Store) newLazyCellLoaderForGeneration(generation uint64) cell.LazyCellLoader {
	var loader cell.LazyCellLoader
	loadMiss := func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := s.loadLazyCellMissFromGeneration(context.Background(), generation, hash[:], loader)
		if err != nil {
			return nil, fmt.Errorf("load lazy cell %x: %w", hash[:], err)
		}
		return loaded, nil
	}
	loader = s.cachedCellLoader(s.decodedCells, generation, loadMiss)
	return loader
}

const activeCellCacheNamespace uint64 = 0

// newActiveCellLoader builds the loader for the active generation. It reads
// through the decoded cell cache and threads ITSELF into every cell it decodes:
// DecodeLazyCellRecordTrusted hands the loader to CreateWithLazyRefsUnsafe,
// which stores it in the meta of every child placeholder it creates, so
// resolving a child re-enters here and its own children inherit it in turn.
//
// That self-threading is why a tree's cache membership is decided ONCE, at the
// first cold decode of its root, and never re-decided afterwards: an
// already-decoded tree handed to a new caller keeps the loader it was built
// with. Any future attempt to route consumers to different caches has to start
// from that fact rather than from the entry points.
func (s *Store) newActiveCellLoader() cell.LazyCellLoader {
	loadMiss := func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := s.loadActiveLazyCellThrough(context.Background(), hash[:])
		if err != nil {
			return nil, fmt.Errorf("load lazy cell %x: %w", hash[:], err)
		}
		return loaded, nil
	}

	return s.cachedCellLoader(s.decodedCells, activeCellCacheNamespace, loadMiss)
}

// forgetCachedCellGeneration drops a retired generation's decoded cells from the
// cache. Skipping it would leave cells of a closed celldb reachable from a live
// cache.
func (s *Store) forgetCachedCellGeneration(generation uint64) {
	s.decodedCells.deleteGeneration(generation)
}

func (s *Store) cachedCellLoader(
	cache *decodedCellCache,
	cacheNamespace uint64,
	loadMiss cell.LazyCellLoader,
) cell.LazyCellLoader {
	// Resolve the cache branch once rather than per lookup.
	if cache == nil {
		return loadMiss
	}

	return func(hash cell.Hash) (*cell.Cell, error) {
		return s.loadDecodedCell(context.Background(), cache, cacheNamespace, hash[:], func(context.Context) (*cell.Cell, error) {
			return loadMiss(hash)
		})
	}
}

type decodedCellLoadFlight struct {
	done            chan struct{}
	leaderCancelled bool
	loaded          *cell.Cell
	err             error
}

// decodedCellLoadGroup coalesces one cold (namespace, hash) load. The caller
// that creates a flight performs the load synchronously; followers wait on its
// result and may cancel independently. Keeping the leader synchronous preserves
// I/O backpressure and avoids a goroutine/context allocation for every unique
// cold cell in a collation.
//
// The in-flight table is sharded because it was the process's single largest
// point of contention. A mutex profile taken on the testnet validator under
// load — sampled as a delta over a 60 s window, so idle time is not counted —
// showed one lock accumulating 136 s of goroutine blocking in those 60 seconds,
// 38% of all contention in the process, on the path
// loadLazyPrunedRefWithTrace -> Dictionary.findKeySliceInto -> loadDecodedCell.
// More than two goroutines were waiting here at any instant, and they came from
// every subsystem at once rather than from one budgeted pool.
type decodedCellLoadGroup struct {
	shards [decodedCellLoadShards]decodedCellLoadShard
}

// decodedCellLoadShards is sized against the machine, not against a worker
// budget. The collator's proof estimator shards against collationParallelism
// because only collation lanes enter it; this group is entered by collation,
// by the validation of every other producer's candidate, by the account
// prewarmer's workers, by live view and by the persistent state serializer, so
// the concurrency it must spread is bounded by GOMAXPROCS. Sixty-four is the
// first power of two above the 48 the validator runs with, and the mask below
// needs a power of two.
const decodedCellLoadShards = 64

// decodedCellLoadShard is one independent slice of the in-flight table. The map
// is built on first use, so a shard that never takes a cold miss costs one
// mutex and one nil pointer.
type decodedCellLoadShard struct {
	mu      sync.Mutex
	flights map[decodedCellCacheKey]*decodedCellLoadFlight
	// Padded for the same reason proofEstimatorShard is: neighbouring shards
	// are locked by different cores at the same moment, and a mutex plus a map
	// header is 16 bytes, so four shards would otherwise share one cache line
	// and give back as false sharing part of what the split removes.
	_ [64]byte
}

// decodedCellLoadShardOf picks a shard from the LAST byte of the hash. The
// decoded-cell cache keys its own shards on the FIRST four bytes, and taking a
// different end keeps a cell's cache shard and its load shard uncorrelated, so
// a hot cache shard does not imply a hot load shard.
func (g *decodedCellLoadGroup) shardOf(key decodedCellCacheKey) *decodedCellLoadShard {
	return &g.shards[int(key.hash[len(key.hash)-1])&(decodedCellLoadShards-1)]
}

func (g *decodedCellLoadGroup) do(
	ctx context.Context,
	key decodedCellCacheKey,
	load func(context.Context) (*cell.Cell, error),
) (*cell.Cell, error) {
	shard := g.shardOf(key)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		shard.mu.Lock()
		flight := shard.flights[key]
		if flight != nil {
			shard.mu.Unlock()

			select {
			case <-flight.done:
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if flight.leaderCancelled {
					// Re-enter so one live follower becomes the next synchronous
					// leader and the rest coalesce behind it. This can repeat I/O only
					// on the rare cancellation edge and keeps every normal unique miss
					// free of shared-context and cancellation-watcher allocations.
					continue
				}

				return flight.loaded, flight.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if shard.flights == nil {
			shard.flights = make(map[decodedCellCacheKey]*decodedCellLoadFlight)
		}
		flight = &decodedCellLoadFlight{done: make(chan struct{})}
		shard.flights[key] = flight
		shard.mu.Unlock()

		loaded, err := load(ctx)
		shard.mu.Lock()
		flight.loaded = loaded
		flight.err = err
		ctxErr := ctx.Err()
		flight.leaderCancelled = ctxErr != nil && errors.Is(err, ctxErr)
		if shard.flights[key] == flight {
			delete(shard.flights, key)
		}
		close(flight.done)
		shard.mu.Unlock()

		return loaded, err
	}
}

// loadDecodedCell is the single cold-miss gate shared by direct state loads and
// lazy child resolution. The cache is checked both before and inside the
// flight: a winner may publish between those points, and consulting it again
// avoids starting I/O after the answer already became resident.
func (s *Store) loadDecodedCell(
	ctx context.Context,
	cache *decodedCellCache,
	cacheNamespace uint64,
	hash []byte,
	loadMiss func(context.Context) (*cell.Cell, error),
) (*cell.Cell, error) {
	if cache == nil {
		return loadMiss(ctx)
	}

	key := newDecodedCellCacheKey(cacheNamespace, hash)
	if loaded, err := cache.getKey(key); err == nil {
		s.lazyCellLoads.observeDecodedCache(key.hash[0])
		return loaded, nil
	}

	return s.decodedCellLoads.do(ctx, key, func(loadCtx context.Context) (*cell.Cell, error) {
		if loaded, cacheErr := cache.getKey(key); cacheErr == nil {
			s.lazyCellLoads.observeDecodedCache(key.hash[0])
			return loaded, nil
		}

		loaded, loadErr := loadMiss(loadCtx)
		if loadErr != nil {
			return nil, loadErr
		}
		if err := loadCtx.Err(); err != nil {
			return nil, err
		}

		return cache.set(cacheNamespace, key.hash[:], loaded), nil
	})
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

	return s.loadDecodedCell(ctx, s.decodedCells, generation, hash, func(loadCtx context.Context) (*cell.Cell, error) {
		return s.loadLazyCellMissFromGeneration(loadCtx, generation, hash, loader)
	})
}

// loadLazyCellMissFromGeneration is the non-active-generation twin of
// loadActiveLazyCellThrough below, and it reads through the SAME record cache
// deliberately: the record tier is keyed by hash alone because cells are
// content-addressed, so a record filed while a generation was active is
// byte-identical when migration or cleanup reads it under another generation —
// sharing the tier is what makes a generation swap start warm instead of cold.
func (s *Store) loadLazyCellMissFromGeneration(ctx context.Context, generation uint64, hash []byte, loader cell.LazyCellLoader) (*cell.Cell, error) {
	return s.loadLazyCellRecordThrough(ctx, hash, loader, func(loadCtx context.Context) (*cellStore, error) {
		return s.acquireCellStore(loadCtx, generation)
	})
}

// loadLazyCellRecordThrough is the body both cold paths share: the record tier
// first, then the celldb generation `acquire` opens, filling the record cache
// from the raw bytes on the way past. The two callers differ only in which
// generation they open and which loader the decoded cell's child placeholders
// resolve through, which is exactly what the two parameters carry.
//
// acquire is a callback rather than an opened store because a record-cache hit
// must not open one at all: on a warm store that is the overwhelming majority
// of calls, and acquiring costs a generation-lock round trip apiece.
func (s *Store) loadLazyCellRecordThrough(
	ctx context.Context,
	hash []byte,
	loader cell.LazyCellLoader,
	acquire func(context.Context) (*cellStore, error),
) (*cell.Cell, error) {
	if loaded, hit, err := s.decodeFromRecordCache(hash, loader); hit {
		if err != nil {
			return nil, err
		}
		return loaded, nil
	}

	cells, err := acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer cells.release()

	readStarted := time.Now()
	raw, closer, err := cells.get(hash)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()

	// Timed around the store read alone: the decode below costs the same at
	// every layer, so including it would blur the bands it is meant to separate.
	s.lazyCellLoads.observeStoreRead(hash[0], time.Since(readStarted))
	// The raw record bytes are in hand exactly here and nowhere cheaper: the
	// arena insert is a copy of them, made before the closer returns them to
	// pebble.
	s.recordCache.put(hash, raw)
	loaded, err := storage.DecodeLazyCellRecordTrusted(hash, raw, loader)
	if err != nil {
		return nil, fmt.Errorf("create lazy cell %x: %w", hash, err)
	}
	return loaded, nil
}

// recordCacheScratchPool holds the copy-out buffers for record-cache hits.
// Records measured on a mainnet-fixture store run 104.5 B mean / 266 B max, so
// one size class covers everything without per-hit allocation; decode copies
// what it keeps, so the buffer is free for reuse the moment it returns.
var recordCacheScratchPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 512)
		return &buf
	},
}

// decodeFromRecordCache answers one cell load from the encoded record tier.
// hit reports whether the record was there; on a hit the returned cell (or
// error) is the answer and the caller files it in the decoded cache under its
// own namespace.
func (s *Store) decodeFromRecordCache(hash []byte, loader cell.LazyCellLoader) (loaded *cell.Cell, hit bool, err error) {
	if s.recordCache == nil {
		return nil, false, nil
	}

	scratch := recordCacheScratchPool.Get().(*[]byte)
	raw := s.recordCache.get(hash, *scratch)
	if raw == nil {
		recordCacheScratchPool.Put(scratch)
		return nil, false, nil
	}
	*scratch = raw

	s.lazyCellLoads.observeRecordCache(hash[0])
	loaded, err = storage.DecodeLazyCellRecordTrusted(hash, raw, loader)
	recordCacheScratchPool.Put(scratch)
	if err != nil {
		return nil, true, fmt.Errorf("create lazy cell %x from record cache: %w", hash, err)
	}
	return loaded, true, nil
}

func (s *Store) loadActiveLazyCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return s.loadDecodedCell(ctx, s.decodedCells, activeCellCacheNamespace, hash, func(loadCtx context.Context) (*cell.Cell, error) {
		return s.loadActiveLazyCellThrough(loadCtx, hash)
	})
}

// loadActiveLazyCellThrough decodes one cell record from celldb for the active
// generation, with s.activeCellLoader as what the decoded cell's child
// placeholders will resolve through. Filing the result in the decoded cell
// cache is loadDecodedCell's job, not this function's.
//
// A note on sizing, since this is where the cache is filled. This used to carry
// a "cold vs resident" pair of distinct-cell counts per collation, presented as
// two measurements. They were not comparable: the cold figure came from the
// heavy bench arm (repeat=3, 747 transactions, 8,208 celldb decodes) and the
// resident figure was DERIVED from the light arm (repeat=1: 5,456 cold minus
// 2,542 rewritten = 2,914), joined by a "roughly half" that does not close
// between the two arms. Both are removed rather than re-stated, because the
// question they were introduced to settle — how much one consumer displaces
// another between two caches — no longer exists with one cache. If a distinct
// cell count per slot is needed again, measure it on ONE named arm and say
// which.
func (s *Store) loadActiveLazyCellThrough(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return s.loadLazyCellRecordThrough(ctx, hash, s.activeCellLoader, s.acquireActiveCellStore)
}
