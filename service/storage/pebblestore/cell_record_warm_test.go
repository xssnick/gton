package pebblestore

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func cellStoreReadCount(cells *cellStore) uint64 {
	var count uint64
	for i := range cells.io.readCells {
		count += cells.io.readCells[i].Load()
	}
	return count
}

func decodedCellEntry(cache *decodedCellCache, hash cell.Hash) *decodedCellCacheEntry {
	key := decodedCellCacheKey{
		generation: activeCellCacheNamespace,
		hash:       [32]byte(hash),
	}
	shard := &cache.shards[cache.shardIndex(key.hash)]
	base := shard.bucketBase(key.hash)
	for way := 0; way < shard.ways; way++ {
		entry := shard.slots[base+way].Load()
		if entry != nil && entry.key == key {
			return entry
		}
	}
	return nil
}

func TestWarmCellRecordsPopulatesOnlyEncodedCache(t *testing.T) {
	const depth = 6
	store, _, hashes := stateChainStore(t, depth, Options{CellRecordCacheBytes: 16 << 20})
	ctx := context.Background()

	readsBefore := cellStoreReadCount(store.cells)
	if err := store.WarmCellRecords(ctx, hashes[0]); err != nil {
		t.Fatalf("warm cell records: %v", err)
	}
	if got := store.decodedCells.len(); got != 0 {
		t.Fatalf("warm inserted %d decoded cells, want 0", got)
	}
	if got := cellStoreReadCount(store.cells) - readsBefore; got != uint64(len(hashes)) {
		t.Fatalf("warm read %d cell records, want %d unique records", got, len(hashes))
	}
	if got := store.recordCache.stats().Entries; got != int64(len(hashes)) {
		t.Fatalf("record cache holds %d entries, want %d", got, len(hashes))
	}
	for level, hash := range hashes {
		if raw := store.recordCache.get(hash[:], nil); raw == nil {
			t.Fatalf("level %d record %x is not cached", level, hash[:4])
		}
	}

	readsBefore = cellStoreReadCount(store.cells)
	if err := store.WarmCellRecords(ctx, hashes[0]); err != nil {
		t.Fatalf("rewarm cell records: %v", err)
	}
	if got := cellStoreReadCount(store.cells) - readsBefore; got != 0 {
		t.Fatalf("record-cache rewarm made %d Pebble reads, want 0", got)
	}
	if got := store.decodedCells.len(); got != 0 {
		t.Fatalf("rewarm inserted %d decoded cells, want 0", got)
	}
}

func TestWarmCellRecordsTouchesDecodedCellsAndWarmsColdDescendants(t *testing.T) {
	const depth = 4
	store, _, hashes := stateChainStore(t, depth, Options{CellRecordCacheBytes: 16 << 20})
	ctx := context.Background()

	root, err := store.LoadCell(ctx, hashes[0][:])
	if err != nil {
		t.Fatalf("load decoded root: %v", err)
	}
	if root.HashKey() != hashes[0] {
		t.Fatalf("loaded root = %x, want %x", root.HashKey(), hashes[0])
	}
	if _, err = store.LoadCell(ctx, hashes[2][:]); err != nil {
		t.Fatalf("load decoded descendant: %v", err)
	}
	if got := store.decodedCells.len(); got != 2 {
		t.Fatalf("decoded cache holds %d entries before warm, want 2", got)
	}

	rootEntry := decodedCellEntry(store.decodedCells, hashes[0])
	if rootEntry == nil {
		t.Fatal("decoded root cache entry is missing")
	}
	descendantEntry := decodedCellEntry(store.decodedCells, hashes[2])
	if descendantEntry == nil {
		t.Fatal("decoded descendant cache entry is missing")
	}
	rootEntry.ref.Store(false)
	descendantEntry.ref.Store(false)

	readsBefore := cellStoreReadCount(store.cells)
	if err = store.WarmCellRecords(ctx, hashes[0]); err != nil {
		t.Fatalf("warm from decoded root: %v", err)
	}
	if !rootEntry.ref.Load() {
		t.Fatal("decoded root CLOCK bit was not refreshed")
	}
	if !descendantEntry.ref.Load() {
		t.Fatal("decoded descendant CLOCK bit was not refreshed")
	}
	if got := store.decodedCells.len(); got != 2 {
		t.Fatalf("warm inserted descendants into decoded cache: entries=%d want=2", got)
	}
	if got := cellStoreReadCount(store.cells) - readsBefore; got != depth-1 {
		t.Fatalf("warm read %d cold descendants, want %d", got, depth-1)
	}
	for level, hash := range hashes[1:] {
		if hash == hashes[2] {
			continue
		}
		if raw := store.recordCache.get(hash[:], nil); raw == nil {
			t.Fatalf("cold descendant level %d record %x is not cached", level+1, hash[:4])
		}
	}
}

func TestWarmCellRecordsDoesNotCacheMalformedRecord(t *testing.T) {
	store, _, hashes := stateChainStore(t, 1, Options{CellRecordCacheBytes: 16 << 20})
	_, shard, err := store.cells.shardForHash(hashes[0][:])
	if err != nil {
		t.Fatal(err)
	}
	if err = shard.db.Set(hashes[0][:], []byte{1, 0}, pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	if err = store.WarmCellRecords(t.Context(), hashes[0]); err == nil {
		t.Fatal("malformed cell record was warmed")
	}
	if raw := store.recordCache.get(hashes[0][:], nil); raw != nil {
		t.Fatalf("malformed cell record was cached: %x", raw)
	}
}
