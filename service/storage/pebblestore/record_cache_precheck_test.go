package pebblestore

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestDecodedCellRecordCacheHitSkipsInFlightLoad pins that a record-cache hit
// never joins a cold load's flight: it has no I/O to share, so it must not wait
// for a leader stuck in the store, and publishing through the decoded cache
// still leaves the cell it returned as the one resident object.
func TestDecodedCellRecordCacheHitSkipsInFlightLoad(t *testing.T) {
	recordCache := newCellRecordCache(cellRecordCacheConfigFromBytes(16 << 20))
	defer recordCache.free()
	cache := newDecodedCellCache(decodedCellCacheConfig{enabled: true, shards: 1, entries: 8})
	store := &Store{decodedCells: cache, recordCache: recordCache}

	want := cell.BeginCell().MustStoreUInt(0x7ec0, 32).EndCell()
	hash := want.HashKey()
	record, err := storage.CellRecordFromCell(want)
	if err != nil {
		t.Fatalf("cell record: %v", err)
	}
	recordCache.put(hash[:], storage.EncodeCellRecord(record))

	// A leader parked in the store: its flight stays open until the test
	// resolves it.
	key := newDecodedCellCacheKey(activeCellCacheNamespace, hash[:])
	flight := &decodedCellLoadFlight{done: make(chan struct{})}
	shard := store.decodedCellLoads.shardOf(key)
	shard.flights = map[decodedCellCacheKey]*decodedCellLoadFlight{key: flight}

	type loadResult struct {
		cell *cell.Cell
		err  error
	}
	done := make(chan loadResult, 1)
	go func() {
		loaded, loadErr := store.loadActiveLazyCell(context.Background(), hash[:])
		done <- loadResult{cell: loaded, err: loadErr}
	}()

	var result loadResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		flight.loaded = want
		close(flight.done)
		<-done
		t.Fatal("record-cache hit waited for the in-flight store load")
	}
	close(flight.done)

	if result.err != nil {
		t.Fatalf("record-cache hit: %v", result.err)
	}
	if got := result.cell.HashKey(); got != hash {
		t.Fatalf("record-cache hit resolved %x, want %x", got[:4], hash[:4])
	}
	if resident, getErr := cache.getHash(activeCellCacheNamespace, hash); getErr != nil || resident != result.cell {
		t.Fatalf("decoded cache after record-cache hit = (%p, %v), want the returned cell %p", resident, getErr, result.cell)
	}
	if got := recordCacheLayerCount(t, store); got != 1 {
		t.Fatalf("record-cache hits counted = %d, want 1", got)
	}
}

// BenchmarkDecodedCellRecordCacheHit is a load that misses the decoded tier and
// is answered by the record tier: eight decoded entries against 4096 cycling
// keys keep every iteration on that path.
func BenchmarkDecodedCellRecordCacheHit(b *testing.B) {
	recordCache := newCellRecordCache(cellRecordCacheConfigFromBytes(64 << 20))
	defer recordCache.free()

	const keys = 4096
	hashes, records := benchEncodedRecords(b, keys)
	for i := range hashes {
		recordCache.put(hashes[i], records[i])
	}
	cache := newDecodedCellCache(decodedCellCacheConfig{enabled: true, shards: 1, entries: 8})
	store := &Store{decodedCells: cache, recordCache: recordCache}
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if _, err := store.loadActiveLazyCell(ctx, hashes[i&(keys-1)]); err != nil {
			b.Fatalf("record-cache hit: %v", err)
		}
	}
}
