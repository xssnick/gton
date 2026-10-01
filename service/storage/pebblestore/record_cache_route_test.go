package pebblestore

import (
	"context"
	"strings"
	"testing"

	"github.com/xssnick/gton/internal/manualmem"
	"github.com/xssnick/gton/service/storage"
)

func recordCacheLayerCount(t *testing.T, store *Store) uint64 {
	t.Helper()

	for _, metric := range store.lazyCellLoads.snapshot() {
		if metric.Layer == storage.LazyCellLoadLayerRecordCache {
			return metric.Count
		}
	}
	t.Fatal("record_cache layer missing from the counter snapshot")
	return 0
}

// The wiring proof: a store-shaped read that misses the decoded cache is
// answered by the record tier — observed on its own layer counter — and
// returns the same cells the direct path would.
func TestRecordCacheServesReadsAfterDecodedCacheSweep(t *testing.T) {
	const depth = 4
	store, block, hashes := stateChainStore(t, depth, Options{CellRecordCacheBytes: 16 << 20})
	ctx := context.Background()
	if store.recordCache == nil {
		t.Fatal("record cache is nil despite a byte budget")
	}

	// Cold pass: every level falls through to pebble and gets filed in BOTH
	// caches on the way back.
	root, err := store.LoadStateCellTree(ctx, block, hashes[0][:])
	if err != nil {
		t.Fatalf("cold state load: %v", err)
	}
	current := root
	for level := 0; level < depth; level++ {
		ref, refErr := current.PeekRef(0)
		if refErr != nil {
			t.Fatalf("level %d peek ref: %v", level, refErr)
		}
		if current, err = ref.Prewarm(); err != nil {
			t.Fatalf("level %d resolve: %v", level, err)
		}
	}
	if got := int64(depth + 1); store.recordCache.stats().Entries < got {
		t.Fatalf("cold pass filed %d records, want at least %d", store.recordCache.stats().Entries, got)
	}
	if got := recordCacheLayerCount(t, store); got != 0 {
		t.Fatalf("cold pass counted %d record-cache hits, want 0", got)
	}

	// Sweep the decoded cache — the record tier is now the first thing a load
	// can hit — and read the whole chain again.
	store.decodedCells.deleteGeneration(activeCellCacheNamespace)
	for level := 0; level <= depth; level++ {
		loaded, loadErr := store.loadActiveLazyCell(ctx, hashes[level][:])
		if loadErr != nil {
			t.Fatalf("level %d warm load: %v", level, loadErr)
		}
		if got := loaded.HashKey(); got != hashes[level] {
			t.Fatalf("level %d resolved to %x, want %x", level, got[:4], hashes[level][:4])
		}
	}
	if got := recordCacheLayerCount(t, store); got != depth+1 {
		t.Fatalf("record cache answered %d of the %d swept reads", got, depth+1)
	}
}

// Zero budget means the tier does not exist — the read path must run exactly
// as before, not against an empty cache.
func TestRecordCacheDisabledByZeroBytes(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if store.recordCache != nil {
		t.Fatal("a zero byte budget should leave the record cache nil")
	}
}

func TestOpenRejectsNegativeRecordCacheBytes(t *testing.T) {
	_, err := Open(Options{Dir: t.TempDir(), CellRecordCacheBytes: -1})
	if err == nil || !strings.Contains(err.Error(), "record cache") {
		t.Fatalf("negative record cache budget opened anyway: %v", err)
	}
}

// Close must return the arenas to the manual allocator: under cgo they are C
// memory that no GC will ever reclaim, so the balance is the leak detector.
func TestStoreCloseFreesRecordCacheArenas(t *testing.T) {
	before := manualmem.Allocated()

	store, err := Open(Options{Dir: t.TempDir(), CellRecordCacheBytes: 16 << 20})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if held := manualmem.Allocated() - before; held <= 0 {
		t.Fatalf("open allocated %d manual bytes, want a positive arena+index budget", held)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if got := manualmem.Allocated() - before; got != 0 {
		t.Fatalf("close left %d manual bytes outstanding", got)
	}
}
