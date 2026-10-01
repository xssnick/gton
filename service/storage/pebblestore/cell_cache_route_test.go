package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// There is ONE decoded cell cache and one loader for the active generation.
// Everything below pins one leg of that: it is sized by its own knob and by
// nothing else, the loader threads itself all the way down so a whole tree lands
// in that one cache rather than only its root, a retired generation is swept out
// of it, and turning it off changes speed rather than answers.

func decodedCellCacheHolds(t *testing.T, cache *decodedCellCache, hash cell.Hash) bool {
	t.Helper()

	_, err := cache.getHash(activeCellCacheNamespace, hash)
	if err == nil {
		return true
	}
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("decoded cell cache lookup: %v", err)
	}
	return false
}

// stateChainStore imports a linear chain of cells deep enough that "lands in the
// cache" and "lands in the cache at depth" are different claims, then reopens
// the store so the cache starts empty and the only way to reach a cell is a real
// read.
func stateChainStore(t *testing.T, depth int, opts Options) (*Store, ton.BlockIDExt, []cell.Hash) {
	t.Helper()

	dir := t.TempDir()
	opts.Dir = dir

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     7,
		RootHash:  bytes.Repeat([]byte{0x31}, 32),
		FileHash:  bytes.Repeat([]byte{0x32}, 32),
	}

	// Leaf upwards, so every level is a distinct cell with a distinct hash.
	current := cell.BeginCell().MustStoreUInt(uint64(depth), 32).EndCell()
	for level := depth - 1; level >= 0; level-- {
		current = cell.BeginCell().MustStoreUInt(uint64(level), 32).MustStoreRef(current).EndCell()
	}
	root := current

	hashes := make([]cell.Hash, 0, depth+1)
	for walk := root; walk != nil; {
		hashes = append(hashes, walk.HashKey())
		if walk.RefsNum() == 0 {
			break
		}
		walk = walk.MustPeekRef(0)
	}
	if len(hashes) != depth+1 {
		t.Fatalf("chain has %d levels, want %d", len(hashes), depth+1)
	}

	ctx := context.Background()
	store, err := Open(opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err = store.ImportStateCellTree(ctx, block, root, uint64(depth+1)); err != nil {
		_ = store.Close()
		t.Fatalf("import state cell tree: %v", err)
	}
	rootHash := root.HashKey(0)
	if err = saveTestBlockState(ctx, store, &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
	}); err != nil {
		_ = store.Close()
		t.Fatalf("save block state metadata: %v", err)
	}
	// Importing warms the cache — it returns the lazy root — so reopen rather
	// than assert against a warm cache.
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if store.decodedCells.len() != 0 {
		t.Fatalf("reopened store has a warm cache: %d entries", store.decodedCells.len())
	}
	return store, block, hashes
}

func TestDecodedCellCacheIsSizedByConfig(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if store.decodedCells == nil {
		t.Fatal("the decoded cell cache should exist by default")
	}
	if got := store.decodedCells.capacity(); got != DefaultDecodedCellCacheEntries {
		t.Fatalf("decoded cell cache capacity = %d, want %d", got, DefaultDecodedCellCacheEntries)
	}
	if store.activeCellLoader == nil {
		t.Fatal("the active cell loader should be wired at open")
	}
}

// The entry count is the knob. Raising the pebble block cache must not move it:
// that coupling is what made a big machine silently multiply live Go objects,
// and the README now promises the two are independent.
func TestDecodedCellCacheCapacityIgnoresPebbleCacheSize(t *testing.T) {
	small, err := Open(Options{Dir: t.TempDir(), CellCacheSize: 1 << 20})
	if err != nil {
		t.Fatalf("open store with small pebble cache: %v", err)
	}
	defer func() { _ = small.Close() }()

	large, err := Open(Options{Dir: t.TempDir(), CellCacheSize: 64 << 30})
	if err != nil {
		t.Fatalf("open store with large pebble cache: %v", err)
	}
	defer func() { _ = large.Close() }()

	if small.decodedCells.capacity() != large.decodedCells.capacity() {
		t.Fatalf("capacity moved with the pebble cache: %d vs %d",
			small.decodedCells.capacity(), large.decodedCells.capacity())
	}
}

func TestDecodedCellCacheExplicitEntriesAreHonoured(t *testing.T) {
	store, err := Open(Options{
		Dir:                     t.TempDir(),
		DecodedCellCacheShards:  4,
		DecodedCellCacheEntries: 256,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if got := store.decodedCells.capacity(); got != 256 {
		t.Fatalf("cache capacity = %d, want 256", got)
	}
	if got := store.decodedCells.shardCount(); got != 4 {
		t.Fatalf("cache shards = %d, want 4", got)
	}
}

// The shard count is clamped down to the entry count, and the EFFECTIVE value is
// what open.go logs. This pins the clamp so the log line cannot drift back to
// reporting the request under an "effective" label.
func TestDecodedCellCacheShardsAreClampedToEntries(t *testing.T) {
	store, err := Open(Options{
		Dir:                     t.TempDir(),
		DecodedCellCacheShards:  64,
		DecodedCellCacheEntries: 32,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if got := store.decodedCells.shardCount(); got != 32 {
		t.Fatalf("effective shards = %d, want 32 (clamped down to the entry count)", got)
	}
	if got := store.decodedCells.capacity(); got != 32 {
		t.Fatalf("effective capacity = %d, want 32", got)
	}
}

// The threading proof. DecodeLazyCellRecordTrusted passes the loader it is given
// to CreateWithLazyRefsUnsafe, which stores it in every child placeholder, so
// resolving a child must re-enter the same loader and file the child in the same
// cache. Reading the code says so; this asserts it at four levels, because a
// scheme that only cached the root would pass any check taken at the root.
//
// It is also the mechanism behind the fact that killed per-consumer routing: a
// tree's cache membership is decided once, at the first cold decode, and an
// already-decoded tree handed to a second consumer carries that decision with
// it.
func TestActiveLoaderKeepsChildPlaceholdersOnTheCache(t *testing.T) {
	const depth = 4
	store, block, hashes := stateChainStore(t, depth, Options{})
	ctx := context.Background()

	root, err := store.LoadStateCellTree(ctx, block, hashes[0][:])
	if err != nil {
		t.Fatalf("load state cell tree: %v", err)
	}

	current := root
	for level := 0; level <= depth; level++ {
		hash := hashes[level]
		if !decodedCellCacheHolds(t, store.decodedCells, hash) {
			t.Fatalf("level %d cell %x is not in the cache", level, hash[:4])
		}
		if got := current.HashKey(); got != hash {
			t.Fatalf("level %d resolved to %x, want %x", level, got[:4], hash[:4])
		}

		if level == depth {
			break
		}
		ref, refErr := current.PeekRef(0)
		if refErr != nil {
			t.Fatalf("level %d peek ref: %v", level, refErr)
		}
		// Prewarm is what actually crosses the lazy boundary and therefore what
		// calls the loader the placeholder is carrying.
		if current, err = ref.Prewarm(); err != nil {
			t.Fatalf("level %d resolve ref: %v", level, err)
		}
	}

	if got := store.decodedCells.len(); got != depth+1 {
		t.Fatalf("cache holds %d entries after a full walk, want %d", got, depth+1)
	}
}

// The tree that comes back is the imported one, at every level, and a mismatched
// root hash is rejected rather than answered.
func TestStateRouteReturnsTheImportedTree(t *testing.T) {
	const depth = 4
	store, block, hashes := stateChainStore(t, depth, Options{})
	ctx := context.Background()

	root, err := store.LoadStateCellTree(ctx, block, hashes[0][:])
	if err != nil {
		t.Fatalf("state load: %v", err)
	}

	walk := root
	for level := 0; level <= depth; level++ {
		if walk.HashKey() != hashes[level] {
			t.Fatalf("level %d is not the imported cell", level)
		}
		if level == depth {
			break
		}
		ref, refErr := walk.PeekRef(0)
		if refErr != nil {
			t.Fatalf("peek ref: %v", refErr)
		}
		if walk, err = ref.Prewarm(); err != nil {
			t.Fatalf("resolve: %v", err)
		}
	}

	wrong := bytes.Repeat([]byte{0xaa}, 32)
	if _, err = store.LoadStateCellTree(ctx, block, wrong); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load with wrong root = %v, want ErrNotFound", err)
	}
}

// A cache too small for the walk must evict rather than grow: the entry cap is
// the whole point, since every resident entry is scanned by every GC mark.
func TestDecodedCellCacheRespectsItsEntryCap(t *testing.T) {
	const depth = 8
	store, block, hashes := stateChainStore(t, depth, Options{
		DecodedCellCacheShards:  1,
		DecodedCellCacheEntries: 2,
	})
	ctx := context.Background()

	if got := store.decodedCells.capacity(); got != 2 {
		t.Fatalf("capacity = %d, want 2", got)
	}

	root, err := store.LoadStateCellTree(ctx, block, hashes[0][:])
	if err != nil {
		t.Fatalf("state load: %v", err)
	}
	current := root
	for level := 0; level < depth; level++ {
		ref, refErr := current.PeekRef(0)
		if refErr != nil {
			t.Fatalf("level %d peek ref: %v", level, refErr)
		}
		if current, err = ref.Prewarm(); err != nil {
			t.Fatalf("level %d resolve ref: %v", level, err)
		}
	}

	if got := store.decodedCells.len(); got > 2 {
		t.Fatalf("cache holds %d entries over a capacity of 2", got)
	}
	if !decodedCellCacheHolds(t, store.decodedCells, hashes[depth]) {
		t.Fatal("cache did not retain the most recently read leaf")
	}
}

// Retiring a generation must drop it from the cache; skipping it would leave
// decoded cells of a closed celldb reachable from a live cache.
func TestForgetCachedCellGenerationSweepsTheCache(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	const generation = uint64(9)
	retired := cell.BeginCell().MustStoreUInt(0xfe, 8).EndCell()
	kept := cell.BeginCell().MustStoreUInt(0xef, 8).EndCell()
	retiredHash, keptHash := retired.HashKey(), kept.HashKey()

	store.decodedCells.set(generation, retiredHash[:], retired)
	store.decodedCells.set(generation+1, keptHash[:], kept)

	store.forgetCachedCellGeneration(generation)

	if _, err = store.decodedCells.getHash(generation, retiredHash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cache still holds the retired generation: %v", err)
	}
	if _, err = store.decodedCells.getHash(generation+1, keptHash); err != nil {
		t.Fatalf("cache lost an unrelated generation: %v", err)
	}
}

func TestDisablingDecodedCellCacheLeavesTheLoaderWired(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir(), DisableDecodedCellCache: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if store.decodedCells != nil {
		t.Fatal("the cache should be nil when the decoded cell cache is disabled")
	}
	if store.activeCellLoader == nil {
		t.Fatal("the loader must still work with the cache disabled")
	}
	if got := store.decodedCells.capacity(); got != 0 {
		t.Fatalf("nil cache reports capacity %d", got)
	}
	if got := store.decodedCells.shardCount(); got != 0 {
		t.Fatalf("nil cache reports %d shards", got)
	}
}

// With the cache off the state route must still return the whole tree: the cache
// is a lookup shortcut, never the source of truth.
func TestStateRouteWorksWithDecodedCellCacheDisabled(t *testing.T) {
	const depth = 3
	store, block, hashes := stateChainStore(t, depth, Options{DisableDecodedCellCache: true})
	ctx := context.Background()

	current, err := store.LoadStateCellTree(ctx, block, hashes[0][:])
	if err != nil {
		t.Fatalf("state load: %v", err)
	}
	for level := 0; level <= depth; level++ {
		if got := current.HashKey(); got != hashes[level] {
			t.Fatalf("level %d = %x, want %x", level, got[:4], hashes[level][:4])
		}
		if level == depth {
			break
		}
		ref, refErr := current.PeekRef(0)
		if refErr != nil {
			t.Fatalf("peek ref: %v", refErr)
		}
		if current, err = ref.Prewarm(); err != nil {
			t.Fatalf("resolve: %v", err)
		}
	}
}
