package service

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestArchiveImportCacheKeepsSuccessfulLoadUntilDropBefore(t *testing.T) {
	ctx := context.Background()
	cache := newArchiveImportCache()
	block := testArchiveImportCacheBlock(10, 0x11, 0x12)
	masterRef := testArchiveImportCacheBlock(100, 0x21, 0x22)
	blockKey := storage.BlockKey(block)
	key := archiveImportCacheKey{
		masterchainSeqno: block.SeqNo,
		shard:            archive.ShardID{Workchain: -1, Shard: topShard},
	}

	loads := 0
	//nolint:unparam // archiveImportCache.load requires an error result; this fixture is intentionally successful.
	load := func(context.Context) (*archiveImportResult, error) {
		loads++
		return &archiveImportResult{
			stats: &archive.ImportStats{},
			blocks: map[storage.BlockRootHash]PreparedBlock{
				blockKey: {
					ID:   block,
					Meta: &storage.BlockMeta{ID: block},
				},
			},
		}, nil
	}

	first, err := cache.load(ctx, key, load)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	if first.cached {
		t.Fatal("first load unexpectedly came from cache")
	}
	if loads != 1 {
		t.Fatalf("loads after first load = %d, want 1", loads)
	}

	firstBlock := first.imported.blocks[blockKey]
	firstBlock.Meta.MasterchainRefSeqno = masterRef.SeqNo

	second, err := cache.load(ctx, key, load)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if !second.cached {
		t.Fatal("second load did not come from cache")
	}
	if loads != 1 {
		t.Fatalf("loads after cached load = %d, want 1", loads)
	}
	secondBlock := second.imported.blocks[blockKey]
	if secondBlock.Meta == nil {
		t.Fatal("cached block meta is nil")
	}
	if secondBlock.Meta.MasterchainRefSeqno != 0 {
		t.Fatalf("cached block meta was mutated through first load: %d", secondBlock.Meta.MasterchainRefSeqno)
	}

	cache.dropBefore(block.SeqNo + 1)
	third, err := cache.load(ctx, key, load)
	if err != nil {
		t.Fatalf("third load: %v", err)
	}
	if third.cached {
		t.Fatal("third load unexpectedly came from cache after dropBefore")
	}
	if loads != 2 {
		t.Fatalf("loads after dropBefore = %d, want 2", loads)
	}
}

func TestArchiveImportCacheDropRemovesOnlyExactEntry(t *testing.T) {
	ctx := context.Background()
	cache := newArchiveImportCache()
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	firstKey := archiveImportCacheKey{masterchainSeqno: 10, shard: shard}
	secondKey := archiveImportCacheKey{masterchainSeqno: 10, shard: shard, splitDepth: 1}
	loads := map[archiveImportCacheKey]int{}

	load := func(key archiveImportCacheKey) func(context.Context) (*archiveImportResult, error) {
		return func(context.Context) (*archiveImportResult, error) {
			loads[key]++
			return &archiveImportResult{stats: &archive.ImportStats{}, blocks: map[storage.BlockRootHash]PreparedBlock{}}, nil
		}
	}

	if _, err := cache.load(ctx, firstKey, load(firstKey)); err != nil {
		t.Fatalf("load first entry: %v", err)
	}
	if _, err := cache.load(ctx, secondKey, load(secondKey)); err != nil {
		t.Fatalf("load second entry: %v", err)
	}

	cache.drop(firstKey)
	first, err := cache.load(ctx, firstKey, load(firstKey))
	if err != nil {
		t.Fatalf("reload dropped entry: %v", err)
	}
	second, err := cache.load(ctx, secondKey, load(secondKey))
	if err != nil {
		t.Fatalf("reload retained entry: %v", err)
	}
	if first.cached || !second.cached {
		t.Fatalf("cache state after exact drop = first:%v second:%v", first.cached, second.cached)
	}
	if loads[firstKey] != 2 || loads[secondKey] != 1 {
		t.Fatalf("loads after exact drop = first:%d second:%d", loads[firstKey], loads[secondKey])
	}
}

func TestDropArchiveWindowShardImportCachePreservesMasterSource(t *testing.T) {
	ctx := context.Background()
	cache := newArchiveImportCache()
	masterKey := archiveImportCacheKey{
		masterchainSeqno: 10,
		shard:            archive.ShardID{Workchain: -1, Shard: topShard},
	}
	shardKey := archiveImportCacheKey{
		masterchainSeqno: 10,
		shard:            archive.ShardID{Workchain: 0, Shard: topShard},
	}
	load := func(key archiveImportCacheKey) func(context.Context) (*archiveImportResult, error) {
		return func(context.Context) (*archiveImportResult, error) {
			return &archiveImportResult{
				stats:    &archive.ImportStats{},
				blocks:   map[storage.BlockRootHash]PreparedBlock{},
				cacheKey: key,
			}, nil
		}
	}
	master, err := cache.load(ctx, masterKey, load(masterKey))
	if err != nil {
		t.Fatalf("load master cache: %v", err)
	}
	shard, err := cache.load(ctx, shardKey, load(shardKey))
	if err != nil {
		t.Fatalf("load shard cache: %v", err)
	}

	runner := &archiveCatchUpRun{importCache: cache}
	runner.dropArchiveWindowShardImportCache(&shardClientArchiveWindow{
		archiveImports: []*archiveImportResult{master.imported, shard.imported},
	})

	masterReloads := 0
	if loaded, err := cache.load(ctx, masterKey, func(context.Context) (*archiveImportResult, error) {
		masterReloads++
		return &archiveImportResult{
			stats:  &archive.ImportStats{},
			blocks: map[storage.BlockRootHash]PreparedBlock{},
		}, nil
	}); err != nil || !loaded.cached {
		t.Fatalf("master cache after window drop = cached:%v err:%v", loaded.cached, err)
	}
	shardReloads := 0
	if loaded, err := cache.load(ctx, shardKey, func(context.Context) (*archiveImportResult, error) {
		shardReloads++
		return &archiveImportResult{
			stats:  &archive.ImportStats{},
			blocks: map[storage.BlockRootHash]PreparedBlock{},
		}, nil
	}); err != nil || loaded.cached {
		t.Fatalf("shard cache after window drop = cached:%v err:%v", loaded.cached, err)
	}
	if masterReloads != 0 || shardReloads != 1 {
		t.Fatalf("window cache reloads = master:%d shard:%d, want 0/1", masterReloads, shardReloads)
	}
}

func testArchiveImportCacheBlock(seqno uint32, root byte, file byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{root}, 32),
		FileHash:  bytes.Repeat([]byte{file}, 32),
	}
}

// TestArchiveImportCacheConcurrentMissIsolatesEveryCaller pins the ownership
// contract behind the single loader-side clone: the cache entry doubles as the
// waiters' source, so every waiter must still receive an independently mutable
// result and the shared entry must stay pristine.
func TestArchiveImportCacheConcurrentMissIsolatesEveryCaller(t *testing.T) {
	ctx := context.Background()
	cache := newArchiveImportCache()
	block := testArchiveImportCacheBlock(10, 0x31, 0x32)
	blockKey := storage.BlockKey(block)
	key := archiveImportCacheKey{
		masterchainSeqno: block.SeqNo,
		shard:            archive.ShardID{Workchain: -1, Shard: topShard},
	}

	const callers = 32
	var loads atomic.Int64
	release := make(chan struct{})
	//nolint:unparam // archiveImportCache.load requires an error result; this fixture is intentionally successful.
	load := func(context.Context) (*archiveImportResult, error) {
		loads.Add(1)
		// Hold the loader open so the other callers pile up as waiters.
		<-release
		return &archiveImportResult{
			stats: &archive.ImportStats{},
			blocks: map[storage.BlockRootHash]PreparedBlock{
				blockKey: {ID: block, Meta: &storage.BlockMeta{ID: block}},
			},
			splitDepth: 3,
		}, nil
	}

	var start, done sync.WaitGroup
	start.Add(callers)
	done.Add(callers)
	results := make([]archiveImportDownload, callers)
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer done.Done()
			start.Done()
			results[i], errs[i] = cache.load(ctx, key, load)
		}(i)
	}
	start.Wait()
	close(release)
	done.Wait()

	if got := loads.Load(); got != 1 {
		t.Fatalf("underlying loader ran %d times, want exactly 1", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
		if results[i].imported == nil {
			t.Fatalf("caller %d got a nil result", i)
		}
		if results[i].imported.splitDepth != 3 {
			t.Fatalf("caller %d split depth = %d, want 3", i, results[i].imported.splitDepth)
		}
	}

	// Every caller must own its result graph.
	seen := map[*storage.BlockMeta]int{}
	for i := range results {
		meta := results[i].imported.blocks[blockKey].Meta
		if meta == nil {
			t.Fatalf("caller %d block meta is nil", i)
		}
		if previous, ok := seen[meta]; ok {
			t.Fatalf("callers %d and %d share one *BlockMeta", previous, i)
		}
		seen[meta] = i
		meta.MasterchainRefSeqno = uint32(i + 1)
		results[i].imported.blocks = nil
	}
	for i := range results {
		if results[i].imported.blocks != nil {
			t.Fatalf("caller %d block map aliases another caller", i)
		}
	}

	// The shared cache entry survived every caller mutating its own copy.
	next, err := cache.load(ctx, key, func(context.Context) (*archiveImportResult, error) {
		t.Fatal("cache miss after a successful load")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("load after concurrent miss: %v", err)
	}
	if !next.cached {
		t.Fatal("load after concurrent miss did not hit the cache")
	}
	cachedBlock, ok := next.imported.blocks[blockKey]
	if !ok {
		t.Fatal("cached entry lost its blocks")
	}
	if cachedBlock.Meta.MasterchainRefSeqno != 0 {
		t.Fatalf("cached entry was mutated through a caller result: %d", cachedBlock.Meta.MasterchainRefSeqno)
	}
	if next.imported.splitDepth != 3 {
		t.Fatalf("cached split depth = %d, want 3", next.imported.splitDepth)
	}
}
