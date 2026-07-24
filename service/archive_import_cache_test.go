package service

import (
	"bytes"
	"context"
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

func TestDropArchiveWindowImportCacheRemovesEverySource(t *testing.T) {
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

	runner := &archiveCatchUpRunner{importCache: cache}
	runner.dropArchiveWindowImportCache(&shardClientArchiveWindow{
		archiveImports: []*archiveImportResult{master.imported, shard.imported},
	})

	masterReloads := 0
	if loaded, err := cache.load(ctx, masterKey, func(context.Context) (*archiveImportResult, error) {
		masterReloads++
		return &archiveImportResult{
			stats:  &archive.ImportStats{},
			blocks: map[storage.BlockRootHash]PreparedBlock{},
		}, nil
	}); err != nil || loaded.cached {
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
	if masterReloads != 1 || shardReloads != 1 {
		t.Fatalf("window cache reloads = master:%d shard:%d, want 1/1", masterReloads, shardReloads)
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
