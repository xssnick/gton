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

func testArchiveImportCacheBlock(seqno uint32, root byte, file byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{root}, 32),
		FileHash:  bytes.Repeat([]byte{file}, 32),
	}
}
