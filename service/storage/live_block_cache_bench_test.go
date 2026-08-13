package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func benchLiveBlockCacheBlock(i int) ton.BlockIDExt {
	root := bytes.Repeat([]byte{0xaa}, 32)
	file := bytes.Repeat([]byte{0xbb}, 32)
	binary.BigEndian.PutUint64(root, uint64(i))
	binary.BigEndian.PutUint64(file, uint64(i))
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     1 << 3,
		SeqNo:     uint32(i + 1),
		RootHash:  root,
		FileHash:  file,
	}
}

func benchFilledLiveBlockCache(b *testing.B, blocks int) (*LiveBlockCache, []ton.BlockIDExt) {
	b.Helper()

	cache := NewLiveBlockCache(blocks)
	ids := make([]ton.BlockIDExt, 0, blocks)
	data := bytes.Repeat([]byte{0x01}, 1024)
	for i := 0; i < blocks; i++ {
		block := benchLiveBlockCacheBlock(i)
		ids = append(ids, block)
		if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
			Block:     block,
			BlockData: data,
		}); err != nil {
			b.Fatalf("publish live block: %v", err)
		}
	}
	return cache, ids
}

func BenchmarkLiveBlockCacheReadParallel(b *testing.B) {
	cache, ids := benchFilledLiveBlockCache(b, 4096)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			block := ids[rnd.Intn(len(ids))]
			if _, err := cache.CachedBlockData(context.Background(), block); err != nil {
				b.Fatalf("cached block data: %v", err)
			}
		}
	})
}

func BenchmarkLiveBlockCacheMixedParallel(b *testing.B) {
	cache, ids := benchFilledLiveBlockCache(b, 4096)
	data := bytes.Repeat([]byte{0x02}, 1024)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rnd := rand.New(rand.NewSource(rand.Int63()))
		i := 0
		for pb.Next() {
			i++
			if i%1024 == 0 {
				block := benchLiveBlockCacheBlock(4096 + rnd.Intn(1<<20))
				if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
					Block:     block,
					BlockData: data,
				}); err != nil {
					b.Fatalf("publish live block: %v", err)
				}
				continue
			}

			block := ids[rnd.Intn(len(ids))]
			if _, err := cache.CachedBlockData(context.Background(), block); err != nil && i < 1024 {
				b.Fatalf("cached block data: %v", err)
			}
		}
	})
}

func BenchmarkLiveBlockCachePublishWithPinnedOverflow(b *testing.B) {
	const (
		maxBlocks    = 8192
		pinnedBlocks = 15000
	)

	cache, _ := benchFilledLiveBlockCache(b, pinnedBlocks)
	cache.max = maxBlocks
	data := []byte{0x01}
	next := pinnedBlocks

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		block := benchLiveBlockCacheBlock(next)
		next++
		if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
			Block:     block,
			BlockData: data,
		}); err != nil {
			b.Fatalf("publish live block: %v", err)
		}
		cache.MarkBlockFlushed(block)
	}
}

func BenchmarkSelectLiveBlockCacheSplitNext(b *testing.B) {
	prev := ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 10}
	left := ton.BlockIDExt{Workchain: 0, Shard: int64(0x4000000000000000), SeqNo: 11}
	right := ton.BlockIDExt{Workchain: 0, Shard: int64(-0x4000000000000000), SeqNo: 11}

	var selected ton.BlockIDExt
	var ok bool
	b.ReportAllocs()
	for b.Loop() {
		selected, ok = selectLiveBlockCacheSplitNext(prev, right, left)
	}
	if !ok || selected.Shard != left.Shard {
		b.Fatal("left child was not selected")
	}
}
