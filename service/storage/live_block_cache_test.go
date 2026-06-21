package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestLiveBlockCachePinsUnflushedArtifactsUntilFlush(t *testing.T) {
	cache := NewLiveBlockCache(1)
	first := testLiveBlockCacheBlockID(1)
	second := testLiveBlockCacheBlockID(2)
	firstData := []byte{0x11}
	firstProof := []byte{0x21}

	if err := cache.PublishLiveBlockArtifacts(LiveBlockArtifacts{
		Block:     first,
		BlockData: firstData,
		Proofs: []LiveBlockProofArtifact{{
			Kind: ServedProofBlock,
			Data: firstProof,
		}},
	}); err != nil {
		t.Fatalf("publish first block: %v", err)
	}
	if err := cache.PublishLiveBlockArtifacts(LiveBlockArtifacts{
		Block:     second,
		BlockData: []byte{0x12},
	}); err != nil {
		t.Fatalf("publish second block: %v", err)
	}

	gotData, err := cache.BlockData(context.Background(), first)
	if err != nil {
		t.Fatalf("unflushed block data was evicted: %v", err)
	}
	if !bytes.Equal(gotData, firstData) {
		t.Fatalf("block data = %x, want %x", gotData, firstData)
	}

	gotProof, err := cache.BlockProof(context.Background(), ServedProofBlock, first)
	if err != nil {
		t.Fatalf("unflushed block proof was evicted: %v", err)
	}
	if !bytes.Equal(gotProof, firstProof) {
		t.Fatalf("block proof = %x, want %x", gotProof, firstProof)
	}

	cache.MarkBlockFlushed(first)
	if _, err = cache.BlockData(context.Background(), first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("flushed block data error = %v, want ErrNotFound", err)
	}
}

func TestLiveBlockCacheEvictsFlushedArtifacts(t *testing.T) {
	cache := NewLiveBlockCache(1)
	first := testLiveBlockCacheBlockID(1)
	second := testLiveBlockCacheBlockID(2)

	if err := cache.PublishLiveBlockArtifacts(LiveBlockArtifacts{
		Block:           first,
		BlockData:       []byte{0x11},
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish first block: %v", err)
	}
	if err := cache.PublishLiveBlockArtifacts(LiveBlockArtifacts{
		Block:           second,
		BlockData:       []byte{0x12},
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish second block: %v", err)
	}

	if _, err := cache.BlockData(context.Background(), first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old flushed block data error = %v, want ErrNotFound", err)
	}
	if _, err := cache.BlockData(context.Background(), second); err != nil {
		t.Fatalf("latest flushed block data: %v", err)
	}
}

func TestLiveBlockCacheCachedBlockDataReportsArtifactFlushState(t *testing.T) {
	cache := NewLiveBlockCache(1)
	block := testLiveBlockCacheBlockID(1)
	data := []byte{0x11}

	if err := cache.PublishLiveBlockArtifacts(LiveBlockArtifacts{
		Block:     block,
		BlockData: data,
	}); err != nil {
		t.Fatalf("publish unflushed block: %v", err)
	}
	cached, err := cache.CachedBlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("cached unflushed block data: %v", err)
	}
	if cached.ArtifactFlushed {
		t.Fatal("unflushed block data reported as flushed")
	}
	if !bytes.Equal(cached.Data, data) {
		t.Fatalf("cached block data = %x, want %x", cached.Data, data)
	}

	if err = cache.PublishLiveBlockArtifacts(LiveBlockArtifacts{
		Block:           block,
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish flushed marker: %v", err)
	}
	cached, err = cache.CachedBlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("cached flushed block data: %v", err)
	}
	if !cached.ArtifactFlushed {
		t.Fatal("flushed block data reported as unflushed")
	}
}

func testLiveBlockCacheBlockID(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 100)}, 32),
	}
}
