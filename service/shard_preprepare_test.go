package service

import (
	"encoding/binary"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func testPreparedShardBlockID(seqno uint32) ton.BlockIDExt {
	rootHash := make([]byte, 32)
	binary.BigEndian.PutUint32(rootHash, seqno)
	fileHash := make([]byte, 32)
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}
}

func TestPreparedShardBlockCacheTakeAndDedup(t *testing.T) {
	var cache preparedShardBlockCache
	block := testPreparedShardBlockID(1)

	if !cache.beginPrepare(block) {
		t.Fatal("first beginPrepare rejected")
	}
	if cache.beginPrepare(block) {
		t.Fatal("beginPrepare accepted while inflight")
	}

	cache.storePrepared(PreparedBlock{ID: block, BlockBOC: []byte{1}})
	if cache.beginPrepare(block) {
		t.Fatal("beginPrepare accepted while cached")
	}

	got, ok := cache.take(block)
	if !ok {
		t.Fatal("prepared block not taken")
	}
	if !got.ID.Equals(&block) {
		t.Fatalf("taken block %s, want seqno %d", got.BlockRef(), block.SeqNo)
	}
	if _, ok = cache.take(block); ok {
		t.Fatal("prepared block taken twice")
	}

	if !cache.beginPrepare(block) {
		t.Fatal("beginPrepare rejected after take")
	}
	cache.abortPrepare(block)
	if !cache.beginPrepare(block) {
		t.Fatal("beginPrepare rejected after abort")
	}
}

func TestPreparedShardBlockCacheEvictsOldest(t *testing.T) {
	var cache preparedShardBlockCache

	for i := uint32(0); i < preparedShardBlockMaxItems+1; i++ {
		cache.storePrepared(PreparedBlock{ID: testPreparedShardBlockID(i), BlockBOC: []byte{byte(i)}})
	}

	if _, ok := cache.take(testPreparedShardBlockID(0)); ok {
		t.Fatal("oldest entry survived eviction")
	}
	if _, ok := cache.take(testPreparedShardBlockID(preparedShardBlockMaxItems)); !ok {
		t.Fatal("newest entry evicted")
	}
}
