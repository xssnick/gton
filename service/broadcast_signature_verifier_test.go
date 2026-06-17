package service

import (
	"testing"

	"github.com/xssnick/gton/service/blockproof"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestBroadcastValidatorCacheResetsByConfigRoot(t *testing.T) {
	var cache broadcastValidatorCache

	firstKey := broadcastValidatorCacheKey{
		configRootHash:   testBroadcastConfigHash(1),
		workchain:        0,
		shard:            topShard,
		catchainSeqno:    7,
		validatorSetHash: 11,
	}
	firstSet := &blockproof.PreparedValidatorSet{}
	cache.put(firstKey, firstSet)

	if set, ok := cache.get(firstKey); !ok || set != firstSet {
		t.Fatal("expected broadcast validator cache hit before config root reset")
	}

	secondKey := firstKey
	secondKey.configRootHash = testBroadcastConfigHash(2)
	secondKey.catchainSeqno = 8
	secondKey.validatorSetHash = 22
	secondSet := &blockproof.PreparedValidatorSet{}
	cache.put(secondKey, secondSet)

	if _, ok := cache.get(firstKey); ok {
		t.Fatal("expected broadcast validator cache miss after config root changed")
	}
	if set, ok := cache.get(secondKey); !ok || set != secondSet {
		t.Fatal("expected broadcast validator cache hit for the new config root")
	}
}

func TestBroadcastValidatorCacheKeepsShardVariants(t *testing.T) {
	var cache broadcastValidatorCache

	root := testBroadcastConfigHash(1)
	baseKey := broadcastValidatorCacheKey{
		configRootHash:   root,
		workchain:        0,
		shard:            topShard,
		catchainSeqno:    7,
		validatorSetHash: 11,
	}
	shardKey := baseKey
	shardKey.shard = 1 << 62
	shardKey.validatorSetHash = 22

	baseSet := &blockproof.PreparedValidatorSet{}
	shardSet := &blockproof.PreparedValidatorSet{}
	cache.put(baseKey, baseSet)
	cache.put(shardKey, shardSet)

	if set, ok := cache.get(baseKey); !ok || set != baseSet {
		t.Fatal("expected first shard validator set to stay cached")
	}
	if set, ok := cache.get(shardKey); !ok || set != shardSet {
		t.Fatal("expected second shard validator set to stay cached")
	}
}

func TestBroadcastValidatorCacheKeepsConfigUntilReset(t *testing.T) {
	var cache broadcastValidatorCache

	firstBlock := testBlockID(-1, topShard, 10)
	firstConfig := broadcastValidatorConfig{rootHash: testBroadcastConfigHash(1)}
	if _, ok := cache.getConfig(firstBlock); ok {
		t.Fatal("empty broadcast validator config cache returned a hit")
	}
	cache.putConfig(firstBlock, firstConfig)

	got, ok := cache.getConfig(firstBlock)
	if !ok || got.rootHash != firstConfig.rootHash {
		t.Fatal("expected broadcast validator config cache hit for the same master block")
	}

	secondBlock := testBlockID(-1, topShard, 11)
	got, ok = cache.getConfig(secondBlock)
	if !ok || got.rootHash != firstConfig.rootHash {
		t.Fatal("expected broadcast validator config cache hit for a later non-key master block")
	}

	key := broadcastValidatorCacheKey{
		configRootHash:   firstConfig.rootHash,
		workchain:        0,
		shard:            topShard,
		catchainSeqno:    7,
		validatorSetHash: 11,
	}
	cache.put(key, &blockproof.PreparedValidatorSet{})

	cache.reset()
	if _, ok = cache.getConfig(secondBlock); ok {
		t.Fatal("expected broadcast validator config cache miss after reset")
	}
	if _, ok = cache.get(key); ok {
		t.Fatal("expected broadcast validator entries to be cleared after reset")
	}
}

func testBroadcastConfigHash(value uint64) cell.Hash {
	return cell.BeginCell().MustStoreUInt(value, 8).EndCell().HashKey()
}
