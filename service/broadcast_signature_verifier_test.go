package service

import (
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
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
	firstValidators := []*tlb.ValidatorAddr{nil}
	cache.put(firstKey, firstValidators)

	if validators, ok := cache.get(firstKey); !ok || len(validators) != len(firstValidators) {
		t.Fatal("expected broadcast validator cache hit before config root reset")
	}

	secondKey := firstKey
	secondKey.configRootHash = testBroadcastConfigHash(2)
	secondKey.catchainSeqno = 8
	secondKey.validatorSetHash = 22
	secondValidators := []*tlb.ValidatorAddr{nil, nil}
	cache.put(secondKey, secondValidators)

	if _, ok := cache.get(firstKey); ok {
		t.Fatal("expected broadcast validator cache miss after config root changed")
	}
	if validators, ok := cache.get(secondKey); !ok || len(validators) != len(secondValidators) {
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

	cache.put(baseKey, []*tlb.ValidatorAddr{nil})
	cache.put(shardKey, []*tlb.ValidatorAddr{nil, nil})

	if validators, ok := cache.get(baseKey); !ok || len(validators) != 1 {
		t.Fatal("expected first shard validator set to stay cached")
	}
	if validators, ok := cache.get(shardKey); !ok || len(validators) != 2 {
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
	cache.put(key, []*tlb.ValidatorAddr{nil})

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
