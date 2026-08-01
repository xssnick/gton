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

	if set, err := cache.get(firstKey); err != nil || set != firstSet {
		t.Fatal("expected broadcast validator cache hit before config root reset")
	}

	secondKey := firstKey
	secondKey.configRootHash = testBroadcastConfigHash(2)
	secondKey.catchainSeqno = 8
	secondKey.validatorSetHash = 22
	secondSet := &blockproof.PreparedValidatorSet{}
	cache.put(secondKey, secondSet)

	if _, err := cache.get(firstKey); err == nil {
		t.Fatal("expected broadcast validator cache miss after config root changed")
	}
	if set, err := cache.get(secondKey); err != nil || set != secondSet {
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

	if set, err := cache.get(baseKey); err != nil || set != baseSet {
		t.Fatal("expected first shard validator set to stay cached")
	}
	if set, err := cache.get(shardKey); err != nil || set != shardSet {
		t.Fatal("expected second shard validator set to stay cached")
	}
}

func TestBroadcastValidatorCacheRejectsStaleConfigPut(t *testing.T) {
	var cache broadcastValidatorCache

	oldBlock := testBlockID(-1, topShard, 10)
	oldConfig := broadcastValidatorConfig{rootHash: testBroadcastConfigHash(1)}
	if _, err := cache.getConfig(); err == nil {
		t.Fatal("empty broadcast validator config cache returned a hit")
	}
	cache.putConfig(oldBlock, oldConfig)

	oldKey := broadcastValidatorCacheKey{
		configRootHash:   oldConfig.rootHash,
		workchain:        0,
		shard:            topShard,
		catchainSeqno:    7,
		validatorSetHash: 11,
	}
	oldSet := &blockproof.PreparedValidatorSet{}
	cache.put(oldKey, oldSet)

	keyBlock := testBlockID(-1, topShard, 20)
	keyConfig := broadcastValidatorConfig{rootHash: testBroadcastConfigHash(2)}
	cache.putConfig(keyBlock, keyConfig)

	got := cache.putConfig(oldBlock, oldConfig)
	if got.rootHash != keyConfig.rootHash {
		t.Fatal("stale config put did not return the newer key-block config")
	}

	got, err := cache.getConfig()
	if err != nil || got.rootHash != keyConfig.rootHash {
		t.Fatal("stale config put replaced the newer key-block config")
	}
	if _, err = cache.get(oldKey); err == nil {
		t.Fatal("old validator entries remained cached after the config root changed")
	}

	cache.put(oldKey, oldSet)
	if _, err = cache.get(oldKey); err == nil {
		t.Fatal("late validator set put restored an entry from the old config root")
	}

	keyBlockKey := broadcastValidatorCacheKey{
		configRootHash:   keyConfig.rootHash,
		workchain:        0,
		shard:            topShard,
		catchainSeqno:    8,
		validatorSetHash: 22,
	}
	keyBlockSet := &blockproof.PreparedValidatorSet{}
	cache.put(keyBlockKey, keyBlockSet)
	if set, err := cache.get(keyBlockKey); err != nil || set != keyBlockSet {
		t.Fatal("new key-block validator set was not cached")
	}
}

func testBroadcastConfigHash(value uint64) cell.Hash {
	return cell.BeginCell().MustStoreUInt(value, 8).EndCell().HashKey()
}
