package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/blockproof"

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

func TestBroadcastValidatorConfigAcceptsKnownEpochsAndCacheHits(t *testing.T) {
	const catchainSeqno = uint32(7)

	previous := fastSyncConfigTestValidator{
		publicKey: fastSyncConfigTestPublicKey(0x31),
		adnlID:    fastSyncConfigTestPeerID(0x41),
	}
	current := fastSyncConfigTestValidator{
		publicKey: fastSyncConfigTestPublicKey(0x32),
		adnlID:    fastSyncConfigTestPeerID(0x42),
	}
	next := fastSyncConfigTestValidator{
		publicKey: fastSyncConfigTestPublicKey(0x33),
		adnlID:    fastSyncConfigTestPeerID(0x43),
	}
	cfg := fastSyncConfigTestBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:    broadcastValidatorTestCatchainConfig(),
		tlb.ConfigParamPrevValidators:    fastSyncConfigTestValidatorSetCell(t, previous),
		tlb.ConfigParamCurrentValidators: fastSyncConfigTestValidatorSetCell(t, current),
		tlb.ConfigParamNextValidators:    fastSyncConfigTestValidatorSetCell(t, next),
	})
	block := testBlockID(0, topShard, 10)

	previousValidators, err := blockproof.PrevValidatorsForBlock(cfg, &block, catchainSeqno)
	if err != nil {
		t.Fatalf("load previous validators: %v", err)
	}
	previousSet, err := blockproof.PrepareValidatorSet(catchainSeqno, previousValidators)
	if err != nil {
		t.Fatalf("prepare previous validators: %v", err)
	}
	currentValidators, err := blockproof.CurrentValidatorsForBlock(cfg, &block, catchainSeqno)
	if err != nil {
		t.Fatalf("load current validators: %v", err)
	}
	currentSet, err := blockproof.PrepareValidatorSet(catchainSeqno, currentValidators)
	if err != nil {
		t.Fatalf("prepare current validators: %v", err)
	}
	nextValidators, err := blockproof.NextValidatorsForBlock(cfg, &block, catchainSeqno)
	if err != nil {
		t.Fatalf("load next validators: %v", err)
	}
	nextSet, err := blockproof.PrepareValidatorSet(catchainSeqno, nextValidators)
	if err != nil {
		t.Fatalf("prepare next validators: %v", err)
	}

	for name, hash := range map[string]uint32{
		"previous": previousSet.Hash(),
		"current":  currentSet.Hash(),
		"next":     nextSet.Hash(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := broadcastValidatorSetFromConfig(
				cfg,
				block,
				catchainSeqno,
				hash,
			); err != nil {
				t.Fatalf("known-epochs policy rejected %s set: %v", name, err)
			}
		})
	}

	var coordinator SyncCoordinator
	config := broadcastValidatorConfig{rootHash: cfg.Root.HashKey(), cfg: cfg}
	coordinator.broadcastValidatorCache.putConfig(testBlockID(-1, topShard, 100), config)
	warmed, err := coordinator.broadcastValidatorSetForSignatures(
		context.Background(),
		block,
		catchainSeqno,
		previousSet.Hash(),
	)
	if err != nil || warmed.Hash() != previousSet.Hash() {
		t.Fatalf("warm previous validator set cache: set=%v err=%v", warmed, err)
	}
	cached, err := coordinator.broadcastValidatorSetForSignatures(
		context.Background(),
		block,
		catchainSeqno,
		previousSet.Hash(),
	)
	if err != nil || cached != warmed {
		t.Fatalf("cached previous validator set: set=%p want=%p err=%v", cached, warmed, err)
	}
}

func broadcastValidatorTestCatchainConfig() *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0xc1, 8).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(1, 32).
		EndCell()
}

func testBroadcastConfigHash(value uint64) cell.Hash {
	return cell.BeginCell().MustStoreUInt(value, 8).EndCell().HashKey()
}
