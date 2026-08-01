package service

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func newCompressedStateCacheTestService() *Service {
	return &Service{
		compressedStateCache: make(map[storage.BlockRootHash]compressedBlockStateEntry),
	}
}

func newMonitorSplitDepthTestService() *Service {
	return &Service{
		monitorSplitDepth: make(map[monitorSplitDepthKey]uint32),
	}
}

func newMasterStateCacheTestService(t testing.TB) *Service {
	t.Helper()

	store := openTestPebbleStorage(t)
	return &Service{
		log:                  zerolog.Nop(),
		node:                 newServiceTestNodeWithStorage(t, store),
		storage:              store,
		masterStateCache:     make(map[storage.BlockRootHash]*storage.BlockState),
		compressedStateCache: make(map[storage.BlockRootHash]compressedBlockStateEntry),
		monitorSplitDepth:    make(map[monitorSplitDepthKey]uint32),
	}
}

func TestMasterBlockShardTargetsUsesBlockExtra(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)
	prepared := PreparedBlock{
		ID:        downloaded.ID,
		BlockRoot: downloaded.Block,
		BlockBOC:  downloaded.BlockBOC,
		Meta:      downloaded.Meta,
	}

	shards, err := (&Service{}).masterBlockShardTargets(context.Background(), &storage.BlockState{Block: downloaded.ID}, &prepared, nil)
	if err != nil {
		t.Fatalf("masterBlockShardTargets returned error: %v", err)
	}
	if len(shards) == 0 {
		t.Fatal("expected shard targets from master block extra")
	}
}

func TestMasterBlockShardTargetsReusesPrecomputed(t *testing.T) {
	// With a prepared block present, precomputed targets must be returned verbatim
	// without re-parsing the block, so the apply pipeline pays a single parse.
	precomputed := []ton.BlockIDExt{testBlockID(0, topShard, 42)}
	prepared := PreparedBlock{ID: testBlockID(-1, topShard, 41)}

	shards, err := (&Service{}).masterBlockShardTargets(context.Background(), &storage.BlockState{Block: prepared.ID}, &prepared, precomputed)
	if err != nil {
		t.Fatalf("masterBlockShardTargets returned error: %v", err)
	}
	if len(shards) != 1 || !shards[0].Equals(&precomputed[0]) {
		t.Fatalf("masterBlockShardTargets did not return precomputed targets: %v", shards)
	}
}

func TestMasterBlockShardTargetsUsesLoadedStateBeforePreparedBlock(t *testing.T) {
	master := testBlockID(-1, topShard, 43)
	shard := testBlockID(0, topShard, 44)
	state := testShardTargetsMasterState(t, master, shard)
	prepared := PreparedBlock{ID: master}

	shards, err := (&Service{}).masterBlockShardTargets(context.Background(), state, &prepared, nil)
	if err != nil {
		t.Fatalf("masterBlockShardTargets returned error: %v", err)
	}
	if len(shards) != 1 || !shards[0].Equals(&shard) {
		t.Fatalf("masterBlockShardTargets returned %v, want %s", shards, storage.FormatBlockRef(shard))
	}
}

func TestMasterBlockShardTargetsFallsBackToState(t *testing.T) {
	master := testBlockID(-1, topShard, 10)
	shard := testBlockID(0, topShard, 11)
	state := testShardTargetsMasterState(t, master, shard)

	svc := &Service{storage: openTestPebbleStorage(t)}
	shards, err := svc.masterBlockShardTargets(context.Background(), state, nil, nil)
	if err != nil {
		t.Fatalf("masterBlockShardTargets returned error: %v", err)
	}
	if len(shards) != 1 {
		t.Fatalf("masterBlockShardTargets returned %d shards, want 1", len(shards))
	}
	if !shards[0].Equals(&shard) {
		t.Fatalf("masterBlockShardTargets shard = %s, want %s", storage.FormatBlockRef(shards[0]), storage.FormatBlockRef(shard))
	}
}

func TestCompressedBlockStateCacheRefreshPreservesFreshEntry(t *testing.T) {
	root := cell.BeginCell().EndCell()
	state := func(seqno uint32) *storage.BlockState {
		return &storage.BlockState{
			Block: testBlockID(0, topShard, seqno),
			Cell:  root,
		}
	}

	svc := newCompressedStateCacheTestService()
	refreshed := state(1)
	svc.RememberCompressedBlockState(refreshed)
	for seqno := uint32(2); seqno <= compressedBlockStateCacheLimit; seqno++ {
		svc.RememberCompressedBlockState(state(seqno))
	}
	svc.RememberCompressedBlockState(refreshed)
	svc.RememberCompressedBlockState(state(compressedBlockStateCacheLimit + 1))

	if _, err := svc.cachedCompressedBlockState(refreshed.Block); err != nil {
		t.Fatal("refreshed compressed state was evicted as the oldest entry")
	}
	if _, err := svc.cachedCompressedBlockState(state(2).Block); err == nil {
		t.Fatal("oldest non-refreshed compressed state was retained")
	}
	if got := len(svc.compressedStateCache); got != compressedBlockStateCacheLimit {
		t.Fatalf("compressed state cache size = %d, want %d", got, compressedBlockStateCacheLimit)
	}
}

func TestCompressedBlockStateCacheCompactsRefreshHistory(t *testing.T) {
	state := &storage.BlockState{
		Block: testBlockID(0, topShard, 1),
		Cell:  cell.BeginCell().EndCell(),
	}
	svc := newCompressedStateCacheTestService()
	for i := 0; i < compressedBlockStateCacheLimit*3; i++ {
		svc.RememberCompressedBlockState(state)
	}

	if got := len(svc.compressedStateCache); got != 1 {
		t.Fatalf("compressed state cache size = %d, want 1", got)
	}
	if got := len(svc.compressedStateOrder) - svc.compressedStateOrderHead; got > compressedBlockStateCacheLimit*2 {
		t.Fatalf("compressed state order size = %d, want at most %d", got, compressedBlockStateCacheLimit*2)
	}
}

func TestCompressedBlockStateCacheRejectsExpiredEntry(t *testing.T) {
	state := &storage.BlockState{
		Block: testBlockID(0, topShard, 2),
		Cell:  cell.BeginCell().EndCell(),
	}
	svc := newCompressedStateCacheTestService()
	svc.RememberCompressedBlockState(state)

	key := storage.BlockKey(state.Block)
	svc.compressedStateMu.Lock()
	entry := svc.compressedStateCache[key]
	entry.storedAt = time.Now().Add(-compressedBlockStateCacheTTL)
	svc.compressedStateCache[key] = entry
	svc.compressedStateMu.Unlock()

	if _, err := svc.cachedCompressedBlockState(state.Block); err == nil {
		t.Fatal("expired compressed state was returned")
	}
	svc.compressedStateMu.Lock()
	_, cached := svc.compressedStateCache[key]
	svc.compressedStateMu.Unlock()
	if cached {
		t.Fatal("expired compressed state was not invalidated")
	}
}

func TestRememberMasterStateCacheStaysBounded(t *testing.T) {
	root := cell.BeginCell().EndCell()
	svc := newMasterStateCacheTestService(t)
	for seqno := uint32(1); seqno <= masterStateCacheLimit+100; seqno++ {
		svc.rememberMasterState(context.Background(), &storage.BlockState{
			Block: testBlockID(-1, topShard, seqno),
			Cell:  root,
		}, nil, nil)
	}

	if got := len(svc.masterStateCache); got != masterStateCacheLimit {
		t.Fatalf("master state cache size = %d, want %d", got, masterStateCacheLimit)
	}
	if got := len(svc.masterStateCacheKeys) - svc.masterStateCacheHead; got != masterStateCacheLimit {
		t.Fatalf("master state cache order size = %d, want %d", got, masterStateCacheLimit)
	}
	if _, ok := svc.masterStateCache[storage.BlockKey(testBlockID(-1, topShard, 100))]; ok {
		t.Fatal("old master state remained cached")
	}
	if _, ok := svc.masterStateCache[storage.BlockKey(testBlockID(-1, topShard, masterStateCacheLimit+100))]; !ok {
		t.Fatal("newest master state was not cached")
	}
}

func TestCachedMonitorMinSplitDepthHitNeedsNoStateParse(t *testing.T) {
	keyBlock := testMasterBlockID(1)
	master := testMonitorMasterStateWithKeyBlock(t, keyBlock, 7, keyBlock, true)

	svc := newMonitorSplitDepthTestService()
	depth, err := svc.cachedMonitorMinSplitDepth(master, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth initial read: %v", err)
	}
	if depth != 7 {
		t.Fatalf("cachedMonitorMinSplitDepth initial read = %d, want 7", depth)
	}

	// The same master without a parsed state can only be answered from the
	// cache: any compute attempt would fail on the missing mc_state_extra.
	probe := &storage.BlockState{Block: master.Block}
	got, err := svc.cachedMonitorMinSplitDepth(probe, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth cached read: %v", err)
	}
	if got != depth {
		t.Fatalf("cachedMonitorMinSplitDepth cached read = %d, want %d", got, depth)
	}

	svc.resetMonitorSplitDepthCache()
	if _, err = svc.cachedMonitorMinSplitDepth(probe, 0); err == nil {
		t.Fatal("cachedMonitorMinSplitDepth after cache reset did not recompute")
	}
}

func TestUpdateMasterDependentCachesKeepsMonitorSplitDepthUntilKeyBlock(t *testing.T) {
	svc := newMasterStateCacheTestService(t)
	keyBlock := testMasterBlockID(1)
	first := testMonitorMasterStateWithKeyBlock(t, keyBlock, 7, keyBlock, true)
	if _, err := svc.cachedMonitorMinSplitDepth(first, 0); err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth initial read: %v", err)
	}

	nonKey := testMasterBlockID(2)
	nonKeyState := testMonitorMasterStateWithKeyBlock(t, nonKey, 9, keyBlock, false)
	nonKeyPrepared := PreparedBlock{
		ID:   nonKey,
		Meta: &storage.BlockMeta{ID: nonKey},
	}
	if err := svc.updateMasterDependentCachesForKeyBlock(nonKeyState, &nonKeyPrepared); err != nil {
		t.Fatalf("updateMasterDependentCachesForKeyBlock non-key: %v", err)
	}
	svc.rememberMasterState(context.Background(), nonKeyState, &nonKeyPrepared, nil)

	// A non-key master must not reset the cache: the first master still hits
	// without a state parse.
	probe := &storage.BlockState{Block: first.Block}
	got, err := svc.cachedMonitorMinSplitDepth(probe, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth after non-key master: %v", err)
	}
	if got != 7 {
		t.Fatalf("cachedMonitorMinSplitDepth after non-key master = %d, want 7", got)
	}

	key := testMasterBlockID(3)
	meta := &storage.BlockMeta{ID: key}
	meta.Mark(storage.BlockMetaIsKeyBlock)
	keyState := testMonitorMasterStateWithKeyBlock(t, key, 9, key, true)
	keyPrepared := PreparedBlock{
		ID:   key,
		Meta: meta,
	}
	if err = svc.updateMasterDependentCachesForKeyBlock(keyState, &keyPrepared); err != nil {
		t.Fatalf("updateMasterDependentCachesForKeyBlock key: %v", err)
	}
	svc.rememberMasterState(context.Background(), keyState, &keyPrepared, nil)

	if _, err = svc.cachedMonitorMinSplitDepth(probe, 0); err == nil {
		t.Fatal("key block did not invalidate the monitor split depth cache")
	}
	got, err = svc.cachedMonitorMinSplitDepth(keyState, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth after key master: %v", err)
	}
	if got != 9 {
		t.Fatalf("cachedMonitorMinSplitDepth after key master = %d, want 9", got)
	}
}

func TestUpdateMasterDependentCachesPublishesKeyBlockValidatorConfigBeforeCurrentStatus(t *testing.T) {
	old := testBlockID(-1, topShard, 10)
	key := testBlockID(-1, topShard, 11)
	keyState := testMonitorMasterStateWithKeyBlock(t, key, 9, key, true)
	expected, err := broadcastValidatorConfigFromMasterchainState(keyState)
	if err != nil {
		t.Fatalf("broadcastValidatorConfigFromMasterchainState: %v", err)
	}

	oldConfig := broadcastValidatorConfig{rootHash: testBroadcastConfigHash(1)}
	if oldConfig.rootHash == expected.rootHash {
		t.Fatal("test requires different old and key-block config roots")
	}
	svc := &Service{
		node: &p2p.Node{},
		currentStatus: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: old},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
	}
	svc.broadcastValidatorCache.putConfig(old, oldConfig)

	meta := &storage.BlockMeta{ID: key}
	meta.Mark(storage.BlockMetaIsKeyBlock)
	if err = svc.updateMasterDependentCachesForKeyBlock(keyState, &PreparedBlock{
		ID:   key,
		Meta: meta,
	}); err != nil {
		t.Fatalf("updateMasterDependentCachesForKeyBlock: %v", err)
	}

	got, err := svc.currentBroadcastValidatorConfig(context.Background())
	if err != nil {
		t.Fatalf("currentBroadcastValidatorConfig: %v", err)
	}
	if got.rootHash != expected.rootHash {
		t.Fatal("broadcast validator config was not updated from the applied key-block state")
	}
	if !svc.currentStatus.Masterchain.Block.Equals(&old) {
		t.Fatal("test current status unexpectedly advanced to the key block")
	}
}

func TestUpdateMasterDependentCachesKeepsConfigWhenKeyBlockStateIsInvalid(t *testing.T) {
	old := testBlockID(-1, topShard, 20)
	oldConfig := broadcastValidatorConfig{rootHash: testBroadcastConfigHash(1)}
	svc := &Service{}
	svc.broadcastValidatorCache.putConfig(old, oldConfig)

	key := testBlockID(-1, topShard, 21)
	meta := &storage.BlockMeta{ID: key}
	meta.Mark(storage.BlockMetaIsKeyBlock)
	err := svc.updateMasterDependentCachesForKeyBlock(&storage.BlockState{
		Block: key,
		Cell:  cell.BeginCell().EndCell(),
	}, &PreparedBlock{ID: key, Meta: meta})
	if err == nil {
		t.Fatal("invalid key-block state did not return an error")
	}

	got, err := svc.broadcastValidatorCache.getConfig()
	if err != nil || got.rootHash != oldConfig.rootHash {
		t.Fatal("invalid key-block state replaced the previous validator config")
	}
}

// A shard after-apply can query with an inclusion master from before a key
// block after later masters were already cached; per-master entries must never
// alias across the epoch boundary.
func TestCachedMonitorMinSplitDepthSeparatesKeyBlockEpochs(t *testing.T) {
	oldKey := testMasterBlockID(1)
	oldMaster := testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(2), 7, oldKey, false)
	newKey := testMasterBlockID(3)
	newMaster := testMonitorMasterStateWithKeyBlock(t, newKey, 9, newKey, true)

	svc := newMonitorSplitDepthTestService()
	oldDepth, err := svc.cachedMonitorMinSplitDepth(oldMaster, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth old key: %v", err)
	}
	newDepth, err := svc.cachedMonitorMinSplitDepth(newMaster, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth new key: %v", err)
	}
	got, err := svc.cachedMonitorMinSplitDepth(&storage.BlockState{Block: oldMaster.Block}, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth old master after new master: %v", err)
	}
	if oldDepth != 7 || newDepth != 9 || got != 7 {
		t.Fatalf("split depths old=%d new=%d old-again=%d, want 7/9/7", oldDepth, newDepth, got)
	}
}

func testMonitorMasterStateWithKeyBlock(t *testing.T, block ton.BlockIDExt, splitDepth uint32, keyBlock ton.BlockIDExt, afterKeyBlock bool) *storage.BlockState {
	t.Helper()

	workchains := cell.NewDict(32)
	descr := cell.BeginCell().
		MustStoreUInt(0xa6, 8).
		MustStoreUInt(123, 32).
		MustStoreUInt(uint64(splitDepth), 8).
		EndCell()
	if err := workchains.SetIntKey(big.NewInt(0), descr); err != nil {
		t.Fatalf("store workchain descriptor: %v", err)
	}

	workchainsCell, err := tlb.ToCell(&tlb.WorkchainsConfig{Workchains: workchains})
	if err != nil {
		t.Fatalf("build workchains config: %v", err)
	}

	configParams := cell.NewDict(32)
	param := cell.BeginCell().MustStoreRef(workchainsCell).EndCell()
	if err = configParams.SetIntKey(big.NewInt(int64(tlb.ConfigParamWorkchains)), param); err != nil {
		t.Fatalf("store workchains config param: %v", err)
	}

	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(nil).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(configParams.AsCell()).
		MustStoreRef(testMonitorMasterInfo(t, keyBlock, afterKeyBlock)).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()

	return &storage.BlockState{
		Block: block,
		Cell:  cell.BeginCell().EndCell(),
		Parsed: &tlb.ShardStateUnsplit{
			McStateExtra: extra,
		},
	}
}

func testMonitorMasterStateWithoutWorkchainConfig(t *testing.T, block ton.BlockIDExt, keyBlock ton.BlockIDExt, afterKeyBlock bool) *storage.BlockState {
	t.Helper()

	configParams := cell.NewDict(32)
	if err := configParams.SetIntKey(big.NewInt(0), cell.BeginCell().EndCell()); err != nil {
		t.Fatalf("store dummy config param: %v", err)
	}

	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(nil).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(configParams.AsCell()).
		MustStoreRef(testMonitorMasterInfo(t, keyBlock, afterKeyBlock)).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()

	return &storage.BlockState{
		Block: block,
		Cell:  cell.BeginCell().EndCell(),
		Parsed: &tlb.ShardStateUnsplit{
			McStateExtra: extra,
		},
	}
}

func testMonitorMasterInfo(t *testing.T, keyBlock ton.BlockIDExt, afterKeyBlock bool) *cell.Cell {
	t.Helper()

	prevBlocks, err := cell.NewAugDict(32, testMonitorOldMcBlocksAugmentation{})
	if err != nil {
		t.Fatalf("create old mc blocks dict: %v", err)
	}
	prevBlocksCell, err := prevBlocks.ToCell()
	if err != nil {
		t.Fatalf("serialize old mc blocks dict: %v", err)
	}

	builder := cell.BeginCell().
		MustStoreUInt(0, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreBuilder(prevBlocksCell.ToBuilder()).
		MustStoreBoolBit(afterKeyBlock)

	if afterKeyBlock {
		return builder.MustStoreBoolBit(false).EndCell()
	}

	ref, err := tlb.ToCell(&tlb.ExtBlkRef{
		SeqNo:    keyBlock.SeqNo,
		RootHash: append([]byte(nil), keyBlock.RootHash...),
		FileHash: append([]byte(nil), keyBlock.FileHash...),
	})
	if err != nil {
		t.Fatalf("build key block ref: %v", err)
	}
	return builder.
		MustStoreBoolBit(true).
		MustStoreBuilder(ref.ToBuilder()).
		EndCell()
}

type testMonitorOldMcBlocksAugmentation struct{}

func (testMonitorOldMcBlocksAugmentation) SkipExtra(loader *cell.Slice) error {
	var extra tlb.KeyMaxLt
	return tlb.LoadFromCell(&extra, loader)
}

func (testMonitorOldMcBlocksAugmentation) EmptyExtra(dst *cell.Builder) error {
	return storeTestMonitorKeyMaxLT(dst, tlb.KeyMaxLt{})
}

func (testMonitorOldMcBlocksAugmentation) LeafExtra(value *cell.Slice, dst *cell.Builder) error {
	var ref tlb.KeyExtBlkRef
	if err := tlb.LoadFromCell(&ref, value.Copy()); err != nil {
		return err
	}
	return storeTestMonitorKeyMaxLT(dst, tlb.KeyMaxLt{IsKey: ref.IsKey, MaxEndLT: ref.BlkRef.EndLt})
}

func (testMonitorOldMcBlocksAugmentation) CombineExtra(leftExtra, rightExtra *cell.Slice, dst *cell.Builder) error {
	var left, right tlb.KeyMaxLt
	if err := tlb.LoadFromCell(&left, leftExtra.Copy()); err != nil {
		return err
	}
	if err := tlb.LoadFromCell(&right, rightExtra.Copy()); err != nil {
		return err
	}
	return storeTestMonitorKeyMaxLT(dst, tlb.KeyMaxLt{
		IsKey:    left.IsKey || right.IsKey,
		MaxEndLT: max(left.MaxEndLT, right.MaxEndLT),
	})
}

func storeTestMonitorKeyMaxLT(dst *cell.Builder, extra tlb.KeyMaxLt) error {
	if err := dst.StoreBoolBit(extra.IsKey); err != nil {
		return err
	}
	return dst.StoreUInt(extra.MaxEndLT, 64)
}

func testShardTargetsMasterState(t *testing.T, master ton.BlockIDExt, shards ...ton.BlockIDExt) *storage.BlockState {
	t.Helper()

	return &storage.BlockState{
		Block: master,
		Cell:  cell.BeginCell().EndCell(),
		Parsed: &tlb.ShardStateUnsplit{
			McStateExtra: testShardTargetsMcStateExtra(t, shards...),
		},
	}
}

func testShardTargetsMcStateExtra(t *testing.T, shards ...ton.BlockIDExt) *cell.Cell {
	t.Helper()

	shardHashes := cell.NewDict(32)
	byWorkchain := map[int32][]ton.BlockIDExt{}
	for _, shard := range shards {
		byWorkchain[shard.Workchain] = append(byWorkchain[shard.Workchain], shard)
	}
	for workchain, workchainShards := range byWorkchain {
		key := cell.BeginCell().MustStoreInt(int64(workchain), 32).EndCell()
		value := cell.BeginCell().MustStoreRef(testShardTargetsBinTree(t, workchainShards)).EndCell()
		if err := shardHashes.Set(key, value); err != nil {
			t.Fatalf("store shard hashes workchain %d: %v", workchain, err)
		}
	}

	configParams := cell.NewDict(32)
	if err := configParams.Set(cell.BeginCell().MustStoreUInt(0, 32).EndCell(), cell.BeginCell().EndCell()); err != nil {
		t.Fatalf("store dummy config param: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(shardHashes).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(configParams.AsCell()).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()
}

func testShardTargetsBinTree(t *testing.T, shards []ton.BlockIDExt) *cell.Cell {
	t.Helper()

	if len(shards) == 0 {
		t.Fatal("empty shard bin tree")
	}
	if len(shards) == 1 {
		return cell.BeginCell().
			MustStoreUInt(0, 1).
			MustStoreBuilder(testShardTargetsDescCell(t, shards[0]).ToBuilder()).
			EndCell()
	}

	mid := len(shards) / 2
	return cell.BeginCell().
		MustStoreUInt(1, 1).
		MustStoreRef(testShardTargetsBinTree(t, shards[:mid])).
		MustStoreRef(testShardTargetsBinTree(t, shards[mid:])).
		EndCell()
}

func testShardTargetsDescCell(t *testing.T, shard ton.BlockIDExt) *cell.Cell {
	t.Helper()

	desc := tlb.ShardDesc{
		SeqNo:              shard.SeqNo,
		RootHash:           append([]byte(nil), shard.RootHash...),
		FileHash:           append([]byte(nil), shard.FileHash...),
		NextValidatorShard: shard.Shard,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	}
	c, err := tlb.ToCell(desc)
	if err != nil {
		t.Fatalf("build shard desc for %s: %v", storage.FormatBlockRef(shard), err)
	}
	return c
}

func TestMasterBlockShardTargetsReusesPrecomputedWithoutPreparedBlock(t *testing.T) {
	// The deferred overlay reconcile carries the targets but not the prepared
	// block, and it must never fall through to a storage read.
	precomputed := []ton.BlockIDExt{testBlockID(0, topShard, 42)}
	master := testBlockID(-1, topShard, 41)

	shards, err := (&Service{}).masterBlockShardTargets(context.Background(), &storage.BlockState{Block: master}, nil, precomputed)
	if err != nil {
		t.Fatalf("masterBlockShardTargets returned error: %v", err)
	}
	if len(shards) != 1 || !shards[0].Equals(&precomputed[0]) {
		t.Fatalf("masterBlockShardTargets did not return precomputed targets: %v", shards)
	}
}

func newShardOverlayUpdateTestService(hook func(context.Context, masterShardOverlayUpdate)) *Service {
	return &Service{
		log:                   zerolog.Nop(),
		shutdownContext:       context.Background(),
		reconcileOverlaysHook: hook,
	}
}

func waitForShardOverlayWorkerIdle(t *testing.T, svc *Service) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		svc.shardOverlayMu.Lock()
		idle := !svc.shardOverlayRunning && svc.shardOverlayPending == nil
		svc.shardOverlayMu.Unlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("shard overlay worker did not become idle")
}

// The reconcile deactivates every shard overlay outside its own set, so two
// updates must never run concurrently and an older one must never land after a
// newer one. Coalescing intermediate states is correct: the desired set is a
// pure function of the newest master state.
func TestScheduleP2PShardOverlayUpdateCoalescesToLatestState(t *testing.T) {
	release := make(chan struct{})
	var seen []uint32
	var mu sync.Mutex
	entered := make(chan struct{}, 8)

	svc := newShardOverlayUpdateTestService(func(_ context.Context, update masterShardOverlayUpdate) {
		mu.Lock()
		seen = append(seen, update.state.Block.SeqNo)
		mu.Unlock()
		entered <- struct{}{}
		if update.state.Block.SeqNo == 10 {
			<-release
		}
	})

	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testMasterBlockID(10)}, nil)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first overlay update never started")
	}

	// Both land while the first reconcile is blocked; only the newest survives.
	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testMasterBlockID(11)}, nil)
	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testMasterBlockID(12)}, nil)
	close(release)

	waitForShardOverlayWorkerIdle(t, svc)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("overlay reconciles = %v, want the first and the coalesced latest", seen)
	}
	if seen[0] != 10 || seen[1] != 12 {
		t.Fatalf("overlay reconciles = %v, want [10 12]", seen)
	}
}

func TestScheduleP2PShardOverlayUpdateIgnoresOlderState(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 8)
	var seen []uint32
	var mu sync.Mutex

	svc := newShardOverlayUpdateTestService(func(_ context.Context, update masterShardOverlayUpdate) {
		mu.Lock()
		seen = append(seen, update.state.Block.SeqNo)
		mu.Unlock()
		entered <- struct{}{}
		if update.state.Block.SeqNo == 10 {
			<-release
		}
	})

	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testMasterBlockID(10)}, nil)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first overlay update never started")
	}

	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testMasterBlockID(12)}, nil)
	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testMasterBlockID(11)}, nil)
	close(release)

	waitForShardOverlayWorkerIdle(t, svc)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[1] != 12 {
		t.Fatalf("overlay reconciles = %v, want the stale seqno 11 to be dropped", seen)
	}
}

func TestScheduleP2PShardOverlayUpdateStopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reconciles := 0
	svc := newShardOverlayUpdateTestService(func(context.Context, masterShardOverlayUpdate) {
		reconciles++
	})
	svc.shutdownContext = ctx

	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testMasterBlockID(10)}, nil)
	waitForShardOverlayWorkerIdle(t, svc)
	svc.Wait()

	if reconciles != 0 {
		t.Fatalf("overlay reconciles after shutdown = %d, want 0", reconciles)
	}
}

func TestScheduleP2PShardOverlayUpdateSkipsNonMasterchainState(t *testing.T) {
	reconciles := 0
	svc := newShardOverlayUpdateTestService(func(context.Context, masterShardOverlayUpdate) {
		reconciles++
	})

	svc.scheduleP2PShardOverlayUpdate(nil, nil)
	svc.scheduleP2PShardOverlayUpdate(&storage.BlockState{Block: testBlockID(0, topShard, 10)}, nil)
	svc.Wait()

	if reconciles != 0 {
		t.Fatalf("overlay reconciles for non-masterchain states = %d, want 0", reconciles)
	}
}

// The masterchain apply loop publishes the applied state to the caches the next
// block needs before doing anything slower with it, so this half must be
// self-contained: both caches populated, no overlay reconcile.
func TestRememberAppliedMasterCompressedStatePopulatesBothCaches(t *testing.T) {
	svc := newMasterStateCacheTestService(t)
	master := testMasterBlockID(41)
	state := testShardTargetsMasterState(t, master, testBlockID(0, topShard, 42))

	svc.rememberAppliedMasterCompressedState(state)

	if _, err := svc.cachedCompressedBlockState(master); err != nil {
		t.Fatalf("compressed state cache after apply: %v", err)
	}
	svc.masterStateCacheMu.Lock()
	cached := svc.masterStateCache[storage.BlockKey(master)]
	svc.masterStateCacheMu.Unlock()
	if cached == nil || !cached.Block.Equals(&master) {
		t.Fatalf("master state cache after apply = %v, want the applied state", cached)
	}

	// The overlay half is deferred, so nothing here may have touched it.
	svc.monitorSplitDepthMu.Lock()
	depths := len(svc.monitorSplitDepth)
	svc.monitorSplitDepthMu.Unlock()
	if depths != 0 {
		t.Fatalf("monitor split depth entries = %d, want the overlay half to stay deferred", depths)
	}
}

func TestRememberAppliedMasterCompressedStateIgnoresNonMasterchainState(t *testing.T) {
	svc := newMasterStateCacheTestService(t)
	shard := testBlockID(0, topShard, 42)

	svc.rememberAppliedMasterCompressedState(&storage.BlockState{
		Block: shard,
		Cell:  cell.BeginCell().EndCell(),
	})

	svc.masterStateCacheMu.Lock()
	entries := len(svc.masterStateCache)
	svc.masterStateCacheMu.Unlock()
	if entries != 0 {
		t.Fatalf("master state cache entries for a shard state = %d, want 0", entries)
	}
}
