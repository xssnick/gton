package service

import (
	"context"
	"math/big"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMasterBlockShardTargetsUsesBlockExtra(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)
	prepared := PreparedBlock{
		ID:        downloaded.ID,
		BlockRoot: downloaded.Block,
		BlockBOC:  downloaded.BlockBOC,
		Meta:      downloaded.Meta,
	}

	shards, err := (&Service{}).masterBlockShardTargets(context.Background(), &storage.BlockState{Block: downloaded.ID}, &prepared)
	if err != nil {
		t.Fatalf("masterBlockShardTargets returned error: %v", err)
	}
	if len(shards) == 0 {
		t.Fatal("expected shard targets from master block extra")
	}
}

func TestMasterBlockShardTargetsFallsBackToState(t *testing.T) {
	master := testBlockID(-1, topShard, 10)
	shard := testBlockID(0, topShard, 11)
	state := testShardTargetsMasterState(t, master, shard)

	shards, err := (&Service{}).masterBlockShardTargets(context.Background(), state, nil)
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

func TestCachedMonitorMinSplitDepthAvoidsConfigAfterFirstRead(t *testing.T) {
	keyBlock := testMasterBlockID(1)
	master := testMonitorMasterStateWithKeyBlock(t, keyBlock, 7, keyBlock, true)

	svc := &Service{}
	depth, err := svc.cachedMonitorMinSplitDepth(master, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth initial read: %v", err)
	}
	if depth != 7 {
		t.Fatalf("cachedMonitorMinSplitDepth initial read = %d, want 7", depth)
	}

	cached := testMonitorMasterStateWithoutWorkchainConfig(t, testMasterBlockID(2), keyBlock, false)
	got, err := svc.cachedMonitorMinSplitDepth(cached, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth cached read: %v", err)
	}
	if got != depth {
		t.Fatalf("cachedMonitorMinSplitDepth cached read = %d, want %d", got, depth)
	}

	svc.resetMonitorSplitDepthCache()
	if _, err = svc.cachedMonitorMinSplitDepth(cached, 0); err == nil {
		t.Fatal("expected cache reset to require mc_state_extra again")
	}
}

func TestRememberMasterStateKeepsMonitorSplitDepthUntilKeyBlock(t *testing.T) {
	svc := &Service{}
	keyBlock := testMasterBlockID(1)
	if _, err := svc.cachedMonitorMinSplitDepth(testMonitorMasterStateWithKeyBlock(t, keyBlock, 7, keyBlock, true), 0); err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth initial read: %v", err)
	}

	nonKey := testMasterBlockID(2)
	svc.rememberMasterState(context.Background(), testMonitorMasterStateWithKeyBlock(t, nonKey, 9, keyBlock, false), &PreparedBlock{
		ID:   nonKey,
		Meta: &storage.BlockMeta{ID: nonKey},
	})

	got, err := svc.cachedMonitorMinSplitDepth(testMonitorMasterStateWithoutWorkchainConfig(t, testMasterBlockID(4), keyBlock, false), 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth after non-key master: %v", err)
	}
	if got != 7 {
		t.Fatalf("cachedMonitorMinSplitDepth after non-key master = %d, want 7", got)
	}

	key := testMasterBlockID(3)
	meta := &storage.BlockMeta{ID: key}
	meta.Mark(storage.BlockMetaIsKeyBlock)
	svc.rememberMasterState(context.Background(), testMonitorMasterStateWithKeyBlock(t, key, 9, key, true), &PreparedBlock{
		ID:   key,
		Meta: meta,
	})

	got, err = svc.cachedMonitorMinSplitDepth(testMonitorMasterStateWithKeyBlock(t, key, 9, key, true), 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth after key master: %v", err)
	}
	if got != 9 {
		t.Fatalf("cachedMonitorMinSplitDepth after key master = %d, want 9", got)
	}
}

func TestCachedMonitorMinSplitDepthSeparatesKeyBlockEpochs(t *testing.T) {
	oldKey := testMasterBlockID(1)
	oldMaster := testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(2), 7, oldKey, false)
	newKey := testMasterBlockID(3)
	newMaster := testMonitorMasterStateWithKeyBlock(t, newKey, 9, newKey, true)

	svc := &Service{}
	oldDepth, err := svc.cachedMonitorMinSplitDepth(oldMaster, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth old key: %v", err)
	}
	newDepth, err := svc.cachedMonitorMinSplitDepth(newMaster, 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth new key: %v", err)
	}
	got, err := svc.cachedMonitorMinSplitDepth(testMonitorMasterStateWithoutWorkchainConfig(t, testMasterBlockID(4), oldKey, false), 0)
	if err != nil {
		t.Fatalf("cachedMonitorMinSplitDepth old key after new key: %v", err)
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

func (testMonitorOldMcBlocksAugmentation) EmptyExtra() (*cell.Cell, error) {
	return tlb.ToCell(&tlb.KeyMaxLt{})
}

func (testMonitorOldMcBlocksAugmentation) LeafExtra(value *cell.Slice) (*cell.Cell, error) {
	var ref tlb.KeyExtBlkRef
	if err := tlb.LoadFromCell(&ref, value.Copy()); err != nil {
		return nil, err
	}
	return tlb.ToCell(&tlb.KeyMaxLt{IsKey: ref.IsKey, MaxEndLT: ref.BlkRef.EndLt})
}

func (testMonitorOldMcBlocksAugmentation) CombineExtra(leftExtra, rightExtra *cell.Slice) (*cell.Cell, error) {
	var left, right tlb.KeyMaxLt
	if err := tlb.LoadFromCell(&left, leftExtra.Copy()); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(&right, rightExtra.Copy()); err != nil {
		return nil, err
	}
	return tlb.ToCell(&tlb.KeyMaxLt{
		IsKey:    left.IsKey || right.IsKey,
		MaxEndLT: max(left.MaxEndLT, right.MaxEndLT),
	})
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
