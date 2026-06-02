package liteserver

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestLiveStoreNonfinalPublishesGaplessCellCache(t *testing.T) {
	store := &fakeLazyCellStore{cells: map[cell.Hash]*cell.Cell{}}
	live, base := testNonfinalLiveStoreWithCurrentStore(t, store)

	sharedLeaf := cell.BeginCell().MustStoreUInt(0xab, 8).EndCell()
	sharedBranch := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(sharedLeaf).EndCell()
	parentState := cell.BeginCell().MustStoreUInt(0x11, 8).MustStoreRef(sharedBranch).EndCell()
	parent := testNonfinalArtifact(t, 0, masterchainShard, 11, parentState, base.Block)

	prunedSharedBranch, err := cell.CreatePrunedBranch(sharedBranch, 1, 0)
	if err != nil {
		t.Fatalf("create pruned shared branch: %v", err)
	}
	childState := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(prunedSharedBranch).EndCell()
	child := testNonfinalArtifact(t, 0, masterchainShard, 12, childState, parent.Block)

	if err := live.PublishNonfinalBlockArtifacts(child, storage.LiveBlockNonfinalCandidate); err != nil {
		t.Fatalf("publish child first: %v", err)
	}
	if err := live.PublishNonfinalBlockArtifacts(child, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish child signed first: %v", err)
	}
	signed, candidates := live.NonfinalPendingShardBlocks(nil)
	if len(signed) != 0 || len(candidates) != 0 {
		t.Fatalf("out-of-order child became visible: signed=%d candidates=%d", len(signed), len(candidates))
	}
	if _, err := live.BlockState(context.Background(), child.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("out-of-order child state err = %v, want ErrNotFound", err)
	}

	if err := live.PublishNonfinalBlockArtifacts(parent, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish parent: %v", err)
	}
	signed, candidates = live.NonfinalPendingShardBlocks(nil)
	if len(signed) != 2 || !blockIDEqual(signed[0], parent.Block) || !blockIDEqual(signed[1], child.Block) {
		t.Fatalf("signed pending = %+v, want parent and child", signed)
	}
	if len(candidates) != 1 || !blockIDEqual(candidates[0], child.Block) {
		t.Fatalf("candidate pending = %+v, want child", candidates)
	}

	state, err := live.BlockState(context.Background(), child.Block)
	if err != nil {
		t.Fatalf("load child state: %v", err)
	}
	loadedSharedBranch, err := state.Cell.PeekRef(0)
	if err != nil {
		t.Fatalf("load child lazy ref from non-final cache: %v", err)
	}
	loadedSharedBranch, err = loadedSharedBranch.Prewarm()
	if err != nil {
		t.Fatalf("prewarm child lazy ref from non-final cache: %v", err)
	}
	loadedSharedLeaf, err := loadedSharedBranch.PeekRef(0)
	if err != nil {
		t.Fatalf("load child nested lazy ref from non-final cache: %v", err)
	}
	if !bytes.Equal(loadedSharedBranch.Hash(0), sharedBranch.Hash(0)) {
		t.Fatalf("loaded non-final dependency hash mismatch")
	}
	if !bytes.Equal(loadedSharedLeaf.Hash(0), sharedLeaf.Hash(0)) {
		t.Fatalf("loaded nested non-final dependency hash mismatch")
	}

	master := testNonfinalArtifact(t, masterchainID, masterchainShard, 101, cell.BeginCell().MustStoreUInt(0x10, 8).EndCell())
	if err := live.PublishLiveBlockArtifacts(master); err != nil {
		t.Fatalf("publish final master: %v", err)
	}
	if err := live.PublishLiveBlockArtifacts(parent); err != nil {
		t.Fatalf("publish final parent: %v", err)
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: *storage.CloneBlockState(master.State),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent.Block): *storage.CloneBlockState(parent.State),
		},
	})
	store.cells[sharedBranch.HashKey()] = sharedBranch

	signed, candidates = live.NonfinalPendingShardBlocks(nil)
	if len(signed) != 1 || !blockIDEqual(signed[0], child.Block) {
		t.Fatalf("signed pending after parent cleanup = %+v, want child", signed)
	}
	if len(candidates) != 1 || !blockIDEqual(candidates[0], child.Block) {
		t.Fatalf("candidate pending after parent cleanup = %+v, want child", candidates)
	}
	state, err = live.BlockState(context.Background(), child.Block)
	if err != nil {
		t.Fatalf("load child state after parent cleanup: %v", err)
	}
	loadedSharedBranch, err = state.Cell.PeekRef(0)
	if err != nil {
		t.Fatalf("load child lazy ref after parent cleanup: %v", err)
	}
	loadedSharedBranch, err = loadedSharedBranch.Prewarm()
	if err != nil {
		t.Fatalf("prewarm child lazy ref after parent cleanup: %v", err)
	}
	loadedSharedLeaf, err = loadedSharedBranch.PeekRef(0)
	if err != nil {
		t.Fatalf("load child nested lazy ref after parent cleanup: %v", err)
	}
	if !bytes.Equal(loadedSharedBranch.Hash(0), sharedBranch.Hash(0)) {
		t.Fatalf("loaded retained dependency hash mismatch")
	}
	if !bytes.Equal(loadedSharedLeaf.Hash(0), sharedLeaf.Hash(0)) {
		t.Fatalf("loaded retained nested dependency hash mismatch")
	}
}

func TestLiveStoreNonfinalPrunedDependencyUsesExactCellKey(t *testing.T) {
	store := &fakeLazyCellStore{cells: map[cell.Hash]*cell.Cell{}}
	live, base := testNonfinalLiveStoreWithCurrentStore(t, store)

	sharedLeaf := cell.BeginCell().MustStoreUInt(0xcd, 8).EndCell()
	sharedBranch := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(sharedLeaf).EndCell()
	prunedSharedBranch, err := cell.CreatePrunedBranch(sharedBranch, 1, 0)
	if err != nil {
		t.Fatalf("create pruned branch: %v", err)
	}
	depHash := sharedBranch.HashKey()
	store.cells[depHash] = sharedBranch

	stateRoot := cell.BeginCell().MustStoreUInt(0x23, 8).MustStoreRef(prunedSharedBranch).EndCell()
	pending := testNonfinalArtifact(t, 0, masterchainShard, 11, stateRoot, base.Block)
	if err = live.PublishNonfinalBlockArtifacts(pending, storage.LiveBlockNonfinalCandidate); err != nil {
		t.Fatalf("publish pending: %v", err)
	}

	_, candidates := live.NonfinalPendingShardBlocks(nil)
	if len(candidates) != 1 || !blockIDEqual(candidates[0], pending.Block) {
		t.Fatalf("candidate pending = %+v, want pending", candidates)
	}

	state, err := live.BlockState(context.Background(), pending.Block)
	if err != nil {
		t.Fatalf("load pending state: %v", err)
	}
	loadedSharedBranch, err := state.Cell.PeekRef(0)
	if err != nil {
		t.Fatalf("load exact pruned dependency: %v", err)
	}
	loadedSharedBranch, err = loadedSharedBranch.Prewarm()
	if err != nil {
		t.Fatalf("prewarm exact pruned dependency: %v", err)
	}
	if !bytes.Equal(loadedSharedBranch.Hash(0), sharedBranch.Hash(0)) {
		t.Fatalf("loaded exact dependency hash mismatch")
	}
	if len(store.loads) == 0 {
		t.Fatalf("cell loader was not used")
	}
	for _, loadedHash := range store.loads {
		if loadedHash != depHash {
			t.Fatalf("loader hash = %x, want exact dependency %x", loadedHash[:], depHash[:])
		}
	}
}

func TestLiveStoreNonfinalUsesConfiguredCellLoaderBeforeStore(t *testing.T) {
	store := &fakeLazyCellStore{cells: map[cell.Hash]*cell.Cell{}}
	live, base := testNonfinalLiveStoreWithCurrentStore(t, store)

	sharedLeaf := cell.BeginCell().MustStoreUInt(0xce, 8).EndCell()
	sharedBranch := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(sharedLeaf).EndCell()
	prunedSharedBranch, err := cell.CreatePrunedBranch(sharedBranch, 1, 0)
	if err != nil {
		t.Fatalf("create pruned branch: %v", err)
	}

	depHash := sharedBranch.HashKey()
	configuredLoads := 0
	live.SetNonfinalCellLoader(func(hash cell.Hash) (*cell.Cell, error) {
		configuredLoads++
		if hash == depHash {
			return sharedBranch, nil
		}
		return nil, cell.ErrLazyRefNotFound
	})

	stateRoot := cell.BeginCell().MustStoreUInt(0x24, 8).MustStoreRef(prunedSharedBranch).EndCell()
	pending := testNonfinalArtifact(t, 0, masterchainShard, 11, stateRoot, base.Block)
	if err = live.PublishNonfinalBlockArtifacts(pending, storage.LiveBlockNonfinalCandidate); err != nil {
		t.Fatalf("publish pending: %v", err)
	}

	state, err := live.BlockState(context.Background(), pending.Block)
	if err != nil {
		t.Fatalf("load pending state: %v", err)
	}
	loadedSharedBranch, err := state.Cell.PeekRef(0)
	if err != nil {
		t.Fatalf("load dependency from configured loader: %v", err)
	}
	loadedSharedBranch, err = loadedSharedBranch.Prewarm()
	if err != nil {
		t.Fatalf("prewarm dependency from configured loader: %v", err)
	}
	if !bytes.Equal(loadedSharedBranch.Hash(0), sharedBranch.Hash(0)) {
		t.Fatalf("loaded configured dependency hash mismatch")
	}
	if configuredLoads == 0 {
		t.Fatalf("configured cell loader was not used")
	}
	if len(store.loads) != 0 {
		t.Fatalf("store cell loader was used before configured loader")
	}
}

func TestLiveStoreFinalLazyStateSkipsNonfinalLoader(t *testing.T) {
	store := &fakeLazyCellStore{cells: map[cell.Hash]*cell.Cell{}}
	live := NewLiveStore(store, LiveStoreOptions{
		MasterBlockCache: 8,
		ShardBlockCache:  8,
		NonFinalEnabled:  true,
		NonFinalCache:    8,
	})

	leaf := cell.BeginCell().MustStoreUInt(0xef, 8).EndCell()
	branch := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(leaf).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x42, 8).MustStoreRef(branch).EndCell()

	finalLoads := 0
	rootRecord, err := storage.PrepareEncodedCellRecordFromCellMetadata(root, root.GetMetadata())
	if err != nil {
		t.Fatalf("prepare lazy final root record: %v", err)
	}
	lazyRoot, err := nonfinalLazyCellRecord(rootRecord.Hash, rootRecord.Data, func(hash cell.Hash) (*cell.Cell, error) {
		finalLoads++
		if hash != branch.HashKey() {
			return nil, cell.ErrLazyRefNotFound
		}
		return branch, nil
	})
	if err != nil {
		t.Fatalf("make lazy final root: %v", err)
	}

	final := testNonfinalArtifact(t, 0, masterchainShard, 20, root)
	final.State.Cell = lazyRoot
	if err = live.PublishLiveBlockArtifacts(final); err != nil {
		t.Fatalf("publish final: %v", err)
	}

	state, err := live.BlockState(context.Background(), final.Block)
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	loadedBranch, err := state.Cell.PeekRef(0)
	if err != nil {
		t.Fatalf("load final lazy ref: %v", err)
	}
	loadedBranch, err = loadedBranch.Prewarm()
	if err != nil {
		t.Fatalf("prewarm final lazy ref: %v", err)
	}
	if !bytes.Equal(loadedBranch.Hash(0), branch.Hash(0)) {
		t.Fatalf("loaded final ref hash mismatch")
	}
	if finalLoads == 0 {
		t.Fatalf("final lazy loader was not used")
	}
	if len(store.loads) != 0 {
		t.Fatalf("non-final loader was used on final path")
	}
}

func TestLiveStoreNonfinalUsesStateUpdateLazyOverlay(t *testing.T) {
	store := &fakeLazyCellStore{cells: map[cell.Hash]*cell.Cell{}}
	live := NewLiveStore(store, LiveStoreOptions{
		MasterBlockCache: 8,
		ShardBlockCache:  8,
		NonFinalEnabled:  true,
		NonFinalCache:    8,
	})

	master := testNonfinalArtifact(t, masterchainID, masterchainShard, 100, cell.BeginCell().MustStoreUInt(0x01, 8).EndCell())
	master.Meta.GenUTime = 1000
	if err := live.PublishLiveBlockArtifacts(master); err != nil {
		t.Fatalf("publish master: %v", err)
	}

	accountID := bytes.Repeat([]byte{0x12}, 32)
	baseStateBlock := ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 10}
	baseState, _ := testShardStateWithAccount(t, baseStateBlock, accountID)
	baseCells, err := storage.PrepareReachableStateCells(baseState)
	if err != nil {
		t.Fatalf("collect base state cells: %v", err)
	}
	testNonfinalStoreStateCellRecords(t, store, baseCells)
	baseBlock, baseRoot := testBlockForState(t, 0, masterchainShard, 10, baseState)
	baseStateHash := baseState.Hash(0)
	base := storage.LiveBlockArtifacts{
		Block:     baseBlock,
		Root:      baseRoot,
		BlockData: testBlockBOC(baseRoot),
		Meta: &storage.BlockMeta{
			ID:            baseBlock,
			StateRootHash: bytes.Clone(baseStateHash),
		},
		State: &storage.BlockState{
			Block:         baseBlock,
			StateRootHash: bytes.Clone(baseStateHash),
			Cell:          baseState,
		},
	}
	if err := live.PublishLiveBlockArtifacts(base); err != nil {
		t.Fatalf("publish base: %v", err)
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: *storage.CloneBlockState(master.State),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(base.Block): *storage.CloneBlockState(base.State),
		},
	})

	usageTree := cell.NewCellUsageTree()
	usageBaseState := baseState.WithTrace(usageTree.RootTrace())
	nextStateBlock := ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 11}
	nextState, wantAccount := testShardStateWithAccount(t, nextStateBlock, accountID)
	nextState = testNonfinalStateWithTracedRootRefs(t, nextState, usageBaseState)
	update, err := usageTree.CreateMerkleUpdate(usageBaseState, nextState)
	if err != nil {
		t.Fatalf("create merkle update: %v", err)
	}
	pendingBlock, pendingRoot := testNonfinalBlockForStateUpdate(t, 0, masterchainShard, 11, baseBlock, update)
	pending := storage.LiveBlockArtifacts{
		Block:     pendingBlock,
		Root:      pendingRoot,
		BlockData: testBlockBOC(pendingRoot),
	}

	if err := live.PublishNonfinalBlockArtifacts(pending, storage.LiveBlockNonfinalCandidate); err != nil {
		t.Fatalf("publish pending: %v", err)
	}

	fragments, err := live.BlockFragments(context.Background(), pendingBlock)
	if err != nil {
		t.Fatalf("load pending fragments: %v", err)
	}
	gotAccount, err := fragments.accountCell(accountID)
	if err != nil {
		t.Fatalf("load account from lazy non-final state: %v", err)
	}
	if !bytes.Equal(gotAccount.Hash(0), wantAccount.Hash(0)) {
		t.Fatalf("account hash mismatch")
	}

	fullCells, err := storage.PrepareReachableStateCells(nextState)
	if err != nil {
		t.Fatalf("count full state cells: %v", err)
	}
	live.mu.RLock()
	overlayCells := live.nonFinalPending[storage.BlockKey(pendingBlock)].cells.Len()
	live.mu.RUnlock()
	if overlayCells >= fullCells.Len() {
		t.Fatalf("overlay cells = %d, want less than full state cells %d", overlayCells, fullCells.Len())
	}
}

func TestLiveStoreNonfinalRejectsStateUpdateFromDifferentPreviousRoot(t *testing.T) {
	live, base := testNonfinalLiveStoreWithCurrent(t)

	wrongBaseState := cell.BeginCell().MustStoreUInt(0x90, 8).EndCell()
	nextState := cell.BeginCell().MustStoreUInt(0x91, 8).EndCell()
	usageTree := cell.NewCellUsageTree()
	usageWrongBase := wrongBaseState.WithTrace(usageTree.RootTrace())
	if _, err := usageWrongBase.BeginParse(); err != nil {
		t.Fatalf("trace wrong base state: %v", err)
	}
	update, err := usageTree.CreateMerkleUpdate(usageWrongBase, nextState)
	if err != nil {
		t.Fatalf("create merkle update: %v", err)
	}

	pendingBlock, pendingRoot := testNonfinalBlockForStateUpdate(t, 0, masterchainShard, 11, base.Block, update)
	err = live.PublishNonfinalBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     pendingBlock,
		Root:      pendingRoot,
		BlockData: testBlockBOC(pendingRoot),
	}, storage.LiveBlockNonfinalCandidate)
	if err == nil {
		t.Fatal("publish non-final block with wrong update source succeeded")
	}

	if _, err = live.BlockState(context.Background(), pendingBlock); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("wrong-source pending state err = %v, want ErrNotFound", err)
	}
	signed, candidates := live.NonfinalPendingShardBlocks(nil)
	if len(signed) != 0 || len(candidates) != 0 {
		t.Fatalf("wrong-source pending became visible: signed=%d candidates=%d", len(signed), len(candidates))
	}
}

func TestLiveStoreNonfinalDoesNotEnterLookupIndexes(t *testing.T) {
	live, base := testNonfinalLiveStoreWithCurrent(t)

	pending := testNonfinalArtifact(t, 0, masterchainShard, 11, cell.BeginCell().MustStoreUInt(0x92, 8).EndCell(), base.Block)
	pending.Meta.StartLT = 100
	pending.Meta.EndLT = 200
	pending.Meta.GenUTime = 1020
	if err := live.PublishNonfinalBlockArtifacts(pending, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish pending: %v", err)
	}
	if _, err := live.BlockState(context.Background(), pending.Block); err != nil {
		t.Fatalf("load exact pending state: %v", err)
	}

	key := storage.BlockHistoryKey{Workchain: pending.Block.Workchain, Shard: pending.Block.Shard}
	if _, err := live.LookupBlockBySeqNo(context.Background(), key, pending.Block.SeqNo); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("non-final seqno lookup err = %v, want ErrNotFound", err)
	}
	if _, err := live.LookupBlockByLT(context.Background(), key, 150); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("non-final lt lookup err = %v, want ErrNotFound", err)
	}
	if _, err := live.LookupBlockByUnixTime(context.Background(), key, 1020); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("non-final utime lookup err = %v, want ErrNotFound", err)
	}
}

func TestShardStateMasterRefReadsInlineStatsMasterRef(t *testing.T) {
	master := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     123,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}

	accounts, err := cell.NewAugDict(256, testShardAccountsAugmentation{})
	if err != nil {
		t.Fatalf("create shard accounts: %v", err)
	}
	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: 0,
			ShardPrefix: 0,
		},
		Seqno:           10,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           testShardStateStatsWithMasterRef(t, master),
	}
	state.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}

	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build shard state: %v", err)
	}

	got, err := shardStateMasterRef(root)
	if err != nil {
		t.Fatalf("load shard state master ref: %v", err)
	}
	if !blockIDEqual(got, master) {
		t.Fatalf("master ref = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(master))
	}
}

func TestLiveStoreNonfinalCleanupCoversSplitShards(t *testing.T) {
	live, base := testNonfinalLiveStoreWithCurrent(t)

	pendingState := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	pending := testNonfinalArtifact(t, 0, masterchainShard, 11, pendingState, base.Block)
	if err := live.PublishNonfinalBlockArtifacts(pending, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish pending: %v", err)
	}

	rootShard := tlb.ShardID(uint64(1) << 63)
	leftShard := int64(rootShard.GetChild(true))
	rightShard := int64(rootShard.GetChild(false))
	master := testNonfinalArtifact(t, masterchainID, masterchainShard, 101, cell.BeginCell().MustStoreUInt(0x44, 8).EndCell())
	left := testNonfinalArtifact(t, 0, leftShard, 11, cell.BeginCell().MustStoreUInt(0x55, 8).EndCell())
	right := testNonfinalArtifact(t, 0, rightShard, 11, cell.BeginCell().MustStoreUInt(0x66, 8).EndCell())

	for _, artifact := range []storage.LiveBlockArtifacts{master, left, right} {
		if err := live.PublishLiveBlockArtifacts(artifact); err != nil {
			t.Fatalf("publish current artifact %s: %v", storage.FormatBlockRef(artifact.Block), err)
		}
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: *storage.CloneBlockState(master.State),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(left.Block):  *storage.CloneBlockState(left.State),
			storage.ShardKeyFromBlock(right.Block): *storage.CloneBlockState(right.State),
		},
	})

	signed, candidates := live.NonfinalPendingShardBlocks(nil)
	if len(signed) != 0 || len(candidates) != 0 {
		t.Fatalf("covered non-final still pending: signed=%d candidates=%d", len(signed), len(candidates))
	}
	if _, err := live.BlockState(context.Background(), pending.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("covered pending state err = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreNonfinalDropsBlocksTooFarAheadOfCurrentMaster(t *testing.T) {
	live, base := testNonfinalLiveStoreWithCurrent(t)

	within := testNonfinalArtifact(t, 0, masterchainShard, 11, cell.BeginCell().MustStoreUInt(0x70, 8).EndCell(), base.Block)
	within.Meta.GenUTime = 1029
	if err := live.PublishNonfinalBlockArtifacts(within, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish within lead: %v", err)
	}

	tooFar := testNonfinalArtifact(t, 0, masterchainShard, 12, cell.BeginCell().MustStoreUInt(0x71, 8).EndCell(), within.Block)
	tooFar.Meta.GenUTime = 1030
	if err := live.PublishNonfinalBlockArtifacts(tooFar, storage.LiveBlockNonfinalCandidate); err != nil {
		t.Fatalf("publish too far: %v", err)
	}

	signed, candidates := live.NonfinalPendingShardBlocks(nil)
	if len(signed) != 1 || !blockIDEqual(signed[0], within.Block) {
		t.Fatalf("signed pending = %+v, want only within-lead block", signed)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0", len(candidates))
	}
	if _, err := live.BlockState(context.Background(), tooFar.Block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("too-far state err = %v, want ErrNotFound", err)
	}

	live.mu.RLock()
	waiting := len(live.nonFinalWaiting)
	live.mu.RUnlock()
	if waiting != 0 {
		t.Fatalf("waiting non-final blocks = %d, want 0", waiting)
	}
}

func TestHandleNonfinalPendingShardBlocksEnabled(t *testing.T) {
	live, base := testNonfinalLiveStoreWithCurrent(t)
	pendingState := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	pending := testNonfinalArtifact(t, 0, masterchainShard, 11, pendingState, base.Block)

	if err := live.PublishNonfinalBlockArtifacts(pending, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish pending: %v", err)
	}

	srv := testServer(live)
	srv.nonFinal = true
	resp := srv.handleQuery(context.Background(), ton.NonfinalGetPendingShardBlocks{Mode: 1, WC: 0, Shard: masterchainShard})
	blocks, ok := resp.(ton.NonfinalPendingShardBlocks)
	if !ok {
		t.Fatalf("response type = %T, want ton.NonfinalPendingShardBlocks", resp)
	}
	if len(blocks.SignedBlocks) != 1 || !blockIDEqual(*blocks.SignedBlocks[0], pending.Block) {
		t.Fatalf("signed blocks = %+v, want pending", blocks.SignedBlocks)
	}
	if len(blocks.Candidates) != 0 {
		t.Fatalf("candidates = %d, want 0", len(blocks.Candidates))
	}

	disallowed := []tl.Serializable{
		ton.NonfinalGetValidatorGroups{},
		ton.NonfinalGetCandidate{},
	}
	for _, query := range disallowed {
		errResp, ok := srv.handleQuery(context.Background(), query).(ton.LSError)
		if !ok {
			t.Fatalf("%T response type = %T, want ton.LSError", query, errResp)
		}
		if errResp.Text != "query is not allowed" {
			t.Fatalf("%T text = %q, want not allowed", query, errResp.Text)
		}
	}
}

func testNonfinalLiveStoreWithCurrent(t *testing.T) (*LiveStore, storage.LiveBlockArtifacts) {
	t.Helper()

	return testNonfinalLiveStoreWithCurrentStore(t, &fakeStore{})
}

func testNonfinalLiveStoreWithCurrentStore(t *testing.T, store Store) (*LiveStore, storage.LiveBlockArtifacts) {
	t.Helper()

	live := NewLiveStore(store, LiveStoreOptions{
		MasterBlockCache: 8,
		ShardBlockCache:  8,
		NonFinalEnabled:  true,
		NonFinalCache:    8,
	})

	master := testNonfinalArtifact(t, masterchainID, masterchainShard, 100, cell.BeginCell().MustStoreUInt(0x01, 8).EndCell())
	master.Meta.GenUTime = 1000
	base := testNonfinalArtifact(t, 0, masterchainShard, 10, cell.BeginCell().MustStoreUInt(0x02, 8).EndCell())
	for _, artifact := range []storage.LiveBlockArtifacts{master, base} {
		if err := live.PublishLiveBlockArtifacts(artifact); err != nil {
			t.Fatalf("publish current artifact %s: %v", storage.FormatBlockRef(artifact.Block), err)
		}
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: *storage.CloneBlockState(master.State),
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(base.Block): *storage.CloneBlockState(base.State),
		},
	})
	return live, base
}

type fakeLazyCellStore struct {
	fakeStore
	cells map[cell.Hash]*cell.Cell
	loads []cell.Hash
}

func (s *fakeLazyCellStore) LazyCellLoader() cell.LazyCellLoader {
	return func(hash cell.Hash) (*cell.Cell, error) {
		s.loads = append(s.loads, hash)
		root := s.cells[hash]
		if root == nil {
			return nil, cell.ErrLazyRefNotFound
		}
		return root, nil
	}
}

func testNonfinalStoreStateCellRecords(t *testing.T, store *fakeLazyCellStore, records storage.StateCellRecords) {
	t.Helper()

	if err := records.ForEach(func(record storage.EncodedCellRecord) error {
		loaded, err := nonfinalLazyCellRecord(record.Hash, record.Data, store.LazyCellLoader())
		if err != nil {
			return err
		}
		store.cells[record.Hash] = loaded
		return nil
	}); err != nil {
		t.Fatalf("store prepared state cell records: %v", err)
	}
}

func testShardStateStatsWithMasterRef(t *testing.T, master ton.BlockIDExt) *cell.Cell {
	t.Helper()

	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("build currency collection: %v", err)
	}
	ref, err := tlb.ToCell(&tlb.ExtBlkRef{
		EndLt:    1,
		SeqNo:    master.SeqNo,
		RootHash: bytes.Clone(master.RootHash),
		FileHash: bytes.Clone(master.FileHash),
	})
	if err != nil {
		t.Fatalf("build master ref: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 64).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreDict(cell.NewDict(256)).
		MustStoreBoolBit(true).
		MustStoreBuilder(ref.ToBuilder()).
		EndCell()
}

func testNonfinalArtifact(t *testing.T, workchain int32, shard int64, seqno uint32, stateRoot *cell.Cell, prevs ...ton.BlockIDExt) storage.LiveBlockArtifacts {
	t.Helper()

	blockRoot := cell.BeginCell().
		MustStoreUInt(uint64(seqno), 32).
		MustStoreUInt(uint64(uint32(workchain)), 32).
		MustStoreUInt(uint64(shard), 64).
		EndCell()
	block := testBlockIDForRoot(workchain, shard, seqno, blockRoot)
	stateRootHash := append([]byte(nil), stateRoot.Hash(0)...)
	prevRefs := make([]ton.BlockIDExt, len(prevs))
	copy(prevRefs, prevs)

	return storage.LiveBlockArtifacts{
		Block:     block,
		Root:      blockRoot,
		BlockData: testBlockBOC(blockRoot),
		Meta: &storage.BlockMeta{
			ID:            block,
			StateRootHash: stateRootHash,
			PrevRefs:      prevRefs,
		},
		State: &storage.BlockState{
			Block:         block,
			StateRootHash: stateRootHash,
			Cell:          stateRoot,
		},
	}
}

func testNonfinalBlockForStateUpdate(t *testing.T, workchain int32, shard int64, seqno uint32, prev ton.BlockIDExt, update *cell.Cell) (ton.BlockIDExt, *cell.Cell) {
	t.Helper()

	var header tlb.BlockHeader
	header.Version = 1
	header.Shard = tlb.ShardIdent{
		PrefixBits:  0,
		WorkchainID: workchain,
		ShardPrefix: uint64(shard),
	}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1001
	header.MinRefMcSeqno = seqno
	header.PrevKeyBlockSeqno = 0
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    1,
		SeqNo:    prev.SeqNo,
		RootHash: bytes.Clone(prev.RootHash),
		FileHash: bytes.Clone(prev.FileHash),
	}}
	if workchain != masterchainID {
		header.NotMaster = true
		header.MasterRef = &tlb.ExtBlkRef{
			EndLt:    1,
			SeqNo:    seqno,
			RootHash: bytes.Repeat([]byte{0x05}, 32),
			FileHash: bytes.Repeat([]byte{0x06}, 32),
		}
	}

	root, err := tlb.ToCell(&tlb.Block{
		GlobalID:    -239,
		BlockInfo:   header,
		ValueFlow:   cell.BeginCell().EndCell(),
		StateUpdate: update,
		Extra: &tlb.BlockExtra{
			InMsgDesc:          cell.BeginCell().EndCell(),
			OutMsgDesc:         cell.BeginCell().EndCell(),
			ShardAccountBlocks: cell.BeginCell().EndCell(),
			RandSeed:           bytes.Repeat([]byte{0x01}, 32),
			CreatedBy:          bytes.Repeat([]byte{0x02}, 32),
		},
	})
	if err != nil {
		t.Fatalf("build non-final block: %v", err)
	}

	return testBlockIDForRoot(workchain, shard, seqno, root), root
}

func testNonfinalStateWithTracedRootRefs(t *testing.T, state *cell.Cell, refsFrom *cell.Cell) *cell.Cell {
	t.Helper()

	stateLoader, err := state.BeginParse()
	if err != nil {
		t.Fatalf("begin state: %v", err)
	}
	stateBits, err := stateLoader.LoadSlice(state.BitsSize())
	if err != nil {
		t.Fatalf("load state bits: %v", err)
	}
	refsLoader, err := refsFrom.BeginParse()
	if err != nil {
		t.Fatalf("begin traced state: %v", err)
	}
	if state.RefsNum() != refsFrom.RefsNum() {
		t.Fatalf("state refs = %d, traced refs = %d", state.RefsNum(), refsFrom.RefsNum())
	}

	builder := cell.BeginCell().MustStoreSlice(stateBits, state.BitsSize())
	for i := 0; i < int(state.RefsNum()); i++ {
		ref, err := refsLoader.PeekRefCellAt(i)
		if err != nil {
			t.Fatalf("load traced ref %d: %v", i, err)
		}
		builder.MustStoreRef(ref)
	}
	traced := builder.EndCell()
	if !bytes.Equal(traced.Hash(0), state.Hash(0)) {
		t.Fatalf("traced state hash mismatch")
	}
	return traced
}
