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
