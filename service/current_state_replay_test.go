package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestLoadBlockStateForApplyTrustsPublishedBlockOnlyState(t *testing.T) {
	ctx := context.Background()
	block := testBlockID(0, topShard, 100)
	state := testReplayBlockState(t, block)
	store := &testReplayStateStore{
		state:        state,
		blockFullErr: storage.ErrNotFound,
	}
	svc := &SyncCoordinator{storage: store}

	got, err := svc.loadBlockStateForApply(ctx, storage.BlockState{Block: block})
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if !got.Block.Equals(&block) {
		t.Fatalf("loaded block = %s, want %s", storage.FormatBlockRef(got.Block), storage.FormatBlockRef(block))
	}
	if store.blockFullCalls != 0 {
		t.Fatalf("BlockFull calls = %d, want 0", store.blockFullCalls)
	}
}

func TestLoadBlockStateForApplyDoesNotProbeFullBlockForPublishedState(t *testing.T) {
	ctx := context.Background()
	block := testBlockID(0, topShard, 101)
	state := testReplayBlockState(t, block)
	store := &testReplayStateStore{
		state:     state,
		blockFull: &storage.ServedBlockFull{ID: block},
	}
	svc := &SyncCoordinator{storage: store}

	got, err := svc.loadBlockStateForApply(ctx, storage.BlockState{Block: block})
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if !got.Block.Equals(&block) {
		t.Fatalf("loaded block = %s, want %s", storage.FormatBlockRef(got.Block), storage.FormatBlockRef(block))
	}
	if store.blockFullCalls != 0 {
		t.Fatalf("BlockFull calls = %d, want 0", store.blockFullCalls)
	}
}

func TestLoadBlockStateForApplyAllowsCurrentBoundaryWithoutFullBlock(t *testing.T) {
	ctx := context.Background()
	block := testBlockID(0, topShard, 102)
	state := testReplayBlockState(t, block)
	store := &testReplayStateStore{
		state:        state,
		blockFullErr: storage.ErrNotFound,
	}
	svc := &SyncCoordinator{storage: store}

	got, err := svc.loadBlockStateForApply(ctx, storage.BlockState{
		Block:         block,
		StateRootHash: state.StateRootHash,
	})
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if !got.Block.Equals(&block) {
		t.Fatalf("loaded block = %s, want %s", storage.FormatBlockRef(got.Block), storage.FormatBlockRef(block))
	}
	if store.blockFullCalls != 0 {
		t.Fatalf("BlockFull calls = %d, want 0", store.blockFullCalls)
	}
}

func testReplayBlockState(tb testing.TB, block ton.BlockIDExt) *storage.BlockState {
	tb.Helper()

	root := testShardStateCell(tb, block)
	rootHash := root.HashKey(0)
	return &storage.BlockState{
		Block:         block,
		StateRootHash: rootHash[:],
		Cell:          root,
	}
}

type testReplayStateStore struct {
	testStorage

	state          *storage.BlockState
	stateErr       error
	blockFull      *storage.ServedBlockFull
	blockFullErr   error
	blockFullCalls int
}

func (s *testReplayStateStore) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	if s.stateErr != nil {
		return nil, s.stateErr
	}
	if s.state == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneBlockState(s.state), nil
}

func (s *testReplayStateStore) BlockFull(context.Context, ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	s.blockFullCalls++
	if s.blockFullErr != nil {
		return nil, s.blockFullErr
	}
	if s.blockFull == nil {
		return nil, storage.ErrNotFound
	}
	return s.blockFull.Clone(), nil
}
