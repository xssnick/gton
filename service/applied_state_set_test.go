package service

import (
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestAppliedStateSetCloneDoesNotClear(t *testing.T) {
	historical := &storage.BlockState{Block: testBlockID(0, topShard, 10)}

	var states appliedStateSet
	states.remember(historical)

	cloned := states.clone()
	if len(cloned) != 1 {
		t.Fatalf("cloned states = %d, want 1", len(cloned))
	}

	taken := states.take()
	if len(taken) != 1 {
		t.Fatalf("taken states after clone = %d, want 1", len(taken))
	}
	if !taken[0].Block.Equals(&historical.Block) {
		t.Fatalf("taken block = %s, want %s", storage.FormatBlockRef(taken[0].Block), storage.FormatBlockRef(historical.Block))
	}
}

func TestAppliedStateCheckpointCompletesOnlyAfterSuccess(t *testing.T) {
	first := &storage.BlockState{Block: testBlockID(0, topShard, 10)}
	second := &storage.BlockState{Block: testBlockID(0, topShard, 11)}

	var states appliedStateSet
	states.remember(first)

	checkpoint := states.checkpoint()
	if len(checkpoint.entries) != 1 {
		t.Fatalf("checkpoint states = %d, want 1", len(checkpoint.entries))
	}

	states.remember(second)
	states.completeCheckpoint(checkpoint)

	remaining := states.clone()
	if len(remaining) != 1 {
		t.Fatalf("remaining states = %d, want 1", len(remaining))
	}
	if !remaining[0].Block.Equals(&second.Block) {
		t.Fatalf("remaining block = %s, want %s", storage.FormatBlockRef(remaining[0].Block), storage.FormatBlockRef(second.Block))
	}
}

func TestAppliedStateCheckpointCarriesCellsWithState(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	hash := root.HashKey()
	state := &storage.BlockState{
		Block:         testBlockID(0, topShard, 10),
		StateRootHash: hash[:],
	}

	var states appliedStateSet
	states.rememberWithCells(state, map[cell.Hash][]byte{hash: {0x01, 0x02}})

	checkpoint := states.checkpoint()
	if len(checkpoint.entries) != 1 {
		t.Fatalf("checkpoint entries = %d, want 1", len(checkpoint.entries))
	}
	if !checkpoint.entries[0].State.Block.Equals(&state.Block) {
		t.Fatalf("checkpoint state = %s, want %s", storage.FormatBlockRef(checkpoint.entries[0].State.Block), storage.FormatBlockRef(state.Block))
	}
	if got := len(checkpoint.entries[0].Cells); got != 1 {
		t.Fatalf("checkpoint cells = %d, want 1", got)
	}
	if checkpoint.entries[0].Cells[0].Hash != hash {
		t.Fatalf("checkpoint cell hash = %x, want %x", checkpoint.entries[0].Cells[0].Hash, hash)
	}
}

func TestAppliedStateCheckpointFailureKeepsStates(t *testing.T) {
	first := &storage.BlockState{Block: testBlockID(0, topShard, 10)}

	var states appliedStateSet
	states.remember(first)

	checkpoint := states.checkpoint()
	if len(checkpoint.entries) != 1 {
		t.Fatalf("checkpoint states = %d, want 1", len(checkpoint.entries))
	}

	nextCheckpoint := states.checkpoint()
	if len(nextCheckpoint.entries) != 1 {
		t.Fatalf("next checkpoint states = %d, want 1", len(nextCheckpoint.entries))
	}
	if !nextCheckpoint.entries[0].State.Block.Equals(&first.Block) {
		t.Fatalf("next checkpoint block = %s, want %s", storage.FormatBlockRef(nextCheckpoint.entries[0].State.Block), storage.FormatBlockRef(first.Block))
	}
}
