package service

import (
	"bytes"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestAppliedStateSetCloneDoesNotClear(t *testing.T) {
	historical := &storage.BlockState{Block: testBlockID(0, topShard, 10)}

	var states appliedStateSet
	states.rememberWithArtifacts(historical, &storage.ServedBlockFull{
		ID:    historical.Block,
		Block: []byte{1},
		Meta:  &storage.BlockMeta{},
	}, nil)

	cloned := states.cloneEntries()
	if len(cloned) != 1 {
		t.Fatalf("cloned states = %d, want 1", len(cloned))
	}

	taken := states.takeEntries()
	if len(taken) != 1 {
		t.Fatalf("taken states after clone = %d, want 1", len(taken))
	}
	if !taken[0].state.Block.Equals(&historical.Block) {
		t.Fatalf("taken block = %s, want %s", storage.FormatBlockRef(taken[0].state.Block), storage.FormatBlockRef(historical.Block))
	}
}

func rememberFullCheckpointStateForTest(t *testing.T, states *appliedStateSet, state *storage.BlockState) {
	t.Helper()
	states.rememberWithArtifacts(state, testStateCheckpointArtifact(state), nil)
}

func TestAppliedStateSetStoresArtifactStateMetadataOnly(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	rootHash := root.HashKey(0)
	historical := &storage.BlockState{
		Block:  testBlockID(0, topShard, 10),
		Cell:   root,
		Parsed: &tlb.ShardStateUnsplit{},
	}

	var states appliedStateSet
	states.rememberWithArtifacts(historical, &storage.ServedBlockFull{
		ID:    historical.Block,
		Block: []byte{1},
		Meta:  &storage.BlockMeta{},
	}, nil)

	cloned := states.cloneEntries()
	if len(cloned) != 1 {
		t.Fatalf("cloned states = %d, want 1", len(cloned))
	}
	if cloned[0].state.Cell != nil {
		t.Fatal("remembered state kept cell graph")
	}
	if cloned[0].state.Parsed != nil {
		t.Fatal("remembered state kept parsed state")
	}
	if !bytes.Equal(cloned[0].state.StateRootHash, rootHash[:]) {
		t.Fatalf("remembered state root hash = %x, want %x", cloned[0].state.StateRootHash, rootHash[:])
	}
}

func TestAppliedStateCheckpointCompletesOnlyAfterSuccess(t *testing.T) {
	first := &storage.BlockState{Block: testBlockID(0, topShard, 10)}
	second := &storage.BlockState{Block: testBlockID(0, topShard, 11)}

	var states appliedStateSet
	states.rememberWithArtifacts(first, &storage.ServedBlockFull{
		ID:    first.Block,
		Block: []byte{1},
		Meta:  &storage.BlockMeta{},
	}, nil)

	checkpoint := states.checkpoint()
	if len(checkpoint.entries) != 1 {
		t.Fatalf("checkpoint states = %d, want 1", len(checkpoint.entries))
	}

	states.rememberWithArtifacts(second, &storage.ServedBlockFull{
		ID:    second.Block,
		Block: []byte{2},
		Meta:  &storage.BlockMeta{},
	}, nil)
	states.completeCheckpoint(checkpoint)

	remaining := states.cloneEntries()
	if len(remaining) != 1 {
		t.Fatalf("remaining states = %d, want 1", len(remaining))
	}
	if !remaining[0].state.Block.Equals(&second.Block) {
		t.Fatalf("remaining block = %s, want %s", storage.FormatBlockRef(remaining[0].state.Block), storage.FormatBlockRef(second.Block))
	}
}

func TestAppliedStateCheckpointFailureKeepsStates(t *testing.T) {
	first := &storage.BlockState{Block: testBlockID(0, topShard, 10)}

	var states appliedStateSet
	states.rememberWithArtifacts(first, &storage.ServedBlockFull{
		ID:    first.Block,
		Block: []byte{1},
		Meta:  &storage.BlockMeta{},
	}, nil)

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

func TestAppliedStateSetStateOnlyUpdateKeepsFullArtifact(t *testing.T) {
	block := testBlockID(0, topShard, 12)
	state := &storage.BlockState{Block: block}

	var states appliedStateSet
	states.rememberWithArtifacts(state, &storage.ServedBlockFull{
		ID:    block,
		Block: []byte{1, 2, 3},
		Proof: []byte{4, 5},
		Meta:  &storage.BlockMeta{ID: block},
	}, nil)
	states.rememberEntry(appliedStateEntry{state: storage.CloneBlockState(state)})

	checkpoint := states.checkpoint()
	if len(checkpoint.entries) != 1 {
		t.Fatalf("checkpoint states = %d, want 1", len(checkpoint.entries))
	}
	artifact := checkpoint.entries[0].Artifact
	if artifact == nil {
		t.Fatal("state-only update dropped full artifact")
	}
	if !bytes.Equal(artifact.Block, []byte{1, 2, 3}) {
		t.Fatalf("artifact block = %x, want 010203", artifact.Block)
	}
	if !bytes.Equal(artifact.Proof, []byte{4, 5}) {
		t.Fatalf("artifact proof = %x, want 0405", artifact.Proof)
	}
	if got, want := states.byteSize(), uint64(5); got != want {
		t.Fatalf("artifact bytes = %d, want %d", got, want)
	}
}

func TestAppliedStateCheckpointCompleteKeepsUpdatedEntry(t *testing.T) {
	block := testBlockID(0, topShard, 13)
	state := &storage.BlockState{Block: block}

	var states appliedStateSet
	states.rememberWithArtifacts(state, &storage.ServedBlockFull{
		ID:    block,
		Block: []byte{1},
		Proof: []byte{2},
		Meta:  &storage.BlockMeta{ID: block},
	}, nil)
	checkpoint := states.checkpoint()

	states.rememberWithArtifacts(state, &storage.ServedBlockFull{
		ID:    block,
		Block: []byte{3},
		Proof: []byte{4},
		Meta:  &storage.BlockMeta{ID: block},
	}, nil)
	states.completeCheckpoint(checkpoint)

	remaining := states.cloneEntries()
	if len(remaining) != 1 {
		t.Fatalf("remaining states = %d, want 1", len(remaining))
	}
	artifact := remaining[0].artifact.block
	if artifact == nil {
		t.Fatal("newer pending entry lost full artifact")
	}
	if !bytes.Equal(artifact.Block, []byte{3}) {
		t.Fatalf("remaining artifact block = %x, want 03", artifact.Block)
	}
	if !bytes.Equal(artifact.Proof, []byte{4}) {
		t.Fatalf("remaining artifact proof = %x, want 04", artifact.Proof)
	}
	if got, want := states.byteSize(), uint64(2); got != want {
		t.Fatalf("artifact bytes = %d, want %d", got, want)
	}
}

func TestAppliedStateSetTracksArtifactBytes(t *testing.T) {
	first := &storage.BlockState{Block: testBlockID(0, topShard, 10)}
	second := &storage.BlockState{Block: testBlockID(0, topShard, 11)}

	var states appliedStateSet
	states.rememberWithArtifacts(first, &storage.ServedBlockFull{
		ID:    first.Block,
		Block: []byte{1, 2, 3},
		Proof: []byte{4, 5},
		Meta:  &storage.BlockMeta{},
	}, nil)
	states.rememberWithArtifacts(second, &storage.ServedBlockFull{
		ID:    second.Block,
		Block: []byte{6, 7},
		Proof: []byte{8},
		Meta:  &storage.BlockMeta{},
	}, nil)

	if got, want := states.byteSize(), uint64(8); got != want {
		t.Fatalf("artifact bytes = %d, want %d", got, want)
	}

	states.rememberWithArtifacts(first, &storage.ServedBlockFull{
		ID:    first.Block,
		Block: []byte{9},
		Proof: []byte{10},
		Meta:  &storage.BlockMeta{},
	}, nil)
	if got, want := states.byteSize(), uint64(5); got != want {
		t.Fatalf("artifact bytes after replace = %d, want %d", got, want)
	}

	checkpoint := appliedStateCheckpoint{keys: []storage.BlockRootHash{storage.BlockKey(first.Block)}}
	states.completeCheckpoint(checkpoint)
	if got, want := states.byteSize(), uint64(3); got != want {
		t.Fatalf("artifact bytes after checkpoint complete = %d, want %d", got, want)
	}

	entries := states.takeEntries()
	if len(entries) != 1 {
		t.Fatalf("taken entries = %d, want 1", len(entries))
	}
	if got := states.byteSize(); got != 0 {
		t.Fatalf("artifact bytes after take = %d, want 0", got)
	}
}

func TestAppliedStateSetCloneCopiesArtifactIDs(t *testing.T) {
	prev := testBlockID(0, topShard, 14)
	next := testBlockID(0, topShard, 15)
	artifact := cloneServedBlockFullSharedPayload(&storage.ServedBlockFull{ID: next})
	links := cloneServedBlockLinks([]storage.ServedBlockLink{{Prev: prev, Next: next}})

	artifact.ID.RootHash[0] = 0xA1
	links[0].Prev.RootHash[0] = 0xA2
	links[0].Next.RootHash[0] = 0xA3

	if next.RootHash[0] == 0xA1 {
		t.Fatal("artifact clone shares block id root hash backing array")
	}
	if prev.RootHash[0] == 0xA2 {
		t.Fatal("link clone shares prev id root hash backing array")
	}
	if next.RootHash[0] == 0xA3 {
		t.Fatal("link clone shares next id root hash backing array")
	}
}
