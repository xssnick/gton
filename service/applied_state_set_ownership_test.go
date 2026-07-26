package service

import (
	"bytes"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func ownershipTestState(seqno uint32) *storage.BlockState {
	state := &storage.BlockState{
		Block:         testBlockID(0, topShard, seqno),
		StateRootHash: bytes.Repeat([]byte{0xa1}, 32),
		StateFileHash: bytes.Repeat([]byte{0xb2}, 32),
	}
	ref := testBlockID(-1, topShard, seqno)
	state.MasterchainRef = &ref
	return state
}

func ownershipTestArtifact(state *storage.BlockState) (*storage.ServedBlockFull, []storage.ServedBlockLink) {
	artifact := &storage.ServedBlockFull{
		ID:    state.Block,
		Block: []byte{0x01, 0x02},
		Proof: []byte{0x03},
		Meta:  &storage.BlockMeta{ID: state.Block, GenUTime: state.Block.SeqNo},
	}
	links := []storage.ServedBlockLink{{
		Prev: testBlockID(0, topShard, state.Block.SeqNo-1),
		Next: state.Block,
	}}
	return artifact, links
}

// TestAppliedStateSetRememberIsolatesCallerGraph pins the ownership contract
// rememberOwnedEntry relies on: rememberWithArtifacts must not leave the stored
// entry aliasing anything the caller can still mutate.
func TestAppliedStateSetRememberIsolatesCallerGraph(t *testing.T) {
	state := ownershipTestState(10)
	artifact, links := ownershipTestArtifact(state)

	var states appliedStateSet
	states.rememberWithArtifacts(state, artifact, links)

	// Mutate every reference the caller still holds.
	state.StateRootHash[0] ^= 0xff
	state.StateFileHash[0] ^= 0xff
	state.Block.RootHash[0] ^= 0xff
	state.MasterchainRef.RootHash[0] ^= 0xff
	artifact.ID.RootHash[0] ^= 0xff
	artifact.Meta.ID.RootHash[0] ^= 0xff
	artifact.Meta.GenUTime ^= 0xffff
	links[0].Prev.RootHash[0] ^= 0xff
	links[0].Next.RootHash[0] ^= 0xff

	stored := states.cloneEntries()
	if len(stored) != 1 {
		t.Fatalf("stored entries = %d, want 1", len(stored))
	}
	entry := stored[0]

	if entry.state.StateRootHash[0] != 0xa1 {
		t.Fatalf("stored StateRootHash tracked the caller mutation: %x", entry.state.StateRootHash[:1])
	}
	if entry.state.StateFileHash[0] != 0xb2 {
		t.Fatalf("stored StateFileHash tracked the caller mutation: %x", entry.state.StateFileHash[:1])
	}
	original := testBlockID(0, topShard, 10)
	if !bytes.Equal(entry.state.Block.RootHash, original.RootHash) {
		t.Fatalf("stored block root hash tracked the caller mutation")
	}
	if entry.state.MasterchainRef == nil || entry.state.MasterchainRef.RootHash[0] == state.MasterchainRef.RootHash[0] {
		t.Fatalf("stored masterchain ref aliases the caller value")
	}
	if !bytes.Equal(entry.artifact.block.ID.RootHash, original.RootHash) {
		t.Fatalf("stored artifact ID tracked the caller mutation")
	}
	if !bytes.Equal(entry.artifact.block.Meta.ID.RootHash, original.RootHash) {
		t.Fatalf("stored artifact meta ID tracked the caller mutation")
	}
	if entry.artifact.block.Meta.GenUTime != 10 {
		t.Fatalf("stored artifact meta GenUTime = %d, want 10", entry.artifact.block.Meta.GenUTime)
	}
	if len(entry.artifact.links) != 1 || !bytes.Equal(entry.artifact.links[0].Next.RootHash, original.RootHash) {
		t.Fatalf("stored links tracked the caller mutation")
	}

	// cloneEntries must also hand out independent copies.
	entry.state.StateRootHash[0] ^= 0xff
	entry.artifact.block.Meta.GenUTime = 999
	again := states.cloneEntries()
	if again[0].state.StateRootHash[0] != 0xa1 || again[0].artifact.block.Meta.GenUTime != 10 {
		t.Fatal("cloneEntries returned an entry aliasing the set")
	}
}

// TestAppliedStateSetTakeEntriesTransfersOwnership covers the archive catch-up
// window handoff: takeEntries empties the source set and rememberAllEntries
// adopts the entries without a second clone, while the adopted entries stay
// independent of anything the source still exposes.
func TestAppliedStateSetTakeEntriesTransfersOwnership(t *testing.T) {
	var window appliedStateSet
	for seqno := uint32(10); seqno < 14; seqno++ {
		state := ownershipTestState(seqno)
		artifact, links := ownershipTestArtifact(state)
		window.rememberWithArtifacts(state, artifact, links)
	}
	wantBytes := window.byteSize()
	if wantBytes == 0 {
		t.Fatal("window artifact bytes = 0, want the staged payload size")
	}

	taken := window.takeEntries()
	if len(taken) != 4 {
		t.Fatalf("taken entries = %d, want 4", len(taken))
	}
	if window.byteSize() != 0 || len(window.states) != 0 {
		t.Fatalf("source set not emptied: bytes=%d states=%d", window.byteSize(), len(window.states))
	}
	for i := 1; i < len(taken); i++ {
		previous := storage.BlockKey(taken[i-1].state.Block)
		current := storage.BlockKey(taken[i].state.Block)
		if bytes.Compare(previous[:], current[:]) >= 0 {
			t.Fatal("taken entries are not in sorted key order")
		}
	}

	var service appliedStateSet
	service.rememberAllEntries(taken)
	if service.byteSize() != wantBytes {
		t.Fatalf("adopted artifact bytes = %d, want %d", service.byteSize(), wantBytes)
	}
	if len(service.states) != 4 {
		t.Fatalf("adopted states = %d, want 4", len(service.states))
	}

	// Re-staging the same window must not double count.
	service.rememberAllEntries(service.takeEntries())
	if service.byteSize() != wantBytes {
		t.Fatalf("re-adopted artifact bytes = %d, want %d", service.byteSize(), wantBytes)
	}
}

// TestAppliedStateSetStateOnlyUpdateKeepsArtifact covers the ownership transfer
// in rememberOwnedEntry: a state-only refresh must inherit the complete
// artifact of the entry it replaces, with the byte accounting still exact.
func TestAppliedStateSetStateOnlyUpdateKeepsArtifact(t *testing.T) {
	state := ownershipTestState(10)
	artifact, links := ownershipTestArtifact(state)

	var states appliedStateSet
	states.rememberWithArtifacts(state, artifact, links)
	fullBytes := states.byteSize()

	refreshed := ownershipTestState(10)
	refreshed.StateRootHash = bytes.Repeat([]byte{0xcc}, 32)
	states.rememberWithArtifacts(refreshed, nil, nil)

	if states.byteSize() != fullBytes {
		t.Fatalf("artifact bytes after state-only update = %d, want %d", states.byteSize(), fullBytes)
	}
	entries := states.cloneEntries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if !entries[0].artifact.complete() {
		t.Fatal("state-only update dropped the complete artifact")
	}
	if len(entries[0].artifact.links) != 1 {
		t.Fatalf("state-only update dropped the artifact links: %d", len(entries[0].artifact.links))
	}
	if entries[0].state.StateRootHash[0] != 0xcc {
		t.Fatalf("state-only update did not refresh the state: %x", entries[0].state.StateRootHash[:1])
	}
}
