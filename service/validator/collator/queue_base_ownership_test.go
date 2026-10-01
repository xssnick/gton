package collator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Advancing a continuously selected local lineage must release its original
// predecessor roots even while the applied pool remains two blocks behind.
func TestLocalQueueBaseDropsRootsAfterCommittedRebase(t *testing.T) {
	const windows, slotsPerWindow, applyLag = 24, 4, 2
	first := simplex.CandidateID{Slot: 4, Hash: [32]byte{0xc1}}
	fixture := newSelectedBaseAcquisitionFixture(t, first)
	destination := targetShardIdent(fixture.session.Shard)
	store := fixture.acquisition.store.(*localAcquisitionTestStore)
	genesis, err := localSourceRef(fixture.session.Genesis[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = fixture.acquisition.ensureInternalSource(destination, destination, genesis, store.state); err != nil {
		t.Fatal(err)
	}

	oldBlock := cell.BeginCell().MustStoreUInt(0xb10c, 16).EndCell()
	oldState := cell.BeginCell().MustStoreUInt(0x57a7, 16).EndCell()
	state, _ := selectedBaseRicherState(fixture)
	state.queueBase = []PreviousBlock{{
		ID: cloneBlockID(fixture.session.Genesis[0]), Block: oldBlock, State: oldState,
	}}
	candidate, tip := first, fixture.queueTip
	fixture.managed.mu.Lock()
	fixture.managed.candidates[candidate] = state
	fixture.managed.blocks[tip] = state
	fixture.managed.mu.Unlock()
	unapplied := []msgpool.SourceRef{{Seqno: state.block.ID.SeqNo, RootHash: tip}}
	update := fixture.update
	for window := range windows {
		for index := range slotsPerWindow {
			next := [32]byte{0xc7, byte(window), byte(index)}
			if err = fixture.managed.branch.AddCandidate(msgpool.CandidateRequest{
				ID: next, Parent: &tip, Seqno: state.block.ID.SeqNo + 1, Delta: &msgpool.InternalsDelta{},
			}); err != nil {
				t.Fatal(err)
			}
			nextState := state
			nextState.block.ID = cloneBlockID(state.block.ID)
			nextState.block.ID.SeqNo++
			nextState.block.ID.RootHash = append([]byte(nil), next[:]...)
			nextState.block.ID.FileHash = append([]byte(nil), next[:]...)
			nextState.queueTip = cloneHashPointer(&next)
			// Keep the fixture richer than a queue cut needs to exercise the
			// selected-base ownership boundary, including its initial anchor.
			nextState.queueBase = clonePreviousBlocks(state.queueBase)
			candidate = simplex.CandidateID{Slot: candidate.Slot + 1, Hash: next}
			fixture.managed.mu.Lock()
			fixture.managed.candidates[candidate] = nextState
			fixture.managed.blocks[next] = nextState
			fixture.managed.mu.Unlock()
			tip, state = next, nextState
			unapplied = append(unapplied, msgpool.SourceRef{Seqno: state.block.ID.SeqNo, RootHash: tip})
		}
		for len(unapplied) > applyLag {
			if err = fixture.acquisition.messages.Internals().ApplyBlock(
				destination, destination, unapplied[0], &msgpool.InternalsDelta{},
			); err != nil {
				t.Fatal(err)
			}
			unapplied = unapplied[1:]
		}
		update.CurrentWindowStart = (candidate.Slot/fixture.session.SlotsPerLeaderWindow + 1) * fixture.session.SlotsPerLeaderWindow
		update.CurrentWindowObservedSlot = update.CurrentWindowStart
		update.CurrentWindowStartAt = fixture.update.CurrentWindowStartAt.Add(
			time.Duration(update.CurrentWindowStart-fixture.update.CurrentWindowStart) * update.TargetRate,
		)
		update.CurrentBase = simplex.Parent(candidate)
		if err = fixture.acquisition.AdvanceConsensusBase(context.Background(), ConsensusBaseUpdate{
			Session: fixture.session, Update: update,
			Base: &SelectedBaseState{sessionID: fixture.session.ID, candidate: candidate, block: clonePreviousBlock(state.block)},
		}); err != nil {
			t.Fatal(err)
		}
		fixture.managed.mu.Lock()
		state = fixture.managed.candidates[candidate]
		fixture.managed.mu.Unlock()
	}

	fixture.managed.mu.Lock()
	defer fixture.managed.mu.Unlock()
	if len(fixture.managed.candidates) != applyLag || len(fixture.managed.blocks) != applyLag ||
		fixture.managed.branch.HasCandidate(fixture.queueTip) {
		t.Fatal("fixture did not successfully rebase and discard old candidate ancestors")
	}
	base := fixture.managed.candidates[candidate]
	if len(base.queueBase) != 1 {
		t.Fatalf("queueBase identity missing: %+v", base.queueBase)
	}
	if !base.queueBase[0].ID.Equals(&fixture.session.Genesis[0]) {
		t.Fatal("rebasing changed the original queue source identity")
	}
	for _, retained := range fixture.managed.candidates {
		for _, previous := range retained.queueBase {
			if previous.Block != nil || previous.State != nil || previous.OutQueueSize != nil || previous.Proven {
				t.Fatal("candidate queue base retains predecessor payload after committed rebase")
			}
		}
	}
	for _, retained := range fixture.managed.blocks {
		for _, previous := range retained.queueBase {
			if previous.Block != nil || previous.State != nil {
				t.Fatal("block index retained predecessor roots after committed rebase")
			}
		}
	}
}

func TestCloneQueueBaseOwnsOnlyIdentities(t *testing.T) {
	block := cell.BeginCell().MustStoreUInt(0xb10c, 16).EndCell()
	state := cell.BeginCell().MustStoreUInt(0x57a7, 16).EndCell()
	blockHash, stateHash := block.HashKey(), state.HashKey()
	queueSize := uint64(123)
	previous := []PreviousBlock{{
		ID: ton.BlockIDExt{
			Workchain: -1, Shard: -1 << 63, SeqNo: 42,
			RootHash: bytes.Repeat([]byte{0x11}, 32), FileHash: bytes.Repeat([]byte{0x22}, 32),
		},
		Block: block, State: state, OutQueueSize: &queueSize, Proven: true,
	}}
	originalID := cloneBlockID(previous[0].ID)
	owned := cloneQueueBase(previous)
	if len(owned) != 1 || !owned[0].ID.Equals(&originalID) {
		t.Fatal("queue base changed block identity")
	}
	if owned[0].Block != nil || owned[0].State != nil || owned[0].OutQueueSize != nil || owned[0].Proven {
		t.Fatal("queue base retained non-identity predecessor fields")
	}
	if previous[0].Block != block || previous[0].State != state ||
		previous[0].OutQueueSize != &queueSize || queueSize != 123 || !previous[0].Proven {
		t.Fatal("queue-base projection mutated the predecessor")
	}

	owned[0].ID.RootHash[0] ^= 0xff
	owned[0].ID.FileHash[0] ^= 0xff
	owned[0].ID.SeqNo++
	if !previous[0].ID.Equals(&originalID) {
		t.Fatal("queue base aliases predecessor identity slices")
	}
	if previous[0].Block.HashKey() != blockHash || previous[0].State.HashKey() != stateHash {
		t.Fatal("queue-base projection changed predecessor cells")
	}
}

func TestCloneQueueBaseEmpty(t *testing.T) {
	for _, test := range []struct {
		name     string
		previous []PreviousBlock
	}{
		{name: "nil"},
		{name: "empty", previous: []PreviousBlock{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := cloneQueueBase(test.previous); got != nil {
				t.Fatalf("empty queue base = %v, want nil", got)
			}
		})
	}
}
