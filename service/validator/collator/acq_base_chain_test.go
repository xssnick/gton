package collator

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// TestLocalAdvanceConsensusBaseForgetsCommittedAncestors opens window after
// window on this node's own candidates — a delegated collator, or a session of
// one — while the pool applies them a few blocks behind. Retain alone keeps
// every ancestor of the selected base, so the private queue lineage and both
// acquisition maps grew for as long as the session lived.
func TestLocalAdvanceConsensusBaseForgetsCommittedAncestors(t *testing.T) {
	const (
		windows        = 24
		slotsPerWindow = 4
		applyLag       = 2
	)
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

	candidate := first
	tip := fixture.queueTip
	state, _ := selectedBaseRicherState(fixture)
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
				t.Fatalf("window %d slot %d: %v", window, index, err)
			}
			nextState := state
			nextState.block.ID = cloneBlockID(state.block.ID)
			nextState.block.ID.SeqNo++
			nextState.block.ID.RootHash = append([]byte(nil), next[:]...)
			nextState.block.ID.FileHash = append([]byte(nil), next[:]...)
			nextState.queueTip = cloneHashPointer(&next)
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
				t.Fatalf("apply seqno %d: %v", unapplied[0].Seqno, err)
			}
			unapplied = unapplied[1:]
		}

		update.CurrentWindowStart = (candidate.Slot/fixture.session.SlotsPerLeaderWindow + 1) *
			fixture.session.SlotsPerLeaderWindow
		update.CurrentWindowObservedSlot = update.CurrentWindowStart
		update.CurrentWindowStartAt = fixture.update.CurrentWindowStartAt.Add(
			time.Duration(update.CurrentWindowStart-fixture.update.CurrentWindowStart) * update.TargetRate,
		)
		update.CurrentBase = simplex.Parent(candidate)
		if err = fixture.acquisition.AdvanceConsensusBase(context.Background(), ConsensusBaseUpdate{
			Session: fixture.session,
			Update:  update,
			Base: &SelectedBaseState{
				sessionID: fixture.session.ID,
				candidate: candidate,
				block:     clonePreviousBlock(state.block),
			},
		}); err != nil {
			t.Fatalf("window %d: %v", window, err)
		}
	}

	fixture.managed.mu.Lock()
	candidateCount := len(fixture.managed.candidates)
	blockCount := len(fixture.managed.blocks)
	base, baseExists := fixture.managed.candidates[candidate]
	fixture.managed.mu.Unlock()
	if candidateCount != applyLag || blockCount != applyLag {
		t.Fatalf("acquisition keeps %d candidates and %d blocks after %d windows, want the %d unapplied ones",
			candidateCount, blockCount, windows, applyLag)
	}
	if fixture.managed.branch.HasCandidate(fixture.queueTip) {
		t.Fatal("private queue lineage still holds the session's first, long committed candidate")
	}
	if !baseExists || base.queueTip == nil || *base.queueTip != tip || !fixture.managed.branch.HasCandidate(tip) {
		t.Fatal("selected base lost its queue lineage")
	}

	successor := [32]byte{0xc8}
	if err = fixture.managed.branch.AddCandidate(msgpool.CandidateRequest{
		ID: successor, Parent: &tip, Seqno: state.block.ID.SeqNo + 1, Delta: &msgpool.InternalsDelta{},
	}); err != nil {
		t.Fatalf("build on the selected base after its ancestors were forgotten: %v", err)
	}
	if _, err = fixture.managed.branch.Cut(msgpool.CutRequest{
		CandidateTip:     &successor,
		CandidateSources: []msgpool.ShardIdent{destination},
	}); err != nil {
		t.Fatalf("cut on the selected base after its ancestors were forgotten: %v", err)
	}
}
