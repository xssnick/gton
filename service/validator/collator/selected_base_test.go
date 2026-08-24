package collator

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

type selectedBaseAcquisitionFixture struct {
	acquisition *LocalAcquisition
	session     ActivatedSession
	update      SessionUpdate
	managed     *localAcquisitionSession
	base        *SelectedBaseState
	blockBOC    []byte
	queueTip    [32]byte
}

func newSelectedBaseAcquisitionFixture(
	t *testing.T,
	candidate simplex.CandidateID,
) selectedBaseAcquisitionFixture {
	t.Helper()

	buildFixture := newMasterBuildFixture(t, false)
	built, err := testBuilder().BuildMaster(context.Background(), buildFixture.request)
	if err != nil {
		t.Fatal(err)
	}
	acquisition, pool, session, update := localRotationFixture(t)
	t.Cleanup(pool.Close)
	managed, err := acquisition.session(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewSelectedBaseState(
		session.ID,
		candidate,
		built.ID,
		built.BlockBOC,
		candidateBlock(t, built),
		built.State,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(session.Genesis) != 1 || !session.Genesis[0].Equals(&buildFixture.request.Previous.ID) {
		t.Fatal("selected-base fixture does not extend the acquisition genesis")
	}
	previous := []PreviousBlock{buildFixture.request.Previous}
	if err = acquisition.ensureCandidateBase(managed.branch, previous, true, &prewarmHints{}); err != nil {
		t.Fatal(err)
	}
	queueTip, err := blockRootKey(built.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = managed.branch.AddCandidate(msgpool.CandidateRequest{
		ID:    queueTip,
		Seqno: built.ID.SeqNo,
		Base:  candidateSources(previous),
		Delta: &msgpool.InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}

	return selectedBaseAcquisitionFixture{
		acquisition: acquisition,
		session:     session,
		update:      update,
		managed:     managed,
		base:        base,
		blockBOC:    built.BlockBOC,
		queueTip:    queueTip,
	}
}

func selectedBaseRicherState(fixture selectedBaseAcquisitionFixture) (localCandidateState, cell.Hash) {
	storage := cell.BeginCell().MustStoreUInt(0x5a, 8).EndCell()
	storageKey := storage.HashKey()

	return localCandidateState{
		block:        clonePreviousBlock(fixture.base.block),
		storageStats: AccountStorageStats{storageKey: storage},
		queueBase:    []PreviousBlock{clonePreviousBlock(fixture.base.block)},
		queueTip:     cloneHashPointer(&fixture.queueTip),
		master:       &localMasterView{},
	}, storageKey
}

func installSelectedBaseFixtureState(
	t *testing.T,
	fixture selectedBaseAcquisitionFixture,
	candidate simplex.CandidateID,
	state localCandidateState,
) {
	t.Helper()

	selectedKey, err := blockRootKey(state.block.ID)
	if err != nil {
		t.Fatal(err)
	}
	staleCandidate := simplex.CandidateID{Slot: candidate.Slot, Hash: [32]byte{0xee}}
	staleBlock := [32]byte{0xef}
	fixture.managed.mu.Lock()
	fixture.managed.candidates[candidate] = state
	fixture.managed.candidates[staleCandidate] = localCandidateState{}
	fixture.managed.blocks[selectedKey] = state
	fixture.managed.blocks[staleBlock] = localCandidateState{}
	fixture.managed.mu.Unlock()
}

func TestLocalAdvanceConsensusBasePreservesExistingRicherState(t *testing.T) {
	candidate := simplex.CandidateID{Slot: 4, Hash: [32]byte{0x81}}
	fixture := newSelectedBaseAcquisitionFixture(t, candidate)
	state, storageKey := selectedBaseRicherState(fixture)
	installSelectedBaseFixtureState(t, fixture, candidate, state)

	next := fixture.update
	next.CurrentWindowObservedSlot++
	next.CurrentBase = simplex.Parent(candidate)
	if err := fixture.acquisition.AdvanceConsensusBase(context.Background(), ConsensusBaseUpdate{
		Session: fixture.session,
		Update:  next,
		Base:    fixture.base,
	}); err != nil {
		t.Fatal(err)
	}

	fixture.managed.mu.Lock()
	adopted, exists := fixture.managed.candidates[candidate]
	installedUpdate := cloneSessionUpdate(fixture.managed.update)
	candidateCount := len(fixture.managed.candidates)
	blockCount := len(fixture.managed.blocks)
	fixture.managed.mu.Unlock()
	if !exists || candidateCount != 1 || blockCount != 1 {
		t.Fatalf("selected maps = (%t, %d candidates, %d blocks), want only selected", exists, candidateCount, blockCount)
	}
	if adopted.queueTip == nil || *adopted.queueTip != fixture.queueTip ||
		adopted.storageStats[storageKey] == nil || adopted.master != state.master ||
		len(adopted.queueBase) != 1 {
		t.Fatal("selected base lost richer queue, storage, or master state")
	}
	if !installedUpdate.Equal(next) {
		t.Fatalf("installed update = %+v, want %+v", installedUpdate, next)
	}
	if err := fixture.managed.branch.Retain(&fixture.queueTip); err != nil {
		t.Fatalf("selected queue tip was not retained: %v", err)
	}
}

func TestLocalAdvanceConsensusBaseRekeysEmptyAliasWithoutStoreRead(t *testing.T) {
	ordinaryID := simplex.CandidateID{Slot: 4, Hash: [32]byte{0x88}}
	fixture := newSelectedBaseAcquisitionFixture(t, ordinaryID)
	state, storageKey := selectedBaseRicherState(fixture)
	installSelectedBaseFixtureState(t, fixture, ordinaryID, state)

	empty := simplex.Candidate{
		Parent: simplex.Parent(ordinaryID),
		Leader: 0,
		Empty:  true,
		Block:  fixture.base.block.ID,
	}
	empty.ID = empty.ComputeID(5)
	emptyBase, err := NewSelectedBaseState(
		fixture.session.ID,
		empty.ID,
		fixture.base.block.ID,
		fixture.blockBOC,
		fixture.base.block.Block,
		fixture.base.block.State,
	)
	if err != nil {
		t.Fatal(err)
	}
	store := &finalizedAnchorNoReadStore{}
	fixture.acquisition.store = store

	next := fixture.update
	next.CurrentWindowObservedSlot += 2
	next.CurrentBase = simplex.Parent(empty.ID)
	if err = fixture.acquisition.AdvanceConsensusBase(context.Background(), ConsensusBaseUpdate{
		Session: fixture.session,
		Update:  next,
		Base:    emptyBase,
	}); err != nil {
		t.Fatal(err)
	}
	if store.calls != 0 {
		t.Fatalf("empty selected-base alias caused %d store reads", store.calls)
	}

	fixture.managed.mu.Lock()
	adopted, emptyExists := fixture.managed.candidates[empty.ID]
	_, ordinaryExists := fixture.managed.candidates[ordinaryID]
	candidateCount := len(fixture.managed.candidates)
	blockCount := len(fixture.managed.blocks)
	fixture.managed.mu.Unlock()
	if !emptyExists || ordinaryExists || candidateCount != 1 || blockCount != 1 {
		t.Fatalf(
			"empty alias maps = (empty %t, ordinary %t, %d candidates, %d blocks)",
			emptyExists,
			ordinaryExists,
			candidateCount,
			blockCount,
		)
	}
	if adopted.block.Block != state.block.Block || adopted.block.State != state.block.State ||
		adopted.queueTip == nil || *adopted.queueTip != fixture.queueTip ||
		adopted.storageStats[storageKey] == nil || adopted.master != state.master {
		t.Fatal("empty candidate did not reuse the richer state of its referenced block")
	}
}

func TestLocalAdvanceConsensusBaseRejectsExistingRootMismatchBeforeMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*localCandidateState)
	}{
		{
			name: "block",
			mutate: func(state *localCandidateState) {
				state.block.Block = cell.BeginCell().MustStoreUInt(0xb1, 8).EndCell()
			},
		},
		{
			name: "state",
			mutate: func(state *localCandidateState) {
				state.block.State = cell.BeginCell().MustStoreUInt(0xb2, 8).EndCell()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := simplex.CandidateID{Slot: 4, Hash: [32]byte{0x91}}
			fixture := newSelectedBaseAcquisitionFixture(t, candidate)
			state, _ := selectedBaseRicherState(fixture)
			test.mutate(&state)
			installSelectedBaseFixtureState(t, fixture, candidate, state)
			store := &finalizedAnchorNoReadStore{}
			fixture.acquisition.store = store
			seed := [32]byte{0xa1}
			fixture.managed.mu.Lock()
			fixture.managed.seeds[1] = seed
			initialMaster := fixture.managed.master
			fixture.managed.mu.Unlock()

			next := fixture.update
			next.CurrentWindowObservedSlot++
			next.CurrentBase = simplex.Parent(candidate)
			err := fixture.acquisition.AdvanceConsensusBase(context.Background(), ConsensusBaseUpdate{
				Session: fixture.session,
				Update:  next,
				Base:    fixture.base,
			})
			if !errors.Is(err, ErrCandidateConflict) {
				t.Fatalf("mismatch error = %v, want ErrCandidateConflict", err)
			}
			if store.calls != 0 {
				t.Fatalf("mismatch caused %d store reads", store.calls)
			}

			fixture.managed.mu.Lock()
			retained, exists := fixture.managed.candidates[candidate]
			candidateCount := len(fixture.managed.candidates)
			blockCount := len(fixture.managed.blocks)
			installedUpdate := cloneSessionUpdate(fixture.managed.update)
			retainedSeed, seedExists := fixture.managed.seeds[1]
			retainedMaster := fixture.managed.master
			fixture.managed.mu.Unlock()
			if !exists || candidateCount != 2 || blockCount != 2 || !installedUpdate.Equal(fixture.update) ||
				retained.block.Block != state.block.Block || retained.block.State != state.block.State ||
				!seedExists || retainedSeed != seed || retainedMaster != initialMaster {
				t.Fatal("mismatch mutated the previously installed acquisition state")
			}
			if err = fixture.managed.branch.Retain(&fixture.queueTip); err != nil {
				t.Fatalf("mismatch mutated the private queue branch: %v", err)
			}
		})
	}
}
