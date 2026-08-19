package validator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// liveSuccessorFixture is a transition of the shape the invariant is about: a
// parent with a subtree the block never reads, and an update that therefore
// prunes that subtree onto a boundary instead of carrying it.
//
// narrowParent is the source proof the update itself carries, virtualized the
// way a proof-backed predecessor reaches the collator. Applying to it produces
// a root with the SAME hash as applying to the full parent and a pruned branch
// where the untouched subtree should be — which is precisely why a test that
// only compares hashes proves nothing.
type liveSuccessorFixture struct {
	parent       *cell.Cell
	narrowParent *cell.Cell
	update       *cell.Cell
	// prepared is update as the verifier hands it back: the verdict decided
	// once and the update-side walks recorded, which is what every production
	// caller of announce and validatedCandidateState now carries.
	prepared     *cell.PreparedMerkleUpdate
	successor    *cell.Cell
	untouchedBit uint64
}

const liveSuccessorUntouchedBits = 0x5aa5c3c3

func newLiveSuccessorFixture(tb testing.TB, marker uint64) liveSuccessorFixture {
	tb.Helper()

	// The leaf carries a child of its own because a childless subtree is never
	// replaced by a boundary — a pruned branch weighs more than it does — and
	// the transition of the grandchild in the no-store test needs a boundary to
	// prune onto here.
	leaf := cell.BeginCell().
		MustStoreUInt(liveSuccessorUntouchedBits, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(marker, 16).EndCell()).
		EndCell()
	untouched := cell.BeginCell().MustStoreUInt(0xdead0000+marker, 32).MustStoreRef(leaf).EndCell()
	touched := cell.BeginCell().MustStoreUInt(1, 32).EndCell()
	parent := cell.BeginCell().
		MustStoreUInt(marker, 32).
		MustStoreRef(touched).
		MustStoreRef(untouched).
		EndCell()

	// Read the root and the half the block rewrites, and nothing else. The read
	// set is the only record the update is built from, so the untouched subtree
	// becomes a boundary exactly as it does in a real collation.
	reads := cell.NewReadSet(parent)
	root, err := reads.Root().BeginParse()
	if err != nil {
		tb.Fatalf("read the fixture parent: %v", err)
	}
	root.MustLoadUInt(32)
	root.MustLoadRef().MustLoadUInt(32)

	successor := cell.BeginCell().
		MustStoreUInt(marker, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(2, 32).EndCell()).
		MustStoreRef(untouched).
		EndCell()
	update, err := reads.CreateMerkleUpdate(successor)
	if err != nil {
		tb.Fatalf("build the fixture state update: %v", err)
	}
	source, err := update.PeekRef(0)
	if err != nil {
		tb.Fatalf("load the update source proof: %v", err)
	}

	prepared, err := cell.PrepareMerkleUpdatePlanned(update)
	if err != nil {
		tb.Fatalf("prepare the fixture state update: %v", err)
	}

	return liveSuccessorFixture{
		parent:       parent,
		narrowParent: source.Virtualize(0),
		update:       update,
		prepared:     prepared,
		successor:    successor,
		untouchedBit: liveSuccessorUntouchedBits,
	}
}

// state builds a normal one-tip ChainState on this fixture's parent, through
// the real constructor.
func (f liveSuccessorFixture) state(tb testing.TB, marker uint64) (*ChainState, *CandidateArtifact) {
	tb.Helper()

	parent := chainStateBlock(shard.Root, 10, marker)
	request := ChainStateRequest{
		Shard:  groups.ShardID{Workchain: 0, Shard: shard.Root},
		Blocks: []ton.BlockIDExt{parent},
	}
	data := chainStateData(parent)
	data.Tips[0].State = f.parent
	state, err := newChainState(request, data)
	if err != nil {
		tb.Fatalf("build the fixture chain state: %v", err)
	}

	return state, liveSuccessorArtifact(marker + 1)
}

// liveSuccessorArtifact is the candidate the transition belongs to. Its id has
// to name a real cell, because the successor carries the parsed block root and
// validatedCandidateState binds the two.
func liveSuccessorArtifact(marker uint64) *CandidateArtifact {
	return &CandidateArtifact{
		Candidate: simplex.Candidate{
			Block: ton.BlockIDExt{
				Workchain: 0,
				Shard:     shard.Root,
				SeqNo:     11,
				RootHash:  testTipRootHash(marker),
				FileHash:  testTipFileHash(marker),
			},
		},
		BlockBOC: []byte{0x02},
	}
}

func (f liveSuccessorFixture) successorOf(artifact *CandidateArtifact) CandidateSuccessor {
	return CandidateSuccessor{
		BlockRoot:   testTipBlockFor(artifact.Candidate.Block),
		StateUpdate: f.update,
		Prepared:    f.prepared,
		StateHash:   f.successor.HashKeyAt(0),
	}
}

// untouchedLeafBits walks past the subtree the update never touched and reads
// the leaf under it. It is the whole point of the test that calls it: a narrow
// successor answers the hash question identically and fails here.
func untouchedLeafBits(root *cell.Cell) (uint64, error) {
	branch, err := root.PeekRef(1)
	if err != nil {
		return 0, err
	}
	if branch.GetType() == cell.PrunedCellType {
		return 0, errors.New("the untouched subtree is a pruned boundary, not a materialized tree")
	}
	leaf, err := branch.PeekRef(0)
	if err != nil {
		return 0, err
	}
	slice, err := leaf.BeginParse()
	if err != nil {
		return 0, err
	}

	return slice.LoadUInt(32)
}

// A validated candidate's successor is produced by applying its transition to
// the full live parent this state holds, never by republishing the root the
// verifier executed against — that root is proof-backed on the full-collated
// path, and a narrow state in the lineage breaks every descendant.
//
// The assertion that matters is the last one. A narrow successor has the same
// root hash as a full one, so comparing hashes cannot tell them apart; only
// reading a leaf of a subtree the transition never touched can.
func TestChainStateValidatedSuccessorAppliesToFullParent(t *testing.T) {
	fixture := newLiveSuccessorFixture(t, 0x10)
	state, artifact := fixture.state(t, 0x10)

	next, err := state.validatedCandidateState(artifact, fixture.successorOf(artifact), nil)
	if err != nil {
		t.Fatalf("apply a validated candidate to the live parent: %v", err)
	}
	if next.root.HashKeyAt(0) != fixture.successor.HashKeyAt(0) {
		t.Fatal("the live successor is not the state the candidate asserts")
	}
	if len(next.tips) != 1 {
		t.Fatalf("successor has %d tips, want one normal tip", len(next.tips))
	}
	tip := &next.tips[0]
	if tip.State != next.root || tip.Block != fixture.successorOf(artifact).BlockRoot ||
		!bytes.Equal(tip.BlockBOC, artifact.BlockBOC) || tip.ID.SeqNo != artifact.Candidate.Block.SeqNo {
		t.Fatal("the successor tip does not carry the candidate it was built from")
	}

	// The control that makes the assertion below necessary: the root the
	// verifier produces on a proof-backed predecessor passes the hash check and
	// carries a pruned branch where the untouched subtree belongs.
	narrow, err := cell.ApplyMerkleUpdate(fixture.narrowParent, fixture.update)
	if err != nil {
		t.Fatalf("apply to the proof-backed parent: %v", err)
	}
	if narrow.HashKeyAt(0) != next.root.HashKeyAt(0) {
		t.Fatal("the narrow successor has another hash, so the hash check would have caught it")
	}
	if _, err = untouchedLeafBits(narrow); err == nil {
		t.Fatal("the fixture's untouched subtree survives a narrow apply, so it proves nothing")
	}

	bits, err := untouchedLeafBits(next.root)
	if err != nil {
		t.Fatalf("walk the untouched subtree of the live successor: %v", err)
	}
	if bits != fixture.untouchedBit {
		t.Fatalf("untouched leaf = %#x, want %#x", bits, fixture.untouchedBit)
	}

	// The parent is immutable and shared between competing children; applying
	// to it must leave it exactly as it was.
	if state.root != fixture.parent || state.root.HashKeyAt(0) != fixture.parent.HashKeyAt(0) {
		t.Fatal("the apply replaced the parent it was given")
	}
}

// The guards on the transition itself. Each of them is a plumbing fault rather
// than a semantic one — the candidate has already been accepted at this point —
// so each must fail the attempt loudly instead of publishing a state.
func TestChainStateValidatedSuccessorRejectsAMisroutedTransition(t *testing.T) {
	fixture := newLiveSuccessorFixture(t, 0x20)
	state, artifact := fixture.state(t, 0x20)
	valid := fixture.successorOf(artifact)

	cases := []struct {
		name      string
		successor CandidateSuccessor
		want      string
	}{
		{
			name:      "no block root",
			successor: CandidateSuccessor{StateUpdate: valid.StateUpdate, StateHash: valid.StateHash},
			want:      "no parsed block root",
		},
		{
			name:      "no state update",
			successor: CandidateSuccessor{BlockRoot: valid.BlockRoot, StateHash: valid.StateHash},
			want:      "no state update",
		},
		{
			name: "foreign block root",
			successor: CandidateSuccessor{
				BlockRoot:   testTipBlock(0x2f),
				StateUpdate: valid.StateUpdate,
				StateHash:   valid.StateHash,
			},
			want: "differs from the candidate id",
		},
		{
			name: "state hash of another transition",
			successor: CandidateSuccessor{
				BlockRoot:   valid.BlockRoot,
				StateUpdate: valid.StateUpdate,
				StateHash:   fixture.parent.HashKeyAt(0),
			},
			want: "live successor differs from the validated state",
		},
		{
			name: "update of another parent",
			successor: CandidateSuccessor{
				BlockRoot:   valid.BlockRoot,
				StateUpdate: newLiveSuccessorFixture(t, 0x21).update,
				StateHash:   valid.StateHash,
			},
			want: "apply validated candidate to the live parent",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, err := state.validatedCandidateState(artifact, tc.successor, nil)
			if err == nil {
				t.Fatal("a misrouted transition produced a state")
			}
			if next != nil {
				t.Fatal("a failed apply returned a state as well as an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
			// Never a rejection: the candidate was already found valid, and
			// voting against it because our own bookkeeping failed would be a
			// vote against a good block.
			if errors.Is(err, ErrCandidateRejected) {
				t.Fatal("a local bookkeeping failure was classified as a candidate rejection")
			}
		})
	}
}

// The three predecessor shapes a session can start from all reduce to one root
// that the successor is applied to, and the successor is one normal tip in
// every case. The zerostate is the one that has no block of its own, and its
// successor still has to carry one.
func TestChainStateValidatedSuccessorFromEveryParentShape(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(shard.Root, false)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("zerostate", func(t *testing.T) {
		fixture := newLiveSuccessorFixture(t, 0x30)
		zero := chainStateBlock(shard.Root, 0, 0x31)
		request := ChainStateRequest{
			Shard:  groups.ShardID{Workchain: 0, Shard: shard.Root},
			Blocks: []ton.BlockIDExt{zero},
		}
		data := chainStateData(zero)
		data.Tips[0].State = fixture.parent
		state, err := newChainState(request, data)
		if err != nil {
			t.Fatalf("build the zerostate parent: %v", err)
		}
		if state.tips[0].Block != nil {
			t.Fatal("the zerostate parent carries a block")
		}

		artifact := liveSuccessorArtifact(0x32)
		next, err := state.validatedCandidateState(artifact, fixture.successorOf(artifact), nil)
		if err != nil {
			t.Fatalf("advance a zerostate: %v", err)
		}
		if len(next.tips) != 1 || next.tips[0].Block == nil || len(next.tips[0].BlockBOC) == 0 {
			t.Fatal("the successor of a zerostate does not carry its own block")
		}
		if bits, bitsErr := untouchedLeafBits(next.root); bitsErr != nil || bits != fixture.untouchedBit {
			t.Fatalf("untouched leaf after a zerostate apply = %#x, %v", bits, bitsErr)
		}
	})

	t.Run("merge", func(t *testing.T) {
		leftBlock := chainStateBlock(left, 11, 0x40)
		rightBlock := chainStateBlock(right, 12, 0x41)
		request := ChainStateRequest{
			Shard:  groups.ShardID{Workchain: 0, Shard: shard.Root},
			Blocks: []ton.BlockIDExt{leftBlock, rightBlock},
		}
		data := chainStateData(leftBlock, rightBlock)
		data.Tips[0].State = newLiveSuccessorFixture(t, 0x42).parent
		data.Tips[1].State = newLiveSuccessorFixture(t, 0x43).parent
		state, err := newChainState(request, data)
		if err != nil {
			t.Fatalf("build the merge parent: %v", err)
		}

		// The merge root is built by newChainState, so the transition has to be
		// derived from it rather than from a tip.
		update, successor := testStateUpdate(state.root, 0x44)
		artifact := liveSuccessorArtifact(0x45)
		next, err := state.validatedCandidateState(artifact, CandidateSuccessor{
			BlockRoot:   testTipBlockFor(artifact.Candidate.Block),
			StateUpdate: update,
			StateHash:   successor.HashKeyAt(0),
		}, nil)
		if err != nil {
			t.Fatalf("advance a merge parent: %v", err)
		}
		if len(next.tips) != 1 || next.tips[0].ID.Shard != shard.Root {
			t.Fatal("the successor of a merge is not a single normal tip")
		}
		// The merged parent is reachable through the successor, so both halves
		// of the merge are still materialized in the lineage.
		merged, err := next.root.PeekRef(0)
		if err != nil {
			t.Fatal(err)
		}
		if merged.HashKeyAt(0) != state.root.HashKeyAt(0) {
			t.Fatal("the successor lost the merged parent it was applied to")
		}
	})

	t.Run("before split", func(t *testing.T) {
		fixture := newLiveSuccessorFixture(t, 0x50)
		parent := chainStateBlock(shard.Root, 10, 0x51)
		request := ChainStateRequest{
			Shard:  groups.ShardID{Workchain: 0, Shard: left},
			Blocks: []ton.BlockIDExt{parent},
		}
		data := chainStateData(parent)
		data.Tips[0].State = fixture.parent
		state, err := newChainState(request, data)
		if err != nil {
			t.Fatalf("build the before-split parent: %v", err)
		}

		// Both children of a split advance the same parent root, so applying
		// for one must leave the other's apply intact.
		first := liveSuccessorArtifact(0x52)
		second := liveSuccessorArtifact(0x53)
		firstNext, err := state.validatedCandidateState(first, fixture.successorOf(first), nil)
		if err != nil {
			t.Fatalf("advance the first split child: %v", err)
		}
		secondNext, err := state.validatedCandidateState(second, fixture.successorOf(second), nil)
		if err != nil {
			t.Fatalf("advance the second split child: %v", err)
		}
		for _, next := range []*ChainState{firstNext, secondNext} {
			bits, bitsErr := untouchedLeafBits(next.root)
			if bitsErr != nil || bits != fixture.untouchedBit {
				t.Fatalf("untouched leaf after a before-split apply = %#x, %v", bits, bitsErr)
			}
		}
		if firstNext.tips[0].ID.RootHash[0] == secondNext.tips[0].ID.RootHash[0] &&
			bytes.Equal(firstNext.tips[0].ID.RootHash, secondNext.tips[0].ID.RootHash) {
			t.Fatal("the two split children were not distinct candidates")
		}
	})
}

// The announcement is an overlap optimization and nothing else: the successor,
// and the error when there is one, must be identical whether the apply ran
// ahead of validation or at the join. An announcement for a different
// transition must be ignored rather than consumed.
func TestChainStateAnnouncedSuccessorMatchesTheInlineApply(t *testing.T) {
	fixture := newLiveSuccessorFixture(t, 0x60)
	state, artifact := fixture.state(t, 0x60)
	successor := fixture.successorOf(artifact)

	inline, err := state.validatedCandidateState(artifact, successor, nil)
	if err != nil {
		t.Fatalf("apply without an announcement: %v", err)
	}

	announced := state.pendingSuccessor(context.Background())
	announced.announce(fixture.prepared)
	early, err := state.validatedCandidateState(artifact, successor, announced)
	if err != nil {
		t.Fatalf("apply with an announcement: %v", err)
	}
	if early.root.HashKeyAt(0) != inline.root.HashKeyAt(0) {
		t.Fatal("the announced apply produced another state")
	}
	if bits, bitsErr := untouchedLeafBits(early.root); bitsErr != nil || bits != fixture.untouchedBit {
		t.Fatalf("untouched leaf after an announced apply = %#x, %v", bits, bitsErr)
	}

	// A second announcement starts nothing: the walk in flight is the one this
	// candidate-validation task needs, including across any input waits inside
	// that task.
	before := announced.done
	announced.announce(fixture.prepared)
	if announced.done != before {
		t.Fatal("a second announcement started another walk")
	}

	// A walk started for a transition the join does not ask for is a wiring
	// fault, and it is reported rather than quietly redone: silently applying
	// again is how an announcement ends up wasted on every call with nothing to
	// say so.
	foreign := newLiveSuccessorFixture(t, 0x61)
	misannounced := state.pendingSuccessor(context.Background())
	misannounced.announce(foreign.prepared)
	if _, err = state.validatedCandidateState(artifact, successor, misannounced); err == nil ||
		!strings.Contains(err.Error(), "carries another transition") {
		t.Fatalf("apply after an announcement of another transition = %v, want the mismatch reported", err)
	}

	// The same for a handle opened on another parent, which is the mismatch a
	// pointer comparison on the parent cell used to swallow.
	other, _ := fixture.state(t, 0x62)
	if _, err = state.validatedCandidateState(artifact, successor, other.pendingSuccessor(context.Background())); err == nil ||
		!strings.Contains(err.Error(), "belongs to another chain state") {
		t.Fatalf("apply with a handle of another parent = %v, want the mismatch reported", err)
	}

	// The same holds for the failure: an announced apply that fails reports the
	// error the inline one would have, and nothing is published.
	wrongUpdate := successor
	wrongUpdate.StateUpdate = foreign.update
	wrongUpdate.Prepared = foreign.prepared
	badPending := state.pendingSuccessor(context.Background())
	badPending.announce(foreign.prepared)
	_, earlyErr := state.validatedCandidateState(artifact, wrongUpdate, badPending)
	_, inlineErr := state.validatedCandidateState(artifact, wrongUpdate, nil)
	if earlyErr == nil || inlineErr == nil || earlyErr.Error() != inlineErr.Error() {
		t.Fatalf("announced error %v differs from the inline error %v", earlyErr, inlineErr)
	}

	// A dead context starts nothing, and the join still produces the same
	// answer on its own.
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := state.pendingSuccessor(dead)
	cancelled.announce(fixture.prepared)
	if cancelled.done != nil {
		t.Fatal("a cancelled validation still started an apply")
	}
	after, err := state.validatedCandidateState(artifact, successor, cancelled)
	if err != nil || after.root.HashKeyAt(0) != inline.root.HashKeyAt(0) {
		t.Fatalf("apply after cancellation = %v, %v", after, err)
	}
}

// A speculative parent is by construction absent from the node store: block
// acceptance is only queued, so the applied pipeline never runs ahead of
// consensus. The invariant therefore has to hold with nothing to fall back on —
// which is also why there is no store-fallback branch to write. The narrow
// subtest is the bug this replaces, and it is what makes the full one evidence:
// both resolve the same speculative parent from the resolver and then advance
// it with the same transition.
func TestStateResolverSpeculativeChildValidatesWithoutNodeStore(t *testing.T) {
	fixture := newLiveSuccessorFixture(t, 0x70)

	newResolver := func(t *testing.T) (*stateResolver, *ChainState, *CandidateArtifact, *int) {
		t.Helper()

		storage := newRuntimeTestStorage()
		config, privateKey := runtimeTestConfig(0x93, &runtimeTestJournal{})
		parentArtifact := runtimeBlockArtifact(t, config, privateKey, 1, simplex.Genesis(), 1, 0x71)
		candidates := newResolverForTest(
			storage,
			&retryCandidateProvider{called: make(chan struct{}, 1)},
			1,
			simplex.DefaultParams(),
		)
		t.Cleanup(candidates.close)
		if err := candidates.stage(parentArtifact, []byte{0x01}); err != nil {
			t.Fatal(err)
		}

		backend := newRuntimeTestBackend()
		// The genesis zerostate is the parent the speculative one is built on.
		backend.stateRoot = fixture.parent
		afterStart := 0
		started := false
		backend.load = func(ctx context.Context, request ChainStateRequest) (ChainStateData, error) {
			if started {
				afterStart++

				return ChainStateData{}, ErrBlockNotReady
			}
			backend.load = nil
			data, err := backend.LoadChainState(ctx, request)
			backend.load = func(ctx context.Context, request ChainStateRequest) (ChainStateData, error) {
				afterStart++

				return ChainStateData{}, ErrBlockNotReady
			}

			return data, err
		}
		resolver := newStateResolver(
			config.Shard,
			config.StorageID,
			storage,
			backend,
			candidates,
			StoredSessionState{},
			nil,
			simplex.DefaultParams(),
			4,
		)
		t.Cleanup(resolver.close)
		if err := resolver.start(context.Background(), runtimeTestStart()); err != nil {
			t.Fatal(err)
		}
		started = true

		resolver.mu.Lock()
		genesis := resolver.genesis
		resolver.mu.Unlock()
		if genesis.root != fixture.parent {
			t.Fatal("the genesis state is not the fixture parent")
		}

		return resolver, genesis, parentArtifact, &afterStart
	}

	// The transition of the child, built the way a leader builds it: on the full
	// state its parent produced, and reading into the subtree the parent's own
	// transition left untouched. That read is what makes the child's source
	// proof descend where a narrow parent has nothing to descend into, and it
	// is exactly how the bug surfaces one candidate after the one that caused
	// it.
	childTransition := func(t *testing.T, parent *cell.Cell) (*cell.Cell, cell.Hash) {
		t.Helper()

		reads := cell.NewReadSet(parent)
		root, err := reads.Root().BeginParse()
		if err != nil {
			t.Fatal(err)
		}
		root.MustLoadUInt(32)
		root.MustLoadRef()
		root.MustLoadRef().MustLoadUInt(32)

		// The child rewrites the subtree its parent left alone, keeping the leaf
		// under it. That leaf becomes the boundary the child's source proof has
		// to expose, so the proof descends exactly where a narrow parent stops.
		branch, err := parent.PeekRef(1)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := branch.PeekRef(0)
		if err != nil {
			t.Fatal(err)
		}
		successor := cell.BeginCell().
			MustStoreUInt(0x72, 32).
			MustStoreRef(cell.BeginCell().MustStoreUInt(3, 32).EndCell()).
			MustStoreRef(cell.BeginCell().MustStoreUInt(0xbeef, 32).MustStoreRef(leaf).EndCell()).
			EndCell()
		update, err := reads.CreateMerkleUpdate(successor)
		if err != nil {
			t.Fatal(err)
		}

		return update, successor.HashKeyAt(0)
	}

	t.Run("narrow parent", func(t *testing.T) {
		resolver, genesis, parentArtifact, afterStart := newResolver(t)

		// What the old code published: the root the verifier produced on a
		// proof-backed predecessor, republished as a lineage state.
		narrowRoot, err := cell.ApplyMerkleUpdate(fixture.narrowParent, fixture.update)
		if err != nil {
			t.Fatal(err)
		}
		resolver.rememberValidatedState(parentArtifact.Candidate.ID, ResolvedState{State: &ChainState{
			shard: genesis.shard,
			tips: []ChainTip{{
				ID:       *parentArtifact.Candidate.Block.Copy(),
				BlockBOC: parentArtifact.BlockBOC,
				Block:    testTipBlockFor(parentArtifact.Candidate.Block),
				State:    narrowRoot,
			}},
			root: narrowRoot,
		}})

		resolved, err := resolver.resolve(context.Background(), simplex.Parent(parentArtifact.Candidate.ID))
		if err != nil {
			t.Fatalf("resolve the speculative parent: %v", err)
		}
		update, stateHash := childTransition(t, fixture.successor)
		child := liveSuccessorArtifact(0x73)
		_, err = resolved.State.validatedCandidateState(child, CandidateSuccessor{
			BlockRoot:   testTipBlockFor(child.Candidate.Block),
			StateUpdate: update,
			StateHash:   stateHash,
		}, nil)
		if err == nil {
			t.Fatal("a child advanced a narrow speculative parent")
		}
		if !strings.Contains(err.Error(), "merkle update source") {
			t.Fatalf("narrow parent error = %v, want the source materialization failure", err)
		}
		if *afterStart != 0 {
			t.Fatalf("the store was read %d times, and there is no fallback that may read it", *afterStart)
		}
	})

	t.Run("full parent", func(t *testing.T) {
		resolver, genesis, parentArtifact, afterStart := newResolver(t)

		parentState, err := genesis.validatedCandidateState(
			parentArtifact,
			CandidateSuccessor{
				BlockRoot:   testTipBlockFor(parentArtifact.Candidate.Block),
				StateUpdate: fixture.update,
				StateHash:   fixture.successor.HashKeyAt(0),
			},
			nil,
		)
		if err != nil {
			t.Fatalf("advance the genesis with a validated candidate: %v", err)
		}
		resolver.rememberValidatedState(parentArtifact.Candidate.ID, ResolvedState{State: parentState})

		resolved, err := resolver.resolve(context.Background(), simplex.Parent(parentArtifact.Candidate.ID))
		if err != nil {
			t.Fatalf("resolve the speculative parent: %v", err)
		}
		update, stateHash := childTransition(t, fixture.successor)
		child := liveSuccessorArtifact(0x74)
		next, err := resolved.State.validatedCandidateState(child, CandidateSuccessor{
			BlockRoot:   testTipBlockFor(child.Candidate.Block),
			StateUpdate: update,
			StateHash:   stateHash,
		}, nil)
		if err != nil {
			t.Fatalf("advance a speculative parent with no store to fall back on: %v", err)
		}
		if next.root.HashKeyAt(0) != stateHash {
			t.Fatal("the grandchild state is not the one its transition asserts")
		}
		if *afterStart != 0 {
			t.Fatalf("a speculative lineage read the store %d times after start", *afterStart)
		}
	})
}
