package collator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// speculativeLineageFixture is a real two-link uncommitted chain over an
// applied anchor: anchor P (the empty-candidate fixture's predecessor, seeded
// into the pool as the applied frontier), B0 built over P, B1 built over B0.
// A bet on B1 must install candidate nodes for both links before its cut can
// resolve — the exact shape of a bet placed while the foreign window's blocks
// are validated but not yet applied.
type speculativeLineageFixture struct {
	pool        *msgpool.Pool
	branch      *msgpool.Branch
	managed     *localAcquisitionSession
	acquisition *LocalAcquisition
	shard       groups.ShardID
	destination msgpool.ShardIdent
	anchor      PreviousBlock
	b0, b1      *Candidate
	prevB0      PreviousBlock
	prevB1      PreviousBlock
}

func newSpeculativeLineageFixture(t *testing.T) *speculativeLineageFixture {
	t.Helper()

	req0 := emptyCandidateRequest(t)
	anchor := req0.Previous
	b0, err := testBuilder().BuildShard(context.Background(), req0)
	if err != nil {
		t.Fatal(err)
	}
	prevB0 := PreviousBlock{
		ID:           cloneBlockID(b0.ID),
		Block:        candidateBlock(t, b0),
		State:        b0.State,
		OutQueueSize: uint64Pointer(b0.Stats.OutQueueSize),
	}
	req1 := emptyCandidateRequest(t)
	req1.Previous = prevB0
	b1, err := testBuilder().BuildShard(context.Background(), req1)
	if err != nil {
		t.Fatal(err)
	}
	prevB1 := PreviousBlock{
		ID:           cloneBlockID(b1.ID),
		Block:        candidateBlock(t, b1),
		State:        b1.State,
		OutQueueSize: uint64Pointer(b1.Stats.OutQueueSize),
	}

	destination := blockShardIdent(anchor.ID)
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	anchorRef, err := localSourceRef(anchor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, destination, anchorRef, nil, 0); err != nil {
		t.Fatal(err)
	}
	branch := openLocalTestBranch(t, pool, destination)

	managed := &localAcquisitionSession{
		branch:     branch,
		candidates: map[simplex.CandidateID]localCandidateState{},
		blocks:     map[[32]byte]localCandidateState{},
	}
	// B0 resolves from the session's block cache, the way a block this node
	// carried through validation does; the fixture has no store behind it.
	keyB0, err := blockRootKey(b0.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.blocks[keyB0] = localCandidateState{block: prevB0}

	return &speculativeLineageFixture{
		pool: pool, branch: branch, managed: managed,
		acquisition: &LocalAcquisition{messages: pool, store: &localAcquisitionTestStore{}},
		shard:       groups.ShardID{Workchain: anchor.ID.Workchain, Shard: anchor.ID.Shard},
		destination: destination,
		anchor:      anchor, b0: b0, b1: b1, prevB0: prevB0, prevB1: prevB1,
	}
}

func (f *speculativeLineageFixture) betRequest(t *testing.T) BuildRequest {
	t.Helper()
	var sessionID [32]byte
	sessionID[0] = 0x5a
	candidate := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x5b}}
	base, err := NewSelectedBaseState(
		sessionID, candidate, f.b1.ID, f.b1.BlockBOC, candidateBlock(t, f.b1), f.b1.State,
	)
	if err != nil {
		t.Fatal(err)
	}

	return BuildRequest{
		Session:     ActivatedSession{Session: Session{Shard: f.shard}},
		Slot:        4,
		Parent:      simplex.Parent(candidate),
		speculative: &speculativeBase{state: base, at: time.Unix(1787464000, 0)},
	}
}

func (f *speculativeLineageFixture) key(t *testing.T, candidate *Candidate) [32]byte {
	t.Helper()
	key, err := blockRootKey(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// TestSpeculativeLineageMakesTheBetsBaseACandidateNode is the gate for the
// mechanism that replaced the seeding ban's casualty. The bet's base and its
// parent are validated-but-unapplied foreign blocks: neither position can be
// pinned, seeding is banned, and before this every bet died in acquisition
// with "is not pinned and may not be seeded here" — 84 of 89 bets on the live
// stand. The lineage walk turns both into branch candidate nodes, and the
// resolution takes the pipelined successor's route: CandidateTip, no pins of
// uncommitted positions, no seed.
func TestSpeculativeLineageMakesTheBetsBaseACandidateNode(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	request := f.betRequest(t)

	chain, err := f.acquisition.resolveChain(context.Background(), f.managed, request)
	if err != nil {
		t.Fatal(err)
	}
	tip := f.key(t, f.b1)
	if chain.candidateTip == nil || *chain.candidateTip != tip {
		t.Fatalf("speculative chain tip = %v, want the bet base %x", chain.candidateTip, tip[:8])
	}
	if len(chain.queueBase) != 1 || !chain.queueBase[0].ID.Equals(&f.anchor.ID) {
		t.Fatalf("speculative queue base = %+v, want the applied anchor %v", chain.queueBase, f.anchor.ID)
	}
	if !f.branch.HasCandidate(tip) || !f.branch.HasCandidate(f.key(t, f.b0)) {
		t.Fatal("the lineage did not install both uncommitted links as candidate nodes")
	}
	if len(f.managed.candidates) != 0 {
		t.Fatal("speculative resolution installed a consensus candidate in the session")
	}

	// The point of the lineage: the cut the build performs — CandidateTip into
	// the branch, seeding refused — now resolves. This is the exact call shape
	// that produced "is not pinned and may not be seeded here".
	if _, err = f.acquisition.cutCommittedViews(
		f.branch,
		f.destination,
		map[msgpool.ShardIdent]*localNeighborView{f.destination: {previous: f.prevB1}},
		chain.queueBase,
		chain.candidateTip,
		false,
		&prewarmHints{},
	); err != nil {
		t.Fatalf("the bet's cut still fails over the installed lineage: %v", err)
	}

	// Idempotent on purpose: the commit of an adopted bet resolves the same
	// chain again and must find, not fight, the nodes the acquisition made.
	again, err := f.acquisition.resolveChain(context.Background(), f.managed, request)
	if err != nil {
		t.Fatalf("re-resolving the same bet is not idempotent: %v", err)
	}
	if again.candidateTip == nil || *again.candidateTip != tip {
		t.Fatal("re-resolution lost the candidate tip")
	}
}

// TestSpeculativeLineageStopsAtAnExistingNode pins the exists shortcut. A
// node already in the branch may have been installed by a different path with
// a different shape — a root node with a Base where the walk would install a
// child with a Parent — and re-adding it would be reported as a conflicting
// candidate, killing a bet whose lineage is in fact fine.
func TestSpeculativeLineageStopsAtAnExistingNode(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	// B0's node pre-exists in root form, the way this node's own earlier
	// commit would have left it.
	refB0, err := localSourceRef(f.b0.ID)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := f.branch.DeltaFromBlockRoot(f.destination, refB0, candidateBlock(t, f.b0), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.branch.AddCandidate(msgpool.CandidateRequest{
		ID:    f.key(t, f.b0),
		Seqno: f.b0.ID.SeqNo,
		Delta: delta,
		Base:  candidateSources([]PreviousBlock{f.anchor}),
	}); err != nil {
		t.Fatal(err)
	}

	chain, err := f.acquisition.resolveChain(context.Background(), f.managed, f.betRequest(t))
	if err != nil {
		t.Fatalf("a lineage over an existing node failed: %v", err)
	}
	if chain.candidateTip == nil || *chain.candidateTip != f.key(t, f.b1) {
		t.Fatal("the bet base was not installed over the existing node")
	}
}

// TestSpeculativeLineageFailsClosedWithoutTheParent: a lineage the node cannot
// resolve — the parent is in no cache and no store — declines the bet with the
// error the producer treats as a clean refusal, instead of hanging or seeding.
func TestSpeculativeLineageFailsClosedWithoutTheParent(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	delete(f.managed.blocks, f.key(t, f.b0))

	_, err := f.acquisition.resolveChain(context.Background(), f.managed, f.betRequest(t))
	if !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("unresolvable lineage returned %v, want ErrAcquisitionNotReady", err)
	}
}

// TestAdoptedBetCommitRecordsTheLineageRoot covers the second half of the bet's
// life: the adopted candidate commits, and its queue parent — the foreign base
// — is a branch-only node that recordCandidateLocked has never seen. The chain
// carries the lineage's committed root for exactly that case; losing it fails
// the commit of a block the window has already adopted.
func TestAdoptedBetCommitRecordsTheLineageRoot(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	request := f.betRequest(t)

	chain, err := f.acquisition.resolveChain(context.Background(), f.managed, request)
	if err != nil {
		t.Fatal(err)
	}

	// The bet's own block: built over the base exactly as the adopted build is.
	req2 := emptyCandidateRequest(t)
	req2.Previous = f.prevB1
	built, err := testBuilder().BuildShard(context.Background(), req2)
	if err != nil {
		t.Fatal(err)
	}
	derivation, err := f.acquisition.deriveCommitLocked(built, chain.previous)
	if err != nil {
		t.Fatal(err)
	}
	artifact := CandidateArtifact{
		SessionID: request.Session.ID,
		Candidate: simplex.Candidate{
			ID:    simplex.CandidateID{Slot: request.Slot, Hash: [32]byte{0x77}},
			Block: cloneBlockID(built.ID),
		},
		BlockBOC: built.BlockBOC,
	}
	var hints prewarmHints
	if err = f.acquisition.recordCandidateLocked(
		context.Background(), f.managed, request, built, artifact, chain, derivation, &hints,
	); err != nil {
		t.Fatalf("the adopted bet did not commit: %v", err)
	}
	state, exists := f.managed.candidates[artifact.Candidate.ID]
	if !exists {
		t.Fatal("the committed bet is absent from the session's candidates")
	}
	if len(state.queueBase) != 1 || !state.queueBase[0].ID.Equals(&f.anchor.ID) {
		t.Fatalf("committed queue base = %+v, want the lineage root %v", state.queueBase, f.anchor.ID)
	}
}

// TestSpeculativeLineageSurvivesTheFrontierAdvancing is the second resolution
// of the same bet — the commit's — after the applied frontier moved past a
// block the first resolution installed as a node. The first pass, rooted at
// the anchor, gave B0 a Base shape and B1 a Parent shape; once B0 itself is
// applied, a re-derivation would insert B1 with a Base shape — same block,
// different lineage form — and AddCandidate would report a conflicting
// candidate, failing the commit of a block the window already adopted. The
// exists shortcut is what makes the second pass find, not fight, the first
// pass's nodes.
func TestSpeculativeLineageSurvivesTheFrontierAdvancing(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	request := f.betRequest(t)

	first, err := f.acquisition.resolveChain(context.Background(), f.managed, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.queueBase) != 1 || !first.queueBase[0].ID.Equals(&f.anchor.ID) {
		t.Fatalf("first resolution rooted at %+v, want the anchor", first.queueBase)
	}

	// The frontier advances honestly: B0's own delta applies to the pool.
	refB0, err := localSourceRef(f.b0.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, startLT, err := shardParentBlockID(f.b0.ID, f.prevB0.Block)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := f.branch.DeltaFromBlockRoot(f.destination, refB0, f.prevB0.Block, startLT)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.pool.Internals().ApplyBlock(f.destination, f.destination, refB0, delta); err != nil {
		t.Fatal(err)
	}

	second, err := f.acquisition.resolveChain(context.Background(), f.managed, request)
	if err != nil {
		t.Fatalf("re-resolution after the frontier advanced failed: %v", err)
	}
	if second.candidateTip == nil || *second.candidateTip != f.key(t, f.b1) {
		t.Fatal("re-resolution lost the candidate tip")
	}
}

// A first slot built speculatively over foreign candidate B1 records its own
// block X as Parent=B1 when the pipelined X+1 acquisition starts. Consensus can
// then promote B1 before X commits; the observed retry resolves X as Base=B1.
// X is byte-for-byte identical because its slot seed is stable, so its block
// root must reuse the already installed Parent-shaped node rather than conflict
// with it or replace it underneath a live descendant.
func TestCandidateCommitReusesParentShapeAfterPredecessorPromotion(t *testing.T) {
	f := newSpeculativeLineageFixture(t)
	request := f.betRequest(t)

	speculative, err := f.acquisition.resolveChain(context.Background(), f.managed, request)
	if err != nil {
		t.Fatal(err)
	}

	build := emptyCandidateRequest(t)
	build.Previous = f.prevB1
	x, err := testBuilder().BuildShard(context.Background(), build)
	if err != nil {
		t.Fatal(err)
	}
	xRoot := candidateBlock(t, x)
	xKey := f.key(t, x)
	_, xStartLT, err := shardParentBlockID(x.ID, xRoot)
	if err != nil {
		t.Fatal(err)
	}
	var hints prewarmHints
	if _, err = f.acquisition.installCandidateDeltaLocked(
		f.managed,
		speculative,
		xRoot,
		x.ID,
		xKey,
		xStartLT,
		false,
		&hints,
	); err != nil {
		t.Fatalf("install X through speculative Parent lineage: %v", err)
	}

	apply := func(previous PreviousBlock) {
		t.Helper()
		ref, applyErr := localSourceRef(previous.ID)
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		_, startLT, applyErr := shardParentBlockID(previous.ID, previous.Block)
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		delta, applyErr := f.branch.DeltaFromBlockRoot(f.destination, ref, previous.Block, startLT)
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		if applyErr = f.pool.Internals().ApplyBlock(f.destination, f.destination, ref, delta); applyErr != nil {
			t.Fatal(applyErr)
		}
	}
	apply(f.prevB0)
	apply(f.prevB1)
	b1Key := f.key(t, f.b1)
	if err = f.branch.Retain(&b1Key); err != nil {
		t.Fatal(err)
	}

	// The retry's consensus-owned chain has no CandidateTip: B1 is now its
	// applied predecessor. Before the fix this second install reached
	// AddCandidate with Base=B1 and failed against X's existing Parent=B1 form.
	observed := localResolvedChain{previous: []PreviousBlock{f.prevB1}}
	derivation, err := f.acquisition.deriveCommitLocked(x, observed.previous)
	if err != nil {
		t.Fatal(err)
	}
	artifact := CandidateArtifact{
		SessionID: request.Session.ID,
		Candidate: simplex.Candidate{
			ID:    simplex.CandidateID{Slot: request.Slot, Hash: [32]byte{0x7a}},
			Block: cloneBlockID(x.ID),
		},
		BlockBOC: x.BlockBOC,
	}
	child := [32]byte{0x7b}
	if err = f.branch.AddCandidate(msgpool.CandidateRequest{
		ID: child, Parent: &xKey, Seqno: x.ID.SeqNo + 1, Delta: &msgpool.InternalsDelta{},
	}); err != nil {
		t.Fatalf("append before reusing X: %v", err)
	}
	completeErr := errors.New("complete reused candidate externals")
	failing := &localAcquisitionFailingMessages{pool: f.pool, completeErr: completeErr}
	f.acquisition.messages = failing
	if err = f.acquisition.recordCandidateLocked(
		context.Background(), f.managed, request, x, artifact, observed, derivation, &hints,
	); !errors.Is(err, completeErr) {
		t.Fatalf("failed reused commit error = %v, want %v", err, completeErr)
	}
	if !f.branch.HasCandidate(xKey) || !f.branch.HasCandidate(child) {
		t.Fatal("failed commit rolled back a candidate node owned by the speculative pipeline")
	}
	if _, exists := f.managed.candidates[artifact.Candidate.ID]; exists {
		t.Fatal("failed reused commit published candidate state")
	}

	f.acquisition.messages = f.pool
	if err = f.acquisition.recordCandidateLocked(
		context.Background(), f.managed, request, x, artifact, observed, derivation, &hints,
	); err != nil {
		t.Fatalf("commit X after B1 promotion: %v", err)
	}
	state, exists := f.managed.candidates[artifact.Candidate.ID]
	if !exists || state.queueTip == nil || *state.queueTip != xKey {
		t.Fatal("the committed retry did not retain X as its queue tip")
	}
	if !f.branch.HasCandidate(child) {
		t.Fatal("successful reuse lost X's live descendant")
	}
}
