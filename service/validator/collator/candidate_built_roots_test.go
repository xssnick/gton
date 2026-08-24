package collator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// Consensus exempts no leader from validating: the node that produced a
// candidate validates it like everyone else, and used to do so by parsing back
// the two BOCs its own collation had just written from trees it still held.
// The emitted artifact carries those trees instead.
//
// What these pin is both halves of that. The producer half — the roots on the
// artifact are the very trees the BOCs were serialized from, not equal copies —
// and the consumer half — the capsule takes them and reaches the byte path's
// verdict, which is the only reason removing the parse is allowed at all.

// builtRootsEmission collates a real full-collated shard block and signs it the
// way a leader window does. The full-collated request is deliberate: with the
// capability off the collated data degenerates to the consensus-extra marker
// and the expensive half of the handoff — the predecessor state proof — would
// not be in it.
func builtRootsEmission(t *testing.T) (ShardRequest, *Candidate, CandidateArtifact) {
	t.Helper()

	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	t.Cleanup(func() { fixture.close(t) })

	session, _ := fixture.session(0x7b, 1, 0, time.Now())
	req := fullCollatedMainnetRequest(t)
	req.Header.GenUtimeMS = uint64(req.Header.GenUtime)*1000 + 321
	req.CreatedBy = session.Validators[0].PublicKey
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate mainnet workload with full collated data: %v", err)
	}
	session.Shard = groups.ShardID{Workchain: candidate.ID.Workchain, Shard: candidate.ID.Shard}

	artifact, err := fixture.service.signArtifact(
		session,
		productionWindow{
			ID:         WindowID{SessionID: session.ID, StartSlot: 0},
			Leader:     0,
			Authority:  CandidateAuthoritySelf,
			SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
		},
		0,
		simplex.Genesis(),
		candidate,
	)
	if err != nil {
		t.Fatalf("sign the built candidate: %v", err)
	}

	return req, candidate, artifact
}

func TestEmittedCandidateHandsItsBuiltRootsToValidation(t *testing.T) {
	req, candidate, artifact := builtRootsEmission(t)
	if milliseconds, known := artifact.GenerationTimeMS(); !known || milliseconds != req.Header.GenUtimeMS {
		t.Fatalf("emitted generation time = %d, %v; want %d, true",
			milliseconds, known, req.Header.GenUtimeMS)
	}

	blockRoot, collatedRoots := artifact.ValidationRoots()
	if blockRoot == nil || len(collatedRoots) == 0 {
		t.Fatal("the emitted artifact carries no parsed roots, so its own validation decodes both BOCs again")
	}
	if blockRoot != candidate.built.root {
		t.Fatal("the emitted block root is not the tree the block BOC was written from")
	}
	if len(collatedRoots) != len(candidate.built.collated) {
		t.Fatalf("the artifact carries %d collated roots, the build wrote %d",
			len(collatedRoots), len(candidate.built.collated))
	}
	for i := range collatedRoots {
		if collatedRoots[i] != candidate.built.collated[i] {
			t.Fatalf("collated root %d is not the tree the collated BOC was written from", i)
		}
	}
	// Below two roots the collated data is the bare consensus-extra marker and
	// the proof set this handoff exists for was never built.
	if len(collatedRoots) < 2 {
		t.Fatalf("the fixture produced %d collated roots, so full collated data was off", len(collatedRoots))
	}

	previous := []PreviousBlock{req.Previous}
	borrowed, err := prepareValidationCandidate(t.Context(), artifact, req.CreatedBy, false, previous)
	if err != nil {
		t.Fatalf("prepare the emitted candidate: %v", err)
	}
	if borrowed.root != blockRoot {
		t.Fatal("validation decoded the block BOC instead of taking the root it was handed")
	}
	if len(borrowed.verified.collated.roots) != len(collatedRoots) {
		t.Fatal("validation changed the collated root set")
	}
	for i := range collatedRoots {
		if borrowed.verified.collated.roots[i] != collatedRoots[i] {
			t.Fatalf("validation decoded collated root %d again", i)
		}
	}
	if err = borrowed.bindConfig(t.Context(), req.Masterchain.Config); err != nil {
		t.Fatalf("bind the emitted candidate to its config: %v", err)
	}

	// The verdict, not the plumbing. The same artifact stripped of its roots is
	// the path every other node takes over these exact bytes; both must end at
	// the same successor over the same predecessor.
	decoded := artifact
	decoded.blockRoot, decoded.collatedRoots = nil, nil
	replayed, err := prepareValidationCandidate(t.Context(), decoded, req.CreatedBy, false, previous)
	if err != nil {
		t.Fatalf("prepare the same candidate from its bytes: %v", err)
	}
	// Without this the comparison below could be one path against itself.
	if replayed.root == blockRoot || replayed.verified.collated.roots[0] == collatedRoots[0] {
		t.Fatal("the byte path did not decode anything, so it is not a control")
	}
	if err = replayed.bindConfig(t.Context(), req.Masterchain.Config); err != nil {
		t.Fatalf("bind the replayed candidate to its config: %v", err)
	}
	for _, check := range []struct {
		what      string
		got, want cell.Hash
	}{
		{"block root", borrowed.root.HashKeyAt(0), replayed.root.HashKeyAt(0)},
		{"state update", borrowed.candidate.StateUpdate.HashKeyAt(0), replayed.candidate.StateUpdate.HashKeyAt(0)},
		{"successor state", borrowed.stateRoot.HashKeyAt(0), replayed.stateRoot.HashKeyAt(0)},
		{"update source", borrowed.sourceRoot, replayed.sourceRoot},
	} {
		if check.got != check.want {
			t.Fatalf("the %s the borrowed roots produced is %x, the byte path produced %x",
				check.what, check.got, check.want)
		}
	}
	if borrowed.substituted != replayed.substituted ||
		borrowed.verified.collated.full != replayed.verified.collated.full {
		t.Fatal("the two paths disagree on how the candidate proves its predecessor")
	}
	// The proof-backed shard path is the one the collated roots feed; a fixture
	// that did not reach it would compare two runs of the cheap path.
	if !borrowed.substituted {
		t.Fatal("the fixture did not exercise the proof-backed shard path")
	}
}

// The window's memory of what it emitted is a retention route, and the roots
// are a handoff to one validation. Keeping them there would pin the block DAG
// and the whole collated proof set for the rest of the window, for a replay
// that parses the bytes anyway.
func TestRememberedCandidateDropsTheBuiltRoots(t *testing.T) {
	_, _, artifact := builtRootsEmission(t)

	if root, collated := artifact.ValidationRoots(); root == nil || collated == nil {
		t.Fatal("the emitted artifact carries no roots, so this test proves nothing")
	}
	managed := &managedCollatorSession{}
	managed.rememberEmitted(artifact.WindowID, artifact)
	recalled, found := managed.recallEmitted(artifact.WindowID, artifact.Candidate.ID.Slot)
	if !found {
		t.Fatal("the emitted artifact was not remembered")
	}
	if root, collated := recalled.ValidationRoots(); root != nil || collated != nil {
		t.Fatal("the remembered artifact pins the block DAG and the collated proof set")
	}
	// The payload capsule is what this map is worth its memory for and must not
	// be collateral damage of the strip above.
	if recalled.Prepared() != artifact.Prepared() || !recalled.Digested() {
		t.Fatal("the remembered artifact lost its prepared payload or its digest provenance")
	}
	wantTime, wantKnown := artifact.GenerationTimeMS()
	gotTime, gotKnown := recalled.GenerationTimeMS()
	if !wantKnown || !gotKnown || gotTime != wantTime {
		t.Fatal("the remembered artifact lost its exact generation time provenance")
	}
}

// runtimeCandidateWithBuiltRoots is a produced candidate carrying what a sealed
// build carries: the roots its two BOCs were written from, plus the provenance
// binding them to the hashes beside them. Service attaches those roots to the
// artifact it emits only for a candidate shaped like this one, so a runtime test
// about their lifetime needs it rather than a bare test candidate.
func runtimeCandidateWithBuiltRoots(request BuildRequest) (*Candidate, error) {
	candidate := runtimeBuiltCandidate(request)
	slot := uint64(request.Slot)
	root := cell.BeginCell().MustStoreUInt(slot, 32).MustStoreUInt(0xb10c, 32).EndCell()
	candidate.ID.RootHash = append([]byte(nil), root.Hash()...)
	candidate.State = cell.BeginCell().MustStoreUInt(slot, 32).MustStoreUInt(0x57a7e, 32).EndCell()
	candidate.StateUpdate = cell.BeginCell().MustStoreUInt(slot, 32).MustStoreUInt(0x11da7e, 32).EndCell()
	candidate.built = newBuiltCandidate(
		candidate.ID,
		root,
		[]*cell.Cell{
			cell.BeginCell().MustStoreUInt(slot, 32).MustStoreUInt(1, 8).EndCell(),
			cell.BeginCell().MustStoreUInt(slot, 32).MustStoreUInt(2, 8).EndCell(),
		},
		candidate.State,
		candidate.StateUpdate,
		0,
		0,
		uint64(request.Slot)*1000+321,
	)
	if candidate.built == nil {
		return nil, errors.New("test candidate carries no build capsule")
	}
	if err := sealBuiltCandidate(candidate); err != nil {
		return nil, err
	}

	return candidate, nil
}

// A leader window is many slots long, and each one hands the next its own
// artifact as BuildRequest.Previous. That pointer outlives the validation the
// roots were lent to — the build it feeds runs for the whole of the next slot,
// and a build future can carry it further still — so the lineage keeps the
// retained form. Nothing downstream reads the roots off Previous: a predecessor
// is resolved from the committed candidate state, never re-parsed from there.
func TestProducerLineageDropsTheBuiltRoots(t *testing.T) {
	var (
		mu      sync.Mutex
		lineage = make(map[uint32]*CandidateArtifact)
		pending = make(map[uint32]*SuccessorOffer)
	)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		mu.Lock()
		lineage[request.Slot] = request.Previous
		pending[request.Slot] = request.PreviousPending
		mu.Unlock()

		return runtimeCandidateWithBuiltRoots(request)
	}
	emitted := make(chan CandidateArtifact, 2)
	fixture := newRuntimeSelfFixture(t, pipeline, nil, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(0x6d, 2, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}

	first := runtimeAwaitArtifact(t, emitted)
	// The emission is the one route the roots exist for, so a fixture whose
	// artifact reaches it without them would prove nothing about the lineage.
	if root, collated := first.ValidationRoots(); root == nil || len(collated) == 0 {
		t.Fatal("the emitted artifact carries no roots, so this test proves nothing")
	}
	second := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != 0 || second.Candidate.ID.Slot != 1 {
		t.Fatalf("emitted slots = %d, %d, want 0, 1", first.Candidate.ID.Slot, second.Candidate.ID.Slot)
	}
	if root, collated := second.ValidationRoots(); root == nil || len(collated) == 0 {
		t.Fatal("the second emission lost the roots its own validation borrows")
	}

	mu.Lock()
	defer mu.Unlock()
	if previous := lineage[0]; previous != nil {
		t.Fatalf("the first slot of the window was handed a predecessor artifact %+v", previous.Candidate.ID)
	}
	previous, built := lineage[1]
	offer, handed := pending[1]
	if !built && !handed {
		t.Fatal("the second slot was built with no predecessor at all, so the lineage is not under test")
	}
	if previous != nil && offer != nil {
		t.Fatal("the second slot was handed both a committed predecessor and a pending one")
	}
	switch {
	case previous != nil:
		if previous.Candidate.ID != first.Candidate.ID {
			t.Fatal("the second slot was chained to a candidate the window did not emit")
		}
		if root, collated := previous.ValidationRoots(); root != nil || collated != nil {
			t.Fatal("the lineage pointer pins the preceding block DAG and its whole collated proof set")
		}
	case offer != nil:
		// The pipelined shape. A successor started before its predecessor is
		// committed cannot resolve that predecessor out of the session, so it is
		// handed the three cells it genuinely reads — and must be handed nothing
		// else. What the artifact path is careful never to pin is exactly what
		// must not appear here either: the serialized block, the collated payload
		// and the broadcast capsule.
		if !offer.ID.Equals(&first.Candidate.Block) {
			t.Fatalf("the pending predecessor is %v, not the block the window emitted %v",
				offer.ID, first.Candidate.Block)
		}
		if offer.State == nil || offer.StateUpdate == nil || offer.Root == nil {
			t.Fatal("the pending predecessor is missing a cell the successor reads")
		}
	}
}
