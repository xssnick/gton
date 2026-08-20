package collator

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

type capturingCandidateTransitionVerifier struct {
	transition CandidateTransition
	calls      int
}

func (v *capturingCandidateTransitionVerifier) VerifyCandidateTransition(
	_ context.Context,
	transition CandidateTransition,
) error {
	v.transition = transition
	v.calls++

	return nil
}

// The capsule is the single parse of one candidate, and the cheapest proof of
// that is cell identity: the state update the successor tree was built from and
// the one the structural pass verifies must be the very same cell. With two
// decodes of the same BOC they carry equal hashes and different pointers, so
// this fails the moment a second parse comes back.
func TestPreparedValidationCandidateIsTheOnlyParse(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	prepared, err := prepareValidationCandidate(
		t.Context(),
		candidateValidationArtifact(candidate),
		req.CreatedBy,
		false,
		[]PreviousBlock{req.Previous},
	)
	if err != nil {
		t.Fatalf("prepare validation candidate: %v", err)
	}
	update, err := prepared.root.PeekRef(2)
	if err != nil {
		t.Fatalf("load the state update of the parsed root: %v", err)
	}
	if prepared.verified.block.StateUpdate != update {
		t.Fatal("the parsed block does not share its cells with the capsule root")
	}
	// The control that makes the identity above evidence rather than a
	// tautology: a second decode of the same bytes yields equal hashes and
	// different cells, which is exactly what the removed parse produced.
	second, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	secondUpdate, err := second.PeekRef(2)
	if err != nil {
		t.Fatal(err)
	}
	if secondUpdate == update || secondUpdate.HashKeyAt(0) != update.HashKeyAt(0) {
		t.Fatal("a second decode of the block is indistinguishable from the first")
	}
	if prepared.candidate.StateUpdate != prepared.verified.block.StateUpdate {
		t.Fatal("the candidate carries a state update from another parse")
	}
	if prepared.candidate.State != nil || prepared.stateRoot != nil {
		t.Fatal("preparation applied the state update before the config-bound stage")
	}
	if len(prepared.resident) != 1 || prepared.resident[0].State != req.Previous.State {
		t.Fatal("the capsule lost the resident predecessor list")
	}

	// Stage 3 is the same capsule, one config later: nothing is re-decoded and
	// every view the structural pass reads is filled in place.
	if err = prepared.bindConfig(t.Context(), req.Masterchain.Config); err != nil {
		t.Fatalf("bind candidate to config: %v", err)
	}
	if prepared.candidate.State != prepared.stateRoot {
		t.Fatal("the candidate carries a state root from another apply")
	}
	if prepared.stateRoot.HashKeyAt(0) != candidate.State.HashKeyAt(0) {
		t.Fatal("the successor state differs from the collated one")
	}
	if prepared.verified.state.OutMsgQueueInfo == nil || prepared.verified.queue.OutQueue == nil ||
		prepared.verified.inMessages == nil || prepared.verified.outMessages == nil ||
		prepared.verified.accountBlocks == nil {
		t.Fatal("bindConfig left the candidate views unbound")
	}
	if prepared.verified.block.StateUpdate != update {
		t.Fatal("bindConfig replaced the block parse")
	}
	if err = verifyPreparedShardCandidate(t.Context(), shardVerificationRequest(req, prepared.candidate), &prepared.verified); err != nil {
		t.Fatalf("verify the candidate the capsule prepared: %v", err)
	}
}

func TestPreparedValidationCandidateUsesBorrowedNetworkRoots(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	artifact := candidateValidationArtifact(candidate)
	artifact.digested = true
	artifact.blockRoot, err = cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	artifact.collatedRoots, err = cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareValidationCandidate(
		t.Context(),
		artifact,
		req.CreatedBy,
		false,
		[]PreviousBlock{req.Previous},
	)
	if err != nil {
		t.Fatalf("prepare candidate from borrowed roots: %v", err)
	}
	if prepared.root != artifact.blockRoot {
		t.Fatal("validation decoded another block root")
	}
	if len(prepared.verified.collated.roots) != len(artifact.collatedRoots) {
		t.Fatal("validation changed the collated root set")
	}
	for i := range artifact.collatedRoots {
		if prepared.verified.collated.roots[i] != artifact.collatedRoots[i] {
			t.Fatalf("validation decoded collated root %d again", i)
		}
	}

	incomplete := artifact
	incomplete.collatedRoots = nil
	if _, err = prepareValidationCandidate(
		t.Context(), incomplete, req.CreatedBy, false, []PreviousBlock{req.Previous},
	); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "parsed candidate payload is incomplete") {
		t.Fatalf("incomplete parsed payload error = %v", err)
	}

	unbound := artifact
	unbound.digested = false
	if _, err = prepareValidationCandidate(
		t.Context(), unbound, req.CreatedBy, false, []PreviousBlock{req.Previous},
	); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "no digest provenance") {
		t.Fatalf("unbound parsed payload error = %v", err)
	}

	wrong := artifact
	wrong.blockRoot = cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	if _, err = prepareValidationCandidate(
		t.Context(), wrong, req.CreatedBy, false, []PreviousBlock{req.Previous},
	); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "root hash mismatch") {
		t.Fatalf("foreign parsed block error = %v", err)
	}
}

func BenchmarkPrepareValidationCandidatePayload(b *testing.B) {
	req := benchMainnetCollatedRequest(b, 1)
	candidate, err := testBuilder().BuildShard(b.Context(), req)
	if err != nil {
		b.Fatal(err)
	}

	decoded := candidateValidationArtifact(candidate)
	decoded.digested = true
	prepared := decoded
	prepared.blockRoot, err = cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		b.Fatal(err)
	}
	prepared.collatedRoots, err = cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		b.Fatal(err)
	}
	previous := []PreviousBlock{req.Previous}

	for _, benchmark := range []struct {
		name     string
		artifact CandidateArtifact
	}{
		{name: "decode_boc", artifact: decoded},
		{name: "borrowed_roots", artifact: prepared},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, prepareErr := prepareValidationCandidate(
					b.Context(), benchmark.artifact, req.CreatedBy, false, previous,
				); prepareErr != nil {
					b.Fatal(prepareErr)
				}
			}
		})
	}
}

// The same property end to end: the block the semantic verifier is handed and
// the state root the caller gets back are the ones the capsule produced, not a
// second decode made on the way.
func TestValidateCandidateVerifiesTheParseItRestored(t *testing.T) {
	fixture, shardState := localValidationMasterFixture(t)
	session := localMasterTestSession(t, fixture.request.Groups)
	fixture.request.CreatedBy = session.Validators[0].PublicKey
	candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}

	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	semantics := &capturingCandidateTransitionVerifier{}
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder: testBuilder(),
		Store: &localValidationStore{states: []localValidationState{
			{block: fixture.request.Previous.ID, root: fixture.request.Previous.State},
			{block: fixture.oldShard, root: shardState},
		}},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: semantics,
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := acquisition.ValidateCandidate(t.Context(), ValidationRequest{
		Session:  session,
		Update:   update,
		Previous: []PreviousBlock{fixture.request.Previous},
		Candidate: simplex.Candidate{
			Leader:           0,
			Block:            candidate.ID,
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	})
	if err != nil {
		t.Fatalf("validate master candidate: %v", err)
	}
	if semantics.calls != 1 || semantics.transition.prepared == nil {
		t.Fatalf("semantic verifier ran %d times with prepared=%v", semantics.calls, semantics.transition.prepared)
	}
	verified := semantics.transition.prepared.candidate
	if verified.block.StateUpdate != semantics.transition.Candidate.StateUpdate {
		t.Fatal("the verified block and the validated candidate come from different parses")
	}
	if result.Successor.StateUpdate != verified.block.StateUpdate {
		t.Fatal("the transition the caller was given comes from another parse")
	}
	if result.Successor.StateHash != semantics.transition.Candidate.State.HashKeyAt(0) {
		t.Fatal("the reported successor hash is not the hash of the state the verifier read")
	}
	if !bytes.Equal(result.Successor.BlockRoot.Hash(), candidate.ID.RootHash) {
		t.Fatal("the transition names a block that is not the candidate")
	}
}

// Everything the capsule checks in stage 1 is a function of the candidate bytes
// and its consensus-fixed id, so it must reject before the masterchain view is
// resolved: a node too far behind to build that view still votes a forged
// candidate down instead of abstaining. The creator is deliberately not part of
// it — it is measured against a roster this node has not authenticated yet, and
// TestValidationAbstainsWhenTheCreatorRosterIsNotAuthenticated pins that.
func TestValidationRejectsAForgedCandidateWithoutAMasterView(t *testing.T) {
	fixture, _ := localValidationMasterFixture(t)
	session := localMasterTestSession(t, fixture.request.Groups)
	fixture.request.CreatedBy = session.Validators[0].PublicKey
	candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}

	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	// No state in the store, so the masterchain view cannot be resolved at all.
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := ValidationRequest{
		Session:  session,
		Update:   update,
		Previous: []PreviousBlock{fixture.request.Previous},
		Candidate: simplex.Candidate{
			Leader:           0,
			Block:            candidate.ID,
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	}

	// The control: the same candidate, untouched, cannot get past the master
	// view here. Without it the assertion below would pass on any error.
	if _, err = acquisition.ValidateCandidate(t.Context(), request); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("validation without a resolvable master view = %v, want ErrAcquisitionNotReady", err)
	}

	forged := request
	forged.Candidate.Block = cloneBlockID(candidate.ID)
	forged.Candidate.Block.FileHash[0] ^= 1
	_, err = acquisition.ValidateCandidate(t.Context(), forged)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "file hash mismatch") {
		t.Fatalf("forged file hash error = %v, want an invalid-input file hash mismatch", err)
	}

	// A malformed creator field is still stage 1: it is a property of the block
	// alone and needs no roster to judge.
	if len(session.Validators) < 2 {
		t.Fatal("fixture roster has a single validator")
	}
}

// The creator a candidate must carry is named by the session roster, and that
// roster is LOCAL state until validationMasterView authenticates it against the
// masterchain group snapshot. Comparing the block's creator against it before
// that authentication lets a locally stale or corrupted roster turn an honest
// candidate into a REJECTION — a vote against a block the rest of the network
// will finalize — where the same node with the same view has to abstain. The
// flip has to be impossible in that direction: with an unresolvable masterchain
// view the verdict must be the abstain, whatever this node believes the roster
// to be.
func TestValidationAbstainsWhenTheCreatorRosterIsNotAuthenticated(t *testing.T) {
	fixture, _ := localValidationMasterFixture(t)
	session := localMasterTestSession(t, fixture.request.Groups)
	fixture.request.CreatedBy = session.Validators[0].PublicKey
	candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	// An empty store, so the masterchain view cannot be resolved and the roster
	// is never checked against the group snapshot during this call.
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := ValidationRequest{
		Session:  session,
		Update:   update,
		Previous: []PreviousBlock{fixture.request.Previous},
		Candidate: simplex.Candidate{
			Leader:           0,
			Block:            candidate.ID,
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	}
	// The control: with the roster this node happens to hold being the right
	// one, the honest candidate is unjudged rather than rejected.
	if _, err = acquisition.ValidateCandidate(t.Context(), request); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("honest candidate without a master view = %v, want ErrAcquisitionNotReady", err)
	}

	// Now corrupt only LOCAL state — the roster this node believes in — and
	// leave the candidate byte for byte the honest one.
	stale := request
	stale.Session.Validators = append([]SessionValidator(nil), session.Validators...)
	stale.Session.Validators[0].PublicKey[0] ^= 1
	if _, err = acquisition.ValidateCandidate(t.Context(), stale); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("honest candidate against an unauthenticated roster = %v, want ErrAcquisitionNotReady", err)
	}

	// And the check is not merely gone: once the view resolves, the roster is
	// authenticated and the creator is bound to it. A candidate naming another
	// leader of the same authenticated roster is still rejected.
	resolvable, resolvableRequest, _ := advSessionValidation(t)
	wrongCreator := resolvableRequest
	if len(wrongCreator.Session.Validators) < 2 {
		t.Fatal("fixture roster has a single validator")
	}
	wrongCreator.Candidate.Leader = 1
	_, err = resolvable.ValidateCandidate(t.Context(), wrongCreator)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "creator differs") {
		t.Fatalf("wrong creator with an authenticated roster = %v, want an invalid-input creator mismatch", err)
	}
}

// A node that is behind suspends one validation task while exact inputs arrive.
// Nothing may be announced before that suspension point: it would start an
// apply while the task still cannot know it will reach semantic replay. The
// masterchain path, whose successor is handed back instead, must never announce
// at all.
func TestValidationAnnouncesNothingBeforeItCanSucceed(t *testing.T) {
	fixture, _ := localValidationMasterFixture(t)
	session := localMasterTestSession(t, fixture.request.Groups)
	fixture.request.CreatedBy = session.Validators[0].PublicKey
	candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	// An empty store: the masterchain view cannot be resolved, which is exactly
	// the shape of a node lagging behind the candidate it is asked to judge.
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	announced := 0
	request := ValidationRequest{
		Session:  session,
		Update:   update,
		Previous: []PreviousBlock{fixture.request.Previous},
		Candidate: simplex.Candidate{
			Leader:           0,
			Block:            candidate.ID,
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:           candidate.BlockBOC,
		CollatedData:       candidate.CollatedData,
		AnnounceTransition: func(*cell.PreparedMerkleUpdate) { announced++ },
	}
	if _, err = acquisition.ValidateCandidate(t.Context(), request); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("validation = %v, want ErrAcquisitionNotReady", err)
	}
	if announced != 0 {
		t.Fatalf("a candidate that never got past the master view announced %d applies", announced)
	}

	// And with the view resolved and the candidate accepted, the masterchain
	// path still announces nothing: it hands its successor back instead.
	resolvable, resolvableRequest, _ := advSessionValidation(t)
	resolvableRequest.AnnounceTransition = func(*cell.PreparedMerkleUpdate) { announced++ }
	result, err := resolvable.ValidateCandidate(t.Context(), resolvableRequest)
	if err != nil {
		t.Fatalf("validate a masterchain candidate: %v", err)
	}
	if announced != 0 {
		t.Fatalf("the masterchain path announced %d applies", announced)
	}
	parent := resolvableRequest.Previous[0].State
	carried, ok := result.Successor.Live.Over(parent, parent)
	if !ok || carried.HashKeyAt(0) != result.Successor.StateHash {
		t.Fatal("the masterchain path did not hand back the successor it built")
	}
}

// advSessionValidation builds an acquisition whose masterchain view actually
// resolves, so a verdict observed through it is the one the session path
// produces with the roster already authenticated rather than an abstain.
func advSessionValidation(t *testing.T) (*LocalAcquisition, ValidationRequest, *Candidate) {
	t.Helper()

	fixture, shardState := localValidationMasterFixture(t)
	session := localMasterTestSession(t, fixture.request.Groups)
	fixture.request.CreatedBy = session.Validators[0].PublicKey
	candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	update := localValidationSessionUpdate(t, session.Session, fixture.request.Groups, fixture.request.Previous.ID)
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder: testBuilder(),
		Store: &localValidationStore{states: []localValidationState{
			{block: fixture.request.Previous.ID, root: fixture.request.Previous.State},
			{block: fixture.oldShard, root: shardState},
		}},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}

	return acquisition, ValidationRequest{
		Session:  session,
		Update:   update,
		Previous: []PreviousBlock{fixture.request.Previous},
		Candidate: simplex.Candidate{
			Leader:           0,
			Block:            candidate.ID,
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	}, candidate
}

// The acquisition pipeline verifies the masterchain predecessor while it
// resolves the view that predecessor defines, and hands the result to
// verification instead of having it repeat the call. The reuse must be exactly
// that — the state of this predecessor — which is what the foreign-state case
// pins.
func TestMasterVerificationReusesTheAcquiredPredecessorState(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	request := MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}
	if err = VerifyMasterCandidate(t.Context(), request); err != nil {
		t.Fatalf("verify baseline candidate: %v", err)
	}

	acquired, err := verifyPredecessor("master", &request.Previous)
	if err != nil {
		t.Fatalf("verify the predecessor the acquisition resolves: %v", err)
	}
	reused := request
	reused.previousState = &acquiredPredecessorState{root: request.Previous.State, state: acquired}
	if err = VerifyMasterCandidate(t.Context(), reused); err != nil {
		t.Fatalf("verify with the already verified predecessor: %v", err)
	}

	foreign := acquired
	foreign.GlobalID++
	changed := request
	changed.previousState = &acquiredPredecessorState{root: request.Previous.State, state: foreign}
	err = VerifyMasterCandidate(t.Context(), changed)
	if err == nil || !strings.Contains(err.Error(), "global id differs from predecessor state") {
		t.Fatalf("foreign predecessor state error = %v, want the supplied state to be the one used", err)
	}

	// The assertion a field comparison could not make. A state routed in from
	// another shard or another fork at the same sequence number agrees on every
	// field the old check compared, so what has to be compared is the tree it
	// was parsed from — the very cell the rest of verification runs against.
	// Re-parsing this same tree produces a value equal in all those fields and a
	// different root object, which is the closest a fixture gets to a same-seqno
	// fork without building one, and it has to be refused.
	var reparsed tlb.ShardStateUnsplit
	if err = parseExact(&reparsed, request.Previous.State.ToBuilder().EndCell()); err != nil {
		t.Fatal(err)
	}
	elsewhere := request
	elsewhere.previousState = &acquiredPredecessorState{
		root:  request.Previous.State.ToBuilder().EndCell(),
		state: reparsed,
	}
	err = VerifyMasterCandidate(t.Context(), elsewhere)
	if err == nil || !strings.Contains(err.Error(), "supplied master predecessor state differs from its block id") {
		t.Fatalf("state of another tree error = %v, want the plumbing assertion", err)
	}
}

// The hot path must not read more from the store than it did before. This
// measures it with the probe harness rather than asserting it: the whole
// validation of one candidate over a store-shaped (lazy) predecessor is
// counted, then the two pieces this change removed are counted on the same
// tree. The block decode that no longer happens reads nothing — so dropping it
// cannot have moved the count — while the duplicate predecessor verification
// the masterchain path no longer runs does read, so that path strictly
// improves.
func TestPreparedValidationCandidateLazyLoadBudget(t *testing.T) {
	req, candidate := lazyBudgetProbeCandidate(t)

	lazified := newAdvLazifier()
	lazyReq := req
	lazyReq.Previous.State = lazified.root(t, req.Previous.State)

	if _, err := runLazyBudgetValidation(t, lazyReq, candidate, nil, nil, nil); err != nil {
		t.Fatalf("validate the candidate over a store-shaped predecessor: %v", err)
	}
	validation, distinct := advLazyLoads(lazified)
	logAdvLazySites(t, lazified)

	// The decode the capsule removed: it walks the candidate BOC, which is in
	// memory, and never reaches the predecessor tree.
	if _, _, err := parseCandidateBlock(candidate.ID, candidate.BlockBOC); err != nil {
		t.Fatalf("parse the candidate the removed decode used to parse: %v", err)
	}
	afterDecode, _ := advLazyLoads(lazified)
	if afterDecode != validation {
		t.Fatalf("the removed block decode read %d cells from the store", afterDecode-validation)
	}

	// The duplicate the masterchain path removed: this one does read.
	duplicate := newAdvLazifier()
	previous := req.Previous
	previous.State = duplicate.root(t, req.Previous.State)
	if _, err := verifyPredecessor("master", &previous); err != nil {
		t.Fatalf("verify a store-shaped predecessor: %v", err)
	}
	predecessorLoads, _ := advLazyLoads(duplicate)
	if predecessorLoads == 0 {
		t.Fatal("the predecessor fixture is not store-shaped: verifying it read nothing")
	}
	t.Logf(
		"validation over a lazy predecessor: %d loads (%d distinct cells); removed block decode: 0; removed duplicate predecessor verification: %d loads",
		validation, distinct, predecessorLoads,
	)
}

func advLazyLoads(l *advLazifier) (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.calls, len(l.seen)
}

func logAdvLazySites(tb testing.TB, l *advLazifier) {
	tb.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()

	sites := make([]string, 0, len(l.allSites))
	for site := range l.allSites {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	for _, site := range sites {
		tb.Logf("%-70s first %4d calls %4d warm %4d", site, l.firstSite[site], l.allSites[site], l.warmFromOther[site])
	}
}

func candidateValidationArtifact(candidate *Candidate) CandidateArtifact {
	return CandidateArtifact{
		Candidate: simplex.Candidate{
			Block:            cloneBlockID(candidate.ID),
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	}
}

// prunedWithin reports whether a pruned boundary occurs within the first limit
// cells of a breadth-first walk. It is bounded on purpose: the trees it is
// asked about are mainnet states, and the question is only whether this one is
// a proof or a materialized tree.
func prunedWithin(root *cell.Cell, limit int) bool {
	queue := []*cell.Cell{root}
	seen := map[cell.Hash]struct{}{}
	for len(queue) > 0 && len(seen) < limit {
		current := queue[0]
		queue = queue[1:]
		if _, ok := seen[current.HashKey()]; ok {
			continue
		}
		seen[current.HashKey()] = struct{}{}
		if current.GetType() == cell.PrunedCellType {
			return true
		}
		for i := 0; i < int(current.RefsNum()); i++ {
			ref, err := current.PeekRef(i)
			if err != nil {
				return false
			}
			queue = append(queue, ref)
		}
	}

	return false
}

// Handing the caller a transition instead of a state root must not change what
// the verifier itself runs on. With full collated data that root is
// proof-backed by design — the reference validator rejects a candidate whose
// proof is narrower than the reads it needs, and reading around the proof from
// a resident tree is how a node ends up accepting a block the network will not.
// Widening it here would be the worst possible regression of this change, so it
// is pinned twice over: the resident tree is narrowed to nothing, and the
// structural views are asserted to be parses of the proof-backed successor by
// cell identity rather than by hash.
func TestValidatedSuccessorLeavesVerificationOnTheProofBackedRoot(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate mainnet candidate: %v", err)
	}
	assertBenchMainnetCollated(t, candidate)

	narrowed := req.Previous
	narrowed.State = narrowedStateRoot(t, req.Previous.State)
	prepared, err := prepareValidationCandidate(
		t.Context(),
		candidateValidationArtifact(candidate),
		req.CreatedBy,
		false,
		[]PreviousBlock{narrowed},
	)
	if err != nil {
		t.Fatalf("prepare a candidate whose resident predecessor holds nothing: %v", err)
	}
	if !prepared.verified.collated.full {
		t.Fatal("fixture candidate does not carry full collated data")
	}
	if prepared.previous[0].State == narrowed.State {
		t.Fatal("the successor was built on the resident tree after all")
	}
	if prepared.stateRoot != nil || prepared.candidate.State != nil {
		t.Fatal("preparation applied the state update before the config and size limits were known")
	}

	// The successor really is a proof: applying the same update to the full
	// resident state gives an equal hash and a materialized tree, which is the
	// tree this must NOT be.
	resident, err := cell.ApplyMerkleUpdate(req.Previous.State, prepared.verified.block.StateUpdate)
	if err != nil {
		t.Fatalf("apply the same transition to the resident predecessor: %v", err)
	}
	if err = prepared.bindConfig(t.Context(), req.Masterchain.Config); err != nil {
		t.Fatalf("bind the candidate to its config: %v", err)
	}
	if resident.HashKeyAt(0) != prepared.stateRoot.HashKeyAt(0) {
		t.Fatal("the two successors differ by hash, so cell identity below proves nothing")
	}
	if !prunedWithin(prepared.stateRoot, 4096) {
		t.Fatal("the successor the verifier runs on carries no pruned boundary at all")
	}
	if prunedWithin(resident, 4096) {
		t.Fatal("the resident successor is pruned too, so the previous assertion is not evidence")
	}

	// Every structural view is a parse of that exact root after the config is
	// bound and the deferred update is applied.
	queueInfo, err := prepared.stateRoot.PeekRef(0)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.verified.state.OutMsgQueueInfo != queueInfo {
		t.Fatal("the parsed state does not come from the proof-backed successor")
	}
	if prepared.candidate.State != prepared.stateRoot {
		t.Fatal("the verified candidate carries another state root")
	}

	verification := shardVerificationRequest(req, prepared.candidate)
	verification.Previous = narrowed
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	verification.stateProven = prepared.verified.collated.full
	if err = verifyPreparedShardCandidate(t.Context(), verification, &prepared.verified); err != nil {
		t.Fatalf("verify on the proof-backed successor with nothing resident: %v", err)
	}

	// What the caller is handed carries none of it: the block root, the update
	// and a hash. The state tree the verifier used stays inside this package.
	successor := ValidatedSuccessor{
		BlockRoot:   prepared.root,
		StateUpdate: prepared.verified.block.StateUpdate,
		StateHash:   prepared.stateRoot.HashKeyAt(0),
	}
	if successor.StateHash != candidate.State.HashKeyAt(0) {
		t.Fatal("the reported successor hash is not the state the candidate asserts")
	}
	if successor.BlockRoot != prepared.root || successor.StateUpdate != prepared.verified.block.StateUpdate {
		t.Fatal("the transition names cells from another parse")
	}
}

// lazyBudgetProbeCandidate is the fixture both I/O probes share: a predecessor
// with real account cells, so a store-shaped tree built from it has something
// to hand out on demand, and a candidate that actually executes against that
// account.
func lazyBudgetProbeCandidate(t *testing.T) (ShardRequest, *Candidate) {
	t.Helper()

	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	account := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
	req.Previous.State = stateWithAccounts(
		t,
		req.Previous.State,
		accountsWithActiveContract(t, account, req.Header.GenUtime, 100_000_000_000),
	)
	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: account,
		Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, message)}
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	return req, candidate
}

// runLazyBudgetValidation is one whole candidate validation, in the same order
// LocalAcquisition.ValidateCandidate runs it, with the announcement hook wired
// through exactly as the request carries it. semantics replaces the stub when a
// probe needs the replay to actually execute, which is where most of a real
// validation's reads come from.
func runLazyBudgetValidation(
	t *testing.T,
	req ShardRequest,
	candidate *Candidate,
	announce TransitionAnnouncer,
	semantics CandidateTransitionVerifier,
	tweak func(*ShardVerificationRequest),
) (*preparedValidationCandidate, error) {
	t.Helper()

	prepared, err := prepareValidationCandidate(
		t.Context(),
		candidateValidationArtifact(candidate),
		req.CreatedBy,
		false,
		[]PreviousBlock{req.Previous},
	)
	if err != nil {
		return nil, err
	}
	if err = prepared.bindConfig(t.Context(), req.Masterchain.Config); err != nil {
		return nil, err
	}
	verification := shardVerificationRequest(req, prepared.candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	if semantics != nil {
		verification.Semantics = semantics
	}
	if tweak != nil {
		tweak(&verification)
	}
	// Where validateShardCandidate announces: after everything a lagging node
	// retries on, before the replay, and only for a proof-backed run.
	if announce != nil && prepared.substituted {
		announce(prepared.verified.stateUpdate)
	}
	if err = verifyPreparedShardCandidate(t.Context(), verification, &prepared.verified); err != nil {
		return nil, err
	}

	return prepared, nil
}

type probeApplyResult struct {
	root *cell.Cell
	err  error
}

// probeLiveSuccessorApply is the validator package's announced apply, standing
// in for it here because the probe harness attributes a load to the innermost
// named function of this package on the stack. It is deliberately a package
// function rather than a closure: a closure is reported as collator.<closure>
// and would be indistinguishable from the collator's own.
func probeLiveSuccessorApply(from *cell.Cell, update *cell.PreparedMerkleUpdate, done chan<- probeApplyResult) {
	root, err := update.ApplyTo(from)
	done <- probeApplyResult{root: root, err: err}
}

// probeJoinedSuccessorApply is the same apply performed where the validation
// call ends, which is what happens when no hook is supplied.
func probeJoinedSuccessorApply(from *cell.Cell, update *cell.PreparedMerkleUpdate) (*cell.Cell, error) {
	return update.ApplyTo(from)
}

func advLazySiteLoads(l *advLazifier, function string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	total := 0
	for site, calls := range l.allSites {
		if strings.HasSuffix(site, function) {
			total += calls
		}
	}

	return total
}

// The live-successor invariant must not cost a second walk over the parent
// where the verifier already walked that very parent. This is that path — a
// shard candidate without full collated data, which is also the whole of the
// masterchain path — and what it measures is that the call reads exactly what
// it read before the wave, that nothing is announced, and what the duplicate
// apply the carry-back removes would have cost on the same tree.
//
// The harness attributes every lazy load to the innermost function of this
// package on the stack, which is what lets the removed apply be priced
// separately from the validation that precedes it.
func TestLiveSuccessorApplyLazyLoadBudget(t *testing.T) {
	// Runs in the shared-fixture parallel batch: it reads the cached mainnet
	// workload and keeps every mutation on its own copy, holds no package-level
	// counter and derives nothing from wall-clock timing.
	t.Parallel()
	// The mainnet fixture, because it is the only one where the question is
	// meaningful: most of its accounts are untouched, so the update prunes onto
	// them and an apply genuinely has to walk the parent. A small synthetic
	// block rebuilds its whole state, leaves nothing to prune onto, and its
	// apply reads zero cells however lazy the parent is.
	req, _ := benchMainnetRequest(t, benchMainnetFiller)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate the mainnet fixture: %v", err)
	}

	// (1) The whole call over a store-shaped parent, with the hook wired exactly
	// as the backend wires it.
	lazified := newAdvLazifier()
	lazyReq := req
	lazyReq.Previous.State = lazified.root(t, req.Previous.State)
	announced := 0
	prepared, err := runLazyBudgetValidation(
		t,
		lazyReq,
		candidate,
		func(*cell.PreparedMerkleUpdate) { announced++ },
		NewSemanticVerifier(tvm.NewTVM()),
		nil,
	)
	if err != nil {
		t.Fatalf("validate over a store-shaped predecessor: %v", err)
	}
	if prepared.substituted {
		t.Fatal("the fixture is proof-backed, so it measures the other path")
	}
	if announced != 0 {
		t.Fatalf("the caller was asked to apply the transition %d times on a path that needs no apply", announced)
	}
	baseLoads, baseDistinct := advLazyLoads(lazified)
	if baseLoads == 0 {
		t.Fatal("the fixture predecessor is not store-shaped: validation read nothing")
	}

	// The successor the caller receives is the tree this call built, released
	// against the very cells the caller handed in.
	live := prepared.liveSuccessor()
	carried, ok := live.Over(lazyReq.Previous.State, lazyReq.Previous.State)
	if !ok || carried != prepared.stateRoot {
		t.Fatal("the successor built on the caller's own parent was not offered back")
	}
	// And for nothing else. req.Previous.State is the same tree by hash and a
	// different materialization of it, which is exactly the substitution the
	// pointer comparison exists to refuse.
	if req.Previous.State.HashKeyAt(0) != lazyReq.Previous.State.HashKeyAt(0) {
		t.Fatal("the two parent shapes differ by hash, so the check below proves nothing")
	}
	if _, opened := live.Over(req.Previous.State, req.Previous.State); opened {
		t.Fatal("the successor opened against a parent it was not built on")
	}
	if _, opened := live.Over(lazyReq.Previous.State); opened {
		t.Fatal("the successor opened with no parent presented")
	}

	// (2) The apply the carry-back removes, on the same tree: this is what the
	// caller would pay to recompute a root it has just been handed.
	//
	// The placeholder is immutable and does not memoize what its loader returned,
	// so this second walk reaches the store again. That is precisely the I/O the
	// live successor removes in production; the explicit duplicate below prices
	// it and must not be mistaken for part of the real path.
	duplicate, err := probeJoinedSuccessorApply(
		lazyReq.Previous.State,
		prepared.verified.stateUpdate,
	)
	if err != nil {
		t.Fatalf("apply the transition to the full parent at the join: %v", err)
	}
	if duplicate.HashKeyAt(0) != prepared.stateRoot.HashKeyAt(0) {
		t.Fatal("the removed apply produced another successor, so it was not a duplicate")
	}
	repeatLoads := advLazySiteLoads(lazified, "collator.probeJoinedSuccessorApply")
	total, distinct := advLazyLoads(lazified)
	logAdvLazySites(t, lazified)
	if repeatLoads == 0 {
		t.Fatal("the removed second walk read nothing, so it does not exercise the duplicate I/O")
	}
	if total != baseLoads+repeatLoads || distinct != baseDistinct {
		t.Fatalf("the second walk moved the count from %d loads over %d cells to %d over %d; duplicate site=%d",
			baseLoads, baseDistinct, total, distinct, repeatLoads)
	}

	// The same apply where it is the first walk rather than the second: a parent
	// materialized out of the same records that nothing has descended yet. This
	// is the number that prices the removed work, and it has to stay above zero
	// for there to be a question at all.
	cold := newAdvLazifier()
	coldRoot, err := probeJoinedSuccessorApply(
		cold.root(t, req.Previous.State),
		prepared.verified.stateUpdate,
	)
	if err != nil {
		t.Fatalf("apply the transition to a parent nothing has descended: %v", err)
	}
	if coldRoot.HashKeyAt(0) != prepared.stateRoot.HashKeyAt(0) {
		t.Fatal("the apply over an undescended parent produced another successor")
	}
	duplicateLoads, _ := advLazyLoads(cold)
	if duplicateLoads == 0 {
		t.Fatal("the removed apply read nothing, so there is no I/O question to answer here")
	}

	// (3) The other parent shape, and the common one: a speculative parent is
	// the result of this node's own previous apply and is resident. The lazifier
	// is armed with the same records and handed the resident tree, so a load
	// here would mean validation reached the store through something other than
	// the parent it was given.
	resident := newAdvLazifier()
	resident.index(t, req.Previous.State)
	if _, err = runLazyBudgetValidation(
		t, req, candidate, nil, NewSemanticVerifier(tvm.NewTVM()), nil,
	); err != nil {
		t.Fatalf("validate with a resident full parent: %v", err)
	}
	if residentLoads, _ := advLazyLoads(resident); residentLoads != 0 {
		t.Fatalf("a resident parent still read %d cells from the store", residentLoads)
	}

	t.Logf(
		"non-proof-backed, store-shaped parent: whole call %d loads over %d cells, nothing announced; "+
			"the second apply this removes adds 0 loads on the tree already walked (%d total) and "+
			"%d on a parent nothing has descended. resident parent: 0 loads.",
		baseLoads, baseDistinct, total, duplicateLoads,
	)
}

// The same measurement on the path the invariant exists for. With full collated
// data the collator applies the update to the candidate's own proof and never
// touches the parent this node holds, so the second apply is not a duplicate
// there — it is the only walk over the live parent in the whole call, and its
// cost is exactly what the announcement moves off the critical path.
func TestLiveSuccessorApplyLazyLoadBudgetOnProofBackedValidation(t *testing.T) {
	// Runs in the shared-fixture parallel batch: it reads the cached mainnet
	// workload and keeps every mutation on its own copy, holds no package-level
	// counter and derives nothing from wall-clock timing.
	t.Parallel()
	req := benchMainnetCollatedRequest(t, 1)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate the mainnet fixture: %v", err)
	}
	assertBenchMainnetCollated(t, candidate)

	proven := func(request *ShardVerificationRequest) {
		request.Neighbors = collatedNeighborQueues(t, req, candidate)
		request.stateProven = true
	}

	// Today: validation reads the candidate's proofs, so a store-shaped parent
	// is never consulted at all.
	baseline := newAdvLazifier()
	baseReq := req
	baseReq.Previous.State = baseline.root(t, req.Previous.State)
	if _, err = runLazyBudgetValidation(
		t, baseReq, candidate, nil, NewSemanticVerifier(tvm.NewTVM()), proven,
	); err != nil {
		t.Fatalf("validate a proof-backed candidate: %v", err)
	}
	baseLoads, _ := advLazyLoads(baseline)

	announced := newAdvLazifier()
	announcedReq := req
	announcedReq.Previous.State = announced.root(t, req.Previous.State)
	done := make(chan probeApplyResult, 1)
	announcements := 0
	prepared, err := runLazyBudgetValidation(
		t,
		announcedReq,
		candidate,
		func(update *cell.PreparedMerkleUpdate) {
			announcements++
			go probeLiveSuccessorApply(announcedReq.Previous.State, update, done)
		},
		NewSemanticVerifier(tvm.NewTVM()),
		proven,
	)
	if err != nil {
		t.Fatalf("validate a proof-backed candidate with an announced transition: %v", err)
	}
	if announcements != 1 {
		t.Fatalf("the transition was announced %d times, want exactly once", announcements)
	}
	// Nothing is offered back on this path: the root the verifier holds is the
	// candidate's own proof, and it opens for no parent at all.
	if _, opened := prepared.liveSuccessor().Over(
		announcedReq.Previous.State,
		announcedReq.Previous.State,
	); opened {
		t.Fatal("a proof-backed run offered its narrow root back to the caller")
	}
	result := <-done
	if result.err != nil {
		t.Fatalf("announced apply over the full parent: %v", result.err)
	}
	// The two roots share a hash and nothing else: one is the proof the
	// verifier ran on, the other is the live successor of this node's lineage.
	if result.root.HashKeyAt(0) != prepared.stateRoot.HashKeyAt(0) {
		t.Fatal("the live successor is not the state the candidate asserts")
	}
	if result.root == prepared.stateRoot {
		t.Fatal("the live apply returned the proof-backed root itself")
	}
	total, _ := advLazyLoads(announced)
	logAdvLazySites(t, announced)
	asyncLoads := advLazySiteLoads(announced, "collator.probeLiveSuccessorApply")
	if critical := total - asyncLoads; critical != baseLoads {
		t.Fatalf("the critical path read %d cells, against %d before the change", critical, baseLoads)
	}
	if asyncLoads == 0 {
		t.Fatal("the live apply read nothing from a store-shaped parent")
	}

	// The other parent shape, and the common one: a speculative parent is the
	// result of this node's own previous apply and is resident, so the one apply
	// this path does need reads nothing at all.
	resident := newAdvLazifier()
	residentReq := req
	residentReq.Previous.State = resident.root(t, req.Previous.State)
	residentDone := make(chan probeApplyResult, 1)
	if _, err = runLazyBudgetValidation(
		t,
		residentReq,
		candidate,
		func(update *cell.PreparedMerkleUpdate) {
			go probeLiveSuccessorApply(req.Previous.State, update, residentDone)
		},
		NewSemanticVerifier(tvm.NewTVM()),
		proven,
	); err != nil {
		t.Fatalf("validate a proof-backed candidate over a resident parent: %v", err)
	}
	if residentResult := <-residentDone; residentResult.err != nil {
		t.Fatalf("announced apply over a resident parent: %v", residentResult.err)
	}
	residentTotal, _ := advLazyLoads(resident)
	if got := advLazySiteLoads(resident, "collator.probeLiveSuccessorApply"); got != 0 {
		t.Fatalf("the apply over a resident parent read %d cells from the store", got)
	}

	t.Logf(
		"proof-backed validation over a store-shaped parent: critical path %d loads (unchanged), "+
			"live successor apply %d loads, all on the announced site. "+
			"resident parent: %d loads in the whole call, none from the apply.",
		baseLoads, asyncLoads, residentTotal,
	)
}
