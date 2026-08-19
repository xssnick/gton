package validator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// appliedStateHarness is the resolver of a validator that has finalized one shard
// block itself, wired to a node that answers only for what acceptance published —
// the field condition, where no commit is coming until the shard client arrives.
//
// It exists so each of the three wirings below can be reverted one at a time and
// seen to fail. Every one of them was, in the reviewer's snapshot, invisible to the
// whole suite: reverting it left go test ./service/validator fully green.
type appliedStateHarness struct {
	resolver *stateResolver
	live     *nodeLiveView
	artifact *CandidateArtifact
	computed *cell.Cell

	mu       sync.Mutex
	requests []ChainStateRequest
}

func newAppliedStateHarness(t *testing.T, marker byte) *appliedStateHarness {
	t.Helper()

	store := newRuntimeTestStorage()
	config, privateKey := runtimeTestConfig(marker, &runtimeTestJournal{})
	candidates := newResolverForTest(
		store,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	t.Cleanup(candidates.close)

	artifact := runtimeBlockArtifact(t, config, privateKey, 1, simplex.Genesis(), 1, uint64(marker)+1)
	if err := candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	candidates.observeNotarization(
		artifact.Candidate.ID,
		resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
	)

	harness := &appliedStateHarness{live: newNodeLiveView(), artifact: artifact}
	backend := newRuntimeTestBackend()
	backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		return harness.live.publish(acceptance)
	}
	resolver := newStateResolver(
		config.Shard,
		config.StorageID,
		store,
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
	// From here the modelled node answers publications only, and every request it
	// receives is recorded.
	backend.load = func(ctx context.Context, request ChainStateRequest) (ChainStateData, error) {
		harness.mu.Lock()
		harness.requests = append(harness.requests, request)
		harness.mu.Unlock()

		return harness.live.load(ctx, request)
	}

	harness.computed = cell.BeginCell().
		MustStoreUInt(0x5eed, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0x0fed, 32).EndCell()).
		EndCell()
	resolver.rememberValidatedState(artifact.Candidate.ID, ResolvedState{State: &ChainState{
		shard: config.Shard,
		tips: []ChainTip{{
			ID:       *artifact.Candidate.Block.Copy(),
			BlockBOC: artifact.BlockBOC,
			Block:    testTipBlockFor(artifact.Candidate.Block),
			State:    harness.computed,
		}},
		root: harness.computed,
	}})
	harness.resolver = resolver

	return harness
}

// finalize finalizes the block and releases the in-memory margin, which is what
// the retention sweeps do about a second later on the measured network.
func (h *appliedStateHarness) finalize(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.resolver.finalizeWith(ctx, h.artifact.Candidate.ID, simplex.VerifiedCertificate{}, nil); err != nil {
		t.Fatalf("finalize the locally produced shard block: %v", err)
	}
	h.resolver.notifyFinalized(1_000_000, retentionFloorNone)
	h.releaseFlight()
	h.resolver.mu.Lock()
	h.resolver.finalized[h.artifact.Candidate.ID].appliedState = nil
	delete(h.resolver.applied, h.artifact.Candidate.ID)
	h.resolver.mu.Unlock()
}

// releaseFlight drops the resolve flight for this parent, so the next resolve runs
// resolveInner again instead of joining a finished one. That is what the resolver
// does on its own between candidates once the margin is released.
func (h *appliedStateHarness) releaseFlight() {
	h.resolver.mu.Lock()
	delete(h.resolver.states, simplex.Parent(h.artifact.Candidate.ID))
	h.resolver.mu.Unlock()
}

func (h *appliedStateHarness) parent() simplex.ParentID {
	return simplex.Parent(h.artifact.Candidate.ID)
}

func (h *appliedStateHarness) recorded() []ChainStateRequest {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]ChainStateRequest(nil), h.requests...)
}

func (h *appliedStateHarness) resolve(t *testing.T, within time.Duration) (ResolvedState, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()

	return h.resolver.resolve(ctx, h.parent())
}

// MAJOR 3, item 1: the resident fast path. A finalized parent this session already
// holds must be answered from memory, without a read.
//
// Two things were wrong with going straight to the load, and the second is the
// expensive one: it re-read a state that was in memory, and — when the store could
// not answer — it WAITED for a state it was holding. That is the standstill, on a
// node that had the answer the whole time.
//
// The assertion is the read count, because that is what the fast path is: the
// second resolve of the same finalized parent must cost zero reads. Reverting the
// three lines makes it cost one.
func TestFinalizedParentResolvesFromMemoryWithoutAReRead(t *testing.T) {
	harness := newAppliedStateHarness(t, 0x60)
	harness.finalize(t)

	// The first resolve is the one that loads, and it is what puts the state under
	// the finalization marker.
	first, err := harness.resolve(t, 2*time.Second)
	if err != nil {
		t.Fatalf("first resolve of the finalized parent: %v", err)
	}
	if loads := len(harness.recorded()); loads != 1 {
		t.Fatalf("the first resolve took %d reads, want the one that populates the marker", loads)
	}
	harness.releaseFlight()

	second, err := harness.resolve(t, 2*time.Second)
	if err != nil {
		t.Fatalf("second resolve of the finalized parent: %v", err)
	}
	if loads := len(harness.recorded()); loads != 1 {
		t.Errorf("the second resolve took %d reads in total, want no read at all beyond the first", loads)
	}
	// And it is the same object, not merely the same content: chain_state.go
	// compares tip states BY POINTER for the live-successor carry-back.
	if second.State != first.State {
		t.Error("the second resolve returned another materialization of the same finalized parent")
	}
}

// MAJOR 3, item 2: the canonical object. Where the load DOES run, what is returned
// must be the state the finalization marker adopted, not the one this call built.
//
// The race is real and it is not rare: two children of one finalized parent resolve
// concurrently, both miss the resident fast path, and both build a state from the
// same bytes. Only one can be the marker's. Handing the loser its own copy gives
// one block two materializations, and chain_state.go's pointer comparison turns
// that into a silent full re-apply per candidate — no error, no metric.
//
// The concurrent winner is modelled deterministically: the marker gains its state
// while the load this resolve is waiting on is in flight, which is exactly the
// window the race lives in.
func TestFinalizedParentResolveReturnsTheMarkersCanonicalState(t *testing.T) {
	harness := newAppliedStateHarness(t, 0x64)
	harness.finalize(t)

	canonical := &ChainState{
		shard: harness.resolver.shard,
		tips: []ChainTip{{
			ID:       *harness.artifact.Candidate.Block.Copy(),
			BlockBOC: harness.artifact.BlockBOC,
			Block:    testTipBlockFor(harness.artifact.Candidate.Block),
			State:    harness.computed,
		}},
		root: harness.computed,
	}
	// The other resolve wins the race while this one is inside the read.
	harness.live.beforeLoad = func() {
		harness.resolver.mu.Lock()
		if state := harness.resolver.finalized[harness.artifact.Candidate.ID]; state != nil {
			state.appliedState = canonical
		}
		harness.resolver.mu.Unlock()
	}

	resolved, err := harness.resolve(t, 2*time.Second)
	if err != nil {
		t.Fatalf("resolve the finalized parent: %v", err)
	}
	if resolved.State != canonical {
		t.Error("the resolve returned the state it built rather than the one the marker adopted, so one block now has two materializations")
	}
	// The marker itself must not have been overwritten either.
	harness.resolver.mu.Lock()
	adopted := harness.resolver.finalized[harness.artifact.Candidate.ID].appliedState
	harness.resolver.mu.Unlock()
	if adopted != canonical {
		t.Error("the resolve replaced the state the marker had already adopted")
	}
}

// MAJOR 3, item 3: the ONE line that connects the caller which exists in order to
// wait to the backend wait that replaced the poll.
//
// loadFinalizedChainState is that caller. Without request.Wait the node silently
// returns to the 1 Hz poll this change was ranked #3 for removing — and the test
// that covers the wait, TestChainTipReadWaitsForThePublicationEdgeRatherThanPolling,
// sets Wait in its own request, so R3 could have shipped INERT with a green suite.
//
// Both halves are asserted here, because either alone can be satisfied by accident:
// the request the backend actually received carries Wait, and a backend that only
// answers a waiting request is served inside a deadline far below the 1 s fallback
// interval.
func TestFinalizedChainStateLoadAsksTheBackendToWait(t *testing.T) {
	// Far below the 1 s interval that paces the fallback, so a resolve that fell
	// back to it cannot pass.
	const withoutPolling = 250 * time.Millisecond

	harness := newAppliedStateHarness(t, 0x68)
	harness.finalize(t)
	// A backend that does not answer a request which did not ask it to wait. That
	// is not a caricature: it is what LocalSessionBackend.loadChainTip does — with
	// wait false it reads once and hands back the not-ready error.
	harness.live.requireWait = true

	resolved, err := harness.resolve(t, withoutPolling)
	if err != nil {
		t.Fatalf("resolve of a finalized parent that needs the backend wait: %v", err)
	}
	if resolved.State == nil || resolved.State.root != harness.computed {
		t.Fatal("the resolve did not come back with the published state")
	}
	requests := harness.recorded()
	if len(requests) == 0 {
		t.Fatal("the resolve made no request at all, so nothing about the wait is being checked")
	}
	for i, request := range requests {
		if !request.Wait {
			t.Fatalf(
				"request %d did not ask the backend to wait, so the node is back to polling a read "+
					"that fails in microseconds once a second against a 200 ms slot",
				i,
			)
		}
	}
}

// MAJOR 3, the deeper problem: the carry-back branch of validatedCandidateState —
// the branch whose pointer comparison the entire "one materialization" argument
// rests on — was never exercised in the package that owns it. Over was called 25
// times in this suite and every call had an empty Live, so every one took the
// fallback apply.
//
// This drives the branch with a REAL token: collator.LiveSuccessorOf performs the
// apply over this state's own full parent and packages the result the way the
// verifier does, so what is being tested is the production Over, the production
// pointer comparison and the production carry-back — not a stand-in.
//
// The strongest assertion is the pending handle. It belongs to ANOTHER ChainState,
// so successorOf refuses it: if the carry-back branch did not run, the fallback
// would take that handle and the call would fail. A green result is therefore proof
// that the root came back through Over rather than from a second apply.
func TestValidatedCandidateStateCarriesBackTheLiveSuccessor(t *testing.T) {
	fixture := newLiveSuccessorFixture(t, 0x70)
	state, artifact := fixture.state(t, 0x70)

	live, err := collator.LiveSuccessorOf(fixture.prepared, state.root, state.tipStates()...)
	if err != nil {
		t.Fatalf("build the live successor over this state's own parent: %v", err)
	}
	carried, opened := live.Over(state.root, state.tipStates()...)
	if !opened {
		t.Fatal("the token does not open for the state it was built over, so the fixture is wrong")
	}

	successor := fixture.successorOf(artifact)
	successor.Live = live
	// A handle on another state: the fallback apply cannot use it, so reaching the
	// fallback is a failure rather than a silent duplicate walk.
	foreign := (&ChainState{shard: state.shard, root: fixture.parent}).pendingSuccessor(context.Background())

	next, err := state.validatedCandidateState(artifact, successor, foreign)
	if err != nil {
		t.Fatalf("advance the parent through the carried-back successor: %v", err)
	}
	if next.root != carried {
		t.Fatal("the successor state is not the root the verifier carried back, so the apply was performed twice")
	}
	if tips := next.tipStates(); len(tips) != 1 || tips[0] != carried {
		t.Fatal("the successor tip is not the carried-back root")
	}
	// FULL, and a hash check cannot tell: a narrow successor has the same hash.
	bits, err := untouchedLeafBits(next.root)
	if err != nil {
		t.Fatalf("walk the untouched subtree of the carried-back successor: %v", err)
	}
	if bits != fixture.untouchedBit {
		t.Fatalf("untouched leaf of the carried-back successor = %#x, want %#x", bits, fixture.untouchedBit)
	}

	// THE CONTROL, and the reason the carry-back is safe at all: the token opens for
	// the trees it was built over and for nothing else. A parent of equal content but
	// another materialization is refused, which is what keeps a proof-backed root out
	// of the lineage — it has the same hash as the full one.
	twin := cell.BeginCell().MustStoreBuilder(fixture.parent.ToBuilder()).EndCell()
	if twin.HashKeyAt(0) != fixture.parent.HashKeyAt(0) {
		t.Fatal("the twin parent is not the same content, so it does not test identity")
	}
	if _, refused := live.Over(twin, twin); refused {
		t.Fatal("the token opened for another materialization of the same content")
	}
	// And a state presenting that twin falls back to its own apply, which needs a
	// handle of its own; with the foreign one it fails loudly.
	twinState := &ChainState{
		shard: state.shard,
		tips:  []ChainTip{{ID: state.tips[0].ID, Block: state.tips[0].Block, State: twin}},
		root:  twin,
	}
	_, err = twinState.validatedCandidateState(artifact, successor, foreign)
	if err == nil {
		t.Fatal("a state the token does not open for silently took the carried-back root")
	}
	if !strings.Contains(err.Error(), "apply validated candidate to the live parent") {
		t.Fatalf("refusal = %v, want the fallback apply reporting the handle it cannot use", err)
	}
	// Never a rejection: the candidate was found valid, and a local bookkeeping
	// failure must not turn into a vote against a good block.
	if errors.Is(err, ErrCandidateRejected) {
		t.Fatal("a local bookkeeping failure was classified as a candidate rejection")
	}
}
