package validator

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/validator/simplex"
)

func newWarmupRuntime(t *testing.T, config SessionConfig, provider CandidateProvider) *sessionRuntime {
	t.Helper()

	params := simplex.DefaultParams()
	params.CandidateResolveTimeout = time.Minute
	params.CandidateResolveTimeoutCap = time.Minute
	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(storage, provider, 2, params)
	t.Cleanup(candidates.close)

	states := newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		newRuntimeTestBackend(),
		candidates,
		StoredSessionState{},
		nil,
		params,
		config.Protocol.SlotsPerLeaderWindow,
	)
	t.Cleanup(states.close)
	if err := states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	log := zerolog.Nop()
	runtime := &sessionRuntime{
		config:     config,
		candidates: candidates,
		states:     states,
		log:        &log,
		warming:    make(map[simplex.CandidateID]struct{}),
		workActive: true,
		workCtx:    ctx,
	}
	// Cleanups run last-registered-first, and this order is the whole of what
	// makes the test terminate: a warm-up parked on a candidate that never
	// arrives leaves only when its context is cancelled, so the cancel has to
	// run before the join, not after it.
	t.Cleanup(runtime.workWG.Wait)
	t.Cleanup(cancel)

	return runtime
}

// TestObserverWarmsTheStateOfAReceivedCandidate is the window-entry half of the
// standalone collator's parity with a validator. A validator resolves the state
// of every candidate it validates, so the window that opens on one finds it
// resident; an observer validates nothing, and without this warm-up it pays the
// whole chain of applies at the instant its first block is due.
func TestObserverWarmsTheStateOfAReceivedCandidate(t *testing.T) {
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 4),
		finished: make(chan struct{}, 4),
	}
	config, _ := runtimeTestConfig(0x71, &runtimeTestJournal{})
	runtime := newWarmupRuntime(t, config, provider)

	id := simplex.CandidateID{Slot: 11, Hash: [32]byte{0x11}}
	runtime.warmCandidateState(id)

	select {
	case <-provider.called:
	case <-time.After(2 * time.Second):
		t.Fatal("received candidate did not start a background state resolution")
	}

	// The bound on background work: a candidate already being warmed does not
	// park a second waiter, no matter how many events name it.
	runtime.warmCandidateState(id)
	runtime.warmCandidateState(id)
	runtime.warmingMu.Lock()
	inflight := len(runtime.warming)
	runtime.warmingMu.Unlock()
	if inflight != 1 {
		t.Fatalf("in-flight warm-ups = %d, want 1", inflight)
	}
}

// TestValidatorDoesNotWarmCandidateStates pins the asymmetry deliberately. The
// validator builds the successor state inside validation and publishes it
// through rememberValidatedState, which does not replace a resolver flight that
// already finished. A background warm-up racing it would leave the session
// holding a second materialization of the same state, and chain_state.go
// compares tip states by pointer — so every later candidate would silently pay
// a full re-apply, with no error and no metric.
func TestValidatorDoesNotWarmCandidateStates(t *testing.T) {
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 4),
		finished: make(chan struct{}, 4),
	}
	config, _ := runtimeTestConfig(0x72, &runtimeTestJournal{})
	config.Identity.Validator = &ValidatorIdentity{Index: 0}
	runtime := newWarmupRuntime(t, config, provider)

	runtime.warmCandidateState(simplex.CandidateID{Slot: 11, Hash: [32]byte{0x11}})

	select {
	case <-provider.called:
		t.Fatal("a validator session started a background state resolution")
	case <-time.After(100 * time.Millisecond):
	}
	runtime.warmingMu.Lock()
	inflight := len(runtime.warming)
	runtime.warmingMu.Unlock()
	if inflight != 0 {
		t.Fatalf("validator recorded %d in-flight warm-ups, want 0", inflight)
	}
}

// A standalone collator's own block never reaches the receive path — it is
// broadcast from here — so the one window entry that matters most would be the
// coldest without this: a collator leading two windows in a row would resolve
// and apply its own last block at the instant the next window's first block is
// due, which is the block the cross-window handoff already built and parked.
//
// It must warm without betting. The producer that built the block has already
// parked a successor for the next window under this very candidate, and a second
// bet would either be refused as a duplicate or race the first out of the slot.
func TestPublishedCandidateIsWarmedWithoutPlacingABet(t *testing.T) {
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 4),
		finished: make(chan struct{}, 4),
	}
	config, _ := runtimeTestConfig(0x74, &runtimeTestJournal{})
	runtime := newWarmupRuntime(t, config, provider)

	offered := make(chan sessionSpeculativeWindow, 1)
	runtime.speculate = func(_ context.Context, window sessionSpeculativeWindow) error {
		offered <- window

		return nil
	}

	// The last slot of a window: the one slot a received candidate would bet on.
	last := config.Protocol.SlotsPerLeaderWindow - 1
	runtime.warmPublishedCandidateState(simplex.CandidateID{Slot: last, Hash: [32]byte{0x74}})

	select {
	case <-provider.called:
	case <-time.After(2 * time.Second):
		t.Fatal("a published candidate did not warm its own state")
	}
	select {
	case window := <-offered:
		t.Fatalf("publishing placed a bet on window %d; the producer already parked one", window.StartSlot)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWarmUpWaitsForTheCertificateInsteadOfPollingPeers is the cost half of the
// warm-up. A resolve completes only when the resolver holds both halves — the
// payload and the notarization — and while either is missing its flight asks a
// random session validator for it every CandidateResolveCooldown, which is ten
// milliseconds. Both moments the warm-up is started from have the payload
// already in hand and are missing only the certificate: candidate receipt and
// local publication. Starting the resolve there turned every received and every
// published candidate into a burst of ADNL queries aimed at the validators
// assembling that very quorum, and bought nothing — the resolve still could not
// finish before the certificate this node was about to observe anyway.
func TestWarmUpWaitsForTheCertificateInsteadOfPollingPeers(t *testing.T) {
	provider := &retryCandidateProvider{
		called:   make(chan struct{}, 4),
		finished: make(chan struct{}, 4),
	}
	// The resolver every test builds verifies candidates against the session
	// runtimeTestConfig derives from this tag, so a staged payload has to be
	// encoded under the same one.
	config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	runtime := newWarmupRuntime(t, config, provider)
	runtime.candidates.session = config.StorageID
	runtime.state.Params = simplex.DefaultParams()

	artifact, _ := acceptanceSplitBlock(t, runtime.states.genesis.root, 2, 0x73)
	if err := runtime.candidates.stage(artifact, []byte{0x73}); err != nil {
		t.Fatal(err)
	}

	id := artifact.Candidate.ID
	runtime.warmCandidateState(id)
	select {
	case <-provider.called:
		t.Fatal("a candidate whose payload is already here was asked of a peer")
	case <-time.After(200 * time.Millisecond):
	}
	runtime.warmingMu.Lock()
	inflight := len(runtime.warming)
	runtime.warmingMu.Unlock()
	if inflight != 1 {
		t.Fatalf("in-flight warm-ups while waiting = %d, want the warm-up parked", inflight)
	}

	// The certificate this node was going to observe anyway releases it.
	runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.warmingMu.Lock()
		inflight = len(runtime.warming)
		runtime.warmingMu.Unlock()
		if inflight == 0 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("the notarization did not release the parked warm-up")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
