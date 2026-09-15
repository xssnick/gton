package validator

import (
	"errors"
	"testing"
	"testing/synctest"

	"github.com/xssnick/gton/service/validator/simplex"
)

// An observer prepares the successor of every candidate it receives and then
// waits for that candidate's certificate. A candidate the committee skipped is
// never certified, so nothing but the flight TTL used to end that wait, and
// every such flight kept its successor state and two goroutines alive for the
// whole horizon while finalization moved on.
func TestObserverFlightBelowFinalizationIsReleased(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
		runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
		artifact, _ := acceptanceSplitBlock(t, runtime.states.genesis.root, 8, 0xb1)
		id := artifact.Candidate.ID
		if err := runtime.candidates.stage(artifact, []byte{0xb1}); err != nil {
			t.Fatal(err)
		}

		offers := 0
		warm := make(chan error, 1)
		go func() {
			_, err := runtime.states.resolveWithPreparation(t.Context(), simplex.Parent(id), func(ResolvedState) {
				offers++
			})
			warm <- err
		}()
		synctest.Wait()
		if offers != 1 {
			t.Fatalf("prepared offers = %d, want 1", offers)
		}
		// A reader that joined the same flight, as a window opening on the
		// candidate would, has to be told to retry rather than fail its session.
		joined := make(chan error, 1)
		go func() {
			_, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
			joined <- err
		}()
		synctest.Wait()

		margin := runtime.states.stateRetainedSlots()
		runtime.states.notifyFinalized(id.Slot+margin, retentionFloorNone)
		synctest.Wait()
		if len(warm) != 0 || len(joined) != 0 {
			t.Fatal("a flight inside the retained margin was released")
		}

		runtime.states.notifyFinalized(id.Slot+margin+1, retentionFloorNone)
		synctest.Wait()
		if len(warm) == 0 || len(joined) == 0 {
			t.Fatal("an uncertified flight below finalization outlived the finalization")
		}
		for _, err := range []error{<-warm, <-joined} {
			if !errors.Is(err, ErrBlockNotReady) {
				t.Fatalf("released flight error = %v, want a retryable not-ready", err)
			}
		}
		runtime.states.mu.Lock()
		_, retained := runtime.states.states[simplex.Parent(id)]
		runtime.states.mu.Unlock()
		if retained {
			t.Fatal("released flight stayed cached")
		}
	})
}

// The finalization walk fetches the certificate of a finalized ancestor this
// node never observed, and that certificate completes the waiting flight. A
// flight the walk has already reached is therefore still going to be needed.
func TestObserverFlightOfFinalizedCandidateSurvivesTheSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
		runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
		artifact, expected := acceptanceSplitBlock(t, runtime.states.genesis.root, 8, 0xb2)
		id := artifact.Candidate.ID
		if err := runtime.candidates.stage(artifact, []byte{0xb2}); err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() {
			_, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
			done <- err
		}()
		synctest.Wait()
		runtime.states.mu.Lock()
		runtime.states.finalized[id] = &finalizedState{inFlight: &resolverFlight{done: make(chan struct{})}}
		runtime.states.mu.Unlock()

		runtime.states.notifyFinalized(id.Slot+runtime.states.stateRetainedSlots()+1, retentionFloorNone)
		synctest.Wait()
		if len(done) != 0 {
			t.Fatal("the sweep released a flight the finalization walk is certifying")
		}

		runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		state, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
		if err != nil || state.State.root.HashKey() != expected.HashKey() {
			t.Fatalf("certified successor after the sweep = %v", err)
		}
	})
}
