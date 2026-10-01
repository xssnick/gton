package validator

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestObserverOffersPreparedSuccessorBeforeCertification(t *testing.T) {
	for _, test := range []struct {
		name string
		own  bool
		join bool
	}{
		{name: "received"},
		{name: "join existing preparation", join: true},
		{name: "own published candidate", own: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
				runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
				runtime.codec = runtime.candidates.codec
				runtime.state.Params = simplex.DefaultParams()
				offered := make(chan sessionSpeculativeWindow, 2)
				runtime.speculate = func(_ context.Context, window sessionSpeculativeWindow) error {
					offered <- window

					return nil
				}
				artifact, expected := acceptanceSplitBlock(t, runtime.states.genesis.root, 8, 0xa1)
				id := artifact.Candidate.ID
				if err := runtime.candidates.stage(artifact, []byte{0xa1}); err != nil {
					t.Fatal(err)
				}
				resolved := make(chan ResolvedState, 1)
				if test.join {
					go func() {
						state, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
						if err != nil {
							t.Errorf("ordinary resolve: %v", err)
						}
						resolved <- state
					}()
					synctest.Wait()
				}
				if test.own {
					runtime.warmPublishedCandidateState(id)
				} else {
					runtime.warmCandidateState(id)
				}
				synctest.Wait()
				if test.own {
					if len(offered) != 0 {
						t.Fatal("own publication duplicated the producer's parked successor")
					}
				} else {
					select {
					case window := <-offered:
						if window.Base != id || window.StartSlot != 8 || window.BaseState.root.HashKey() != expected.HashKey() {
							t.Fatalf("wrong speculative successor: %+v", window)
						}
						genUtime, err := artifact.generationTime()
						if err != nil || !window.StartAt.Equal(genUtime.Add(runtime.state.Params.TargetRate)) {
							t.Fatalf("prepared successor generation time: %v, start %v", err, window.StartAt)
						}
					default:
						t.Fatal("prepared successor was not offered before its certificate")
					}
				}

				ctx, cancel := context.WithCancel(t.Context())
				ordinary := make(chan error, 1)
				go func() {
					_, err := runtime.states.resolve(ctx, simplex.Parent(id))
					ordinary <- err
				}()
				runtime.warmCandidateState(id)
				runtime.candidates.observeNotarization(id, simplex.VerifiedCertificate{})
				synctest.Wait()
				if len(ordinary) != 0 || len(resolved) != 0 {
					t.Fatal("ordinary reader crossed the certificate gate")
				}
				cancel()
				if err := <-ordinary; !errors.Is(err, context.Canceled) {
					t.Fatalf("ordinary waiter cancellation = %v", err)
				}

				runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
				synctest.Wait()
				state, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
				if err != nil || state.State.root.HashKey() != expected.HashKey() {
					t.Fatalf("certified successor changed after cancelling another waiter: %v", err)
				}
				// A late notarization warm-up finds the completed flight and must
				// neither repeat a received bet nor create one for our own block.
				runtime.warmCandidateState(id)
				synctest.Wait()
				if len(offered) != 0 {
					t.Fatal("warm-up offered the same successor twice")
				}
			})
		})
	}
}

func TestObserverEmptyCandidatePreparationWaitsForItsParentCertificate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
		runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
		runtime.codec = runtime.candidates.codec
		runtime.state.Params = simplex.DefaultParams()
		offered := make(chan sessionSpeculativeWindow, 1)
		runtime.speculate = func(_ context.Context, window sessionSpeculativeWindow) error {
			offered <- window

			return nil
		}
		parent, expected := acceptanceSplitBlock(t, runtime.states.genesis.root, 5, 0xa2)
		empty := &CandidateArtifact{Candidate: simplex.Candidate{
			ID: simplex.CandidateID{Slot: 5, Hash: [32]byte{0xa3}}, Parent: simplex.Parent(parent.Candidate.ID),
			Block: parent.Candidate.Block, Empty: true,
		}}
		if err := runtime.candidates.stage(parent, []byte{0xa2}); err != nil {
			t.Fatal(err)
		}
		if err := runtime.candidates.stage(empty, []byte{0xa3}); err != nil {
			t.Fatal(err)
		}
		runtime.warmCandidateState(empty.Candidate.ID)
		synctest.Wait()
		if len(offered) != 0 {
			t.Fatal("empty candidate exposed its uncertified parent's state")
		}
		runtime.candidates.observeNotarization(parent.Candidate.ID, resolverTestSeal(t, simplex.NotarizeVote(parent.Candidate.ID)))
		synctest.Wait()
		select {
		case window := <-offered:
			genUtime, err := parent.generationTime()
			if err != nil || window.Base != empty.Candidate.ID || window.BaseState.root.HashKey() != expected.HashKey() ||
				!window.StartAt.Equal(genUtime.Add(runtime.state.Params.TargetRate)) {
				t.Fatalf("empty candidate did not inherit the parent state and time: %+v, %v", window, err)
			}
		case <-time.After(time.Second):
			t.Fatal("empty tail candidate was not offered after its parent certified")
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := runtime.states.resolve(ctx, simplex.Parent(empty.Candidate.ID))
			done <- err
		}()
		synctest.Wait()
		if len(done) != 0 {
			t.Fatal("empty candidate escaped its own certificate gate")
		}
		runtime.candidates.observeNotarization(empty.Candidate.ID, resolverTestSeal(t, simplex.NotarizeVote(empty.Candidate.ID)))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestObserverPreparedOfferEndsWithResolverLifetime(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "expiry"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
				runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
				params := simplex.DefaultParams()
				params.TargetRate = 10 * time.Millisecond
				params.MaxLeaderWindowDesync = 1
				params.CandidateResolveTimeoutCap = 20 * time.Millisecond
				runtime.states.updateParams(params)
				artifact, expected := acceptanceSplitBlock(t, runtime.states.genesis.root, 8, 0xa4)
				id := artifact.Candidate.ID
				if err := runtime.candidates.stage(artifact, []byte{0xa4}); err != nil {
					t.Fatal(err)
				}
				offers := 0
				done := make(chan error, 1)
				go func() {
					_, err := runtime.states.resolveWithPreparation(t.Context(), simplex.Parent(id), func(state ResolvedState) {
						if state.State.root.HashKey() != expected.HashKey() {
							t.Error("prepared another successor")
						}
						offers++
					})
					done <- err
				}()
				synctest.Wait()
				if offers != 1 || len(done) != 0 {
					t.Fatalf("offers = %d, completed before certificate = %t", offers, len(done) != 0)
				}
				if shutdown {
					runtime.states.close()
				}
				err := <-done
				if shutdown {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("shutdown error = %v", err)
					}
				} else if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expiry error = %v", err)
				}
				synctest.Wait()
				if offers != 1 {
					t.Fatal("cancelled preparation offered a successor again")
				}
				runtime.states.mu.Lock()
				_, retained := runtime.states.states[simplex.Parent(id)]
				runtime.states.mu.Unlock()
				if retained {
					t.Fatal("cancelled uncertified state remained cached")
				}
				if !shutdown {
					runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
					state, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
					if err != nil || state.State.root.HashKey() != expected.HashKey() {
						t.Fatalf("certified retry after expiry = %v", err)
					}
				}
			})
		})
	}
}
