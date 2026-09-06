package validator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func awaitPreparedObserverState(t *testing.T, runtime *sessionRuntime, id simplex.CandidateID) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		runtime.candidates.mu.Lock()
		entry := runtime.candidates.entries[id]
		waiting := entry != nil && entry.notarized != nil
		runtime.candidates.mu.Unlock()
		if waiting {
			return
		}
		select {
		case <-deadline:
			t.Fatal("successor was not prepared before notarization")
		case <-tick.C:
		}
	}
}

func TestObserverPreparesSuccessorBeforeCertification(t *testing.T) {
	for _, own := range []bool{false, true} {
		name := "received"
		if own {
			name = "own"
		}
		t.Run(name, func(t *testing.T) {
			provider := &retryCandidateProvider{called: make(chan struct{}, 4), finished: make(chan struct{}, 4)}
			config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
			runtime := newWarmupRuntime(t, config, provider)
			artifact, expected := acceptanceSplitBlock(t, runtime.states.genesis.root, 2, 0x92)
			root, err := cell.FromBOC(artifact.BlockBOC)
			if err != nil {
				t.Fatal(err)
			}
			artifact.validationRoots = &candidateValidationRoots{block: root}
			if own {
				parser, err := root.BeginParse()
				if err != nil {
					t.Fatal(err)
				}
				var block tlb.Block
				if err = tlb.LoadFromCell(&block, parser); err != nil {
					t.Fatal(err)
				}
				update, err := cell.PrepareMerkleUpdatePlanned(block.StateUpdate)
				if err != nil {
					t.Fatal(err)
				}
				live, err := collator.LiveSuccessorOf(update, runtime.states.genesis.root, runtime.states.genesis.tipStates()...)
				if err != nil {
					t.Fatal(err)
				}
				artifact.validationRoots.builtSuccessor = live
				expected, _ = live.Over(runtime.states.genesis.root, runtime.states.genesis.tipStates()...)
			}
			id := artifact.Candidate.ID
			if err = runtime.candidates.stage(artifact, []byte{0x92}); err != nil {
				t.Fatal(err)
			}
			if own {
				runtime.warmPublishedCandidateState(id)
			} else {
				runtime.warmCandidateState(id)
			}
			awaitPreparedObserverState(t, runtime, id)

			runtime.states.mu.Lock()
			flight := runtime.states.states[simplex.Parent(id)]
			unpublished := !flight.finished && flight.result.State == nil
			runtime.states.mu.Unlock()
			if !unpublished {
				t.Fatal("uncertified successor escaped its preparation flight")
			}
			runtime.candidates.mu.Lock()
			roots := runtime.candidates.entries[id].validationRoots
			runtime.candidates.mu.Unlock()
			if roots != nil {
				t.Fatal("resolver retained the one-use successor capsule")
			}
			select {
			case <-provider.called:
				t.Fatal("preparing a resident candidate queried the committee")
			default:
			}

			result := make(chan ResolvedState, 1)
			go func() {
				resolved, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
				if err != nil {
					t.Errorf("resolve prepared successor: %v", err)
				}
				result <- resolved
			}()
			// A zero value carries no verified evidence and must not wake the
			// preparation's certificate channel.
			runtime.candidates.observeNotarization(id, simplex.VerifiedCertificate{})
			select {
			case <-result:
				t.Fatal("window resolved a successor before certification")
			case <-time.After(10 * time.Millisecond):
			}
			runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
			select {
			case resolved := <-result:
				if resolved.State == nil || resolved.State.root.HashKey() != expected.HashKey() {
					t.Fatal("certification released another successor")
				}
				if own && resolved.State.root != expected {
					t.Fatal("own successor was recomputed instead of reusing the builder's root")
				}
				again, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
				if err != nil || again.State != resolved.State {
					t.Fatalf("successor materialization changed after certification: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("certificate did not release the prepared successor")
			}
		})
	}
}

func TestObserverPreparationCancellationDiscardsUncertifiedState(t *testing.T) {
	config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
	artifact, _ := acceptanceSplitBlock(t, runtime.states.genesis.root, 2, 0x93)
	id := artifact.Candidate.ID
	if err := runtime.candidates.stage(artifact, []byte{0x93}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := runtime.states.resolve(ctx, simplex.Parent(id))
		done <- err
	}()
	awaitPreparedObserverState(t, runtime, id)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("preparation cancellation = %v", err)
	}
	runtime.states.close()
	runtime.states.mu.Lock()
	flight := runtime.states.states[simplex.Parent(id)]
	runtime.states.mu.Unlock()
	if flight != nil {
		t.Fatal("cancelled uncertified state remained cached")
	}
}

func TestObserverRejectsInvalidParentBeforeCertification(t *testing.T) {
	config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
	otherParent := cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()
	artifact, _ := acceptanceSplitBlock(t, otherParent, 2, 0x94)
	id := artifact.Candidate.ID
	if err := runtime.candidates.stage(artifact, []byte{0x94}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.states.resolve(t.Context(), simplex.Parent(id)); err == nil {
		t.Fatal("observer accepted a state update built on another parent")
	}
	runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
	if _, err := runtime.states.resolve(t.Context(), simplex.Parent(id)); err == nil {
		t.Fatal("certificate hid an invalid state update")
	}
}

func TestConsensusObserverForwardsNotarizationPace(t *testing.T) {
	fixture := newObserverFixture(t, nil, nil)
	t.Cleanup(func() { _ = fixture.observer.Close(context.Background()) })
	var gotShard groups.ShardID
	var gotID simplex.CandidateID
	var gotAt time.Time
	fixture.observer.mu.Lock()
	fixture.observer.events.Notarized = func(shard groups.ShardID, id simplex.CandidateID, at time.Time) {
		gotShard, gotID, gotAt = shard, id, at
	}
	fixture.observer.mu.Unlock()
	if err := fixture.observer.PrepareSession(t.Context(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.activation.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	runtime := session.runtime.(*sessionRuntime)
	id := simplex.CandidateID{Slot: 1, Hash: [32]byte{0x95}}
	before := time.Now()
	runtime.OnNotarized(id, runtimeTestSeal(t, runtime.config, fixture.validatorKey, simplex.NotarizeVote(id)))
	if gotID != id || gotShard != runtime.config.Shard || gotAt.Before(before) || gotAt.After(time.Now()) {
		t.Fatalf("notarization feedback = %v %v %v", gotShard, gotID, gotAt)
	}
}

func TestObserverPreparationExpiresWithoutPoisoningCertifiedRetry(t *testing.T) {
	config, _ := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
	params := simplex.DefaultParams()
	params.TargetRate = 10 * time.Millisecond
	params.MaxLeaderWindowDesync = 1
	params.CandidateResolveTimeoutCap = 20 * time.Millisecond
	runtime.states.updateParams(params)
	artifact, expected := acceptanceSplitBlock(t, runtime.states.genesis.root, 2, 0x96)
	id := artifact.Candidate.ID
	if err := runtime.candidates.stage(artifact, []byte{0x96}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.states.resolve(t.Context(), simplex.Parent(id)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("uncertified preparation expiry = %v", err)
	}
	runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
	resolved, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.State.root.HashKey() != expected.HashKey() {
		t.Fatal("expired preparation poisoned its certified retry")
	}
}

func TestObserverCertifiedSlowParentUsesResolverLifetime(t *testing.T) {
	config, key := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	runtime := newWarmupRuntime(t, config, &retryCandidateProvider{called: make(chan struct{}, 4)})
	params := simplex.DefaultParams()
	params.TargetRate = 10 * time.Millisecond
	runtime.states.updateParams(params)

	parent := runtimeOrdinaryArtifact(t, config, key, 0, simplex.Genesis())
	if err := runtime.candidates.stage(parent, []byte{0x97}); err != nil {
		t.Fatal(err)
	}
	runtime.candidates.observeNotarization(parent.Candidate.ID, resolverTestSeal(t, simplex.NotarizeVote(parent.Candidate.ID)))
	runtime.states.mu.Lock()
	runtime.states.finalized[parent.Candidate.ID] = &finalizedState{isDone: true}
	runtime.states.mu.Unlock()

	loading := make(chan struct{})
	release := make(chan struct{})
	backend := runtime.states.backend.(*runtimeTestBackend)
	backend.load = func(ctx context.Context, request ChainStateRequest) (ChainStateData, error) {
		close(loading)
		select {
		case <-ctx.Done():
			return ChainStateData{}, ctx.Err()
		case <-release:
			return ChainStateData{Tips: []ChainTip{{
				ID: request.Blocks[0], BlockBOC: parent.BlockBOC,
				Block: testTipBlockFor(parent.Candidate.Block), State: runtime.states.genesis.root,
			}}}, nil
		}
	}
	child, expected := acceptanceSplitBlock(t, runtime.states.genesis.root, 2, 0x98)
	child.Candidate.Parent = simplex.Parent(parent.Candidate.ID)
	id := child.Candidate.ID
	if err := runtime.candidates.stage(child, []byte{0x98}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		resolved, err := runtime.states.resolve(t.Context(), simplex.Parent(id))
		if err == nil && resolved.State.root.HashKey() != expected.HashKey() {
			err = errors.New("slow parent produced another successor")
		}
		done <- err
	}()
	select {
	case <-loading:
	case <-time.After(2 * time.Second):
		t.Fatal("parent state did not start loading")
	}
	runtime.candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
	// One leader window lasts 20 ms. Loading a certified parent can legitimately
	// outlast a window and is bounded by the shared state flight's own TTL.
	select {
	case err := <-done:
		t.Fatalf("certified preparation ended before its parent loaded: %v", err)
	case <-time.After(60 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("certified preparation inherited the speculative timeout: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("certified successor did not resume after parent load")
	}
}
