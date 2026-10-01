package validator

import (
	"context"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

type shardReplayFixture struct {
	resolver *stateResolver
	backend  *runtimeTestBackend
	ids      []simplex.CandidateID

	mu       sync.Mutex
	accepted []BlockAcceptance
}

// newShardReplayFixture restarts a shard session whose journal and store hold
// persisted finalized candidates, with notReady naming the block seqnos the
// node had not applied when the process stopped.
func newShardReplayFixture(t *testing.T, length int, persisted int, notReady map[uint32]bool) *shardReplayFixture {
	t.Helper()

	storage := newRuntimeTestStorage()
	config, privateKey := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	candidates := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	t.Cleanup(candidates.close)

	fixture := &shardReplayFixture{backend: newRuntimeTestBackend()}
	parent := simplex.Genesis()
	for i := range length {
		artifact := runtimeBlockArtifact(t, config, privateKey, uint32(3*i+1), parent, uint32(i+1), uint64(0xc1+i))
		if err := candidates.stage(artifact, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(
			artifact.Candidate.ID,
			resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
		)
		fixture.ids = append(fixture.ids, artifact.Candidate.ID)
		parent = simplex.Parent(artifact.Candidate.ID)
	}

	fixture.backend.load = func(_ context.Context, request ChainStateRequest) (ChainStateData, error) {
		block := request.Blocks[0]
		if notReady[block.SeqNo] {
			return ChainStateData{}, ErrBlockNotReady
		}
		tip := ChainTip{ID: block, State: fixture.backend.stateRoot}
		if block.SeqNo != 0 {
			tip.BlockBOC = testTipBOCFor(block)
			tip.Block = testTipBlockFor(block)
		}

		return ChainStateData{Tips: []ChainTip{tip}}, nil
	}
	fixture.backend.acceptance = func(_ context.Context, acceptance BlockAcceptance) error {
		fixture.mu.Lock()
		fixture.accepted = append(fixture.accepted, acceptance)
		fixture.mu.Unlock()

		return nil
	}
	fixture.resolver = newStateResolver(
		config.Shard,
		config.StorageID,
		storage,
		fixture.backend,
		candidates,
		StoredSessionState{Finalized: fixture.ids[:persisted]},
		nil,
		simplex.DefaultParams(),
		config.Protocol.SlotsPerLeaderWindow,
	)
	t.Cleanup(fixture.resolver.close)
	if err := fixture.resolver.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}

	return fixture
}

func (f *shardReplayFixture) acceptances() []BlockAcceptance {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]BlockAcceptance(nil), f.accepted...)
}

// The reference loads its finalized markers as done and never walks them
// again. A shard session used to replay every persisted finalization down to
// the session genesis on the first finalization after a restart.
func TestShardRestartDoesNotReplayAppliedFinalizations(t *testing.T) {
	f := newShardReplayFixture(t, 6, 5, nil)
	newest := f.ids[5]

	if err := f.resolver.finalize(context.Background(), newest, resolverTestSeal(t, simplex.FinalizeVote(newest))); err != nil {
		t.Fatal(err)
	}
	accepted := f.acceptances()
	if len(accepted) != 1 || accepted[0].Candidate.Candidate.ID != newest || accepted[0].Replay {
		ids := make([]simplex.CandidateID, len(accepted))
		for i := range accepted {
			ids[i] = accepted[i].Candidate.Candidate.ID
		}
		t.Fatalf("accepted after restart = %v, want only the new finalization %s", ids, newest)
	}

	f.resolver.mu.Lock()
	defer f.resolver.mu.Unlock()
	for _, id := range f.ids[:5] {
		if state := f.resolver.finalized[id]; state == nil || !state.isDone || !state.reconciled {
			t.Fatalf("applied persisted finalization %s is not reconciled: %+v", id, state)
		}
	}
	if f.resolver.finalized[f.ids[4]].appliedState == nil {
		t.Fatal("the applied state read to reconcile the replay was not kept for the next acceptance")
	}
}

// A persisted marker is written after the acceptance was only queued, so the
// node may not have applied the newest persisted blocks. Those, and nothing
// older, are replayed, parent first.
func TestShardRestartReplaysOnlyTheUnappliedFinalizations(t *testing.T) {
	f := newShardReplayFixture(t, 6, 5, map[uint32]bool{4: true, 5: true})
	newest := f.ids[5]

	if err := f.resolver.finalize(context.Background(), newest, resolverTestSeal(t, simplex.FinalizeVote(newest))); err != nil {
		t.Fatal(err)
	}
	accepted := f.acceptances()
	want := []simplex.CandidateID{f.ids[3], f.ids[4], newest}
	if len(accepted) != len(want) {
		t.Fatalf("accepted %d blocks after restart, want %d", len(accepted), len(want))
	}
	for i := range want {
		if accepted[i].Candidate.Candidate.ID != want[i] || accepted[i].Replay != (i < 2) {
			t.Fatalf("acceptance %d = %s replay %t, want %s replay %t",
				i, accepted[i].Candidate.Candidate.ID, accepted[i].Replay, want[i], i < 2)
		}
	}

	// A later finalization of an older, applied block has nothing to replay.
	if err := f.resolver.finalize(context.Background(), f.ids[1], resolverTestSeal(t, simplex.FinalizeVote(f.ids[1]))); err != nil {
		t.Fatal(err)
	}
	if len(f.acceptances()) != len(want) {
		t.Fatal("an applied persisted finalization was accepted again")
	}
}
