package collator

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestRuntimePaceReusedFutureKeepsBuildBudgetAndProvenance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		softRequests := make(chan SoftTimeoutRequest, 2)
		commits := make(chan CandidateCommit, 2)
		pipeline := &runtimeTestPipeline{}
		pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				built := runtimeBuiltCandidate(request)
				built.Stats.Transactions = request.MaxTransactions

				return built, nil
			}
		}
		pipeline.soft = func(_ context.Context, request SoftTimeoutRequest) (SoftTimeoutDecision, error) {
			softRequests <- request
			if request.Current.Slot != 2 {
				return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
			}

			return SoftTimeoutDecision{
				Action: SoftTimeoutEmitEmpty,
				Block: runtimeTestBlockID(
					request.Current.Session.Shard.Workchain,
					request.Current.Session.Shard.Shard,
					77,
				),
			}, nil
		}
		pipeline.commit = func(_ context.Context, commit CandidateCommit) error {
			commits <- commit

			return nil
		}
		emitted := make(chan CandidateArtifact, 2)
		fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		})
		defer fixture.close(t)

		session, update := fixture.session(0xa1, 2, 2, time.Now())
		update.TargetRate = 20 * time.Millisecond
		update.CurrentBase = simplex.Parent(simplex.CandidateID{Slot: 1, Hash: sha256.Sum256([]byte("pace-base"))})
		fixture.prepare(t, session, update)
		if err := fixture.service.CommitDelegation(t.Context(), fixture.request(t, session, 2)); err != nil {
			t.Fatal(err)
		}

		firstTimeout := <-softRequests
		if firstTimeout.Active.MaxTransactions != firstSlotTransactions {
			t.Fatalf("first-slot build cap = %d, want %d", firstTimeout.Active.MaxTransactions, firstSlotTransactions)
		}
		first := runtimeAwaitArtifact(t, emitted)
		if !first.Candidate.Empty || first.Candidate.ID.Slot != 2 {
			t.Fatalf("first candidate = %+v, want an empty slot 2", first.Candidate)
		}
		firstCommit := <-commits
		if firstCommit.Built != nil {
			t.Fatal("empty candidate committed the unfinished build")
		}

		secondTimeout := <-softRequests
		if secondTimeout.Active.Slot != 2 || secondTimeout.Current.Slot != 3 {
			t.Fatalf("reused future slots = active %d current %d", secondTimeout.Active.Slot, secondTimeout.Current.Slot)
		}
		if secondTimeout.Current.MaxTransactions != 0 {
			t.Fatalf("ordinary slot cap = %d, want protocol-only transaction limit", secondTimeout.Current.MaxTransactions)
		}
		close(release)

		second := runtimeAwaitArtifact(t, emitted)
		secondCommit := <-commits
		if second.Candidate.Empty || second.Candidate.ID.Slot != 3 ||
			second.Candidate.Parent != simplex.Parent(first.Candidate.ID) {
			t.Fatalf("reused future candidate = %+v", second.Candidate)
		}
		if secondCommit.Request.Slot != 3 || secondCommit.Request.MaxTransactions != firstTimeout.Active.MaxTransactions {
			t.Fatalf("reused future commit = slot %d cap %d, want slot 3 cap %d",
				secondCommit.Request.Slot, secondCommit.Request.MaxTransactions, firstTimeout.Active.MaxTransactions)
		}
		if secondCommit.Built == nil || secondCommit.Built.Stats.Transactions != firstTimeout.Active.MaxTransactions {
			t.Fatal("reused commit does not contain the original build")
		}
		if pipeline.buildCount() != 1 {
			t.Fatalf("build future started %d times, want 1", pipeline.buildCount())
		}

		pace := fixture.service.existingPace(session.ID)
		pace.mu.Lock()
		emission, recorded := pace.emitted[second.Candidate.ID]
		pace.mu.Unlock()
		original := firstTimeout.Active
		if original.PaceBudget <= 0 || original.paceRevision == 0 || !original.paceArtificial {
			t.Fatalf("first-slot future has no pacing provenance: %+v", original)
		}
		if !recorded || emission.budget.duration != original.PaceBudget ||
			emission.budget.revision != original.paceRevision || !emission.artificial {
			t.Fatalf("reused future feedback = %+v, recorded %t", emission, recorded)
		}
		if emission.window != second.WindowID || emission.parent != second.Candidate.Parent {
			t.Fatal("reused future lost its published lineage")
		}
	})
}

type runtimePaceWindowCase struct {
	name        string
	slot        uint32
	updateStart uint32
	artificial  bool
}

func TestRuntimePaceArtificialCapPreservesBuildAndArtifactWindow(t *testing.T) {
	cases := []runtimePaceWindowCase{
		{name: "first slot with an older update", slot: 16, updateStart: 0, artificial: true},
		{name: "later slot with a newer update", slot: 17, updateStart: 17, artificial: false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
			defer fixture.close(t)
			session, update := fixture.session(0xa2, 16, 16, time.Now())
			update.TargetRate = 400 * time.Millisecond
			fixture.prepare(t, session, update)
			record, err := fixture.service.Session(t.Context(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			managed, err := fixture.service.runningSession(session.ID)
			if err != nil {
				t.Fatal(err)
			}

			update.CurrentWindowStart = test.updateStart
			commit := runtimePaceCommit(t, fixture, session, update, test.slot, 16)
			err = fixture.service.persistAndEmit(t.Context(), t.Context(), time.Now(), managed,
				record, commit.Artifact, &commit, time.Now(), CandidateKindBlock)
			if err != nil {
				t.Fatal(err)
			}

			pace := fixture.service.existingPace(session.ID)
			pace.mu.Lock()
			emission, recorded := pace.emitted[commit.Artifact.Candidate.ID]
			pace.mu.Unlock()
			if !recorded || emission.artificial != test.artificial {
				t.Fatalf("emission = %+v, recorded %t, want artificial cap %t", emission, recorded, test.artificial)
			}

			before := pace.budget(update.TargetRate)
			fixture.service.ObserveConsensusNotarized(session.ID, commit.Artifact.Candidate.ID, emission.at.Add(40*time.Millisecond))
			if after := pace.budget(update.TargetRate); after != before {
				t.Fatalf("isolated certificate changed capacity without cadence evidence: %+v -> %+v", before, after)
			}
		})
	}
}

func TestRuntimePaceFailedEmitCannotTrainEstimate(t *testing.T) {
	emitErr := errors.New("test delivery failed")
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, func(context.Context, CandidateArtifact) error {
		return emitErr
	})
	defer fixture.close(t)
	session, update := fixture.session(0xa3, 16, 16, time.Now())
	update.TargetRate = 400 * time.Millisecond
	fixture.prepare(t, session, update)
	record, err := fixture.service.Session(t.Context(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	commit := runtimePaceCommit(t, fixture, session, update, 17, 16)
	pace := fixture.service.pace(session, update.TargetRate)
	before := pace.snapshot()

	err = fixture.service.persistAndEmit(t.Context(), t.Context(), time.Now(), managed,
		record, commit.Artifact, &commit, time.Now(), CandidateKindBlock)
	if !errors.Is(err, emitErr) {
		t.Fatalf("persist and emit error = %v, want delivery failure", err)
	}
	pace.mu.Lock()
	pending := len(pace.emitted)
	pace.mu.Unlock()
	if pending != 0 {
		t.Fatalf("failed delivery retained %d pace emissions", pending)
	}

	fixture.service.ObserveConsensusNotarized(session.ID, commit.Artifact.Candidate.ID, time.Now().Add(40*time.Millisecond))
	if after := pace.snapshot(); after != before {
		t.Fatalf("failed delivery trained the estimate: before %+v, after %+v", before, after)
	}
}

type runtimePaceCapacityCase struct {
	name     string
	cap      uint32
	cancel   bool
	deadline bool
}

func TestRuntimePaceCapacityWaitBeforeAcquisition(t *testing.T) {
	cases := []runtimePaceCapacityCase{
		{name: "certificate releases acquisition"},
		{name: "abandoned future never acquires", cancel: true},
		{name: "deadline never acquires", deadline: true},
		{name: "first slot bypasses backlog", cap: firstSlotTransactions},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				built := make(chan BuildRequest, 1)
				pipeline := &runtimeTestPipeline{}
				pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
					built <- request

					return runtimeBuiltCandidate(request), nil
				}
				fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
				defer fixture.close(t)
				session, update := fixture.session(0xa4, 16, 0, time.Now())
				update.TargetRate = 400 * time.Millisecond
				pace := fixture.service.pace(session, update.TargetRate)
				for slot := range uint32(committeePaceOutstandingLimit) {
					pace.noteEmitted(paceCandidate(slot), paceTestEmission(pace, slot, time.Now(), update.TargetRate))
				}
				request := BuildRequest{
					Session: ActivatedSession{Session: session}, Update: update,
					Slot: 4, MaxTransactions: test.cap,
				}
				future := fixture.service.startBuildFuture(t.Context(), request, time.Now().Add(time.Second))
				defer future.stop()
				synctest.Wait()
				if test.cap == 0 && pipeline.buildCount() != 0 {
					t.Fatal("acquired or built while the committee pipeline was full")
				}
				var wantErr error
				switch {
				case test.cancel:
					future.cancel()
					wantErr = context.Canceled
				case test.deadline:
					time.Sleep(time.Second)
					wantErr = context.DeadlineExceeded
				case test.cap == 0:
					fixture.service.ObserveConsensusNotarized(session.ID, paceCandidate(0), time.Now())
				}
				result := <-future.result
				if !errors.Is(result.err, wantErr) {
					t.Fatalf("build error = %v, want %v", result.err, wantErr)
				}
				if wantErr != nil {
					if pipeline.buildCount() != 0 || result.candidate != nil {
						t.Fatal("canceled admission started a build")
					}
					return
				}
				actual := <-built
				if actual.PaceBudget != future.request.PaceBudget || actual.paceRevision != future.request.paceRevision {
					t.Fatal("waiting changed immutable build provenance")
				}
				if pipeline.buildCount() != 1 {
					t.Fatalf("started %d builds, want one", pipeline.buildCount())
				}
			})
		})
	}
}

func runtimePaceCommit(
	t *testing.T,
	fixture *runtimeFixture,
	session Session,
	update SessionUpdate,
	slot uint32,
	windowStart uint32,
) CandidateCommit {
	t.Helper()
	request := BuildRequest{
		Session: ActivatedSession{Session: session},
		Update:  update,
		Slot:    slot,
	}
	if slot == windowStart {
		request.MaxTransactions = firstSlotTransactions
	}
	request.paceOwner = fixture.service.pace(session, update.TargetRate)
	budget := request.paceOwner.budget(update.TargetRate)
	request.PaceBudget = budget.duration
	request.PaceFinishReserve = budget.finishReserve
	request.paceRevision = budget.revision
	request.paceTargetRate = update.TargetRate
	request.paceArtificial = request.MaxTransactions != 0
	built := runtimeBuiltCandidate(request)
	built.Stats.Transactions = 400
	built.Stats.PaceLimited = true
	built.Stats.PaceElapsed = request.PaceBudget
	built.Stats.PaceTail = request.PaceFinishReserve
	if request.paceArtificial {
		built.Stats.Transactions = request.MaxTransactions
	}
	window := productionWindow{
		ID:         WindowID{SessionID: session.ID, StartSlot: windowStart},
		Leader:     0,
		Authority:  CandidateAuthoritySelf,
		SelfSigner: runtimePrivateSigner(fixture.leaderPriv),
	}
	artifact, err := fixture.service.signArtifact(session, window, slot, simplex.Genesis(), built)
	if err != nil {
		t.Fatal(err)
	}

	return CandidateCommit{Request: request, Built: built, Artifact: artifact}
}
