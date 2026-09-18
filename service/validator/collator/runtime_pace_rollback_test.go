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

func TestRuntimePaceReusedFutureKeepsBuildTransactionCap(t *testing.T) {
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
		if secondTimeout.Current.MaxTransactions != adaptiveTransactionStart {
			t.Fatalf("new slot cap = %d, want %d", secondTimeout.Current.MaxTransactions, adaptiveTransactionStart)
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
		if !recorded || emission.transactionCap != firstTimeout.Active.MaxTransactions {
			t.Fatalf("reused future feedback = %+v, recorded %t", emission, recorded)
		}
	})
}

type runtimePaceWindowCase struct {
	name        string
	slot        uint32
	updateStart uint32
	artificial  bool
}

func TestRuntimePaceArtificialCapUsesArtifactWindow(t *testing.T) {
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
			if !recorded || emission.artificialCap != test.artificial {
				t.Fatalf("emission = %+v, recorded %t, want artificial cap %t", emission, recorded, test.artificial)
			}

			fixture.service.ObserveConsensusNotarized(session.ID, commit.Artifact.Candidate.ID, emission.at.Add(40*time.Millisecond))
			cap := pace.transactionCap(update.TargetRate)
			if test.artificial && cap != adaptiveTransactionStart {
				t.Fatalf("first-slot certificate raised cap to %d", cap)
			}
			if !test.artificial && cap <= adaptiveTransactionStart {
				t.Fatalf("natural cap-bound certificate did not allow upward probing: cap %d", cap)
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
	if cap := pace.transactionCap(update.TargetRate); cap != adaptiveTransactionStart {
		t.Fatalf("failed delivery changed transaction cap to %d", cap)
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
		Session:         ActivatedSession{Session: session},
		Update:          update,
		Slot:            slot,
		MaxTransactions: adaptiveTransactionStart,
	}
	if slot == windowStart {
		request.MaxTransactions = firstSlotTransactions
	}
	built := runtimeBuiltCandidate(request)
	built.Stats.Transactions = request.MaxTransactions
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
