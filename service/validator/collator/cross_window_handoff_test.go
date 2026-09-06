package collator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// recordedCrossWindowBuilds collects every build request a window issued, so a
// test can ask whether the first slot of the NEXT window was started while this
// one was still producing.
type recordedCrossWindowBuilds struct {
	mu       sync.Mutex
	requests []BuildRequest
}

func (r *recordedCrossWindowBuilds) record(request BuildRequest) {
	r.mu.Lock()
	r.requests = append(r.requests, request)
	r.mu.Unlock()
}

func (r *recordedCrossWindowBuilds) forSlot(slot uint32) []BuildRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	var matched []BuildRequest
	for i := range r.requests {
		if r.requests[i].Slot == slot {
			matched = append(matched, r.requests[i])
		}
	}

	return matched
}

func crossWindowFixture(t *testing.T, builds *recordedCrossWindowBuilds) (*runtimeFixture, chan CandidateArtifact) {
	t.Helper()

	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		builds.record(request)

		return runtimeBuiltCandidate(request), nil
	}
	emitted := make(chan CandidateArtifact, 8)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})

	return fixture, emitted
}

// The pipeline used to stop at the window boundary: the last slot's handoff was
// declined with declined_window_end, and the first slot of the next window
// started from nothing. For a standalone collator that is the common case, not
// an edge one — one collator serves several validators of the same group, and
// the leader of window w is w mod len(validators), so consecutive windows land
// on the same collator through different validators all the time.
//
// This asserts the whole path: the successor for the next window's first slot is
// built while this window is still running, it carries the pipelined predecessor
// rather than resolving one, and it is parked under the candidate this window
// actually committed — which is what the next window's producer matches against
// the base consensus selects.
func TestLastSlotHandsOffIntoTheNextDelegatedWindow(t *testing.T) {
	const firstBlockBudget = 700 * time.Millisecond

	builds := &recordedCrossWindowBuilds{}
	fixture, emitted := crossWindowFixture(t, builds)
	defer fixture.close(t)

	session, update := fixture.session(0x71, 2, 0, time.Now())
	fixture.prepare(t, session, update)
	// The delegation for the window after this one, committed while this one
	// runs — the ordering a validator really produces, because it authorizes
	// W+1 when it observes W.
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 2)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}

	first := runtimeAwaitArtifact(t, emitted)
	last := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != 0 || last.Candidate.ID.Slot != 1 {
		t.Fatalf("emitted slots = %d, %d, want 0, 1", first.Candidate.ID.Slot, last.Candidate.ID.Slot)
	}

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var startSlot uint32
	var base simplex.CandidateID
	for {
		slot, parked, exists := managed.speculation.pending()
		if exists {
			startSlot, base = slot, parked

			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the last slot of the window handed nothing over to the next window")
		}
		time.Sleep(time.Millisecond)
	}
	if startSlot != 2 {
		t.Fatalf("parked bet is for slot %d, want the first slot of the next window (2)", startSlot)
	}
	if base != last.Candidate.ID {
		t.Fatalf("parked bet names base %+v, want the candidate this window committed %+v", base, last.Candidate.ID)
	}

	requests := builds.forSlot(2)
	if len(requests) != 1 {
		t.Fatalf("builds for the next window's first slot = %d, want 1", len(requests))
	}
	request := requests[0]
	if request.PreviousPending == nil {
		t.Fatal("the cross-window successor resolved its predecessor instead of taking the one handed to it")
	}
	// It is a wager, and it has to say so. Consensus opening the next window
	// somewhere else drops this build with a cancellation, and a build that does
	// not declare itself a bet reports that as a failed collation: a warning
	// line, a canceled sample on the collation-build histogram sized by the whole
	// lifetime of the bet, and an inflight gauge counting a window this node was
	// never asked to produce.
	if !request.crossWindowBet {
		t.Fatal("the cross-window bet is accounted for as a collation this node owed")
	}
	if request.Leader != 0 {
		t.Fatalf("cross-window build leader = %d, want the next window's leader", request.Leader)
	}
	// The session's own update, unchanged: the acquisition binds the build to
	// the session it is really in, and a guessed window is unrepresentable there.
	if request.Update.CurrentWindowStart != 0 {
		t.Fatalf("cross-window build carries window %d in its update, want the session's own (0)",
			request.Update.CurrentWindowStart)
	}
	// Already expired: this build must not park on the external stream waiting
	// for a window start it only estimated.
	if request.ExternalWaitUntil.After(time.Now()) {
		t.Fatal("the cross-window build waits for externals until an estimated window start")
	}

	// Consensus now opens the predicted window on the candidate the previous
	// window really emitted. The parked build must survive the old producer's
	// teardown, be adopted as slot 2's only build, and reach Emit with the exact
	// predecessor it speculated on.
	next := update
	next.CurrentWindowStart = 2
	next.CurrentWindowObservedSlot = 2
	// Deliberately diverge the newly observed schedule from the estimate under
	// which the build started. Once consensus has opened this window, an already
	// completed first-slot build is due immediately; waiting five seconds here
	// would lose it to the validators' first-block timeout.
	next.CurrentWindowStartAt = time.Now().Add(5 * time.Second)
	next.CurrentBase = simplex.Parent(last.Candidate.ID)
	progress := runtimeConsensusProgress(session, next)
	progress.Base = runtimeSelectedBase(t, session, last.Candidate.ID)
	openedAt := time.Now()
	if err := fixture.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatalf("open next window: %v", err)
	}
	if elapsed := time.Since(openedAt); elapsed > firstBlockBudget {
		t.Fatalf("opening the adopted window took %s, budget %s", elapsed, firstBlockBudget)
	}

	timer := time.NewTimer(time.Until(openedAt.Add(firstBlockBudget)))
	defer timer.Stop()
	var adopted CandidateArtifact
	select {
	case adopted = <-emitted:
	case <-timer.C:
		t.Fatalf("adopted first candidate missed the %s delivery budget", firstBlockBudget)
	}
	if adopted.Candidate.ID.Slot != 2 {
		t.Fatalf("adopted candidate slot = %d, want 2", adopted.Candidate.ID.Slot)
	}
	if adopted.Candidate.Parent != simplex.Parent(last.Candidate.ID) {
		t.Fatalf("adopted candidate parent = %+v, want %+v",
			adopted.Candidate.Parent, simplex.Parent(last.Candidate.ID))
	}
	if requests := builds.forSlot(2); len(requests) != 1 {
		t.Fatalf("slot 2 was rebuilt after adoption: %d builds, want 1", len(requests))
	}
}

// The bet is only legitimate where the authority for it already exists. Without
// a delegation for the next window this collator has no leader to sign as, and
// AcquireShard stamps the creator from the leader the request carries — so a
// build started here would be unsignable rather than merely wasted.
func TestLastSlotHandsOffNothingWithoutTheNextDelegation(t *testing.T) {
	builds := &recordedCrossWindowBuilds{}
	fixture, emitted := crossWindowFixture(t, builds)
	defer fixture.close(t)

	session, update := fixture.session(0x72, 2, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}

	runtimeAwaitArtifact(t, emitted)
	runtimeAwaitArtifact(t, emitted)

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Settle: the handoff runs on the last block's own build goroutine, so a
	// bet placed wrongly would appear shortly after the emission, not with it.
	time.Sleep(100 * time.Millisecond)
	if _, _, exists := managed.speculation.pending(); exists {
		t.Fatal("a window with no delegation for its successor parked a bet on it")
	}
	if requests := builds.forSlot(2); len(requests) != 0 {
		t.Fatalf("builds for an unauthorized next window = %d, want 0", len(requests))
	}
}
