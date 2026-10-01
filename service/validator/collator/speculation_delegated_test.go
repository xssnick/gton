package collator

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// A standalone collator has no window of its own to speak for. Its authority is
// the delegation the leader of that window committed to it, and that delegation
// arrives a whole window ahead — so at the instant the candidate this bets on is
// broadcast, either the authorization is already in receiver memory or the bet
// is not this collator's to place.
//
// The bet costs one collation either way, but an unauthorized one costs more
// than CPU: AcquireShard stamps the block's creator from the leader the request
// carries, and the producer refuses a candidate created by anyone but the leader
// it is producing for. A bet placed without the delegation would therefore
// produce a block that cannot be signed, discovered only at the window it was
// meant to save.
func TestDelegatedSpeculationRequiresADelegationForThatWindow(t *testing.T) {
	const windowSize = 2

	probe := newSpeculationProbe()
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)

		return runtimeBuiltCandidate(request), nil
	}
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, nil)
	defer fixture.close(t)

	session, update := fixture.session(0x84, windowSize, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)

	base := simplex.CandidateID{Slot: windowSize - 1}
	base.Hash[0] = 0x21
	state := runtimeSelectedBase(t, session, base)

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture's canned block runs far ahead of the session's masterchain
	// watermark, which would make the empty-block policy refuse every bet for a
	// reason that has nothing to do with authority.
	managed.policyMu.Lock()
	managed.emptyPolicy.ObserveMCFinalized(state.successor.NextSeqno)
	managed.policyMu.Unlock()

	request := SpeculativeWindowRequest{
		SessionID: session.ID,
		StartSlot: windowSize,
		Leader:    0,
		Base:      state,
		StartAt:   time.Now(),
		Deadline:  time.Now().Add(5 * time.Second),
	}
	if err = fixture.service.SpeculateWindow(context.Background(), request); err != nil {
		t.Fatalf("an unauthorized bet reported an error rather than declining: %v", err)
	}
	if _, _, exists := managed.speculation.pending(); exists {
		t.Fatal("a bet was placed for a window this collator holds no delegation for")
	}

	if err = fixture.service.CommitDelegation(
		context.Background(),
		fixture.request(t, session, windowSize),
	); err != nil {
		t.Fatal(err)
	}
	if err = fixture.service.SpeculateWindow(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	startSlot, parked, exists := managed.speculation.pending()
	if !exists {
		t.Fatal("a delegated window refused the bet placed for it")
	}
	if startSlot != windowSize || parked != base {
		t.Fatalf("parked bet = (slot %d, base %+v), want (slot %d, base %+v)",
			startSlot, parked, windowSize, base)
	}
}
