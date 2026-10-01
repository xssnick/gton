package collator

import (
	"context"
	"testing"
	"time"
)

// A self window that ends on a terminal error is never produced again, and its
// receiver record is dropped with it, so reconcileWindows cannot find it later.
// The payloads it remembered for a relaunch must be released with the record;
// otherwise every slot it emitted stays on the heap until this session's next
// own window.
func TestTerminalSelfWindowReleasesItsEmittedPayloads(t *testing.T) {
	emitted := make(chan CandidateArtifact, 2)
	fixture := newRuntimeSelfFixture(t, nil, nil, nil, func(_ context.Context, artifact CandidateArtifact) error {
		if artifact.Candidate.ID.Slot == 1 {
			// What the consensus router answers once the leader window has closed.
			return ErrStaleWindow
		}
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(0x6e, 2, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
	runtimeAwaitArtifact(t, emitted)

	managed, err := fixture.service.runningSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	id := WindowID{SessionID: session.ID, StartSlot: 0}
	deadline := time.Now().Add(2 * time.Second)
	for {
		managed.mu.Lock()
		running := managed.productions[id] != nil
		_, pending := managed.selfWindows[id]
		managed.mu.Unlock()
		if !running {
			if pending {
				t.Fatal("the self window did not end terminally; this test says nothing about its release")
			}

			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the self window did not end")
		}
		time.Sleep(time.Millisecond)
	}

	if _, found := managed.recallEmitted(id, 0); found {
		t.Fatal("a terminally ended self window still holds the payloads it emitted")
	}
}

// A production cancelled by the session lifecycle stays pending and may be
// launched again, and a relaunch without its payloads would end the window at
// its first slot already signed. Releasing on a terminal result must not reach
// this case.
func TestCancelledProductionKeepsItsEmittedPayloads(t *testing.T) {
	id := WindowID{StartSlot: 4}
	artifact := CandidateArtifact{WindowID: id}
	artifact.Candidate.ID.Slot = 4
	managed := &managedCollatorSession{
		selfWindows: map[WindowID]SelfWindowRequest{id: {}},
		productions: map[WindowID]*productionJob{id: {}},
	}
	managed.rememberEmitted(id, artifact)

	managed.releaseProduction(id, context.Canceled)
	if _, found := managed.recallEmitted(id, 4); !found {
		t.Fatal("a cancelled production lost the payloads its relaunch resumes from")
	}
}
