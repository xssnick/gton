package validator

import (
	"testing"
	"time"
)

// A preparation that fails while every other preparation slot is taken leaves a
// retry deadline the supervisor cannot act on until a slot frees, and a freed
// slot arrives as a preparation result that reconciles by itself. Arming the
// timer for that deadline fired it at once and forever: every pass skipped the
// session on the preparation limit and re-armed a zero delay, spinning the
// goroutine that owns every session for as long as the other preparations ran.
func TestSessionRetryTimerWaitsForAFreePreparationSlot(t *testing.T) {
	desired := make(map[sessionActorID]desiredSession)
	preparing := make(map[sessionActorID]*sessionPreparation)
	for i := range maxConcurrentSessionPreparations {
		id := sessionActorID{SessionID: [32]byte{byte(i + 1)}}
		desired[id] = desiredSession{}
		preparing[id] = &sessionPreparation{}
	}
	failed := sessionActorID{SessionID: [32]byte{0xf0}}
	desired[failed] = desiredSession{}
	retryAt := map[sessionActorID]sessionRetry{failed: {at: time.Now().Add(-time.Second), prepareFailures: 1}}
	managed := make(map[sessionActorID]*managedConsensusSession)

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	if retryC := resetSessionRetryTimer(timer, desired, managed, preparing, retryAt); retryC != nil {
		t.Fatal("retry timer was armed for a session the preparation limit holds back")
	}

	// A close retry does not wait for a preparation slot and stays armed.
	closing := sessionActorID{SessionID: [32]byte{0xf1}}
	managed[closing] = &managedConsensusSession{closeRetryAt: time.Now().Add(time.Hour)}
	if retryC := resetSessionRetryTimer(timer, desired, managed, preparing, retryAt); retryC == nil {
		t.Fatal("close retry was not armed while the preparation limit was reached")
	}
	delete(managed, closing)

	// A freed slot makes the same overdue preparation retry actionable again.
	for id := range preparing {
		delete(preparing, id)

		break
	}
	retryC := resetSessionRetryTimer(timer, desired, managed, preparing, retryAt)
	if retryC == nil {
		t.Fatal("overdue preparation retry was not armed once a slot freed")
	}
	select {
	case <-retryC:
	case <-time.After(time.Second):
		t.Fatal("overdue preparation retry did not fire")
	}
}
