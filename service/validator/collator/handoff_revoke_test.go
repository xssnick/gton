package collator

import (
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A revoke names the block it handed over, and it has to. By the time one
// arrives the slot may already hold a successor started from a later, valid
// offer — the retry that caused the revoke will have produced one — and taking
// that down would turn a recovered rebuild into a lost slot.
func TestSuccessorSlotRevokesOnlyTheOfferItNames(t *testing.T) {
	stale := [32]byte{1}
	fresh := [32]byte{2}

	var slot successorSlot
	resident := &candidateBuildFuture{done: make(chan struct{}), cancel: func() {}}
	close(resident.done)
	if !slot.install(resident, fresh) {
		t.Fatal("the empty slot refused an install")
	}
	if taken := slot.takeIf(stale); taken != nil {
		t.Fatal("a revoke naming one predecessor took down a successor started on another")
	}
	if taken := slot.takeIf(fresh); taken != resident {
		t.Fatal("a revoke naming the resident predecessor did not take it")
	}
	if taken, _ := slot.take(); taken != nil {
		t.Fatal("the slot still holds something after it was taken")
	}
}

// The slot holds one successor and refuses the rest, and closing it takes down
// whatever is resident. Without the refusal a second offer would start a build
// nobody parks, so nobody stops it either.
func TestSuccessorSlotHoldsOneAndClosesForGood(t *testing.T) {
	var slot successorSlot
	first := &candidateBuildFuture{done: make(chan struct{}), cancel: func() {}}
	close(first.done)
	second := &candidateBuildFuture{done: make(chan struct{}), cancel: func() {}}
	close(second.done)

	if !slot.install(first, [32]byte{1}) {
		t.Fatal("the empty slot refused an install")
	}
	if slot.install(second, [32]byte{2}) {
		t.Fatal("an occupied slot accepted a second successor")
	}

	stopped := false
	third := &candidateBuildFuture{done: make(chan struct{}), cancel: func() { stopped = true }}
	close(third.done)
	slot.take()
	if !slot.install(third, [32]byte{3}) {
		t.Fatal("the emptied slot refused an install")
	}
	slot.close()
	if !stopped {
		t.Error("closing the slot left its successor running")
	}
	if slot.install(first, [32]byte{1}) {
		t.Error("a closed slot accepted an install; the window is over and nobody will stop it")
	}
}

// Withdrawing an offer must reach both halves and must happen once. The producer
// half stops the doomed build; the acquisition half drops the queue node that
// build installed for a predecessor no candidate will carry. A second revoke —
// and there are several paths that can raise one — must do neither again.
func TestRevokingAnOfferReachesBothHalvesExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	var revoked []PipelineHandoffOutcome
	var discarded [][32]byte

	port := &successorPort{
		revoke: func(root [32]byte, outcome PipelineHandoffOutcome) {
			mu.Lock()
			revoked = append(revoked, outcome)
			mu.Unlock()
		},
		discard: func(root [32]byte) {
			mu.Lock()
			discarded = append(discarded, root)
			mu.Unlock()
		},
	}

	// Nothing offered yet: a revoke is a no-op rather than a nil root. Nothing
	// is spawned either, so a short settle is enough to say so.
	port.revokeOffered(PipelineHandoffAbandonedFailed)
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if len(revoked) != 0 || len(discarded) != 0 {
		t.Fatal("a port that never offered anything withdrew something")
	}
	mu.Unlock()

	root := [32]byte{7}
	port.noteOffered(root)
	port.revokeOffered(PipelineHandoffAbandonedRetry)
	port.revokeOffered(PipelineHandoffAbandonedFailed)

	// The withdrawal runs on its own goroutine, so the second revoke being a
	// no-op is observed by waiting for the first to land rather than by
	// assuming it already has.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		landed := len(discarded) == 1
		mu.Unlock()
		if landed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the withdrawal never reached the acquisition side")
		}
		time.Sleep(time.Millisecond)
	}
	// And nothing more arrives after it.
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(revoked) != 1 || revoked[0] != PipelineHandoffAbandonedRetry {
		t.Errorf("producer side saw %v, want exactly one abandoned_retry", revoked)
	}
	if len(discarded) != 1 || discarded[0] != root {
		t.Errorf("acquisition side saw %v, want exactly one drop of the offered root", discarded)
	}
}

// A nil port is the ordinary case — every deterministic entry point has one —
// so every method on it has to be safe to call without a check at the call site.
func TestNilSuccessorPortIsInert(t *testing.T) {
	var port *successorPort
	port.noteOffered([32]byte{1})
	port.revokeOffered(PipelineHandoffAbandonedFailed)
}

// What a successor must not be offered is exactly what its predecessor took out
// of the pool's reach, and no more. Both halves of that matter: excluding too
// little makes the successor execute a message twice and lose the feedback,
// excluding too much strands a message for the rest of the window.
//
// The one that is easy to get wrong is the limit skip. It reads like a
// consumption — the message was selected and then not included — but nothing
// happened to it: no TVM attempt, no generation change. It is precisely the
// message the next slot should pick up first.
func TestTheExclusionSetIsWhatThePredecessorConsumed(t *testing.T) {
	hash := func(b byte) [32]byte {
		var out [32]byte
		out[0] = b

		return out
	}
	feedback := []msgpool.ExternalFeedback{
		{Ref: msgpool.ExternalRef{Hash: hash(1)}, Outcome: msgpool.ExternalIncluded},
		{Ref: msgpool.ExternalRef{Hash: hash(2)}, Outcome: msgpool.ExternalInvalid},
		{Ref: msgpool.ExternalRef{Hash: hash(3)}, Outcome: msgpool.ExternalNotAccepted},
		{Ref: msgpool.ExternalRef{Hash: hash(4)}, Outcome: msgpool.ExternalSkippedLimit},
	}

	consumed := consumedExternals(feedback)
	got := map[[32]byte]bool{}
	for _, entry := range consumed {
		got[entry] = true
	}
	for _, want := range []byte{1, 2, 3} {
		if !got[hash(want)] {
			t.Errorf("outcome %d was not excluded; the successor will execute it again and its "+
				"feedback will be dropped as stale", want)
		}
	}
	if got[hash(4)] {
		t.Error("a message skipped for a block limit was excluded; it was never executed and the " +
			"next slot is exactly where it belongs")
	}
	if len(consumed) != 3 {
		t.Errorf("the exclusion set holds %d entries, want 3", len(consumed))
	}
	if consumedExternals(nil) != nil {
		t.Error("an empty feedback set produced a non-nil exclusion set")
	}
}

// An overlap exists only where a successor actually ran. The last slot of every
// window hands over an offer the producer must decline, and counting those as
// overlaps would put a saving in the series on precisely the slots that saved
// nothing — a quarter of them on a four-slot window.
func TestOverlapIsMeasuredOnlyForAnAdoptedOffer(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	declined := &successorPort{}
	declined.noteOffered([32]byte{1})
	if _, handed := declined.overlap(base.Add(time.Second)); handed {
		t.Error("a declined offer reported an overlap; nothing ran beside that block")
	}

	adopted := &successorPort{}
	adopted.noteOffered([32]byte{2})
	adopted.noteAdopted()
	overlap, handed := adopted.overlap(time.Now().Add(50 * time.Millisecond))
	if !handed {
		t.Fatal("an adopted offer reported no overlap")
	}
	if overlap <= 0 {
		t.Errorf("an adopted offer measured %v", overlap)
	}

	// A revoked successor stops counting at the revoke, not at the end of the
	// build it was abandoned by.
	revoked := &successorPort{}
	revoked.noteOffered([32]byte{3})
	revoked.noteAdopted()
	revoked.revokeOffered(PipelineHandoffAbandonedRetry)
	short, handed := revoked.overlap(time.Now().Add(time.Hour))
	if !handed {
		t.Fatal("a revoked offer reported no overlap")
	}
	if short > time.Minute {
		t.Errorf("a revoked successor measured %v; it should stop at the revoke", short)
	}
}

// The withdrawal must not run on the caller. Its caller is the predecessor's own
// collation on the first line of a size-limit rebuild — the slot's last chance —
// and the revoke half joins a build that may be inside a TVM execution. What it
// must still guarantee is the order of its two halves: the queue node is dropped
// only after the successor that would install it has stopped, or the message
// branch is left holding a node for a block that never existed.
func TestRevokingAnOfferDoesNotBlockTheCallerAndKeepsItsOrder(t *testing.T) {
	revokeEntered := make(chan struct{})
	releaseRevoke := make(chan struct{})
	discarded := make(chan struct{})

	port := &successorPort{
		revoke: func([32]byte, PipelineHandoffOutcome) {
			close(revokeEntered)
			<-releaseRevoke
		},
		discard: func([32]byte) { close(discarded) },
	}
	port.noteOffered([32]byte{9})

	returned := make(chan struct{})
	go func() {
		port.revokeOffered(PipelineHandoffAbandonedRetry)
		close(returned)
	}()

	// The caller is back while the revoke is still stuck.
	select {
	case <-revokeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the withdrawal never started")
	}
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("revokeOffered blocked its caller on the successor unwinding")
	}

	// The discard has not run: it waits for the revoke, which is what stops it
	// from overtaking a build still installing the node it is about to drop.
	select {
	case <-discarded:
		t.Fatal("the queue node was dropped before the successor had stopped")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseRevoke)
	select {
	case <-discarded:
	case <-time.After(2 * time.Second):
		t.Fatal("the queue node was never dropped")
	}
}
