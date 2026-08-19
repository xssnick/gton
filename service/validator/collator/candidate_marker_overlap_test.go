package collator

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestCandidateMarkerCommitOverlapsTheStateCommit is the F4 change.
//
// The anti-equivocation marker is one synced commit — measured at 4.88 ms per
// candidate against 4.87 ms for a bare synced Pebble commit on the same device,
// so essentially all of it is the fsync — and the producer used to sit on it
// before it did anything else. What the marker has to be earlier than is the
// broadcast, not the state commit that follows building the block, so the write
// is now submitted where it was and collected immediately before the candidate
// is emitted. The reference does not wait for its equivalent at all: the
// collator producer detaches its store (simplex/collator-producer.cpp:81) and
// consensus.cpp starts one at :228 and collects it at :252, after the parent
// wait, the state resolve, the block interval and a whole validation.
//
// This asserts the overlap. The property it must not have cost —
// no emission before the marker is durable — is pinned separately and
// unchanged by TestRuntimeSelfCandidateMarkerPrecedesEmission.
func TestCandidateMarkerCommitOverlapsTheStateCommit(t *testing.T) {
	baseStorage := newRuntimeMemoryStorage()
	storage := &runtimeLateCandidateSaveStorage{
		runtimeMemoryStorage: baseStorage,
		saved:                make(chan CandidateRecord, 1),
		release:              make(chan struct{}),
	}

	var mu sync.Mutex
	committed := make(chan struct{})
	var committedOnce sync.Once
	pipeline := &runtimeTestPipeline{}
	pipeline.commit = func(context.Context, CandidateCommit) error {
		mu.Lock()
		defer mu.Unlock()
		committedOnce.Do(func() { close(committed) })

		return nil
	}

	emitted := make(chan CandidateArtifact, 1)
	fixture := newRuntimeSelfFixture(
		t,
		pipeline,
		storage,
		baseStorage,
		func(_ context.Context, artifact CandidateArtifact) error {
			emitted <- artifact

			return nil
		},
	)
	defer func() {
		storage.releaseCallback()
		fixture.close(t)
	}()

	session, update := fixture.session(0x76, 1, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.ActivateSelfWindow(
		context.Background(),
		fixture.selfRequest(session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}

	select {
	case <-storage.saved:
	case <-time.After(5 * time.Second):
		t.Fatal("candidate marker was not submitted")
	}

	// The marker's callback is still withheld. The state commit must have run
	// anyway — that is the whole point — and nothing may have been emitted.
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("state commit waited for the candidate marker's synced commit")
	}
	select {
	case artifact := <-emitted:
		t.Fatalf("candidate emitted while its marker was not durable: %+v", artifact)
	default:
	}

	storage.releaseCallback()
	runtimeAwaitArtifact(t, emitted)
}
