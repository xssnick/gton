package collator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// countingExternalSource wraps the real stream and counts how often the
// collation reaches into it. The count is the test's whole point: a closed
// intake must not touch the pool at all, and every observable the skip path
// used to produce — snapshots, parses, prewarms — started with one of these
// calls.
type countingExternalSource struct {
	inner readyExternalSource
	mu    sync.Mutex
	take  int
	next  int
}

func (s *countingExternalSource) TakeReady(limit int) []msgpool.ExternalSnapshot {
	s.mu.Lock()
	s.take++
	s.mu.Unlock()
	return s.inner.TakeReady(limit)
}

func (s *countingExternalSource) Next(ctx context.Context, limit int) ([]msgpool.ExternalSnapshot, error) {
	s.mu.Lock()
	s.next++
	s.mu.Unlock()
	return s.inner.Next(ctx, limit)
}

func (s *countingExternalSource) calls() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.take, s.next
}

// brakeClosedFixture is a deep-queue request beside a real pool holding ready
// messages for an account that exists in that request's state.
func brakeClosedFixture(t *testing.T, ready int) (ShardRequest, *msgpool.Pool, *countingExternalSource) {
	t.Helper()
	req := queueDepthRequest(t, int(skipExternalsQueueSize)+1)
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	for i := 0; i < ready; i++ {
		addReadyExternal(t, pool, predecessorAddress(0x11), uint64(0x9100+i))
	}
	stream, err := pool.OpenExternalStream(targetShardIdent(req.Shard), 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return req, pool, &countingExternalSource{inner: stream}
}

// TestClosedIntakeSkipsPreAdmittedBatchWithoutPrewarm pins the hoisted batch
// skip in processExternalBatch: a batch that starts under a closed intake gets
// its verdicts straight away — no account prewarm, no wave planning. The
// verdicts are identical either way, so this is the one gate that can tell the
// shortcut from the per-message loop it replaced.
func TestClosedIntakeSkipsPreAdmittedBatchWithoutPrewarm(t *testing.T) {
	req, _, source := brakeClosedFixture(t, 0)
	warmer := &recordedAccountPrewarmer{}
	req.accountPrewarmer = warmer
	if len(req.Externals) == 0 {
		t.Fatal("fixture carries no pre-admitted external, the hoist has nothing to skip")
	}

	candidate, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(), req, source, time.Time{}, time.Time{}, 4, time.Time{}, time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.ExternalSkippedLimit != uint32(len(req.Externals)) {
		t.Fatalf("skips = %d, want the whole pre-admitted batch of %d",
			candidate.Stats.ExternalSkippedLimit, len(req.Externals))
	}
	if warmer.immediate != 0 || len(warmer.accounts) != 0 {
		t.Fatalf("a closed intake prewarmed %d account(s) for messages it was always going to skip",
			warmer.immediate+len(warmer.accounts))
	}
}

// TestClosedIntakeLeavesThePoolUntouched pins the cost model of the brake. On
// the loaded stand the outbound queue sits at the brake for the whole load
// period, and every zero-external block was still snapshotting, parsing and
// account-prewarming the entire pool — thousands of messages — only to record
// a skip verdict for each and leave them pooled for the next block to pay
// again. A closed intake now takes nothing: no TakeReady call, no consumed
// generation, no feedback, and the stop reason says why the phase carried
// nothing instead of reporting an ordinary drain.
func TestClosedIntakeLeavesThePoolUntouched(t *testing.T) {
	const ready = 3
	req, pool, source := brakeClosedFixture(t, ready)
	// The fixture's own pre-admitted external goes through the hoisted batch
	// skip — that path must still record its verdict, because those messages
	// were already taken from the pool by acquisition.
	preAdmitted := uint32(len(req.Externals))

	candidate, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(), req, source, time.Time{}, time.Time{}, 4, time.Time{}, time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if take, _ := source.calls(); take != 0 {
		t.Fatalf("a closed intake called TakeReady %d times; the pool must stay untouched", take)
	}
	if candidate.Stats.ExternalStop != ExternalStopQueueBrake {
		t.Fatalf("external stop = %v, want queue_brake", candidate.Stats.ExternalStop)
	}
	if candidate.Stats.ExternalIncluded != 0 {
		t.Fatalf("closed intake included externals: %+v", candidate.Stats)
	}
	// The one counted batch is the fixture's pre-admitted message: acquisition
	// already took it from the pool, so it must reach processing and get its
	// skip verdict. Only that batch — nothing came from the stream.
	if candidate.Stats.ExternalBatches != 1 {
		t.Fatalf("batches = %d, want only the pre-admitted one", candidate.Stats.ExternalBatches)
	}
	if candidate.Stats.ExternalSkippedLimit != preAdmitted {
		t.Fatalf("skips = %d, want exactly the %d pre-admitted message(s)",
			candidate.Stats.ExternalSkippedLimit, preAdmitted)
	}
	if pooled := pool.Stats().Pooled; pooled != ready {
		t.Fatalf("pool holds %d messages after a closed intake, want the untouched %d", pooled, ready)
	}
}

// TestClosedIntakeSkipsWaitedAdmissionsWithoutPreparingThem covers the wait
// branch: a message admitted during the slot still reaches the build through
// Next, and the closed intake answers it with a skip recorded from the
// snapshot's reference alone — no parse, no prewarm, no consumed batch.
func TestClosedIntakeSkipsWaitedAdmissionsWithoutPreparingThem(t *testing.T) {
	req, pool, source := brakeClosedFixture(t, 0)
	preAdmitted := uint32(len(req.Externals))
	waitUntil := time.Now().Add(250 * time.Millisecond)

	type buildResult struct {
		candidate *Candidate
		err       error
	}
	result := make(chan buildResult, 1)
	go func() {
		candidate, _, err := testBuilder().buildShardWithReadyExternals(
			t.Context(), req, source, waitUntil, waitUntil, 4, time.Time{}, time.Time{},
		)
		result <- buildResult{candidate: candidate, err: err}
	}()

	time.Sleep(40 * time.Millisecond)
	addReadyExternal(t, pool, predecessorAddress(0x11), 0x9201)

	select {
	case built := <-result:
		if built.err != nil {
			t.Fatal(built.err)
		}
		if built.candidate.Stats.ExternalStop != ExternalStopQueueBrake {
			t.Fatalf("external stop = %v, want queue_brake", built.candidate.Stats.ExternalStop)
		}
		// The pre-admitted batch counts as before; the waited admission must
		// not — it was skipped from its snapshot without entering processing.
		if built.candidate.Stats.ExternalBatches != 1 {
			t.Fatalf("batches = %d, want only the pre-admitted one", built.candidate.Stats.ExternalBatches)
		}
		if built.candidate.Stats.ExternalSkippedLimit != preAdmitted+1 {
			t.Fatalf("skips = %d, want pre-admitted %d plus the one waited admission",
				built.candidate.Stats.ExternalSkippedLimit, preAdmitted)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closed-intake collation did not finish")
	}
}

// TestOpenIntakeStillConsumesReadyExternals is the complement that keeps the
// shortcut honest: with the queue below the brake the same fixture shape must
// take from the pool and include the message, or the "optimization" would be
// quietly declining externals on healthy blocks.
func TestOpenIntakeStillConsumesReadyExternals(t *testing.T) {
	req, pool, stream := readyExternalFixture(t, externalAcceptCode(t), 4)
	addReadyExternal(t, pool, readyExternalAddress(), 0x9301)
	source := &countingExternalSource{inner: stream}

	candidate, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(), req, source, time.Time{}, time.Time{}, 4, time.Time{}, time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if take, _ := source.calls(); take == 0 {
		t.Fatal("an open intake never called TakeReady")
	}
	if candidate.Stats.ExternalIncluded != 1 {
		t.Fatalf("open intake included %d externals, want 1", candidate.Stats.ExternalIncluded)
	}
	if candidate.Stats.ExternalStop == ExternalStopQueueBrake {
		t.Fatal("an open intake reported the queue brake")
	}
}
