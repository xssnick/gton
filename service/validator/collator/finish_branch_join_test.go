package collator

import (
	"context"
	"testing"
	"time"
)

// The block-BOC branch parks on the fence until this collation stops recording,
// and every return inside the section that follows the fork happens while it is
// parked. Both joins are therefore deferred, and their registration order is
// load-bearing: wg.Wait first and the gate second, so LIFO opens the gate and
// only then waits. Registered the other way round, a build that returns between
// the fork and the fence leaves the branch waiting on a channel nobody will ever
// close — the goroutine is lost, and with it the block buffer and the whole cell
// graph it is walking.
//
// A deadlock cannot be asserted, only timed out, so the shape of this test is a
// deadline: cancel at a sweep of instants across the build and require every
// attempt to come back. On the wrong ordering the attempts that land after the
// fork never do.
func TestFinishJoinsTheBlockBranchOnEveryReturn(t *testing.T) {
	// Full collated data, because the validation closure only does real work when
	// the proofs are being built — without the capability it is fourteen
	// microseconds wide and nothing can be timed into it.
	req := benchMainnetCollatedRequest(t, benchMainnetHeavyRepeat)

	// A control build with the assembly recorder attached, so the sweep below
	// aims at the closure rather than at the build. Timing alone cannot find this
	// window: on a small fixture the closure is a fraction of a millisecond and a
	// sweep across the whole build lands in it essentially never — an earlier
	// version of this test swept sixty points and caught nothing when the joins
	// were deliberately reversed.
	// Warmed first: the very first build of a fixture carries its lazy setup and
	// runs a third slower than every one after it, and a sweep aimed with that
	// number lands past the end of the builds it is aiming at.
	if _, err := testBuilder().BuildShard(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var control candidateAssemblyDurations
	timed := req
	timed.assembly = &control
	start := time.Now()
	if _, err := testBuilder().BuildShard(context.Background(), timed); err != nil {
		t.Fatal(err)
	}
	full := time.Since(start)

	closure := control.stages[CollationStageValidationClosure]
	if closure <= 0 {
		t.Fatal("the control build recorded no validation closure; the sweep has nothing to aim at")
	}
	// Everything the build does before the closure opens. The fork is a few
	// statements ahead of it, so this offset is the near edge of the window.
	var before time.Duration
	for stage := CollationStage(0); stage < collationStageCount; stage++ {
		switch stage {
		case CollationStageValidationClosure, CollationStageSerializeCandidate,
			CollationStageFinalizeCandidate:
		default:
			before += control.stages[stage]
		}
	}
	t.Logf("control build %v, closure %v starting around %v", full, closure, before)

	// Twelve points across the closure and a little either side, walked twice.
	// The margin absorbs the run-to-run spread; landing early exercises the fork
	// itself, landing late exercises the fence, and both are returns this test is
	// about.
	var cancelled, completed int
	for i := 0; i < 24; i++ {
		delay := before - closure/2 + closure*time.Duration(i%12)/6
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(delay, cancel)

		done := make(chan error, 1)
		go func() {
			_, err := testBuilder().BuildShard(ctx, req)
			done <- err
		}()

		select {
		case err := <-done:
			if err != nil {
				cancelled++
			} else {
				completed++
			}
		case <-time.After(30 * time.Second):
			cancel()
			t.Fatalf("a build cancelled %v in did not return; the block branch is parked on the "+
				"fence and the deferred joins are registered in the wrong order", delay)
		}
		cancel()
	}

	if cancelled == 0 {
		t.Fatalf("all %d attempts ran to completion; the sweep never cancelled anything and this "+
			"test proves nothing", completed)
	}
	t.Logf("%d cancelled, %d completed", cancelled, completed)
}
