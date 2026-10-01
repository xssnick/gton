package collator

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A block that is already past the soft limit — full, in the reference's
// sense — does not wait for the slot boundary: it has work the committee could
// be validating, and the candidate leaves the moment it is committed, so the
// wait would be the only thing between a finished block and its broadcast.
// Measured on the stand, the committee sat idle for up to 428 ms between
// notarizing one of our blocks and receiving the next one, which had been ready
// the whole time — and the tail of the window was skipped for want of exactly
// that slack.
//
// The block below the limit keeps the wait; TestBuildShardWithReadyExternalsConsumesAdmissionsUntilSlotBoundary
// pins it. A block with one external is far below the limit, so that fixture is
// also the control here: the same wait, decided by the same class. Here the
// soft limit is one byte, so the fixture's block is full from its first.
func TestBuildShardWithReadyExternalsShipsALoadedBlockWithoutWaiting(t *testing.T) {
	req, _ := benchMainnetRequest(t, 0)
	full := *req.Masterchain.Config
	full.basechain.limits.bytes[LoadUnderload] = 1
	full.basechain.limits.bytes[LoadNormal] = 1
	req.Masterchain.Config = &full
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	stream, err := pool.OpenExternalStream(targetShardIdent(req.Shard), 500)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	// A boundary no test should ever reach: if the phase waits for it, the
	// build takes the whole minute and the assertion on the wait fails first.
	waitUntil := time.Now().Add(time.Minute)
	started := time.Now()
	candidate, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(),
		req,
		stream,
		waitUntil,
		waitUntil,
		500,
		time.Time{},
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions == 0 {
		t.Fatal("fixture built an empty block; it cannot say anything about a loaded one")
	}
	if candidate.Stats.ExternalStop != ExternalStopLoaded {
		t.Fatalf("external stop = %v, want the loaded block to ship without waiting", candidate.Stats.ExternalStop)
	}
	if candidate.Stats.ExternalWait != 0 {
		t.Fatalf("external wait = %s, want none for a loaded block", candidate.Stats.ExternalWait)
	}
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Fatalf("the build took %s, so it waited for the boundary after all", elapsed)
	}
}
