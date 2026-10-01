package simplex

import (
	"testing"
	"time"
)

func TestEmptyBlockPolicy(t *testing.T) {
	clock := newFakeClock()
	p := &EmptyBlockPolicy{Clock: clock}

	// Session starts after block 10.
	p.ObserveSessionStart(10)

	// Masterchain rule: an empty block only once consensus finality lags by
	// more than one block behind the produced chain.
	if p.ShouldGenerateEmptyBlock(true, false, 11) {
		t.Fatal("mc: next block right after the finalized one must collate")
	}
	if !p.ShouldGenerateEmptyBlock(true, false, 12) {
		t.Fatal("mc: a two-block finality lag must degrade to empty")
	}
	p.ObserveConsensusFinalized(11)
	if p.ShouldGenerateEmptyBlock(true, false, 12) {
		t.Fatal("mc: finality caught up, must collate")
	}

	// Shard rule: 8 blocks past the masterchain inclusion.
	if p.ShouldGenerateEmptyBlock(false, false, 18) {
		t.Fatal("shard: within the 8-block window must collate")
	}
	if !p.ShouldGenerateEmptyBlock(false, false, 19) {
		t.Fatal("shard: past the 8-block window must degrade to empty")
	}
	p.ObserveMCFinalized(12)
	if p.ShouldGenerateEmptyBlock(false, false, 20) {
		t.Fatal("shard: mc inclusion advanced, must collate")
	}
	// Masterchain finality implies consensus finality.
	if p.LastConsensusFinalizedSeqno != 12 {
		t.Fatalf("consensus watermark = %d, want 12", p.LastConsensusFinalizedSeqno)
	}

	// A pre-split state always produces an empty block.
	if !p.ShouldGenerateEmptyBlock(false, true, 13) {
		t.Fatal("before split must be empty")
	}

	// Failure degradation is allowed only while the session finalized
	// something recently; masterchain observations do not refresh it.
	timeout := 15 * time.Second
	if !p.AllowEmptyOnGenerationFailure(timeout) {
		t.Fatal("fresh finality must allow empty on failure")
	}
	clock.advance(16 * time.Second)
	if p.AllowEmptyOnGenerationFailure(timeout) {
		t.Fatal("stale finality must forbid empty on failure")
	}
	p.ObserveMCFinalized(13)
	if p.AllowEmptyOnGenerationFailure(timeout) {
		t.Fatal("mc finality must not refresh the failure timer")
	}
	p.ObserveConsensusFinalized(13)
	if !p.AllowEmptyOnGenerationFailure(timeout) {
		t.Fatal("consensus finality must refresh the failure timer")
	}
}
