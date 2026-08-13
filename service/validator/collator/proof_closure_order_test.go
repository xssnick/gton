package collator

import (
	"slices"
	"testing"
)

// Where the validation-closure replay sits inside finish() is not a matter of
// taste, and the cost of getting it wrong is invisible: the block still
// validates, it is just bigger than the reference node's and no longer
// byte-identical to it.
//
// The state update is built from the read set's source graph, and that graph
// descends only through cells the read set recorded. A closure read taken
// before the update therefore lands in the update's old side. Measured on the
// heavy mainnet fixture, moving these three calls after buildStateAndBlockParts
// took the block from 678703 to 669409 bytes with the collated data unchanged
// at 421534 — the same proof, 9 KB less block.
//
// Collator has the same split for the same reason: prepare_proofs is called
// from create_collated_data (collator.cpp:6297), long after create_shard_state
// generated the update (:5806).
func TestValidationClosureRunsAfterTheStateUpdate(t *testing.T) {
	order := methodCallOrder(t, "build.go", "finish", "c")

	update := slices.Index(order, "buildStateAndBlockParts")
	closure := slices.Index(order, "traceValidationClosure")
	switch {
	case update < 0:
		t.Fatalf("finish() no longer calls buildStateAndBlockParts: %v", order)
	case closure < 0:
		t.Fatalf("finish() no longer replays the validation closure: %v", order)
	case closure < update:
		t.Fatalf(
			"traceValidationClosure runs before buildStateAndBlockParts, so its reads widen the block's state update: %v",
			order,
		)
	}
	// The proof is selected after the collated goroutine starts, so the replay
	// has to be finished by then or its cells never reach collated data.
	if serialize := slices.Index(order, "serializeCollatedData"); serialize >= 0 && serialize < closure {
		t.Fatalf("traceValidationClosure runs after collated data is serialized: %v", order)
	}
}

// The three parts are one unit, and each one exists because a specific
// validator read has no counterpart in collation. Losing one is silent until a
// validator running on proofs meets a pruned branch, so the composition is
// pinned here rather than left to the call site.
func TestValidationClosureReplaysEveryPart(t *testing.T) {
	want := []string{
		"traceAccountValidationClosure",
		"traceOutQueueValidationClosure",
		"traceImmediateQueueValidationClosure",
		"traceDispatchQueueValidationClosure",
	}
	if got := methodCallOrder(t, "proof_closure.go", "traceValidationClosure", "c"); !slices.Equal(got, want) {
		t.Fatalf("traceValidationClosure replays %v, want %v", got, want)
	}
}
