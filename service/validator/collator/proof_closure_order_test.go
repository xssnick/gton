package collator

import (
	"go/ast"
	"slices"
	"strings"
	"testing"
)

// Where the validation-closure replay sits inside finish() is not a matter of
// taste, and the cost of getting it wrong is invisible: the block still
// validates, it is just no longer byte-identical to the reference node's.
//
// The state update is built from the read set's source graph, and that graph
// descends only through cells the read set recorded. A closure read that
// reached the table before the update therefore landed in the update's old
// side — measured at 9 KB of block on the heavy mainnet fixture. That used to
// force the replay to run strictly after buildStateAndBlockParts; today the
// replay overlaps the update instead, and the deferred-recording window is
// what keeps the old invariant: the window opens before the replay starts, so
// its reads land in a buffer while the table the source graph walks stays
// frozen, and the join flushes the buffer before the collated goroutine
// selects the proof out of the table. Both window edges are load-bearing, and
// each has a distinct silent failure: a replay started before the window
// widens the block's state update, and a flush after serializeCollatedData
// starts loses the closure's cells from the collated proof — peers report
// those back as pruned branches.
//
// Collator stays sequential here (prepare_proofs after create_shard_state,
// collator.cpp:6297 vs :5806); the overlap moves when the same reads happen,
// not what they record, and byte identity of block and proof across the
// reorder is pinned by the full-collated goldens on both predecessor shapes.
func TestValidationClosureOverlapKeepsTheWindowBracket(t *testing.T) {
	order := dottedCallOrder(t, "build.go", "finish")

	begin := slices.Index(order, "c.usage.BeginDeferredRecording")
	closure := slices.Index(order, "c.traceValidationClosure")
	join := slices.Index(order, "joinClosure")
	detach := slices.Index(order, "c.usage.Detach")
	serialize := slices.Index(order, "c.serializeCollatedData")
	switch {
	case begin < 0:
		t.Fatalf("finish() no longer opens the deferred-recording window: %v", order)
	case closure < 0:
		t.Fatalf("finish() no longer replays the validation closure: %v", order)
	case closure < begin:
		t.Fatalf(
			"the validation closure starts before the deferred-recording window opens, so its reads widen the block's state update: %v",
			order,
		)
	case join < 0:
		t.Fatalf("finish() no longer joins the overlapped closure: %v", order)
	case detach >= 0 && detach < join:
		t.Fatalf("the recorder is detached before the closure join flushes its reads: %v", order)
	case serialize >= 0 && serialize < join:
		t.Fatalf(
			"collated data is serialized before the closure join, so the replay's cells never reach the collated proof: %v",
			order,
		)
	}
	if !slices.Contains(order, "c.usage.FlushDeferredRecording") {
		t.Fatalf("finish() opens the deferred-recording window but never flushes it: %v", order)
	}
}

// dottedCallOrder returns, in source order, the dotted selector path of every
// call inside fn whose chain bottoms out in plain identifiers — "joinClosure",
// "c.traceValidationClosure", "c.usage.BeginDeferredRecording". Calls hanging
// off expressions (returns of other calls, indexes) are skipped: the pins here
// name methods on the collation and its fields, and those are always reached
// through identifier chains.
func dottedCallOrder(tb testing.TB, file, fn string) []string {
	tb.Helper()

	var names []string
	forEachCall(tb, file, fn, func(call *ast.CallExpr) {
		var parts []string
		expr := call.Fun
		for {
			switch typed := expr.(type) {
			case *ast.Ident:
				parts = append(parts, typed.Name)
				slices.Reverse(parts)
				names = append(names, strings.Join(parts, "."))
				return
			case *ast.SelectorExpr:
				parts = append(parts, typed.Sel.Name)
				expr = typed.X
			default:
				return
			}
		}
	})
	return names
}

// The five parts are one unit, and each one exists because a specific
// validator read has no counterpart in collation. Losing one is silent until a
// validator running on proofs meets a pruned branch, so the composition is
// pinned here rather than left to the call site.
func TestValidationClosureReplaysEveryPart(t *testing.T) {
	want := []string{
		// The gate is in the list because cleanupClaimedLocalDequeues asks the
		// same question, to decide whether to keep the prefix cells this pass
		// records. Inlining the condition here again is how the two drift.
		"closureRecordsPredecessorReads",
		"traceAccountValidationClosure",
		"traceOutQueueValidationClosure",
		"traceImmediateQueueValidationClosure",
		"traceProcessedQueueValidationClosure",
		"traceDispatchQueueValidationClosure",
	}
	if got := methodCallOrder(t, "proof_closure.go", "traceValidationClosure", "c"); !slices.Equal(got, want) {
		t.Fatalf("traceValidationClosure replays %v, want %v", got, want)
	}
	for _, call := range methodCallOrder(t, "execute.go", "finishAccounts", "c") {
		if strings.HasPrefix(call, "trace") {
			t.Fatalf("finishAccounts calls %s before the state update", call)
		}
	}
}
