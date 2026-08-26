package collator

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestGeneratedOutputTraceCertification(t *testing.T) {
	req := benchRequest(t, benchProfile{accounts: 16, externals: 8})
	req.internalWaveWorkers = 1

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.processExternals(); err != nil {
		t.Fatal(err)
	}

	var rootTraced, certified int
	for _, message := range c.new {
		if message.root.Trace() != nil {
			rootTraced++
		}
		if message.parallelSafe {
			certified++
		}
	}
	if got := c.new.Len(); got != 8 {
		t.Fatalf("generated messages = %d, want 8", got)
	}
	if rootTraced != 0 {
		t.Fatalf("root-traced generated messages = %d, want 0", rootTraced)
	}
	if certified != 8 {
		t.Fatalf("parallel-certified generated messages = %d, want 8", certified)
	}
}

func TestGeneratedOutputTraceCertificationRejectsTracedRoot(t *testing.T) {
	trace := cell.NewTraceForListener(cell.NewReadSet(nil))
	root := cell.BeginCell().MustStoreUInt(0x71, 8).EndCell().WithTrace(trace)

	c := outboundReadsCollation()
	parallelSafe := c.recordOutboundMessageReads(root, true)
	if c.usage.RecordedCell(root.HashKey()) == nil {
		t.Fatal("traced root was not recorded by the canonical outbound read walk")
	}
	if parallelSafe {
		t.Fatal("generated safety certification accepted a traced root")
	}
}

func TestGeneratedPlanningTraceDoesNotReachSharedReadSet(t *testing.T) {
	queue := nonEmptyDispatchQueue(t, 1_900_000_000, 1_000_000)

	var firstID [32]byte
	for i := range firstID {
		firstID[i] = 0x91
	}
	valueSlice, err := queue.LoadValue(dispatchAccountKey(firstID))
	if err != nil {
		t.Fatal(err)
	}
	value, err := valueSlice.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	shared := cell.NewReadSet(nil)
	queue.AugmentedDictionary.SetTrace(shared.Trace())
	var secondID [32]byte
	for i := range secondID {
		secondID[i] = 0x81
	}
	if err = queue.Set(dispatchAccountKey(secondID), value); err != nil {
		t.Fatal(err)
	}
	before := shared.Size()
	if before == 0 {
		t.Fatal("traced dispatch mutation did not exercise the shared read set")
	}

	c := &collation{dispatchQueue: queue}
	state := c.newGeneratedWavePlanState()
	queued, err := state.queuedDispatch(firstID)
	if err != nil {
		t.Fatal(err)
	}
	if !queued {
		t.Fatal("planning snapshot lost the existing dispatch account")
	}
	if after := shared.Size(); after != before {
		t.Fatalf("planning snapshot wrote shared read set: %d -> %d", before, after)
	}

	scratch := c.generatedWaves.dispatchQueued
	state = c.newGeneratedWavePlanState()
	if len(scratch) != 0 {
		t.Fatalf("dispatch planning scratch retained %d entries", len(scratch))
	}
	queued, err = state.queuedDispatch(firstID)
	if err != nil {
		t.Fatal(err)
	}
	if !queued || !scratch[firstID] {
		t.Fatal("dispatch planning did not reuse its cleared scratch map")
	}
}
