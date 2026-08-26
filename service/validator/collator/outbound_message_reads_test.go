package collator

import (
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// outboundReadsCollation is the smallest collation that can record: a read set
// and nothing else. recordOutboundMessageReads only ever reaches
// recordExecutionRead, and that writes the read set and, when present, the
// collated-size estimator.
func outboundReadsCollation() *collation {
	return &collation{usage: cell.NewReadSet(nil)}
}

// The walk descends from a detached root so PeekRef stops copying a cell and
// building a trace node for every reference it hands back. What must not change
// is the set of cells it records: the collated proof selects by hash over what
// this recorded, so a cell dropped here is a pruned branch on a peer's side, and
// a cell added here is a proof that no longer matches the reference.
func TestOutboundMessageReadsRecordTheSameSetDetachedOrNot(t *testing.T) {
	deep := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	middle := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(deep).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	root := cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreRef(middle).
		MustStoreRef(shared).
		EndCell()

	want := map[cell.Hash]struct{}{
		root.HashKey():   {},
		middle.HashKey(): {},
		deep.HashKey():   {},
		shared.HashKey(): {},
	}

	// A traced root is what the retire goroutine actually holds: the lane's
	// trace rides on everything the transaction produced.
	traced := root.WithTrace(cell.NewTraceForListener(cell.NewReadSet(nil)))

	for _, test := range []struct {
		name string
		root *cell.Cell
	}{
		{name: "detached", root: root},
		{name: "traced", root: traced},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := outboundReadsCollation()
			c.recordOutboundMessageReads(test.root, false)
			got := map[cell.Hash]struct{}{}
			for hash := range want {
				if c.usage.RecordedCell(hash) != nil {
					got[hash] = struct{}{}
				}
			}
			if len(got) != len(want) {
				t.Fatalf("recorded %d of %d cells", len(got), len(want))
			}
			if c.usage.Size() != len(want) {
				t.Fatalf("read set holds %d cells, want exactly the %d walked", c.usage.Size(), len(want))
			}
		})
	}
}

// The dedup set became scratch shared across the messages of one collation
// rather than a map per message. What makes that safe is that recording is
// idempotent — a cell skipped because a previous message already visited it is a
// cell already in the record — and this pins the consequence: every message's
// cells are in the record afterwards, whichever message first reached the ones
// they share.
func TestOutboundMessageReadsRecordEveryMessageThroughTheSharedScratch(t *testing.T) {
	first := cell.BeginCell().MustStoreUInt(0x61, 8).EndCell()
	second := cell.BeginCell().MustStoreUInt(0x62, 8).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0x63, 8).EndCell()
	firstRoot := cell.BeginCell().MustStoreRef(first).MustStoreRef(shared).EndCell()
	secondRoot := cell.BeginCell().MustStoreRef(second).MustStoreRef(shared).EndCell()

	c := outboundReadsCollation()
	c.recordOutboundMessageReads(firstRoot, false)
	c.recordOutboundMessageReads(secondRoot, false)

	for name, want := range map[string]*cell.Cell{
		"first message leaf":  first,
		"second message leaf": second,
		"shared leaf":         shared,
		"first root":          firstRoot,
		"second root":         secondRoot,
	} {
		if c.usage.RecordedCell(want.HashKey()) == nil {
			t.Fatalf("%s was not recorded", name)
		}
	}
}

func TestGeneratedParallelSafetyRejectsTracedChildren(t *testing.T) {
	trace := cell.NewTraceForListener(cell.NewReadSet(nil))
	child := cell.BeginCell().MustStoreUInt(0x71, 8).EndCell().WithTrace(trace)
	root := cell.BeginCell().MustStoreRef(child).EndCell()

	c := outboundReadsCollation()
	parallelSafe := c.recordOutboundMessageReads(root, true)
	if c.usage.RecordedCell(root.HashKey()) == nil {
		t.Fatal("root was not recorded")
	}
	if c.usage.RecordedCell(child.HashKey()) == nil {
		t.Fatal("traced child was not recorded")
	}
	if parallelSafe {
		t.Fatal("generated safety certification accepted a traced child")
	}
}

func TestGeneratedParallelSafetyRejectsEqualHashTraceWrappers(t *testing.T) {
	loads := 0
	trace := cell.NewTrace(cell.TraceHooks{OnLoad: func(*cell.Cell) { loads++ }})
	raw := cell.BeginCell().MustStoreUInt(0x72, 8).EndCell()
	traced := raw.WithTrace(trace)
	root := cell.BeginCell().MustStoreRef(raw).MustStoreRef(traced).EndCell()

	c := outboundReadsCollation()
	parallelSafe := c.recordOutboundMessageReads(root, true)
	if loads != 0 {
		t.Fatalf("canonical hash-dedup walk notified the second equal-hash wrapper %d times", loads)
	}
	if parallelSafe {
		t.Fatal("generated safety certification accepted a traced equal-hash wrapper")
	}
}

func TestGeneratedParallelSafetyRejectsVirtualizedCells(t *testing.T) {
	branch := cell.BeginCell().
		MustStoreUInt(0xBEEF, 16).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 1).EndCell()).
		EndCell()
	root := cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreRef(branch).
		EndCell()

	proof, err := root.CreateProof(cell.CreateProofSkeleton())
	if err != nil {
		t.Fatalf("create proof: %v", err)
	}
	body, err := cell.UnwrapProofVirtualized(proof, root.Hash())
	if err != nil {
		t.Fatalf("unwrap virtualized proof: %v", err)
	}

	c := outboundReadsCollation()
	if c.recordOutboundMessageReads(body, true) {
		t.Fatal("generated safety certification accepted a virtualized cell")
	}
}

func TestGeneratedParallelSafetyRejectsMessagesBeyondTheWalkDepth(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	for range maxOutboundMessageRecordDepth + 1 {
		root = cell.BeginCell().MustStoreRef(root).EndCell()
	}

	c := outboundReadsCollation()
	if c.recordOutboundMessageReads(root, true) {
		t.Fatal("generated safety certification accepted a message beyond the walk depth")
	}
	if got, want := c.usage.Size(), maxOutboundMessageRecordDepth+1; got != want {
		t.Fatalf("recorded %d cells before the depth bound, want %d", got, want)
	}
}
