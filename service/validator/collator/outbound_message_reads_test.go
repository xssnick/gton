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
			c.recordOutboundMessageReads(test.root)
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
	c.recordOutboundMessageReads(firstRoot)
	c.recordOutboundMessageReads(secondRoot)

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
