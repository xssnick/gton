package collator

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The collated proof is selected by hash out of the predecessor tree, and the
// selector is fed from two independent places: the read set's record callback,
// which prepare() installs beside the estimator, and recordExecutionRead, which
// files the machine's own first-load reports. A cell that reaches neither is a
// cell the emitted proof omits, and an account's code or data missing from the
// collated proof is what a peer reports back as a pruned branch — the block
// still verifies locally, so nothing here would notice.
//
// These pin the property that makes the two feeds add up to one set: every cell
// the collation recorded is in the selector, marked loaded.

// TestPreparedCollationRecordsEveryReadIntoTheEstimator pins the wiring.
// prepare() opens the estimator and installs the record callback together, so a
// cell entering the record is a cell the selector learns. Opening one without
// the other loses hashes from the proof.
func TestPreparedCollationRecordsEveryReadIntoTheEstimator(t *testing.T) {
	c, err := testBuilder().prepare(context.Background(), fullCollatedMainnetRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if c.collatedProofEstimate == nil {
		t.Fatal("the full-collated request prepared no estimator")
	}

	probe := cell.BeginCell().MustStoreUInt(0x5A, 8).EndCell()
	c.usage.Record(probe)

	if !c.collatedProofEstimate.loadedHashes().loaded(probe.HashKey()) {
		t.Fatal("a cell recorded into the read set did not reach the collated-proof selector")
	}
}

// TestCollatedMainnetBlockKeepsEveryReadCellLoadedInTheEstimator is the same
// invariant over a real block. It stops at finishAccounts because finish()
// seals the read set and empties it, so a caller that needs to see what the
// collation read has to stop there.
func TestCollatedMainnetBlockKeepsEveryReadCellLoadedInTheEstimator(t *testing.T) {
	c := collateThroughAccounts(t, testBuilder(), fullCollatedMainnetRequest(t), newPhaseTimer())
	if c.collatedProofEstimate == nil {
		t.Fatal("the full-collated request prepared no estimator")
	}

	recorded := c.usage.Cells()
	// The fixture records 2089 cells across 234 account lanes. The floor is
	// well under that and only has to reject a run that collated nothing.
	if len(recorded) < 1024 {
		t.Fatalf("the collation recorded %d cells, so the fixture did not exercise the real read paths", len(recorded))
	}

	loaded := c.collatedProofEstimate.loadedHashes()
	missing := 0
	var first cell.Hash
	for _, read := range recorded {
		if !loaded.loaded(read.HashKey()) {
			if missing == 0 {
				first = read.HashKey()
			}
			missing++
		}
	}
	if missing != 0 {
		t.Fatalf("%d of %d recorded cells are absent from the collated-proof selector, first %x",
			missing, len(recorded), first)
	}
}
