package collator

import "testing"

// Every presize this builder feeds carries a ceiling, and the memo is the one
// that most needs it: it replaced rs.Size() — a count of the cells the current
// build actually read, and therefore self-limiting — with a figure carried over
// from the previous build, which is not.
func TestUpdateMemoHintIsCapped(t *testing.T) {
	b := testBuilder()

	for _, observed := range []int{0, 1, 4686, updateMemoMaxPresizedCells, 64 * updateMemoMaxPresizedCells} {
		b.observeBuildSizes(0, 0, 0, observed)

		hint := b.updateMemoHint()
		if hint > updateMemoMaxPresizedCells {
			t.Fatalf("a memo of %d cells hinted %d, above the %d ceiling",
				observed, hint, updateMemoMaxPresizedCells)
		}
		// Below the ceiling the hint must still be the measured figure with the
		// eighth on top, or the cap has eaten the sizing it exists to bound.
		if want := observed + observed/8; want <= updateMemoMaxPresizedCells && hint != want {
			t.Fatalf("a memo of %d cells hinted %d, want %d", observed, hint, want)
		}
	}
}

// The four hints are one observation of one finished build. A build that reports
// zero for a field is reporting that the field's structure was not filled, and
// the next build must be sized from that report — not from a figure left behind
// by a build two ago for one field while the other three are reset.
func TestObserveBuildSizesTracksEveryFieldOfTheLastBuild(t *testing.T) {
	b := testBuilder()

	b.observeBuildSizes(9367, 5297, 3222, 4686)
	if got := b.readSetHint(); got != 9367 {
		t.Fatalf("read set hint %d, want 9367", got)
	}
	cells, proofCells := b.storageHints()
	if cells != 5297 || proofCells != 3222 {
		t.Fatalf("storage hints %d/%d, want 5297/3222", cells, proofCells)
	}
	if got := b.updateMemoHint(); got != 4686+4686/8 {
		t.Fatalf("memo hint %d, want %d", got, 4686+4686/8)
	}

	b.observeBuildSizes(0, 0, 0, 0)
	if got := b.readSetHint(); got != 0 {
		t.Fatalf("read set hint %d after a build that read nothing, want 0", got)
	}
	if cells, proofCells = b.storageHints(); cells != 0 || proofCells != 0 {
		t.Fatalf("storage hints %d/%d after a build that walked nothing, want 0/0", cells, proofCells)
	}
	if got := b.updateMemoHint(); got != 0 {
		t.Fatalf("memo hint %d after a build that memoised nothing, want 0 — "+
			"the field kept a stale hint while the other three were reset", got)
	}
}
