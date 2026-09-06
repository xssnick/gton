package collator

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// A capped block reads as full to every admission class below hard the moment it
// reaches its cap, and never to the hard check: the cap bounds what the block
// admits, not what it may weigh, so the hard-overflow diagnosis and the size
// retry must not see it.
func TestTransactionCapCountsAsFullBelowHard(t *testing.T) {
	roomy := blockLimits{
		bytes:        limitThresholds{1 << 29, 1 << 30, 1 << 31, 1 << 32},
		gas:          limitThresholds{1 << 29, 1 << 30, 1 << 31, 1 << 32},
		ltDelta:      limitThresholds{1 << 29, 1 << 30, 1 << 31, 1 << 32},
		collatedData: limitThresholds{1 << 29, 1 << 30, 1 << 31, 1 << 32},
	}
	status := newBlockLimitStatus(roomy, 0, nil, 0, 0)
	status.transactions = 2
	for _, class := range []LoadClass{LoadUnderload, LoadNormal, LoadSoft, LoadMedium, LoadHard} {
		if !status.fits(class) {
			t.Fatalf("an uncapped block with room does not fit %v", class)
		}
	}

	status.maxTransactions = 2
	for _, class := range []LoadClass{LoadUnderload, LoadNormal, LoadSoft, LoadMedium} {
		if status.fits(class) {
			t.Fatalf("a block at its transaction cap still fits %v", class)
		}
	}
	if !status.fits(LoadHard) {
		t.Fatal("the transaction cap reads as a hard overflow")
	}
	if axis, _, _, over := status.hardOverflow(); over {
		t.Fatalf("the transaction cap reports a hard overflow on %q", axis)
	}

	status.transactions = 1
	if !status.fits(LoadNormal) {
		t.Fatal("a block below its transaction cap counts as full")
	}
}

// slotBuildRequest carries the cap it is handed, and the first slot's cap is
// the smaller of firstSlotTransactions and the committee-paced one: the first
// block has to be notarized inside the committee's first-block alarm on its own,
// while every later slot can spend the slack the slots before it left.
func TestFirstSlotBuildRequestCapsTransactions(t *testing.T) {
	record := SessionRecord{
		Update: SessionUpdate{
			CurrentWindowStart:   10,
			CurrentWindowStartAt: time.Unix(1_700_000_000, 0),
			TargetRate:           400 * time.Millisecond,
		},
	}
	window := productionWindow{Leader: 3}
	window.ID.StartSlot = 10
	session := ActivatedSession{}
	parent := simplex.ParentID{Exists: true}

	request := slotBuildRequest(session, record, window, 12, parent, &CandidateArtifact{}, 240, time.Time{})
	if request.MaxTransactions != 240 {
		t.Fatalf("MaxTransactions = %d, want the cap handed in", request.MaxTransactions)
	}
	if got := firstSlotTransactionCap(240); got != firstSlotTransactions {
		t.Fatalf("first-slot cap under a paced cap of 240 = %d, want %d", got, firstSlotTransactions)
	}
	if got := firstSlotTransactionCap(firstSlotTransactions / 2); got != firstSlotTransactions/2 {
		t.Fatalf("first-slot cap under a smaller paced cap = %d, want the paced cap", got)
	}
}
