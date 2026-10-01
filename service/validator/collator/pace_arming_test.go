package collator

import (
	"testing"
	"time"
)

// The arming rule, stated as a table because each clause switches the heuristic
// off for a different reason and dropping any one of them is a silent change to
// a consensus-visible header bit.
func TestPaceArmsOnlyForABuildThatBeganAtOrAfterItsSlot(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	later := base.Add(time.Second)

	for _, tc := range []struct {
		name      string
		started   time.Time
		scheduled time.Time
		waitUntil time.Time
		want      bool
	}{
		{name: "on time", started: base, scheduled: base, waitUntil: later, want: true},
		{name: "late", started: base.Add(50 * time.Millisecond), scheduled: base, waitUntil: later, want: true},
		{
			name: "handed over early: the ratios have no meaning", started: base,
			scheduled: base.Add(50 * time.Millisecond), waitUntil: later, want: false,
		},
		{
			name:    "no live schedule at all, which is every deterministic entry point",
			started: base, scheduled: base, waitUntil: time.Time{}, want: false,
		},
		{name: "no start instant", started: time.Time{}, scheduled: base, waitUntil: later, want: false},
	} {
		if got := paceArmed(tc.started, tc.scheduled, tc.waitUntil); got != tc.want {
			t.Errorf("%s: armed = %t, want %t", tc.name, got, tc.want)
		}
	}
}

// What disarming buys, stated as the defect it prevents. A build handed over
// before its slot has its body span begin before its total span; measured
// anyway, the three-fifths filter that is supposed to separate a CPU-bound
// collation from an idle one stops separating anything.
func TestAnEarlyBuildWouldDeclareItselfOverloadedIfItWereMeasured(t *testing.T) {
	const (
		lead    = 300 * time.Millisecond
		acquire = 40 * time.Millisecond
		waited  = 30 * time.Millisecond
		collate = 100 * time.Millisecond
	)
	scheduled := time.Unix(1_700_000_000, 0)
	end := scheduled.Add(acquire + waited + collate)

	// Measured from its own start, an early build waited for most of the time it
	// existed — which is what it did, and the heuristic would call that idle.
	honest := collationPace{
		started:      scheduled.Add(-lead),
		body:         scheduled.Add(-lead + acquire),
		externalWait: func() time.Duration { return waited + lead },
	}
	if overload, _ := honest.longCollation(end); overload {
		t.Error("the fixture is CPU-bound even measured honestly; it cannot show the distortion")
	}

	// Measured from the schedule — the clamp this replaced — the body span and
	// the total span become the same interval and the filter passes regardless.
	clamped := collationPace{
		started:      scheduled,
		body:         scheduled,
		externalWait: func() time.Duration { return waited },
	}
	if overload, _ := clamped.longCollation(end); !overload {
		t.Error("the clamped pace did not flip the verdict; this test no longer shows why the " +
			"heuristic is disarmed instead of clamped")
	}

	if paceArmed(scheduled.Add(-lead), scheduled, end) {
		t.Error("the build that produces the distortion above is still armed")
	}
}
