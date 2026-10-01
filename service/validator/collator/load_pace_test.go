package collator

import (
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
)

// The reference predicates, for reference while reading the table below:
//
//	total > 0.1s && wait < total*0.2 && body > total*0.6  -> overload
//	total > 0.1s && wait < total*0.7 && body > total*0.6  -> underload suppressed
//
// They differ only in the external-wait ratio, and that difference is the whole
// point of keeping two of them.
func TestCollationPaceLongCollation(t *testing.T) {
	now := time.Now()
	pace := func(total, body, wait time.Duration) collationPace {
		return collationPace{
			started:      now.Add(-total),
			body:         now.Add(-body),
			externalWait: func() time.Duration { return wait },
		}
	}
	second := time.Second

	cases := []struct {
		name          string
		pace          collationPace
		wantOverload  bool
		wantUnderload bool
	}{
		{name: "zero value is inert"},
		{
			name: "no external wait and a long body",
			pace: pace(second, 900*time.Millisecond, 0),
			// Both, because zero wait satisfies the strict ratio too.
			wantOverload: true, wantUnderload: true,
		},
		{
			name:          "waiting a third of the slot only suppresses underload",
			pace:          pace(second, 900*time.Millisecond, 333*time.Millisecond),
			wantUnderload: true,
		},
		{
			name: "waiting most of the slot decides nothing",
			pace: pace(second, 900*time.Millisecond, 800*time.Millisecond),
		},
		{
			name: "a short body decides nothing",
			pace: pace(second, 500*time.Millisecond, 0),
		},
		{
			name: "total at the 100ms gate is too short",
			pace: pace(collationPaceMinTotal, collationPaceMinTotal, 0),
		},
		{
			name:         "total just past the gate counts",
			pace:         pace(collationPaceMinTotal+time.Millisecond, collationPaceMinTotal, 0),
			wantOverload: true, wantUnderload: true,
		},
		{
			name:          "external wait exactly at a fifth is not below it",
			pace:          pace(second, 900*time.Millisecond, 200*time.Millisecond),
			wantUnderload: true,
		},
		{
			name: "body exactly at three fifths is not above it",
			pace: pace(second, 600*time.Millisecond, 0),
		},
		{
			name: "external wait exactly at seven tenths decides nothing",
			pace: pace(second, 900*time.Millisecond, 700*time.Millisecond),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			overload, underload := tc.pace.longCollation(now)
			if overload != tc.wantOverload || underload != tc.wantUnderload {
				t.Fatalf(
					"longCollation() = (%t, %t), want (%t, %t)",
					overload, underload, tc.wantOverload, tc.wantUnderload,
				)
			}
		})
	}
}

// TestLoadHistoryLongCollation covers what the predicates do to the histories
// the next state carries. A CPU-bound shard has to end up wanting to split even
// though every block-limit axis stayed normal, and must not be able to claim it
// is idle enough to merge.
func TestLoadHistoryLongCollation(t *testing.T) {
	now := time.Now()
	cpuBound := func(wait time.Duration) collationPace {
		return collationPace{
			started:      now.Add(-time.Second),
			body:         now.Add(-900 * time.Millisecond),
			externalWait: func() time.Duration { return wait },
		}
	}

	cases := []struct {
		name          string
		load          LoadClass
		queueSize     uint64
		pace          collationPace
		wantOverload  uint64
		wantUnderload uint64
		wantReason    OverloadReason
	}{
		{
			name: "cpu bound normal block overloads",
			load: LoadNormal, pace: cpuBound(0),
			wantOverload: 1, wantReason: OverloadLongCollation,
		},
		{
			name: "cpu bound idle block cannot claim underload",
			load: LoadUnderload, pace: cpuBound(0),
			wantOverload: 1, wantReason: OverloadLongCollation,
		},
		{
			name: "middle wait band suppresses underload without overloading",
			load: LoadUnderload, pace: cpuBound(500 * time.Millisecond),
		},
		{
			name: "waiting for externals keeps a real underload",
			load: LoadUnderload, pace: cpuBound(800 * time.Millisecond),
			wantUnderload: 1,
		},
		{
			name: "a queue too big to split blocks the cpu bound overload",
			load: LoadNormal, queueSize: splitMaxQueueSize + 1, pace: cpuBound(0),
		},
		{
			name: "soft load still reports the block limit",
			load: LoadSoft, pace: cpuBound(0),
			wantOverload: 1, wantReason: OverloadBlockLimit,
		},
		{
			name:          "an idle block without a pace still underloads",
			load:          LoadUnderload,
			wantUnderload: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := collation{
				oldStats:  tlb.ShardStateStats{OverloadHistory: 1 << 4, UnderloadHistory: 1 << 5},
				limits:    idleLimitStatus(),
				peakLoad:  tc.load,
				queueSize: tc.queueSize,
				pace:      tc.pace,
			}

			overload, underload, wantSplit, wantMerge := c.loadHistory()
			if overload != 1<<5|tc.wantOverload {
				t.Fatalf("overload history = %#x, want %#x", overload, 1<<5|tc.wantOverload)
			}
			if underload != 1<<6|tc.wantUnderload {
				t.Fatalf("underload history = %#x, want %#x", underload, 1<<6|tc.wantUnderload)
			}
			if c.stats.OverloadReason != tc.wantReason {
				t.Fatalf("overload reason = %s, want %s", c.stats.OverloadReason, tc.wantReason)
			}
			if wantSplit || wantMerge {
				t.Fatalf("unexpected split flags: split=%t merge=%t", wantSplit, wantMerge)
			}
		})
	}
}

// A collation that never installs a pace must produce the same block it did
// before the heuristic existed. Every deterministic entry point relies on this:
// a wall clock inside the produced block would make collation unrepeatable.
func TestBuildShardIgnoresPaceWithoutLiveSchedule(t *testing.T) {
	req := emptyCandidateRequest(t)
	builder := testBuilder()

	c, err := builder.prepare(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !c.pace.started.IsZero() || !c.pace.body.IsZero() || c.pace.externalWait != nil {
		t.Fatal("prepare installed a collation pace")
	}
	overload, underload := c.pace.longCollation(time.Now())
	if overload || underload {
		t.Fatalf("zero pace decided (%t, %t)", overload, underload)
	}
}
