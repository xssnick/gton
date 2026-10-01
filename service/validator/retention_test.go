package validator

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// Every bound in retention.go is a wall-clock or byte budget converted at the
// session's own slot rate. This pins both ends of the conversion: the protocol
// default, which is what mainnet gets and what the ported reasoning was written
// for, and the 200 ms of the test network, which is where writing these bounds
// as slot counts went wrong by a factor of twelve.
func TestRetentionBoundsAreDerivedFromTheSessionSlotRate(t *testing.T) {
	const (
		mainnetRate = 2400 * time.Millisecond
		fastRate    = 200 * time.Millisecond
	)
	if got := simplex.DefaultParams().TargetRate; got != mainnetRate {
		t.Fatalf("protocol default target rate = %v, want %v: the derivations below are pinned to it", got, mainnetRate)
	}

	budget := defaultRetentionBudget(false)
	if got := budget.capSlots(mainnetRate); got != 250 {
		t.Fatalf("retention backstop at %v = %d slots, want 250 (10 min)", mainnetRate, got)
	}
	if got := budget.capSlots(fastRate); got != 3000 {
		t.Fatalf("retention backstop at %v = %d slots, want 3000 (10 min)", fastRate, got)
	}
	if budget.Bytes != retentionFloorCapBytes || budget.Payloads != retentionFloorCapPayloads {
		t.Fatalf("shard retention budget = %+v", budget)
	}
	if master := defaultRetentionBudget(true); master.Bytes != retentionFloorCapMasterchainBytes {
		t.Fatalf("masterchain retention budget bytes = %d, want %d",
			master.Bytes, retentionFloorCapMasterchainBytes)
	}

	// The candidate payload margin is the shard-top inclusion delay, which is a
	// wall-clock quantity: 4 slots at the protocol default and 40 on a 200 ms
	// network, both of which are 8 s and neither of which is a slot count carried
	// over from another rate. TestCandidateMarginIsTheDurationItClaims is what
	// makes that a property rather than two literals.
	if got := candidateRetainedSlots(mainnetRate); got != 4 {
		t.Fatalf("candidate margin at %v = %d slots, want 4 (8 s)", mainnetRate, got)
	}
	if got := candidateRetainedSlots(fastRate); got != 40 {
		t.Fatalf("candidate margin at %v = %d slots, want 40 (8 s)", fastRate, got)
	}

	// The resolved-parent margin absorbs one leader window, and a leader window
	// is a configured number of slots rather than four.
	for spw, want := range map[uint32]uint32{0: 1, 1: 2, 4: 5, 16: 17} {
		if got := stateRetainedSlots(spw); got != want {
			t.Fatalf("state margin for %d slots per window = %d, want %d", spw, got, want)
		}
	}

	if got := resolverCacheLogPeriod(mainnetRate); got != 25 {
		t.Fatalf("resolver cache log period at %v = %d slots, want 25 (1 min)", mainnetRate, got)
	}
	if got := resolverCacheLogPeriod(fastRate); got != 300 {
		t.Fatalf("resolver cache log period at %v = %d slots, want 300 (1 min)", fastRate, got)
	}
}

// The margin's unit is wall clock, so the property to assert is a DURATION and
// not a slot count: converting it at any rate and multiplying back must land in
// [candidateRetentionMarginDuration, + one slot), the rounding-up interval.
//
// The old test asserted candidateRetainedSlots(mainnetRate) ==
// candidateCacheRetainedSlots, which is true for any floor whatsoever, because
// the constant under test was also the floor handed to the conversion. It
// therefore certified nothing and passed while mainnet's effective margin was
// 16 × 2400 ms = 38.4 s against a constant that says 8 s. This test fails on
// that value at every rate where the floor exceeds the duration.
func TestCandidateMarginIsTheDurationItClaims(t *testing.T) {
	// Every rate at or below the protocol default, which is every rate this
	// project runs. The duration must bind here and the floor must not be
	// reachable.
	for _, rate := range []time.Duration{
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		2400 * time.Millisecond,
	} {
		slots := candidateRetainedSlots(rate)
		got := time.Duration(slots) * rate
		if got < candidateRetentionMarginDuration || got >= candidateRetentionMarginDuration+rate {
			t.Fatalf("candidate margin at %v = %d slots = %v, want inside [%v, %v): the margin is a "+
				"wall-clock bound and a slot floor is overriding it",
				rate, slots, got, candidateRetentionMarginDuration, candidateRetentionMarginDuration+rate)
		}
	}

	// Slower than the protocol default the leader-window floor takes over, which
	// is its stated purpose and the only regime where it may. It must never
	// SHORTEN the margin, so what it yields there is longer than the duration.
	for _, rate := range []time.Duration{5 * time.Second, time.Minute} {
		slots := candidateRetainedSlots(rate)
		if slots != candidateCacheRetainedSlots {
			t.Fatalf("candidate margin at %v = %d slots, want the leader-window floor %d",
				rate, slots, candidateCacheRetainedSlots)
		}
		if got := time.Duration(slots) * rate; got < candidateRetentionMarginDuration {
			t.Fatalf("candidate margin at %v = %v, below the %v it claims",
				rate, got, candidateRetentionMarginDuration)
		}
	}

	// And the floor itself must not be able to take over from the duration at the
	// rate mainnet runs, which is the specific way this went wrong.
	if want := candidateRetainedSlots(simplex.DefaultParams().TargetRate); candidateCacheRetainedSlots != want {
		t.Fatalf("the protocol-default margin is %d slots but the floor constant says %d",
			want, candidateCacheRetainedSlots)
	}
}

// The validation deadline has to outlive the parent-state wait it mostly
// consists of, and stay inside the retention that wait resumes into. Both
// bounds are wall clock, and both have been violated by a version of this
// constant: 60 s derived into 6.4 s of leader windows abstained on states the
// field measured arriving 40 s in, and anything above the retention backstop
// would wait for a lineage the sweeps have already released.
func TestCandidateValidationTimeoutOutlivesTheFieldParentStateWait(t *testing.T) {
	// The longest basechain standstill measured on the 3-node test network, and
	// the p99 apply lag inside it. A candidate whose parent state arrives at the
	// far end of either must still be able to get this node's vote.
	const (
		worstMeasuredStandstill = 168 * time.Second
		p99ApplyLag             = 132 * time.Second
	)

	if candidateValidationTimeout < worstMeasuredStandstill {
		t.Fatalf("validation timeout %v does not outlive the worst measured standstill %v",
			candidateValidationTimeout, worstMeasuredStandstill)
	}
	if candidateValidationTimeout < p99ApplyLag {
		t.Fatalf("validation timeout %v does not outlive the p99 apply lag %v",
			candidateValidationTimeout, p99ApplyLag)
	}
	if candidateValidationTimeout > retentionFloorCapDuration {
		t.Fatalf("validation timeout %v outlives the retention backstop %v, so it would wait "+
			"for a lineage the sweeps have released", candidateValidationTimeout, retentionFloorCapDuration)
	}
}

// A conversion that cannot compute an answer must not answer "the maximum".
// Every caller spends the result on retention, so failing open meant a session
// with an unusable slot rate held 4096 slots of candidate payloads, of resolved
// parent states and of applied states at once — the largest possible answer to
// a question with no answer. The floor costs at worst a lineage walk that reads
// from storage.
func TestSlotsForDurationFailsTowardTheMinimum(t *testing.T) {
	for _, rate := range []time.Duration{0, -time.Second} {
		if got := slotsForDuration(time.Minute, rate, 7, 4096); got != 7 {
			t.Fatalf("slots for a %v rate = %d, want the floor 7", rate, got)
		}
	}

	// The derived bounds that ride on it inherit the same direction.
	if got := candidateRetainedSlots(0); got != candidateCacheRetainedSlots {
		t.Fatalf("candidate margin at an unusable rate = %d, want the floor %d",
			got, candidateCacheRetainedSlots)
	}
	if got := resolverCacheLogPeriod(0); got != 1 {
		t.Fatalf("resolver cache log period at an unusable rate = %d, want the floor 1", got)
	}
	budget := defaultRetentionBudget(false)
	if got := budget.capSlots(0); got != retentionFloorMinCapSlots {
		t.Fatalf("retention backstop at an unusable rate = %d, want the floor %d",
			got, retentionFloorMinCapSlots)
	}
}
