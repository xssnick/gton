package collator

import (
	"context"
	"errors"
	"testing"
)

// A block that overflows the consensus size limit used to end the slot with no
// block at all: the check runs on the serialized candidate, and the production
// loop would have retried the identical collation forever. Collation now rebuilds
// itself a bounded number of times with a narrower byte budget, so the slot gets
// a smaller block instead of nothing.
func TestBuildShardRebuildsUnderSizeLimit(t *testing.T) {
	req, _ := benchMainnetRequest(t, 0)
	natural, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("baseline collation: %v", err)
	}

	// Just under what this workload naturally produces, so the first attempt
	// overflows and a narrower one has room to fit.
	req.Masterchain.Config.maxBlockBytes = uint32(len(natural.BlockBOC)) - 1

	smaller, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collation did not recover from the size limit: %v", err)
	}
	if len(smaller.BlockBOC) > int(req.Masterchain.Config.maxBlockBytes) {
		t.Fatalf("rebuilt block is %d bytes, over the %d limit",
			len(smaller.BlockBOC), req.Masterchain.Config.maxBlockBytes)
	}
	if smaller.Stats.Transactions == 0 {
		t.Fatal("rebuilt block carries no transactions")
	}
	if smaller.Stats.Transactions > natural.Stats.Transactions {
		t.Fatalf("rebuilt block grew to %d transactions from %d",
			smaller.Stats.Transactions, natural.Stats.Transactions)
	}
	t.Logf("natural %d bytes / %d tx, rebuilt %d bytes / %d tx",
		len(natural.BlockBOC), natural.Stats.Transactions,
		len(smaller.BlockBOC), smaller.Stats.Transactions)
}

// A limit no block can meet — not even an empty one — must end in a reported
// failure rather than in a loop. The rebuild is bounded by construction, and
// this is what proves the bound is reached rather than merely intended.
func TestBuildShardGivesUpOnUnreachableSizeLimit(t *testing.T) {
	req, _ := benchMainnetRequest(t, 0)
	req.Masterchain.Config.maxBlockBytes = 64

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err == nil {
		t.Fatalf("collation produced a %d byte block under a 64 byte limit", len(candidate.BlockBOC))
	}
	if !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("err = %v, want ErrSizeLimit", err)
	}
}

// The test above shaves a single byte off what the workload produces, which the
// first narrowing clears easily. That leaves the arithmetic itself uncovered: a
// ceiling is only useful if it is derived on the scale admission enforces, and a
// ratio measured against some wider estimate loosens every rebuild by the
// difference. Such a bug survives the one-byte case and fails everywhere else,
// so this drives a heavy block into limits that genuinely bind.
//
// It has already earned its keep: adding the finished-state proof to the
// estimate (build.go) silently moved the ratio onto the post-admission scale,
// and every case below stopped recovering while the one-byte case still passed.
func TestBuildShardRebuildsUnderBindingSizeLimits(t *testing.T) {
	// Runs in the shared-fixture parallel batch: it reads the cached mainnet
	// workload and keeps every mutation on its own copy, holds no package-level
	// counter and derives nothing from wall-clock timing.
	t.Parallel()
	req, _ := benchMainnetRequestRepeated(t, benchMainnetFiller, benchMainnetHeavyRepeat)
	natural, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("baseline collation: %v", err)
	}

	// 30, 35 and 65 are here because they are where it broke. Recovering at all is
	// the weaker half of what this asks: a rebuild that fits by throwing away half
	// the block it could have carried is a rebuild that cost the shard the slot's
	// throughput, and it looks identical from the outside to one that fit
	// properly. Compounding the ceiling with the late attempts' divisor — taking
	// min(config, cap)/divisor instead of min(config/divisor, cap) — put exactly
	// these three shaves at 50-54% while every other level stayed above 89%, which
	// is why a floor and not just a fit is asserted, and why the levels between the
	// old ones are covered.
	const utilisationFloor = 80
	for _, shave := range []int{10, 25, 30, 35, 40, 50, 60, 65, 80} {
		limit := len(natural.BlockBOC) * (100 - shave) / 100
		attempt := req
		attempt.Masterchain.Config.maxBlockBytes = uint32(limit)

		smaller, err := testBuilder().BuildShard(context.Background(), attempt)
		if err != nil {
			t.Errorf("limit %d%% below the natural %d bytes: no recovery: %v",
				shave, len(natural.BlockBOC), err)
			continue
		}
		if len(smaller.BlockBOC) > limit {
			t.Errorf("rebuilt block is %d bytes, over the %d limit", len(smaller.BlockBOC), limit)
			continue
		}
		if smaller.Stats.Transactions == 0 {
			t.Errorf("rebuilt block under a %d-byte limit carries no transactions", limit)
			continue
		}
		if used := 100 * len(smaller.BlockBOC) / limit; used < utilisationFloor {
			t.Errorf("limit %d bytes (%d%% under natural): rebuilt %d bytes, only %d%% of what was "+
				"allowed, over %d attempts — the rebuild fits by discarding throughput it could have kept",
				limit, shave, len(smaller.BlockBOC), used, smaller.Stats.CollationAttempts)
			continue
		}
		t.Logf("limit %d bytes (%d%% under natural): rebuilt %d bytes / %d tx over %d attempts",
			limit, shave, len(smaller.BlockBOC), smaller.Stats.Transactions, smaller.Stats.CollationAttempts)
	}
}

// The concession schedule is a port, not a policy of ours, so it is pinned
// index by index against the reference. Every one of these numbers is a place
// an off-by-one hides: shifting the externals cut one attempt earlier ships
// blocks with no external messages under load that the reference would have
// carried, and shifting the divisor one later means the last attempt this
// collator is allowed to make is the one the reference makes second to last.
func TestCollationAttemptConcessionsMatchTheReference(t *testing.T) {
	type concessions struct {
		dispatchTail bool
		externals    bool
		divisor      uint64
	}
	// collator.cpp:4382-4385 (dispatch tail at >=1), 4229-4232 (externals at
	// >=2), 863-873 (halve at 3, quarter at 4), 57 (MAX_ATTEMPTS = 5).
	want := []concessions{
		{dispatchTail: false, externals: false, divisor: 1},
		{dispatchTail: true, externals: false, divisor: 1},
		{dispatchTail: true, externals: true, divisor: 1},
		{dispatchTail: true, externals: true, divisor: 2},
		{dispatchTail: true, externals: true, divisor: 4},
	}
	if len(want) != maxCollationAttempts {
		t.Fatalf("the schedule covers %d attempts, maxCollationAttempts is %d", len(want), maxCollationAttempts)
	}
	for index, expected := range want {
		attempt := collationAttempt{index: index}
		got := concessions{
			dispatchTail: attempt.skipDispatchTail(),
			externals:    attempt.skipExternals(),
			divisor:      attempt.limitDivisor(),
		}
		if got != expected {
			t.Errorf("attempt %d concedes %+v, want %+v", index, got, expected)
		}
	}
}

// The reference scales bytes, gas and collated data and leaves logical time
// alone. Scaling lt delta as well would refuse transactions for a reason that
// has no bearing on how large the serialized block is, and forgetting collated
// data would leave the axis that overflows second most often untouched.
func TestApplyAttemptScalesOnlyTheAxesTheReferenceScales(t *testing.T) {
	original := blockLimits{
		bytes:        limitThresholds{100, 200, 300, 400},
		gas:          limitThresholds{1000, 2000, 3000, 4000},
		ltDelta:      limitThresholds{10, 20, 30, 40},
		collatedData: limitThresholds{500, 600, 700, 800},
	}

	for _, tc := range []struct {
		index   int
		divisor uint64
	}{{index: 0, divisor: 1}, {index: 2, divisor: 1}, {index: 3, divisor: 2}, {index: 4, divisor: 4}} {
		status := &blockLimitStatus{limits: original}
		status.applyAttempt(collationAttempt{index: tc.index})

		scaled := func(from limitThresholds) limitThresholds {
			for i := range from {
				from[i] /= tc.divisor
			}
			return from
		}
		if got, want := status.limits.bytes, scaled(original.bytes); got != want {
			t.Errorf("attempt %d bytes = %v, want %v", tc.index, got, want)
		}
		if got, want := status.limits.gas, scaled(original.gas); got != want {
			t.Errorf("attempt %d gas = %v, want %v", tc.index, got, want)
		}
		if got, want := status.limits.collatedData, scaled(original.collatedData); got != want {
			t.Errorf("attempt %d collated data = %v, want %v", tc.index, got, want)
		}
		if got := status.limits.ltDelta; got != original.ltDelta {
			t.Errorf("attempt %d moved lt delta to %v, want %v untouched", tc.index, got, original.ltDelta)
		}
	}
}

func alwaysOverflows(produced uint64) error {
	return sizeLimitError{what: "block BOC", produced: produced, limit: 1000, estimate: 2000}
}

// A collation that never fits must reach the last attempt the reference allows
// and stop there, with each attempt seeing a strictly higher index than the one
// before it. The narrowing this replaced returned early as soon as its ceiling
// stopped moving, which with concessions attached would mean the attempt that
// drops external messages never runs.
func TestRetryUnderSizeLimitEscalatesToTheBound(t *testing.T) {
	var seen []collationAttempt
	_, err := retryUnderSizeLimit(context.Background(), func(attempt collationAttempt) (*Candidate, error) {
		seen = append(seen, attempt)
		// A produced size that never changes keeps aimBelow's ceiling fixed
		// after the first attempt, which is exactly the case the old loop
		// treated as a dead end.
		return nil, alwaysOverflows(1500)
	})
	if !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("exhausted retries returned %v, want a size-limit error", err)
	}
	if len(seen) != maxCollationAttempts {
		t.Fatalf("ran %d attempts, want %d", len(seen), maxCollationAttempts)
	}
	for i, attempt := range seen {
		if attempt.index != i {
			t.Fatalf("attempt %d reported index %d", i, attempt.index)
		}
	}
	if seen[0].cap != 0 {
		t.Errorf("the first attempt ran under a ceiling of %d, want the configured limits", seen[0].cap)
	}
	if seen[1].cap == 0 {
		t.Error("the second attempt ran under no ceiling; the overflow ratio was not carried")
	}
}

// The attempt count is the only trace a successful rebuild leaves. Without it
// the alarm, the log line and the operator all see a block that fit at once.
func TestRetryUnderSizeLimitStampsTheAttemptCount(t *testing.T) {
	for _, succeedAt := range []int{0, 1, 3} {
		candidate, err := retryUnderSizeLimit(context.Background(), func(attempt collationAttempt) (*Candidate, error) {
			if attempt.index < succeedAt {
				return nil, alwaysOverflows(uint64(1500 + attempt.index))
			}
			return &Candidate{}, nil
		})
		if err != nil {
			t.Fatalf("succeeding at attempt %d returned %v", succeedAt, err)
		}
		if got, want := candidate.Stats.CollationAttempts, uint32(succeedAt)+1; got != want {
			t.Errorf("succeeding at attempt %d stamped %d attempts, want %d", succeedAt, got, want)
		}
	}
}

// collator.cpp:359 declines to repeat once the collation timeout has passed. A
// rebuild that starts after the deadline cannot win the slot it is rebuilding
// for and spends the next slot's CPU losing it.
func TestRetryUnderSizeLimitStopsWhenTheDeadlinePassed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	_, err := retryUnderSizeLimit(ctx, func(attempt collationAttempt) (*Candidate, error) {
		attempts++
		cancel()
		return nil, alwaysOverflows(1500)
	})
	if attempts != 1 {
		t.Fatalf("ran %d attempts after the deadline passed, want 1", attempts)
	}
	if !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("the deadline stop returned %v, want it to still name the size limit", err)
	}
}

// The dispatch queue reads its attempt index off the request, and until this
// wiring existed nothing in production ever set it: the phase-2 and phase-3 cut
// was reachable only from a test that filled the field by hand, so every rebuild
// drained the dispatch queue exactly as expensively as the attempt that had
// just overflowed.
func TestShardAttemptCarriesTheAttemptIndexIntoDispatch(t *testing.T) {
	for _, index := range []int{0, 1, 3} {
		req, _ := benchMainnetRequest(t, benchMainnetFiller)
		c, err := testBuilder().prepareShardPhases(context.Background(), req, collationAttempt{index: index})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := c.req.dispatch.AttemptIndex, uint32(index); got != want {
			t.Errorf("attempt %d reached dispatch as index %d, want %d", index, got, want)
		}
	}
}

// The concession has to reach the block, not just the predicate. A rebuild that
// still admits external messages is a rebuild that produces the same oversized
// block it just threw away, which is how a bounded retry turns into a lost slot.
//
// The mainnet fixture carries twelve externals and 345 transactions at full
// admission; the attempt the reference would run without externals must include
// none of them and must still produce a block, because the internals it does
// admit are not optional.
func TestShardAttemptDropsExternalsOnceTheReferenceWould(t *testing.T) {
	req, _ := benchMainnetRequest(t, benchMainnetFiller)
	if len(req.Externals) == 0 {
		t.Fatal("the fixture carries no external messages; this test would pass on any implementation")
	}

	full, err := testBuilder().buildShardAttemptPaced(
		context.Background(), req, collationAttempt{}, collationPace{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if full.Stats.ExternalIncluded == 0 {
		t.Fatal("the first attempt included no externals; the fixture cannot show the cut")
	}

	cut, err := testBuilder().buildShardAttemptPaced(
		context.Background(), req, collationAttempt{index: 2}, collationPace{},
	)
	if err != nil {
		t.Fatalf("the attempt that drops externals failed to produce a block: %v", err)
	}
	if cut.Stats.ExternalIncluded != 0 || cut.Stats.ExternalAttempts != 0 {
		t.Errorf("attempt 2 included %d externals over %d attempts, want none of either",
			cut.Stats.ExternalIncluded, cut.Stats.ExternalAttempts)
	}
	if cut.Stats.Transactions == 0 {
		t.Error("attempt 2 produced an empty block; dropping externals must not drop the internals too")
	}
	if cut.Stats.Transactions >= full.Stats.Transactions {
		t.Errorf("attempt 2 produced %d transactions against %d at full admission; the cut did not bite",
			cut.Stats.Transactions, full.Stats.Transactions)
	}
}

// The rebuild ceiling must never push admission up to the hard threshold. When
// it does, the two become the same number: admission runs until the estimate
// reaches it, and the mandatory own-shard drain that runs afterwards then finds
// the block already at its hard bound and refuses. That refusal is
// ErrMandatoryDequeueOverflow, which is not a size-limit error, so it does not
// narrow the next attempt — it ends the ladder and loses the slot, reported as a
// predecessor-queue fault that the predecessor did not commit.
func TestTheRebuildCeilingNeverReachesTheHardThreshold(t *testing.T) {
	original := blockLimits{
		bytes:        limitThresholds{100, 200, 300, 400},
		gas:          limitThresholds{1000, 2000, 3000, 4000},
		ltDelta:      limitThresholds{10, 20, 30, 40},
		collatedData: limitThresholds{500, 600, 700, 800},
	}
	const hard = LoadHard - 1

	for _, tc := range []struct {
		name    string
		attempt collationAttempt
	}{
		{name: "ceiling below every configured threshold", attempt: collationAttempt{index: 1, cap: 50}},
		{name: "ceiling between normal and soft", attempt: collationAttempt{index: 2, cap: 250}},
		{name: "ceiling with the halving attempt", attempt: collationAttempt{index: 3, cap: 250}},
		{name: "ceiling with the quartering attempt", attempt: collationAttempt{index: 4, cap: 120}},
		{name: "ceiling of one byte", attempt: collationAttempt{index: 4, cap: 1}},
	} {
		status := &blockLimitStatus{limits: original}
		status.applyAttempt(tc.attempt)

		admission := status.limits.bytes[LoadNormal]
		if admission >= status.limits.bytes[hard] {
			t.Errorf("%s: admission stops at %d and the hard bound is %d; the drain that runs after "+
				"admission cannot fit and the retry ladder ends there",
				tc.name, admission, status.limits.bytes[hard])
		}
	}
}

// The masterchain half of the escalation had no test of its own, and a whole
// deletion of it — the retry ladder, the limit scaling, the dispatch index and
// the external cut, all four at once — left the package green. A masterchain
// block over the consensus limit would then be rebuilt with none of the
// reference's concessions, or not rebuilt at all, and nothing would say so.
func TestMasterAttemptAppliesTheSameConcessionsAsAShard(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)

	for _, index := range []int{0, 1, 2, 3} {
		attempt := collationAttempt{index: index}
		c, err := testBuilder().prepareMasterPhases(context.Background(), fixture.request, attempt)
		if err != nil {
			t.Fatalf("attempt %d: %v", index, err)
		}
		if got, want := c.req.dispatch.AttemptIndex, uint32(index); got != want {
			t.Errorf("attempt %d reached the masterchain dispatch queue as index %d, want %d",
				index, got, want)
		}
		if divisor := attempt.limitDivisor(); divisor > 1 {
			plain, err := testBuilder().prepareMasterPhases(
				context.Background(), fixture.request, collationAttempt{},
			)
			if err != nil {
				t.Fatal(err)
			}
			for axis, pair := range map[string][2]uint64{
				"bytes":         {c.limits.limits.bytes[LoadHard-1], plain.limits.limits.bytes[LoadHard-1]},
				"gas":           {c.limits.limits.gas[LoadHard-1], plain.limits.limits.gas[LoadHard-1]},
				"collated data": {c.limits.limits.collatedData[LoadHard-1], plain.limits.limits.collatedData[LoadHard-1]},
			} {
				if pair[0] != pair[1]/divisor {
					t.Errorf("attempt %d masterchain %s hard limit = %d, want %d scaled by %d",
						index, axis, pair[0], pair[1], divisor)
				}
			}
		}
	}
}

// The masterchain external cut, end to end. Attempt 2 must produce a block with
// no external messages in it and still produce one.
func TestMasterAttemptDropsExternalsOnceTheReferenceWould(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)

	full, err := testBuilder().buildMasterAttempt(context.Background(), fixture.request, collationAttempt{})
	if err != nil {
		t.Fatal(err)
	}
	cut, err := testBuilder().buildMasterAttempt(
		context.Background(), fixture.request, collationAttempt{index: 2},
	)
	if err != nil {
		t.Fatalf("the masterchain attempt that drops externals produced no block: %v", err)
	}
	if cut.Stats.ExternalIncluded != 0 || cut.Stats.ExternalAttempts != 0 {
		t.Errorf("masterchain attempt 2 included %d externals over %d attempts, want none of either",
			cut.Stats.ExternalIncluded, cut.Stats.ExternalAttempts)
	}
	if full.Stats.ExternalAttempts == 0 && full.Stats.ExternalIncluded == 0 {
		t.Log("the masterchain fixture carries no external messages; the cut is asserted but not exercised")
	}
}

// BuildMaster must run the ladder, not a single attempt. Deleting
// retryUnderSizeLimit from it is a one-line change that no other test notices.
//
// The masterchain block is asserted through the attempt count rather than
// through a smaller block, because there is no smaller block to reach: a
// masterchain candidate is almost entirely shard descriptions and configuration,
// so dropping external messages and halving the admission limits removes nothing
// from it. Every concession the ladder has still runs, and running them is what
// this pins — the count only exists because the ladder produced it.
func TestBuildMasterRebuildsUnderTheSizeLimit(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)

	natural, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if got := natural.Stats.CollationAttempts; got != 1 {
		t.Errorf("the unconstrained masterchain block took %d attempts, want 1", got)
	}

	narrowed := fixture.request
	config := *narrowed.Config
	config.maxBlockBytes = uint32(len(natural.BlockBOC)) - 1
	narrowed.Config = &config

	smaller, err := testBuilder().BuildMaster(context.Background(), narrowed)
	if err == nil {
		if len(smaller.BlockBOC) > len(natural.BlockBOC)-1 {
			t.Errorf("the rebuilt masterchain block is %d bytes, over the %d limit",
				len(smaller.BlockBOC), len(natural.BlockBOC)-1)
		}
		if smaller.Stats.CollationAttempts < 2 {
			t.Errorf("the rebuilt masterchain block reports %d attempts; the ladder did not run",
				smaller.Stats.CollationAttempts)
		}

		return
	}
	if !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("a masterchain block over its limit failed for an unrelated reason: %v", err)
	}
	spent, counted := collationAttemptsSpent(err)
	if !counted {
		t.Fatal("BuildMaster returned a size-limit failure that ran through no retry ladder at all")
	}
	if spent != maxCollationAttempts {
		t.Errorf("the masterchain ladder stopped after %d attempts, want %d", spent, maxCollationAttempts)
	}
}
