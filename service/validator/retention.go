package validator

import "time"

// This file holds every session-scoped retention bound in one place, because
// they are one policy and they were previously eight independent literals.
//
// The rule the literals broke: a bound written as a slot count means a
// different thing on every network, because the slot rate is a noncritical
// consensus parameter. Sixty-four slots is 153.6 s at the protocol default
// TargetRate of 2400 ms and 12.8 s at the 200 ms this project's test network
// runs, a factor of twelve — and the constant was reasoned about in minutes and
// megabytes, so the reasoning was silently invalidated by a config value the
// constant never read. Every bound below is therefore stated in the unit its
// justification is written in — bytes, payloads, or wall-clock — and converted
// to slots against the session's own TargetRate at the point of use.

// retentionBudget is what one session's finalization sweeps may hold back for
// the local producer, in the units the memory is actually spent in.
//
// Bytes and Payloads are the primary axis and they bound different things.
// Bytes bounds the candidate payloads directly and self-adapts in the right
// direction: a fast network with small blocks gets a deep floor measured in
// slots, a slow network with large blocks gets a shallow one, and both get the
// same memory. Payloads bounds what bytes cannot see — the applied ChainState
// held beside each finalized non-empty candidate, whose size this project has
// no in-process measurement for (state_block_boc_bytes is documented as a floor
// rather than a measurement, and applied successors share subtrees). One
// retained payload is at most one such state, so capping the payload count caps
// that side too, without pretending to know what it weighs.
//
// Duration is a backstop, not the operating policy. It is deliberately far
// above any lag a node can recover from, because a slot-distance bound is
// exactly the mistake this file exists to correct: skipped slots advance the
// slot number without producing a candidate, so slot distance charges a node
// for memory it is not holding.
//
// What this budget does NOT bound, stated here so the published ceiling is the
// real one: it governs only the finalized payloads a session holds back below
// the fixed margin. The other terms are enumerated on
// candidateResolver.retentionCapFloor, which is where the walk that spends this
// budget lives; the short form is that a session's real payload ceiling is
// Bytes + fixed margin + in-flight-deferred + admitted-above-tip + externally
// pinned, and only the first term is what retentionFloorCapBytes names.
//
// Which of Bytes and Payloads actually binds is a property of the network, and
// on neither network this project has measured is it Bytes — see the note on
// retentionFloorCapPayloads and the one on retentionFloorCapDuration.
type retentionBudget struct {
	Bytes    int64
	Payloads int
	Duration time.Duration
}

const (
	// retentionFloorCapBytes is the candidate payload a shard session may
	// retain below the fixed margin on the local producer's behalf.
	//
	// It is a deliberate flat budget and nothing derives it — not GOMEMLIMIT,
	// not the machine's free memory, not the block size. Retaining the lineage a
	// leader window is about to walk is worth far more than the memory it costs,
	// and the protection against an unbounded session is not the exact number
	// but the degradation at it: crossing the cap raises the retention floor,
	// the finalization sweep releases the payloads below it, and the next read
	// comes back out of the candidate store. Nothing is allocated on the way
	// past the cap and no validation fails because of it, so the number only has
	// to be generous — precision would buy nothing.
	//
	// For scale, measured on the 3-node test network (go-0, 5.017 h, 54,273
	// validated basechain candidates): 1,635,760,493 payload bytes over those
	// candidates is 29.4 KiB mean, the p90 is 336 KiB, and the worst retained
	// set the old bound ever held was 27.15 MiB across 56 entries. 512 MiB is
	// nineteen times that worst case; at mainnet's ~880 KiB candidates it is 596
	// retained blocks, and at the measured p90 it is 1,560.
	//
	// Say plainly what those numbers mean: on neither measured network does this
	// cap bind. At the protocol default rate the backstop below admits at most
	// capSlots(2400 ms) = 250 slots of retained payloads, so mainnet's ceiling is
	// 250 × ~880 KiB ≈ 215 MiB and mainnet is TIME-bound, not byte-bound. On the
	// 200 ms test network the backstop admits 3,000 slots, but
	// retentionFloorCapPayloads stops the walk at 2,048 payloads — 59 MiB at that
	// run's 29.4 KiB mean — so the test network is PAYLOAD-bound. Bytes binds
	// only where both hold: a slot rate fast enough to fit more than 2,048 slots
	// into retentionFloorCapDuration (below ~293 ms) AND candidates above
	// 512 MiB / 2048 = 256 KiB. That combination is a plausible future network,
	// which is why the cap exists, and it is not either network measured today.
	retentionFloorCapBytes int64 = 512 << 20
	// retentionFloorCapMasterchainBytes is the same bound for a masterchain
	// session, kept at a quarter of the shard budget as it was before. Measured
	// on the same run: 172,758,284 B over 74,689 masterchain slots is 2,313 B
	// per slot, and the largest retained set was 232 KiB, so this never binds —
	// it is here so a masterchain session cannot spend a shard-sized budget if
	// that ever stops being true.
	retentionFloorCapMasterchainBytes int64 = 128 << 20
	// retentionFloorCapPayloads bounds the retained non-empty candidates, and
	// through them the applied states held beside their finalization markers:
	// one retained payload is at most one such state, and this project has no
	// in-process measurement of what one of those weighs.
	//
	// The crossover is exact and worth stating rather than approximating: this
	// cap binds before the byte cap for every candidate smaller than
	// 512 MiB / 2048 = 256 KiB, and the byte cap binds first above it. Both
	// readings of the measured run therefore have to be given, because they land
	// on opposite sides. Bytes binds first at the p90 (336 KiB → 1,560 payloads)
	// and at the 496.4 KiB mean of the worst retained set actually observed
	// (1,056). This cap binds first at the run's OVERALL mean of 29.4 KiB, where
	// 2,048 payloads is 59 MiB and the byte cap would need 17,825 — so on the test
	// network as a whole it is this cap that is operative, not bytes.
	//
	// That is the intended outcome and not a leftover: below 256 KiB the payload
	// is not the cost, the applied ChainState held beside it is, and that is the
	// term this cap exists to bound. Raising it until bytes bound first at 29.4
	// KiB would mean admitting 17,825 applied states on the strength of a
	// quantity this project does not measure, which is the opposite of why the
	// cap is here.
	retentionFloorCapPayloads = 2048
	// retentionFloorCapDuration is the backstop above. Ten minutes is 3.6x the
	// longest standstill measured on the test network (168 s) and 4.5x the p99
	// basechain apply lag (132 s), so a node that is still catching up is never
	// cut off by it; it exists so a session that somehow retains payload-free
	// entries forever still has a bound. It is also twice
	// candidateValidationTimeout, so a validation that spends its whole deadline
	// waiting for a parent state still finds its lineage retained.
	retentionFloorCapDuration = 10 * time.Minute
	// retentionFloorMinCapSlots and retentionFloorMaxCapSlots clamp the
	// conversion of that duration into slots. The lower bound keeps a
	// hypothetically very slow network from converting ten minutes into a
	// margin thinner than the fixed one; the upper bound is an overflow guard on
	// a very fast one.
	retentionFloorMinCapSlots uint32 = 64
	retentionFloorMaxCapSlots uint32 = 4096
)

// defaultRetentionBudget returns the budget for one session's chain.
func defaultRetentionBudget(masterchain bool) retentionBudget {
	budget := retentionBudget{
		Bytes:    retentionFloorCapBytes,
		Payloads: retentionFloorCapPayloads,
		Duration: retentionFloorCapDuration,
	}
	if masterchain {
		budget.Bytes = retentionFloorCapMasterchainBytes
	}

	return budget
}

// capSlots converts the backstop duration into a slot distance at this
// session's own rate: 250 slots at the 2400 ms protocol default, 3000 at the
// 200 ms of the test network.
func (b retentionBudget) capSlots(targetRate time.Duration) uint32 {
	return slotsForDuration(b.Duration, targetRate, retentionFloorMinCapSlots, retentionFloorMaxCapSlots)
}

// slotsForDuration converts a wall-clock budget into slots at targetRate,
// rounding up, and clamps the result. A non-positive rate cannot happen —
// Params.Validate rejects it — but the conversion still has to be total, and
// where it lands when it cannot compute an answer is a decision.
//
// It lands on minSlots. Every caller spends the result on retention, so an
// unusable rate used to mean the maximum of every derived bound at once: 4096
// slots of candidate payloads, of resolved parent states and of applied states,
// which is the largest possible answer to a question that had no answer.
// Failing open on a memory bound is the wrong direction; the floor is the safe
// one, and it costs at worst a lineage walk that reads from storage.
func slotsForDuration(budget, targetRate time.Duration, minSlots, maxSlots uint32) uint32 {
	if budget <= 0 || targetRate <= 0 {
		return minSlots
	}
	slots := uint64((budget + targetRate - 1) / targetRate)
	if slots < uint64(minSlots) {
		return minSlots
	}
	if slots > uint64(maxSlots) {
		return maxSlots
	}

	return uint32(slots)
}

// candidateRetentionMarginDuration is the wall-clock margin of already
// finalized candidate payloads kept unconditionally, independent of what any
// lineage walk asked for.
//
// Its justification has always been a duration: the walk goes back to the
// masterchain-visible finalized block, which trails this session's finalized
// slot by the shard-top inclusion delay. That delay is a wall-clock quantity —
// a masterchain block interval plus its own collation — so the margin is stated
// as one here.
//
// THE UNIT IS WALL CLOCK AND NOTHING ELSE, and the previous version of this
// comment is the reason that has to be shouted. It claimed "eight seconds is the
// protocol default's sixteen slots". Sixteen slots at the protocol default
// TargetRate of 2400 ms is 38.4 s, not 8 s, and because sixteen was also the
// FLOOR handed to slotsForDuration it was the value that won: mainnet retained
// 38.4 s while the constant, the test and the published ceiling all said 8 s.
// That is the same class of defect as the slot-count caps this file was written
// to remove — a bound justified in one unit and implemented in another — and it
// is the fourth of them in two days. The floor is now smaller than the duration
// at every rate this project runs, so the duration is what binds.
const candidateRetentionMarginDuration = 8 * time.Second

// candidateCacheRetainedSlots is the margin at the protocol default rate: it is
// both the slot floor handed to slotsForDuration and, at that rate, the value
// the conversion lands on anyway, because ceil(8 s / 2400 ms) = 4. The two
// coincide on purpose so neither can silently dominate the other, and the
// coincidence is asserted by TestCandidateMarginIsTheDurationItClaims.
//
// Four is also a justified slot floor in its own right rather than a number that
// merely fits: it is this project's default SlotsPerLeaderWindow, so on an
// arbitrarily slow network the margin still spans the leader window that opened
// on the finalization it now trails.
//
// It is the value mainnet gets — 4 slots, 9.6 s, about 3.4 MiB at mainnet's
// ~880 KiB candidates — and the value every default-parameter test sees;
// candidateRetainedSlots is what the code actually uses. On the 200 ms test
// network the duration gives 40 slots, 8.0 s, 13.1 MiB at that run's 336 KiB
// p90.
const candidateCacheRetainedSlots uint32 = 4

// candidateRetainedSlots derives the payload margin for one session's rate.
func candidateRetainedSlots(targetRate time.Duration) uint32 {
	return slotsForDuration(
		candidateRetentionMarginDuration,
		targetRate,
		candidateCacheRetainedSlots,
		retentionFloorMaxCapSlots,
	)
}

// stateRetainedSlots is the margin of already-resolved parent states kept
// after the finalization that made them unreachable.
//
// The comment on the literal this replaces said it "absorbs a leader window",
// and a leader window is SlotsPerLeaderWindow slots — a configured value, not
// four. Four was right only because this network happens to configure four.
// One slot of headroom is added for the window that opened exactly on the
// finalization it now trails.
func stateRetainedSlots(slotsPerLeaderWindow uint32) uint32 {
	if slotsPerLeaderWindow == 0 {
		return 1
	}

	return slotsPerLeaderWindow + 1
}

// resolverCacheLogInterval is how often the debug projection of the resolver
// caches is emitted. Both snapshots walk their whole map, so this is a rate
// limit and its unit is time; the literal it replaces was 32 slots with a
// comment claiming "roughly a minute", which was a minute only at 2 s slots and
// 6.4 s at 200 ms.
const resolverCacheLogInterval = time.Minute

// resolverCacheLogPeriod derives that interval in slots: 25 at the protocol
// default, 300 on the 200 ms test network.
func resolverCacheLogPeriod(targetRate time.Duration) uint32 {
	return slotsForDuration(resolverCacheLogInterval, targetRate, 1, retentionFloorMaxCapSlots)
}

// candidateValidationTimeout bounds one candidate validation, including the
// wait for its parent's applied state. Once it expires this validator
// abstains; it never turns local lag into a semantic rejection.
//
// It is a flat wall-clock constant, and deriving it from the slot rate is the
// specific mistake this value has already been made twice. The reference states
// it as wall clock and nowhere else: the consensus layer starts every candidate
// validation with td::Timestamp::in(60.0)
// (validator/consensus/block-validator.cpp:104), and ValidateQuery then extends
// that deadline rather than shortening it when the block it is checking is old
// — new_timeout = now + min(60.0, (now - prev_utime) / 2), applied only if it
// is later than the deadline already set
// (validator/impl/validate-query.cpp:1399-1406), so the reference's effective
// bound runs to about 120 s and never scales with the block interval. A slot
// count is not a port of that; deriving 8 leader windows from it produced 6.4 s
// on this project's 200 ms test network, which is the same class of bug as the
// slot-count retention cap this file exists to correct, only inverted.
//
// The size is then a field question, because what this deadline actually covers
// is not what the reference's covers. C++ shard validation with full collated
// data takes the previous state out of the collated data
// (validate-query.cpp:1386-1398) and its consensus layer waits for a chain
// state under a separate 10 s bound (validator/consensus/chain-state.cpp:23-34)
// — the 60 s bounds work. Here the same deadline also covers a wait on this
// node's own apply pipeline, because a shard block this node finalized becomes
// readable only once the masterchain shard client processes a masterchain block
// carrying that shard top. That wait is measured in tens of seconds: on the
// 3-node test network the basechain standstills ran 94-168 s, with a p99 apply
// lag of 132 s, while the validation work itself was 1.7 ms median and 4.8 ms
// p90. Practically none of this budget is computation; it is the wait, and the
// bound has to outlive what the field says the wait legitimately takes.
//
// Five minutes is 1.8x the worst standstill measured and 2.3x the p99 apply
// lag, and it is deliberately half of retentionFloorCapDuration so the lineage
// a validation resumes into is still retained when its wait ends.
//
// The honest cost is the other side: a validation still pending long after its
// slot was settled cannot change that outcome. What it holds is one goroutine
// parked in stateResolver.resolve plus the candidate it already had in hand, and
// the deadline releases exactly those two.
//
// What the deadline does NOT bound — stated precisely, because the previous
// version of this comment claimed it bounded the wait itself:
//
//   - The wait for the parent state is served by a shared flight, and
//     stateResolver.resolve (state_resolver.go:339-362) selects between the
//     caller's ctx and the flight's completion. The deadline ends this
//     validation's participation in the flight. It does not end the flight:
//     resolveLoop runs under context.WithCancel(r.ctx), the resolver's
//     session-scoped context, so the flight outlives every waiter and is ended
//     only by the state arriving, by another waiter's success, or by resolver
//     close — which session retirement performs.
//   - Inside that flight there is no 1 Hz poll of the node backend any more.
//     LocalSessionBackend.loadChainTip blocks on the publication signal and is
//     bounded per iteration by chainTipWaitBackstop (30 s), which alarms, counts
//     and returns not-ready; loadFinalizedChainState then retries. The 1 s
//     interval that survives there paces only a backend that does not wait at
//     all.
//
// So the layering is: chainTipWaitBackstop bounds one blind wait,
// retentionFloorCapDuration bounds how far back the payloads that wait resumes
// into stay in memory, session retirement bounds the flight, and this constant
// bounds only the validation attached to it. Bounding this one tighter would not
// reclaim the slot or the flight; it would only convert a vote this node could
// still have cast into an abstention, which is exactly what the field caught the
// 6.4 s version doing.
const candidateValidationTimeout = 5 * time.Minute
