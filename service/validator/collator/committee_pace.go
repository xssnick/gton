package collator

import (
	"math"
	"sync"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// The committee validates a shard's blocks one after another, and a leader
// window is a fixed span of wall clock: slot j of a window must be notarized by
// the window's observation plus FirstBlockTimeout plus (j+1) target rates
// (simplex/voter.go voterNotarizationObserved). A leader whose blocks each cost
// the committee more than one target rate to validate runs a debt that no head
// start repays, because the deadline never moves and the validation is
// sequential. Measured on the stand against the reference validators: blocks of
// 400 transactions notarized at a 466 ms cadence against a 400 ms slot, the 887
// ms of initial slack gone after fourteen slots, the fifteenth block missed by 32
// ms and the sixteenth skipped — while a window of the same blocks that had
// spilled a queue into the next one saw that window get no certificate at all.
//
// So the block is sized to the committee, not to the byte limits. What the
// committee costs per transaction is measured from the only evidence this node
// has — the certificates on its own candidates — and the cap is what keeps one
// block inside a fraction of a target rate at that cost.
const (
	// adaptiveTransactionStart is the cap before the first certificate has been
	// measured — on a fresh session of a shard this node has not produced for
	// since it started. Measured on the stand 2026-09-05, one shard, blocks of
	// the settled window: the estimator climbs to 416 and stays there (p50 416,
	// p75 416, p90 421) while the committee certifies without complaint, and
	// the reference's own blocks reach 458 at p90. A cold start at 300 gives
	// away the first windows of every session to a cap the committee has never
	// asked for, so start where the estimate settles and let the measured pace
	// take it from there in either direction.
	adaptiveTransactionStart uint32 = 400
	// adaptiveTransactionCeiling is the most a measured pace may raise the cap
	// to. It is deliberately far above what today's committee sustains: the
	// cap must follow a faster committee up — a reference release with a
	// quicker validation would otherwise be capped at yesterday's number — and
	// what bounds a block beyond this is the byte and gas limits, which the
	// reference obeys too. Its only job is to keep one absurd reading from
	// producing a cap the limits would never let a block reach anyway.
	adaptiveTransactionCeiling uint32 = 1000
	// adaptiveTransactionFloor stops a run of slow certificates — a committee
	// busy with someone else's queue — from shrinking blocks to nothing.
	adaptiveTransactionFloor uint32 = 40
	// A block is allowed to cost the committee one target rate plus the share
	// of the first-block timeout that falls on each slot, less a reserve for
	// jitter. Slot j of a window is due at the window's observation plus
	// FirstBlockTimeout plus (j+1) target rates (simplex/voter.go), so the
	// timeout is slack the committee may spend across the whole window: with
	// the default 1 s over 16 slots, 62 ms per block. The reference collator
	// sizes nothing to the committee and ships ~900 kB blocks that the stand's
	// committee certifies at a 375-500 ms cadence, losing an occasional tail;
	// the earlier target of 0.8 of a slot (320 ms) kept our windows full at
	// half the reference's bytes per window — polished emptiness. Both slack
	// and reserve are the default protocol parameters' figures; the collator
	// does not see the session's simplex parameters.
	//
	// 112 ms, not the 62 ms that arithmetic gives, because the committee
	// measurably does not use the rest and the cap is what our blocks stand in.
	// Stand, 2026-09-04, one loaded shard, 300 externals/s, medians over blocks
	// that are not a window's first slot:
	//
	//	slack   tx/block  cap  blocks at the cap  block kB  blocks/window
	//	 62 ms       397  400               68%      1283           11.7
	//	112 ms       416  434               22%      1359           11.8
	//
	// The window kept the same number of blocks and each carried 5% more, so
	// the slack is not a risk this committee charges for. Read the effect on
	// medians and not on a five-minute mean: a mean mixes in the first slot of
	// every window, which is capped at firstSlotTransactions, and windows the
	// committee truncated — both moved the two configurations to within 0.2
	// transactions of each other and hid the difference entirely.
	//
	// The measurement that says the cap binds at all is the last column but
	// one: at 62 ms four blocks in five sit exactly on it.
	//
	// 169 ms was tried on 2026-09-05 and reverted the same hour. It is the
	// measurement that says this knob is finished, so do not take it again:
	//
	//	slack   tx/block  kB/block  blocks/window  last slot  tx/window
	//	112 ms       416      1115           10.0        9.5       3121
	//	169 ms       416      1128            8.4        7.4       2497
	//
	// The budget went up 15% and the block did not move at all: the estimate
	// re-measured the cost from 0.918 to 1.055 ms per transaction and handed
	// the whole raise back. The block's p90 was 424 against a cap that now
	// stood for 478, so the block is not what the cap was holding — the cap
	// only settles wherever the block already stops, which is why raising it
	// cannot buy anything. What the raise did buy was a shorter window: the
	// budget is also the wall clock a build may spend, so every block was
	// emitted later and the committee's alarm arrived two slots earlier. The
	// same shape as the block-limit multiplier on 2026-09-04, where
	// transactions per window fell 4464, 3922, 3452 at 1.0, 1.05 and 1.2.
	committeePaceWindowSlack   = 112 * time.Millisecond
	committeePaceJitterReserve = 30 * time.Millisecond
	// committeeFixedCost is the part of a certificate's latency that does not
	// scale with the block: delivery, the vote round and the committee's own
	// scheduling. Fitted on the stand: 5 transactions took 120 ms, 200 took 285,
	// 400 in a full pipeline took 466 — one intercept fits all three within the
	// noise, and it is what keeps a small block from reading as an expensive
	// transaction.
	committeeFixedCost = 100 * time.Millisecond
	// committeePaceSampleMinTransactions is the smallest block whose
	// certificate is a measurement of the per-transaction cost at all: below it
	// the fixed cost dominates and the division amplifies its error.
	committeePaceSampleMinTransactions = 50
	// committeePaceSmoothing is the weight of the newest sample in the moving
	// estimate. Windows are a minute apart per shard and a window yields a dozen
	// samples, so this follows the committee within one window and forgets a
	// stale reading within two.
	committeePaceSmoothing = 0.3
	// The per-transaction cost is clamped to a range that contains every
	// reading ever taken on the stand with room to spare, so one absurd sample —
	// a certificate delayed by something other than validation — cannot pin the
	// cap at either end.
	committeeMinMillisPerTransaction = 0.2
	committeeMaxMillisPerTransaction = 5.0
	// committeePaceEmissionRetention bounds the emitted-candidate table: a
	// candidate whose certificate has not arrived within this span was skipped,
	// and its record is only what a later cert for it would be matched against.
	committeePaceEmissionRetention = 30 * time.Second
	// committeePaceSlack is how much longer than our own emission interval a
	// certificate interval has to be before it says the committee fell behind.
	// A committee that keeps pace certifies at exactly the cadence we emit at,
	// give or take the vote round's jitter; that interval is our number, not
	// theirs, and reading it as their cost is what collapsed the cap to 76
	// transactions on the stand while the committee was certifying 300 in 330
	// ms — a 1.1 s pipeline latency, flat across the window, mistaken for the
	// validation time of every block.
	committeePaceSlack = 50 * time.Millisecond
	// committeePaceRelax is the factor the estimated cost shrinks by on every
	// certificate that arrived on our cadence rather than behind it. The cap
	// then probes upward, about five percent per block, until the committee is
	// measured falling behind again — which is the only observation that says
	// what a block really costs.
	committeePaceRelax = 0.95
	// committeePaceIdleRelax is the factor for a certificate the committee
	// produced while idle — before our next block was even emitted. Twelve
	// such certificates take the cap from the floor back to the start value
	// inside one window, where the pace-keeping factor needed forty.
	committeePaceIdleRelax = 0.85
	// committeePaceMaxSampleStep bounds how far one sample can raise the
	// estimate relative to its current value.
	committeePaceMaxSampleStep = 2.0
)

// committeePace is the per-shard estimate of what the committee costs per
// transaction of ours, and the transaction cap derived from it.
type committeePace struct {
	mu sync.Mutex
	// millisPerTransaction is the moving estimate; zero until the first sample.
	millisPerTransaction float64
	samples              int
	// lastCertificate is when the committee last certified one of our blocks,
	// and lastCertifiedEmission when that block had left this node. A block
	// emitted before the previous certificate was queued behind it: the
	// interval between the two certificates is then the committee's time on
	// this block — but only when that interval exceeds the interval between the
	// two emissions, because a committee that keeps pace certifies at the rate
	// we emit and says nothing about its own.
	lastCertificate       time.Time
	lastCertifiedEmission time.Time
	emitted               map[simplex.CandidateID]paceEmission
}

type paceEmission struct {
	at           time.Time
	transactions uint32
}

func newCommitteePace() *committeePace {
	return &committeePace{emitted: make(map[simplex.CandidateID]paceEmission)}
}

// noteEmitted records that a candidate carrying transactions left this node at
// the given instant. Empty candidates carry nothing to measure and are not
// recorded.
func (p *committeePace) noteEmitted(id simplex.CandidateID, at time.Time, transactions uint32) {
	if transactions == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	for old, emission := range p.emitted {
		if at.Sub(emission.at) > committeePaceEmissionRetention {
			delete(p.emitted, old)
		}
	}
	p.emitted[id] = paceEmission{at: at, transactions: transactions}
}

// noteCertified records the notarization certificate for one of our
// candidates. It reports the interval the certificate was measured over and
// whether it produced a cost sample.
//
// Only a block that was queued behind the previous certificate, and whose
// certificate came later than our own emission cadence would have put it, is
// a measurement: the committee was busy the whole interval and fell behind us
// by the difference, so the interval is what the block cost it. A certificate
// that keeps our cadence proves the committee is at least that fast and
// nothing more; it relaxes the estimate a little so the cap probes upward. A
// certificate for a block the committee received while idle — the first of a
// window, after a gap — measures delivery and their backlog from someone
// else's window, and is not used at all. Blocks too small to measure a
// per-transaction cost are skipped either way, though their certificates still
// mark the committee busy.
func (p *committeePace) noteCertified(id simplex.CandidateID, at time.Time) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	emission, known := p.emitted[id]
	if !known {
		return 0, false
	}
	delete(p.emitted, id)
	queued := !p.lastCertificate.IsZero() && !emission.at.After(p.lastCertificate)
	certInterval := at.Sub(p.lastCertificate)
	emitInterval := emission.at.Sub(p.lastCertifiedEmission)
	if at.After(p.lastCertificate) {
		p.lastCertificate = at
		p.lastCertifiedEmission = emission.at
	}
	if emission.transactions < committeePaceSampleMinTransactions {
		return certInterval, false
	}
	// Every certificate bounds the committee's cost from above: it cannot
	// have spent longer on this block than the whole interval since it was
	// free to start on it — since the previous certificate when the block
	// was queued, since our emission when it was not — and that interval
	// includes delivery and the vote round on top of validation. When the
	// bound is below the estimate the estimate is wrong, and it drops to
	// the bound at once. This is what lets one stall's sample — a 1.3 s
	// certificate on a 125-transaction block at a session start, read as
	// 5 ms per transaction and a cap of 44 — be undone by the next
	// certificate that comes back in 90 ms instead of by thirty certificates
	// of relaxation while the committee sat idle behind 60-transaction
	// blocks.
	busy := certInterval
	if !queued {
		busy = at.Sub(emission.at)
	}
	if busy > 0 && p.millisPerTransaction > 0 {
		bound := float64(busy) / float64(time.Millisecond) / float64(emission.transactions)
		if bound < p.millisPerTransaction {
			p.millisPerTransaction = math.Max(bound, committeeMinMillisPerTransaction)
		}
	}
	if !queued {
		// The committee was idle when this block reached it: it certified the
		// previous one before this one was even emitted. That says nothing
		// about what a block costs and everything about the committee having
		// time to spare, so the estimate relaxes — harder than for a block
		// that merely kept pace. Without this the cap froze: on the stand a
		// shard sat at 44 transactions for four windows in a row with every
		// block certified on time, because a certificate that arrives before
		// our next emission never counted as keeping pace and the estimate
		// had no way down.
		p.relaxLocked(committeePaceIdleRelax)

		return certInterval, false
	}
	if certInterval <= 0 {
		return certInterval, false
	}
	if certInterval <= emitInterval+committeePaceSlack {
		// Keeping pace. The block cost at most this interval, and probably
		// less; the estimate moves toward "less" until a certificate says
		// otherwise.
		p.relaxLocked(committeePaceRelax)

		return certInterval, false
	}
	perTransaction := float64(certInterval-committeeFixedCost) / float64(time.Millisecond) / float64(emission.transactions)
	perTransaction = math.Min(math.Max(perTransaction, committeeMinMillisPerTransaction), committeeMaxMillisPerTransaction)
	if p.millisPerTransaction > 0 {
		// One certificate may at most double the estimate. A committee that
		// really got slower shows it on every certificate and the estimate
		// follows in two or three; a single stall — a session start, a
		// masterchain block landing mid-window — is not what a block costs,
		// and without the limit it moved the estimate from 0.7 to 2 ms per
		// transaction in one step.
		perTransaction = math.Min(perTransaction, p.millisPerTransaction*committeePaceMaxSampleStep)
	}
	if p.samples == 0 {
		p.millisPerTransaction = perTransaction
	} else {
		p.millisPerTransaction += committeePaceSmoothing * (perTransaction - p.millisPerTransaction)
	}
	p.samples++

	return certInterval, true
}

// relaxLocked shrinks the estimate by factor, from the start value's implied
// cost when nothing has been measured yet.
func (p *committeePace) relaxLocked(factor float64) {
	if p.millisPerTransaction == 0 {
		p.millisPerTransaction = impliedMillisPerTransaction(adaptiveTransactionStart)
	}
	p.millisPerTransaction = math.Max(p.millisPerTransaction*factor, committeeMinMillisPerTransaction)
}

// pacedBudget is the committee time one block may cost, in nanoseconds as a
// float: the slot, the window slack that falls on it, less the jitter reserve
// and the fixed part of a certificate's latency.
func pacedBudget(targetRate time.Duration) float64 {
	return float64(targetRate+committeePaceWindowSlack-committeePaceJitterReserve) - float64(committeeFixedCost)
}

// impliedMillisPerTransaction is the per-transaction cost at which the paced
// budget of one 400 ms slot buys exactly cap transactions: the estimate a cap
// stands for, used to start relaxing from the start value before any
// certificate has measured the committee falling behind.
func impliedMillisPerTransaction(cap uint32) float64 {
	budget := pacedBudget(400 * time.Millisecond)

	return budget / float64(time.Millisecond) / float64(cap)
}

// transactionCap is the number of transactions one block may admit so that the
// committee's time on it stays inside the paced budget of one slot, at the
// measured per-transaction cost. With no measurement yet it is the start value;
// a measured pace moves it anywhere between the floor and the ceiling.
func (p *committeePace) transactionCap(targetRate time.Duration) uint32 {
	p.mu.Lock()
	perTransaction := p.millisPerTransaction
	p.mu.Unlock()

	if perTransaction <= 0 || targetRate <= 0 {
		return adaptiveTransactionStart
	}
	budget := pacedBudget(targetRate)
	if budget <= 0 {
		return adaptiveTransactionFloor
	}
	cap := budget / float64(time.Millisecond) / perTransaction
	switch {
	case cap >= float64(adaptiveTransactionCeiling):
		return adaptiveTransactionCeiling
	case cap <= float64(adaptiveTransactionFloor):
		return adaptiveTransactionFloor
	default:
		return uint32(cap)
	}
}

// estimate reports the moving per-transaction cost and the sample count, for
// logs and tests.
func (p *committeePace) estimate() (float64, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.millisPerTransaction, p.samples
}
