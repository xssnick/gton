package collator

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	committeePaceStartFraction = 0.5
	committeePaceMaxFraction   = 0.95
	committeePaceMinFraction   = 0.125
	committeePaceMinBudget     = 20 * time.Millisecond

	committeePaceEmissionRetention = 30 * time.Second
	committeePaceEmissionLimit     = 256
	committeePaceOutstandingLimit  = 4
)

// paceBudget bounds active collation work, not transaction count. The builder
// leaves finishReserve inside duration for state/proof construction. revision
// identifies the capacity decision, not the continuously measured finish cost.
type paceBudget struct {
	duration      time.Duration
	finishReserve time.Duration
	revision      uint64
}

type paceEmission struct {
	at           time.Time
	targetRate   time.Duration
	budget       paceBudget
	window       WindowID
	parent       simplex.ParentID
	transactions uint32
	elapsed      time.Duration
	tail         time.Duration
	limited      bool
	demand       bool
	artificial   bool
}

func (e paceEmission) saturated() bool {
	return e.limited && e.transactions > 0 && !e.artificial
}

type paceCertificate struct {
	id       simplex.CandidateID
	at       time.Time
	emission paceEmission
}

// committeePace has two timescales. Build completion measures the finish reserve
// immediately; adjacent own certificates adjust the total work budget. Neither
// divides by transaction count: a costly transaction consumes more of the same
// wall-time budget. All candidate bookkeeping remains session-local.
type committeePace struct {
	mu sync.Mutex

	fraction      float64
	finishReserve time.Duration
	tailMeasured  bool
	revision      uint64
	congested     bool
	goodIntervals uint8
	slowIntervals uint8
	intervals     uint8
	certifiedSpan time.Duration
	emittedSpan   time.Duration
	slowSpan      bool
	backpressured bool
	samples       uint32
	lastSample    time.Time
	last          paceCertificate
	emitted       map[simplex.CandidateID]paceEmission
	capacityWake  chan struct{}
}

func newCommitteePace() *committeePace {
	return &committeePace{
		fraction: committeePaceStartFraction,
		revision: 1,
		emitted:  make(map[simplex.CandidateID]paceEmission),
	}
}

func (p *committeePace) budget(targetRate time.Duration) paceBudget {
	p.mu.Lock()
	defer p.mu.Unlock()

	if targetRate <= 0 {
		return paceBudget{revision: p.revision}
	}

	duration := time.Duration(float64(targetRate) * clampPaceFraction(p.fraction, targetRate))
	reserve := p.finishReserve
	if reserve == 0 {
		reserve = min(25*time.Millisecond, targetRate/10)
	}

	return paceBudget{
		duration:      duration,
		finishReserve: min(reserve, duration/2),
		revision:      p.revision,
	}
}

// noteBuilt is called once for a successful build, using its original budget.
// Acquisition and external waiting are not included in elapsed or tail. A slow
// finish gets its reserve immediately; a cheaper finish releases it gradually.
// Reserve samples alone never renew committee-capacity history.
func (p *committeePace) noteBuilt(
	used paceBudget,
	elapsed, tail time.Duration,
	limited, artificial bool,
) {
	if !limited || artificial || elapsed <= 0 || tail < 0 || tail > elapsed {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if used.revision != p.revision || used.duration <= 0 {
		return
	}

	// Bound the reserve so one exceptional finish cannot eliminate useful work.
	// The remaining overrun is visible to certificate feedback and build metrics.
	observed := min(tail+tail/4, used.duration/2)
	reserve := p.finishReserve
	if reserve == 0 {
		reserve = used.finishReserve
	}
	if observed >= reserve {
		p.finishReserve = observed
		p.tailMeasured = true
		return
	}
	p.finishReserve = reserve - (reserve-observed)/16
	p.tailMeasured = true
}

func (p *committeePace) noteEmitted(id simplex.CandidateID, emission paceEmission) {
	if emission.transactions == 0 || emission.targetRate <= 0 || emission.budget.duration <= 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.emitted[id]; exists {
		return
	}
	var oldestID simplex.CandidateID
	var oldestAt time.Time
	for pending, previous := range p.emitted {
		if emission.at.Sub(previous.at) > committeePaceEmissionRetention {
			delete(p.emitted, pending)
			continue
		}
		if oldestAt.IsZero() || previous.at.Before(oldestAt) {
			oldestID, oldestAt = pending, previous.at
		}
	}
	if len(p.emitted) >= committeePaceEmissionLimit {
		delete(p.emitted, oldestID)
	}
	p.emitted[id] = emission
}

func (p *committeePace) discardEmission(id simplex.CandidateID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.emitted, id)
	p.notifyCapacityLocked()
}

// waitForCapacity bounds the work already handed to a slower committee. It
// waits before acquiring/building another natural candidate, never while
// holding the controller lock. Already running speculative futures can finish;
// this is backpressure, not cancellation of useful work or shrinking old debt.
func (p *committeePace) waitForCapacity(ctx context.Context, targetRate time.Duration) error {
	if targetRate <= 0 {
		return nil
	}
	waited := false
	for {
		if waited {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		p.mu.Lock()
		now := time.Now()
		var pending int
		var expires time.Time
		for id, emission := range p.emitted {
			deadline := emission.at.Add(committeePaceEmissionRetention)
			if !deadline.After(now) {
				delete(p.emitted, id)
				p.notifyCapacityLocked()
				continue
			}
			if !emission.demand || emission.artificial {
				continue
			}
			pending++
			if expires.IsZero() || deadline.Before(expires) {
				expires = deadline
			}
		}
		if pending < committeePaceOutstandingLimit {
			p.mu.Unlock()
			// An open gate preserves the existing pipeline cancellation path.
			// Only actual capacity waits take ownership of cancellation here.
			return nil
		}
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return err
		}
		if p.capacityWake == nil {
			p.capacityWake = make(chan struct{})
		}
		p.backpressured = true
		wake := p.capacityWake
		p.mu.Unlock()

		waited = true
		timer := time.NewTimer(time.Until(expires))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (p *committeePace) notifyCapacityLocked() {
	if p.capacityWake != nil {
		close(p.capacityWake)
		p.capacityWake = nil
	}
}

// noteCertified returns the adjacent certificate interval and whether it was
// informative about current capacity. Only the growth of lag matters: an old,
// constant queue is not charged repeatedly to newly timely blocks. Every change
// gets a revision, so already emitted blocks cannot compound an old slowdown.
func (p *committeePace) noteCertified(id simplex.CandidateID, at time.Time) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	emission, known := p.emitted[id]
	// A later certificate also retires skipped or superseded earlier slots.
	// They must not hold admission closed until the retention timer: consensus
	// has already moved past them, even when the new candidate was not ours.
	for pending := range p.emitted {
		if pending.Slot <= id.Slot {
			delete(p.emitted, pending)
		}
	}
	p.notifyCapacityLocked()
	if !known {
		return 0, false
	}
	if at.Before(emission.at) || at.Sub(emission.at) > committeePaceEmissionRetention {
		p.resetIntervalsLocked()
		return 0, false
	}
	previous := p.last
	if !previous.at.IsZero() && (id.Slot <= previous.id.Slot || !at.After(previous.at)) {
		p.resetIntervalsLocked()
		return 0, false
	}
	p.last = paceCertificate{id: id, at: at, emission: emission}

	adjacent := emission.window == previous.emission.window &&
		emission.parent == simplex.Parent(previous.id) && id.Slot == previous.id.Slot+1
	fresh := emission.budget.revision == p.revision && previous.emission.budget.revision == p.revision
	ordered := !previous.at.IsZero() && emission.at.After(previous.emission.at)
	currentWork := emission.demand && !emission.artificial && emission.elapsed > 0
	previousWork := previous.emission.demand && !previous.emission.artificial && previous.emission.elapsed > 0
	if !adjacent || !fresh || !ordered || !currentWork || !previousWork {
		p.resetIntervalsLocked()
		return 0, false
	}

	interval := at.Sub(previous.at)
	emitInterval := emission.at.Sub(previous.emission.at)
	slack := max(20*time.Millisecond, emission.targetRate/10)
	lagGrowth := interval - emitInterval
	p.intervals++
	p.certifiedSpan += interval
	p.emittedSpan += emitInterval
	meanInterval := p.certifiedSpan / time.Duration(p.intervals)
	meanLagGrowth := (p.certifiedSpan - p.emittedSpan) / time.Duration(p.intervals)
	if interval > emission.targetRate+slack && (lagGrowth > slack || p.backpressured) {
		p.slowIntervals++
	}
	// Once admission is backpressured, emission follows the slow certificates
	// and lag stops growing by construction. That does not make the old budget
	// sustainable. Measure the whole span, not two individual delayed ACKs:
	// delivery jitter can be followed by a batch of timely certificates.
	slow := meanInterval > emission.targetRate+slack && (meanLagGrowth > slack || p.backpressured)
	timely := interval <= emission.targetRate+slack && lagGrowth <= slack
	spanTimely := meanInterval <= emission.targetRate+slack && meanLagGrowth <= slack
	// One exceptional interval is not sustained congestion. Repeated slow
	// spans are: a heavy block every fourth slot must not evade the detector.
	if p.intervals == 4 && slow && (p.slowIntervals >= 2 || p.slowSpan) {
		p.recordEvidenceLocked(at)
		factor := math.Max(0.5, math.Min(0.8, float64(emission.targetRate)/float64(meanInterval)))
		p.congested = true
		p.changeFractionLocked(p.fraction*factor, emission.targetRate)
		p.resetIntervalsLocked()
		return interval, true
	}
	// Two short catch-up intervals must not erase an earlier long stall. The
	// entire observed prefix must keep up before another upward probe is safe.
	canGrow := timely && spanTimely && emission.saturated() && emission.elapsed <= emission.targetRate+slack
	if !canGrow {
		p.goodIntervals = 0
		if p.intervals == 4 {
			p.resetIntervalsLocked()
			p.slowSpan = slow
		}
		return interval, false
	}

	p.recordEvidenceLocked(at)
	p.goodIntervals++
	factor := 1.5
	if p.congested {
		// Leader windows are intermittent. Four samples per 8% step kept a
		// recovered committee underfilled for minutes after one slowdown.
		factor = 1.25
	}
	// After a slow span, verify a complete recovery span before probing.
	// Otherwise two catch-up ACKs between periodic stalls erase the evidence.
	if p.goodIntervals >= 2 && (!p.slowSpan || p.intervals == 4) {
		p.changeFractionLocked(p.fraction*factor, emission.targetRate)
		p.resetIntervalsLocked()
	} else if p.intervals == 4 {
		p.resetIntervalsLocked()
	}
	return interval, true
}

func (p *committeePace) recordEvidenceLocked(at time.Time) {
	p.lastSample = at
	if p.samples < math.MaxUint32 {
		p.samples++
	}
}

func (p *committeePace) resetIntervalsLocked() {
	p.goodIntervals = 0
	p.slowIntervals = 0
	p.intervals = 0
	p.certifiedSpan = 0
	p.emittedSpan = 0
	p.slowSpan = false
}

func (p *committeePace) changeFractionLocked(fraction float64, targetRate time.Duration) {
	fraction = clampPaceFraction(fraction, targetRate)
	if p.fraction == fraction {
		return
	}
	p.fraction = fraction
	p.revision++
	p.backpressured = false
}

func clampPaceFraction(fraction float64, targetRate time.Duration) float64 {
	minimum := min(committeePaceMaxFraction,
		max(committeePaceMinFraction, float64(committeePaceMinBudget)/float64(targetRate)))
	return max(minimum, min(committeePaceMaxFraction, fraction))
}
