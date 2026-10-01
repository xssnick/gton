package collator

import "time"

// admissionPace is owned by the canonical retirement goroutine. Workers never
// read it: a deadline closes admission, not an already running transaction.
// Keeping the clock out of the VM preserves consensus execution exactly.
type admissionPace struct {
	budget    time.Duration
	reserve   time.Duration
	started   time.Time
	workStart time.Time
	allowance time.Duration
	wait      time.Duration
	lastWave  time.Duration
	closed    time.Time
	closeWait time.Duration
	limited   bool
}

func newAdmissionPace(budget, reserve time.Duration) admissionPace {
	if budget <= 0 {
		return admissionPace{}
	}

	return admissionPace{
		budget:  budget,
		reserve: max(reserve, 0),
		started: time.Now(),
	}
}

// startWork separates fixed predecessor/queue preparation from marginal
// admission. Preparation is included in the reported body cost, but cannot
// spend every block's entire useful budget merely because the inherited queue
// is large. A quarter of the body budget remains available for useful work;
// the first canonical transaction is also indivisible even if it costs more.
func (p *admissionPace) startWork() {
	if p.budget == 0 {
		return
	}

	p.workStart = time.Now()
	fixed := p.workStart.Sub(p.started)
	p.allowance = max(p.budget-p.reserve-fixed, p.budget/4)
}

func (p *admissionPace) remaining(now time.Time) time.Duration {
	return p.allowance - (now.Sub(p.workStart) - p.wait)
}

// exhausted also serves the empty ready-source path: there is no reason to
// wait for more traffic once useful time is spent, but no actual work was
// refused there, so it must not report a demand-saturated PaceLimited sample.
func (p *admissionPace) exhausted() bool {
	return p.budget > 0 && p.remaining(time.Now()) <= 0
}

func (c *collation) paceExpired() bool {
	p := &c.admission
	if p.budget == 0 {
		return false
	}
	if p.limited {
		return true
	}
	// Do not indefinitely defer an expensive first transaction. Rejected
	// externals and forwarded internals also count as progress: an invalid
	// external flood must not disable the budget by producing no transactions.
	// Speculative ExternalAttempts does not count until a result is retired.
	progress := c.limits.transactions != 0 || c.stats.InternalsImported != 0 ||
		c.stats.ExternalInvalid != 0 || c.stats.ExternalNotAccepted != 0
	if !progress || !p.exhausted() {
		return false
	}

	p.limited = true
	p.close()
	return true
}

func (p *admissionPace) close() {
	if p.budget == 0 || !p.closed.IsZero() {
		return
	}

	p.closed = time.Now()
	p.closeWait = p.wait
}

func (p *admissionPace) observe(stats *Stats) {
	if p.budget == 0 {
		return
	}

	now := time.Now()
	stats.PaceElapsed = max(now.Sub(p.started)-p.wait, 0)
	stats.PaceTail = max(now.Sub(p.closed)-(p.wait-p.closeWait), 0)
	stats.PaceLimited = p.limited
}

// waveLimit trims queued lookahead near the boundary without reducing worker
// concurrency. Last wave wall time is a tail warning, never a per-transaction
// throughput estimate: serial account chains and independent accounts need not
// scale alike. Away from the boundary the existing broad wave is unchanged.
func (p *admissionPace) waveLimit(workers int) int {
	if p.budget == 0 {
		return internalWaveLength
	}
	margin := max(p.lastWave*2, p.allowance/4)
	if p.remaining(time.Now()) > margin {
		return internalWaveLength
	}

	return min(internalWaveLength, max(workers, 1))
}

func (p *admissionPace) beginWave() time.Time {
	if p.budget == 0 {
		return time.Time{}
	}
	return time.Now()
}

func (p *admissionPace) endWave(started time.Time) {
	if !started.IsZero() {
		p.lastWave = time.Since(started)
	}
}
