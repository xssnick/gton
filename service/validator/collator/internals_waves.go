package collator

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// internalWaveWorkers is the default shared budget for message-wave pools.
//
// A wave is a bounded run of consecutive queued messages. Different destination
// accounts execute independently; repeated destinations form per-account chains
// whose successor starts only after its predecessor commits. An account's
// transaction reads nothing another account's transaction writes, and its
// logical time is floored by its own last transaction and the block start, never
// by another account's, so the ready chain heads can be emulated on separate
// goroutines. What cannot be parallel is everything that is order-dependent:
// whether the block still has room for the message, the processed bound, the
// in/out descriptors, the queue, the size estimate, the account's committed
// state. All of that is done by retiring the results in queue order, on the
// main goroutine, which is also where each lane's reads are replayed into the
// shared record. The block is therefore the block a sequential run produces:
// the same admission decisions at the same points, the same record, the same
// bytes. A result the block turns out not to have room for is discarded
// together with every trace of having computed it.
//
// It is sized against collationParallelism like the other worker pools the
// build runs.
const internalWaveWorkers = collationParallelism

// internalWaveLength caps how far ahead of admission the collation plans and
// speculates, and therefore how much work it can discard at the end of a full
// block.
const internalWaveLength = 4 * internalWaveWorkers

// internalWaveParallelism is the shared message-wave worker budget, after the
// request's override: zero takes the default, a negative value keeps the
// sequential loops, and one runs the wave machinery inline — every message is
// speculated through its lane tracer and retired on the spot, which is the arm
// the byte-identity tests compare against both others.
func (c *collation) internalWaveParallelism() int {
	override := c.req.internalWaveWorkers
	switch {
	case override < 0:
		return 0
	case override > 0:
		return min(override, runtime.GOMAXPROCS(0))
	}
	return min(internalWaveWorkers, runtime.GOMAXPROCS(0))
}

// waveState is everything the wave loop reuses across the waves of one
// collation: the plans, the per-account dependency tails and the worker pool.
//
// Reused rather than rebuilt because a wave is a small unit of work — sixty-four
// messages at most — and a block is many of them. Rebuilding meant a goroutine
// per worker per wave, a channel per wave, a map per wave and a heap plan per
// message: on a thousand-message block, megabytes of garbage for a phase whose
// own working set is tiny. None of it made the phase faster; it made the
// collector busier, and with two collations now running side by side the
// allocation rate is the binding resource rather than the CPU.
type waveState struct {
	// arena is the plans themselves, sized once at the cap a wave cannot exceed
	// so it never grows and never moves — every pointer handed out stays valid
	// for the wave that holds it.
	arena []internalPlan
	plans []*internalPlan
	tails map[[32]byte]*internalPlan

	queue       chan *internalPlan
	successors  chan *internalPlan
	workers     sync.WaitGroup
	workerCount int
	// abandoned tells the workers the current wave's results are no longer
	// wanted. It belongs to the wave, not to the pool, and is reset before each
	// wave is dispatched — safe because retirement waits for every plan of a
	// wave before the next one is planned, so no worker of an earlier wave can
	// still be looking at it.
	abandoned atomic.Bool
}

// start brings up the pool. Workers outlive the wave, so a block pays for them
// once instead of once per wave.
func (w *waveState) start(c *collation, workers int) {
	if w.arena == nil {
		w.arena = make([]internalPlan, internalWaveLength)
		w.plans = make([]*internalPlan, 0, internalWaveLength)
		w.tails = make(map[[32]byte]*internalPlan, internalWaveLength)
	}
	if workers <= 1 {
		return
	}
	if w.queue == nil {
		w.queue = make(chan *internalPlan, internalWaveLength)
		w.successors = make(chan *internalPlan, internalWaveLength)
	}
	for w.workerCount < workers {
		w.workerCount++
		w.workers.Add(1)
		go func() {
			defer w.workers.Done()
			for {
				plan, ok := w.next()
				if !ok {
					return
				}
				if w.abandoned.Load() {
					plan.wg.Done()

					continue
				}
				c.speculateInternalChain(plan)
			}
		}()
	}
}

// next gives an account-chain successor priority over speculative look-ahead.
// The first select makes the priority strict when both queues are already
// ready. If a worker was waiting before the successor became ready, the second
// select may take one ordinary plan first, but never drains the whole look-ahead
// queue ahead of the dependency that canonical retirement is about to need.
func (w *waveState) next() (*internalPlan, bool) {
	select {
	case plan := <-w.successors:
		return plan, true
	default:
	}

	select {
	case plan := <-w.successors:
		return plan, true
	case plan, ok := <-w.queue:
		return plan, ok
	}
}

// stop drains the pool. Nothing may still be emulating when the phase ends: a
// worker running into the next phase would be reading a lane that phase writes.
func (w *waveState) stop() {
	if w.queue == nil {
		return
	}
	close(w.queue)
	w.workers.Wait()
	w.queue = nil
	w.successors = nil
	w.workerCount = 0
}

// take hands out the next plan of the wave being planned, zeroed. The arena is
// only ever reused after the wave that held it has fully retired or discarded,
// so a reused entry cannot be one another goroutine still reads. The event
// buffer is the one thing carried over: it was emptied when its wave replayed
// or discarded it, and its capacity is the whole point of keeping it.
func (w *waveState) take(msg *msgpool.InternalMessage) *internalPlan {
	plan := &w.arena[len(w.plans)]
	events := plan.events
	*plan = internalPlan{msg: msg, events: events[:0]}

	return plan
}

// processInternalsInWaves is processInternals with the transactions of each
// wave emulated concurrently. See internalWaveWorkers for why this produces
// the sequential loop's block.
// base is the index the first of inputs holds in the block's whole canonical
// order, so a resumed pass reports the cursor in the caller's terms rather than
// in those of the tail it was handed.
func (c *collation) processInternalsInWaves(inputs []*msgpool.InternalMessage, records []tlb.ProcessedUptoRecord, workers, base int) error {
	c.waves.start(c, 1)
	defer c.waves.stop()

	next := 0
	for next < len(inputs) {
		// The same three questions the retire loop asks before every plan, asked
		// once more before a wave is planned at all. The answers cannot differ —
		// nothing has run in between — so the block is the same block; what it
		// avoids is planning and speculating a whole wave that the first
		// retirement would have thrown away, which is precisely what a nearly
		// full block does on every remaining wave.
		c.updateCollatedEstimate()
		if !c.limits.fits(c.fullMark()) {
			c.blockFull = true
			c.internalsCursor = base + next

			return nil
		}
		if c.internalMsgExpired() {
			c.blockFull = true
			c.blockFullTimeout = true
			c.stats.InternalMsgTimeouts++
			c.internalsCursor = base + next

			return nil
		}
		if err := c.ctx.Err(); err != nil {
			return err
		}

		plans, ready := c.planWave(inputs[next:], records)
		if len(plans) == 0 {
			// Unreachable by construction: the first message of a wave is always
			// planned, because the length cap cannot be met at zero. Kept
			// because the failure it guards is the worst one this loop has — no
			// progress means no return, and a producer that never returns loses
			// the whole leader window rather than one slot. Reusing the plan state
			// across waves makes that reachable from a missing reset, and a hang is
			// the one defect a test reports by never finishing.
			return fmt.Errorf("%w: inbound internal wave planned no messages", ErrInvalidInput)
		}
		waveWorkers := min(workers, ready)
		c.waves.start(c, waveWorkers)
		stop, err := c.runWave(plans, ready, waveWorkers, base+next)
		if err != nil || stop {
			return err
		}
		next += len(plans)
		c.internalsCursor = base + next
	}

	return nil
}

// planWave takes the longest prefix of inputs, up to internalWaveLength. A
// repeated destination no longer ends the wave: the first plan of an account's
// chain becomes ready immediately and each later plan hangs off it, to be
// started once retirement commits its predecessor. Skip and relay plans take no
// account and never block another one.
func (c *collation) planWave(inputs []*msgpool.InternalMessage, records []tlb.ProcessedUptoRecord) ([]*internalPlan, int) {
	w := &c.waves
	w.plans = w.plans[:0]
	clear(w.tails)
	ready := 0
	for _, msg := range inputs {
		if len(w.plans) == internalWaveLength {
			break
		}
		plan := w.take(msg)
		plan.err = c.planInternalInto(plan, records)
		if plan.err == nil && plan.action == internalExecute {
			plan.executes = true
			if tail := w.tails[plan.key]; tail != nil {
				plan.dependsOn = tail
				tail.follows = plan
			} else {
				if lane := c.lanes[plan.key]; lane != nil {
					plan.lane = lane
				}
				ready++
			}
			w.tails[plan.key] = plan
		}
		w.plans = append(w.plans, plan)
	}

	return w.plans, ready
}

// runWave emulates the wave's transactions and retires every plan in order.
// It reports stop when the block filled or the internal-message budget ran
// out, which ends the phase exactly as the sequential loop's early return
// does, and it never returns while a worker is still running.
func (c *collation) runWave(plans []*internalPlan, ready, workers, base int) (stop bool, err error) {
	w := &c.waves
	w.abandoned.Store(false)
	retired := 0
	defer func() {
		w.abandoned.Store(true)
		// Plans past the stopping point were speculated for nothing. Their
		// lanes drop the detached reads unreplayed; a lane resolved for such a
		// plan was never registered and is simply forgotten.
		//
		// Two passes, joins first: a chained successor may still be emulating
		// through the tracer its retired-or-discarded predecessor also used, so
		// no tracer is touched until every plan of the wave has finished. The
		// started flag of a chained plan is written by the worker, but always
		// before its predecessor's wg is released and the predecessor always
		// stands earlier — in the retired prefix or in this same loop — so by
		// the time the flag is read here, a join has ordered the write.
		for _, plan := range plans[retired:] {
			if !plan.executes || !plan.started {
				continue
			}
			plan.wg.Wait()
			c.stats.InternalsDiscarded++
		}
		for _, plan := range plans[retired:] {
			if !plan.executes || !plan.started {
				continue
			}
			plan.releaseEvents()
			if plan.lane != nil {
				plan.lane.tracer.discard()
			}
		}
	}()
	if ready > 0 {
		for _, plan := range plans {
			if plan.executes && plan.dependsOn == nil {
				if err = c.dispatchInternalPlan(plan, workers); err != nil {
					return false, err
				}
			}
		}
	}

	for _, plan := range plans {
		c.updateCollatedEstimate()
		if !c.limits.fits(c.fullMark()) {
			c.blockFull = true
			c.internalsCursor = base + retired
			return true, nil
		}
		// collator.cpp:4141-4146: the reference stops importing at the soft
		// boundary and sets block_full_, so the rest of the collation behaves
		// as if a limit axis had filled — the remaining generated messages are
		// enqueued rather than delivered — and the block still publishes.
		if c.internalMsgExpired() {
			c.blockFull = true
			c.blockFullTimeout = true
			c.stats.InternalMsgTimeouts++
			c.internalsCursor = base + retired
			return true, nil
		}
		if err = c.ctx.Err(); err != nil {
			return false, err
		}
		if err = c.retireInternal(plan); err != nil {
			return false, err
		}
		retired++
		// A chained successor is already running — the worker started it off
		// this plan's emulated post-state — and dispatching it again would
		// start it twice. The flag is settled: retirement waited on this plan,
		// and the worker decides before releasing it.
		if plan.follows != nil && !plan.follows.chained {
			if err = c.dispatchInternalPlan(plan.follows, workers); err != nil {
				return false, err
			}
		}
		c.updatePeakLoad()
	}
	return false, nil
}

func (c *collation) dispatchInternalPlan(plan *internalPlan, workers int) error {
	if !plan.executes {
		return fmt.Errorf("%w: inbound internal plan %x does not execute", ErrInvalidInput, plan.hash)
	}
	if plan.started {
		return fmt.Errorf("%w: inbound internal plan %x already started", ErrInvalidInput, plan.hash)
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if plan.lane == nil {
		if lane := c.lanes[plan.key]; lane != nil {
			plan.lane = lane
		}
	}
	if plan.lane != nil {
		plan.lane.tracer.speculate()
	}
	plan.started = true
	plan.wg.Add(1)
	if workers <= 1 || c.waves.queue == nil {
		c.speculateInternal(plan)
		return nil
	}
	queue := c.waves.queue
	if plan.dependsOn != nil {
		queue = c.waves.successors
	}

	select {
	case <-c.ctx.Done():
		plan.wg.Done()
		plan.started = false
		if plan.lane != nil {
			plan.lane.tracer.discard()
		}
		return c.ctx.Err()
	case queue <- plan:
		return nil
	}
}

// speculateInternal resolves the plan's account if it has no lane yet and
// emulates its transaction, all against the lane's own tracer. It is the
// inline arm's form — one plan, no chaining — so the byte-parity arm keeps
// exactly the schedule it always had.
func (c *collation) speculateInternal(plan *internalPlan) {
	c.speculateInternalPlan(plan, nil)
	plan.wg.Done()
}

// speculateInternalChain runs on a worker: it emulates the plan and then keeps
// going down the plan's account chain, each successor started straight off its
// predecessor's emulated post-state instead of waiting for the main goroutine
// to retire that predecessor and dispatch it — the wait that used to park
// retirement for a whole emulation on every in-wave chain link. Retirement
// itself still happens strictly in queue order on the main goroutine; only the
// moment a successor's emulation starts moves.
//
// The ordering that makes the handoff safe: everything a chained start writes
// — the successor's lane, its started, chained and wg state, the
// predecessor's detached event segment — is written before the predecessor's
// wg is released, and the main goroutine always waits on the predecessor
// before it looks at any of it.
func (c *collation) speculateInternalChain(plan *internalPlan) {
	var from *tvm.TransactionExecutionResult
	for {
		c.speculateInternalPlan(plan, from)
		next := plan.follows
		if next == nil || !c.chainInternal(plan, next) {
			plan.wg.Done()
			return
		}
		from = plan.result
		plan.wg.Done()
		plan = next
	}
}

// chainInternal decides whether the worker may start follows now, and when it
// may, marks it started — on the worker, strictly before the predecessor's wg
// release, so the retire loop and the discard path read a settled flag.
//
// Not on the masterchain: its retire path parses lane-traced cells
// (publicLibraryDiffCount walks both account states), and those parses must
// reach the shared record at the retirement they happen in — which they do
// only with the tracer in pass-through, which a lane with a chained successor
// in flight is not. Off the masterchain the retire path parses no lane-traced
// cell: the estimate walks descend raw references and never notify a trace,
// and the outbound-message walk detaches its root and stops at special cells.
//
// A failed predecessor never chains: its successor would have nothing to start
// from, and retirement is about to fail the collation on it anyway, exactly
// where the sequential loop would have. An abandoned wave never chains either
// — the flag is racy by nature, but late is safe: the successor is emulated
// for nothing and the discard path joins and drops it like any other.
func (c *collation) chainInternal(plan, next *internalPlan) bool {
	if c.master != nil || plan.execErr != nil || c.waves.abandoned.Load() {
		return false
	}
	next.lane = plan.lane
	next.chained = true
	next.started = true
	next.wg.Add(1)
	atomic.AddUint32(&c.stats.InternalsChained, 1)

	return true
}

// speculateInternalPlan is one emulation. from is the predecessor's emulated
// result for a chained successor — the state its retirement will commit — and
// nil for every other plan, whose lane fields are current by construction: the
// last committed state for a lane that exists, the resolve below for one that
// does not. The plan's reads are detached into the plan before the caller
// releases its wg, so the shared tracer buffer never holds two plans' events
// when a successor starts appending. The wg release is the caller's: a chained
// start must be published before it.
func (c *collation) speculateInternalPlan(plan *internalPlan, from *tvm.TransactionExecutionResult) {
	// Counted by the planner's goroutine would be cleaner, but the plan count
	// is what the planner knows and the speculation count is what matters;
	// the increment is on the worker, under the plan's own done channel, and
	// the main goroutine reads stats only after the wave has drained.
	atomic.AddUint32(&c.stats.InternalsSpeculated, 1)
	lane := plan.lane
	if lane == nil {
		resolved, err := c.resolveLaneSpeculative(plan.key)
		if err != nil {
			plan.execErr = err
			return
		}
		lane = resolved
		plan.lane = lane
		plan.fresh = true
	}
	// The lane fields are read only for a chain head: for a chained successor
	// the main goroutine may be committing the predecessor into them right
	// now, and the predecessor's result already carries the state that commit
	// will install.
	var current *tvm.PreparedAccount
	var storageStat *cell.Cell
	if from != nil {
		current, storageStat = from.NextAccount, from.AccountStorageStat
	} else {
		current, storageStat = lane.current, lane.storageStat
	}
	plan.result, plan.execErr = c.emulateFrom(lane, current, storageStat, plan.prepared, c.importAfterLT(lane))
	plan.events = lane.tracer.detachEvents(plan.events)
}

// resolveLaneSpeculative is resolveLane with the tracer buffering from the
// first read, so the descent that finds the account is buffered with the rest.
func (c *collation) resolveLaneSpeculative(key [32]byte) (*accountLane, error) {
	return c.resolveLaneWith(key, true)
}
