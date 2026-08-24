package collator

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// internalWaveWorkers is how many inbound internal messages execute at once.
//
// A wave is a run of consecutive queued messages whose destinations are all
// different accounts. Within it the transactions are independent — an
// account's transaction reads nothing another account's transaction writes,
// and its logical time is floored by its own last transaction and the block
// start, never by another account's — so they can be emulated on separate
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

// internalWaveLength caps a wave. Waves end at the first repeated destination
// anyway; the cap bounds how far ahead of admission the collation speculates,
// and so how much work it can discard at the end of a full block.
const internalWaveLength = 4 * internalWaveWorkers

// internalWaveParallelism is the worker count the build uses for inbound
// internal messages, after the request's override: zero takes the default, a
// negative value keeps the sequential loop, and one runs the wave machinery
// inline — every message speculated through its lane tracer and retired on the
// spot, which is the arm the byte-identity tests compare against both others.
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
// collation: the plans, the map that ends a wave at a repeated destination, and
// the worker pool.
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
	seen  map[[32]byte]struct{}

	queue   chan *internalPlan
	workers sync.WaitGroup
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
		w.seen = make(map[[32]byte]struct{}, internalWaveLength)
	}
	if workers <= 1 || w.queue != nil {
		return
	}
	w.queue = make(chan *internalPlan, internalWaveLength)
	for range workers {
		w.workers.Add(1)
		go func() {
			defer w.workers.Done()
			for plan := range w.queue {
				if w.abandoned.Load() {
					plan.wg.Done()

					continue
				}
				c.speculateInternal(plan)
			}
		}()
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
}

// take hands out the next plan of the wave being planned, zeroed. The arena is
// only ever reused after the wave that held it has fully retired or discarded,
// so a reused entry cannot be one another goroutine still reads.
func (w *waveState) take(msg *msgpool.InternalMessage) *internalPlan {
	plan := &w.arena[len(w.plans)]
	*plan = internalPlan{msg: msg}

	return plan
}

// processInternalsInWaves is processInternals with the transactions of each
// wave emulated concurrently. See internalWaveWorkers for why this produces
// the sequential loop's block.
// base is the index the first of inputs holds in the block's whole canonical
// order, so a resumed pass reports the cursor in the caller's terms rather than
// in those of the tail it was handed.
func (c *collation) processInternalsInWaves(inputs []*msgpool.InternalMessage, records []tlb.ProcessedUptoRecord, workers, base int) error {
	c.waves.start(c, workers)
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

		plans, executable := c.planWave(inputs[next:], records)
		if len(plans) == 0 {
			// Unreachable by construction: the first message of a wave is always
			// planned, because the length cap cannot be met at zero and the
			// repeated-destination break cannot fire against an empty set. Kept
			// because the failure it guards is the worst one this loop has — no
			// progress means no return, and a producer that never returns loses
			// the whole leader window rather than one slot. Reusing the wave map
			// across waves made that reachable from a single missing reset, and
			// a hang is the one defect a test reports by never finishing.
			return fmt.Errorf("%w: inbound internal wave planned no messages", ErrInvalidInput)
		}
		stop, err := c.runWave(plans, executable, workers, base+next)
		if err != nil || stop {
			return err
		}
		next += len(plans)
		c.internalsCursor = base + next
	}

	return nil
}

// planWave takes the longest prefix of inputs, up to internalWaveLength, in
// which no two executing messages share a destination account. Messages that
// skip or relay take no account and never end a wave. Every executing plan's
// lane tracer is switched to buffering here, on the main goroutine, before any
// worker exists.
func (c *collation) planWave(inputs []*msgpool.InternalMessage, records []tlb.ProcessedUptoRecord) ([]*internalPlan, int) {
	w := &c.waves
	w.plans = w.plans[:0]
	clear(w.seen)
	executable := 0
	for _, msg := range inputs {
		if len(w.plans) == internalWaveLength {
			break
		}
		plan := w.take(msg)
		plan.err = c.planInternalInto(plan, records)
		if plan.err == nil && plan.action == internalExecute {
			if _, repeated := w.seen[plan.key]; repeated {
				break
			}
			w.seen[plan.key] = struct{}{}
			plan.executes = true
			plan.wg.Add(1)
			if lane := c.lanes[plan.key]; lane != nil {
				plan.lane = lane
				lane.tracer.speculate()
			}
			executable++
		}
		w.plans = append(w.plans, plan)
	}

	return w.plans, executable
}

// runWave emulates the wave's transactions and retires every plan in order.
// It reports stop when the block filled or the internal-message budget ran
// out, which ends the phase exactly as the sequential loop's early return
// does, and it never returns while a worker is still running.
func (c *collation) runWave(plans []*internalPlan, executable, workers, base int) (stop bool, err error) {
	w := &c.waves
	w.abandoned.Store(false)
	if executable > 0 {
		if min(workers, executable) <= 1 || w.queue == nil {
			// One worker is the inline arm: the same speculation and replay,
			// no goroutine. A wave of one executing message lands here too,
			// which is what keeps a run of repeated destinations from paying
			// for a pool it cannot use.
			for _, plan := range plans {
				if plan.executes {
					c.speculateInternal(plan)
				}
			}
		} else {
			// The queue is buffered to the wave cap, so this never blocks and
			// the dispatch stays on the main goroutine ahead of retirement.
			for _, plan := range plans {
				if plan.executes {
					w.queue <- plan
				}
			}
		}
	}

	retired := 0
	defer func() {
		w.abandoned.Store(true)
		// Plans past the stopping point were speculated for nothing. Their
		// lanes drop the buffered reads unreplayed; a lane resolved for such a
		// plan was never registered and is simply forgotten.
		for _, plan := range plans[retired:] {
			if !plan.executes {
				continue
			}
			plan.wg.Wait()
			c.stats.InternalsDiscarded++
			if plan.lane != nil {
				plan.lane.tracer.discard()
			}
		}
	}()

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
		c.updatePeakLoad()
	}
	return false, nil
}

// speculateInternal resolves the plan's account if it has no lane yet and
// emulates its transaction, all against the lane's own tracer. It runs on a
// worker and writes only into the plan and the lane it owns for the duration
// of the wave.
func (c *collation) speculateInternal(plan *internalPlan) {
	defer plan.wg.Done()
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
	plan.result, plan.execErr = c.emulate(lane, plan.prepared, c.importAfterLT(lane))
}

// resolveLaneSpeculative is resolveLane with the tracer buffering from the
// first read, so the descent that finds the account is buffered with the rest.
func (c *collation) resolveLaneSpeculative(key [32]byte) (*accountLane, error) {
	return c.resolveLaneWith(key, true)
}
