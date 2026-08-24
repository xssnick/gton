package collator

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// The external-message phase runs in waves the way the inbound-internal phase
// does, and for the same reason: two transactions on different accounts are
// independent, so a run of messages with distinct destinations can be emulated
// on separate goroutines and retired in order. Everything order-dependent —
// admission, the attempt budget, the in-descriptor, the generated outputs, the
// committed account state — happens at retirement on the main goroutine, where
// each lane's buffered reads are replayed into the shared record. The block is
// the sequential block: the same decisions at the same points, the same record,
// the same bytes.
//
// Two things the internal wave does not have to think about.
//
// The lt floor. An external transaction's lt is floored by c.lastProcLT, the
// reference's rule that such a transaction must come after every processed
// internal message (executePrepared). That floor is read when the message is
// planned, not when it is retired, so it has to be a constant across the wave
// — and it is: the only writers of lastProcLT are the import phase and
// immediate delivery, neither of which runs inside an external batch.
// retireExternal checks this rather than assuming it, because a future phase
// that moved the bound mid-batch would otherwise produce a block whose lts the
// validator rejects, with nothing in the producer to say why.
//
// The attempt budget. An external costs one TVM attempt whether or not it is
// accepted, and the budget is what stops a flood of rejected messages from
// eating the slot. A wave reserves attempts for what it speculates and gives
// back what it discards, so Stats.ExternalAttempts — which the pool's feedback
// is computed from — reads exactly what a sequential run would have counted.

// externalPlan is one external message as a wave sees it: the plan's verdict
// taken on the main goroutine, the transaction taken on a worker, the two
// joined at retirement.
type externalPlan struct {
	input ExternalInput
	hash  [32]byte
	key   [32]byte
	// verdict is a planning outcome that needs no execution: an invalid
	// message, one outside the shard, one held behind the queue brake. Zero
	// means the message executes.
	verdict msgpool.ExternalOutcome
	// executes marks a plan a worker emulates; the remaining fields are its.
	executes bool
	afterLT  uint64

	lane    *accountLane
	fresh   bool
	result  *tvm.TransactionExecutionResult
	execErr error
	wg      sync.WaitGroup
}

// stopWaves releases both wave pools at the end of the collation that owns
// them. The external one has to be released here rather than where it is
// started: its phase runs once per ready batch, and a pool per batch is a
// goroutine set and a join barrier per batch.
func (c *collation) stopWaves() {
	c.externalWaves.stop()
}

// externalWaveState is the external phase's reusable state, one per collation.
// See waveState: the same shape, kept separate because the two phases can
// interleave around an external wait and a plan of one must never be mistaken
// for a plan of the other.
type externalWaveState struct {
	arena     []externalPlan
	plans     []*externalPlan
	seen      map[[32]byte]struct{}
	queue     chan *externalPlan
	workers   sync.WaitGroup
	abandoned atomic.Bool
}

func (w *externalWaveState) start(c *collation, workers int) {
	if w.arena == nil {
		w.arena = make([]externalPlan, internalWaveLength)
		w.plans = make([]*externalPlan, 0, internalWaveLength)
		w.seen = make(map[[32]byte]struct{}, internalWaveLength)
	}
	if workers <= 1 || w.queue != nil {
		return
	}
	w.queue = make(chan *externalPlan, internalWaveLength)
	for range workers {
		w.workers.Add(1)
		go func() {
			defer w.workers.Done()
			for plan := range w.queue {
				if w.abandoned.Load() {
					plan.wg.Done()

					continue
				}
				c.speculateExternal(plan)
			}
		}()
	}
}

func (w *externalWaveState) stop() {
	if w.queue == nil {
		return
	}
	close(w.queue)
	w.workers.Wait()
	w.queue = nil
}

func (w *externalWaveState) take(input ExternalInput) *externalPlan {
	plan := &w.arena[len(w.plans)]
	*plan = externalPlan{input: input}

	return plan
}

// processExternalBatchInWaves is processExternalBatch with each wave's
// transactions emulated concurrently. It returns exactly what the sequential
// loop returns, for exactly the same prefix of the batch.
func (c *collation) processExternalBatchInWaves(
	externals []ExternalInput,
	deadline time.Time,
	workers int,
) (externalBatchResult, error) {
	if len(externals) == 0 {
		// Nothing to plan, so nothing to start a pool for: a build with no
		// externals would otherwise pay for a worker set it never uses. The
		// result is the one a fully consumed batch returns — the batch is empty
		// and so is its consumed prefix.
		return externalBatchResult{}, nil
	}
	c.prewarmExternalInputs(externals)
	// Started here and stopped by the collation, not by this call. The external
	// phase is entered once per ready batch — eighteen times in a field block —
	// and tearing the pool down at the end of each one costs a goroutine set and
	// a full join barrier per batch, for waves of about five messages. It is the
	// same pathology the internal waves were built to avoid, and start() is
	// idempotent precisely so this call can stay where it reads naturally.
	c.externalWaves.start(c, workers)

	next := 0
	for next < len(externals) {
		// The gates the retire loop asks before every message, asked once more
		// before a wave is planned. Nothing has run in between, so the answers
		// are the same and the block is the same; what this saves is planning
		// and speculating a wave the first retirement would throw away.
		if stop, result := c.externalBatchStops(externals, next, deadline); stop {
			return result, nil
		}
		if err := c.ctx.Err(); err != nil {
			return externalBatchResult{}, err
		}

		plans, lastProcLT := c.planExternalWave(externals[next:])
		if len(plans) == 0 {
			return externalBatchResult{}, fmt.Errorf("%w: external wave planned no messages", ErrInvalidInput)
		}
		result, stop, err := c.runExternalWave(externals, next, plans, lastProcLT, deadline, workers)
		if err != nil || stop {
			return result, err
		}
		next += len(plans)
	}

	return externalBatchResult{consumed: len(externals)}, nil
}

// externalBatchStops is the pair of stops the sequential loop checks ahead of
// every message: the soft limit and the batch deadline. Both record the rest of
// the batch as skipped, exactly as the loop does.
func (c *collation) externalBatchStops(
	externals []ExternalInput,
	at int,
	deadline time.Time,
) (bool, externalBatchResult) {
	c.updateCollatedEstimate()
	if !c.limits.fits(LoadSoft) {
		for _, skipped := range externals[at:] {
			c.recordExternal(skipped.Ref, msgpool.ExternalSkippedLimit)
		}

		return true, externalBatchResult{stop: ExternalStopSoftLimit, consumed: at}
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		for _, skipped := range externals[at:] {
			c.recordExternal(skipped.Ref, msgpool.ExternalSkippedLimit)
		}

		return true, externalBatchResult{stop: ExternalStopDeadline, consumed: at}
	}

	return false, externalBatchResult{}
}

// planExternalWave takes the longest prefix of the batch, up to the wave cap,
// in which no two executing messages share a destination. A message with a
// planning verdict takes no account and never ends a wave.
//
// The attempt budget is reserved here, one per executing plan, so a wave can
// never speculate past the budget a sequential run would have stopped at. The
// reservation is on c.stats directly, which is what the sequential loop
// increments; discarded plans hand theirs back at the end of the wave.
//
// It also returns the lt floor every plan of the wave was given, so that
// retirement can check the floor did not move underneath it.
func (c *collation) planExternalWave(externals []ExternalInput) ([]*externalPlan, uint64) {
	w := &c.externalWaves
	w.plans = w.plans[:0]
	clear(w.seen)
	lastProcLT := c.lastProcLT
	for i := range externals {
		if len(w.plans) == internalWaveLength {
			break
		}
		plan := w.take(externals[i])
		c.planExternalInto(plan)
		if plan.verdict == 0 {
			if _, repeated := w.seen[plan.key]; repeated {
				break
			}
			if int(c.stats.ExternalAttempts) >= c.req.maxExternalAttempts {
				// The budget is gone. The plan stays unexecuted and the retire
				// loop reports the attempt-limit stop at it, as the sequential
				// loop would at the same message.
				w.plans = append(w.plans, plan)

				break
			}
			c.stats.ExternalAttempts++
			w.seen[plan.key] = struct{}{}
			plan.executes = true
			plan.wg.Add(1)
			if lane := c.lanes[plan.key]; lane != nil {
				plan.lane = lane
				plan.afterLT = max(lastProcLT, c.importAfterLT(lane))
				lane.tracer.speculate()
			} else {
				// The lane is resolved on the worker; its dispatch floor is read
				// there, by key, from a map nothing writes during the phase.
				plan.afterLT = lastProcLT
			}
		}
		w.plans = append(w.plans, plan)
	}

	return w.plans, lastProcLT
}

// planExternalInto takes the verdicts the sequential loop takes before it
// executes a message, in the same order. Everything here is a pure function of
// the message and of collation state the wave does not move.
func (c *collation) planExternalInto(plan *externalPlan) {
	prepared := plan.input.message
	root := prepared.Cell()
	message := prepared.Message()
	plan.hash = root.HashKey()
	if root.Level() != 0 || message.MsgType != tlb.MsgTypeExternalIn {
		plan.verdict = msgpool.ExternalInvalid

		return
	}
	destination, err := accountPrefixFromAddress(message.AsExternalIn().DstAddr)
	if err != nil || !c.shard.ContainsPrefix(destination) {
		plan.verdict = msgpool.ExternalInvalid

		return
	}
	// Checked after the message is known to be well-formed and ours, so a
	// message that would never be executable is still purged from the pool
	// rather than held behind the brake.
	if c.externalIntakeClosed() {
		plan.verdict = msgpool.ExternalSkippedLimit

		return
	}
	key, err := accountIDFromAddress(message.AsExternalIn().DstAddr)
	if err != nil {
		plan.verdict = msgpool.ExternalInvalid

		return
	}
	plan.key = key
}

// runExternalWave emulates the wave's transactions and retires every plan in
// order. It never returns while a worker is still running.
func (c *collation) runExternalWave(
	externals []ExternalInput,
	first int,
	plans []*externalPlan,
	lastProcLT uint64,
	deadline time.Time,
	workers int,
) (result externalBatchResult, stop bool, err error) {
	w := &c.externalWaves
	w.abandoned.Store(false)
	executable := 0
	for _, plan := range plans {
		if plan.executes {
			executable++
		}
	}
	if executable > 0 {
		if min(workers, executable) <= 1 || w.queue == nil {
			for _, plan := range plans {
				if plan.executes {
					c.speculateExternal(plan)
				}
			}
		} else {
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
		// lanes drop the buffered reads unreplayed, and the attempts reserved
		// for them go back to the budget: a sequential run never reached them,
		// so it never counted them.
		for _, plan := range plans[retired:] {
			if !plan.executes {
				continue
			}
			plan.wg.Wait()
			c.stats.ExternalAttempts--
			c.stats.InternalsDiscarded++
			if plan.lane != nil {
				plan.lane.tracer.discard()
			}
		}
	}()

	for i, plan := range plans {
		at := first + i
		if halt, batch := c.externalBatchStops(externals, at, deadline); halt {
			return batch, true, nil
		}
		if err = c.ctx.Err(); err != nil {
			return externalBatchResult{}, false, err
		}
		if plan.verdict != 0 {
			c.recordExternal(plan.input.Ref, plan.verdict)
			retired++

			continue
		}
		if !plan.executes {
			// The budget ran out at this message during planning.
			for _, skipped := range externals[at:] {
				c.recordExternal(skipped.Ref, msgpool.ExternalSkippedLimit)
			}

			return externalBatchResult{stop: ExternalStopAttemptLimit, consumed: at}, true, nil
		}
		if err = c.retireExternal(plan, lastProcLT); err != nil {
			return externalBatchResult{}, false, err
		}
		retired++
		c.updatePeakLoad()
	}

	return externalBatchResult{}, false, nil
}

// speculateExternal resolves the plan's account if it has no lane yet and
// emulates its transaction against the lane's own tracer. It runs on a worker
// and writes only into the plan and the lane it owns for the duration of the
// wave.
func (c *collation) speculateExternal(plan *externalPlan) {
	defer plan.wg.Done()
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
		plan.afterLT = max(plan.afterLT, c.importAfterLT(lane))
	}
	plan.result, plan.execErr = c.emulate(lane, plan.input.message, plan.afterLT)
}

// retireExternal applies one speculated external to the collation, in batch
// order: the lane is registered if new, its reads are replayed, and the
// sequential loop's post-execution steps run unchanged.
func (c *collation) retireExternal(plan *externalPlan, lastProcLT uint64) error {
	if c.lastProcLT != lastProcLT {
		// See the file comment. This cannot happen today; if it ever does the
		// wave's transactions were floored against a bound that has since moved,
		// and their lts are wrong in a way only a validator would notice.
		return fmt.Errorf("%w: processed bound moved during an external wave", ErrInvalidInput)
	}
	plan.wg.Wait()
	if plan.execErr != nil {
		return fmt.Errorf("execute external message %x: %w", plan.hash, plan.execErr)
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	lane := plan.lane
	if plan.fresh {
		c.registerLane(lane)
	}
	lane.tracer.replay()
	result := plan.result
	if !result.Accepted {
		c.recordExternal(plan.input.Ref, msgpool.ExternalNotAccepted)

		return nil
	}
	if err := c.commitExecution(result, lane, true); err != nil {
		return fmt.Errorf("execute external message %x: %w", plan.hash, err)
	}
	c.prewarmGeneratedOutputs(result)
	root := plan.input.message.Cell()
	in, err := descriptor(0b000, 3, root, result.TransactionCell) // msg_import_ext$000
	if err != nil {
		return err
	}
	if err = c.insert(c.inMessages.AugmentedDictionary, &c.inDescr, root, in); err != nil {
		return err
	}
	c.recordExternal(plan.input.Ref, msgpool.ExternalIncluded)

	return c.registerOutputs(result, lane, nil, true)
}
