package collator

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// generatedWaveLTWindow is the narrow speculative window for generated
// messages. A generated transaction starts strictly after the input message lt,
// and every output starts after that transaction, so an item at lt L can only
// emit messages at L+2 or above. That makes the [L, L+1] window closed under
// heap order: nothing retired inside it can inject a message that belongs back
// into it.
const generatedWaveLTWindow = 1

// generatedWaveMinParallelWidth is the first width at which the worker pool
// consistently beats the sequential generated-message drain. Smaller runs keep
// the canonical serial path and do not start the pool.
const generatedWaveMinParallelWidth = 8

// generatedWaveWorkers caps the generated-message pool. Four workers were
// faster than wider pools across measured 8, 16 and 32-message waves; this phase
// has enough serial retirement between executions that more workers only add
// scheduling pressure.
const generatedWaveWorkers = 4

// Chunks keep plan pointers stable while avoiding a full internalWaveLength
// arena allocation for the common short generated-message run.
const generatedWaveArenaChunk = generatedWaveMinParallelWidth

// generatedPlanningTrace replaces every inherited trace in the dispatch-queue
// view used by the eligibility probe. It is immutable and records nothing, so
// the one instance is safe to share between collations.
type generatedPlanningTrace struct {
	trace *cell.Trace
}

var generatedPlanTrace = func() *cell.Trace {
	listener := &generatedPlanningTrace{}
	listener.trace = cell.NewTraceForListener(listener)

	return listener.trace
}()

func (*generatedPlanningTrace) OnLoad(*cell.Cell) {}

func (*generatedPlanningTrace) OnCreate() {}

func (t *generatedPlanningTrace) ChildTrace(int) *cell.Trace {
	return t.trace
}

func (*generatedPlanningTrace) PendingError() error { return nil }

type generatedPlan struct {
	item        newMessage
	prepared    *tvm.PreparedMessage
	internal    *tlb.InternalMessage
	source      msgpool.AccountPrefix
	destination msgpool.AccountPrefix
	sourceID    [32]byte
	key         [32]byte

	started   bool
	discarded bool
	lane      *accountLane
	fresh     bool
	result    *tvm.TransactionExecutionResult
	execErr   error
	wg        sync.WaitGroup
}

type generatedWaveState struct {
	arena           []*[generatedWaveArenaChunk]generatedPlan
	plans           []*generatedPlan
	seen            map[[32]byte]struct{}
	senderGenerated map[[32]byte]uint32
	dispatchQueued  map[[32]byte]bool
	queue           chan *generatedPlan
	workers         sync.WaitGroup
	abandoned       atomic.Bool
}

func (w *generatedWaveState) start(c *collation, workers int) {
	if w.plans == nil {
		w.plans = make([]*generatedPlan, 0, generatedWaveArenaChunk)
		w.seen = make(map[[32]byte]struct{}, generatedWaveArenaChunk)
	}
	if workers <= 1 || w.queue != nil {
		return
	}
	w.queue = make(chan *generatedPlan, internalWaveLength)
	for range workers {
		w.workers.Add(1)
		go func() {
			defer w.workers.Done()
			for plan := range w.queue {
				if w.abandoned.Load() {
					plan.wg.Done()

					continue
				}
				c.speculateGenerated(plan)
			}
		}()
	}
}

func (w *generatedWaveState) stop() {
	if w.queue == nil {
		return
	}
	close(w.queue)
	w.workers.Wait()
	w.queue = nil
}

func (w *generatedWaveState) take(item newMessage) *generatedPlan {
	index := len(w.plans)
	chunk := index / generatedWaveArenaChunk
	if chunk == len(w.arena) {
		w.arena = append(w.arena, new([generatedWaveArenaChunk]generatedPlan))
	}
	plan := &w.arena[chunk][index%generatedWaveArenaChunk]
	*plan = generatedPlan{item: item}
	return plan
}

func (c *collation) generatedWaveParallelism(enqueueOnly bool) int {
	if enqueueOnly || c.master != nil {
		return 0
	}
	workers := min(c.internalWaveParallelism(), generatedWaveWorkers)
	// A default single worker cannot overlap execution and only pays for wave
	// planning. The explicit one-worker override remains the byte-parity arm.
	if workers < 2 && c.req.internalWaveWorkers == 0 {
		return 0
	}
	return workers
}

func (c *collation) processNewMessagesInWaves(enqueueOnly bool, workers int) error {
	enqueueOnly = enqueueOnly || c.enqueueOnlyLatched
	defer func() {
		if enqueueOnly {
			c.enqueueOnlyLatched = true
		}
	}()
	c.generatedWaves.start(c, 1)

	for c.new.Len() > 0 {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		c.checkNewMessageTop()
		if c.blockFull || c.haveUnprocessedDispatchQueue || c.new[0].dispatchEnvelope != nil {
			if err := c.processNextNewMessage(&enqueueOnly); err != nil {
				return err
			}
			continue
		}

		plans := c.planGeneratedWave()
		planWorkers := workers
		if workers > 1 && len(plans) < generatedWaveMinParallelWidth {
			planWorkers = 0
		}
		if len(plans) == 0 {
			if err := c.processNextNewMessage(&enqueueOnly); err != nil {
				return err
			}
			continue
		}

		c.generatedWaves.start(c, planWorkers)
		if err := c.runGeneratedPlans(plans, planWorkers, &enqueueOnly, true); err != nil {
			return err
		}
	}
	return nil
}

func (c *collation) planGeneratedWave() []*generatedPlan {
	w := &c.generatedWaves
	w.plans = w.plans[:0]
	clear(w.seen)

	// This is only an eligibility probe. Every boundary or failure leaves the
	// canonical top item in the heap so the sequential path owns the exact error
	// and mutation order; already eligible predecessors may still run as a wave.
	baseLT := c.new[0].lt
	state := c.newGeneratedWavePlanState()
	for c.new.Len() > 0 && len(w.plans) < internalWaveLength {
		item := c.new[0]
		if item.dispatchEnvelope != nil {
			break
		}
		if item.parsed.MsgType != tlb.MsgTypeInternal {
			break
		}
		if !item.parallelSafe {
			break
		}
		if item.lt-baseLT > generatedWaveLTWindow {
			break
		}

		item = c.new.popMin()
		plan := w.take(item)
		if err := c.prepareGeneratedPlan(plan); err != nil {
			c.new.push(item)
			break
		}

		immediate, err := c.planGeneratedImmediate(plan, &state)
		if err != nil {
			c.new.push(item)
			break
		}
		if !immediate {
			c.new.push(item)
			break
		}
		key, err := accountIDFromAddress(plan.internal.DstAddr)
		if err != nil {
			c.new.push(item)
			break
		}
		plan.key = key
		if _, repeated := w.seen[plan.key]; repeated {
			c.new.push(item)
			break
		}
		prepared, err := tvm.PrepareMessage(item.root)
		if err != nil {
			c.new.push(item)
			break
		}

		w.seen[plan.key] = struct{}{}
		plan.prepared = prepared
		if lane := c.lanes[plan.key]; lane != nil {
			plan.lane = lane
		}
		w.plans = append(w.plans, plan)
	}
	return w.plans
}

func (c *collation) prepareGeneratedPlan(plan *generatedPlan) error {
	switch plan.item.parsed.MsgType {
	case tlb.MsgTypeExternalOut:
		return nil
	case tlb.MsgTypeInternal:
	default:
		return fmt.Errorf("%w: transaction emitted an inbound external message", ErrInvalidInput)
	}

	internal := plan.item.parsed.AsInternal()
	source, err := accountPrefixFromAddress(internal.SrcAddr)
	if err != nil || !c.shard.ContainsPrefix(source) {
		return fmt.Errorf("%w: generated message source is outside the current shard", ErrInvalidInput)
	}
	destination, err := accountPrefixFromAddress(internal.DstAddr)
	if err != nil {
		return fmt.Errorf("%w: generated message destination: %v", ErrInvalidInput, err)
	}
	sourceID, err := accountIDFromAddress(internal.SrcAddr)
	if err != nil {
		return fmt.Errorf("%w: generated message source: %v", ErrInvalidInput, err)
	}

	plan.internal = internal
	plan.source = source
	plan.destination = destination
	plan.sourceID = sourceID
	return nil
}

type generatedWavePlanState struct {
	queueSize uint64
	dispatch  tlb.DispatchQueueAugDict
	scratch   *generatedWaveState
}

func (s *generatedWavePlanState) currentSenderGenerated(c *collation, sourceID [32]byte) uint32 {
	if value, ok := s.scratch.senderGenerated[sourceID]; ok {
		return value
	}
	return c.senderGenerated[sourceID]
}

func (s *generatedWavePlanState) bumpSenderGenerated(c *collation, sourceID [32]byte) uint32 {
	if s.scratch.senderGenerated == nil {
		s.scratch.senderGenerated = make(map[[32]byte]uint32, 8)
	}
	value := s.currentSenderGenerated(c, sourceID) + 1
	s.scratch.senderGenerated[sourceID] = value
	return value
}

func (c *collation) newGeneratedWavePlanState() generatedWavePlanState {
	w := &c.generatedWaves
	clear(w.senderGenerated)
	clear(w.dispatchQueued)

	return generatedWavePlanState{
		queueSize: c.queueSize,
		dispatch: tlb.DispatchQueueAugDict{
			AugmentedDictionary: c.dispatchQueue.AugmentedDictionary.CopyWithTrace(generatedPlanTrace),
		},
		scratch: w,
	}
}

func (s *generatedWavePlanState) queuedDispatch(sourceID [32]byte) (bool, error) {
	if s.dispatch.IsEmpty() {
		return false, nil
	}
	if queued, ok := s.scratch.dispatchQueued[sourceID]; ok {
		return queued, nil
	}
	if s.scratch.dispatchQueued == nil {
		s.scratch.dispatchQueued = make(map[[32]byte]bool, 8)
	}
	var value cell.Slice
	err := s.dispatch.LoadValueByBytesKeyInto(sourceID[:], &value)
	switch {
	case err == nil:
		s.scratch.dispatchQueued[sourceID] = true
		return true, nil
	case isMissingKey(err):
		s.scratch.dispatchQueued[sourceID] = false
		return false, nil
	default:
		return false, fmt.Errorf("%w: lookup dispatch account %x: %v", ErrInvalidInput, sourceID, err)
	}
}

func (c *collation) planGeneratedImmediate(plan *generatedPlan, state *generatedWavePlanState) (bool, error) {
	if plan.item.dispatchEnvelope != nil {
		return false, nil
	}

	sourceID := plan.sourceID
	source := DispatchAccount{Workchain: plan.source.Workchain, AccountID: sourceID}
	policy := &c.req.dispatch
	if c.config.capabilities&capDeferMessages != 0 && policy.DeferringEnabled && plan.item.index != 0 {
		if !dispatchAccountListed(policy.Whitelist, source) {
			generated := state.bumpSenderGenerated(c, sourceID)
			deferQueueLimit := max(policy.DeferOutQueueSizeLimit, c.config.deferOutQueueSizeLimit)
			if generated >= policy.DeferMessagesAfter || state.queueSize > deferQueueLimit {
				return false, nil
			}
		}
	}
	if c.unprocessedDeferred[sourceID] != 0 {
		return false, nil
	}
	queued, err := state.queuedDispatch(sourceID)
	if err != nil {
		return false, err
	}
	if queued {
		return false, nil
	}

	if !c.shard.ContainsPrefix(plan.destination) {
		return false, nil
	}
	return true, nil
}

func (c *collation) runGeneratedPlans(
	plans []*generatedPlan,
	workers int,
	enqueueOnly *bool,
	firstChecked bool,
) error {
	w := &c.generatedWaves
	w.abandoned.Store(false)
	retired := 0
	defer func() {
		w.abandoned.Store(true)
		for _, plan := range plans[retired:] {
			c.new.push(plan.item)
			c.discardGeneratedPlan(plan)
		}
	}()
	if workers > 0 {
		for _, plan := range plans {
			if err := c.ctx.Err(); err != nil {
				return err
			}
			if plan.lane != nil {
				plan.lane.tracer.speculate()
			}
			plan.started = true
			plan.wg.Add(1)
			if min(workers, len(plans)) <= 1 || w.queue == nil {
				c.speculateGenerated(plan)
				continue
			}
			w.queue <- plan
		}
	}

	for i, plan := range plans {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if i != 0 || !firstChecked {
			c.checkNewMessageTop()
		}
		if err := c.retireGeneratedPlan(plan, enqueueOnly); err != nil {
			return err
		}
		retired++
	}
	return nil
}

func (c *collation) speculateGenerated(plan *generatedPlan) {
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
	}
	plan.result, plan.execErr = c.emulate(lane, plan.prepared, plan.item.lt)
}

func (c *collation) commitGeneratedPlan(plan *generatedPlan) (*tvm.TransactionExecutionResult, *accountLane, error) {
	plan.wg.Wait()
	if plan.execErr != nil {
		return nil, nil, plan.execErr
	}
	lane := plan.lane
	if plan.fresh {
		c.registerLane(lane)
	}
	lane.tracer.replay()
	if err := c.commitExecution(plan.result, lane, true); err != nil {
		return nil, nil, err
	}
	return plan.result, lane, nil
}

func (c *collation) discardGeneratedPlan(plan *generatedPlan) {
	if !plan.started || plan.discarded {
		return
	}
	plan.wg.Wait()
	if plan.lane != nil {
		plan.lane.tracer.discard()
	}
	plan.discarded = true
}

func (c *collation) retireGeneratedPlan(plan *generatedPlan, enqueueOnly *bool) error {
	c.limits.extraOutMsgs--
	if c.blockFull || c.haveUnprocessedDispatchQueue {
		*enqueueOnly = true
		c.enqueueOnlyLatched = true
	}

	switch plan.item.parsed.MsgType {
	case tlb.MsgTypeExternalOut:
		out, err := descriptor(0b000, 3, plan.item.root, plan.item.transaction) // msg_export_ext$000
		if err != nil {
			return err
		}
		return c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, plan.item.root, out)
	case tlb.MsgTypeInternal:
	default:
		return fmt.Errorf("%w: transaction emitted an inbound external message", ErrInvalidInput)
	}
	if plan.internal == nil {
		if err := c.prepareGeneratedPlan(plan); err != nil {
			return err
		}
	}

	if plan.item.dispatchEnvelope != nil {
		pending := c.unprocessedDeferred[plan.sourceID]
		if pending == 0 {
			return fmt.Errorf("%w: dispatch message %x has no pending source entry", ErrInvalidInput, plan.item.hash)
		}
		if pending == 1 {
			delete(c.unprocessedDeferred, plan.sourceID)
		} else {
			c.unprocessedDeferred[plan.sourceID] = pending - 1
		}
	} else {
		deferMessage, err := c.shouldDeferGenerated(&plan.item, plan.source, plan.sourceID)
		if err != nil {
			return err
		}
		if deferMessage {
			c.discardGeneratedPlan(plan)
			return c.deferGenerated(&plan.item, plan.sourceID)
		}
	}

	if *enqueueOnly || !c.shard.ContainsPrefix(plan.destination) {
		c.discardGeneratedPlan(plan)
		return c.enqueue(&plan.item, plan.source, plan.destination)
	}

	c.prewarmImmediateAccount(plan.internal.DstAddr)
	return c.retireGeneratedImmediate(plan)
}
