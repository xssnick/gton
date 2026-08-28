package collator

import (
	"bytes"
	"fmt"
	"math/big"
	"sync"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// validateInternalInputs checks the shape and canonical (lt, hash) order of
// the pool cut before collation starts.
func validateInternalInputs(inputs []*msgpool.InternalMessage) error {
	for i, msg := range inputs {
		if msg == nil || msg.EnvelopeCell == nil || msg.Root == nil {
			return fmt.Errorf("%w: internal %d is incomplete", ErrInvalidInput, i)
		}
		if i > 0 && !internalOrderAdvances(inputs[i-1], msg) {
			return fmt.Errorf("%w: internal %d breaks the (lt, hash) order", ErrInvalidInput, i)
		}
	}
	return nil
}

func internalOrderAdvances(prev, next *msgpool.InternalMessage) bool {
	return msgpool.CompareLtHash(prev, next) < 0
}

// fullMark is the limit class this block calls full. It is the soft boundary
// while the byte budget above it is still reserved for external messages, and
// the medium one once that reserve has been released to the drain. See
// escalateToMediumMark.
func (c *collation) fullMark() LoadClass {
	if c.mediumMark {
		return LoadSoft
	}

	return LoadNormal
}

// escalateToMediumMark releases the reserve. Internals stop at the soft
// boundary (collator.cpp:4141, our LoadNormal) so that externals may carry the
// block from there to the medium one (collator.cpp:4276, our LoadSoft); the
// half between the two marks belongs to externals and to nothing else. When
// the external phase has taken what it can and left some of it, the block would
// otherwise ship short — with the queue it exists to drain untouched — so the
// remainder goes back to internal work.
//
// The latch is cleared with the mark, or raising it would change nothing: every
// consumer of blockFull reads the latch, not the class, and a block that stayed
// latched would enqueue its generated messages instead of delivering them —
// feeding the very queue this pass drains. The one latch that must survive is
// the internal-message timeout, which is a statement about the age of the
// traffic rather than about how full the block is, and no mark makes it false.
func (c *collation) escalateToMediumMark() {
	c.mediumMark = true
	if !c.blockFullTimeout && c.limits.fits(c.fullMark()) {
		c.blockFull = false
	}
}

// processInternals imports the inbound internal message slice: messages are
// consumed in
// (lt, hash) order until the normal limit class is exhausted; the remainder
// stays unimported and forces generated messages into the queue.
func (c *collation) processInternals() error {
	if err := c.processInternalsFrom(0); err != nil {
		return err
	}
	return c.flushQueueAdds()
}

// internalsRemain reports whether the canonical order has messages the block
// has not imported yet.
func (c *collation) internalsRemain() bool {
	return c.internalsCursor < len(c.req.internalMessages())
}

// topUpInternals resumes the inbound import against the raised mark, once per
// block, with whatever of the external phase's own time budget is left.
//
// It is called where the build would otherwise idle: the ready externals are
// drained (or refused outright by the outbound-queue brake) and the phase is
// about to park until the slot boundary in case more arrive. That wait is the
// only slack a loaded shard has, and spending it on the queue costs the
// externals nothing they were going to use — a message that arrives during the
// top-up is still taken on the next pass, under the same medium mark it would
// have been taken under anyway.
//
// The time budget is the INTERNAL one — InternalMsgUntil, enforced per message
// by internalMsgExpired inside the import — and deliberately not the external
// phase's. That distinction is the whole reason the first slot of a window used
// to get nothing out of this: its external deadlines are both
// slotStartTime(slot), which for the first slot is the instant the window
// opened, so both are already in the past when the build starts. Bounding the
// top-up by them made it return on its first line exactly where it was needed
// most, while every other internal phase of the same block went on running
// against the soft deadline. Same budget as the import it resumes, and no
// other.
func (c *collation) topUpInternals() error {
	if c.toppedUp || c.haveUnprocessedDispatchQueue || !c.internalsRemain() {
		return nil
	}
	// Only while the shard is in backlog. A bigger block is not free: it lands
	// on the reference validators' notarization chain, which is what bounds the
	// leader window, and lifting the block limits by half on the test stand cost
	// the network a third of its throughput. So the reserve is released for the
	// one thing that pays for the bytes — a queue deep enough that the brake is
	// already refusing the externals it was reserved for. Read live, after this
	// block's own cleanup and imports have moved it, because that is the depth
	// the next block inherits.
	if !c.externalIntakeClosed() {
		return nil
	}
	c.updateCollatedEstimate()
	if !c.limits.fits(LoadSoft) {
		return nil
	}
	if c.internalMsgExpired() {
		return nil
	}
	c.toppedUp = true
	c.escalateToMediumMark()

	if err := c.processInternalsFrom(c.internalsCursor); err != nil {
		return err
	}
	return c.flushQueueAdds()
}

func (c *collation) processInternalsFrom(from int) error {
	if c.haveUnprocessedDispatchQueue {
		return nil
	}
	all := c.req.internalMessages()
	if from >= len(all) {
		return nil
	}
	inputs := all[from:]
	if len(inputs) == 0 {
		return nil
	}
	records, err := c.parentProcessedRecords()
	if err != nil {
		return err
	}
	if workers := c.internalWaveParallelism(); workers > 0 {
		return c.processInternalsInWaves(inputs, records, workers, from)
	}

	for i, msg := range inputs {
		c.updateCollatedEstimate()
		if !c.limits.fits(c.fullMark()) {
			c.blockFull = true
			c.internalsCursor = from + i
			return nil
		}
		// collator.cpp:4141-4146: the reference stops importing at the soft
		// boundary and sets block_full_, so the rest of the collation behaves as
		// if a limit axis had filled — the remaining generated messages are
		// enqueued rather than delivered — and the block still publishes.
		if c.internalMsgExpired() {
			c.blockFull = true
			c.blockFullTimeout = true
			c.stats.InternalMsgTimeouts++
			c.internalsCursor = from + i
			return nil
		}
		if err = c.ctx.Err(); err != nil {
			return err
		}
		if err = c.importInternal(msg, records); err != nil {
			return err
		}
		c.internalsCursor = from + i + 1
		c.updatePeakLoad()
	}
	return nil
}

// importInternal handles one queued message: validate the envelope against its
// queue key, skip if the parent ProcessedUpto already covers it, execute the
// destination transaction and record the msg_import_fin descriptor — plus the
// msg_export_deq_imm dequeue when the message comes from our own out-queue.
//
// It is planInternal followed by retireInternal. The split exists for
// processInternalsInWaves, which plans several messages, executes the
// transactions they call for on several goroutines, and retires them in order;
// this sequential form is the same two steps with the execution done inside
// retireInternal.
func (c *collation) importInternal(msg *msgpool.InternalMessage, records []tlb.ProcessedUptoRecord) error {
	plan := c.planInternal(msg, records)
	return c.retireInternal(plan)
}

// internalAction is what planInternal decided a message needs.
type internalAction uint8

const (
	// internalSkip: the parent's ProcessedUpto already covers the message.
	internalSkip internalAction = iota
	// internalRelay: the destination is outside this shard; transit only.
	internalRelay
	// internalExecute: the destination is ours and a transaction runs.
	internalExecute
)

// internalPlan is one inbound message taken as far as it can be taken without
// touching the collation: validated, classified, and — when it executes —
// resolved to its account and emulated. Everything that writes into the
// collation, from advancing the processed bound to committing the
// transaction, happens in retireInternal, in message order.
type internalPlan struct {
	msg      *msgpool.InternalMessage
	hash     cell.Hash
	prepared *tvm.PreparedMessage
	cur      msgpool.AccountPrefix
	next     msgpool.AccountPrefix
	action   internalAction
	key      [32]byte
	// err is a planning failure. It is not returned by planInternal, because a
	// sequential collation never examines a message the block filled before;
	// raising it at retirement keeps that order, so a wave reports exactly the
	// error, or the full block, a sequential run would have reported.
	err error

	// executes marks a plan the wave machinery emulates through a lane tracer,
	// either inline or on a worker. It tells retirement to consume that result
	// instead of executing the transaction itself.
	executes  bool
	started   bool
	dependsOn *internalPlan
	follows   *internalPlan
	// chained marks a successor the worker started itself, straight off its
	// predecessor's emulated post-state, without waiting for the predecessor
	// to retire. The worker sets it — together with started and the wg count —
	// strictly before releasing the predecessor's wg, so the main goroutine,
	// which always waits on the predecessor first, observes a settled decision
	// and never dispatches such a plan a second time.
	chained bool

	// Set by speculateInternal, read by retireInternal.
	lane    *accountLane
	fresh   bool
	result  *tvm.TransactionExecutionResult
	execErr error
	// events is the plan's own segment of its lane tracer's buffer: every read
	// its emulation made, detached by the goroutine that emulated it the moment
	// the emulation finished. A chained successor speculates through the same
	// tracer while its predecessor is still unretired, so the buffer cannot be
	// read out of the tracer at retirement — replay must forward exactly the
	// retired plan's own reads, and this is where they live. The slice is kept
	// across arena reuse for its capacity.
	events []laneTraceEvent
	// wg is one worker's completion, not a wave's. A WaitGroup rather than a
	// channel because a plan is reused across the waves of one collation and a
	// closed channel cannot be: at a thousand inbound messages a block that is a
	// thousand channel allocations, for a signal that is raised once and awaited
	// once.
	wg sync.WaitGroup
}

// planInternal validates one queued message and decides what it needs. It
// reads the collation and writes nothing into it.
func (c *collation) planInternal(msg *msgpool.InternalMessage, records []tlb.ProcessedUptoRecord) *internalPlan {
	plan := &internalPlan{msg: msg}
	plan.err = c.planInternalInto(plan, records)
	return plan
}

func (c *collation) planInternalInto(plan *internalPlan, records []tlb.ProcessedUptoRecord) error {
	msg := plan.msg
	env := &msg.Envelope
	hash := msg.Root.HashKey()
	plan.hash = hash
	if msg.EnvelopeCell.Level() != 0 {
		return fmt.Errorf("%w: inbound message %x envelope has a non-zero level", ErrInvalidInput, hash)
	}
	if env.CurAddr.Type != tlb.IntermediateAddressRegular || env.NextAddr.Type != tlb.IntermediateAddressRegular {
		return fmt.Errorf("%w: inbound message %x envelope has a non-regular intermediate address", ErrInvalidInput, hash)
	}

	prepared, err := tvm.PrepareMessage(msg.Root)
	if err != nil {
		return fmt.Errorf("%w: inbound message %x: %v", ErrInvalidInput, hash, err)
	}
	if prepared.Message().MsgType != tlb.MsgTypeInternal {
		return fmt.Errorf("%w: inbound message %x is not internal", ErrInvalidInput, hash)
	}
	internal := prepared.Message().AsInternal()
	if env.EmittedLT != nil {
		if *env.EmittedLT != msg.EnqueuedLT {
			return fmt.Errorf("%w: inbound message %x emitted lt differs from its queue position", ErrInvalidInput, hash)
		}
	} else if internal.CreatedLT != msg.EnqueuedLT {
		return fmt.Errorf("%w: inbound message %x created lt differs from its queue position", ErrInvalidInput, hash)
	}
	if msg.QueueLT < msg.EnqueuedLT {
		// An enqueued_lt below the canonical emission lt is not a valid queue
		// entry: a message cannot have been queued before it was emitted.
		return fmt.Errorf("%w: inbound message %x queue entry lt precedes its emission lt", ErrInvalidInput, hash)
	}
	if env.FwdFeeRemaining.Compare(internal.FwdFee) > 0 {
		return fmt.Errorf("%w: inbound message %x remaining fee exceeds its forward fee", ErrInvalidInput, hash)
	}

	source, err := accountPrefixFromAddress(internal.SrcAddr)
	if err != nil {
		return fmt.Errorf("%w: inbound message %x source: %v", ErrInvalidInput, hash, err)
	}
	destination, err := accountPrefixFromAddress(internal.DstAddr)
	if err != nil {
		return fmt.Errorf("%w: inbound message %x destination: %v", ErrInvalidInput, hash, err)
	}
	cur := msgpool.InterpolatePrefix(source, destination, int(env.CurAddr.UseDestBits))
	next := msgpool.InterpolatePrefix(source, destination, int(env.NextAddr.UseDestBits))
	if !msg.Source.ContainsPrefix(cur) {
		return fmt.Errorf("%w: inbound message %x current address is outside its source shard", ErrInvalidInput, hash)
	}
	if !c.shard.ContainsPrefix(next) {
		return fmt.Errorf("%w: inbound message %x next hop is outside the current shard", ErrInvalidInput, hash)
	}
	if msg.Key.NextHop() != next || msg.Key.MsgHash() != hash {
		return fmt.Errorf("%w: inbound message %x queue key differs from its envelope", ErrInvalidInput, hash)
	}
	if env.CurAddr.UseDestBits >= env.NextAddr.UseDestBits && env.NextAddr.UseDestBits < routingAddressBits {
		return fmt.Errorf("%w: inbound message %x next hop is not nearer to the destination", ErrInvalidInput, hash)
	}
	plan.prepared = prepared
	plan.cur = cur
	plan.next = next

	descr := tlb.ProcessedMsgDescr{
		CurWorkchain:  cur.Workchain,
		CurPrefix:     cur.Prefix,
		NextWorkchain: next.Workchain,
		NextPrefix:    next.Prefix,
		LT:            msg.EnqueuedLT,
		EnqueuedLT:    msg.QueueLT,
		Hash:          hash,
	}
	processed, err := c.shardEndLT.alreadyProcessed(
		records,
		c.shard.Workchain,
		c.shard.Shard,
		&descr,
	)
	if err != nil {
		return fmt.Errorf("%w: check processed info for inbound message %x: %v", ErrInvalidInput, hash, err)
	}
	switch {
	case processed:
		plan.action = internalSkip
	case !c.shard.ContainsPrefix(destination):
		plan.action = internalRelay
		plan.key = [32]byte{}
	default:
		plan.action = internalExecute
		key, err := accountIDFromAddress(internal.DstAddr)
		if err != nil {
			// The same wrapping executePrepared's caller applies, since that is
			// where a sequential run meets this failure.
			return fmt.Errorf("execute inbound message %x: %w", hash, err)
		}
		plan.key = key
	}
	return nil
}

// retireInternal applies one planned message to the collation, in the order
// the messages were queued. A plan that was speculated carries its lane and
// result and is committed from them; one that was not is executed here.
func (c *collation) retireInternal(plan *internalPlan) error {
	if plan.err != nil {
		return plan.err
	}
	msg := plan.msg
	env := &msg.Envelope
	hash := plan.hash

	// The bound advances BEFORE the covered-check: messages skipped as already
	// covered still participate in the ProcessedUpto record, and a validator
	// replaying the block has to arrive at the same bound. The queue-entry lt
	// feeds the shard-end-lt branch of the coverage check; the canonical lt
	// bounds the processing order.
	if err := c.advanceProcessedBound("imported", msg.EnqueuedLT, hash); err != nil {
		return err
	}
	switch plan.action {
	case internalSkip:
		c.stats.InternalsSkipped++
		return nil
	case internalRelay:
		destination, err := accountPrefixFromAddress(plan.prepared.Message().AsInternal().DstAddr)
		if err != nil {
			return fmt.Errorf("%w: inbound message %x destination: %v", ErrInvalidInput, hash, err)
		}
		if err = c.relayInternal(msg, plan.cur, plan.next, destination); err != nil {
			return err
		}
		c.stats.InternalsImported++
		return nil
	}

	var result *tvm.TransactionExecutionResult
	var lane *accountLane
	var err error
	if plan.executes {
		if !plan.started {
			return fmt.Errorf("%w: inbound internal plan %x was not started", ErrInvalidInput, hash)
		}
		result, lane, err = c.commitSpeculated(plan)
	} else {
		result, lane, err = c.executePrepared(plan.prepared, 0)
	}
	if err != nil {
		return fmt.Errorf("execute inbound message %x: %w", hash, err)
	}
	if err = c.ctx.Err(); err != nil {
		return err
	}
	c.prewarmGeneratedOutputs(result)

	in, err := descriptorFee(0b100, 3, msg.EnvelopeCell, result.TransactionCell, env.FwdFeeRemaining) // msg_import_fin$100
	if err != nil {
		return err
	}
	// Both descriptor dictionaries key this message by the same hash, so the
	// key cell is built once for the pair.
	messageKey := descriptorKey(msg.Root)
	if c.shard.ContainsPrefix(plan.cur) {
		// The message originates from our own out-queue: dequeue it with a
		// msg_export_deq_imm$100 record referencing the import.
		out, err := descriptor(0b100, 3, msg.EnvelopeCell, in) // msg_export_deq_imm$100
		if err != nil {
			return err
		}
		if err = c.insertKeyed(c.outMessages.AugmentedDictionary, &c.outDescr, messageKey, out); err != nil {
			return err
		}
		if err = c.dequeueOwn(msg); err != nil {
			return err
		}
	}
	if err = c.insertKeyed(c.inMessages.AugmentedDictionary, &c.inDescr, messageKey, in); err != nil {
		return err
	}

	c.stats.InternalsImported++
	return c.registerOutputs(result, lane, env.Metadata, false)
}

// commitSpeculated takes a plan whose transaction already ran off the main
// goroutine and makes it part of the collation: the lane is registered if it
// is new, its detached reads are replayed into the shared recorders, and the
// result is committed. After this the collation is in the state a sequential
// executePrepared would have left it in.
//
// The tracer returns to pass-through only when this plan's successor was not
// chained: a chained successor is still buffering — or already holds its own
// detached segment — and dropping to pass-through under it would leak the
// reads the retire path itself makes into the shared record out of order.
// That successor is real only off the masterchain (see chainInternal), and off
// the masterchain the retire path parses no lane-traced cell, so nothing fires
// the tracer between this replay and the successor's own.
func (c *collation) commitSpeculated(plan *internalPlan) (*tvm.TransactionExecutionResult, *accountLane, error) {
	plan.wg.Wait()
	if plan.execErr != nil {
		return nil, nil, plan.execErr
	}
	lane := plan.lane
	if plan.fresh {
		c.registerLane(lane)
	}
	lane.tracer.replaySegment(plan.events)
	plan.releaseEvents()
	if next := plan.follows; next == nil || !next.chained {
		lane.tracer.discard()
	}
	if err := c.commitExecution(plan.result, lane, true); err != nil {
		return nil, nil, err
	}
	return plan.result, lane, nil
}

// releaseEvents empties the plan's replayed or discarded segment. The cells go
// now — a retained segment would keep predecessor subtrees alive for as long
// as the arena does — while the slice stays for its capacity.
func (p *internalPlan) releaseEvents() {
	clear(p.events)
	p.events = p.events[:0]
}

func (c *collation) relayInternal(
	msg *msgpool.InternalMessage,
	current msgpool.AccountPrefix,
	next msgpool.AccountPrefix,
	destination msgpool.AccountPrefix,
) error {
	if c.queueSize == maxOutMsgQueueSize {
		if err := c.flushQueueAdds(); err != nil {
			return err
		}
		return fmt.Errorf("%w: outbound queue size overflow", ErrInvalidInput)
	}
	curBits, nextBits, err := performHypercubeRouting(next, destination, c.shard, 0)
	if err != nil {
		return fmt.Errorf("route transit message: %w", err)
	}

	remainingNano := msg.Envelope.FwdFeeRemaining.Nano()
	transitNano := standardTransitFee(c.config, remainingNano)
	remainingNano.Sub(remainingNano, transitNano)
	transitFee := tlb.FromNanoTON(transitNano)

	envelope := tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: uint8(curBits)},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: uint8(nextBits)},
		FwdFeeRemaining: tlb.FromNanoTON(remainingNano),
		Msg:             msg.Root,
		EmittedLT:       msg.Envelope.EmittedLT,
		Metadata:        msg.Envelope.Metadata,
		V2:              msg.Envelope.V2,
	}
	envelopeCell, err := envelope.ToCell()
	if err != nil {
		return fmt.Errorf("serialize transit envelope: %w", err)
	}
	in, err := descriptorFee(0b101, 3, msg.EnvelopeCell, envelopeCell, transitFee) // msg_import_tr$101
	if err != nil {
		return err
	}
	requeue := c.shard.ContainsPrefix(current)
	outTag := uint64(0b011)
	if requeue {
		outTag = 0b111
	}
	out, err := descriptor(outTag, 3, envelopeCell, in) // msg_export_tr$011 / msg_export_tr_req$111
	if err != nil {
		return err
	}
	if err = c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, msg.Root, out); err != nil {
		return err
	}
	if err = c.insert(c.inMessages.AugmentedDictionary, &c.inDescr, msg.Root, in); err != nil {
		return err
	}

	nextHop := msgpool.InterpolatePrefix(next, destination, nextBits)
	key := msgpool.MakeQueueKey(nextHop, msg.Root.HashKey())
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: c.header.StartLt, Msg: envelopeCell}).ToCell()
	if err != nil {
		return err
	}
	if err = c.deferQueueAdd(key, enqueued, queuePendingAddTransit); err != nil {
		return err
	}
	c.queueSize++
	c.stats.EnqueuedMessages++
	if err = c.registerQueueOp(); err != nil {
		return err
	}
	if requeue {
		if err = c.dequeueOwn(msg); err != nil {
			return err
		}
	}

	return nil
}

// standardTransitFee is the forwarding fee charged for relaying a message
// onwards. Transit relaying uses config 25 even inside a masterchain block;
// config 24 applies to messages newly created by masterchain transactions.
func standardTransitFee(config *Config, remaining *big.Int) *big.Int {
	// remaining is read-only here: Mul writes to the fresh receiver only.
	fee := new(big.Int).Mul(
		remaining,
		new(big.Int).SetUint64(uint64(config.basechain.fwdPrices.NextFrac)),
	)
	return fee.Rsh(fee, 16)
}

// dequeueOwn removes a re-imported message from the collation out-queue,
// verifying that the queued envelope is the exact imported cell.
func (c *collation) dequeueOwn(msg *msgpool.InternalMessage) error {
	if err := c.flushQueueAdds(); err != nil {
		return err
	}
	if c.queueDeletePending(msg.Key) {
		return fmt.Errorf("%w: own-queue entry %x is absent", ErrInvalidInput, msg.Key)
	}
	var value cell.Slice
	err := c.outQueue.LoadValueByBytesKeyInto(msg.Key[:], &value)
	if isMissingKey(err) {
		return fmt.Errorf("%w: own-queue entry %x is absent", ErrInvalidInput, msg.Key)
	}
	if err != nil {
		return fmt.Errorf("dequeue own message %x: %w", msg.Key, err)
	}
	var enqueued tlb.EnqueuedMsg
	if err = loadExactSlice(&enqueued, &value); err != nil {
		return fmt.Errorf("%w: decode own-queue entry %x: %v", ErrInvalidInput, msg.Key, err)
	}
	if enqueued.Msg.HashKey() != msg.EnvelopeCell.HashKey() {
		return fmt.Errorf("%w: own-queue entry %x envelope differs from the imported one", ErrInvalidInput, msg.Key)
	}
	if c.queueSize == 0 {
		return fmt.Errorf("%w: outbound queue size underflow", ErrInvalidInput)
	}
	if err = c.deferQueueDelete(msg.Key); err != nil {
		return err
	}
	c.queueSize--
	return c.registerQueueOp()
}

// advanceProcessedBound enforces the strict (lt, hash) processing order of
// inbound internal messages. source names the feed -- "imported" for a queue
// entry, "generated" for a message this block delivers in-block -- because the
// two carry different lt provenance and the field log otherwise cannot tell
// which one raised the error.
//
// EXACTLY TWO CALL SITES, and that is parity, not an omission. The reference has
// two calls to update_last_proc_int_msg and no more:
//
//	collator.cpp:3956  the inbound queue import     <-> imports.go importInternal
//	collator.cpp:3668  process_one_new_message      <-> execute.go retireGeneratedImmediate
//
// The second is reached only AFTER the `if (enqueue || defer) { ... return; }`
// exit at collator.cpp:3649-3663 and is additionally gated on `!is_special`. So
// the reference does NOT advance the bound when a generated message is ENQUEUED
// rather than delivered, and neither do we: enqueue() writes
// tlb.EnqueuedMsg{EnqueuedLT: item.lt} with no bound advance, matching
// enqueue_message's own `LogicalTime enqueued_lt = msg.lt;`
// (collator.cpp:4743, stored at :4794-4795) which likewise never touches
// last_proc_int_msg_. The defer branch takes the same early exit on both sides,
// and specials are routed around retireGeneratedImmediate on purpose (master.go), which
// is the `!is_special` gate.
//
// The reason it cannot diverge is structural rather than incidental: the bound
// orders messages this block PROCESSES, and an enqueued message is not processed
// by this block at all — it runs no transaction and is assigned no transaction
// lt. Advancing the bound on an enqueue would therefore be a divergence, not a
// fix: it would raise the floor for later transactions on the strength of work
// this block never did. TestProcessedBoundHasExactlyTwoCallSites pins the count,
// because the two-call-site structure IS the invariant and nothing else in the
// package would notice a third.
func (c *collation) advanceProcessedBound(source string, lt uint64, hash [32]byte) error {
	if lt == 0 {
		return fmt.Errorf("%w: processed internal message has zero lt", ErrInvalidInput)
	}
	if c.lastProcLT > lt || (c.lastProcLT == lt && bytes.Compare(c.lastProcHash[:], hash[:]) >= 0) {
		// The detail is parity with collator.cpp:3500-3501, which logs both
		// pairs at ERROR before failing with this message.
		return fmt.Errorf("%w: internal message processing order violated: %s message (%d, %x) after message (%d, %x)",
			ErrInvalidInput, source, lt, hash, c.lastProcLT, c.lastProcHash)
	}
	c.lastProcLT = lt
	c.lastProcHash = hash
	return nil
}

// parentProcessedRecords caches the parsed parent ProcessedInfo entries.
func (c *collation) parentProcessedRecords() ([]tlb.ProcessedUptoRecord, error) {
	if !c.processedParsed {
		records, err := tlb.LoadProcessedUptoRecords(c.processed, c.shard.Shard)
		if err != nil {
			return nil, fmt.Errorf("%w: decode processed info: %v", ErrInvalidInput, err)
		}
		c.processedRecords = records
		c.processedParsed = true
	}
	return c.processedRecords, nil
}

// processedInfinityHash is the all-ones bound hash of an infinity record: the
// bound that covers every message below its lt.
var processedInfinityHash = func() (hash [32]byte) {
	for i := range hash {
		hash[i] = 0xff
	}
	return hash
}()

// updateProcessedInfo records the processed bound into ProcessedInfo: insert a
// record for our shard at
// the reference masterchain seqno — the (lt, hash) bound over every inbound
// message the import phase enumerated (imported and skipped-as-covered
// alike), or the infinity bound when nothing was
// enumerated and the inbound queues were drained completely — then
// compactify. The minimum
// referenced masterchain seqno is refreshed from the surviving records
// after the update, since compaction can drop the parent records the
// prepare-time
// minimum came from. Without an insert the parent dictionary passes through
// byte-identical.
func (c *collation) updateProcessedInfo() error {
	boundLT, boundHash := c.lastProcLT, c.lastProcHash
	if boundLT == 0 {
		// The inbound-queues-empty analog: the pool cut was complete and
		// empty. The infinity bound sits just below the reference masterchain
		// state lt.
		if len(c.req.internalMessages()) != 0 || c.req.internalsIncomplete() {
			return nil
		}
		_, referenceEndLT := c.processedReference()
		if referenceEndLT == 0 {
			// A zerostate reference has no lt to anchor the infinity bound —
			// subtracting one would underflow — so the drained record is simply
			// not written for the first block.
			return nil
		}
		boundLT = referenceEndLT - 1
		boundHash = processedInfinityHash
	}
	records, err := c.parentProcessedRecords()
	if err != nil {
		return err
	}
	referenceSeqno, _ := c.processedReference()
	records, err = tlb.InsertProcessedUpto(records, c.shard.Shard, referenceSeqno, boundLT, boundHash)
	if err != nil {
		return fmt.Errorf("%w: update processed info: %v", ErrInvalidInput, err)
	}
	records = tlb.CompactifyProcessedUpto(records)
	c.processedMinMC = minRecordedMCSeqno(records)
	dict, err := tlb.ProcessedUptoDict(records)
	if err != nil {
		return fmt.Errorf("serialize processed info: %w", err)
	}
	// The bound is retained for traceProcessedQueueValidationClosure, which has
	// to replay the validator's inbound-queue scan against the very bound this
	// block claims. It is recorded only when the dictionary actually moved,
	// because that is the validator's own gate: verifyProcessedInfo compares the
	// two ProcessedInfo dictionaries and returns before the source loop when
	// they are equal, so a bound that compaction absorbed into an existing
	// record starts no scan and needs no proof.
	if !equalDictionary(c.processed, dict) {
		c.processedClaim = semanticMessageBound{lt: boundLT, hash: boundHash}
		c.processedClaimed = true
	}
	c.processed = dict
	return nil
}

func (c *collation) processedReference() (uint32, uint64) {
	if c.master != nil {
		return c.header.SeqNo, c.oldState.GenLT
	}
	return c.header.MasterRef.SeqNo, c.header.MasterRef.EndLt
}
