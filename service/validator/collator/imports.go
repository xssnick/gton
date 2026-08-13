package collator

import (
	"bytes"
	"fmt"
	"math/big"

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

// processInternals imports the inbound internal message slice: messages are
// consumed in
// (lt, hash) order until the normal limit class is exhausted; the remainder
// stays unimported and forces generated messages into the queue.
func (c *collation) processInternals() error {
	if c.haveUnprocessedDispatchQueue {
		return nil
	}
	inputs := c.req.internalMessages()
	if len(inputs) == 0 {
		return nil
	}
	records, err := c.parentProcessedRecords()
	if err != nil {
		return err
	}

	for _, msg := range inputs {
		c.updateCollatedEstimate()
		if !c.limits.fits(LoadNormal) {
			c.blockFull = true
			return nil
		}
		if err = c.ctx.Err(); err != nil {
			return err
		}
		if err = c.importInternal(msg, records); err != nil {
			return err
		}
		c.updatePeakLoad()
	}
	return nil
}

// importInternal handles one queued message: validate the envelope against its queue key, skip if the
// parent ProcessedUpto already covers it, execute the destination
// transaction and record the msg_import_fin descriptor — plus the
// msg_export_deq_imm dequeue when the message comes from our own out-queue.
func (c *collation) importInternal(msg *msgpool.InternalMessage, records []tlb.ProcessedUptoRecord) error {
	env := &msg.Envelope
	hash := msg.Root.HashKey()
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

	// The bound advances BEFORE the covered-check: messages skipped as already
	// covered still participate in the ProcessedUpto record, and a validator
	// replaying the block has to arrive at the same bound. The queue-entry lt
	// feeds the shard-end-lt branch of the coverage check; the canonical lt
	// bounds the processing order.
	if err = c.advanceProcessedBound(msg.EnqueuedLT, hash); err != nil {
		return err
	}
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
	if processed {
		c.stats.InternalsSkipped++
		return nil
	}
	if !c.shard.ContainsPrefix(destination) {
		if err = c.relayInternal(msg, cur, next, destination); err != nil {
			return err
		}
		c.stats.InternalsImported++
		return nil
	}

	result, lane, err := c.executePrepared(prepared, 0)
	if err != nil {
		return fmt.Errorf("execute inbound message %x: %w", hash, err)
	}
	if err = c.ctx.Err(); err != nil {
		return err
	}

	in, err := descriptorFee(0b100, 3, msg.EnvelopeCell, result.TransactionCell, env.FwdFeeRemaining) // msg_import_fin$100
	if err != nil {
		return err
	}
	// Both descriptor dictionaries key this message by the same hash, so the
	// key cell is built once for the pair.
	messageKey := descriptorKey(msg.Root)
	if c.shard.ContainsPrefix(cur) {
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

func (c *collation) relayInternal(
	msg *msgpool.InternalMessage,
	current msgpool.AccountPrefix,
	next msgpool.AccountPrefix,
	destination msgpool.AccountPrefix,
) error {
	if c.queueSize == maxOutMsgQueueSize {
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
	if c.queueDeletePending(key) {
		if err = c.flushQueueDeletes(); err != nil {
			return err
		}
	}
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: c.header.StartLt, Msg: envelopeCell}).ToCell()
	if err != nil {
		return err
	}
	inserted, err := c.outQueue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
	if err != nil {
		return fmt.Errorf("enqueue transit message: %w", err)
	}
	if !inserted {
		return fmt.Errorf("%w: duplicate transit queue key %x", ErrInvalidInput, key)
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
	if c.queueDeletePending(msg.Key) {
		return fmt.Errorf("%w: own-queue entry %x is absent", ErrInvalidInput, msg.Key)
	}
	keyCell := cell.BeginCell().MustStoreSlice(msg.Key[:], 352).EndCell()
	value, err := c.outQueue.LoadValue(keyCell)
	if isMissingKey(err) {
		return fmt.Errorf("%w: own-queue entry %x is absent", ErrInvalidInput, msg.Key)
	}
	if err != nil {
		return fmt.Errorf("dequeue own message %x: %w", msg.Key, err)
	}
	var enqueued tlb.EnqueuedMsg
	if err = loadExactSlice(&enqueued, value); err != nil {
		return fmt.Errorf("%w: decode own-queue entry %x: %v", ErrInvalidInput, msg.Key, err)
	}
	if enqueued.Msg.HashKey() != msg.EnvelopeCell.HashKey() {
		return fmt.Errorf("%w: own-queue entry %x envelope differs from the imported one", ErrInvalidInput, msg.Key)
	}
	if c.queueSize == 0 {
		return fmt.Errorf("%w: outbound queue size underflow", ErrInvalidInput)
	}
	c.deferQueueDelete(msg.Key, keyCell)
	c.queueSize--
	return c.registerQueueOp()
}

// advanceProcessedBound enforces the strict (lt, hash) processing order of
// inbound internal messages.
func (c *collation) advanceProcessedBound(lt uint64, hash [32]byte) error {
	if lt == 0 {
		return fmt.Errorf("%w: processed internal message has zero lt", ErrInvalidInput)
	}
	if c.lastProcLT > lt || (c.lastProcLT == lt && bytes.Compare(c.lastProcHash[:], hash[:]) >= 0) {
		return fmt.Errorf("%w: internal message processing order violated", ErrInvalidInput)
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
	c.processed = dict
	return nil
}

func (c *collation) processedReference() (uint32, uint64) {
	if c.master != nil {
		return c.header.SeqNo, c.oldState.GenLT
	}
	return c.header.MasterRef.SeqNo, c.header.MasterRef.EndLt
}
