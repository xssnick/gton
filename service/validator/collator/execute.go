package collator

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type accountLane struct {
	address            *address.Address
	original           *tlb.ShardAccount
	current            *tvm.PreparedAccount
	storageStat        *cell.Cell
	initialStorageStat *cell.Cell
	// storageProof traces every read of the account's storage-stat dictionary
	// for the whole collation; the proof the block carries is created from it at
	// the end, once all of the account's transactions have run.
	storageProof *cell.MerkleProofBuilder
	// initialStorageProof is that proof as it stood after the first transaction.
	// It only seeds the collated size estimate — the emitted proof is a superset
	// of it, see trackAccountStorageProof.
	initialStorageProof *cell.Cell
	transactions        *tlb.AccountTransactionsAugDict
	key                 [32]byte
	originallyExists    bool
	// written records that the account has already been put into the accounts
	// dictionary once, ahead of finishAccounts, to feed the size estimate.
	// writtenHash is what was put there, so the final pass can skip an account
	// whose state never moved again.
	written     bool
	writtenHash cell.Hash
	// writtenState is the account this lane held when the early write rendered
	// it. current only ever moves in commitExecution, so an unmoved pointer is
	// proof that re-rendering the entry would reproduce the cell the early
	// write already put in the dictionary — which finishAccounts would then
	// throw away. Comparing the pointer is free; rendering it again costs a
	// ShardAccount serialization and its hashes, once per touched account.
	writtenState *tvm.PreparedAccount
	// keyCell is the account's 256-bit dictionary key. Both dictionaries the
	// account lands in take the same key and cells are immutable, so it is
	// built once instead of once per use.
	keyCell *cell.Cell
}

// laneKey returns the account's dictionary key cell, building it on first use.
func (l *accountLane) laneKey() *cell.Cell {
	if l.keyCell == nil {
		l.keyCell = cell.BeginCell().MustStoreSlice(l.key[:], 256).EndCell()
	}
	return l.keyCell
}

type newMessage struct {
	lt               uint64
	hash             [32]byte
	root             *cell.Cell
	parsed           *tlb.Message
	transaction      *cell.Cell
	metadata         *tlb.MsgMetadata
	index            uint32
	dispatchEnvelope *cell.Cell
}

// newMessageHeap orders generated messages by (lt, hash) — the canonical
// processing order. Immediate delivery grows the set while it is drained, so
// a heap replaces a sort-once slice; push and popMin avoid the interface
// boxing of heap.Push/heap.Pop.
type newMessageHeap []newMessage

func (h newMessageHeap) Len() int { return len(h) }
func (h newMessageHeap) Less(i, j int) bool {
	if h[i].lt != h[j].lt {
		return h[i].lt < h[j].lt
	}
	return bytes.Compare(h[i].hash[:], h[j].hash[:]) < 0
}
func (h newMessageHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *newMessageHeap) Push(x any)   { *h = append(*h, x.(newMessage)) }
func (h *newMessageHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = newMessage{}
	*h = old[:last]
	return item
}

func (h *newMessageHeap) push(item newMessage) {
	*h = append(*h, item)
	heap.Fix(h, len(*h)-1)
}

func (h *newMessageHeap) popMin() newMessage {
	top := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	(*h)[last] = newMessage{}
	*h = (*h)[:last]
	if last > 0 {
		heap.Fix(h, 0)
	}
	return top
}

func (c *collation) recordExternal(ref msgpool.ExternalRef, outcome msgpool.ExternalOutcome) {
	c.externals = append(c.externals, msgpool.ExternalFeedback{Ref: ref, Outcome: outcome})
	switch outcome {
	case msgpool.ExternalIncluded:
		c.stats.ExternalIncluded++
	case msgpool.ExternalInvalid:
		c.stats.ExternalInvalid++
	case msgpool.ExternalNotAccepted:
		c.stats.ExternalNotAccepted++
	case msgpool.ExternalSkippedLimit:
		c.stats.ExternalSkippedLimit++
	}
}

func (c *collation) processExternals() error {
	_, err := c.processExternalBatch(c.req.externals, time.Time{})
	return err
}

type externalBatchResult struct {
	stop     ExternalStopReason
	consumed int
}

func (c *collation) processExternalBatch(
	externals []ExternalInput,
	deadline time.Time,
) (externalBatchResult, error) {
	for i, external := range externals {
		c.updateCollatedEstimate()
		if !c.limits.fits(LoadSoft) {
			for _, skipped := range externals[i:] {
				c.recordExternal(skipped.Ref, msgpool.ExternalSkippedLimit)
			}
			return externalBatchResult{stop: ExternalStopSoftLimit, consumed: i}, nil
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			for _, skipped := range externals[i:] {
				c.recordExternal(skipped.Ref, msgpool.ExternalSkippedLimit)
			}
			return externalBatchResult{stop: ExternalStopDeadline, consumed: i}, nil
		}
		if err := c.ctx.Err(); err != nil {
			return externalBatchResult{}, err
		}

		prepared := external.message
		root := prepared.Cell()
		message := prepared.Message()
		hash := root.HashKey()
		if root.Level() != 0 {
			c.recordExternal(external.Ref, msgpool.ExternalInvalid)
			continue
		}
		if message.MsgType != tlb.MsgTypeExternalIn {
			c.recordExternal(external.Ref, msgpool.ExternalInvalid)
			continue
		}
		destination, err := accountPrefixFromAddress(message.AsExternalIn().DstAddr)
		if err != nil {
			c.recordExternal(external.Ref, msgpool.ExternalInvalid)
			continue
		}
		if !c.shard.ContainsPrefix(destination) {
			c.recordExternal(external.Ref, msgpool.ExternalInvalid)
			continue
		}
		// Checked after the message is known to be well-formed and ours, so a
		// message that would never be executable is still purged from the pool
		// rather than held behind the brake, and before the attempt counter,
		// which measures executions this message does not get.
		if c.queueSize > skipExternalsQueueSize {
			c.recordExternal(external.Ref, msgpool.ExternalSkippedLimit)
			continue
		}
		if int(c.stats.ExternalAttempts) >= c.req.maxExternalAttempts {
			for _, skipped := range externals[i:] {
				c.recordExternal(skipped.Ref, msgpool.ExternalSkippedLimit)
			}
			return externalBatchResult{stop: ExternalStopAttemptLimit, consumed: i}, nil
		}
		c.stats.ExternalAttempts++
		result, lane, err := c.executePrepared(prepared, 0)
		if err != nil {
			return externalBatchResult{}, fmt.Errorf("execute external message %x: %w", hash, err)
		}
		if err = c.ctx.Err(); err != nil {
			return externalBatchResult{}, err
		}
		if !result.Accepted {
			c.recordExternal(external.Ref, msgpool.ExternalNotAccepted)
			continue
		}
		in, err := descriptor(0b000, 3, root, result.TransactionCell) // msg_import_ext$000
		if err != nil {
			return externalBatchResult{}, err
		}
		if err = c.insert(c.inMessages.AugmentedDictionary, &c.inDescr, root, in); err != nil {
			return externalBatchResult{}, err
		}

		c.recordExternal(external.Ref, msgpool.ExternalIncluded)
		if err = c.registerOutputs(result, lane, nil, true); err != nil {
			return externalBatchResult{}, err
		}
		c.updatePeakLoad()
	}
	return externalBatchResult{consumed: len(externals)}, nil
}

// processNewMessages drains the generated message heap in strict (lt, hash)
// order: messages destined inside the shard are executed in the same block
// until the normal limit class fills up; everything else — and the remainder
// after the cutoff — is enqueued.
func (c *collation) processNewMessages(enqueueOnly bool) error {
	for c.new.Len() > 0 {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		// Parity with collator.cpp:4847-4854, which re-reads the normal limit
		// class and the soft timeout at the TOP of every heap item — before
		// extra_out_msgs-- and before the enqueue_only latch, which is why both
		// checks sit above popMin here. Reading only the c.blockFull FIELD (the
		// pre-fix behaviour) misses every way the estimate grows without an
		// immediate delivery: enqueue() charges cells through insert() and
		// re-proofs the queue root every 64 ops, so a drain could cross the
		// normal class and keep executing in-block messages the reference would
		// have enqueued. deliverImmediate's own post-delivery check
		// (:377-379, the port of collator.cpp:3727-3731) covers only the
		// delivery path.
		c.updateCollatedEstimate()
		if !c.limits.fits(LoadNormal) {
			c.blockFull = true
		}
		if !c.blockFull && c.internalMsgExpired() {
			c.blockFull = true
			c.stats.InternalMsgTimeouts++
		}
		item := c.new.popMin()
		c.limits.extraOutMsgs--
		if c.blockFull || c.haveUnprocessedDispatchQueue {
			enqueueOnly = true
		}

		switch item.parsed.MsgType {
		case tlb.MsgTypeExternalOut:
			out, err := descriptor(0b000, 3, item.root, item.transaction) // msg_export_ext$000
			if err != nil {
				return err
			}
			if err = c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, item.root, out); err != nil {
				return err
			}
			continue
		case tlb.MsgTypeInternal:
		default:
			return fmt.Errorf("%w: transaction emitted an inbound external message", ErrInvalidInput)
		}

		internal := item.parsed.AsInternal()
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

		if item.dispatchEnvelope != nil {
			pending := c.unprocessedDeferred[sourceID]
			if pending == 0 {
				return fmt.Errorf("%w: dispatch message %x has no pending source entry", ErrInvalidInput, item.hash)
			}
			if pending == 1 {
				delete(c.unprocessedDeferred, sourceID)
			} else {
				c.unprocessedDeferred[sourceID] = pending - 1
			}
		} else {
			deferMessage, deferErr := c.shouldDeferGenerated(&item, source, sourceID)
			if deferErr != nil {
				return deferErr
			}
			if deferMessage {
				if err = c.deferGenerated(&item, sourceID); err != nil {
					return err
				}
				continue
			}
		}

		if enqueueOnly || !c.shard.ContainsPrefix(destination) {
			if err = c.enqueue(&item, source, destination); err != nil {
				return err
			}
			continue
		}
		if err = c.deliverImmediate(&item, internal, destination); err != nil {
			return err
		}
	}
	return nil
}

// deliverImmediate executes a generated in-shard message in the same block:
// the message is wrapped into a use_dest_bits=96 envelope carrying the full
// forward fee, the consuming transaction runs after the creating one, and the
// descriptors form the msg_import_imm/msg_export_imm pair.
func (c *collation) deliverImmediate(
	item *newMessage,
	internal *tlb.InternalMessage,
	destination msgpool.AccountPrefix,
) error {
	if err := c.advanceProcessedBound("generated", item.lt, item.hash); err != nil {
		return err
	}
	prepared, err := tvm.PrepareMessage(item.root)
	if err != nil {
		return fmt.Errorf("%w: generated message %x: %v", ErrInvalidInput, item.hash, err)
	}
	result, lane, err := c.executePrepared(prepared, item.lt)
	if err != nil {
		return fmt.Errorf("execute generated message %x: %w", item.hash, err)
	}
	if err = c.ctx.Err(); err != nil {
		return err
	}

	var envelopeCell, in *cell.Cell
	if item.dispatchEnvelope != nil {
		var envelope tlb.MsgEnvelope
		if err = parseExact(&envelope, item.dispatchEnvelope); err != nil {
			return fmt.Errorf("%w: decode emitted dispatch envelope %x: %v", ErrInvalidInput, item.hash, err)
		}
		envelopeCell = item.dispatchEnvelope
		in, err = descriptorFee(0b00100, 5, envelopeCell, result.TransactionCell, envelope.FwdFeeRemaining) // msg_import_deferred_fin$00100
	} else {
		envelopeCell, err = (tlb.MsgEnvelope{
			CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
			NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
			FwdFeeRemaining: internal.FwdFee,
			Msg:             item.root,
			Metadata:        item.metadata,
		}).ToCell()
		if err == nil {
			in, err = descriptorFee(0b011, 3, envelopeCell, result.TransactionCell, internal.FwdFee) // msg_import_imm$011
		}
	}
	if err != nil {
		return err
	}
	if err = c.insert(c.inMessages.AugmentedDictionary, &c.inDescr, item.root, in); err != nil {
		return err
	}
	if item.dispatchEnvelope == nil {
		out, outErr := descriptor(0b010, 3, envelopeCell, item.transaction, in) // msg_export_imm$010
		if outErr != nil {
			return outErr
		}
		if err = c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, item.root, out); err != nil {
			return err
		}
		if c.fullCollated {
			// The envelope routes straight to the destination, so this is the
			// key the validator will prove absent. Recorded rather than looked
			// up here: a lookup taken now would land in the state update.
			c.immediateQueueKeys = append(c.immediateQueueKeys, msgpool.MakeQueueKey(destination, item.hash))
		}
	}

	if err = c.registerOutputs(result, lane, item.metadata, false); err != nil {
		return err
	}
	c.stats.ImmediateDelivered++
	c.updatePeakLoad()
	if !c.limits.fits(LoadNormal) {
		c.blockFull = true
	} else if c.internalMsgExpired() {
		// collator.cpp:3732-3736, the second half of process_one_new_message's
		// "check whether the block is full now" step. Both arms return 3 there,
		// which latches enqueue_only for the next heap item; setting c.blockFull
		// is how the loop-top latch above reaches the same state.
		c.blockFull = true
		c.stats.InternalMsgTimeouts++
	}
	return nil
}

func (c *collation) enqueue(item *newMessage, source, destination msgpool.AccountPrefix) error {
	if c.queueSize == maxOutMsgQueueSize {
		return fmt.Errorf("%w: outbound queue size overflow", ErrInvalidInput)
	}

	curBits, nextBits, err := performHypercubeRouting(source, destination, c.shard, 0)
	if err != nil {
		return fmt.Errorf("route generated message: %w", err)
	}
	// A dispatched message keeps the envelope it was deferred with — fee,
	// metadata and emitted lt alike — and only its routing addresses are
	// recomputed, so the two shapes share nothing but those two fields.
	var envelope tlb.MsgEnvelope
	if item.dispatchEnvelope != nil {
		if err = parseExact(&envelope, item.dispatchEnvelope); err != nil {
			return fmt.Errorf("%w: decode emitted dispatch envelope %x: %v", ErrInvalidInput, item.hash, err)
		}
		if envelope.EmittedLT == nil || *envelope.EmittedLT != item.lt {
			return fmt.Errorf("%w: dispatch envelope %x emitted lt mismatch", ErrInvalidInput, item.hash)
		}
		envelope.CurAddr = tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: uint8(curBits)}
		envelope.NextAddr = tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: uint8(nextBits)}
	} else {
		envelope = tlb.MsgEnvelope{
			CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: uint8(curBits)},
			NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: uint8(nextBits)},
			FwdFeeRemaining: item.parsed.AsInternal().FwdFee,
			Msg:             item.root,
			Metadata:        item.metadata,
		}
	}
	envelopeCell, err := envelope.ToCell()
	if err != nil {
		return err
	}
	if item.dispatchEnvelope != nil {
		// msg_import_deferred_tr$00101: unlike a regular transit import, this
		// constructor has no transit-fee tail.
		in, inErr := descriptor(0b00101, 5, item.dispatchEnvelope, envelopeCell)
		if inErr != nil {
			return inErr
		}
		out, outErr := descriptor(0b10101, 5, envelopeCell, in) // msg_export_deferred_tr$10101
		if outErr != nil {
			return outErr
		}
		if err = c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, item.root, out); err != nil {
			return err
		}
		if err = c.insert(c.inMessages.AugmentedDictionary, &c.inDescr, item.root, in); err != nil {
			return err
		}
	} else {
		out, outErr := descriptor(0b001, 3, envelopeCell, item.transaction) // msg_export_new$001
		if outErr != nil {
			return outErr
		}
		if err = c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, item.root, out); err != nil {
			return err
		}
	}

	nextHop := msgpool.InterpolatePrefix(source, destination, nextBits)
	key := msgpool.MakeQueueKey(nextHop, item.hash)
	if c.queueDeletePending(key) {
		// The sequential order deleted this key before re-adding it; apply the
		// pending removals so Add mode sees the same dictionary it always did.
		if err = c.flushQueueDeletes(); err != nil {
			return err
		}
	}
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: item.lt, Msg: envelopeCell}).ToCell()
	if err != nil {
		return err
	}
	inserted, err := c.outQueue.SetWithMode(keyCell, enqueued, cell.DictSetModeAdd)
	if err != nil {
		return fmt.Errorf("enqueue generated message: %w", err)
	}
	if !inserted {
		return fmt.Errorf("%w: duplicate outbound queue key %x", ErrInvalidInput, key)
	}
	c.queueSize++
	c.stats.EnqueuedMessages++
	return c.registerQueueOp()
}

func (c *collation) executePrepared(message *tvm.PreparedMessage, afterLT uint64) (*tvm.TransactionExecutionResult, *accountLane, error) {
	destination := message.Message().Msg.DestAddr()
	lane, err := c.account(destination)
	if err != nil {
		return nil, nil, err
	}
	// Parity with collator.cpp:3317-3319, whose own comment at :3398-3399 states
	// the rule: "transactions processing external messages must have lt larger
	// than all processed internal messages". Without this floor an external
	// message always restarts at StartLt+1, so once an immediate delivery or a
	// queue import has moved the processed bound above StartLt+1, the messages
	// this transaction generates carry a created_lt BENEATH the bound and
	// advanceProcessedBound -- the port of collator.cpp:3493-3505 -- rejects
	// them. The reference gates the clause on `external`: an imported internal
	// message is deliberately NOT floored.
	if message.Message().MsgType == tlb.MsgTypeExternalIn {
		afterLT = max(afterLT, c.lastProcLT)
	}
	if emittedLT, ok := c.lastDispatchEmitted[lane.key]; ok {
		afterLT = max(afterLT, emittedLT)
	}

	minLT := max(c.header.StartLt, afterLT)
	if minLT >= math.MaxInt64 {
		return nil, nil, fmt.Errorf("%w: transaction lt overflow", ErrInvalidInput)
	}
	result, err := c.builder.machine.EmulateTransaction(c.blockCtx, lane.current, message, tvm.TransactionOptions{
		LogicalTime:        int64(minLT + 1),
		AccountStorageStat: lane.storageStat,
		OnCellLoad:         c.recordExecutionRead,
	})
	if err != nil {
		return nil, nil, err
	}
	if err = c.ctx.Err(); err != nil {
		return nil, nil, err
	}
	if message.Message().MsgType == tlb.MsgTypeExternalIn && !result.Accepted {
		return result, lane, nil
	}
	if err = c.commitExecution(result, lane, true); err != nil {
		return nil, nil, err
	}

	return result, lane, nil
}

func (c *collation) commitExecution(
	result *tvm.TransactionExecutionResult,
	lane *accountLane,
	includeGas bool,
) error {
	if result.GasUsed < 0 {
		return fmt.Errorf("%w: negative gas usage", ErrInvalidInput)
	}
	var err error

	firstForAccount := lane.transactions == nil
	if firstForAccount {
		lane.transactions, err = tlb.NewAccountTransactionsAugDict()
		if err != nil {
			return err
		}
		if err = c.trackAccountStorageProof(lane); err != nil {
			return err
		}
	}
	txKey := cell.BeginCell().MustStoreUInt(result.StartLT, 64).EndCell()
	txValue := cell.BeginCell().MustStoreRef(result.TransactionCell).EndCell()
	inserted, err := lane.transactions.SetWithMode(txKey, txValue, cell.DictSetModeAdd)
	if err != nil {
		return err
	}
	if !inserted {
		return fmt.Errorf("%w: duplicate transaction lt %d", ErrInvalidInput, result.StartLT)
	}

	if c.master != nil {
		// Masterchain public-library visibility changes are weighed into the
		// block size estimate. lane.current still holds the pre-transaction
		// state at this point.
		diff, diffErr := publicLibraryDiffCount(lane.current.State(), result.NextAccount.State())
		if diffErr != nil {
			return fmt.Errorf("count public library diff of %x: %w", lane.key, diffErr)
		}
		c.limits.publicLibraryDiff += diff
	}

	lane.current = result.NextAccount
	lane.storageStat = result.AccountStorageStat
	c.maxLT = max(c.maxLT, result.EndLT)
	gas := uint64(0)
	if includeGas {
		gas = uint64(result.GasUsed)
	}
	if err = c.limits.addTransaction(result.NextAccount.ShardAccount().Account, result.TransactionCell, result.EndLT, gas); err != nil {
		return err
	}
	if err = c.updateAccountEstimate(lane); err != nil {
		return err
	}
	if c.limits.transactions%accountEstimateSampleInterval == 0 {
		if err = c.flushAccountEstimate(); err != nil {
			return err
		}
		if err = c.limits.addProof(c.accounts.RootCell()); err != nil {
			return err
		}
	}

	if !currencyZero(result.Burned) {
		c.burned, err = c.burned.Add(result.Burned)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *collation) account(addr *address.Address) (*accountLane, error) {
	key, err := accountIDFromAddress(addr)
	if err != nil {
		return nil, err
	}
	if lane := c.lanes[key]; lane != nil {
		return lane, nil
	}

	keyCell := cell.BeginCell().MustStoreSlice(key[:], 256).EndCell()
	value, err := c.loadPredecessorAccount(key, keyCell)
	originallyExists := true
	var shardAccount tlb.ShardAccount
	if isMissingKey(err) {
		originallyExists = false
		shardAccount = tlb.ShardAccount{
			Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
			LastTransHash: make([]byte, 32),
		}
	} else if err != nil {
		return nil, err
	} else if err = loadExactSlice(&shardAccount, value); err != nil {
		return nil, fmt.Errorf("decode shard account %x: %w", key, err)
	}

	effectiveAddr := address.NewAddress(0, byte(addr.Workchain()), key[:])
	prepared, err := tvm.PrepareAccount(&shardAccount, effectiveAddr)
	if err != nil {
		return nil, err
	}
	if originallyExists && prepared.State().LastTransactionLT >= c.header.StartLt {
		return nil, fmt.Errorf("%w: account %x has a transaction after block start", ErrInvalidInput, key)
	}

	var storageStat *cell.Cell
	if extra, ok := prepared.State().StorageInfo.StorageExtra.(tlb.StorageExtraInfo); ok {
		var hash cell.Hash
		copy(hash[:], extra.DictHash)
		storageStat = c.req.storageStats[hash]
		if storageStat == nil && c.fullCollated {
			storageStat, err = c.blockCtx.ComputeAccountStorageStat(prepared)
			if err != nil {
				return nil, fmt.Errorf("compute account storage stat %x: %w", key, err)
			}
			if storageStat.HashKey() != hash {
				return nil, fmt.Errorf("%w: computed account storage stat %x differs from declared hash %x", ErrInvalidInput, storageStat.HashKey(), hash)
			}
		}
	}
	if storageStat != nil {
		if err = c.blockCtx.BindAccountStorageStat(prepared, storageStat); err != nil {
			return nil, fmt.Errorf("bind account storage stat %x: %w", key, err)
		}
	}

	initialStorageStat := storageStat
	var storageProof *cell.MerkleProofBuilder
	if storageStat != nil && c.fullCollated {
		storageProof = cell.NewMerkleProofBuilder(storageStat)
		storageStat = storageProof.Root()
	}

	lane := &accountLane{
		key:                key,
		keyCell:            keyCell,
		address:            prepared.Address(),
		original:           &shardAccount,
		originallyExists:   originallyExists,
		current:            prepared,
		storageStat:        storageStat,
		initialStorageStat: initialStorageStat,
		storageProof:       storageProof,
	}
	c.lanes[key] = lane
	return lane, nil
}

func (c *collation) loadPredecessorAccount(key [32]byte, keyCell *cell.Cell) (*cell.Slice, error) {
	prefix := binary.BigEndian.Uint64(key[:8])
	for _, source := range c.accountSources {
		if source.accounts == nil {
			continue
		}
		shard := msgpool.ShardIdent{
			Workchain: source.shard.WorkchainID,
			Shard:     uint64(source.shard.GetShardID()),
		}
		if !shard.Contains(c.shard.Workchain, prefix) {
			continue
		}

		return source.accounts.LoadValue(keyCell)
	}

	return nil, fmt.Errorf("%w: account %x is outside predecessor shards", ErrInvalidInput, key)
}

// accountEstimateSampleInterval is how many transactions pass between reads of
// the account-size estimate. The interval is a local pacing choice, not a
// protocol one: nothing outside collation replays this estimate, and sampling
// more often only inflates it, because the storage stat keeps every
// intermediate version of the dictionary nodes it walks.
const accountEstimateSampleInterval = 16

// updateAccountEstimate records an account for the size estimate. The write is
// deferred: the estimate is only read once per accountEstimateSampleInterval
// transactions, so the accounts touched in between are applied together, as one
// descent instead of one per account.
func (c *collation) updateAccountEstimate(lane *accountLane) error {
	if lane.written {
		return nil
	}
	if lane.original.Account.HashKey() == lane.current.ShardAccount().Account.HashKey() {
		return nil
	}

	// The entry is rendered now, not at flush time: the estimate has always used
	// the account as it stood after its first transaction, and later
	// transactions on the same account never revised it.
	entry, deleted, err := accountDictionaryEntry(lane)
	if err != nil {
		return err
	}
	lane.written = true
	lane.writtenState = lane.current
	if deleted {
		return c.accounts.Delete(entry.Key)
	}
	if entry.Value == nil {
		return nil
	}
	lane.writtenHash = entry.Value.HashKey()
	c.pendingEstimate = append(c.pendingEstimate, entry)
	return nil
}

func (c *collation) flushAccountEstimate() error {
	if len(c.pendingEstimate) == 0 {
		return nil
	}
	if err := c.accounts.SetMany(c.pendingEstimate); err != nil {
		return fmt.Errorf("%w: account dictionary update did not apply: %v", ErrInvalidInput, err)
	}
	c.pendingEstimate = c.pendingEstimate[:0]
	return nil
}

// registerOutputs queues the transaction outputs for the new-message phase.
// Metadata follows one rule: an external-initiated transaction
// starts a fresh depth-0 chain, an internal-initiated one extends the
// consumed message's metadata by one hop, and no inbound metadata means none.
func (c *collation) registerOutputs(
	result *tvm.TransactionExecutionResult,
	lane *accountLane,
	inbound *tlb.MsgMetadata,
	external bool,
) error {
	if len(result.OutMessages) == 0 {
		return nil
	}

	c.new = slices.Grow(c.new, len(result.OutMessages))
	for _, output := range result.OutMessages {
		c.recordOutboundMessageReads(output.Cell)
	}
	var metadata *tlb.MsgMetadata
	metadataResolved := false
	for index, output := range result.OutMessages {
		lt := uint64(0)
		var messageMetadata *tlb.MsgMetadata
		switch output.Msg.MsgType {
		case tlb.MsgTypeInternal:
			if !metadataResolved {
				metadataResolved = true
				if c.config.capabilities&capMsgMetadata != 0 {
					if external {
						metadata = &tlb.MsgMetadata{Depth: 0, Initiator: lane.address, InitiatorLT: result.StartLT}
					} else if inbound != nil {
						derived := *inbound
						derived.Depth++
						metadata = &derived
					}
				}
			}
			lt = output.Msg.AsInternal().CreatedLT
			messageMetadata = metadata
		case tlb.MsgTypeExternalOut:
			lt = output.Msg.AsExternalOut().CreatedLT
		}
		c.new.push(newMessage{
			lt:          lt,
			hash:        output.Cell.HashKey(),
			root:        output.Cell,
			parsed:      output.Msg,
			transaction: result.TransactionCell,
			metadata:    messageMetadata,
			index:       uint32(index),
		})
		c.limits.extraOutMsgs++
		c.stats.NewMessages++
	}
	return nil
}

func (c *collation) insert(
	dict *cell.AugmentedDictionary,
	batch *descriptorBatch,
	message, descriptor *cell.Cell,
) error {
	return c.insertKeyed(dict, batch, descriptorKey(message), descriptor)
}

// insertKeyed is insert for a caller that already holds the descriptor key. A
// message both imported and dequeued in the same block is written to two
// dictionaries under the identical key cell, and building it twice costs a
// cell and a hash for nothing.
func (c *collation) insertKeyed(
	dict *cell.AugmentedDictionary,
	batch *descriptorBatch,
	key, descriptor *cell.Cell,
) error {
	batch.addKeyed(key, descriptor)
	if err := c.limits.storage.AddCell(descriptor); err != nil {
		return err
	}
	batch.ops++
	if batch.ops&descriptorFlushMask != 0 {
		return nil
	}
	// The root is sampled on the same cadence it always was, and the batch is
	// applied immediately before that read, so what the limiter measures is a
	// current dictionary rather than a stale one.
	if err := batch.flush(dict); err != nil {
		return err
	}
	return c.limits.storage.AddCell(dict.RootCell())
}

func (c *collation) registerQueueOp() error {
	c.queueOps++
	if c.queueOps&63 == 0 {
		if err := c.flushQueueDeletes(); err != nil {
			return err
		}
		return c.limits.addProof(c.outQueue.RootCell())
	}
	return nil
}

// finishAccounts writes every touched account and its account block. Both
// dictionaries are filled in one bulk pass each: a per-account write would walk
// its own root-to-leaf path and recombine the augmentation of every node on the
// way up, so the nodes near the root — the root's balance total above all —
// would be rebuilt once per account instead of once per block.
//
// Most accounts are already in place: the size estimate put each one there when
// it first changed, and an account that never ran a second transaction still
// holds exactly that state. Those are skipped here rather than rewritten.
func (c *collation) finishAccounts() error {
	if err := c.flushAccountEstimate(); err != nil {
		return err
	}
	if err := c.traceDispatchValidationClosure(); err != nil {
		return err
	}
	accounts := make([]cell.AugmentedEntry, 0, len(c.lanes))
	blocks := make([]cell.AugmentedEntry, 0, len(c.lanes))
	for _, lane := range c.lanes {
		if lane.transactions == nil {
			continue
		}
		// An account whose state has not moved since the early write renders to
		// exactly the cell already in the dictionary, and every branch below
		// then discards it. Skip the rendering, not just its result.
		if !lane.written || lane.writtenState != lane.current {
			entry, deleted, err := accountDictionaryEntry(lane)
			if err != nil {
				return err
			}
			switch {
			case deleted:
				if lane.written {
					// The early write already removed it.
					break
				}
				// Destroyed accounts are rare and shrink the tree, which the
				// bulk write does not model; they keep the one-by-one path.
				if err = c.accounts.Delete(entry.Key); err != nil {
					return err
				}
			case entry.Value == nil:
			case lane.written && lane.writtenHash == entry.Value.HashKey():
				// Unchanged since the early write.
			default:
				if lane.written {
					entry.Mode = cell.DictSetModeReplace
				}
				accounts = append(accounts, entry)
			}
		}

		stateUpdate, err := tlb.ToCell(tlb.HashUpdate{
			OldHash: lane.original.Account.Hash(),
			NewHash: lane.current.ShardAccount().Account.Hash(),
		})
		if err != nil {
			return err
		}
		accountBlock, err := (tlb.AccountBlock{
			Addr:         lane.key[:],
			Transactions: lane.transactions,
			StateUpdate:  stateUpdate,
		}).ToCell()
		if err != nil {
			return err
		}
		blocks = append(blocks, cell.AugmentedEntry{
			Key:   lane.laneKey(),
			Value: accountBlock,
			Mode:  cell.DictSetModeAdd,
		})
		if lane.storageStat != nil {
			if _, ok := lane.current.State().StorageInfo.StorageExtra.(tlb.StorageExtraInfo); ok {
				if c.storageStats == nil {
					c.storageStats = make(AccountStorageStats)
				}
				c.storageStats[lane.storageStat.HashKey()] = lane.storageStat
			}
		}
	}

	if err := c.accounts.SetMany(accounts); err != nil {
		return fmt.Errorf("%w: account dictionary update did not apply: %v", ErrInvalidInput, err)
	}
	// Add mode makes SetMany reject a repeated account, which is the duplicate
	// account block check the per-entry insert used to make.
	if err := c.accountBlocks.SetMany(blocks); err != nil {
		return fmt.Errorf("%w: account block insert did not apply: %v", ErrInvalidInput, err)
	}
	return nil
}

func (c *collation) traceDispatchValidationClosure() error {
	if err := c.oldDispatchQueue.ScanDiff(c.dispatchQueue.AugmentedDictionary, true, func(
		keyCell *cell.Cell,
		oldValueExtra, newValueExtra *cell.Slice,
	) error {
		keyLoader, err := keyCell.BeginParse()
		if err != nil {
			return fmt.Errorf("%w: load dispatch diff key: %v", ErrInvalidInput, err)
		}
		var accountID [32]byte
		if err = keyLoader.LoadSliceInto(accountID[:], 256); err != nil {
			return fmt.Errorf("%w: decode dispatch diff key: %v", ErrInvalidInput, err)
		}
		var oldAccount *tlb.AccountDispatchQueue
		if oldValueExtra != nil {
			var traced bool
			oldAccount, traced = c.oldDispatchAccounts[accountID]
			if !traced {
				oldAccount, err = c.loadPredecessorDispatchAccount(accountID)
				if err != nil {
					return fmt.Errorf("%w: decode predecessor dispatch account: %v", ErrInvalidInput, err)
				}
			}
			if _, _, err = oldAccount.Messages.LoadMax(); err != nil {
				return fmt.Errorf("%w: load predecessor dispatch account maximum: %v", ErrInvalidInput, err)
			}
		}
		if newValueExtra != nil {
			newAccount, err := dispatchDiffAccount(newValueExtra)
			if err != nil {
				return fmt.Errorf("%w: decode candidate dispatch account: %v", ErrInvalidInput, err)
			}
			minimum, _, err := newAccount.Messages.LoadMin()
			if err != nil {
				return fmt.Errorf("%w: load candidate dispatch account minimum: %v", ErrInvalidInput, err)
			}
			// Mutating the nested dictionary rebuilds its Patricia path. Trace the
			// same minimum through the immutable predecessor view so a reused leaf
			// is retained in the previous-state proof.
			if oldAccount != nil {
				_, oldErr := oldAccount.Messages.LoadValue(minimum)
				if oldErr != nil && !isMissingKey(oldErr) {
					return fmt.Errorf("%w: load candidate dispatch account minimum from predecessor: %v", ErrInvalidInput, oldErr)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for accountID, lane := range c.lanes {
		if lane.transactions == nil {
			continue
		}
		_, err := c.loadPredecessorDispatchValue(accountID)
		if err != nil && !isMissingKey(err) {
			return fmt.Errorf("%w: lookup transaction account %x in predecessor dispatch queue: %v", ErrInvalidInput, accountID, err)
		}
	}
	return nil
}

func dispatchDiffAccount(valueExtra *cell.Slice) (*tlb.AccountDispatchQueue, error) {
	value := valueExtra.Copy()
	if err := (tlb.AugDispatchQueue{}).SkipExtra(value); err != nil {
		return nil, err
	}
	var account tlb.AccountDispatchQueue
	if err := loadExactSlice(&account, value); err != nil {
		return nil, err
	}
	return &account, nil
}

// accountDictionaryEntry renders one lane as a dictionary write. A lane whose
// account no longer exists reports deleted instead, and one that never existed
// and still does not is neither.
func accountDictionaryEntry(lane *accountLane) (cell.AugmentedEntry, bool, error) {
	key := lane.laneKey()
	state := lane.current.State()
	if !state.IsValid || state.Status == tlb.AccountStatusNonExist {
		return cell.AugmentedEntry{Key: key}, lane.originallyExists, nil
	}

	// ShardAccountCell is the hand-written equivalent of tlb.ToCell over the
	// same three fields, minus the reflection. It cannot report an error, and
	// it left-pads a short last-transaction hash where tlb.ToCell refused it,
	// so the shape it would have rejected is rejected here instead.
	shard := lane.current.ShardAccount()
	if shard == nil || shard.Account == nil || len(shard.LastTransHash) != 32 {
		return cell.AugmentedEntry{}, false, fmt.Errorf("%w: account %x has a malformed shard account", ErrInvalidInput, lane.key)
	}
	value := lane.current.ShardAccountCell()
	mode := cell.DictSetModeReplace
	if !lane.originallyExists {
		mode = cell.DictSetModeAdd
	}
	return cell.AugmentedEntry{Key: key, Value: value, Mode: mode}, false, nil
}

func loadExactSlice(dst any, s *cell.Slice) error {
	if err := tlb.LoadFromCell(dst, s); err != nil {
		return err
	}
	if s.BitsLeft() != 0 || s.RefsNum() != 0 {
		return fmt.Errorf("trailing data: %d bits, %d refs", s.BitsLeft(), s.RefsNum())
	}
	return nil
}

func currencyZero(value tlb.CurrencyCollection) bool {
	return value.Coins.IsZero() && value.ExtraCurrencies.IsEmpty()
}

// recordExecutionRead adds a cell the machine loaded to the predecessor read
// record. The machine reports every first load with the cell in hand, because
// gas has to charge a first load differently from a repeat, so this is an exact
// account of what execution read — and, unlike the traversal record, it holds
// even when the cell reached the machine through a route that lost the trace.
//
// It is the union that makes an account's code or data impossible to omit from
// the collated proof: those are exactly the cells a validator's own replay
// reads, and a missing one is what a peer reports as a pruned branch.
//
// Reads are unbilled: execution also loads the inbound message and cells it
// built itself, which are not part of the predecessor tree and can never appear
// in its proof, so charging them to the collated-size estimate would shrink the
// block for bytes that are never emitted.
// recordOutboundMessageReads records everything reachable from an emitted
// message.
//
// A contract can put a predecessor subtree into a message without the machine
// ever opening it: PUSHREF pushes a reference without registering a load, and a
// continuation window drops the recording trace, so neither the traversal record
// nor the machine's load reports see those cells. The fee accounting then walks
// the whole message to size it, and the reference validator walks it the same
// way — so a cell missing here is a pruned branch on the far side.
//
// The walk costs one more pass over cells the fee accounting already traverses,
// bounded by the message size limit. Cells the transaction built are recorded
// too and are simply inert: the proof selects by hash over the predecessor tree
// and never finds them.
func (c *collation) recordOutboundMessageReads(root *cell.Cell) {
	if root == nil {
		return
	}
	var visited map[cell.Hash]struct{}
	var walk func(cur *cell.Cell, depth int)
	walk = func(cur *cell.Cell, depth int) {
		if cur == nil || depth > maxOutboundMessageRecordDepth {
			return
		}
		hash := cur.HashKey()
		if _, seen := visited[hash]; seen {
			return
		}
		visited[hash] = struct{}{}
		c.recordExecutionRead(cur)
		if cur.IsSpecial() {
			return
		}
		for i := 0; i < int(cur.RefsNum()); i++ {
			ref, err := cur.PeekRef(i)
			if err != nil {
				return
			}
			walk(ref, depth+1)
		}
	}
	visited = make(map[cell.Hash]struct{}, 16)
	walk(root, 0)
}

// maxOutboundMessageRecordDepth bounds the walk at the cell depth limit, so a
// malformed message cannot turn recording into unbounded recursion.
const maxOutboundMessageRecordDepth = 512

func (c *collation) recordExecutionRead(loaded *cell.Cell) {
	c.usage.RecordUnbilled(loaded)
	if c.collatedProofEstimate != nil {
		c.collatedProofEstimate.addExecutionRead(loaded)
	}
}
