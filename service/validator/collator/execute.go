package collator

import (
	"bytes"
	"cmp"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
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
	//
	// It is shared with every other account that starts from the same initial
	// dictionary, and is owned by collation.accountStorageProofs rather than by
	// the lane — see sharedAccountStorageProof.
	storageProof *cell.MerkleProofBuilder
	// transactions accumulates what the account executed, in execution order.
	// The AccountBlock dictionary is built from it once in finishAccounts —
	// see buildAccountTransactions.
	transactions []accountTransaction
	// touched records that the account produced at least one transaction. It is
	// the question every phase outside execution asks; the slice above is the
	// build input of finishAccounts, and reading its length instead would tie
	// those phases to how and when the dictionary gets built.
	touched          bool
	key              [32]byte
	originallyExists bool
	// accountPath is the predecessor ShardAccounts spine loaded for this key.
	// Its cells are charged to the block estimate only after the account changes.
	accountPath      []*cell.Cell
	estimateRecorded bool
	// keyCell is the account's 256-bit dictionary key. Both dictionaries the
	// account lands in take the same key and cells are immutable, so it is
	// built once instead of once per use.
	keyCell *cell.Cell
	// tracer is what every cell of this lane carries instead of the shared
	// recorders' traces, so the lane can execute ahead of its admission without
	// writing into the shared record. See laneTracer.
	tracer *laneTracer
}

// accountTransaction is one executed transaction of an account, held by its
// logical time until the AccountBlock dictionary is built. It mirrors the
// reference's LtCellRef (crypto/block/transaction.h:40), the element type of
// block::Account::transactions.
type accountTransaction struct {
	startLT uint64
	root    *cell.Cell
}

// accountStorageProof is the collation-wide record of one initial storage-stat
// dictionary: the single traced builder every account bound to that dictionary
// reads through, and how much of its serialized proof the collated size estimate
// has already been charged for.
type accountStorageProof struct {
	builder  *cell.MerkleProofBuilder
	estimate *accountStorageProofSizeEstimator
	// charged is the conservative standalone-BOC size this dictionary last
	// charged to collatedFixedEstimate. The estimate can shrink when loading a
	// small cell replaces its pruned boundary, so this is a high-watermark: bytes
	// already admitted are never subtracted, and later reads charge only growth
	// beyond it. buildCollatedRoots emits one proof for all accounts sharing the
	// dictionary.
	charged uint64
}

// accountStorageProofBOCOverhead bounds everything outside the traced proof
// body in a standalone CRC-protected BOC: the account_storage_dict_proof wrapper,
// the MerkleProof special cell, the BOC header and its checksum. At the format's
// maximum four-byte reference index and eight-byte offset they occupy 85 bytes.
// The final collated BOC shares one header across all roots, so charging the
// standalone bound preserves the conservative policy the old exact snapshot
// serialization provided.
const accountStorageProofBOCOverhead = uint64(85)

// accountStorageProofSizeEstimator incrementally bounds the serialized cells of
// one ReadSet proof body. A boundary is not always a 38-byte pruned branch:
// proof generation keeps resident terminal cells in full, including maximum-size
// leaves, while a high-level boundary can carry several hashes and depths. The
// estimator therefore remembers the price at which every hash entered, and
// replaces that exact price if the cell is loaded later.
//
// It is safe for concurrent ReadSet callbacks. The current pipeline replays
// speculative lanes on the collation goroutine, but the recorder contract allows
// callbacks from several goroutines and a shared storage dictionary must not
// make admission depend on which account won a race.
type accountStorageProofSizeEstimator struct {
	mu    sync.Mutex
	bytes uint64
	cells map[cell.Hash]accountStorageProofCellEstimate
}

type accountStorageProofCellEstimate struct {
	bytes  uint64
	loaded bool
}

type accountStorageProofBoundaryEstimate struct {
	hash  cell.Hash
	bytes uint64
}

func newAccountStorageProofSizeEstimator() *accountStorageProofSizeEstimator {
	return &accountStorageProofSizeEstimator{
		cells: make(map[cell.Hash]accountStorageProofCellEstimate),
	}
}

func accountStorageProofLoadedCellBytes(loaded *cell.Cell) uint64 {
	// Two descriptors and at most four bytes per reference are enough for the
	// exact BOC body. Five keeps the same three-byte per-cell safety margin as
	// the collated-proof estimator while four-byte refs cover the format maximum.
	return uint64(5+(loaded.BitsSize()+7)/8) + uint64(loaded.RefsNum())*4
}

func accountStorageProofBoundaryBytes(boundary *cell.Cell) uint64 {
	// A pruned boundary stores one hash and depth for every significant level
	// of the source mask, plus level zero. Applying the proof's virtual level can
	// only remove mask bits, so the complete source mask is a tight upper bound
	// without constructing the boundary. Seven is its two-byte payload prefix,
	// two BOC descriptors, and the same three-byte per-cell safety margin used
	// for loaded cells.
	hashes := bits.OnesCount8(boundary.LevelMask().Mask) + 1
	bytes := uint64(7 + hashes*(sha256.Size+2))
	if boundary.RefsNum() == 0 {
		// Proof generation retains an ordinary terminal cell verbatim. A lazy
		// boundary also has no refs, but exposes the payload of the pruned cell it
		// will materialize; taking the larger value covers sparse/high level masks
		// whose payload contains more than one hash and depth.
		bytes = max(bytes, accountStorageProofLoadedCellBytes(boundary))
	}
	return bytes
}

func (e *accountStorageProofSizeEstimator) addLoadedCell(loaded *cell.Cell) {
	hash := loaded.HashKey()
	loadedBytes := accountStorageProofLoadedCellBytes(loaded)
	var refBuf [4]accountStorageProofBoundaryEstimate
	refs := refBuf[:loaded.RefsNum()]
	for i := range refs {
		boundary := loaded.MustPeekRef(i)
		refs[i] = accountStorageProofBoundaryEstimate{
			hash:  boundary.HashKey(),
			bytes: accountStorageProofBoundaryBytes(boundary),
		}
	}

	e.mu.Lock()
	current, seen := e.cells[hash]
	if seen && current.loaded {
		e.mu.Unlock()
		return
	}
	if seen {
		e.bytes -= current.bytes
	}
	e.bytes += loadedBytes
	e.cells[hash] = accountStorageProofCellEstimate{bytes: loadedBytes, loaded: true}
	for _, ref := range refs {
		child, exists := e.cells[ref.hash]
		if exists && (child.loaded || child.bytes >= ref.bytes) {
			continue
		}
		if exists {
			e.bytes -= child.bytes
		}
		e.cells[ref.hash] = accountStorageProofCellEstimate{bytes: ref.bytes}
		e.bytes += ref.bytes
	}
	e.mu.Unlock()
}

func (e *accountStorageProofSizeEstimator) size() uint64 {
	e.mu.Lock()
	bytes := e.bytes
	e.mu.Unlock()
	return bytes
}

func newAccountStorageProof(root *cell.Cell) *accountStorageProof {
	proof := &accountStorageProof{
		builder:  cell.NewMerkleProofBuilder(root),
		estimate: newAccountStorageProofSizeEstimator(),
	}
	// Installed before the traced root can leave this function: no dictionary
	// read can enter the proof without entering its size estimate as well.
	proof.builder.ReadSet().SetRecordCallback(proof.estimate.addLoadedCell)

	return proof
}

func (p *accountStorageProof) estimatedBOCSize() uint64 {
	body := p.estimate.size()
	if body == 0 {
		return 0
	}
	if math.MaxUint64-body < accountStorageProofBOCOverhead {
		return math.MaxUint64
	}

	return body + accountStorageProofBOCOverhead
}

// accountPathRecorder observes only the ShardAccounts lookup. The trace is
// removed from the returned value before the account itself is decoded, so the
// recorded cells are the Patricia spine rather than the account payload that
// addTransaction already charges exactly.
type accountPathRecorder struct {
	trace *cell.Trace
	path  []*cell.Cell
}

func newAccountPathRecorder() *accountPathRecorder {
	r := &accountPathRecorder{path: make([]*cell.Cell, 0, 24)}
	r.trace = cell.NewTraceForListener(r)
	return r
}

func (r *accountPathRecorder) OnLoad(loaded *cell.Cell) {
	r.path = append(r.path, loaded)
}

func (*accountPathRecorder) OnCreate() {}

func (r *accountPathRecorder) ChildTrace(int) *cell.Trace {
	return r.trace
}

func (*accountPathRecorder) PendingError() error {
	return nil
}

func (c *collation) prepareAccountPathRecorder() {
	c.accountSiblings = newAccountSiblingPrefetch(c.ctx)
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
	parallelSafe     bool
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

// externalIntakeClosed reports the outbound-queue brake: past this depth the
// collation declines every ordinary external it is offered, one verdict per
// message (SKIP_EXTERNALS_QUEUE_SIZE, collator.cpp:54). The queue only grows
// once cleanup has run — the sole decrement is the cleanup phase, which
// precedes externals — so a batch that starts closed stays closed, and the
// callers below use that to skip the whole batch without paying to prepare it.
func (c *collation) externalIntakeClosed() bool {
	return c.queueSize > skipExternalsQueueSize
}

func (c *collation) processExternalBatch(
	externals []ExternalInput,
	deadline time.Time,
) (externalBatchResult, error) {
	// Hoisted from the per-message loop for the batch that starts closed: the
	// verdicts are already decided, so account prewarm and wave planning for
	// messages none of which can be taken is pure cost. On the loaded stand this
	// was the whole external stage of every zero-external block — the pool holds
	// thousands of messages under spam, and each one was prewarmed from storage
	// per block just to be declined. The per-message check below stays, for the
	// batch that crosses the threshold mid-way.
	if len(externals) != 0 && c.externalIntakeClosed() {
		for _, skipped := range externals {
			c.recordExternal(skipped.Ref, msgpool.ExternalSkippedLimit)
		}
		return externalBatchResult{consumed: len(externals)}, nil
	}
	if workers := c.internalWaveParallelism(); workers > 0 {
		return c.processExternalBatchInWaves(externals, deadline, workers)
	}
	c.prewarmExternalInputs(externals)

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
		if c.externalIntakeClosed() {
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
		c.prewarmGeneratedOutputs(result)
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
	if workers := c.generatedWaveParallelism(enqueueOnly); workers > 0 {
		if err := c.processNewMessagesInWaves(enqueueOnly, workers); err != nil {
			return err
		}
		return c.flushQueueAdds()
	}

	for c.new.Len() > 0 {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		c.checkNewMessageTop()
		if err := c.processNextNewMessage(&enqueueOnly); err != nil {
			return err
		}
	}
	return c.flushQueueAdds()
}

func (c *collation) checkNewMessageTop() {
	// Parity with collator.cpp:4847-4854, which re-reads the normal limit
	// class and the soft timeout at the TOP of every heap item — before
	// extra_out_msgs-- and before the enqueue_only latch, which is why both
	// checks sit above popMin here. Reading only the c.blockFull FIELD (the
	// pre-fix behaviour) misses every way the estimate grows without an
	// immediate delivery: enqueue() charges cells through insert() and
	// re-proofs the queue root every 64 ops, so a drain could cross the
	// normal class and keep executing in-block messages the reference would
	// have enqueued. completeGeneratedImmediate's own post-delivery check
	// covers only the delivery path.
	c.updateCollatedEstimate()
	if !c.limits.fits(c.fullMark()) {
		c.blockFull = true
	}
	if !c.blockFull && c.internalMsgExpired() {
		c.blockFull = true
		c.blockFullTimeout = true
		c.stats.InternalMsgTimeouts++
	}
}

func (c *collation) processNextNewMessage(enqueueOnly *bool) error {
	plan := generatedPlan{item: c.new.popMin()}
	return c.retireGeneratedPlan(&plan, enqueueOnly)
}

// retireGeneratedImmediate executes or commits one generated in-shard message:
// the message is wrapped into a use_dest_bits=96 envelope carrying the full
// forward fee, the consuming transaction runs after the creating one, and the
// descriptors form the msg_import_imm/msg_export_imm pair.
func (c *collation) retireGeneratedImmediate(plan *generatedPlan) error {
	item := &plan.item
	if err := c.advanceProcessedBound("generated", item.lt, item.hash); err != nil {
		return err
	}
	var (
		result *tvm.TransactionExecutionResult
		lane   *accountLane
		err    error
	)
	if plan.started {
		result, lane, err = c.commitGeneratedPlan(plan)
	} else {
		prepared := plan.prepared
		if prepared == nil {
			var prepareErr error
			prepared, prepareErr = tvm.PrepareMessage(item.root)
			if prepareErr != nil {
				return fmt.Errorf("%w: generated message %x: %v", ErrInvalidInput, item.hash, prepareErr)
			}
		}
		result, lane, err = c.executePrepared(prepared, item.lt)
		if err != nil {
			return fmt.Errorf("execute generated message %x: %w", item.hash, err)
		}
	}
	if err != nil {
		return fmt.Errorf("execute generated message %x: %w", item.hash, err)
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	return c.completeGeneratedImmediate(plan, result, lane)
}

func (c *collation) completeGeneratedImmediate(
	plan *generatedPlan,
	result *tvm.TransactionExecutionResult,
	lane *accountLane,
) error {
	item := &plan.item
	c.prewarmGeneratedOutputs(result)

	var (
		envelopeCell *cell.Cell
		in           *cell.Cell
		err          error
	)
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
			FwdFeeRemaining: plan.internal.FwdFee,
			Msg:             item.root,
			Metadata:        item.metadata,
		}).ToCell()
		if err == nil {
			in, err = descriptorFee(0b011, 3, envelopeCell, result.TransactionCell, plan.internal.FwdFee) // msg_import_imm$011
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
			c.immediateQueueKeys = append(c.immediateQueueKeys, msgpool.MakeQueueKey(plan.destination, item.hash))
		}
	}

	if err = c.registerOutputs(result, lane, item.metadata, false); err != nil {
		return err
	}
	c.stats.ImmediateDelivered++
	c.updatePeakLoad()
	if !c.limits.fits(c.fullMark()) {
		c.blockFull = true
	} else if c.internalMsgExpired() {
		// collator.cpp:3732-3736, the second half of process_one_new_message's
		// "check whether the block is full now" step. Both arms return 3 there,
		// which latches enqueue_only for the next heap item; setting c.blockFull
		// is how the loop-top latch above reaches the same state.
		c.blockFull = true
		c.blockFullTimeout = true
		c.stats.InternalMsgTimeouts++
	}
	return nil
}

func (c *collation) enqueue(item *newMessage, source, destination msgpool.AccountPrefix) error {
	if c.queueSize == maxOutMsgQueueSize {
		if err := c.flushQueueAdds(); err != nil {
			return err
		}
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
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: item.lt, Msg: envelopeCell}).ToCell()
	if err != nil {
		return err
	}
	if err = c.deferQueueAdd(key, enqueued, queuePendingAddGenerated); err != nil {
		return err
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
	afterLT = max(afterLT, c.importAfterLT(lane))

	result, err := c.emulate(lane, message, afterLT)
	if err != nil {
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

// importAfterLT is the floor an imported internal message's transaction lt
// takes from the dispatch phase: the last emission the account made there.
// It is the only floor an internal import has besides the block start and the
// account's own chain, which is what makes an import's lt independent of every
// other account's transactions — the property the wave executor relies on.
func (c *collation) importAfterLT(lane *accountLane) uint64 {
	if emittedLT, ok := c.lastDispatchEmitted[lane.key]; ok {
		return emittedLT
	}
	return 0
}

// emulate runs one transaction against the lane's current state and returns
// its result without committing it. It mutates nothing shared — the lane's
// state moves only in commitExecution — and traces every read to the lane's
// tracer, which is what lets it run off the main goroutine for a lane the
// collation has not yet admitted. afterLT must already carry every floor the
// caller applies; see executePrepared.
func (c *collation) emulate(lane *accountLane, message *tvm.PreparedMessage, afterLT uint64) (*tvm.TransactionExecutionResult, error) {
	minLT := max(c.header.StartLt, afterLT)
	if minLT >= math.MaxInt64 {
		return nil, fmt.Errorf("%w: transaction lt overflow", ErrInvalidInput)
	}
	result, err := c.builder.machine.EmulateTransaction(c.blockCtx, lane.current, message, tvm.TransactionOptions{
		LogicalTime:        int64(minLT + 1),
		AccountStorageStat: lane.storageStat,
		OnCellLoad:         lane.tracer.onExecutionRead,
	})
	if err != nil {
		return nil, err
	}
	if err = c.ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
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

	lane.touched = true
	c.trackAccountStorageProof(lane)
	// A repeated lt would replace an earlier transaction rather than add one; it
	// is refused where the keys first meet, in buildAccountTransactions.
	lane.transactions = append(lane.transactions, accountTransaction{
		startLT: result.StartLT,
		root:    result.TransactionCell,
	})

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
	c.updateAccountEstimate(lane)
	if c.limits.transactions%accountEstimateSampleInterval == 0 {
		c.limits.commitAccountPaths()
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
	lane, err := c.resolveLane(key)
	if err != nil {
		return nil, err
	}
	c.registerLane(lane)
	return lane, nil
}

// resolveLane loads the account behind key out of the predecessor and prepares
// it for execution. It writes nothing the collation shares: the lane it returns
// is not in c.lanes, its storage dictionary is bound to no shared proof, and its
// spine has not been handed to the sibling prefetch. All of that is
// registerLane's, and the split is what lets several lanes be resolved at once
// by several goroutines while the collation decides, in message order, which
// of them it will keep.
//
// Every read it makes is traced to the lane's own tracer and to nothing else.
// The ShardAccounts descent goes through a per-lane view of the predecessor
// dictionary — CopyWithTrace replaces the root's trace rather than adding to
// it — so the spine is seen by the lane's recorder and the lane's tracer and
// not by the shared record, and the account payload underneath inherits the
// tracer alone once the recorder is stripped off the value.
func (c *collation) resolveLane(key [32]byte) (*accountLane, error) {
	return c.resolveLaneWith(key, false)
}

// resolveLaneWith is resolveLane with the tracer's mode chosen up front:
// buffering for a lane resolved on a worker ahead of its admission,
// pass-through for one resolved on the main goroutine.
func (c *collation) resolveLaneWith(key [32]byte, speculative bool) (*accountLane, error) {
	lane := &accountLane{key: key}
	lane.tracer = newLaneTracer(c, lane)
	if speculative {
		lane.tracer.speculate()
	}
	keyCell := cell.BeginCell().MustStoreSlice(key[:], 256).EndCell()
	value, accountPath, err := c.loadPredecessorAccount(lane.tracer, key)
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
	} else if err = loadExactSlice(&shardAccount, &value); err != nil {
		return nil, fmt.Errorf("decode shard account %x: %w", key, err)
	}

	effectiveAddr := address.NewAddress(0, byte(c.shard.Workchain), key[:])
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
	if storageStat != nil && c.fullCollated {
		// The dictionary the transaction walks carries the lane's stat trace.
		// Before lanes had tracers it carried the shared proof builder's own
		// trace; the tracer forwards to that builder in pass-through and
		// buffers for it in speculation, so what the builder records is the
		// same set of cells either way.
		storageStat = storageStat.WithTrace(lane.tracer.statTrace)
	}

	lane.keyCell = keyCell
	lane.address = prepared.Address()
	lane.original = &shardAccount
	lane.originallyExists = originallyExists
	lane.current = prepared
	lane.storageStat = storageStat
	lane.initialStorageStat = initialStorageStat
	lane.accountPath = accountPath
	return lane, nil
}

// registerLane makes a resolved lane part of the collation. It runs on the
// main goroutine, in message order, and is the only place a lane enters
// c.lanes, binds its shared storage proof, or hands its spine to the prefetch.
func (c *collation) registerLane(lane *accountLane) {
	if lane.initialStorageStat != nil && c.fullCollated {
		lane.storageProof = c.sharedAccountStorageProof(lane.initialStorageStat)
	}
	c.lanes[lane.key] = lane
	// The earliest point at which the spine is known. A lane that never
	// transacts is submitted too, deliberately: finishAccounts hands the bulk
	// write its path as well, so its fork nodes carry the same untouched
	// siblings.
	c.accountSiblings.submit(lane.accountPath)
}

func (c *collation) sharedAccountStorageProof(stat *cell.Cell) *cell.MerkleProofBuilder {
	hash := stat.HashKey()
	if shared := c.accountStorageProofs[hash]; shared != nil {
		return shared.builder
	}
	if c.accountStorageProofs == nil {
		c.accountStorageProofs = make(map[cell.Hash]*accountStorageProof)
	}
	proof := newAccountStorageProof(stat)
	c.accountStorageProofs[hash] = proof

	return proof.builder
}

func (c *collation) loadPredecessorAccount(
	tracer *laneTracer,
	key [32]byte,
) (cell.Slice, []*cell.Cell, error) {
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

		// The descent is observed by two listeners and the shared record is
		// not one of them: the lane's tracer, which carries the reads to the
		// record when the lane retires, and a recorder that keeps the spine
		// for the size estimate. The recorder is stripped off the value
		// before the account is decoded, so the spine stays the spine and the
		// payload reaches the tracer alone.
		recorder := newAccountPathRecorder()
		view := &tlb.ShardAccountsAugDict{
			AugmentedDictionary: source.accounts.CopyWithTrace(cell.CombineTraces(tracer.trace, recorder.trace)),
		}
		var value cell.Slice
		err := view.LoadValueByBytesKeyInto(key[:], &value)
		if err == nil {
			value.SetTrace(value.Trace().WithoutTrace(recorder.trace))
		}
		return value, recorder.path, err
	}

	return cell.Slice{}, nil, fmt.Errorf("%w: account %x is outside predecessor shards", ErrInvalidInput, key)
}

// accountEstimateSampleInterval preserves the reference estimator's pacing
// without materializing intermediate ShardAccounts roots. Each window owns a
// path union; common ancestors touched in a later window represent the new
// versions the old SetMany-based estimator counted again.
const accountEstimateSampleInterval = 16

// updateAccountEstimate adds the predecessor Patricia spine once, when the
// account first changes. Transaction and account payload cells are already
// charged by addTransaction; rebuilding ShardAccounts used to be necessary
// only to discover this path cost.
func (c *collation) updateAccountEstimate(lane *accountLane) {
	if lane.estimateRecorded {
		return
	}
	if lane.original.Account.HashKey() == lane.current.ShardAccount().Account.HashKey() {
		return
	}

	lane.estimateRecorded = true
	state := lane.current.State()
	deleted := !state.IsValid || state.Status == tlb.AccountStatusNonExist
	c.limits.addAccountPath(lane.accountPath, lane.originallyExists, deleted, lane.original.Account)
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
	certifyGenerated := c.generatedWaveParallelism(false) > 0
	var metadata *tlb.MsgMetadata
	metadataResolved := false
	for index, output := range result.OutMessages {
		certifyParallel := certifyGenerated && output.Msg.MsgType == tlb.MsgTypeInternal
		parallelSafe := c.recordOutboundMessageReads(output.Cell, certifyParallel)
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
			lt:           lt,
			hash:         output.Cell.HashKey(),
			root:         output.Cell,
			parsed:       output.Msg,
			transaction:  result.TransactionCell,
			metadata:     messageMetadata,
			index:        uint32(index),
			parallelSafe: parallelSafe,
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
	key cell.Hash,
	descriptor *cell.Cell,
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
// would be rebuilt once per account instead of once per block. Each account's
// own transaction dictionary is built here for the same reason, from what
// execution accumulated.
//
// The size estimator never mutates accounts, so this is the only bulk update of
// the dictionary during a collation.
func (c *collation) finishAccounts() error {
	accountPaths := make([][]*cell.Cell, 0, len(c.lanes))
	builds := make([]accountLaneBuild, 0, len(c.lanes))
	for _, lane := range c.lanes {
		if len(lane.accountPath) != 0 {
			// Paths from every lookup are useful, including a lane which did not
			// produce a transaction: it can already contain an untouched sibling
			// root another lane's update would otherwise load again.
			accountPaths = append(accountPaths, lane.accountPath)
		}
		if lane.touched {
			builds = append(builds, accountLaneBuild{lane: lane})
		}
	}
	buildAccountLanes(builds)

	accounts := make([]cell.AugmentedEntry, 0, len(builds))
	accountDeletes := make([]*cell.Cell, 0)
	blocks := make([]cell.AugmentedBytesEntry, 0, len(builds))
	for i := range builds {
		build := &builds[i]
		if build.err != nil {
			return build.err
		}
		switch {
		case build.deleted:
			// Destroyed accounts are rare and shrink the tree, which the
			// bulk write does not model; they keep the one-by-one path. Apply
			// them after the batch so a delete cannot compress a label and make
			// the batch's already-loaded predecessor paths stale.
			accountDeletes = append(accountDeletes, build.entry.Key)
		case build.entry.Value != nil:
			accounts = append(accounts, build.entry)
		}
		blocks = append(blocks, build.block)
		// Merged here rather than in the worker: the lanes own nothing of the
		// collation, and this map is the one thing the loop used to write into it.
		if build.storageStat != nil {
			if c.storageStats == nil {
				c.storageStats = make(AccountStorageStats)
			}
			c.storageStats[build.storageStat.HashKey()] = build.storageStat
		}
	}

	// The prefetched untouched siblings enter as one more loaded path. The
	// resolver is a hash index over every cell of every entry and consults it
	// before it asks the store, so an extra entry can only remove a load; order
	// and duplicates are irrelevant to it.
	if siblings := c.accountSiblings.join(); len(siblings) != 0 {
		accountPaths = append(accountPaths, siblings)
	}

	if c.fullCollated && c.master == nil && len(accountDeletes) == 0 {
		diff, err := c.accounts.SetManyWithLoadedPathsAndDiff(
			accounts,
			accountPaths,
			collationParallelism,
		)
		if err != nil {
			return fmt.Errorf("%w: account dictionary update did not apply: %v", ErrInvalidInput, err)
		}
		c.accountMutationDiff = diff
	} else if err := c.accounts.SetManyWithLoadedPaths(
		accounts,
		accountPaths,
		collationParallelism,
	); err != nil {
		return fmt.Errorf("%w: account dictionary update did not apply: %v", ErrInvalidInput, err)
	}
	for _, key := range accountDeletes {
		if err := c.accounts.Delete(key); err != nil {
			return fmt.Errorf("%w: account dictionary delete did not apply: %v", ErrInvalidInput, err)
		}
	}
	// Add mode makes SetMany reject a repeated account, which is the duplicate
	// account block check the per-entry insert used to make.
	if err := c.accountBlocks.SetManyByBytes(blocks, collationParallelism); err != nil {
		return fmt.Errorf("%w: account block insert did not apply: %v", ErrInvalidInput, err)
	}
	return nil
}

// accountLaneBuild is one touched lane's share of the final account stage: the
// two dictionary entries it contributes and the storage stat it declares. It
// exists so the work can be done off the collation goroutine — a worker reads
// only its own lane and writes only its own element.
type accountLaneBuild struct {
	lane        *accountLane
	entry       cell.AugmentedEntry
	deleted     bool
	block       cell.AugmentedBytesEntry
	storageStat *cell.Cell
	err         error
}

// buildAccountLanes renders every touched lane, in parallel when there is more
// than one worker's worth of them.
//
// The lanes are independent: each one reads its own accountLane and the
// predecessor cells hanging off it, and cell hashes are computed at creation
// rather than memoised on first read, so nothing shared is written. The one
// collation-wide effect the loop used to have — the storage-stat map — is
// merged by the caller instead.
//
// Order is not a correctness question here. c.lanes is a Go map, so the batches
// this feeds have always been assembled in an arbitrary order, and both bulk
// writes sort what they are given. The index still matters for one thing: the
// lowest failing index is the error a sequential pass over the same snapshot
// would have reported, so a malformed lane names itself the same way whether or
// not the batch was split.
func buildAccountLanes(builds []accountLaneBuild) {
	workers := min(collationParallelism, runtime.GOMAXPROCS(0), len(builds))
	if workers < 2 {
		for i := range builds {
			buildAccountLane(&builds[i])
		}
		return
	}

	var wg sync.WaitGroup
	var next atomic.Int64
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				i := next.Add(1) - 1
				if i >= int64(len(builds)) {
					return
				}
				buildAccountLane(&builds[i])
			}
		}()
	}
	wg.Wait()
}

func buildAccountLane(build *accountLaneBuild) {
	lane := build.lane
	entry, deleted, err := accountDictionaryEntry(lane)
	if err != nil {
		build.err = err
		return
	}
	build.entry, build.deleted = entry, deleted

	oldHash := lane.original.Account.HashKey()
	newHash := lane.current.ShardAccount().Account.HashKey()
	stateUpdate, err := (tlb.HashUpdate{
		OldHash: oldHash[:],
		NewHash: newHash[:],
	}).ToCell()
	if err != nil {
		build.err = err
		return
	}
	transactions, err := buildAccountTransactions(lane)
	if err != nil {
		build.err = err
		return
	}
	accountBlock, err := (tlb.AccountBlock{
		Addr:         lane.key[:],
		Transactions: transactions,
		StateUpdate:  stateUpdate,
	}).ToCell()
	if err != nil {
		build.err = err
		return
	}
	build.block = cell.AugmentedBytesEntry{
		Key:   lane.key[:],
		Value: accountBlock,
		Mode:  cell.DictSetModeAdd,
	}
	if lane.storageStat != nil {
		if _, ok := lane.current.State().StorageInfo.StorageExtra.(tlb.StorageExtraInfo); ok {
			build.storageStat = lane.storageStat
		}
	}
}

func dispatchDiffAccount(valueExtra cell.Slice) (*tlb.AccountDispatchQueue, error) {
	value := valueExtra
	if err := (tlb.AugDispatchQueue{}).SkipExtra(&value); err != nil {
		return nil, err
	}
	var account tlb.AccountDispatchQueue
	if err := loadExactSlice(&account, &value); err != nil {
		return nil, err
	}
	return &account, nil
}

// buildAccountTransactions renders the lane's executed transactions as the
// AccountBlock dictionary in a single bulk write. SetMany produces the
// dictionary repeated Set would have produced — same cells, same augmentations
// (aug_dict_bulk.go) — but visits the union of the key paths once, where a Set
// per transaction walks its own root-to-leaf path and recombines the
// augmentation of every node on the way back up, rebuilding the nodes an
// account's chain shares once per transaction.
//
// This is the shape of the reference: block::Account::transactions is a plain
// vector appended to by push_transaction while the account executes, and
// Account::create_account_block (crypto/block/transaction.cpp:4176) turns it
// into the dictionary once, from Collator::combine_account_transactions
// (validator/impl/collator.cpp:3020). A repeated logical time is refused there
// at build time as well, not where the transaction was pushed.
func buildAccountTransactions(lane *accountLane) (*tlb.AccountTransactionsAugDict, error) {
	// Sorted so the duplicate check below can name the offending lt; SetMany
	// orders the batch itself and does not require it.
	slices.SortFunc(lane.transactions, func(left, right accountTransaction) int {
		return cmp.Compare(left.startLT, right.startLT)
	})
	entries := make([]cell.AugmentedUintEntry, len(lane.transactions))
	for i, transaction := range lane.transactions {
		if i > 0 && transaction.startLT == lane.transactions[i-1].startLT {
			return nil, fmt.Errorf("%w: duplicate transaction lt %d", ErrInvalidInput, transaction.startLT)
		}
		entries[i] = cell.AugmentedUintEntry{
			Key:   transaction.startLT,
			Value: cell.BeginCell().MustStoreRef(transaction.root).EndCell(),
			Mode:  cell.DictSetModeAdd,
		}
	}

	transactions, err := tlb.NewAccountTransactionsAugDict()
	if err != nil {
		return nil, err
	}
	if err = transactions.SetManyByUint(entries); err != nil {
		return nil, fmt.Errorf("%w: transactions of account %x did not apply: %v", ErrInvalidInput, lane.key, err)
	}
	return transactions, nil
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
// message. When certifyParallel is set, the same descent also verifies that no
// cell can carry a trace or another shape unsafe to inspect from a generated
// worker.
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
// It runs on the goroutine that retires a transaction, once per emitted message,
// so both of its costs are paid serially in the middle of a phase: the visited
// set is scratch reused across messages rather than allocated per message, and
// the descent starts from a detached root.
//
// Detached because PeekRef on a traced cell copies the cell and creates a trace
// node for every reference it hands back (tvm/cell/cell.go, PeekRef), and this
// walk wants none of it. What it records, it records explicitly through
// recordExecutionRead; propagating the lane's trace down an emitted message
// would instead record the same cells a second way — billed, through the
// traversal record — which is exactly what recording them unbilled is avoiding.
// Only the root is copied; PeekRef on an untraced cell returns the reference
// itself.
//
// Recording and certification deliberately keep different visited sets. The
// proof record deduplicates by hash, while certification deduplicates by
// pointer: equal-hash wrappers may carry different traces, and skipping the
// traced wrapper would let a worker write into a serial lane recorder. The two
// recursion flags preserve both rules while sharing one PeekRef walk.
func (c *collation) recordOutboundMessageReads(root *cell.Cell, certifyParallel bool) bool {
	if root == nil {
		return false
	}
	// Cleared per message, though recording is idempotent and a stale set would
	// mostly just skip cells already in the record. The one place it would not
	// is the depth bound: a subtree truncated at maxOutboundMessageRecordDepth
	// in one message and reachable shallowly in the next would stay truncated.
	// Clearing a map of tens of entries is cheaper than reasoning about that.
	if c.outboundVisited == nil {
		c.outboundVisited = make(map[cell.Hash]struct{}, 64)
	} else {
		clear(c.outboundVisited)
	}
	if certifyParallel {
		if c.outboundSafetyVisited == nil {
			c.outboundSafetyVisited = make(map[*cell.Cell]struct{}, 64)
		} else {
			clear(c.outboundSafetyVisited)
		}
	}

	parallelSafe := certifyParallel && root.Trace() == nil
	return c.walkOutboundMessage(root.WithoutTrace(), 0, true, parallelSafe) && parallelSafe
}

func (c *collation) walkOutboundMessage(cur *cell.Cell, depth int, record, certify bool) bool {
	if cur == nil || (!record && !certify) {
		return true
	}
	if depth > maxOutboundMessageRecordDepth {
		return !certify
	}

	recordChildren := false
	if record {
		hash := cur.HashKey()
		if _, seen := c.outboundVisited[hash]; !seen {
			c.outboundVisited[hash] = struct{}{}
			c.recordExecutionRead(cur)
			recordChildren = !cur.IsSpecial()
		}
	}

	certifyChildren := false
	parallelSafe := true
	if certify {
		unsafe := cur.Trace() != nil || cur.IsSpecial() || cur.IsLazy() ||
			cur.IsVirtualized() || cur.Level() != 0
		if unsafe {
			parallelSafe = false
		} else if _, seen := c.outboundSafetyVisited[cur]; !seen {
			c.outboundSafetyVisited[cur] = struct{}{}
			certifyChildren = true
		}
	}
	if !recordChildren && !certifyChildren {
		return parallelSafe
	}

	for i := 0; i < int(cur.RefsNum()); i++ {
		ref, err := cur.PeekRef(i)
		if err != nil {
			if certifyChildren {
				parallelSafe = false
			}
			return parallelSafe
		}
		if !c.walkOutboundMessage(ref, depth+1, recordChildren, certifyChildren) {
			parallelSafe = false
			certifyChildren = false
		}
	}
	return parallelSafe
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
