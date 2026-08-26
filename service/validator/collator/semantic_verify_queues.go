package collator

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

const (
	semanticInExternal = 0
	// Deferred constructors are normalized to the non-wire enum values 8/9,
	// keeping one tag space; their actual wire prefixes are 00100/00101.
	semanticInDeferredFinal   = 8
	semanticInDeferredTransit = 9
	semanticInIHR             = 2
	semanticInImmediate       = 3
	semanticInFinal           = 4
	semanticInTransit         = 5
	semanticInDiscardFinal    = 6
	semanticInDiscardTransit  = 7

	semanticOutExternal         = 0
	semanticOutNew              = 1
	semanticOutImmediate        = 2
	semanticOutTransit          = 3
	semanticOutDequeueImmediate = 4
	semanticOutNewDeferred      = 20
	semanticOutDeferredTransit  = 21
	semanticOutDequeue          = 12
	semanticOutDequeueShort     = 13
	semanticOutTransitRequest   = 7
)

type semanticMessageBound struct {
	lt   uint64
	hash [32]byte
}

func (b semanticMessageBound) less(other semanticMessageBound) bool {
	return b.lt < other.lt || b.lt == other.lt && bytes.Compare(b.hash[:], other.hash[:]) < 0
}

func (b semanticMessageBound) lessEqual(other semanticMessageBound) bool {
	return !other.less(b)
}

type semanticEnvelope struct {
	root        *cell.Cell
	value       tlb.MsgEnvelope
	message     *cell.Cell
	internal    tlb.InternalMessage
	source      msgpool.AccountPrefix
	destination msgpool.AccountPrefix
	current     msgpool.AccountPrefix
	next        msgpool.AccountPrefix
	bound       semanticMessageBound
}

// semanticInternalMessageInfo is the CommonMsgInfo prefix consumed by C++
// EnqueuedMsgDescr. Neighbor queue validation deliberately stops before
// StateInit and body: those cells are authenticated by their hashes but are
// not part of the neighbor queue proof's read set.
type semanticInternalMessageInfo struct {
	_               tlb.Magic        `tlb:"$0"`
	IHRDisabled     bool             `tlb:"bool"`
	Bounce          bool             `tlb:"bool"`
	Bounced         bool             `tlb:"bool"`
	SrcAddr         *address.Address `tlb:"addr"`
	DstAddr         *address.Address `tlb:"addr"`
	Amount          tlb.Coins        `tlb:"."`
	ExtraCurrencies *cell.Dictionary `tlb:"dict 32"`
	IHRFee          tlb.Coins        `tlb:"."`
	FwdFee          tlb.Coins        `tlb:"."`
	CreatedLT       uint64           `tlb:"## 64"`
	CreatedAt       uint32           `tlb:"## 32"`
}

type semanticInDescriptor struct {
	tag         uint8
	root        *cell.Cell
	message     *cell.Cell
	envelope    *semanticEnvelope
	transaction *cell.Cell
	outEnvelope *semanticEnvelope
	fee         tlb.Coins
}

type semanticOutDescriptor struct {
	tag           uint8
	root          *cell.Cell
	message       *cell.Cell
	envelope      *semanticEnvelope
	transaction   *cell.Cell
	reimport      *cell.Cell
	next          msgpool.AccountPrefix
	envelopeHash  [32]byte
	importBlockLT uint64
}

type semanticQueueSource struct {
	neighbor *Neighbor
	owner    msgpool.ShardIdent
	queue    tlb.OutMsgQueueInfo
}

type semanticQueueEntry struct {
	enqueued tlb.EnqueuedMsg
	envelope semanticEnvelope
	descr    tlb.ProcessedMsgDescr
}

type semanticInDescriptorEntry struct {
	hash       cell.Hash
	descriptor *semanticInDescriptor
}

type semanticOutDescriptorEntry struct {
	hash       cell.Hash
	descriptor *semanticOutDescriptor
}

type semanticQueueValidation struct {
	replay *semanticReplay
	target msgpool.ShardIdent

	old       tlb.OutMsgQueueInfo
	candidate *tlb.OutMsgQueueInfo
	outQueue  *tlb.OutMsgQueueAugDict
	dispatch  *tlb.DispatchQueueAugDict
	queueSize uint64

	shardEndLT *shardEndLTResolver

	in  map[cell.Hash]*semanticInDescriptor
	out map[cell.Hash]*semanticOutDescriptor
	// Descriptor keys in ascending dictionary order. Every loop that names a
	// message hash in its rejection message walks these rather than the maps, so
	// two validators rejecting the same block report the same reason -- the same
	// property mergeAccountLanes goes serial to get. The maps stay for the point
	// lookups in verifyReimport and verifyTransitRewrite.
	inOrder  []semanticInDescriptorEntry
	outOrder []semanticOutDescriptorEntry

	processedMax     semanticMessageBound
	minimumEnqueued  semanticMessageBound
	hasProcessed     bool
	hasMinimumQueued bool
	matchedImports   map[cell.Hash]struct{}
	sources          []semanticQueueSource
	oldRecords       []tlb.ProcessedUptoRecord
}

func (r *semanticReplay) prepareQueueValidation() (*semanticQueueValidation, error) {
	validation, err := newSemanticQueueValidation(r)
	if err != nil {
		return nil, err
	}
	if err = validation.precheckOutQueueUpdate(); err != nil {
		return nil, err
	}
	if err = validation.precheckDispatchQueueUpdate(); err != nil {
		return nil, err
	}
	if err = validation.loadDescriptors(); err != nil {
		return nil, err
	}
	r.parsedIn = validation.in
	r.parsedOut = validation.out
	r.parsedInOrder = validation.inOrder
	r.parsedOutOrder = validation.outOrder

	return validation, nil
}

// precheckOutQueueUpdate mirrors ValidateQuery::precheck_message_queue_update:
// the reference validator performs a structural mode-2 diff before semantic
// descriptor replay and validates both changed EnqueuedMsg values. The final
// root comparison alone cannot detect a pruned cell required by that pass.
func (v *semanticQueueValidation) precheckOutQueueUpdate() error {
	err := v.old.OutQueue.ScanDiffRaw(v.candidate.OutQueue.AugmentedDictionary, true, func(view cell.AugDictDiffRawView) error {
		var key msgpool.QueueKey
		if view.KeyBits != 352 || len(view.Key) != len(key) {
			return fmt.Errorf("%w: outbound queue key is malformed", ErrInvalidInput)
		}
		copy(key[:], view.Key)

		for side := 0; side < 2; side++ {
			hasValue := view.HasOld
			valueExtra := view.OldValueExtra
			if side == 1 {
				hasValue = view.HasNew
				valueExtra = view.NewValueExtra
			}
			if !hasValue {
				continue
			}
			value := valueExtra
			if err := (tlb.AugOutMsgQueue{}).SkipExtra(&value); err != nil {
				return fmt.Errorf("%w: decode outbound queue augmentation: %v", ErrInvalidInput, err)
			}
			entry, err := parseSemanticQueueEntryKeyWithMode(key, &value, nil, false, semanticQueueLeafCells{}, &v.replay.envelopes)
			if err != nil {
				return err
			}
			// EnqueuedMsg's generated validator opens an indirect Anything body
			// root. Its descendants remain opaque.
			var body cell.Slice
			if err = entry.envelope.internal.Body.BeginParseInto(&body); err != nil {
				return fmt.Errorf("%w: open outbound queue message body: %v", ErrInvalidInput, err)
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrInvalidInput) {
			return err
		}
		return fmt.Errorf("%w: invalid OutMsgQueue dictionary difference: %v", ErrInvalidInput, err)
	}
	return nil
}

// precheckDispatchQueueUpdate mirrors the reference DispatchQueue mode-2 diff.
// The old maximum and new minimum are boundary reads performed after each
// changed account queue has been unpacked.
func (v *semanticQueueValidation) precheckDispatchQueueUpdate() error {
	candidate, err := copiedDispatchQueue(v.candidate)
	if err != nil {
		return fmt.Errorf("%w: decode candidate dispatch queue: %v", ErrInvalidInput, err)
	}
	err = v.dispatch.ScanDiffRaw(candidate.AugmentedDictionary, true, func(view cell.AugDictDiffRawView) error {
		if view.HasOld {
			oldAccount, parseErr := dispatchDiffAccount(view.OldValueExtra)
			if parseErr != nil {
				return fmt.Errorf("%w: decode predecessor dispatch account: %v", ErrInvalidInput, parseErr)
			}
			if _, _, parseErr = oldAccount.Messages.LoadMax(); parseErr != nil {
				return fmt.Errorf("%w: load predecessor dispatch account maximum: %v", ErrInvalidInput, parseErr)
			}
		}
		if view.HasNew {
			newAccount, parseErr := dispatchDiffAccount(view.NewValueExtra)
			if parseErr != nil {
				return fmt.Errorf("%w: decode candidate dispatch account: %v", ErrInvalidInput, parseErr)
			}
			if _, _, parseErr = newAccount.Messages.LoadMin(); parseErr != nil {
				return fmt.Errorf("%w: load candidate dispatch account minimum: %v", ErrInvalidInput, parseErr)
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrInvalidInput) {
			return err
		}
		return fmt.Errorf("%w: invalid DispatchQueue dictionary difference: %v", ErrInvalidInput, err)
	}
	return nil
}

func (v *semanticQueueValidation) verifyAfterReplay() error {
	if err := v.verifyDescriptorCoverage(); err != nil {
		return err
	}
	if err := v.applyDispatchChanges(); err != nil {
		return err
	}
	if err := v.applyOutQueueChanges(); err != nil {
		return err
	}
	if err := v.verifyInboundMessages(); err != nil {
		return err
	}
	if err := v.verifyQueueRoots(); err != nil {
		return err
	}
	if err := v.verifyProcessedInfo(); err != nil {
		return err
	}
	if v.replay.candidate.block.BlockInfo.AfterMerge {
		if err := v.verifyMergedQueueCleanup(); err != nil {
			return err
		}
	}

	return nil
}

func newSemanticQueueValidation(r *semanticReplay) (*semanticQueueValidation, error) {
	header := &r.candidate.block.BlockInfo
	target := msgpool.ShardIdent{
		Workchain: header.Shard.WorkchainID,
		Shard:     uint64(header.Shard.GetShardID()),
	}
	var old tlb.OutMsgQueueInfo
	if err := parseExact(&old, r.previous.OutMsgQueueInfo); err != nil {
		return nil, fmt.Errorf("%w: decode predecessor outbound queue: %v", ErrInvalidInput, err)
	}
	queueSize, err := semanticPredecessorQueueSize(r, &old)
	if err != nil {
		return nil, err
	}
	dispatch, err := copiedDispatchQueue(&old)
	if err != nil {
		return nil, fmt.Errorf("%w: copy predecessor dispatch queue: %v", ErrInvalidInput, err)
	}
	oldRecords, err := tlb.LoadProcessedUptoRecords(old.ProcInfo, target.Shard)
	if err != nil {
		return nil, fmt.Errorf("%w: decode predecessor processed info: %v", ErrInvalidInput, err)
	}

	validation := &semanticQueueValidation{
		replay:         r,
		target:         target,
		old:            old,
		candidate:      &r.candidate.queue,
		shardEndLT:     newShardEndLTResolver(r.transition.NeighborShardEndLT),
		outQueue:       &tlb.OutMsgQueueAugDict{AugmentedDictionary: old.OutQueue.Copy()},
		dispatch:       dispatch,
		queueSize:      queueSize,
		in:             make(map[cell.Hash]*semanticInDescriptor),
		out:            make(map[cell.Hash]*semanticOutDescriptor),
		matchedImports: make(map[cell.Hash]struct{}),
		oldRecords:     oldRecords,
	}
	if err = validation.loadQueueSources(); err != nil {
		return nil, err
	}

	return validation, nil
}

func semanticPredecessorQueueSize(r *semanticReplay, queue *tlb.OutMsgQueueInfo) (uint64, error) {
	header := &r.candidate.block.BlockInfo
	if !header.AfterSplit && !header.AfterMerge {
		return existingQueueSize(queue, r.transition.Previous.OutQueueSize)
	}
	if queue.Extra != nil && queue.Extra.OutQueueSize != nil {
		return *queue.Extra.OutQueueSize, nil
	}
	count, err := queue.OutQueue.Count()
	if err != nil {
		return 0, fmt.Errorf("%w: count effective predecessor outbound queue: %v", ErrInvalidInput, err)
	}
	return uint64(count), nil
}

// parseSemanticNeighborQueueEntryLoaded parses cells the caller has already
// taken out of storage. Handing them in is what
// lets walkSemanticQueuePrefix raise a load failure from the loading step
// instead of from the parse; see materialiseSemanticQueueLeaf for why that
// distinction is load-bearing and not tidiness.
func parseSemanticNeighborQueueEntryLoaded(
	key msgpool.QueueKey,
	value *cell.Slice,
	extra *cell.Slice,
	leaf semanticQueueLeafCells,
) (semanticQueueEntry, error) {
	return parseSemanticQueueEntryKeyWithMode(key, value, extra, true, leaf, nil)
}

func parseSemanticQueueEntryKeyWithMode(
	key msgpool.QueueKey,
	value *cell.Slice,
	extra *cell.Slice,
	neighborProof bool,
	leaf semanticQueueLeafCells,
	envelopes *semanticEnvelopeCache,
) (semanticQueueEntry, error) {
	var enqueued tlb.EnqueuedMsg
	if err := loadExactSlice(&enqueued, value); err != nil {
		return semanticQueueEntry{}, fmt.Errorf("%w: decode outbound queue entry %x: %v", ErrInvalidInput, key, err)
	}
	// A caller that already materialised the entry's cells hands them in, and
	// they are the same cells by hash: the loader validates every lazy load
	// against the placeholder it replaces (cell.validateLoadedLazyRef).
	envelopeRoot := enqueued.Msg
	if leaf.envelope != nil {
		envelopeRoot = leaf.envelope
	}
	var envelope *semanticEnvelope
	var err error
	if neighborProof {
		envelope, err = parseSemanticNeighborEnvelopeLoaded(envelopeRoot, leaf.message)
	} else {
		envelope, err = envelopes.parse(envelopeRoot)
	}
	if err != nil {
		return semanticQueueEntry{}, fmt.Errorf("%w: decode outbound queue envelope %x: %v", ErrInvalidInput, key, err)
	}
	if msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey()) != key {
		return semanticQueueEntry{}, fmt.Errorf("%w: outbound queue entry %x key differs from envelope", ErrInvalidInput, key)
	}
	if extra != nil {
		canonicalLT, loadErr := extra.LoadUInt(64)
		if loadErr != nil || extra.BitsLeft() != 0 || extra.RefsNum() != 0 || canonicalLT != envelope.bound.lt {
			return semanticQueueEntry{}, fmt.Errorf("%w: outbound queue entry %x augmentation mismatch", ErrInvalidInput, key)
		}
	}

	return semanticQueueEntry{
		enqueued: enqueued,
		envelope: *envelope,
		descr: tlb.ProcessedMsgDescr{
			CurWorkchain:  envelope.current.Workchain,
			CurPrefix:     envelope.current.Prefix,
			NextWorkchain: envelope.next.Workchain,
			NextPrefix:    envelope.next.Prefix,
			LT:            envelope.bound.lt,
			EnqueuedLT:    enqueued.EnqueuedLT,
			Hash:          envelope.message.HashKey(),
		},
	}, nil
}

func loadSemanticQueueEntry(
	queue *tlb.OutMsgQueueAugDict,
	key msgpool.QueueKey,
	envelopes *semanticEnvelopeCache,
) (semanticQueueEntry, error) {
	return loadSemanticQueueEntryWithMode(queue, key, false, envelopes)
}

// loadSemanticQueueEntryFromNeighbor reads an entry out of a neighbour's queue,
// which is backed by that neighbour's proof rather than by a state we hold.
//
// It must read no deeper than the walk that produced the proof did. That walk is
// header-only — it stops after created_at, which is all the reference node's
// EnqueuedMsgDescr::unpack reads too — so everything below is a pruned boundary
// on this side. Parsing a boundary does not fail: its body begins with the 0x01
// type byte, so an optional field reads as absent and the decode quietly yields a
// zero value. Nothing consumes that value today, which is exactly why the
// mismatch has to be closed here rather than left to be discovered by the first
// rule that does.
func loadSemanticQueueEntryFromNeighbor(
	queue *tlb.OutMsgQueueAugDict,
	key msgpool.QueueKey,
) (semanticQueueEntry, error) {
	return loadSemanticQueueEntryWithMode(queue, key, true, nil)
}

func loadSemanticQueueEntryWithMode(
	queue *tlb.OutMsgQueueAugDict,
	key msgpool.QueueKey,
	neighborProof bool,
	envelopes *semanticEnvelopeCache,
) (semanticQueueEntry, error) {
	var value, extra cell.Slice
	if err := queue.LoadValueExtraByBytesKeyInto(key[:], &value, &extra); err != nil {
		return semanticQueueEntry{}, err
	}
	return parseSemanticQueueEntryKeyWithMode(
		key,
		&value,
		&extra,
		neighborProof,
		semanticQueueLeafCells{},
		envelopes,
	)
}

func (v *semanticQueueValidation) verifyQueueRoots() error {
	if !equalCell(v.outQueue.RootCell(), v.candidate.OutQueue.RootCell()) {
		return fmt.Errorf("%w: outbound queue delta differs from message descriptors", ErrInvalidInput)
	}
	candidateDispatch, err := copiedDispatchQueue(v.candidate)
	if err != nil {
		return fmt.Errorf("%w: decode candidate dispatch queue: %v", ErrInvalidInput, err)
	}
	if !equalCell(v.dispatch.RootCell(), candidateDispatch.RootCell()) {
		return fmt.Errorf("%w: dispatch queue delta differs from message descriptors", ErrInvalidInput)
	}
	if v.candidate.Extra != nil && v.candidate.Extra.OutQueueSize != nil &&
		*v.candidate.Extra.OutQueueSize != v.queueSize {
		return fmt.Errorf("%w: candidate outbound queue size differs from semantic delta", ErrInvalidInput)
	}

	return nil
}

func (v *semanticQueueValidation) verifyMergedQueueCleanup() error {
	iterator, err := v.old.OutQueue.IteratorExtra(false, false)
	if err != nil {
		return fmt.Errorf("%w: iterate merged predecessor queue: %v", ErrInvalidInput, err)
	}
	for iterator.Next() {
		if err = v.replay.ctx.Err(); err != nil {
			return err
		}
		item := iterator.View()
		var keyLoader cell.Slice
		item.Key.CopyInto(&keyLoader)
		var key msgpool.QueueKey
		keyErr := keyLoader.LoadSliceInto(key[:], 352)
		if keyErr != nil || keyLoader.BitsLeft() != 0 || keyLoader.RefsNum() != 0 {
			return fmt.Errorf("%w: outbound queue key is malformed", ErrInvalidInput)
		}
		entry, parseErr := parseSemanticQueueEntryKeyWithMode(
			key,
			&item.Value,
			&item.Extra,
			false,
			semanticQueueLeafCells{},
			&v.replay.envelopes,
		)
		if parseErr != nil {
			return parseErr
		}
		delivered := false
		for j := range v.sources {
			source := &v.sources[j]
			delivered, err = v.shardEndLT.alreadyProcessed(
				source.neighbor.Processed,
				source.owner.Workchain,
				source.owner.Shard,
				&entry.descr,
			)
			if err != nil {
				return fmt.Errorf("%w: check merged queue delivery: %v", ErrInvalidInput, err)
			}
			if delivered {
				break
			}
		}
		if !delivered {
			continue
		}
		var value cell.Slice
		if err = v.candidate.OutQueue.LoadValueBySliceKeyInto(&item.Key, &value); err == nil {
			return fmt.Errorf("%w: delivered message %x remains after merge", ErrInvalidInput, entry.descr.Hash)
		} else if !isMissingKey(err) {
			return err
		}
		outbound := v.out[cell.Hash(entry.descr.Hash)]
		if outbound == nil ||
			(outbound.tag != semanticOutDequeue && outbound.tag != semanticOutDequeueShort) {
			return fmt.Errorf("%w: delivered merged message %x has no dequeue descriptor", ErrInvalidInput, entry.descr.Hash)
		}
	}
	if err = iterator.Err(); err != nil {
		return fmt.Errorf("%w: iterate merged predecessor queue: %v", ErrInvalidInput, err)
	}

	return nil
}
