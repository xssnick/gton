package collator

import (
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func (v *semanticQueueValidation) verifyOutQueueChange(key msgpool.QueueKey, entry *semanticQueueEntry, added bool) error {
	hash := entry.envelope.message.HashKey()
	descriptor := v.out[hash]
	if descriptor == nil {
		return fmt.Errorf("%w: outbound queue change %x has no message descriptor", ErrInvalidInput, key)
	}

	envelope := descriptor.envelope
	if added {
		switch descriptor.tag {
		case semanticOutNew, semanticOutTransit, semanticOutTransitRequest, semanticOutDeferredTransit:
		default:
			return fmt.Errorf("%w: outbound descriptor %x does not enqueue a message", ErrInvalidInput, hash)
		}
	} else {
		switch descriptor.tag {
		case semanticOutDequeue, semanticOutDequeueImmediate:
		case semanticOutDequeueShort:
			if descriptor.next != entry.envelope.next || cell.Hash(descriptor.envelopeHash) != entry.envelope.root.HashKey() {
				return fmt.Errorf("%w: short dequeue descriptor %x differs from outbound queue entry", ErrInvalidInput, hash)
			}
			return nil
		case semanticOutTransitRequest:
			inbound := v.in[hash]
			if inbound == nil || inbound.tag != semanticInTransit {
				return fmt.Errorf("%w: transit-request descriptor %x has no transit reimport", ErrInvalidInput, hash)
			}
			envelope = inbound.envelope
		default:
			return fmt.Errorf("%w: outbound descriptor %x does not dequeue a message", ErrInvalidInput, hash)
		}
	}
	if !equalCell(envelope.root, entry.envelope.root) {
		return fmt.Errorf("%w: outbound queue change %x contains another envelope", ErrInvalidInput, key)
	}

	return nil
}

func (v *semanticQueueValidation) verifyOutQueueChanges() error {
	for _, entry := range v.outOrder {
		hash, descriptor := entry.hash, entry.descriptor
		if err := v.replay.ctx.Err(); err != nil {
			return err
		}
		if descriptor.envelope != nil {
			if err := v.verifyOutboundEnvelope(hash, descriptor); err != nil {
				return err
			}
		}
		switch descriptor.tag {
		case semanticOutExternal:
			continue
		case semanticOutImmediate:
			if err := v.verifyReimport(hash, descriptor, semanticInImmediate); err != nil {
				return err
			}
			if err := v.requireQueueAbsent(descriptor.envelope); err != nil {
				return fmt.Errorf("immediate outbound message %x: %w", hash, err)
			}
		case semanticOutNew:
			if err := v.verifyEnqueued(descriptor.envelope); err != nil {
				return fmt.Errorf("new outbound message %x: %w", hash, err)
			}
		case semanticOutTransit:
			if err := v.verifyReimport(hash, descriptor, semanticInTransit); err != nil {
				return err
			}
			if err := v.verifyTransitRewrite(hash, descriptor, semanticInTransit); err != nil {
				return err
			}
			if err := v.verifyEnqueued(descriptor.envelope); err != nil {
				return fmt.Errorf("transit outbound message %x: %w", hash, err)
			}
		case semanticOutDequeue:
			entry, err := v.verifyDequeued(descriptor.envelope.next, hash, descriptor.envelope.root.HashKey())
			if err != nil {
				return fmt.Errorf("dequeue outbound message %x: %w", hash, err)
			}
			if err = v.verifyDelivered(entry, descriptor.importBlockLT); err != nil {
				return err
			}
		case semanticOutDequeueShort:
			entry, err := v.verifyDequeued(descriptor.next, hash, cell.Hash(descriptor.envelopeHash))
			if err != nil {
				return fmt.Errorf("short dequeue outbound message %x: %w", hash, err)
			}
			if err = v.verifyDelivered(entry, descriptor.importBlockLT); err != nil {
				return err
			}
		case semanticOutTransitRequest:
			if err := v.verifyReimport(hash, descriptor, semanticInTransit); err != nil {
				return err
			}
			inbound := v.in[cell.Hash(hash)]
			if _, err := v.verifyDequeued(inbound.envelope.next, hash, inbound.envelope.root.HashKey()); err != nil {
				return fmt.Errorf("transit-request dequeue %x: %w", hash, err)
			}
			if err := v.verifyTransitRewrite(hash, descriptor, semanticInTransit); err != nil {
				return err
			}
			if err := v.verifyEnqueued(descriptor.envelope); err != nil {
				return fmt.Errorf("transit-request enqueue %x: %w", hash, err)
			}
		case semanticOutDequeueImmediate:
			if err := v.verifyReimport(hash, descriptor, semanticInFinal); err != nil {
				return err
			}
			if _, err := v.verifyDequeued(descriptor.envelope.next, hash, descriptor.envelope.root.HashKey()); err != nil {
				return fmt.Errorf("immediate dequeue %x: %w", hash, err)
			}
		case semanticOutNewDeferred:
			// No outbound queue lookup here, deliberately, and it is not an
			// omission: the reference validator cannot perform this one either.
			//
			// ValidateQuery::check_out_msg builds its queue key from
			// next_prefix (validate-query.cpp:4786-4791), but for
			// msg_export_new_defer it never computes next_prefix — the
			// interpolation lives in the else of the deferred branch
			// (:4699-4706), because a message in the DispatchQueue is required
			// to carry zero cur_addr/next_addr. next_prefix therefore keeps
			// AccountIdPrefixFull's default, workchainInvalid (0x80000000) with
			// a zero account prefix (ton-types.h:151), so its "shouldn't exist
			// in the old and the new message queues" check (:4793-4797) always
			// looks up a key no real queue entry can have and always passes.
			//
			// The consequence is what matters to us: a reference collator never
			// walks the real key's path, so it is not in the collated proof,
			// and demanding it rejects blocks every other node accepts. The
			// property itself is not lost: precheckOutQueueUpdate rejects an
			// enqueued entry whose descriptor is deferred.
			continue
		case semanticOutDeferredTransit:
			if err := v.verifyReimport(hash, descriptor, semanticInDeferredTransit); err != nil {
				return err
			}
			if err := v.verifyTransitRewrite(hash, descriptor, semanticInDeferredTransit); err != nil {
				return err
			}
			if err := v.verifyEnqueued(descriptor.envelope); err != nil {
				return fmt.Errorf("deferred transit message %x: %w", hash, err)
			}
		default:
			return fmt.Errorf("%w: unsupported outbound descriptor %x tag %d", ErrUnsupported, hash, descriptor.tag)
		}
	}

	return nil
}

func (v *semanticQueueValidation) verifyOutboundEnvelope(
	hash cell.Hash,
	descriptor *semanticOutDescriptor,
) error {
	envelope := descriptor.envelope
	// A deferred message never entered the queue, so it has zeroed hops and none
	// of the routing rules below apply to it. applyDispatchChanges runs first and
	// has already rejected dispatch routing and a foreign source, with a stricter
	// source check than this function could make.
	if descriptor.tag == semanticOutNewDeferred {
		return nil
	}

	if !v.target.ContainsPrefix(envelope.current) {
		return fmt.Errorf("%w: outbound message %x current hop is outside this shard", ErrInvalidInput, hash)
	}
	if semanticRoutingCommonBits(envelope.destination, envelope.next) <
		semanticRoutingCommonBits(envelope.destination, envelope.current) {
		return fmt.Errorf("%w: outbound message %x moves away from its destination", ErrInvalidInput, hash)
	}
	if envelope.current == envelope.next && envelope.current != envelope.destination {
		return fmt.Errorf("%w: outbound message %x does not advance toward its destination", ErrInvalidInput, hash)
	}
	if descriptor.transaction != nil && !v.target.ContainsPrefix(envelope.source) {
		return fmt.Errorf("%w: outbound message %x transaction source is outside this shard", ErrInvalidInput, hash)
	}

	switch descriptor.tag {
	case semanticOutImmediate:
		if envelope.value.EmittedLT != nil {
			return fmt.Errorf("%w: immediate outbound message %x has a custom emitted lt", ErrInvalidInput, hash)
		}
		if !v.target.ContainsPrefix(envelope.destination) ||
			envelope.current != envelope.destination || envelope.next != envelope.destination {
			return fmt.Errorf("%w: immediate outbound message %x is not routed to its destination", ErrInvalidInput, hash)
		}
		// The interpolated prefixes above can coincide with the destination for
		// more than one raw hop width when source and destination share tail
		// bits, so validate-query.cpp:4904 additionally pins the raw route to
		// exactly (96,96) and rejects every other encoding as non-canonical.
		if envelope.value.CurAddr.UseDestBits != routingAddressBits ||
			envelope.value.NextAddr.UseDestBits != routingAddressBits {
			return fmt.Errorf("%w: immediate outbound message %x has a non-canonical raw route", ErrInvalidInput, hash)
		}
	case semanticOutNew:
		if envelope.value.EmittedLT != nil {
			return fmt.Errorf("%w: new outbound message %x has a custom emitted lt", ErrInvalidInput, hash)
		}
		if err := v.verifyNewMessageRoute(hash, envelope); err != nil {
			return err
		}
	case semanticOutDeferredTransit:
		if envelope.value.EmittedLT == nil {
			return fmt.Errorf("%w: deferred transit message %x has no emitted lt", ErrInvalidInput, hash)
		}
		if emitted := *envelope.value.EmittedLT; emitted < v.replay.candidate.block.BlockInfo.StartLt ||
			emitted >= v.replay.candidate.block.BlockInfo.EndLt {
			return fmt.Errorf("%w: deferred transit message %x emitted lt is outside the block", ErrInvalidInput, hash)
		}
		if err := v.verifyNewMessageRoute(hash, envelope); err != nil {
			return err
		}
	}

	return nil
}

func (v *semanticQueueValidation) verifyNewMessageRoute(hash cell.Hash, envelope *semanticEnvelope) error {
	currentBits, nextBits, err := performHypercubeRouting(envelope.source, envelope.destination, v.target, 0)
	if err != nil {
		return fmt.Errorf("%w: route outbound message %x: %v", ErrInvalidInput, hash, err)
	}
	expectedCurrent := msgpool.InterpolatePrefix(envelope.source, envelope.destination, currentBits)
	expectedNext := msgpool.InterpolatePrefix(envelope.source, envelope.destination, nextBits)
	if envelope.current != expectedCurrent || envelope.next != expectedNext {
		return fmt.Errorf("%w: outbound message %x does not follow hypercube routing", ErrInvalidInput, hash)
	}
	// Prefix equality alone leaves room for a second encoding: interpolation
	// stops distinguishing hop widths once source and destination agree on the
	// bits in between. validate-query.cpp:4929 therefore also requires the raw
	// route to be the very pair hypercube routing returned.
	if int(envelope.value.CurAddr.UseDestBits) != currentBits ||
		int(envelope.value.NextAddr.UseDestBits) != nextBits {
		return fmt.Errorf("%w: outbound message %x has a non-canonical raw route", ErrInvalidInput, hash)
	}

	return nil
}

func (v *semanticQueueValidation) verifyReimport(
	hash cell.Hash,
	descriptor *semanticOutDescriptor,
	wantTag uint8,
) error {
	inbound := v.in[hash]
	if inbound == nil || inbound.tag != wantTag || descriptor.reimport == nil ||
		!equalCell(descriptor.reimport, inbound.root) {
		return fmt.Errorf("%w: outbound message %x has an invalid reimport descriptor", ErrInvalidInput, hash)
	}
	if wantTag == semanticInImmediate || wantTag == semanticInFinal {
		if !equalCell(descriptor.envelope.root, inbound.envelope.root) {
			return fmt.Errorf("%w: outbound message %x reimports another envelope", ErrInvalidInput, hash)
		}
	}

	return nil
}

func (v *semanticQueueValidation) verifyTransitRewrite(
	hash cell.Hash,
	descriptor *semanticOutDescriptor,
	wantTag uint8,
) error {
	inbound := v.in[hash]
	if inbound == nil || inbound.tag != wantTag || inbound.outEnvelope == nil ||
		!equalCell(inbound.outEnvelope.root, descriptor.envelope.root) {
		return fmt.Errorf("%w: transit message %x rewritten envelope mismatch", ErrInvalidInput, hash)
	}
	currentBits, nextBits, err := performHypercubeRouting(
		inbound.envelope.next,
		inbound.envelope.destination,
		v.target,
		0,
	)
	if err != nil {
		return fmt.Errorf("%w: route transit message %x: %v", ErrInvalidInput, hash, err)
	}
	expectedCurrent := msgpool.InterpolatePrefix(inbound.envelope.next, inbound.envelope.destination, currentBits)
	expectedNext := msgpool.InterpolatePrefix(inbound.envelope.next, inbound.envelope.destination, nextBits)
	if descriptor.envelope.current != expectedCurrent || descriptor.envelope.next != expectedNext {
		return fmt.Errorf("%w: transit message %x does not follow hypercube routing", ErrInvalidInput, hash)
	}
	// Same non-canonical encoding hazard as on the new-message path: the
	// rewritten envelope must carry the raw hop widths hypercube routing
	// returned, not merely a pair interpolating to them
	// (validate-query.cpp:4386).
	if int(descriptor.envelope.value.CurAddr.UseDestBits) != currentBits ||
		int(descriptor.envelope.value.NextAddr.UseDestBits) != nextBits {
		return fmt.Errorf("%w: transit message %x has a non-canonical raw route", ErrInvalidInput, hash)
	}
	if !semanticMetadataEqual(inbound.envelope.value.Metadata, descriptor.envelope.value.Metadata) {
		return fmt.Errorf("%w: transit message %x changes metadata", ErrInvalidInput, hash)
	}
	if !semanticOptionalLTEqual(inbound.envelope.value.EmittedLT, descriptor.envelope.value.EmittedLT) {
		return fmt.Errorf("%w: transit message %x changes emitted lt", ErrInvalidInput, hash)
	}

	incomingFee := inbound.envelope.value.FwdFeeRemaining.Nano()
	expectedTransitFee := new(big.Int)
	if wantTag == semanticInTransit {
		expectedTransitFee = standardTransitFee(v.replay.transition.Config, incomingFee)
	}
	if wantTag == semanticInTransit && inbound.fee.Nano().Cmp(expectedTransitFee) != 0 {
		return fmt.Errorf("%w: transit message %x collected forwarding fee mismatch", ErrInvalidInput, hash)
	}
	expectedRemaining := new(big.Int).Sub(new(big.Int).Set(incomingFee), expectedTransitFee)
	if expectedRemaining.Sign() < 0 ||
		expectedRemaining.Cmp(descriptor.envelope.value.FwdFeeRemaining.Nano()) != 0 {
		return fmt.Errorf("%w: transit message %x forwarding fee remainder mismatch", ErrInvalidInput, hash)
	}

	return nil
}

func semanticOptionalLTEqual(left, right *uint64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func (v *semanticQueueValidation) verifyEnqueued(envelope *semanticEnvelope) error {
	key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
	var value, extra cell.Slice
	if err := v.candidate.OutQueue.LoadValueExtraByBytesKeyInto(key[:], &value, &extra); err != nil {
		return fmt.Errorf("%w: outbound queue has no new message leaf", ErrInvalidInput)
	}
	entry, err := parseSemanticQueueEntryKeyWithMode(key, &value, &extra, false, semanticQueueLeafCells{}, &v.replay.envelopes)
	if err != nil {
		return err
	}
	if !equalCell(entry.envelope.root, envelope.root) {
		return fmt.Errorf("%w: new outbound queue leaf contains another envelope", ErrInvalidInput)
	}
	header := &v.replay.candidate.block.BlockInfo
	if entry.enqueued.EnqueuedLT < header.StartLt || entry.enqueued.EnqueuedLT >= header.EndLt {
		return fmt.Errorf("%w: new outbound queue enqueued lt is outside the block", ErrInvalidInput)
	}
	// The block lt window still admits an entry stamped with the block start
	// while its message was emitted by a later transaction, so
	// precheck_one_message_queue_update pairs it with emitted_lt <= enqueued_lt
	// (validate-query.cpp:3533). entry.envelope is the descriptor envelope
	// compared above, so its bound carries the same emitted lt the reference
	// reads back from the queue leaf: the explicit envelope one when present,
	// otherwise the message created lt.
	if entry.enqueued.EnqueuedLT < entry.envelope.bound.lt {
		return fmt.Errorf("%w: new outbound queue enqueued lt is below the message emitted lt", ErrInvalidInput)
	}
	if err = v.old.OutQueue.LoadValueByBytesKeyInto(key[:], &value); err == nil {
		return fmt.Errorf("%w: outbound queue key already exists", ErrInvalidInput)
	} else if !isMissingKey(err) {
		return err
	}
	if v.queueSize == maxOutMsgQueueSize {
		return fmt.Errorf("%w: outbound queue size overflow", ErrInvalidInput)
	}
	v.queueSize++
	if v.target.ContainsPrefix(envelope.next) {
		if !v.hasMinimumQueued || envelope.bound.less(v.minimumEnqueued) {
			v.minimumEnqueued = envelope.bound
			v.hasMinimumQueued = true
		}
	}

	return nil
}

func (v *semanticQueueValidation) verifyDequeued(
	next msgpool.AccountPrefix,
	hash cell.Hash,
	envelopeHash cell.Hash,
) (semanticQueueEntry, error) {
	key := msgpool.MakeQueueKey(next, hash)
	var value cell.Slice
	if err := v.old.OutQueue.LoadValueByBytesKeyInto(key[:], &value); err != nil {
		return semanticQueueEntry{}, fmt.Errorf("%w: outbound queue entry is absent", ErrInvalidInput)
	}
	entry, err := parseSemanticQueueEntryKeyWithMode(key, &value, nil, false, semanticQueueLeafCells{}, &v.replay.envelopes)
	if err != nil {
		return semanticQueueEntry{}, err
	}
	if entry.enqueued.EnqueuedLT >= v.replay.candidate.block.BlockInfo.StartLt {
		return semanticQueueEntry{}, fmt.Errorf("%w: predecessor outbound queue enqueued lt is not before the block", ErrInvalidInput)
	}
	if entry.enqueued.Msg.HashKey() != envelopeHash {
		return semanticQueueEntry{}, fmt.Errorf("%w: dequeued envelope differs from descriptor", ErrInvalidInput)
	}
	if err = v.candidate.OutQueue.LoadValueByBytesKeyInto(key[:], &value); err == nil {
		return semanticQueueEntry{}, fmt.Errorf("%w: dequeued message remains in candidate outbound queue", ErrInvalidInput)
	} else if !isMissingKey(err) {
		return semanticQueueEntry{}, err
	}
	if v.queueSize == 0 {
		return semanticQueueEntry{}, fmt.Errorf("%w: outbound queue size underflow", ErrInvalidInput)
	}
	v.queueSize--

	return entry, nil
}

func (v *semanticQueueValidation) requireQueueAbsent(envelope *semanticEnvelope) error {
	key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
	var value cell.Slice
	if err := v.old.OutQueue.LoadValueByBytesKeyInto(key[:], &value); err == nil {
		return fmt.Errorf("%w: message exists in predecessor outbound queue", ErrInvalidInput)
	} else if !isMissingKey(err) {
		return err
	}
	if err := v.candidate.OutQueue.LoadValueByBytesKeyInto(key[:], &value); err == nil {
		return fmt.Errorf("%w: message exists in candidate outbound queue", ErrInvalidInput)
	} else if !isMissingKey(err) {
		return err
	}

	return nil
}

func (v *semanticQueueValidation) verifyDelivered(entry semanticQueueEntry, importBlockLT uint64) error {
	for i := range v.sources {
		source := &v.sources[i]
		// AlreadyProcessed includes the collection-owner prefix check. Calling
		// every source in order mirrors ValidateQuery and makes the first true
		// frontier authoritative for import_block_lt.
		processed, err := v.shardEndLT.alreadyProcessed(
			source.neighbor.Processed,
			source.owner.Workchain,
			source.owner.Shard,
			&entry.descr,
		)
		if err != nil {
			return fmt.Errorf("%w: check dequeue delivery: %v", ErrInvalidInput, err)
		}
		if processed {
			if importBlockLT != source.neighbor.EndLT {
				return fmt.Errorf("%w: dequeue import block lt differs from neighbor", ErrInvalidInput)
			}
			return nil
		}
	}

	return fmt.Errorf("%w: dequeued message was not processed by a neighbor", ErrInvalidInput)
}
