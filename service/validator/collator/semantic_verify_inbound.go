package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func (v *semanticQueueValidation) verifyInboundMessages() error {
	for _, entry := range v.inOrder {
		hash, descriptor := entry.hash, entry.descriptor
		if err := v.replay.ctx.Err(); err != nil {
			return err
		}
		// Every tag below carries an envelope; only the external constructor does
		// not, and parseSemanticInDescriptor admits no other tag. This continue is
		// therefore the sole thing keeping verifyInboundEnvelope off a nil
		// envelope, and the precondition it relies on lives in that parser.
		if descriptor.tag == semanticInExternal {
			continue
		}
		if err := v.verifyInboundEnvelope(hash, descriptor); err != nil {
			return err
		}

		switch descriptor.tag {
		case semanticInImmediate:
			outbound := v.out[hash]
			if outbound == nil {
				if !v.isMasterSpecial(descriptor.root) {
					return fmt.Errorf("%w: immediate inbound message %x has no outbound pair", ErrInvalidInput, hash)
				}
			} else if outbound.tag != semanticOutImmediate {
				return fmt.Errorf("%w: immediate inbound message %x has wrong outbound pair", ErrInvalidInput, hash)
			}
			if !v.isMasterSpecial(descriptor.root) {
				v.recordProcessed(descriptor.envelope.bound)
			}
		case semanticInFinal, semanticInTransit:
			entry, local, err := v.findImportedMessage(descriptor.envelope)
			if err != nil {
				return fmt.Errorf("inbound message %x: %w", hash, err)
			}
			outbound := v.out[hash]
			if descriptor.tag == semanticInFinal {
				if local {
					if outbound == nil || outbound.tag != semanticOutDequeueImmediate {
						return fmt.Errorf("%w: local final inbound message %x has no dequeue pair", ErrInvalidInput, hash)
					}
				} else if outbound != nil {
					return fmt.Errorf("%w: neighbor final inbound message %x has an outbound pair", ErrInvalidInput, hash)
				}
			} else {
				want := uint8(semanticOutTransit)
				if local {
					want = semanticOutTransitRequest
				}
				if outbound == nil || outbound.tag != want {
					return fmt.Errorf("%w: transit inbound message %x has wrong outbound pair", ErrInvalidInput, hash)
				}
			}
			// Only a genuine import from a neighbor moves the processed bound.
			// C++ advances proc_lt_/proc_hash_ solely inside check_imported_message
			// (validate-query.cpp:3884), and the local branches of msg_import_fin
			// and of the tr_req variant of msg_import_tr never call it: a message
			// this shard re-dequeues from its own predecessor queue after a merge
			// was not imported from anywhere (validate-query.cpp:4256, :4317).
			if !local {
				v.recordProcessed(entry.envelope.bound)
			}
		case semanticInDeferredFinal:
			if v.out[hash] != nil {
				return fmt.Errorf("%w: deferred final inbound message %x has an outbound pair", ErrInvalidInput, hash)
			}
			v.recordProcessed(descriptor.envelope.bound)
		case semanticInDeferredTransit:
			outbound := v.out[hash]
			if outbound == nil || outbound.tag != semanticOutDeferredTransit {
				return fmt.Errorf("%w: deferred transit inbound message %x has wrong outbound pair", ErrInvalidInput, hash)
			}
		default:
			return fmt.Errorf("%w: unsupported inbound descriptor %x tag %d", ErrUnsupported, hash, descriptor.tag)
		}
	}

	return nil
}

func (v *semanticQueueValidation) verifyInboundEnvelope(
	hash cell.Hash,
	descriptor *semanticInDescriptor,
) error {
	envelope := descriptor.envelope
	if !v.target.ContainsPrefix(envelope.next) {
		return fmt.Errorf("%w: inbound message %x next hop is outside this shard", ErrInvalidInput, hash)
	}
	if semanticRoutingCommonBits(envelope.destination, envelope.next) <
		semanticRoutingCommonBits(envelope.destination, envelope.current) {
		return fmt.Errorf("%w: inbound message %x moves away from its destination", ErrInvalidInput, hash)
	}
	if descriptor.tag == semanticInDeferredFinal || descriptor.tag == semanticInDeferredTransit {
		if envelope.current != envelope.next {
			return fmt.Errorf("%w: deferred inbound message %x changes route before dispatch", ErrInvalidInput, hash)
		}
	} else if envelope.current == envelope.next && envelope.current != envelope.destination {
		return fmt.Errorf("%w: inbound message %x does not advance toward destination", ErrInvalidInput, hash)
	}
	if descriptor.transaction != nil && !v.target.ContainsPrefix(envelope.destination) {
		return fmt.Errorf("%w: inbound message %x transaction destination is outside this shard", ErrInvalidInput, hash)
	}
	if descriptor.transaction == nil && descriptor.tag != semanticInDeferredTransit &&
		v.target.ContainsPrefix(envelope.destination) {
		return fmt.Errorf("%w: inbound message %x reached destination without a transaction", ErrInvalidInput, hash)
	}
	if (descriptor.tag == semanticInImmediate || descriptor.tag == semanticInFinal ||
		descriptor.tag == semanticInDeferredFinal) &&
		descriptor.fee.Nano().Cmp(envelope.value.FwdFeeRemaining.Nano()) != 0 {
		return fmt.Errorf("%w: inbound message %x collected forwarding fee mismatch", ErrInvalidInput, hash)
	}
	if descriptor.tag == semanticInImmediate {
		// The rule is unconditional, the masterchain fee recovery and mint
		// messages included: check_special_message rejects a special message
		// whose envelope carries emitted_lt outright (validate-query.cpp:6577).
		// Those two are also the only immediate descriptors allowed to have no
		// outbound pair, so the msg_export_imm rule (validate-query.cpp:4546)
		// never sees them and this is their only gate.
		if envelope.value.EmittedLT != nil {
			return fmt.Errorf("%w: immediate inbound message %x has a custom emitted lt", ErrInvalidInput, hash)
		}
		if !v.target.ContainsPrefix(envelope.source) || envelope.current != envelope.destination {
			return fmt.Errorf("%w: immediate inbound message %x is not local", ErrInvalidInput, hash)
		}
	}
	if descriptor.tag == semanticInTransit && envelope.current == envelope.destination {
		return fmt.Errorf("%w: transit inbound message %x already reached its destination", ErrInvalidInput, hash)
	}

	return nil
}

func (v *semanticQueueValidation) findImportedMessage(
	envelope *semanticEnvelope,
) (semanticQueueEntry, bool, error) {
	key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
	if v.target.ContainsPrefix(envelope.current) {
		entry, err := loadSemanticQueueEntry(v.old.OutQueue, key, &v.replay.envelopes)
		if err != nil {
			return semanticQueueEntry{}, true, fmt.Errorf("%w: local outbound queue entry is absent", ErrInvalidInput)
		}
		if !equalCell(entry.enqueued.Msg, envelope.root) {
			return semanticQueueEntry{}, true, fmt.Errorf("%w: local outbound queue envelope mismatch", ErrInvalidInput)
		}
		return entry, true, nil
	}

	for i := range v.sources {
		source := &v.sources[i]
		if !source.owner.ContainsPrefix(envelope.current) {
			continue
		}
		entry, err := loadSemanticQueueEntryFromNeighbor(source.queue.OutQueue, key)
		if err != nil {
			if !isMissingKey(err) {
				return semanticQueueEntry{}, false, fmt.Errorf(
					"%w: load message from neighbor (%d,%016x,%d): %v",
					ErrInvalidInput,
					source.neighbor.Block.Workchain,
					uint64(source.neighbor.Block.Shard),
					source.neighbor.Block.SeqNo,
					err,
				)
			}
			return semanticQueueEntry{}, false, fmt.Errorf(
				"%w: message is absent from neighbor (%d,%016x,%d)",
				ErrInvalidInput,
				source.neighbor.Block.Workchain,
				uint64(source.neighbor.Block.Shard),
				source.neighbor.Block.SeqNo,
			)
		}
		if !equalCell(entry.enqueued.Msg, envelope.root) {
			return semanticQueueEntry{}, false, fmt.Errorf("%w: neighbor outbound queue envelope mismatch", ErrInvalidInput)
		}
		processed, err := v.shardEndLT.alreadyProcessed(
			v.oldRecords,
			v.target.Workchain,
			v.target.Shard,
			&entry.descr,
		)
		if err != nil {
			return semanticQueueEntry{}, false, fmt.Errorf("%w: check predecessor processed info: %v", ErrInvalidInput, err)
		}
		if processed {
			return semanticQueueEntry{}, false, fmt.Errorf("%w: neighbor message was processed by a previous block", ErrInvalidInput)
		}
		return entry, false, nil
	}

	return semanticQueueEntry{}, false, fmt.Errorf("%w: message current hop belongs to no authenticated neighbor", ErrInvalidInput)
}

func (v *semanticQueueValidation) isMasterSpecial(descriptor *cell.Cell) bool {
	extra := v.replay.candidate.block.Extra.Custom
	return extra != nil &&
		(equalCell(descriptor, extra.Details.RecoverCreateMsg) || equalCell(descriptor, extra.Details.MintMsg))
}

func (v *semanticQueueValidation) recordProcessed(bound semanticMessageBound) {
	if !v.hasProcessed || v.processedMax.less(bound) {
		v.processedMax = bound
		v.hasProcessed = true
	}
}
