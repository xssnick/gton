package msgpool

import (
	"errors"
	"fmt"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const shardStateMagic = 0x9023afe2

// ErrQueueSizeNotStored means the state predates capStoreOutMsgQueueSize.
var ErrQueueSizeNotStored = errors.New("msgpool: state stores no out-queue size")

func routedDeltasFromBlockRoot(
	root *cell.Cell,
	startLT uint64,
	source ShardIdent,
	ref SourceRef,
	snapshot *internalsSnapshot,
) ([]RoutedDelta, error) {
	outDescr, err := blockOutMsgDescr(root)
	if err != nil {
		return nil, err
	}
	loader, err := outDescr.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse OutMsgDescr: %w", err)
	}
	dict, err := loader.LoadAugDict(256, tlb.AugOutMsgDescr{}, false)
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot load OutMsgDescr dictionary: %w", err)
	}

	deltas := make([]RoutedDelta, len(snapshot.destinations))
	values := make([]InternalsDelta, len(snapshot.destinations))
	for index := range deltas {
		deltas[index] = RoutedDelta{
			Destination: snapshot.destinations[index].shard,
			Delta:       &values[index],
		}
	}
	sink := routedSink{
		snapshot: snapshot,
		deltas:   deltas,
		source:   source,
		seqno:    ref.Seqno,
		startLT:  startLT,
	}
	ok, err := dict.CheckForEachExtra(func(value, extra *cell.Slice, key *cell.Cell) (bool, error) {
		v := value.Copy()
		tag, err := v.LoadUInt(3)
		if err != nil {
			return false, fmt.Errorf("cannot load OutMsg tag: %w", err)
		}
		switch tag {
		case 0b000, 0b010:
			// msg_export_ext leaves the chain, msg_export_imm was delivered
			// inside the block: no queue effect.
			return true, nil

		case 0b001, 0b011: // msg_export_new$001 / msg_export_tr$011
			envCell, err := v.LoadRefCell()
			if err != nil {
				return false, fmt.Errorf("cannot unpack queued export: %w", err)
			}
			placement := queueAtMessageLT
			if tag == 0b011 {
				placement = queueAtBlockStart
			}
			return true, sink.addEnvelope(envCell, placement)

		case 0b101: // msg_export_new_defer$10100 / msg_export_deferred_tr$10101
			deferredTag, err := v.LoadUInt(2)
			if err != nil {
				return false, fmt.Errorf("cannot load deferred export tag: %w", err)
			}
			switch deferredTag {
			case 0b00:
				// msg_export_new_defer stays in DispatchQueue.
				return true, nil
			case 0b01:
				envCell, err := v.LoadRefCell()
				if err != nil {
					return false, fmt.Errorf("cannot unpack msg_export_deferred_tr: %w", err)
				}
				return true, sink.addEnvelope(envCell, queueAtEmissionLT)
			default:
				return false, fmt.Errorf("unknown OutMsg tag 101%02b", deferredTag)
			}

		case 0b100: // msg_export_deq_imm$100 out_msg:^MsgEnvelope reimport:^InMsg
			return true, sink.removeEnvelopeFrom(v)

		case 0b110: // msg_export_deq$1100 / msg_export_deq_short$1101
			short, err := v.LoadBoolBit()
			if err != nil {
				return false, fmt.Errorf("cannot load dequeue tag bit: %w", err)
			}
			if !short {
				return true, sink.removeEnvelopeFrom(v)
			}
			// msg_export_deq_short$1101 msg_env_hash:bits256
			// next_workchain:int32 next_addr_pfx:uint64 import_block_lt:uint64
			envHash, err := v.LoadSlice(256)
			if err != nil {
				return false, fmt.Errorf("cannot load dequeued envelope hash: %w", err)
			}
			workchain, err := v.LoadInt(32)
			if err != nil {
				return false, fmt.Errorf("cannot load dequeue next workchain: %w", err)
			}
			prefix, err := v.LoadUInt(64)
			if err != nil {
				return false, fmt.Errorf("cannot load dequeue next prefix: %w", err)
			}
			var hash [32]byte
			copy(hash[:], envHash)
			sink.removeByEnvHash(AccountPrefix{Workchain: int32(workchain), Prefix: prefix}, hash)
			return true, nil

		case 0b111: // msg_export_tr_req$111: dequeue old envelope and enqueue rewritten one
			envCell, err := v.LoadRefCell()
			if err != nil {
				return false, fmt.Errorf("cannot unpack msg_export_tr_req envelope: %w", err)
			}
			if err = sink.addEnvelope(envCell, queueAtBlockStart); err != nil {
				return false, err
			}

			imported, err := v.LoadRefCell()
			if err != nil {
				return false, fmt.Errorf("cannot unpack msg_export_tr_req import: %w", err)
			}
			oldEnv, err := transitInputEnvelope(imported)
			if err != nil {
				return false, err
			}
			return true, sink.removeEnvelopeCell(oldEnv)
		}

		return false, fmt.Errorf("unknown OutMsg tag %03b", tag)
	}, false)
	if err != nil {
		return nil, fmt.Errorf("msgpool: failed to iterate OutMsgDescr: %w", err)
	}
	if !ok {
		return nil, errors.New("msgpool: failed to iterate OutMsgDescr")
	}

	for index := range deltas {
		delta := deltas[index].Delta
		delta.AddedTotal = sink.addedTotal
		delta.RemovedTotal = sink.removedTotal
		added := delta.Added
		slices.SortFunc(added, CompareLtHash)
	}
	return deltas, nil
}

// routedSink routes one parsed operation through the immutable prefix trie;
// full-queue totals remain source invariants and therefore reach every view.
type routedSink struct {
	snapshot     *internalsSnapshot
	deltas       []RoutedDelta
	source       ShardIdent
	seqno        uint32
	startLT      uint64
	addedTotal   int
	removedTotal int
}

type queuePlacement uint8

const (
	queueAtMessageLT queuePlacement = iota
	queueAtBlockStart
	queueAtEmissionLT
)

func (s *routedSink) addEnvelope(envCell *cell.Cell, placement queuePlacement) error {
	s.addedTotal++
	msg, err := internalFromEnvelope(envCell)
	if err != nil {
		return err
	}
	switch placement {
	case queueAtMessageLT:
	case queueAtBlockStart:
		// An ordinary transit re-enqueue enters the queue at the relaying
		// block's start_lt, not at the message creation lt.
		msg.QueueLT = s.startLT
	case queueAtEmissionLT:
		// A message released from DispatchQueue is emitted inside this block.
		// C++ stores that emitted_lt as EnqueuedMsg.enqueued_lt; start_lt is
		// necessarily earlier and would make the queue entry invalid.
		msg.QueueLT = msg.EnqueuedLT
	}
	msg.Source = s.source
	msg.SourceSeqno = s.seqno
	hop := msg.Key.NextHop()
	s.snapshot.router.walk(hop, func(index int) {
		delta := s.deltas[index].Delta
		delta.Added = append(delta.Added, msg)
	})
	return nil
}

// removeEnvelopeFrom resolves a dequeue record carrying the full envelope
// into its queue key.
func (s *routedSink) removeEnvelopeFrom(v *cell.Slice) error {
	envCell, err := v.LoadRefCell()
	if err != nil {
		return fmt.Errorf("cannot unpack dequeue record: %w", err)
	}
	return s.removeEnvelopeCell(envCell)
}

func (s *routedSink) removeEnvelopeCell(envCell *cell.Cell) error {
	s.removedTotal++
	msg, err := internalFromEnvelope(envCell)
	if err != nil {
		return err
	}
	hop := msg.Key.NextHop()
	s.snapshot.router.walk(hop, func(index int) {
		delta := s.deltas[index].Delta
		delta.RemovedKeys = append(delta.RemovedKeys, msg.Key)
	})
	return nil
}

func (s *routedSink) removeByEnvHash(hop AccountPrefix, envHash [32]byte) {
	s.removedTotal++
	s.snapshot.router.walk(hop, func(index int) {
		delta := s.deltas[index].Delta
		delta.RemovedEnvHashes = append(delta.RemovedEnvHashes, envHash)
	})
}

// transitInputEnvelope extracts the old queued envelope from
// msg_import_tr$101, the reimport record carried by msg_export_tr_req.
func transitInputEnvelope(imported *cell.Cell) (*cell.Cell, error) {
	v, err := imported.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("cannot parse msg_export_tr_req import: %w", err)
	}
	tag, err := v.LoadUInt(3)
	if err != nil {
		return nil, fmt.Errorf("cannot load msg_export_tr_req import tag: %w", err)
	}
	if tag != 0b101 {
		return nil, fmt.Errorf("msg_export_tr_req import has tag %03b, want msg_import_tr$101", tag)
	}
	oldEnv, err := v.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("cannot unpack msg_import_tr input envelope: %w", err)
	}
	return oldEnv, nil
}

func routedSeedsFromStateRoot(
	stateRoot *cell.Cell,
	source ShardIdent,
	top SourceRef,
	snapshot *internalsSnapshot,
) ([]RoutedSeed, uint64, error) {
	queueInfo, err := stateOutMsgQueueInfo(stateRoot)
	if err != nil {
		return nil, 0, err
	}

	seeds := make([]RoutedSeed, len(snapshot.destinations))
	for index := range seeds {
		seeds[index].Destination = snapshot.destinations[index].shard
	}
	var total uint64
	ok, err := queueInfo.OutQueue.CheckForEachExtra(func(value, extra *cell.Slice, key *cell.Cell) (bool, error) {
		total++
		keyLoader, err := key.BeginParse()
		if err != nil {
			return false, fmt.Errorf("cannot parse queue key: %w", err)
		}
		keyBits, err := keyLoader.LoadSlice(352)
		if err != nil {
			return false, fmt.Errorf("cannot load queue key bits: %w", err)
		}
		var queueKey QueueKey
		copy(queueKey[:], keyBits)
		canonicalLT, err := extra.Copy().LoadUInt(64)
		if err != nil {
			return false, fmt.Errorf("cannot load queue leaf augmentation lt: %w", err)
		}

		var enqueued tlb.EnqueuedMsg
		if err = tlb.LoadFromCell(&enqueued, value.Copy()); err != nil {
			return false, fmt.Errorf("cannot decode enqueued message: %w", err)
		}
		msg, err := internalFromEnvelope(enqueued.Msg)
		if err != nil {
			return false, err
		}
		if msg.Key.MsgHash() != queueKey.MsgHash() {
			return false, fmt.Errorf("%w: queue key %x does not match its envelope", ErrApplyDisorder, queueKey)
		}
		// AugOutMsgQueue's leaf augmentation is the canonical import-order
		// lt: MsgEnvelope.emitted_lt for v2 envelopes, otherwise
		// Message.created_lt. EnqueuedMsg.enqueued_lt records when the
		// envelope entered this particular queue and may differ in transit.
		msg.EnqueuedLT = canonicalLT
		msg.QueueLT = enqueued.EnqueuedLT
		msg.Key = queueKey
		msg.Source = source
		msg.SourceSeqno = top.Seqno
		snapshot.router.walk(queueKey.NextHop(), func(index int) {
			seeds[index].Messages = append(seeds[index].Messages, msg)
		})
		return true, nil
	}, false)
	if err != nil {
		return nil, 0, fmt.Errorf("msgpool: failed to walk the out-queue: %w", err)
	}
	if !ok {
		return nil, 0, errors.New("msgpool: failed to walk the out-queue")
	}

	for index := range seeds {
		messages := seeds[index].Messages
		slices.SortFunc(messages, CompareLtHash)
	}

	return seeds, total, nil
}

// QueueSizeFromStateRoot reads the out-queue size stored by
// capStoreOutMsgQueueSize.
func QueueSizeFromStateRoot(stateRoot *cell.Cell) (uint64, error) {
	queueInfo, err := stateOutMsgQueueInfo(stateRoot)
	if err != nil {
		return 0, err
	}
	if queueInfo.Extra == nil || queueInfo.Extra.OutQueueSize == nil {
		return 0, ErrQueueSizeNotStored
	}
	return *queueInfo.Extra.OutQueueSize, nil
}

// stateOutMsgQueueInfo walks a ShardStateUnsplit root to its parsed
// OutMsgQueueInfo, touching only the state magic and the first reference.
func stateOutMsgQueueInfo(stateRoot *cell.Cell) (*tlb.OutMsgQueueInfo, error) {
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse state root: %w", err)
	}
	magic, err := loader.LoadUInt(32)
	if err != nil || magic != shardStateMagic {
		return nil, errors.New("msgpool: cell is not a shard state root")
	}
	// shard_state#9023afe2 … out_msg_queue_info:^OutMsgQueueInfo is the
	// first reference of the state root.
	infoCell, err := stateRoot.PeekRef(0)
	if err != nil {
		return nil, fmt.Errorf("msgpool: state root has no out-queue info: %w", err)
	}
	infoLoader, err := infoCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse out-queue info: %w", err)
	}
	var queueInfo tlb.OutMsgQueueInfo
	if err = tlb.LoadFromCell(&queueInfo, infoLoader); err != nil {
		return nil, fmt.Errorf("msgpool: cannot decode out-queue info: %w", err)
	}
	return &queueInfo, nil
}

// internalFromEnvelope builds the pooled view of one queued envelope: the
// parsed envelope plus the queue key derived from the next-hop address.
//
// Of int_msg_info only the header the derivation reads is decoded — both
// addresses and created_lt. value, the extra-currency dictionary, both fees,
// the state init and the body are skipped and never materialized: no field of
// InternalMessage carries them, and the reference node walks the same queue
// through unpack_cell_inexact (block.cpp EnqueuedMsgDescr::unpack), a
// header-only decode as well. The addresses still go through
// cell.Slice.LoadAddr, exactly the routine tlb's `addr` tag calls, so the
// addr_std/addr_var split and the anycast rewrite behind every derived key
// stay bit-identical.
func internalFromEnvelope(envCell *cell.Cell) (*InternalMessage, error) {
	envLoader, err := envCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("cannot parse message envelope: %w", err)
	}
	var env tlb.MsgEnvelope
	if err = env.LoadFromCell(envLoader); err != nil {
		return nil, fmt.Errorf("cannot decode message envelope: %w", err)
	}

	msgLoader, err := env.Msg.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("cannot parse enveloped message: %w", err)
	}
	// int_msg_info$0 ihr_disabled:Bool bounce:Bool bounced:Bool
	header, err := msgLoader.LoadUInt(4)
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message header: %w", err)
	}
	if header&0b1000 != 0 {
		return nil, errors.New("enveloped message is not int_msg_info$0")
	}
	srcAddr, err := msgLoader.LoadAddr()
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message source: %w", err)
	}
	dstAddr, err := msgLoader.LoadAddr()
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message destination: %w", err)
	}
	// value:CurrencyCollection — grams, then the extra-currency HashmapE,
	// whose root is a reference and therefore costs a single bit here.
	if err = skipGrams(msgLoader); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message value: %w", err)
	}
	if _, err = msgLoader.LoadBoolBit(); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message extra currencies: %w", err)
	}
	if err = skipGrams(msgLoader); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message ihr fee: %w", err)
	}
	if err = skipGrams(msgLoader); err != nil {
		return nil, fmt.Errorf("cannot skip enveloped message forward fee: %w", err)
	}
	createdLT, err := msgLoader.LoadUInt(64)
	if err != nil {
		return nil, fmt.Errorf("cannot load enveloped message created lt: %w", err)
	}

	var hop AccountPrefix
	switch env.NextAddr.Type {
	case tlb.IntermediateAddressRegular:
		src, err := AccountPrefixFromAddress(srcAddr)
		if err != nil {
			return nil, fmt.Errorf("envelope source: %w", err)
		}
		dst, err := AccountPrefixFromAddress(dstAddr)
		if err != nil {
			return nil, fmt.Errorf("envelope destination: %w", err)
		}
		hop = InterpolatePrefix(src, dst, int(env.NextAddr.UseDestBits))
	case tlb.IntermediateAddressSimple, tlb.IntermediateAddressExt:
		hop = AccountPrefix{Workchain: env.NextAddr.Workchain, Prefix: env.NextAddr.AddrPfx}
	default:
		return nil, fmt.Errorf("unknown envelope next-hop type %d", env.NextAddr.Type)
	}

	// The queue lt rule of AugOutMsgQueue: a v2 envelope's emitted lt takes
	// precedence over the message creation lt.
	enqueuedLT := createdLT
	if env.EmittedLT != nil {
		enqueuedLT = *env.EmittedLT
	}

	return &InternalMessage{
		Key:          MakeQueueKey(hop, env.Msg.HashKey()),
		EnqueuedLT:   enqueuedLT,
		QueueLT:      createdLT,
		EnvHash:      envCell.HashKey(),
		Envelope:     env,
		EnvelopeCell: envCell,
		Root:         env.Msg,
	}, nil
}

// skipGrams advances past one Grams (VarUInteger 16) field: a 4-bit byte
// length followed by that many bytes.
func skipGrams(loader *cell.Slice) error {
	length, err := loader.LoadUInt(4)
	if err != nil {
		return err
	}
	return loader.SkipBits(uint(length) * 8)
}

// blockOutMsgDescr walks a block root to its OutMsgDescr cell, touching
// only the header magics.
func blockOutMsgDescr(root *cell.Cell) (*cell.Cell, error) {
	extra, err := blockExtra(root)
	if err != nil {
		return nil, err
	}
	// block_extra in_msg_descr:^InMsgDescr out_msg_descr:^OutMsgDescr ...
	outMsg, err := extra.PeekRef(1)
	if err != nil {
		return nil, fmt.Errorf("msgpool: block extra has no OutMsgDescr: %w", err)
	}
	return outMsg, nil
}
