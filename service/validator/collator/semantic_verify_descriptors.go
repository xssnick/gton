package collator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func (v *semanticQueueValidation) loadDescriptors() error {
	iterator, err := v.replay.inMessages.IteratorExtra(false, false)
	if err != nil {
		return fmt.Errorf("%w: iterate inbound descriptors: %v", ErrInvalidInput, err)
	}
	for iterator.Next() {
		if err = v.replay.ctx.Err(); err != nil {
			return err
		}
		item := iterator.View()
		hash, keyErr := semanticDescriptorKey(&item.Key)
		if keyErr != nil {
			return keyErr
		}
		descriptor, parseErr := parseSemanticInDescriptor(item.Value, hash)
		if parseErr != nil {
			return parseErr
		}
		v.in[cell.Hash(hash)] = descriptor
		v.inOrder = append(v.inOrder, cell.Hash(hash))
		// The tag is already decoded here, so the candidate's message mix costs
		// nothing beyond these increments. This phase runs before the account
		// lanes start, so the write is single-threaded.
		switch descriptor.tag {
		case semanticInExternal:
			v.replay.shape.ExternalMessages++
		case semanticInFinal, semanticInTransit:
			// The pair collation writes for an internal taken off a queue,
			// executed here or relayed onward. Immediates never touched a queue
			// and deferred descriptors come from the dispatch queue; collation
			// counts both under their own Stats fields, not this one.
			v.replay.shape.InternalMessages++
		}
	}
	if err = iterator.Err(); err != nil {
		return fmt.Errorf("%w: iterate inbound descriptors: %v", ErrInvalidInput, err)
	}

	iterator, err = v.replay.outMessages.IteratorExtra(false, false)
	if err != nil {
		return fmt.Errorf("%w: iterate outbound descriptors: %v", ErrInvalidInput, err)
	}
	for iterator.Next() {
		if err = v.replay.ctx.Err(); err != nil {
			return err
		}
		item := iterator.View()
		hash, keyErr := semanticDescriptorKey(&item.Key)
		if keyErr != nil {
			return keyErr
		}
		descriptor, parseErr := parseSemanticOutDescriptor(item.Value, hash)
		if parseErr != nil {
			return parseErr
		}
		v.out[cell.Hash(hash)] = descriptor
		v.outOrder = append(v.outOrder, cell.Hash(hash))
	}
	if err = iterator.Err(); err != nil {
		return fmt.Errorf("%w: iterate outbound descriptors: %v", ErrInvalidInput, err)
	}

	return nil
}

func semanticDescriptorKey(key *cell.Slice) ([32]byte, error) {
	bits, err := key.LoadSlice(256)
	if err != nil || key.BitsLeft() != 0 || key.RefsNum() != 0 {
		return [32]byte{}, fmt.Errorf("%w: message descriptor key is malformed", ErrInvalidInput)
	}
	var hash [32]byte
	copy(hash[:], bits)
	return hash, nil
}

func parseSemanticInDescriptor(value cell.Slice, key [32]byte) (*semanticInDescriptor, error) {
	raw, err := value.ToCell()
	if err != nil {
		return nil, fmt.Errorf("%w: materialize inbound descriptor %x: %v", ErrInvalidInput, key, err)
	}
	loader := value
	tag, err := loader.LoadUInt(3)
	if err != nil {
		return nil, fmt.Errorf("%w: load inbound descriptor %x tag: %v", ErrInvalidInput, key, err)
	}
	descriptor := &semanticInDescriptor{tag: uint8(tag), root: raw}
	var first, second *cell.Cell
	switch tag {
	case semanticInExternal:
		first, second, err = semanticLoadTwoRefs(&loader)
		descriptor.transaction = second
	case 0b001:
		suffix, suffixErr := loader.LoadUInt(2)
		if suffixErr != nil || suffix > 1 {
			return nil, fmt.Errorf("%w: invalid deferred inbound descriptor %x", ErrInvalidInput, key)
		}
		descriptor.tag = semanticInDeferredFinal + uint8(suffix)
		first, second, err = semanticLoadTwoRefs(&loader)
		if suffix == 0 {
			descriptor.transaction = second
			if err == nil {
				descriptor.fee, err = semanticLoadCoins(&loader)
			}
		}
	case semanticInIHR, semanticInDiscardFinal, semanticInDiscardTransit:
		return nil, fmt.Errorf("%w: inbound descriptor %x uses disabled IHR constructor %03b", ErrUnsupported, key, tag)
	case semanticInImmediate, semanticInFinal:
		first, second, err = semanticLoadTwoRefs(&loader)
		descriptor.transaction = second
		if err == nil {
			descriptor.fee, err = semanticLoadCoins(&loader)
		}
	case semanticInTransit:
		first, second, err = semanticLoadTwoRefs(&loader)
		if err == nil {
			descriptor.fee, err = semanticLoadCoins(&loader)
		}
	default:
		return nil, fmt.Errorf("%w: unknown inbound descriptor %x tag %03b", ErrInvalidInput, key, tag)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: decode inbound descriptor %x: %v", ErrInvalidInput, key, err)
	}
	if err = requireEmptyDescriptor(&loader); err != nil {
		return nil, fmt.Errorf("%w: inbound descriptor %x: %v", ErrInvalidInput, key, err)
	}

	if descriptor.tag == semanticInExternal {
		descriptor.message = first
	} else {
		descriptor.envelope, err = parseSemanticEnvelope(first)
		if err != nil {
			return nil, fmt.Errorf("%w: inbound descriptor %x envelope: %v", ErrInvalidInput, key, err)
		}
		descriptor.message = descriptor.envelope.message
	}
	if descriptor.tag == semanticInTransit || descriptor.tag == semanticInDeferredTransit {
		descriptor.outEnvelope, err = parseSemanticEnvelope(second)
		if err != nil {
			return nil, fmt.Errorf("%w: inbound descriptor %x rewritten envelope: %v", ErrInvalidInput, key, err)
		}
		if descriptor.outEnvelope.message.HashKey() != descriptor.message.HashKey() {
			return nil, fmt.Errorf("%w: inbound transit descriptor %x rewrites another message", ErrInvalidInput, key)
		}
	}
	if descriptor.message == nil || descriptor.message.HashKey() != cell.Hash(key) {
		return nil, fmt.Errorf("%w: inbound descriptor %x message hash mismatch", ErrInvalidInput, key)
	}

	return descriptor, nil
}

func parseSemanticOutDescriptor(value cell.Slice, key [32]byte) (*semanticOutDescriptor, error) {
	raw, err := value.ToCell()
	if err != nil {
		return nil, fmt.Errorf("%w: materialize outbound descriptor %x: %v", ErrInvalidInput, key, err)
	}
	loader := value
	tag3, err := loader.LoadUInt(3)
	if err != nil {
		return nil, fmt.Errorf("%w: load outbound descriptor %x tag: %v", ErrInvalidInput, key, err)
	}
	descriptor := &semanticOutDescriptor{tag: uint8(tag3), root: raw}
	var first, second, third *cell.Cell
	switch tag3 {
	case semanticOutExternal:
		first, second, err = semanticLoadTwoRefs(&loader)
		descriptor.message = first
		descriptor.transaction = second
	case semanticOutNew:
		first, second, err = semanticLoadTwoRefs(&loader)
		descriptor.transaction = second
	case semanticOutImmediate:
		first, second, third, err = semanticLoadThreeRefs(&loader)
		descriptor.transaction = second
		descriptor.reimport = third
	case semanticOutTransit, semanticOutTransitRequest, semanticOutDequeueImmediate:
		first, second, err = semanticLoadTwoRefs(&loader)
		descriptor.reimport = second
	case 0b101:
		suffix, suffixErr := loader.LoadUInt(2)
		if suffixErr != nil || suffix > 1 {
			return nil, fmt.Errorf("%w: invalid deferred outbound descriptor %x", ErrInvalidInput, key)
		}
		descriptor.tag = semanticOutNewDeferred + uint8(suffix)
		first, second, err = semanticLoadTwoRefs(&loader)
		if suffix == 0 {
			descriptor.transaction = second
		} else {
			descriptor.reimport = second
		}
	case 0b110:
		short, tagErr := loader.LoadBoolBit()
		if tagErr != nil {
			return nil, fmt.Errorf("%w: load dequeue descriptor %x tag: %v", ErrInvalidInput, key, tagErr)
		}
		if short {
			descriptor.tag = semanticOutDequeueShort
			envelopeHash, loadErr := loader.LoadSlice(256)
			if loadErr != nil {
				err = loadErr
				break
			}
			copy(descriptor.envelopeHash[:], envelopeHash)
			workchain, loadErr := loader.LoadInt(32)
			if loadErr != nil {
				err = loadErr
				break
			}
			prefix, loadErr := loader.LoadUInt(64)
			if loadErr != nil {
				err = loadErr
				break
			}
			descriptor.next = msgpool.AccountPrefix{Workchain: int32(workchain), Prefix: prefix}
			descriptor.importBlockLT, err = loader.LoadUInt(64)
		} else {
			descriptor.tag = semanticOutDequeue
			first, err = loader.LoadRefCell()
			if err == nil {
				descriptor.importBlockLT, err = loader.LoadUInt(63)
			}
		}
	default:
		return nil, fmt.Errorf("%w: unknown outbound descriptor %x tag %03b", ErrInvalidInput, key, tag3)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: decode outbound descriptor %x: %v", ErrInvalidInput, key, err)
	}
	if err = requireEmptyDescriptor(&loader); err != nil {
		return nil, fmt.Errorf("%w: outbound descriptor %x: %v", ErrInvalidInput, key, err)
	}

	if descriptor.tag != semanticOutExternal && descriptor.tag != semanticOutDequeueShort {
		descriptor.envelope, err = parseSemanticEnvelope(first)
		if err != nil {
			return nil, fmt.Errorf("%w: outbound descriptor %x envelope: %v", ErrInvalidInput, key, err)
		}
		descriptor.message = descriptor.envelope.message
	}
	if descriptor.message != nil && descriptor.message.HashKey() != cell.Hash(key) {
		return nil, fmt.Errorf("%w: outbound descriptor %x message hash mismatch", ErrInvalidInput, key)
	}

	return descriptor, nil
}

func semanticLoadTwoRefs(loader *cell.Slice) (*cell.Cell, *cell.Cell, error) {
	first, err := loader.LoadRefCell()
	if err != nil {
		return nil, nil, err
	}
	second, err := loader.LoadRefCell()
	return first, second, err
}

func semanticLoadThreeRefs(loader *cell.Slice) (*cell.Cell, *cell.Cell, *cell.Cell, error) {
	first, second, err := semanticLoadTwoRefs(loader)
	if err != nil {
		return nil, nil, nil, err
	}
	third, err := loader.LoadRefCell()
	return first, second, third, err
}

func semanticLoadCoins(loader *cell.Slice) (tlb.Coins, error) {
	var coins tlb.Coins
	err := coins.LoadFromCell(loader)
	return coins, err
}

func parseSemanticEnvelope(root *cell.Cell) (*semanticEnvelope, error) {
	var envelope tlb.MsgEnvelope
	if err := parseExact(&envelope, root); err != nil {
		return nil, err
	}
	if envelope.CurAddr.Type != tlb.IntermediateAddressRegular ||
		envelope.NextAddr.Type != tlb.IntermediateAddressRegular {
		return nil, errors.New("message envelope uses non-regular routing addresses")
	}
	if err := requireCanonicalEnvelopeTag(envelope); err != nil {
		return nil, err
	}
	var internal tlb.InternalMessage
	if err := parseExact(&internal, envelope.Msg); err != nil {
		return nil, fmt.Errorf("message is not internal: %w", err)
	}

	return semanticEnvelopeFromInternal(root, envelope, internal)
}

// requireCanonicalEnvelopeTag rejects msg_envelope_v2#5 carrying neither
// emitted_lt nor metadata. The constraint is not visible in block.tlb's text: it
// is the trailing `&& (with_emitted_lt || with_metadata)` of MsgEnvelope::unpack
// (crypto/block/block-parse.cpp:919, with the reference's own comment
// "otherwise it should be msg_envelope#4"), so an envelope that spends the v2
// tag on an empty payload makes the reference's unpack fail and the block is
// rejected before any semantic rule runs. Both envelope parsers share this
// because the rule is a property of the constructor, not of the descriptor that
// carries it. Note the two shapes are distinct blocks, not a re-encoding: the
// message dictionaries are keyed by message hash, so the envelope bytes differ
// while the key does not.
func requireCanonicalEnvelopeTag(envelope tlb.MsgEnvelope) error {
	if envelope.V2 && envelope.EmittedLT == nil && envelope.Metadata == nil {
		return errors.New("message envelope uses the v2 tag without emitted lt or metadata")
	}
	return nil
}

func parseSemanticNeighborEnvelope(root *cell.Cell) (*semanticEnvelope, error) {
	return parseSemanticNeighborEnvelopeLoaded(root, nil)
}

// parseSemanticNeighborEnvelopeLoaded is parseSemanticNeighborEnvelope with the
// message cell optionally supplied by the caller. A caller that has already
// taken the message out of storage passes it here so this parse dereferences
// nothing: every error it can then raise is about the envelope's or the
// message's CONTENT, which is the property walkSemanticQueuePrefix's
// classification rests on.
//
// The supplied cell is the envelope's own reference — the caller obtains it by
// loading that reference and nothing else — and the lazy loader validates each
// load against the placeholder's hash and depth before returning it
// (cell.validateLoadedLazyRef), so substituting it cannot change which message
// is parsed.
func parseSemanticNeighborEnvelopeLoaded(root, loaded *cell.Cell) (*semanticEnvelope, error) {
	var envelope tlb.MsgEnvelope
	if err := parseExact(&envelope, root); err != nil {
		return nil, err
	}
	if envelope.CurAddr.Type != tlb.IntermediateAddressRegular ||
		envelope.NextAddr.Type != tlb.IntermediateAddressRegular {
		return nil, errors.New("message envelope uses non-regular routing addresses")
	}
	if err := requireCanonicalEnvelopeTag(envelope); err != nil {
		return nil, err
	}
	if loaded != nil {
		envelope.Msg = loaded
	}
	message, err := envelope.Msg.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("open internal message: %w", err)
	}
	var info semanticInternalMessageInfo
	if err = tlb.LoadFromCell(&info, message); err != nil {
		return nil, fmt.Errorf("message is not internal: %w", err)
	}
	internal := tlb.InternalMessage{
		IHRDisabled:     info.IHRDisabled,
		Bounce:          info.Bounce,
		Bounced:         info.Bounced,
		SrcAddr:         info.SrcAddr,
		DstAddr:         info.DstAddr,
		Amount:          info.Amount,
		ExtraCurrencies: info.ExtraCurrencies,
		IHRFee:          info.IHRFee,
		FwdFee:          info.FwdFee,
		CreatedLT:       info.CreatedLT,
		CreatedAt:       info.CreatedAt,
	}

	return semanticEnvelopeFromInternal(root, envelope, internal)
}

func semanticEnvelopeFromInternal(
	root *cell.Cell,
	envelope tlb.MsgEnvelope,
	internal tlb.InternalMessage,
) (*semanticEnvelope, error) {
	source, err := semanticAccountPrefixFromAddress(internal.SrcAddr)
	if err != nil {
		return nil, fmt.Errorf("source address: %w", err)
	}
	destination, err := semanticAccountPrefixFromAddress(internal.DstAddr)
	if err != nil {
		return nil, fmt.Errorf("destination address: %w", err)
	}
	current := semanticIntermediatePrefix(source, destination, envelope.CurAddr)
	next := semanticIntermediatePrefix(source, destination, envelope.NextAddr)
	if envelope.FwdFeeRemaining.Nano().Cmp(internal.FwdFee.Nano()) > 0 {
		return nil, fmt.Errorf("remaining forwarding fee exceeds the original fee")
	}
	lt := internal.CreatedLT
	if envelope.EmittedLT != nil {
		lt = *envelope.EmittedLT
	}

	return &semanticEnvelope{
		root:        root,
		value:       envelope,
		message:     envelope.Msg,
		internal:    internal,
		source:      source,
		destination: destination,
		current:     current,
		next:        next,
		bound:       semanticMessageBound{lt: lt, hash: envelope.Msg.HashKey()},
	}, nil
}

// semanticAccountPrefixFromAddress extracts the routing prefix of an address.
// Unlike outbound construction, validation must accept addr_var even when a
// shorter canonical addr_std encoding exists.
func semanticAccountPrefixFromAddress(addr *address.Address) (msgpool.AccountPrefix, error) {
	if addr == nil {
		return msgpool.AccountPrefix{}, fmt.Errorf("%w: account address is absent", ErrInvalidInput)
	}
	switch addr.Type() {
	case address.StdAddress:
		if addr.BitsLen() != 256 || len(addr.Data()) != 32 {
			return msgpool.AccountPrefix{}, fmt.Errorf("%w: malformed standard account address", ErrInvalidInput)
		}
	case address.VarAddress:
		if addr.BitsLen() < 64 || uint(len(addr.Data())*8) < addr.BitsLen() {
			return msgpool.AccountPrefix{}, fmt.Errorf("%w: variable account address has no routing prefix", ErrInvalidInput)
		}
	default:
		return msgpool.AccountPrefix{}, fmt.Errorf("%w: address has no internal routing prefix", ErrInvalidInput)
	}

	var prefix [8]byte
	copy(prefix[:], addr.Data()[:8])
	if err := msgpool.RewriteAnycast(prefix[:], addr); err != nil {
		return msgpool.AccountPrefix{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	return msgpool.AccountPrefix{
		Workchain: addr.Workchain(),
		Prefix:    binary.BigEndian.Uint64(prefix[:]),
	}, nil
}

func semanticIntermediatePrefix(
	source msgpool.AccountPrefix,
	destination msgpool.AccountPrefix,
	intermediate tlb.IntermediateAddress,
) msgpool.AccountPrefix {
	if intermediate.Type == tlb.IntermediateAddressRegular {
		return msgpool.InterpolatePrefix(source, destination, int(intermediate.UseDestBits))
	}
	return msgpool.AccountPrefix{Workchain: intermediate.Workchain, Prefix: intermediate.AddrPfx}
}

func semanticRoutingCommonBits(left, right msgpool.AccountPrefix) int {
	workchainDifference := uint32(left.Workchain) ^ uint32(right.Workchain)
	if workchainDifference != 0 {
		return bits.LeadingZeros32(workchainDifference)
	}

	return 32 + bits.LeadingZeros64(left.Prefix^right.Prefix)
}

func (v *semanticQueueValidation) verifyDescriptorCoverage() error {
	for _, hash := range v.inOrder {
		descriptor := v.in[hash]
		_, consumed := v.replay.consumedIn[hash]
		if (descriptor.transaction != nil) != consumed {
			return fmt.Errorf("%w: inbound descriptor %x transaction coverage mismatch", ErrInvalidInput, hash)
		}
	}
	for _, hash := range v.outOrder {
		descriptor := v.out[hash]
		_, consumed := v.replay.consumedOut[hash]
		if (descriptor.transaction != nil) != consumed {
			return fmt.Errorf("%w: outbound descriptor %x transaction coverage mismatch", ErrInvalidInput, hash)
		}
	}

	return nil
}
