package collator

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// claimedQueueEntry is the slice of one predecessor queue entry that the
// claimed-prefix cleanup consumes: the dictionary key (verified against the
// envelope's own routing before it is handed out), the envelope cell hash for
// the changed-envelope check, the current-hop prefix for the shard-ownership
// gate and the coverage-check descriptor. Everything else the full
// semanticQueueEntry parse produces — the decoded envelope, the message's
// addresses, values and fees — is validated and dropped, never returned.
type claimedQueueEntry struct {
	key          msgpool.QueueKey
	envelopeHash cell.Hash
	current      msgpool.AccountPrefix
	descr        tlb.ProcessedMsgDescr
}

// walkClaimedQueuePrefix is walkSemanticQueuePrefix with the leaf parse
// narrowed to what cleanupClaimedLocalDequeues reads. The traversal is the
// shared walkSemanticQueuePrefixLeaves, so the two walks open the same nodes
// and materialise the same leaves by construction; the narrow parse must then
// keep two properties the wide one has, because the cells this walk touches
// are collected into claimedPrefixCells and later recorded as the validation
// closure's read set:
//
//   - it opens the same cells below each leaf — the envelope and the message,
//     and nothing further down (the claimedPrefixCells parity tests compare the
//     resulting read sets cell for cell against the full walk's);
//   - it accepts and rejects exactly the entries the full parse does, since a
//     candidate this walk lets through is re-walked by every validator with the
//     full parse, and a rejection here must already be that validator's verdict.
func walkClaimedQueuePrefix(
	queue *tlb.OutMsgQueueAugDict,
	target msgpool.ShardIdent,
	bound semanticMessageBound,
	visit func(claimedQueueEntry) error,
) error {
	return walkSemanticQueuePrefixLeaves(queue, target, bound,
		func(key msgpool.QueueKey, value, extra *cell.Slice, leaf semanticQueueLeafCells) error {
			entry, parseErr := parseClaimedQueueEntryLight(key, value, extra, leaf)
			if parseErr != nil {
				return semanticQueueEntryVerdict{err: parseErr}
			}
			return visit(entry)
		})
}

// parseClaimedQueueEntryLight is parseSemanticNeighborQueueEntryLoaded reduced
// to the fields cleanup consumes. The wide parse decodes the full MsgEnvelope
// and CommonMsgInfo through their reflective/boxed forms — two addresses, four
// Coins values and an optional dictionary allocated per leaf — while cleanup
// reads only the routing prefixes, the two logical times and the two hashes.
// Every structural and semantic check the wide parse applies is applied here
// on the same cells; only the boxing is gone.
func parseClaimedQueueEntryLight(
	key msgpool.QueueKey,
	value, extra *cell.Slice,
	leaf semanticQueueLeafCells,
) (claimedQueueEntry, error) {
	// EnqueuedMsg: enqueued_lt:uint64 out_msg:^MsgEnvelope, with nothing
	// trailing. The envelope reference itself was taken and resolved by
	// materialiseSemanticQueueLeaf, so only its exact count is checked here.
	enqueuedLT, err := value.LoadUInt(64)
	if err != nil {
		return claimedQueueEntry{}, fmt.Errorf(
			"%w: decode outbound queue entry %x: failed to load enqueued lt: %v", ErrInvalidInput, key, err)
	}
	if value.BitsLeft() != 0 || value.RefsNum() != 1 {
		return claimedQueueEntry{}, fmt.Errorf(
			"%w: decode outbound queue entry %x: trailing data: %d bits, %d refs",
			ErrInvalidInput, key, value.BitsLeft(), value.RefsNum()-1)
	}

	envelope, err := parseClaimedEnvelopeLight(leaf)
	if err != nil {
		return claimedQueueEntry{}, fmt.Errorf(
			"%w: decode outbound queue envelope %x: %v", ErrInvalidInput, key, err)
	}
	if msgpool.MakeQueueKey(envelope.next, envelope.hash) != key {
		return claimedQueueEntry{}, fmt.Errorf(
			"%w: outbound queue entry %x key differs from envelope", ErrInvalidInput, key)
	}
	if extra != nil {
		canonicalLT, loadErr := extra.LoadUInt(64)
		if loadErr != nil || extra.BitsLeft() != 0 || extra.RefsNum() != 0 || canonicalLT != envelope.lt {
			return claimedQueueEntry{}, fmt.Errorf(
				"%w: outbound queue entry %x augmentation mismatch", ErrInvalidInput, key)
		}
	}

	return claimedQueueEntry{
		key:          key,
		envelopeHash: leaf.envelope.HashKey(),
		current:      envelope.current,
		descr: tlb.ProcessedMsgDescr{
			CurWorkchain:  envelope.current.Workchain,
			CurPrefix:     envelope.current.Prefix,
			NextWorkchain: envelope.next.Workchain,
			NextPrefix:    envelope.next.Prefix,
			LT:            envelope.lt,
			EnqueuedLT:    enqueuedLT,
			Hash:          envelope.hash,
		},
	}, nil
}

// claimedEnvelopeLight is what survives of a MsgEnvelope + CommonMsgInfo pair
// after validation: the two resolved routing prefixes, the emitted (or created)
// logical time and the message hash.
type claimedEnvelopeLight struct {
	current msgpool.AccountPrefix
	next    msgpool.AccountPrefix
	lt      uint64
	hash    [32]byte
}

func parseClaimedEnvelopeLight(leaf semanticQueueLeafCells) (claimedEnvelopeLight, error) {
	var s cell.Slice
	if err := leaf.envelope.BeginParseInto(&s); err != nil {
		return claimedEnvelopeLight{}, err
	}
	// materialiseSemanticQueueLeaf already took reference 0 as the message, so
	// at least one reference exists; the envelope must carry exactly it.
	refs := s.RefsNum()

	tag, err := s.LoadUInt(4)
	if err != nil {
		return claimedEnvelopeLight{}, fmt.Errorf("failed to load message envelope tag: %w", err)
	}
	if tag != 4 && tag != 5 {
		return claimedEnvelopeLight{}, fmt.Errorf("unsupported message envelope tag %d", tag)
	}
	curBits, err := loadClaimedRegularRoute(&s)
	if err != nil {
		return claimedEnvelopeLight{}, fmt.Errorf("failed to load current intermediate address: %w", err)
	}
	nextBits, err := loadClaimedRegularRoute(&s)
	if err != nil {
		return claimedEnvelopeLight{}, fmt.Errorf("failed to load next intermediate address: %w", err)
	}
	feeRemaining, err := loadClaimedCoins(&s)
	if err != nil {
		return claimedEnvelopeLight{}, fmt.Errorf("failed to load remaining forward fee: %w", err)
	}
	var emittedLT uint64
	hasEmittedLT := false
	if tag == 5 {
		if hasEmittedLT, err = s.LoadBoolBit(); err != nil {
			return claimedEnvelopeLight{}, fmt.Errorf("failed to load emitted lt flag: %w", err)
		}
		if hasEmittedLT {
			if emittedLT, err = s.LoadUInt(64); err != nil {
				return claimedEnvelopeLight{}, fmt.Errorf("failed to load emitted lt: %w", err)
			}
		}
		hasMetadata, metaErr := s.LoadBoolBit()
		if metaErr != nil {
			return claimedEnvelopeLight{}, fmt.Errorf("failed to load metadata flag: %w", metaErr)
		}
		if hasMetadata {
			if err = skipClaimedMetadata(&s); err != nil {
				return claimedEnvelopeLight{}, fmt.Errorf("failed to load metadata: %w", err)
			}
		}
		// The reference's own "otherwise it should be msg_envelope#4" rule; see
		// requireCanonicalEnvelopeTag for the citation.
		if !hasEmittedLT && !hasMetadata {
			return claimedEnvelopeLight{}, errors.New(
				"message envelope uses the v2 tag without emitted lt or metadata")
		}
	}
	if s.BitsLeft() != 0 || refs != 1 {
		return claimedEnvelopeLight{}, fmt.Errorf("trailing data: %d bits, %d refs", s.BitsLeft(), refs-1)
	}

	message, err := parseClaimedMessageLight(leaf.message)
	if err != nil {
		return claimedEnvelopeLight{}, err
	}
	if claimedCoinsExceed(feeRemaining, message.fwdFee) {
		return claimedEnvelopeLight{}, errors.New("remaining forwarding fee exceeds the original fee")
	}
	lt := message.createdLT
	if hasEmittedLT {
		lt = emittedLT
	}

	return claimedEnvelopeLight{
		current: msgpool.InterpolatePrefix(message.source, message.destination, curBits),
		next:    msgpool.InterpolatePrefix(message.source, message.destination, nextBits),
		lt:      lt,
		hash:    leaf.message.HashKey(),
	}, nil
}

// loadClaimedRegularRoute reads interm_addr_regular$0 use_dest_bits:(#<= 96).
// The wide parse also decodes the simple and ext variants, but only to reject
// them right after ("non-regular routing addresses"), so rejecting at the tag
// keeps the accepted set identical without carrying the dead decoders.
func loadClaimedRegularRoute(s *cell.Slice) (int, error) {
	notRegular, err := s.LoadBoolBit()
	if err != nil {
		return 0, fmt.Errorf("failed to load intermediate address tag: %w", err)
	}
	if notRegular {
		return 0, errors.New("message envelope uses non-regular routing addresses")
	}
	useDestBits, err := s.LoadUInt(7)
	if err != nil {
		return 0, fmt.Errorf("failed to load use dest bits: %w", err)
	}
	if useDestBits > 96 {
		return 0, fmt.Errorf("use dest bits %d is above 96", useDestBits)
	}
	return int(useDestBits), nil
}

// skipClaimedMetadata validates msg_metadata#0 without keeping it: cleanup
// consumes nothing out of it, but a malformed one must fail the entry exactly
// as MsgMetadata.LoadFromCell does, including the initiator's
// standard-without-anycast constraint.
func skipClaimedMetadata(s *cell.Slice) error {
	magic, err := s.LoadUInt(4)
	if err != nil || magic != 0 {
		return errors.New("message metadata magic is not correct")
	}
	if err = s.SkipBits(32); err != nil {
		return fmt.Errorf("failed to load metadata depth: %w", err)
	}
	initiatorType, err := s.LoadUInt(2)
	if err != nil {
		return fmt.Errorf("failed to load metadata initiator: %w", err)
	}
	if initiatorType != 2 {
		return errors.New("metadata initiator is not a standard address without anycast")
	}
	anycast, err := s.LoadBoolBit()
	if err != nil {
		return fmt.Errorf("failed to load metadata initiator: %w", err)
	}
	if anycast {
		return errors.New("metadata initiator is not a standard address without anycast")
	}
	if err = s.SkipBits(8 + 256); err != nil {
		return fmt.Errorf("failed to load metadata initiator: %w", err)
	}
	if err = s.SkipBits(64); err != nil {
		return fmt.Errorf("failed to load metadata initiator lt: %w", err)
	}
	return nil
}

// claimedMessageLight is the CommonMsgInfo prefix reduced to what the envelope
// consumes: the routing prefixes of both parties, the original forwarding fee
// for the remaining-fee bound and created_lt.
type claimedMessageLight struct {
	source      msgpool.AccountPrefix
	destination msgpool.AccountPrefix
	fwdFee      claimedCoins
	createdLT   uint64
}

// parseClaimedMessageLight walks the same int_msg_info$0 prefix as
// semanticInternalMessageInfo.LoadFromCell — every field is consumed in order
// and with the same failure points — and stops after created_at, like it. The
// extra-currency dictionary root is taken as a reference and never opened,
// which is the property the neighbor-queue read set depends on (see
// semanticQueueLeafCells).
func parseClaimedMessageLight(message *cell.Cell) (claimedMessageLight, error) {
	var s cell.Slice
	if err := message.BeginParseInto(&s); err != nil {
		return claimedMessageLight{}, fmt.Errorf("open internal message: %w", err)
	}
	var parsed claimedMessageLight
	if err := parsed.load(&s); err != nil {
		return claimedMessageLight{}, fmt.Errorf("message is not internal: %w", err)
	}
	return parsed, nil
}

func (m *claimedMessageLight) load(s *cell.Slice) error {
	magic, err := s.LoadUInt(1)
	if err != nil {
		return fmt.Errorf("failed to load internal message magic: %w", err)
	}
	if magic != 0 {
		return fmt.Errorf("invalid internal message magic")
	}
	// ihr_disabled, bounce, bounced: consumed, unread.
	if err = s.SkipBits(3); err != nil {
		return fmt.Errorf("failed to load internal message flags: %w", err)
	}
	if m.source, err = loadClaimedAccountPrefix(s); err != nil {
		return fmt.Errorf("failed to load source address: %w", err)
	}
	if m.destination, err = loadClaimedAccountPrefix(s); err != nil {
		return fmt.Errorf("failed to load destination address: %w", err)
	}
	if err = skipClaimedCoins(s); err != nil {
		return fmt.Errorf("failed to load amount: %w", err)
	}
	hasExtra, err := s.LoadBoolBit()
	if err != nil {
		return fmt.Errorf("failed to load extra currencies: %w", err)
	}
	if hasExtra {
		// The dictionary root travels as this reference. Taken, not opened,
		// exactly as LoadOptionalDict takes it.
		if err = s.SkipBitsAndRefs(0, 1); err != nil {
			return fmt.Errorf("failed to load extra currencies: %w", err)
		}
	}
	if err = skipClaimedCoins(s); err != nil {
		return fmt.Errorf("failed to load ihr fee: %w", err)
	}
	if m.fwdFee, err = loadClaimedCoins(s); err != nil {
		return fmt.Errorf("failed to load forward fee: %w", err)
	}
	if m.createdLT, err = s.LoadUInt(64); err != nil {
		return fmt.Errorf("failed to load created lt: %w", err)
	}
	if err = s.SkipBits(32); err != nil {
		return fmt.Errorf("failed to load created at: %w", err)
	}
	return nil
}

// loadClaimedAccountPrefix is LoadAddr composed with
// semanticAccountPrefixFromAddress, without the address allocation between
// them: the routable head is read straight off the slice and the anycast
// rewrite is applied to it in place. The accepted set is the composition's —
// addr_std always, addr_var with at least 64 address bits — and every reject
// of either stage stays a reject here.
func loadClaimedAccountPrefix(s *cell.Slice) (msgpool.AccountPrefix, error) {
	addrType, err := s.LoadUInt(2)
	if err != nil {
		return msgpool.AccountPrefix{}, err
	}
	if addrType != 2 && addrType != 3 {
		// addr_none / addr_extern: the wide path finishes decoding them before
		// semanticAccountPrefixFromAddress rejects them, but rejects them always.
		return msgpool.AccountPrefix{}, fmt.Errorf("%w: address has no internal routing prefix", ErrInvalidInput)
	}
	anycast, err := s.LoadBoolBit()
	if err != nil {
		return msgpool.AccountPrefix{}, fmt.Errorf("failed to load anycast bit: %w", err)
	}
	var depth, rewrite uint64
	if anycast {
		if depth, err = s.LoadUInt(5); err != nil {
			return msgpool.AccountPrefix{}, fmt.Errorf("failed to load depth: %w", err)
		}
		if depth == 0 || depth > 30 {
			return msgpool.AccountPrefix{}, fmt.Errorf("invalid anycast depth: %d", depth)
		}
		if rewrite, err = s.LoadUInt(uint(depth)); err != nil {
			return msgpool.AccountPrefix{}, fmt.Errorf("failed to load prefix: %w", err)
		}
	}
	var workchain int32
	addrBits := uint(256)
	if addrType == 2 {
		wc, wcErr := s.LoadUInt(8)
		if wcErr != nil {
			return msgpool.AccountPrefix{}, fmt.Errorf("failed to load workchain: %w", wcErr)
		}
		workchain = int32(int8(wc))
	} else {
		ln, lnErr := s.LoadUInt(9)
		if lnErr != nil {
			return msgpool.AccountPrefix{}, fmt.Errorf("failed to load len: %w", lnErr)
		}
		wc, wcErr := s.LoadInt(32)
		if wcErr != nil {
			return msgpool.AccountPrefix{}, fmt.Errorf("failed to load workchain: %w", wcErr)
		}
		workchain = int32(wc)
		if ln < 64 {
			return msgpool.AccountPrefix{}, fmt.Errorf("%w: variable account address has no routing prefix", ErrInvalidInput)
		}
		addrBits = uint(ln)
	}
	prefix, err := s.LoadUInt(64)
	if err != nil {
		return msgpool.AccountPrefix{}, fmt.Errorf("failed to load addr data: %w", err)
	}
	if err = s.SkipBits(addrBits - 64); err != nil {
		return msgpool.AccountPrefix{}, fmt.Errorf("failed to load addr data: %w", err)
	}
	if anycast {
		// msgpool.RewriteAnycast over the head 64 bits: the top depth bits are
		// replaced by the anycast rewrite prefix; depth <= 30 keeps this within
		// the loaded word.
		prefix = rewrite<<(64-depth) | prefix&(^uint64(0)>>depth)
	}
	return msgpool.AccountPrefix{Workchain: workchain, Prefix: prefix}, nil
}

// claimedCoins is a VarUInteger 16 held as its wire bytes; ln is 4 bits so the
// value always fits the array. Ordering ignores leading zero bytes, the way
// the big.Int comparison it replaces did.
type claimedCoins struct {
	size  uint8
	value [15]byte
}

func loadClaimedCoins(s *cell.Slice) (claimedCoins, error) {
	ln, err := s.LoadUInt(4)
	if err != nil {
		return claimedCoins{}, err
	}
	coins := claimedCoins{size: uint8(ln)}
	if ln == 0 {
		return coins, nil
	}
	if err = s.LoadSliceInto(coins.value[:ln], uint(ln)*8); err != nil {
		return claimedCoins{}, err
	}
	return coins, nil
}

func skipClaimedCoins(s *cell.Slice) error {
	ln, err := s.LoadUInt(4)
	if err != nil {
		return err
	}
	return s.SkipBits(uint(ln) * 8)
}

// claimedCoinsExceed reports a > b numerically.
func claimedCoinsExceed(a, b claimedCoins) bool {
	av := bytes.TrimLeft(a.value[:a.size], "\x00")
	bv := bytes.TrimLeft(b.value[:b.size], "\x00")
	if len(av) != len(bv) {
		return len(av) > len(bv)
	}
	return bytes.Compare(av, bv) > 0
}
