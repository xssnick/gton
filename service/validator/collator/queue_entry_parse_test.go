package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// parseQueueEntry walks an out-queue entry by bits. It replaced a decode through
// tlb.EnqueuedMsg, tlb.MsgEnvelope and tlb.InternalMessage, which is kept below
// verbatim as parseQueueEntryReference. What the walk opens lands in the
// collated proof, so this file holds it to the decode on three things at once:
// the verdict, the entry it derives, and the cells it opened in their order —
// the last one on refusals too, where the collation aborts anyway, so that the
// walk cannot drift there unnoticed either.

// parseQueueEntryReference is parseQueueEntry as it stood before the bit walk.
func parseQueueEntryReference(value *cell.Slice, key msgpool.QueueKey) (queueEntry, error) {
	var enqueued tlb.EnqueuedMsg
	if err := loadExactSlice(&enqueued, value); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queue entry %x: %v", ErrInvalidInput, key, err)
	}
	var env tlb.MsgEnvelope
	if err := parseExact(&env, enqueued.Msg); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued envelope %x: %v", ErrInvalidInput, key, err)
	}
	if env.CurAddr.Type != tlb.IntermediateAddressRegular || env.NextAddr.Type != tlb.IntermediateAddressRegular {
		return queueEntry{}, fmt.Errorf("%w: queued envelope %x has a non-regular intermediate address", ErrInvalidInput, key)
	}
	var internal tlb.InternalMessage
	if err := parseExact(&internal, env.Msg); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued message %x: %v", ErrInvalidInput, key, err)
	}
	if err := validateQueuedExtraCurrencies(internal.ExtraCurrencies); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued message extra currencies %x: %v", ErrInvalidInput, key, err)
	}
	if internal.StateInit != nil {
		roots := [3]*cell.Cell{internal.StateInit.Code, internal.StateInit.Data}
		if internal.StateInit.Lib != nil && !internal.StateInit.Lib.IsEmpty() {
			roots[2] = internal.StateInit.Lib.AsCell()
		}
		for _, root := range roots {
			if root == nil {
				continue
			}
			var content cell.Slice
			if err := root.BeginParseInto(&content); err != nil {
				return queueEntry{}, fmt.Errorf("%w: decode queued message StateInit %x: %v", ErrInvalidInput, key, err)
			}
		}
	}
	// The reference validator's generated EnqueuedMsg validation opens an
	// indirect message body and every StateInit reference as Anything cells.
	// Parsing InternalMessage only loads those references, so explicitly
	// materialize their roots as part of the same validation closure.
	// Descendants stay opaque, matching Anything.
	var body cell.Slice
	if err := internal.Body.BeginParseInto(&body); err != nil {
		return queueEntry{}, fmt.Errorf("%w: decode queued message body %x: %v", ErrInvalidInput, key, err)
	}
	lt := internal.CreatedLT
	if env.EmittedLT != nil {
		lt = *env.EmittedLT
	}

	source, err := accountPrefixFromAddress(internal.SrcAddr)
	if err != nil {
		return queueEntry{}, fmt.Errorf("%w: queued message %x source: %v", ErrInvalidInput, key, err)
	}
	destination, err := accountPrefixFromAddress(internal.DstAddr)
	if err != nil {
		return queueEntry{}, fmt.Errorf("%w: queued message %x destination: %v", ErrInvalidInput, key, err)
	}
	cur := msgpool.InterpolatePrefix(source, destination, int(env.CurAddr.UseDestBits))
	next := msgpool.InterpolatePrefix(source, destination, int(env.NextAddr.UseDestBits))
	hash := env.Msg.HashKey()
	if msgpool.MakeQueueKey(next, hash) != key {
		return queueEntry{}, fmt.Errorf("%w: queue entry %x key differs from its envelope", ErrInvalidInput, key)
	}

	return queueEntry{
		key:      key,
		envelope: enqueued.Msg,
		msg:      env.Msg,
		descr: tlb.ProcessedMsgDescr{
			CurWorkchain:  cur.Workchain,
			CurPrefix:     cur.Prefix,
			NextWorkchain: next.Workchain,
			NextPrefix:    next.Prefix,
			LT:            lt,
			EnqueuedLT:    enqueued.EnqueuedLT,
			Hash:          hash,
		},
	}, nil
}

// compareQueueEntryParses runs the walk and the reference over one entry, each
// on its own freshly traced copy, and fails on any difference in verdict,
// recorded cells, derived entry or proof. It returns the reference's verdict.
func compareQueueEntryParses(t *testing.T, value *cell.Cell, key msgpool.QueueKey) error {
	t.Helper()

	var walked, decoded queueEntry
	walkOrder, walkUsage, walkErr := recordQueueEntryReads(t, value, func(s *cell.Slice) error {
		var err error
		walked, err = parseQueueEntry(s, key)
		return err
	})
	decodeOrder, decodeUsage, decodeErr := recordQueueEntryReads(t, value, func(s *cell.Slice) error {
		var err error
		decoded, err = parseQueueEntryReference(s, key)
		return err
	})

	if (walkErr == nil) != (decodeErr == nil) {
		t.Fatalf("walk verdict %v, reference verdict %v", walkErr, decodeErr)
	}
	if walkErr != nil && !errors.Is(walkErr, ErrInvalidInput) {
		t.Fatalf("walk refused with an error outside ErrInvalidInput: %v", walkErr)
	}
	if len(walkOrder) != len(decodeOrder) {
		t.Fatalf("walk opened %d cells, reference %d (verdict %v)", len(walkOrder), len(decodeOrder), decodeErr)
	}
	for i := range decodeOrder {
		if walkOrder[i] != decodeOrder[i] {
			t.Fatalf("opened cell %d of %d differs: walk %x, reference %x", i, len(decodeOrder), walkOrder[i], decodeOrder[i])
		}
	}
	if decodeErr != nil {
		return decodeErr
	}

	if walked.key != decoded.key || walked.descr != decoded.descr ||
		walked.envelope.HashKey() != decoded.envelope.HashKey() || walked.msg.HashKey() != decoded.msg.HashKey() {
		t.Fatalf("walk derived key %x descr %+v, reference key %x descr %+v",
			walked.key, walked.descr, decoded.key, decoded.descr)
	}
	if !bytes.Equal(queueEntryProofBOC(t, walkUsage), queueEntryProofBOC(t, decodeUsage)) {
		t.Fatal("walk and reference read sets yield different proofs")
	}
	return nil
}

// referenceQueueKey derives the key an entry belongs under the way the reference
// derives it, so the axis a case breaks is the only thing wrong with it. An
// entry whose source does not route gets the zero key.
//
// A destination that does not route is left zero instead. Under the default
// use_dest_bits of 0 the next hop takes none of its bits, so a case can break
// the destination alone and the entry still carries its own key — the only way
// a refusal of the address itself, rather than of the key, gets exercised.
func referenceQueueKey(value *cell.Cell) msgpool.QueueKey {
	var enqueued tlb.EnqueuedMsg
	var env tlb.MsgEnvelope
	var internal tlb.InternalMessage
	if parseExact(&enqueued, value) != nil || parseExact(&env, enqueued.Msg) != nil || parseExact(&internal, env.Msg) != nil {
		return msgpool.QueueKey{}
	}
	source, err := accountPrefixFromAddress(internal.SrcAddr)
	if err != nil {
		return msgpool.QueueKey{}
	}
	destination, _ := accountPrefixFromAddress(internal.DstAddr)
	return msgpool.MakeQueueKey(msgpool.InterpolatePrefix(source, destination, int(env.NextAddr.UseDestBits)), env.Msg.HashKey())
}

// TestQueueEntryParseMatchesReferenceAcrossShapes drives the shape matrix the
// trace descent is gated on — the axes mainnet queues lack — once under the key
// each entry belongs under and once under one it does not.
func TestQueueEntryParseMatchesReferenceAcrossShapes(t *testing.T) {
	for _, spec := range queueEntryShapeMatrix() {
		if spec.srcAddr == "" {
			spec.srcAddr = "std"
		}
		if spec.dstAddr == "" {
			spec.dstAddr = "std"
		}

		t.Run(spec.name, func(t *testing.T) {
			value, key := buildQueueEntryShape(t, spec)
			if err := compareQueueEntryParses(t, value, key); (err != nil) != spec.parseRejects {
				t.Fatalf("reference verdict %v, the shape expects a refusal: %v", err, spec.parseRejects)
			}

			foreign := key
			foreign[len(foreign)-1] ^= 0xff
			if err := compareQueueEntryParses(t, value, foreign); err == nil {
				t.Fatal("the reference accepted the entry under a key it does not belong under")
			}
		})
	}
}

// queueEntryWire assembles an entry field by field. Every writer defaults to a
// well-formed field, so a case replaces the one field it breaks and the rest of
// the entry stays valid; most of the breakages are wire shapes the TL-B
// builders refuse to produce.
type queueEntryWire struct {
	flags    func(*cell.Builder) // int_msg_info$0 and its three flags
	src      func(*cell.Builder)
	dst      func(*cell.Builder)
	value    func(*cell.Builder) // value:CurrencyCollection
	fees     func(*cell.Builder) // ihr_fee, fwd_fee, created_lt, created_at
	init     func(*cell.Builder)
	body     func(*cell.Builder)
	envelope func(b *cell.Builder, msg *cell.Cell)
	entry    func(b *cell.Builder, envelope *cell.Cell)
}

func defaultQueueEntryWire() queueEntryWire {
	src, dst := malformedQueueEntryAddresses()
	return queueEntryWire{
		flags: func(b *cell.Builder) { b.MustStoreUInt(0b0010, 4) },
		src:   func(b *cell.Builder) { b.MustStoreAddr(src) },
		dst:   func(b *cell.Builder) { b.MustStoreAddr(dst) },
		value: func(b *cell.Builder) { b.MustStoreCoins(0xABABABAB).MustStoreBoolBit(false) },
		fees: func(b *cell.Builder) {
			b.MustStoreCoins(7).MustStoreCoins(11).MustStoreUInt(10_000_001, 64).MustStoreUInt(1_900_000_000, 32)
		},
		init: func(b *cell.Builder) { b.MustStoreBoolBit(false) },
		body: func(b *cell.Builder) {
			b.MustStoreBoolBit(true).MustStoreRef(cell.BeginCell().MustStoreUInt(0xB0D1, 16).EndCell())
		},
		envelope: func(b *cell.Builder, msg *cell.Cell) {
			storeRegularEnvelopeHead(b, 4)
			b.MustStoreRef(msg)
		},
		entry: func(b *cell.Builder, envelope *cell.Cell) {
			b.MustStoreUInt(10_000_777, 64).MustStoreRef(envelope)
		},
	}
}

func (w queueEntryWire) build() *cell.Cell {
	msg := cell.BeginCell()
	for _, field := range []func(*cell.Builder){w.flags, w.src, w.dst, w.value, w.fees, w.init, w.body} {
		field(msg)
	}
	envelope := cell.BeginCell()
	w.envelope(envelope, msg.EndCell())
	entry := cell.BeginCell()
	w.entry(entry, envelope.EndCell())
	return entry.EndCell()
}

// storeRegularEnvelopeHead writes the envelope tag, two regular intermediate
// addresses with use_dest_bits 0 and the remaining forward fee.
func storeRegularEnvelopeHead(b *cell.Builder, tag uint64) {
	b.MustStoreUInt(tag, 4).MustStoreUInt(0, 8).MustStoreUInt(0, 8).MustStoreCoins(100_000)
}

func storeEnvelopeMetadata(b *cell.Builder, magic uint64, initiator func(*cell.Builder)) {
	b.MustStoreUInt(magic, 4).MustStoreUInt(2, 32)
	initiator(b)
	b.MustStoreUInt(9_000_000, 64)
}

// storeAnycastStdAddress writes addr_std$10 with an anycast of the given depth.
// Depths the TL-B allows but LoadAddr refuses are the point.
func storeAnycastStdAddress(b *cell.Builder, depth uint64) {
	b.MustStoreUInt(0b10, 2).MustStoreBoolBit(true).MustStoreUInt(depth, 5)
	b.MustStoreUInt(0x5A5A5A5A&(1<<depth-1), uint(depth))
	b.MustStoreUInt(0, 8).MustStoreSlice(bytes.Repeat([]byte{0x31}, 32), 256)
}

// storeVarAddress writes addr_var$11, with an anycast when anycastDepth is set.
func storeVarAddress(b *cell.Builder, workchain int64, bits uint, anycastDepth uint64) {
	b.MustStoreUInt(0b11, 2).MustStoreBoolBit(anycastDepth != 0)
	if anycastDepth != 0 {
		b.MustStoreUInt(anycastDepth, 5).MustStoreUInt(0xA5A&(1<<anycastDepth-1), uint(anycastDepth))
	}
	b.MustStoreUInt(uint64(bits), 9).MustStoreInt(workchain, 32)
	b.MustStoreSlice(bytes.Repeat([]byte{0x77}, int(bits+7)/8), bits)
}

func stateInitWire(code, data, libraries *cell.Cell) *cell.Builder {
	return cell.BeginCell().MustStoreBoolBit(false).MustStoreBoolBit(false).
		MustStoreMaybeRef(code).MustStoreMaybeRef(data).MustStoreMaybeRef(libraries)
}

func libraryDictRoot(tb testing.TB) *cell.Cell {
	tb.Helper()

	dict := cell.NewDict(256)
	for i := uint64(1); i <= 2; i++ {
		key := cell.BeginCell().MustStoreUInt(i, 256).EndCell()
		value := cell.BeginCell().MustStoreBoolBit(true).MustStoreRef(cell.BeginCell().MustStoreUInt(0x1B+i, 32).EndCell())
		if err := dict.Set(key, value.EndCell()); err != nil {
			tb.Fatal(err)
		}
	}
	root, err := dict.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	return root
}

func prunedQueueCell(tb testing.TB, c *cell.Cell) *cell.Cell {
	tb.Helper()

	pruned, err := cell.CreatePrunedBranch(c, 1, 0)
	if err != nil {
		tb.Fatal(err)
	}
	return pruned
}

// TestQueueEntryParseMatchesReferenceOnWireCases is the refusal gate. Every
// failure point of the three TL-B loaders the walk replaced gets a case, next
// to accepted shapes that sit right beside a refusal: a v2 envelope with
// neither optional field, a remaining fee above the original one, canonical
// variable addresses, a StateInit cell with trailing data, a library root the
// decode never opens.
func TestQueueEntryParseMatchesReferenceOnWireCases(t *testing.T) {
	code := cell.BeginCell().MustStoreUInt(0xC0DE, 32).EndCell()
	data := cell.BeginCell().MustStoreUInt(0xDA7A, 32).EndCell()
	stray := cell.BeginCell().MustStoreUInt(0xDEADBEEF, 32).EndCell()
	src, _ := malformedQueueEntryAddresses()
	messageBody := func(b *cell.Builder) {
		b.MustStoreBoolBit(true).MustStoreRef(cell.BeginCell().MustStoreUInt(0xB0D1, 16).EndCell())
	}
	nothing := func(*cell.Builder) {}

	cases := []struct {
		name     string
		accepted bool
		mutate   func(t *testing.T, w *queueEntryWire)
	}{
		{"well-formed entry", true, func(*testing.T, *queueEntryWire) {}},

		// EnqueuedMsg
		{"entry without its envelope", false, func(_ *testing.T, w *queueEntryWire) {
			w.entry = func(b *cell.Builder, _ *cell.Cell) { b.MustStoreUInt(10_000_777, 64) }
		}},
		{"entry with a second reference", false, func(_ *testing.T, w *queueEntryWire) {
			w.entry = func(b *cell.Builder, env *cell.Cell) {
				b.MustStoreUInt(10_000_777, 64).MustStoreRef(env).MustStoreRef(env)
			}
		}},
		{"entry with a short lt", false, func(_ *testing.T, w *queueEntryWire) {
			w.entry = func(b *cell.Builder, env *cell.Cell) { b.MustStoreUInt(10_000_777, 32).MustStoreRef(env) }
		}},
		{"entry with a trailing bit", false, func(_ *testing.T, w *queueEntryWire) {
			w.entry = func(b *cell.Builder, env *cell.Cell) {
				b.MustStoreUInt(10_000_777, 64).MustStoreBoolBit(true).MustStoreRef(env)
			}
		}},
		{"pruned envelope", false, func(t *testing.T, w *queueEntryWire) {
			w.entry = func(b *cell.Builder, env *cell.Cell) {
				b.MustStoreUInt(10_000_777, 64).MustStoreRef(prunedQueueCell(t, env))
			}
		}},

		// MsgEnvelope
		{"pruned message", false, func(t *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 4)
				b.MustStoreRef(prunedQueueCell(t, msg))
			}
		}},
		{"envelope with an unknown tag", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 7)
				b.MustStoreRef(msg)
			}
		}},
		{"truncated envelope", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) { b.MustStoreUInt(4, 4).MustStoreRef(msg) }
		}},
		{"envelope without its message", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, _ *cell.Cell) { storeRegularEnvelopeHead(b, 4) }
		}},
		{"envelope with a second reference", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 4)
				b.MustStoreRef(msg).MustStoreRef(stray)
			}
		}},
		{"envelope with trailing bits", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 4)
				b.MustStoreRef(msg).MustStoreUInt(1, 3)
			}
		}},
		{"simple current intermediate address", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				b.MustStoreUInt(4, 4).MustStoreUInt(0b10, 2).MustStoreInt(0, 8).MustStoreUInt(0x8000000000000000, 64)
				b.MustStoreUInt(0, 8).MustStoreCoins(100_000).MustStoreRef(msg)
			}
		}},
		{"ext next intermediate address inside int8", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				b.MustStoreUInt(4, 4).MustStoreUInt(0, 8).MustStoreUInt(0b11, 2).MustStoreInt(5, 32).MustStoreUInt(0, 64)
				b.MustStoreCoins(100_000).MustStoreRef(msg)
			}
		}},
		{"ext next intermediate address outside int8", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				b.MustStoreUInt(4, 4).MustStoreUInt(0, 8).MustStoreUInt(0b11, 2).MustStoreInt(1000, 32).MustStoreUInt(0, 64)
				b.MustStoreCoins(100_000).MustStoreRef(msg)
			}
		}},
		{"use_dest_bits above 96", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				b.MustStoreUInt(4, 4).MustStoreUInt(97, 8).MustStoreUInt(0, 8).MustStoreCoins(100_000).MustStoreRef(msg)
			}
		}},
		{"remaining fee above the forward fee", true, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				b.MustStoreUInt(4, 4).MustStoreUInt(0, 8).MustStoreUInt(0, 8)
				b.MustStoreBigCoins(new(big.Int).Lsh(big.NewInt(1), 100)).MustStoreRef(msg)
			}
		}},
		{"v2 envelope with neither emitted lt nor metadata", true, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(false).MustStoreBoolBit(false)
			}
		}},
		{"v2 envelope with emitted lt", true, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(true).MustStoreUInt(10_000_500, 64).MustStoreBoolBit(false)
			}
		}},
		{"v2 envelope with a truncated emitted lt", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(true).MustStoreUInt(10_000_500, 32)
			}
		}},
		{"v2 envelope with metadata", true, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(false).MustStoreBoolBit(true)
				storeEnvelopeMetadata(b, 0, func(b *cell.Builder) { b.MustStoreAddr(src) })
			}
		}},
		{"v2 metadata with a bad magic", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(false).MustStoreBoolBit(true)
				storeEnvelopeMetadata(b, 1, func(b *cell.Builder) { b.MustStoreAddr(src) })
			}
		}},
		{"v2 metadata with an anycast initiator", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(false).MustStoreBoolBit(true)
				storeEnvelopeMetadata(b, 0, func(b *cell.Builder) { storeAnycastStdAddress(b, 8) })
			}
		}},
		{"v2 metadata with a variable initiator", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(false).MustStoreBoolBit(true)
				storeEnvelopeMetadata(b, 0, func(b *cell.Builder) { storeVarAddress(b, 1000, 256, 0) })
			}
		}},
		{"v2 metadata truncated", false, func(_ *testing.T, w *queueEntryWire) {
			w.envelope = func(b *cell.Builder, msg *cell.Cell) {
				storeRegularEnvelopeHead(b, 5)
				b.MustStoreRef(msg).MustStoreBoolBit(false).MustStoreBoolBit(true).MustStoreUInt(0, 4).MustStoreUInt(2, 32)
			}
		}},

		// int_msg_info
		{"message that is not internal", false, func(_ *testing.T, w *queueEntryWire) {
			w.flags = func(b *cell.Builder) { b.MustStoreUInt(0b1010, 4) }
		}},
		{"truncated message", false, func(_ *testing.T, w *queueEntryWire) {
			w.fees, w.init, w.body = nothing, nothing, nothing
		}},
		{"truncated amount", false, func(_ *testing.T, w *queueEntryWire) {
			w.value = func(b *cell.Builder) { b.MustStoreUInt(15, 4).MustStoreUInt(0xFFFF, 16) }
			w.fees, w.init, w.body = nothing, nothing, nothing
		}},
		{"source with anycast depth 0", false, func(_ *testing.T, w *queueEntryWire) {
			w.src = func(b *cell.Builder) { storeAnycastStdAddress(b, 0) }
		}},
		{"source with anycast depth 31", false, func(_ *testing.T, w *queueEntryWire) {
			w.src = func(b *cell.Builder) { storeAnycastStdAddress(b, 31) }
		}},
		{"source with anycast depth 12", true, func(_ *testing.T, w *queueEntryWire) {
			w.src = func(b *cell.Builder) { storeAnycastStdAddress(b, 12) }
		}},
		{"absent source", false, func(_ *testing.T, w *queueEntryWire) {
			w.src = func(b *cell.Builder) { b.MustStoreUInt(0, 2) }
		}},
		{"external destination", false, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { b.MustStoreUInt(1, 2).MustStoreUInt(40, 9).MustStoreUInt(0x3333333333, 40) }
		}},
		{"variable destination in basechain", false, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { storeVarAddress(b, 0, 256, 0) }
		}},
		{"variable destination in masterchain", false, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { storeVarAddress(b, -1, 100, 0) }
		}},
		{"variable 256-bit destination inside int8", false, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { storeVarAddress(b, 5, 256, 0) }
		}},
		{"variable 255-bit destination inside int8", true, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { storeVarAddress(b, 5, 255, 0) }
		}},
		{"variable source outside int8", true, func(_ *testing.T, w *queueEntryWire) {
			w.src = func(b *cell.Builder) { storeVarAddress(b, 1000, 256, 0) }
		}},
		{"variable 64-bit source outside int8", true, func(_ *testing.T, w *queueEntryWire) {
			w.src = func(b *cell.Builder) { storeVarAddress(b, 1000, 64, 0) }
		}},
		{"variable 64-bit destination outside int8", true, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { storeVarAddress(b, 1000, 64, 0) }
		}},
		{"variable anycast destination outside int8", true, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { storeVarAddress(b, 1000, 100, 12) }
		}},
		{"variable destination shorter than 64 bits", false, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) { storeVarAddress(b, 1000, 63, 0) }
		}},
		{"variable destination cut short", false, func(_ *testing.T, w *queueEntryWire) {
			w.dst = func(b *cell.Builder) {
				b.MustStoreUInt(0b11, 2).MustStoreBoolBit(false).MustStoreUInt(500, 9).MustStoreInt(1000, 32)
			}
			w.value, w.fees, w.init, w.body = nothing, nothing, nothing, nothing
		}},
		{"extra currencies", true, func(t *testing.T, w *queueEntryWire) {
			extra := extraCurrencyDict(t, map[uint64]*cell.Cell{
				100: canonicalExtraCurrencyValue(), 237: canonicalExtraCurrencyValue(), 4000: canonicalExtraCurrencyValue(),
			})
			w.value = func(b *cell.Builder) { b.MustStoreCoins(5).MustStoreBoolBit(true).MustStoreRef(extra) }
		}},
		{"extra currency with a leading zero byte", false, func(t *testing.T, w *queueEntryWire) {
			extra := extraCurrencyDict(t, map[uint64]*cell.Cell{
				100: cell.BeginCell().MustStoreUInt(3, 5).MustStoreUInt(0x002233, 24).EndCell(),
			})
			w.value = func(b *cell.Builder) { b.MustStoreCoins(5).MustStoreBoolBit(true).MustStoreRef(extra) }
		}},
		{"extra currency value carrying a reference", false, func(t *testing.T, w *queueEntryWire) {
			extra := extraCurrencyDict(t, map[uint64]*cell.Cell{
				100: cell.BeginCell().MustStoreUInt(3, 5).MustStoreUInt(0x112233, 24).MustStoreRef(stray).EndCell(),
			})
			w.value = func(b *cell.Builder) { b.MustStoreCoins(5).MustStoreBoolBit(true).MustStoreRef(extra) }
		}},
		{"extra currency fork with a third reference", true, func(t *testing.T, w *queueEntryWire) {
			root := extraCurrencyDict(t, map[uint64]*cell.Cell{
				100: canonicalExtraCurrencyValue(), 237: canonicalExtraCurrencyValue(),
			})
			s := root.MustBeginParse()
			bits, label, err := s.RestBits()
			if err != nil {
				t.Fatal(err)
			}
			forked := cell.BeginCell().MustStoreSlice(label, bits).
				MustStoreRef(root.MustPeekRef(0)).MustStoreRef(root.MustPeekRef(1)).MustStoreRef(prunedQueueCell(t, stray))
			extra := forked.EndCell()
			w.value = func(b *cell.Builder) { b.MustStoreCoins(5).MustStoreBoolBit(true).MustStoreRef(extra) }
		}},
		{"extra currencies flag without its reference", false, func(_ *testing.T, w *queueEntryWire) {
			w.value = func(b *cell.Builder) { b.MustStoreCoins(5).MustStoreBoolBit(true) }
		}},
		{"StateInit by reference", true, func(t *testing.T, w *queueEntryWire) {
			init := stateInitWire(code, data, libraryDictRoot(t)).EndCell()
			w.init = func(b *cell.Builder) { b.MustStoreBoolBit(true).MustStoreBoolBit(true).MustStoreRef(init) }
		}},
		{"StateInit by reference with trailing data", true, func(_ *testing.T, w *queueEntryWire) {
			init := stateInitWire(code, nil, nil).MustStoreUInt(0xFF, 8).MustStoreRef(stray).EndCell()
			w.init = func(b *cell.Builder) { b.MustStoreBoolBit(true).MustStoreBoolBit(true).MustStoreRef(init) }
		}},
		{"StateInit by reference missing its code", false, func(_ *testing.T, w *queueEntryWire) {
			init := cell.BeginCell().MustStoreUInt(0b00100, 5).EndCell()
			w.init = func(b *cell.Builder) { b.MustStoreBoolBit(true).MustStoreBoolBit(true).MustStoreRef(init) }
		}},
		{"StateInit by reference with an empty library root", true, func(_ *testing.T, w *queueEntryWire) {
			init := stateInitWire(code, data, cell.BeginCell().EndCell()).EndCell()
			w.init = func(b *cell.Builder) { b.MustStoreBoolBit(true).MustStoreBoolBit(true).MustStoreRef(init) }
		}},
		{"inline StateInit with code, data and libraries", true, func(t *testing.T, w *queueEntryWire) {
			init := stateInitWire(code, data, libraryDictRoot(t))
			w.init = func(b *cell.Builder) { b.MustStoreBoolBit(true).MustStoreBoolBit(false).MustStoreBuilder(init) }
		}},
		{"inline StateInit with an empty library root", true, func(_ *testing.T, w *queueEntryWire) {
			init := stateInitWire(nil, data, cell.BeginCell().EndCell())
			w.init = func(b *cell.Builder) { b.MustStoreBoolBit(true).MustStoreBoolBit(false).MustStoreBuilder(init) }
		}},
		{"inline StateInit cut short", false, func(_ *testing.T, w *queueEntryWire) {
			w.init = func(b *cell.Builder) {
				b.MustStoreBoolBit(true).MustStoreBoolBit(false).MustStoreBoolBit(true).MustStoreUInt(1, 2)
			}
			w.body = nothing
		}},
		{"inline body", true, func(_ *testing.T, w *queueEntryWire) {
			w.body = func(b *cell.Builder) { b.MustStoreBoolBit(false).MustStoreUInt(0xB0D1, 16) }
		}},
		{"inline body with a reference", true, func(_ *testing.T, w *queueEntryWire) {
			w.body = func(b *cell.Builder) { b.MustStoreBoolBit(false).MustStoreUInt(0xB0D1, 16).MustStoreRef(stray) }
		}},
		{"body flag without its reference", false, func(_ *testing.T, w *queueEntryWire) {
			w.body = func(b *cell.Builder) { b.MustStoreBoolBit(true) }
		}},
		{"trailing bits after an indirect body", false, func(_ *testing.T, w *queueEntryWire) {
			w.body = func(b *cell.Builder) {
				messageBody(b)
				b.MustStoreUInt(0x5A5A, 16)
			}
		}},
		{"trailing reference after an indirect body", false, func(_ *testing.T, w *queueEntryWire) {
			w.body = func(b *cell.Builder) {
				messageBody(b)
				b.MustStoreRef(stray)
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := defaultQueueEntryWire()
			tc.mutate(t, &wire)
			value := wire.build()

			err := compareQueueEntryParses(t, value, referenceQueueKey(value))
			if (err == nil) != tc.accepted {
				t.Fatalf("reference verdict %v, the case expects acceptance: %v", err, tc.accepted)
			}
			t.Logf("verdict: %v", err)
		})
	}
}

// referenceCleanupOutcome is what the cleanup phase leaves behind that the rest
// of the block, and the proof, can see.
type referenceCleanupOutcome struct {
	cleaned    uint32
	stop       CleanupStopReason
	blockFull  bool
	queueRoot  cell.Hash
	descrRoot  cell.Hash
	readHashes []cell.Hash
}

// cleanupOutQueueWithReferenceParse is cleanupOutQueue over the reference decode.
// Only the round-robin is copied: the merge branch and the own-shard frontier
// capture change nothing the fixtures below observe.
func (c *collation) cleanupOutQueueWithReferenceParse() error {
	neighbors, err := c.effectiveNeighbors()
	if err != nil {
		return err
	}
	parts := make([]cleanupPart, 0, len(neighbors))
	for i := range neighbors {
		stream, streamErr := c.neighborQueueStream(neighbors[i].Shard)
		if streamErr != nil {
			return streamErr
		}
		parts = append(parts, cleanupPart{neighbor: neighbors[i], stream: stream})
	}

	stop := CleanupStopExhausted
	for i := 0; len(parts) > 0; {
		if c.blockFull {
			stop = CleanupStopBlockFull
			break
		}
		if c.queueCleanupExpired() {
			stop = CleanupStopBudget
			break
		}
		if i == len(parts) {
			i = 0
		}

		part := &parts[i]
		if !part.stream.Next() {
			if err = part.stream.Err(); err != nil {
				return err
			}
			parts[i] = parts[len(parts)-1]
			parts = parts[:len(parts)-1]
			continue
		}
		view := part.stream.View()
		var key msgpool.QueueKey
		if err = view.Key.LoadSliceInto(key[:], 352); err != nil {
			return err
		}
		entry, err := parseQueueEntryReference(&view.Value, key)
		if err != nil {
			return err
		}
		processed, err := c.shardEndLT.alreadyProcessed(
			part.neighbor.Processed,
			part.neighbor.Shard.Workchain,
			part.neighbor.Shard.Shard,
			&entry.descr,
		)
		if err != nil {
			return err
		}
		if !processed {
			parts[i] = parts[len(parts)-1]
			parts = parts[:len(parts)-1]
			continue
		}
		if err = c.dequeueDelivered(entry, part.neighbor.EndLT); err != nil {
			return err
		}
		c.stats.QueueCleaned++
		i++
	}
	c.stats.QueueCleanupStop = stop
	if err = c.flushQueueDeletes(); err != nil {
		return err
	}
	return c.limits.addProof(c.outQueue.RootCell())
}

func runCleanupPhase(t *testing.T, req ShardRequest, cleanup func(*collation) error) referenceCleanupOutcome {
	t.Helper()

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// The preamble a build runs before cleanup, so the block-full gate sees the
	// limits it sees in a build.
	if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
		t.Fatal(err)
	}
	if err = c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		t.Fatal(err)
	}
	if err = cleanup(c); err != nil {
		t.Fatal(err)
	}
	if err = c.flushDescriptors(); err != nil {
		t.Fatal(err)
	}

	hashes := c.usage.Hashes()
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	return referenceCleanupOutcome{
		cleaned:    c.stats.QueueCleaned,
		stop:       c.stats.QueueCleanupStop,
		blockFull:  c.blockFull,
		queueRoot:  dictionaryRootHash(c.outQueue.RootCell()),
		descrRoot:  dictionaryRootHash(c.outMessages.RootCell()),
		readHashes: hashes,
	}
}

// dictionaryRootHash names a dictionary by its root, the zero hash standing for
// an empty one: a cleanup that drains the queue leaves no root at all.
func dictionaryRootHash(root *cell.Cell) cell.Hash {
	if root == nil {
		return cell.Hash{}
	}
	return root.HashKey()
}

// TestQueueCleanupMatchesReferenceParseOnFixtures runs the cleanup phase over the
// cleanup fixtures twice, once as production runs it and once over the reference
// decode, and requires the same dequeues, the same stop, the same queue and
// OutMsgDescr and the same recorded read set. Every entry of each predecessor
// queue is then compared one by one, including those cleanup never reaches.
func TestQueueCleanupMatchesReferenceParseOnFixtures(t *testing.T) {
	fixtures := []struct {
		name string
		req  func(*testing.T) ShardRequest
	}{
		{"replay full collated", func(t *testing.T) ShardRequest { return benchRequest(t, replayProfile) }},
		{"deep queue full collated", func(t *testing.T) ShardRequest {
			return benchRequest(t, benchProfile{name: "deep-queue-3000-full-collated", accounts: 2_000, queued: 3_000, fullCollated: true})
		}},
		{"multi-neighbor", func(t *testing.T) ShardRequest { return multiNeighborQueueRequest(t, 140, 20, 8, false) }},
		{"multi-neighbor full collated", func(t *testing.T) ShardRequest { return multiNeighborQueueRequest(t, 140, 20, 8, true) }},
	}

	stops := map[CleanupStopReason]bool{}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			req := fixture.req(t)

			walked := runCleanupPhase(t, req, (*collation).cleanupOutQueue)
			decoded := runCleanupPhase(t, req, (*collation).cleanupOutQueueWithReferenceParse)
			if walked.cleaned != decoded.cleaned || walked.stop != decoded.stop || walked.blockFull != decoded.blockFull {
				t.Fatalf("walk dequeued %d and stopped at %v (block full %v), reference %d at %v (block full %v)",
					walked.cleaned, walked.stop, walked.blockFull, decoded.cleaned, decoded.stop, decoded.blockFull)
			}
			if walked.queueRoot != decoded.queueRoot || walked.descrRoot != decoded.descrRoot {
				t.Fatal("walk and reference left different out queues or OutMsgDescr")
			}
			if len(walked.readHashes) != len(decoded.readHashes) {
				t.Fatalf("walk recorded %d cells, reference %d", len(walked.readHashes), len(decoded.readHashes))
			}
			for i := range decoded.readHashes {
				if walked.readHashes[i] != decoded.readHashes[i] {
					t.Fatalf("recorded read sets differ at %d: walk %x, reference %x", i, walked.readHashes[i], decoded.readHashes[i])
				}
			}
			stops[walked.stop] = true
			t.Logf("dequeued %d, stop %v, recorded %d cells", walked.cleaned, walked.stop, len(walked.readHashes))

			c, err := testBuilder().prepare(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			candidates, err := c.queueCandidates()
			if err != nil {
				t.Fatal(err)
			}
			if len(candidates) == 0 {
				t.Fatal("the fixture queue is empty")
			}
			for i := range candidates {
				var value cell.Slice
				if err = c.outQueue.LoadValueByBytesKeyInto(candidates[i].key[:], &value); err != nil {
					t.Fatal(err)
				}
				valueCell, err := value.ToCell()
				if err != nil {
					t.Fatal(err)
				}
				t.Run(strconv.Itoa(i), func(t *testing.T) {
					if err := compareQueueEntryParses(t, valueCell, candidates[i].key); err != nil {
						t.Fatalf("the reference refused fixture entry %x: %v", candidates[i].key, err)
					}
				})
			}
		})
	}
	if !stops[CleanupStopExhausted] || !stops[CleanupStopBlockFull] {
		t.Fatalf("the fixtures reached stops %v; both exhaustion and the block-full gate have to be exercised", stops)
	}
}
