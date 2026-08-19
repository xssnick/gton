package collator

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// The out-queue trace closure descends the entry structurally instead of
// decoding a TL-B value it would throw away. Since the cells it opens end up in
// the collated proof, the descent has to open the same cells in the same order
// as the parse it replaced, on every shape and not only on the shapes mainnet
// happens to carry today: mainnet basechain has no extra currencies, no
// emitted_lt and no non-regular intermediate addresses, so a fixture-only check
// would prove almost nothing. The matrix below drives the axes the fixture
// lacks.

type queueEntryShape struct {
	name string

	// message
	extraCurrencies int    // extra-currency entries; 0 leaves the dict absent
	initForm        string // "none", "inline" or "ref"
	initCode        bool
	initData        bool
	initLibraries   int // library dictionary entries; 0 leaves it absent
	initSplitDepth  bool
	initTickTock    bool
	bodyRef         bool
	bodyBits        uint
	gramsBytes      int
	srcAddr         string // "std", "anycast", "var", "none", "ext"
	dstAddr         string

	// envelope
	v2        bool
	emittedLT bool
	metadata  bool
	curBits   uint8
	nextBits  uint8

	// parseQueueEntry validates things the descent deliberately does not, so
	// some shapes only decode on one side. The recorded cells must still match:
	// every check the parse makes past this point runs after its last read.
	parseRejects bool
}

func storeShapeAddress(tb testing.TB, b *cell.Builder, form string, addr *address.Address) {
	tb.Helper()

	must := func(err error) {
		tb.Helper()
		if err != nil {
			tb.Fatalf("store %s address: %v", form, err)
		}
	}
	switch form {
	case "std":
		must(b.StoreAddr(addr))
	case "anycast": // addr_std$10 with anycast, depth 8
		must(b.StoreUInt(2, 2))
		must(b.StoreBoolBit(true))
		must(b.StoreUInt(8, 5))
		must(b.StoreUInt(0x5A, 8))
		must(b.StoreUInt(0, 8)) // workchain
		must(b.StoreSlice(addr.Data(), 256))
	case "var": // addr_var$11, 100-bit address, no anycast
		must(b.StoreUInt(3, 2))
		must(b.StoreBoolBit(false))
		must(b.StoreUInt(100, 9))
		must(b.StoreUInt(0, 32)) // workchain
		must(b.StoreSlice(bytes.Repeat([]byte{0x77}, 13), 100))
	case "none": // addr_none$00
		must(b.StoreUInt(0, 2))
	case "ext": // addr_extern$01, 40 bits
		must(b.StoreUInt(1, 2))
		must(b.StoreUInt(40, 9))
		must(b.StoreSlice(bytes.Repeat([]byte{0x33}, 5), 40))
	default:
		tb.Fatalf("unknown address form %q", form)
	}
}

func buildQueueEntryShapeMessage(tb testing.TB, spec queueEntryShape, src, dst *address.Address) *cell.Cell {
	tb.Helper()

	must := func(err error) {
		tb.Helper()
		if err != nil {
			tb.Fatalf("build message %s: %v", spec.name, err)
		}
	}

	b := cell.BeginCell()
	must(b.StoreUInt(0, 1))     // int_msg_info$0
	must(b.StoreBoolBit(false)) // ihr_disabled
	must(b.StoreBoolBit(true))  // bounce
	must(b.StoreBoolBit(false)) // bounced
	storeShapeAddress(tb, b, spec.srcAddr, src)
	storeShapeAddress(tb, b, spec.dstAddr, dst)
	must(b.StoreBigCoins(new(big.Int).SetBytes(bytes.Repeat([]byte{0xAB}, spec.gramsBytes))))

	if spec.extraCurrencies > 0 {
		extra := cell.NewDict(32)
		for i := 0; i < spec.extraCurrencies; i++ {
			value := cell.BeginCell()
			must(value.StoreUInt(3, 5))
			must(value.StoreUInt(0x112233, 24))
			key := cell.BeginCell()
			must(key.StoreUInt(uint64(100+i*37), 32))
			must(extra.Set(key.EndCell(), value.EndCell()))
		}
		must(b.StoreDict(extra))
	} else {
		must(b.StoreBoolBit(false))
	}

	must(b.StoreBigCoins(big.NewInt(7)))  // ihr_fee
	must(b.StoreBigCoins(big.NewInt(11))) // fwd_fee
	must(b.StoreUInt(10_000_001, 64))     // created_lt
	must(b.StoreUInt(1_900_000_000, 32))  // created_at

	if spec.initForm == "none" {
		must(b.StoreBoolBit(false))
	} else {
		must(b.StoreBoolBit(true))
		init := cell.BeginCell()
		if spec.initSplitDepth {
			must(init.StoreBoolBit(true))
			must(init.StoreUInt(9, 5))
		} else {
			must(init.StoreBoolBit(false))
		}
		if spec.initTickTock {
			must(init.StoreBoolBit(true))
			must(init.StoreUInt(2, 2))
		} else {
			must(init.StoreBoolBit(false))
		}
		if spec.initCode {
			must(init.StoreMaybeRef(cell.BeginCell().MustStoreUInt(0xC0DE, 32).EndCell()))
		} else {
			must(init.StoreBoolBit(false))
		}
		if spec.initData {
			must(init.StoreMaybeRef(cell.BeginCell().MustStoreUInt(0xDA7A, 32).EndCell()))
		} else {
			must(init.StoreBoolBit(false))
		}
		if spec.initLibraries > 0 {
			libs := cell.NewDict(256)
			for i := 0; i < spec.initLibraries; i++ {
				key := cell.BeginCell()
				must(key.StoreUInt(uint64(i+1), 256))
				value := cell.BeginCell()
				must(value.StoreBoolBit(true))
				must(value.StoreRef(cell.BeginCell().MustStoreUInt(uint64(0x1B+i), 32).EndCell()))
				must(libs.Set(key.EndCell(), value.EndCell()))
			}
			must(init.StoreDict(libs))
		} else {
			must(init.StoreBoolBit(false))
		}
		if spec.initForm == "ref" {
			must(b.StoreBoolBit(true))
			must(b.StoreRef(init.EndCell()))
		} else {
			must(b.StoreBoolBit(false))
			must(b.StoreBuilder(init))
		}
	}

	body := cell.BeginCell()
	must(body.StoreUInt(0xB0D1, 16))
	if spec.bodyBits > 16 {
		must(body.StoreSlice(bytes.Repeat([]byte{0x5A}, int((spec.bodyBits-16)/8)), spec.bodyBits-16))
	}
	if spec.bodyRef {
		must(b.StoreBoolBit(true))
		must(b.StoreRef(body.EndCell()))
	} else {
		must(b.StoreBoolBit(false))
		must(b.StoreBuilder(body))
	}
	return b.EndCell()
}

// buildQueueEntryShape returns one synthetic EnqueuedMsg cell and the queue key
// that entry belongs under.
func buildQueueEntryShape(tb testing.TB, spec queueEntryShape) (*cell.Cell, msgpool.QueueKey) {
	tb.Helper()

	src := address.NewAddress(0, 0, bytes.Repeat([]byte{0x31}, 32))
	dst := address.NewAddress(0, 0, bytes.Repeat([]byte{0x32}, 32))
	msg := buildQueueEntryShapeMessage(tb, spec, src, dst)

	env := tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: spec.curBits},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: spec.nextBits},
		FwdFeeRemaining: tlb.FromNanoTONU(100_000),
		Msg:             msg,
		V2:              spec.v2,
	}
	if spec.emittedLT {
		lt := uint64(10_000_500)
		env.EmittedLT = &lt
	}
	if spec.metadata {
		env.Metadata = &tlb.MsgMetadata{Depth: 2, Initiator: src, InitiatorLT: 9_000_000}
	}
	envCell, err := env.ToCell()
	if err != nil {
		tb.Fatalf("build envelope %s: %v", spec.name, err)
	}
	value, err := (tlb.EnqueuedMsg{EnqueuedLT: 10_000_777, Msg: envCell}).ToCell()
	if err != nil {
		tb.Fatalf("build entry %s: %v", spec.name, err)
	}

	source, err := accountPrefixFromAddress(src)
	if err != nil {
		tb.Fatalf("source prefix: %v", err)
	}
	destination, err := accountPrefixFromAddress(dst)
	if err != nil {
		tb.Fatalf("destination prefix: %v", err)
	}
	next := msgpool.InterpolatePrefix(source, destination, int(spec.nextBits))
	return value, msgpool.MakeQueueKey(next, msg.HashKey())
}

// recordQueueEntryReads runs fn over a freshly traced copy of value and reports
// the cells it opened in the order it opened them, together with the proof that
// read set yields. Both are needed: the proof is what actually ships, and the
// order is what this project has already seen move produced bytes when only the
// finishing order changed.
func recordQueueEntryReads(tb testing.TB, value *cell.Cell, fn func(*cell.Slice) error) ([]cell.Hash, *cell.ReadSet, error) {
	tb.Helper()

	usage := cell.NewReadSet(value)
	var order []cell.Hash
	usage.SetRecordCallback(func(c *cell.Cell) { order = append(order, c.HashKey()) })

	var entry cell.Slice
	if err := usage.Root().BeginParseInto(&entry); err != nil {
		tb.Fatalf("open queue entry: %v", err)
	}
	runErr := fn(&entry)

	usage.SetRecordCallback(nil)
	return order, usage, runErr
}

func queueEntryProofBOC(tb testing.TB, usage *cell.ReadSet) []byte {
	tb.Helper()

	proof, err := usage.Proof()
	if err != nil {
		tb.Fatalf("read set proof: %v", err)
	}
	return proof.ToBOC()
}

func queueEntryShapeMatrix() []queueEntryShape {
	return []queueEntryShape{
		{name: "inline body", gramsBytes: 4, initForm: "none", bodyBits: 16},
		{name: "ref body", gramsBytes: 4, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "init inline code+data", gramsBytes: 4, initForm: "inline", initCode: true, initData: true, bodyRef: true, bodyBits: 800},
		{name: "init inline code only", gramsBytes: 4, initForm: "inline", initCode: true, bodyBits: 16},
		{name: "init inline libraries", gramsBytes: 4, initForm: "inline", initLibraries: 3, bodyBits: 16},
		{name: "init inline code+data+lib", gramsBytes: 4, initForm: "inline", initCode: true, initData: true, initLibraries: 2, bodyBits: 16},
		{name: "init inline split depth + ticktock", gramsBytes: 4, initForm: "inline", initSplitDepth: true, initTickTock: true, initCode: true, initData: true, bodyRef: true, bodyBits: 800},
		{name: "init ref full", gramsBytes: 4, initForm: "ref", initCode: true, initData: true, initLibraries: 4, bodyRef: true, bodyBits: 800},
		{name: "init ref empty", gramsBytes: 4, initForm: "ref", bodyBits: 16},
		{name: "init ref split depth + ticktock", gramsBytes: 4, initForm: "ref", initSplitDepth: true, initTickTock: true, initCode: true, bodyBits: 16},
		{name: "extra currencies 1", gramsBytes: 4, extraCurrencies: 1, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "extra currencies 3", gramsBytes: 4, extraCurrencies: 3, initForm: "none", bodyBits: 16},
		{name: "extra currencies 5", gramsBytes: 4, extraCurrencies: 5, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "extra currencies 7", gramsBytes: 4, extraCurrencies: 7, initForm: "none", bodyRef: true, bodyBits: 800},
		// The one ordering hazard: the parse opens a by-reference StateInit
		// while decoding the message, before it walks the extra-currency dict.
		{name: "extra currencies + init ref + ref body", gramsBytes: 4, extraCurrencies: 5, initForm: "ref", initCode: true, initData: true, initLibraries: 2, bodyRef: true, bodyBits: 800},
		{name: "extra currencies + init ref + inline body", gramsBytes: 4, extraCurrencies: 7, initForm: "ref", initCode: true, initData: true, initLibraries: 3, bodyBits: 16},
		{name: "extra currencies + init inline + inline body", gramsBytes: 4, extraCurrencies: 3, initForm: "inline", initCode: true, initData: true, bodyBits: 16},
		{name: "grams 15 bytes", gramsBytes: 15, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "grams 1 byte", gramsBytes: 1, initForm: "none", bodyBits: 16},
		{name: "envelope v2 plain", gramsBytes: 4, v2: true, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "envelope v2 emitted lt", gramsBytes: 4, v2: true, emittedLT: true, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "envelope v2 metadata", gramsBytes: 4, v2: true, metadata: true, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "envelope v2 emitted lt + metadata + init ref", gramsBytes: 4, v2: true, emittedLT: true, metadata: true, initForm: "ref", initCode: true, initData: true, bodyRef: true, bodyBits: 800},
		{name: "use dest bits 96", gramsBytes: 4, curBits: 96, nextBits: 96, initForm: "none", bodyRef: true, bodyBits: 800},
		{name: "use dest bits 0 then 96", gramsBytes: 4, curBits: 0, nextBits: 96, initForm: "none", bodyBits: 16},
		// Address forms the fixture never carries. The parse re-derives routing
		// from the addresses and refuses anything but a standard pair, so it
		// rejects these — after its last read, which is what the matrix checks.
		{name: "anycast source", gramsBytes: 4, srcAddr: "anycast", initForm: "none", bodyRef: true, bodyBits: 800, parseRejects: true},
		{name: "var destination", gramsBytes: 4, dstAddr: "var", initForm: "none", bodyRef: true, bodyBits: 800, parseRejects: true},
		{name: "absent source", gramsBytes: 4, srcAddr: "none", initForm: "none", bodyBits: 16, parseRejects: true},
		{name: "external destination", gramsBytes: 4, dstAddr: "ext", initForm: "none", bodyRef: true, bodyBits: 800, parseRejects: true},
		{name: "anycast source + var destination + init ref + extra currencies", gramsBytes: 4, srcAddr: "anycast", dstAddr: "var", extraCurrencies: 4, initForm: "ref", initCode: true, initData: true, initLibraries: 2, bodyRef: true, bodyBits: 800, parseRejects: true},
	}
}

// TestQueueEntryDescentMatchesParseAcrossShapes is the gate that decides whether
// the descent may replace the parse: same cells, same order, same proof bytes.
func TestQueueEntryDescentMatchesParseAcrossShapes(t *testing.T) {
	for _, spec := range queueEntryShapeMatrix() {
		if spec.srcAddr == "" {
			spec.srcAddr = "std"
		}
		if spec.dstAddr == "" {
			spec.dstAddr = "std"
		}

		t.Run(spec.name, func(t *testing.T) {
			value, key := buildQueueEntryShape(t, spec)

			parseOrder, parseUsage, parseErr := recordQueueEntryReads(t, value, func(s *cell.Slice) error {
				_, err := parseQueueEntry(s, key)
				return err
			})
			switch {
			case parseErr != nil && !spec.parseRejects:
				t.Fatalf("parseQueueEntry: %v", parseErr)
			case parseErr == nil && spec.parseRejects:
				t.Fatalf("parseQueueEntry accepted a shape it was expected to reject")
			}

			descentOrder, descentUsage, descentErr := recordQueueEntryReads(t, value, traceQueueEntryClosure)
			if descentErr != nil {
				t.Fatalf("traceQueueEntryClosure: %v", descentErr)
			}
			parseProof, descentProof := queueEntryProofBOC(t, parseUsage), queueEntryProofBOC(t, descentUsage)

			if len(parseOrder) != len(descentOrder) {
				t.Fatalf("recorded %d cells, parse recorded %d", len(descentOrder), len(parseOrder))
			}
			for i := range parseOrder {
				if parseOrder[i] != descentOrder[i] {
					t.Fatalf("recorded cell %d of %d differs: parse %x, descent %x",
						i, len(parseOrder), parseOrder[i], descentOrder[i])
				}
			}
			if !bytes.Equal(parseProof, descentProof) {
				t.Fatalf("proof differs: parse %d B, descent %d B", len(parseProof), len(descentProof))
			}
			t.Logf("cells=%d proof=%d B parse-rejects=%v", len(descentOrder), len(descentProof), spec.parseRejects)
		})
	}
}

// TestQueueEntryDescentRejectsMalformedEntries pins the failure behaviour the
// descent keeps. It drops the parse's semantic validation on purpose, but the
// structural checks that decide whether a broken or pruned entry is noticed have
// to survive, otherwise a proof that is missing a cell would ship silently.
func TestQueueEntryDescentRejectsMalformedEntries(t *testing.T) {
	spec := queueEntryShape{name: "base", gramsBytes: 4, initForm: "none", bodyRef: true, bodyBits: 800, srcAddr: "std", dstAddr: "std"}
	value, key := buildQueueEntryShape(t, spec)

	var envelope *cell.Cell
	if err := func() error {
		s, err := value.BeginParse()
		if err != nil {
			return err
		}
		if err = s.SkipBits(64); err != nil {
			return err
		}
		envelope, err = s.LoadRefCell()
		return err
	}(); err != nil {
		t.Fatalf("take envelope: %v", err)
	}
	message, err := envelope.PeekRef(0)
	if err != nil {
		t.Fatalf("take message: %v", err)
	}

	prunedEnvelope, err := cell.CreatePrunedBranch(envelope, 1, 0)
	if err != nil {
		t.Fatalf("prune envelope: %v", err)
	}
	prunedMessage, err := cell.CreatePrunedBranch(message, 1, 0)
	if err != nil {
		t.Fatalf("prune message: %v", err)
	}

	rebuild := func(env *cell.Cell) *cell.Cell {
		return cell.BeginCell().MustStoreUInt(10_000_777, 64).MustStoreRef(env).EndCell()
	}
	reenvelope := func(msg *cell.Cell) *cell.Cell {
		s, err := envelope.BeginParse()
		if err != nil {
			t.Fatalf("reopen envelope: %v", err)
		}
		b := cell.BeginCell()
		bits, data, err := s.RestBits()
		if err != nil {
			t.Fatalf("copy envelope bits: %v", err)
		}
		if err = b.StoreSlice(data, bits); err != nil {
			t.Fatalf("store envelope bits: %v", err)
		}
		if err = b.StoreRef(msg); err != nil {
			t.Fatalf("store message: %v", err)
		}
		return rebuild(b.EndCell())
	}

	cases := []struct {
		name  string
		value *cell.Cell
	}{
		{"entry with no reference", cell.BeginCell().MustStoreUInt(10_000_777, 64).EndCell()},
		{"entry with two references", cell.BeginCell().MustStoreUInt(10_000_777, 64).
			MustStoreRef(envelope).MustStoreRef(envelope).EndCell()},
		{"entry with short prefix", cell.BeginCell().MustStoreUInt(1, 32).MustStoreRef(envelope).EndCell()},
		{"pruned envelope", rebuild(prunedEnvelope)},
		{"pruned message", reenvelope(prunedMessage)},
		{"truncated envelope", rebuild(cell.BeginCell().MustStoreUInt(4, 4).EndCell())},
		{"envelope with an unknown tag", rebuild(cell.BeginCell().MustStoreUInt(7, 4).MustStoreRef(message).EndCell())},
		{"message that is not internal", reenvelope(cell.BeginCell().MustStoreUInt(1, 1).EndCell())},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, parseErr := recordQueueEntryReads(t, tc.value, func(s *cell.Slice) error {
				_, err := parseQueueEntry(s, key)
				return err
			})
			_, _, descentErr := recordQueueEntryReads(t, tc.value, traceQueueEntryClosure)

			if parseErr == nil {
				t.Fatalf("parseQueueEntry accepted %s", tc.name)
			}
			if descentErr == nil {
				t.Fatalf("descent accepted %s, parse rejected it with: %v", tc.name, parseErr)
			}
			t.Logf("parse:   %v", parseErr)
			t.Logf("descent: %v", descentErr)
		})
	}
}
