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

// The descent drops the parse's semantic validation on purpose — the key
// re-derivation and the intermediate-address check are redundant on a trace
// path, and the reference implementation's proof preparation performs neither.
// What it may not drop is any rejection whose absence changes what the trace
// records or lets a malformed entry through into a proof a remote validator
// checks. This is the gate for that line: entries the TL-B parse refuses, built
// one axis at a time, with the descent required to refuse them too.
//
// The extra-currency dictionary is where the two can genuinely disagree about
// cells rather than about bits, because it is the one dictionary this path walks
// whole. A structural pre-order descent has no way to tell a fork's two
// references from a leaf value's, so it opens whatever a malformed leaf carries
// and records cells the parse never recorded — see also the test below this one,
// where the same blindness shows up on an entry the parse accepts.

func malformedQueueEntryAddresses() (*address.Address, *address.Address) {
	return address.NewAddress(0, 0, bytes.Repeat([]byte{0x31}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x32}, 32))
}

// buildMalformedQueuedMessage builds one int_msg_info with a by-reference body:
// extraRoot is stored as the extra-currency dictionary when non-nil, and
// trailing is appended after the body reference, which is where the parse's
// exactness check bites.
func buildMalformedQueuedMessage(tb testing.TB, extraRoot *cell.Cell, trailing func(*cell.Builder)) *cell.Cell {
	tb.Helper()

	must := func(err error) {
		tb.Helper()
		if err != nil {
			tb.Fatalf("build malformed message: %v", err)
		}
	}
	src, dst := malformedQueueEntryAddresses()

	b := cell.BeginCell()
	must(b.StoreUInt(0, 1))     // int_msg_info$0
	must(b.StoreBoolBit(false)) // ihr_disabled
	must(b.StoreBoolBit(true))  // bounce
	must(b.StoreBoolBit(false)) // bounced
	must(b.StoreAddr(src))
	must(b.StoreAddr(dst))
	must(b.StoreBigCoins(new(big.Int).SetBytes(bytes.Repeat([]byte{0xAB}, 4))))
	if extraRoot != nil {
		must(b.StoreBoolBit(true))
		must(b.StoreRef(extraRoot))
	} else {
		must(b.StoreBoolBit(false))
	}
	must(b.StoreBigCoins(big.NewInt(7)))  // ihr_fee
	must(b.StoreBigCoins(big.NewInt(11))) // fwd_fee
	must(b.StoreUInt(10_000_001, 64))     // created_lt
	must(b.StoreUInt(1_900_000_000, 32))  // created_at
	must(b.StoreBoolBit(false))           // no StateInit
	must(b.StoreBoolBit(true))            // body by reference
	must(b.StoreRef(cell.BeginCell().MustStoreUInt(0xB0D1, 16).EndCell()))
	if trailing != nil {
		trailing(b)
	}
	return b.EndCell()
}

// wrapQueueEntry envelopes a message and returns the entry cell together with
// the queue key that entry genuinely belongs under, so nothing but the axis
// under test is wrong with it.
func wrapQueueEntry(tb testing.TB, msg *cell.Cell, reenvelope func(*cell.Cell) *cell.Cell) (*cell.Cell, msgpool.QueueKey) {
	tb.Helper()

	envCell, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		FwdFeeRemaining: tlb.FromNanoTONU(100_000),
		Msg:             msg,
	}).ToCell()
	if err != nil {
		tb.Fatalf("build envelope: %v", err)
	}
	if reenvelope != nil {
		envCell = reenvelope(envCell)
	}
	value, err := (tlb.EnqueuedMsg{EnqueuedLT: 10_000_777, Msg: envCell}).ToCell()
	if err != nil {
		tb.Fatalf("build entry: %v", err)
	}

	src, dst := malformedQueueEntryAddresses()
	source, err := accountPrefixFromAddress(src)
	if err != nil {
		tb.Fatalf("source prefix: %v", err)
	}
	destination, err := accountPrefixFromAddress(dst)
	if err != nil {
		tb.Fatalf("destination prefix: %v", err)
	}
	return value, msgpool.MakeQueueKey(msgpool.InterpolatePrefix(source, destination, 0), msg.HashKey())
}

// extraCurrencyDict builds an extra-currency dictionary from raw value cells, so
// a value can carry whatever a well-formed one may not.
func extraCurrencyDict(tb testing.TB, values map[uint64]*cell.Cell) *cell.Cell {
	tb.Helper()

	dict := cell.NewDict(32)
	for id, value := range values {
		key := cell.BeginCell().MustStoreUInt(id, 32).EndCell()
		if err := dict.Set(key, value); err != nil {
			tb.Fatalf("set extra currency %d: %v", id, err)
		}
	}
	root, err := dict.ToCell()
	if err != nil {
		tb.Fatalf("build extra currency dictionary: %v", err)
	}
	return root
}

func canonicalExtraCurrencyValue() *cell.Cell {
	// VarUInteger 32: a 3-byte length prefix and three non-zero bytes.
	return cell.BeginCell().MustStoreUInt(3, 5).MustStoreUInt(0x112233, 24).EndCell()
}

func TestQueueEntryDescentRejectsWhatTheParseRejected(t *testing.T) {
	// A cell nothing well-formed on this path points at, so a descent that
	// wanders into a value reference is caught by identity and not only by the
	// count of what it recorded.
	strayRef := cell.BeginCell().MustStoreUInt(0xDEADBEEF, 32).EndCell()

	cases := []struct {
		name  string
		build func(testing.TB) (*cell.Cell, msgpool.QueueKey)
		// recordedStray marks the class where accepting the entry was not the
		// whole of the defect: the descent also opened the stray cell and put it
		// in the proof. Rejecting is necessary but not sufficient there, so the
		// stray assertion below runs before the rejection one.
		recordedStray bool
	}{
		{
			// The class that changes the proof: the descent goes into the value.
			name: "extra currency value carrying a reference",
			build: func(tb testing.TB) (*cell.Cell, msgpool.QueueKey) {
				value := cell.BeginCell().MustStoreUInt(3, 5).MustStoreUInt(0x112233, 24).
					MustStoreRef(strayRef).EndCell()
				extra := extraCurrencyDict(tb, map[uint64]*cell.Cell{100: value})
				return wrapQueueEntry(tb, buildMalformedQueuedMessage(tb, extra, nil), nil)
			},
			recordedStray: true,
		},
		{
			name: "extra currency value with a leading zero byte",
			build: func(tb testing.TB) (*cell.Cell, msgpool.QueueKey) {
				value := cell.BeginCell().MustStoreUInt(3, 5).MustStoreUInt(0x002233, 24).EndCell()
				extra := extraCurrencyDict(tb, map[uint64]*cell.Cell{100: value})
				return wrapQueueEntry(tb, buildMalformedQueuedMessage(tb, extra, nil), nil)
			},
		},
		{
			name: "message with a trailing reference after a by-reference body",
			build: func(tb testing.TB) (*cell.Cell, msgpool.QueueKey) {
				msg := buildMalformedQueuedMessage(tb, nil, func(b *cell.Builder) {
					b.MustStoreRef(strayRef)
				})
				return wrapQueueEntry(tb, msg, nil)
			},
		},
		{
			name: "message with trailing bits after a by-reference body",
			build: func(tb testing.TB) (*cell.Cell, msgpool.QueueKey) {
				msg := buildMalformedQueuedMessage(tb, nil, func(b *cell.Builder) {
					b.MustStoreUInt(0x5A5A, 16)
				})
				return wrapQueueEntry(tb, msg, nil)
			},
		},
		{
			name: "envelope carrying a second reference",
			build: func(tb testing.TB) (*cell.Cell, msgpool.QueueKey) {
				return wrapQueueEntry(tb, buildMalformedQueuedMessage(tb, nil, nil), func(env *cell.Cell) *cell.Cell {
					slice, err := env.BeginParse()
					if err != nil {
						t.Fatal(err)
					}
					bits, data, err := slice.RestBits()
					if err != nil {
						t.Fatal(err)
					}
					msg, err := env.PeekRef(0)
					if err != nil {
						t.Fatal(err)
					}
					return cell.BeginCell().MustStoreSlice(data, bits).
						MustStoreRef(msg).MustStoreRef(strayRef).EndCell()
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, key := tc.build(t)

			_, _, parseErr := recordQueueEntryReads(t, value, func(s *cell.Slice) error {
				_, err := parseQueueEntry(s, key)
				return err
			})
			if parseErr == nil {
				t.Fatalf("parseQueueEntry accepted %s, so this is not a shape it rejected", tc.name)
			}
			t.Logf("parse:   %v", parseErr)

			order, _, descentErr := recordQueueEntryReads(t, value, traceQueueEntryClosure)
			stray := false
			for _, hash := range order {
				if hash == strayRef.HashKey() {
					stray = true
				}
			}
			if stray {
				t.Errorf("the descent recorded a cell the parse never opened, which lands in the proof")
			} else if tc.recordedStray {
				t.Log("note: the descent no longer reaches the value's reference at all")
			}
			if descentErr == nil {
				t.Fatalf("the descent accepted %s; the parse rejected it with: %v", tc.name, parseErr)
			}
			t.Logf("descent: %v", descentErr)
		})
	}
}

// The fourth class is not a rejection at all, which is what makes it the worst
// of them: the dictionary parse ACCEPTS a fork node carrying a third reference,
// because it reads references 0 and 1 of a fork and never looks further. A
// structural descent has no such notion and follows it.
//
// Two things then go wrong at once. The trace records a cell the parse never
// recorded, so an entry every other node accepts produces a different proof
// here; and because the stray subtree is opened rather than merely counted, a
// pruned branch hung there fails the collation outright — over a message this
// node did not write and cannot refuse.
func TestQueueEntryDescentIgnoresWhatTheDictionaryIgnores(t *testing.T) {
	strayLeaf := cell.BeginCell().MustStoreUInt(0xDEADBEEF, 32).EndCell()
	stray, err := cell.CreatePrunedBranch(strayLeaf, 1, 0)
	if err != nil {
		t.Fatalf("prune the stray subtree: %v", err)
	}

	root := extraCurrencyDict(t, map[uint64]*cell.Cell{
		100: canonicalExtraCurrencyValue(),
		237: canonicalExtraCurrencyValue(),
	})
	if root.RefsNum() != 2 {
		t.Fatalf("the two-entry dictionary root holds %d references, want a fork", root.RefsNum())
	}
	slice, err := root.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	bits, data, err := slice.RestBits()
	if err != nil {
		t.Fatal(err)
	}
	forked := cell.BeginCell().MustStoreSlice(data, bits)
	for i := 0; i < 2; i++ {
		ref, refErr := root.PeekRef(i)
		if refErr != nil {
			t.Fatal(refErr)
		}
		forked.MustStoreRef(ref)
	}
	value, key := wrapQueueEntry(t,
		buildMalformedQueuedMessage(t, forked.MustStoreRef(stray).EndCell(), nil), nil)

	parseOrder, parseUsage, parseErr := recordQueueEntryReads(t, value, func(s *cell.Slice) error {
		_, err := parseQueueEntry(s, key)
		return err
	})
	if parseErr != nil {
		t.Fatalf("parseQueueEntry rejected the entry, so this is not the class it is here for: %v", parseErr)
	}

	descentOrder, descentUsage, descentErr := recordQueueEntryReads(t, value, traceQueueEntryClosure)
	if descentErr != nil {
		t.Fatalf("the descent failed an entry the parse accepted: %v", descentErr)
	}
	if len(parseOrder) != len(descentOrder) {
		t.Fatalf("the descent recorded %d cells against the parse's %d", len(descentOrder), len(parseOrder))
	}
	for i := range parseOrder {
		if parseOrder[i] != descentOrder[i] {
			t.Fatalf("recorded cell %d of %d differs: parse %x, descent %x",
				i, len(parseOrder), parseOrder[i], descentOrder[i])
		}
	}
	if !bytes.Equal(queueEntryProofBOC(t, parseUsage), queueEntryProofBOC(t, descentUsage)) {
		t.Fatal("the entry produced different proof bytes on the two paths")
	}
}
