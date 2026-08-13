package msgpool

import (
	"encoding/binary"
	"errors"
	"math/big"
	"math/rand"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func deltaAddr(wc int32, fill byte) *address.Address {
	data := make([]byte, 32)
	for i := range data {
		data[i] = fill
	}
	return address.NewAddress(0, byte(wc), data)
}

// deltaInternalMsg builds a real int_msg_info$0 message cell.
func deltaInternalMsg(t testing.TB, src, dst *address.Address, lt uint64) *cell.Cell {
	t.Helper()
	return cell.BeginCell().
		MustStoreUInt(0, 1).     // int_msg_info$0
		MustStoreBoolBit(true).  // ihr_disabled
		MustStoreBoolBit(false). // bounce
		MustStoreBoolBit(false). // bounced
		MustStoreAddr(src).
		MustStoreAddr(dst).
		MustStoreCoins(1_000_000).
		MustStoreBoolBit(false). // no extra currencies
		MustStoreCoins(0).       // ihr fee
		MustStoreCoins(1_000).   // fwd fee
		MustStoreUInt(lt, 64).
		MustStoreUInt(0, 32).    // created_at
		MustStoreBoolBit(false). // no state init
		MustStoreBoolBit(false). // inline (empty) body
		EndCell()
}

// deltaVarAddr builds a canonical addr_var: a workchain outside the byte range
// and a bit length that is not 256, the only shape AccountPrefixFromAddress
// accepts as a genuine variable address.
func deltaVarAddr(workchain int32, fill byte) *address.Address {
	data := make([]byte, 38)
	for i := range data {
		data[i] = fill
	}
	return address.NewAddressVar(0, workchain, 300, data)
}

// deltaAnycastAddr builds an addr_std carrying a non-zero anycast rewrite, so
// the routing prefix differs from the raw account bits.
func deltaAnycastAddr(wc int32, fill byte, depth uint, prefix []byte) *address.Address {
	return deltaAddr(wc, fill).WithAnycast(address.NewAnycast(depth, prefix))
}

// deltaInternalMsgRich builds an int_msg_info$0 cell that carries every field
// the queue-key derivation does not read: a non-empty extra-currency
// dictionary, non-zero fees, a state init and a body behind a reference. The
// plain fixtures build none of them, so this is what keeps the header parser
// honest about its field skips.
func deltaInternalMsgRich(t testing.TB, src, dst *address.Address, lt uint64) *cell.Cell {
	t.Helper()
	extra := cell.NewDict(32)
	if err := extra.SetIntKey(big.NewInt(7), cell.BeginCell().MustStoreUInt(0x2a, 8).EndCell()); err != nil {
		t.Fatal(err)
	}
	stateInit, err := tlb.ToCell(tlb.StateInit{
		Code: cell.BeginCell().MustStoreUInt(0xc0de, 32).EndCell(),
		Data: cell.BeginCell().MustStoreUInt(0xda7a, 32).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}

	return cell.BeginCell().
		MustStoreUInt(0, 1).    // int_msg_info$0
		MustStoreBoolBit(true). // ihr_disabled
		MustStoreBoolBit(true). // bounce
		MustStoreBoolBit(true). // bounced
		MustStoreAddr(src).
		MustStoreAddr(dst).
		MustStoreBigCoins(new(big.Int).Lsh(big.NewInt(1), 100)). // a 13-byte Grams
		MustStoreDict(extra).
		MustStoreCoins(123_456_789). // ihr fee
		MustStoreCoins(987_654_321). // fwd fee
		MustStoreUInt(lt, 64).
		MustStoreUInt(0x6789abcd, 32). // created_at
		MustStoreBoolBit(true).        // state init present
		MustStoreBoolBit(true).        // ... behind a reference
		MustStoreRef(stateInit).
		MustStoreBoolBit(true). // body behind a reference
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xb0d1, 32).EndCell()).
		EndCell()
}

func deltaEnvelope(t testing.TB, msg *cell.Cell, next tlb.IntermediateAddress) *cell.Cell {
	t.Helper()
	env, err := tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		NextAddr:        next,
		FwdFeeRemaining: tlb.MustFromTON("0.001"),
		Msg:             msg,
	}.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func deltaEnvelopeWithEmittedLT(t testing.TB, msg *cell.Cell, next tlb.IntermediateAddress, emittedLT uint64) *cell.Cell {
	t.Helper()
	env, err := tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		NextAddr:        next,
		FwdFeeRemaining: tlb.MustFromTON("0.001"),
		Msg:             msg,
		EmittedLT:       &emittedLT,
	}.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func regularNext(bits uint8) tlb.IntermediateAddress {
	return tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: bits}
}

// deltaBlockRoot wraps an OutMsgDescr cell into a minimal block root.
func deltaBlockRoot(t testing.TB, outDescr *cell.Cell) *cell.Cell {
	t.Helper()
	stub := cell.BeginCell().EndCell()
	extra := cell.BeginCell().
		MustStoreUInt(blockExtraMagic, 32).
		MustStoreRef(stub). // in_msg_descr
		MustStoreRef(outDescr).
		MustStoreRef(stub). // account_blocks
		MustStoreSlice(make([]byte, 64), 512).
		EndCell()
	return cell.BeginCell().
		MustStoreUInt(blockMagic, 32).
		MustStoreUInt(0, 32).
		MustStoreRef(stub).
		MustStoreRef(stub).
		MustStoreRef(stub).
		MustStoreRef(extra).
		EndCell()
}

func newOutDescrDict(t testing.TB) *cell.AugmentedDictionary {
	t.Helper()
	dict, err := cell.NewAugDict(256, tlb.AugOutMsgDescr{})
	if err != nil {
		t.Fatal(err)
	}
	return dict
}

func setDescr(t testing.TB, dict *cell.AugmentedDictionary, msgHash []byte, value *cell.Cell) {
	t.Helper()
	if err := dict.SetIntKey(new(big.Int).SetBytes(msgHash), value); err != nil {
		t.Fatal(err)
	}
}

// referenceInternalFromEnvelope derives the pooled view through the full
// tlb.InternalMessage decode — the shape internalFromEnvelope had before it was
// narrowed to the int_msg_info header the queue key actually needs. It is the
// oracle the header parser is pinned against.
func referenceInternalFromEnvelope(t testing.TB, envCell *cell.Cell) *InternalMessage {
	t.Helper()
	envLoader, err := envCell.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	var env tlb.MsgEnvelope
	if err = env.LoadFromCell(envLoader); err != nil {
		t.Fatal(err)
	}
	msgLoader, err := env.Msg.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	var msg tlb.InternalMessage
	if err = tlb.LoadFromCell(&msg, msgLoader); err != nil {
		t.Fatal(err)
	}

	var hop AccountPrefix
	switch env.NextAddr.Type {
	case tlb.IntermediateAddressRegular:
		src, srcErr := AccountPrefixFromAddress(msg.SrcAddr)
		if srcErr != nil {
			t.Fatal(srcErr)
		}
		dst, dstErr := AccountPrefixFromAddress(msg.DstAddr)
		if dstErr != nil {
			t.Fatal(dstErr)
		}
		hop = InterpolatePrefix(src, dst, int(env.NextAddr.UseDestBits))
	case tlb.IntermediateAddressSimple, tlb.IntermediateAddressExt:
		hop = AccountPrefix{Workchain: env.NextAddr.Workchain, Prefix: env.NextAddr.AddrPfx}
	default:
		t.Fatalf("unknown envelope next-hop type %d", env.NextAddr.Type)
	}

	enqueuedLT := msg.CreatedLT
	if env.EmittedLT != nil {
		enqueuedLT = *env.EmittedLT
	}
	return &InternalMessage{
		Key:        MakeQueueKey(hop, env.Msg.HashKey()),
		EnqueuedLT: enqueuedLT,
		QueueLT:    msg.CreatedLT,
		EnvHash:    envCell.HashKey(),
	}
}

// TestInternalFromEnvelopeMatchesFullDecode pins every value the pool derives
// from an envelope against the exhaustive tlb decode, across the envelope
// shapes the rest of the fixtures never build: addr_var endpoints, a non-zero
// anycast rewrite, a populated extra-currency dictionary, a state init and a
// referenced body, every intermediate-address form and the interpolation
// boundaries of hypercube routing. Key, EnqueuedLT, QueueLT and EnvHash select
// which messages a collator imports and in what order, so they are the ones
// that must not drift.
func TestInternalFromEnvelopeMatchesFullDecode(t *testing.T) {
	std := deltaAddr(0, 0x11)
	stdDst := deltaAddr(0, 0x22)
	anycastSrc := deltaAnycastAddr(0, 0x33, 13, []byte{0xab, 0xc8})
	anycastDst := deltaAnycastAddr(0, 0x44, 8, []byte{0x5f})
	varSrc := deltaVarAddr(0x1000, 0x55)
	varDst := deltaVarAddr(0x2000, 0x66)

	// The anycast rewrite has to actually move the routing prefix, otherwise
	// the case below proves nothing.
	rawPrefix, err := AccountPrefixFromAddress(deltaAddr(0, 0x33))
	if err != nil {
		t.Fatal(err)
	}
	anycastPrefix, err := AccountPrefixFromAddress(anycastSrc)
	if err != nil {
		t.Fatal(err)
	}
	if rawPrefix == anycastPrefix {
		t.Fatal("anycast fixture does not rewrite the routing prefix")
	}

	cases := []struct {
		name string
		env  *cell.Cell
	}{
		{"plain", deltaEnvelope(t, deltaInternalMsg(t, std, stdDst, 100), regularNext(96))},
		{"rich-fields", deltaEnvelope(t, deltaInternalMsgRich(t, std, stdDst, 200), regularNext(96))},
		{"anycast-dst", deltaEnvelope(t, deltaInternalMsgRich(t, std, anycastDst, 300), regularNext(96))},
		{"anycast-src-partial", deltaEnvelope(t, deltaInternalMsgRich(t, anycastSrc, stdDst, 400), regularNext(48))},
		{"anycast-both", deltaEnvelope(t, deltaInternalMsg(t, anycastSrc, anycastDst, 500), regularNext(64))},
		{"var-addr", deltaEnvelope(t, deltaInternalMsgRich(t, varSrc, varDst, 600), regularNext(96))},
		{"var-src-partial", deltaEnvelope(t, deltaInternalMsg(t, varSrc, varDst, 700), regularNext(16))},
		{"use-dest-zero", deltaEnvelope(t, deltaInternalMsgRich(t, std, stdDst, 800), regularNext(0))},
		{"emitted-lt", deltaEnvelopeWithEmittedLT(t, deltaInternalMsgRich(t, std, stdDst, 900), regularNext(96), 12_345)},
		{"next-simple", deltaEnvelope(t, deltaInternalMsgRich(t, std, stdDst, 1_000), tlb.IntermediateAddress{
			Type:      tlb.IntermediateAddressSimple,
			Workchain: 0,
			AddrPfx:   0x8877665544332211,
		})},
		{"next-ext", deltaEnvelope(t, deltaInternalMsg(t, std, stdDst, 1_100), tlb.IntermediateAddress{
			Type:      tlb.IntermediateAddressExt,
			Workchain: 0x1000,
			AddrPfx:   0x1122334455667788,
		})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := referenceInternalFromEnvelope(t, tc.env)
			got, err := internalFromEnvelope(tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if got.Key != want.Key {
				t.Fatalf("queue key = %x, want %x", got.Key, want.Key)
			}
			if got.EnqueuedLT != want.EnqueuedLT {
				t.Fatalf("enqueued lt = %d, want %d", got.EnqueuedLT, want.EnqueuedLT)
			}
			if got.QueueLT != want.QueueLT {
				t.Fatalf("queue lt = %d, want %d", got.QueueLT, want.QueueLT)
			}
			if got.EnvHash != want.EnvHash {
				t.Fatalf("envelope hash = %x, want %x", got.EnvHash, want.EnvHash)
			}
			if got.EnvelopeCell != tc.env || got.Root == nil {
				t.Fatal("pooled view lost the exact envelope or message cell")
			}
		})
	}
}

func transitImport(t *testing.T, oldEnv, newEnv *cell.Cell) *cell.Cell {
	t.Helper()
	return cell.BeginCell().
		MustStoreUInt(0b101, 3).
		MustStoreRef(oldEnv).
		MustStoreRef(newEnv).
		MustStoreCoins(0).
		EndCell()
}

func TestDeltaFromBlockRootExportNewFilteringAndOrder(t *testing.T) {
	srcBase := deltaAddr(0, 0x11)
	dict := newOutDescrDict(t)

	// Self-shard message, fully routed: lands in the owner view.
	selfMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x22), 2000)
	selfEnv := deltaEnvelope(t, selfMsg, regularNext(96))
	setDescr(t, dict, selfMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b001, 3).MustStoreRef(selfEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 8).EndCell()).EndCell())

	// Message to the masterchain: filtered out of the basechain owner view
	// but counted in the total.
	mcMsg := deltaInternalMsg(t, srcBase, deltaAddr(-1, 0x33), 1500)
	mcEnv := deltaEnvelope(t, mcMsg, regularNext(96))
	setDescr(t, dict, mcMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b001, 3).MustStoreRef(mcEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(2, 8).EndCell()).EndCell())

	// Simple next-hop variant with a lower lt: must sort first.
	simpleMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x44), 1000)
	simpleEnv := deltaEnvelope(t, simpleMsg, tlb.IntermediateAddress{
		Type: tlb.IntermediateAddressSimple, Workchain: 0, AddrPfx: 0x4444444444444444,
	})
	setDescr(t, dict, simpleMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b001, 3).MustStoreRef(simpleEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(3, 8).EndCell()).EndCell())

	// An outbound external is ignored.
	setDescr(t, dict, cell.BeginCell().MustStoreUInt(7, 8).EndCell().Hash(), cell.BeginCell().
		MustStoreUInt(0b000, 3).
		MustStoreRef(cell.BeginCell().MustStoreUInt(4, 8).EndCell()).
		MustStoreRef(cell.BeginCell().MustStoreUInt(5, 8).EndCell()).EndCell())

	delta, err := deltaFromBlockRoot(deltaBlockRoot(t, dict.AsCell()), 0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.RemovedKeys) != 0 || len(delta.RemovedEnvHashes) != 0 || delta.RemovedTotal != 0 {
		t.Fatalf("unexpected removals: %+v", delta)
	}
	if len(delta.Added) != 2 || delta.AddedTotal != 3 {
		t.Fatalf("added = %d (total %d), want 2 (total 3)", len(delta.Added), delta.AddedTotal)
	}

	first, second := delta.Added[0], delta.Added[1]
	if first.EnqueuedLT != 1000 || second.EnqueuedLT != 2000 {
		t.Fatalf("order: %d, %d", first.EnqueuedLT, second.EnqueuedLT)
	}
	wantSimpleKey := MakeQueueKey(AccountPrefix{Workchain: 0, Prefix: 0x4444444444444444}, simpleMsg.HashKey())
	if first.Key != wantSimpleKey {
		t.Fatalf("simple-hop key = %x, want %x", first.Key, wantSimpleKey)
	}
	dstPrefix, err := AccountPrefixFromAddress(deltaAddr(0, 0x22))
	if err != nil {
		t.Fatal(err)
	}
	wantSelfKey := MakeQueueKey(dstPrefix, selfMsg.HashKey())
	if second.Key != wantSelfKey {
		t.Fatalf("self key = %x, want %x", second.Key, wantSelfKey)
	}
	if second.EnvHash != selfEnv.HashKey() || second.EnvelopeCell.HashKey() != selfEnv.HashKey() {
		t.Fatal("envelope identity lost")
	}
	if second.Root.HashKey() != selfMsg.HashKey() {
		t.Fatal("message root lost")
	}
	if second.Envelope.NextAddr.UseDestBits != 96 {
		t.Fatal("parsed envelope lost")
	}
}

func TestDeltaFromBlockRootRemovals(t *testing.T) {
	srcBase := deltaAddr(0, 0x11)
	dict := newOutDescrDict(t)

	// deq_imm resolves through its envelope to a full key.
	deqImmMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x55), 3000)
	deqImmEnv := deltaEnvelope(t, deqImmMsg, regularNext(96))
	setDescr(t, dict, deqImmMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b100, 3).MustStoreRef(deqImmEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 8).EndCell()).EndCell())

	// msg_export_deq carries the envelope plus a 63-bit import lt.
	deqMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x66), 3100)
	deqEnv := deltaEnvelope(t, deqMsg, regularNext(96))
	setDescr(t, dict, deqMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b1100, 4).MustStoreRef(deqEnv).
		MustStoreUInt(12345, 63).EndCell())

	// deq_short in the owner shard removes by envelope hash…
	shortEnvHash := make([]byte, 32)
	for i := range shortEnvHash {
		shortEnvHash[i] = 0x77
	}
	setDescr(t, dict, cell.BeginCell().MustStoreUInt(1, 8).EndCell().Hash(), cell.BeginCell().
		MustStoreUInt(0b1101, 4).
		MustStoreSlice(shortEnvHash, 256).
		MustStoreInt(0, 32).
		MustStoreUInt(0x9000000000000000, 64).
		MustStoreUInt(777, 64).EndCell())

	// …while a foreign-shard deq_short is counted but not materialized.
	setDescr(t, dict, cell.BeginCell().MustStoreUInt(2, 8).EndCell().Hash(), cell.BeginCell().
		MustStoreUInt(0b1101, 4).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreInt(-1, 32).
		MustStoreUInt(0x9000000000000000, 64).
		MustStoreUInt(778, 64).EndCell())

	delta, err := deltaFromBlockRoot(deltaBlockRoot(t, dict.AsCell()), 0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.Added) != 0 || delta.AddedTotal != 0 {
		t.Fatalf("unexpected additions: %+v", delta)
	}
	if delta.RemovedTotal != 4 {
		t.Fatalf("removed total = %d, want 4", delta.RemovedTotal)
	}
	if len(delta.RemovedKeys) != 2 {
		t.Fatalf("removed keys = %d, want 2", len(delta.RemovedKeys))
	}
	seen := map[QueueKey]bool{}
	for _, key := range delta.RemovedKeys {
		seen[key] = true
	}
	immPrefix, _ := AccountPrefixFromAddress(deltaAddr(0, 0x55))
	deqPrefix, _ := AccountPrefixFromAddress(deltaAddr(0, 0x66))
	if !seen[MakeQueueKey(immPrefix, deqImmMsg.HashKey())] || !seen[MakeQueueKey(deqPrefix, deqMsg.HashKey())] {
		t.Fatalf("removed keys mismatch: %x", delta.RemovedKeys)
	}
	if len(delta.RemovedEnvHashes) != 1 {
		t.Fatalf("removed env hashes = %d, want 1", len(delta.RemovedEnvHashes))
	}
	var wantEnv [32]byte
	copy(wantEnv[:], shortEnvHash)
	if delta.RemovedEnvHashes[0] != wantEnv {
		t.Fatal("removed envelope hash mismatch")
	}
}

func TestDeltaFromBlockRootTransitAndDeferred(t *testing.T) {
	srcBase := deltaAddr(0, 0x11)
	dict := newOutDescrDict(t)

	trMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x88), 4000)
	trEnv := deltaEnvelopeWithEmittedLT(t, trMsg, regularNext(96), 4100)
	setDescr(t, dict, trMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b011, 3).MustStoreRef(trEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 8).EndCell()).EndCell())

	requeueMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x99), 5000)
	oldRequeueEnv := deltaEnvelope(t, requeueMsg, regularNext(32))
	newRequeueEnv := deltaEnvelopeWithEmittedLT(t, requeueMsg, regularNext(96), 5100)
	setDescr(t, dict, requeueMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b111, 3).
		MustStoreRef(newRequeueEnv).
		MustStoreRef(transitImport(t, oldRequeueEnv, newRequeueEnv)).EndCell())

	deferredMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0xaa), 6000)
	deferredEnv := deltaEnvelope(t, deferredMsg, regularNext(96))
	setDescr(t, dict, deferredMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b10100, 5).
		MustStoreRef(deferredEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(2, 8).EndCell()).EndCell())

	deferredTrMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0xbb), 6050)
	deferredTrEnv := deltaEnvelopeWithEmittedLT(t, deferredTrMsg, regularNext(96), 6100)
	setDescr(t, dict, deferredTrMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b10101, 5).
		MustStoreRef(deferredTrEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(3, 8).EndCell()).EndCell())

	delta, err := deltaFromBlockRoot(deltaBlockRoot(t, dict.AsCell()), 6200, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if delta.AddedTotal != 3 || len(delta.Added) != 3 {
		t.Fatalf("additions = %d (total %d), want 3", len(delta.Added), delta.AddedTotal)
	}
	if delta.RemovedTotal != 1 || len(delta.RemovedKeys) != 1 {
		t.Fatalf("removals = %d (total %d), want 1", len(delta.RemovedKeys), delta.RemovedTotal)
	}
	if delta.Added[0].EnqueuedLT != 4100 || delta.Added[1].EnqueuedLT != 5100 || delta.Added[2].EnqueuedLT != 6100 {
		t.Fatalf("addition order = %d, %d, %d", delta.Added[0].EnqueuedLT, delta.Added[1].EnqueuedLT, delta.Added[2].EnqueuedLT)
	}
	if delta.Added[0].QueueLT != 6200 || delta.Added[1].QueueLT != 6200 || delta.Added[2].QueueLT != 6100 {
		t.Fatalf(
			"queue positions = %d, %d, %d, want block start, block start, emitted lt",
			delta.Added[0].QueueLT,
			delta.Added[1].QueueLT,
			delta.Added[2].QueueLT,
		)
	}
	oldRequeue, err := internalFromEnvelope(oldRequeueEnv)
	if err != nil {
		t.Fatal(err)
	}
	if delta.RemovedKeys[0] != oldRequeue.Key {
		t.Fatalf("removed key = %x, want %x", delta.RemovedKeys[0], oldRequeue.Key)
	}
	for _, added := range delta.Added {
		if added.Root.HashKey() == deferredMsg.HashKey() {
			t.Fatal("msg_export_new_defer must stay outside OutMsgQueue")
		}
	}
}

func TestDeltaFromBlockRootRejectsMalformedTransitRequest(t *testing.T) {
	msg := deltaInternalMsg(t, deltaAddr(0, 0x11), deltaAddr(0, 0x22), 7000)
	env := deltaEnvelope(t, msg, regularNext(96))
	dict := newOutDescrDict(t)
	setDescr(t, dict, msg.Hash(), cell.BeginCell().
		MustStoreUInt(0b111, 3).
		MustStoreRef(env).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0b000, 3).EndCell()).EndCell())

	if _, err := deltaFromBlockRoot(deltaBlockRoot(t, dict.AsCell()), 0, testOwner); err == nil {
		t.Fatal("msg_export_tr_req with a non-transit import accepted")
	}
}

func TestDeltaFromBlockRootRejectsMalformedRoots(t *testing.T) {
	if _, err := deltaFromBlockRoot(cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell(), 0, testOwner); err == nil {
		t.Fatal("bad magic accepted")
	}
	// A stub OutMsgDescr cell is not a dictionary.
	if _, err := deltaFromBlockRoot(deltaBlockRoot(t, cell.BeginCell().EndCell()), 0, testOwner); err == nil {
		t.Fatal("stub OutMsgDescr accepted")
	}
}

// queueDictCell assembles an out-queue HashmapAugE with the given entries.
func queueDictCell(t testing.TB, entries map[QueueKey]tlb.EnqueuedMsg) *cell.Cell {
	t.Helper()
	dict, err := cell.NewAugDict(352, tlb.AugOutMsgQueue{})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range entries {
		valueCell, err := value.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
		if _, err = dict.SetWithMode(keyCell, valueCell, cell.DictSetModeAdd); err != nil {
			t.Fatal(err)
		}
	}
	return dict.AsCell()
}

// stateRootWithQueue wraps an out-queue dictionary into a minimal
// ShardStateUnsplit cell; withSize additionally stores the queue size the
// way capStoreOutMsgQueueSize does.
func stateRootWithQueue(t testing.TB, queue *cell.Cell, size uint64, withSize bool) *cell.Cell {
	t.Helper()
	queueInfo := cell.BeginCell().
		MustStoreBuilder(queue.ToBuilder()).
		MustStoreBoolBit(false) // proc_info: empty HashmapE
	if withSize {
		dispatchQueue, err := tlb.NewDispatchQueueAugDict()
		if err != nil {
			t.Fatal(err)
		}
		extra, err := tlb.OutMsgQueueExtra{
			DispatchQueue: dispatchQueue,
			OutQueueSize:  &size,
		}.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		queueInfo.MustStoreBoolBit(true).MustStoreBuilder(extra.ToBuilder())
	} else {
		queueInfo.MustStoreBoolBit(false)
	}
	return cell.BeginCell().
		MustStoreUInt(shardStateMagic, 32).
		MustStoreRef(queueInfo.EndCell()).
		EndCell()
}

func TestSeedFromStateRoot(t *testing.T) {
	srcBase := deltaAddr(0, 0x11)

	// Two owner-routed entries with out-of-order lts. The leaf augmentation,
	// which falls back to created_lt here, is authoritative for the second
	// one instead of EnqueuedMsg.enqueued_lt.
	msgLate := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x22), 2000)
	envLate := deltaEnvelope(t, msgLate, regularNext(96))
	lateHop, _ := AccountPrefixFromAddress(deltaAddr(0, 0x22))
	lateKey := MakeQueueKey(lateHop, msgLate.HashKey())

	msgEarly := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x33), 1700)
	envEarly := deltaEnvelope(t, msgEarly, regularNext(96))
	earlyHop, _ := AccountPrefixFromAddress(deltaAddr(0, 0x33))
	earlyKey := MakeQueueKey(earlyHop, msgEarly.HashKey())

	// A masterchain-routed entry is filtered out by its key prefix but
	// counted in the total.
	msgForeign := deltaInternalMsg(t, srcBase, deltaAddr(-1, 0x99), 1)
	envForeign := deltaEnvelope(t, msgForeign, regularNext(96))
	foreignKey := MakeQueueKey(AccountPrefix{Workchain: -1, Prefix: 0x9999999999999999}, msgForeign.HashKey())

	state := stateRootWithQueue(t, queueDictCell(t, map[QueueKey]tlb.EnqueuedMsg{
		lateKey:    {EnqueuedLT: 2000, Msg: envLate},
		earlyKey:   {EnqueuedLT: 555, Msg: envEarly},
		foreignKey: {EnqueuedLT: 1, Msg: envForeign},
	}), 0, false)

	entries, total, err := seedFromStateRoot(state, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || total != 3 {
		t.Fatalf("entries = %d (total %d), want 2 (total 3)", len(entries), total)
	}
	if entries[0].EnqueuedLT != 1700 || entries[1].EnqueuedLT != 2000 {
		t.Fatalf("order: %d, %d", entries[0].EnqueuedLT, entries[1].EnqueuedLT)
	}
	if entries[0].Key != earlyKey || entries[1].Key != lateKey {
		t.Fatal("keys must come from the dictionary")
	}
	if entries[0].EnvHash != envEarly.HashKey() {
		t.Fatal("envelope hash mismatch")
	}

	// A key that does not match its envelope is a disorder.
	badKey := MakeQueueKey(earlyHop, msgLate.HashKey())
	badState := stateRootWithQueue(t, queueDictCell(t, map[QueueKey]tlb.EnqueuedMsg{
		badKey: {EnqueuedLT: 10, Msg: envEarly},
	}), 0, false)
	if _, _, err = seedFromStateRoot(badState, testOwner); !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("mismatched key = %v", err)
	}
}

func TestSeedFromStateRootOrdersByLeafAugmentation(t *testing.T) {
	src := deltaAddr(0, 0x11)

	transitMsg := deltaInternalMsg(t, src, deltaAddr(0, 0x22), 900)
	transitEnv := deltaEnvelopeWithEmittedLT(t, transitMsg, regularNext(96), 1000)
	transitHop, err := AccountPrefixFromAddress(deltaAddr(0, 0x22))
	if err != nil {
		t.Fatal(err)
	}
	transitKey := MakeQueueKey(transitHop, transitMsg.HashKey())

	regularMsg := deltaInternalMsg(t, src, deltaAddr(0, 0x33), 2000)
	regularEnv := deltaEnvelope(t, regularMsg, regularNext(96))
	regularHop, err := AccountPrefixFromAddress(deltaAddr(0, 0x33))
	if err != nil {
		t.Fatal(err)
	}
	regularKey := MakeQueueKey(regularHop, regularMsg.HashKey())

	state := stateRootWithQueue(t, queueDictCell(t, map[QueueKey]tlb.EnqueuedMsg{
		transitKey: {EnqueuedLT: 5000, Msg: transitEnv},
		regularKey: {EnqueuedLT: 2000, Msg: regularEnv},
	}), 0, false)

	entries, total, err := seedFromStateRoot(state, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(entries) != 2 {
		t.Fatalf("entries = %d (total %d), want 2 (total 2)", len(entries), total)
	}
	if entries[0].Key != transitKey || entries[0].EnqueuedLT != 1000 {
		t.Fatalf("first entry = %x at %d, want transit %x at emitted lt 1000", entries[0].Key, entries[0].EnqueuedLT, transitKey)
	}
	if entries[1].Key != regularKey || entries[1].EnqueuedLT != 2000 {
		t.Fatalf("second entry = %x at %d, want regular %x at created lt 2000", entries[1].Key, entries[1].EnqueuedLT, regularKey)
	}
}

func TestQueueSizeFromStateRoot(t *testing.T) {
	empty := queueDictCell(t, nil)

	size, err := QueueSizeFromStateRoot(stateRootWithQueue(t, empty, 42, true))
	if err != nil || size != 42 {
		t.Fatalf("stored size = %d %v", size, err)
	}
	_, err = QueueSizeFromStateRoot(stateRootWithQueue(t, empty, 0, false))
	if !errors.Is(err, ErrQueueSizeNotStored) {
		t.Fatalf("size without extra: err=%v", err)
	}
	if _, err = QueueSizeFromStateRoot(cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell()); err == nil {
		t.Fatal("bad state accepted")
	}
}

// ---- rebuild equivalence: seed(prev) + delta(block) == seed(cur) ----

// chainEntry is one queued message tracked by the model chain.
type chainEntry struct {
	key QueueKey
	env *cell.Cell
	lt  uint64
}

func chainAddr(wc int32, id uint64) *address.Address {
	data := make([]byte, 32)
	binary.BigEndian.PutUint64(data[:8], 0x4000000000000000|id)
	binary.BigEndian.PutUint64(data[24:], id)
	return address.NewAddress(0, byte(wc), data)
}

func makeChainEntry(t *testing.T, wc int32, id, lt uint64) chainEntry {
	t.Helper()
	msg := deltaInternalMsg(t, chainAddr(0, 0xeee), chainAddr(wc, id), lt)
	env := deltaEnvelope(t, msg, regularNext(96))
	hop, err := AccountPrefixFromAddress(chainAddr(wc, id))
	if err != nil {
		t.Fatal(err)
	}
	return chainEntry{key: MakeQueueKey(hop, msg.HashKey()), env: env, lt: lt}
}

func queueOfChain(entries map[QueueKey]chainEntry) map[QueueKey]tlb.EnqueuedMsg {
	queue := make(map[QueueKey]tlb.EnqueuedMsg, len(entries))
	for key, e := range entries {
		queue[key] = tlb.EnqueuedMsg{EnqueuedLT: e.lt, Msg: e.env}
	}
	return queue
}

// chainBlockRoot builds a block enqueueing adds and dequeuing removes;
// every second removal is a deq_short.
func chainBlockRoot(t *testing.T, adds, removes []chainEntry) *cell.Cell {
	t.Helper()
	dict := newOutDescrDict(t)
	txStub := cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()
	for _, e := range adds {
		msgHash := e.key.MsgHash()
		setDescr(t, dict, msgHash[:], cell.BeginCell().
			MustStoreUInt(0b001, 3).MustStoreRef(e.env).MustStoreRef(txStub).EndCell())
	}
	for i, e := range removes {
		msgHash := e.key.MsgHash()
		if i%2 == 0 { // msg_export_deq_imm
			setDescr(t, dict, msgHash[:], cell.BeginCell().
				MustStoreUInt(0b100, 3).MustStoreRef(e.env).MustStoreRef(txStub).EndCell())
			continue
		}
		hop := e.key.NextHop()
		envHash := e.env.HashKey()
		setDescr(t, dict, msgHash[:], cell.BeginCell().
			MustStoreUInt(0b1101, 4).
			MustStoreSlice(envHash[:], 256).
			MustStoreInt(int64(hop.Workchain), 32).
			MustStoreUInt(hop.Prefix, 64).
			MustStoreUInt(e.lt+1, 64).EndCell())
	}
	return deltaBlockRoot(t, dict.AsCell())
}

// requireSameView asserts that two runs serve identical cuts and queue
// sizes for the same position.
func requireSameView(t *testing.T, incremental, fresh *destinationState, ref SourceRef) {
	t.Helper()
	sizeA, errA := incremental.queueSize(baseSource)
	sizeB, errB := fresh.queueSize(baseSource)
	if errA != nil || errB != nil || sizeA != sizeB {
		t.Fatalf("queue sizes: %d (%v) vs %d (%v)", sizeA, errA, sizeB, errB)
	}
	req := CutRequest{Sources: map[ShardIdent]CutSource{baseSource: {Visible: ref}}}
	cutA, err := incremental.Cut(req)
	if err != nil {
		t.Fatal(err)
	}
	cutB, err := fresh.Cut(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(cutA.Messages) != len(cutB.Messages) {
		t.Fatalf("cut sizes: %d vs %d", len(cutA.Messages), len(cutB.Messages))
	}
	for i := range cutA.Messages {
		a, b := cutA.Messages[i], cutB.Messages[i]
		if a.Key != b.Key || a.EnqueuedLT != b.EnqueuedLT || a.EnvHash != b.EnvHash {
			t.Fatalf("cut diverges at %d: %x/%d vs %x/%d", i, a.Key, a.EnqueuedLT, b.Key, b.EnqueuedLT)
		}
	}
}

// seedFreshFromChain builds a fresh section seeded from the chain state.
func seedFreshFromChain(t *testing.T, entries map[QueueKey]chainEntry, ref SourceRef) *destinationState {
	t.Helper()
	state := stateRootWithQueue(t, queueDictCell(t, queueOfChain(entries)), 0, false)
	msgs, total, err := seedFromStateRoot(state, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	fresh := testInternals(t)
	if err = fresh.Seed(baseSource, ref, msgs, total); err != nil {
		t.Fatal(err)
	}
	return fresh
}

// TestInternalsRebuildEquivalence: a deterministic two-block chain where
// the incrementally advanced run must equal a fresh seed of every reached
// state — including foreign entries that only affect the size.
func TestInternalsRebuildEquivalence(t *testing.T) {
	chain := map[QueueKey]chainEntry{}
	put := func(e chainEntry) chainEntry { chain[e.key] = e; return e }

	ownerA := put(makeChainEntry(t, 0, 1, 100))
	ownerB := put(makeChainEntry(t, 0, 2, 200))
	foreignC := put(makeChainEntry(t, -1, 3, 300))

	incremental := testInternals(t)
	state0 := stateRootWithQueue(t, queueDictCell(t, queueOfChain(chain)), 0, false)
	msgs, total, err := seedFromStateRoot(state0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err = incremental.Seed(baseSource, sref(10, 0x10), msgs, total); err != nil {
		t.Fatal(err)
	}

	// Block 11: consumes ownerA and foreignC, enqueues two more.
	adds1 := []chainEntry{put(makeChainEntry(t, 0, 4, 1100)), put(makeChainEntry(t, -1, 5, 1101))}
	removes1 := []chainEntry{ownerA, foreignC}
	delete(chain, ownerA.key)
	delete(chain, foreignC.key)
	delta1, err := deltaFromBlockRoot(chainBlockRoot(t, adds1, removes1), 0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err = incremental.ApplyBlock(baseSource, sref(11, 0x11), delta1); err != nil {
		t.Fatal(err)
	}
	requireSameView(t, incremental, seedFreshFromChain(t, chain, sref(11, 0x11)), sref(11, 0x11))

	// Block 12: consumes ownerB and one block-11 entry (deq_short path).
	adds2 := []chainEntry{put(makeChainEntry(t, 0, 6, 1200))}
	removes2 := []chainEntry{adds1[0], ownerB}
	delete(chain, adds1[0].key)
	delete(chain, ownerB.key)
	delta2, err := deltaFromBlockRoot(chainBlockRoot(t, adds2, removes2), 0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err = incremental.ApplyBlock(baseSource, sref(12, 0x12), delta2); err != nil {
		t.Fatal(err)
	}
	requireSameView(t, incremental, seedFreshFromChain(t, chain, sref(12, 0x12)), sref(12, 0x12))
}

func TestInternalsRebuildEquivalenceTransitAndDeferred(t *testing.T) {
	src := deltaAddr(0, 0x11)
	requeuedMsg := deltaInternalMsg(t, src, deltaAddr(0, 0x77), 7000)
	oldEnv := deltaEnvelope(t, requeuedMsg, regularNext(32))
	newEnv := deltaEnvelope(t, requeuedMsg, regularNext(96))
	oldParsed, err := internalFromEnvelope(oldEnv)
	if err != nil {
		t.Fatal(err)
	}
	newParsed, err := internalFromEnvelope(newEnv)
	if err != nil {
		t.Fatal(err)
	}
	oldEntry := chainEntry{key: oldParsed.Key, env: oldEnv, lt: 7000}
	newEntry := chainEntry{key: newParsed.Key, env: newEnv, lt: 7000}
	laterEntry := makeChainEntry(t, 0, 0x78, 9000)
	chain := map[QueueKey]chainEntry{oldEntry.key: oldEntry, laterEntry.key: laterEntry}

	incremental := testInternals(t)
	state := stateRootWithQueue(t, queueDictCell(t, queueOfChain(chain)), 0, false)
	entries, total, err := seedFromStateRoot(state, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err = incremental.Seed(baseSource, sref(20, 0x20), entries, total); err != nil {
		t.Fatal(err)
	}

	requeueDescr := newOutDescrDict(t)
	setDescr(t, requeueDescr, requeuedMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b111, 3).
		MustStoreRef(newEnv).
		MustStoreRef(transitImport(t, oldEnv, newEnv)).EndCell())
	delta, err := deltaFromBlockRoot(deltaBlockRoot(t, requeueDescr.AsCell()), 0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err = incremental.ApplyBlock(baseSource, sref(21, 0x21), delta); err != nil {
		t.Fatal(err)
	}
	delete(chain, oldEntry.key)
	chain[newEntry.key] = newEntry
	requireSameView(t, incremental, seedFreshFromChain(t, chain, sref(21, 0x21)), sref(21, 0x21))

	deferredMsg := deltaInternalMsg(t, src, deltaAddr(0, 0x88), 8000)
	deferredEnv := deltaEnvelopeWithEmittedLT(t, deferredMsg, regularNext(96), 8100)
	deferredParsed, err := internalFromEnvelope(deferredEnv)
	if err != nil {
		t.Fatal(err)
	}
	deferredEntry := chainEntry{key: deferredParsed.Key, env: deferredEnv, lt: 8100}
	staysDeferredMsg := deltaInternalMsg(t, src, deltaAddr(0, 0x99), 8200)
	staysDeferredEnv := deltaEnvelope(t, staysDeferredMsg, regularNext(96))

	deferredDescr := newOutDescrDict(t)
	setDescr(t, deferredDescr, deferredMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b10101, 5).
		MustStoreRef(deferredEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 8).EndCell()).EndCell())
	setDescr(t, deferredDescr, staysDeferredMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b10100, 5).
		MustStoreRef(staysDeferredEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(2, 8).EndCell()).EndCell())
	delta, err = deltaFromBlockRoot(deltaBlockRoot(t, deferredDescr.AsCell()), 0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err = incremental.ApplyBlock(baseSource, sref(22, 0x22), delta); err != nil {
		t.Fatal(err)
	}
	chain[deferredEntry.key] = deferredEntry
	requireSameView(t, incremental, seedFreshFromChain(t, chain, sref(22, 0x22)), sref(22, 0x22))
}

// TestInternalsRebuildEquivalenceRandomized fuzzes the same equivalence
// over a longer random chain with mixed owner/foreign adds and removals.
func TestInternalsRebuildEquivalenceRandomized(t *testing.T) {
	rnd := rand.New(rand.NewSource(0xf1e5))
	chain := map[QueueKey]chainEntry{}
	var liveKeys []QueueKey

	incremental := testInternals(t)
	if err := incremental.Seed(baseSource, sref(100, 0x64), nil, 0); err != nil {
		t.Fatal(err)
	}

	nextID := uint64(1)
	for seq := uint32(101); seq <= 112; seq++ {
		var adds, removes []chainEntry

		for i := rnd.Intn(5); i > 0; i-- {
			wc := int32(0)
			if rnd.Intn(3) == 0 {
				wc = -1
			}
			e := makeChainEntry(t, wc, nextID, uint64(seq)*1000+uint64(i))
			nextID++
			adds = append(adds, e)
		}
		for i := rnd.Intn(3); i > 0 && len(liveKeys) > 0; i-- {
			pick := rnd.Intn(len(liveKeys))
			key := liveKeys[pick]
			liveKeys = append(liveKeys[:pick], liveKeys[pick+1:]...)
			removes = append(removes, chain[key])
			delete(chain, key)
		}
		for _, e := range adds {
			chain[e.key] = e
			liveKeys = append(liveKeys, e.key)
		}

		delta, err := deltaFromBlockRoot(chainBlockRoot(t, adds, removes), 0, testOwner)
		if err != nil {
			t.Fatal(err)
		}
		ref := sref(seq, byte(seq))
		if err = incremental.ApplyBlock(baseSource, ref, delta); err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		requireSameView(t, incremental, seedFreshFromChain(t, chain, ref), ref)
	}
}

// TestDeltasFromBlockRootMultiOwner: one descriptor walk feeds disjoint
// owners — the masterchain and the basechain sections of a node holding
// both sessions — with identical totals and disjoint materialization.
func TestDeltasFromBlockRootMultiOwner(t *testing.T) {
	srcBase := deltaAddr(0, 0x11)
	dict := newOutDescrDict(t)
	txStub := cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()

	toBase := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x22), 1000)
	toMC := deltaInternalMsg(t, srcBase, deltaAddr(-1, 0x33), 2000)
	for _, msg := range []*cell.Cell{toBase, toMC} {
		setDescr(t, dict, msg.Hash(), cell.BeginCell().
			MustStoreUInt(0b001, 3).MustStoreRef(deltaEnvelope(t, msg, regularNext(96))).
			MustStoreRef(txStub).EndCell())
	}

	base := ShardIdent{Workchain: 0, Shard: ShardAll}
	mc := ShardIdent{Workchain: -1, Shard: ShardAll}
	deltas, err := deltasFromBlockRoot(deltaBlockRoot(t, dict.AsCell()), 0, base, mc)
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 2 {
		t.Fatalf("deltas = %d", len(deltas))
	}
	if len(deltas[0].Added) != 1 || deltas[0].Added[0].EnqueuedLT != 1000 {
		t.Fatalf("base delta: %+v", deltas[0].Added)
	}
	if len(deltas[1].Added) != 1 || deltas[1].Added[0].EnqueuedLT != 2000 {
		t.Fatalf("mc delta: %+v", deltas[1].Added)
	}
	if deltas[0].AddedTotal != 2 || deltas[1].AddedTotal != 2 {
		t.Fatalf("totals: %d, %d", deltas[0].AddedTotal, deltas[1].AddedTotal)
	}

	// Both sections consume their delta; the size invariant holds in each.
	for i, owner := range []ShardIdent{base, mc} {
		section := newDestinationState(owner)
		if err = section.Seed(baseSource, sref(1, 1), nil, 0); err != nil {
			t.Fatal(err)
		}
		if err = section.ApplyBlock(baseSource, sref(2, 2), deltas[i]); err != nil {
			t.Fatal(err)
		}
		if size, _ := section.queueSize(baseSource); size != 2 {
			t.Fatalf("owner %d queue size = %d", i, size)
		}
	}
}

// TestSeedFeedsInternalsEndToEnd closes the loop: a state-seeded run
// advanced by a block delta serves a correctly ordered cut.
func TestSeedFeedsInternalsEndToEnd(t *testing.T) {
	srcBase := deltaAddr(0, 0x11)

	queuedMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x22), 1000)
	queuedEnv := deltaEnvelope(t, queuedMsg, regularNext(96))
	queuedHop, _ := AccountPrefixFromAddress(deltaAddr(0, 0x22))
	queuedKey := MakeQueueKey(queuedHop, queuedMsg.HashKey())

	state := stateRootWithQueue(t, queueDictCell(t, map[QueueKey]tlb.EnqueuedMsg{
		queuedKey: {EnqueuedLT: 1000, Msg: queuedEnv},
	}), 0, false)
	entries, total, err := seedFromStateRoot(state, testOwner)
	if err != nil {
		t.Fatal(err)
	}

	n := testInternals(t)
	if err = n.Seed(baseSource, sref(7, 0x07), entries, total); err != nil {
		t.Fatal(err)
	}

	newMsg := deltaInternalMsg(t, srcBase, deltaAddr(0, 0x44), 2000)
	newEnv := deltaEnvelope(t, newMsg, regularNext(96))
	dict := newOutDescrDict(t)
	setDescr(t, dict, newMsg.Hash(), cell.BeginCell().
		MustStoreUInt(0b001, 3).MustStoreRef(newEnv).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 8).EndCell()).EndCell())
	delta, err := deltaFromBlockRoot(deltaBlockRoot(t, dict.AsCell()), 0, testOwner)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.ApplyBlock(baseSource, sref(8, 0x08), delta); err != nil {
		t.Fatal(err)
	}

	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(8, 0x08)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1000, 2000)
	if cut.Messages[0].Root.HashKey() != queuedMsg.HashKey() || cut.Messages[1].Root.HashKey() != newMsg.HashKey() {
		t.Fatal("cut messages lost their roots")
	}
	if size, _ := n.queueSize(baseSource); size != 2 {
		t.Fatalf("queue size = %d", size)
	}
}
