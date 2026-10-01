package simplex

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The candidate this file measures and pins is sized like a mainnet shard
// candidate, not like the four-cell fixture the rest of the package uses. Every
// branch on this path is size-gated — a TL bytes field carries a one-byte length
// below 254 and a four-byte one above it, and the whole point of the layout is
// where those headers land — so a fixture that fits in a single byte of length
// exercises none of what is at stake here.
//
// The two BOCs are synthetic but their two numbers are not: the combined bag and
// its LZ4 output are within a few percent of what the collator produced on the
// mainnet block this work was measured against (909,242 B combined, 691,940 B
// compressed). Cell payloads are drawn either from a shared dictionary or from a
// PRNG, in a mix tuned to that compression ratio, because the ratio is what
// decides how much of the buffer the layout has to carry.
const (
	mainnetFixtureCells           = 5200
	mainnetFixtureIncompressible  = 0.70
	mainnetFixtureDictionaryBytes = 4096
)

type mainnetSizedFixture struct {
	candidate     Candidate
	blockRoot     *cell.Cell
	collatedRoots []*cell.Cell
	blockBOC      []byte
	collatedData  []byte
	hint          int
}

// mainnetSizedPayload is memoized: building it costs a PRNG pass over ~900 KB
// and two BOC serializations, and every row in this file wants the same bytes.
var mainnetSizedPayloadCache struct {
	blockRoot     *cell.Cell
	collatedRoots []*cell.Cell
	blockBOC      []byte
	collatedData  []byte
}

func mainnetSizedRoots(tb testing.TB) (*cell.Cell, []*cell.Cell, []byte, []byte) {
	tb.Helper()

	if mainnetSizedPayloadCache.blockRoot != nil {
		c := mainnetSizedPayloadCache

		return c.blockRoot, c.collatedRoots, c.blockBOC, c.collatedData
	}

	// A fixed seed keeps the fixture identical across processes, which is what
	// lets a byte pin compare two runs at all.
	prng := rand.New(rand.NewPCG(0x5f3759df, 0x9e3779b9))
	dictionary := make([]byte, mainnetFixtureDictionaryBytes)
	for i := range dictionary {
		dictionary[i] = byte(prng.Uint32())
	}

	// A cell holds 1023 bits, so 127 bytes is the largest whole-byte payload one
	// carries. Unique payloads matter: the BOC serializer deduplicates identical
	// cells, and a fixture built from repeats would collapse to a fraction of
	// its intended size.
	payload := func() []byte {
		data := make([]byte, 127)
		if prng.Float64() < mainnetFixtureIncompressible {
			for i := range data {
				data[i] = byte(prng.Uint32())
			}

			return data
		}
		offset := int(prng.Uint32N(uint32(len(dictionary) - len(data))))
		copy(data, dictionary[offset:])
		// One unique byte per dictionary cell keeps the cell distinct without
		// spoiling the match LZ4 finds in the rest of it.
		binary.LittleEndian.PutUint32(data[:4], prng.Uint32())

		return data
	}

	// Four refs per cell is the cell limit; a tree of that arity over the cell
	// budget is a few thousand cells deep enough to look like a block without
	// tripping the serializer's depth limits.
	build := func(cells int) *cell.Cell {
		level := make([]*cell.Cell, 0, cells)
		for range cells {
			level = append(level, cell.BeginCell().MustStoreSlice(payload(), 1016).EndCell())
		}
		for len(level) > 1 {
			next := make([]*cell.Cell, 0, (len(level)+3)/4)
			for i := 0; i < len(level); i += 4 {
				b := cell.BeginCell().MustStoreSlice(payload(), 1016)
				for _, ref := range level[i:min(i+4, len(level))] {
					b.MustStoreRef(ref)
				}
				next = append(next, b.EndCell())
			}
			level = next
		}

		return level[0]
	}

	blockRoot := build(mainnetFixtureCells / 3)
	collatedRoots := []*cell.Cell{
		build(mainnetFixtureCells / 3),
		build(mainnetFixtureCells / 3),
	}
	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		tb.Fatal(err)
	}
	collatedData, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		tb.Fatal(err)
	}

	mainnetSizedPayloadCache.blockRoot = blockRoot
	mainnetSizedPayloadCache.collatedRoots = collatedRoots
	mainnetSizedPayloadCache.blockBOC = blockBOC
	mainnetSizedPayloadCache.collatedData = collatedData

	return blockRoot, collatedRoots, blockBOC, collatedData
}

func newMainnetSizedFixture(tb testing.TB, delegated bool) mainnetSizedFixture {
	tb.Helper()

	blockRoot, collatedRoots, blockBOC, collatedData := mainnetSizedRoots(tb)
	fileHash := sha256.Sum256(blockBOC)
	candidate := Candidate{
		Parent: Parent(CandidateID{Slot: 4, Hash: [32]byte{0x44}}),
		Leader: 2,
		Block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     19,
			RootHash:  bytes.Clone(blockRoot.Hash()),
			FileHash:  fileHash[:],
		},
		CollatedFileHash: sha256.Sum256(collatedData),
		Signature:        bytes.Repeat([]byte{0x55}, ed25519.SignatureSize),
	}
	if delegated {
		candidate.Delegation = &Delegation{
			CollatorKey: bytes.Repeat([]byte{0x66}, ed25519.PublicKeySize),
			Signature:   bytes.Repeat([]byte{0x77}, ed25519.SignatureSize),
		}
	}
	candidate.ID = candidate.ComputeID(8)

	return mainnetSizedFixture{
		candidate:     candidate,
		blockRoot:     blockRoot,
		collatedRoots: collatedRoots,
		blockBOC:      blockBOC,
		collatedData:  collatedData,
		hint:          PayloadCellHint(blockBOC, collatedData),
	}
}

func (f mainnetSizedFixture) prepare(tb testing.TB) *PreparedCandidate {
	tb.Helper()

	prepared, err := PrepareCandidate(
		f.candidate.Block.SeqNo,
		f.blockRoot,
		f.collatedRoots,
		sha256.Sum256(f.blockBOC),
		f.candidate.CollatedFileHash,
		f.hint,
	)
	if err != nil {
		tb.Fatal(err)
	}

	return prepared
}

// referenceCandidateWire is the wire as it was built before the payload stopped
// being copied out of the compressor: two independent tl.Serialize passes, one
// wrapping the LZ4 output into validatorSession.compressedCandidate and one
// wrapping that into consensus.block, plus the delegation wrapper. It shares no
// code with the production path — that is the whole point, since the two tests
// that looked like they guarded these bytes called the same function on both
// sides of their comparison and would have stayed green through any change to
// it.
func referenceCandidateWire(tb testing.TB, f mainnetSizedFixture) []byte {
	tb.Helper()

	return referenceWrapDelegation(tb, referenceBareWire(tb, f), f.candidate.Delegation)
}

func referenceBareWire(tb testing.TB, f mainnetSizedFixture) []byte {
	tb.Helper()

	roots := append([]*cell.Cell{f.blockRoot}, f.collatedRoots...)
	combined, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		tb.Fatal(err)
	}
	compressed := make([]byte, lz4.CompressBlockBound(len(combined)))
	size, err := lz4.CompressBlock(combined, compressed, nil)
	if err != nil {
		tb.Fatal(err)
	}
	payload, err := tl.Serialize(ValidatorSessionCompressedCandidate{
		Source:           make([]byte, 32),
		Round:            int32(f.candidate.Block.SeqNo),
		RootHash:         f.candidate.Block.RootHash,
		DecompressedSize: int32(len(combined)),
		Data:             compressed[:size],
	}, true)
	if err != nil {
		tb.Fatal(err)
	}
	wire, err := tl.Serialize(ConsensusBlockData{
		Slot:      int32(f.candidate.ID.Slot),
		Parent:    parentToTL(f.candidate.Parent),
		Candidate: payload,
		Signature: f.candidate.Signature,
	}, true)
	if err != nil {
		tb.Fatal(err)
	}

	return wire
}

func referenceWrapDelegation(tb testing.TB, wire []byte, delegation *Delegation) []byte {
	tb.Helper()

	if delegation == nil {
		return wire
	}
	deleg, err := tl.Serialize(delegationToTL(delegation), true)
	if err != nil {
		tb.Fatal(err)
	}
	out := make([]byte, 8, 8+len(wire)+len(deleg))
	binary.LittleEndian.PutUint32(out[:4], idCandidateWrapped)
	binary.LittleEndian.PutUint32(out[4:], 1)
	out = append(out, wire...)

	return append(out, deleg...)
}

// TestMainnetSizedFixtureIsMainnetSized keeps the rows below honest. A fixture
// that drifted small would stop exercising the four-byte TL length branch and
// every benchmark and pin here would silently become a measurement of the
// four-cell case.
func TestMainnetSizedFixtureIsMainnetSized(t *testing.T) {
	f := newMainnetSizedFixture(t, false)
	roots := append([]*cell.Cell{f.blockRoot}, f.collatedRoots...)
	combined, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	compressed := make([]byte, lz4.CompressBlockBound(len(combined)))
	size, err := lz4.CompressBlock(combined, compressed, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("block BOC %d B, collated %d B, combined %d B, compressed %d B (%.1f%%)",
		len(f.blockBOC), len(f.collatedData), len(combined), size,
		100*float64(size)/float64(len(combined)))
	if len(combined) < 700<<10 || len(combined) > 1200<<10 {
		t.Fatalf("combined BOC is %d B, outside the mainnet band this file measures", len(combined))
	}
	if ratio := float64(size) / float64(len(combined)); ratio < 0.6 || ratio > 0.9 {
		t.Fatalf("compression ratio %.2f is nothing like the mainnet one", ratio)
	}
}

// TestCandidateWireIsByteIdenticalToTheCopyingEncoder is the byte pin. The wire
// is content-addressed inside this node: it is rebuilt on three separate paths
// and compared by hash, and a byte that moved raises ErrCandidateConflict
// against the node's own candidate.
//
// The two tests that look like they already guard this — the prepared-path pair
// in candidate_prepared_test.go — call the same serializer on both sides of
// their comparison, so they gate which route the bytes were reached by and not
// what the bytes are; change the encoder and both arms change together. Here the
// expected side is built out of tl.Serialize alone and shares no code with the
// path under test.
func TestCandidateWireIsByteIdenticalToTheCopyingEncoder(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		name := "ordinary"
		if delegated {
			name = "delegated"
		}
		t.Run(name, func(t *testing.T) {
			f := newMainnetSizedFixture(t, delegated)
			bare := referenceBareWire(t, f)
			wrapped := referenceWrapDelegation(t, bare, f.candidate.Delegation)

			wire, err := SerializeCandidate(f.candidate, f.blockBOC, f.collatedData)
			if err != nil {
				t.Fatal(err)
			}
			assertWireEqual(t, "SerializeCandidate", wrapped, wire)

			wire, err = SerializeCandidatePrepared(f.candidate, f.prepare(t))
			if err != nil {
				t.Fatal(err)
			}
			assertWireEqual(t, "SerializeCandidatePrepared", wrapped, wire)

			broadcast, err := SerializeCandidateForBroadcast(f.candidate, f.blockBOC, f.collatedData)
			if err != nil {
				t.Fatal(err)
			}
			assertWireEqual(t, "SerializeCandidateForBroadcast data", bare, broadcast.Data)
			wire, err = broadcast.CandidateWire(f.candidate.Delegation)
			if err != nil {
				t.Fatal(err)
			}
			assertWireEqual(t, "SerializeCandidateForBroadcast wire", wrapped, wire)

			broadcast, err = SerializeCandidateForBroadcastPrepared(f.candidate, f.prepare(t))
			if err != nil {
				t.Fatal(err)
			}
			assertWireEqual(t, "SerializeCandidateForBroadcastPrepared data", bare, broadcast.Data)
			wire, err = broadcast.CandidateWire(f.candidate.Delegation)
			if err != nil {
				t.Fatal(err)
			}
			assertWireEqual(t, "SerializeCandidateForBroadcastPrepared wire", wrapped, wire)
		})
	}
}

// An empty candidate carries no payload and so no buffer to lay frames into. It
// is pinned all the same: it travels the same two functions, and "the shape with
// nothing in it" is exactly the one a layout change breaks without any of the
// sized rows noticing.
func TestEmptyCandidateWireIsByteIdenticalToTheCopyingEncoder(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		candidate := Candidate{
			Empty:     true,
			Parent:    Parent(CandidateID{Slot: 4, Hash: [32]byte{0x44}}),
			Leader:    2,
			Block:     ton.BlockIDExt{Shard: -1 << 63, SeqNo: 19, RootHash: bytes.Repeat([]byte{0x11}, 32), FileHash: bytes.Repeat([]byte{0x22}, 32)},
			Signature: bytes.Repeat([]byte{0x55}, ed25519.SignatureSize),
		}
		if delegated {
			candidate.Delegation = &Delegation{
				CollatorKey: bytes.Repeat([]byte{0x66}, ed25519.PublicKeySize),
				Signature:   bytes.Repeat([]byte{0x77}, ed25519.SignatureSize),
			}
		}
		candidate.ID = candidate.ComputeID(8)

		bare, err := tl.Serialize(ConsensusEmptyData{
			Slot:      int32(candidate.ID.Slot),
			Parent:    ton.ConsensusCandidateID{Slot: int32(candidate.Parent.ID.Slot), Hash: candidate.Parent.ID.Hash[:]},
			Block:     candidate.Block,
			Signature: candidate.Signature,
		}, true)
		if err != nil {
			t.Fatal(err)
		}
		wrapped := referenceWrapDelegation(t, bare, candidate.Delegation)

		wire, err := SerializeCandidate(candidate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		assertWireEqual(t, "empty SerializeCandidate", wrapped, wire)

		broadcast, err := SerializeCandidateForBroadcast(candidate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		assertWireEqual(t, "empty broadcast data", bare, broadcast.Data)
		wire, err = broadcast.CandidateWire(candidate.Delegation)
		if err != nil {
			t.Fatal(err)
		}
		assertWireEqual(t, "empty broadcast wire", wrapped, wire)
	}
}

func assertWireEqual(t *testing.T, what string, want, got []byte) {
	t.Helper()

	if bytes.Equal(want, got) {
		return
	}
	if len(want) != len(got) {
		t.Fatalf("%s: wire is %d bytes, want %d", what, len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("%s: wire differs at byte %d of %d: %#x, want %#x", what, i, len(want), got[i], want[i])
		}
	}
}

// TestCandidateEmissionWritesTheFramesInPlace is the vacuum gate on the pin
// above. Every fallback in this path produces exactly the bytes the fast path
// does, by design — so a change that silently never took the fast path would
// leave every byte comparison green and every microsecond unsaved. This asserts
// the frames really are written into the payload's own buffer.
func TestCandidateEmissionWritesTheFramesInPlace(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		f := newMainnetSizedFixture(t, delegated)
		prepared := f.prepare(t)

		broadcast, err := SerializeCandidateForBroadcastPrepared(f.candidate, prepared)
		if err != nil {
			t.Fatal(err)
		}
		payload := prepared.payload
		frame := payload.frame.Load()
		if frame == nil {
			t.Fatalf("delegated=%v: consensus.block went through the copying encoder", delegated)
		}
		if &broadcast.Data[0] != &payload.buf[frame.start] {
			t.Fatalf("delegated=%v: broadcast data is a copy, not the payload buffer", delegated)
		}

		wire, err := broadcast.CandidateWire(f.candidate.Delegation)
		if err != nil {
			t.Fatal(err)
		}
		if !delegated {
			continue
		}
		if &wire[0] != &payload.buf[frame.start-8] {
			t.Fatalf("consensus.candidate went through the copying encoder")
		}
	}
}

// A payload frames once. Nothing serializes one twice today, but bind ties a
// capsule to a block and not to a slot, a parent or a signature, so two
// candidates may legally share one — and a second frame written into the same
// room would rewrite the first caller's wire underneath it rather than produce a
// second wire. The claim sends the second build to the copying encoder, which
// must reach the same bytes and must leave the first result alone.
func TestSecondWireFromOnePayloadDoesNotDisturbTheFirst(t *testing.T) {
	f := newMainnetSizedFixture(t, true)
	expected := referenceCandidateWire(t, f)
	prepared := f.prepare(t)

	first, err := SerializeCandidatePrepared(f.candidate, prepared)
	if err != nil {
		t.Fatal(err)
	}
	pinned := bytes.Clone(first)

	// A second candidate over the same block, at a later slot: everything bind
	// compares is equal, everything the frame is made of is not.
	other := f.candidate
	other.Parent = Parent(CandidateID{Slot: 9, Hash: [32]byte{0x99}})
	other.Signature = bytes.Repeat([]byte{0xAA}, ed25519.SignatureSize)
	other.ID = other.ComputeID(11)
	second, err := SerializeCandidatePrepared(other, prepared)
	if err != nil {
		t.Fatal(err)
	}

	assertWireEqual(t, "first wire", expected, first)
	if !bytes.Equal(pinned, first) {
		t.Fatal("the second serialization rewrote the first wire under its holder")
	}
	otherFixture := f
	otherFixture.candidate = other
	assertWireEqual(t, "second wire", referenceCandidateWire(t, otherFixture), second)
}

// A wire built in place lives in a buffer with room left over around it, so its
// capacity must stop at its length. A transport that appended to a wire whose
// capacity ran on would write into the room the delegation wrapper uses — where
// the copying encoder, which returns a buffer grown to exactly its content,
// would have reallocated and left the wire alone.
func TestCandidateWireHasNoSpareCapacity(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		f := newMainnetSizedFixture(t, delegated)
		broadcast, err := SerializeCandidateForBroadcastPrepared(f.candidate, f.prepare(t))
		if err != nil {
			t.Fatal(err)
		}
		if cap(broadcast.Data) != len(broadcast.Data) {
			t.Fatalf("delegated=%v: broadcast data is %d bytes with capacity %d",
				delegated, len(broadcast.Data), cap(broadcast.Data))
		}
		wire, err := broadcast.CandidateWire(f.candidate.Delegation)
		if err != nil {
			t.Fatal(err)
		}
		if cap(wire) != len(wire) {
			t.Fatalf("delegated=%v: wire is %d bytes with capacity %d", delegated, len(wire), cap(wire))
		}
	}
}

// The LZ4 destination is pooled, so a candidate is compressed into a buffer the
// previous one left dirty, and — since the pool keeps whatever was largest — into
// one that can be far longer than this candidate's own CompressBlockBound.
// Neither may reach the wire.
func TestPooledCompressScratchChangesNoPayloadByte(t *testing.T) {
	small := cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell()
	compress := func(root *cell.Cell, roots []*cell.Cell, hint int) []byte {
		t.Helper()

		payload, err := compressCandidatePayload(19, root.Hash(), roots, hint)
		if err != nil {
			t.Fatal(err)
		}

		return bytes.Clone(payload.bytes())
	}

	before := compress(small, []*cell.Cell{small}, 0)
	f := newMainnetSizedFixture(t, false)
	roots := append([]*cell.Cell{f.blockRoot}, f.collatedRoots...)
	large := compress(f.blockRoot, roots, f.hint)
	// Now on a scratch sized and dirtied by a payload two thousand times its own.
	if after := compress(small, []*cell.Cell{small}, 0); !bytes.Equal(before, after) {
		t.Fatalf("the recycled scratch changed a %d-byte payload", len(before))
	}
	if again := compress(f.blockRoot, roots, f.hint); !bytes.Equal(large, again) {
		t.Fatalf("the recycled scratch changed the %d-byte payload", len(large))
	}
}

// One broadcast wrapped twice is the same hazard one payload framed twice is:
// the second delegation goes into the room the first wrapper's result still
// points at. Only the first wrap may write there.
func TestSecondDelegationWrapDoesNotDisturbTheFirst(t *testing.T) {
	f := newMainnetSizedFixture(t, true)
	broadcast, err := SerializeCandidateForBroadcastPrepared(f.candidate, f.prepare(t))
	if err != nil {
		t.Fatal(err)
	}

	first, err := broadcast.CandidateWire(f.candidate.Delegation)
	if err != nil {
		t.Fatal(err)
	}
	pinned := bytes.Clone(first)

	other := &Delegation{
		CollatorKey: bytes.Repeat([]byte{0xEE}, ed25519.PublicKeySize),
		Signature:   bytes.Repeat([]byte{0xDD}, ed25519.SignatureSize),
	}
	second, err := broadcast.CandidateWire(other)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pinned, first) {
		t.Fatal("the second wrap rewrote the first wire under its holder")
	}
	assertWireEqual(t, "second wrap", referenceWrapDelegation(t, referenceBareWire(t, f), other), second)
}

// The frame a payload holds describes one slice of its own buffer. Handed some
// other bytes together with that payload, the wrapper must wrap the bytes it was
// given and not the ones the payload is holding.
//
// The fixture is deliberately non-delegated: a delegated one wraps during
// SerializeCandidatePrepared and leaves no frame behind, so this test would
// exercise nothing and pass whatever the wrapper did — it did exactly that in
// its first form, and stayed green with the identity check deleted.
func TestDelegationWrapperRefusesAFrameItDoesNotDescribe(t *testing.T) {
	f := newMainnetSizedFixture(t, false)
	prepared := f.prepare(t)
	wire, err := SerializeCandidatePrepared(f.candidate, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.payload.frame.Load() == nil {
		t.Fatal("the fixture left no frame to offer the wrapper")
	}

	// Both shorter than the frame and different inside it, so neither a length
	// check alone nor a prefix comparison can be satisfied by the frame.
	foreign := bytes.Clone(wire[:len(wire)/2])
	foreign[0] ^= 0xFF
	delegation := &Delegation{
		CollatorKey: bytes.Repeat([]byte{0x66}, ed25519.PublicKeySize),
		Signature:   bytes.Repeat([]byte{0x77}, ed25519.SignatureSize),
	}
	wrapped, err := wrapCandidateFrame(foreign, prepared.payload, delegation)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wrapped[8:8+len(foreign)], foreign) {
		t.Fatal("the wrapper wrapped the payload's own frame instead of the bytes it was handed")
	}
	// A refused wrap must also leave the frame alone, or the bytes it declined
	// to wrap would have cost the wrap that comes next.
	if prepared.payload.frame.Load() == nil {
		t.Fatal("a refused wrap consumed the frame")
	}
}

// tlBytesFraming is a second implementation of the encoder's own bytes framing,
// which is the only way to place a frame before the bytes it frames. The two
// must agree at every width, and above all at the boundaries the widths change
// on — a fixture that only ever sees one width is the reason the four-cell
// prepared tests could not have caught a mistake here.
func TestTLBytesFramingMatchesEncoder(t *testing.T) {
	for _, length := range []int{
		0, 1, 2, 3, 4, 5, 252, 253, 254, 255, 256, 257,
		1 << 16, 1<<24 - 2, 1<<24 - 1, 1 << 24, 1<<24 + 1, 1<<24 + 3,
	} {
		header, pad, err := tlBytesFraming(length)
		if err != nil {
			t.Fatalf("length %d: %v", length, err)
		}
		encoded, err := tl.AppendBytes(nil, make([]byte, length))
		if err != nil {
			t.Fatalf("length %d: %v", length, err)
		}
		if header+length+pad != len(encoded) {
			t.Fatalf("length %d: framed %d+%d+%d bytes, encoder wrote %d",
				length, header, length, pad, len(encoded))
		}
		buf := make([]byte, header)
		if at := putTLBytesHeader(buf, 0, length); at != header {
			t.Fatalf("length %d: header write advanced %d, want %d", length, at, header)
		}
		if !bytes.Equal(buf, encoded[:header]) {
			t.Fatalf("length %d: header %x, encoder wrote %x", length, buf, encoded[:header])
		}
	}
	if _, _, err := tlBytesFraming(-1); err == nil {
		t.Fatal("a negative length was framed")
	}
}

// The two constructor ids the in-place frames write are derived from scheme
// strings rather than read out of the encoder's tables. A drift between the two
// would put a wrong four bytes at the front of every candidate.
func TestInPlaceConstructorIDsMatchTheEncoder(t *testing.T) {
	compressed, err := tl.Serialize(ValidatorSessionCompressedCandidate{
		Source: make([]byte, 32), RootHash: make([]byte, 32),
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if id := binary.LittleEndian.Uint32(compressed[:4]); id != idCompressedCandidate {
		t.Fatalf("validatorSession.compressedCandidate id is %#x, encoder writes %#x", idCompressedCandidate, id)
	}
	block, err := tl.Serialize(ConsensusBlockData{Parent: parentToTL(ParentID{})}, true)
	if err != nil {
		t.Fatal(err)
	}
	if id := binary.LittleEndian.Uint32(block[:4]); id != idConsensusBlock {
		t.Fatalf("consensus.block id is %#x, encoder writes %#x", idConsensusBlock, id)
	}
}

// BenchmarkCandidateEmissionWrapMainnet is the work the emission goroutine does
// after the background capsule is already finished: wrap the compressed payload
// into consensus.block and, for a delegated candidate, into consensus.candidate.
// This is the row that matters — the capsule's head start covers everything
// before it and nothing after it.
//
// Every iteration needs its own capsule, because framing consumes the one it is
// handed. Building one leaves ~3 MB of garbage, and a collection of that landing
// inside the next timed section swamps a measurement two orders of magnitude
// smaller — the first version of this row read between 115 us and 1.56 ms for
// the same work.
//
// So capsules are built a batch at a time with the clock stopped, and collected
// once per batch rather than once per iteration. Collecting per iteration also
// gives a stable number, but a misleading one: the first allocation after a full
// collection of a multi-megabyte heap costs about 19 us here, against the ~1 us
// the same call takes in steady state, and that floor lands on both arms and
// hides most of the difference between them. Amortised over a batch it is
// ~0.6 us. The batch is small enough that the capsules it holds live are a
// couple of dozen megabytes and no more.
const emissionBenchBatch = 32

func BenchmarkCandidateEmissionWrapMainnet(b *testing.B) {
	for _, arm := range []struct {
		name      string
		delegated bool
	}{{"plain", false}, {"delegated", true}} {
		b.Run(arm.name, func(b *testing.B) {
			f := newMainnetSizedFixture(b, arm.delegated)
			var batch []*PreparedCandidate
			b.ReportAllocs()
			for b.Loop() {
				if len(batch) == 0 {
					b.StopTimer()
					batch = make([]*PreparedCandidate, emissionBenchBatch)
					for i := range batch {
						batch[i] = f.prepare(b)
					}
					runtime.GC()
					b.StartTimer()
				}
				prepared := batch[len(batch)-1]
				batch = batch[:len(batch)-1]

				broadcast, err := SerializeCandidateForBroadcastPrepared(f.candidate, prepared)
				if err != nil {
					b.Fatal(err)
				}
				wire, err := broadcast.CandidateWire(f.candidate.Delegation)
				if err != nil {
					b.Fatal(err)
				}
				if len(wire) == 0 {
					b.Fatal("empty wire")
				}
			}
		})
	}
}

// BenchmarkCandidatePayloadCompressMainnet is the capsule's own work: the
// combined BOC, the LZ4 pass, and whatever it takes to reach a payload the
// emission path can wrap. Its wall time is off the critical path, but its
// allocations are not — they are garbage the collator's GC pays for in the same
// slot.
func BenchmarkCandidatePayloadCompressMainnet(b *testing.B) {
	f := newMainnetSizedFixture(b, false)
	b.ReportAllocs()
	for b.Loop() {
		if f.prepare(b) == nil {
			b.Fatal("no payload")
		}
	}
}
