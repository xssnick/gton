package simplex

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// l3Shape is one candidate payload in the two forms the hint is derived from
// and the one it is checked against: the block BOC, the collated data, and the
// roots the combined broadcast BOC is written over.
type l3Shape struct {
	name          string
	blockRoot     *cell.Cell
	collatedRoots []*cell.Cell
	blockBOC      []byte
	collatedData  []byte
}

func (s l3Shape) combined() []*cell.Cell {
	roots := make([]*cell.Cell, 1, len(s.collatedRoots)+1)
	roots[0] = s.blockRoot
	return append(roots, s.collatedRoots...)
}

func newL3Shape(t testing.TB, name string, blockRoot *cell.Cell, collatedRoots []*cell.Cell) l3Shape {
	t.Helper()

	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	collatedData, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}

	return l3Shape{
		name:          name,
		blockRoot:     blockRoot,
		collatedRoots: collatedRoots,
		blockBOC:      blockBOC,
		collatedData:  collatedData,
	}
}

// l3Tree builds a tree of roughly 4/3*leaves distinct cells, tagged so two trees
// never dedup into one another. It folds by fours rather than chaining so a
// large shape stays far from the cell depth limit; the exact cell count is never
// assumed anywhere — it is read back out of the BOC the serializer writes.
func l3Tree(tag uint64, leaves int) *cell.Cell {
	if leaves < 1 {
		leaves = 1
	}
	var made uint64
	build := func(refs []*cell.Cell) *cell.Cell {
		b := cell.BeginCell().MustStoreUInt(tag, 64).MustStoreUInt(made, 64)
		made++
		for _, ref := range refs {
			b.MustStoreRef(ref)
		}
		return b.EndCell()
	}

	level := make([]*cell.Cell, 0, leaves)
	for range leaves {
		level = append(level, build(nil))
	}
	for len(level) > 1 {
		next := make([]*cell.Cell, 0, (len(level)+3)/4)
		for i := 0; i < len(level); i += 4 {
			next = append(next, build(level[i:min(i+4, len(level))]))
		}
		level = next
	}

	return level[0]
}

func l3Shapes(t testing.TB) []l3Shape {
	t.Helper()

	// A block whose subtree the collated roots also carry. This is the case the
	// hint is loosest on — the union is far smaller than the sum — and the one
	// that would break an "exact count" claim.
	shared := l3Tree(0x5a4ed, 300)
	overlapping := cell.BeginCell().MustStoreUInt(0xb10c, 32).MustStoreRef(shared).EndCell()

	return []l3Shape{
		newL3Shape(t, "tiny", cell.BeginCell().MustStoreUInt(1, 8).EndCell(),
			[]*cell.Cell{cell.BeginCell().MustStoreUInt(2, 8).EndCell()}),
		newL3Shape(t, "disjoint", l3Tree(0xb10c, 400),
			[]*cell.Cell{l3Tree(0xc0114, 200), l3Tree(0xdada, 150)}),
		newL3Shape(t, "overlapping", overlapping,
			[]*cell.Cell{shared, l3Tree(0xe57a, 40)}),
		newL3Shape(t, "one-sided", l3Tree(0xb16, 3000),
			[]*cell.Cell{cell.BeginCell().MustStoreUInt(0xf00, 32).EndCell()}),
	}
}

// l3CombinedCells is the figure the hint must never fall below: the cells count
// the combined broadcast BOC actually declares, read back out of the bytes the
// serializer produced.
func l3CombinedCells(t testing.TB, shape l3Shape) int {
	t.Helper()

	combined, err := cell.ToBOCWithOptionsErr(shape.combined(), cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	cells := bocDeclaredCells(combined)
	if cells <= 0 {
		t.Fatalf("%s: combined BOC declared %d cells", shape.name, cells)
	}

	return cells
}

// The hint is not an estimate, and the whole safety argument rests on that: the
// combined bag holds the union of the two cell sets the two BOCs declare, so
// their sum cannot be below it. If this ever fails, the hint has started
// under-sizing a table it presizes eagerly and the serializer will rehash on the
// broadcast path — which is the failure the hint exists to remove.
func TestL3HintIsUpperBound(t *testing.T) {
	for _, shape := range l3Shapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			hint := PayloadCellHint(shape.blockBOC, shape.collatedData)
			actual := l3CombinedCells(t, shape)
			declared := bocDeclaredCells(shape.blockBOC) + bocDeclaredCells(shape.collatedData)

			if declared < actual {
				t.Fatalf("declared sum %d is below the %d cells the combined BOC holds — "+
					"the union argument is broken", declared, actual)
			}
			// Zero is the floor talking, and the floor is only allowed to fire on
			// a payload too small for a presize to beat the default sizing.
			if hint == 0 {
				if actual >= payloadHintFloorCells {
					t.Fatalf("hint dropped to the default for a payload of %d cells", actual)
				}
				return
			}
			if hint < actual {
				t.Fatalf("hint %d is below the %d cells the combined BOC holds", hint, actual)
			}
			t.Logf("%s: hint %d, actual %d, block %d cells/%d B, collated %d cells/%d B",
				shape.name, hint, actual,
				bocDeclaredCells(shape.blockBOC), len(shape.blockBOC),
				bocDeclaredCells(shape.collatedData), len(shape.collatedData))
		})
	}
}

// The hint sizes scratch structures and nothing else, so every value of it —
// right, absent, absurd in either direction — must produce the same bytes. This
// is the gate that lets the hint be tuned without re-proving the wire.
func TestL3HintChangesNoByte(t *testing.T) {
	for _, shape := range l3Shapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			actual := l3CombinedCells(t, shape)
			want, err := cell.ToBOCWithOptionsErr(shape.combined(), cell.BOCSerializeOptions{WithCRC32C: true})
			if err != nil {
				t.Fatal(err)
			}
			for _, hint := range []int{
				0, -1, 1, actual / 10, actual - 1, actual, actual + 1, 10 * actual,
				payloadHintFloorCells, payloadHintMaxPresizedCells,
				PayloadCellHint(shape.blockBOC, shape.collatedData),
			} {
				got, err := cell.ToBOCWithOptionsErr(shape.combined(), cell.BOCSerializeOptions{
					WithCRC32C:     true,
					CellsCountHint: hint,
				})
				if err != nil {
					t.Fatalf("hint %d: %v", hint, err)
				}
				if !bytes.Equal(want, got) {
					t.Fatalf("hint %d changed the combined BOC", hint)
				}
			}
		})
	}
}

// l3AllocatedPerCall is the bytes and objects one serialization ALLOCATES at the
// given hint, taken as a difference of counts over a settled heap. TotalAlloc and
// Mallocs are cumulative and never decrease, so their deltas over a single call
// are exactly what that call asked the allocator for — the quantity a presize
// inflates.
//
// It is deliberately not called a peak, and the table below does not report one.
// A cumulative allocation delta and a peak live footprint are different
// quantities: this one counts every byte handed out even if it was freed
// immediately, while a peak would need HeapAlloc sampled at the worst instant
// inside the call. Reporting this number under the word "peak" is how a table
// starts answering a question it never measured. If the peak is ever wanted it
// needs its own probe.
func l3AllocatedPerCall(t testing.TB, roots []*cell.Cell, hint int) (bytesAlloc, objects uint64) {
	t.Helper()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	boc, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{
		WithCRC32C:     true,
		CellsCountHint: hint,
	})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(boc)

	return after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs
}

// A hint that helps when it is right and explodes when it is stale is not a win.
// This is the gate on the wrong-hint half: every way the hint can be wrong is
// driven through PayloadCellHint, and what comes out is bounded by the bytes the
// caller is holding, not by the number somebody wrote in a header.
func TestL3HintDegradesUnderWrongHints(t *testing.T) {
	shape := newL3Shape(t, "degradation", l3Tree(0xb10c, 2400),
		[]*cell.Cell{l3Tree(0xc0114, 1200), l3Tree(0xdada, 400)})
	roots := shape.combined()
	actual := l3CombinedCells(t, shape)
	right := PayloadCellHint(shape.blockBOC, shape.collatedData)
	if right < actual {
		t.Fatalf("hint %d under the actual %d", right, actual)
	}

	// A block of a very different size: the fixture's own hint taken from a
	// payload two orders of magnitude smaller.
	foreign := newL3Shape(t, "foreign", l3Tree(0x0ffb, 24),
		[]*cell.Cell{l3Tree(0x0ffc, 8)})

	// Headers a caller must survive: a forged four-byte cells count, and a
	// buffer truncated to the header. Neither may reach the serializer.
	forgedBlock := bytes.Clone(shape.blockBOC)
	sizeBytes := int(forgedBlock[4] & 0b111)
	for i := range sizeBytes {
		forgedBlock[bocHeaderCellsOffset+i] = 0xff
	}
	truncated := bytes.Clone(shape.blockBOC)[:bocHeaderCellsOffset]

	want, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}

	type arm struct {
		name string
		hint int
	}
	arms := []arm{
		{"none (hint absent)", 0},
		{"right", right},
		{"10x too low", right / 10},
		{"10x too high (raw, bypasses the bound)", 10 * right},
		{"foreign block", PayloadCellHint(foreign.blockBOC, foreign.collatedData)},
		{"forged header 0xffffffff", PayloadCellHint(forgedBlock, shape.collatedData)},
		{"truncated header", PayloadCellHint(truncated, shape.collatedData)},
		{"ceiling", payloadHintMaxPresizedCells},
	}

	t.Logf("combined BOC: %d cells, %d bytes; block %d B, collated %d B",
		actual, len(want), len(shape.blockBOC), len(shape.collatedData))
	for _, a := range arms {
		gotBytes, gotObjects := l3AllocatedPerCall(t, roots, a.hint)
		got, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{
			WithCRC32C:     true,
			CellsCountHint: a.hint,
		})
		if err != nil {
			t.Fatalf("%s: %v", a.name, err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("%s: hint %d changed the combined BOC", a.name, a.hint)
		}
		t.Logf("%-42s hint %8d  allocated %9s  mallocs %7d", a.name, a.hint,
			fmt.Sprintf("%d B", gotBytes), gotObjects)
	}

	// The two headers nobody should trust must not reach the serializer with a
	// number taken from them. The forged block header declares 2^32-1 cells; what
	// comes out cannot exceed what its own buffer could hold.
	forgedHint := PayloadCellHint(forgedBlock, shape.collatedData)
	if limit := len(forgedBlock)/bocMinPayloadBytesPerCell +
		len(shape.collatedData)/bocMinPayloadBytesPerCell; forgedHint > limit {
		t.Fatalf("a forged header produced hint %d, above the %d cells its bytes could hold",
			forgedHint, limit)
	}
	if forgedHint > payloadHintMaxPresizedCells {
		t.Fatalf("a forged header produced hint %d, above the %d ceiling",
			forgedHint, payloadHintMaxPresizedCells)
	}
	// A truncated buffer has no readable count, so the block half contributes
	// nothing and the payload falls back to the collated half alone.
	if got, want := PayloadCellHint(truncated, shape.collatedData),
		PayloadCellHint(nil, shape.collatedData); got != want {
		t.Fatalf("a truncated header contributed %d cells, want %d", got, want)
	}
	// A hint from a block two orders of magnitude smaller is wrong downward, and
	// wrong downward is the harmless direction: the serializer grows as it always
	// did. It must still be below the right one, or the arm is not testing that.
	if foreignHint := PayloadCellHint(foreign.blockBOC, foreign.collatedData); foreignHint >= right {
		t.Fatalf("the foreign hint %d is not smaller than the right one %d", foreignHint, right)
	}
}

// The three bounds, each on its own, on inputs chosen to make exactly one of
// them decide the answer.
func TestL3HintBounds(t *testing.T) {
	// Ceiling: two headers each declaring more cells than the ceiling, in
	// buffers long enough that the byte clamp does not fire first. The magic is
	// written because bocDeclaredCells requires it — a synthetic buffer without
	// one is not a header at all, and a bounds test built on a buffer the reader
	// rejects outright proves nothing about the bound it names.
	huge := make([]byte, 4<<20)
	copy(huge, bocSerializedMagic[:])
	huge[4] = 0x04 // ref size 4 bytes
	for i := range 4 {
		huge[bocHeaderCellsOffset+i] = 0xff
	}
	if got := PayloadCellHint(huge, huge); got != payloadHintMaxPresizedCells {
		t.Fatalf("two 2^32-cell headers hinted %d, want the %d ceiling",
			got, payloadHintMaxPresizedCells)
	}

	// Byte clamp: one header declaring far more cells than its own buffer could
	// hold. len/2 is the answer, not the ceiling and not the declared count.
	small := make([]byte, 1024)
	copy(small, bocSerializedMagic[:])
	small[4] = 0x04
	for i := range 4 {
		small[bocHeaderCellsOffset+i] = 0xff
	}
	if got, want := PayloadCellHint(small, nil), len(small)/bocMinPayloadBytesPerCell; got != want {
		t.Fatalf("a 1 KiB buffer declaring 2^32 cells hinted %d, want %d", got, want)
	}

	// Floor: a real, well-formed, tiny payload. Below the serializer's own
	// initial capacity a presize cannot beat the sizing it replaces.
	tiny := newL3Shape(t, "tiny", cell.BeginCell().MustStoreUInt(1, 8).EndCell(),
		[]*cell.Cell{cell.BeginCell().MustStoreUInt(2, 8).EndCell()})
	if got := PayloadCellHint(tiny.blockBOC, tiny.collatedData); got != 0 {
		t.Fatalf("a two-cell payload hinted %d, want the default sizing", got)
	}

	// And a header that is not one: nothing readable contributes nothing.
	for _, boc := range [][]byte{nil, {}, {0xb5}, {0xb5, 0xee, 0x9c, 0x72, 0x00, 0x01},
		{0xb5, 0xee, 0x9c, 0x72, 0x05, 0x01, 0, 0, 0, 0, 0}} {
		if got := bocDeclaredCells(boc); got != 0 {
			t.Fatalf("a %d-byte non-header declared %d cells", len(boc), got)
		}
	}

	// Wrong magic contributes nothing either, however well-formed the rest of the
	// header is. This is the one bound that has no fallback behind it: the byte
	// clamp and the ceiling both bound a number that was still read out of
	// somebody's buffer, and on a buffer that is not a BOC there is no reason to
	// believe offset 6 holds a cell count at all. The two legacy magics the cell
	// parser still accepts are refused here for the same reason, even though they
	// happen to keep the counter at the same offset.
	wellFormed := make([]byte, 64<<10)
	copy(wellFormed, bocSerializedMagic[:])
	wellFormed[4] = 0x04
	wellFormed[bocHeaderCellsOffset+3] = 0x80 // 128 cells, comfortably readable
	if got := bocDeclaredCells(wellFormed); got != 128 {
		t.Fatalf("a well-formed header declared %d cells, want 128", got)
	}
	for name, magic := range map[string][4]byte{
		"legacy indexed":       {0x68, 0xFF, 0x65, 0xF3},
		"legacy indexed crc32": {0xAC, 0xC3, 0xA7, 0x28},
		"not a boc":            {0x00, 0x00, 0x00, 0x00},
		"one byte off":         {0xB5, 0xEE, 0x9C, 0x73},
	} {
		foreign := bytes.Clone(wellFormed)
		copy(foreign, magic[:])
		if got := bocDeclaredCells(foreign); got != 0 {
			t.Fatalf("a %s header declared %d cells, want none", name, got)
		}
		if got := PayloadCellHint(foreign, foreign); got != 0 {
			t.Fatalf("two %s headers hinted %d, want none", name, got)
		}
	}
}

// The counts entry point is what a producer that has no headers to read uses,
// and it is fed two numbers of its own rather than two numbers clamped to the
// buffers they came out of. The byte clamp therefore does not stand behind it,
// which leaves the ceiling and the floor carrying the whole bound — and leaves
// the sum itself, taken over inputs that are already near the top of the range,
// as the one place a bounded hint could turn into an unbounded or negative one.
func TestL3HintFromCountsBounds(t *testing.T) {
	for name, counts := range map[string][2]int{
		"both at the ceiling": {payloadHintMaxPresizedCells, payloadHintMaxPresizedCells},
		"one over it":         {payloadHintMaxPresizedCells + 1, 0},
		"both at max int":     {int(^uint(0) >> 1), int(^uint(0) >> 1)},
		"one at max int":      {int(^uint(0) >> 1), 1},
	} {
		if got := PayloadCellHintFromCounts(counts[0], counts[1]); got != payloadHintMaxPresizedCells {
			t.Fatalf("%s hinted %d, want the %d ceiling", name, got, payloadHintMaxPresizedCells)
		}
	}

	// Below the serializer's own initial capacity, and any count that cannot be
	// one, drop the hint rather than presize to it.
	for name, counts := range map[string][2]int{
		"nothing":              {0, 0},
		"one below the floor":  {payloadHintFloorCells - 1, 0},
		"negative block":       {-1, payloadHintFloorCells * 4},
		"negative collated":    {payloadHintFloorCells * 4, -1},
		"negative both":        {-1, -1},
		"negative at min int":  {-1 - int(^uint(0)>>1), -1 - int(^uint(0)>>1)},
		"negative summing low": {payloadHintFloorCells, -payloadHintFloorCells},
	} {
		if got := PayloadCellHintFromCounts(counts[0], counts[1]); got != 0 {
			t.Fatalf("%s hinted %d, want the default sizing", name, got)
		}
	}

	// And in between it is the plain sum, which is what makes it the same
	// |B| + |C| construction the header entry point applies.
	if got := PayloadCellHintFromCounts(payloadHintFloorCells, payloadHintFloorCells); got != 2*payloadHintFloorCells {
		t.Fatalf("two floor-sized counts hinted %d, want %d", got, 2*payloadHintFloorCells)
	}
}

// The gate one level up from the BOC: compressCandidatePayload is the function
// the hint was threaded into, and what it returns is the LZ4 frame and the TL
// box a broadcast carries. Both are pure functions of the combined BOC, so if
// that is stable under the hint these are too — but that is the kind of thing
// worth asserting rather than reasoning about, since it is the actual wire.
func TestL3HintChangesNoPayloadByte(t *testing.T) {
	for _, shape := range l3Shapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			actual := l3CombinedCells(t, shape)
			rootHash := shape.blockRoot.Hash()
			want, err := compressCandidatePayload(19, rootHash, shape.combined(), 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, hint := range []int{
				-1, 0, 1, actual - 1, actual, actual + 1, 10 * actual,
				payloadHintFloorCells, payloadHintMaxPresizedCells,
				PayloadCellHint(shape.blockBOC, shape.collatedData),
			} {
				got, err := compressCandidatePayload(19, rootHash, shape.combined(), hint)
				if err != nil {
					t.Fatalf("hint %d: %v", hint, err)
				}
				if !bytes.Equal(want.bytes(), got.bytes()) {
					t.Fatalf("hint %d changed the %d-byte broadcast payload", hint, len(want.bytes()))
				}
			}
		})
	}
}
