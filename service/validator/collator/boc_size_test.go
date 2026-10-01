package collator

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCollatedBOCSizeMatchesSerialization(t *testing.T) {
	shared := cell.BeginCell().MustStoreUInt(0xCAFE, 16).EndCell()
	equalShared := cell.BeginCell().MustStoreUInt(0xCAFE, 16).EndCell()
	left := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(shared).EndCell()
	right := cell.BeginCell().MustStoreUInt(0, 1).MustStoreRef(shared).EndCell()
	deep := shared
	for range maxCollatedBOCDepth {
		deep = cell.BeginCell().MustStoreRef(deep).EndCell()
	}

	manyRoots := make([]*cell.Cell, 256)
	for i := range manyRoots {
		manyRoots[i] = cell.BeginCell().MustStoreUInt(uint64(i), 16).EndCell()
	}

	tests := []struct {
		name  string
		roots []*cell.Cell
	}{
		{name: "empty"},
		{name: "single", roots: []*cell.Cell{shared}},
		{name: "duplicate roots", roots: []*cell.Cell{shared, shared}},
		{name: "equal root hashes", roots: []*cell.Cell{shared, equalShared}},
		{name: "shared subtree", roots: []*cell.Cell{left, right, shared}},
		{name: "maximum depth", roots: []*cell.Cell{deep}},
		// A 16-bit leaf occupies four data bytes including its descriptors.
		// These two cases straddle the data-offset width boundary at 256.
		{name: "one-byte data offset", roots: manyRoots[:63]},
		{name: "two-byte data offset", roots: manyRoots[:64]},
		// These straddle the cell/root reference width boundary at 256 cells.
		{name: "one-byte cell indexes", roots: manyRoots[:255]},
		{name: "two-byte cell indexes", roots: manyRoots},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want, err := cell.ToBOCWithOptionsErr(test.roots, cell.BOCSerializeOptions{WithCRC32C: true})
			if err != nil {
				t.Fatal(err)
			}
			got, err := collatedBOCSize(test.roots)
			if err != nil {
				t.Fatal(err)
			}
			if got != uint64(len(want)) {
				t.Fatalf("collated BOC size = %d, want %d", got, len(want))
			}
		})
	}
}

func TestCollatedBOCSizeLazyFallback(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(0xCAFE, 16).EndCell()
	source := cell.BeginCell().MustStoreUInt(0xAA, 8).MustStoreRef(child).EndCell()
	sourceBOC, err := source.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:  true,
		WithIndex:   true,
		WithTopHash: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lazyRoots, _, err := cell.FromBOCMultiRootReader(
		cell.NewBOCNoCopyReader(sourceBOC),
		cell.BOCParseOptions{Lazy: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, resident, residentErr := residentCollatedBOCSize(lazyRoots); residentErr != nil || resident {
		t.Fatalf("lazy graph fast-path result = resident %v, error %v; want serializer fallback", resident, residentErr)
	}
	assertCollatedBOCSizeParity(t, lazyRoots)

	lazyChild, err := lazyRoots[0].PeekRef(0)
	if err != nil {
		t.Fatal(err)
	}
	if !lazyChild.IsLazy() {
		t.Fatal("lazy fixture child was materialized before measurement")
	}
	duplicateRoots := []*cell.Cell{child, lazyChild}
	if _, resident, residentErr := residentCollatedBOCSize(duplicateRoots); residentErr != nil || !resident {
		t.Fatalf("deduplicated lazy graph fast-path result = resident %v, error %v", resident, residentErr)
	}
	assertCollatedBOCSizeParity(t, duplicateRoots)
}

func TestCollatedBOCSizePreservesSerializerErrors(t *testing.T) {
	if _, err := collatedBOCSize([]*cell.Cell{nil}); err == nil || err.Error() != "cell is nil" {
		t.Fatalf("nil root error = %v", err)
	}

	leaf := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	branch := cell.BeginCell().MustStoreRef(leaf).EndCell()
	root := cell.BeginCell().MustStoreRef(branch).EndCell()
	proof, err := root.CreateProof(cell.CreateProofSkeleton())
	if err != nil {
		t.Fatal(err)
	}
	pruned := proof.MustPeekRef(0).MustPeekRef(0)
	virtual := pruned.Virtualize(0)
	if !virtual.IsVirtualized() {
		t.Fatal("virtualized error fixture was not virtualized")
	}
	if _, err = collatedBOCSize([]*cell.Cell{virtual}); !errors.Is(err, cell.ErrVirtualizedCell) {
		t.Fatalf("virtualized root error = %v, want ErrVirtualizedCell", err)
	}

	// The virtual view represents branch's level-zero hash. Canonical BOC
	// deduplication sees the resident branch first and therefore never rejects
	// the duplicate virtual root; the size walk must keep the same ordering.
	assertCollatedBOCSizeParity(t, []*cell.Cell{branch, virtual})
}

func TestCollatedBOCSizeArithmeticBoundaries(t *testing.T) {
	tests := []struct {
		value uint64
		want  uint64
	}{
		{value: 0, want: 1},
		{value: 255, want: 1},
		{value: 256, want: 2},
		{value: 1<<16 - 1, want: 2},
		{value: 1 << 16, want: 3},
		{value: math.MaxUint64, want: 8},
	}
	for _, test := range tests {
		if got := bocUintBytes(test.value); got != test.want {
			t.Fatalf("BOC uint width for %d = %d, want %d", test.value, got, test.want)
		}
	}
	if _, ok := checkedBOCAdd(math.MaxUint64, 1); ok {
		t.Fatal("BOC size addition overflow was accepted")
	}
	if _, ok := checkedBOCMul(math.MaxUint64, 2); ok {
		t.Fatal("BOC size multiplication overflow was accepted")
	}
}

func assertCollatedBOCSizeParity(t *testing.T, roots []*cell.Cell) {
	t.Helper()

	want, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := collatedBOCSize(roots)
	if err != nil {
		t.Fatal(err)
	}
	if got != uint64(len(want)) {
		t.Fatalf("collated BOC size = %d, want %d", got, len(want))
	}
}

func BenchmarkCollatedBOCSize(b *testing.B) {
	for _, leaves := range []int{64, 16_384} {
		b.Run(fmt.Sprintf("leaves=%d", leaves), func(b *testing.B) {
			roots := benchmarkCollatedBOCRoots(b, leaves)

			b.Run("serialize", func(b *testing.B) {
				b.ReportAllocs()
				var sink []byte
				for b.Loop() {
					var err error
					sink, err = cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
					if err != nil {
						b.Fatal(err)
					}
				}
				_ = sink
			})

			b.Run("measure", func(b *testing.B) {
				b.ReportAllocs()
				var sink uint64
				for b.Loop() {
					var err error
					sink, err = collatedBOCSize(roots)
					if err != nil {
						b.Fatal(err)
					}
				}
				_ = sink
			})
		})
	}
}

func benchmarkCollatedBOCRoots(tb testing.TB, leaves int) []*cell.Cell {
	tb.Helper()

	level := make([]*cell.Cell, leaves)
	for i := range level {
		level[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()
	}
	for len(level) > 1 {
		next := make([]*cell.Cell, 0, (len(level)+3)/4)
		for start := 0; start < len(level); start += 4 {
			end := min(start+4, len(level))
			builder := cell.BeginCell().MustStoreUInt(uint64(start), 32)
			for _, child := range level[start:end] {
				builder.MustStoreRef(child)
			}
			next = append(next, builder.EndCell())
		}
		level = next
	}

	return level
}
