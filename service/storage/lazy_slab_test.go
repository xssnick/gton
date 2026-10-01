package storage

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func slabTestCell(t testing.TB, seq uint64, refs int) *cell.Cell {
	builder := cell.BeginCell().MustStoreUInt(seq, 64).MustStoreUInt(uint64(refs), 8)
	for i := 0; i < refs; i++ {
		builder.MustStoreRef(cell.BeginCell().MustStoreUInt(seq*8+uint64(i), 64).EndCell())
	}
	return builder.EndCell()
}

func encodeSlabTestCell(t testing.TB, c *cell.Cell) []byte {
	record, err := CellRecordFromCell(c)
	if err != nil {
		t.Fatal(err)
	}
	return EncodeCellRecord(record)
}

// The decoder must return a cell that hashes as the record says and whose
// references resolve through the loader to the children the record named.
func TestDecodeLazyRecordResolves(t *testing.T) {
	for refs := 0; refs <= 4; refs++ {
		t.Run(fmt.Sprintf("refs%d", refs), func(t *testing.T) {
			c := slabTestCell(t, 0x1000+uint64(refs), refs)
			encoded := encodeSlabTestCell(t, c)

			children := make(map[cell.Hash][]byte, refs)
			for i := 0; i < refs; i++ {
				child := cell.BeginCell().MustStoreUInt((0x1000+uint64(refs))*8+uint64(i), 64).EndCell()
				children[child.HashKey()] = encodeSlabTestCell(t, child)
			}
			var loader cell.LazyCellLoader
			loader = func(h cell.Hash) (*cell.Cell, error) {
				encodedChild, ok := children[h]
				if !ok {
					return nil, fmt.Errorf("unknown child %x", h)
				}
				return DecodeLazyCellRecordTrusted(h[:], encodedChild, loader)
			}

			hash := c.Hash()
			decoded, err := DecodeLazyCellRecordTrusted(hash, encoded, loader)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.HashKey() != c.HashKey() {
				t.Fatalf("hash mismatch")
			}
			ss, err := decoded.BeginParse()
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < refs; i++ {
				if _, err := ss.LoadRefCell(); err != nil {
					t.Fatalf("ref %d: %v", i, err)
				}
			}
		})
	}
}

// The point of the slab layout now behind DecodeLazyCellRecordTrusted: a
// resident decoded cell with two references costs ONE live object, where the
// per-object construction it replaced cost eight. Pinned by HeapObjects deltas
// with the population and its inputs kept alive to the end — freeing anything
// mid-measure lets sweep accounting turn the delta negative — and with a real
// loader, because a nil loader would skip the placeholder metas. The forced-GC
// timing is logged for information only.
func TestSlabDecodeCutsLiveObjectsPerEntry(t *testing.T) {
	const n = 20000

	encoded := make([][]byte, n)
	hashes := make([][]byte, n)
	for i := range encoded {
		c := slabTestCell(t, uint64(i), 2)
		encoded[i] = encodeSlabTestCell(t, c)
		hashes[i] = c.Hash()
	}
	loader := func(h cell.Hash) (*cell.Cell, error) {
		return nil, fmt.Errorf("not resolved in this test")
	}

	snapshot := func() int64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int64(m.HeapObjects)
	}
	build := func(decode func(hash, encoded []byte, loader cell.LazyCellLoader) (*cell.Cell, error)) ([]*cell.Cell, time.Duration) {
		cells := make([]*cell.Cell, n)
		for i := range cells {
			loaded, err := decode(hashes[i], encoded[i], loader)
			if err != nil {
				t.Fatal(err)
			}
			cells[i] = loaded
		}
		gcStarted := time.Now()
		runtime.GC()
		return cells, time.Since(gcStarted)
	}

	base := snapshot()
	cells, gcTook := build(DecodeLazyCellRecordTrusted)
	after := snapshot()

	perEntry := float64(after-base) / n
	t.Logf("live objects per entry: %.2f; forced GC with %d resident: %v", perEntry, n, gcTook)
	runtime.KeepAlive(cells)
	// Liveness, not scope: without these the encoded records and hashes die at
	// their last use inside the build, and the objects freed mid-measure drive
	// the delta NEGATIVE (first observed as exactly -1.00/entry).
	runtime.KeepAlive(encoded)
	runtime.KeepAlive(hashes)

	// Both bounds: an accounting artifact reads as an absurd number, and a
	// one-sided assertion would wave it through. A regression to per-object
	// construction reads ~8 and fails the upper bound.
	if perEntry < 0.5 || perEntry > 2.5 {
		t.Fatalf("decode kept %.2f live objects per entry, want ~1 (the slab); ~8 means the per-object construction is back", perEntry)
	}
}

func benchDecode(b *testing.B, refs int, decode func(hash, encoded []byte, loader cell.LazyCellLoader) (*cell.Cell, error)) {
	c := slabTestCell(b, 0xBEEF, refs)
	encoded := encodeSlabTestCell(b, c)
	hash := c.Hash()
	loader := func(h cell.Hash) (*cell.Cell, error) { return nil, fmt.Errorf("not resolved") }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decode(hash, encoded, loader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeLazyRecord2Refs(b *testing.B) { benchDecode(b, 2, DecodeLazyCellRecordTrusted) }
func BenchmarkDecodeLazyRecord4Refs(b *testing.B) { benchDecode(b, 4, DecodeLazyCellRecordTrusted) }
