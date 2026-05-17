package state

import (
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestLoadWorkchainPersistentStateSplitDepth(t *testing.T) {
	tests := []struct {
		name  string
		cell  *cell.Cell
		depth uint32
	}{
		{
			name:  "v1 descriptor has no persistent split depth",
			cell:  workchainDescriptorCell(0xa6, true, 0),
			depth: 0,
		},
		{
			name:  "v2 basic descriptor reads persistent split depth",
			cell:  workchainDescriptorCell(0xa7, true, 7),
			depth: 7,
		},
		{
			name:  "v2 extended descriptor reads persistent split depth",
			cell:  workchainDescriptorCell(0xa7, false, 11),
			depth: 11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			depth, err := loadWorkchainPersistentStateSplitDepth(tt.cell.MustBeginParse())
			if err != nil {
				t.Fatal(err)
			}
			if depth != tt.depth {
				t.Fatalf("depth mismatch: got %d want %d", depth, tt.depth)
			}
		})
	}
}

func BenchmarkLoadWorkchainPersistentStateSplitDepth(b *testing.B) {
	c := workchainDescriptorCell(0xa7, true, 7)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		depth, err := loadWorkchainPersistentStateSplitDepth(c.MustBeginParse())
		if err != nil {
			b.Fatal(err)
		}
		if depth != 7 {
			b.Fatalf("depth mismatch: got %d want 7", depth)
		}
	}
}

func workchainDescriptorCell(tag uint64, basic bool, depth uint64) *cell.Cell {
	b := cell.BeginCell().
		MustStoreUInt(tag, 8).
		MustStoreUInt(1, 32).
		MustStoreUInt(0, 8).
		MustStoreUInt(0, 8).
		MustStoreUInt(60, 8).
		MustStoreBoolBit(basic).
		MustStoreBoolBit(true).
		MustStoreBoolBit(true).
		MustStoreUInt(0, 13).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreUInt(1, 32)

	if basic {
		b.MustStoreUInt(1, 4).
			MustStoreInt(0, 32).
			MustStoreUInt(0, 64)
	} else {
		b.MustStoreUInt(0, 4).
			MustStoreUInt(64, 12).
			MustStoreUInt(256, 12).
			MustStoreUInt(32, 12).
			MustStoreUInt(1, 32)
	}

	if tag == 0xa7 {
		b.MustStoreUInt(0, 4).
			MustStoreUInt(0, 32).
			MustStoreUInt(0, 32).
			MustStoreUInt(0, 32).
			MustStoreUInt(0, 32).
			MustStoreUInt(depth, 8)
	}

	return b.EndCell()
}
