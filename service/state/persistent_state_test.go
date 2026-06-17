package state

import (
	"errors"
	"math/big"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
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

func TestPersistentStateSplitDepthsReusesMasterConfig(t *testing.T) {
	master := masterStateWithPersistentSplitDepths(t, map[int32]uint64{
		0: 7,
		1: 11,
	})
	blocks := []ton.BlockIDExt{
		{Workchain: 0},
		{Workchain: 0},
		{Workchain: 1},
		{Workchain: -1},
	}

	depths, err := PersistentStateSplitDepths(master, blocks)
	if err != nil {
		t.Fatal(err)
	}

	for workchain, want := range map[int32]uint32{
		-1: 0,
		0:  7,
		1:  11,
	} {
		if got := depths[workchain]; got != want {
			t.Fatalf("workchain %d depth = %d, want %d", workchain, got, want)
		}
	}

	depth, err := PersistentStateSplitDepth(master, 1)
	if err != nil {
		t.Fatal(err)
	}
	if depth != 11 {
		t.Fatalf("single workchain depth = %d, want 11", depth)
	}
}

func TestPersistentStateSplitDepthsMasterchainOnlyDoesNotNeedConfig(t *testing.T) {
	depths, err := PersistentStateSplitDepths(nil, []ton.BlockIDExt{{Workchain: -1}})
	if err != nil {
		t.Fatal(err)
	}
	if got := depths[-1]; got != 0 {
		t.Fatalf("masterchain depth = %d, want 0", got)
	}
}

func TestPersistentStateSplitDepthsReturnsNotFoundForMissingConfig(t *testing.T) {
	tests := []struct {
		name   string
		master *storage.BlockState
	}{
		{
			name: "missing master state",
		},
		{
			name:   "missing parsed config",
			master: &storage.BlockState{},
		},
		{
			name:   "missing workchain descriptor",
			master: masterStateWithPersistentSplitDepths(t, map[int32]uint64{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := PersistentStateSplitDepth(tt.master, 0); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("split depth error = %v, want ErrNotFound", err)
			}
		})
	}
}

func masterStateWithPersistentSplitDepths(t *testing.T, depths map[int32]uint64) *storage.BlockState {
	t.Helper()

	workchains := cell.NewDict(32)
	for workchain, depth := range depths {
		if err := workchains.SetIntKey(big.NewInt(int64(workchain)), workchainDescriptorCell(0xa7, true, depth)); err != nil {
			t.Fatalf("store workchain descriptor %d: %v", workchain, err)
		}
	}
	workchainsCell, err := tlb.ToCell(&tlb.WorkchainsConfig{Workchains: workchains})
	if err != nil {
		t.Fatalf("build workchains config: %v", err)
	}

	configParams := cell.NewDict(32)
	if err := configParams.SetIntKey(big.NewInt(int64(tlb.ConfigParamWorkchains)), cell.BeginCell().MustStoreRef(workchainsCell).EndCell()); err != nil {
		t.Fatalf("store workchains config param: %v", err)
	}

	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(cell.NewDict(32)).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(configParams.AsCell()).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()

	return &storage.BlockState{
		Parsed: &tlb.ShardStateUnsplit{
			McStateExtra: extra,
		},
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
