package service

import (
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stateUpdateFlowFixture struct {
	previous []*storage.BlockState
	next     *cell.Cell
	update   *cell.Cell
	base     cell.LazyCellLoader
	records  int
	bytes    uint64
}

type stateUpdateFlowCase struct {
	name       string
	leaves     int
	allChanged bool
}

// BenchmarkStateUpdatePrepareApplyFold measures the joined preparation/apply/
// commit-fold path, including a fresh window and the root reload performed by
// applyPreparedMerkleUpdate. The resident previous-state loader is shared;
// fixture construction, proof validation, network, and background/durable IO
// are excluded. This is a state-cell phase benchmark, not a full block run.
func BenchmarkStateUpdatePrepareApplyFold(b *testing.B) {
	cases := []stateUpdateFlowCase{
		{name: "quarter-changed/leaves=4096", leaves: 4096},
		{name: "quarter-changed/leaves=16384", leaves: 16384},
		{name: "all-new-level-zero/leaves=1024", leaves: 1024, allChanged: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			fixture := newStateUpdateFlowFixture(b, tc.leaves, tc.allChanged)
			b.ReportAllocs()
			b.ReportMetric(float64(fixture.records), "records/op")
			b.ReportMetric(float64(fixture.bytes), "encoded-B/op")
			for b.Loop() {
				window := newTestStateCellWindowCache(fixture.base)
				prepared, err := storage.PrepareStateUpdateCells(fixture.update)
				if err != nil {
					b.Fatal(err)
				}
				applied, err := window.applyPreparedMerkleUpdate(fixture.previous, fixture.update, prepared)
				if err != nil {
					b.Fatal(err)
				}
				window.compactStagedLayers(0)
				if applied.PreviousRoot.HashKey() != fixture.previous[0].Cell.HashKey() ||
					applied.NextRoot.HashKey() != fixture.next.HashKey() ||
					window.active.len() != fixture.records {
					b.Fatal("applied roots or folded record count changed")
				}
			}
		})
	}
}

func TestStateUpdatePrepareApplyFoldFixture(t *testing.T) {
	cases := []stateUpdateFlowCase{
		{name: "quarter-changed", leaves: 256},
		{name: "all-new-level-zero", leaves: 256, allChanged: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newStateUpdateFlowFixture(t, tc.leaves, tc.allChanged)
			window := newTestStateCellWindowCache(fixture.base)
			prepared, err := storage.PrepareStateUpdateCells(fixture.update)
			if err != nil {
				t.Fatal(err)
			}
			applied, err := window.applyPreparedMerkleUpdate(fixture.previous, fixture.update, prepared)
			if err != nil {
				t.Fatal(err)
			}
			assertResolvedCellTreeEqual(t, applied.NextRoot, fixture.next)
			window.compactStagedLayers(0)
			if window.activeStagedLayers() != 0 || window.active.len() != fixture.records {
				t.Fatal("fold did not absorb exactly the prepared records")
			}
			loaded, err := window.loader()(fixture.next.HashKey())
			if err != nil {
				t.Fatal(err)
			}
			assertResolvedCellTreeEqual(t, loaded, fixture.next)
		})
	}
}

func newStateUpdateFlowFixture(tb testing.TB, leaves int, allChanged bool) stateUpdateFlowFixture {
	tb.Helper()

	before := make([]*cell.Cell, leaves)
	after := make([]*cell.Cell, leaves)
	fromProof := make([]*cell.Cell, leaves)
	toProof := make([]*cell.Cell, leaves)
	baseCells := make(map[cell.Hash]*cell.Cell, leaves*2)
	var payload [64]byte
	for i := range payload {
		payload[i] = byte(i)
	}
	for i := range before {
		before[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).
			MustStoreUInt(0, 8).MustStoreSlice(payload[:], 512).EndCell()
		baseCells[before[i].HashKey()] = before[i]
		if allChanged || i%4 == 0 {
			after[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).
				MustStoreUInt(1, 8).MustStoreSlice(payload[:], 512).EndCell()
			fromProof[i], toProof[i] = before[i], after[i]
		} else {
			after[i] = before[i]
			pruned := mustPrunedBranch(tb, before[i])
			fromProof[i], toProof[i] = pruned, pruned
		}
	}

	previousRoot := buildStateUpdateFlowTree(before, baseCells)
	nextRoot := buildStateUpdateFlowTree(after, nil)
	update, err := cell.CreateMerkleUpdate(
		buildStateUpdateFlowTree(fromProof, nil), buildStateUpdateFlowTree(toProof, nil))
	if err != nil {
		tb.Fatal(err)
	}
	if err = cell.ValidateMerkleUpdate(update); err != nil {
		tb.Fatalf("fixture Merkle update is invalid: %v", err)
	}
	canonical, err := cell.ApplyMerkleUpdate(previousRoot, update)
	if err != nil {
		tb.Fatalf("fixture cannot apply: %v", err)
	}
	assertResolvedCellTreeEqual(tb, canonical, nextRoot)
	prepared, err := storage.PrepareStateUpdateCells(update)
	if err != nil {
		tb.Fatal(err)
	}
	for i, previousLeaf := range before {
		if allChanged || i%4 == 0 {
			if !prepared.Has(after[i].HashKey()) {
				tb.Fatal("fixture omitted a changed destination leaf")
			}
		} else if prepared.Has(previousLeaf.HashKey()) {
			tb.Fatal("fixture did not prune an unchanged destination leaf")
		}
	}
	return stateUpdateFlowFixture{
		previous: []*storage.BlockState{{Cell: previousRoot}},
		next:     nextRoot,
		update:   update,
		base: func(hash cell.Hash) (*cell.Cell, error) {
			if found := baseCells[hash]; found != nil {
				return found, nil
			}
			return nil, fmt.Errorf("unexpected previous-state load %x", hash)
		},
		records: prepared.Len(),
		bytes:   prepared.ByteSize(),
	}
}

func buildStateUpdateFlowTree(nodes []*cell.Cell, byHash map[cell.Hash]*cell.Cell) *cell.Cell {
	for len(nodes) > 1 {
		next := make([]*cell.Cell, 0, (len(nodes)+3)/4)
		for i := 0; i < len(nodes); i += 4 {
			builder := cell.BeginCell()
			for j := i; j < min(i+4, len(nodes)); j++ {
				builder.MustStoreRef(nodes[j])
			}
			root := builder.EndCell()
			next = append(next, root)
			if byHash != nil {
				byHash[root.HashKey()] = root
			}
		}
		nodes = next
	}
	return nodes[0]
}
