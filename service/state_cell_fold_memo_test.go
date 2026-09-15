package service

import (
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestFoldLayersCarriesDecodedMemo pins that folding a staged layer, in bounded
// slices, moves the cells it already decoded into the base map: reads after a
// master commit return the memoized cells instead of decoding every record
// again.
func TestFoldLayersCarriesDecodedMemo(t *testing.T) {
	records := mustPreparedReachableStateCells(t, benchmarkStateCellTree(t, 8))
	flat := records.AppendTo(nil)

	cache := newStateCellEncodedCache(1)
	cache.stageRecords(records, nil)
	staged := make([]*cell.Cell, len(flat))
	for i := range flat {
		loaded, err := cache.loadWith(flat[i].Hash, nil)
		if err != nil {
			t.Fatalf("load staged record %d: %v", i, err)
		}
		staged[i] = loaded
	}

	for {
		if _, owed := cache.foldLayers(1, 3); !owed {
			break
		}
	}
	if got := cache.stagedLayers(); got != 0 {
		t.Fatalf("staged layers after fold = %d, want 0", got)
	}
	for i := range flat {
		loaded, err := cache.loadWith(flat[i].Hash, nil)
		if err != nil {
			t.Fatalf("load folded record %d: %v", i, err)
		}
		if loaded != staged[i] {
			t.Fatalf("folded record %d decoded again instead of keeping the layer's cell", i)
		}
	}
}

// TestFoldLayersKeepsResidentBaseMemo pins that an identical re-staged record
// folds without displacing the cell the base map already memoized.
func TestFoldLayersKeepsResidentBaseMemo(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x63, 8).EndCell()
	hash := root.HashKey()

	cache := newStateCellEncodedCache(1)
	cache.stageRecords(mustPreparedReachableStateCells(t, root), nil)
	cache.foldLayers(1, 1<<20)
	base, err := cache.loadWith(hash, nil)
	if err != nil {
		t.Fatalf("load base record: %v", err)
	}

	cache.stageRecords(mustPreparedReachableStateCells(t, root), nil)
	staged, err := cache.loadWith(hash, nil)
	if err != nil {
		t.Fatalf("load re-staged record: %v", err)
	}
	if staged == base {
		t.Fatal("re-staged layer answered from the base map, want its own decode")
	}

	cache.foldLayers(1, 1<<20)
	if loaded, err := cache.loadWith(hash, nil); err != nil || loaded != base {
		t.Fatalf("load after identical fold = (%p, %v), want resident base cell %p", loaded, err, base)
	}
}

// TestFoldLayersReplacedRecordTakesLayerMemo pins that a fold replacing the base
// record data carries the layer's decode of the new bytes, never the base cell
// decoded from the old ones.
func TestFoldLayersReplacedRecordTakesLayerMemo(t *testing.T) {
	older := cell.BeginCell().MustStoreUInt(0x64, 8).EndCell()
	newer := cell.BeginCell().MustStoreUInt(0x65, 8).EndCell()
	hash := older.HashKey()
	olderData := mustPreparedReachableStateCells(t, older).Data(hash)
	newerData := mustPreparedReachableStateCells(t, newer).Data(newer.HashKey())

	cache := newStateCellEncodedCache(1)
	cache.stageRecords(storage.NewStateCellRecords([]storage.EncodedCellRecord{{Hash: hash, Data: olderData}}), nil)
	cache.foldLayers(1, 1<<20)
	base, err := cache.loadWith(hash, nil)
	if err != nil {
		t.Fatalf("load base record: %v", err)
	}

	cache.stageRecords(storage.NewStateCellRecords([]storage.EncodedCellRecord{{Hash: hash, Data: newerData}}), nil)
	staged, err := cache.loadWith(hash, nil)
	if err != nil {
		t.Fatalf("load replacing record: %v", err)
	}

	cache.foldLayers(1, 1<<20)
	loaded, err := cache.loadWith(hash, nil)
	if err != nil {
		t.Fatalf("load replaced record: %v", err)
	}
	if loaded == base {
		t.Fatal("replaced record still answers the cell decoded from the old bytes")
	}
	if loaded != staged {
		t.Fatal("replaced record decoded again instead of keeping the layer's cell")
	}
}

// BenchmarkStateCellFoldReload is one master commit as the window sees it: a
// staged block read in full, folded, and read in full again.
func BenchmarkStateCellFoldReload(b *testing.B) {
	records := mustPreparedReachableStateCells(b, benchmarkStateCellTree(b, 256))
	flat := records.AppendTo(nil)

	b.ReportAllocs()
	for b.Loop() {
		cache := newStateCellEncodedCache(len(flat))
		cache.stageRecords(records, nil)
		for i := range flat {
			if _, err := cache.loadWith(flat[i].Hash, rejectingBenchmarkCellLoader); err != nil {
				b.Fatalf("load staged record: %v", err)
			}
		}
		cache.foldLayers(1, len(flat))
		for i := range flat {
			if _, err := cache.loadWith(flat[i].Hash, rejectingBenchmarkCellLoader); err != nil {
				b.Fatalf("load folded record: %v", err)
			}
		}
	}
}
