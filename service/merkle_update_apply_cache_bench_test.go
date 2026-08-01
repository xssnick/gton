package service

import (
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkStateCellWindowCacheLoader(b *testing.B) {
	root := benchmarkStateCellTree(b, 4096)
	records, err := storage.PrepareReachableStateCells(root)
	if err != nil {
		b.Fatalf("prepare reachable cells: %v", err)
	}
	rootHash := root.HashKey()

	b.Run("active-root-hit", func(b *testing.B) {
		cache := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		if err := cache.addPreparedRecords(records); err != nil {
			b.Fatalf("add prepared records: %v", err)
		}
		load := cache.loader()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			if loaded.HashKey() != rootHash {
				b.Fatalf("loaded root hash mismatch")
			}
		}
	})

	b.Run("active-first-ref-chain", func(b *testing.B) {
		cache := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		if err := cache.addPreparedRecords(records); err != nil {
			b.Fatalf("add prepared records: %v", err)
		}
		load := cache.loader()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			benchmarkTouchFirstRefChain(b, loaded)
		}
	})

	b.Run("pending-root-hit", func(b *testing.B) {
		cache := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		if err := cache.addPreparedRecords(records); err != nil {
			b.Fatalf("add prepared records: %v", err)
		}
		cache.beginCheckpoint()
		load := cache.loader()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			if loaded.HashKey() != rootHash {
				b.Fatalf("loaded root hash mismatch")
			}
		}
	})

	b.Run("staged-layer-root-hit", func(b *testing.B) {
		cache := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		if _, err := cache.stagePreparedRecords(records); err != nil {
			b.Fatalf("stage prepared records: %v", err)
		}
		load := cache.loader()

		b.ReportAllocs()
		for b.Loop() {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			if loaded.HashKey() != rootHash {
				b.Fatalf("loaded root hash mismatch")
			}
		}
	})

	// The worst case the applier-side valve allows: the wanted cell sits in the
	// oldest of a full set of staged layers, so every layer is probed first.
	b.Run("oldest-of-max-staged-layers", func(b *testing.B) {
		cache := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		if _, err := cache.stagePreparedRecords(records); err != nil {
			b.Fatalf("stage prepared records: %v", err)
		}
		for i := 0; i < stateCellWindowMaxStagedLayers-1; i++ {
			other := cell.BeginCell().MustStoreUInt(uint64(i+1), 32).EndCell()
			if _, err := cache.stagePreparedRecords(mustPreparedReachableStateCells(b, other)); err != nil {
				b.Fatalf("stage prepared records: %v", err)
			}
		}
		load := cache.loader()

		b.ReportAllocs()
		for b.Loop() {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			if loaded.HashKey() != rootHash {
				b.Fatalf("loaded root hash mismatch")
			}
		}
	})

	b.Run("newest-of-32-pending", func(b *testing.B) {
		cache := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		cache.active = newStateCellEncodedCache(1)
		cache.pending = make([]*stateCellEncodedCache, 32)
		for i := 0; i < len(cache.pending)-1; i++ {
			other := cell.BeginCell().MustStoreUInt(uint64(i+1), 32).EndCell()
			pending := newStateCellEncodedCache(1)
			if err := pending.stageRecords(mustPreparedReachableStateCells(b, other), nil).enqueue(); err != nil {
				b.Fatalf("add pending records: %v", err)
			}
			cache.pending[i] = pending
		}
		newest := newStateCellEncodedCache(records.Len())
		if err := newest.stageRecords(records, nil).enqueue(); err != nil {
			b.Fatalf("add newest pending records: %v", err)
		}
		cache.pending[len(cache.pending)-1] = newest
		load := cache.loader()

		b.ReportAllocs()
		for b.Loop() {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			if loaded.HashKey() != rootHash {
				b.Fatalf("loaded root hash mismatch")
			}
		}
	})
}

// BenchmarkStateCellCacheStageRecords is the applier-side staging cost: one
// prepared block's records published into a shard window cache. This is the
// operation that used to hold the exclusive lock for the whole record walk.
func BenchmarkStateCellCacheStageRecords(b *testing.B) {
	root := benchmarkStateCellTree(b, 10240)
	records, err := storage.PrepareReachableStateCells(root)
	if err != nil {
		b.Fatalf("prepare reachable cells: %v", err)
	}
	b.Logf("records per stage: %d", records.Len())

	// fresh-window pays the window allocation too, which is what the original
	// per-record staging benchmark measured; warm-window isolates the
	// publication itself, which is the part that runs under the exclusive lock
	// the loaders share.
	b.Run("fresh-window", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cache := newStateCellEncodedCache(4096)
			if err := cache.stageRecords(records, nil).enqueue(); err != nil {
				b.Fatalf("stage records: %v", err)
			}
		}
	})

	b.Run("warm-window", func(b *testing.B) {
		cache := newStateCellEncodedCache(4096)

		b.ReportAllocs()
		for b.Loop() {
			if err := cache.stageRecords(records, nil).enqueue(); err != nil {
				b.Fatalf("stage records: %v", err)
			}
		}
	})

	// The exclusive hold itself, which is what the loaders and the other shard
	// appliers wait behind: everything else above runs before the lock.
	b.Run("publish-prebuilt-layer", func(b *testing.B) {
		cache := newStateCellEncodedCache(4096)
		layer := newStateCellRecordLayer(records)

		b.ReportAllocs()
		for b.Loop() {
			cache.stageLayer(layer, nil)
		}
	})
}

// BenchmarkStateCellWindowAddPreparedRecords is the end-to-end commit-side
// cost: staging plus whatever indexing/compaction the window performs before
// the records are considered absorbed.
func BenchmarkStateCellWindowAddPreparedRecords(b *testing.B) {
	root := benchmarkStateCellTree(b, 10240)
	records, err := storage.PrepareReachableStateCells(root)
	if err != nil {
		b.Fatalf("prepare reachable cells: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		window := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		if err := window.addPreparedRecords(records); err != nil {
			b.Fatalf("add prepared records: %v", err)
		}
	}
}

func BenchmarkArchiveStateCellOverlayLoader(b *testing.B) {
	root := benchmarkStateCellTree(b, 4096)
	records, err := storage.PrepareReachableStateCells(root)
	if err != nil {
		b.Fatalf("prepare reachable cells: %v", err)
	}
	rootHash := root.HashKey()

	b.Run("active-root-hit", func(b *testing.B) {
		overlay := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		overlay.addPreparedRecords(records)
		load := overlay.loader()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			if loaded.HashKey() != rootHash {
				b.Fatalf("loaded root hash mismatch")
			}
		}
	})

	b.Run("pending-root-hit", func(b *testing.B) {
		overlay := newTestStateCellWindowCache(rejectingBenchmarkCellLoader)
		overlay.addPreparedRecords(records)
		overlay.beginCheckpoint()
		load := overlay.loader()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loaded, err := load(rootHash)
			if err != nil {
				b.Fatalf("load root: %v", err)
			}
			if loaded.HashKey() != rootHash {
				b.Fatalf("loaded root hash mismatch")
			}
		}
	})
}

func benchmarkStateCellTree(tb testing.TB, leaves int) *cell.Cell {
	tb.Helper()

	if leaves < 1 {
		leaves = 1
	}
	level := make([]*cell.Cell, leaves)
	for i := range level {
		level[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()
	}

	for len(level) > 1 {
		next := make([]*cell.Cell, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}
			next = append(next, cell.BeginCell().MustStoreRef(level[i]).MustStoreRef(level[i+1]).EndCell())
		}
		level = next
	}
	return level[0]
}

func benchmarkTouchFirstRefChain(tb testing.TB, root *cell.Cell) {
	tb.Helper()

	current := root
	for current.RefsNum() > 0 {
		next, err := current.PeekRef(0)
		if err != nil {
			tb.Fatalf("load first ref chain: %v", err)
		}
		current = next
	}
}

func rejectingBenchmarkCellLoader(hash cell.Hash) (*cell.Cell, error) {
	return nil, fmt.Errorf("unexpected base cell load %x", hash[:])
}
