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
		cache := newStateCellWindowCache(rejectingBenchmarkCellLoader)
		cache.addPreparedRecords(records)
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
		cache := newStateCellWindowCache(rejectingBenchmarkCellLoader)
		cache.addPreparedRecords(records)
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
		cache := newStateCellWindowCache(rejectingBenchmarkCellLoader)
		cache.addPreparedRecords(records)
		checkpoint := cache.beginCheckpoint()
		if checkpoint == nil {
			b.Fatal("checkpoint is nil")
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
}

func BenchmarkArchiveStateCellOverlayLoader(b *testing.B) {
	root := benchmarkStateCellTree(b, 4096)
	records, err := storage.PrepareReachableStateCells(root)
	if err != nil {
		b.Fatalf("prepare reachable cells: %v", err)
	}
	rootHash := root.HashKey()

	b.Run("active-root-hit", func(b *testing.B) {
		overlay := newArchiveStateCellOverlay(rejectingBenchmarkCellLoader)
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
		overlay := newArchiveStateCellOverlay(rejectingBenchmarkCellLoader)
		overlay.addPreparedRecords(records)
		checkpoint := overlay.beginCheckpoint()
		if checkpoint == nil {
			b.Fatal("checkpoint is nil")
		}
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
