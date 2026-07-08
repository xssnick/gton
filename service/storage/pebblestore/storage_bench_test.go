package pebblestore

import (
	"bytes"
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkCellRecordCodec(b *testing.B) {
	root, _ := benchmarkCellGraph(b, 256, 4)
	record, err := storage.CellRecordFromCell(root)
	if err != nil {
		b.Fatalf("cell record from cell: %v", err)
	}
	encoded := encodeCellRecord(record)
	hash := bytes.Clone(record.Hash)

	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for i := 0; i < b.N; i++ {
			got := encodeCellRecord(record)
			if len(got) != len(encoded) {
				b.Fatalf("encoded length mismatch: got=%d want=%d", len(got), len(encoded))
			}
		}
	})

	b.Run("decode-clone", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for i := 0; i < b.N; i++ {
			if _, err := decodeCellRecord(hash, encoded); err != nil {
				b.Fatalf("decode cell record: %v", err)
			}
		}
	})

	b.Run("decode-trusted-lazy", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for i := 0; i < b.N; i++ {
			loaded, err := storage.LazyCellRecord(storage.DecodeCellRecordTrusted(hash, encoded), nil)
			if err != nil {
				b.Fatalf("lazy cell record: %v", err)
			}
			if loaded.HashKey() != root.HashKey() {
				b.Fatalf("lazy cell hash mismatch")
			}
		}
	})
}

func BenchmarkStateCellEncoder(b *testing.B) {
	root, _ := benchmarkCellGraph(b, 256, 4)
	refs := root.GetMetadata().Refs

	b.Run("cell-record", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			record, err := storage.CellRecordFromCell(root)
			if err != nil {
				b.Fatalf("cell record from cell: %v", err)
			}
			encoded := encodeCellRecord(record)
			if len(encoded) == 0 {
				b.Fatalf("empty encoded cell")
			}
		}
	})

	b.Run("direct-state", func(b *testing.B) {
		valueLen, layout, err := stateCellEncodedLen(root, refs)
		if err != nil {
			b.Fatalf("state cell encoded len: %v", err)
		}
		buf := make([]byte, valueLen)

		b.ReportAllocs()
		b.SetBytes(int64(valueLen))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			encodeStateCellRecordTo(buf, root, refs, layout)
		}
	})
}

func BenchmarkPebbleSaveCells(b *testing.B) {
	root, cells := benchmarkCellGraph(b, 1024, 4)
	records, err := collectCellRecordsForBenchmark(root)
	if err != nil {
		b.Fatalf("collect cell records: %v", err)
	}
	store := openBenchmarkStore(b, Options{})

	b.ReportAllocs()
	b.ReportMetric(float64(cells), "cells/op")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := store.SaveCells(records); err != nil {
			b.Fatalf("save cells: %v", err)
		}
	}
}

func BenchmarkPebbleImportStateCellTree(b *testing.B) {
	b.Run("boc-view", func(b *testing.B) {
		root, cells := benchmarkCellGraph(b, 1024, 4)
		boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{
			WithCRC32C:    true,
			WithIndex:     true,
			WithCacheBits: true,
			WithTopHash:   true,
			WithIntHashes: true,
		})
		view, err := cell.OpenBOCView(bytes.NewReader(boc), int64(len(boc)), cell.BOCViewOptions{
			RequireIndex: true,
			ValidateCRC:  true,
		})
		if err != nil {
			b.Fatalf("open benchmark boc view: %v", err)
		}
		store := openBenchmarkStore(b, Options{})
		block := benchmarkBlockID(1)

		b.ReportAllocs()
		b.ReportMetric(float64(cells), "cells/op")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.ImportStateBOCView(context.Background(), block, view); err != nil {
				b.Fatalf("import boc view state cells: %v", err)
			}
		}
	})

	b.Run("eager-root", func(b *testing.B) {
		root, cells := benchmarkCellGraph(b, 1024, 4)
		store := openBenchmarkStore(b, Options{})
		block := benchmarkBlockID(1)

		b.ReportAllocs()
		b.ReportMetric(float64(cells), "cells/op")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.ImportStateCellTree(context.Background(), block, root, uint64(cells)); err != nil {
				b.Fatalf("import eager-root state cell tree: %v", err)
			}
		}
	})
}

func BenchmarkPebbleSaveStateCellTree(b *testing.B) {
	b.Run("dfs", func(b *testing.B) {
		root, cells := benchmarkCellGraph(b, 1024, 4)
		store := openBenchmarkStore(b, Options{})
		block := benchmarkBlockID(1)

		b.ReportAllocs()
		b.ReportMetric(float64(cells), "cells/op")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.saveStateCellTree(context.Background(), stateCellTreeSave{
				block:      block,
				root:       root,
				totalCells: uint64(cells),
			}); err != nil {
				b.Fatalf("save dfs state cell tree: %v", err)
			}
		}
	})
}

func BenchmarkPebbleSaveStateCellTree1M(b *testing.B) {
	const (
		leaves = 750_000
		fanout = 4
	)

	b.Run("dfs-batch", func(b *testing.B) {
		root, cells := benchmarkCellGraph(b, leaves, fanout)
		store := openBenchmarkStore(b, Options{})
		block := benchmarkBlockID(1)

		b.ReportAllocs()
		b.ReportMetric(float64(cells), "cells/op")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := store.saveStateCellTree(context.Background(), stateCellTreeSave{
				block:      block,
				root:       root,
				totalCells: uint64(cells),
			}); err != nil {
				b.Fatalf("save dfs state cell tree: %v", err)
			}
		}
	})
}

func BenchmarkPebbleLoadCell(b *testing.B) {
	root, _ := benchmarkCellGraph(b, 4096, 4)
	records, err := collectCellRecordsForBenchmark(root)
	if err != nil {
		b.Fatalf("collect cell records: %v", err)
	}
	rootHash := root.HashKey()

	b.Run("lazy-root", func(b *testing.B) {
		store := openBenchmarkStore(b, Options{})
		if err := store.SaveCells(records); err != nil {
			b.Fatalf("save cells: %v", err)
		}

		b.ReportAllocs()
		b.ReportMetric(float64(1), "db-gets/op")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			loaded, err := store.LoadCell(context.Background(), rootHash[:])
			if err != nil {
				b.Fatalf("load lazy root: %v", err)
			}
			if loaded.IsLazy() || loaded.HashKey() != rootHash {
				b.Fatalf("loaded root metadata mismatch")
			}
		}
	})

	b.Run("lazy-first-ref-chain", func(b *testing.B) {
		store := openBenchmarkStore(b, Options{})
		if err := store.SaveCells(records); err != nil {
			b.Fatalf("save cells: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			loaded, err := store.LoadCell(context.Background(), rootHash[:])
			if err != nil {
				b.Fatalf("load lazy root: %v", err)
			}
			benchmarkTouchFirstRefChain(b, loaded)
		}
	})

}

func openBenchmarkStore(b *testing.B, opts Options) *Store {
	b.Helper()

	opts.Dir = filepath.Join(b.TempDir(), "storage")
	store, err := Open(opts)
	if err != nil {
		b.Fatalf("open pebble storage: %v", err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Fatalf("close pebble storage: %v", err)
		}
	})
	return store
}

func benchmarkCellGraph(tb testing.TB, leaves int, fanout int) (*cell.Cell, int) {
	tb.Helper()
	if leaves <= 0 {
		tb.Fatalf("leaves must be positive")
	}
	if fanout < 2 || fanout > 4 {
		tb.Fatalf("fanout must be between 2 and 4")
	}

	level := make([]*cell.Cell, leaves)
	total := 0
	for i := range level {
		level[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()
		total++
	}

	for len(level) > 1 {
		next := make([]*cell.Cell, 0, (len(level)+fanout-1)/fanout)
		for i := 0; i < len(level); i += fanout {
			builder := cell.BeginCell().MustStoreUInt(uint64(total), 32)
			limit := i + fanout
			if limit > len(level) {
				limit = len(level)
			}
			for j := i; j < limit; j++ {
				builder.MustStoreRef(level[j])
			}
			next = append(next, builder.EndCell())
			total++
		}
		level = next
	}

	return level[0], total
}

func collectCellRecordsForBenchmark(root *cell.Cell) ([]*storage.CellRecord, error) {
	if root == nil {
		return nil, nil
	}

	seen := map[cell.Hash]struct{}{}
	records := make([]*storage.CellRecord, 0, 1024)
	stack := []*cell.Cell{root}

	for len(stack) > 0 {
		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]

		hash := current.HashKey()
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}

		record, err := storage.CellRecordFromCell(current)
		if err != nil {
			return nil, err
		}
		records = append(records, record)

		for i := uint(0); i < current.RefsNum(); i++ {
			ref, err := current.PeekRef(int(i))
			if err != nil {
				return nil, err
			}
			stack = append(stack, ref)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Hash, records[j].Hash) < 0
	})
	return records, nil
}

func benchmarkTouchFirstRefChain(tb testing.TB, root *cell.Cell) {
	tb.Helper()

	for current := root; current.RefsNum() > 0; {
		next, err := current.PeekRef(0)
		if err != nil {
			tb.Fatalf("peek ref: %v", err)
		}
		_ = next.HashKey()
		current = next
	}
}

func benchmarkBlockID(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
	}
}
