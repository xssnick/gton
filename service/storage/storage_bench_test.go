package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkLoadCellGraph(b *testing.B) {
	root, cells := benchmarkCellGraph(b, 4096, 4)
	records, err := CollectCellRecords(root)
	if err != nil {
		b.Fatalf("collect cell records: %v", err)
	}

	byHash := make(map[cell.Hash]*CellRecord, len(records))
	for _, record := range records {
		var hash cell.Hash
		copy(hash[:], record.Hash)
		byHash[hash] = record
	}

	rootHash := root.HashKey()
	loadRecord := func(hash []byte) (*CellRecord, error) {
		var key cell.Hash
		copy(key[:], hash)
		record := byHash[key]
		if record == nil {
			return nil, fmt.Errorf("missing record %x", hash)
		}
		return record, nil
	}

	b.ReportAllocs()
	b.ReportMetric(float64(cells), "cells/op")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		loaded, err := LoadCellGraph(context.Background(), rootHash[:], loadRecord)
		if err != nil {
			b.Fatalf("load cell graph: %v", err)
		}
		if loaded.HashKey() != rootHash {
			b.Fatalf("loaded root hash mismatch")
		}
	}
}

func BenchmarkCollectCellRecords(b *testing.B) {
	root, cells := benchmarkCellGraph(b, 4096, 4)

	b.ReportAllocs()
	b.ReportMetric(float64(cells), "cells/op")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		records, err := CollectCellRecords(root)
		if err != nil {
			b.Fatalf("collect cell records: %v", err)
		}
		if len(records) != cells {
			b.Fatalf("records count mismatch: got=%d want=%d", len(records), cells)
		}
	}
}

func BenchmarkPrepareStateUpdateCells(b *testing.B) {
	root, cells := benchmarkCellGraph(b, 4096, 4)
	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(root.Hash(0), 256).
		MustStoreSlice(root.Hash(0), 256).
		MustStoreUInt(uint64(root.Depth(0)), 16).
		MustStoreUInt(uint64(root.Depth(0)), 16).
		MustStoreRef(root).
		MustStoreRef(root).
		EndCellSpecial(true)
	if err != nil {
		b.Fatalf("create merkle update: %v", err)
	}

	b.ReportAllocs()
	b.ReportMetric(float64(cells), "cells/op")
	for b.Loop() {
		records, err := PrepareStateUpdateCells(update)
		if err != nil {
			b.Fatalf("prepare state update cells: %v", err)
		}
		if records.Len() != cells {
			b.Fatalf("records count mismatch: got=%d want=%d", records.Len(), cells)
		}
	}
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
