package service

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkStateSerializerLargeBOCCellCountHint(b *testing.B) {
	const leaves = 8192

	store, err := pebblestore.Open(pebblestore.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	root, records := benchmarkStateSerializerTree(b, leaves)
	if err = saveActiveTestCells(store, records); err != nil {
		b.Fatalf("save cells: %v", err)
	}
	rootHash := root.HashKey()
	activeCells := activeTestCellGeneration(b, store)

	for _, hint := range []uint64{0, uint64(len(records))} {
		b.Run(fmt.Sprintf("hint=%d", hint), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				loader := newLargeBOCStateLoader(context.Background(), activeCells)
				if err := cell.ToLargeBOC(
					io.Discard,
					[]cell.Hash{rootHash},
					persistentStateBOCOptions(),
					loader,
					hint,
					defaultPersistentStateLargeBOCBatchSize,
				); err != nil {
					b.Fatalf("serialize large boc: %v", err)
				}
			}
			b.ReportMetric(float64(len(records)), "cells/op")
		})
	}
}

func benchmarkStateSerializerTree(tb testing.TB, leaves int) (*cell.Cell, []*storage.CellRecord) {
	tb.Helper()

	level := make([]*cell.Cell, leaves)
	for i := range level {
		level[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()
	}
	all := append([]*cell.Cell{}, level...)
	nonce := uint64(leaves)
	for len(level) > 1 {
		next := make([]*cell.Cell, 0, (len(level)+3)/4)
		for i := 0; i < len(level); i += 4 {
			builder := cell.BeginCell().MustStoreUInt(nonce, 32)
			nonce++
			end := min(i+4, len(level))
			for _, child := range level[i:end] {
				builder.MustStoreRef(child)
			}
			next = append(next, builder.EndCell())
		}
		all = append(all, next...)
		level = next
	}

	records := make([]*storage.CellRecord, len(all))
	for i, cl := range all {
		record, err := storage.CellRecordFromCell(cl)
		if err != nil {
			tb.Fatalf("cell record %d: %v", i, err)
		}
		records[i] = record
	}
	return level[0], records
}
