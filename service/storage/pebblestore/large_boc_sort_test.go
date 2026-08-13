package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestLargeBOCLoadCellsPreservesInputOrderAfterShardSort(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	records := make([]*storage.CellRecord, 0, 256)
	hashes := make([]cell.Hash, 0, 256)
	byHash := make(map[cell.Hash]*storage.CellRecord, 256)
	for nonce := uint64(0); nonce < 256; nonce++ {
		cl := cell.BeginCell().MustStoreUInt(nonce, 64).EndCell()
		record, err := storage.CellRecordFromCell(cl)
		if err != nil {
			t.Fatalf("cell record: %v", err)
		}
		records = append(records, record)
		hash := cl.HashKey()
		hashes = append(hashes, hash)
		byHash[hash] = record
	}
	if err = saveActiveTestCells(store, records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	for i, j := 0, len(hashes)-1; i < j; i, j = i+1, j-1 {
		hashes[i], hashes[j] = hashes[j], hashes[i]
	}
	hashes = append(hashes, hashes[len(hashes)/2])

	got, err := loadActiveTestLargeBOCCells(store, context.Background(), hashes, nil)
	if err != nil {
		t.Fatalf("load cells: %v", err)
	}
	if len(got) != len(hashes) {
		t.Fatalf("load cells returned %d records, want %d", len(got), len(hashes))
	}
	for i, hash := range hashes {
		record := byHash[hash]
		wantMeta, err := storage.LargeBOCMetaRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("build expected large boc meta %d: %v", i, err)
		}
		wantPayload, err := storage.LargeBOCPayloadRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("build expected large boc payload %d: %v", i, err)
		}
		if got[i].Meta != wantMeta {
			t.Fatalf("meta[%d] mismatch", i)
		}
		if !bytes.Equal(got[i].Payload.Data, wantPayload.Data) {
			t.Fatalf("payload[%d] mismatch", i)
		}
	}
}

func TestLargeBOCLoadCellsReturnsNotFoundWithoutPartialAppend(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	cl := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	record, err := storage.CellRecordFromCell(cl)
	if err != nil {
		t.Fatalf("cell record: %v", err)
	}
	if err = saveActiveTestCells(store, []*storage.CellRecord{record}); err != nil {
		t.Fatalf("save cell: %v", err)
	}

	missing := cell.BeginCell().MustStoreUInt(3, 8).EndCell().HashKey()
	prefix := []cell.LargeBOCRecord{{Meta: cell.LargeBOCMetaRecord{D1: 0x7f}}}
	got, err := loadActiveTestLargeBOCCells(store, context.Background(), []cell.Hash{cl.HashKey(), missing}, prefix)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load error = %v, want ErrNotFound", err)
	}
	if len(got) != len(prefix) || got[0].Meta.D1 != prefix[0].Meta.D1 {
		t.Fatalf("partial records were appended on error: %#v", got)
	}
}

func TestLargeBOCLoadCellsUsesRequestedGeneration(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	generation, err := store.BeginCellGeneration(ctx, testMasterBlockID(100, 0x31))
	if err != nil {
		t.Fatalf("begin cell generation: %v", err)
	}
	generationCells, err := store.Cells(generation)
	if err != nil {
		t.Fatalf("select cell generation: %v", err)
	}
	cl := cell.BeginCell().MustStoreUInt(2, 8).EndCell()
	record, err := storage.CellRecordFromCell(cl)
	if err != nil {
		t.Fatalf("cell record: %v", err)
	}
	if err = generationCells.Save(ctx, []*storage.CellRecord{record}); err != nil {
		t.Fatalf("save generation cell: %v", err)
	}

	if _, err = loadActiveTestLargeBOCCells(store, ctx, []cell.Hash{cl.HashKey()}, nil); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("active generation load error = %v, want ErrNotFound", err)
	}
	got, err := generationCells.LoadLargeBOCCells(ctx, []cell.Hash{cl.HashKey()}, nil)
	if err != nil {
		t.Fatalf("load requested generation: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("loaded records = %d, want 1", len(got))
	}
}

func BenchmarkLargeBOCLoadCells(b *testing.B) {
	const cells = 32768

	store, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	records := make([]*storage.CellRecord, cells)
	hashes := make([]cell.Hash, cells)
	for i := range cells {
		cl := cell.BeginCell().MustStoreUInt(uint64(i), 64).EndCell()
		record, err := storage.CellRecordFromCell(cl)
		if err != nil {
			b.Fatalf("cell record %d: %v", i, err)
		}
		records[i] = record
		hashes[cells-1-i] = cl.HashKey()
	}
	if err = saveActiveTestCells(store, records); err != nil {
		b.Fatalf("save cells: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		loaded, err := loadActiveTestLargeBOCCells(store, context.Background(), hashes, nil)
		if err != nil {
			b.Fatalf("load cells: %v", err)
		}
		if len(loaded) != cells {
			b.Fatalf("loaded cells = %d, want %d", len(loaded), cells)
		}
	}
	b.ReportMetric(cells, "cells/op")
}
