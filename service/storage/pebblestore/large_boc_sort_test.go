package pebblestore

import (
	"bytes"
	"context"
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
	if err = store.SaveCells(records); err != nil {
		t.Fatalf("save cells: %v", err)
	}

	for i, j := 0, len(hashes)-1; i < j; i, j = i+1, j-1 {
		hashes[i], hashes[j] = hashes[j], hashes[i]
	}

	got, err := store.LargeBOCLoadCells(context.Background(), hashes, nil)
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
