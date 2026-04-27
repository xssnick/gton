package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestLoadCellGraphBuildsScheduledSharedRefs(t *testing.T) {
	shared := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	left := cell.BeginCell().MustStoreUInt(0xBB, 8).MustStoreRef(shared).EndCell()
	root := cell.BeginCell().MustStoreRef(shared).MustStoreRef(left).EndCell()

	records, err := CollectCellRecords(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	byHash := make(map[cell.Hash]*CellRecord, len(records))
	for _, record := range records {
		var hash cell.Hash
		copy(hash[:], record.Hash)
		byHash[hash] = record
	}

	rootHash := root.HashKey()
	loaded, err := LoadCellGraph(context.Background(), rootHash[:], func(hash []byte) (*CellRecord, error) {
		var key cell.Hash
		copy(key[:], hash)
		record := byHash[key]
		if record == nil {
			t.Fatalf("unexpected missing record %x", hash)
		}
		return record, nil
	})
	if err != nil {
		t.Fatalf("load cell graph: %v", err)
	}
	if !bytes.Equal(loaded.ToBOC(), root.ToBOC()) {
		t.Fatalf("loaded cell mismatch")
	}
}
