package pebblestore

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDBStatusIncludesMetaDB(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	status, err := store.DBStatus(context.Background())
	if err != nil {
		t.Fatalf("db status: %v", err)
	}
	if status.Meta == nil {
		t.Fatalf("meta db status is nil")
	}
	if len(status.CellGenerations) != 1 {
		t.Fatalf("cell generations = %d, want 1", len(status.CellGenerations))
	}
}

func TestDBStatusIncludesCellIOCounters(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	root := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	rootHash := root.HashKey()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}
	if _, err = store.ImportStateCellTree(ctx, block, root, 1); err != nil {
		t.Fatalf("import state cell tree: %v", err)
	}
	if _, err = store.CellRecord(ctx, rootHash[:]); err != nil {
		t.Fatalf("load cell record: %v", err)
	}

	status, err := store.DBStatus(ctx)
	if err != nil {
		t.Fatalf("db status: %v", err)
	}
	generation := status.CellGenerations[0]
	shardIdx := int(rootHash[0] >> 5)
	var shardStatus CellDBShardStatus
	found := false
	for _, shard := range generation.Shards {
		if shard.Shard == shardIdx {
			shardStatus = shard
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("shard %d status is missing", shardIdx)
	}
	if shardStatus.WrittenCells == 0 {
		t.Fatalf("shard %d written cells = 0, want writes", shardIdx)
	}
	if shardStatus.ReadCells == 0 {
		t.Fatalf("shard %d read cells = 0, want reads", shardIdx)
	}
	if generation.Total.WrittenCells < shardStatus.WrittenCells {
		t.Fatalf("total written cells = %d, shard written cells = %d", generation.Total.WrittenCells, shardStatus.WrittenCells)
	}
	if generation.Total.ReadCells < shardStatus.ReadCells {
		t.Fatalf("total read cells = %d, shard read cells = %d", generation.Total.ReadCells, shardStatus.ReadCells)
	}
}
