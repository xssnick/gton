package memstore

import (
	"context"
	"errors"
	"testing"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestStoreSaveCurrentStateAlsoIndexesBlockStates(t *testing.T) {
	store := New()

	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	shard := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 11}

	current := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: master},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard): {Block: shard},
		},
	}

	if err := store.SaveCurrentState(context.Background(), current); err != nil {
		t.Fatalf("save current state: %v", err)
	}

	gotCurrent, err := store.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("current state: %v", err)
	}
	if !gotCurrent.Masterchain.Block.Equals(&master) {
		t.Fatalf("unexpected masterchain block: %v", gotCurrent.Masterchain.Block)
	}

	gotMaster, err := store.BlockState(context.Background(), master)
	if err != nil {
		t.Fatalf("master block state: %v", err)
	}
	if !gotMaster.Block.Equals(&master) {
		t.Fatalf("unexpected stored master block: %v", gotMaster.Block)
	}

	gotShard, err := store.BlockState(context.Background(), shard)
	if err != nil {
		t.Fatalf("shard block state: %v", err)
	}
	if !gotShard.Block.Equals(&shard) {
		t.Fatalf("unexpected stored shard block: %v", gotShard.Block)
	}

	meta, err := store.BlockMeta(context.Background(), master)
	if err != nil {
		t.Fatalf("master block meta: %v", err)
	}
	if !meta.Has(storage.BlockMetaHasStateSnapshot) {
		t.Fatalf("expected state snapshot flag, got %v", meta.Flags)
	}
}

func TestStoreReturnsErrNotFound(t *testing.T) {
	store := New()
	block := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 999}

	_, err := store.CurrentState(context.Background())
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state: expected ErrNotFound, got %v", err)
	}

	_, err = store.BlockState(context.Background(), block)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block state: expected ErrNotFound, got %v", err)
	}

	_, err = store.StateSyncProgress(context.Background())
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("state sync progress: expected ErrNotFound, got %v", err)
	}

	_, err = store.BlockMeta(context.Background(), block)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block meta: expected ErrNotFound, got %v", err)
	}
}
