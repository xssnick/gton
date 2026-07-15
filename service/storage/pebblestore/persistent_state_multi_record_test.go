package pebblestore

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func TestDeletePersistentStateFileKeepsMetadataForAnotherMasterGroup(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := testServedBlockID(0, int64(-1<<63), 77, 0x50)
	firstMaster := testArchivePruneBlock(100, 0x10)
	secondMaster := testArchivePruneBlock(200, 0x20)
	if err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:       block,
		GenUTime: 1000,
		StartLT:  2000,
		EndLT:    3000,
	}); err != nil {
		t.Fatalf("save block meta: %v", err)
	}

	firstPath := saveTestPersistentStatePruneFileForBlock(t, store, block, firstMaster)
	secondPath := saveTestPersistentStatePruneFileForBlock(t, store, block, secondMaster)
	if firstPath == secondPath {
		t.Fatalf("persistent state paths are equal: %s", firstPath)
	}

	if err = store.DeletePersistentStateFile(ctx, block, firstMaster, 0); err != nil {
		t.Fatalf("delete first persistent state file: %v", err)
	}
	if _, err = os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("first persistent state file stat error = %v, want not exist", err)
	}
	if _, err = store.PersistentStateSize(ctx, block, secondMaster, 0); err != nil {
		t.Fatalf("load second persistent state size: %v", err)
	}
	meta, err := store.BlockMeta(ctx, block)
	if err != nil {
		t.Fatalf("load block meta after first delete: %v", err)
	}
	wantFileHash := bytes.Repeat([]byte{byte(block.SeqNo)}, 32)
	if !meta.Has(storage.BlockMetaHasStateSnapshot) || !bytes.Equal(meta.StateFileHash, wantFileHash) {
		t.Fatalf("block meta after first delete = %+v, want retained snapshot hash %x", meta, wantFileHash)
	}

	if err = store.DeletePersistentStateFile(ctx, block, secondMaster, 0); err != nil {
		t.Fatalf("delete second persistent state file: %v", err)
	}
	if _, err = os.Stat(secondPath); !os.IsNotExist(err) {
		t.Fatalf("second persistent state file stat error = %v, want not exist", err)
	}
	meta, err = store.BlockMeta(ctx, block)
	if err != nil {
		t.Fatalf("load block meta after second delete: %v", err)
	}
	if meta.Has(storage.BlockMetaHasStateSnapshot) || len(meta.StateFileHash) != 0 {
		t.Fatalf("block meta after last delete = %+v, want snapshot metadata cleared", meta)
	}
}
