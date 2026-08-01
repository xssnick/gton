package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestZeroStateSizeReadsOnlyStoredRef(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		RootHash:  bytes.Repeat([]byte{0x41}, 32),
		FileHash:  bytes.Repeat([]byte{0x42}, 32),
	}
	data := []byte{0x01, 0x02, 0x03, 0x04}
	if err = store.SaveZeroState(block, data, nil); err != nil {
		t.Fatalf("save zerostate: %v", err)
	}

	path, err := store.zeroStateArtifactPath(block)
	if err != nil {
		t.Fatalf("zerostate artifact path: %v", err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatalf("remove zerostate payload: %v", err)
	}

	size, err := store.ZeroStateSize(ctx, block)
	if err != nil {
		t.Fatalf("zerostate size without payload file: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("zerostate size = %d, want %d", size, len(data))
	}
	if _, err = store.ZeroState(ctx, block); err == nil {
		t.Fatal("zerostate payload read succeeded after payload removal")
	}

	if err = store.setHotRecord(
		ctx,
		hotKeyZeroStateRef(block),
		encodeStateFileRef(0),
		pebble.Sync,
	); err != nil {
		t.Fatalf("store corrupt zerostate ref: %v", err)
	}
	if _, err = store.ZeroStateSize(ctx, block); err == nil ||
		errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("corrupt zerostate ref error = %v, want corruption error", err)
	}
}
