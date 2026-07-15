package pebblestore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestNextKeyBlockMetasRejectsIndexWithoutMetadata(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	block := testMasterBlockID(42, 0x42)
	if err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:       block,
		Flags:    storage.BlockMetaIsKeyBlock,
		GenUTime: 42,
	}); err != nil {
		t.Fatalf("save key block meta: %v", err)
	}
	if err = store.deleteHotRecord(ctx, hotKeyBlockMeta(block), pebble.Sync); err != nil {
		t.Fatalf("delete indexed block meta: %v", err)
	}

	_, err = store.NextKeyBlockMetas(ctx, block.SeqNo-1, 1)
	assertKeyBlockIndexInvariantError(t, err, block, "block metadata is missing")
}

func TestNextKeyBlockMetasRejectsIndexForNonKeyMetadata(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	block := testMasterBlockID(43, 0x43)
	if err = store.SaveBlockMeta(&storage.BlockMeta{
		ID:       block,
		Flags:    storage.BlockMetaIsKeyBlock,
		GenUTime: 43,
	}); err != nil {
		t.Fatalf("save key block meta: %v", err)
	}
	nonKeyMeta := &storage.BlockMeta{
		ID:       block,
		GenUTime: 43,
	}
	if err = store.setHotRecord(ctx, hotKeyBlockMeta(block), encodeBlockMeta(nonKeyMeta), pebble.Sync); err != nil {
		t.Fatalf("replace indexed block meta: %v", err)
	}

	_, err = store.NextKeyBlockMetas(ctx, block.SeqNo-1, 1)
	assertKeyBlockIndexInvariantError(t, err, block, "metadata is not marked as key block")
}

func TestNextKeyBlockMetasRetainsNotFoundSemantics(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	ctx := context.Background()
	if _, err = store.NextKeyBlockMetas(ctx, 0, 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty key block index error = %v, want ErrNotFound", err)
	}
	if _, err = store.NextKeyBlockMetas(ctx, 0, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("zero limit error = %v, want ErrNotFound", err)
	}
	if _, err = store.NextKeyBlockMetas(ctx, ^uint32(0), 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("maximum seqno error = %v, want ErrNotFound", err)
	}
}

func assertKeyBlockIndexInvariantError(t *testing.T, err error, block ton.BlockIDExt, detail string) {
	t.Helper()

	if err == nil {
		t.Fatal("corrupt key block index was accepted")
	}
	if errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("corrupt key block index error = %v, must not match ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "key block index invariant violated") {
		t.Fatalf("corrupt key block index error = %v, want invariant violation", err)
	}
	if !strings.Contains(err.Error(), detail) {
		t.Fatalf("corrupt key block index error = %v, want %q", err, detail)
	}
	if !strings.Contains(err.Error(), storage.FormatBlockRef(block)) {
		t.Fatalf("corrupt key block index error = %v, want block context", err)
	}
}
