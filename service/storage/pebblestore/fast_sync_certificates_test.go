package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func TestFastSyncCertificateSnapshotRoundtrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	if _, err = store.FastSyncCertificateSnapshot(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing snapshot error = %v, want %v", err, storage.ErrNotFound)
	}

	first := []byte("certificates-v1")
	if err = store.SaveFastSyncCertificateSnapshot(ctx, first); err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}
	raw, err := store.FastSyncCertificateSnapshot(ctx)
	if err != nil {
		t.Fatalf("load first snapshot: %v", err)
	}
	if !bytes.Equal(raw, first) {
		t.Fatalf("first snapshot = %q, want %q", raw, first)
	}

	second := []byte("certificates-v2")
	if err = store.SaveFastSyncCertificateSnapshot(ctx, second); err != nil {
		t.Fatalf("save second snapshot: %v", err)
	}
	raw, err = store.FastSyncCertificateSnapshot(ctx)
	if err != nil {
		t.Fatalf("load second snapshot: %v", err)
	}
	if !bytes.Equal(raw, second) {
		t.Fatalf("second snapshot = %q, want %q", raw, second)
	}

	if err = store.Close(); err != nil {
		t.Fatalf("close store before reopen: %v", err)
	}
	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	raw, err = store.FastSyncCertificateSnapshot(ctx)
	if err != nil {
		t.Fatalf("load snapshot after reopen: %v", err)
	}
	if !bytes.Equal(raw, second) {
		t.Fatalf("snapshot after reopen = %q, want %q", raw, second)
	}

	if err = store.DeleteFastSyncCertificateSnapshot(ctx); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	if _, err = store.FastSyncCertificateSnapshot(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted snapshot error = %v, want %v", err, storage.ErrNotFound)
	}
}
