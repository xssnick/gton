package pebblestore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestArtifactFileCacheRefreshesAppendedSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.pack")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	cache := newArtifactFileCache(1)
	defer func() { _ = cache.close() }()

	got, err := cache.readRange(context.Background(), path, 0, 3, 3)
	if err != nil {
		t.Fatalf("read initial range: %v", err)
	}
	if !bytes.Equal(got, []byte("abc")) {
		t.Fatalf("initial range mismatch: %q", got)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open artifact for append: %v", err)
	}
	if _, err = file.Write([]byte("def")); err != nil {
		_ = file.Close()
		t.Fatalf("append artifact: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close appended artifact: %v", err)
	}

	got, err = cache.readRange(context.Background(), path, 3, 3, 6)
	if err != nil {
		t.Fatalf("read appended range: %v", err)
	}
	if !bytes.Equal(got, []byte("def")) {
		t.Fatalf("appended range mismatch: %q", got)
	}
}

func TestArtifactFileCacheEvictsIdleFileAtLimit(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "first.pack")
	secondPath := filepath.Join(dir, "second.pack")
	if err := os.WriteFile(firstPath, []byte("a"), 0o644); err != nil {
		t.Fatalf("write first artifact: %v", err)
	}
	if err := os.WriteFile(secondPath, []byte("b"), 0o644); err != nil {
		t.Fatalf("write second artifact: %v", err)
	}

	cache := newArtifactFileCache(1)
	defer func() { _ = cache.close() }()

	if _, err := cache.readRange(context.Background(), firstPath, 0, 1, 1); err != nil {
		t.Fatalf("read first artifact: %v", err)
	}
	if _, err := cache.readRange(context.Background(), secondPath, 0, 1, 1); err != nil {
		t.Fatalf("read second artifact: %v", err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.openCount != 1 {
		t.Fatalf("unexpected open count %d", cache.openCount)
	}
	if cache.entries[secondPath] == nil {
		t.Fatal("second artifact should stay cached")
	}
	if cache.entries[firstPath] != nil {
		t.Fatal("first artifact should be evicted")
	}
}
