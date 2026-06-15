package pebblestore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestArtifactFileCacheReleaseDoesNotBroadcastWithoutWaiters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.pack")
	if err := os.WriteFile(path, []byte("a"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	cache := newArtifactFileCache(1)
	defer func() { _ = cache.close() }()

	notify := cache.notify
	handle, err := cache.acquire(context.Background(), path)
	if err != nil {
		t.Fatalf("acquire artifact: %v", err)
	}
	handle.release()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.notify != notify {
		t.Fatal("artifact file cache broadcasted without waiters")
	}
	if cache.waiters != 0 {
		t.Fatalf("waiters = %d, want 0", cache.waiters)
	}
}

func TestArtifactFileCacheReleaseBroadcastsToWaiters(t *testing.T) {
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

	first, err := cache.acquire(context.Background(), firstPath)
	if err != nil {
		t.Fatalf("acquire first artifact: %v", err)
	}

	type acquireResult struct {
		handle *artifactFileHandle
		err    error
	}
	done := make(chan acquireResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		handle, err := cache.acquire(ctx, secondPath)
		done <- acquireResult{handle: handle, err: err}
	}()

	waitForArtifactCacheWaiters(t, cache, 1)
	first.release()

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("acquire second artifact: %v", result.err)
		}
		result.handle.release()
	case <-time.After(time.Second):
		t.Fatal("artifact file cache waiter was not notified")
	}
}

func waitForArtifactCacheWaiters(t *testing.T, cache *artifactFileCache, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		cache.mu.Lock()
		got := cache.waiters
		cache.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}

	cache.mu.Lock()
	got := cache.waiters
	cache.mu.Unlock()
	t.Fatalf("artifact file cache waiters = %d, want %d", got, want)
}
