package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestDeletePersistentStateFileClosesCachedArtifactFile(t *testing.T) {
	store, block, master, path := openTestPersistentStateStore(t, []byte{1, 2, 3, 4, 5, 6})

	if _, err := store.PersistentStateSlice(context.Background(), block, master, 0, 0, 6); err != nil {
		t.Fatalf("persistent state slice: %v", err)
	}
	if err := store.DeletePersistentStateFile(context.Background(), block, master, 0); err != nil {
		t.Fatalf("delete persistent state file: %v", err)
	}

	assertArtifactFileNotCached(t, store.artifactFiles, path)
}

func TestSavePersistentStateFileServesReplacedFile(t *testing.T) {
	store, block, master, path := openTestPersistentStateStore(t, []byte{1, 2, 3, 4, 5, 6})

	chunk, err := store.PersistentStateSlice(context.Background(), block, master, 0, 0, 6)
	if err != nil {
		t.Fatalf("persistent state slice: %v", err)
	}
	if !bytes.Equal(chunk, []byte{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("persistent state slice = %x", chunk)
	}

	replacement := []byte{9, 8, 7, 6, 5, 4}
	tmpPath := path + ".replacement"
	if err = os.WriteFile(tmpPath, replacement, 0o644); err != nil {
		t.Fatalf("write replacement state file: %v", err)
	}
	if err = os.Rename(tmpPath, path); err != nil {
		t.Fatalf("rename replacement state file: %v", err)
	}
	saveTestPersistentStateFile(t, store, block, master, path, int64(len(replacement)))

	chunk, err = store.PersistentStateSlice(context.Background(), block, master, 0, 0, 6)
	if err != nil {
		t.Fatalf("replaced persistent state slice: %v", err)
	}
	if !bytes.Equal(chunk, replacement) {
		t.Fatalf("replaced persistent state slice = %x, want %x", chunk, replacement)
	}
}

func TestRemoveArchivePackageFilesClosesCachedArtifactFile(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	relPath := filepath.Join("archive", "packages", "arch0000", "archive.1.pack")
	path := store.artifactPath(relPath)
	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create archive dir: %v", err)
	}
	if err = os.WriteFile(path, []byte("pack"), 0o644); err != nil {
		t.Fatalf("write archive pack: %v", err)
	}
	if _, err = store.readArtifactRange(context.Background(), relPath, 0, 4, 4); err != nil {
		t.Fatalf("read archive pack: %v", err)
	}

	store.artifactMu.Lock()
	_, _, err = store.removeArchivePackageFilesLocked([]string{relPath})
	store.artifactMu.Unlock()
	if err != nil {
		t.Fatalf("remove archive package files: %v", err)
	}

	assertArtifactFileNotCached(t, store.artifactFiles, path)
}

func TestArtifactFileCacheInvalidateClosesIdleEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.pack")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	cache := newArtifactFileCache(2)
	defer func() { _ = cache.close() }()

	if _, err := cache.readRange(context.Background(), path, 0, 3, 3); err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	cache.mu.Lock()
	entry := cache.entries[path]
	cache.mu.Unlock()

	cache.invalidate(path)

	assertArtifactFileNotCached(t, cache, path)
	if _, err := entry.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("invalidated idle descriptor stat error = %v, want os.ErrClosed", err)
	}
}

func TestArtifactFileCacheInvalidateInUseEntryClosesOnRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.pack")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	cache := newArtifactFileCache(2)
	defer func() { _ = cache.close() }()

	handle, err := cache.acquire(context.Background(), path)
	if err != nil {
		t.Fatalf("acquire artifact: %v", err)
	}
	cache.invalidate(path)
	assertArtifactFileNotCached(t, cache, path)

	data := make([]byte, 3)
	if _, err = handle.entry.file.ReadAt(data, 0); err != nil {
		t.Fatalf("read through invalidated in-use descriptor: %v", err)
	}
	handle.release()
	if _, err = handle.entry.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("released invalidated descriptor stat error = %v, want os.ErrClosed", err)
	}
	assertArtifactFileNotCached(t, cache, path)

	if _, err = cache.readRange(context.Background(), path, 0, 3, 3); err != nil {
		t.Fatalf("read artifact after invalidation: %v", err)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.entries[path] == nil || cache.openCount != 1 {
		t.Fatalf("reopened artifact cached = %t, open count = %d", cache.entries[path] != nil, cache.openCount)
	}
}

func TestArtifactFileCacheInvalidateWhileOpeningDoesNotCacheDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.pack")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}

	cache := newArtifactFileCache(1)
	defer func() { _ = cache.close() }()

	cache.mu.Lock()
	cache.reserveSlotLocked()
	opening := &artifactFileOpen{done: make(chan struct{})}
	cache.opening[path] = opening
	cache.mu.Unlock()

	cache.invalidate(path)
	handle, err := cache.openReserved(context.Background(), path, opening)
	if err != nil {
		t.Fatalf("open reserved artifact: %v", err)
	}
	assertArtifactFileNotCached(t, cache, path)

	handle.release()
	if _, err = handle.entry.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("stale opened descriptor stat error = %v, want os.ErrClosed", err)
	}
	assertArtifactFileNotCached(t, cache, path)
}

func TestArtifactFileCacheConcurrentReadsAndInvalidations(t *testing.T) {
	dir := t.TempDir()
	paths := make([]string, 4)
	for idx := range paths {
		paths[idx] = filepath.Join(dir, fmt.Sprintf("archive.%d.pack", idx))
		if err := os.WriteFile(paths[idx], []byte{byte(idx)}, 0o644); err != nil {
			t.Fatalf("write artifact %d: %v", idx, err)
		}
	}

	cache := newArtifactFileCache(2)
	defer func() { _ = cache.close() }()

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for step := 0; step < 200; step++ {
				idx := (worker + step) % len(paths)
				if (worker+step)%5 == 0 {
					cache.invalidate(paths[idx])
					continue
				}

				got, err := cache.readRange(context.Background(), paths[idx], 0, 1, 1)
				if err != nil {
					t.Errorf("read artifact %d: %v", idx, err)
					return
				}
				if got[0] != byte(idx) {
					t.Errorf("artifact %d data = %x", idx, got)
					return
				}
			}
		}()
	}
	wg.Wait()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.openCount != len(cache.entries) {
		t.Fatalf("open count = %d, cached entries = %d", cache.openCount, len(cache.entries))
	}
	if len(cache.opening) != 0 || cache.waiters != 0 {
		t.Fatalf("opening = %d, waiters = %d, want idle cache", len(cache.opening), cache.waiters)
	}
}

func TestArtifactFileCacheCanceledWaiterKeepsNextWaiterRegistration(t *testing.T) {
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

	// A registers on the current channel, a broadcast consumes that
	// registration, and only then A leaves through its canceled context.
	cache.mu.Lock()
	notify := cache.notify
	cache.waiters++
	cache.broadcastLocked()
	cache.mu.Unlock()

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
	cache.cancelWaiter(notify)
	first.release()

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("acquire second artifact: %v", result.err)
		}
		result.handle.release()
	case <-time.After(2 * time.Second):
		t.Fatal("artifact file cache waiter was not notified")
	}
}

func openTestPersistentStateStore(t *testing.T, data []byte) (*Store, ton.BlockIDExt, ton.BlockIDExt, string) {
	t.Helper()

	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     77,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     78,
		RootHash:  bytes.Repeat([]byte{0x03}, 32),
		FileHash:  bytes.Repeat([]byte{0x04}, 32),
	}
	name, err := storage.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	path := filepath.Join(store.StateFilesDir(), name)
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	saveTestPersistentStateFile(t, store, block, master, path, int64(len(data)))
	return store, block, master, path
}

func saveTestPersistentStateFile(t *testing.T, store *Store, block ton.BlockIDExt, master ton.BlockIDExt, path string, size int64) {
	t.Helper()

	if err := store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		Ref:              &storage.ArtifactRef{Path: path, Size: size},
		FileHash:         bytes.Repeat([]byte{0x55}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x66}, 32),
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}
}

func assertArtifactFileNotCached(t *testing.T, cache *artifactFileCache, path string) {
	t.Helper()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.entries[path] != nil {
		t.Fatalf("artifact file %s is still cached after removal", path)
	}
	if cache.openCount != 0 {
		t.Fatalf("artifact file cache open count = %d, want 0", cache.openCount)
	}
}
