package pebblestore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type removeArchivePackageFilesResult struct {
	deleted int
	bytes   uint64
	err     error
}

func TestRemoveArchivePackageFilesKeepsArtifactLockUntilDeleteMarkerCleared(t *testing.T) {
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
	if err = store.withHotBatchOptions(pebble.Sync, func(batch *pebble.Batch) error {
		return store.setPackDeletePending(batch, relPath)
	}); err != nil {
		t.Fatalf("set delete pending: %v", err)
	}

	store.hotWriteMu.Lock()
	hotWriteLocked := true
	unlockHotWrite := func() {
		if hotWriteLocked {
			store.hotWriteMu.Unlock()
			hotWriteLocked = false
		}
	}
	defer unlockHotWrite()

	done := make(chan removeArchivePackageFilesResult, 1)
	go func() {
		deleted, bytes, err := store.removeArchivePackageFiles([]string{relPath})
		done <- removeArchivePackageFilesResult{deleted: deleted, bytes: bytes, err: err}
	}()

	waitForArchivePackRemoved(t, path)

	artifactLockAcquired := make(chan struct{})
	go func() {
		store.artifactMu.Lock()
		store.artifactMu.Unlock()
		close(artifactLockAcquired)
	}()

	select {
	case <-artifactLockAcquired:
		unlockHotWrite()
		waitForRemoveArchivePackageFiles(t, done)
		t.Fatal("artifactMu was released before delete marker was cleared")
	case <-time.After(50 * time.Millisecond):
	}

	unlockHotWrite()
	result := waitForRemoveArchivePackageFiles(t, done)
	if result.err != nil {
		t.Fatalf("remove archive package files: %v", result.err)
	}

	select {
	case <-artifactLockAcquired:
	case <-time.After(time.Second):
		t.Fatal("artifactMu remained locked after delete marker was cleared")
	}

	if result.deleted != 1 {
		t.Fatalf("deleted files = %d, want 1", result.deleted)
	}
	if result.bytes != 4 {
		t.Fatalf("deleted bytes = %d, want 4", result.bytes)
	}
}

func waitForArchivePackRemoved(t *testing.T, path string) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			t.Fatalf("stat archive pack: %v", err)
		}

		select {
		case <-deadline:
			t.Fatalf("archive pack %s was not removed", path)
		case <-ticker.C:
		}
	}
}

func waitForRemoveArchivePackageFiles(t *testing.T, done <-chan removeArchivePackageFilesResult) removeArchivePackageFilesResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("removeArchivePackageFiles did not finish")
		return removeArchivePackageFilesResult{}
	}
}
