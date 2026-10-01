package storage

import (
	"errors"
	"os"
	"testing"
)

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := SyncDir(dir); err != nil {
		t.Fatalf("sync directory: %v", err)
	}

	missing := dir + "-missing"
	if err := SyncDir(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sync missing directory = %v, want not exist", err)
	}
}
