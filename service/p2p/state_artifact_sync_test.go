package p2p

import (
	"errors"
	"os"
	"testing"
)

func TestSyncStateFileDir(t *testing.T) {
	dir := t.TempDir()
	if err := syncStateFileDir(dir); err != nil {
		t.Fatalf("sync state file directory: %v", err)
	}

	missing := dir + "-missing"
	if err := syncStateFileDir(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sync missing state file directory = %v, want not exist", err)
	}
}
