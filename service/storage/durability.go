package storage

import (
	"errors"
	"os"
	"runtime"
	"syscall"
)

// SyncDir persists directory metadata after durable file creation or rename.
// Windows cannot flush directory handles, so opening the directory is the
// strongest supported validation there. Some Unix filesystems report EINVAL
// when directory syncing is unsupported.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()

	if runtime.GOOS == "windows" {
		return nil
	}
	if err = dir.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}
