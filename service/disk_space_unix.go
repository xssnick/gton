//go:build unix

package service

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func statFSSyncDiskSpace(path string) (syncDiskSpaceStatus, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return syncDiskSpaceStatus{}, fmt.Errorf("check disk usage %s: %w", path, err)
	}

	blockSize := uint64(stat.Bsize)
	availableBlocks := uint64(stat.Bavail)
	if blockSize != 0 && availableBlocks > ^uint64(0)/blockSize {
		return syncDiskSpaceStatus{Path: path, AvailableBytes: ^uint64(0)}, nil
	}
	return syncDiskSpaceStatus{Path: path, AvailableBytes: availableBlocks * blockSize}, nil
}
