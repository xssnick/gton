//go:build windows

package service

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func statFSSyncDiskSpace(path string) (syncDiskSpaceStatus, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return syncDiskSpaceStatus{}, fmt.Errorf("check disk usage %s: %w", path, err)
	}

	var freeBytesAvailable uint64
	var totalBytes uint64
	var totalFreeBytes uint64
	if err = windows.GetDiskFreeSpaceEx(pathPtr, &freeBytesAvailable, &totalBytes, &totalFreeBytes); err != nil {
		return syncDiskSpaceStatus{}, fmt.Errorf("check disk usage %s: %w", path, err)
	}
	return syncDiskSpaceStatus{Path: path, AvailableBytes: freeBytesAvailable}, nil
}
