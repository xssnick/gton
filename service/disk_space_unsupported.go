//go:build !unix && !windows

package service

import "fmt"

func statFSSyncDiskSpace(path string) (syncDiskSpaceStatus, error) {
	return syncDiskSpaceStatus{}, fmt.Errorf("disk space check is not supported on this platform: %s", path)
}
