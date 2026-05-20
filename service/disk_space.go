package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"
)

var errSyncDiskSpaceInsufficient = errors.New("free disk space is below required minimum")

type syncDiskSpaceStatus struct {
	Path           string
	AvailableBytes uint64
}

type syncDiskSpaceProbe func(path string) (syncDiskSpaceStatus, error)

func (s *Service) waitSyncDiskSpace(ctx context.Context, flow string) error {
	path := strings.TrimSpace(s.syncDiskSpacePath)
	if path == "" || s.minSyncDiskFreeBytes == 0 {
		return nil
	}

	probe := s.syncDiskSpaceProbe
	if probe == nil {
		probe = statFSSyncDiskSpace
	}

	retryDelay := s.syncDiskSpaceRetryDelay
	if retryDelay <= 0 {
		retryDelay = syncDiskSpaceRetryDelay
	}

	for {
		status, err := probe(path)
		if err == nil && status.AvailableBytes >= s.minSyncDiskFreeBytes {
			return nil
		}

		event := s.log.Error().
			Str("flow", flow).
			Str("path", path).
			Uint64("required_bytes", s.minSyncDiskFreeBytes).
			Dur("retry_delay", retryDelay)
		if err != nil {
			event.Err(err).Msg("failed to check free disk space before sync")
		} else {
			event.
				Err(errSyncDiskSpaceInsufficient).
				Uint64("available_bytes", status.AvailableBytes).
				Msg("not enough free disk space before sync")
		}

		s.wakeServiceMaintenance()
		if !sleepContext(ctx, retryDelay) {
			return ctx.Err()
		}
	}
}

func statFSSyncDiskSpace(path string) (syncDiskSpaceStatus, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return syncDiskSpaceStatus{}, fmt.Errorf("statfs %s: %w", path, err)
	}

	blockSize := uint64(stat.Bsize)
	availableBlocks := uint64(stat.Bavail)
	if blockSize != 0 && availableBlocks > ^uint64(0)/blockSize {
		return syncDiskSpaceStatus{Path: path, AvailableBytes: ^uint64(0)}, nil
	}
	return syncDiskSpaceStatus{Path: path, AvailableBytes: availableBlocks * blockSize}, nil
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
