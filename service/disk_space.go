package service

import (
	"context"
	"errors"
	"strings"
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
