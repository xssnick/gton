package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"
)

var errServiceMaintenanceRescan = errors.New("service maintenance priority rescan")

type serviceMaintenanceTask uint8

const (
	serviceMaintenanceTaskNone serviceMaintenanceTask = iota
	serviceMaintenanceTaskPersistentStateGC
	serviceMaintenanceTaskArchiveTTLGC
	serviceMaintenanceTaskCellGenerationMigration
	serviceMaintenanceTaskStateSerialization
)

func (s *Service) runServiceMaintenance(ctx context.Context) {
	nextPersistentStateGC := time.Now()
	nextArchiveGC := time.Now()
	var nextStateSerialization time.Time
	if s.stateSerializer != nil {
		nextStateSerialization = time.Now()
	}

	for {
		now := time.Now()
		persistentStateGCDue := !now.Before(nextPersistentStateGC)
		archiveGCDue := !now.Before(nextArchiveGC)
		stateSerializationDue := !nextStateSerialization.IsZero() && !now.Before(nextStateSerialization)

		migrationPending := false
		if !persistentStateGCDue && !archiveGCDue {
			var err error
			migrationPending, err = s.cellGenerationMigrationPending(ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				s.log.Warn().
					Err(err).
					Dur("retry_delay", stateSerializationRetryDelay).
					Msg("failed to check pending cell generation migration")
				if !s.waitServiceMaintenanceWake(ctx, stateSerializationRetryDelay) {
					return
				}
				continue
			}
		}

		switch nextServiceMaintenanceTask(persistentStateGCDue, archiveGCDue, migrationPending, stateSerializationDue) {
		case serviceMaintenanceTaskPersistentStateGC:
			delay := persistentStateGCInterval
			pruned, err := s.runPersistentStateGCOnce(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				s.log.Warn().
					Err(err).
					Dur("retry_delay", persistentStateGCRetryDelay).
					Msg("persistent state gc failed")
				delay = persistentStateGCRetryDelay
			}
			if pruned {
				delay = 0
			}
			nextPersistentStateGC = time.Now().Add(delay)
			continue

		case serviceMaintenanceTaskArchiveTTLGC:
			delay := archiveGCInterval
			pruned, err := s.runArchiveGCOnce(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				s.log.Warn().
					Err(err).
					Dur("retry_delay", archiveGCRetryDelay).
					Msg("archive ttl gc failed")
				delay = archiveGCRetryDelay
			}
			if pruned {
				delay = 0
			}
			nextArchiveGC = time.Now().Add(delay)
			continue

		case serviceMaintenanceTaskCellGenerationMigration:
			ran, err := s.runPendingCellGenerationMigration(ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				if ran {
					continue
				}
				event := s.log.Debug()
				if !isExpectedRetryError(err) {
					event = s.log.Warn()
				}
				event.
					Err(err).
					Dur("retry_delay", stateSerializationRetryDelay).
					Msg("pending cell generation migration cannot start")
				if !s.waitServiceMaintenanceWake(ctx, stateSerializationRetryDelay) {
					return
				}
				continue
			}
			if ran {
				continue
			}

		case serviceMaintenanceTaskStateSerialization:
			delay := stateSerializationIdlePollDelay
			err := s.processPersistentStateSerialization(ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, errServiceMaintenanceRescan) {
				nextStateSerialization = time.Now()
				continue
			}
			if err != nil {
				delay = stateSerializationRetryDelay
				event := s.log.Warn()
				if errors.Is(err, errStateSerializationRunning) || isExpectedRetryError(err) {
					event = s.log.Debug()
				}
				event.Err(err).Dur("retry_in", delay).Msg("persistent state serializer iteration failed")
			}
			nextStateSerialization = time.Now().Add(delay)
			continue
		}

		if !s.waitServiceMaintenanceWake(ctx, serviceMaintenanceWaitDelay(nextPersistentStateGC, nextArchiveGC, nextStateSerialization)) {
			return
		}
	}
}

func nextServiceMaintenanceTask(persistentStateGCDue bool, archiveGCDue bool, cellGenerationMigrationPending bool, stateSerializationDue bool) serviceMaintenanceTask {
	if persistentStateGCDue {
		return serviceMaintenanceTaskPersistentStateGC
	}
	if archiveGCDue {
		return serviceMaintenanceTaskArchiveTTLGC
	}
	if cellGenerationMigrationPending {
		return serviceMaintenanceTaskCellGenerationMigration
	}
	if stateSerializationDue {
		return serviceMaintenanceTaskStateSerialization
	}
	return serviceMaintenanceTaskNone
}

func (s *Service) cellGenerationMigrationPending(ctx context.Context) (bool, error) {
	store, ok := s.storage.(cellGenerationRotationStore)
	if !ok {
		return false, nil
	}

	_, err := store.PendingCellGenerationMigration(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load pending cell generation migration: %w", err)
	}
	return true, nil
}

func (s *Service) runPendingCellGenerationMigration(ctx context.Context) (bool, error) {
	store, ok := s.storage.(cellGenerationRotationStore)
	if !ok {
		return false, nil
	}

	pending, err := store.PendingCellGenerationMigration(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load pending cell generation migration: %w", err)
	}

	migrationLease, err := s.beginCellGenerationMigration(ctx)
	if err != nil {
		return false, err
	}
	defer migrationLease.release()

	started := time.Now()
	if err = s.runCellGenerationMigration(s.currentStatePersistContext(), store, pending.OriginPersistentState); err != nil {
		if errors.Is(err, context.Canceled) {
			s.log.Info().
				Uint64("cell_generation", pending.ID).
				Str("persistent_state", storage.FormatBlockRef(pending.OriginPersistentState)).
				Msg("pending cell generation migration stopped")
			return true, err
		}
		s.log.Error().
			Err(err).
			Uint64("cell_generation", pending.ID).
			Str("persistent_state", storage.FormatBlockRef(pending.OriginPersistentState)).
			Dur("elapsed", time.Since(started)).
			Msg("pending cell generation migration failed")
		return true, err
	}

	s.log.Info().
		Uint64("cell_generation", pending.ID).
		Str("persistent_state", storage.FormatBlockRef(pending.OriginPersistentState)).
		Dur("elapsed", time.Since(started)).
		Msg("pending cell generation migration finished")
	return true, nil
}

func serviceMaintenanceWaitDelay(times ...time.Time) time.Duration {
	var delay time.Duration
	set := false
	for _, at := range times {
		if at.IsZero() {
			continue
		}
		next := time.Until(at)
		if next < 0 {
			next = 0
		}
		if !set || next < delay {
			delay = next
			set = true
		}
	}
	if !set {
		return stateSerializationIdlePollDelay
	}
	return delay
}

func (s *Service) waitServiceMaintenanceWake(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-s.maintenanceWake:
		return true
	case <-timer.C:
		return true
	}
}
