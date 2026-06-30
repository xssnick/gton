package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"
)

var errServiceMaintenanceRescan = errors.New("service maintenance priority rescan")

const serviceMaintenanceStartDelay = 30 * time.Second

type serviceMaintenanceTask uint8

const (
	serviceMaintenanceTaskNone serviceMaintenanceTask = iota
	serviceMaintenanceTaskPersistentStateGC
	serviceMaintenanceTaskArchiveTTLGC
	serviceMaintenanceTaskCellGenerationMigration
	serviceMaintenanceTaskStateSerialization
)

func (s *Service) runDelayedServiceMaintenance(ctx context.Context) {
	s.log.Info().
		Dur("delay", serviceMaintenanceStartDelay).
		Msg("delaying service maintenance worker start")

	timer := time.NewTimer(serviceMaintenanceStartDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-s.maintenanceWake:
	case <-timer.C:
	}

	if s.syncUntilFrozen() {
		s.log.Info().
			Uint32("sync_until", s.syncUntil).
			Msg("skipping service maintenance after sync_until")
		return
	}

	s.runServiceMaintenance(ctx)
}

func (s *Service) runServiceMaintenance(ctx context.Context) {
	nextPersistentStateGC := time.Now()
	var nextArchiveGC time.Time
	if s.archiveTTL > 0 {
		nextArchiveGC = time.Now()
	}
	var nextCellGenerationMigrationRetry time.Time
	var nextStateSerialization time.Time
	if s.stateSerializer != nil {
		nextStateSerialization = time.Now()
	}

	for {
		if s.syncUntilFrozen() {
			s.log.Info().
				Uint32("sync_until", s.syncUntil).
				Msg("stopping service maintenance after sync_until")
			return
		}

		now := time.Now()
		persistentStateGCDue := !now.Before(nextPersistentStateGC)
		archiveGCDue := !nextArchiveGC.IsZero() && !now.Before(nextArchiveGC)
		stateSerializationDue := !nextStateSerialization.IsZero() && !now.Before(nextStateSerialization)

		migrationPending, err := s.cellGenerationMigrationPending(ctx)
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
		if !migrationPending {
			nextCellGenerationMigrationRetry = time.Time{}
		} else {
			persistentStateGCDue = false
		}

		cellGenerationMigrationDue := migrationPending && !now.Before(nextCellGenerationMigrationRetry)
		switch nextServiceMaintenanceTask(persistentStateGCDue, archiveGCDue, cellGenerationMigrationDue, stateSerializationDue) {
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
			if errors.Is(err, errPendingCellGenerationCompaction) {
				nextCellGenerationMigrationRetry = time.Now().Add(stateSerializationRetryDelay)
				continue
			}
			nextCellGenerationMigrationRetry = time.Time{}
			if err != nil {
				if ran {
					nextCellGenerationMigrationRetry = time.Now().Add(stateSerializationRetryDelay)
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
			var delayErr stateSerializationDelayError
			if errors.As(err, &delayErr) {
				nextStateSerialization = time.Now().Add(delayErr.delay)
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

		nextPersistentStateGCWait := nextPersistentStateGC
		if migrationPending {
			nextPersistentStateGCWait = time.Time{}
		}
		if !s.waitServiceMaintenanceWake(ctx, serviceMaintenanceWaitDelay(nextPersistentStateGCWait, nextArchiveGC, nextCellGenerationMigrationRetry, nextStateSerialization)) {
			return
		}
	}
}

func nextServiceMaintenanceTask(persistentStateGCDue bool, archiveGCDue bool, cellGenerationMigrationPending bool, stateSerializationDue bool) serviceMaintenanceTask {
	if cellGenerationMigrationPending {
		return serviceMaintenanceTaskCellGenerationMigration
	}
	if persistentStateGCDue {
		return serviceMaintenanceTaskPersistentStateGC
	}
	if archiveGCDue {
		return serviceMaintenanceTaskArchiveTTLGC
	}
	if stateSerializationDue {
		return serviceMaintenanceTaskStateSerialization
	}
	return serviceMaintenanceTaskNone
}

func (s *Service) cellGenerationMigrationPending(ctx context.Context) (bool, error) {
	store := s.storage

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
	if s.syncUntilFrozen() {
		return false, nil
	}

	store := s.storage

	pending, err := store.PendingCellGenerationMigration(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load pending cell generation migration: %w", err)
	}

	migrationLease, err := s.beginPendingCellGenerationMigration(ctx)
	if err != nil {
		return false, err
	}
	defer migrationLease.release()

	runCtx, run, err := s.beginCellGenerationMigrationRun(s.currentStatePersistContext())
	if err != nil {
		return false, err
	}
	runFinished := false
	finishRun := func() {
		if runFinished {
			return
		}
		s.finishCellGenerationMigrationRun(run)
		runFinished = true
	}
	defer finishRun()

	generation, err := store.BeginCellGeneration(runCtx, pending.OriginPersistentState)
	if err != nil {
		return false, fmt.Errorf("begin pending cell generation: %w", err)
	}
	if generation != pending.ID {
		return false, fmt.Errorf("pending cell generation changed from %d to %d", pending.ID, generation)
	}

	metrics, waitingForCompaction, err := s.cellGenerationCompactionWait(runCtx, store, pending.ID)
	if err != nil {
		return false, err
	}
	if waitingForCompaction {
		s.log.Info().
			Uint64("cell_generation", pending.ID).
			Str("persistent_state", storage.FormatBlockRef(pending.OriginPersistentState)).
			Int64("max_read_amp", metrics.MaxReadAmp).
			Int64("read_amp_limit", CellGenerationSwitchMaxReadAmp).
			Int64("l0_files", metrics.L0Files).
			Int64("l0_sublevels", metrics.L0Sublevels).
			Int64("l0_size", metrics.L0Size).
			Uint64("compaction_debt", metrics.CompactionDebt).
			Int64("compactions_in_progress", metrics.CompactionsInProgress).
			Int64("compaction_in_progress_bytes", metrics.CompactionInProgressSize).
			Msg("waiting for pending cell generation compaction before migration resume")
		return false, errPendingCellGenerationCompaction
	}

	started := time.Now()
	err = s.runCellGenerationMigration(runCtx, store, pending.OriginPersistentState)
	finishRun()
	if err != nil {
		if errors.Is(err, errCellGenerationMigrationAborted) {
			return true, nil
		}
		if errors.Is(err, context.Canceled) {
			s.log.Info().
				Err(err).
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
			Dur("retry_delay", stateSerializationRetryDelay).
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
