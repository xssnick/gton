package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

var (
	errCellGenerationMigrationRunning  = errors.New("cell generation migration is running")
	errCellGenerationMigrationStopping = errors.New("cell generation migration is stopping")
	errCellGenerationMigrationStopped  = errors.New("cell generation migration stopped")
	errCellGenerationMigrationNotFound = errors.New("cell generation migration is not running")
	errCellGenerationMigrationAborted  = errors.New("cell generation migration aborted")
	errCellGenerationPersistentMissing = errors.New("serialized persistent state for cell generation migration is missing")
	errPendingCellGenerationCompaction = errors.New("pending cell generation is waiting for compaction")
)

const (
	cellGenerationMigrationProgressInterval = 5 * time.Second
	cellGenerationNextBlockYieldInterval    = 2 * time.Minute
)

type cellGenerationMigrationRun struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

func (s *StateLifecycle) afterPersistentStateSerialized(ctx context.Context, persistent ton.BlockIDExt, scope PersistentStateSerializationScope) {
	if scope != PersistentStateSerializationAll {
		s.log.Debug().
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Str("scope", scope.String()).
			Msg("skipping cell generation migration check for partial persistent state serialization")
		return
	}
	if s.syncGate.syncUntilFrozen() {
		s.log.Info().
			Uint32("sync_until", s.syncGate.syncUntilTarget()).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Msg("skipping cell generation migration check after sync_until")
		return
	}

	store := s.generationStore

	needed, err := s.shouldStartCellGenerationMigration(ctx, store, persistent)
	if err != nil {
		s.log.Error().
			Err(err).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Msg("failed to check cell generation migration need")
		return
	}
	if !needed {
		return
	}

	if err := s.queueCellGenerationMigration(ctx, store, persistent, "automatic"); err != nil {
		s.log.Info().
			Err(err).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Msg("cell generation migration cannot be queued")
	}
}

func (s *StateLifecycle) StartCellGenerationMigration(ctx context.Context, masterSeqno uint32) error {
	store := s.generationStore

	migrationLease, err := s.maintenance.beginExclusiveServiceTask(ctx, exclusiveServiceTaskCellGenerationMigration)
	if err != nil {
		return err
	}

	persistent, err := s.durableMasterchainBlock(ctx, store, masterSeqno, "cell generation migration")
	if err != nil {
		migrationLease.release()
		return err
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		migrationLease.release()
		return err
	}
	if active.OriginPersistentState.Equals(&persistent) {
		migrationLease.release()
		return fmt.Errorf("active cell generation %d already starts from persistent state %s", active.ID, storage.FormatBlockRef(persistent))
	}
	if err = s.ensureCellGenerationPersistentStateAvailable(ctx, store, persistent); err != nil {
		migrationLease.release()
		return err
	}

	return s.startCellGenerationMigrationWithLease(store, persistent, "manual", migrationLease)
}

func (s *StateLifecycle) queueCellGenerationMigration(ctx context.Context, store cellGenerationStore, persistent ton.BlockIDExt, source string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.checkCellGenerationMigrationStartAllowed(); err != nil {
		return err
	}
	if err := s.ensureCellGenerationPersistentStateAvailable(ctx, store, persistent); err != nil {
		return err
	}

	generation, err := store.BeginCellGeneration(s.shutdownContext, persistent)
	if err != nil {
		return fmt.Errorf("persist cell generation migration intent: %w", err)
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Str("persistent_state", storage.FormatBlockRef(persistent)).
		Str("source", source).
		Msg("cell generation migration queued")
	s.maintenance.wake()
	return nil
}

func (s *StateLifecycle) startCellGenerationMigrationWithLease(store cellGenerationStore, persistent ton.BlockIDExt, source string, migrationLease *exclusiveServiceTaskLease) error {
	runCtx, run, err := s.beginCellGenerationMigrationRun(s.shutdownContext)
	if err != nil {
		migrationLease.release()
		return err
	}

	generation, err := store.BeginCellGeneration(runCtx, persistent)
	if err != nil {
		s.finishCellGenerationMigrationRun(run)
		migrationLease.release()
		return fmt.Errorf("persist cell generation migration intent: %w", err)
	}

	s.runAsync(func() {
		defer migrationLease.release()

		started := time.Now()
		err := s.runCellGenerationMigration(runCtx, persistent)
		s.finishCellGenerationMigrationRun(run)
		if err != nil {
			if errors.Is(err, errCellGenerationMigrationAborted) {
				return
			}
			if errors.Is(err, context.Canceled) {
				s.log.Info().
					Err(err).
					Uint64("cell_generation", generation).
					Str("persistent_state", storage.FormatBlockRef(persistent)).
					Str("source", source).
					Msg("cell generation migration stopped")
				return
			}
			s.log.Error().
				Err(err).
				Uint64("cell_generation", generation).
				Str("persistent_state", storage.FormatBlockRef(persistent)).
				Str("source", source).
				Dur("elapsed", time.Since(started)).
				Msg("cell generation migration failed")
			return
		}

		s.log.Info().
			Uint64("cell_generation", generation).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Str("source", source).
			Dur("elapsed", time.Since(started)).
			Msg("cell generation migration finished")
	})
	return nil
}

func (s *StateLifecycle) ensureCellGenerationPersistentStateAvailable(ctx context.Context, store cellGenerationStore, persistent ton.BlockIDExt) error {
	if _, err := store.PersistentStateFile(ctx, persistent, persistent, 0); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("%w: masterchain %s", errCellGenerationPersistentMissing, storage.FormatBlockRef(persistent))
		}
		return fmt.Errorf("load serialized masterchain persistent state file %s: %w", storage.FormatBlockRef(persistent), err)
	}
	return nil
}

func (s *StateLifecycle) shouldStartCellGenerationMigration(ctx context.Context, store cellGenerationStore, persistent ton.BlockIDExt) (bool, error) {
	if s.stateTTL <= 0 {
		return false, nil
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		return false, err
	}
	if active.OriginPersistentState.Equals(&persistent) {
		s.log.Info().
			Uint64("cell_generation", active.ID).
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Msg("skipping cell generation migration because active generation already starts from this persistent state")
		return false, nil
	}

	persistentMeta, err := store.BlockMeta(ctx, persistent)
	if err != nil {
		return false, fmt.Errorf("load persistent state block meta %s: %w", storage.FormatBlockRef(persistent), err)
	}
	if persistentMeta.GenUTime == 0 {
		return false, fmt.Errorf("persistent state block %s has no gen utime", storage.FormatBlockRef(persistent))
	}

	if !emptyBlockID(active.OriginPersistentState) {
		originMeta, err := store.BlockMeta(ctx, active.OriginPersistentState)
		if err != nil {
			return false, fmt.Errorf("load active cell generation origin meta %s: %w", storage.FormatBlockRef(active.OriginPersistentState), err)
		}
		if originMeta.GenUTime == 0 {
			return false, fmt.Errorf("active cell generation origin %s has no gen utime", storage.FormatBlockRef(active.OriginPersistentState))
		}
		if persistentMeta.GenUTime <= originMeta.GenUTime {
			return false, nil
		}

		age := time.Duration(persistentMeta.GenUTime-originMeta.GenUTime) * time.Second
		if age < s.stateTTL {
			s.log.Info().
				Uint64("cell_generation", active.ID).
				Str("origin_persistent_state", storage.FormatBlockRef(active.OriginPersistentState)).
				Str("persistent_state", storage.FormatBlockRef(persistent)).
				Dur("generation_age", age).
				Dur("state_ttl", s.stateTTL).
				Msg("skipping cell generation migration because state ttl boundary is not reached")
			return false, nil
		}
	}

	s.log.Info().
		Uint64("cell_generation", active.ID).
		Str("origin_persistent_state", storage.FormatBlockRef(active.OriginPersistentState)).
		Str("persistent_state", storage.FormatBlockRef(persistent)).
		Dur("state_ttl", s.stateTTL).
		Msg("cell generation migration is needed after persistent state serialization")
	return true, nil
}

func (s *MaintenanceRunner) beginPendingCellGenerationMigration(ctx context.Context) (*exclusiveServiceTaskLease, error) {
	s.exclusiveTaskMu.Lock()
	if err := s.canStartExclusiveServiceTaskLocked(exclusiveServiceTaskCellGenerationMigration); err != nil {
		s.exclusiveTaskMu.Unlock()
		return nil, err
	}
	s.exclusiveTaskMu.Unlock()

	if err := s.canStartExclusiveServiceTaskLag(ctx, time.Now()); err != nil {
		return nil, err
	}

	s.exclusiveTaskMu.Lock()
	defer s.exclusiveTaskMu.Unlock()
	if err := s.canStartExclusiveServiceTaskLocked(exclusiveServiceTaskCellGenerationMigration); err != nil {
		return nil, err
	}
	s.exclusiveTask = exclusiveServiceTaskCellGenerationMigration
	return &exclusiveServiceTaskLease{maintenance: s, task: exclusiveServiceTaskCellGenerationMigration}, nil
}

func (s *StateLifecycle) StopCellGenerationMigration(ctx context.Context) error {
	store := s.generationStore

	run, err := s.beginCellGenerationMigrationStop()
	if err != nil {
		return err
	}
	defer s.finishCellGenerationMigrationStop()

	pending, err := store.PendingCellGenerationMigration(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		if run != nil {
			s.log.Info().Msg("cell generation migration stop requested without pending generation")
			return nil
		}
		return errCellGenerationMigrationNotFound
	}
	if err != nil {
		return fmt.Errorf("load pending cell generation migration: %w", err)
	}

	if err := store.DropPendingCellGeneration(ctx, pending.ID); err != nil {
		return fmt.Errorf("drop pending cell generation %d: %w", pending.ID, err)
	}

	s.log.Info().
		Uint64("cell_generation", pending.ID).
		Str("persistent_state", storage.FormatBlockRef(pending.OriginPersistentState)).
		Bool("active_run_canceled", run != nil).
		Msg("pending cell generation migration stopped and queued for removal")
	return nil
}

func (s *StateLifecycle) beginCellGeneration(ctx context.Context, origin ton.BlockIDExt) (uint64, error) {
	return s.generationStore.BeginCellGeneration(ctx, origin)
}

func (s *StateLifecycle) cellGenerationCompactionWait(ctx context.Context, generation uint64) (storage.CellGenerationDBMetrics, bool, error) {
	store := s.generationStore
	metrics, err := store.CellGenerationDBMetrics(ctx, generation)
	if err != nil {
		return storage.CellGenerationDBMetrics{}, false, fmt.Errorf("load cell generation db metrics: %w", err)
	}
	return metrics, metrics.MaxReadAmp > CellGenerationSwitchMaxReadAmp, nil
}

func (s *StateLifecycle) checkCellGenerationMigrationStartAllowed() error {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	if s.cellGenerationMigrationStopping {
		return errCellGenerationMigrationStopping
	}
	return nil
}

func (s *StateLifecycle) beginCellGenerationMigrationRun(ctx context.Context) (context.Context, *cellGenerationMigrationRun, error) {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	if s.cellGenerationMigrationStopping {
		return nil, nil, errCellGenerationMigrationStopping
	}
	if s.cellGenerationMigrationRun != nil {
		return nil, nil, errCellGenerationMigrationRunning
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	run := &cellGenerationMigrationRun{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	s.cellGenerationMigrationRun = run
	return runCtx, run, nil
}

func (s *StateLifecycle) finishCellGenerationMigrationRun(run *cellGenerationMigrationRun) {
	s.cellMigrationMu.Lock()
	if s.cellGenerationMigrationRun == run {
		s.cellGenerationMigrationRun = nil
	}
	s.cellMigrationMu.Unlock()
	close(run.done)
}

func (s *StateLifecycle) beginCellGenerationMigrationStop() (*cellGenerationMigrationRun, error) {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	if s.cellGenerationMigrationStopping {
		return nil, errCellGenerationMigrationStopping
	}

	s.cellGenerationMigrationStopping = true
	run := s.cellGenerationMigrationRun
	if run != nil {
		run.cancel(errCellGenerationMigrationStopped)
	}
	return run, nil
}

func (s *StateLifecycle) finishCellGenerationMigrationStop() {
	s.cellMigrationMu.Lock()
	s.cellGenerationMigrationStopping = false
	s.cellGenerationSwitchRequested = false
	s.cellGenerationNextBlockYieldAt = time.Time{}
	s.cellMigrationMu.Unlock()
	s.maintenance.wake()
}

func (s *StateLifecycle) requestCellGenerationSwitch(generation uint64, target ton.BlockIDExt) {
	s.cellMigrationMu.Lock()
	alreadyRequested := s.cellGenerationSwitchRequested
	s.cellGenerationSwitchRequested = true
	if !alreadyRequested {
		s.cellGenerationNextBlockYieldAt = time.Time{}
	}
	s.cellMigrationMu.Unlock()

	if !alreadyRequested {
		s.log.Info().
			Uint64("cell_generation", generation).
			Str("target", storage.FormatBlockRef(target)).
			Msg("cell generation switch requested")
	}

	s.checkpointTransitions.broadcastCurrentStateWake()
}

func (s *StateLifecycle) clearCellGenerationSwitchRequest() {
	s.cellMigrationMu.Lock()
	s.cellGenerationSwitchRequested = false
	s.cellGenerationNextBlockYieldAt = time.Time{}
	s.cellMigrationMu.Unlock()
}

func (s *StateLifecycle) cellGenerationSwitchRequestActive() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	return s.cellGenerationSwitchRequested
}

func (s *StateLifecycle) shouldYieldNextBlockForCellGenerationSwitch(now time.Time) bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	if !s.cellGenerationSwitchRequested {
		return false
	}
	if !s.cellGenerationNextBlockYieldAt.IsZero() && now.Sub(s.cellGenerationNextBlockYieldAt) < cellGenerationNextBlockYieldInterval {
		return false
	}
	s.cellGenerationNextBlockYieldAt = now
	return true
}

func (s *StateLifecycle) beginCellGenerationSwitch() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()

	if s.cellGenerationSwitching {
		return false
	}
	s.cellGenerationSwitching = true
	return true
}

func (s *StateLifecycle) finishCellGenerationSwitch() {
	s.cellMigrationMu.Lock()
	s.cellGenerationSwitchRequested = false
	s.cellGenerationNextBlockYieldAt = time.Time{}
	s.cellGenerationSwitching = false
	s.cellMigrationMu.Unlock()
}

func (s *StateLifecycle) cellGenerationSwitchActive() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	return s.cellGenerationSwitching
}

func (s *StateLifecycle) checkCurrentStatePersistAllowed() error {
	if err := s.takeCurrentStatePersistError(); err != nil {
		return err
	}
	if s.cellGenerationSwitchActive() {
		return errCellGenerationMigrationRunning
	}
	return nil
}

func emptyBlockID(block ton.BlockIDExt) bool {
	return block.Workchain == 0 &&
		block.Shard == 0 &&
		block.SeqNo == 0 &&
		emptyBlockIDHash(block.RootHash) &&
		emptyBlockIDHash(block.FileHash)
}

func emptyBlockIDHash(hash []byte) bool {
	if len(hash) == 0 {
		return true
	}
	return len(hash) == 32 && [32]byte(hash) == [32]byte{}
}
