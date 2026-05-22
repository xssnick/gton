package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var (
	errCellGenerationMigrationRunning  = errors.New("cell generation migration is running")
	errCellGenerationMigrationStopping = errors.New("cell generation migration is stopping")
	errCellGenerationMigrationStopped  = errors.New("cell generation migration stopped")
	errCellGenerationMigrationNotFound = errors.New("cell generation migration is not running")
	errPendingCellGenerationCompaction = errors.New("pending cell generation is waiting for compaction")
)

const (
	cellGenerationMigrationProgressInterval = 5 * time.Second
	cellGenerationNextBlockYieldInterval    = 2 * time.Minute
)

type cellGenerationRotationStore interface {
	ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error)
	PendingCellGenerationMigration(ctx context.Context) (storage.CellGenerationInfo, error)
	CellGenerationDBMetrics(ctx context.Context, generation uint64) (storage.CellGenerationDBMetrics, error)
	CellGenerationStateImported(ctx context.Context, generation uint64, block ton.BlockIDExt) error
	MarkCellGenerationStateImported(ctx context.Context, generation uint64, block ton.BlockIDExt) error
	CellGenerationMigrationProgress(ctx context.Context, generation uint64) (*storage.CurrentState, error)
	SaveCellGenerationMigrationProgress(ctx context.Context, generation uint64, current *storage.CurrentState) error
	BeginCellGeneration(ctx context.Context, origin ton.BlockIDExt) (uint64, error)
	AbortCellGeneration(ctx context.Context, generation uint64) error
	DropPendingCellGeneration(ctx context.Context, generation uint64) error
	CleanupCellGeneration(ctx context.Context, generation uint64) error
	ThrottleCellGenerationCompactions(ctx context.Context, generation uint64) (func(), error)
	ImportStateCellTreeInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error)
	ImportStateBOCViewInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error)
	LazyCellLoaderInGeneration(generation uint64) cell.LazyCellLoader
	SaveEncodedCellsInGeneration(ctx context.Context, generation uint64, records []storage.EncodedCellRecord, sync bool) error
	SwitchCellGeneration(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *storage.CurrentState) (uint64, error)
	PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error)
}

type cellGenerationCandidate struct {
	generation uint64
	current    *storage.CurrentState
	cells      *stateCellWindowCache
}

type cellGenerationMigrationRun struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

type generationStateCellImporter struct {
	store      cellGenerationRotationStore
	generation uint64
}

func (i generationStateCellImporter) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error) {
	return i.store.ImportStateCellTreeInGeneration(ctx, i.generation, block, root, totalCells)
}

func (i generationStateCellImporter) ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error) {
	return i.store.ImportStateBOCViewInGeneration(ctx, i.generation, block, view)
}

func (i generationStateCellImporter) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (i generationStateCellImporter) TrustImportedStateCellHashes() bool {
	return true
}

func (i generationStateCellImporter) ReuseImportedSplitStatePartCells() bool {
	return false
}

func (s *Service) afterPersistentStateSerialized(ctx context.Context, persistent ton.BlockIDExt, scope PersistentStateSerializationScope) {
	if scope != PersistentStateSerializationAll {
		s.log.Debug().
			Str("persistent_state", storage.FormatBlockRef(persistent)).
			Str("scope", scope.String()).
			Msg("skipping cell generation migration check for partial persistent state serialization")
		return
	}

	store, ok := s.storage.(cellGenerationRotationStore)
	if !ok {
		s.log.Debug().
			Str("storage", fmt.Sprintf("%T", s.storage)).
			Msg("storage does not support cell generation migration")
		return
	}

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

func (s *Service) StartCellGenerationMigration(ctx context.Context, masterSeqno uint32) error {
	store, ok := s.storage.(cellGenerationRotationStore)
	if !ok {
		return fmt.Errorf("storage does not support cell generation migration")
	}

	migrationLease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskCellGenerationMigration)
	if err != nil {
		return err
	}

	persistent, err := s.durableMasterchainBlockForMigration(ctx, masterSeqno)
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

	return s.startCellGenerationMigrationWithLease(store, persistent, "manual", migrationLease)
}

func (s *Service) durableMasterchainBlockForMigration(ctx context.Context, masterSeqno uint32) (ton.BlockIDExt, error) {
	return s.durableMasterchainBlock(ctx, masterSeqno, "cell generation migration")
}

func (s *Service) queueCellGenerationMigration(ctx context.Context, store cellGenerationRotationStore, persistent ton.BlockIDExt, source string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.checkCellGenerationMigrationStartAllowed(); err != nil {
		return err
	}

	generation, err := store.BeginCellGeneration(s.currentStatePersistContext(), persistent)
	if err != nil {
		return fmt.Errorf("persist cell generation migration intent: %w", err)
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Str("persistent_state", storage.FormatBlockRef(persistent)).
		Str("source", source).
		Msg("cell generation migration queued")
	s.wakeServiceMaintenance()
	return nil
}

func (s *Service) startCellGenerationMigrationWithLease(store cellGenerationRotationStore, persistent ton.BlockIDExt, source string, migrationLease *exclusiveServiceTaskLease) error {
	runCtx, run, err := s.beginCellGenerationMigrationRun(s.currentStatePersistContext())
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
		err := s.runCellGenerationMigration(runCtx, store, persistent)
		s.finishCellGenerationMigrationRun(run)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				s.log.Info().
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

func (s *Service) shouldStartCellGenerationMigration(ctx context.Context, store cellGenerationRotationStore, persistent ton.BlockIDExt) (bool, error) {
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

	persistentMeta, err := s.storage.BlockMeta(ctx, persistent)
	if err != nil {
		return false, fmt.Errorf("load persistent state block meta %s: %w", storage.FormatBlockRef(persistent), err)
	}
	if persistentMeta.GenUTime == 0 {
		return false, fmt.Errorf("persistent state block %s has no gen utime", storage.FormatBlockRef(persistent))
	}

	if !emptyBlockID(active.OriginPersistentState) {
		originMeta, err := s.storage.BlockMeta(ctx, active.OriginPersistentState)
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

func (s *Service) beginCellGenerationMigration(ctx context.Context) (*exclusiveServiceTaskLease, error) {
	return s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskCellGenerationMigration)
}

func (s *Service) beginPendingCellGenerationMigration(ctx context.Context) (*exclusiveServiceTaskLease, error) {
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
	return &exclusiveServiceTaskLease{service: s, task: exclusiveServiceTaskCellGenerationMigration}, nil
}

func (s *Service) cellGenerationMigrationActive() bool {
	return s.exclusiveServiceTaskActive(exclusiveServiceTaskCellGenerationMigration)
}

func (s *Service) StopCellGenerationMigration(ctx context.Context) error {
	store, ok := s.storage.(cellGenerationRotationStore)
	if !ok {
		return fmt.Errorf("storage does not support cell generation migration")
	}

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

func (s *Service) pendingCellGenerationCompactionWait(ctx context.Context, store cellGenerationRotationStore, pending storage.CellGenerationInfo) (storage.CellGenerationDBMetrics, bool, error) {
	return s.cellGenerationCompactionWait(ctx, store, pending.ID)
}

func (s *Service) cellGenerationCompactionWait(ctx context.Context, store cellGenerationRotationStore, generation uint64) (storage.CellGenerationDBMetrics, bool, error) {
	metrics, err := store.CellGenerationDBMetrics(ctx, generation)
	if err != nil {
		return storage.CellGenerationDBMetrics{}, false, fmt.Errorf("load cell generation db metrics: %w", err)
	}
	return metrics, metrics.MaxReadAmp > CellGenerationSwitchMaxReadAmp, nil
}

func (s *Service) checkCellGenerationMigrationStartAllowed() error {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	if s.cellGenerationMigrationStopping {
		return errCellGenerationMigrationStopping
	}
	return nil
}

func (s *Service) beginCellGenerationMigrationRun(ctx context.Context) (context.Context, *cellGenerationMigrationRun, error) {
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

func (s *Service) finishCellGenerationMigrationRun(run *cellGenerationMigrationRun) {
	s.cellMigrationMu.Lock()
	if s.cellGenerationMigrationRun == run {
		s.cellGenerationMigrationRun = nil
	}
	s.cellMigrationMu.Unlock()
	close(run.done)
}

func (s *Service) beginCellGenerationMigrationStop() (*cellGenerationMigrationRun, error) {
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

func (s *Service) finishCellGenerationMigrationStop() {
	s.cellMigrationMu.Lock()
	s.cellGenerationMigrationStopping = false
	s.cellGenerationSwitchRequested = false
	s.cellGenerationNextBlockYieldAt = time.Time{}
	s.cellMigrationMu.Unlock()
	s.wakeServiceMaintenance()
}

func (s *Service) requestCellGenerationSwitch(generation uint64, target ton.BlockIDExt) {
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

	select {
	case s.currentStateWake <- struct{}{}:
	default:
	}
}

func (s *Service) clearCellGenerationSwitchRequest() {
	s.cellMigrationMu.Lock()
	s.cellGenerationSwitchRequested = false
	s.cellGenerationNextBlockYieldAt = time.Time{}
	s.cellMigrationMu.Unlock()
}

func (s *Service) cellGenerationSwitchRequestActive() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	return s.cellGenerationSwitchRequested
}

func (s *Service) shouldYieldNextBlockForCellGenerationSwitch(now time.Time) bool {
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

func (s *Service) beginCellGenerationSwitch() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()

	if s.cellGenerationSwitching {
		return false
	}
	s.cellGenerationSwitching = true
	return true
}

func (s *Service) finishCellGenerationSwitch() {
	s.cellMigrationMu.Lock()
	s.cellGenerationSwitchRequested = false
	s.cellGenerationNextBlockYieldAt = time.Time{}
	s.cellGenerationSwitching = false
	s.cellMigrationMu.Unlock()
}

func (s *Service) cellGenerationSwitchActive() bool {
	s.cellMigrationMu.Lock()
	defer s.cellMigrationMu.Unlock()
	return s.cellGenerationSwitching
}

func (s *Service) checkCurrentStatePersistAllowed() error {
	if err := s.takeCurrentStatePersistError(); err != nil {
		return err
	}
	if s.cellGenerationSwitchActive() {
		return errCellGenerationMigrationRunning
	}
	return nil
}

func (s *Service) runCellGenerationMigration(ctx context.Context, store cellGenerationRotationStore, origin ton.BlockIDExt) (err error) {
	if err := s.waitCurrentStatePersist(ctx); err != nil {
		return err
	}

	switchRequested := false
	defer func() {
		if switchRequested {
			s.clearCellGenerationSwitchRequest()
		}
	}()

	generation, err := store.BeginCellGeneration(ctx, origin)
	if err != nil {
		return fmt.Errorf("begin candidate cell generation: %w", err)
	}
	abort := true
	defer func() {
		if !abort {
			return
		}
		if ctxErr := ctx.Err(); errors.Is(err, context.Canceled) || ctxErr != nil {
			if ctxErr != nil && !errors.Is(err, ctxErr) {
				err = errors.Join(err, ctxErr)
			}
			s.log.Info().
				Uint64("cell_generation", generation).
				Str("persistent_state", storage.FormatBlockRef(origin)).
				Msg("leaving pending cell generation migration for startup resume")
			return
		}
		if err := store.AbortCellGeneration(context.Background(), generation); err != nil {
			s.log.Warn().
				Err(err).
				Uint64("cell_generation", generation).
				Msg("failed to remove aborted cell generation")
		}
	}()

	candidate, err := s.loadCellGenerationMigrationProgress(ctx, store, generation)
	if err == nil {
		s.log.Info().
			Uint64("cell_generation", generation).
			Str("masterchain", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
			Uint32("shard_client_seqno", candidate.current.ShardClientSeqno).
			Int("shards", len(candidate.current.Shards)).
			Msg("resumed cell generation migration from flushed progress")
	} else {
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		candidate, err = s.importSerializedPersistentCurrent(ctx, store, generation, origin)
		if err != nil {
			return err
		}
	}

	for {
		target, err := s.storage.CurrentState(ctx)
		if err != nil {
			return fmt.Errorf("load durable current state: %w", err)
		}
		if target.Masterchain.Block.SeqNo < candidate.current.Masterchain.Block.SeqNo {
			return fmt.Errorf("durable current state %s is before migration persistent state %s", storage.FormatBlockRef(target.Masterchain.Block), storage.FormatBlockRef(candidate.current.Masterchain.Block))
		}

		if err = s.catchUpAndFlushCellGenerationCandidate(ctx, store, candidate, target); err != nil {
			return err
		}

		metrics, waitingForCompaction, err := s.cellGenerationCompactionWait(ctx, store, candidate.generation)
		if err != nil {
			return err
		}
		if waitingForCompaction {
			s.log.Info().
				Uint64("cell_generation", candidate.generation).
				Str("candidate", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
				Int64("max_read_amp", metrics.MaxReadAmp).
				Int64("read_amp_limit", CellGenerationSwitchMaxReadAmp).
				Int64("l0_files", metrics.L0Files).
				Int64("l0_sublevels", metrics.L0Sublevels).
				Int64("l0_size", metrics.L0Size).
				Uint64("compaction_debt", metrics.CompactionDebt).
				Int64("compactions_in_progress", metrics.CompactionsInProgress).
				Int64("compaction_in_progress_bytes", metrics.CompactionInProgressSize).
				Msg("waiting for cell generation compaction before switch")
			if err = waitRetry(ctx, stateSerializationRetryDelay); err != nil {
				return err
			}
			continue
		}

		s.requestCellGenerationSwitch(candidate.generation, candidate.current.Masterchain.Block)
		switchRequested = true

		for {
			switched, latest, oldGeneration, err := s.trySwitchCellGenerationCandidate(ctx, store, candidate, origin)
			if err != nil {
				return err
			}
			if switched {
				switchRequested = false
				abort = false
				if oldGeneration != 0 {
					if err := store.CleanupCellGeneration(ctx, oldGeneration); err != nil {
						return fmt.Errorf("cleanup old cell generation %d: %w", oldGeneration, err)
					}
					s.log.Info().
						Uint64("old_cell_generation", oldGeneration).
						Msg("old cell generation cleaned up")
				}
				return nil
			}
			if latest == nil {
				return fmt.Errorf("cell generation switch did not return latest current state")
			}

			s.log.Info().
				Uint64("cell_generation", generation).
				Str("candidate", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
				Str("latest", storage.FormatBlockRef(latest.Masterchain.Block)).
				Msg("durable current state advanced during cell generation switch, finishing catch-up with switch requested")

			if err = s.catchUpAndFlushCellGenerationCandidate(ctx, store, candidate, latest); err != nil {
				return err
			}
		}
	}
}

func (s *Service) catchUpAndFlushCellGenerationCandidate(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate, target *storage.CurrentState) error {
	release, err := s.throttleActiveCellGenerationCompactions(ctx, store, candidate, target)
	if err != nil {
		return err
	}
	defer release()

	if err := s.catchUpCellGenerationCandidate(ctx, store, candidate, target); err != nil {
		return err
	}
	if err := s.flushCellGenerationCandidate(ctx, store, candidate); err != nil {
		return err
	}
	if err := store.SaveCellGenerationMigrationProgress(ctx, candidate.generation, candidate.current); err != nil {
		return fmt.Errorf("save cell generation migration progress: %w", err)
	}
	return nil
}

func (s *Service) throttleActiveCellGenerationCompactions(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate, target *storage.CurrentState) (func(), error) {
	if candidate == nil || candidate.current == nil || target == nil || target.Masterchain.Block.SeqNo <= candidate.current.Masterchain.Block.SeqNo {
		return func() {}, nil
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		return nil, fmt.Errorf("load active cell generation before migration catch-up: %w", err)
	}
	if active.ID == candidate.generation {
		return func() {}, nil
	}

	release, err := store.ThrottleCellGenerationCompactions(ctx, active.ID)
	if err != nil {
		return nil, fmt.Errorf("throttle active cell generation %d compactions: %w", active.ID, err)
	}

	s.log.Info().
		Uint64("active_cell_generation", active.ID).
		Uint64("candidate_cell_generation", candidate.generation).
		Str("from", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(target.Masterchain.Block)).
		Msg("throttled active cell generation compactions during candidate catch-up")
	return release, nil
}

func (s *Service) importSerializedPersistentCurrent(ctx context.Context, store cellGenerationRotationStore, generation uint64, master ton.BlockIDExt) (*cellGenerationCandidate, error) {
	masterState, err := s.storage.BlockState(ctx, master)
	if err != nil {
		return nil, fmt.Errorf("load serialized masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}

	importer := generationStateCellImporter{store: store, generation: generation}
	importedMaster, err := s.importSerializedPersistentBlockState(ctx, store, importer, generation, masterState.Block, master, 0, masterState.StateRootHash)
	if err != nil {
		return nil, fmt.Errorf("import serialized masterchain persistent state: %w", err)
	}

	shards, err := state2.ShardBlocksFromMasterState(importedMaster)
	if err != nil {
		return nil, fmt.Errorf("load shard list from imported persistent state %s: %w", storage.FormatBlockRef(master), err)
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: importedMaster.Block.SeqNo,
		Masterchain:      *storage.CloneBlockState(importedMaster),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(shards)),
	}
	splitDepths, err := state2.PersistentStateSplitDepths(importedMaster, shards)
	if err != nil {
		return nil, fmt.Errorf("load persistent state split depths for imported persistent state %s: %w", storage.FormatBlockRef(master), err)
	}

	for _, shard := range shards {
		canonicalShard, err := s.storage.BlockState(ctx, shard)
		if err != nil {
			return nil, fmt.Errorf("load serialized shard state metadata %s: %w", storage.FormatBlockRef(shard), err)
		}
		importedShard, err := s.importSerializedPersistentBlockState(ctx, store, importer, generation, shard, master, splitDepths[shard.Workchain], canonicalShard.StateRootHash)
		if err != nil {
			return nil, fmt.Errorf("import serialized shard persistent state %s: %w", storage.FormatBlockRef(shard), err)
		}
		current.Shards[storage.ShardKeyFromBlock(shard)] = *storage.CloneBlockState(importedShard)
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Int("shards", len(current.Shards)).
		Msg("imported persistent state into next celldb generation")

	if err := store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
		return nil, fmt.Errorf("save initial cell generation migration progress: %w", err)
	}

	candidate := &cellGenerationCandidate{
		generation: generation,
		current:    current,
		cells:      newStateCellWindowCache(store.LazyCellLoaderInGeneration(generation)),
	}
	return candidate, nil
}

func (s *Service) loadCellGenerationMigrationProgress(ctx context.Context, store cellGenerationRotationStore, generation uint64) (*cellGenerationCandidate, error) {
	current, err := store.CellGenerationMigrationProgress(ctx, generation)
	if err != nil {
		return nil, err
	}
	if err = s.verifyCellGenerationMigrationProgress(ctx, store, generation, current); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Progress is only useful when every recorded current-state root is
			// present in the candidate generation. If the process stopped before
			// the progress sync became durable, rebuilding from the origin state is
			// slower but preserves correctness.
			return nil, storage.ErrNotFound
		}
		return nil, err
	}

	return &cellGenerationCandidate{
		generation: generation,
		current:    current,
		cells:      newStateCellWindowCache(store.LazyCellLoaderInGeneration(generation)),
	}, nil
}

func (s *Service) verifyCellGenerationMigrationProgress(ctx context.Context, store cellGenerationRotationStore, generation uint64, current *storage.CurrentState) error {
	if current == nil {
		return fmt.Errorf("cell generation migration progress is nil")
	}
	if err := s.verifyCellGenerationMigrationProgressBlock(ctx, store, generation, current.Masterchain); err != nil {
		return fmt.Errorf("verify migration progress masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		if err := s.verifyCellGenerationMigrationProgressBlock(ctx, store, generation, shard); err != nil {
			return fmt.Errorf("verify migration progress shard state %s: %w", storage.FormatBlockRef(shard.Block), err)
		}
	}
	return nil
}

func (s *Service) verifyCellGenerationMigrationProgressBlock(ctx context.Context, store cellGenerationRotationStore, generation uint64, state storage.BlockState) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(state.StateCellHash) != 32 {
		return fmt.Errorf("state cell hash size is %d", len(state.StateCellHash))
	}

	var hash cell.Hash
	copy(hash[:], state.StateCellHash)
	root, err := store.LazyCellLoaderInGeneration(generation)(hash)
	if err != nil {
		return err
	}
	if len(state.StateRootHash) == 0 {
		return nil
	}
	rootHash := root.HashKey(0)
	if !bytes.Equal(rootHash[:], state.StateRootHash) {
		return fmt.Errorf("state root hash mismatch")
	}
	return nil
}

func (s *Service) importSerializedPersistentBlockState(
	ctx context.Context,
	store cellGenerationRotationStore,
	importer generationStateCellImporter,
	generation uint64,
	block ton.BlockIDExt,
	master ton.BlockIDExt,
	splitDepth uint32,
	stateRootHash []byte,
) (*storage.BlockState, error) {
	imported, err := s.loadMarkedCellGenerationState(ctx, store, generation, block)
	if err == nil {
		s.log.Info().
			Uint64("cell_generation", generation).
			Str("block", storage.FormatBlockRef(block)).
			Msg("reusing imported persistent state in next celldb generation")
		return imported, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	artifact, err := s.serializedPersistentStateArtifact(ctx, store, block, master, splitDepth, stateRootHash)
	if err != nil {
		return nil, err
	}
	state, err := artifact.ImportCells(ctx, importer)
	if err != nil {
		return nil, err
	}
	state.CellGeneration = generation
	if err := store.MarkCellGenerationStateImported(ctx, generation, block); err != nil {
		return nil, fmt.Errorf("mark persistent state imported %s: %w", storage.FormatBlockRef(block), err)
	}
	return state, nil
}

func (s *Service) loadMarkedCellGenerationState(ctx context.Context, store cellGenerationRotationStore, generation uint64, block ton.BlockIDExt) (*storage.BlockState, error) {
	if err := store.CellGenerationStateImported(ctx, generation, block); err != nil {
		return nil, err
	}

	state, err := s.storage.BlockState(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load imported persistent state metadata %s: %w", storage.FormatBlockRef(block), err)
	}
	if len(state.StateCellHash) != 32 {
		return nil, fmt.Errorf("imported persistent state %s has invalid cell hash size %d", storage.FormatBlockRef(block), len(state.StateCellHash))
	}

	var hash cell.Hash
	copy(hash[:], state.StateCellHash)
	root, err := store.LazyCellLoaderInGeneration(generation)(hash)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("load imported persistent state root %s from cell generation %d: %w", storage.FormatBlockRef(block), generation, err)
	}
	rootHash := root.HashKey(0)
	if len(state.StateRootHash) > 0 && !bytes.Equal(rootHash[:], state.StateRootHash) {
		return nil, fmt.Errorf("imported persistent state root hash mismatch %s", storage.FormatBlockRef(block))
	}
	rootCellHash := root.HashKey()
	if !bytes.Equal(rootCellHash[:], state.StateCellHash) {
		return nil, fmt.Errorf("imported persistent state cell hash mismatch %s", storage.FormatBlockRef(block))
	}

	state.Cell = root
	state.CellGeneration = generation
	return state, nil
}

func (s *Service) serializedPersistentStateArtifact(ctx context.Context, store cellGenerationRotationStore, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32, stateRootHash []byte) (storage.DownloadedState, error) {
	if block.Workchain == -1 || splitDepth <= uint32(state2.ShardPrefixLength(block.Shard)) {
		file, err := store.PersistentStateFile(ctx, block, master, 0)
		if err != nil {
			return nil, err
		}
		return p2p.NewPersistentStateSnapshotArtifactFromFile(s.node, file)
	}

	return p2p.NewSplitPersistentStateSnapshotArtifactFromStoredFiles(ctx, s.node, block, master, splitDepth, stateRootHash, func(effectiveShard int64) (*storage.PersistentStateFile, error) {
		return store.PersistentStateFile(ctx, block, master, effectiveShard)
	})
}

func (s *Service) catchUpCellGenerationCandidate(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate, target *storage.CurrentState) error {
	if target == nil {
		return fmt.Errorf("target current state is nil")
	}
	if target.Masterchain.Block.SeqNo < candidate.current.Masterchain.Block.SeqNo {
		return fmt.Errorf("durable current state %s is before candidate current state %s", storage.FormatBlockRef(target.Masterchain.Block), storage.FormatBlockRef(candidate.current.Masterchain.Block))
	}

	shardCache := map[string]*storage.BlockState{}
	resolver := s.newCellGenerationShardResolver(ctx, candidate, shardCache)
	processed := uint32(0)
	started := time.Now()
	startSeqno := candidate.current.Masterchain.Block.SeqNo
	lastLog := started
	lastLogProcessed := processed
	shardBlocksApplied := uint64(0)
	shardBlocksReused := uint64(0)

	if target.Masterchain.Block.SeqNo > startSeqno {
		s.log.Info().
			Uint64("cell_generation", candidate.generation).
			Str("from", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
			Str("target", storage.FormatBlockRef(target.Masterchain.Block)).
			Str("catchup_method", "cell_generation_migration").
			Uint32("total_masterchain_blocks", target.Masterchain.Block.SeqNo-startSeqno).
			Uint32("remaining", target.Masterchain.Block.SeqNo-startSeqno).
			Int("shards", len(candidate.current.Shards)).
			Msg("catching up next celldb generation from stored blocks")
	}

	for candidate.current.Masterchain.Block.SeqNo < target.Masterchain.Block.SeqNo {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		downloaded, err := s.loadCandidateNextMasterBlock(ctx, candidate.current.Masterchain.Block)
		if err != nil {
			return err
		}
		nextMaster, _, err := s.applyStoredMasterchainTransition(&candidate.current.Masterchain, downloaded, candidate.cells)
		if err != nil {
			return err
		}
		nextMaster.CellGeneration = candidate.generation

		targets, err := state2.ShardBlocksFromMasterState(nextMaster)
		if err != nil {
			return fmt.Errorf("load shard targets from candidate master state %s: %w", storage.FormatBlockRef(nextMaster.Block), err)
		}
		nextCurrent, shardStats, err := s.currentStateForNextMasterState(ctx, candidate.current, nextMaster, targets, resolver)
		if err != nil {
			return err
		}
		setCurrentCellGeneration(nextCurrent, candidate.generation)

		candidate.current = nextCurrent
		resolver.updateCurrent(candidate.current.Shards)
		shardBlocksApplied += uint64(shardStats.applied)
		shardBlocksReused += uint64(shardStats.reused)
		processed++

		now := time.Now()
		if now.Sub(lastLog) >= cellGenerationMigrationProgressInterval && candidate.current.Masterchain.Block.SeqNo < target.Masterchain.Block.SeqNo {
			s.logCellGenerationCandidateCatchUpProgress(
				candidate, target, startSeqno, processed, lastLogProcessed,
				started, lastLog, now, shardBlocksApplied, shardBlocksReused, false,
			)
			lastLog = now
			lastLogProcessed = processed
		}

		if processed%s.nextCheckpointBlocks() == 0 {
			if err = s.flushCellGenerationCandidate(ctx, store, candidate); err != nil {
				return err
			}
			if err = store.SaveCellGenerationMigrationProgress(ctx, candidate.generation, candidate.current); err != nil {
				return fmt.Errorf("save cell generation migration progress: %w", err)
			}
		}
	}

	if !candidate.current.Masterchain.Block.Equals(&target.Masterchain.Block) {
		return fmt.Errorf("candidate masterchain caught up to %s, durable target is %s", storage.FormatBlockRef(candidate.current.Masterchain.Block), storage.FormatBlockRef(target.Masterchain.Block))
	}
	if processed > 0 {
		now := time.Now()
		s.logCellGenerationCandidateCatchUpProgress(
			candidate, target, startSeqno, processed, lastLogProcessed,
			started, lastLog, now, shardBlocksApplied, shardBlocksReused, true,
		)
	}
	return nil
}

func (s *Service) logCellGenerationCandidateCatchUpProgress(
	candidate *cellGenerationCandidate,
	target *storage.CurrentState,
	startSeqno uint32,
	processed uint32,
	lastLogProcessed uint32,
	started time.Time,
	lastLog time.Time,
	now time.Time,
	shardBlocksApplied uint64,
	shardBlocksReused uint64,
	done bool,
) {
	total := target.Masterchain.Block.SeqNo - startSeqno
	remaining := target.Masterchain.Block.SeqNo - candidate.current.Masterchain.Block.SeqNo
	windowBlocks := processed - lastLogProcessed
	windowElapsed := now.Sub(lastLog)
	elapsed := now.Sub(started)

	event := s.log.Info().
		Uint64("cell_generation", candidate.generation).
		Str("current", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(target.Masterchain.Block)).
		Str("catchup_method", "cell_generation_migration").
		Uint32("processed_masterchain_blocks", processed).
		Uint32("total_masterchain_blocks", total).
		Uint32("remaining", remaining).
		Uint32("pending_checkpoint_blocks", processed%s.nextCheckpointBlocks()).
		Uint64("pending_checkpoint_bytes", candidate.cells.byteSize()).
		Uint64("applied_shard_blocks", shardBlocksApplied).
		Uint64("reused_shard_blocks", shardBlocksReused).
		Dur("elapsed", elapsed).
		Str("progress", formatCatchUpProgress(processed, total)).
		Str("speed", formatBlockRate(processed, elapsed)).
		Str("window_speed", formatBlockRate(windowBlocks, windowElapsed)).
		Str("eta", formatCatchUpETA(processed, total, elapsed))
	if done {
		event.Msg("cell generation migration catch-up finished")
		return
	}
	event.Msg("cell generation migration catch-up progress")
}

func (s *Service) newCellGenerationShardResolver(ctx context.Context, candidate *cellGenerationCandidate, cache map[string]*storage.BlockState) *shardStateResolver {
	return newShardStateResolver(ctx, shardStateResolverConfig{
		current: candidate.current.Shards,
		cache:   cache,
		loadState: func(_ context.Context, state storage.BlockState) (*storage.BlockState, error) {
			if state.Cell != nil {
				return storage.CloneBlockState(&state), nil
			}
			return nil, storage.ErrNotFound
		},
		loadBlock: s.loadCandidateBlockForApply,
		apply: func(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*storage.BlockState, error) {
			next, err := s.applyResolvedShardBlock(ctx, target, previous, downloaded, candidate.cells)
			if err != nil {
				return nil, err
			}
			next.CellGeneration = candidate.generation
			return next, nil
		},
		afterApplyState: func(_ context.Context, state *storage.BlockState, _ p2p.DownloadedBlock, _ time.Duration) error {
			return nil
		},
	})
}

func (s *Service) loadCandidateNextMasterBlock(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	next, err := s.storage.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, prev.SeqNo+1)
	if err != nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("load stored next masterchain block after %s: %w", storage.FormatBlockRef(prev), err)
	}
	return s.loadCandidateBlockForApply(ctx, next)
}

func (s *Service) loadCandidateBlockForApply(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	downloaded, err := loadStoredBlockForApply(ctx, s.storage, block, true)
	if err != nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("load stored candidate block %s: %w", storage.FormatBlockRef(block), err)
	}
	return downloaded, nil
}

func (s *Service) flushCellGenerationCandidate(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate) error {
	checkpoint := candidate.cells.beginCheckpoint()
	if checkpoint == nil {
		return nil
	}

	records := checkpoint.records()
	if len(records) > 0 {
		if err := store.SaveEncodedCellsInGeneration(ctx, candidate.generation, records, true); err != nil {
			return err
		}
	}
	checkpoint.complete()
	return nil
}

func (s *Service) trySwitchCellGenerationCandidate(ctx context.Context, store cellGenerationRotationStore, candidate *cellGenerationCandidate, origin ton.BlockIDExt) (bool, *storage.CurrentState, uint64, error) {
	s.stateMu.Lock()
	s.currentStatePersistMu.Lock()

	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, err
	}

	latest, err := s.storage.CurrentState(ctx)
	if err != nil {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, fmt.Errorf("load durable current state before cell generation switch: %w", err)
	}
	if latest.Masterchain.Block.SeqNo > candidate.current.Masterchain.Block.SeqNo {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, latest, 0, nil
	}
	if !latest.Masterchain.Block.Equals(&candidate.current.Masterchain.Block) {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, fmt.Errorf("candidate current state %s does not match durable current state %s", storage.FormatBlockRef(candidate.current.Masterchain.Block), storage.FormatBlockRef(latest.Masterchain.Block))
	}
	if !s.beginCellGenerationSwitch() {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return false, nil, 0, errCellGenerationMigrationRunning
	}
	s.currentStatePersistMu.Unlock()
	s.stateMu.Unlock()
	defer s.finishCellGenerationSwitch()

	oldGeneration, err := store.SwitchCellGeneration(ctx, candidate.generation, origin, candidate.current.Masterchain.Block, currentStateWithoutCells(candidate.current))
	if err != nil {
		if errors.Is(err, storage.ErrCurrentStateAdvanced) {
			latest, loadErr := s.storage.CurrentState(ctx)
			if loadErr != nil {
				return false, nil, 0, fmt.Errorf("load durable current state after rejected cell generation switch: %w", loadErr)
			}
			return false, latest, 0, nil
		}
		return false, nil, 0, err
	}
	s.publishCommittedCurrentState(candidate.current)
	return true, latest, oldGeneration, nil
}

func setCurrentCellGeneration(current *storage.CurrentState, generation uint64) {
	if current == nil {
		return
	}
	current.Masterchain.CellGeneration = generation
	for key, shard := range current.Shards {
		shard.CellGeneration = generation
		current.Shards[key] = shard
	}
}

func emptyBlockID(block ton.BlockIDExt) bool {
	return block.Workchain == 0 &&
		block.Shard == 0 &&
		block.SeqNo == 0 &&
		zeroHash(block.RootHash) &&
		zeroHash(block.FileHash)
}

func zeroHash(hash []byte) bool {
	for _, b := range hash {
		if b != 0 {
			return false
		}
	}
	return true
}
