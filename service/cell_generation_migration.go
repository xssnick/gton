package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type cellGenerationCandidate struct {
	generation      uint64
	generationCells storage.CellGeneration
	loader          cell.LazyCellLoader
	current         *storage.CurrentState
	cells           *stateCellWindowCache
}

func (s *StateLifecycle) runCellGenerationMigration(ctx context.Context, origin ton.BlockIDExt) (err error) {
	store := s.generationStore
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
	switched := false
	defer func() {
		if err == nil || switched {
			return
		}

		ctxErr := ctx.Err()
		if ctxErr != nil && !errors.Is(err, ctxErr) {
			err = errors.Join(err, ctxErr)
		}
		if errors.Is(context.Cause(ctx), errCellGenerationMigrationStopped) {
			s.log.Info().
				Err(err).
				Uint64("cell_generation", generation).
				Str("persistent_state", storage.FormatBlockRef(origin)).
				Msg("cell generation migration interrupted by stop")
			return
		}

		event := s.log.Warn()
		message := "leaving pending cell generation migration for retry"
		if errors.Is(err, context.Canceled) || ctxErr != nil {
			event = s.log.Info()
			message = "leaving pending cell generation migration for startup resume"
		}
		if errors.Is(err, errCellGenerationPersistentMissing) {
			dropErr := store.DropPendingCellGeneration(s.shutdownContext, generation)
			event = s.log.Error()
			message = "aborting pending cell generation migration because serialized persistent state is missing"
			if dropErr != nil {
				err = errors.Join(err, dropErr)
				event = event.AnErr("drop_error", dropErr)
			}
			err = errors.Join(errCellGenerationMigrationAborted, err)
		}
		event.Err(err).
			Uint64("cell_generation", generation).
			Str("persistent_state", storage.FormatBlockRef(origin)).
			Msg(message)
	}()

	candidate, err := s.loadCellGenerationMigrationProgress(ctx, store, generation)
	if err == nil {
		s.log.Info().
			Uint64("cell_generation", generation).
			Str("masterchain", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
			Uint32("shard_client_seqno", candidate.current.ShardClientSeqno).
			Int("shards", len(candidate.current.Shards)).
			Msg("resumed cell generation migration from flushed progress")
		if candidate.current.Masterchain.Block.Equals(&origin) {
			complete, err := s.cellGenerationPersistentImportComplete(candidate)
			if err != nil {
				return err
			}
			if !complete {
				candidate, err = s.importSerializedPersistentCurrent(ctx, store, generation, origin, candidate)
				if err != nil {
					return err
				}
			}
		}
	} else {
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		candidate, err = s.importSerializedPersistentCurrent(ctx, store, generation, origin, nil)
		if err != nil {
			return err
		}
	}

	for {
		target, err := store.CurrentState(ctx)
		if err != nil {
			return fmt.Errorf("load durable current state: %w", err)
		}
		if target.Masterchain.Block.SeqNo < candidate.current.Masterchain.Block.SeqNo {
			return fmt.Errorf("durable current state %s is before migration persistent state %s", storage.FormatBlockRef(target.Masterchain.Block), storage.FormatBlockRef(candidate.current.Masterchain.Block))
		}

		if err = s.catchUpAndFlushCellGenerationCandidate(ctx, store, candidate, target); err != nil {
			return err
		}

		metrics, waitingForCompaction, err := s.cellGenerationCompactionWait(ctx, candidate.generation)
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
		if err = s.prepareCellGenerationSwitchCandidate(ctx, store, candidate, origin); err != nil {
			return err
		}

		for {
			oldGeneration, err := s.trySwitchCellGenerationCandidate(ctx, store, candidate, origin)
			if err == nil {
				switchRequested = false
				switched = true
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
			if !errors.Is(err, storage.ErrCurrentStateAdvanced) {
				return err
			}

			latest, err := store.CurrentState(ctx)
			if err != nil {
				return fmt.Errorf("load durable current state after rejected cell generation switch: %w", err)
			}

			s.log.Info().
				Uint64("cell_generation", generation).
				Str("candidate", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
				Str("latest", storage.FormatBlockRef(latest.Masterchain.Block)).
				Msg("durable current state advanced during cell generation switch, finishing catch-up with switch requested")

			if err = s.catchUpAndFlushCellGenerationCandidate(ctx, store, candidate, latest); err != nil {
				return err
			}
			if err = s.prepareCellGenerationSwitchCandidate(ctx, store, candidate, origin); err != nil {
				return err
			}
		}
	}
}

func (s *StateLifecycle) throttleActiveCellGenerationCompactions(ctx context.Context, store cellGenerationStore, candidate *cellGenerationCandidate, target *storage.CurrentState) (func(), error) {
	if target.Masterchain.Block.SeqNo <= candidate.current.Masterchain.Block.SeqNo {
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
