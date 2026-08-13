package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *StateLifecycle) cellGenerationSwitchPreserveStateMeta(ctx context.Context, store cellGenerationStore, generation uint64, origin ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	originState, err := s.persistentImportStateMetadata(ctx, store, origin)
	if err != nil {
		return nil, fmt.Errorf("load origin persistent state metadata %s before switch: %w", storage.FormatBlockRef(origin), err)
	}

	cells, err := store.Cells(generation)
	if err != nil {
		return nil, fmt.Errorf("select candidate cell generation %d before switch: %w", generation, err)
	}
	root, err := s.loadCellGenerationMigrationProgressBlock(ctx, cells.Loader(), *originState)
	if err != nil {
		return nil, fmt.Errorf("load origin persistent state root %s from candidate generation before switch: %w", storage.FormatBlockRef(origin), err)
	}
	originState.Cell = root

	parsedOrigin, err := parsedCellGenerationProgressState(originState)
	if err != nil {
		return nil, fmt.Errorf("parse origin persistent state %s before switch: %w", storage.FormatBlockRef(origin), err)
	}

	shards, err := state2.ShardBlocksFromMasterState(parsedOrigin)
	if err != nil {
		return nil, fmt.Errorf("load origin persistent shard list %s before switch: %w", storage.FormatBlockRef(origin), err)
	}
	return shards, nil
}

func (s *StateLifecycle) prepareCellGenerationSwitchCandidate(ctx context.Context, store cellGenerationStore, candidate *cellGenerationCandidate, origin ton.BlockIDExt) error {
	preserveStateMeta, err := s.cellGenerationSwitchPreserveStateMeta(ctx, store, candidate.generation, origin)
	if err != nil {
		return err
	}

	latest, err := store.CurrentState(ctx)
	if err != nil {
		return fmt.Errorf("load durable current state before cell generation switch metadata cleanup: %w", err)
	}
	if latest.Masterchain.Block.SeqNo > candidate.current.Masterchain.Block.SeqNo ||
		!latest.BlockRefsEqual(candidate.current) ||
		!latest.RootsEqual(candidate.current) {
		s.log.Info().
			Uint64("cell_generation", candidate.generation).
			Str("candidate", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
			Str("latest", storage.FormatBlockRef(latest.Masterchain.Block)).
			Msg("skipping state metadata cleanup because durable current advanced before cell generation switch")
		return nil
	}

	started := time.Now()
	deleted, err := store.DeleteStateMetadataBeforeCellGenerationSwitch(ctx, origin, latest, preserveStateMeta)
	if err != nil {
		return fmt.Errorf("delete state metadata before cell generation switch: %w", err)
	}
	s.log.Info().
		Uint64("cell_generation", candidate.generation).
		Str("origin_persistent_state", storage.FormatBlockRef(origin)).
		Str("masterchain", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
		Int("deleted_state_metadata_records", deleted).
		Dur("elapsed", time.Since(started)).
		Msg("prepared state metadata for cell generation switch")
	return nil
}

func (s *StateLifecycle) trySwitchCellGenerationCandidate(ctx context.Context, store cellGenerationStore, candidate *cellGenerationCandidate, origin ton.BlockIDExt) (uint64, error) {
	s.stateMu.Lock()
	s.currentStatePersistMu.Lock()

	if err := s.checkCurrentStatePersistAllowed(); err != nil {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return 0, err
	}

	latest, err := store.CurrentState(ctx)
	if err != nil {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return 0, fmt.Errorf("load durable current state before cell generation switch: %w", err)
	}
	if latest.Masterchain.Block.SeqNo > candidate.current.Masterchain.Block.SeqNo {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return 0, storage.ErrCurrentStateAdvanced
	}
	if !latest.BlockRefsEqual(candidate.current) {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return 0, fmt.Errorf("candidate current state %s does not match durable current state %s", storage.FormatBlockRef(candidate.current.Masterchain.Block), storage.FormatBlockRef(latest.Masterchain.Block))
	}
	if !latest.RootsEqual(candidate.current) {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return 0, fmt.Errorf("candidate current state roots do not match durable current state roots at %s", storage.FormatBlockRef(latest.Masterchain.Block))
	}
	if !s.beginCellGenerationSwitch() {
		s.currentStatePersistMu.Unlock()
		s.stateMu.Unlock()
		return 0, errCellGenerationMigrationRunning
	}
	s.currentStatePersistMu.Unlock()
	s.stateMu.Unlock()
	defer s.finishCellGenerationSwitch()

	published, err := loadActiveCurrentState(ctx, store, latest)
	if err != nil {
		return 0, fmt.Errorf("load active current state before cell generation switch: %w", err)
	}

	switchStarted := time.Now()
	s.log.Info().
		Uint64("cell_generation", candidate.generation).
		Str("masterchain", storage.FormatBlockRef(candidate.current.Masterchain.Block)).
		Uint32("shard_client_seqno", candidate.current.ShardClientSeqno).
		Msg("switching cell generation")
	oldGeneration, err := store.SwitchCellGeneration(ctx, candidate.generation, origin, latest.Masterchain.Block, currentStateWithoutCells(latest))
	if err != nil {
		if errors.Is(err, storage.ErrCurrentStateAdvanced) {
			return 0, storage.ErrCurrentStateAdvanced
		}
		return 0, err
	}
	s.checkpointTransitions.publishCommittedCurrentState(published)
	s.log.Info().
		Uint64("cell_generation", candidate.generation).
		Uint64("old_cell_generation", oldGeneration).
		Dur("elapsed", time.Since(switchStarted)).
		Msg("cell generation switch completed")
	return oldGeneration, nil
}

func loadActiveCurrentState(ctx context.Context, store cellGenerationStore, current *storage.CurrentState) (*storage.CurrentState, error) {
	master, err := store.BlockState(ctx, current.Masterchain.Block)
	if err != nil {
		return nil, fmt.Errorf("load masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

	loaded := &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: current.ShardClientSeqno,
		Masterchain:      *master,
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(current.Shards)),
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		state, err := store.BlockState(ctx, shard.Block)
		if err != nil {
			return nil, fmt.Errorf("load shard state %s: %w", storage.FormatBlockRef(shard.Block), err)
		}
		loaded.Shards[key] = *state
	}
	return loaded, nil
}
