package service

import (
	"context"
	"time"

	"github.com/xssnick/gton/service/storage"
)

type stateCheckpointData struct {
	live                   *storage.CurrentState
	persisted              *storage.CurrentState
	entries                []storage.StateCheckpointBlock
	cells                  storage.StateCellRecords
	cellPrewriteTarget     uint64
	artifactPrewriteTarget uint64
}

func prepareStateCheckpoint(current *storage.CurrentState, entries []storage.StateCheckpointBlock, cells *stateCellCheckpointCache) stateCheckpointData {
	// entries always come from appliedStateSet.checkpoint(), which already
	// produced an independent snapshot (deep-cloned block states, shared immutable
	// payload bytes) decoupled from the live set. That snapshot is single-use and
	// is not read again after the persist, so re-cloning it here would just be a
	// second full clone of ~400 block states per checkpoint. The archive path
	// (persistArchiveCurrentState) already hands the same checkpoint() entries
	// straight to saveStateCheckpoint without re-cloning; mirror that.
	appliedEntries := entries
	checkpointCells := storage.StateCellRecords{}
	cellPrewriteTarget, prewritten := cells.prewriteTarget()
	if !prewritten {
		checkpointCells = cells.cells()
	}
	return stateCheckpointData{
		live:               storage.CloneCurrentState(current),
		persisted:          currentStateWithoutCells(current),
		entries:            appliedEntries,
		cells:              checkpointCells,
		cellPrewriteTarget: cellPrewriteTarget,
	}
}

func (s *StateLifecycle) saveStateCheckpoint(ctx context.Context, current *storage.CurrentState, entries []storage.StateCheckpointBlock, cells storage.StateCellRecords, cellPrewriteTarget uint64, artifactPrewriteTarget uint64) (*storage.CurrentState, []SyncPersistStageObservation, error) {
	var stages []SyncPersistStageObservation

	if cellPrewriteTarget > 0 {
		stageStarted := time.Now()
		if err := s.stateCellPrewrite.wait(ctx, cellPrewriteTarget); err != nil {
			stages = appendSyncPersistStage(stages, "wait_cell_prewrite", time.Since(stageStarted))
			return nil, stages, err
		}
		stages = appendSyncPersistStage(stages, "wait_cell_prewrite", time.Since(stageStarted))

		stageStarted = time.Now()
		if err := s.checkpointStore.FlushStateCells(ctx); err != nil {
			stages = appendSyncPersistStage(stages, "flush_prewrite_cells", time.Since(stageStarted))
			return nil, stages, err
		}
		stages = appendSyncPersistStage(stages, "flush_prewrite_cells", time.Since(stageStarted))
	}

	// Waits after the cell flush on purpose: artifact appends keep streaming in
	// the background while the flush runs, so this wait is usually near-zero.
	if artifactPrewriteTarget > 0 {
		stageStarted := time.Now()
		if err := s.artifactPrewrite.wait(ctx, artifactPrewriteTarget); err != nil {
			stages = appendSyncPersistStage(stages, "wait_artifact_prewrite", time.Since(stageStarted))
			return nil, stages, err
		}
		stages = appendSyncPersistStage(stages, "wait_artifact_prewrite", time.Since(stageStarted))
	}

	timing, err := s.checkpointStore.SaveStateCheckpointEntries(ctx, entries, cells, current)
	stages = appendStateCheckpointTimingStages(stages, timing)
	if err != nil {
		return nil, stages, err
	}
	return currentStateWithSavedBlockStates(current, checkpointEntryStates(entries)), stages, nil
}

func appendStateCheckpointTimingStages(stages []SyncPersistStageObservation, timing storage.StateCheckpointTiming) []SyncPersistStageObservation {
	stages = appendSyncPersistStage(stages, "write_cells", timing.CellsWrite)
	stages = appendSyncPersistStage(stages, "flush_cells", timing.CellsFlush)
	stages = appendSyncPersistStage(stages, "sync_artifacts", timing.ArtifactSync)
	stages = appendSyncPersistStage(stages, "metadata_sync", timing.MetadataSync)
	return stages
}

func appendSyncPersistStage(stages []SyncPersistStageObservation, stage string, duration time.Duration) []SyncPersistStageObservation {
	if duration <= 0 {
		return stages
	}
	return append(stages, SyncPersistStageObservation{
		Stage:    stage,
		Duration: duration,
	})
}

func currentStateWithSavedBlockStates(current *storage.CurrentState, states []*storage.BlockState) *storage.CurrentState {
	next := storage.CloneCurrentState(current)
	byBlock := make(map[storage.BlockRootHash]*storage.BlockState, len(states))
	for _, state := range states {
		byBlock[storage.BlockKey(state.Block)] = state
	}

	if state := byBlock[storage.BlockKey(next.Masterchain.Block)]; state != nil {
		next.Masterchain = *storage.CloneBlockState(state)
	}
	for _, key := range storage.SortedShardKeys(next.Shards) {
		shard := next.Shards[key]
		if state := byBlock[storage.BlockKey(shard.Block)]; state != nil {
			next.Shards[key] = *storage.CloneBlockState(state)
		}
	}
	return next
}

func checkpointEntryStates(entries []storage.StateCheckpointBlock) []*storage.BlockState {
	if len(entries) == 0 {
		return nil
	}
	states := make([]*storage.BlockState, 0, len(entries))
	for _, entry := range entries {
		states = append(states, storage.CloneBlockState(entry.State))
	}
	return states
}

func currentStateWithoutCells(current *storage.CurrentState) *storage.CurrentState {
	next := &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: current.ShardClientSeqno,
		Masterchain:      storage.BlockStateWithoutCells(&current.Masterchain),
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(current.Shards)),
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		next.Shards[key] = storage.BlockStateWithoutCells(&shard)
	}
	return next
}
