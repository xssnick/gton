package service

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/service/storage"
)

type stateCheckpointData struct {
	live      *storage.CurrentState
	persisted *storage.CurrentState
	entries   []storage.StateCheckpointBlock
	cells     storage.StateCellRecords
}

func prepareStateCheckpoint(current *storage.CurrentState, entries []storage.StateCheckpointBlock, cells storage.StateCellRecords) (stateCheckpointData, error) {
	appliedEntries := cloneStateCheckpointEntries(entries)
	if len(appliedEntries) == 0 {
		return stateCheckpointData{}, fmt.Errorf("state checkpoint has no applied block states")
	}
	return stateCheckpointData{
		live:      storage.CloneCurrentState(current),
		persisted: currentStateWithoutCells(current),
		entries:   appliedEntries,
		cells:     cells,
	}, nil
}

func (s *Service) saveStateCheckpoint(ctx context.Context, current *storage.CurrentState, entries []storage.StateCheckpointBlock, cells storage.StateCellRecords, artifactTarget uint64) (*storage.CurrentState, error) {
	if err := s.waitAppliedBlockArtifacts(ctx, artifactTarget); err != nil {
		return nil, err
	}
	if err := s.storage.SaveStateCheckpointEntries(ctx, entries, cells, current); err != nil {
		return nil, err
	}
	return currentStateWithSavedBlockStates(current, checkpointEntryStates(entries)), nil
}

func currentStateWithSavedBlockStates(current *storage.CurrentState, states []*storage.BlockState) *storage.CurrentState {
	next := storage.CloneCurrentState(current)
	byBlock := make(map[string]*storage.BlockState, len(states))
	for _, state := range states {
		if state != nil {
			byBlock[storage.BlockKey(state.Block)] = state
		}
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

func cloneStateCheckpointEntries(entries []storage.StateCheckpointBlock) []storage.StateCheckpointBlock {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]storage.StateCheckpointBlock, 0, len(entries))
	for _, entry := range entries {
		if entry.State == nil {
			continue
		}
		cloned = append(cloned, storage.StateCheckpointBlock{
			State: storage.CloneBlockState(entry.State),
		})
	}
	return cloned
}

func checkpointEntryStates(entries []storage.StateCheckpointBlock) []*storage.BlockState {
	if len(entries) == 0 {
		return nil
	}
	states := make([]*storage.BlockState, 0, len(entries))
	for _, entry := range entries {
		if entry.State == nil {
			continue
		}
		states = append(states, storage.CloneBlockState(entry.State))
	}
	return states
}

func currentStateWithoutCells(current *storage.CurrentState) *storage.CurrentState {
	if current == nil {
		return nil
	}

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
