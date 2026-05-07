package service

import (
	"context"
	"fmt"

	"flexserver/service/storage"
)

type stateCheckpointData struct {
	live      *storage.CurrentState
	persisted *storage.CurrentState
	states    []*storage.BlockState
}

func prepareStateCheckpoint(current *storage.CurrentState, states []*storage.BlockState) (stateCheckpointData, error) {
	if current == nil {
		return stateCheckpointData{}, fmt.Errorf("current state is nil")
	}

	appliedStates := cloneBlockStateSlice(states)
	if len(appliedStates) == 0 {
		appliedStates = currentBlockStates(current)
	}
	return stateCheckpointData{
		live:      storage.CloneCurrentState(current),
		persisted: currentStateWithoutCells(current),
		states:    appliedStates,
	}, nil
}

func (s *Service) saveStateCheckpoint(ctx context.Context, current *storage.CurrentState, states []*storage.BlockState, artifactTarget uint64, cells []storage.EncodedCellRecord) (*storage.CurrentState, error) {
	if err := s.waitAppliedBlockArtifacts(ctx, artifactTarget); err != nil {
		return nil, err
	}
	if err := s.storage.SaveStateCheckpointWithCells(ctx, states, current, cells); err != nil {
		return nil, err
	}
	return currentStateWithSavedBlockStates(current, states), nil
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

func currentBlockStates(current *storage.CurrentState) []*storage.BlockState {
	if current == nil {
		return nil
	}

	states := make([]*storage.BlockState, 0, 1+len(current.Shards))
	states = append(states, storage.CloneBlockState(&current.Masterchain))
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		states = append(states, storage.CloneBlockState(&shard))
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
