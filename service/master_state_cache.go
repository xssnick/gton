package service

import (
	"context"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Service) rememberMasterState(state *storage.BlockState) {
	if state == nil || state.Block.Workchain != -1 || state.Block.Shard != topShard || state.Cell == nil {
		return
	}

	key := storage.BlockKey(state.Block)
	cloned := storage.CloneBlockState(state)

	s.masterStateCacheMu.Lock()
	defer s.masterStateCacheMu.Unlock()

	if s.masterStateCache == nil {
		s.masterStateCache = make(map[string]*storage.BlockState, masterStateCacheLimit)
	}
	if _, ok := s.masterStateCache[key]; !ok {
		s.masterStateCacheKeys = append(s.masterStateCacheKeys, key)
	}
	s.masterStateCache[key] = cloned

	for len(s.masterStateCacheKeys) > masterStateCacheLimit {
		evict := s.masterStateCacheKeys[0]
		copy(s.masterStateCacheKeys, s.masterStateCacheKeys[1:])
		s.masterStateCacheKeys = s.masterStateCacheKeys[:len(s.masterStateCacheKeys)-1]
		delete(s.masterStateCache, evict)
	}
}

func (s *Service) loadMasterStateForConsensus(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if block.Workchain == -1 && block.Shard == topShard {
		key := storage.BlockKey(block)

		s.masterStateCacheMu.Lock()
		cached := s.masterStateCache[key]
		s.masterStateCacheMu.Unlock()
		if cached != nil {
			return storage.CloneBlockState(cached), nil
		}
	}

	return s.storage.BlockState(ctx, block)
}

func (s *Service) StateRootForCompressedBlock(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if block.Workchain != -1 || block.Shard != topShard {
		return nil, storage.ErrNotFound
	}

	state, err := s.loadMasterStateForConsensus(ctx, block)
	if err != nil {
		return nil, err
	}
	if state.Cell != nil {
		return state.Cell, nil
	}
	if len(state.StateRootHash) != 32 {
		return nil, storage.ErrNotFound
	}

	root, err := s.storage.LoadStateCellTree(ctx, block, state.StateRootHash)
	return root, err
}
