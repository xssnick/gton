package service

import (
	"context"
	"errors"

	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Service) rememberMasterState(state *storage.BlockState) {
	if state == nil || state.Block.Workchain != -1 || state.Block.Shard != topShard || state.Cell == nil {
		return
	}

	s.updateP2PShardOverlays(state)

	key := storage.BlockKey(state.Block)
	cloned := storage.CloneBlockState(state)

	s.masterStateCacheMu.Lock()
	if s.masterStateCache == nil {
		s.masterStateCache = make(map[storage.BlockRootHash]*storage.BlockState, masterStateCacheLimit)
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
	s.masterStateCacheMu.Unlock()

	if s.node != nil {
		s.node.NotifyCompressedBlockStateReady()
	}
}

func (s *Service) updateP2PShardOverlays(state *storage.BlockState) {
	if s.node == nil {
		return
	}

	depth, err := monitorMinSplitDepth(state, 0)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("masterchain", storage.FormatBlockRef(state.Block)).
			Msg("failed to update p2p monitor split depth")
		return
	}

	s.node.SetMonitorMinSplitDepth(0, depth)

	shards, err := state2.ShardBlocksFromMasterState(state)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("masterchain", storage.FormatBlockRef(state.Block)).
			Msg("failed to update p2p shard overlays")
		return
	}
	if err = s.node.SetActiveShardOverlays(shards); err != nil {
		s.log.Debug().
			Err(err).
			Str("masterchain", storage.FormatBlockRef(state.Block)).
			Msg("failed to update p2p shard overlays")
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
	s.currentStatusMu.RLock()
	state, err := currentStateBlockState(s.currentStatus, block)
	s.currentStatusMu.RUnlock()

	if err == nil {
		if state.Cell != nil {
			return state.Cell, nil
		}
		if len(state.StateRootHash) == 32 && s.storage != nil {
			root, err := s.storage.LoadStateCellTree(ctx, block, state.StateRootHash)
			if err == nil {
				return root, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	if live, ok := s.liveState.(liveStateRootStore); ok {
		state, err = live.BlockState(ctx, block)
		if err == nil {
			if state.Cell != nil {
				return state.Cell, nil
			}
			if len(state.StateRootHash) == 32 {
				root, err := live.LoadStateCellTree(ctx, block, state.StateRootHash)
				if err == nil {
					return root, nil
				}
				if !errors.Is(err, storage.ErrNotFound) {
					return nil, err
				}
			}
		} else if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}

	if block.Workchain != -1 || block.Shard != topShard {
		return nil, storage.ErrNotFound
	}

	state, err = s.loadMasterStateForConsensus(ctx, block)
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

type liveStateRootStore interface {
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
}

func currentStateBlockState(current *storage.CurrentState, block ton.BlockIDExt) (*storage.BlockState, error) {
	if current == nil {
		return nil, storage.ErrNotFound
	}
	if current.Masterchain.Block.Equals(&block) {
		return storage.CloneBlockState(&current.Masterchain), nil
	}
	for _, shard := range current.Shards {
		if shard.Block.Equals(&block) {
			return storage.CloneBlockState(&shard), nil
		}
	}
	return nil, storage.ErrNotFound
}
