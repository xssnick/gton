package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	compressedBlockStateCacheLimit = 1024
	compressedBlockStateCacheTTL   = 3 * time.Minute
)

type compressedBlockStateEntry struct {
	state    *storage.BlockState
	storedAt time.Time
}

type monitorSplitDepthKey struct {
	keyBlockRoot storage.BlockRootHash
	keyBlockSeq  uint32
	workchain    int32
}

func (s *Service) rememberMasterState(ctx context.Context, state *storage.BlockState, block *PreparedBlock, shardTargets []ton.BlockIDExt) {
	if state == nil || state.Block.Workchain != -1 || state.Block.Shard != topShard || state.Cell == nil {
		return
	}

	s.resetMasterDependentCachesForKeyBlock(block)

	rememberedCompressedState := s.rememberCompressedBlockState(state)
	s.updateP2PShardOverlays(ctx, state, block, shardTargets)

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

	if rememberedCompressedState && s.node != nil {
		s.node.NotifyCompressedBlockStateReady(state.Block)
	}
}

func (s *Service) resetMasterDependentCachesForKeyBlock(block *PreparedBlock) {
	if block == nil || block.Meta == nil || !block.Meta.Has(storage.BlockMetaIsKeyBlock) {
		return
	}

	s.resetMonitorSplitDepthCache()
	s.broadcastValidatorCache.reset()
}

func (s *Service) rememberCompressedBlockState(state *storage.BlockState) bool {
	if state == nil || state.Cell == nil {
		return false
	}

	key := storage.BlockKey(state.Block)
	now := time.Now()
	entry := compressedBlockStateEntry{
		state:    storage.CloneBlockState(state),
		storedAt: now,
	}

	s.compressedStateMu.Lock()
	defer s.compressedStateMu.Unlock()

	if s.compressedStateCache == nil {
		s.compressedStateCache = make(map[storage.BlockRootHash]compressedBlockStateEntry, compressedBlockStateCacheLimit)
	}
	if _, ok := s.compressedStateCache[key]; !ok {
		s.compressedStateOrder = append(s.compressedStateOrder, key)
	}
	s.compressedStateCache[key] = entry

	s.pruneCompressedBlockStatesLocked(now)
	for len(s.compressedStateOrder) > compressedBlockStateCacheLimit {
		evict := s.compressedStateOrder[0]
		copy(s.compressedStateOrder, s.compressedStateOrder[1:])
		s.compressedStateOrder = s.compressedStateOrder[:len(s.compressedStateOrder)-1]
		delete(s.compressedStateCache, evict)
	}
	return true
}

func (s *Service) cachedCompressedBlockState(block ton.BlockIDExt) (*storage.BlockState, bool) {
	key := storage.BlockKey(block)
	now := time.Now()

	s.compressedStateMu.Lock()
	defer s.compressedStateMu.Unlock()

	entry, ok := s.compressedStateCache[key]
	if !ok {
		return nil, false
	}
	if !entry.storedAt.Add(compressedBlockStateCacheTTL).After(now) {
		delete(s.compressedStateCache, key)
		return nil, false
	}
	return storage.CloneBlockState(entry.state), true
}

func (s *Service) pruneCompressedBlockStatesLocked(now time.Time) {
	if len(s.compressedStateCache) == 0 {
		s.compressedStateOrder = nil
		return
	}

	write := 0
	for _, key := range s.compressedStateOrder {
		entry, ok := s.compressedStateCache[key]
		if !ok {
			continue
		}
		if !entry.storedAt.Add(compressedBlockStateCacheTTL).After(now) {
			delete(s.compressedStateCache, key)
			continue
		}
		s.compressedStateOrder[write] = key
		write++
	}
	s.compressedStateOrder = s.compressedStateOrder[:write]
}

func (s *Service) resetMonitorSplitDepthCache() {
	s.monitorSplitDepthMu.Lock()
	clear(s.monitorSplitDepth)
	s.monitorSplitDepthMu.Unlock()
}

func (s *Service) cachedMonitorMinSplitDepth(state *storage.BlockState, workchain int32) (uint32, error) {
	key, err := monitorSplitDepthCacheKey(state, workchain)
	if err != nil {
		return 0, err
	}

	s.monitorSplitDepthMu.Lock()
	if s.monitorSplitDepth != nil {
		if depth, ok := s.monitorSplitDepth[key]; ok {
			s.monitorSplitDepthMu.Unlock()
			return depth, nil
		}
	}
	s.monitorSplitDepthMu.Unlock()

	depth, err := monitorMinSplitDepth(state, workchain)
	if err != nil {
		return 0, err
	}

	s.monitorSplitDepthMu.Lock()
	if s.monitorSplitDepth == nil {
		s.monitorSplitDepth = make(map[monitorSplitDepthKey]uint32)
	}
	s.monitorSplitDepth[key] = depth
	s.monitorSplitDepthMu.Unlock()

	return depth, nil
}

func monitorSplitDepthCacheKey(state *storage.BlockState, workchain int32) (monitorSplitDepthKey, error) {
	keyBlock, err := monitorSplitDepthKeyBlock(state)
	if err != nil {
		return monitorSplitDepthKey{}, err
	}

	return monitorSplitDepthKey{
		keyBlockRoot: storage.BlockKey(keyBlock),
		keyBlockSeq:  keyBlock.SeqNo,
		workchain:    workchain,
	}, nil
}

func monitorSplitDepthKeyBlock(state *storage.BlockState) (ton.BlockIDExt, error) {
	if state.Parsed == nil || state.Parsed.McStateExtra == nil {
		return ton.BlockIDExt{}, fmt.Errorf("masterchain state %s is missing mc_state_extra", storage.FormatBlockRef(state.Block))
	}

	var extra tlb.McStateExtra
	loader, err := state.Parsed.McStateExtra.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if err = tlb.LoadFromCell(&extra, loader); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if extra.Info == nil {
		return ton.BlockIDExt{}, fmt.Errorf("masterchain state %s is missing mc_state_extra info", storage.FormatBlockRef(state.Block))
	}

	info, err := extra.Info.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra info for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if _, err = info.LoadUInt(16); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra info flags for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if _, err = info.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra validator list hash for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if _, err = info.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra catchain seqno for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if _, err = info.LoadBoolBit(); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra next cc flag for %s: %w", storage.FormatBlockRef(state.Block), err)
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err = prevBlocks.LoadFromCell(info); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra prev blocks for %s: %w", storage.FormatBlockRef(state.Block), err)
	}

	afterKeyBlock, err := info.LoadBoolBit()
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra after key block flag for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	hasLastKeyBlock, err := info.LoadBoolBit()
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra last key block flag for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if afterKeyBlock {
		return state.Block, nil
	}
	if !hasLastKeyBlock {
		return ton.BlockIDExt{}, fmt.Errorf("masterchain state %s has no last key block", storage.FormatBlockRef(state.Block))
	}

	var ref tlb.ExtBlkRef
	if err = tlb.LoadFromCell(&ref, info); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse mc_state_extra last key block for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     ref.SeqNo,
		RootHash:  append([]byte(nil), ref.RootHash...),
		FileHash:  append([]byte(nil), ref.FileHash...),
	}, nil
}

func (s *Service) updateP2PShardOverlays(ctx context.Context, state *storage.BlockState, block *PreparedBlock, shardTargets []ton.BlockIDExt) {
	if s.node == nil {
		return
	}

	depth, err := s.cachedMonitorMinSplitDepth(state, 0)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("masterchain", storage.FormatBlockRef(state.Block)).
			Msg("failed to update p2p monitor split depth")
		return
	}

	s.node.SetMonitorMinSplitDepth(0, depth)

	shards, err := s.masterBlockShardTargets(ctx, state, block, shardTargets)
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

func (s *Service) masterBlockShardTargets(ctx context.Context, state *storage.BlockState, block *PreparedBlock, precomputed []ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	if block != nil {
		// The apply pipeline already parsed the block and derived the shard
		// targets before applying its transition; reuse them instead of a second
		// full block parse on the hot path. ShardBlocksFromMasterBlock is a pure
		// function of the block's shard hashes, so the result is identical.
		if precomputed != nil {
			return precomputed, nil
		}

		parsed, err := parsePreparedBlock(*block)
		if err != nil {
			return nil, fmt.Errorf("parse master block %s: %w", block.BlockRef(), err)
		}
		return state2.ShardBlocksFromMasterBlock(block.ID, parsed)
	}

	if s.storage == nil {
		return state2.ShardBlocksFromMasterState(state)
	}

	// Startup remembers the already-current master state before a prepared block
	// flows through the live pipeline, so parse the stored block data there. A
	// state-only restore can still recover shard targets from mc_state_extra.
	data, err := s.storage.BlockData(ctx, state.Block)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return state2.ShardBlocksFromMasterState(state)
		}
		return nil, err
	}
	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("parse stored master block BOC %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	parsed, err := storage.ParseVerifiedBlockCell(state.Block, root)
	if err != nil {
		return nil, fmt.Errorf("parse stored master block %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	return state2.ShardBlocksFromMasterBlock(state.Block, parsed)
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

	if s.liveState != nil {
		state, err = s.liveState.BlockState(ctx, block)
		if err == nil {
			if state.Cell != nil {
				return state.Cell, nil
			}
			if len(state.StateRootHash) == 32 {
				root, err := s.liveState.LoadStateCellTree(ctx, block, state.StateRootHash)
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

	if state, ok := s.cachedCompressedBlockState(block); ok {
		return state.Cell, nil
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
