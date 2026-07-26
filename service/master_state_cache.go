package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
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
	version  uint64
}

type compressedBlockStateOrderEntry struct {
	key     storage.BlockRootHash
	version uint64
}

type monitorSplitDepthKey struct {
	keyBlockRoot storage.BlockRootHash
	keyBlockSeq  uint32
	workchain    int32
}

func (s *Service) rememberMasterState(ctx context.Context, state *storage.BlockState, block *PreparedBlock, shardTargets []ton.BlockIDExt) {
	if state.Block.Workchain != -1 || state.Block.Shard != topShard || state.Cell == nil {
		return
	}

	rememberedCompressedState := s.RememberCompressedBlockState(state)
	s.updateP2PShardOverlays(ctx, state, block, shardTargets)

	key := storage.BlockKey(state.Block)
	cloned := storage.CloneBlockState(state)

	s.masterStateCacheMu.Lock()
	if _, ok := s.masterStateCache[key]; !ok {
		if len(s.masterStateCacheKeys) == cap(s.masterStateCacheKeys) && s.masterStateCacheHead > 0 {
			copy(s.masterStateCacheKeys, s.masterStateCacheKeys[s.masterStateCacheHead:])
			s.masterStateCacheKeys = s.masterStateCacheKeys[:len(s.masterStateCacheKeys)-s.masterStateCacheHead]
			s.masterStateCacheHead = 0
		}
		s.masterStateCacheKeys = append(s.masterStateCacheKeys, key)
	}
	s.masterStateCache[key] = cloned

	for len(s.masterStateCacheKeys)-s.masterStateCacheHead > masterStateCacheLimit {
		evict := s.masterStateCacheKeys[s.masterStateCacheHead]
		s.masterStateCacheHead++
		delete(s.masterStateCache, evict)
	}
	s.masterStateCacheMu.Unlock()

	if rememberedCompressedState {
		s.node.NotifyCompressedBlockStateReady(state.Block)
	}
}

func (s *Service) updateMasterDependentCachesForKeyBlock(state *storage.BlockState, block *PreparedBlock) error {
	if !block.Meta.Has(storage.BlockMetaIsKeyBlock) {
		return nil
	}

	config, err := broadcastValidatorConfigFromMasterchainState(state)
	if err != nil {
		return fmt.Errorf("load broadcast validator config from key block %s: %w", block.BlockRef(), err)
	}

	s.publishBroadcastValidatorConfig(state.Block, config)
	s.resetMonitorSplitDepthCache()
	return nil
}

// RememberCompressedBlockState stores a state for compressed p2p broadcasts,
// which chain freshly decoded merkle updates onto their base state.
func (s *Service) RememberCompressedBlockState(state *storage.BlockState) bool {
	if state.Cell == nil {
		return false
	}

	key := storage.BlockKey(state.Block)
	cloned := storage.CloneBlockState(state)
	s.compressedStateMu.Lock()
	defer s.compressedStateMu.Unlock()
	now := time.Now()

	s.compressedStateVersion++
	entry := compressedBlockStateEntry{
		state:    cloned,
		storedAt: now,
		version:  s.compressedStateVersion,
	}
	s.compressedStateCache[key] = entry
	if len(s.compressedStateOrder) == cap(s.compressedStateOrder) && s.compressedStateOrderHead > 0 {
		copy(s.compressedStateOrder, s.compressedStateOrder[s.compressedStateOrderHead:])
		s.compressedStateOrder = s.compressedStateOrder[:len(s.compressedStateOrder)-s.compressedStateOrderHead]
		s.compressedStateOrderHead = 0
	}
	s.compressedStateOrder = append(s.compressedStateOrder, compressedBlockStateOrderEntry{
		key:     key,
		version: entry.version,
	})

	s.pruneCompressedBlockStatesLocked(now)
	for len(s.compressedStateCache) > compressedBlockStateCacheLimit {
		s.evictOldestCompressedBlockStateLocked()
	}
	if len(s.compressedStateOrder)-s.compressedStateOrderHead > compressedBlockStateCacheLimit*2 {
		s.compactCompressedBlockStateOrderLocked()
	}
	return true
}

func (s *Service) cachedCompressedBlockState(block ton.BlockIDExt) (*storage.BlockState, error) {
	key := storage.BlockKey(block)
	now := time.Now()

	s.compressedStateMu.Lock()
	defer s.compressedStateMu.Unlock()

	entry, ok := s.compressedStateCache[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	if !entry.storedAt.Add(compressedBlockStateCacheTTL).After(now) {
		delete(s.compressedStateCache, key)
		return nil, storage.ErrNotFound
	}
	return storage.CloneBlockState(entry.state), nil
}

func (s *Service) pruneCompressedBlockStatesLocked(now time.Time) {
	if len(s.compressedStateCache) == 0 {
		s.compressedStateOrder = nil
		s.compressedStateOrderHead = 0
		return
	}

	for s.compressedStateOrderHead < len(s.compressedStateOrder) {
		oldest := s.compressedStateOrder[s.compressedStateOrderHead]
		entry, ok := s.compressedStateCache[oldest.key]
		if !ok || entry.version != oldest.version {
			s.compressedStateOrderHead++
			continue
		}
		if entry.storedAt.Add(compressedBlockStateCacheTTL).After(now) {
			break
		}
		delete(s.compressedStateCache, oldest.key)
		s.compressedStateOrderHead++
	}
	if s.compressedStateOrderHead == len(s.compressedStateOrder) {
		s.compressedStateOrder = s.compressedStateOrder[:0]
		s.compressedStateOrderHead = 0
	}
}

func (s *Service) evictOldestCompressedBlockStateLocked() {
	for s.compressedStateOrderHead < len(s.compressedStateOrder) {
		oldest := s.compressedStateOrder[s.compressedStateOrderHead]
		s.compressedStateOrderHead++

		entry, ok := s.compressedStateCache[oldest.key]
		if !ok || entry.version != oldest.version {
			continue
		}
		delete(s.compressedStateCache, oldest.key)
		if s.compressedStateOrderHead == len(s.compressedStateOrder) {
			s.compressedStateOrder = s.compressedStateOrder[:0]
			s.compressedStateOrderHead = 0
		}
		return
	}
	s.compressedStateOrder = s.compressedStateOrder[:0]
	s.compressedStateOrderHead = 0
}

func (s *Service) compactCompressedBlockStateOrderLocked() {
	write := 0
	for _, ordered := range s.compressedStateOrder[s.compressedStateOrderHead:] {
		entry, ok := s.compressedStateCache[ordered.key]
		if !ok || entry.version != ordered.version {
			continue
		}
		s.compressedStateOrder[write] = ordered
		write++
	}
	s.compressedStateOrder = s.compressedStateOrder[:write]
	s.compressedStateOrderHead = 0
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
	if depth, ok := s.monitorSplitDepth[key]; ok {
		s.monitorSplitDepthMu.Unlock()
		return depth, nil
	}
	s.monitorSplitDepthMu.Unlock()

	depth, err := monitorMinSplitDepth(state, workchain)
	if err != nil {
		return 0, err
	}

	s.monitorSplitDepthMu.Lock()
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
		return
	}

	config, err := s.broadcastValidatorCache.getConfig()
	if errors.Is(err, storage.ErrNotFound) {
		config, err = broadcastValidatorConfigFromMasterchainState(state)
		if err == nil {
			config = s.publishBroadcastValidatorConfig(state.Block, config)
		}
	}
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("masterchain", storage.FormatBlockRef(state.Block)).
			Msg("failed to load FastSync overlay config")
		return
	}

	fastSyncShards := make([]p2p.FastSyncShard, len(shards))
	for i := range shards {
		fastSyncShards[i] = p2p.FastSyncShard{
			Workchain: shards[i].Workchain,
			Shard:     shards[i].Shard,
		}
	}
	err = s.node.SetFastSyncOverlays(p2p.FastSyncState{
		Roster:                     config.fastSync.roster,
		Shards:                     fastSyncShards,
		MasterchainPlumtreeEnabled: config.fastSync.plumtreeEnabled(-1),
		ShardPlumtreeEnabled:       config.fastSync.plumtreeEnabled(0),
	})
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("masterchain", storage.FormatBlockRef(state.Block)).
			Msg("failed to update FastSync overlays")
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
		// Archive apply already has the parsed destination state. Its shard
		// hashes are the committed result of this block, so avoid parsing the
		// prepared block again just to update overlays.
		if state.Block.Equals(&block.ID) && state.Parsed != nil && state.Parsed.McStateExtra != nil {
			return state2.ShardBlocksFromMasterState(state)
		}

		parsed, err := parsePreparedBlock(*block)
		if err != nil {
			return nil, fmt.Errorf("parse master block %s: %w", block.BlockRef(), err)
		}
		return state2.ShardBlocksFromMasterBlock(block.ID, parsed)
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
		cached, ok := s.masterStateCache[key]
		s.masterStateCacheMu.Unlock()
		if ok {
			return storage.CloneBlockState(cached), nil
		}
	}

	return s.storage.BlockState(ctx, block)
}

func (s *Service) StateRootForCompressedBlock(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	// The compressed-state cache holds in-memory apply-time or decode-chained
	// trees (materialized at least along recently changed paths); it must be
	// consulted before currentStatus/liveState, whose Cell roots are swapped
	// for lazy celldb roots after every checkpoint — a lazy root would make
	// state-aware decompression walk pebble cell by cell.
	state, err := s.cachedCompressedBlockState(block)
	if err == nil {
		return state.Cell, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	s.currentStatusMu.RLock()
	state, err = currentStateBlockState(s.currentStatus, block)
	s.currentStatusMu.RUnlock()

	if err == nil {
		if state.Cell != nil {
			return state.Cell, nil
		}
		if len(state.StateRootHash) == 32 {
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
