package service

import (
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

const (
	compressedBlockStateCacheLimit = 1024
	compressedBlockStateCacheTTL   = 3 * time.Minute

	// monitorSplitDepthCacheLimit bounds the per-master entries between key
	// blocks; only the masters currently in the apply/commit pipeline are ever
	// re-queried, so overflow simply recomputes.
	monitorSplitDepthCacheLimit = 64
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

// monitorSplitDepthKey identifies the exact master state the depth was computed
// from, so a hit needs no state parse at all and an entry can never be aliased
// by a state from another config epoch: the shard after-apply path queries with
// inclusion masters that may lag or lead the key-block reset, and any key
// shared between masters would let a stale-epoch compute overwrite a fresh one.
// The key-block reset is therefore only a freshness bound, not what keeps the
// cache correct.
type monitorSplitDepthKey struct {
	masterRoot storage.BlockRootHash
	workchain  int32
}

func (s *Service) rememberMasterState(ctx context.Context, state *storage.BlockState, block *PreparedBlock, shardTargets []ton.BlockIDExt) {
	if state.Block.Workchain != -1 || state.Block.Shard != topShard || state.Cell == nil {
		return
	}

	s.rememberAppliedMasterCompressedState(state)
	s.updateP2PShardOverlays(ctx, state, block, shardTargets)
}

// rememberAppliedMasterCompressedState publishes a freshly applied masterchain
// state to the two caches the *next* block depends on: the state-aware
// decompression cache read by the p2p broadcast decode, and the consensus cache
// read when the next block's proof is validated. Both gate the next live head,
// so the apply loop runs this before the slower publish and overlay work.
func (s *Service) rememberAppliedMasterCompressedState(state *storage.BlockState) {
	if state.Block.Workchain != -1 || state.Block.Shard != topShard || state.Cell == nil {
		return
	}

	rememberedCompressedState := s.RememberCompressedBlockState(state)

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
	key := monitorSplitDepthKey{
		masterRoot: storage.BlockKey(state.Block),
		workchain:  workchain,
	}

	s.monitorSplitDepthMu.Lock()
	depth, ok := s.monitorSplitDepth[key]
	s.monitorSplitDepthMu.Unlock()
	if ok {
		return depth, nil
	}

	depth, err := monitorMinSplitDepth(state, workchain)
	if err != nil {
		return 0, err
	}

	s.monitorSplitDepthMu.Lock()
	if len(s.monitorSplitDepth) >= monitorSplitDepthCacheLimit {
		clear(s.monitorSplitDepth)
	}
	s.monitorSplitDepth[key] = depth
	s.monitorSplitDepthMu.Unlock()

	return depth, nil
}

func (s *Service) updateP2PShardOverlays(ctx context.Context, state *storage.BlockState, block *PreparedBlock, shardTargets []ton.BlockIDExt) {
	if !s.updateP2PMonitorMinSplitDepth(state) {
		return
	}
	s.reconcileP2PShardOverlays(ctx, state, block, shardTargets)
}

// updateP2PMonitorMinSplitDepth is the cheap half of the overlay update: on
// non-key masters it is a cache hit, and node.overlayBlockForDownload needs the
// depth before the next shard download picks an overlay, so it stays on the
// apply loop even when the reconcile below is deferred.
func (s *Service) updateP2PMonitorMinSplitDepth(state *storage.BlockState) bool {
	depth, err := s.cachedMonitorMinSplitDepth(state, 0)
	if err != nil {
		s.log.Debug().
			Err(err).
			Stringer("masterchain", storage.BlockRef(state.Block)).
			Msg("failed to update p2p monitor split depth")
		return false
	}

	s.node.SetMonitorMinSplitDepth(0, depth)
	return true
}

// reconcileP2PShardOverlays is the expensive half: per-shard overlay specs, a
// roster-sized map per shard in the FastSync spec build, and a full
// activate/deactivate sweep. It must run after updateP2PMonitorMinSplitDepth
// (the active set is computed at the monitor depth) and after the key-block
// validator config was published.
func (s *Service) reconcileP2PShardOverlays(ctx context.Context, state *storage.BlockState, block *PreparedBlock, shardTargets []ton.BlockIDExt) {
	// Every reconcile — deferred or synchronous (startup, archive import) —
	// funnels through here, because each one deactivates every shard overlay
	// outside its own set: two running at once, or an older one landing after a
	// newer one, wipes overlays the newer set just activated.
	s.shardOverlayReconcileMu.Lock()
	defer s.shardOverlayReconcileMu.Unlock()
	if state.Block.SeqNo < s.shardOverlayReconciledSeqno {
		return
	}
	s.shardOverlayReconciledSeqno = state.Block.SeqNo

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

type masterShardOverlayUpdate struct {
	state   *storage.BlockState
	targets []ton.BlockIDExt
}

// scheduleP2PShardOverlayUpdate moves the overlay reconcile off the masterchain
// apply loop. Updates are latest-wins with a single in-flight worker: the
// desired overlay set is a pure function of the newest master state, so
// coalescing intermediate states is correct, while running two reconciles
// concurrently is not — each one deactivates every shard overlay outside its
// own set, so a late older update would wipe overlays a newer one just
// activated.
func (s *Service) scheduleP2PShardOverlayUpdate(state *storage.BlockState, targets []ton.BlockIDExt) {
	if state == nil || state.Block.Workchain != -1 || state.Block.Shard != topShard {
		return
	}

	s.shardOverlayMu.Lock()
	if pending := s.shardOverlayPending; pending != nil && pending.state.Block.SeqNo >= state.Block.SeqNo {
		s.shardOverlayMu.Unlock()
		return
	}
	s.shardOverlayPending = &masterShardOverlayUpdate{state: state, targets: targets}
	if s.shardOverlayRunning {
		s.shardOverlayMu.Unlock()
		return
	}
	s.shardOverlayRunning = true
	s.shardOverlayMu.Unlock()

	s.runAsync(s.runP2PShardOverlayUpdates)
}

func (s *Service) runP2PShardOverlayUpdates() {
	ctx := s.shutdownContext
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		s.shardOverlayMu.Lock()
		update := s.shardOverlayPending
		s.shardOverlayPending = nil
		if update == nil || ctx.Err() != nil {
			s.shardOverlayRunning = false
			s.shardOverlayMu.Unlock()
			return
		}
		s.shardOverlayMu.Unlock()

		if s.reconcileOverlaysHook != nil {
			s.reconcileOverlaysHook(ctx, *update)
			continue
		}
		s.reconcileP2PShardOverlays(ctx, update.state, nil, update.targets)
	}
}

func (s *Service) masterBlockShardTargets(ctx context.Context, state *storage.BlockState, block *PreparedBlock, precomputed []ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	// The apply pipeline already parsed the block and derived the shard targets
	// before applying its transition; reuse them instead of a second full block
	// parse on the hot path. ShardBlocksFromMasterBlock is a pure function of the
	// block's shard hashes, so the result is identical — which is also why the
	// deferred overlay reconcile can carry the targets without the prepared block.
	if precomputed != nil {
		return precomputed, nil
	}
	if block != nil {
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
