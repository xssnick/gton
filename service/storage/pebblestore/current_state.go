package pebblestore

import (
	"context"
	"errors"
	"flexserver/service/storage"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Store) SaveCurrentState(ctx context.Context, state *storage.CurrentState) error {
	if state == nil {
		return fmt.Errorf("current state is nil")
	}
	return s.saveCurrentStateRecord(ctx, hotKeyCurrentState(), state, pebble.NoSync)
}

func (s *Store) SaveStateSyncProgress(ctx context.Context, state *storage.CurrentState) error {
	if state == nil {
		return fmt.Errorf("state sync progress is nil")
	}
	return s.saveCurrentStateRecord(ctx, hotKeyStateSyncProgress(), state, pebble.Sync)
}

func (s *Store) saveCurrentStateRecord(ctx context.Context, key []byte, state *storage.CurrentState, writeOptions *pebble.WriteOptions) error {
	if err := s.FlushStagedBlockStates(ctx); err != nil {
		return err
	}

	if err := s.saveBlockStateIfMissing(ctx, &state.Masterchain); err != nil {
		return err
	}
	for _, key := range storage.SortedShardKeys(state.Shards) {
		shard := state.Shards[key]
		if err := s.saveBlockStateIfMissing(ctx, &shard); err != nil {
			return err
		}
	}

	payload := encodeCurrentState(state)
	return s.setHotRecord(ctx, key, payload, writeOptions)
}

func (s *Store) saveBlockStateIfMissing(ctx context.Context, state *storage.BlockState) error {
	if state.Cell == nil && len(state.StateRootHash) == 0 {
		return nil
	}
	exists, err := s.blockStateExists(ctx, state.Block)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.SaveBlockState(ctx, state)
}

func (s *Store) blockStateExists(ctx context.Context, block ton.BlockIDExt) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, errPebbleClosed
	}
	return pebbleReaderHas(s.hot, hotKeyStateMeta(block))
}

func (s *Store) CurrentState(ctx context.Context) (*storage.CurrentState, error) {
	return s.currentStateRecord(ctx, hotKeyCurrentState(), "current", true)
}

func (s *Store) StateSyncProgress(ctx context.Context) (*storage.CurrentState, error) {
	return s.currentStateRecord(ctx, hotKeyStateSyncProgress(), "state sync progress", false)
}

func (s *Store) ClearStateSyncProgress(ctx context.Context) error {
	return s.deleteHotRecord(ctx, hotKeyStateSyncProgress(), pebble.Sync)
}

func (s *Store) SaveSeenMasterchainBlock(ctx context.Context, block ton.BlockIDExt) error {
	if block.Workchain != -1 || block.Shard != int64(-1<<63) {
		return fmt.Errorf("seen masterchain block is not masterchain: %s", storage.FormatBlockRef(block))
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}

	key := hotKeySeenMasterchainBlock()
	raw, err := pebbleReaderGetCopy(s.hot, key)
	if err == nil {
		current, err := decodeBlockID(raw)
		if err != nil {
			return err
		}
		if current.SeqNo >= block.SeqNo {
			return nil
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(key, encodeBlockID(block), pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) SeenMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	raw, err := s.getHotCopy(ctx, hotKeySeenMasterchainBlock())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return decodeBlockID(raw)
}

func (s *Store) currentStateRecord(ctx context.Context, key []byte, label string, missingMetaIsAbsent bool) (*storage.CurrentState, error) {
	raw, err := s.getHotCopy(ctx, key)
	if err != nil {
		return nil, err
	}
	state, err := decodeCurrentState(raw)
	if err != nil {
		return nil, err
	}
	master, err := s.blockStateMeta(ctx, state.Masterchain.Block)
	if err == nil {
		state.Masterchain = master
	} else if errors.Is(err, storage.ErrNotFound) {
		if missingMetaIsAbsent {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("load %s masterchain state %s: block state metadata is missing", label, storage.FormatBlockRef(state.Masterchain.Block))
	} else {
		return nil, fmt.Errorf("load %s masterchain state %s: %w", label, storage.FormatBlockRef(state.Masterchain.Block), err)
	}

	for key, shard := range state.Shards {
		loaded, err := s.blockStateMeta(ctx, shard.Block)
		if err == nil {
			state.Shards[key] = loaded
			continue
		}
		if errors.Is(err, storage.ErrNotFound) {
			if missingMetaIsAbsent {
				return nil, storage.ErrNotFound
			}
			return nil, fmt.Errorf("load %s shard state %s: block state metadata is missing", label, storage.FormatBlockRef(shard.Block))
		}
		return nil, fmt.Errorf("load %s shard state %s: %w", label, storage.FormatBlockRef(shard.Block), err)
	}
	return state, nil
}

type preparedBlockStateSave struct {
	original     *storage.BlockState
	saved        storage.BlockState
	lazyRoot     *cell.Cell
	cellSyncHash cell.Hash
	syncCells    bool
	flushCells   bool
}

func (s *Store) SaveBlockState(ctx context.Context, state *storage.BlockState) error {
	if err := s.FlushStagedBlockStates(ctx); err != nil {
		return err
	}

	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, []*storage.BlockState{state})
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, nil, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func (s *Store) StageBlockState(ctx context.Context, state *storage.BlockState) error {
	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, []*storage.BlockState{state})
	if err != nil {
		return err
	}
	return s.stagePreparedBlockStates(prepared, cellsElapsed)
}

func (s *Store) FlushStagedBlockStates(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	flush, ok := s.takePendingStateFlush()
	if !ok {
		return nil
	}

	flushStarted := time.Now()
	if err := s.flushCellDBs(); err != nil {
		s.requeuePendingStateFlush(flush)
		return fmt.Errorf("flush staged state cells before metadata marker: %w", err)
	}
	flushElapsed := time.Since(flushStarted)

	if err := s.syncPendingArchiveFiles(); err != nil {
		s.requeuePendingStateFlush(flush)
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		s.requeuePendingStateFlush(flush)
		return errPebbleClosed
	}

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()

	keys := make([]string, 0, len(flush.state.states))
	for key := range flush.state.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	prepared := make([]preparedBlockStateSave, 0, len(keys))
	for _, key := range keys {
		state := flush.state.states[key]
		sync := flush.state.syncs[key]
		if err := batch.Set(hotKeyStateCellSync(sync.block), encodeStateCellSync(sync.rootHash, sync.cellCount), pebble.NoSync); err != nil {
			s.requeuePendingStateFlush(flush)
			return err
		}
		if err := s.setHotMaybeReplace(batch, hotKeyStateMeta(state.Block), encodeBlockStateMeta(&state)); err != nil {
			s.requeuePendingStateFlush(flush)
			return err
		}
		if err := s.setMergedBlockMeta(batch, storage.BuildBlockMetaFromState(state)); err != nil {
			s.requeuePendingStateFlush(flush)
			return err
		}
		prepared = append(prepared, preparedBlockStateSave{
			saved:      state,
			syncCells:  true,
			flushCells: true,
		})
	}

	hotSyncStarted := time.Now()
	if err := batch.Commit(pebble.Sync); err != nil {
		s.requeuePendingStateFlush(flush)
		return err
	}
	hotSyncElapsed := time.Since(hotSyncStarted)

	s.finishPendingStateFlush(flush.id)

	s.logBlockStateCheckpoint(prepared, nil, true, 0, flushElapsed, hotSyncElapsed)
	return nil
}

func (s *Store) stagePreparedBlockStates(prepared []preparedBlockStateSave, cellsElapsed time.Duration) error {
	if len(prepared) == 0 {
		return nil
	}

	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.ensurePendingStateLocked()

	for _, preparedState := range prepared {
		state := storage.BlockStateWithoutCells(&preparedState.saved)
		key := storage.BlockKey(state.Block)
		s.pendingState.states[key] = state
		s.pendingState.syncs[key] = stateCellTreeSync{
			block:     state.Block,
			rootHash:  preparedState.cellSyncHash,
			cellCount: state.CellsCount,
		}

		meta := storage.BuildBlockMetaFromState(state)
		s.pendingState.blockMetas[key] = storage.MergeBlockMeta(s.pendingState.blockMetas[key], meta)

		historyKey := storage.BlockHistoryKey{Workchain: state.Block.Workchain, Shard: state.Block.Shard}
		seqIndex := s.pendingState.seqIndex[historyKey]
		if seqIndex == nil {
			seqIndex = map[uint32]ton.BlockIDExt{}
			s.pendingState.seqIndex[historyKey] = seqIndex
		}
		seqIndex[state.Block.SeqNo] = state.Block
	}

	s.log.Debug().
		Int("states", len(prepared)).
		Dur("save_cells_batch_elapsed", cellsElapsed).
		Int("pending_states", len(s.pendingState.states)).
		Msg("block states staged for checkpoint")
	return nil
}

func (s *Store) ensurePendingStateLocked() {
	if s.pendingState.states == nil {
		s.pendingState.states = map[string]storage.BlockState{}
	}
	if s.pendingState.blockMetas == nil {
		s.pendingState.blockMetas = map[string]*storage.BlockMeta{}
	}
	if s.pendingState.seqIndex == nil {
		s.pendingState.seqIndex = map[storage.BlockHistoryKey]map[uint32]ton.BlockIDExt{}
	}
	if s.pendingState.syncs == nil {
		s.pendingState.syncs = map[string]stateCellTreeSync{}
	}
}

func newPendingStateOverlay() pendingStateOverlay {
	return pendingStateOverlay{
		states:     map[string]storage.BlockState{},
		blockMetas: map[string]*storage.BlockMeta{},
		seqIndex:   map[storage.BlockHistoryKey]map[uint32]ton.BlockIDExt{},
		syncs:      map[string]stateCellTreeSync{},
	}
}

func (s *Store) takePendingStateFlush() (pendingStateFlush, bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()

	if len(s.pendingState.states) == 0 {
		return pendingStateFlush{}, false
	}

	s.nextPendingFlushID++
	flush := pendingStateFlush{
		id:    s.nextPendingFlushID,
		state: s.pendingState,
	}
	s.pendingFlushes = append(s.pendingFlushes, flush)
	s.pendingState = newPendingStateOverlay()
	return flush, true
}

func (s *Store) finishPendingStateFlush(id uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()

	s.removePendingStateFlushLocked(id)
}

func (s *Store) requeuePendingStateFlush(flush pendingStateFlush) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()

	s.removePendingStateFlushLocked(flush.id)
	s.pendingState = mergePendingStateOverlays(flush.state, s.pendingState)
}

func (s *Store) removePendingStateFlushLocked(id uint64) {
	for i := range s.pendingFlushes {
		if s.pendingFlushes[i].id != id {
			continue
		}
		copy(s.pendingFlushes[i:], s.pendingFlushes[i+1:])
		s.pendingFlushes = s.pendingFlushes[:len(s.pendingFlushes)-1]
		return
	}
}

func mergePendingStateOverlays(base pendingStateOverlay, next pendingStateOverlay) pendingStateOverlay {
	merged := newPendingStateOverlay()
	mergePendingStateOverlayInto(&merged, base)
	mergePendingStateOverlayInto(&merged, next)
	return merged
}

func mergePendingStateOverlayInto(dst *pendingStateOverlay, src pendingStateOverlay) {
	for key, state := range src.states {
		dst.states[key] = state
	}
	for key, sync := range src.syncs {
		dst.syncs[key] = sync
	}
	for key, meta := range src.blockMetas {
		dst.blockMetas[key] = mergePendingBlockMeta(dst.blockMetas[key], meta)
	}
	for historyKey, blocks := range src.seqIndex {
		dstBlocks := dst.seqIndex[historyKey]
		if dstBlocks == nil {
			dstBlocks = map[uint32]ton.BlockIDExt{}
			dst.seqIndex[historyKey] = dstBlocks
		}
		for seqno, block := range blocks {
			dstBlocks[seqno] = block
		}
	}
}

func (s *Store) pendingBlockState(block ton.BlockIDExt) (storage.BlockState, bool) {
	s.pendingMu.RLock()
	defer s.pendingMu.RUnlock()

	key := storage.BlockKey(block)
	if state, ok := s.pendingState.states[key]; ok {
		return *storage.CloneBlockState(&state), true
	}
	for i := len(s.pendingFlushes) - 1; i >= 0; i-- {
		if state, ok := s.pendingFlushes[i].state.states[key]; ok {
			return *storage.CloneBlockState(&state), true
		}
	}
	return storage.BlockState{}, false
}

func (s *Store) pendingStateCellSync(block ton.BlockIDExt) (stateCellTreeSync, bool) {
	s.pendingMu.RLock()
	defer s.pendingMu.RUnlock()

	key := storage.BlockKey(block)
	if sync, ok := s.pendingState.syncs[key]; ok {
		return sync, true
	}
	for i := len(s.pendingFlushes) - 1; i >= 0; i-- {
		if sync, ok := s.pendingFlushes[i].state.syncs[key]; ok {
			return sync, true
		}
	}
	return stateCellTreeSync{}, false
}

func (s *Store) pendingBlockMeta(block ton.BlockIDExt) *storage.BlockMeta {
	s.pendingMu.RLock()
	defer s.pendingMu.RUnlock()

	key := storage.BlockKey(block)
	var meta *storage.BlockMeta
	for i := range s.pendingFlushes {
		meta = mergePendingBlockMeta(meta, s.pendingFlushes[i].state.blockMetas[key])
	}
	meta = mergePendingBlockMeta(meta, s.pendingState.blockMetas[key])
	return meta
}

func mergePendingBlockMeta(base *storage.BlockMeta, next *storage.BlockMeta) *storage.BlockMeta {
	if next == nil {
		if base == nil {
			return nil
		}
		return base.Clone()
	}
	return storage.MergeBlockMeta(base, next)
}

func (s *Store) pendingBlockSeq(key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, bool) {
	s.pendingMu.RLock()
	defer s.pendingMu.RUnlock()

	if blocks := s.pendingState.seqIndex[key]; blocks != nil {
		if block, ok := blocks[seqno]; ok {
			return block, true
		}
	}
	for i := len(s.pendingFlushes) - 1; i >= 0; i-- {
		blocks := s.pendingFlushes[i].state.seqIndex[key]
		if blocks == nil {
			continue
		}
		if block, ok := blocks[seqno]; ok {
			return block, true
		}
	}
	return ton.BlockIDExt{}, false
}

func (s *Store) SaveBlockStateAndCurrentState(ctx context.Context, block *storage.BlockState, current *storage.CurrentState) error {
	var blocks []*storage.BlockState
	if block != nil {
		blocks = []*storage.BlockState{block}
	}
	return s.SaveBlockStatesAndCurrentState(ctx, blocks, current)
}

func (s *Store) SaveBlockStatesAndCurrentState(ctx context.Context, blocks []*storage.BlockState, current *storage.CurrentState) error {
	if current == nil {
		return fmt.Errorf("current state is nil")
	}
	if err := s.FlushStagedBlockStates(ctx); err != nil {
		return err
	}

	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, blocks)
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, current, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func (s *Store) prepareBlockStatesForSave(ctx context.Context, states []*storage.BlockState) ([]preparedBlockStateSave, time.Duration, error) {
	started := time.Now()
	prepared := make([]preparedBlockStateSave, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	exists := newStateCellExistenceCache()
	for _, state := range states {
		if state == nil {
			continue
		}
		key := storage.BlockKey(state.Block)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		next, err := s.prepareBlockStateForSave(ctx, state, exists)
		if err != nil {
			return nil, 0, err
		}
		prepared = append(prepared, next)
	}
	return prepared, time.Since(started), nil
}

func (s *Store) prepareBlockStateForSave(ctx context.Context, state *storage.BlockState, exists *stateCellExistenceCache) (preparedBlockStateSave, error) {
	var zero cell.Hash
	var prepared preparedBlockStateSave
	if state == nil {
		return prepared, fmt.Errorf("block state is nil")
	}
	saved := *state
	if len(saved.StateRootHash) == 0 && saved.Cell != nil {
		hash := saved.Cell.HashKey(0)
		saved.StateRootHash = hash[:]
	}
	if len(saved.StateCellHash) == 0 && saved.Cell != nil {
		hash := saved.Cell.HashKey()
		saved.StateCellHash = hash[:]
	}
	if len(saved.StateRootHash) == 0 {
		return prepared, fmt.Errorf("block state root hash is empty")
	}
	if len(saved.StateCellHash) == 0 {
		return prepared, fmt.Errorf("block state cell hash is empty")
	}
	if len(saved.StateCellHash) != len(zero) {
		return prepared, fmt.Errorf("block state cell hash size mismatch: got %d", len(saved.StateCellHash))
	}

	var cellSyncHash cell.Hash
	copy(cellSyncHash[:], saved.StateCellHash)
	var lazyRoot *cell.Cell
	syncCells := false
	flushCells := false

	if saved.Cell != nil {
		syncCells = true
		if saved.Cell.IsLazy() {
			lazyRoot = saved.Cell
		} else {
			cellSyncHash = saved.Cell.HashKey()
			var err error
			flushCells, err = s.saveStateCellTree(ctx, stateCellTreeSave{
				block:            saved.Block,
				root:             saved.Cell,
				totalCells:       saved.CellsCount,
				reusedStateCells: saved.ReusedStateCells,
				reusedStateRefs:  saved.ReusedStateRefs,
				exists:           exists,
			})
			if err != nil {
				return prepared, err
			}
			lazyRoot, err = s.loadLazyCell(ctx, cellSyncHash[:])
			if err != nil {
				return prepared, fmt.Errorf("load persisted lazy state root: %w", err)
			}
		}
	}

	return preparedBlockStateSave{
		original:     state,
		saved:        saved,
		lazyRoot:     lazyRoot,
		cellSyncHash: cellSyncHash,
		syncCells:    syncCells,
		flushCells:   flushCells,
	}, nil
}

func (s *Store) savePreparedBlockStateRecords(prepared []preparedBlockStateSave, current *storage.CurrentState, cellsElapsed time.Duration) error {
	flushCells := false
	for _, state := range prepared {
		if state.flushCells {
			flushCells = true
			break
		}
	}

	flushStarted := time.Now()
	if flushCells {
		if err := s.flushCellDBs(); err != nil {
			return fmt.Errorf("flush state cells before metadata marker: %w", err)
		}
	}
	flushElapsed := time.Since(flushStarted)

	if err := s.syncPendingArchiveFiles(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errPebbleClosed
	}

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, state := range prepared {
		if state.syncCells {
			if err := batch.Set(hotKeyStateCellSync(state.saved.Block), encodeStateCellSync(state.cellSyncHash, state.saved.CellsCount), pebble.NoSync); err != nil {
				return err
			}
		}
		if err := s.setHotMaybeReplace(batch, hotKeyStateMeta(state.saved.Block), encodeBlockStateMeta(&state.saved)); err != nil {
			return err
		}
		if err := s.setMergedBlockMeta(batch, storage.BuildBlockMetaFromState(state.saved)); err != nil {
			return err
		}
	}
	if current != nil {
		if err := s.setHotMaybeReplace(batch, hotKeyCurrentState(), encodeCurrentState(current)); err != nil {
			return err
		}
	}

	hotSyncStarted := time.Now()
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	hotSyncElapsed := time.Since(hotSyncStarted)

	s.logBlockStateCheckpoint(prepared, current, flushCells, cellsElapsed, flushElapsed, hotSyncElapsed)
	return nil
}

func (s *Store) replacePreparedBlockStatesWithLazyRoots(prepared []preparedBlockStateSave) error {
	for _, state := range prepared {
		if state.saved.Cell == nil || state.saved.Parsed == nil {
			continue
		}
		if err := s.replaceBlockStateWithLazyRoot(state.original, state.saved, state.lazyRoot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) logBlockStateCheckpoint(prepared []preparedBlockStateSave, current *storage.CurrentState, flushCells bool, cellsElapsed time.Duration, flushElapsed time.Duration, hotSyncElapsed time.Duration) {
	metrics := s.cells.metrics()
	event := s.log.Debug().
		Int("states", len(prepared)).
		Bool("flush_cells", flushCells).
		Dur("save_cells_batch_elapsed", cellsElapsed).
		Dur("flush_cell_dbs_elapsed", flushElapsed).
		Dur("hot_metadata_sync_elapsed", hotSyncElapsed).
		Int64("cell_flush_count", metrics.flushCount).
		Uint64("cell_ingest_count", metrics.ingestCount).
		Int64("cell_l0_files", metrics.l0Files).
		Int64("cell_l0_size", metrics.l0Size).
		Uint64("cell_compaction_debt", metrics.compactionDebt).
		Int64("cell_compactions_in_progress", metrics.compactionsInProgress).
		Int64("cell_compaction_bytes_pending", metrics.compactionBytesPending).
		Uint64("cell_memtable_size", metrics.memTableSize).
		Int64("cell_memtable_count", metrics.memTableCount)

	if current != nil {
		event.
			Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Uint32("shard_client_seqno", current.ShardClientSeqno).
			Int("shards", len(current.Shards))
	}

	event.Msg("block state checkpoint persisted")
}
