package pebblestore

import (
	"context"
	"errors"
	"flexserver/service/storage"
	"fmt"
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
	return s.saveMasterchainBlockHint(ctx, hotKeySeenMasterchainBlock(), "seen masterchain block", block)
}

func (s *Store) SeenMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	return s.masterchainBlockHint(ctx, hotKeySeenMasterchainBlock())
}

func (s *Store) SaveVerifiedKeyBlockProgress(ctx context.Context, block ton.BlockIDExt) error {
	return s.saveMasterchainBlockHint(ctx, hotKeyVerifiedKeyBlockProgress(), "verified key block progress", block)
}

func (s *Store) VerifiedKeyBlockProgress(ctx context.Context) (ton.BlockIDExt, error) {
	return s.masterchainBlockHint(ctx, hotKeyVerifiedKeyBlockProgress())
}

func (s *Store) saveMasterchainBlockHint(ctx context.Context, key []byte, label string, block ton.BlockIDExt) error {
	if block.Workchain != -1 || block.Shard != int64(-1<<63) {
		return fmt.Errorf("%s is not masterchain: %s", label, storage.FormatBlockRef(block))
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

func (s *Store) masterchainBlockHint(ctx context.Context, key []byte) (ton.BlockIDExt, error) {
	raw, err := s.getHotCopy(ctx, key)
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
	parsed       *storage.BlockState
	lazyRoot     *cell.Cell
	cellSyncHash cell.Hash
	flushCells   bool
}

func (s *Store) SaveBlockState(ctx context.Context, state *storage.BlockState) error {
	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, []*storage.BlockState{state}, nil)
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, nil, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func (s *Store) SaveStateCheckpoint(ctx context.Context, blocks []*storage.BlockState, current *storage.CurrentState) error {
	return s.SaveStateCheckpointWithCells(ctx, blocks, current, nil)
}

func (s *Store) SaveStateCheckpointWithCells(ctx context.Context, blocks []*storage.BlockState, current *storage.CurrentState, cells []storage.EncodedCellRecord) error {
	if current == nil {
		return fmt.Errorf("current state is nil")
	}

	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, blocks, cells)
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, current, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func (s *Store) prepareBlockStatesForSave(ctx context.Context, states []*storage.BlockState, cells []storage.EncodedCellRecord) ([]preparedBlockStateSave, time.Duration, error) {
	started := time.Now()
	prepared := make([]preparedBlockStateSave, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	exists := newStateCellExistenceCache()
	trees := make([]stateCellTreeSave, 0, len(states))
	treePreparedIndexes := make([]int, 0, len(states))
	usePreparedCells := len(cells) > 0
	for _, state := range states {
		if state == nil {
			continue
		}
		key := storage.BlockKey(state.Block)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		saved, cellSyncHash, err := prepareBlockStateHeader(state)
		if err != nil {
			return nil, 0, err
		}

		next := preparedBlockStateSave{
			original:     state,
			saved:        saved,
			cellSyncHash: cellSyncHash,
		}
		if saved.Cell != nil {
			if saved.Cell.IsLazy() {
				next.lazyRoot = saved.Cell
			} else {
				next.cellSyncHash = saved.Cell.HashKey()
				if !usePreparedCells {
					trees = append(trees, stateCellTreeSave{
						block:      saved.Block,
						root:       saved.Cell,
						totalCells: saved.CellsCount,
						exists:     exists,
					})
				}
				treePreparedIndexes = append(treePreparedIndexes, len(prepared))
			}
		}
		prepared = append(prepared, next)
	}

	preparedCellsFlushed, err := s.savePreparedStateCellRecords(ctx, cells)
	if err != nil {
		return nil, 0, err
	}
	dfsFlushed, err := s.saveStateCellTreesDFSBatch(ctx, trees, exists)
	if err != nil {
		return nil, 0, err
	}
	flushCells := preparedCellsFlushed || dfsFlushed
	for _, idx := range treePreparedIndexes {
		prepared[idx].flushCells = flushCells
		lazyRoot, err := s.loadLazyCell(ctx, prepared[idx].cellSyncHash[:])
		if err != nil {
			return nil, 0, fmt.Errorf("load persisted lazy state root: %w", err)
		}
		prepared[idx].lazyRoot = lazyRoot
	}
	for i := range prepared {
		if prepared[i].lazyRoot == nil || !shouldParseSavedLazyState(prepared[i].saved) {
			continue
		}
		parsed, err := storage.ParseStateProof(&prepared[i].saved.Block, prepared[i].lazyRoot, nil, prepared[i].saved.StateRootHash, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("parse persisted lazy state root: %w", err)
		}
		prepared[i].parsed = parsed
	}
	return prepared, time.Since(started), nil
}

func prepareBlockStateHeader(state *storage.BlockState) (storage.BlockState, cell.Hash, error) {
	var zero cell.Hash
	var cellSyncHash cell.Hash
	if state == nil {
		return storage.BlockState{}, cellSyncHash, fmt.Errorf("block state is nil")
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
		return storage.BlockState{}, cellSyncHash, fmt.Errorf("block state root hash is empty")
	}
	if len(saved.StateCellHash) == 0 {
		return storage.BlockState{}, cellSyncHash, fmt.Errorf("block state cell hash is empty")
	}
	if len(saved.StateCellHash) != len(zero) {
		return storage.BlockState{}, cellSyncHash, fmt.Errorf("block state cell hash size mismatch: got %d", len(saved.StateCellHash))
	}
	copy(cellSyncHash[:], saved.StateCellHash)
	return saved, cellSyncHash, nil
}

func shouldParseSavedLazyState(state storage.BlockState) bool {
	return state.Parsed != nil && (state.Block.Workchain != -1 || state.Parsed.McStateExtra != nil)
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

	if err := s.syncPendingArtifactFiles(); err != nil {
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
		if state.saved.Cell == nil {
			continue
		}
		if err := s.replaceBlockStateWithLazyRoot(state.original, state.saved, state.parsed, state.lazyRoot); err != nil {
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
