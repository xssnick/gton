package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"github.com/xssnick/gton/service/storage"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
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
	if err := s.saveCurrentStateBlockState(ctx, &state.Masterchain); err != nil {
		return err
	}
	for _, key := range storage.SortedShardKeys(state.Shards) {
		shard := state.Shards[key]
		if err := s.saveCurrentStateBlockState(ctx, &shard); err != nil {
			return err
		}
	}

	payload := encodeCurrentState(state)
	return s.setHotRecord(ctx, key, payload, writeOptions)
}

func (s *Store) saveCurrentStateBlockState(ctx context.Context, state *storage.BlockState) error {
	if state.Cell == nil && len(state.StateRootHash) == 0 {
		return nil
	}
	return s.SaveBlockState(ctx, state)
}

func (s *Store) CurrentState(ctx context.Context) (*storage.CurrentState, error) {
	return s.currentStateRecord(ctx, hotKeyCurrentState(), "current")
}

func (s *Store) StateSyncProgress(ctx context.Context) (*storage.CurrentState, error) {
	return s.currentStateRecord(ctx, hotKeyStateSyncProgress(), "state sync progress")
}

func (s *Store) ClearStateSyncProgress(ctx context.Context) error {
	return s.deleteHotRecord(ctx, hotKeyStateSyncProgress(), pebble.Sync)
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

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	raw, err := pebbleReaderGetCopy(db, key)
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

	batch := db.NewBatch()
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

func (s *Store) currentStateRecord(ctx context.Context, key []byte, label string) (*storage.CurrentState, error) {
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
			return nil, fmt.Errorf("load %s shard state %s: block state metadata is missing", label, storage.FormatBlockRef(shard.Block))
		}
		return nil, fmt.Errorf("load %s shard state %s: %w", label, storage.FormatBlockRef(shard.Block), err)
	}
	return state, nil
}

type preparedBlockStateSave struct {
	original      *storage.BlockState
	saved         storage.BlockState
	parsed        *storage.BlockState
	lazyRoot      *cell.Cell
	stateCellHash cell.Hash
	flushCells    bool
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

func (s *Store) SwitchCellGeneration(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *storage.CurrentState) (uint64, error) {
	if generation == 0 {
		return 0, fmt.Errorf("cell generation is zero")
	}
	if current == nil {
		return 0, fmt.Errorf("current state is nil")
	}
	if isEmptyBlockID(origin) {
		return 0, fmt.Errorf("cell generation origin persistent state is empty")
	}
	if isEmptyBlockID(expectedCurrent) {
		return 0, fmt.Errorf("expected current state block is empty")
	}

	flushStarted := time.Now()
	if err := s.flushCellDBs(generation); err != nil {
		return 0, fmt.Errorf("flush generation %d before switch: %w", generation, err)
	}
	flushElapsed := time.Since(flushStarted)

	if err := s.syncPendingArtifactFiles(); err != nil {
		return 0, err
	}

	if err := s.verifyDurableCurrentMaster(ctx, expectedCurrent); err != nil {
		return 0, err
	}
	if err := s.verifyCurrentStateMetadataInGeneration(ctx, generation, current); err != nil {
		return 0, fmt.Errorf("verify candidate generation state metadata: %w", err)
	}

	deletedStateMetadata, err := s.DeleteStateMetadataMissingInGeneration(ctx, generation)
	if err != nil {
		return 0, fmt.Errorf("delete state metadata missing in generation %d: %w", generation, err)
	}

	oldGeneration, err := s.saveCellGenerationSwitch(ctx, generation, origin, expectedCurrent, current, flushElapsed, deletedStateMetadata)
	if err != nil {
		return 0, err
	}
	return oldGeneration, nil
}

func (s *Store) prepareBlockStatesForSave(ctx context.Context, states []*storage.BlockState, cells []storage.EncodedCellRecord) ([]preparedBlockStateSave, time.Duration, error) {
	cellGeneration, err := s.activeCellGenerationID()
	if err != nil {
		return nil, 0, err
	}
	return s.prepareBlockStatesForSaveInGeneration(ctx, cellGeneration, states, cells, false)
}

func (s *Store) prepareBlockStatesForSaveInGeneration(ctx context.Context, cellGeneration uint64, states []*storage.BlockState, cells []storage.EncodedCellRecord, strictGeneration bool) ([]preparedBlockStateSave, time.Duration, error) {
	if cellGeneration == 0 {
		return nil, 0, fmt.Errorf("cell generation is zero")
	}
	started := time.Now()
	prepared := make([]preparedBlockStateSave, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	trees := make([]stateCellTreeSave, 0, len(states))
	treePreparedIndexes := make([]int, 0, len(states))
	usePreparedCells := len(cells) > 0
	preparedCells := encodedCellRecordsByHash(cells)
	for _, state := range states {
		if state == nil {
			continue
		}
		key := storage.BlockKey(state.Block)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		saved, stateCellHash, err := prepareBlockStateHeader(state)
		if err != nil {
			return nil, 0, err
		}
		if saved.CellGeneration == 0 {
			saved.CellGeneration = cellGeneration
		}
		if strictGeneration && saved.CellGeneration != cellGeneration {
			return nil, 0, fmt.Errorf("block state %s belongs to cell generation %d, expected %d", storage.FormatBlockRef(saved.Block), saved.CellGeneration, cellGeneration)
		}

		next := preparedBlockStateSave{
			original:      state,
			saved:         saved,
			stateCellHash: stateCellHash,
		}
		if saved.Cell != nil {
			if saved.Cell.IsLazy() {
				next.lazyRoot = saved.Cell
			} else {
				next.stateCellHash = saved.Cell.HashKey()
				if !usePreparedCells {
					trees = append(trees, stateCellTreeSave{
						block:          saved.Block,
						root:           saved.Cell,
						cellGeneration: saved.CellGeneration,
					})
				}
				treePreparedIndexes = append(treePreparedIndexes, len(prepared))
			}
		}
		if usePreparedCells && next.lazyRoot == nil {
			if len(preparedCells[next.stateCellHash]) == 0 {
				return nil, 0, fmt.Errorf("checkpoint state %s root %x is not in prepared cells", storage.FormatBlockRef(saved.Block), next.stateCellHash)
			}
		}
		prepared = append(prepared, next)
	}

	preparedCellsFlushed, err := s.savePreparedStateCellRecords(ctx, cells, cellGeneration)
	if err != nil {
		return nil, 0, err
	}
	dfsFlushed, err := s.saveStateCellTreesDFSBatch(ctx, trees)
	if err != nil {
		return nil, 0, err
	}
	flushCells := preparedCellsFlushed || dfsFlushed
	if flushCells {
		for i := range prepared {
			prepared[i].flushCells = true
		}
	}
	for _, idx := range treePreparedIndexes {
		var lazyRoot *cell.Cell
		if usePreparedCells {
			lazyRoot, err = lazyPreparedStateCell(preparedCells, prepared[idx].stateCellHash, s.lazyCellLoaderForGeneration(prepared[idx].saved.CellGeneration))
			if err != nil {
				return nil, 0, fmt.Errorf("load prepared lazy state root: %w", err)
			}
		} else {
			lazyRoot, err = s.loadLazyCellFromGeneration(ctx, prepared[idx].saved.CellGeneration, prepared[idx].stateCellHash[:])
			if err != nil {
				return nil, 0, fmt.Errorf("load persisted lazy state root: %w", err)
			}
		}
		prepared[idx].lazyRoot = lazyRoot
	}
	if !usePreparedCells {
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
	}
	return prepared, time.Since(started), nil
}

func encodedCellRecordsByHash(records []storage.EncodedCellRecord) map[cell.Hash][]byte {
	if len(records) == 0 {
		return nil
	}

	byHash := make(map[cell.Hash][]byte, len(records))
	for _, record := range records {
		if len(record.Data) == 0 {
			continue
		}
		if _, exists := byHash[record.Hash]; exists {
			continue
		}
		byHash[record.Hash] = record.Data
	}
	return byHash
}

func lazyPreparedStateCell(records map[cell.Hash][]byte, hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	data := records[hash]
	if len(data) == 0 {
		return nil, storage.ErrNotFound
	}
	record, err := decodeCellRecord(hash[:], data)
	if err != nil {
		return nil, err
	}
	return storage.LazyCellRecord(record, loader)
}

func prepareBlockStateHeader(state *storage.BlockState) (storage.BlockState, cell.Hash, error) {
	var zero cell.Hash
	var stateCellHash cell.Hash
	if state == nil {
		return storage.BlockState{}, stateCellHash, fmt.Errorf("block state is nil")
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
		return storage.BlockState{}, stateCellHash, fmt.Errorf("block state root hash is empty")
	}
	if len(saved.StateCellHash) == 0 {
		return storage.BlockState{}, stateCellHash, fmt.Errorf("block state cell hash is empty")
	}
	if len(saved.StateCellHash) != len(zero) {
		return storage.BlockState{}, stateCellHash, fmt.Errorf("block state cell hash size mismatch: got %d", len(saved.StateCellHash))
	}
	copy(stateCellHash[:], saved.StateCellHash)
	return saved, stateCellHash, nil
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
		generations := make(map[uint64]struct{})
		for _, state := range prepared {
			if state.flushCells {
				generations[state.saved.CellGeneration] = struct{}{}
			}
		}
		for generation := range generations {
			if err := s.flushCellDBs(generation); err != nil {
				return fmt.Errorf("flush generation %d state cells before state metadata: %w", generation, err)
			}
		}
	}
	flushElapsed := time.Since(flushStarted)

	if err := s.syncPendingArtifactFiles(); err != nil {
		return err
	}

	db, err := s.acquireHotDB(context.Background())
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	var newActiveOrigin *ton.BlockIDExt

	if current != nil {
		if err := s.validateCurrentStateMetadata(db, current, prepared); err != nil {
			return err
		}
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}

	for _, state := range prepared {
		if err := batch.Set(hotKeyStateMeta(state.saved.Block), encodeBlockStateMeta(&state.saved), pebble.NoSync); err != nil {
			s.mu.Unlock()
			return err
		}
		if err := s.setMergedBlockMeta(batch, storage.BuildBlockMetaFromState(state.saved)); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	if current != nil {
		if err := batch.Set(hotKeyCurrentState(), encodeCurrentState(current), pebble.NoSync); err != nil {
			s.mu.Unlock()
			return err
		}
		if isEmptyBlockID(s.activeCellOrigin) {
			origin := current.Masterchain.Block
			manifest := s.manifestLocked()
			manifest.activeOrigin = origin
			if err := batch.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.NoSync); err != nil {
				s.mu.Unlock()
				return err
			}
			newActiveOrigin = &origin
		}
	}
	s.mu.Unlock()

	hotSyncStarted := time.Now()
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	if newActiveOrigin != nil {
		s.mu.Lock()
		s.activeCellOrigin = *newActiveOrigin
		s.mu.Unlock()
	}
	hotSyncElapsed := time.Since(hotSyncStarted)

	s.logBlockStateCheckpoint(prepared, current, flushCells, cellsElapsed, flushElapsed, hotSyncElapsed)
	return nil
}

func (s *Store) validateCurrentStateMetadata(db *pebble.DB, current *storage.CurrentState, prepared []preparedBlockStateSave) error {
	preparedByBlock := make(map[string]storage.BlockState, len(prepared))
	for _, state := range prepared {
		preparedByBlock[storage.BlockKey(state.saved.Block)] = state.saved
	}

	if err := s.validateCurrentStateBlockMetadata(db, "masterchain", current.Masterchain, preparedByBlock); err != nil {
		return err
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		if err := s.validateCurrentStateBlockMetadata(db, "shard", shard, preparedByBlock); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) validateCurrentStateBlockMetadata(db *pebble.DB, label string, current storage.BlockState, prepared map[string]storage.BlockState) error {
	if saved, ok := prepared[storage.BlockKey(current.Block)]; ok {
		return validateCurrentStateBlockMetaMatch(label, current, saved)
	}

	raw, err := pebbleReaderGetCopy(db, hotKeyStateMeta(current.Block))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("current %s state %s metadata is missing", label, storage.FormatBlockRef(current.Block))
		}
		return fmt.Errorf("load current %s state %s metadata: %w", label, storage.FormatBlockRef(current.Block), err)
	}
	rootHash, cellHash, _, _, err := decodeBlockStateMeta(raw)
	if err != nil {
		return fmt.Errorf("decode current %s state %s metadata: %w", label, storage.FormatBlockRef(current.Block), err)
	}
	if len(rootHash) == 0 {
		return fmt.Errorf("current %s state %s metadata is empty", label, storage.FormatBlockRef(current.Block))
	}

	return validateCurrentStateBlockMetaMatch(label, current, storage.BlockState{
		Block:         current.Block,
		StateRootHash: rootHash,
		StateCellHash: cellHash,
	})
}

func validateCurrentStateBlockMetaMatch(label string, current storage.BlockState, saved storage.BlockState) error {
	if len(current.StateRootHash) > 0 && !bytes.Equal(current.StateRootHash, saved.StateRootHash) {
		return fmt.Errorf("current %s state %s root hash mismatch: current=%x metadata=%x", label, storage.FormatBlockRef(current.Block), current.StateRootHash, saved.StateRootHash)
	}
	if len(current.StateCellHash) > 0 && !bytes.Equal(current.StateCellHash, saved.StateCellHash) {
		return fmt.Errorf("current %s state %s cell hash mismatch: current=%x metadata=%x", label, storage.FormatBlockRef(current.Block), current.StateCellHash, saved.StateCellHash)
	}
	return nil
}

func (s *Store) saveCellGenerationSwitch(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *storage.CurrentState, flushElapsed time.Duration, deletedStateMetadata int) (uint64, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return 0, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	var oldGeneration uint64
	var nextCellGeneration uint64
	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.closed {
			return errPebbleClosed
		}
		if s.cellGenerations[generation] == nil {
			return fmt.Errorf("cell generation %d is not open", generation)
		}
		oldGeneration = s.activeCellGeneration
		if generation == oldGeneration {
			return fmt.Errorf("cell generation %d is already active", generation)
		}
		nextCellGeneration = s.nextCellGeneration
		if nextCellGeneration <= generation {
			nextCellGeneration = generation + 1
		}
		durableCurrent, err := s.currentStateMasterBlockLocked()
		if err != nil {
			return fmt.Errorf("load durable current state before cell generation switch: %w", err)
		}
		if !durableCurrent.Equals(&expectedCurrent) {
			return fmt.Errorf("durable current state changed from %s to %s before cell generation switch: %w", storage.FormatBlockRef(expectedCurrent), storage.FormatBlockRef(durableCurrent), storage.ErrCurrentStateAdvanced)
		}

		if err := batch.Set(hotKeyCurrentState(), encodeCurrentState(current), pebble.NoSync); err != nil {
			return err
		}
		manifest := s.manifestLocked()
		manifest.active = generation
		manifest.next = nextCellGeneration
		manifest.activeOrigin = origin
		manifest.pending = nil
		manifest.retired = appendRetiredCellGeneration(manifest.retired, oldGeneration)
		return batch.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.NoSync)
	}(); err != nil {
		return 0, err
	}

	hotSyncStarted := time.Now()
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	hotSyncElapsed := time.Since(hotSyncStarted)

	s.mu.Lock()
	s.activeCellGeneration = generation
	s.activeCellOrigin = origin
	s.cells = s.cellGenerations[generation]
	s.pendingCellMigration = nil
	s.nextCellGeneration = nextCellGeneration
	s.retiredGenerations = appendRetiredCellGeneration(s.retiredGenerations, oldGeneration)
	s.mu.Unlock()

	s.log.Info().
		Uint64("old_cell_generation", oldGeneration).
		Uint64("cell_generation", generation).
		Str("origin_persistent_state", storage.FormatBlockRef(origin)).
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Int("shards", len(current.Shards)).
		Int("deleted_state_metadata_records", deletedStateMetadata).
		Dur("flush_cell_dbs_elapsed", flushElapsed).
		Dur("hot_metadata_sync_elapsed", hotSyncElapsed).
		Msg("cell generation switched")

	return oldGeneration, nil
}

func (s *Store) verifyDurableCurrentMaster(ctx context.Context, expectedCurrent ton.BlockIDExt) error {
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

	durableCurrent, err := s.currentStateMasterBlockLocked()
	if err != nil {
		return fmt.Errorf("load durable current state before cell generation switch: %w", err)
	}
	if !durableCurrent.Equals(&expectedCurrent) {
		return fmt.Errorf("durable current state changed from %s to %s before cell generation switch: %w", storage.FormatBlockRef(expectedCurrent), storage.FormatBlockRef(durableCurrent), storage.ErrCurrentStateAdvanced)
	}
	return nil
}

func (s *Store) currentStateMasterBlockLocked() (ton.BlockIDExt, error) {
	raw, err := pebbleReaderGetCopy(s.hot, hotKeyCurrentState())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	current, err := decodeCurrentState(raw)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return current.Masterchain.Block, nil
}

func (s *Store) verifyCurrentStateMetadataInGeneration(ctx context.Context, generation uint64, current *storage.CurrentState) error {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return err
	}
	defer cells.release()

	if err := s.verifyBlockStateMetadataInCellStore(ctx, cells, current.Masterchain); err != nil {
		return fmt.Errorf("masterchain %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		if err := s.verifyBlockStateMetadataInCellStore(ctx, cells, shard); err != nil {
			return fmt.Errorf("shard %s: %w", storage.FormatBlockRef(shard.Block), err)
		}
	}
	return nil
}

func (s *Store) verifyBlockStateMetadataInCellStore(ctx context.Context, cells *cellStore, state storage.BlockState) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	meta, err := s.blockStateMeta(ctx, state.Block)
	if err != nil {
		return fmt.Errorf("load state metadata: %w", err)
	}
	if len(state.StateRootHash) > 0 && !bytes.Equal(meta.StateRootHash, state.StateRootHash) {
		return fmt.Errorf("state root hash mismatch: metadata=%x current=%x", meta.StateRootHash, state.StateRootHash)
	}
	if len(state.StateCellHash) > 0 && !bytes.Equal(meta.StateCellHash, state.StateCellHash) {
		return fmt.Errorf("state cell hash mismatch: metadata=%x current=%x", meta.StateCellHash, state.StateCellHash)
	}

	exists, err := cells.has(meta.StateCellHash)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("state root cell %x is missing in candidate cell generation", meta.StateCellHash)
	}
	return nil
}

func (s *Store) DeleteStateMetadataMissingInGeneration(ctx context.Context, generation uint64) (int, error) {
	if generation == 0 {
		return 0, fmt.Errorf("cell generation is zero")
	}

	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return 0, err
	}
	defer cells.release()

	const batchLimit = 4096
	deleted := 0
	var start []byte
	for {
		keys, next, done, err := s.stateMetadataKeysMissingInCellStore(ctx, hotPrefixStateMeta, cells, start, batchLimit)
		if err != nil {
			return deleted, err
		}
		if len(keys) > 0 {
			if err = s.deleteHotRecords(ctx, keys, pebble.Sync); err != nil {
				return deleted, err
			}
			deleted += len(keys)
		}
		if done {
			break
		}
		start = next
	}
	return deleted, nil
}

func (s *Store) stateMetadataKeysMissingInCellStore(ctx context.Context, prefix []byte, cells *cellStore, start []byte, limit int) ([][]byte, []byte, bool, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	defer s.releaseHotDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = iter.Close() }()

	keys := make([][]byte, 0, limit)
	valid := iter.First()
	if len(start) > 0 {
		valid = iter.SeekGE(start)
	}

	for ; valid && bytes.HasPrefix(iter.Key(), prefix); valid = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		default:
		}

		cellHash, err := stateMetadataCellHash(prefix, iter.Value())
		if err != nil {
			return nil, nil, false, err
		}
		exists, err := cells.has(cellHash)
		if err != nil {
			return nil, nil, false, err
		}
		if exists {
			continue
		}

		key := bytes.Clone(iter.Key())
		keys = append(keys, key)
		if len(keys) >= limit {
			return keys, nextPebbleScanKey(key), false, nil
		}
	}
	if err := iter.Error(); err != nil {
		return nil, nil, false, err
	}
	return keys, nil, true, nil
}

func nextPebbleScanKey(key []byte) []byte {
	next := bytes.Clone(key)
	return append(next, 0)
}

func stateMetadataCellHash(prefix []byte, value []byte) ([]byte, error) {
	if bytes.Equal(prefix, hotPrefixStateMeta) {
		_, cellHash, _, _, err := decodeBlockStateMeta(value)
		return cellHash, err
	}
	return nil, fmt.Errorf("unsupported state metadata prefix %x", prefix)
}

func (s *Store) deleteHotRecords(ctx context.Context, keys [][]byte, writeOptions *pebble.WriteOptions) error {
	if len(keys) == 0 {
		return nil
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, key := range keys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := batch.Delete(key, pebble.NoSync); err != nil {
			return err
		}
	}
	return batch.Commit(writeOptions)
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

func isEmptyBlockID(block ton.BlockIDExt) bool {
	return block.Workchain == 0 && block.Shard == 0 && block.SeqNo == 0 && block.RootHash == nil && block.FileHash == nil
}
