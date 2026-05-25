package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
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
	raw, closer, err := pebbleReaderGet(db, key)
	if err == nil {
		defer func() { _ = closer.Close() }()
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
	stateRootHash cell.Hash
	flushCells    bool
}

func (s *Store) SaveBlockState(ctx context.Context, state *storage.BlockState) error {
	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, []*storage.BlockState{state}, storage.StateCheckpointCells{})
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, nil, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func (s *Store) SaveStateCheckpoint(ctx context.Context, blocks []*storage.BlockState, current *storage.CurrentState) error {
	return s.SaveStateCheckpointEntries(ctx, checkpointEntriesFromStates(blocks), current)
}

func (s *Store) SaveStateCheckpointEntries(ctx context.Context, blocks []storage.StateCheckpointBlock, current *storage.CurrentState) error {
	if current == nil {
		return fmt.Errorf("current state is nil")
	}

	states, cells := flattenStateCheckpointEntries(blocks)
	prepared, cellsElapsed, err := s.prepareBlockStatesForSave(ctx, states, cells)
	if err != nil {
		return err
	}
	if err = s.savePreparedBlockStateRecords(prepared, current, cellsElapsed); err != nil {
		return err
	}
	return s.replacePreparedBlockStatesWithLazyRoots(prepared)
}

func checkpointEntriesFromStates(states []*storage.BlockState) []storage.StateCheckpointBlock {
	if len(states) == 0 {
		return nil
	}
	entries := make([]storage.StateCheckpointBlock, 0, len(states))
	for _, state := range states {
		if state == nil {
			continue
		}
		entries = append(entries, storage.StateCheckpointBlock{State: state})
	}
	return entries
}

func flattenStateCheckpointEntries(entries []storage.StateCheckpointBlock) ([]*storage.BlockState, storage.StateCheckpointCells) {
	if len(entries) == 0 {
		return nil, storage.StateCheckpointCells{}
	}

	states := make([]*storage.BlockState, 0, len(entries))
	totalCells := 0
	for _, entry := range entries {
		if entry.State == nil {
			continue
		}
		states = append(states, entry.State)
		totalCells += len(entry.Cells)
	}

	if totalCells == 0 {
		return states, storage.StateCheckpointCells{}
	}
	cells := make([]storage.EncodedCellRecord, 0, totalCells)
	for _, entry := range entries {
		if entry.State == nil {
			continue
		}
		cells = append(cells, entry.Cells...)
	}
	return states, storage.StateCheckpointCells{Records: cells}
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

	if err := s.verifyCurrentStateRootsPresentInGeneration(ctx, generation, current); err != nil {
		return 0, fmt.Errorf("verify candidate generation current state roots: %w", err)
	}

	oldGeneration, err := s.saveCellGenerationSwitch(ctx, generation, origin, expectedCurrent, current)
	if err != nil {
		return 0, err
	}
	return oldGeneration, nil
}

func (s *Store) DeleteStateMetadataBeforeCellGenerationSwitch(ctx context.Context, origin ton.BlockIDExt, current *storage.CurrentState, preserveStateMeta []ton.BlockIDExt) (int, error) {
	if current == nil {
		return 0, fmt.Errorf("current state is nil")
	}
	if isEmptyBlockID(origin) {
		return 0, fmt.Errorf("cell generation origin persistent state is empty")
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return 0, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	started := time.Now()
	deleted, err := deleteStateMetadataBeforeImportedState(ctx, db, batch, origin, currentStateBlockKeepSet(current, preserveStateMeta))
	if err != nil {
		return deleted, err
	}
	if deleted == 0 {
		return 0, nil
	}

	syncStarted := time.Now()
	if err = batch.Commit(pebble.Sync); err != nil {
		return deleted, err
	}

	s.log.Info().
		Str("origin_persistent_state", storage.FormatBlockRef(origin)).
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Int("shards", len(current.Shards)).
		Int("deleted_state_metadata_records", deleted).
		Dur("elapsed", time.Since(started)).
		Dur("hot_metadata_sync_elapsed", time.Since(syncStarted)).
		Msg("deleted state metadata before cell generation switch")
	return deleted, nil
}

func (s *Store) prepareBlockStatesForSave(ctx context.Context, states []*storage.BlockState, cells storage.StateCheckpointCells) ([]preparedBlockStateSave, time.Duration, error) {
	cellGeneration, err := s.activeCellGenerationID()
	if err != nil {
		return nil, 0, err
	}
	return s.prepareBlockStatesForSaveInGeneration(ctx, cellGeneration, states, cells, false)
}

func (s *Store) prepareBlockStatesForSaveInGeneration(ctx context.Context, cellGeneration uint64, states []*storage.BlockState, cells storage.StateCheckpointCells, strictGeneration bool) ([]preparedBlockStateSave, time.Duration, error) {
	if cellGeneration == 0 {
		return nil, 0, fmt.Errorf("cell generation is zero")
	}
	started := time.Now()
	prepared := make([]preparedBlockStateSave, 0, len(states))
	seen := make(map[string]struct{}, len(states))
	trees := make([]stateCellTreeSave, 0, len(states))
	treePreparedIndexes := make([]int, 0, len(states))
	usePreparedCells := len(cells.Records) > 0
	for _, state := range states {
		if state == nil {
			continue
		}
		key := storage.BlockKey(state.Block)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		saved, stateRootHash, err := prepareBlockStateHeader(state)
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
			stateRootHash: stateRootHash,
		}
		if saved.Cell != nil {
			if saved.Cell.IsLazy() {
				next.lazyRoot = saved.Cell
			} else {
				next.stateRootHash = saved.Cell.HashKey(0)
				if !usePreparedCells {
					trees = append(trees, stateCellTreeSave{
						block:          saved.Block,
						root:           saved.Cell,
						cellGeneration: saved.CellGeneration,
					})
				} else {
					next.lazyRoot = saved.Cell
				}
				treePreparedIndexes = append(treePreparedIndexes, len(prepared))
			}
		}
		prepared = append(prepared, next)
	}

	preparedStats, err := s.savePreparedStateCellRecords(ctx, cells.Records, cellGeneration)
	if err != nil {
		return nil, 0, err
	}
	dfsStats, err := s.saveStateCellTreesDFSBatch(ctx, trees)
	if err != nil {
		return nil, 0, err
	}
	flushCells := preparedStats.applied || dfsStats.applied
	if flushCells {
		for i := range prepared {
			prepared[i].flushCells = true
		}
	}
	for _, idx := range treePreparedIndexes {
		if prepared[idx].lazyRoot != nil {
			continue
		}

		var lazyRoot *cell.Cell
		lazyRoot, err = s.loadLazyCellFromGeneration(ctx, prepared[idx].saved.CellGeneration, prepared[idx].stateRootHash[:])
		if err != nil {
			return nil, 0, fmt.Errorf("load persisted lazy state root: %w", err)
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

func prepareBlockStateHeader(state *storage.BlockState) (storage.BlockState, cell.Hash, error) {
	var zero cell.Hash
	var stateRootHash cell.Hash
	if state == nil {
		return storage.BlockState{}, stateRootHash, fmt.Errorf("block state is nil")
	}

	saved := *state
	if len(saved.StateRootHash) == 0 && saved.Cell != nil {
		hash := saved.Cell.HashKey(0)
		saved.StateRootHash = hash[:]
	}
	if len(saved.StateRootHash) == 0 {
		return storage.BlockState{}, stateRootHash, fmt.Errorf("block state root hash is empty")
	}
	if len(saved.StateRootHash) != len(zero) {
		return storage.BlockState{}, stateRootHash, fmt.Errorf("block state root hash size mismatch: got %d", len(saved.StateRootHash))
	}
	copy(stateRootHash[:], saved.StateRootHash)
	if saved.Cell != nil {
		cellRootHash := saved.Cell.HashKey(0)
		if cellRootHash != stateRootHash {
			return storage.BlockState{}, stateRootHash, fmt.Errorf("block state root hash mismatch: got=%x want=%x", cellRootHash[:], stateRootHash[:])
		}
		cellHash := saved.Cell.HashKey()
		if !saved.Cell.IsVirtualized() && cellHash != stateRootHash {
			return storage.BlockState{}, stateRootHash, fmt.Errorf("block state root is not a level-0 state root: root=%x cell=%x", stateRootHash[:], cellHash[:])
		}
	}
	return saved, stateRootHash, nil
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

	// Durability order is deliberate: write and flush celldb first, then publish
	// hot metadata. If the process dies between the two, orphan cells are fine;
	// metadata still points at the previous durable state.
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

	raw, closer, err := pebbleReaderGet(db, hotKeyStateMeta(current.Block))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("current %s state %s metadata is missing", label, storage.FormatBlockRef(current.Block))
		}
		return fmt.Errorf("load current %s state %s metadata: %w", label, storage.FormatBlockRef(current.Block), err)
	}
	defer func() { _ = closer.Close() }()
	rootHash, _, _, err := decodeBlockStateMeta(raw)
	if err != nil {
		return fmt.Errorf("decode current %s state %s metadata: %w", label, storage.FormatBlockRef(current.Block), err)
	}
	if len(rootHash) == 0 {
		return fmt.Errorf("current %s state %s metadata is empty", label, storage.FormatBlockRef(current.Block))
	}

	return validateCurrentStateBlockMetaMatch(label, current, storage.BlockState{
		Block:         current.Block,
		StateRootHash: rootHash,
	})
}

func validateCurrentStateBlockMetaMatch(label string, current storage.BlockState, saved storage.BlockState) error {
	if len(current.StateRootHash) > 0 && !bytes.Equal(current.StateRootHash, saved.StateRootHash) {
		return fmt.Errorf("current %s state %s root hash mismatch: current=%x metadata=%x", label, storage.FormatBlockRef(current.Block), current.StateRootHash, saved.StateRootHash)
	}
	return nil
}

func (s *Store) saveCellGenerationSwitch(ctx context.Context, generation uint64, origin ton.BlockIDExt, expectedCurrent ton.BlockIDExt, current *storage.CurrentState) (uint64, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return 0, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err = verifyCellGenerationSwitchCurrent(db, expectedCurrent, current); err != nil {
		return 0, err
	}
	if err = verifyCellGenerationSwitchProgress(db, generation, current); err != nil {
		return 0, err
	}

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
		if err := batch.Delete(hotKeyCellGenerationCurrent(generation), pebble.NoSync); err != nil {
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
		Dur("hot_metadata_sync_elapsed", hotSyncElapsed).
		Msg("cell generation switched")

	return oldGeneration, nil
}

func currentStateBlockKeepSet(current *storage.CurrentState, additional []ton.BlockIDExt) map[string]struct{} {
	if current == nil {
		return nil
	}

	keep := make(map[string]struct{}, 1+len(current.Shards)+len(additional))
	keep[storage.BlockKey(current.Masterchain.Block)] = struct{}{}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		keep[storage.BlockKey(shard.Block)] = struct{}{}
	}
	for _, block := range additional {
		keep[storage.BlockKey(block)] = struct{}{}
	}
	return keep
}

func verifyCellGenerationSwitchCurrent(db *pebble.DB, expectedMaster ton.BlockIDExt, current *storage.CurrentState) error {
	if current == nil {
		return fmt.Errorf("current state is nil")
	}
	if !current.Masterchain.Block.Equals(&expectedMaster) {
		return fmt.Errorf("candidate current state %s does not match expected switch current %s", storage.FormatBlockRef(current.Masterchain.Block), storage.FormatBlockRef(expectedMaster))
	}

	raw, closer, err := pebbleReaderGet(db, hotKeyCurrentState())
	if err != nil {
		return fmt.Errorf("load durable current state before cell generation switch: %w", err)
	}
	defer func() { _ = closer.Close() }()
	durable, err := decodeCurrentState(raw)
	if err != nil {
		return fmt.Errorf("decode durable current state before cell generation switch: %w", err)
	}
	if !durable.Masterchain.Block.Equals(&expectedMaster) {
		return fmt.Errorf("durable current state changed from %s to %s before cell generation switch: %w", storage.FormatBlockRef(expectedMaster), storage.FormatBlockRef(durable.Masterchain.Block), storage.ErrCurrentStateAdvanced)
	}
	if !currentStateBlockRefsEqual(durable, current) {
		return fmt.Errorf("durable current state no longer matches candidate before cell generation switch: %w", storage.ErrCurrentStateAdvanced)
	}
	if err := verifyCellGenerationSwitchStateMetadata(db, current); err != nil {
		return err
	}
	return nil
}

func verifyCellGenerationSwitchProgress(db *pebble.DB, generation uint64, current *storage.CurrentState) error {
	raw, closer, err := pebbleReaderGet(db, hotKeyCellGenerationCurrent(generation))
	if err != nil {
		return fmt.Errorf("load durable cell generation migration progress before switch: %w", err)
	}
	defer func() { _ = closer.Close() }()

	progress, err := decodeCellGenerationMigrationProgress(raw)
	if err != nil {
		return fmt.Errorf("decode durable cell generation migration progress before switch: %w", err)
	}
	if !currentStateBlockRefsEqual(progress, current) {
		return fmt.Errorf("durable cell generation migration progress no longer matches switch current")
	}
	if !currentStateRootsEqual(progress, current) {
		return fmt.Errorf("durable cell generation migration progress roots no longer match switch current")
	}
	return nil
}

func verifyCellGenerationSwitchStateMetadata(db *pebble.DB, current *storage.CurrentState) error {
	if err := verifyCellGenerationSwitchBlockStateMetadata(db, "masterchain", current.Masterchain); err != nil {
		return err
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		if err := verifyCellGenerationSwitchBlockStateMetadata(db, "shard", shard); err != nil {
			return err
		}
	}
	return nil
}

func verifyCellGenerationSwitchBlockStateMetadata(db *pebble.DB, label string, current storage.BlockState) error {
	raw, closer, err := pebbleReaderGet(db, hotKeyStateMeta(current.Block))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("durable current %s state %s metadata is missing", label, storage.FormatBlockRef(current.Block))
		}
		return fmt.Errorf("load durable current %s state %s metadata: %w", label, storage.FormatBlockRef(current.Block), err)
	}
	defer func() { _ = closer.Close() }()

	rootHash, _, _, err := decodeBlockStateMeta(raw)
	if err != nil {
		return fmt.Errorf("decode durable current %s state %s metadata: %w", label, storage.FormatBlockRef(current.Block), err)
	}
	return validateCurrentStateBlockMetaMatch(label, current, storage.BlockState{
		Block:         current.Block,
		StateRootHash: rootHash,
	})
}

func currentStateBlockRefsEqual(left *storage.CurrentState, right *storage.CurrentState) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.ShardClientSeqno != right.ShardClientSeqno {
		return false
	}
	if !left.Masterchain.Block.Equals(&right.Masterchain.Block) {
		return false
	}
	if len(left.Shards) != len(right.Shards) {
		return false
	}
	for _, key := range storage.SortedShardKeys(left.Shards) {
		leftShard := left.Shards[key]
		rightShard, ok := right.Shards[key]
		if !ok {
			return false
		}
		if !leftShard.Block.Equals(&rightShard.Block) {
			return false
		}
	}
	return true
}

func currentStateRootsEqual(left *storage.CurrentState, right *storage.CurrentState) bool {
	if left == nil || right == nil {
		return left == right
	}
	if !bytes.Equal(left.Masterchain.StateRootHash, right.Masterchain.StateRootHash) {
		return false
	}
	if len(left.Shards) != len(right.Shards) {
		return false
	}
	for _, key := range storage.SortedShardKeys(left.Shards) {
		leftShard := left.Shards[key]
		rightShard, ok := right.Shards[key]
		if !ok {
			return false
		}
		if !bytes.Equal(leftShard.StateRootHash, rightShard.StateRootHash) {
			return false
		}
	}
	return true
}

func deleteStateMetadataBeforeImportedState(ctx context.Context, db *pebble.DB, batch *pebble.Batch, origin ton.BlockIDExt, keep map[string]struct{}) (int, error) {
	if !isMasterchainBlock(origin) {
		return 0, fmt.Errorf("cell generation origin is not masterchain: %s", storage.FormatBlockRef(origin))
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: hotPrefixStateMeta,
		UpperBound: prefixUpperBound(hotPrefixStateMeta),
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	deleted := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return deleted, ctx.Err()
		default:
		}

		block, err := decodeStateMetaBlockKey(iter.Key())
		if err != nil {
			return deleted, err
		}
		if _, ok := keep[storage.BlockKey(block)]; ok {
			continue
		}
		_, _, masterRef, err := decodeBlockStateMeta(iter.Value())
		if err != nil {
			return deleted, fmt.Errorf("decode state metadata %s: %w", storage.FormatBlockRef(block), err)
		}
		if !stateMetadataBeforeImportedState(block, masterRef, origin) {
			continue
		}

		if err = batch.Delete(bytes.Clone(iter.Key()), pebble.NoSync); err != nil {
			return deleted, err
		}
		deleted++
	}
	if err := iter.Error(); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func stateMetadataBeforeImportedState(block ton.BlockIDExt, masterRef *ton.BlockIDExt, origin ton.BlockIDExt) bool {
	if isMasterchainBlock(block) {
		return block.SeqNo < origin.SeqNo
	}
	if masterRef == nil {
		return false
	}
	if !isMasterchainBlock(*masterRef) {
		return false
	}
	return masterRef.SeqNo < origin.SeqNo
}

func decodeStateMetaBlockKey(key []byte) (ton.BlockIDExt, error) {
	if len(key) != len(hotPrefixStateMeta)+80 {
		return ton.BlockIDExt{}, fmt.Errorf("invalid state metadata key size %d", len(key))
	}
	return decodeBlockID(key[len(hotPrefixStateMeta):])
}

func (s *Store) verifyCurrentStateRootsPresentInGeneration(ctx context.Context, generation uint64, current *storage.CurrentState) error {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return err
	}
	defer cells.release()

	if dirty := cells.dirtyShardsSnapshot(); len(dirty) > 0 {
		return fmt.Errorf("cell generation %d has unflushed cell shards: %v", generation, dirty)
	}

	if err := s.verifyBlockStateRootPresentInCellStore(ctx, cells, current.Masterchain); err != nil {
		return fmt.Errorf("masterchain %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		if err := s.verifyBlockStateRootPresentInCellStore(ctx, cells, shard); err != nil {
			return fmt.Errorf("shard %s: %w", storage.FormatBlockRef(shard.Block), err)
		}
	}
	return nil
}

func (s *Store) verifyBlockStateRootPresentInCellStore(ctx context.Context, cells *cellStore, state storage.BlockState) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(state.StateRootHash) != 32 {
		return fmt.Errorf("state root hash has size %d", len(state.StateRootHash))
	}

	exists, err := cells.has(state.StateRootHash)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("state root cell %x is missing in candidate cell generation", state.StateRootHash)
	}
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

func isEmptyBlockID(block ton.BlockIDExt) bool {
	return block.Workchain == 0 && block.Shard == 0 && block.SeqNo == 0 && block.RootHash == nil && block.FileHash == nil
}
