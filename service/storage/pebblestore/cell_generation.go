package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) closeCellGenerations() error {
	var err error
	for id, cells := range s.cellGenerations {
		if cells == nil {
			continue
		}
		started := time.Now()
		refs := cells.refs.Load()
		event := s.log.Info()
		if refs > 0 {
			event = s.log.Warn()
		}
		event.
			Uint64("cell_generation", id).
			Int64("refs", refs).
			Msg("closing celldb generation")
		logger := s.log.With().Uint64("cell_generation", id).Logger()
		if closeErr := cells.closeWithLogger(logger); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close celldb generation %d: %w", id, closeErr))
		}
		s.log.Info().
			Uint64("cell_generation", id).
			Dur("elapsed", time.Since(started)).
			Msg("closed celldb generation")
	}
	return err
}

func (s *Store) getCellCopyFromGeneration(ctx context.Context, generation uint64, hash []byte) ([]byte, error) {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return nil, err
	}
	defer cells.release()
	return cells.getCopy(hash)
}

func (s *Store) acquireCellStore(ctx context.Context, generation uint64) (*cellStore, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errPebbleClosed
	}
	cells, err := s.cellStoreForGenerationLocked(generation)
	if err != nil {
		return nil, err
	}
	if err = cells.acquire(); err != nil {
		return nil, err
	}
	return cells, nil
}

func (s *Store) ThrottleCellCompactions() func() {
	s.mu.RLock()
	releases := make([]func(), 0, len(s.cellGenerations))
	for _, cells := range s.cellGenerations {
		releases = append(releases, cells.throttleCompactions())
	}
	s.mu.RUnlock()

	if len(releases) == 0 {
		return func() {}
	}

	s.log.Info().
		Int("cell_generations", len(releases)).
		Msg("throttled cell compactions for foreground read")

	var once sync.Once
	return func() {
		once.Do(func() {
			for _, release := range releases {
				release()
			}
			s.log.Info().
				Int("cell_generations", len(releases)).
				Msg("resumed cell compactions after foreground read")
		})
	}
}

func (s *Store) ThrottleCellGenerationCompactions(ctx context.Context, generation uint64) (func(), error) {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return nil, err
	}

	releaseThrottle := cells.throttleCompactions()
	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("throttled cell generation compactions for foreground migration")

	var once sync.Once
	return func() {
		once.Do(func() {
			releaseThrottle()
			cells.release()
			s.log.Info().
				Uint64("cell_generation", generation).
				Msg("resumed cell generation compactions after foreground migration")
		})
	}, nil
}

func (s *Store) flushCellDBs(generation uint64) error {
	cells, err := s.acquireCellStore(context.Background(), generation)
	if err != nil {
		return err
	}
	defer cells.release()
	return cells.flush()
}

func (s *Store) cellStoreForGenerationLocked(generation uint64) (*cellStore, error) {
	if generation == 0 {
		generation = s.activeCellGeneration
	}
	cells := s.cellGenerations[generation]
	if cells == nil {
		return nil, fmt.Errorf("%w: %d", errCellGenerationNotOpen, generation)
	}
	return cells, nil
}

func (s *Store) activeCellGenerationID() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, errPebbleClosed
	}
	if s.activeCellGeneration == 0 {
		return 0, fmt.Errorf("active cell generation is zero")
	}
	return s.activeCellGeneration, nil
}

func (s *Store) ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error) {
	select {
	case <-ctx.Done():
		return storage.CellGenerationInfo{}, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return storage.CellGenerationInfo{}, errPebbleClosed
	}
	if s.activeCellGeneration == 0 {
		return storage.CellGenerationInfo{}, fmt.Errorf("active cell generation is zero")
	}
	return storage.CellGenerationInfo{
		ID:                    s.activeCellGeneration,
		OriginPersistentState: s.activeCellOrigin,
	}, nil
}

func (s *Store) PendingCellGenerationMigration(ctx context.Context) (storage.CellGenerationInfo, error) {
	select {
	case <-ctx.Done():
		return storage.CellGenerationInfo{}, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return storage.CellGenerationInfo{}, errPebbleClosed
	}
	if s.pendingCellMigration == nil {
		return storage.CellGenerationInfo{}, storage.ErrNotFound
	}
	return storage.CellGenerationInfo{
		ID:                    s.pendingCellMigration.generation,
		OriginPersistentState: s.pendingCellMigration.origin,
	}, nil
}

func (s *Store) CellGenerationMigrationProgress(ctx context.Context, generation uint64) (*storage.CurrentState, error) {
	if generation == 0 {
		return nil, fmt.Errorf("cell generation is zero")
	}

	raw, err := s.getHotCopy(ctx, hotKeyCellGenerationCurrent(generation))
	if err != nil {
		return nil, err
	}
	return decodeCellGenerationMigrationProgress(raw)
}

func (s *Store) SaveCellGenerationMigrationProgress(ctx context.Context, generation uint64, current *storage.CurrentState) error {
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}

	current, err := s.cellGenerationMigrationProgressForEncode(ctx, current)
	if err != nil {
		return err
	}
	if err := validateMigrationProgressBlockIDs(current); err != nil {
		return err
	}
	return s.setHotRecord(ctx, hotKeyCellGenerationCurrent(generation), encodeCellGenerationMigrationProgress(current), pebble.Sync)
}

func (s *Store) cellGenerationMigrationProgressForEncode(ctx context.Context, current *storage.CurrentState) (*storage.CurrentState, error) {
	encoded := storage.CloneCurrentState(current)
	if err := s.resolveMigrationProgressBlockMasterRef(ctx, &encoded.Masterchain); err != nil {
		return nil, fmt.Errorf("resolve migration progress masterchain state ref: %w", err)
	}
	for key, shard := range encoded.Shards {
		if err := s.resolveMigrationProgressBlockMasterRef(ctx, &shard); err != nil {
			return nil, fmt.Errorf("resolve migration progress shard state %s ref: %w", storage.FormatBlockRef(shard.Block), err)
		}
		encoded.Shards[key] = shard
	}
	return encoded, nil
}

func validateMigrationProgressBlockIDs(current *storage.CurrentState) error {
	if err := validateMigrationProgressBlockStateID(current.Masterchain); err != nil {
		return err
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		if err := validateMigrationProgressBlockStateID(shard); err != nil {
			return err
		}
	}
	return nil
}

func validateMigrationProgressBlockStateID(state storage.BlockState) error {
	if err := storage.ValidateBlockIDHashes(state.Block); err != nil {
		return err
	}
	if state.MasterchainRef != nil {
		if err := storage.ValidateBlockIDHashes(*state.MasterchainRef); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) resolveMigrationProgressBlockMasterRef(ctx context.Context, state *storage.BlockState) error {
	if state == nil || isMasterchainBlock(state.Block) {
		return nil
	}
	if state.MasterchainRef == nil {
		return fmt.Errorf("shard state %s has no masterchain ref", storage.FormatBlockRef(state.Block))
	}

	ref := *state.MasterchainRef
	if !isMasterchainBlock(ref) {
		return fmt.Errorf("shard state %s masterchain ref is not masterchain: %s", storage.FormatBlockRef(state.Block), storage.FormatBlockRef(ref))
	}
	if storage.BlockIDHashesKnown(ref) {
		return nil
	}

	resolved, err := s.lookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: -1, Shard: topShard, SeqNo: ref.SeqNo})
	if err != nil {
		return fmt.Errorf("lookup masterchain ref #%d: %w", ref.SeqNo, err)
	}
	if !storage.BlockIDHashesKnown(resolved) {
		return fmt.Errorf("resolved masterchain ref is not full: %s", storage.FormatBlockRef(resolved))
	}
	state.MasterchainRef = &resolved
	return nil
}

func (s *Store) BeginCellGeneration(ctx context.Context, origin ton.BlockIDExt) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if err := s.ensureWritable(); err != nil {
		return 0, err
	}
	if err := storage.ValidateBlockIDHashes(origin); err != nil {
		return 0, err
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return 0, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.hotWriteMu.Unlock()
		return 0, errPebbleClosed
	}
	if s.pendingCellMigration != nil {
		pending := *s.pendingCellMigration
		alreadyOpen := s.cellGenerations[pending.generation] != nil
		s.mu.Unlock()
		s.hotWriteMu.Unlock()

		if !pending.origin.Equals(&origin) {
			return 0, fmt.Errorf("pending cell generation migration uses origin %s, requested %s", storage.FormatBlockRef(pending.origin), storage.FormatBlockRef(origin))
		}
		if alreadyOpen {
			return pending.generation, nil
		}
		if err := s.openCellGeneration(ctx, pending.generation); err != nil {
			return 0, err
		}
		return pending.generation, nil
	}

	generation := s.nextCellGeneration
	if generation <= s.activeCellGeneration {
		s.mu.Unlock()
		s.hotWriteMu.Unlock()
		return 0, fmt.Errorf("next cell generation %d is not above active %d", generation, s.activeCellGeneration)
	}
	s.nextCellGeneration++
	pending := &cellGenerationPendingMigration{
		generation: generation,
		origin:     origin,
	}
	manifest := cellGenerationManifest{
		active:       s.activeCellGeneration,
		next:         s.nextCellGeneration,
		activeOrigin: s.activeCellOrigin,
		pending:      pending,
		retired:      cloneUint64Slice(s.retiredGenerations),
	}
	s.mu.Unlock()

	if err := db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		s.mu.Lock()
		s.nextCellGeneration--
		s.mu.Unlock()
		s.hotWriteMu.Unlock()
		return 0, err
	}

	s.mu.Lock()
	s.pendingCellMigration = cloneCellGenerationPendingMigration(pending)
	s.mu.Unlock()
	s.hotWriteMu.Unlock()

	if err := s.openCellGeneration(ctx, generation); err != nil {
		return 0, err
	}
	return generation, nil
}

func (s *Store) openCellGeneration(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	cells, err := openCellStore(s.dir, generation, s.fs, s.cellCacheSize, s.cellShardMemTable, s.cellMemTableStopWritesThreshold, s.bytesPerSync, s.readOnly, s.log)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = cells.close()
		return errPebbleClosed
	}
	if s.cellGenerations[generation] != nil {
		_ = cells.close()
		return nil
	}
	s.cellGenerations[generation] = cells

	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("opened cell generation")
	return nil
}

func (s *Store) AbortCellGeneration(ctx context.Context, generation uint64) error {
	if err := s.deleteCellGenerationMigrationProgress(ctx, generation); err != nil {
		return err
	}
	if err := s.clearPendingCellGeneration(ctx, generation); err != nil {
		return err
	}
	return s.closeAndRemoveCellGeneration(ctx, generation, false)
}

func (s *Store) DropPendingCellGeneration(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.ensureWritable(); err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}
	if generation == s.activeCellGeneration {
		s.mu.Unlock()
		return fmt.Errorf("cannot drop active cell generation %d", generation)
	}
	if s.pendingCellMigration == nil || s.pendingCellMigration.generation != generation {
		s.mu.Unlock()
		return nil
	}

	cells := s.cellGenerations[generation]
	manifest := s.manifestLocked()
	manifest.pending = nil
	s.mu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Delete(hotKeyCellGenerationCurrent(generation), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	s.mu.Lock()
	detached := false
	if s.pendingCellMigration != nil && s.pendingCellMigration.generation == generation {
		delete(s.cellGenerations, generation)
		s.pendingCellMigration = nil
		detached = true
	}
	s.mu.Unlock()

	if !detached {
		return nil
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("detached pending cell generation migration")

	go s.removeDetachedCellGeneration(generation, cells)
	return nil
}

func (s *Store) DeleteCellGeneration(ctx context.Context, generation uint64) error {
	if err := s.deleteCellGenerationMigrationProgress(ctx, generation); err != nil {
		return err
	}
	if err := s.closeAndRemoveCellGeneration(ctx, generation, false); err != nil {
		return err
	}
	return s.removeRetiredCellGeneration(ctx, generation)
}

func (s *Store) CleanupCellGeneration(ctx context.Context, generation uint64) error {
	if err := s.DeleteCellGeneration(ctx, generation); err != nil {
		return err
	}
	return nil
}

func (s *Store) CleanupRetiredCellGenerations(ctx context.Context) error {
	retired := s.retiredCellGenerationSnapshot()
	for _, generation := range retired {
		if err := s.CleanupCellGeneration(ctx, generation); err != nil {
			return fmt.Errorf("cleanup retired cell generation %d: %w", generation, err)
		}
	}
	return nil
}

func (s *Store) removeDetachedCellGeneration(generation uint64, cells *cellStore) {
	var closeErr error
	if cells != nil {
		closeErr = cells.closeAggressively()
	}
	removeErr := s.removeCellGenerationDirs(generation)
	if err := errors.Join(closeErr, removeErr); err != nil {
		s.log.Warn().
			Err(err).
			Uint64("cell_generation", generation).
			Msg("failed to remove detached cell generation")
		return
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("removed detached cell generation")
}

func (s *Store) removeCellGenerationDirs(generation uint64) error {
	var err error
	for shard := 0; shard < cellDBShardCount; shard++ {
		if removeErr := os.RemoveAll(cellGenerationShardDir(s.dir, generation, shard)); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}

func (s *Store) closeAndRemoveCellGeneration(ctx context.Context, generation uint64, allowActive bool) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.ensureWritable(); err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}
	if !allowActive && generation == s.activeCellGeneration {
		s.mu.Unlock()
		return fmt.Errorf("cannot remove active cell generation %d", generation)
	}
	cells := s.cellGenerations[generation]
	if cells != nil {
		delete(s.cellGenerations, generation)
	}
	s.mu.Unlock()

	var errs []error
	if cells != nil {
		if err := cells.close(); err != nil {
			errs = append(errs, err)
		}
	}
	for shard := 0; shard < cellDBShardCount; shard++ {
		if err := os.RemoveAll(cellGenerationShardDir(s.dir, generation, shard)); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("removed cell generation")
	return nil
}

func (s *Store) deleteCellGenerationMigrationProgress(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.ensureWritable(); err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}

	return s.deleteHotRecord(ctx, hotKeyCellGenerationCurrent(generation), pebble.Sync)
}

func (s *Store) retiredCellGenerationSnapshot() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneUint64Slice(s.retiredGenerations)
}

func (s *Store) clearPendingCellGeneration(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}
	if s.pendingCellMigration == nil || s.pendingCellMigration.generation != generation {
		s.mu.Unlock()
		return nil
	}

	manifest := s.manifestLocked()
	manifest.pending = nil
	s.mu.Unlock()

	if err := db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		return err
	}

	s.mu.Lock()
	if s.pendingCellMigration != nil && s.pendingCellMigration.generation == generation {
		s.pendingCellMigration = nil
	}
	s.mu.Unlock()
	return nil
}

func (s *Store) removeRetiredCellGeneration(ctx context.Context, generation uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errPebbleClosed
	}
	if !containsUint64(s.retiredGenerations, generation) {
		s.mu.Unlock()
		return nil
	}

	manifest := s.manifestLocked()
	manifest.retired = removeUint64(manifest.retired, generation)
	s.mu.Unlock()

	if err := db.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		return err
	}

	s.mu.Lock()
	s.retiredGenerations = removeUint64(s.retiredGenerations, generation)
	s.mu.Unlock()
	return nil
}
