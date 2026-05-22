package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

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
		if closeErr := cells.close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close celldb generation %d: %w", id, closeErr))
		}
	}
	return err
}

func (s *Store) getCellCopy(ctx context.Context, hash []byte) ([]byte, error) {
	return s.getCellCopyFromGeneration(ctx, 0, hash)
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

func (s *Store) activeCellStoreLocked() (*cellStore, error) {
	return s.cellStoreForGenerationLocked(0)
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

func (s *Store) CellGenerationStateImported(ctx context.Context, generation uint64, block ton.BlockIDExt) error {
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}
	if isEmptyBlockID(block) {
		return fmt.Errorf("cell generation imported state block is empty")
	}

	raw, err := s.getHotCopy(ctx, hotKeyCellGenerationStateImport(generation, block))
	if err != nil {
		return err
	}
	if len(raw) != 1 || raw[0] != cellGenerationStateImportVersion {
		return fmt.Errorf("unsupported cell generation state import marker")
	}
	return nil
}

func (s *Store) MarkCellGenerationStateImported(ctx context.Context, generation uint64, block ton.BlockIDExt) error {
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}
	if isEmptyBlockID(block) {
		return fmt.Errorf("cell generation imported state block is empty")
	}

	return s.setHotRecord(ctx, hotKeyCellGenerationStateImport(generation, block), []byte{cellGenerationStateImportVersion}, pebble.Sync)
}

func (s *Store) CellGenerationMigrationProgress(ctx context.Context, generation uint64) (*storage.CurrentState, error) {
	if generation == 0 {
		return nil, fmt.Errorf("cell generation is zero")
	}

	return s.currentStateRecord(ctx, hotKeyCellGenerationCurrent(generation), "cell generation migration progress")
}

func (s *Store) SaveCellGenerationMigrationProgress(ctx context.Context, generation uint64, current *storage.CurrentState) error {
	if generation == 0 {
		return fmt.Errorf("cell generation is zero")
	}
	if current == nil {
		return fmt.Errorf("cell generation migration progress is nil")
	}

	return s.setHotRecord(ctx, hotKeyCellGenerationCurrent(generation), encodeCurrentState(current), pebble.Sync)
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
	if err := s.deleteCellGenerationStateImportMarkers(ctx, generation); err != nil {
		return err
	}
	if err := s.closeAndRemoveCellGeneration(ctx, generation, false); err != nil {
		return err
	}
	return s.clearPendingCellGeneration(ctx, generation)
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
	delete(s.cellGenerations, generation)
	s.pendingCellMigration = nil
	manifest := s.manifestLocked()
	s.mu.Unlock()

	s.log.Info().
		Uint64("cell_generation", generation).
		Msg("detached pending cell generation migration")

	go s.cleanupDroppedPendingCellGeneration(generation, manifest, cells)
	return nil
}

func (s *Store) cleanupDroppedPendingCellGeneration(generation uint64, manifest cellGenerationManifest, cells *cellStore) {
	s.removeDetachedCellGeneration(generation, cells)
	if err := s.deleteDroppedPendingCellGenerationMetadata(generation, manifest); err != nil {
		s.log.Warn().
			Err(err).
			Uint64("cell_generation", generation).
			Msg("failed to delete dropped pending cell generation metadata")
	}
}

func (s *Store) deleteDroppedPendingCellGenerationMetadata(generation uint64, manifest cellGenerationManifest) error {
	if err := s.ensureWritable(); err != nil {
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
	if err := batch.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Delete(hotKeyCellGenerationCurrent(generation), pebble.NoSync); err != nil {
		return err
	}
	importPrefix := hotKeyCellGenerationStateImportPrefix(generation)
	if err := batch.DeleteRange(importPrefix, prefixUpperBound(importPrefix), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (s *Store) DeleteCellGeneration(ctx context.Context, generation uint64) error {
	if err := s.deleteCellGenerationMigrationProgress(ctx, generation); err != nil {
		return err
	}
	if err := s.deleteCellGenerationStateImportMarkers(ctx, generation); err != nil {
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
	closeErr := cells.closeAggressively()
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
	if err := cells.close(); err != nil {
		errs = append(errs, err)
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

func (s *Store) deleteCellGenerationStateImportMarkers(ctx context.Context, generation uint64) error {
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

	prefix := hotKeyCellGenerationStateImportPrefix(generation)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	for valid := iter.First(); valid; valid = iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := batch.Delete(iter.Key(), pebble.NoSync); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
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
