package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

var (
	errPebbleClosed          = errors.New("pebble storage is closed")
	errCellGenerationNotOpen = errors.New("cell generation is not open")
)

type Store struct {
	log zerolog.Logger

	hot                             *pebble.DB
	cells                           *cellStore
	cellGenerations                 map[uint64]*cellStore
	activeCellGeneration            uint64
	activeCellOrigin                ton.BlockIDExt
	pendingCellMigration            *cellGenerationPendingMigration
	retiredGenerations              []uint64
	nextCellGeneration              uint64
	cellCache                       *decodedCellCache
	dir                             string
	cellCacheSize                   int64
	cellShardMemTable               int
	cellMemTableStopWritesThreshold int
	artifactFiles                   *artifactFileCache
	bytesPerSync                    int
	fs                              vfs.FS
	hotOpts                         *pebble.Options
	hotCache                        *pebble.Cache
	readOnly                        bool
	hotWriteMu                      sync.Mutex
	hotClosing                      atomic.Bool
	hotRefs                         atomic.Int64
	hotDrained                      chan struct{}
	hotDrainOnce                    sync.Once

	mu                  sync.RWMutex
	artifactMu          sync.Mutex
	artifactSyncSeq     uint64
	pendingArchiveSync  map[string]uint64
	pendingKeyProofSync map[string]uint64
	dirtyArchivePacks   map[string]struct{}
	dirtyKeyProofPacks  map[string]struct{}
	closed              bool
}

func (s *Store) Close() error {
	var firstErr error
	if err := s.syncPendingArtifactFiles(); err != nil && firstErr == nil {
		firstErr = err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return firstErr
	}
	s.closed = true
	s.hotClosing.Store(true)
	if s.hotRefs.Load() == 0 {
		s.signalHotDrained()
	}
	s.mu.Unlock()

	<-s.hotDrained
	if err := s.hot.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.closeCellGenerations(); err != nil && firstErr == nil {
		firstErr = err
	}
	if s.artifactFiles != nil {
		if err := s.artifactFiles.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.mu.Lock()
	s.cells = nil
	s.cellGenerations = nil
	s.mu.Unlock()
	if s.hotCache != nil {
		s.hotCache.Unref()
	}
	return firstErr
}

func (s *Store) acquireHotDB(ctx context.Context) (*pebble.DB, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.hotClosing.Load() || s.hot == nil {
		return nil, errPebbleClosed
	}
	s.hotRefs.Add(1)
	return s.hot, nil
}

func (s *Store) releaseHotDB() {
	if s.hotRefs.Add(-1) == 0 && s.hotClosing.Load() {
		s.signalHotDrained()
	}
}

func (s *Store) signalHotDrained() {
	s.hotDrainOnce.Do(func() {
		close(s.hotDrained)
	})
}

func (s *Store) ensureWritable() error {
	if s.readOnly {
		return pebble.ErrReadOnly
	}
	return nil
}

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

func (s *Store) StateFilesDir() string {
	return filepath.Join(s.dir, "archive", "states")
}

func (s *Store) withHotBatch(fn func(batch *pebble.Batch) error) error {
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
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) setHotRecord(ctx context.Context, key, value []byte, writeOptions *pebble.WriteOptions) error {
	if err := s.ensureWritable(); err != nil {
		return err
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
	if err := batch.Set(key, value, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) deleteHotRecord(ctx context.Context, key []byte, writeOptions *pebble.WriteOptions) error {
	if err := s.ensureWritable(); err != nil {
		return err
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
	if err := batch.Delete(key, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) getHotCopy(ctx context.Context, key []byte) ([]byte, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()
	return pebbleReaderGetCopy(db, key)
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
	if err := s.closeAndRemoveCellGeneration(ctx, generation, false); err != nil {
		return err
	}
	return s.clearPendingCellGeneration(ctx, generation)
}

func (s *Store) DeleteCellGeneration(ctx context.Context, generation uint64) error {
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

func (s *Store) setHotUnique(batch *pebble.Batch, key, value []byte) error {
	current, err := pebbleReaderGetCopy(s.hot, key)
	if err == nil {
		if !bytes.Equal(current, value) {
			return fmt.Errorf("hot unique record %x already has different value", key)
		}
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, value, pebble.NoSync)
}

func pebbleReaderGetCopy(reader interface {
	Get(key []byte) ([]byte, io.Closer, error)
}, key []byte) ([]byte, error) {
	value, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return bytes.Clone(value), nil
}

func pebbleReaderHas(reader interface {
	Get(key []byte) ([]byte, io.Closer, error)
}, key []byte) (bool, error) {
	_, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if closer != nil {
		_ = closer.Close()
	}
	return true, nil
}
