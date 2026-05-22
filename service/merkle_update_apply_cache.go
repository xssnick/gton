package service

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stateCellEncodedCache struct {
	mu      sync.RWMutex
	records map[cell.Hash][]byte
	bytes   uint64
}

func newStateCellEncodedCache(capacity int) *stateCellEncodedCache {
	if capacity < 1 {
		capacity = 1
	}
	return &stateCellEncodedCache{
		records: make(map[cell.Hash][]byte, capacity),
	}
}

type stateCellWindowCache struct {
	mu      sync.RWMutex
	active  *stateCellEncodedCache
	pending []*stateCellEncodedCache
	base    cell.LazyCellLoader
}

type stateCellCheckpointCache struct {
	window *stateCellWindowCache
	caches []*stateCellEncodedCache
}

type stateCellWindowLoaderSources struct {
	active  *stateCellEncodedCache
	pending []*stateCellEncodedCache
	base    cell.LazyCellLoader
}

type archiveStateCellRecordCache struct {
	mu      sync.RWMutex
	records map[cell.Hash][]byte
	bytes   uint64
}

type archiveStateCellOverlay struct {
	mu      sync.RWMutex
	active  *archiveStateCellRecordCache
	pending []*archiveStateCellRecordCache
	base    cell.LazyCellLoader
}

type archiveStateCellApplyStats struct {
	mu      sync.Mutex
	cells   uint64
	elapsed time.Duration
}

type archiveStateCellApplier struct {
	overlay *archiveStateCellOverlay
	stats   *archiveStateCellApplyStats
}

type archiveStateCellCheckpoint struct {
	window *archiveStateCellOverlay
	caches []*archiveStateCellRecordCache
}

type archiveStateCellLoaderSources struct {
	active  *archiveStateCellRecordCache
	pending []*archiveStateCellRecordCache
	base    cell.LazyCellLoader
}

func newStateCellWindowCache(base cell.LazyCellLoader) *stateCellWindowCache {
	return &stateCellWindowCache{
		active: newStateCellEncodedCache(4096),
		base:   base,
	}
}

func newArchiveStateCellOverlay(base cell.LazyCellLoader) *archiveStateCellOverlay {
	return &archiveStateCellOverlay{
		active: newArchiveStateCellRecordCache(4096),
		base:   base,
	}
}

func newArchiveStateCellRecordCache(capacity int) *archiveStateCellRecordCache {
	if capacity < 1 {
		capacity = 1
	}
	return &archiveStateCellRecordCache{
		records: make(map[cell.Hash][]byte, capacity),
	}
}

func (s *archiveStateCellApplyStats) observe(cells int, elapsed time.Duration) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.cells += uint64(cells)
	s.elapsed += elapsed
}

func (s *archiveStateCellApplyStats) snapshot() (uint64, time.Duration) {
	if s == nil {
		return 0, 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cells, s.elapsed
}

func (w *archiveStateCellOverlay) metered(stats *archiveStateCellApplyStats) archiveStateCellApplier {
	return archiveStateCellApplier{
		overlay: w,
		stats:   stats,
	}
}

func merkleUpdateToRef(update *cell.Cell) (*cell.Cell, error) {
	if update == nil {
		return nil, fmt.Errorf("merkle update cell is nil")
	}
	loader, err := update.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load merkle update cell: %w", err)
	}
	update = loader.BaseCell()
	if update.Level() != 0 {
		return nil, fmt.Errorf("merkle update has non-zero level")
	}
	if update.GetType() != cell.MerkleUpdateCellType {
		return nil, fmt.Errorf("not a MerkleUpdate cell")
	}
	if loader.RefsNum() != 2 {
		return nil, fmt.Errorf("wrong references count for a merkle update special cell")
	}

	updateTo, err := loader.PeekRefCellAt(1)
	if err != nil {
		return nil, fmt.Errorf("failed to load merkle update second ref: %w", err)
	}
	return updateTo, nil
}

func (c *stateCellEncodedCache) addRecord(hash cell.Hash, encoded []byte) {
	if c == nil || len(encoded) == 0 {
		return
	}

	c.mu.Lock()
	c.setRecordLocked(hash, encoded)
	c.mu.Unlock()
}

func (c *stateCellEncodedCache) addRecords(records map[cell.Hash][]byte) {
	if c == nil {
		return
	}

	if len(records) == 0 {
		return
	}

	c.mu.Lock()
	for hash, encoded := range records {
		if len(encoded) == 0 {
			continue
		}
		c.setRecordLocked(hash, encoded)
	}
	c.mu.Unlock()
}

func (c *stateCellEncodedCache) setRecordLocked(hash cell.Hash, encoded []byte) {
	if previous, ok := c.records[hash]; ok {
		c.bytes -= uint64(len(previous))
	}
	c.records[hash] = encoded
	c.bytes += uint64(len(encoded))
}

func (c *stateCellEncodedCache) loadWith(hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	c.mu.RLock()
	encoded := c.records[hash]
	c.mu.RUnlock()
	if len(encoded) == 0 {
		return nil, storage.ErrNotFound
	}

	loaded, err := cachedLazyCell(hash, encoded, loader)
	if err != nil {
		return nil, fmt.Errorf("create cached lazy cell %x: %w", hash[:], err)
	}
	return loaded, nil
}

func (c *stateCellEncodedCache) appendRecords(records []storage.EncodedCellRecord, seen map[cell.Hash]struct{}) []storage.EncodedCellRecord {
	if c == nil {
		return records
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for hash, encoded := range c.records {
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		records = append(records, storage.EncodedCellRecord{Hash: hash, Data: encoded})
	}
	return records
}

func (c *stateCellEncodedCache) len() int {
	if c == nil {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.records)
}

func (c *stateCellEncodedCache) byteSize() uint64 {
	if c == nil {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bytes
}

func (w *stateCellWindowCache) applyMerkleUpdate(previous []*storage.BlockState, update *cell.Cell, prepared map[cell.Hash][]byte) (*cell.Cell, error) {
	updateTo, err := merkleUpdateToRef(update)
	if err != nil {
		return nil, err
	}

	loader := w.loader()
	currentRoot, err := previousStateRootWithLoader(previous, loader)
	if err != nil {
		return nil, err
	}

	if err = cell.MayApplyMerkleUpdate(currentRoot, update); err != nil {
		return nil, err
	}

	nextRoot := updateTo.Virtualize(0)
	if err = w.rememberApplied(nextRoot, prepared); err != nil {
		return nil, err
	}
	return w.reloadAppliedRoot(nextRoot)
}

func (w *stateCellWindowCache) applyBlockStateUpdate(previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*cell.Cell, error) {
	return w.applyMerkleUpdate(previous, downloaded.Parsed.StateUpdate, downloaded.StateUpdateToCells)
}

func (w *stateCellWindowCache) rememberApplied(root *cell.Cell, prepared map[cell.Hash][]byte) error {
	if w == nil {
		return nil
	}
	if prepared == nil {
		return fmt.Errorf("prepared state update cells are missing")
	}

	hash := root.GetMetadata().Hash
	if len(prepared[hash]) == 0 {
		return fmt.Errorf("prepared state update cells do not contain destination root %x", hash[:])
	}
	w.addPreparedRecords(prepared)
	return nil
}

func (w *stateCellWindowCache) reloadAppliedRoot(root *cell.Cell) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("applied state root is nil")
	}

	hash := root.GetMetadata().Hash
	loaded, err := w.loader()(hash)
	if err != nil {
		return nil, fmt.Errorf("reload applied state root %x from state cell window cache: %w", hash[:], err)
	}
	loadedHash := loaded.GetMetadata().Hash
	if loadedHash != hash {
		return nil, fmt.Errorf("reloaded applied state root hash mismatch: got=%x want=%x", loadedHash[:], hash[:])
	}
	return loaded, nil
}

func (w *stateCellWindowCache) addPreparedRecords(records map[cell.Hash][]byte) {
	if w == nil || len(records) == 0 {
		return
	}

	w.mu.RLock()
	active := w.active
	if active != nil {
		active.addRecords(records)
		w.mu.RUnlock()
		return
	}
	w.mu.RUnlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active == nil {
		w.active = newStateCellEncodedCache(4096)
	}
	w.active.addRecords(records)
}

func (w *stateCellWindowCache) loader() cell.LazyCellLoader {
	if w == nil {
		return nil
	}

	sources := w.loaderSources()
	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := sources.active.loadWith(hash, load)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		for _, cache := range sources.pending {
			loaded, err = cache.loadWith(hash, load)
			if err == nil {
				return loaded, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
		}

		if sources.base == nil {
			return nil, fmt.Errorf("state cell %x is not in state cell window cache and base loader is not set", hash[:])
		}
		return sources.base(hash)
	}
	return load
}

func (w *stateCellWindowCache) loaderSources() stateCellWindowLoaderSources {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var pending []*stateCellEncodedCache
	if len(w.pending) > 0 {
		pending = append([]*stateCellEncodedCache(nil), w.pending...)
	}
	return stateCellWindowLoaderSources{
		active:  w.active,
		pending: pending,
		base:    w.base,
	}
}

func (w *stateCellWindowCache) beginCheckpoint() *stateCellCheckpointCache {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	cache := w.active
	w.active = newStateCellEncodedCache(4096)
	if cache != nil && cache.len() > 0 {
		w.pending = append(w.pending, cache)
	}
	if len(w.pending) == 0 {
		return nil
	}

	return &stateCellCheckpointCache{
		window: w,
		caches: append([]*stateCellEncodedCache(nil), w.pending...),
	}
}

func (w *stateCellWindowCache) byteSize() uint64 {
	if w == nil {
		return 0
	}

	sources := w.loaderSources()
	total := sources.active.byteSize()
	for _, cache := range sources.pending {
		total += cache.byteSize()
	}
	return total
}

func (c *stateCellCheckpointCache) records() []storage.EncodedCellRecord {
	if c == nil || len(c.caches) == 0 {
		return nil
	}

	total := 0
	for _, cache := range c.caches {
		total += cache.len()
	}

	seen := make(map[cell.Hash]struct{}, total)
	records := make([]storage.EncodedCellRecord, 0, total)
	for _, cache := range c.caches {
		records = cache.appendRecords(records, seen)
	}
	return records
}

func (c *stateCellCheckpointCache) byteSize() uint64 {
	if c == nil {
		return 0
	}

	total := uint64(0)
	for _, cache := range c.caches {
		total += cache.byteSize()
	}
	return total
}

func (c *stateCellCheckpointCache) complete() {
	if c == nil || c.window == nil || len(c.caches) == 0 {
		return
	}

	done := make(map[*stateCellEncodedCache]struct{}, len(c.caches))
	for _, cache := range c.caches {
		done[cache] = struct{}{}
	}

	w := c.window
	w.mu.Lock()
	defer w.mu.Unlock()
	pending := w.pending[:0]
	for _, cache := range w.pending {
		if _, ok := done[cache]; ok {
			continue
		}
		pending = append(pending, cache)
	}
	clear(w.pending[len(pending):])
	w.pending = pending
}

func (c *archiveStateCellRecordCache) addRecords(records map[cell.Hash][]byte) {
	if c == nil || len(records) == 0 {
		return
	}

	c.mu.Lock()
	for hash, encoded := range records {
		if len(encoded) == 0 {
			continue
		}
		c.setRecordLocked(hash, encoded)
	}
	c.mu.Unlock()
}

func (c *archiveStateCellRecordCache) setRecordLocked(hash cell.Hash, encoded []byte) {
	if previous, ok := c.records[hash]; ok {
		c.bytes -= uint64(len(previous))
	}
	c.records[hash] = encoded
	c.bytes += uint64(len(encoded))
}

func (c *archiveStateCellRecordCache) loadWith(hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	c.mu.RLock()
	encoded := c.records[hash]
	c.mu.RUnlock()
	if len(encoded) == 0 {
		return nil, storage.ErrNotFound
	}

	loaded, err := cachedLazyCell(hash, encoded, loader)
	if err != nil {
		return nil, fmt.Errorf("create cached archive lazy cell %x: %w", hash[:], err)
	}
	return loaded, nil
}

func (c *archiveStateCellRecordCache) appendRecords(records []storage.EncodedCellRecord, seen map[cell.Hash]struct{}) []storage.EncodedCellRecord {
	if c == nil {
		return records
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for hash, encoded := range c.records {
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		records = append(records, storage.EncodedCellRecord{Hash: hash, Data: encoded})
	}
	return records
}

func (c *archiveStateCellRecordCache) len() int {
	if c == nil {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.records)
}

func (c *archiveStateCellRecordCache) byteSize() uint64 {
	if c == nil {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bytes
}

func (w archiveStateCellApplier) applyBlockStateUpdate(previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*cell.Cell, error) {
	if w.overlay == nil {
		return nil, fmt.Errorf("archive state cell overlay is nil")
	}
	return w.overlay.applyPreparedMerkleUpdate(previous, downloaded.Parsed.StateUpdate, downloaded.StateUpdateToCells, w.stats, downloaded.StateUpdateToCellsElapsed)
}

func (w *archiveStateCellOverlay) applyPreparedMerkleUpdate(previous []*storage.BlockState, update *cell.Cell, prepared map[cell.Hash][]byte, stats *archiveStateCellApplyStats, prepareElapsed time.Duration) (*cell.Cell, error) {
	if len(prepared) == 0 {
		return nil, fmt.Errorf("archive block state update target cells are not prepared")
	}
	updateTo, err := merkleUpdateToRef(update)
	if err != nil {
		return nil, err
	}
	return w.applyPreparedMerkleUpdateToRoot(previous, update, updateTo.Virtualize(0), prepared, stats, prepareElapsed)
}

func (w *archiveStateCellOverlay) applyPreparedMerkleUpdateToRoot(previous []*storage.BlockState, update *cell.Cell, nextRoot *cell.Cell, prepared map[cell.Hash][]byte, stats *archiveStateCellApplyStats, prepareElapsed time.Duration) (*cell.Cell, error) {
	loader := w.loader()
	currentRoot, err := previousStateRootWithLoader(previous, loader)
	if err != nil {
		return nil, err
	}

	if err = cell.MayApplyMerkleUpdate(currentRoot, update); err != nil {
		return nil, err
	}

	if err = w.rememberPrepared(nextRoot, prepared, stats, prepareElapsed); err != nil {
		return nil, err
	}
	return w.reloadAppliedRoot(nextRoot)
}

func (w *archiveStateCellOverlay) applyBlockStateUpdate(previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*cell.Cell, error) {
	return w.applyPreparedMerkleUpdate(previous, downloaded.Parsed.StateUpdate, downloaded.StateUpdateToCells, nil, downloaded.StateUpdateToCellsElapsed)
}

func (w *archiveStateCellOverlay) rememberPrepared(root *cell.Cell, prepared map[cell.Hash][]byte, stats *archiveStateCellApplyStats, prepareElapsed time.Duration) error {
	if w == nil {
		return nil
	}

	hash := root.GetMetadata().Hash
	if len(prepared[hash]) == 0 {
		return fmt.Errorf("archive state cell overlay does not contain destination root %x", hash[:])
	}
	w.addPreparedRecords(prepared)
	stats.observe(len(prepared), prepareElapsed)
	return nil
}

func (w *archiveStateCellOverlay) rememberPreparedCells(prepared map[cell.Hash][]byte) {
	if w == nil || len(prepared) == 0 {
		return
	}
	w.addPreparedRecords(prepared)
}

func (w *archiveStateCellOverlay) reloadAppliedRoot(root *cell.Cell) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("applied archive state root is nil")
	}

	hash := root.GetMetadata().Hash
	loaded, err := w.loader()(hash)
	if err != nil {
		return nil, fmt.Errorf("reload applied archive state root %x from overlay: %w", hash[:], err)
	}
	loadedHash := loaded.GetMetadata().Hash
	if loadedHash != hash {
		return nil, fmt.Errorf("reloaded archive state root hash mismatch: got=%x want=%x", loadedHash[:], hash[:])
	}
	return loaded, nil
}

func (w *archiveStateCellOverlay) addPreparedRecords(records map[cell.Hash][]byte) {
	if w == nil || len(records) == 0 {
		return
	}

	w.mu.RLock()
	active := w.active
	if active != nil {
		active.addRecords(records)
		w.mu.RUnlock()
		return
	}
	w.mu.RUnlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active == nil {
		w.active = newArchiveStateCellRecordCache(4096)
	}
	w.active.addRecords(records)
}

func (w *archiveStateCellOverlay) loader() cell.LazyCellLoader {
	if w == nil {
		return nil
	}

	sources := w.loaderSources()
	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := sources.active.loadWith(hash, load)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		for _, cache := range sources.pending {
			loaded, err = cache.loadWith(hash, load)
			if err == nil {
				return loaded, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
		}

		if sources.base == nil {
			return nil, fmt.Errorf("archive state cell %x is not in overlay and base loader is not set", hash[:])
		}
		return sources.base(hash)
	}
	return load
}

func cachedLazyCell(hash cell.Hash, encoded []byte, loader cell.LazyCellLoader) (*cell.Cell, error) {
	record := storage.DecodeCellRecordTrusted(hash[:], encoded)
	return storage.LazyCellRecord(record, loader)
}

func (w *archiveStateCellOverlay) loaderSources() archiveStateCellLoaderSources {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var pending []*archiveStateCellRecordCache
	if len(w.pending) > 0 {
		pending = append([]*archiveStateCellRecordCache(nil), w.pending...)
	}
	return archiveStateCellLoaderSources{
		active:  w.active,
		pending: pending,
		base:    w.base,
	}
}

func (w *archiveStateCellOverlay) byteSize() uint64 {
	if w == nil {
		return 0
	}

	sources := w.loaderSources()
	total := sources.active.byteSize()
	for _, cache := range sources.pending {
		total += cache.byteSize()
	}
	return total
}

func (w *archiveStateCellOverlay) beginCheckpoint() *archiveStateCellCheckpoint {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	cache := w.active
	w.active = newArchiveStateCellRecordCache(4096)
	if cache != nil && cache.len() > 0 {
		w.pending = append(w.pending, cache)
	}
	if len(w.pending) == 0 {
		return nil
	}

	return &archiveStateCellCheckpoint{
		window: w,
		caches: append([]*archiveStateCellRecordCache(nil), w.pending...),
	}
}

func (c *archiveStateCellCheckpoint) records() []storage.EncodedCellRecord {
	if c == nil || len(c.caches) == 0 {
		return nil
	}

	total := 0
	for _, cache := range c.caches {
		total += cache.len()
	}

	seen := make(map[cell.Hash]struct{}, total)
	records := make([]storage.EncodedCellRecord, 0, total)
	for _, cache := range c.caches {
		records = cache.appendRecords(records, seen)
	}
	return records
}

func (c *archiveStateCellCheckpoint) byteSize() uint64 {
	if c == nil {
		return 0
	}

	total := uint64(0)
	for _, cache := range c.caches {
		total += cache.byteSize()
	}
	return total
}

func (c *archiveStateCellCheckpoint) loader() cell.LazyCellLoader {
	if c == nil || len(c.caches) == 0 {
		return nil
	}

	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		for _, cache := range c.caches {
			loaded, err := cache.loadWith(hash, load)
			if err == nil {
				return loaded, nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, err
			}
		}
		return nil, storage.ErrNotFound
	}
	return load
}

func (c *archiveStateCellCheckpoint) complete() {
	if c == nil || c.window == nil || len(c.caches) == 0 {
		return
	}

	done := make(map[*archiveStateCellRecordCache]struct{}, len(c.caches))
	for _, cache := range c.caches {
		done[cache] = struct{}{}
	}

	w := c.window
	w.mu.Lock()
	defer w.mu.Unlock()
	pending := w.pending[:0]
	for _, cache := range w.pending {
		if _, ok := done[cache]; ok {
			continue
		}
		pending = append(pending, cache)
	}
	clear(w.pending[len(pending):])
	w.pending = pending
}

func previousStateRootWithLoader(previous []*storage.BlockState, loader cell.LazyCellLoader) (*cell.Cell, error) {
	switch len(previous) {
	case 1:
		current := previous[0]
		if current == nil {
			return nil, fmt.Errorf("current state is nil")
		}
		return stateRootWithLoader(current, loader)
	case 2:
		left := previous[0]
		right := previous[1]
		if left == nil || right == nil {
			return nil, fmt.Errorf("merge previous state is nil")
		}

		leftRoot, err := stateRootWithLoader(left, loader)
		if err != nil {
			return nil, fmt.Errorf("load left merge state root: %w", err)
		}
		rightRoot, err := stateRootWithLoader(right, loader)
		if err != nil {
			return nil, fmt.Errorf("load right merge state root: %w", err)
		}

		return cell.BeginCell().
			MustStoreUInt(0x5f327da5, 32).
			MustStoreRef(leftRoot).
			MustStoreRef(rightRoot).
			EndCell(), nil
	default:
		return nil, fmt.Errorf("unsupported previous state count %d", len(previous))
	}
}

func stateRootWithLoader(state *storage.BlockState, loader cell.LazyCellLoader) (*cell.Cell, error) {
	root := state.Cell
	if root == nil {
		return nil, fmt.Errorf("current state cell is missing")
	}

	view := root.Virtualize(0)
	if len(state.StateRootHash) > 0 {
		rootHash := view.HashKey(0)
		if !bytes.Equal(rootHash[:], state.StateRootHash) {
			return nil, fmt.Errorf("current state root hash mismatch for %s: got=%x want=%x", storage.FormatBlockRef(state.Block), rootHash[:], state.StateRootHash)
		}
	}
	record, err := storage.CellRecordFromCellMetadata(view, view.GetMetadata())
	if err != nil {
		return nil, err
	}
	return storage.LazyCellRecord(record, loader)
}
