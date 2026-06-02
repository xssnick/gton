package service

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stateCellEncodedCache struct {
	mu            sync.RWMutex
	records       []storage.EncodedCellRecord
	index         map[cell.Hash]int
	bytes         uint64
	prewriteToken uint64
}

func newStateCellEncodedCache(capacity int) *stateCellEncodedCache {
	if capacity < 1 {
		capacity = 1
	}
	return &stateCellEncodedCache{
		records: make([]storage.EncodedCellRecord, 0, capacity),
		index:   make(map[cell.Hash]int, capacity),
	}
}

type stateCellWindowCache struct {
	mu        sync.RWMutex
	active    *stateCellEncodedCache
	pending   []*stateCellEncodedCache
	base      cell.LazyCellLoader
	prewriter *stateCellPrewriter
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
	records []storage.EncodedCellRecord
	index   map[cell.Hash]int
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

func (w *stateCellWindowCache) setPrewriter(prewriter *stateCellPrewriter) {
	if w == nil {
		return
	}

	w.mu.Lock()
	w.prewriter = prewriter
	w.mu.Unlock()
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
		records: make([]storage.EncodedCellRecord, 0, capacity),
		index:   make(map[cell.Hash]int, capacity),
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

func (w *archiveStateCellOverlay) metered(stats *archiveStateCellApplyStats) archiveStateCellApplier {
	return archiveStateCellApplier{
		overlay: w,
		stats:   stats,
	}
}

func merkleUpdateToRef(update *cell.Cell) (*cell.Cell, error) {
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

func (c *stateCellEncodedCache) addRecords(records storage.StateCellRecords, prewriter *stateCellPrewriter) error {
	if c == nil {
		return nil
	}

	if records.Empty() {
		return nil
	}

	c.mu.Lock()
	var prewrite []storage.EncodedCellRecord
	_ = records.ForEach(func(record storage.EncodedCellRecord) error {
		if c.setRecordLocked(record.Hash, record.Data) {
			prewrite = append(prewrite, record)
		}
		return nil
	})
	if len(prewrite) > 0 && prewriter != nil {
		token, err := prewriter.enqueue(storage.NewStateCellRecords(prewrite))
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.prewriteToken = token
	}
	c.mu.Unlock()
	return nil
}

func (c *stateCellEncodedCache) addStateRecords(root cell.Hash, records storage.StateCellRecords, prewriter *stateCellPrewriter) error {
	if c == nil {
		return nil
	}
	if !records.Has(root) {
		return fmt.Errorf("prepared state cells do not contain root %x", root[:])
	}

	c.mu.Lock()
	var prewrite []storage.EncodedCellRecord
	_ = records.ForEach(func(record storage.EncodedCellRecord) error {
		if c.setRecordLocked(record.Hash, record.Data) {
			prewrite = append(prewrite, record)
		}
		return nil
	})
	if len(prewrite) > 0 && prewriter != nil {
		token, err := prewriter.enqueue(storage.NewStateCellRecords(prewrite))
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.prewriteToken = token
	}
	c.mu.Unlock()
	return nil
}

func (c *stateCellEncodedCache) setRecordLocked(hash cell.Hash, encoded []byte) bool {
	if len(encoded) == 0 {
		return false
	}
	if idx, ok := c.index[hash]; ok {
		if bytes.Equal(c.records[idx].Data, encoded) {
			return false
		}
		c.bytes -= uint64(len(c.records[idx].Data))
		c.records[idx].Data = encoded
		c.bytes += uint64(len(encoded))
		return true
	}
	c.index[hash] = len(c.records)
	c.records = append(c.records, storage.EncodedCellRecord{Hash: hash, Data: encoded})
	c.bytes += uint64(len(encoded))
	return true
}

func (c *stateCellEncodedCache) data(hash cell.Hash) ([]byte, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, ok := c.index[hash]
	if !ok {
		return nil, false
	}
	return c.records[idx].Data, len(c.records[idx].Data) > 0
}

func (c *stateCellEncodedCache) loadWith(hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	encoded, ok := c.data(hash)
	if !ok {
		return nil, storage.ErrNotFound
	}

	loaded, err := cachedLazyCell(hash, encoded, loader)
	if err != nil {
		return nil, fmt.Errorf("create cached lazy cell %x: %w", hash[:], err)
	}
	return loaded, nil
}

func loadStateCellEncodedCaches(active *stateCellEncodedCache, pending []*stateCellEncodedCache, hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	loaded, err := active.loadWith(hash, loader)
	if err == nil {
		return loaded, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	for _, cache := range pending {
		loaded, err = cache.loadWith(hash, loader)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	return nil, storage.ErrNotFound
}

func (c *stateCellEncodedCache) appendRecordChunks(chunks [][]storage.EncodedCellRecord) ([][]storage.EncodedCellRecord, uint64) {
	if c == nil {
		return chunks, 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.records) == 0 {
		return chunks, 0
	}
	return append(chunks, c.records), c.bytes
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

func (c *stateCellEncodedCache) token() uint64 {
	if c == nil {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.prewriteToken
}

func (w *stateCellWindowCache) applyMerkleUpdate(previous []*storage.BlockState, update *cell.Cell, prepared storage.StateCellRecords) (*cell.Cell, error) {
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

func (w *stateCellWindowCache) applyBlockStateUpdate(previous []*storage.BlockState, block PreparedBlock) (*cell.Cell, error) {
	return w.applyMerkleUpdate(previous, block.StateUpdate, block.StateUpdateToCells)
}

func (w *stateCellWindowCache) rememberApplied(root *cell.Cell, prepared storage.StateCellRecords) error {
	if w == nil {
		return nil
	}
	if prepared.Empty() {
		return fmt.Errorf("prepared state update cells are missing")
	}

	hash := root.GetMetadata().Hash
	if !prepared.Has(hash) {
		return fmt.Errorf("prepared state update cells do not contain destination root %x", hash[:])
	}
	return w.addPreparedStateRecords(hash, prepared)
}

func (w *stateCellWindowCache) reloadAppliedRoot(root *cell.Cell) (*cell.Cell, error) {
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

func (w *stateCellWindowCache) addPreparedRecords(records storage.StateCellRecords) error {
	if w == nil || records.Empty() {
		return nil
	}

	w.mu.RLock()
	active := w.active
	prewriter := w.prewriter
	if active != nil {
		err := active.addRecords(records, prewriter)
		w.mu.RUnlock()
		return err
	}
	w.mu.RUnlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active == nil {
		w.active = newStateCellEncodedCache(4096)
	}
	return w.active.addRecords(records, w.prewriter)
}

func (w *stateCellWindowCache) addPreparedStateRecords(root cell.Hash, records storage.StateCellRecords) error {
	if w == nil || records.Empty() {
		return nil
	}

	w.mu.RLock()
	active := w.active
	prewriter := w.prewriter
	if active != nil {
		err := active.addStateRecords(root, records, prewriter)
		w.mu.RUnlock()
		return err
	}
	w.mu.RUnlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active == nil {
		w.active = newStateCellEncodedCache(4096)
	}
	return w.active.addStateRecords(root, records, w.prewriter)
}

func (w *stateCellWindowCache) loader() cell.LazyCellLoader {
	if w == nil {
		return nil
	}

	sources := w.loaderSources()
	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := loadStateCellEncodedCaches(sources.active, sources.pending, hash, load)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		if sources.base == nil {
			return nil, fmt.Errorf("state cell %x is not in state cell window cache and base loader is not set", hash[:])
		}
		return sources.base(hash)
	}
	return load
}

func (w *stateCellWindowCache) retainedLoader(base cell.LazyCellLoader) cell.LazyCellLoader {
	if w == nil {
		return nil
	}

	sources := w.loaderSources()
	var refs cell.LazyCellLoader
	refs = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := loadStateCellEncodedCaches(sources.active, sources.pending, hash, refs)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if base == nil {
			return nil, storage.ErrNotFound
		}
		return base(hash)
	}

	return func(hash cell.Hash) (*cell.Cell, error) {
		return loadStateCellEncodedCaches(sources.active, sources.pending, hash, refs)
	}
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
	return c.cells().AppendTo(nil)
}

func (c *stateCellCheckpointCache) cells() storage.StateCellRecords {
	if c == nil || len(c.caches) == 0 {
		return storage.StateCellRecords{}
	}

	total := 0
	for _, cache := range c.caches {
		total += cache.len()
	}
	if total == 0 {
		return storage.StateCellRecords{}
	}

	chunks := make([][]storage.EncodedCellRecord, 0, len(c.caches))
	var bytes uint64
	for _, cache := range c.caches {
		var chunkBytes uint64
		chunks, chunkBytes = cache.appendRecordChunks(chunks)
		bytes += chunkBytes
	}
	return storage.NewStateCellRecordChunks(chunks, bytes)
}

func (c *stateCellCheckpointCache) prewriteTarget() (uint64, bool) {
	if c == nil || len(c.caches) == 0 {
		return 0, false
	}

	var target uint64
	for _, cache := range c.caches {
		if cache.len() == 0 {
			continue
		}
		token := cache.token()
		if token == 0 {
			return 0, false
		}
		if token > target {
			target = token
		}
	}
	return target, target > 0
}

func (c *stateCellCheckpointCache) retainedLoader(base cell.LazyCellLoader) cell.LazyCellLoader {
	if c == nil || len(c.caches) == 0 {
		return nil
	}

	caches := append([]*stateCellEncodedCache(nil), c.caches...)
	var refs cell.LazyCellLoader
	refs = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := loadStateCellEncodedCaches(nil, caches, hash, refs)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if base == nil {
			return nil, storage.ErrNotFound
		}
		return base(hash)
	}

	return func(hash cell.Hash) (*cell.Cell, error) {
		return loadStateCellEncodedCaches(nil, caches, hash, refs)
	}
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

func (c *archiveStateCellRecordCache) addRecords(records storage.StateCellRecords) {
	if c == nil || records.Empty() {
		return
	}

	c.mu.Lock()
	_ = records.ForEach(func(record storage.EncodedCellRecord) error {
		c.setRecordLocked(record.Hash, record.Data)
		return nil
	})
	c.mu.Unlock()
}

func (c *archiveStateCellRecordCache) addStateRecords(root cell.Hash, records storage.StateCellRecords) error {
	if c == nil {
		return nil
	}
	if !records.Has(root) {
		return fmt.Errorf("archive prepared state cells do not contain root %x", root[:])
	}

	c.mu.Lock()
	_ = records.ForEach(func(record storage.EncodedCellRecord) error {
		c.setRecordLocked(record.Hash, record.Data)
		return nil
	})
	c.mu.Unlock()
	return nil
}

func (c *archiveStateCellRecordCache) setRecordLocked(hash cell.Hash, encoded []byte) {
	if len(encoded) == 0 {
		return
	}
	if idx, ok := c.index[hash]; ok {
		c.bytes -= uint64(len(c.records[idx].Data))
		c.records[idx].Data = encoded
		c.bytes += uint64(len(encoded))
		return
	}
	c.index[hash] = len(c.records)
	c.records = append(c.records, storage.EncodedCellRecord{Hash: hash, Data: encoded})
	c.bytes += uint64(len(encoded))
}

func (c *archiveStateCellRecordCache) data(hash cell.Hash) ([]byte, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, ok := c.index[hash]
	if !ok {
		return nil, false
	}
	return c.records[idx].Data, len(c.records[idx].Data) > 0
}

func (c *archiveStateCellRecordCache) loadWith(hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	encoded, ok := c.data(hash)
	if !ok {
		return nil, storage.ErrNotFound
	}

	loaded, err := cachedLazyCell(hash, encoded, loader)
	if err != nil {
		return nil, fmt.Errorf("create cached archive lazy cell %x: %w", hash[:], err)
	}
	return loaded, nil
}

func loadArchiveStateCellRecordCaches(active *archiveStateCellRecordCache, pending []*archiveStateCellRecordCache, hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	loaded, err := active.loadWith(hash, loader)
	if err == nil {
		return loaded, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	for _, cache := range pending {
		loaded, err = cache.loadWith(hash, loader)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	return nil, storage.ErrNotFound
}

func (c *archiveStateCellRecordCache) appendRecordChunks(chunks [][]storage.EncodedCellRecord) ([][]storage.EncodedCellRecord, uint64) {
	if c == nil {
		return chunks, 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.records) == 0 {
		return chunks, 0
	}
	return append(chunks, c.records), c.bytes
}

func (c *archiveStateCellRecordCache) copyRecordsTo(dst *archiveStateCellOverlay) {
	if c == nil || dst == nil {
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	dst.addPreparedRecords(storage.NewStateCellRecords(c.records))
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

func (w archiveStateCellApplier) applyBlockStateUpdate(previous []*storage.BlockState, block PreparedBlock) (*cell.Cell, error) {
	return w.overlay.applyPreparedMerkleUpdate(previous, block.StateUpdate, block.StateUpdateToCells, w.stats, block.StateUpdateToCellsElapsed)
}

func (w *archiveStateCellOverlay) applyPreparedMerkleUpdate(previous []*storage.BlockState, update *cell.Cell, prepared storage.StateCellRecords, stats *archiveStateCellApplyStats, prepareElapsed time.Duration) (*cell.Cell, error) {
	if prepared.Empty() {
		return nil, fmt.Errorf("archive block state update target cells are not prepared")
	}
	updateTo, err := merkleUpdateToRef(update)
	if err != nil {
		return nil, err
	}
	return w.applyPreparedMerkleUpdateToRoot(previous, update, updateTo.Virtualize(0), prepared, stats, prepareElapsed)
}

func (w *archiveStateCellOverlay) applyPreparedMerkleUpdateToRoot(previous []*storage.BlockState, update *cell.Cell, nextRoot *cell.Cell, prepared storage.StateCellRecords, stats *archiveStateCellApplyStats, prepareElapsed time.Duration) (*cell.Cell, error) {
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

func (w *archiveStateCellOverlay) applyBlockStateUpdate(previous []*storage.BlockState, block PreparedBlock) (*cell.Cell, error) {
	return w.applyPreparedMerkleUpdate(previous, block.StateUpdate, block.StateUpdateToCells, nil, block.StateUpdateToCellsElapsed)
}

func (w *archiveStateCellOverlay) rememberPrepared(root *cell.Cell, prepared storage.StateCellRecords, stats *archiveStateCellApplyStats, prepareElapsed time.Duration) error {
	if w == nil {
		return nil
	}

	hash := root.GetMetadata().Hash
	if !prepared.Has(hash) {
		return fmt.Errorf("archive state cell overlay does not contain destination root %x", hash[:])
	}
	if err := w.addPreparedStateRecords(hash, prepared); err != nil {
		return err
	}
	stats.observe(prepared.Len(), prepareElapsed)
	return nil
}

func (w *archiveStateCellOverlay) copyRecordsTo(dst *archiveStateCellOverlay) {
	if w == nil || dst == nil || w == dst {
		return
	}

	sources := w.loaderSources()
	sources.active.copyRecordsTo(dst)
	for _, cache := range sources.pending {
		cache.copyRecordsTo(dst)
	}
}

func (w *archiveStateCellOverlay) reloadAppliedRoot(root *cell.Cell) (*cell.Cell, error) {
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

func (w *archiveStateCellOverlay) addPreparedRecords(records storage.StateCellRecords) {
	if w == nil || records.Empty() {
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

func (w *archiveStateCellOverlay) addPreparedStateRecords(root cell.Hash, records storage.StateCellRecords) error {
	if w == nil || records.Empty() {
		return nil
	}

	w.mu.RLock()
	active := w.active
	if active != nil {
		err := active.addStateRecords(root, records)
		w.mu.RUnlock()
		return err
	}
	w.mu.RUnlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active == nil {
		w.active = newArchiveStateCellRecordCache(4096)
	}
	return w.active.addStateRecords(root, records)
}

func (w *archiveStateCellOverlay) loader() cell.LazyCellLoader {
	if w == nil {
		return nil
	}

	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		sources := w.loaderSources()
		loaded, err := loadArchiveStateCellRecordCaches(sources.active, sources.pending, hash, load)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		if sources.base == nil {
			return nil, fmt.Errorf("archive state cell %x is not in overlay and base loader is not set", hash[:])
		}
		return sources.base(hash)
	}
	return load
}

func (w *archiveStateCellOverlay) releaseRecordsToBase(base cell.LazyCellLoader) {
	if w == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.active = newArchiveStateCellRecordCache(1)
	clear(w.pending)
	w.pending = nil
	w.base = base
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
	return c.cells().AppendTo(nil)
}

func (c *archiveStateCellCheckpoint) cells() storage.StateCellRecords {
	if c == nil || len(c.caches) == 0 {
		return storage.StateCellRecords{}
	}

	total := 0
	for _, cache := range c.caches {
		total += cache.len()
	}
	if total == 0 {
		return storage.StateCellRecords{}
	}

	chunks := make([][]storage.EncodedCellRecord, 0, len(c.caches))
	var bytes uint64
	for _, cache := range c.caches {
		var chunkBytes uint64
		chunks, chunkBytes = cache.appendRecordChunks(chunks)
		bytes += chunkBytes
	}
	return storage.NewStateCellRecordChunks(chunks, bytes)
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

func (c *archiveStateCellCheckpoint) retainedLoader(base cell.LazyCellLoader) cell.LazyCellLoader {
	if c == nil || len(c.caches) == 0 {
		return nil
	}

	caches := append([]*archiveStateCellRecordCache(nil), c.caches...)
	var refs cell.LazyCellLoader
	refs = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := loadArchiveStateCellRecordCaches(nil, caches, hash, refs)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		if base == nil {
			return nil, storage.ErrNotFound
		}
		return base(hash)
	}

	return func(hash cell.Hash) (*cell.Cell, error) {
		return loadArchiveStateCellRecordCaches(nil, caches, hash, refs)
	}
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
		return stateRootWithLoader(current, loader)
	case 2:
		left := previous[0]
		right := previous[1]

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
