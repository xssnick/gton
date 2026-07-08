package service

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stateCellEncodedCache struct {
	mu      sync.RWMutex
	records []storage.EncodedCellRecord
	// decoded memoizes the lazy cell decoded from records[i].Data. Slots are
	// read and written under mu (either mode): the slice only grows under the
	// write lock, and concurrent readers race benignly on Store because the
	// decoded content is identical for identical record data. A slot is reset
	// whenever setRecordLocked replaces the record data.
	decoded         []atomic.Pointer[cell.Cell]
	index           map[cell.Hash]int
	bytes           uint64
	prewritePending int
	prewriteToken   uint64
}

func newStateCellEncodedCache(capacity int) *stateCellEncodedCache {
	if capacity < 1 {
		capacity = 1
	}
	return &stateCellEncodedCache{
		records: make([]storage.EncodedCellRecord, 0, capacity),
		decoded: make([]atomic.Pointer[cell.Cell], 0, capacity),
		index:   make(map[cell.Hash]int, capacity),
	}
}

// stateCellWindowCache overlays recently applied state cells on top of a base
// loader. Both the next-block pipeline and archive catch-up attach the service
// prewriter so applied cells stream to celldb ahead of the checkpoint; with a
// nil prewriter all prewrite plumbing turns into no-ops.
type stateCellWindowCache struct {
	mu        sync.RWMutex
	active    *stateCellEncodedCache
	pending   []*stateCellEncodedCache
	base      cell.LazyCellLoader
	prewriter *stateCellPrewriter
	metrics   *lazyCellLoadCounters
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

type stateCellPrewriteRequest struct {
	cache     *stateCellEncodedCache
	prewriter *stateCellPrewriter
	records   storage.StateCellRecords
}

type archiveStateCellApplyStats struct {
	mu      sync.Mutex
	cells   uint64
	elapsed time.Duration
}

type archiveStateCellApplier struct {
	window *stateCellWindowCache
	stats  *archiveStateCellApplyStats
}

func newStateCellWindowCache(base cell.LazyCellLoader, metrics ...*lazyCellLoadCounters) *stateCellWindowCache {
	var counters *lazyCellLoadCounters
	if len(metrics) > 0 {
		counters = metrics[0]
	}
	return &stateCellWindowCache{
		active:  newStateCellEncodedCache(4096),
		base:    base,
		metrics: counters,
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

func (s *archiveStateCellApplyStats) observe(cells int, elapsed time.Duration) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.cells += uint64(cells)
	s.elapsed += elapsed
}

func (w *stateCellWindowCache) metered(stats *archiveStateCellApplyStats) archiveStateCellApplier {
	return archiveStateCellApplier{
		window: w,
		stats:  stats,
	}
}

func (w archiveStateCellApplier) applyBlockStateUpdate(previous []*storage.BlockState, block PreparedBlock) (stateUpdateApplyResult, error) {
	return w.window.applyPreparedMerkleUpdate(previous, block.StateUpdate, block.StateUpdateToCells, w.stats, block.StateUpdateToCellsElapsed)
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

func (c *stateCellEncodedCache) stageRecords(records storage.StateCellRecords, prewriter *stateCellPrewriter) stateCellPrewriteRequest {
	if c == nil {
		return stateCellPrewriteRequest{}
	}

	if records.Empty() {
		return stateCellPrewriteRequest{}
	}

	c.mu.Lock()
	var prewrite []storage.EncodedCellRecord
	_ = records.ForEach(func(record storage.EncodedCellRecord) error {
		if c.setRecordLocked(record.Hash, record.Data) {
			prewrite = append(prewrite, record)
		}
		return nil
	})
	var request stateCellPrewriteRequest
	if prewriter != nil && len(prewrite) > 0 {
		c.prewritePending++
		request = stateCellPrewriteRequest{
			cache:     c,
			prewriter: prewriter,
			records:   storage.NewStateCellRecords(prewrite),
		}
	}
	c.mu.Unlock()
	return request
}

func (c *stateCellEncodedCache) stageStateRecords(root cell.Hash, records storage.StateCellRecords, prewriter *stateCellPrewriter) (stateCellPrewriteRequest, error) {
	if c == nil {
		return stateCellPrewriteRequest{}, nil
	}
	if !records.Has(root) {
		return stateCellPrewriteRequest{}, fmt.Errorf("prepared state cells do not contain root %x", root[:])
	}
	return c.stageRecords(records, prewriter), nil
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
		c.decoded[idx].Store(nil)
		return true
	}
	c.index[hash] = len(c.records)
	c.records = append(c.records, storage.EncodedCellRecord{Hash: hash, Data: encoded})
	c.decoded = append(c.decoded, atomic.Pointer[cell.Cell]{})
	c.bytes += uint64(len(encoded))
	return true
}

func (r stateCellPrewriteRequest) enqueue() error {
	if r.cache == nil || r.prewriter == nil || r.records.Empty() {
		return nil
	}

	token, err := r.prewriter.enqueue(r.records)
	r.cache.completePrewrite(token, err)
	return err
}

func (c *stateCellEncodedCache) completePrewrite(token uint64, err error) {
	c.mu.Lock()
	if c.prewritePending > 0 {
		c.prewritePending--
	}
	if err == nil && token > c.prewriteToken {
		c.prewriteToken = token
	}
	c.mu.Unlock()
}

func (c *stateCellEncodedCache) loadWith(hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if c == nil {
		return nil, storage.ErrNotFound
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, ok := c.index[hash]
	if !ok || len(c.records[idx].Data) == 0 {
		return nil, storage.ErrNotFound
	}
	slot := &c.decoded[idx]
	if cached := slot.Load(); cached != nil {
		return cached, nil
	}

	loaded, err := cachedLazyCell(hash, c.records[idx].Data, loader)
	if err != nil {
		return nil, fmt.Errorf("create cached lazy cell %x: %w", hash[:], err)
	}
	slot.Store(loaded)
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

func (c *stateCellEncodedCache) prewriteTarget() (uint64, bool) {
	if c == nil {
		return 0, true
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.records) == 0 {
		return 0, true
	}
	if c.prewritePending > 0 || c.prewriteToken == 0 {
		return 0, false
	}
	return c.prewriteToken, true
}

func (w *stateCellWindowCache) applyMerkleUpdate(previous []*storage.BlockState, update *cell.Cell, prepared storage.StateCellRecords) (stateUpdateApplyResult, error) {
	return w.applyPreparedMerkleUpdate(previous, update, prepared, nil, 0)
}

func (w *stateCellWindowCache) applyBlockStateUpdate(previous []*storage.BlockState, block PreparedBlock) (stateUpdateApplyResult, error) {
	return w.applyPreparedMerkleUpdate(previous, block.StateUpdate, block.StateUpdateToCells, nil, block.StateUpdateToCellsElapsed)
}

func (w *stateCellWindowCache) applyPreparedMerkleUpdate(previous []*storage.BlockState, update *cell.Cell, prepared storage.StateCellRecords, stats *archiveStateCellApplyStats, prepareElapsed time.Duration) (stateUpdateApplyResult, error) {
	updateTo, err := merkleUpdateToRef(update)
	if err != nil {
		return stateUpdateApplyResult{}, err
	}

	loader := w.loader()
	currentRoot, err := previousStateRootWithLoader(previous, loader)
	if err != nil {
		return stateUpdateApplyResult{}, err
	}

	if err = cell.MayApplyMerkleUpdate(currentRoot, update); err != nil {
		return stateUpdateApplyResult{}, err
	}

	nextRoot := updateTo.Virtualize(0)
	if err = w.rememberApplied(nextRoot, prepared); err != nil {
		return stateUpdateApplyResult{}, err
	}
	stats.observe(prepared.Len(), prepareElapsed)
	reloadedRoot, err := w.reloadAppliedRoot(nextRoot)
	if err != nil {
		return stateUpdateApplyResult{}, err
	}
	return stateUpdateApplyResult{
		PreviousRoot: currentRoot,
		NextRoot:     reloadedRoot,
	}, nil
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
		prewrite := active.stageRecords(records, prewriter)
		w.mu.RUnlock()
		return prewrite.enqueue()
	}
	w.mu.RUnlock()

	w.mu.Lock()
	if w.active == nil {
		w.active = newStateCellEncodedCache(4096)
	}
	prewrite := w.active.stageRecords(records, w.prewriter)
	w.mu.Unlock()
	return prewrite.enqueue()
}

func (w *stateCellWindowCache) addPreparedStateRecords(root cell.Hash, records storage.StateCellRecords) error {
	if w == nil || records.Empty() {
		return nil
	}

	w.mu.RLock()
	active := w.active
	prewriter := w.prewriter
	if active != nil {
		prewrite, err := active.stageStateRecords(root, records, prewriter)
		w.mu.RUnlock()
		if err != nil {
			return err
		}
		return prewrite.enqueue()
	}
	w.mu.RUnlock()

	w.mu.Lock()
	if w.active == nil {
		w.active = newStateCellEncodedCache(4096)
	}
	prewrite, err := w.active.stageStateRecords(root, records, w.prewriter)
	w.mu.Unlock()
	if err != nil {
		return err
	}
	return prewrite.enqueue()
}

func (w *stateCellWindowCache) loader() cell.LazyCellLoader {
	if w == nil {
		return nil
	}

	var load cell.LazyCellLoader
	load = func(hash cell.Hash) (*cell.Cell, error) {
		w.mu.RLock()
		loaded, err := loadStateCellEncodedCaches(w.active, w.pending, hash, load)
		if err == nil {
			metrics := w.metrics
			w.mu.RUnlock()
			metrics.observeStateWindow()
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			w.mu.RUnlock()
			return nil, err
		}

		base := w.base
		w.mu.RUnlock()

		if base == nil {
			return nil, fmt.Errorf("state cell %x is not in state cell window cache and base loader is not set", hash[:])
		}
		return base(hash)
	}
	return load
}

func (w *stateCellWindowCache) retainedLoader(base cell.LazyCellLoader) cell.LazyCellLoader {
	if w == nil {
		return nil
	}

	sources := w.loaderSources()
	metrics := w.metrics
	var refs cell.LazyCellLoader
	refs = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := loadStateCellEncodedCaches(sources.active, sources.pending, hash, refs)
		if err == nil {
			metrics.observeStateWindow()
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
		loaded, err := loadStateCellEncodedCaches(sources.active, sources.pending, hash, refs)
		if err == nil {
			metrics.observeStateWindow()
		}
		return loaded, err
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

// adoptRecordsFrom moves the populated record caches of src into this window's
// pending set, so cells applied through src stay resolvable until they are
// checkpointed here. Used by archive catch-up when a window is committed.
func (w *stateCellWindowCache) adoptRecordsFrom(src *stateCellWindowCache) {
	if w == nil || src == nil || w == src {
		return
	}

	sources := src.loaderSources()
	caches := make([]*stateCellEncodedCache, 0, 1+len(sources.pending))
	if sources.active.len() > 0 {
		caches = append(caches, sources.active)
	}
	for _, cache := range sources.pending {
		if cache.len() > 0 {
			caches = append(caches, cache)
		}
	}
	if len(caches) == 0 {
		return
	}

	w.mu.Lock()
	w.pending = append(w.pending, caches...)
	w.mu.Unlock()
}

// releaseRecordsToBase drops all owned record caches and redirects the window
// to resolve everything through base. Callers adopt the records elsewhere
// first (see adoptRecordsFrom) so loaders handed out earlier keep working.
func (w *stateCellWindowCache) releaseRecordsToBase(base cell.LazyCellLoader) {
	if w == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.active = newStateCellEncodedCache(1)
	clear(w.pending)
	w.pending = nil
	w.base = base
}

func cachedLazyCell(hash cell.Hash, encoded []byte, loader cell.LazyCellLoader) (*cell.Cell, error) {
	record := storage.DecodeCellRecordTrusted(hash[:], encoded)
	return storage.LazyCellRecord(record, loader)
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
		token, ok := cache.prewriteTarget()
		if !ok {
			return 0, false
		}
		if token > target {
			target = token
		}
	}
	return target, target > 0
}

func (c *stateCellCheckpointCache) loader() cell.LazyCellLoader {
	return c.retainedLoader(nil)
}

func (c *stateCellCheckpointCache) retainedLoader(base cell.LazyCellLoader) cell.LazyCellLoader {
	if c == nil || len(c.caches) == 0 {
		return nil
	}

	caches := append([]*stateCellEncodedCache(nil), c.caches...)
	var metrics *lazyCellLoadCounters
	if c.window != nil {
		metrics = c.window.metrics
	}
	var refs cell.LazyCellLoader
	refs = func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := loadStateCellEncodedCaches(nil, caches, hash, refs)
		if err == nil {
			metrics.observeStateWindow()
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
		loaded, err := loadStateCellEncodedCaches(nil, caches, hash, refs)
		if err == nil {
			metrics.observeStateWindow()
		}
		return loaded, err
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
