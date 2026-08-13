package service

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	decoded []atomic.Pointer[cell.Cell]
	index   map[cell.Hash]int
	bytes   uint64
	// layers are staged blocks not yet folded into the base map above, newest
	// last. Staging is an O(1) append reusing the records' own hash index, so
	// appliers never hold the write lock for a per-record walk; loads search
	// layers newest-first, which resolves a re-staged hash to its newest
	// encoding exactly like the old in-place replace did.
	layers     []*stateCellRecordLayer
	layerBytes uint64
	// layerFolded is the fold cursor into layers[0]: folding runs in bounded
	// slices so loaders are never stalled behind a whole-block record walk. A
	// partially folded layer keeps shadowing the base copies of its already
	// folded prefix, which carry the same data.
	layerFolded     int
	prewritePending int
	prewriteToken   uint64
}

const (
	// stateCellWindowFoldSliceRecords bounds one fold slice's exclusive hold
	// (~90ns per record) so loaders and stagers get the lock between slices.
	stateCellWindowFoldSliceRecords = 4096
	// stateCellWindowMaxStagedLayers is the applier-side valve: past this many
	// unfolded layers a load miss (one index probe per layer) starts to rival a
	// base-store read, and an unfolded layer also pins its block's prepared
	// index map, which folding releases. The commit stage folds to zero on
	// every master commit, so at the live head appliers never trip this; it
	// engages on pipelines without a per-block commit fold, such as archive
	// catch-up.
	stateCellWindowMaxStagedLayers = 32
	// stateCellLayerLinearScanLimit bounds the linear-scan fallback for record
	// sets without a builder index (only test-built sets); anything larger
	// gets a private lookup map built off the shared lock.
	stateCellLayerLinearScanLimit = 64
)

// stateCellRecordLayer is one staged block's prepared records published as an
// immutable overlay. Builder-produced sets carry their own hash index, so
// publication reuses it instead of rebuilding one under the cache lock; the
// whole layer is built before the lock is taken.
type stateCellRecordLayer struct {
	records storage.StateCellRecords
	indexed bool
	flat    []storage.EncodedCellRecord
	lookup  map[cell.Hash]int
	// decoded memoizes the lazy cell per record position, exactly like the base
	// cache's slots: racing stores are benign because identical record data
	// decodes to identical content. Positional slots keep a layer probe free of
	// allocations, which a hash-keyed map cannot do (boxing the hash allocates
	// on every probe, hit or miss).
	decoded []atomic.Pointer[cell.Cell]
	bytes   uint64
}

func newStateCellRecordLayer(records storage.StateCellRecords) *stateCellRecordLayer {
	layer := &stateCellRecordLayer{
		records: records,
		indexed: records.Indexed(),
		bytes:   records.ByteSize(),
	}
	if layer.indexed {
		layer.flat = records.Records
	} else {
		layer.flat = records.AppendTo(make([]storage.EncodedCellRecord, 0, records.Len()))
		if len(layer.flat) > stateCellLayerLinearScanLimit {
			layer.lookup = make(map[cell.Hash]int, len(layer.flat))
			for i, record := range layer.flat {
				// First occurrence wins to match the linear-scan order of
				// StateCellRecords.Data.
				if _, ok := layer.lookup[record.Hash]; !ok {
					layer.lookup[record.Hash] = i
				}
			}
		}
	}
	layer.decoded = make([]atomic.Pointer[cell.Cell], len(layer.flat))
	return layer
}

func (l *stateCellRecordLayer) indexOf(hash cell.Hash) (int, bool) {
	if l.indexed {
		return l.records.IndexOf(hash)
	}
	if l.lookup != nil {
		idx, ok := l.lookup[hash]
		return idx, ok
	}
	for i := range l.flat {
		if l.flat[i].Hash == hash {
			return i, true
		}
	}
	return 0, false
}

// newStateCellRecordLayerForRoot builds the layer and checks that it carries
// the applied state root, which is the invariant the state-cell stagers verify
// before publishing a block's cells.
func newStateCellRecordLayerForRoot(records storage.StateCellRecords, root cell.Hash) (*stateCellRecordLayer, error) {
	layer := newStateCellRecordLayer(records)
	if idx, ok := layer.indexOf(root); !ok || len(layer.dataAt(idx)) == 0 {
		return nil, fmt.Errorf("prepared state cells do not contain root %x", root[:])
	}
	return layer, nil
}

func (l *stateCellRecordLayer) dataAt(idx int) []byte {
	return l.flat[idx].Data
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

func newStateCellWindowCache(base cell.LazyCellLoader, metrics *lazyCellLoadCounters) *stateCellWindowCache {
	return &stateCellWindowCache{
		active:  newStateCellEncodedCache(4096),
		base:    base,
		metrics: metrics,
	}
}

func (w *stateCellWindowCache) setPrewriter(prewriter *stateCellPrewriter) {
	w.mu.Lock()
	w.prewriter = prewriter
	w.mu.Unlock()
}

func (c *stateCellEncodedCache) stageRecords(records storage.StateCellRecords, prewriter *stateCellPrewriter) stateCellPrewriteRequest {
	if records.Empty() {
		return stateCellPrewriteRequest{}
	}
	return c.stageLayer(newStateCellRecordLayer(records), prewriter)
}

// stageLayer publishes an already-built layer, which is all the exclusive hold
// this costs: appliers build the layer before taking any shared lock, so no
// loader waits behind a per-record walk.
func (c *stateCellEncodedCache) stageLayer(layer *stateCellRecordLayer, prewriter *stateCellPrewriter) stateCellPrewriteRequest {
	c.mu.Lock()
	c.layers = append(c.layers, layer)
	c.layerBytes += layer.bytes
	var request stateCellPrewriteRequest
	if prewriter != nil {
		// The whole immutable set goes to the prewriter: filtering out records
		// the window already holds would need the per-record walk this layer
		// publication exists to avoid, and re-writing an unchanged cell record
		// is a same-key overwrite.
		c.prewritePending++
		request = stateCellPrewriteRequest{
			cache:     c,
			prewriter: prewriter,
			records:   layer.records,
		}
	}
	c.mu.Unlock()
	return request
}

// foldLayers folds the oldest staged layers into the base map until either
// remove of them are fully folded or budget records were processed. It reports
// how many layers it fully folded and whether the budget ran out with folding
// still owed, so the caller can resume. The budget keeps a single exclusive
// hold bounded regardless of how large the staged blocks are.
func (c *stateCellEncodedCache) foldLayers(remove, budget int) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	folded := 0
	for folded < remove && len(c.layers) > 0 {
		if budget <= 0 {
			return folded, true
		}

		flat := c.layers[0].flat
		next := min(len(flat), c.layerFolded+budget)
		for i := c.layerFolded; i < next; i++ {
			c.layerBytes -= uint64(len(flat[i].Data))
			c.setRecordLocked(flat[i].Hash, flat[i].Data)
		}
		budget -= next - c.layerFolded
		c.layerFolded = next
		if next < len(flat) {
			continue
		}

		copy(c.layers, c.layers[1:])
		c.layers[len(c.layers)-1] = nil
		c.layers = c.layers[:len(c.layers)-1]
		c.layerFolded = 0
		folded++
	}
	return folded, false
}

func (c *stateCellEncodedCache) stagedLayers() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.layers)
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
	wait, err := r.enqueueDetached()
	if err != nil {
		return err
	}
	return wait()
}

// enqueueDetached hands the records to the prewriter without blocking and
// returns the queue's backpressure wait, which the caller must invoke once the
// staged state is visible to its dependents and no loader-shared lock is held.
// The prewrite token is accounted here, so a checkpoint taken before the wait
// resolves still knows the prewriter sequence covering these records.
func (r stateCellPrewriteRequest) enqueueDetached() (func() error, error) {
	if r.cache == nil || r.prewriter == nil || r.records.Empty() {
		return noPrewriteBackpressure, nil
	}

	token, wait, err := r.prewriter.enqueueDetached(r.records)
	r.cache.completePrewrite(token, err)
	if err != nil {
		return nil, err
	}
	return wait, nil
}

func (c *stateCellEncodedCache) completePrewrite(token uint64, err error) {
	c.mu.Lock()
	c.prewritePending--
	if err == nil && token > c.prewriteToken {
		c.prewriteToken = token
	}
	c.mu.Unlock()
}

func (c *stateCellEncodedCache) loadWith(hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Newest layer first, then the folded base map. The decode is written out
	// here instead of behind a layer method on purpose: a hash handed to a
	// callee that retains it (the decoded record keeps hash[:]) is heap-copied
	// on entry to that callee, so probing N layers through one would allocate N
	// times, misses included. Here only this frame's copy is ever retained.
	//
	// For the same reason the error paths below format hash rather than hash[:]:
	// %x renders both identically, but slicing takes the array's address, and
	// escape analysis is blind to which branch runs — one hash[:] in a cold
	// error path moves the parameter to the heap on every call, hits included.
	for i := len(c.layers) - 1; i >= 0; i-- {
		layer := c.layers[i]
		idx, ok := layer.indexOf(hash)
		if !ok || len(layer.dataAt(idx)) == 0 {
			continue
		}

		slot := &layer.decoded[idx]
		if cached := slot.Load(); cached != nil {
			return cached, nil
		}
		loaded, err := cachedLazyCell(hash, layer.dataAt(idx), loader)
		if err != nil {
			return nil, fmt.Errorf("create cached lazy cell %x: %w", hash, err)
		}
		slot.Store(loaded)
		return loaded, nil
	}

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
		return nil, fmt.Errorf("create cached lazy cell %x: %w", hash, err)
	}
	slot.Store(loaded)
	return loaded, nil
}

func loadStateCellEncodedCaches(active *stateCellEncodedCache, pending []*stateCellEncodedCache, hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if active != nil {
		loaded, err := active.loadWith(hash, loader)
		if err == nil {
			return loaded, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	for i := len(pending) - 1; i >= 0; i-- {
		cache := pending[i]
		loaded, err := cache.loadWith(hash, loader)
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
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Base first, then layers oldest to newest: batch writers consume chunks
	// in order, so on a duplicated hash the newest staged encoding lands last
	// and wins, matching the load path. A partially folded layer re-emits its
	// folded prefix; the duplicate write is a same-key overwrite.
	if len(c.records) > 0 {
		chunks = append(chunks, c.records)
	}
	for _, layer := range c.layers {
		if len(layer.flat) > 0 {
			chunks = append(chunks, layer.flat)
		}
	}
	return chunks, c.bytes + c.layerBytes
}

func (c *stateCellEncodedCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := len(c.records)
	for _, layer := range c.layers {
		total += len(layer.flat)
	}
	return total
}

func (c *stateCellEncodedCache) byteSize() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bytes + c.layerBytes
}

func (c *stateCellEncodedCache) prewriteTarget() (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Staged layers hold records too: reporting an empty cache while a layer is
	// unfolded would drop this cache's prewrite token from the checkpoint's
	// wait target and let metadata commit ahead of its cells.
	if len(c.records) == 0 && len(c.layers) == 0 {
		return 0, true
	}
	if c.prewritePending > 0 || c.prewriteToken == 0 {
		return 0, false
	}
	return c.prewriteToken, true
}

func (w *stateCellWindowCache) applyBlockStateUpdate(previous []*storage.BlockState, block PreparedBlock) (stateUpdateApplyResult, error) {
	return w.applyPreparedMerkleUpdate(previous, block.StateUpdate, block.StateUpdateToCells)
}

func (w *stateCellWindowCache) applyPreparedMerkleUpdate(previous []*storage.BlockState, update *cell.Cell, prepared storage.StateCellRecords) (stateUpdateApplyResult, error) {
	updateTo, err := storage.MerkleUpdateTarget(update)
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
	wait, err := w.rememberApplied(nextRoot, prepared)
	if err != nil {
		return stateUpdateApplyResult{}, err
	}
	reloadedRoot, err := w.reloadAppliedRoot(nextRoot)
	if err != nil {
		return stateUpdateApplyResult{}, err
	}
	// The prewriter's queue backpressure runs only now: the applied state is
	// already resolvable through the window, so a full queue parks this
	// applier without hiding the state from dependents or holding any lock
	// loaders need.
	if err = wait(); err != nil {
		return stateUpdateApplyResult{}, err
	}
	return stateUpdateApplyResult{
		PreviousRoot: currentRoot,
		NextRoot:     reloadedRoot,
	}, nil
}

func (w *stateCellWindowCache) rememberApplied(root *cell.Cell, prepared storage.StateCellRecords) (func() error, error) {
	if prepared.Empty() {
		return nil, fmt.Errorf("prepared state update cells are missing")
	}

	// The destination root check itself runs while the layer is built, off any
	// shared lock.
	return w.stagePreparedStateRecords(root.GetMetadata().Hash, prepared)
}

func (w *stateCellWindowCache) reloadAppliedRoot(root *cell.Cell) (*cell.Cell, error) {
	hash := root.GetMetadata().Hash
	loaded, err := w.loader()(hash)
	if err != nil {
		return nil, fmt.Errorf("reload applied state root %x from state cell window cache: %w", hash, err)
	}
	loadedHash := loaded.GetMetadata().Hash
	if loadedHash != hash {
		return nil, fmt.Errorf("reloaded applied state root hash mismatch: got=%x want=%x", loadedHash, hash)
	}
	return loaded, nil
}

func (w *stateCellWindowCache) addPreparedRecords(records storage.StateCellRecords) error {
	wait, err := w.stagePreparedRecords(records)
	if err != nil {
		return err
	}
	// Commit-side staging: fold every staged layer while on the commit
	// goroutine, so the apply hot path never pays for the base-map inserts.
	// Folding before the backpressure wait keeps the window compact even when
	// the wait parks this goroutine.
	w.compactStagedLayers(0)
	return wait()
}

func (w *stateCellWindowCache) stagePreparedRecords(records storage.StateCellRecords) (func() error, error) {
	if records.Empty() {
		return noPrewriteBackpressure, nil
	}
	return w.stageLayer(newStateCellRecordLayer(records))
}

// stagePreparedStateRecords publishes an applied block's cells into the shared
// window without invoking the prewriter's queue backpressure: the returned
// wait carries it and must run once the state is visible to dependents.
func (w *stateCellWindowCache) stagePreparedStateRecords(root cell.Hash, records storage.StateCellRecords) (func() error, error) {
	if records.Empty() {
		return noPrewriteBackpressure, nil
	}

	layer, err := newStateCellRecordLayerForRoot(records, root)
	if err != nil {
		return nil, err
	}
	return w.stageLayer(layer)
}

// stageLayer takes the window read lock only to hand the finished layer to the
// active cache, so building it (its lookup slots included) never delays a
// checkpoint swap or another applier.
func (w *stateCellWindowCache) stageLayer(layer *stateCellRecordLayer) (func() error, error) {
	w.mu.RLock()
	request := w.active.stageLayer(layer, w.prewriter)
	w.mu.RUnlock()

	wait, err := request.enqueueDetached()
	if err != nil {
		return nil, err
	}
	if w.activeStagedLayers() > stateCellWindowMaxStagedLayers {
		w.compactStagedLayers(stateCellWindowMaxStagedLayers)
	}
	return wait, nil
}

// compactStagedLayers folds staged layers of the active cache into its base map
// until at most target remain, in bounded slices so concurrent loaders and
// stagers get the lock between slices. The layer count is sampled once, so an
// applier staging new layers meanwhile cannot keep this goroutine folding.
// Only the active cache is ever folded: a cache frozen by beginCheckpoint must
// stay immutable for the checkpoint readers, which is what taking the window
// read lock around each slice guarantees — after a swap the fold simply finds
// the fresh active empty and stops.
func (w *stateCellWindowCache) compactStagedLayers(target int) {
	for remove := w.activeStagedLayers() - target; remove > 0; {
		w.mu.RLock()
		folded, owed := w.active.foldLayers(remove, stateCellWindowFoldSliceRecords)
		w.mu.RUnlock()
		if !owed {
			return
		}
		remove -= folded
	}
}

func (w *stateCellWindowCache) activeStagedLayers() int {
	w.mu.RLock()
	active := w.active
	w.mu.RUnlock()
	return active.stagedLayers()
}

func (w *stateCellWindowCache) loader() cell.LazyCellLoader {
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
			return nil, fmt.Errorf("state cell %x is not in state cell window cache and base loader is not set", hash)
		}
		return base(hash)
	}
	return load
}

func (w *stateCellWindowCache) retainedLoader(base cell.LazyCellLoader) cell.LazyCellLoader {
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
	// Fold before freezing: a frozen cache is immutable, so any layer left
	// unfolded here stays unfolded for the checkpoint's whole lifetime, and its
	// readers then have to flatten base plus layers into a fresh copy of every
	// record. Pipelines that never fold on a commit goroutine (cell generation
	// rotation, archive import) would otherwise pay that copy at every
	// checkpoint. Checkpoint boundaries are not the apply hot path, and the
	// work is bounded by the staged-layer valve.
	w.compactStagedLayers(0)

	w.mu.Lock()
	defer w.mu.Unlock()

	cache := w.active
	w.active = newStateCellEncodedCache(4096)
	if cache.len() > 0 {
		w.pending = append(w.pending, cache)
	}

	return &stateCellCheckpointCache{
		window: w,
		caches: append([]*stateCellEncodedCache(nil), w.pending...),
	}
}

func (w *stateCellWindowCache) byteSize() uint64 {
	sources := w.loaderSources()
	total := sources.active.byteSize()
	for _, cache := range sources.pending {
		total += cache.byteSize()
	}
	return total
}

func (w *stateCellWindowCache) activeByteSize() uint64 {
	w.mu.RLock()
	active := w.active
	w.mu.RUnlock()
	return active.byteSize()
}

// adoptRecordsFrom moves the populated record caches of src into this window's
// pending set, so cells applied through src stay resolvable until they are
// checkpointed here. Used by archive catch-up when a window is committed.
func (w *stateCellWindowCache) adoptRecordsFrom(src *stateCellWindowCache) {
	if w == src {
		return
	}

	sources := src.loaderSources()
	caches := make([]*stateCellEncodedCache, 0, 1+len(sources.pending))
	for _, cache := range sources.pending {
		if cache.len() > 0 {
			caches = append(caches, cache)
		}
	}
	if sources.active.len() > 0 {
		caches = append(caches, sources.active)
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
	if len(c.caches) == 1 {
		// beginCheckpoint detaches this cache from the active writer, so its
		// record slice and staged layers stay immutable until complete removes
		// it from pending. The zero-copy path needs everything in one slice,
		// which holds only when no staged layer is left unfolded.
		cache := c.caches[0]
		cache.mu.RLock()
		records := cache.records
		layered := len(cache.layers) > 0
		cache.mu.RUnlock()
		if !layered {
			return records
		}
	}
	return c.cells().AppendTo(nil)
}

func (c *stateCellCheckpointCache) cells() storage.StateCellRecords {
	if len(c.caches) == 0 {
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
	if len(c.caches) == 0 {
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

func (c *stateCellCheckpointCache) retainedLoader(base cell.LazyCellLoader) cell.LazyCellLoader {
	if len(c.caches) == 0 {
		return nil
	}

	caches := append([]*stateCellEncodedCache(nil), c.caches...)
	metrics := c.window.metrics
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
	if len(c.caches) == 0 {
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
		rootHash := view.HashKeyAt(0)
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
