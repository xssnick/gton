package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flexserver/service/storage"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Store) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64) (*cell.Cell, error) {
	roots, err := s.importStateCellTrees(ctx, []stateCellTreeImport{{
		block:       block,
		root:        root,
		parsedCells: parsedCells,
		totalCells:  totalCells,
	}})
	if err != nil {
		return nil, err
	}
	return roots[0], nil
}

func (s *Store) ImportStateCellTrees(ctx context.Context, trees []storage.StateCellTreeImport) ([]*cell.Cell, error) {
	imports := make([]stateCellTreeImport, len(trees))
	for i, tree := range trees {
		imports[i] = stateCellTreeImport{
			block:       tree.Block,
			root:        tree.Root,
			parsedCells: tree.ParsedCells,
			totalCells:  tree.TotalCells,
		}
	}
	return s.importStateCellTrees(ctx, imports)
}

type stateCellTreeImport struct {
	block       ton.BlockIDExt
	root        *cell.Cell
	parsedCells []cell.Cell
	totalCells  uint64
}

type stateCellTreeSync struct {
	block     ton.BlockIDExt
	rootHash  cell.Hash
	cellCount uint64
}

func (s *Store) importStateCellTrees(ctx context.Context, trees []stateCellTreeImport) ([]*cell.Cell, error) {
	if len(trees) == 0 {
		return nil, nil
	}

	roots := make([]*cell.Cell, len(trees))
	syncs := make([]stateCellTreeSync, len(trees))
	for i, tree := range trees {
		if tree.root == nil {
			return nil, fmt.Errorf("state cell tree root is nil")
		}

		rootCellHash := tree.root.HashKey()
		if _, err := s.saveStateCellTree(ctx, stateCellTreeSave{
			block:       tree.block,
			root:        tree.root,
			parsedCells: tree.parsedCells,
			totalCells:  tree.totalCells,
		}); err != nil {
			return nil, err
		}
		syncs[i] = stateCellTreeSync{
			block:     tree.block,
			rootHash:  rootCellHash,
			cellCount: tree.totalCells,
		}
	}

	if err := s.syncStateCellTrees(syncs); err != nil {
		return nil, fmt.Errorf("sync state cells: %w", err)
	}

	for i, sync := range syncs {
		lazyRoot, err := s.loadLazyCell(ctx, sync.rootHash[:])
		if err != nil {
			return nil, fmt.Errorf("load persisted lazy state root: %w", err)
		}
		roots[i] = lazyRoot

		s.log.Debug().
			Str("block", storage.FormatBlockRef(sync.block)).
			Uint64("cells", sync.cellCount).
			Msg("state cell tree imported and switched to lazy celldb root")
	}
	return roots, nil
}

func (s *Store) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, uint64, error) {
	state, err := s.blockStateMeta(ctx, block)
	if err == nil {
		if len(rootHash) > 0 && !bytes.Equal(state.StateRootHash, rootHash) {
			return nil, 0, storage.ErrNotFound
		}

		root, err := s.loadLazyCell(ctx, state.StateCellHash)
		if err != nil {
			return nil, 0, err
		}
		hash := root.HashKey(0)
		if !bytes.Equal(hash[:], state.StateRootHash) {
			return nil, 0, storage.ErrNotFound
		}
		return root, state.CellsCount, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, 0, err
	}

	raw, err := s.getHotCopy(ctx, hotKeyStateCellSync(block))
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, 0, err
		}

		root, err := s.loadStateCellTreeByRootHash(ctx, rootHash)
		if err != nil {
			return nil, 0, err
		}
		return root, 0, nil
	}

	cellHash, cellsCount, err := decodeStateCellSync(raw)
	if err != nil {
		return nil, 0, err
	}

	root, err := s.loadLazyCell(ctx, cellHash[:])
	if err != nil {
		return nil, 0, err
	}
	if len(rootHash) > 0 {
		hash := root.HashKey(0)
		if !bytes.Equal(hash[:], rootHash) {
			return nil, 0, storage.ErrNotFound
		}
	}
	return root, cellsCount, nil
}

func (s *Store) loadStateCellTreeByRootHash(ctx context.Context, rootHash []byte) (*cell.Cell, error) {
	if len(rootHash) != 32 {
		return nil, storage.ErrNotFound
	}

	root, err := s.loadLazyCell(ctx, rootHash)
	if err != nil {
		return nil, err
	}

	hash := root.HashKey(0)
	if !bytes.Equal(hash[:], rootHash) {
		return nil, storage.ErrNotFound
	}
	return root, nil
}

func (s *Store) syncStateCellTree(block ton.BlockIDExt, rootHash cell.Hash, totalCells uint64) error {
	return s.syncStateCellTrees([]stateCellTreeSync{{
		block:     block,
		rootHash:  rootHash,
		cellCount: totalCells,
	}})
}

func (s *Store) syncStateCellTrees(syncs []stateCellTreeSync) error {
	if err := s.flushCellDBs(); err != nil {
		return fmt.Errorf("flush state cells before sync marker: %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errPebbleClosed
	}

	s.log.Debug().
		Int("trees", len(syncs)).
		Msg("syncing persisted state cells before returning lazy roots")

	batch := s.hot.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, sync := range syncs {
		if err := batch.Set(hotKeyStateCellSync(sync.block), encodeStateCellSync(sync.rootHash, sync.cellCount), pebble.NoSync); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) replaceBlockStateWithLazyRoot(state *storage.BlockState, saved storage.BlockState, parsed *storage.BlockState, root *cell.Cell) error {
	var err error
	if root == nil {
		root, err = s.loadLazyCell(context.Background(), saved.StateCellHash)
		if err != nil {
			return fmt.Errorf("load persisted lazy state root: %w", err)
		}
	}

	state.Block = saved.Block
	state.StateRootHash = bytes.Clone(saved.StateRootHash)
	state.StateCellHash = bytes.Clone(saved.StateCellHash)
	state.StateFileHash = bytes.Clone(saved.StateFileHash)
	state.CellsCount = saved.CellsCount
	state.Cell = root
	state.Parsed = saved.Parsed
	if parsed != nil {
		state.Parsed = parsed.Parsed
	}
	state.DownloadedAt = saved.DownloadedAt

	s.log.Debug().
		Str("block", storage.FormatBlockRef(saved.Block)).
		Uint64("cells", saved.CellsCount).
		Msg("block state switched to lazy celldb root")
	return nil
}

const (
	stateCellSaveSourceDFS         = "dfs"
	stateCellSaveSourceParsedBatch = "parsed_batch"
)

type stateCellTreeSave struct {
	block       ton.BlockIDExt
	root        *cell.Cell
	parsedCells []cell.Cell
	totalCells  uint64
	exists      *stateCellExistenceCache
}

func (s *Store) saveStateCellTree(ctx context.Context, req stateCellTreeSave) (bool, error) {
	if req.root != nil && req.root.GetType() == cell.PrunedCellType {
		return false, fmt.Errorf("state cell tree root is pruned")
	}
	if len(req.parsedCells) > 0 {
		return s.saveParsedStateCellsBatch(ctx, req)
	}
	return s.saveStateCellTreeDFS(ctx, req)
}

func (s *Store) saveStateCellTreeDFS(ctx context.Context, req stateCellTreeSave) (bool, error) {
	return s.saveStateCellTreesDFSBatch(ctx, []stateCellTreeSave{req}, req.exists)
}

func (s *Store) saveStateCellTreesDFSBatch(ctx context.Context, trees []stateCellTreeSave, exists *stateCellExistenceCache) (bool, error) {
	if len(trees) == 0 {
		return false, nil
	}

	writer, err := s.newStateCellBatchWriter(exists)
	if err != nil {
		return false, err
	}
	defer writer.close()

	totalCells := uint64(0)
	for _, tree := range trees {
		if tree.root == nil {
			return false, fmt.Errorf("state cell tree root is nil")
		}
		if tree.root.GetType() == cell.PrunedCellType {
			return false, fmt.Errorf("state cell tree root is pruned")
		}
		totalCells += tree.totalCells
	}

	progress := newStateCellSaveProgress(s.log, trees[0].block, stateCellSaveSourceDFS, totalCells, writer, zerolog.DebugLevel)
	progress.logStart()

	stack := make([]*cell.Cell, 0, len(trees))
	visited := make(map[cell.Hash]struct{}, cellVisitSetCapacity(totalCells))
	lazyBoundaries := map[cell.Hash]struct{}{}
	for _, tree := range trees {
		rootHash := stateCellStorageHash(tree.root)
		if _, ok := visited[rootHash]; ok {
			continue
		}
		visited[rootHash] = struct{}{}
		stack = append(stack, tree.root)
	}

	for len(stack) > 0 {
		if progress.processed&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
			}
		}

		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]
		currentMeta := current.GetMetadata()
		currentHash := currentMeta.Hash

		if current.IsLazy() {
			progress.skippedExisting++
			progress.processed++
			continue
		}
		if current.GetType() == cell.PrunedCellType {
			progress.skippedExisting++
			progress.processed++
			continue
		}

		refs, refCells, err := stateCellRefs(current, currentMeta)
		if err != nil {
			return false, fmt.Errorf("load state cell refs hash=%x lazy=%t virtual=%t type=%d: %w", currentHash[:], current.IsLazy(), current.IsVirtualized(), current.GetType(), err)
		}
		rememberLazyStateBoundaries(lazyBoundaries, refs)
		for i := 0; i < len(refs); i++ {
			ref := refs[i]
			refHash := ref.Hash
			if _, ok := visited[refHash]; ok {
				continue
			}
			if ref.Lazy {
				progress.skippedExisting++
				continue
			}
			refCell := refCells[i]
			if refCell == nil || refCell.IsLazy() {
				return false, fmt.Errorf("state ref %x from parent %x ref=%d has no body", refHash[:], currentHash[:], i)
			}

			visited[refHash] = struct{}{}
			stack = append(stack, refCell)
		}

		if err := writer.add(current, currentMeta, refs); err != nil {
			return false, err
		}
		progress.processed++

		if writer.pendingBytes() >= stateCellImportBatchTargetBytes {
			if err := progress.flush(); err != nil {
				return false, err
			}
		}
	}

	if err := progress.flush(); err != nil {
		return false, err
	}
	if err := validateLazyStateBoundaries(writer, lazyBoundaries); err != nil {
		return false, fmt.Errorf("validate state lazy boundaries for %s: %w", storage.FormatBlockRef(trees[0].block), err)
	}
	progress.logDone()
	return progress.applied > 0, nil
}

func (s *Store) saveParsedStateCellsBatch(ctx context.Context, req stateCellTreeSave) (bool, error) {
	totalCells := req.totalCells
	if totalCells == 0 {
		totalCells = uint64(len(req.parsedCells))
	}

	writer, err := s.newStateCellBatchWriter(nil)
	if err != nil {
		return false, err
	}
	defer writer.close()

	progress := newStateCellSaveProgress(s.log, req.block, stateCellSaveSourceParsedBatch, totalCells, writer, zerolog.InfoLevel)
	progress.logStart()

	for i := range req.parsedCells {
		if progress.processed&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
			}
		}

		current := &req.parsedCells[i]
		if current.GetType() == cell.PrunedCellType {
			progress.skippedExisting++
			progress.processed++
			continue
		}

		_, err := writer.addStateCell(current)
		if err != nil {
			return false, err
		}
		for level := 0; level < current.Level(); level++ {
			view := current.Virtualize(uint8(level))
			if view == current {
				continue
			}
			_, err := writer.addStateCell(view)
			if err != nil {
				return false, err
			}
		}
		progress.processed++

		if writer.pendingBytes() >= stateCellImportBatchTargetBytes {
			if err := progress.flush(); err != nil {
				return false, err
			}
		}
	}

	if err := progress.flush(); err != nil {
		return false, err
	}
	progress.logDone()
	return progress.applied > 0, nil
}

func rememberLazyStateBoundaries(boundaries map[cell.Hash]struct{}, refs []cell.RefMetadata) {
	for _, ref := range refs {
		if ref.Lazy {
			boundaries[ref.Hash] = struct{}{}
		}
	}
}

func validateLazyStateBoundaries(writer *stateCellBatchWriter, boundaries map[cell.Hash]struct{}) error {
	for hash := range boundaries {
		exists, err := writer.has(hash)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("lazy boundary %x is missing from state cell db", hash[:])
		}
	}
	return nil
}

type stateCellSaveProgress struct {
	log      zerolog.Logger
	writer   *stateCellBatchWriter
	blockRef string
	source   string
	level    zerolog.Level
	total    uint64
	started  time.Time
	lastLog  time.Time

	processed       int64
	applied         int64
	bytesWritten    int64
	skippedExisting int64
}

func newStateCellSaveProgress(log zerolog.Logger, block ton.BlockIDExt, source string, total uint64, writer *stateCellBatchWriter, level zerolog.Level) stateCellSaveProgress {
	now := time.Now()
	return stateCellSaveProgress{
		log:      log,
		writer:   writer,
		blockRef: storage.FormatBlockRef(block),
		source:   source,
		level:    level,
		total:    total,
		started:  now,
		lastLog:  now,
	}
}

func (p *stateCellSaveProgress) logStart() {
	event := p.log.Debug().
		Str("block", p.blockRef).
		Str("source", p.source).
		Uint64("total_cells", p.total)
	addCellProgress(event, 0, p.total)
	event.Msg("persisting state cells")
}

func (p *stateCellSaveProgress) flush() error {
	stats, err := p.writer.flush()
	if err != nil {
		return err
	}
	p.applied += stats.cells
	p.bytesWritten += stats.bytes

	now := time.Now()
	if now.Sub(p.lastLog) >= stateCellSaveProgressInterval {
		p.logProgress(false, now)
		p.lastLog = now
	}
	return nil
}

func (p *stateCellSaveProgress) logDone() {
	p.logProgress(true, time.Now())
}

func (p *stateCellSaveProgress) logProgress(done bool, now time.Time) {
	elapsed := now.Sub(p.started)
	event := p.log.WithLevel(p.level).
		Str("block", p.blockRef).
		Str("source", p.source).
		Int64("processed_cells", p.processed).
		Int64("applied_cells", p.applied).
		Int64("skipped_existing_cells", p.skippedExisting).
		Int64("bytes", p.bytesWritten).
		Uint64("total_cells", p.total).
		Dur("elapsed", elapsed).
		Str("speed", formatCellRate(p.processed, elapsed))
	addCellProgress(event, p.processed, p.total)
	if done {
		event.Msg("state cells persisted")
		return
	}
	event.Int("pending_batch_cells", p.writer.pendingCells()).Msg("state cell persistence progress")
}

func stateCellRefs(cl *cell.Cell, meta cell.Metadata) ([]cell.RefMetadata, [4]*cell.Cell, error) {
	if len(meta.Refs) > 4 {
		return nil, [4]*cell.Cell{}, fmt.Errorf("cell refs count is too large: %d", len(meta.Refs))
	}

	refs := append([]cell.RefMetadata(nil), meta.Refs...)
	var refCells [4]*cell.Cell
	for i := range refs {
		if refs[i].Lazy {
			continue
		}

		refCell, err := cl.PeekRef(i)
		if err != nil {
			return nil, [4]*cell.Cell{}, err
		}
		refCell = stateCellRefView(cl, refCell)
		if refCell.GetType() == cell.PrunedCellType {
			refs[i].Lazy = true
			continue
		}
		refCells[i] = refCell
	}
	return refs, refCells, nil
}

func stateCellStorageHash(cl *cell.Cell) cell.Hash {
	return cl.GetMetadata().Hash
}

func stateCellRefView(parent *cell.Cell, ref *cell.Cell) *cell.Cell {
	if parent.IsVirtualized() {
		return ref
	}

	effectiveLevel := uint8(0)
	switch parent.GetType() {
	case cell.MerkleProofCellType, cell.MerkleUpdateCellType:
		effectiveLevel = 1
	}
	return ref.Virtualize(effectiveLevel)
}

func cellVisitSetCapacity(totalCells uint64) int {
	if totalCells == 0 {
		return 1024
	}
	const maxInitialVisitSetCapacity = 1 << 20
	if totalCells > maxInitialVisitSetCapacity {
		return maxInitialVisitSetCapacity
	}
	maxInt := int(^uint(0) >> 1)
	if totalCells > uint64(maxInt) {
		return maxInt
	}
	return int(totalCells)
}

type stateCellWriteStats struct {
	cells int64
	bytes int64
}

type stateCellExistenceCache struct {
	values map[cell.Hash]bool
}

func newStateCellExistenceCache() *stateCellExistenceCache {
	return &stateCellExistenceCache{values: map[cell.Hash]bool{}}
}

func (c *stateCellExistenceCache) get(hash cell.Hash) (bool, bool) {
	if c == nil {
		return false, false
	}
	exists, ok := c.values[hash]
	return exists, ok
}

func (c *stateCellExistenceCache) set(hash cell.Hash, exists bool) {
	if c == nil {
		return
	}
	c.values[hash] = exists
}

func (c *stateCellExistenceCache) clear() {
	if c == nil {
		return
	}
	clear(c.values)
}

type stateCellBatchWriter struct {
	cells  *cellBatchWriter
	exists *stateCellExistenceCache
}

func (s *Store) newStateCellBatchWriter(exists *stateCellExistenceCache) (*stateCellBatchWriter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errPebbleClosed
	}

	return &stateCellBatchWriter{
		cells:  s.cells.newBatchWriter(),
		exists: exists,
	}, nil
}

func (w *stateCellBatchWriter) add(cl *cell.Cell, meta cell.Metadata, refs []cell.RefMetadata) error {
	valueLen, d1, d2, err := stateCellEncodedLen(cl, refs)
	if err != nil {
		return err
	}
	hash := meta.Hash
	if err = w.cells.setDeferred(hash[:], valueLen, func(value []byte) {
		encodeStateCellRecordTo(value, cl, refs, d1, d2)
	}); err != nil {
		return err
	}
	w.exists.set(hash, true)
	return nil
}

func (w *stateCellBatchWriter) addStateCell(cl *cell.Cell) ([]cell.RefMetadata, error) {
	meta := cl.GetMetadata()
	refs, _, err := stateCellRefs(cl, meta)
	if err != nil {
		hash := stateCellStorageHash(cl)
		return nil, fmt.Errorf("load state cell refs hash=%x lazy=%t virtual=%t type=%d: %w", hash[:], cl.IsLazy(), cl.IsVirtualized(), cl.GetType(), err)
	}
	if err := w.add(cl, meta, refs); err != nil {
		return nil, err
	}
	return refs, nil
}

func stateCellEncodedLen(cl *cell.Cell, refs []cell.RefMetadata) (int, byte, byte, error) {
	cellBits := cl.BitsSize()
	if cellBits > 1023 {
		return 0, 0, 0, fmt.Errorf("cell bits length is too large: %d", cellBits)
	}
	if len(refs) > 4 {
		return 0, 0, 0, fmt.Errorf("cell refs count is too large: %d", len(refs))
	}

	d1, d2 := stateCellRecordDescriptors(cl, len(refs), cellBits)
	size := 2 + cl.SerializedBOCBodySize()
	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	for _, ref := range refs {
		hashesCount := storage.CellRefHashesCount(ref.LevelMask.Mask)
		if len(ref.Hashes) != hashesCount || len(ref.Depths) != hashesCount {
			return 0, 0, 0, fmt.Errorf("invalid ref metadata for %x: hashes=%d depths=%d want=%d", ref.Hash[:], len(ref.Hashes), len(ref.Depths), hashesCount)
		}
		refSize := 1 + hashesCount*(cellRecordHashSize+cellRecordDepthSize)
		refsSize += refSize
		if stateCellRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		compactRefsSize += refSize
	}
	if hasCommonRef && compactRefsSize <= refsSize {
		size += compactRefsSize
	} else {
		size += refsSize
	}
	return size, d1, d2, nil
}

func stateCellRefCommon(ref cell.RefMetadata) bool {
	return ref.LevelMask.Mask == 0 && len(ref.Hashes) == 1 && len(ref.Depths) == 1
}

func stateCellCompactRefLayout(refs []cell.RefMetadata) (byte, bool) {
	if len(refs) == 0 {
		return 0, false
	}

	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	var slowRefs byte
	for i, ref := range refs {
		refSize := 1 + len(ref.Hashes)*(cellRecordHashSize+cellRecordDepthSize)
		refsSize += refSize
		if stateCellRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	return slowRefs, hasCommonRef && compactRefsSize <= refsSize
}

func stateCellRecordDescriptors(cl *cell.Cell, refsCount int, bitLen uint) (byte, byte) {
	d1 := byte(refsCount)
	if cl.IsSpecial() {
		d1 += 8
	}
	d1 += cl.LevelMask().Mask * 32

	d2 := byte((bitLen / 8) * 2)
	if bitLen%8 != 0 {
		d2++
	}
	return d1, d2
}

func encodeStateCellRecordTo(buf []byte, cl *cell.Cell, refs []cell.RefMetadata, d1 byte, d2 byte) {
	slowRefs, compactRefs := stateCellCompactRefLayout(refs)
	pos := 0
	if compactRefs {
		d1 |= cellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = d2
	pos += 2

	pos += cl.SerializeBOCBodyTo(buf[pos:])
	if compactRefs {
		buf[pos] = slowRefs
		pos++
	}
	for i, ref := range refs {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			copy(buf[pos:pos+cellRecordHashSize], ref.Hashes[0][:])
			pos += cellRecordHashSize
			binary.BigEndian.PutUint16(buf[pos:pos+cellRecordDepthSize], ref.Depths[0])
			pos += cellRecordDepthSize
			continue
		}

		buf[pos] = ref.LevelMask.Mask
		pos++

		for _, hash := range ref.Hashes {
			copy(buf[pos:pos+32], hash[:])
			pos += 32
		}
		for _, depth := range ref.Depths {
			binary.BigEndian.PutUint16(buf[pos:pos+2], depth)
			pos += 2
		}
	}
}

func (w *stateCellBatchWriter) flush() (stateCellWriteStats, error) {
	stats, err := w.cells.flush()
	if err != nil {
		return stateCellWriteStats{}, err
	}
	w.exists.clear()
	return stats, nil
}

func (w *stateCellBatchWriter) close() {
	w.cells.close()
}

func (w *stateCellBatchWriter) has(hash cell.Hash) (bool, error) {
	if exists, ok := w.exists.get(hash); ok {
		return exists, nil
	}
	exists, err := w.cells.store.has(hash[:])
	if err != nil {
		return false, err
	}
	w.exists.set(hash, exists)
	return exists, nil
}

func (w *stateCellBatchWriter) pendingCells() int {
	return w.cells.cellsInBatch
}

func (w *stateCellBatchWriter) pendingBytes() int {
	return w.cells.bytesInBatch
}

func addCellProgress(event *zerolog.Event, processed int64, total uint64) {
	if total == 0 {
		return
	}
	event.Str("progress", formatPercent(processed, total))
}

func formatPercent(processed int64, total uint64) string {
	if total == 0 {
		return "0.0%"
	}
	progress := float64(processed) / float64(total) * 100
	if progress > 100 {
		progress = 100
	}
	return fmt.Sprintf("%.1f%%", progress)
}

func formatCellRate(cells int64, elapsed time.Duration) string {
	if cells <= 0 || elapsed <= 0 {
		return "0 cells/s"
	}
	rate := float64(cells) / elapsed.Seconds()
	switch {
	case rate >= 1_000_000:
		return fmt.Sprintf("%.2f Mcells/s", rate/1_000_000)
	case rate >= 1_000:
		return fmt.Sprintf("%.2f Kcells/s", rate/1_000)
	default:
		return fmt.Sprintf("%.0f cells/s", rate)
	}
}

func (s *Store) SaveCells(records []*storage.CellRecord) error {
	encoded := make([]storage.EncodedCellRecord, 0, len(records))
	for _, record := range records {
		if record == nil {
			return fmt.Errorf("cell record is nil")
		}
		if len(record.Hash) != 32 {
			return fmt.Errorf("cell record hash size mismatch: %d", len(record.Hash))
		}

		var hash cell.Hash
		copy(hash[:], record.Hash)
		encoded = append(encoded, storage.EncodedCellRecord{
			Hash: hash,
			Data: encodeCellRecord(record),
		})
	}

	_, err := s.saveCellRecordBatch(context.Background(), encoded, true)
	return err
}

type cellRecordBatchStats struct {
	written int
	skipped int
	bytes   int64
}

func (s *Store) savePreparedStateCellRecords(ctx context.Context, records []storage.EncodedCellRecord) (bool, error) {
	stats, err := s.saveCellRecordBatch(ctx, records, false)
	if err != nil {
		return false, err
	}
	return stats.written > 0, nil
}

func (s *Store) saveCellRecordBatch(ctx context.Context, records []storage.EncodedCellRecord, sync bool) (cellRecordBatchStats, error) {
	var stats cellRecordBatchStats
	if len(records) == 0 {
		return stats, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return stats, errPebbleClosed
	}

	writer := s.cells.newBatchWriter()
	defer writer.close()

	written := make(map[cell.Hash]struct{}, len(records))
	for i, record := range records {
		if i&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return stats, ctx.Err()
			default:
			}
		}

		if len(record.Data) == 0 {
			return stats, fmt.Errorf("encoded cell record is empty")
		}

		if _, ok := written[record.Hash]; ok {
			stats.skipped++
			continue
		}

		if err := writer.set(record.Hash[:], record.Data); err != nil {
			return stats, err
		}
		stats.written++
		stats.bytes += int64(len(record.Data))
		written[record.Hash] = struct{}{}

		if writer.bytesInBatch >= stateCellImportBatchTargetBytes {
			if _, err := writer.flush(); err != nil {
				return stats, err
			}
		}
	}

	if _, err := writer.flush(); err != nil {
		return stats, err
	}
	if sync {
		if err := s.cells.flush(); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (s *Store) CellRecord(ctx context.Context, hash []byte) (*storage.CellRecord, error) {
	raw, err := s.getCellCopy(ctx, hash)
	if err != nil {
		return nil, err
	}
	record, err := decodeCellRecord(hash, raw)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return s.loadLazyCell(ctx, hash)
}

func (s *Store) LazyCellLoader() cell.LazyCellLoader {
	return func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := s.loadLazyCell(context.Background(), hash[:])
		if err != nil {
			return nil, fmt.Errorf("load lazy cell %x: %w", hash[:], err)
		}
		return loaded, nil
	}
}

func (s *Store) loadLazyCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	if loaded, ok := s.cellCache.get(hash); ok {
		return loaded, nil
	}

	raw, err := s.getCellCopy(ctx, hash)
	if err != nil {
		return nil, err
	}
	loaded, err := storage.LazyCellRecord(decodeCellRecordTrusted(hash, raw), s.LazyCellLoader())
	if err != nil {
		return nil, fmt.Errorf("create lazy cell %x: %w", hash, err)
	}
	s.cellCache.set(hash, loaded)
	return loaded, nil
}
