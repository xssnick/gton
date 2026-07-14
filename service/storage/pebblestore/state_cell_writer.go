package pebblestore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	stateCellSaveSourceDFS     = "dfs"
	stateCellSaveSourceBOCView = "boc_view"

	stateBOCImportMinCellsPerWorker = 4096
)

type stateCellTreeSave struct {
	block          ton.BlockIDExt
	root           *cell.Cell
	totalCells     uint64
	cellGeneration uint64
}

type stateCellSaveStats struct {
	applied bool
}

func (s *Store) saveStateCellTree(ctx context.Context, req stateCellTreeSave) (stateCellSaveStats, error) {
	if req.root.GetType() == cell.PrunedCellType {
		return stateCellSaveStats{}, fmt.Errorf("state cell tree root is pruned")
	}
	if req.root.IsVirtualized() {
		return stateCellSaveStats{}, fmt.Errorf("state cell tree root is virtualized")
	}
	return s.saveStateCellTreesDFSBatch(ctx, []stateCellTreeSave{req})
}

func (s *Store) saveStateBOCView(ctx context.Context, generation uint64, block ton.BlockIDExt, view *cell.BOCView) error {
	workerCount := stateBOCImportWorkerCount(view.Cells())
	workerBatchTargetBytes := stateBOCImportWorkerBatchTarget(workerCount)
	workerBatchInitialBytes := stateCellImportBatchTargetBytes / workerCount
	writers := make([]*stateCellBatchWriter, workerCount)
	for i := range writers {
		writer, err := s.newStateCellBatchWriter(ctx, generation, workerBatchInitialBytes)
		if err != nil {
			for j := 0; j < i; j++ {
				writers[j].close()
			}
			return err
		}
		writers[i] = writer
	}
	defer func() {
		for _, writer := range writers {
			writer.close()
		}
	}()

	progress := newStateCellSaveProgress(
		s.log,
		block,
		stateCellSaveSourceBOCView,
		uint64(view.Cells()),
		writers[0],
		zerolog.InfoLevel,
	)
	progress.logStart()

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	work := stateBOCImport{
		ctx:              workerCtx,
		view:             view,
		batchTargetBytes: workerBatchTargetBytes,
		progress:         &progress,
	}

	workerErrs := make([]error, workerCount)
	var wg sync.WaitGroup
	for i, writer := range writers {
		start := uint32(uint64(view.Cells()) * uint64(i) / uint64(workerCount))
		end := uint32(uint64(view.Cells()) * uint64(i+1) / uint64(workerCount))
		wg.Add(1)
		go func(i int, writer *stateCellBatchWriter, start, end uint32) {
			defer wg.Done()

			workerErrs[i] = work.saveRange(writer, start, end)
			if workerErrs[i] != nil {
				cancel()
			}
		}(i, writer, start, end)
	}
	wg.Wait()

	if err := stateBOCImportError(ctx, workerErrs); err != nil {
		return err
	}

	progress.logDone()
	return nil
}

type stateBOCImport struct {
	ctx              context.Context
	view             *cell.BOCView
	batchTargetBytes int
	progress         *stateCellSaveProgress

	flushMu sync.Mutex
}

func (s *stateBOCImport) saveRange(writer *stateCellBatchWriter, start, end uint32) error {
	reader := s.view.NewReader()
	var refs stateBOCRefs
	processed := int64(0)

	flush := func() error {
		s.flushMu.Lock()
		defer s.flushMu.Unlock()

		if err := s.ctx.Err(); err != nil {
			return err
		}
		stats, err := writer.flush()
		if err != nil {
			return err
		}

		s.progress.processed += processed
		processed = 0
		s.progress.addFlush(stats, -1)
		return nil
	}

	for idx := start; idx < end; idx++ {
		if (idx-start)&0x3fff == 0 {
			if err := s.ctx.Err(); err != nil {
				return err
			}
		}

		bocCell, err := reader.ReadCell(idx)
		if err != nil {
			return fmt.Errorf("load boc state cell #%d: %w", idx, err)
		}
		if err = stateCellRefsForBOCView(s.view, bocCell, &refs); err != nil {
			return fmt.Errorf("load boc state cell refs hash=%x index=%d: %w", bocCell.Meta.Hash, idx, err)
		}
		if err = writer.addBOCCell(bocCell, &refs); err != nil {
			return err
		}
		processed++

		if writer.pendingBytes() >= s.batchTargetBytes {
			if err = flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func stateBOCImportWorkerCount(cells uint32) int {
	workers := int((uint64(cells) + stateBOCImportMinCellsPerWorker - 1) / stateBOCImportMinCellsPerWorker)
	workers = min(workers, runtime.GOMAXPROCS(0), cellDBShardCount)
	if workers == 0 {
		return 1
	}
	return workers
}

func stateBOCImportWorkerBatchTarget(workers int) int {
	aggregate := min(stateBOCImportMaxBatchBytes, stateCellImportBatchTargetBytes*workers)
	return aggregate / workers
}

func stateBOCImportError(ctx context.Context, workerErrs []error) error {
	var firstErr error
	for _, err := range workerErrs {
		if err != nil {
			firstErr = err
			break
		}
	}
	if firstErr == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !errors.Is(firstErr, context.Canceled) {
		return firstErr
	}

	for _, err := range workerErrs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return firstErr
}

func (s *Store) saveStateCellTreesDFSBatch(ctx context.Context, trees []stateCellTreeSave) (stateCellSaveStats, error) {
	if len(trees) == 0 {
		return stateCellSaveStats{}, nil
	}

	generation := trees[0].cellGeneration
	for _, tree := range trees {
		if generation == 0 {
			generation = tree.cellGeneration
			continue
		}
		if tree.cellGeneration != 0 && tree.cellGeneration != generation {
			return stateCellSaveStats{}, fmt.Errorf("mixed cell generations in state cell tree batch: %d and %d", generation, tree.cellGeneration)
		}
	}

	writer, err := s.newStateCellBatchWriter(ctx, generation, stateCellImportBatchTargetBytes)
	if err != nil {
		return stateCellSaveStats{}, err
	}
	defer writer.close()

	totalCells := uint64(0)
	for _, tree := range trees {
		if tree.root.GetType() == cell.PrunedCellType {
			return stateCellSaveStats{}, fmt.Errorf("state cell tree root is pruned")
		}
		if tree.root.IsVirtualized() {
			return stateCellSaveStats{}, fmt.Errorf("state cell tree root is virtualized")
		}
		totalCells += tree.totalCells
	}

	progress := newStateCellSaveProgress(s.log, trees[0].block, stateCellSaveSourceDFS, totalCells, writer, zerolog.DebugLevel)
	progress.logStart()

	stack := make([]*cell.Cell, 0, len(trees))
	visited := make(map[cell.Hash]struct{}, cellVisitSetCapacity(totalCells))
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
				return stateCellSaveStats{}, ctx.Err()
			default:
			}
		}

		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]
		currentMeta := current.GetMetadata()
		currentHash := currentMeta.Hash

		if current.IsLazy() {
			progress.processed++
			continue
		}
		refs, refCells, err := stateCellRefs(current, currentMeta)
		if err != nil {
			return stateCellSaveStats{}, fmt.Errorf(
				"load state cell refs hash=%x lazy=%t virtual=%t type=%d: %w",
				currentHash,
				current.IsLazy(),
				current.IsVirtualized(),
				current.GetType(),
				err,
			)
		}
		for i := 0; i < len(refs); i++ {
			ref := refs[i]
			refHash := ref.Hash
			if _, ok := visited[refHash]; ok {
				continue
			}
			if ref.Lazy {
				continue
			}
			refCell := refCells[i]
			if refCell == nil || refCell.IsLazy() {
				return stateCellSaveStats{}, fmt.Errorf("state ref %x from parent %x ref=%d has no body", refHash, currentHash, i)
			}

			visited[refHash] = struct{}{}
			stack = append(stack, refCell)
		}

		if err := writer.addWithHash(currentMeta.Hash, current, refs); err != nil {
			return stateCellSaveStats{}, err
		}
		progress.processed++

		if writer.pendingBytes() >= stateCellImportBatchTargetBytes {
			if err := progress.flush(); err != nil {
				return stateCellSaveStats{}, err
			}
		}
	}

	if err := progress.flush(); err != nil {
		return stateCellSaveStats{}, err
	}
	progress.logDone()
	return stateCellSaveStats{applied: progress.applied > 0}, nil
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

	processed    int64
	applied      int64
	bytesWritten int64
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
	p.addFlush(stats, p.writer.pendingCells())
	return nil
}

func (p *stateCellSaveProgress) addFlush(stats stateCellWriteStats, pendingCells int) {
	p.applied += stats.cells
	p.bytesWritten += stats.bytes

	now := time.Now()
	if now.Sub(p.lastLog) >= stateCellSaveProgressInterval {
		p.logProgress(false, now, pendingCells)
		p.lastLog = now
	}
}

func (p *stateCellSaveProgress) logDone() {
	p.logProgress(true, time.Now(), -1)
}

func (p *stateCellSaveProgress) logProgress(done bool, now time.Time, pendingCells int) {
	elapsed := now.Sub(p.started)
	event := p.log.WithLevel(p.level).
		Str("block", p.blockRef).
		Str("source", p.source).
		Int64("processed_cells", p.processed).
		Int64("applied_cells", p.applied).
		Int64("bytes", p.bytesWritten).
		Uint64("total_cells", p.total).
		Dur("elapsed", elapsed).
		Str("speed", logutil.FormatCellRate(uint64(p.processed), elapsed))
	addCellProgress(event, p.processed, p.total)
	if done {
		event.Msg("state cells imported into celldb")
		return
	}
	if pendingCells >= 0 {
		event.Int("pending_batch_cells", pendingCells)
	}
	event.Msg("state to cell db import progress")
}

func stateCellRefs(cl *cell.Cell, meta cell.Metadata) ([]cell.RefMetadata, [4]*cell.Cell, error) {
	if len(meta.Refs) > 4 {
		return nil, [4]*cell.Cell{}, fmt.Errorf("cell refs count is too large: %d", len(meta.Refs))
	}

	refs := meta.Refs
	var refCells [4]*cell.Cell
	for i, metaRef := range meta.Refs {
		if metaRef.Lazy {
			continue
		}

		refCell, err := cl.PeekRef(i)
		if err != nil {
			return nil, [4]*cell.Cell{}, err
		}
		refCells[i] = refCell

		// GetMetadata exposes the logical ref at the parent's effective level.
		// A concrete branch above a pruned boundary must instead be persisted by
		// its raw hash and metadata so it can be loaded back without the proof.
		if metaRef.Hash != refCell.HashKey() {
			refMeta := refCell.GetMetadata()
			refs[i] = cell.RefMetadata{
				Hash:      refMeta.Hash,
				LevelMask: refMeta.LevelMask,
				Hashes:    refMeta.Hashes,
				Depths:    refMeta.Depths,
				Lazy:      refCell.IsLazy(),
			}
		}
	}
	return refs, refCells, nil
}

type stateBOCRefs struct {
	items [4]cell.BOCCellMeta
	count int
}

type stateCellRecordLayout struct {
	d1          byte
	d2          byte
	slowRefs    byte
	compactRefs bool
}

func stateCellRefsForBOCView(view *cell.BOCView, cl cell.BOCCellView, refs *stateBOCRefs) error {
	if cl.Refs.Count > 4 {
		return fmt.Errorf("cell refs count is too large: %d", cl.Refs.Count)
	}
	refs.count = int(cl.Refs.Count)
	for i := 0; i < refs.count; i++ {
		meta, err := view.CellMeta(cl.Refs.Items[i])
		if err != nil {
			return err
		}
		refs.items[i] = meta
	}
	for i := refs.count; i < len(refs.items); i++ {
		refs.items[i] = cell.BOCCellMeta{}
	}
	return nil
}

func stateCellStorageHash(cl *cell.Cell) cell.Hash {
	return cl.GetMetadata().Hash
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

type stateCellBatchWriter struct {
	cells          *cellBatchWriter
	cellStore      *cellStore
	cellGeneration uint64
}

func (s *Store) newStateCellBatchWriter(ctx context.Context, generation uint64, batchInitialBytes int) (*stateCellBatchWriter, error) {
	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return nil, err
	}

	return &stateCellBatchWriter{
		cells:          cells.newBatchWriter(cellShardBatchInitialSize(batchInitialBytes)),
		cellStore:      cells,
		cellGeneration: cells.generation,
	}, nil
}

func (w *stateCellBatchWriter) addWithHash(hash cell.Hash, cl *cell.Cell, refs []cell.RefMetadata) error {
	valueLen, layout, err := stateCellEncodedLen(cl, refs)
	if err != nil {
		return err
	}
	if err = w.cells.setDeferred(hash[:], valueLen, func(value []byte) {
		encodeStateCellRecordTo(value, cl, refs, layout)
	}); err != nil {
		return err
	}
	return nil
}

func (w *stateCellBatchWriter) addBOCCell(cl cell.BOCCellView, refs *stateBOCRefs) error {
	valueLen, layout, err := stateBOCCellEncodedLen(cl, refs)
	if err != nil {
		return err
	}
	return w.cells.setDeferred(cl.Meta.Hash[:], valueLen, func(value []byte) {
		encodeStateBOCCellRecordTo(value, cl, refs, layout)
	})
}

func stateCellEncodedLen(cl *cell.Cell, refs []cell.RefMetadata) (int, stateCellRecordLayout, error) {
	cellBits := cl.BitsSize()
	if cellBits > 1023 {
		return 0, stateCellRecordLayout{}, fmt.Errorf("cell bits length is too large: %d", cellBits)
	}
	if len(refs) > 4 {
		return 0, stateCellRecordLayout{}, fmt.Errorf("cell refs count is too large: %d", len(refs))
	}

	d1, d2 := stateCellRecordDescriptors(cl, len(refs), cellBits)
	layout := stateCellRecordLayout{d1: d1, d2: d2}
	size := 2 + cl.SerializedBOCBodySize()
	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	for i := range refs {
		ref := &refs[i]
		hashesCount := storage.CellRefHashesCount(ref.LevelMask.Mask)
		if len(ref.Hashes) != hashesCount || len(ref.Depths) != hashesCount {
			return 0, stateCellRecordLayout{}, fmt.Errorf(
				"invalid ref metadata for %x: hashes=%d depths=%d want=%d",
				ref.Hash,
				len(ref.Hashes),
				len(ref.Depths),
				hashesCount,
			)
		}
		refSize := 1 + hashesCount*(cellRecordHashSize+cellRecordDepthSize)
		refsSize += refSize
		if stateCellRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		layout.slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	if hasCommonRef && compactRefsSize <= refsSize {
		layout.compactRefs = true
		size += compactRefsSize
	} else {
		size += refsSize
	}
	return size, layout, nil
}

func stateBOCCellEncodedLen(cl cell.BOCCellView, refs *stateBOCRefs) (int, stateCellRecordLayout, error) {
	if refs.count > 4 {
		return 0, stateCellRecordLayout{}, fmt.Errorf("cell refs count is too large: %d", refs.count)
	}
	if int(cl.D1&7) != refs.count {
		return 0, stateCellRecordLayout{}, fmt.Errorf("cell refs count mismatch: descriptor=%d refs=%d", cl.D1&7, refs.count)
	}
	dataLen := int(cl.D2/2 + cl.D2%2)
	if len(cl.Body) != dataLen {
		return 0, stateCellRecordLayout{}, fmt.Errorf("cell body size mismatch: got=%d want=%d", len(cl.Body), dataLen)
	}

	layout := stateCellRecordLayout{d1: cl.D1, d2: cl.D2}
	size := 2 + len(cl.Body)
	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	for i := 0; i < refs.count; i++ {
		ref := &refs.items[i]
		hashesCount := storage.CellRefHashesCount(ref.LevelMask.Mask)
		if int(ref.Count) < hashesCount {
			return 0, stateCellRecordLayout{}, fmt.Errorf(
				"invalid boc ref metadata for %x: hashes=%d want=%d",
				ref.Hash,
				ref.Count,
				hashesCount,
			)
		}
		refSize := 1 + hashesCount*(cellRecordHashSize+cellRecordDepthSize)
		refsSize += refSize
		if stateBOCRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		layout.slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	if hasCommonRef && compactRefsSize <= refsSize {
		layout.compactRefs = true
		size += compactRefsSize
	} else {
		size += refsSize
	}
	return size, layout, nil
}

func stateBOCRefCommon(ref *cell.BOCCellMeta) bool {
	return ref.LevelMask.Mask == 0 && ref.Count >= 1
}

func stateCellRefCommon(ref *cell.RefMetadata) bool {
	return ref.LevelMask.Mask == 0 && len(ref.Hashes) == 1 && len(ref.Depths) == 1
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

func encodeStateCellRecordTo(buf []byte, cl *cell.Cell, refs []cell.RefMetadata, layout stateCellRecordLayout) {
	pos := 0
	d1 := layout.d1
	if layout.compactRefs {
		d1 |= cellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = layout.d2
	pos += 2

	pos += cl.SerializeBOCBodyTo(buf[pos:])
	if layout.compactRefs {
		buf[pos] = layout.slowRefs
		pos++
	}
	for i, ref := range refs {
		if layout.compactRefs && layout.slowRefs&(1<<uint(i)) == 0 {
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

func encodeStateBOCCellRecordTo(buf []byte, cl cell.BOCCellView, refs *stateBOCRefs, layout stateCellRecordLayout) {
	pos := 0
	d1 := layout.d1
	if layout.compactRefs {
		d1 |= cellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = layout.d2
	pos += 2

	copy(buf[pos:], cl.Body)
	pos += len(cl.Body)
	if layout.compactRefs {
		buf[pos] = layout.slowRefs
		pos++
	}
	for i := 0; i < refs.count; i++ {
		ref := refs.items[i]
		if layout.compactRefs && layout.slowRefs&(1<<uint(i)) == 0 {
			copy(buf[pos:pos+cellRecordHashSize], ref.Hashes[0][:])
			pos += cellRecordHashSize
			binary.BigEndian.PutUint16(buf[pos:pos+cellRecordDepthSize], ref.Depths[0])
			pos += cellRecordDepthSize
			continue
		}

		buf[pos] = ref.LevelMask.Mask
		pos++

		hashesCount := storage.CellRefHashesCount(ref.LevelMask.Mask)
		for j := 0; j < hashesCount; j++ {
			copy(buf[pos:pos+32], ref.Hashes[j][:])
			pos += 32
		}
		for j := 0; j < hashesCount; j++ {
			binary.BigEndian.PutUint16(buf[pos:pos+2], ref.Depths[j])
			pos += 2
		}
	}
}

func (w *stateCellBatchWriter) flush() (stateCellWriteStats, error) {
	stats, err := w.cells.flush()
	if err != nil {
		return stateCellWriteStats{}, err
	}
	return stats, nil
}

func (w *stateCellBatchWriter) close() {
	w.cells.close()
	w.cellStore.release()
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
