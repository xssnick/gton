package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestImportStateBOCViewParallel(t *testing.T) {
	root, _ := benchmarkCellGraph(t, stateBOCImportMinCellsPerWorker*2)
	view := testStateBOCView(t, root, nil)
	if workers := stateBOCImportWorkerCount(view.Cells()); workers < 2 {
		t.Fatalf("workers = %d, want at least 2", workers)
	}

	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	lazyRoot, err := store.ImportStateBOCView(context.Background(), benchmarkBlockID(901), view)
	if err != nil {
		t.Fatalf("import boc view: %v", err)
	}
	if lazyRoot.HashKey() != root.HashKey() {
		t.Fatal("imported root hash mismatch")
	}

	written := uint64(0)
	for _, shard := range store.cells.ioStatus(time.Now()) {
		written += shard.writtenCells
	}
	if written != uint64(view.Cells()) {
		t.Fatalf("written cells = %d, want %d", written, view.Cells())
	}

	for _, idx := range []uint32{0, view.Cells() / 2, view.Cells() - 1} {
		meta, err := view.CellMeta(idx)
		if err != nil {
			t.Fatalf("cell %d metadata: %v", idx, err)
		}
		exists, err := store.cells.has(meta.Hash[:])
		if err != nil {
			t.Fatalf("cell %d lookup: %v", idx, err)
		}
		if !exists {
			t.Fatalf("cell %d was not persisted", idx)
		}
	}
}

func TestImportStateBOCViewParallelReturnsReaderError(t *testing.T) {
	root, _ := benchmarkCellGraph(t, stateBOCImportMinCellsPerWorker*2)
	boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithIndex: true})
	source := &failOnceReaderAt{data: boc}
	view := testStateBOCView(t, root, source)
	if workers := stateBOCImportWorkerCount(view.Cells()); workers < 2 {
		t.Fatalf("workers = %d, want at least 2", workers)
	}
	source.fail.Store(true)

	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	_, err = store.ImportStateBOCView(context.Background(), benchmarkBlockID(902), view)
	if !errors.Is(err, errTestStateBOCRead) {
		t.Fatalf("import error = %v, want reader error", err)
	}
}

func TestImportStateBOCViewParallelCancellation(t *testing.T) {
	root, _ := benchmarkCellGraph(t, stateBOCImportMinCellsPerWorker*2)
	view := testStateBOCView(t, root, nil)

	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.ImportStateBOCView(ctx, benchmarkBlockID(903), view)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("import error = %v, want context canceled", err)
	}
}

func TestStateBOCImportBatchMemoryBound(t *testing.T) {
	for workers := 1; workers <= cellDBShardCount; workers++ {
		workerTarget := stateBOCImportWorkerBatchTarget(workers)
		aggregateTarget := workerTarget * workers
		expectedTarget := min(stateBOCImportMaxBatchBytes, stateCellImportBatchTargetBytes*workers)
		if aggregateTarget > stateBOCImportMaxBatchBytes {
			t.Fatalf("workers=%d aggregate target=%d exceeds max=%d", workers, aggregateTarget, stateBOCImportMaxBatchBytes)
		}
		if aggregateTarget > stateCellImportBatchTargetBytes*workers {
			t.Fatalf("workers=%d aggregate target=%d exceeds per-worker bound=%d", workers, aggregateTarget, stateCellImportBatchTargetBytes*workers)
		}
		if expectedTarget-aggregateTarget >= workers {
			t.Fatalf("workers=%d aggregate target=%d want approximately %d", workers, aggregateTarget, expectedTarget)
		}

		workerInitial := stateCellImportBatchTargetBytes / workers
		initialBytes := cellShardBatchInitialSize(workerInitial) * cellDBShardCount * workers
		if initialBytes > stateCellImportBatchTargetBytes {
			t.Fatalf("workers=%d initial batch bytes=%d exceed target=%d", workers, initialBytes, stateCellImportBatchTargetBytes)
		}
		if stateCellImportBatchTargetBytes-initialBytes >= workers*cellDBShardCount {
			t.Fatalf("workers=%d initial batch bytes=%d want approximately %d", workers, initialBytes, stateCellImportBatchTargetBytes)
		}
	}
}

func testStateBOCView(t *testing.T, root *cell.Cell, source io.ReaderAt) *cell.BOCView {
	t.Helper()

	boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithIndex: true})
	if source == nil {
		source = bytes.NewReader(boc)
	}
	view, err := cell.OpenBOCView(source, int64(len(boc)), cell.BOCViewOptions{RequireIndex: true})
	if err != nil {
		t.Fatalf("open boc view: %v", err)
	}
	return view
}

var errTestStateBOCRead = errors.New("test state boc read failed")

type failOnceReaderAt struct {
	data []byte
	fail atomic.Bool
	did  atomic.Bool
}

func (r *failOnceReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if r.fail.Load() && r.did.CompareAndSwap(false, true) {
		return 0, errTestStateBOCRead
	}
	if offset < 0 || offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(dst, r.data[offset:])
	if n != len(dst) {
		return n, io.EOF
	}
	return n, nil
}
