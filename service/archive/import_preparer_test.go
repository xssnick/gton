package archive

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestImportedBlockDispatcherUsesProcessWorkerBudget(t *testing.T) {
	importer := NewImporter()
	t.Cleanup(importer.Close)

	if got, want := importer.dispatcher.workers, importedBlockPrepareParallelism(); got != want {
		t.Fatalf("importer prepare workers = %d, want GOMAXPROCS %d", got, want)
	}
}

func TestImportedBlockDispatcherNormalizesWorkerCount(t *testing.T) {
	dispatcher := newImportedBlockPrepareDispatcher(0, func(ton.BlockIDExt, []byte) (PreparedBlock, error) {
		return PreparedBlock{}, nil
	})
	t.Cleanup(dispatcher.close)

	if dispatcher.workers != 1 {
		t.Fatalf("workers = %d, want 1", dispatcher.workers)
	}
}

func TestImportedBlockDispatcherLimitsConcurrentImports(t *testing.T) {
	const (
		workers    = 3
		imports    = 4
		jobsPerRun = 2
	)

	started := make(chan struct{}, imports*jobsPerRun)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseAll()
	var active atomic.Int64
	var maximum atomic.Int64
	dispatcher := newTestImportedBlockDispatcher(t, workers, func(id ton.BlockIDExt, _ []byte) (PreparedBlock, error) {
		current := active.Add(1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return testPreparedImportedBlock(id), nil
	})
	preparers := make([]*importedBlockPreparer, imports)
	for importIndex := range preparers {
		preparer := newImportedBlockPreparer(WithHotImportPrepare(context.Background()), dispatcher)
		preparers[importIndex] = preparer
		for jobIndex := 0; jobIndex < jobsPerRun; jobIndex++ {
			seqno := uint32(importIndex*jobsPerRun + jobIndex + 1)
			if err := preparer.submit(testImportedBlockFull(seqno)); err != nil {
				t.Fatalf("submit import %d block %d: %v", importIndex, jobIndex, err)
			}
		}
	}

	finished := make(chan error, imports)
	for _, preparer := range preparers {
		go func() {
			finished <- preparer.finish(testImportedResult(), &ImportStats{})
		}()
	}

	for worker := 0; worker < workers; worker++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("prepare workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("dispatcher exceeded its global worker limit")
	case <-time.After(30 * time.Millisecond):
	}

	releaseAll()
	for range preparers {
		select {
		case err := <-finished:
			if err != nil {
				t.Fatalf("finish concurrent import: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent import did not finish")
		}
	}
	if got := maximum.Load(); got != workers {
		t.Fatalf("maximum concurrent prepares = %d, want %d", got, workers)
	}
}

func TestImportedBlockDispatcherReservesHotWorker(t *testing.T) {
	const workers = 2

	started := make(chan uint32, 3)
	releasePrefetch := make(chan struct{})
	var releaseOnce sync.Once
	releaseAllPrefetch := func() {
		releaseOnce.Do(func() {
			close(releasePrefetch)
		})
	}
	defer releaseAllPrefetch()
	dispatcher := newTestImportedBlockDispatcher(t, workers, func(id ton.BlockIDExt, _ []byte) (PreparedBlock, error) {
		started <- id.SeqNo
		if id.SeqNo < 3 {
			<-releasePrefetch
		}
		return testPreparedImportedBlock(id), nil
	})
	prefetch := newImportedBlockPreparer(context.Background(), dispatcher)
	if err := prefetch.submit(testImportedBlockFull(1)); err != nil {
		t.Fatal(err)
	}
	if err := prefetch.submit(testImportedBlockFull(2)); err != nil {
		t.Fatal(err)
	}
	prefetchDone := make(chan error, 1)
	go func() {
		prefetchDone <- prefetch.finish(testImportedResult(), &ImportStats{})
	}()

	select {
	case seqno := <-started:
		if seqno != 1 {
			t.Fatalf("first prefetch block = %d, want 1", seqno)
		}
	case <-time.After(time.Second):
		t.Fatal("prefetch worker did not start")
	}

	hot := newImportedBlockPreparer(WithHotImportPrepare(context.Background()), dispatcher)
	if err := hot.submit(testImportedBlockFull(3)); err != nil {
		t.Fatal(err)
	}
	hotDone := make(chan error, 1)
	go func() {
		hotDone <- hot.finish(testImportedResult(), &ImportStats{})
	}()

	select {
	case seqno := <-started:
		if seqno != 3 {
			t.Fatalf("block started while shared worker was occupied = %d, want hot block 3", seqno)
		}
	case <-time.After(time.Second):
		t.Fatal("hot block did not use its reserved worker")
	}

	select {
	case err := <-hotDone:
		if err != nil {
			t.Fatalf("finish hot import: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("hot import did not finish")
	}

	releaseAllPrefetch()
	select {
	case err := <-prefetchDone:
		if err != nil {
			t.Fatalf("finish prefetch import: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prefetch import did not finish")
	}
}

func TestImportedBlockDispatcherIsolatesCancellationAndErrors(t *testing.T) {
	const workers = 2

	prepareErr := errors.New("invalid block")
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseSlow := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseSlow()
	dispatcher := newTestImportedBlockDispatcher(t, workers, func(id ton.BlockIDExt, _ []byte) (PreparedBlock, error) {
		switch id.SeqNo {
		case 1:
			close(started)
			<-release
		case 3:
			return PreparedBlock{}, prepareErr
		}
		return testPreparedImportedBlock(id), nil
	})
	slow := newImportedBlockPreparer(context.Background(), dispatcher)
	if err := slow.submit(testImportedBlockFull(1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow prepare did not start")
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	canceled := newImportedBlockPreparer(canceledCtx, dispatcher)
	if err := canceled.submit(testImportedBlockFull(2)); err != nil {
		t.Fatal(err)
	}
	failed := newImportedBlockPreparer(context.Background(), dispatcher)
	if err := failed.submit(testImportedBlockFull(3)); err != nil {
		t.Fatal(err)
	}
	successful := newImportedBlockPreparer(context.Background(), dispatcher)
	if err := successful.submit(testImportedBlockFull(4)); err != nil {
		t.Fatal(err)
	}
	cancel()

	type importFinish struct {
		name string
		err  error
	}
	finished := make(chan importFinish, 4)
	finish := func(name string, preparer *importedBlockPreparer) {
		finished <- importFinish{
			name: name,
			err:  preparer.finish(testImportedResult(), &ImportStats{}),
		}
	}
	go finish("slow", slow)
	go finish("canceled", canceled)
	go finish("failed", failed)
	go finish("successful", successful)

	releaseSlow()

	results := make(map[string]error, 4)
	for len(results) < 4 {
		select {
		case result := <-finished:
			results[result.name] = result.err
		case <-time.After(time.Second):
			t.Fatal("concurrent imports did not finish after cancellation and error")
		}
	}
	if results["slow"] != nil {
		t.Fatalf("slow import failed: %v", results["slow"])
	}
	if !errors.Is(results["canceled"], context.Canceled) {
		t.Fatalf("canceled import error = %v, want context.Canceled", results["canceled"])
	}
	if !errors.Is(results["failed"], prepareErr) {
		t.Fatalf("failed import error = %v, want wrapped prepare error", results["failed"])
	}
	if results["successful"] != nil {
		t.Fatalf("independent import failed: %v", results["successful"])
	}
}

func TestImportedBlockPreparerPreservesSubmissionOrder(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirstBlock := func() {
		releaseOnce.Do(func() {
			close(releaseFirst)
		})
	}
	defer releaseFirstBlock()
	dispatcher := newTestImportedBlockDispatcher(t, 3, func(id ton.BlockIDExt, _ []byte) (PreparedBlock, error) {
		if id.SeqNo == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return testPreparedImportedBlock(id), nil
	})
	preparer := newImportedBlockPreparer(context.Background(), dispatcher)
	for seqno := uint32(1); seqno <= 3; seqno++ {
		if err := preparer.submit(testImportedBlockFull(seqno)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first prepare did not start")
	}

	imported := testImportedResult()
	finished := make(chan error, 1)
	go func() {
		finished <- preparer.finish(imported, &ImportStats{})
	}()
	releaseFirstBlock()

	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("finish ordered import: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ordered import did not finish")
	}
	for index, full := range imported.FullBlocks {
		if got, want := full.ID.SeqNo, uint32(index+1); got != want {
			t.Fatalf("full block at %d has seqno %d, want %d", index, got, want)
		}
	}
}

func TestImportedBlockDispatcherCloseCompletesQueuedSubmissions(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseActive := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseActive()

	dispatcher := newImportedBlockPrepareDispatcher(1, func(id ton.BlockIDExt, _ []byte) (PreparedBlock, error) {
		if id.SeqNo == 1 {
			close(started)
			<-release
		}
		return testPreparedImportedBlock(id), nil
	})
	t.Cleanup(dispatcher.close)

	preparer := newImportedBlockPreparer(context.Background(), dispatcher)
	for seqno := uint32(1); seqno <= 3; seqno++ {
		if err := preparer.submit(testImportedBlockFull(seqno)); err != nil {
			t.Fatalf("submit block %d: %v", seqno, err)
		}
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("active prepare did not start")
	}

	submitted := make(chan error, 1)
	go func() {
		submitted <- preparer.submit(testImportedBlockFull(4))
	}()
	waitImportedBlockPending(t, preparer, 4)

	finished := make(chan error, 1)
	go func() {
		finished <- preparer.finish(testImportedResult(), &ImportStats{})
	}()
	closed := make(chan struct{})
	go func() {
		dispatcher.close()
		close(closed)
	}()

	select {
	case err := <-submitted:
		if !errors.Is(err, errImporterClosed) {
			t.Fatalf("blocked submit error = %v, want errImporterClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher close did not unblock queued submit")
	}
	select {
	case <-closed:
		t.Fatal("dispatcher closed before active prepare completed")
	default:
	}

	releaseActive()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not close after active prepare completed")
	}
	select {
	case err := <-finished:
		if !errors.Is(err, errImporterClosed) {
			t.Fatalf("finish error = %v, want errImporterClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("import finish remained pending after dispatcher close")
	}
}

func TestImporterRejectsImportAfterClose(t *testing.T) {
	importer := NewImporter()
	importer.Close()

	_, err := importer.ImportBytes(context.Background(), &Downloaded{}, nil)
	if !errors.Is(err, errImporterClosed) {
		t.Fatalf("import after close error = %v, want errImporterClosed", err)
	}
}

func TestImporterCloseWaitsForActiveImport(t *testing.T) {
	block, blockData := readMasterchainBlockFixture(t)
	path := writeTestPackage(t, []testEntry{
		{name: testEntryName("block", block), data: blockData},
		{name: testEntryName("proof", block), data: []byte{0x02}},
	})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test archive: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseImport := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseImport()

	importer := newImporter(1, func(id ton.BlockIDExt, _ []byte) (PreparedBlock, error) {
		close(started)
		<-release
		return testPreparedImportedBlock(id), nil
	})
	t.Cleanup(importer.Close)

	imported := make(chan error, 1)
	go func() {
		_, importErr := importer.ImportBytes(context.Background(), &Downloaded{}, data)
		imported <- importErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("archive import did not start")
	}

	closed := make(chan struct{})
	go func() {
		importer.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("importer closed before active import completed")
	default:
	}

	releaseImport()
	select {
	case err = <-imported:
		if err != nil {
			t.Fatalf("active import failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active import did not finish")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("importer did not close after active import completed")
	}
}

func testImportedBlockFull(seqno uint32) *storage.ServedBlockFull {
	id := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
	}
	return &storage.ServedBlockFull{
		ID:    id,
		Block: []byte{byte(seqno)},
		Meta:  &storage.BlockMeta{ID: id},
	}
}

func testPreparedImportedBlock(id ton.BlockIDExt) PreparedBlock {
	return PreparedBlock{
		Meta: &storage.BlockMeta{ID: id},
	}
}

func testImportedResult() *Imported {
	return &Imported{
		PreparedBlocks: map[storage.BlockRootHash]PreparedBlock{},
	}
}

func newTestImportedBlockDispatcher(t *testing.T, workers int, prepare importedBlockPrepareFunc) *importedBlockPrepareDispatcher {
	t.Helper()

	dispatcher := newImportedBlockPrepareDispatcher(workers, prepare)
	t.Cleanup(dispatcher.close)
	return dispatcher
}

func waitImportedBlockPending(t *testing.T, preparer *importedBlockPreparer, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for {
		preparer.mu.Lock()
		pending := preparer.pending
		preparer.mu.Unlock()
		if pending == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending prepares = %d, want %d", pending, want)
		}
		runtime.Gosched()
	}
}
