package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestStateCellPrewriterWaitsForQueuedRecords(t *testing.T) {
	store := &stateCellPrewriterTestStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	writer := newStateCellPrewriter(zerolog.Nop(), store, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer.start(ctx, context.Background(), func(fn func()) {
		go fn()
	})

	root := cell.BeginCell().MustStoreUInt(0x71, 8).EndCell()
	records := mustPreparedReachableStateCells(t, root)
	token, err := writer.enqueue(records)
	if err != nil {
		t.Fatalf("enqueue prewrite records: %v", err)
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- writer.wait(context.Background(), token)
	}()

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("prewriter did not start saving queued records")
	}

	select {
	case err := <-waitDone:
		t.Fatalf("wait finished before store write was released: %v", err)
	default:
	}

	close(store.release)
	if err := <-waitDone; err != nil {
		t.Fatalf("wait for prewrite records: %v", err)
	}
	if got := store.recordCount(); got != records.Len() {
		t.Fatalf("prewritten records = %d, want %d", got, records.Len())
	}
}

func TestStateCellWindowCheckpointUsesPrewriteTarget(t *testing.T) {
	store := &stateCellPrewriterTestStore{}
	writer := newStateCellPrewriter(zerolog.Nop(), store, 1<<20)
	window := newTestStateCellWindowCache(nil)
	window.setPrewriter(writer)

	root := cell.BeginCell().MustStoreUInt(0x72, 8).EndCell()
	if err := window.addPreparedRecords(mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("add prepared records: %v", err)
	}

	checkpoint := window.beginCheckpoint()
	target, ok := checkpoint.prewriteTarget()
	if !ok || target == 0 {
		t.Fatalf("checkpoint prewrite target ok=%v target=%d, want non-zero target", ok, target)
	}
}

func TestStateCellWindowCheckpointDoesNotWaitForBlockedPrewriteEnqueue(t *testing.T) {
	store := &stateCellPrewriterTestStore{}
	writer := newStateCellPrewriter(zerolog.Nop(), store, 1)
	window := newTestStateCellWindowCache(nil)
	window.setPrewriter(writer)

	first := cell.BeginCell().MustStoreUInt(0x73, 8).EndCell()
	firstRecords := mustPreparedReachableStateCells(t, first)
	if err := window.addPreparedRecords(firstRecords); err != nil {
		t.Fatalf("add first prepared records: %v", err)
	}

	firstBytes := window.byteSize()
	second := cell.BeginCell().MustStoreUInt(0x74, 8).EndCell()
	secondRecords := mustPreparedReachableStateCells(t, second)

	writer.mu.Lock()
	writerLocked := true
	unlockWriter := func() {
		if writerLocked {
			writer.mu.Unlock()
			writerLocked = false
		}
	}
	defer unlockWriter()

	addDone := make(chan error, 1)
	go func() {
		addDone <- window.addPreparedRecords(secondRecords)
	}()

	wantBytes := firstBytes + secondRecords.ByteSize()
	var failure string
	if got, ok := waitStateCellWindowByteSize(window, wantBytes, time.Second); !ok {
		failure = "second prepared records were not visible while prewriter enqueue was blocked"
	} else if got < wantBytes {
		failure = "window byte size did not include second prepared records"
	}

	var checkpointDone chan *stateCellCheckpointCache
	if failure == "" {
		checkpointDone = make(chan *stateCellCheckpointCache, 1)
		go func() {
			checkpointDone <- window.beginCheckpoint()
		}()

		select {
		case checkpoint := <-checkpointDone:
			if target, ok := checkpoint.prewriteTarget(); ok || target != 0 {
				failure = "checkpoint used prewrite target while enqueue was still pending"
			}
		case <-time.After(time.Second):
			failure = "begin checkpoint waited for blocked prewrite enqueue"
		}
	}

	unlockWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.start(ctx, context.Background(), func(fn func()) {
		go fn()
	})

	select {
	case err := <-addDone:
		if err != nil && failure == "" {
			failure = "add second prepared records: " + err.Error()
		}
	case <-time.After(time.Second):
		if failure == "" {
			failure = "blocked prewrite enqueue did not finish after writer started"
		}
	}

	if checkpointDone != nil {
		select {
		case <-checkpointDone:
		default:
		}
	}

	if failure != "" {
		t.Fatal(failure)
	}
}

func TestStateCellPrewriterBatchesQueuedRecords(t *testing.T) {
	store := &stateCellPrewriterTestStore{}
	writer := newStateCellPrewriter(zerolog.Nop(), store, 1<<20)

	first := cell.BeginCell().MustStoreUInt(0x81, 8).EndCell()
	firstRecords := mustPreparedReachableStateCells(t, first)
	if _, err := writer.enqueue(firstRecords); err != nil {
		t.Fatalf("enqueue first records: %v", err)
	}
	second := cell.BeginCell().MustStoreUInt(0x82, 8).EndCell()
	secondRecords := mustPreparedReachableStateCells(t, second)
	target, err := writer.enqueue(secondRecords)
	if err != nil {
		t.Fatalf("enqueue second records: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.start(ctx, context.Background(), func(fn func()) {
		go fn()
	})

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWait()
	if err := writer.wait(waitCtx, target); err != nil {
		t.Fatalf("wait for batched records: %v", err)
	}
	if got := store.saveCount(); got != 1 {
		t.Fatalf("prewriter save calls = %d, want 1", got)
	}
	if got, want := store.recordCount(), firstRecords.Len()+secondRecords.Len(); got != want {
		t.Fatalf("prewritten records = %d, want %d", got, want)
	}
}

func TestStateCellPrewriterStopsAfterRunContextCancelAndDrain(t *testing.T) {
	store := &stateCellPrewriterTestStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	writer := newStateCellPrewriter(zerolog.Nop(), store, 1<<20)
	runCtx, cancelRun := context.WithCancel(context.Background())
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	defer cancelShutdown()

	root := cell.BeginCell().MustStoreUInt(0x83, 8).EndCell()
	records := mustPreparedReachableStateCells(t, root)
	target, err := writer.enqueue(records)
	if err != nil {
		t.Fatalf("enqueue prewrite records: %v", err)
	}

	done := make(chan struct{})
	writer.start(runCtx, shutdownCtx, func(fn func()) {
		go func() {
			fn()
			close(done)
		}()
	})

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("prewriter did not start saving queued records")
	}

	cancelRun()

	select {
	case <-done:
		t.Fatal("prewriter exited before draining queued records")
	default:
	}

	close(store.release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("prewriter did not stop after run context cancel and queue drain")
	}
	if err := writer.wait(context.Background(), target); err != nil {
		t.Fatalf("wait after prewriter drain: %v", err)
	}
	if got := store.recordCount(); got != records.Len() {
		t.Fatalf("prewritten records = %d, want %d", got, records.Len())
	}
}

type stateCellPrewriterTestStore struct {
	mu      sync.Mutex
	records []storage.EncodedCellRecord
	started chan struct{}
	release chan struct{}
	once    sync.Once
	saves   int
}

func (s *stateCellPrewriterTestStore) SaveStateCellRecords(ctx context.Context, records storage.StateCellRecords) error {
	if s.started != nil {
		s.once.Do(func() {
			close(s.started)
		})
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	s.records = records.AppendTo(s.records)
	return nil
}

func (s *stateCellPrewriterTestStore) recordCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func (s *stateCellPrewriterTestStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func waitStateCellWindowByteSize(window *stateCellWindowCache, want uint64, timeout time.Duration) (uint64, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := make(chan uint64, 1)
		go func() {
			got <- window.byteSize()
		}()

		select {
		case size := <-got:
			if size >= want {
				return size, true
			}
		case <-time.After(10 * time.Millisecond):
			return 0, false
		}
		time.Sleep(time.Millisecond)
	}
	return 0, false
}
