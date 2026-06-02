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

	writer.start(ctx, func(fn func()) {
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
	window := newStateCellWindowCache(nil)
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
	writer.start(ctx, func(fn func()) {
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

type stateCellPrewriterTestStore struct {
	mu      sync.Mutex
	records []storage.EncodedCellRecord
	started chan struct{}
	release chan struct{}
	once    sync.Once
	saves   int
}

func (s *stateCellPrewriterTestStore) SaveStateCellRecords(_ context.Context, records storage.StateCellRecords) error {
	if s.started != nil {
		s.once.Do(func() {
			close(s.started)
		})
	}
	if s.release != nil {
		<-s.release
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
