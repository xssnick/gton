package service

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

type artifactPrewriterTestStore struct {
	mu      sync.Mutex
	entries []storage.StateCheckpointBlock
	started chan struct{}
	release chan struct{}
	err     error
}

func (s *artifactPrewriterTestStore) PrewriteCheckpointArtifacts(entries []storage.StateCheckpointBlock) error {
	s.mu.Lock()
	started := s.started
	release := s.release
	s.mu.Unlock()
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	if release != nil {
		<-release
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *artifactPrewriterTestStore) entryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func testArtifactPrewriterBlockState(seqno uint32, seed byte) (*storage.BlockState, *storage.ServedBlockFull) {
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{seed}, 32),
		FileHash:  bytes.Repeat([]byte{seed + 1}, 32),
	}
	state := &storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Repeat([]byte{seed + 2}, 32),
	}
	artifact := &storage.ServedBlockFull{
		ID:    block,
		Block: []byte{seed, 0x01},
		Proof: []byte{seed, 0x02},
		Meta:  &storage.BlockMeta{ID: block, GenUTime: seqno},
	}
	return state, artifact
}

func newTestArtifactPrewriter(store checkpointArtifactPrewriteStore, maxBytes uint64) *artifactPrewriter {
	return &artifactPrewriter{
		log:      zerolog.Nop(),
		store:    store,
		maxBytes: maxBytes,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func TestArtifactPrewriterWaitsForQueuedArtifacts(t *testing.T) {
	store := &artifactPrewriterTestStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	writer := newTestArtifactPrewriter(store, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.start(ctx, func(fn func()) { go fn() })

	state, artifact := testArtifactPrewriterBlockState(100, 0x10)
	target, err := writer.enqueue(state, artifact)
	if err != nil {
		t.Fatalf("enqueue artifact: %v", err)
	}
	if target == 0 {
		t.Fatal("enqueue returned zero target")
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- writer.wait(context.Background(), target)
	}()

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("prewriter did not start processing queued artifacts")
	}
	select {
	case err := <-waitDone:
		t.Fatalf("wait finished before store append was released: %v", err)
	default:
	}

	close(store.release)
	if err := <-waitDone; err != nil {
		t.Fatalf("wait for prewritten artifacts: %v", err)
	}
	if got := store.entryCount(); got != 1 {
		t.Fatalf("prewritten entries = %d, want 1", got)
	}
}

func TestArtifactPrewriterAllowsMissingCapability(t *testing.T) {
	state, artifact := testArtifactPrewriterBlockState(101, 0x20)
	var nilWriter *artifactPrewriter
	if seq, err := nilWriter.enqueue(state, artifact); err != nil || seq != 0 {
		t.Fatalf("nil prewriter enqueue = %d/%v, want 0/nil", seq, err)
	}
	if err := nilWriter.wait(context.Background(), 5); err != nil {
		t.Fatalf("nil prewriter wait: %v", err)
	}
}

func TestArtifactPrewriterFailureIsSticky(t *testing.T) {
	wantErr := errors.New("append failed")
	// The append is gated so the first enqueue provably lands before the writer
	// fails: enqueue drains the queue backpressure, which reports an existing
	// writer error, so an ungated failure could surface on this first call.
	release := make(chan struct{})
	store := &artifactPrewriterTestStore{err: wantErr, release: release}
	writer := newTestArtifactPrewriter(store, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.start(ctx, func(fn func()) { go fn() })

	state, artifact := testArtifactPrewriterBlockState(102, 0x30)
	target, err := writer.enqueue(state, artifact)
	if err != nil {
		t.Fatalf("enqueue artifact: %v", err)
	}
	close(release)

	if err = writer.wait(context.Background(), target); !errors.Is(err, wantErr) {
		t.Fatalf("wait error = %v, want %v", err, wantErr)
	}
	if _, err = writer.enqueue(state, artifact); !errors.Is(err, wantErr) {
		t.Fatalf("enqueue after failure = %v, want %v", err, wantErr)
	}
}

func TestArtifactPrewriterDrainsQueueOnContextCancel(t *testing.T) {
	store := &artifactPrewriterTestStore{}
	writer := newTestArtifactPrewriter(store, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	runAsync := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}

	var lastTarget uint64
	for i := 0; i < 3; i++ {
		state, artifact := testArtifactPrewriterBlockState(200+uint32(i), 0x40+byte(i))
		target, err := writer.enqueue(state, artifact)
		if err != nil {
			t.Fatalf("enqueue artifact %d: %v", i, err)
		}
		lastTarget = target
	}

	writer.start(ctx, runAsync)
	cancel()
	wg.Wait()

	if got := store.entryCount(); got != 3 {
		t.Fatalf("drained entries = %d, want 3", got)
	}
	if err := writer.wait(context.Background(), lastTarget); err != nil {
		t.Fatalf("wait after drain: %v", err)
	}
}

func TestArtifactPrewriterDetachedEnqueueDoesNotBlockOnFullQueue(t *testing.T) {
	store := &artifactPrewriterTestStore{}
	writer := newTestArtifactPrewriter(store, 1)

	first, firstArtifact := testArtifactPrewriterBlockState(100, 0x50)
	if _, _, err := writer.enqueueDetached(first, firstArtifact); err != nil {
		t.Fatalf("enqueue first artifact: %v", err)
	}

	// The queue is over its byte limit now; the append must still not block.
	second, secondArtifact := testArtifactPrewriterBlockState(101, 0x60)
	seq, wait, err := writer.enqueueDetached(second, secondArtifact)
	if err != nil {
		t.Fatalf("enqueue second artifact: %v", err)
	}
	if seq != 2 {
		t.Fatalf("second artifact sequence = %d, want 2", seq)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- wait() }()
	select {
	case err := <-waitDone:
		t.Fatalf("backpressure wait returned before the queue drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.start(ctx, func(fn func()) { go fn() })

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("backpressure wait after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backpressure wait did not release after the queue drained")
	}
	if err := writer.wait(context.Background(), seq); err != nil {
		t.Fatalf("wait for prewritten artifacts: %v", err)
	}
	if got := store.entryCount(); got != 2 {
		t.Fatalf("prewritten entries = %d, want 2", got)
	}
}
