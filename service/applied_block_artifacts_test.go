package service

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

func TestAppliedBlockArtifactWriterWaitsUntilBlockAndProofStored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newAppliedBlockArtifactTestStore()
	store.saveStarted = make(chan struct{})
	store.releaseSave = make(chan struct{})
	flusher := &appliedBlockArtifactTestFlusher{}
	writer := newAppliedBlockArtifactWriter(zerolog.Nop(), store, flusher)
	go writer.run(ctx)

	prev := testBlockID(0, topShard, 10)
	block := testAppliedDownloadedBlock(testBlockID(0, topShard, 11), prev, false)
	if err := writer.enqueue(ctx, block, 4); err != nil {
		t.Fatalf("enqueue block: %v", err)
	}
	target := writer.target()

	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("SaveBlockFull was not called")
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWait()
	waitErr := make(chan error, 1)
	go func() {
		waitErr <- writer.wait(waitCtx, target)
	}()

	select {
	case err := <-waitErr:
		t.Fatalf("wait finished before artifact save was released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.releaseSave)

	if err := <-waitErr; err != nil {
		t.Fatalf("wait saved artifacts: %v", err)
	}
	if got := store.blockData(block.ID); !bytes.Equal(got, block.BlockBOC) {
		t.Fatalf("stored block data = %x, want %x", got, block.BlockBOC)
	}
	if got := store.proofData(storage.ServedProofBlockLink, block.ID); !bytes.Equal(got, block.ProofBOC) {
		t.Fatalf("stored block proof = %x, want %x", got, block.ProofBOC)
	}
	if got, ok := store.nextBlock(prev); !ok || !got.Equals(&block.ID) {
		t.Fatalf("next block link = %s ok=%v, want %s", storage.FormatBlockRef(got), ok, storage.FormatBlockRef(block.ID))
	}
	if !flusher.has(block.ID) {
		t.Fatalf("live block was not marked flushed")
	}
}

func TestAppliedBlockArtifactWriterFailsWhenProofIsMissing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newAppliedBlockArtifactTestStore()
	writer := newAppliedBlockArtifactWriter(zerolog.Nop(), store, nil)
	go writer.run(ctx)

	block := testAppliedDownloadedBlock(testBlockID(0, topShard, 21), testBlockID(0, topShard, 20), false)
	block.ProofBOC = nil
	if err := writer.enqueue(ctx, block, 4); err != nil {
		t.Fatalf("enqueue block: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWait()
	err := writer.wait(waitCtx, writer.target())
	if err == nil || !strings.Contains(err.Error(), "block proof is missing") {
		t.Fatalf("wait error = %v, want missing proof", err)
	}
}

func TestAppliedBlockArtifactWriterRetriesLinkAfterPartialSave(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newAppliedBlockArtifactTestStore()
	store.linkFailures = 1
	writer := newAppliedBlockArtifactWriter(zerolog.Nop(), store, nil)
	go writer.run(ctx)

	prev := testBlockID(0, topShard, 24)
	block := testAppliedDownloadedBlock(testBlockID(0, topShard, 25), prev, false)
	if err := writer.enqueue(ctx, block, 4); err != nil {
		t.Fatalf("enqueue block: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWait()
	if err := writer.wait(waitCtx, writer.target()); err != nil {
		t.Fatalf("wait after partial save: %v", err)
	}
	if saves := store.saveCount(); saves != 1 {
		t.Fatalf("SaveBlockFull calls = %d, want 1", saves)
	}
	if got, ok := store.nextBlock(prev); !ok || !got.Equals(&block.ID) {
		t.Fatalf("next block link = %s ok=%v, want %s", storage.FormatBlockRef(got), ok, storage.FormatBlockRef(block.ID))
	}
}

func TestAppliedBlockArtifactWriterAcceptsAlreadyDurableBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newAppliedBlockArtifactTestStore()
	flusher := &appliedBlockArtifactTestFlusher{}
	writer := newAppliedBlockArtifactWriter(zerolog.Nop(), store, flusher)
	go writer.run(ctx)

	block := testAppliedDownloadedBlock(testBlockID(0, topShard, 31), testBlockID(0, topShard, 30), false)
	if err := store.SaveBlockFull(&storage.ServedBlockFull{
		ID:     block.ID,
		Block:  block.BlockBOC,
		Proof:  block.ProofBOC,
		Meta:   block.Meta,
		IsLink: true,
	}); err != nil {
		t.Fatalf("pre-save full block: %v", err)
	}

	if err := writer.enqueue(ctx, p2p.DownloadedBlock{ID: block.ID}, 4); err != nil {
		t.Fatalf("enqueue durable block: %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWait()
	if err := writer.wait(waitCtx, writer.target()); err != nil {
		t.Fatalf("wait durable block: %v", err)
	}
	if saves := store.saveCount(); saves != 1 {
		t.Fatalf("SaveBlockFull calls = %d, want only pre-save", saves)
	}
	if !flusher.has(block.ID) {
		t.Fatalf("live block was not marked flushed")
	}
}

type appliedBlockArtifactTestStore struct {
	mu sync.Mutex

	blocks map[string][]byte
	proofs map[string][]byte
	next   map[string]ton.BlockIDExt

	saveStartedOnce sync.Once
	saveStarted     chan struct{}
	releaseSave     chan struct{}
	saveErr         error
	linkFailures    int
	saves           int
}

func newAppliedBlockArtifactTestStore() *appliedBlockArtifactTestStore {
	return &appliedBlockArtifactTestStore{
		blocks: map[string][]byte{},
		proofs: map[string][]byte{},
		next:   map[string]ton.BlockIDExt{},
	}
}

func (s *appliedBlockArtifactTestStore) SaveBlockFull(block *storage.ServedBlockFull) error {
	if s.saveStarted != nil {
		s.saveStartedOnce.Do(func() {
			close(s.saveStarted)
		})
	}
	if s.releaseSave != nil {
		<-s.releaseSave
	}
	if s.saveErr != nil {
		return s.saveErr
	}
	if block == nil {
		return errors.New("served block is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.saves++
	if len(block.Block) > 0 {
		s.blocks[storage.BlockKey(block.ID)] = bytes.Clone(block.Block)
	}
	if len(block.Proof) > 0 {
		isKey := block.Meta != nil && block.Meta.Has(storage.BlockMetaIsKeyBlock)
		for _, kind := range storage.StoredProofKindsForBlock(block.IsLink, isKey) {
			s.proofs[s.proofKey(kind, block.ID)] = bytes.Clone(block.Proof)
		}
	}
	return nil
}

func (s *appliedBlockArtifactTestStore) LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.linkFailures > 0 {
		s.linkFailures--
		return errors.New("link failed")
	}
	s.next[storage.BlockKey(prev)] = next
	return nil
}

func (s *appliedBlockArtifactTestStore) BlockData(_ context.Context, block ton.BlockIDExt) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.blocks[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return bytes.Clone(data), nil
}

func (s *appliedBlockArtifactTestStore) BlockProof(_ context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.proofs[s.proofKey(kind, block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return bytes.Clone(data), nil
}

func (s *appliedBlockArtifactTestStore) blockData(block ton.BlockIDExt) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.blocks[storage.BlockKey(block)])
}

func (s *appliedBlockArtifactTestStore) proofData(kind storage.ServedProofKind, block ton.BlockIDExt) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.proofs[s.proofKey(kind, block)])
}

func (s *appliedBlockArtifactTestStore) nextBlock(prev ton.BlockIDExt) (ton.BlockIDExt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next, ok := s.next[storage.BlockKey(prev)]
	return next, ok
}

func (s *appliedBlockArtifactTestStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func (s *appliedBlockArtifactTestStore) proofKey(kind storage.ServedProofKind, block ton.BlockIDExt) string {
	return string(kind) + ":" + storage.BlockKey(block)
}

type appliedBlockArtifactTestFlusher struct {
	mu      sync.Mutex
	flushed map[string]struct{}
}

func (f *appliedBlockArtifactTestFlusher) MarkLiveBlockFlushed(block ton.BlockIDExt) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flushed == nil {
		f.flushed = map[string]struct{}{}
	}
	f.flushed[storage.BlockKey(block)] = struct{}{}
}

func (f *appliedBlockArtifactTestFlusher) has(block ton.BlockIDExt) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.flushed[storage.BlockKey(block)]
	return ok
}

func testAppliedDownloadedBlock(block ton.BlockIDExt, prev ton.BlockIDExt, isKey bool) p2p.DownloadedBlock {
	meta := &storage.BlockMeta{
		ID:       block,
		PrevRefs: []ton.BlockIDExt{prev},
	}
	if isKey {
		meta.Mark(storage.BlockMetaIsKeyBlock)
	}
	return p2p.DownloadedBlock{
		ID:               block,
		BlockBOC:         []byte("block boc"),
		ProofBOC:         []byte("proof boc"),
		Meta:             meta,
		VerifiedFileHash: true,
	}
}
