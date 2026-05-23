package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	appliedBlockArtifactRetryCount     = 3
	appliedBlockArtifactRetryDelay     = 200 * time.Millisecond
	appliedBlockArtifactQueueJobsLimit = 4096
	appliedBlockArtifactQueueByteLimit = 512 << 20
)

var errAppliedBlockArtifactsClosed = errors.New("applied block artifact writer is closed")

type appliedBlockArtifactStore interface {
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	SaveBlockFull(block *storage.ServedBlockFull) error
	LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) error
}

type blockArtifactFlusher interface {
	MarkLiveBlockFlushed(block ton.BlockIDExt)
}

func appliedBlockArtifactFlusher(publisher CurrentStatePublisher) blockArtifactFlusher {
	flusher, _ := publisher.(blockArtifactFlusher)
	return flusher
}

type appliedBlockArtifactJob struct {
	seq        uint64
	block      p2p.DownloadedBlock
	splitDepth uint32
	bytes      uint64
}

type appliedBlockArtifactWriter struct {
	log     zerolog.Logger
	store   appliedBlockArtifactStore
	flusher blockArtifactFlusher

	mu     sync.Mutex
	wake   chan struct{}
	done   chan struct{}
	jobs   []appliedBlockArtifactJob
	head   int
	bytes  uint64
	next   uint64
	saved  uint64
	err    error
	closed bool
}

func newAppliedBlockArtifactWriter(log zerolog.Logger, store appliedBlockArtifactStore, flusher blockArtifactFlusher) *appliedBlockArtifactWriter {
	if store == nil {
		return nil
	}
	return &appliedBlockArtifactWriter{
		log:     log,
		store:   store,
		flusher: flusher,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

func (w *appliedBlockArtifactWriter) run(ctx context.Context) {
	for {
		job, ok := w.popJob()
		if ok {
			w.process(job)
			continue
		}

		select {
		case <-w.wake:
		case <-ctx.Done():
			for {
				job, ok = w.popJob()
				if !ok {
					w.close(ctx.Err())
					return
				}
				w.process(job)
			}
		}
	}
}

func (w *appliedBlockArtifactWriter) enqueue(ctx context.Context, block p2p.DownloadedBlock, splitDepth uint32) error {
	if w == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	jobBytes := downloadedBlockArtifactBytes(block)

	w.mu.Lock()
	for {
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return err
		}
		if w.closed {
			w.mu.Unlock()
			return errAppliedBlockArtifactsClosed
		}
		if w.hasQueueRoomLocked(jobBytes) {
			break
		}

		done := w.done
		w.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		w.mu.Lock()
	}

	w.next++
	w.bytes += jobBytes
	w.jobs = append(w.jobs, appliedBlockArtifactJob{
		seq:        w.next,
		block:      cloneDownloadedBlockForArtifact(block),
		splitDepth: splitDepth,
		bytes:      jobBytes,
	})
	w.signalWake()
	w.mu.Unlock()
	return nil
}

func (w *appliedBlockArtifactWriter) hasQueueRoomLocked(jobBytes uint64) bool {
	queuedJobs := len(w.jobs) - w.head
	if queuedJobs == 0 {
		return true
	}
	if queuedJobs >= appliedBlockArtifactQueueJobsLimit {
		return false
	}
	if jobBytes > 0 && w.bytes+jobBytes > appliedBlockArtifactQueueByteLimit {
		return false
	}
	return true
}

func (w *appliedBlockArtifactWriter) target() uint64 {
	if w == nil {
		return 0
	}

	w.mu.Lock()
	target := w.next
	w.mu.Unlock()
	return target
}

func (w *appliedBlockArtifactWriter) wait(ctx context.Context, target uint64) error {
	if w == nil || target == 0 {
		return nil
	}

	for {
		w.mu.Lock()
		if w.saved >= target {
			w.mu.Unlock()
			return nil
		}
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return err
		}
		if w.closed {
			w.mu.Unlock()
			return errAppliedBlockArtifactsClosed
		}
		done := w.done
		w.mu.Unlock()

		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *appliedBlockArtifactWriter) popJob() (appliedBlockArtifactJob, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil || len(w.jobs) == 0 {
		return appliedBlockArtifactJob{}, false
	}

	job := w.jobs[w.head]
	w.jobs[w.head] = appliedBlockArtifactJob{}
	w.head++
	if job.bytes > w.bytes {
		w.bytes = 0
	} else {
		w.bytes -= job.bytes
	}
	if w.head == len(w.jobs) {
		w.jobs = nil
		w.head = 0
	} else if w.head > 1024 && w.head*2 >= len(w.jobs) {
		copy(w.jobs, w.jobs[w.head:])
		w.jobs = w.jobs[:len(w.jobs)-w.head]
		w.head = 0
	}
	w.broadcastDone()
	return job, true
}

func (w *appliedBlockArtifactWriter) process(job appliedBlockArtifactJob) {
	var err error
	for attempt := 1; attempt <= appliedBlockArtifactRetryCount; attempt++ {
		err = w.storeBlock(context.Background(), job.block, job.splitDepth)
		if err == nil {
			w.markSaved(job.seq)
			return
		}
		if attempt < appliedBlockArtifactRetryCount {
			time.Sleep(appliedBlockArtifactRetryDelay)
		}
	}

	w.fail(fmt.Errorf("store applied block artifact %s: %w", storage.FormatBlockRef(job.block.ID), err))
}

func (w *appliedBlockArtifactWriter) storeBlock(ctx context.Context, block p2p.DownloadedBlock, splitDepth uint32) error {
	if err := w.blockArtifactsReady(ctx, block.ID); err == nil {
		return w.finishBlock(ctx, block)
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}

	if len(block.BlockBOC) == 0 {
		return fmt.Errorf("block data is missing")
	}
	if !block.VerifiedFileHash {
		return fmt.Errorf("block file hash is not verified")
	}
	if len(block.ProofBOC) == 0 {
		return fmt.Errorf("block proof is missing")
	}
	if block.Meta == nil {
		prepared, err := prepareDownloadedBlock(block)
		if err != nil {
			return err
		}
		block = prepared
	}

	full := &storage.ServedBlockFull{
		ID:                     block.ID,
		Proof:                  append([]byte(nil), block.ProofBOC...),
		Block:                  append([]byte(nil), block.BlockBOC...),
		Meta:                   block.Meta.Clone(),
		IsLink:                 appliedBlockProofIsLink(block.ID),
		ArchiveShardSplitDepth: splitDepth,
	}
	if err := w.store.SaveBlockFull(full); err != nil {
		return err
	}

	return w.finishBlock(ctx, block)
}

func (w *appliedBlockArtifactWriter) finishBlock(ctx context.Context, block p2p.DownloadedBlock) error {
	if block.Meta != nil {
		for _, prev := range block.Meta.PrevRefs {
			if err := w.store.LinkNextBlock(prev, block.ID); err != nil {
				return err
			}
		}
	}
	if err := w.blockArtifactsReady(ctx, block.ID); err != nil {
		return err
	}

	w.markLiveBlockFlushed(block.ID)
	return nil
}

func (w *appliedBlockArtifactWriter) blockArtifactsReady(ctx context.Context, block ton.BlockIDExt) error {
	if _, err := w.store.BlockData(ctx, block); err != nil {
		return err
	}

	var lastErr error = storage.ErrNotFound
	for _, kind := range appliedBlockProofKinds(block) {
		if _, err := w.store.BlockProof(ctx, kind, block); err == nil {
			return nil
		} else if !errors.Is(err, storage.ErrNotFound) {
			lastErr = err
		}
	}
	return lastErr
}

func (w *appliedBlockArtifactWriter) markSaved(seq uint64) {
	w.mu.Lock()
	if seq > w.saved {
		w.saved = seq
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *appliedBlockArtifactWriter) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
		w.log.Error().Err(err).Msg("applied block artifact writer failed")
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *appliedBlockArtifactWriter) close(err error) {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		if err != nil && !errors.Is(err, context.Canceled) {
			w.err = err
		}
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *appliedBlockArtifactWriter) markLiveBlockFlushed(block ton.BlockIDExt) {
	if w.flusher != nil {
		w.flusher.MarkLiveBlockFlushed(block)
	}
}

func (w *appliedBlockArtifactWriter) signalWake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *appliedBlockArtifactWriter) broadcastDone() {
	close(w.done)
	w.done = make(chan struct{})
}

func (s *Service) stageAppliedBlockArtifact(ctx context.Context, block p2p.DownloadedBlock, splitDepth uint32) error {
	if s.appliedArtifacts == nil {
		return nil
	}
	if err := s.appliedArtifacts.enqueue(ctx, block, splitDepth); err != nil {
		return fmt.Errorf("stage applied block artifact %s: %w", block.BlockRef(), err)
	}
	return nil
}

func (s *Service) appliedBlockArtifactTarget() uint64 {
	if s.appliedArtifacts == nil {
		return 0
	}
	return s.appliedArtifacts.target()
}

func (s *Service) waitAppliedBlockArtifacts(ctx context.Context, target uint64) error {
	if s.appliedArtifacts == nil {
		return nil
	}
	if err := s.appliedArtifacts.wait(ctx, target); err != nil {
		return fmt.Errorf("flush applied block artifacts: %w", err)
	}
	return nil
}

func appliedBlockProofKinds(block ton.BlockIDExt) []storage.ServedProofKind {
	if appliedBlockProofIsLink(block) {
		return []storage.ServedProofKind{storage.ServedProofBlockLink, storage.ServedProofKeyBlockLink}
	}
	return []storage.ServedProofKind{storage.ServedProofBlock, storage.ServedProofKeyBlock}
}

func appliedBlockProofIsLink(block ton.BlockIDExt) bool {
	return block.Workchain != -1 || block.Shard != topShard
}

func cloneDownloadedBlockForArtifact(block p2p.DownloadedBlock) p2p.DownloadedBlock {
	block.ProofBOC = append([]byte(nil), block.ProofBOC...)
	block.BlockBOC = append([]byte(nil), block.BlockBOC...)
	if block.Meta != nil {
		block.Meta = block.Meta.Clone()
	}
	return block
}

func downloadedBlockArtifactBytes(block p2p.DownloadedBlock) uint64 {
	return uint64(len(block.ProofBOC) + len(block.BlockBOC))
}
