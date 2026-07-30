package service

import (
	"context"
	"errors"
	"sync"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

const (
	artifactPrewriteQueueJobsLimit = 4096
	artifactPrewriteBatchJobsLimit = 256
	artifactPrewriteBatchBytes     = uint64(128 << 20)
)

var errArtifactPrewriterClosed = errors.New("artifact prewriter is closed")

// checkpointArtifactPrewriteStore is implemented by storages that can stream
// checkpoint artifact appends ahead of the owning checkpoint. Storages without
// it fall back to checkpoint-time appends.
type checkpointArtifactPrewriteStore interface {
	PrewriteCheckpointArtifacts(entries []storage.StateCheckpointBlock) error
}

type artifactPrewriteJob struct {
	seq   uint64
	entry storage.StateCheckpointBlock
	bytes uint64
}

type artifactPrewriteBatch struct {
	entries []storage.StateCheckpointBlock
	bytes   uint64
	lastSeq uint64
}

// artifactPrewriter streams staged block artifacts (block data and proofs)
// into archive pack files between checkpoints, so the checkpoint-time
// sync_artifacts stage only has to sync a short dirty tail instead of the
// whole window's appends. Mirrors stateCellPrewriter.
type artifactPrewriter struct {
	log      zerolog.Logger
	store    checkpointArtifactPrewriteStore
	maxBytes uint64

	startOnce sync.Once
	mu        sync.Mutex
	wake      chan struct{}
	done      chan struct{}
	jobs      []artifactPrewriteJob
	head      int
	bytes     uint64
	next      uint64
	written   uint64
	err       error
	closed    bool
}

func newArtifactPrewriter(log zerolog.Logger, store storage.Storage, maxBytes uint64) *artifactPrewriter {
	prewriteStore, ok := store.(checkpointArtifactPrewriteStore)
	if !ok {
		return nil
	}
	return &artifactPrewriter{
		log:      log,
		store:    prewriteStore,
		maxBytes: maxBytes,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func (w *artifactPrewriter) start(ctx context.Context, runAsync func(func())) {
	if w == nil {
		return
	}
	w.startOnce.Do(func() {
		runAsync(func() {
			w.run(ctx)
		})
	})
}

func (w *artifactPrewriter) run(ctx context.Context) {
	for {
		batch, ok := w.popBatch()
		if ok {
			w.process(batch)
			continue
		}

		select {
		case <-w.wake:
		case <-ctx.Done():
			// Drain the queue so a shutdown checkpoint waiting on the prewriter
			// still reaches its target before the store closes.
			for {
				batch, ok = w.popBatch()
				if !ok {
					w.close(ctx.Err())
					return
				}
				w.process(batch)
			}
		}
	}
}

func (w *artifactPrewriter) enqueue(state *storage.BlockState, artifact *storage.ServedBlockFull) (uint64, error) {
	seq, wait, err := w.enqueueDetached(state, artifact)
	if err != nil {
		return 0, err
	}
	return seq, wait()
}

// enqueueDetached assigns the sequence and appends the job under the prewriter
// mutex without ever blocking, so callers may enqueue while holding their own
// locks. The returned wait is the queue's backpressure (it blocks while the
// queue is over its limits) and must always be invoked — but only after the
// caller released any lock its appliers or dependents contend on. Each
// producer can overshoot the queue bound by at most its own in-flight job.
func (w *artifactPrewriter) enqueueDetached(state *storage.BlockState, artifact *storage.ServedBlockFull) (uint64, func() error, error) {
	if w == nil {
		return 0, noPrewriteBackpressure, nil
	}

	jobBytes := servedBlockFullPayloadBytes(artifact)
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return 0, nil, err
	}
	if w.closed {
		w.mu.Unlock()
		return 0, nil, errArtifactPrewriterClosed
	}

	w.next++
	seq := w.next
	w.bytes += jobBytes
	w.jobs = append(w.jobs, artifactPrewriteJob{
		seq: seq,
		entry: storage.StateCheckpointBlock{
			State:    checkpointBlockStateMetadata(state),
			Artifact: cloneServedBlockFullSharedPayload(artifact),
		},
		bytes: jobBytes,
	})
	w.signalWake()
	w.mu.Unlock()
	return seq, w.waitQueueRoom, nil
}

func (w *artifactPrewriter) waitQueueRoom() error {
	for {
		w.mu.Lock()
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return err
		}
		if w.closed {
			w.mu.Unlock()
			return errArtifactPrewriterClosed
		}
		if w.queueWithinLimitsLocked() {
			w.mu.Unlock()
			return nil
		}

		done := w.done
		w.mu.Unlock()
		<-done
	}
}

// queueWithinLimitsLocked is the post-append counterpart of the old admission
// gate: a single (possibly oversized) queued job never blocks its producer.
func (w *artifactPrewriter) queueWithinLimitsLocked() bool {
	queuedJobs := len(w.jobs) - w.head
	if queuedJobs <= 1 {
		return true
	}
	if queuedJobs >= artifactPrewriteQueueJobsLimit {
		return false
	}
	return w.maxBytes == 0 || w.bytes <= w.maxBytes
}

func (w *artifactPrewriter) wait(ctx context.Context, target uint64) error {
	if w == nil || target == 0 {
		return nil
	}

	for {
		w.mu.Lock()
		if w.written >= target {
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
			return errArtifactPrewriterClosed
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

func (w *artifactPrewriter) popJob() (artifactPrewriteJob, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil || len(w.jobs) == 0 {
		return artifactPrewriteJob{}, false
	}

	job := w.jobs[w.head]
	w.jobs[w.head] = artifactPrewriteJob{}
	w.head++
	w.bytes -= job.bytes
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

func (w *artifactPrewriter) popBatch() (artifactPrewriteBatch, bool) {
	job, ok := w.popJob()
	if !ok {
		return artifactPrewriteBatch{}, false
	}

	var batch artifactPrewriteBatch
	batch.add(job)
	for batch.canAddMore() {
		job, ok = w.popJob()
		if !ok {
			break
		}
		batch.add(job)
	}
	return batch, true
}

func (b *artifactPrewriteBatch) add(job artifactPrewriteJob) {
	b.entries = append(b.entries, job.entry)
	b.bytes += job.bytes
	b.lastSeq = job.seq
}

func (b artifactPrewriteBatch) canAddMore() bool {
	if len(b.entries) >= artifactPrewriteBatchJobsLimit {
		return false
	}
	return artifactPrewriteBatchBytes == 0 || b.bytes < artifactPrewriteBatchBytes
}

func (w *artifactPrewriter) process(batch artifactPrewriteBatch) {
	if err := w.store.PrewriteCheckpointArtifacts(batch.entries); err != nil {
		w.fail(err)
		return
	}
	w.markWritten(batch.lastSeq)
}

func (w *artifactPrewriter) markWritten(seq uint64) {
	w.mu.Lock()
	if seq > w.written {
		w.written = seq
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *artifactPrewriter) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
		w.log.Error().Err(err).Msg("artifact prewriter failed")
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *artifactPrewriter) close(err error) {
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

func (w *artifactPrewriter) signalWake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *artifactPrewriter) broadcastDone() {
	close(w.done)
	w.done = make(chan struct{})
}
