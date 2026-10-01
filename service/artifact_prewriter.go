package service

import (
	"context"
	"errors"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

const (
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

type artifactPrewriteBatch struct {
	entries []storage.StateCheckpointBlock
	bytes   uint64
	lastSeq uint64
}

func (b *artifactPrewriteBatch) add(job prewriteJob[storage.StateCheckpointBlock]) {
	b.entries = append(b.entries, job.value)
	b.bytes += job.bytes
	b.lastSeq = job.seq
}

// artifactPrewriter supplies artifact ownership and batching policy to the
// common prewriter queue engine.
type artifactPrewriter struct {
	queue *prewriter[storage.StateCheckpointBlock]
	store checkpointArtifactPrewriteStore
}

func newArtifactPrewriterWithStore(log zerolog.Logger, store checkpointArtifactPrewriteStore, maxBytes uint64) *artifactPrewriter {
	return &artifactPrewriter{
		queue: newPrewriter[storage.StateCheckpointBlock](log, prewriterConfig{
			closedError:   errArtifactPrewriterClosed,
			failureLog:    "artifact prewriter failed",
			maxQueueBytes: maxBytes,
			maxBatchJobs:  artifactPrewriteBatchJobsLimit,
			maxBatchBytes: artifactPrewriteBatchBytes,
		}),
		store: store,
	}
}

func (w *artifactPrewriter) start(ctx context.Context, runAsync func(func())) {
	if w == nil {
		return
	}

	w.queue.start(ctx, ctx, runAsync, w.processNext)
}

func (w *artifactPrewriter) processNext(_ context.Context) bool {
	job, ok := w.queue.popJob()
	if !ok {
		return false
	}

	var batch artifactPrewriteBatch
	batch.add(job)
	for w.queue.batchCanAdd(len(batch.entries), batch.bytes) {
		job, ok = w.queue.popJob()
		if !ok {
			break
		}
		batch.add(job)
	}

	if err := w.store.PrewriteCheckpointArtifacts(batch.entries); err != nil {
		w.queue.fail(err)
		return true
	}
	w.queue.markWritten(batch.lastSeq)
	return true
}

func (w *artifactPrewriter) enqueue(state *storage.BlockState, artifact *storage.ServedBlockFull) (uint64, error) {
	seq, wait, err := w.enqueueDetached(state, artifact)
	if err != nil {
		return 0, err
	}

	return seq, wait()
}

// enqueueDetached assigns the sequence and appends without blocking. The
// returned backpressure wait must run after the caller releases its locks.
func (w *artifactPrewriter) enqueueDetached(state *storage.BlockState, artifact *storage.ServedBlockFull) (uint64, func() error, error) {
	if w == nil {
		return 0, noPrewriteBackpressure, nil
	}

	entry := storage.StateCheckpointBlock{
		State:    checkpointBlockStateMetadata(state),
		Artifact: cloneServedBlockFullSharedPayload(artifact),
	}

	return w.queue.enqueueDetached(entry, servedBlockFullPayloadBytes(artifact))
}

func (w *artifactPrewriter) wait(ctx context.Context, target uint64) error {
	if w == nil {
		return nil
	}

	return w.queue.wait(ctx, target)
}
