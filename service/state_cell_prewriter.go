package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

const (
	stateCellPrewriteBatchJobsLimit = 256
	stateCellPrewriteBatchBytes     = 128 << 20
)

var errStateCellPrewriterClosed = errors.New("state cell prewriter is closed")

type stateCellPrewriteStore interface {
	SaveStateCellRecords(ctx context.Context, cells storage.StateCellRecords) error
}

type stateCellPrewriteBatch struct {
	chunks  [][]storage.EncodedCellRecord
	bytes   uint64
	jobs    int
	lastSeq uint64
}

func (b *stateCellPrewriteBatch) add(job prewriteJob[storage.StateCellRecords]) {
	b.chunks = job.value.AppendChunks(b.chunks)
	b.bytes += job.bytes
	b.jobs++
	b.lastSeq = job.seq
}

// stateCellPrewriter supplies cell-record ownership and batching policy to the
// common prewriter queue engine.
type stateCellPrewriter struct {
	queue *prewriter[storage.StateCellRecords]
	store stateCellPrewriteStore
}

func newStateCellPrewriter(log zerolog.Logger, store stateCellPrewriteStore, maxBytes uint64) *stateCellPrewriter {
	return &stateCellPrewriter{
		queue: newPrewriter[storage.StateCellRecords](log, prewriterConfig{
			closedError:   errStateCellPrewriterClosed,
			failureLog:    "state cell prewriter failed",
			maxQueueBytes: maxBytes,
			maxBatchJobs:  stateCellPrewriteBatchJobsLimit,
			maxBatchBytes: stateCellPrewriteBatchBytes,
		}),
		store: store,
	}
}

func (w *stateCellPrewriter) start(ctx context.Context, writeCtx context.Context, runAsync func(func())) {
	w.queue.start(ctx, writeCtx, runAsync, w.processNext)
}

func (w *stateCellPrewriter) processNext(ctx context.Context) bool {
	job, ok := w.queue.popJob()
	if !ok {
		return false
	}

	var batch stateCellPrewriteBatch
	batch.add(job)
	for w.queue.batchCanAdd(batch.jobs, batch.bytes) {
		job, ok = w.queue.popJob()
		if !ok {
			break
		}
		batch.add(job)
	}

	records := storage.NewStateCellRecordChunks(batch.chunks, batch.bytes)
	if err := w.store.SaveStateCellRecords(ctx, records); err != nil {
		w.queue.fail(fmt.Errorf("prewrite state cells: %w", err))
		return true
	}
	w.queue.markWritten(batch.lastSeq)
	return true
}

func (w *stateCellPrewriter) enqueue(records storage.StateCellRecords) (uint64, error) {
	seq, wait, err := w.enqueueDetached(records)
	if err != nil {
		return 0, err
	}

	return seq, wait()
}

// enqueueDetached assigns the sequence and appends without blocking. The
// returned backpressure wait must run after the caller releases its locks.
func (w *stateCellPrewriter) enqueueDetached(records storage.StateCellRecords) (uint64, func() error, error) {
	if records.Empty() {
		return 0, noPrewriteBackpressure, nil
	}

	return w.queue.enqueueDetached(records, records.ByteSize())
}

func noPrewriteBackpressure() error { return nil }

func (w *stateCellPrewriter) wait(ctx context.Context, target uint64) error {
	return w.queue.wait(ctx, target)
}
