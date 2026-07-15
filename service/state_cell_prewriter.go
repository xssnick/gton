package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

const (
	stateCellPrewriteQueueJobsLimit = 4096
	stateCellPrewriteBatchJobsLimit = 256
	stateCellPrewriteBatchBytes     = 128 << 20
)

var errStateCellPrewriterClosed = errors.New("state cell prewriter is closed")

type stateCellPrewriteStore interface {
	SaveStateCellRecords(ctx context.Context, cells storage.StateCellRecords) error
}

type stateCellPrewriteJob struct {
	seq     uint64
	records storage.StateCellRecords
	bytes   uint64
}

type stateCellPrewriteBatch struct {
	chunks  [][]storage.EncodedCellRecord
	bytes   uint64
	jobs    int
	lastSeq uint64
}

type stateCellPrewriter struct {
	log      zerolog.Logger
	store    stateCellPrewriteStore
	maxBytes uint64

	startOnce sync.Once
	mu        sync.Mutex
	wake      chan struct{}
	done      chan struct{}
	jobs      []stateCellPrewriteJob
	head      int
	bytes     uint64
	next      uint64
	written   uint64
	err       error
	closed    bool
}

func newStateCellPrewriter(log zerolog.Logger, store stateCellPrewriteStore, maxBytes uint64) *stateCellPrewriter {
	if store == nil {
		return nil
	}
	return &stateCellPrewriter{
		log:      log,
		store:    store,
		maxBytes: maxBytes,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func (w *stateCellPrewriter) start(ctx context.Context, writeCtx context.Context, runAsync func(func())) {
	if w == nil {
		return
	}
	w.startOnce.Do(func() {
		runAsync(func() {
			w.run(ctx, writeCtx)
		})
	})
}

func (w *stateCellPrewriter) run(ctx context.Context, writeCtx context.Context) {
	for {
		batch, ok := w.popBatch()
		if ok {
			w.process(writeCtx, batch)
			continue
		}

		select {
		case <-w.wake:
		case <-ctx.Done():
			for {
				batch, ok = w.popBatch()
				if !ok {
					w.close(ctx.Err())
					return
				}
				w.process(writeCtx, batch)
			}
		}
	}
}

func (w *stateCellPrewriter) enqueue(records storage.StateCellRecords) (uint64, error) {
	if w == nil || records.Empty() {
		return 0, nil
	}

	jobBytes := records.ByteSize()
	w.mu.Lock()
	for {
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return 0, err
		}
		if w.closed {
			w.mu.Unlock()
			return 0, errStateCellPrewriterClosed
		}
		if w.hasQueueRoomLocked(jobBytes) {
			break
		}

		done := w.done
		w.mu.Unlock()
		<-done
		w.mu.Lock()
	}

	w.next++
	seq := w.next
	w.bytes += jobBytes
	w.jobs = append(w.jobs, stateCellPrewriteJob{
		seq:     seq,
		records: records,
		bytes:   jobBytes,
	})
	w.signalWake()
	w.mu.Unlock()
	return seq, nil
}

func (w *stateCellPrewriter) hasQueueRoomLocked(jobBytes uint64) bool {
	queuedJobs := len(w.jobs) - w.head
	if queuedJobs == 0 {
		return true
	}
	if queuedJobs >= stateCellPrewriteQueueJobsLimit {
		return false
	}
	if w.maxBytes > 0 && jobBytes > 0 && w.bytes+jobBytes > w.maxBytes {
		return false
	}
	return true
}

func (w *stateCellPrewriter) wait(ctx context.Context, target uint64) error {
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
			return errStateCellPrewriterClosed
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

func (w *stateCellPrewriter) popJob() (stateCellPrewriteJob, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil || len(w.jobs) == 0 {
		return stateCellPrewriteJob{}, false
	}

	job := w.jobs[w.head]
	w.jobs[w.head] = stateCellPrewriteJob{}
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

func (w *stateCellPrewriter) popBatch() (stateCellPrewriteBatch, bool) {
	job, ok := w.popJob()
	if !ok {
		return stateCellPrewriteBatch{}, false
	}

	var batch stateCellPrewriteBatch
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

func (b *stateCellPrewriteBatch) add(job stateCellPrewriteJob) {
	bytes := job.records.ByteSize()
	b.chunks = job.records.AppendChunks(b.chunks)
	if bytes == 0 {
		bytes = job.bytes
	}
	b.bytes += bytes
	b.jobs++
	b.lastSeq = job.seq
}

func (b stateCellPrewriteBatch) canAddMore() bool {
	if b.jobs >= stateCellPrewriteBatchJobsLimit {
		return false
	}
	return stateCellPrewriteBatchBytes == 0 || b.bytes < stateCellPrewriteBatchBytes
}

func (w *stateCellPrewriter) process(ctx context.Context, batch stateCellPrewriteBatch) {
	records := storage.NewStateCellRecordChunks(batch.chunks, batch.bytes)
	if err := w.store.SaveStateCellRecords(ctx, records); err != nil {
		w.fail(fmt.Errorf("prewrite state cells: %w", err))
		return
	}
	w.markWritten(batch.lastSeq)
}

func (w *stateCellPrewriter) markWritten(seq uint64) {
	w.mu.Lock()
	if seq > w.written {
		w.written = seq
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *stateCellPrewriter) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
		w.log.Error().Err(err).Msg("state cell prewriter failed")
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *stateCellPrewriter) close(err error) {
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

func (w *stateCellPrewriter) signalWake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *stateCellPrewriter) broadcastDone() {
	close(w.done)
	w.done = make(chan struct{})
}
