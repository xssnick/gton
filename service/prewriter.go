package service

import (
	"context"
	"errors"
	"sync"

	"github.com/rs/zerolog"
)

const prewriteQueueJobsLimit = 4096

type prewriteJob[T any] struct {
	seq   uint64
	value T
	bytes uint64
}

type prewriterConfig struct {
	closedError   error
	failureLog    string
	maxQueueBytes uint64
	maxBatchJobs  int
	maxBatchBytes uint64
}

// prewriter owns the queue, backpressure, completion sequencing, and shutdown
// drain shared by the state-cell and artifact prewriters. Payload ownership and
// batch construction stay with their domain wrappers.
type prewriter[T any] struct {
	log    zerolog.Logger
	config prewriterConfig

	startOnce sync.Once
	mu        sync.Mutex
	wake      chan struct{}
	done      chan struct{}
	jobs      []prewriteJob[T]
	head      int
	bytes     uint64
	next      uint64
	written   uint64
	err       error
	isClosed  bool
}

func newPrewriter[T any](log zerolog.Logger, config prewriterConfig) *prewriter[T] {
	return &prewriter[T]{
		log:    log,
		config: config,
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

func (w *prewriter[T]) start(runCtx, writeCtx context.Context, runAsync func(func()), processNext func(context.Context) bool) {
	w.startOnce.Do(func() {
		runAsync(func() {
			w.run(runCtx, writeCtx, processNext)
		})
	})
}

func (w *prewriter[T]) run(runCtx, writeCtx context.Context, processNext func(context.Context) bool) {
	for {
		if processNext(writeCtx) {
			continue
		}

		select {
		case <-w.wake:
		case <-runCtx.Done():
			for processNext(writeCtx) {
			}
			w.close(runCtx.Err())
			return
		}
	}
}

// enqueueDetached appends without blocking so producers may call it while
// holding their own locks. The returned backpressure wait must run after those
// locks are released.
func (w *prewriter[T]) enqueueDetached(value T, bytes uint64) (uint64, func() error, error) {
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return 0, nil, err
	}
	if w.isClosed {
		w.mu.Unlock()
		return 0, nil, w.config.closedError
	}

	w.next++
	seq := w.next
	w.bytes += bytes
	w.jobs = append(w.jobs, prewriteJob[T]{
		seq:   seq,
		value: value,
		bytes: bytes,
	})
	w.signalWake()
	w.mu.Unlock()

	return seq, w.waitQueueRoom, nil
}

func (w *prewriter[T]) waitQueueRoom() error {
	for {
		w.mu.Lock()
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return err
		}
		if w.isClosed {
			w.mu.Unlock()
			return w.config.closedError
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

func (w *prewriter[T]) queueWithinLimitsLocked() bool {
	queuedJobs := len(w.jobs) - w.head
	if queuedJobs <= 1 {
		return true
	}
	if queuedJobs >= prewriteQueueJobsLimit {
		return false
	}

	return w.config.maxQueueBytes == 0 || w.bytes <= w.config.maxQueueBytes
}

func (w *prewriter[T]) wait(ctx context.Context, target uint64) error {
	if target == 0 {
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
		if w.isClosed {
			w.mu.Unlock()
			return w.config.closedError
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

func (w *prewriter[T]) popJob() (prewriteJob[T], bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil || len(w.jobs) == 0 {
		return prewriteJob[T]{}, false
	}

	job := w.jobs[w.head]
	w.jobs[w.head] = prewriteJob[T]{}
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

func (w *prewriter[T]) batchCanAdd(jobs int, bytes uint64) bool {
	return jobs < w.config.maxBatchJobs && (w.config.maxBatchBytes == 0 || bytes < w.config.maxBatchBytes)
}

func (w *prewriter[T]) markWritten(seq uint64) {
	w.mu.Lock()
	if seq > w.written {
		w.written = seq
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *prewriter[T]) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
		w.log.Error().Err(err).Msg(w.config.failureLog)
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *prewriter[T]) close(err error) {
	w.mu.Lock()
	if !w.isClosed {
		w.isClosed = true
		if err != nil && !errors.Is(err, context.Canceled) {
			w.err = err
		}
		w.broadcastDone()
	}
	w.mu.Unlock()
}

func (w *prewriter[T]) signalWake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *prewriter[T]) broadcastDone() {
	close(w.done)
	w.done = make(chan struct{})
}
