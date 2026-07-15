package liteserver

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// benchWorkerPool replicates the previous tonutils-go server model: a fixed
// pool of live workers pulling from one shared queue, producers block when the
// queue is full.
type benchWorkerPool struct {
	queue chan func()
	wg    sync.WaitGroup
}

func newBenchWorkerPool(workers int, queuePerWorker int) *benchWorkerPool {
	p := &benchWorkerPool{queue: make(chan func(), workers*queuePerWorker)}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer p.wg.Done()
			for fn := range p.queue {
				fn()
			}
		}()
	}
	return p
}

func (p *benchWorkerPool) dispatch(fn func()) {
	p.queue <- fn
}

func (p *benchWorkerPool) close() {
	close(p.queue)
	p.wg.Wait()
}

var benchWorkSink atomic.Uint64

//go:noinline
func benchWork(iters int) {
	var x uint64
	for i := 0; i < iters; i++ {
		x += uint64(i) ^ (x >> 3)
	}
	benchWorkSink.Store(x)
}

func benchDispatchers(b *testing.B, workIters int, parallelProducers bool) {
	workers := runtime.GOMAXPROCS(0) * 4

	b.Run("query-executor", func(b *testing.B) {
		executor := newQueryExecutor(workers)
		benchRunDispatch(b, parallelProducers, func(complete func()) {
			executor.run(context.Background(), func() {
				benchWork(workIters)
				complete()
			})
		})
	})

	b.Run("worker-pool", func(b *testing.B) {
		pool := newBenchWorkerPool(workers, 10)
		defer pool.close()
		benchRunDispatch(b, parallelProducers, func(complete func()) {
			pool.dispatch(func() {
				benchWork(workIters)
				complete()
			})
		})
	})
}

func benchRunDispatch(b *testing.B, parallelProducers bool, dispatch func(complete func())) {
	var done sync.WaitGroup

	// Real clients keep a bounded number of requests in flight per connection;
	// the same window for both models also keeps queued goroutines bounded.
	window := make(chan struct{}, runtime.GOMAXPROCS(0)*32)
	complete := func() {
		<-window
		done.Done()
	}

	b.ReportAllocs()
	b.ResetTimer()
	if parallelProducers {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				window <- struct{}{}
				done.Add(1)
				dispatch(complete)
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			window <- struct{}{}
			done.Add(1)
			dispatch(complete)
		}
	}
	done.Wait()
}

func BenchmarkDispatchModelSingleProducer(b *testing.B) {
	for _, tc := range []struct {
		name  string
		iters int
	}{
		{"work-0", 0},
		{"work-1us", 1000},
		{"work-50us", 50000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchDispatchers(b, tc.iters, false)
		})
	}
}

func BenchmarkDispatchModelParallelProducers(b *testing.B) {
	for _, tc := range []struct {
		name  string
		iters int
	}{
		{"work-0", 0},
		{"work-1us", 1000},
		{"work-50us", 50000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchDispatchers(b, tc.iters, true)
		})
	}
}
