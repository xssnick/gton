package archive

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"
)

const (
	importedBlockPrepareQueuePerWorker = 2
)

// Prepare slots are shared by every concurrent ImportBytes. A hot import (the
// catch-up pipeline's serial master lane) must not queue behind a full house
// of prefetch prepares, so a small reserve is only takeable by hot jobs; total
// parallelism stays at importedBlockPrepareParallelism and hot jobs may use
// shared slots too, so rare hot imports cannot starve prefetch.
var (
	importedBlockPrepareSharedSlots = make(chan struct{}, importedBlockPrepareParallelism()-importedBlockPrepareHotReserve())
	importedBlockPrepareHotSlots    = make(chan struct{}, importedBlockPrepareHotReserve())
)

type hotImportPrepareKey struct{}

// WithHotImportPrepare marks ctx so the ImportBytes prepare stage may take the
// hot-reserved CPU slots instead of queueing behind prefetch imports.
func WithHotImportPrepare(ctx context.Context) context.Context {
	return context.WithValue(ctx, hotImportPrepareKey{}, true)
}

func hotImportPrepare(ctx context.Context) bool {
	hot, _ := ctx.Value(hotImportPrepareKey{}).(bool)
	return hot
}

func importedBlockPrepareHotReserve() int {
	total := importedBlockPrepareParallelism()
	if total <= 1 {
		return 0
	}
	if total >= 16 {
		return 2
	}
	return 1
}

type importedBlockPrepareJob struct {
	order int
	full  *storage.ServedBlockFull
}

type importedBlockPrepareResult struct {
	order    int
	full     *storage.ServedBlockFull
	prepared PreparedBlock
	elapsed  time.Duration
}

type importedBlockPreparer struct {
	ctx     context.Context
	cancel  context.CancelFunc
	hot     bool
	jobs    chan importedBlockPrepareJob
	results chan importedBlockPrepareResult

	workers   sync.WaitGroup
	collector sync.WaitGroup
	stopOnce  sync.Once
	stopped   chan struct{}

	mu       sync.Mutex
	prepared []importedBlockPrepareResult
	firstErr error
	next     int
}

func newImportedBlockPreparer(ctx context.Context) *importedBlockPreparer {
	prepareCtx, cancel := context.WithCancel(ctx)
	workers := importedBlockPrepareParallelism()
	p := &importedBlockPreparer{
		ctx:     prepareCtx,
		cancel:  cancel,
		hot:     hotImportPrepare(ctx),
		jobs:    make(chan importedBlockPrepareJob, workers*importedBlockPrepareQueuePerWorker),
		results: make(chan importedBlockPrepareResult, workers*importedBlockPrepareQueuePerWorker),
		stopped: make(chan struct{}),
	}

	p.collector.Add(1)
	go p.collectResults()

	for worker := 0; worker < workers; worker++ {
		p.workers.Add(1)
		go p.runWorker()
	}
	return p
}

func importedBlockPrepareParallelism() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}
	return workers
}

func (p *importedBlockPreparer) submit(full *storage.ServedBlockFull) error {
	if err := p.err(); err != nil {
		return err
	}

	job := importedBlockPrepareJob{
		order: p.next,
		full:  full,
	}
	p.next++

	select {
	case p.jobs <- job:
		return nil
	case <-p.ctx.Done():
		return p.err()
	}
}

func (p *importedBlockPreparer) finish(imported *Imported, stats *ImportStats) error {
	p.stop()
	if err := p.err(); err != nil {
		return err
	}

	results := p.resultsSnapshot()
	if len(results) != p.next {
		return fmt.Errorf("prepared archive block count mismatch: got=%d want=%d", len(results), p.next)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].order < results[j].order
	})

	for _, result := range results {
		result.full.Meta = result.prepared.Meta.Clone()
		imported.PreparedBlocks[storage.BlockKey(result.full.ID)] = result.prepared
		imported.FullBlocks = append(imported.FullBlocks, result.full)
		stats.BlockPrepareElapsed += result.elapsed
		stats.StateUpdateCells += uint64(result.prepared.StateUpdateToCells.Len())
		stats.StateUpdateCellBytes += result.prepared.StateUpdateToCells.ByteSize()
		stats.StateUpdateCellPrepare += result.prepared.StateUpdateToCellsElapsed
	}
	return nil
}

func (p *importedBlockPreparer) abort() {
	p.cancel()
	p.stop()
}

func (p *importedBlockPreparer) stop() {
	p.stopOnce.Do(func() {
		close(p.jobs)
		p.workers.Wait()
		close(p.results)
		p.collector.Wait()
		close(p.stopped)
	})
	<-p.stopped
}

func (p *importedBlockPreparer) runWorker() {
	defer p.workers.Done()

	for job := range p.jobs {
		releaseSlot, err := p.acquireSlot()
		if err != nil {
			p.fail(err)
			return
		}

		started := time.Now()
		prepared, err := prepareImportedBlock(job.full.ID, job.full.Block)
		elapsed := time.Since(started)
		releaseSlot()
		if err != nil {
			p.fail(fmt.Errorf("prepare imported block %s: %w", storage.FormatBlockRef(job.full.ID), err))
			return
		}

		result := importedBlockPrepareResult{
			order:    job.order,
			full:     job.full,
			prepared: prepared,
			elapsed:  elapsed,
		}
		select {
		case p.results <- result:
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *importedBlockPreparer) acquireSlot() (func(), error) {
	if p.hot {
		select {
		case importedBlockPrepareSharedSlots <- struct{}{}:
			return releaseImportedBlockPrepareSharedSlot, nil
		case importedBlockPrepareHotSlots <- struct{}{}:
			return releaseImportedBlockPrepareHotSlot, nil
		case <-p.ctx.Done():
			return nil, p.err()
		}
	}

	select {
	case importedBlockPrepareSharedSlots <- struct{}{}:
		return releaseImportedBlockPrepareSharedSlot, nil
	case <-p.ctx.Done():
		return nil, p.err()
	}
}

func releaseImportedBlockPrepareSharedSlot() {
	<-importedBlockPrepareSharedSlots
}

func releaseImportedBlockPrepareHotSlot() {
	<-importedBlockPrepareHotSlots
}

func (p *importedBlockPreparer) collectResults() {
	defer p.collector.Done()

	for result := range p.results {
		p.mu.Lock()
		p.prepared = append(p.prepared, result)
		p.mu.Unlock()
	}
}

func (p *importedBlockPreparer) fail(err error) {
	if err == nil {
		return
	}

	p.mu.Lock()
	if p.firstErr == nil {
		p.firstErr = err
		p.cancel()
	}
	p.mu.Unlock()
}

func (p *importedBlockPreparer) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.firstErr != nil {
		return p.firstErr
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (p *importedBlockPreparer) resultsSnapshot() []importedBlockPrepareResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	results := make([]importedBlockPrepareResult, len(p.prepared))
	copy(results, p.prepared)
	return results
}
