package archive

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const importedBlockPrepareQueuePerWorker = 2

var errImporterClosed = errors.New("archive importer is closed")

type hotImportPrepareKey struct{}

// WithHotImportPrepare marks ctx so the ImportBytes prepare stage uses the
// hot-priority lane instead of queueing behind prefetch imports.
func WithHotImportPrepare(ctx context.Context) context.Context {
	return context.WithValue(ctx, hotImportPrepareKey{}, true)
}

func hotImportPrepare(ctx context.Context) bool {
	hot, _ := ctx.Value(hotImportPrepareKey{}).(bool)
	return hot
}

func importedBlockPrepareParallelism() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}
	return workers
}

func importedBlockPrepareHotReserve(workers int) int {
	if workers <= 1 {
		return 0
	}
	if workers >= 16 {
		return 2
	}
	return 1
}

type importedBlockPrepareFunc func(ton.BlockIDExt, []byte) (PreparedBlock, error)

type importedBlockPrepareJob struct {
	preparer *importedBlockPreparer
	order    int
	full     *storage.ServedBlockFull
}

type importedBlockPrepareResult struct {
	full     *storage.ServedBlockFull
	prepared PreparedBlock
	elapsed  time.Duration
}

type importedBlockPrepareDispatcher struct {
	hot      chan importedBlockPrepareJob
	prefetch chan importedBlockPrepareJob
	prepare  importedBlockPrepareFunc
	workers  int

	mu        sync.Mutex
	closed    bool
	closing   chan struct{}
	stop      chan struct{}
	closeOnce sync.Once
	jobs      sync.WaitGroup
	workerWG  sync.WaitGroup
}

func newImportedBlockPrepareDispatcher(workers int, prepare importedBlockPrepareFunc) *importedBlockPrepareDispatcher {
	if workers < 1 {
		workers = 1
	}

	d := &importedBlockPrepareDispatcher{
		hot:      make(chan importedBlockPrepareJob, workers*importedBlockPrepareQueuePerWorker),
		prefetch: make(chan importedBlockPrepareJob, workers*importedBlockPrepareQueuePerWorker),
		prepare:  prepare,
		workers:  workers,
		closing:  make(chan struct{}),
		stop:     make(chan struct{}),
	}

	hotWorkers := importedBlockPrepareHotReserve(workers)
	sharedWorkers := workers - hotWorkers
	for worker := 0; worker < sharedWorkers; worker++ {
		d.workerWG.Go(d.runSharedWorker)
	}
	for worker := 0; worker < hotWorkers; worker++ {
		d.workerWG.Go(d.runHotWorker)
	}
	return d
}

func (d *importedBlockPrepareDispatcher) submit(ctx context.Context, hot bool, job importedBlockPrepareJob) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errImporterClosed
	}
	d.jobs.Add(1)
	d.mu.Unlock()

	jobs := d.prefetch
	if hot {
		jobs = d.hot
	}

	select {
	case jobs <- job:
		return nil
	case <-ctx.Done():
		d.jobs.Done()
		return ctx.Err()
	case <-d.closing:
		d.jobs.Done()
		return errImporterClosed
	}
}

func (d *importedBlockPrepareDispatcher) runSharedWorker() {
	for {
		job, ok := d.nextSharedJob()
		if !ok {
			return
		}
		job.preparer.prepareBlock(job, d.prepare)
		d.jobs.Done()
	}
}

func (d *importedBlockPrepareDispatcher) runHotWorker() {
	for {
		select {
		case job := <-d.hot:
			job.preparer.prepareBlock(job, d.prepare)
			d.jobs.Done()
		case <-d.stop:
			return
		}
	}
}

func (d *importedBlockPrepareDispatcher) nextSharedJob() (importedBlockPrepareJob, bool) {
	select {
	case job := <-d.hot:
		return job, true
	default:
	}

	select {
	case job := <-d.hot:
		return job, true
	case job := <-d.prefetch:
		return job, true
	case <-d.stop:
		return importedBlockPrepareJob{}, false
	}
}

func (d *importedBlockPrepareDispatcher) close() {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		close(d.closing)
		d.mu.Unlock()

		d.jobs.Wait()
		close(d.stop)
		d.workerWG.Wait()
	})
}

// Importer owns the bounded block preparation workers shared by its concurrent
// archive imports.
type Importer struct {
	dispatcher *importedBlockPrepareDispatcher

	mu        sync.Mutex
	closed    bool
	active    sync.WaitGroup
	closeOnce sync.Once
}

// NewImporter starts one owner-managed archive block preparation pool.
func NewImporter() *Importer {
	return newImporter(importedBlockPrepareParallelism(), prepareImportedBlock)
}

func newImporter(workers int, prepare importedBlockPrepareFunc) *Importer {
	return &Importer{
		dispatcher: newImportedBlockPrepareDispatcher(workers, prepare),
	}
}

func (i *Importer) begin() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return errImporterClosed
	}
	i.active.Add(1)
	return nil
}

func (i *Importer) end() {
	i.active.Done()
}

// Close stops accepting imports, completes admitted block preparations and
// waits for every active import and worker to finish.
func (i *Importer) Close() {
	i.closeOnce.Do(func() {
		i.mu.Lock()
		i.closed = true
		i.mu.Unlock()

		i.dispatcher.close()
		i.active.Wait()
	})
}

type importedBlockPreparer struct {
	ctx        context.Context
	cancel     context.CancelFunc
	hot        bool
	dispatcher *importedBlockPrepareDispatcher
	done       chan struct{}

	mu         sync.Mutex
	closed     bool
	doneClosed bool
	firstErr   error
	next       int
	pending    int
	prepared   []importedBlockPrepareResult
}

func newImportedBlockPreparer(ctx context.Context, dispatcher *importedBlockPrepareDispatcher) *importedBlockPreparer {
	prepareCtx, cancel := context.WithCancel(ctx)
	return &importedBlockPreparer{
		ctx:        prepareCtx,
		cancel:     cancel,
		hot:        hotImportPrepare(ctx),
		dispatcher: dispatcher,
		done:       make(chan struct{}),
	}
}

func (p *importedBlockPreparer) submit(full *storage.ServedBlockFull) error {
	p.mu.Lock()
	if p.firstErr != nil {
		err := p.firstErr
		p.mu.Unlock()
		return err
	}
	if err := p.ctx.Err(); err != nil {
		p.mu.Unlock()
		return err
	}
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("archive block preparer is closed")
	}

	job := importedBlockPrepareJob{
		preparer: p,
		order:    p.next,
		full:     full,
	}
	p.next++
	p.pending++
	p.prepared = append(p.prepared, importedBlockPrepareResult{})
	p.mu.Unlock()

	if err := p.dispatcher.submit(p.ctx, p.hot, job); err != nil {
		p.complete(job, importedBlockPrepareResult{}, err)
		return p.err()
	}
	return nil
}

func (p *importedBlockPreparer) finish(imported *Imported, stats *ImportStats) error {
	p.closeAndWait()
	if err := p.err(); err != nil {
		return err
	}

	if len(p.prepared) != p.next {
		return fmt.Errorf("prepared archive block count mismatch: got=%d want=%d", len(p.prepared), p.next)
	}

	for _, result := range p.prepared {
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
	p.closeAndWait()
}

func (p *importedBlockPreparer) closeAndWait() {
	p.mu.Lock()
	p.closed = true
	p.signalDoneLocked()
	p.mu.Unlock()

	<-p.done
}

func (p *importedBlockPreparer) prepareBlock(job importedBlockPrepareJob, prepare importedBlockPrepareFunc) {
	if err := p.err(); err != nil {
		p.complete(job, importedBlockPrepareResult{}, err)
		return
	}

	started := time.Now()
	prepared, err := prepare(job.full.ID, job.full.Block)
	elapsed := time.Since(started)
	if err != nil {
		p.complete(
			job,
			importedBlockPrepareResult{},
			fmt.Errorf("prepare imported block %s: %w", storage.FormatBlockRef(job.full.ID), err),
		)
		return
	}

	p.complete(job, importedBlockPrepareResult{
		full:     job.full,
		prepared: prepared,
		elapsed:  elapsed,
	}, nil)
}

func (p *importedBlockPreparer) complete(job importedBlockPrepareJob, result importedBlockPrepareResult, err error) {
	p.mu.Lock()
	if err != nil {
		if p.firstErr == nil {
			p.firstErr = err
			p.cancel()
		}
	} else {
		p.prepared[job.order] = result
	}
	p.pending--
	p.signalDoneLocked()
	p.mu.Unlock()
}

func (p *importedBlockPreparer) signalDoneLocked() {
	if p.closed && p.pending == 0 && !p.doneClosed {
		close(p.done)
		p.doneClosed = true
	}
}

func (p *importedBlockPreparer) err() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.firstErr != nil {
		return p.firstErr
	}
	return p.ctx.Err()
}
