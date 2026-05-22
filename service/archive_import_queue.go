package service

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/xssnick/gton/service/archive"
)

type archiveImportPriority uint8

const (
	archiveImportPriorityHot archiveImportPriority = iota
	archiveImportPriorityPrefetch
)

type archiveImportQueue struct {
	downloadHot      chan archiveDownloadJob
	downloadPrefetch chan archiveDownloadJob
	prepareHot       chan archivePrepareJob
	preparePrefetch  chan archivePrepareJob
	activeDownload   atomic.Int64
	activePrepare    atomic.Int64
}

type archiveDownloadJob struct {
	ctx              context.Context
	masterchainSeqno uint32
	shard            archive.ShardID
	splitDepth       uint32
	priority         archiveImportPriority
	done             chan archiveImportQueueResult
}

type archivePrepareJob struct {
	ctx        context.Context
	downloaded *archive.Downloaded
	splitDepth uint32
	priority   archiveImportPriority
	done       chan archiveImportQueueResult
}

type archiveImportQueueResult struct {
	imported *archiveImportResult
	err      error
}

func (r *archiveCatchUpRunner) startArchiveImportQueue(ctx context.Context) *archiveImportQueue {
	downloadWorkers := archiveDownloadWorkers()
	downloadBudget := archiveDownloadWorkerBudget(downloadWorkers)
	prepareWorkers := archiveCPUWorkers()
	prepareBudget := archivePrepareWorkerBudget(prepareWorkers)
	queue := &archiveImportQueue{
		downloadHot:      make(chan archiveDownloadJob, downloadWorkers),
		downloadPrefetch: make(chan archiveDownloadJob, downloadWorkers),
		prepareHot:       make(chan archivePrepareJob, prepareWorkers),
		preparePrefetch:  make(chan archivePrepareJob, prepareWorkers),
	}
	for worker := 0; worker < downloadBudget.hotOnly; worker++ {
		go func() {
			for {
				job, ok := queue.nextHotDownload(ctx)
				if !ok {
					return
				}
				queue.runDownloadJob(r, job)
			}
		}()
	}
	for worker := 0; worker < downloadBudget.shared; worker++ {
		go func() {
			for {
				job, ok := queue.nextDownload(ctx)
				if !ok {
					return
				}
				queue.runDownloadJob(r, job)
			}
		}()
	}
	for worker := 0; worker < prepareBudget.hotOnly; worker++ {
		go func() {
			for {
				job, ok := queue.nextHotPrepare(ctx)
				if !ok {
					return
				}
				queue.runPrepareJob(r, job)
			}
		}()
	}
	for worker := 0; worker < prepareBudget.shared; worker++ {
		go func() {
			for {
				job, ok := queue.nextPrepare(ctx)
				if !ok {
					return
				}
				queue.runPrepareJob(r, job)
			}
		}()
	}
	return queue
}

func (q *archiveImportQueue) snapshot() archiveImportQueueSnapshot {
	if q == nil {
		return archiveImportQueueSnapshot{}
	}

	return archiveImportQueueSnapshot{
		activeDownload:         q.activeDownload.Load(),
		activePrepare:          q.activePrepare.Load(),
		downloadHotQueued:      len(q.downloadHot),
		downloadPrefetchQueued: len(q.downloadPrefetch),
		prepareHotQueued:       len(q.prepareHot),
		preparePrefetchQueued:  len(q.preparePrefetch),
	}
}

func (q *archiveImportQueue) importArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority) (*archiveImportResult, error) {
	done, err := q.submitArchive(ctx, masterchainSeqno, shard, splitDepth, priority)
	if err != nil {
		return nil, err
	}

	select {
	case result := <-done:
		return result.imported, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *archiveImportQueue) submitArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority) (<-chan archiveImportQueueResult, error) {
	if q == nil {
		return nil, fmt.Errorf("archive import queue is nil")
	}
	done := make(chan archiveImportQueueResult, 1)
	job := archiveDownloadJob{
		ctx:              ctx,
		masterchainSeqno: masterchainSeqno,
		shard:            shard,
		splitDepth:       splitDepth,
		priority:         priority,
		done:             done,
	}

	select {
	case q.downloadJobs(priority) <- job:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return done, nil
}

func (q *archiveImportQueue) runDownloadJob(r *archiveCatchUpRunner, job archiveDownloadJob) {
	if err := job.ctx.Err(); err != nil {
		job.done <- archiveImportQueueResult{err: err}
		return
	}

	q.activeDownload.Add(1)
	defer q.activeDownload.Add(-1)

	downloaded, err := r.downloadArchiveFile(job.ctx, job.masterchainSeqno, job.shard)
	if err != nil {
		job.done <- archiveImportQueueResult{err: err}
		return
	}

	prepare := archivePrepareJob{
		ctx:        job.ctx,
		downloaded: downloaded,
		splitDepth: job.splitDepth,
		priority:   job.priority,
		done:       job.done,
	}
	select {
	case q.prepareJobs(job.priority) <- prepare:
	case <-job.ctx.Done():
		job.done <- archiveImportQueueResult{err: job.ctx.Err()}
	}
}

func (q *archiveImportQueue) runPrepareJob(r *archiveCatchUpRunner, job archivePrepareJob) {
	if err := job.ctx.Err(); err != nil {
		job.done <- archiveImportQueueResult{err: err}
		return
	}

	q.activePrepare.Add(1)
	defer q.activePrepare.Add(-1)

	imported, err := r.prepareArchiveDownload(job.ctx, job.downloaded.MasterchainSeqno, job.downloaded.Shard, job.splitDepth, job.downloaded)
	job.done <- archiveImportQueueResult{imported: imported, err: err}
}

func (q *archiveImportQueue) nextDownload(ctx context.Context) (archiveDownloadJob, bool) {
	return nextArchivePriorityJob(ctx, q.downloadHot, q.downloadPrefetch, archiveDownloadJob{})
}

func (q *archiveImportQueue) nextHotDownload(ctx context.Context) (archiveDownloadJob, bool) {
	return nextArchiveHotJob(ctx, q.downloadHot, archiveDownloadJob{})
}

func (q *archiveImportQueue) nextPrepare(ctx context.Context) (archivePrepareJob, bool) {
	return nextArchivePriorityJob(ctx, q.prepareHot, q.preparePrefetch, archivePrepareJob{})
}

func (q *archiveImportQueue) nextHotPrepare(ctx context.Context) (archivePrepareJob, bool) {
	return nextArchiveHotJob(ctx, q.prepareHot, archivePrepareJob{})
}

func nextArchivePriorityJob[T any](ctx context.Context, hot <-chan T, prefetch <-chan T, zero T) (T, bool) {
	select {
	case job := <-hot:
		return job, true
	default:
	}

	select {
	case job := <-hot:
		return job, true
	case job := <-prefetch:
		return job, true
	case <-ctx.Done():
		return zero, false
	}
}

func nextArchiveHotJob[T any](ctx context.Context, hot <-chan T, zero T) (T, bool) {
	select {
	case job := <-hot:
		return job, true
	case <-ctx.Done():
		return zero, false
	}
}

func (q *archiveImportQueue) downloadJobs(priority archiveImportPriority) chan archiveDownloadJob {
	if priority == archiveImportPriorityHot {
		return q.downloadHot
	}
	return q.downloadPrefetch
}

func (q *archiveImportQueue) prepareJobs(priority archiveImportPriority) chan archivePrepareJob {
	if priority == archiveImportPriorityHot {
		return q.prepareHot
	}
	return q.preparePrefetch
}

func archiveCPUWorkers() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}
	return workers
}

func archiveDownloadWorkers() int {
	workers := archiveCPUWorkers() * archiveDownloadWorkerMultiplier
	if workers < archiveDownloadWorkerMin {
		return archiveDownloadWorkerMin
	}
	if workers > archiveDownloadWorkerMax {
		return archiveDownloadWorkerMax
	}
	return workers
}

type archiveWorkerBudget struct {
	hotOnly int
	shared  int
}

func archiveDownloadWorkerBudget(workers int) archiveWorkerBudget {
	if workers <= 1 {
		return archiveWorkerBudget{shared: workers}
	}

	hotOnly := workers / 4
	if hotOnly < archiveDownloadHotWorkerMin {
		hotOnly = archiveDownloadHotWorkerMin
	}
	if hotOnly > archiveDownloadHotWorkerMax {
		hotOnly = archiveDownloadHotWorkerMax
	}
	if hotOnly >= workers {
		hotOnly = workers - 1
	}

	return archiveWorkerBudget{
		hotOnly: hotOnly,
		shared:  workers - hotOnly,
	}
}

func archivePrepareWorkerBudget(workers int) archiveWorkerBudget {
	if workers <= 1 {
		return archiveWorkerBudget{shared: workers}
	}

	hotOnly := workers / 8
	if hotOnly < archivePrepareHotWorkerMin {
		hotOnly = archivePrepareHotWorkerMin
	}
	if hotOnly > archivePrepareHotWorkerMax {
		hotOnly = archivePrepareHotWorkerMax
	}
	if hotOnly >= workers {
		hotOnly = workers - 1
	}

	return archiveWorkerBudget{
		hotOnly: hotOnly,
		shared:  workers - hotOnly,
	}
}
