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
	imported  *archiveImportResult
	peer      string
	archiveID int64
	err       error
}

type archiveImportPeerError struct {
	peer      string
	archiveID int64
	err       error
}

func (e *archiveImportPeerError) Error() string {
	return fmt.Sprintf("archive %d from %s: %v", e.archiveID, e.peer, e.err)
}

func (e *archiveImportPeerError) Unwrap() error {
	return e.err
}

func (r *archiveCatchUpRunner) startArchiveImportQueue(ctx context.Context) *archiveImportQueue {
	downloadWorkers := archiveDownloadWorkers()
	prepareWorkers := archiveCPUWorkers()
	queue := &archiveImportQueue{
		downloadHot:      make(chan archiveDownloadJob, downloadWorkers),
		downloadPrefetch: make(chan archiveDownloadJob, downloadWorkers),
		prepareHot:       make(chan archivePrepareJob, prepareWorkers),
		preparePrefetch:  make(chan archivePrepareJob, prepareWorkers),
	}
	for worker := 0; worker < downloadWorkers; worker++ {
		go func() {
			for {
				job, ok := nextArchivePriorityJob(ctx, queue.downloadHot, queue.downloadPrefetch, archiveDownloadJob{})
				if !ok {
					return
				}
				queue.runDownloadJob(r, job)
			}
		}()
	}
	for worker := 0; worker < prepareWorkers; worker++ {
		go func() {
			for {
				job, ok := nextArchivePriorityJob(ctx, queue.prepareHot, queue.preparePrefetch, archivePrepareJob{})
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
	snapshot := archiveImportQueueSnapshot{
		activeDownload:         q.activeDownload.Load(),
		activePrepare:          q.activePrepare.Load(),
		downloadHotQueued:      len(q.downloadHot),
		downloadPrefetchQueued: len(q.downloadPrefetch),
		prepareHotQueued:       len(q.prepareHot),
		preparePrefetchQueued:  len(q.preparePrefetch),
	}
	return snapshot
}

func (q *archiveImportQueue) importArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority) (*archiveImportResult, error) {
	done, err := q.submitArchive(ctx, masterchainSeqno, shard, splitDepth, priority)
	if err != nil {
		return nil, err
	}

	select {
	case result := <-done:
		if result.err != nil && result.peer != "" {
			return nil, &archiveImportPeerError{
				peer:      result.peer,
				archiveID: result.archiveID,
				err:       result.err,
			}
		}
		return result.imported, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *archiveImportQueue) submitArchive(ctx context.Context, masterchainSeqno uint32, shard archive.ShardID, splitDepth uint32, priority archiveImportPriority) (<-chan archiveImportQueueResult, error) {
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

	downloaded, err := r.downloadArchiveFile(job.ctx, job.masterchainSeqno, job.shard, q.archiveDownloadsStarved())
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
		downloaded.Data = nil
		job.done <- archiveImportQueueResult{err: job.ctx.Err()}
	}
}

func (q *archiveImportQueue) runPrepareJob(r *archiveCatchUpRunner, job archivePrepareJob) {
	releaseDownloadedData := func() {
		if job.downloaded != nil {
			job.downloaded.Data = nil
		}
	}

	if err := job.ctx.Err(); err != nil {
		releaseDownloadedData()
		job.done <- archiveImportQueueResult{err: err}
		return
	}

	q.activePrepare.Add(1)
	defer q.activePrepare.Add(-1)

	prepareCtx := job.ctx
	if job.priority == archiveImportPriorityHot {
		prepareCtx = archive.WithHotImportPrepare(prepareCtx)
	}
	imported, err := r.prepareArchiveDownload(prepareCtx, job.downloaded.MasterchainSeqno, job.downloaded.Shard, job.splitDepth, job.downloaded)
	peer := job.downloaded.Peer
	archiveID := job.downloaded.ArchiveID
	releaseDownloadedData()
	job.done <- archiveImportQueueResult{imported: imported, peer: peer, archiveID: archiveID, err: err}
}

func (q *archiveImportQueue) archiveDownloadsStarved() bool {
	if q.activePrepare.Load() > 0 || len(q.prepareHot) > 0 || len(q.preparePrefetch) > 0 {
		return false
	}
	return true
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
