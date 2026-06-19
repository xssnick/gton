package service

import (
	"context"
	"runtime"
	"sync"
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
	downloadedBytes  *archiveDownloadByteGate
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
	bytes      uint64
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
		downloadedBytes:  newArchiveDownloadByteGate(r.archiveCheckpointBackpressureBytes()),
	}
	for worker := 0; worker < downloadBudget.hotOnly; worker++ {
		go func() {
			for {
				job, ok := nextArchiveHotJob(ctx, queue.downloadHot, archiveDownloadJob{})
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
				job, ok := nextArchivePriorityJob(ctx, queue.downloadHot, queue.downloadPrefetch, archiveDownloadJob{})
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
				job, ok := nextArchiveHotJob(ctx, queue.prepareHot, archivePrepareJob{})
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
	if q == nil {
		return archiveImportQueueSnapshot{}
	}

	snapshot := archiveImportQueueSnapshot{
		activeDownload:         q.activeDownload.Load(),
		activePrepare:          q.activePrepare.Load(),
		downloadHotQueued:      len(q.downloadHot),
		downloadPrefetchQueued: len(q.downloadPrefetch),
		prepareHotQueued:       len(q.prepareHot),
		preparePrefetchQueued:  len(q.preparePrefetch),
	}
	if q.downloadedBytes != nil {
		snapshot.downloadedBytes = q.downloadedBytes.size()
		snapshot.downloadedBytesLimit = q.downloadedBytes.limit()
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

	if q.downloadedBytes != nil {
		if err := q.downloadedBytes.wait(job.ctx); err != nil {
			job.done <- archiveImportQueueResult{err: err}
			return
		}
	}

	downloaded, err := r.downloadArchiveFile(job.ctx, job.masterchainSeqno, job.shard, q.archiveDownloadsStarved())
	if err != nil {
		job.done <- archiveImportQueueResult{err: err}
		return
	}
	downloadedBytes := uint64(len(downloaded.Data))
	if q.downloadedBytes != nil {
		q.downloadedBytes.add(downloadedBytes)
	}

	prepare := archivePrepareJob{
		ctx:        job.ctx,
		downloaded: downloaded,
		bytes:      downloadedBytes,
		splitDepth: job.splitDepth,
		priority:   job.priority,
		done:       job.done,
	}
	select {
	case q.prepareJobs(job.priority) <- prepare:
	case <-job.ctx.Done():
		if q.downloadedBytes != nil {
			q.downloadedBytes.release(downloadedBytes)
		}
		job.done <- archiveImportQueueResult{err: job.ctx.Err()}
	}
}

func (q *archiveImportQueue) runPrepareJob(r *archiveCatchUpRunner, job archivePrepareJob) {
	releaseDownloadedData := func() {
		if job.downloaded != nil {
			job.downloaded.Data = nil
		}
		if q.downloadedBytes != nil {
			q.downloadedBytes.release(job.bytes)
		}
	}

	if err := job.ctx.Err(); err != nil {
		releaseDownloadedData()
		job.done <- archiveImportQueueResult{err: err}
		return
	}

	q.activePrepare.Add(1)
	defer q.activePrepare.Add(-1)

	imported, err := r.prepareArchiveDownload(job.ctx, job.downloaded.MasterchainSeqno, job.downloaded.Shard, job.splitDepth, job.downloaded)
	peer := job.downloaded.Peer
	archiveID := job.downloaded.ArchiveID
	releaseDownloadedData()
	job.done <- archiveImportQueueResult{imported: imported, peer: peer, archiveID: archiveID, err: err}
}

func (q *archiveImportQueue) archiveDownloadsStarved() bool {
	if q == nil {
		return false
	}
	if q.activePrepare.Load() > 0 || len(q.prepareHot) > 0 || len(q.preparePrefetch) > 0 {
		return false
	}
	return q.downloadedBytes == nil || q.downloadedBytes.size() == 0
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

type archiveDownloadByteGate struct {
	max   uint64
	mu    sync.Mutex
	done  chan struct{}
	bytes uint64
}

func newArchiveDownloadByteGate(max uint64) *archiveDownloadByteGate {
	if max == 0 {
		return nil
	}
	return &archiveDownloadByteGate{
		max:  max,
		done: make(chan struct{}),
	}
}

func (g *archiveDownloadByteGate) wait(ctx context.Context) error {
	if g == nil || g.max == 0 {
		return nil
	}

	for {
		g.mu.Lock()
		if g.bytes < g.max {
			g.mu.Unlock()
			return nil
		}
		done := g.done
		g.mu.Unlock()

		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *archiveDownloadByteGate) add(bytes uint64) {
	if g == nil || bytes == 0 {
		return
	}

	g.mu.Lock()
	if g.bytes > ^uint64(0)-bytes {
		g.bytes = ^uint64(0)
	} else {
		g.bytes += bytes
	}
	g.mu.Unlock()
}

func (g *archiveDownloadByteGate) release(bytes uint64) {
	if g == nil || bytes == 0 {
		return
	}

	g.mu.Lock()
	if bytes > g.bytes {
		g.bytes = 0
	} else {
		g.bytes -= bytes
	}
	g.broadcast()
	g.mu.Unlock()
}

func (g *archiveDownloadByteGate) size() uint64 {
	if g == nil {
		return 0
	}

	g.mu.Lock()
	bytes := g.bytes
	g.mu.Unlock()
	return bytes
}

func (g *archiveDownloadByteGate) limit() uint64 {
	if g == nil {
		return 0
	}
	return g.max
}

func (g *archiveDownloadByteGate) broadcast() {
	close(g.done)
	g.done = make(chan struct{})
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
