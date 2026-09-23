package service

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func newTestArchiveSession(t *testing.T) *p2p.ArchiveSession {
	t.Helper()

	store := openTestPebbleStorage(t)
	node, err := p2p.New(p2p.Options{
		PeerStorage:          store,
		StateArtifactStorage: store,
		StateFilesDir:        t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new archive session node: %v", err)
	}
	session := node.BeginArchiveSession()
	t.Cleanup(session.Close)
	return session
}

func TestArchiveDownloadWorkersUsesNetworkWorkerBudget(t *testing.T) {
	old := runtime.GOMAXPROCS(2)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(old)
	})

	if got := archiveDownloadWorkers(); got != archiveDownloadWorkerMin {
		t.Fatalf("download workers at low GOMAXPROCS = %d want %d", got, archiveDownloadWorkerMin)
	}

	runtime.GOMAXPROCS(12)
	if got := archiveDownloadWorkers(); got != archiveDownloadWorkerMax {
		t.Fatalf("download workers at GOMAXPROCS=12 = %d want %d", got, archiveDownloadWorkerMax)
	}

	runtime.GOMAXPROCS(64)
	if got := archiveDownloadWorkers(); got != archiveDownloadWorkerMax {
		t.Fatalf("download workers at high GOMAXPROCS = %d want %d", got, archiveDownloadWorkerMax)
	}
}

func TestArchiveImportQueuePreservesPeerOnPrepareError(t *testing.T) {
	queue := &archiveImportQueue{
		downloadHot: make(chan archiveDownloadJob, 1),
	}
	wantErr := errors.New("invalid archive")
	done := make(chan error, 1)
	go func() {
		_, err := queue.importArchive(context.Background(), 100, archive.ShardID{Workchain: -1, Shard: -1 << 63}, 0, archiveImportPriorityHot)
		done <- err
	}()

	job := <-queue.downloadHot
	job.done <- archiveImportQueueResult{
		peer:      "192.0.2.1:30303",
		archiveID: 42,
		err:       wantErr,
	}

	err := <-done
	var peerErr *archiveImportPeerError
	if !errors.As(err, &peerErr) {
		t.Fatalf("prepare error type = %T, want archiveImportPeerError", err)
	}
	if peerErr.peer != "192.0.2.1:30303" || peerErr.archiveID != 42 {
		t.Fatalf("prepare error peer metadata = %+v", peerErr)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("prepare error does not wrap original error: %v", err)
	}
}

func TestNextArchivePriorityJobDrainsReadyHotBeforePrefetch(t *testing.T) {
	hot := make(chan int, 1)
	prefetch := make(chan int, 1)
	hot <- 1
	prefetch <- 2

	got, ok := nextArchivePriorityJob(context.Background(), hot, prefetch, 0)
	if !ok || got != 1 {
		t.Fatalf("priority job = %d ok=%v want hot job", got, ok)
	}
}

func TestArchiveImportQueueSnapshotReportsActiveAndQueuedJobs(t *testing.T) {
	queue := &archiveImportQueue{
		downloadHot:      make(chan archiveDownloadJob, 2),
		downloadPrefetch: make(chan archiveDownloadJob, 2),
		prepareHot:       make(chan archivePrepareJob, 2),
		preparePrefetch:  make(chan archivePrepareJob, 2),
	}
	queue.downloadHot <- archiveDownloadJob{}
	queue.downloadPrefetch <- archiveDownloadJob{}
	queue.preparePrefetch <- archivePrepareJob{}
	queue.activeDownload.Add(3)
	queue.activePrepare.Add(5)

	snapshot := queue.snapshot()
	if snapshot.activeDownload != 3 || snapshot.activePrepare != 5 {
		t.Fatalf("active jobs = download:%d prepare:%d, want download:3 prepare:5", snapshot.activeDownload, snapshot.activePrepare)
	}
	if snapshot.downloadHotQueued != 1 || snapshot.downloadPrefetchQueued != 1 || snapshot.prepareHotQueued != 0 || snapshot.preparePrefetchQueued != 1 {
		t.Fatalf("queued jobs = download:%d/%d prepare:%d/%d, want download:1/1 prepare:0/1", snapshot.downloadHotQueued, snapshot.downloadPrefetchQueued, snapshot.prepareHotQueued, snapshot.preparePrefetchQueued)
	}
}

func TestArchiveImportQueueWaitJoinsCanceledWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &archiveCatchUpRun{}
	queue := runner.startArchiveImportQueue(ctx)
	cancel()
	downloaded := &archive.Downloaded{Data: make([]byte, 1024)}
	queue.preparePrefetch <- archivePrepareJob{
		ctx:        ctx,
		downloaded: downloaded,
		done:       make(chan archiveImportQueueResult, 1),
	}

	joined := make(chan struct{})
	go func() {
		queue.wait()
		close(joined)
	}()

	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("archive import queue workers did not join after cancellation")
	}
	if queue.activeDownload.Load() != 0 || queue.activePrepare.Load() != 0 {
		t.Fatalf("archive import queue retained active workers: download=%d prepare=%d", queue.activeDownload.Load(), queue.activePrepare.Load())
	}
	if downloaded.Data != nil {
		t.Fatal("archive import queue retained a queued prepare payload after cancellation")
	}
}

func TestArchiveDownloadBackpressureGateWaitsUntilResumed(t *testing.T) {
	runner := &archiveCatchUpRun{}
	resume := runner.pauseArchiveDownloadsForCheckpointBackpressure()
	t.Cleanup(resume)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- runner.downloadGate.wait(context.Background())
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("download backpressure wait finished before resume: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	resume()

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("download backpressure wait after resume: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("download backpressure wait did not finish after resume")
	}
}

func TestArchiveImportQueueDownloadJobWaitsForCheckpointBackpressure(t *testing.T) {
	runner := &archiveCatchUpRun{archiveSession: newTestArchiveSession(t)}
	resume := runner.pauseArchiveDownloadsForCheckpointBackpressure()
	t.Cleanup(resume)

	queue := &archiveImportQueue{}
	done := make(chan archiveImportQueueResult, 1)
	go queue.runDownloadJob(runner, archiveDownloadJob{
		ctx:              context.Background(),
		masterchainSeqno: 100,
		shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		splitDepth:       0,
		priority:         archiveImportPriorityHot,
		done:             done,
	})

	started := time.After(time.Second)
	for queue.activeDownload.Load() == 0 {
		select {
		case result := <-done:
			t.Fatalf("download job finished before reaching checkpoint backpressure gate: %v", result.err)
		case <-started:
			t.Fatal("download job did not reach checkpoint backpressure gate")
		case <-time.After(time.Millisecond):
		}
	}

	select {
	case result := <-done:
		t.Fatalf("download job finished while checkpoint backpressure was active: %v", result.err)
	case <-time.After(20 * time.Millisecond):
	}

	resume()

	select {
	case result := <-done:
		if result.err == nil {
			t.Fatal("download job unexpectedly succeeded without an archive session")
		}
	case <-time.After(time.Second):
		t.Fatal("download job did not finish after checkpoint backpressure resumed")
	}
}

func TestArchiveImportQueueDownloadStarvedRequiresEmptyPrepareBacklog(t *testing.T) {
	queue := &archiveImportQueue{
		prepareHot:      make(chan archivePrepareJob, 1),
		preparePrefetch: make(chan archivePrepareJob, 1),
	}
	if !queue.archiveDownloadsStarved() {
		t.Fatal("empty archive queue should be download-starved")
	}

	queue.activePrepare.Add(1)
	if queue.archiveDownloadsStarved() {
		t.Fatal("active prepare should disable archive download hedge")
	}
	queue.activePrepare.Add(-1)

	queue.prepareHot <- archivePrepareJob{}
	if queue.archiveDownloadsStarved() {
		t.Fatal("queued prepare should disable archive download hedge")
	}
	<-queue.prepareHot

}

func TestArchiveImportQueueReleasesDownloadedDataWhenPrepareContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	queue := &archiveImportQueue{}
	downloaded := &archive.Downloaded{Data: make([]byte, 80)}

	done := make(chan archiveImportQueueResult, 1)
	queue.runPrepareJob(&archiveCatchUpRun{}, archivePrepareJob{
		ctx:        ctx,
		downloaded: downloaded,
		done:       done,
	})

	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("prepare result err = %v, want context.Canceled", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepare job did not finish")
	}
	if downloaded.Data != nil {
		t.Fatal("downloaded data was not released after canceled prepare")
	}
}

func TestDownloadAndImportShardArchivesLimitsSubmittedImports(t *testing.T) {
	plans := make([]archiveShardImportPlan, archiveShardArchiveImportInFlight+3)
	for i := range plans {
		plans[i] = archiveShardImportPlan{shard: archive.ShardID{Workchain: 0, Shard: int64(i+1) << 48}}
	}

	queue := &archiveImportQueue{
		downloadHot:      make(chan archiveDownloadJob, len(plans)),
		downloadPrefetch: make(chan archiveDownloadJob, len(plans)),
	}
	runner := &archiveCatchUpRun{archive: &ArchiveRunner{}, importCache: newArchiveImportCache()}
	done := make(chan error, 1)
	go func() {
		imports, err := runner.downloadAndImportShardArchives(context.Background(), queue, 100, plans, 0, archiveImportPriorityPrefetch)
		if err == nil && len(imports) != len(plans) {
			err = errors.New("unexpected import count")
		}
		done <- err
	}()

	jobs := make([]archiveDownloadJob, 0, len(plans))
	for i := 0; i < archiveShardArchiveImportInFlight; i++ {
		jobs = append(jobs, receiveArchiveDownloadJob(t, queue.downloadPrefetch))
	}

	select {
	case job := <-queue.downloadPrefetch:
		t.Fatalf("submitted archive %d before an in-flight import completed", job.masterchainSeqno)
	default:
	}

	jobs[0].done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}
	jobs = append(jobs, receiveArchiveDownloadJob(t, queue.downloadPrefetch))

	for _, job := range jobs[1:] {
		job.done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}
	}
	for len(jobs) < len(plans) {
		job := receiveArchiveDownloadJob(t, queue.downloadPrefetch)
		job.done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}
		jobs = append(jobs, job)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("downloadAndImportShardArchives did not finish")
	}
}

func TestDownloadAndImportShardArchivesRetriesFailedShardWithoutCancelingWindow(t *testing.T) {
	plans := []archiveShardImportPlan{
		{shard: archive.ShardID{Workchain: 0, Shard: 1 << 60}},
		{shard: archive.ShardID{Workchain: 0, Shard: 2 << 60}},
	}
	queue := &archiveImportQueue{
		downloadHot:      make(chan archiveDownloadJob, len(plans)+1),
		downloadPrefetch: make(chan archiveDownloadJob, len(plans)+1),
	}

	runner := &archiveCatchUpRun{archive: &ArchiveRunner{}, importCache: newArchiveImportCache()}
	done := make(chan error, 1)
	go func() {
		_, err := runner.downloadAndImportShardArchives(context.Background(), queue, 100, plans, 0, archiveImportPriorityPrefetch)
		done <- err
	}()

	first := receiveArchiveDownloadJob(t, queue.downloadPrefetch)
	second := receiveArchiveDownloadJob(t, queue.downloadPrefetch)

	first.done <- archiveImportQueueResult{err: errors.New("seed timeout")}
	retry := receiveArchiveDownloadJob(t, queue.downloadPrefetch)
	if retry.shard != first.shard {
		t.Fatalf("retried shard = %s, want %s", retry.shard.String(), first.shard.String())
	}

	second.done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}
	retry.done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("downloadAndImportShardArchives did not finish")
	}
}

func TestShardArchiveImportsSurviveWindowRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()
	plans := make([]archiveShardImportPlan, archiveShardArchiveImportInFlight+1)
	for i := range plans {
		plans[i].shard = archive.ShardID{Workchain: 0, Shard: int64(i*2+1) << 56}
	}
	queue := &archiveImportQueue{downloadHot: make(chan archiveDownloadJob, len(plans))}
	runner := &archiveCatchUpRun{archive: &ArchiveRunner{}, importCache: newArchiveImportCache()}
	done := make(chan error, 1)
	start := func() {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := runner.downloadAndImportShardArchives(ctx, queue, 7301281, plans, 4, archiveImportPriorityHot)
			done <- err
		}()
	}
	start()
	jobs := make([]archiveDownloadJob, 0, archiveShardArchiveImportInFlight)
	for range archiveShardArchiveImportInFlight {
		jobs = append(jobs, receiveArchiveDownloadJob(t, queue.downloadHot))
	}
	for _, job := range jobs {
		job.done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}
	}
	missing := plans[len(plans)-1].shard
	for range archiveImportPeerRetries + 1 {
		job := receiveArchiveDownloadJob(t, queue.downloadHot)
		if job.shard != missing {
			t.Fatalf("retry shard = %s, want %s", job.shard.String(), missing.String())
		}
		job.done <- archiveImportQueueResult{err: p2p.ErrNoArchivePeers}
	}
	if err := <-done; !errors.Is(err, p2p.ErrNoArchivePeers) {
		t.Fatalf("first window error = %v, want no archive peers", err)
	}

	start()
	job := receiveArchiveDownloadJob(t, queue.downloadHot)
	if job.shard != missing {
		t.Fatalf("window restart downloads completed shard %s again, want only missing %s", job.shard.String(), missing.String())
	}
	job.done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("window did not complete after the missing shard arrived")
	}
	if entries, _ := runner.importCache.stats(); entries != len(plans) {
		t.Fatalf("retained imports = %d, want %d", entries, len(plans))
	}
}

func TestShardArchiveRetryLetsSlowSiblingFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		plans := []archiveShardImportPlan{
			{shard: archive.ShardID{Workchain: 0, Shard: 1 << 60}},
			{shard: archive.ShardID{Workchain: 0, Shard: 3 << 60}},
		}
		queue := &archiveImportQueue{downloadHot: make(chan archiveDownloadJob, len(plans))}
		runner := &archiveCatchUpRun{archive: &ArchiveRunner{}, importCache: newArchiveImportCache()}
		done := make(chan error, 1)
		go func() {
			_, err := runner.downloadAndImportShardArchives(ctx, queue, 100, plans, 0, archiveImportPriorityHot)
			done <- err
		}()
		failed := receiveArchiveDownloadJob(t, queue.downloadHot)
		slow := receiveArchiveDownloadJob(t, queue.downloadHot)
		for attempt := 0; attempt <= archiveImportPeerRetries; attempt++ {
			failed.done <- archiveImportQueueResult{err: p2p.ErrNoArchivePeers}
			if attempt < archiveImportPeerRetries {
				failed = receiveArchiveDownloadJob(t, queue.downloadHot)
			}
		}
		synctest.Wait()
		if err := slow.ctx.Err(); err != nil {
			t.Fatalf("unavailable shard canceled a slow sibling before import: %v", err)
		}
		slow.done <- archiveImportQueueResult{imported: &archiveImportResult{stats: &archive.ImportStats{}}}
		if err := <-done; !errors.Is(err, p2p.ErrNoArchivePeers) {
			t.Fatalf("window error = %v, want no archive peers", err)
		}
		if entries, _ := runner.importCache.stats(); entries != 1 {
			t.Fatalf("retained slow imports = %d, want 1", entries)
		}
	})
}

func TestShardArchiveImportCancellationJoinsWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		plans := []archiveShardImportPlan{{shard: archive.ShardID{Workchain: 0, Shard: topShard}}}
		queue := &archiveImportQueue{downloadHot: make(chan archiveDownloadJob, 1)}
		runner := &archiveCatchUpRun{archive: &ArchiveRunner{}, importCache: newArchiveImportCache()}
		done := make(chan error, 1)
		go func() {
			_, err := runner.downloadAndImportShardArchives(ctx, queue, 100, plans, 0, archiveImportPriorityHot)
			done <- err
		}()
		receiveArchiveDownloadJob(t, queue.downloadHot)
		cancel()
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled import error = %v", err)
		}
		if len(runner.importCache.waiters) != 0 {
			t.Fatal("canceled import retained active cache loaders")
		}
	})
}

func TestShardArchiveImportRetriesIncompleteCachedResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	block := testArchiveImportCacheBlock(42, 0x41, 0x42)
	block.Workchain = 0
	shard := archive.ShardID{Workchain: block.Workchain, Shard: block.Shard}
	plan := archiveShardImportPlan{shard: shard, needed: []ton.BlockIDExt{block}}
	cache := newArchiveImportCache()
	key := archiveImportCacheKey{masterchainSeqno: 100, shard: shard}
	cache.entries[key] = &archiveImportResult{stats: &archive.ImportStats{Peer: "gone-peer"}}
	runner := &archiveCatchUpRun{archive: &ArchiveRunner{}, importCache: cache, archiveSession: newTestArchiveSession(t)}
	queue := &archiveImportQueue{downloadHot: make(chan archiveDownloadJob, 1)}
	done := make(chan error, 1)
	go func() {
		_, err := runner.downloadAndImportShardArchives(ctx, queue, 100, []archiveShardImportPlan{plan}, 0, archiveImportPriorityHot)
		done <- err
	}()
	job := receiveArchiveDownloadJob(t, queue.downloadHot)
	job.done <- archiveImportQueueResult{imported: &archiveImportResult{
		stats: &archive.ImportStats{},
		blocks: map[storage.BlockRootHash]PreparedBlock{
			storage.BlockKey(block): {ID: block, Meta: &storage.BlockMeta{ID: block}},
		},
	}}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.entries[key].blocks[storage.BlockKey(block)]; !ok {
		t.Fatal("incomplete cached import was not replaced with the complete archive")
	}
}

func receiveArchiveDownloadJob(t *testing.T, jobs <-chan archiveDownloadJob) archiveDownloadJob {
	t.Helper()

	select {
	case job := <-jobs:
		return job
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for archive download job")
		return archiveDownloadJob{}
	}
}

func TestArchiveWindowPipelineProgressSnapshotTracksFrontWindow(t *testing.T) {
	task := newArchiveWindowShardImportTask()
	task.setStage("shard_archives")

	progress := newArchiveWindowPipelineProgress()
	progress.setPending([]archivePendingWindow{{
		window: &shardClientArchiveWindow{startSeqno: 42},
		shards: task,
	}}, 0, "planning")

	snapshot := progress.snapshot()
	if snapshot.frontSeqno != 42 || snapshot.stage != "shard_archives" {
		t.Fatalf("pipeline front = seqno:%d stage:%s, want seqno:42 stage:shard_archives", snapshot.frontSeqno, snapshot.stage)
	}
	if snapshot.pendingWindows != 1 || snapshot.readyWindows != 0 {
		t.Fatalf("pipeline windows = pending:%d ready:%d, want pending:1 ready:0", snapshot.pendingWindows, snapshot.readyWindows)
	}

	task.finishStage("ready")
	snapshot = progress.snapshot()
	if snapshot.stage != "ready" || snapshot.readyWindows != 1 {
		t.Fatalf("ready pipeline front = stage:%s ready:%d, want stage:ready ready:1", snapshot.stage, snapshot.readyWindows)
	}
}
