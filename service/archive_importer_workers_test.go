package service

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
)

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

func TestArchiveDownloadWorkerBudgetReservesHotWorkers(t *testing.T) {
	tests := []struct {
		workers int
		hotOnly int
		shared  int
	}{
		{workers: 1, hotOnly: 0, shared: 1},
		{workers: 2, hotOnly: 1, shared: 1},
		{workers: archiveDownloadWorkerMin, hotOnly: 4, shared: archiveDownloadWorkerMin - 4},
		{workers: archiveDownloadWorkerMax, hotOnly: archiveDownloadWorkerMax / 4, shared: archiveDownloadWorkerMax - archiveDownloadWorkerMax/4},
	}

	for _, tt := range tests {
		got := archiveDownloadWorkerBudget(tt.workers)
		if got.hotOnly != tt.hotOnly || got.shared != tt.shared {
			t.Fatalf("worker budget for %d = hotOnly:%d shared:%d want hotOnly:%d shared:%d", tt.workers, got.hotOnly, got.shared, tt.hotOnly, tt.shared)
		}
		if got.hotOnly+got.shared != tt.workers {
			t.Fatalf("worker budget for %d does not preserve worker count: %#v", tt.workers, got)
		}
	}
}

func TestArchivePrepareWorkerBudgetReservesSmallHotPool(t *testing.T) {
	tests := []struct {
		workers int
		hotOnly int
		shared  int
	}{
		{workers: 1, hotOnly: 0, shared: 1},
		{workers: 2, hotOnly: 1, shared: 1},
		{workers: 8, hotOnly: 1, shared: 7},
		{workers: 16, hotOnly: archivePrepareHotWorkerMax, shared: 14},
		{workers: 32, hotOnly: archivePrepareHotWorkerMax, shared: 30},
	}

	for _, tt := range tests {
		got := archivePrepareWorkerBudget(tt.workers)
		if got.hotOnly != tt.hotOnly || got.shared != tt.shared {
			t.Fatalf("prepare budget for %d = hotOnly:%d shared:%d want hotOnly:%d shared:%d", tt.workers, got.hotOnly, got.shared, tt.hotOnly, tt.shared)
		}
		if got.hotOnly+got.shared != tt.workers {
			t.Fatalf("prepare budget for %d does not preserve worker count: %#v", tt.workers, got)
		}
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
		downloadedBytes:  newArchiveDownloadByteGate(1024),
	}
	queue.downloadHot <- archiveDownloadJob{}
	queue.downloadPrefetch <- archiveDownloadJob{}
	queue.preparePrefetch <- archivePrepareJob{}
	queue.activeDownload.Add(3)
	queue.activePrepare.Add(5)
	queue.downloadedBytes.add(256)

	snapshot := queue.snapshot()
	if snapshot.activeDownload != 3 || snapshot.activePrepare != 5 {
		t.Fatalf("active jobs = download:%d prepare:%d, want download:3 prepare:5", snapshot.activeDownload, snapshot.activePrepare)
	}
	if snapshot.downloadHotQueued != 1 || snapshot.downloadPrefetchQueued != 1 || snapshot.prepareHotQueued != 0 || snapshot.preparePrefetchQueued != 1 {
		t.Fatalf("queued jobs = download:%d/%d prepare:%d/%d, want download:1/1 prepare:0/1", snapshot.downloadHotQueued, snapshot.downloadPrefetchQueued, snapshot.prepareHotQueued, snapshot.preparePrefetchQueued)
	}
	if snapshot.downloadedBytes != 256 || snapshot.downloadedBytesLimit != 1024 {
		t.Fatalf("downloaded bytes = %d/%d, want 256/1024", snapshot.downloadedBytes, snapshot.downloadedBytesLimit)
	}
}

func TestArchiveDownloadByteGateWaitsUntilDownloadedBytesDrop(t *testing.T) {
	gate := newArchiveDownloadByteGate(100)
	gate.add(100)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- gate.wait(context.Background())
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("gate wait finished before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	gate.release(100)

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("gate wait after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gate wait did not finish after release")
	}
}

func TestArchiveDownloadBackpressureGateWaitsUntilResumed(t *testing.T) {
	runner := &archiveCatchUpRunner{}
	resume := runner.pauseArchiveDownloadsForCheckpointBackpressure()
	t.Cleanup(resume)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- runner.waitArchiveDownloadBackpressure(context.Background())
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
	runner := &archiveCatchUpRunner{}
	resume := runner.pauseArchiveDownloadsForCheckpointBackpressure()
	t.Cleanup(resume)

	queue := &archiveImportQueue{
		downloadedBytes: newArchiveDownloadByteGate(100),
	}
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
		downloadedBytes: newArchiveDownloadByteGate(100),
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

	queue.downloadedBytes.add(1)
	if queue.archiveDownloadsStarved() {
		t.Fatal("downloaded bytes waiting for prepare should disable archive download hedge")
	}
}

func TestArchiveImportQueueReleasesDownloadedBytesWhenPrepareContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	queue := &archiveImportQueue{
		downloadedBytes: newArchiveDownloadByteGate(100),
	}
	downloaded := &archive.Downloaded{Data: make([]byte, 80)}
	queue.downloadedBytes.add(80)

	done := make(chan archiveImportQueueResult, 1)
	queue.runPrepareJob(&archiveCatchUpRunner{}, archivePrepareJob{
		ctx:        ctx,
		downloaded: downloaded,
		bytes:      80,
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
	if got := queue.downloadedBytes.size(); got != 0 {
		t.Fatalf("downloaded bytes after canceled prepare = %d, want 0", got)
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
	runner := &archiveCatchUpRunner{}
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

	jobs[0].done <- archiveImportQueueResult{imported: &archiveImportResult{}}
	jobs = append(jobs, receiveArchiveDownloadJob(t, queue.downloadPrefetch))

	for _, job := range jobs[1:] {
		job.done <- archiveImportQueueResult{imported: &archiveImportResult{}}
	}
	for len(jobs) < len(plans) {
		job := receiveArchiveDownloadJob(t, queue.downloadPrefetch)
		job.done <- archiveImportQueueResult{imported: &archiveImportResult{}}
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

	runner := &archiveCatchUpRunner{}
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

	second.done <- archiveImportQueueResult{imported: &archiveImportResult{}}
	retry.done <- archiveImportQueueResult{imported: &archiveImportResult{}}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("downloadAndImportShardArchives did not finish")
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
