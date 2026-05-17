package service

import (
	"context"
	"runtime"
	"testing"
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
	if got := archiveDownloadWorkers(); got != 48 {
		t.Fatalf("download workers at GOMAXPROCS=12 = %d want 48", got)
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
		{workers: 48, hotOnly: archiveDownloadHotWorkerMax, shared: 40},
		{workers: archiveDownloadWorkerMax, hotOnly: archiveDownloadHotWorkerMax, shared: archiveDownloadWorkerMax - archiveDownloadHotWorkerMax},
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
