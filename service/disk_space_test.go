package service

import (
	"context"
	"testing"
	"time"
)

func TestWaitSyncDiskSpaceReturnsWhenEnoughSpace(t *testing.T) {
	calls := 0
	svc := &SyncCoordinator{
		syncDiskSpacePath:    "/db",
		maintenance:          &MaintenanceRunner{maintenanceWake: make(chan struct{}, 1)},
		minSyncDiskFreeBytes: 100,
	}
	probe := func(path string) (syncDiskSpaceStatus, error) {
		calls++
		return syncDiskSpaceStatus{Path: path, AvailableBytes: 100}, nil
	}

	if err := svc.waitSyncDiskSpace(context.Background(), "next_block", probe, time.Millisecond); err != nil {
		t.Fatalf("wait disk space: %v", err)
	}
	if calls != 1 {
		t.Fatalf("probe calls = %d, want 1", calls)
	}
}

func TestWaitSyncDiskSpaceRetriesAndWakesMaintenance(t *testing.T) {
	calls := 0
	svc := &SyncCoordinator{
		syncDiskSpacePath:    "/db",
		maintenance:          &MaintenanceRunner{maintenanceWake: make(chan struct{}, 1)},
		minSyncDiskFreeBytes: 100,
	}
	probe := func(path string) (syncDiskSpaceStatus, error) {
		calls++
		if calls == 1 {
			return syncDiskSpaceStatus{Path: path, AvailableBytes: 99}, nil
		}
		return syncDiskSpaceStatus{Path: path, AvailableBytes: 100}, nil
	}

	if err := svc.waitSyncDiskSpace(context.Background(), "archive_catchup", probe, time.Millisecond); err != nil {
		t.Fatalf("wait disk space: %v", err)
	}
	if calls != 2 {
		t.Fatalf("probe calls = %d, want 2", calls)
	}
	select {
	case <-svc.maintenance.maintenanceWake:
	default:
		t.Fatal("maintenance worker was not woken on low disk space")
	}
}

func TestWaitSyncDiskSpaceReturnsContextErrorWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	svc := &SyncCoordinator{
		syncDiskSpacePath:    "/db",
		maintenance:          &MaintenanceRunner{maintenanceWake: make(chan struct{}, 1)},
		minSyncDiskFreeBytes: 100,
	}
	probe := func(path string) (syncDiskSpaceStatus, error) {
		return syncDiskSpaceStatus{Path: path, AvailableBytes: 99}, nil
	}

	if err := svc.waitSyncDiskSpace(ctx, "next_block", probe, time.Hour); err != context.Canceled {
		t.Fatalf("wait disk space error = %v, want context canceled", err)
	}
}
