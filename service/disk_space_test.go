package service

import (
	"context"
	"testing"
	"time"
)

func TestWaitSyncDiskSpaceReturnsWhenEnoughSpace(t *testing.T) {
	calls := 0
	svc := &Service{
		syncDiskSpacePath:    "/db",
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
	svc := &Service{
		maintenanceWake:      make(chan struct{}, 1),
		syncDiskSpacePath:    "/db",
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
	case <-svc.maintenanceWake:
	default:
		t.Fatal("maintenance worker was not woken on low disk space")
	}
}

func TestWaitSyncDiskSpaceReturnsContextErrorWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	svc := &Service{
		syncDiskSpacePath:    "/db",
		minSyncDiskFreeBytes: 100,
	}
	probe := func(path string) (syncDiskSpaceStatus, error) {
		return syncDiskSpaceStatus{Path: path, AvailableBytes: 99}, nil
	}

	if err := svc.waitSyncDiskSpace(ctx, "next_block", probe, time.Hour); err != context.Canceled {
		t.Fatalf("wait disk space error = %v, want context canceled", err)
	}
}
