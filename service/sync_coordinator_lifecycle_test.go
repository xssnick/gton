package service

import (
	"context"
	"errors"
	"testing"
)

func TestSyncCoordinatorStartRequiresArchiveRunner(t *testing.T) {
	coordinator := &SyncCoordinator{}

	if err := coordinator.Start(context.Background()); !errors.Is(err, errSyncCoordinatorArchiveNotBound) {
		t.Fatalf("start error = %v, want archive not bound", err)
	}
}

func TestSyncCoordinatorArchiveBindingIsOneShot(t *testing.T) {
	coordinator := &SyncCoordinator{}
	archive := &ArchiveRunner{}

	if err := coordinator.BindArchiveRunner(archive); err != nil {
		t.Fatalf("bind archive runner: %v", err)
	}
	if coordinator.archive != archive {
		t.Fatal("coordinator did not retain the bound archive runner")
	}
	if err := coordinator.BindArchiveRunner(&ArchiveRunner{}); !errors.Is(err, errSyncCoordinatorArchiveAlreadyBound) {
		t.Fatalf("second bind error = %v, want already bound", err)
	}
}

func TestSyncCoordinatorArchiveBindingRejectsNilAndLateBind(t *testing.T) {
	coordinator := &SyncCoordinator{}
	if err := coordinator.BindArchiveRunner(nil); !errors.Is(err, errSyncCoordinatorArchiveNotBound) {
		t.Fatalf("nil bind error = %v, want archive not bound", err)
	}

	coordinator.started = true
	if err := coordinator.BindArchiveRunner(&ArchiveRunner{}); !errors.Is(err, errSyncCoordinatorStarted) {
		t.Fatalf("late bind error = %v, want coordinator started", err)
	}
}
