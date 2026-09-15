package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
)

func TestStopCellGenerationMigrationWaitsForCanceledRunBeforeDrop(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	store := &testCellGenerationMigrationStore{
		pending: &pending,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	runCtx, run, err := state.beginCellGenerationMigrationRun(context.Background())
	if err != nil {
		t.Fatalf("begin migration run: %v", err)
	}

	stopped := make(chan error, 1)
	go func() {
		stopped <- state.StopCellGenerationMigration(context.Background())
	}()
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the migration run")
	}

	// Canceled workers may still be inside pebble calls on the pending
	// generation, so its cell DB must not be detached until the run returns.
	select {
	case err = <-stopped:
		t.Fatalf("stop returned before the canceled migration run finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	state.finishCellGenerationMigrationRun(run)
	select {
	case err = <-stopped:
		if err != nil {
			t.Fatalf("stop migration: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not return after the migration run finished")
	}
	if !store.dropped {
		t.Fatal("pending generation was not dropped")
	}
	if store.droppedGeneration != pending.ID {
		t.Fatalf("dropped generation = %d, want %d", store.droppedGeneration, pending.ID)
	}
}

func TestStopCellGenerationMigrationKeepsPendingWhenStopContextEnds(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	store := &testCellGenerationMigrationStore{
		pending: &pending,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	runCtx, run, err := state.beginCellGenerationMigrationRun(context.Background())
	if err != nil {
		t.Fatalf("begin migration run: %v", err)
	}
	defer state.finishCellGenerationMigrationRun(run)

	stopCtx, cancelStop := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() {
		stopped <- state.StopCellGenerationMigration(stopCtx)
	}()
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the migration run")
	}
	cancelStop()

	select {
	case err = <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stop migration error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not return after its context ended")
	}
	if store.dropped {
		t.Fatal("pending generation was dropped while its migration run was still running")
	}
	if err = state.checkCellGenerationMigrationStartAllowed(); err != nil {
		t.Fatalf("migration start after interrupted stop: %v", err)
	}
}
