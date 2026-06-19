package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/blocksync"
)

func TestNextSyncedBlockProcessorJobDrainsPriorityFirst(t *testing.T) {
	priorityJobs := make(chan blocksync.SyncedBlock, 1)
	jobs := make(chan blocksync.SyncedBlock, 1)
	jobs <- blocksync.SyncedBlock{CatchUp: true}
	priorityJobs <- blocksync.SyncedBlock{Priority: true}

	got, ok := nextSyncedBlockProcessorJob(context.Background(), priorityJobs, jobs)
	if !ok {
		t.Fatal("expected synced block job")
	}
	if !got.Priority {
		t.Fatalf("got regular job before priority job: %+v", got)
	}
}

func TestNextSyncedBlockProcessorJobUsesRegularAfterPriorityClosed(t *testing.T) {
	priorityJobs := make(chan blocksync.SyncedBlock)
	close(priorityJobs)
	jobs := make(chan blocksync.SyncedBlock, 1)
	jobs <- blocksync.SyncedBlock{CatchUp: true}

	got, ok := nextSyncedBlockProcessorJob(context.Background(), priorityJobs, jobs)
	if !ok {
		t.Fatal("expected regular synced block job")
	}
	if !got.CatchUp {
		t.Fatalf("got unexpected job after priority close: %+v", got)
	}
}
