package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/p2p"
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

func TestIsHotMasterchainSyncedBlockRequiresPriorityLiveMaster(t *testing.T) {
	master := testMasterBlockID(10)
	if !isHotMasterchainSyncedBlock(blocksync.SyncedBlock{
		Priority:   true,
		Downloaded: p2p.DownloadedBlock{ID: master},
	}) {
		t.Fatal("priority live master block should use hot queue")
	}

	if isHotMasterchainSyncedBlock(blocksync.SyncedBlock{
		Priority:   true,
		CatchUp:    true,
		Downloaded: p2p.DownloadedBlock{ID: master},
	}) {
		t.Fatal("catch-up master block should not use hot queue")
	}

	base := master
	base.Workchain = 0
	if isHotMasterchainSyncedBlock(blocksync.SyncedBlock{
		Priority:   true,
		Downloaded: p2p.DownloadedBlock{ID: base},
	}) {
		t.Fatal("basechain block should not use master hot queue")
	}
}
