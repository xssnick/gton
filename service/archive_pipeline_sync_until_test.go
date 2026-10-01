package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestApplyArchiveMasterBlocksMarksSyncUntilOnWindow(t *testing.T) {
	const syncUntil = uint32(1_000)
	start := &storage.BlockState{
		Block: ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10},
	}
	window := &shardClientArchiveWindow{
		startSeqno: 11,
		masterSequence: []PreparedBlock{{
			ID:   ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 11},
			Meta: &storage.BlockMeta{GenUTime: syncUntil + 1},
		}},
		masterStates:  map[uint32]*storage.BlockState{},
		archiveBlocks: map[storage.BlockRootHash]PreparedBlock{},
		stateCells:    newTestStateCellWindowCache(nil),
	}
	runner := &archiveCatchUpRun{
		archive: &ArchiveRunner{syncUntil: syncUntil},
	}

	last, err := runner.applyArchiveMasterBlocks(context.Background(), start, window)
	if err != nil {
		t.Fatalf("apply archive master blocks: %v", err)
	}
	if last != start {
		t.Fatal("sync_until window advanced past the cutoff")
	}
	if !window.syncUntilReached {
		t.Fatal("sync_until cutoff was not recorded on its archive window")
	}
	if len(window.masterStates) != 0 {
		t.Fatalf("sync_until window applied %d blocks past the cutoff", len(window.masterStates))
	}
}

func TestCompletedArchiveWindowShardImportTaskIsReady(t *testing.T) {
	task := newCompletedArchiveWindowShardImportTask()
	if !task.ready() {
		t.Fatal("completed shard import task is not ready")
	}
	if err := task.wait(context.Background()); err != nil {
		t.Fatalf("wait completed shard import task: %v", err)
	}
	if !task.finishedSnapshot() || task.stageSnapshot() != "ready" {
		t.Fatalf("completed shard import task state = %q finished=%v", task.stageSnapshot(), task.finishedSnapshot())
	}

	runner := &archiveCatchUpRun{archive: &ArchiveRunner{prefetchWindows: 2}}
	pending := []archivePendingWindow{{
		window: &shardClientArchiveWindow{syncUntilReached: true},
		shards: task,
	}}
	if !runner.shouldEmitReadyArchiveWindow(pending) {
		t.Fatal("zero-state sync_until window cannot be emitted after older pending windows drain")
	}
}
