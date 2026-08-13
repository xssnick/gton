package gton

import (
	"testing"

	"github.com/xssnick/gton/service"
)

func TestRunNodeRejectsNegativeArchivePrefetchWindows(t *testing.T) {
	err := RunNode(t.Context(), NodeOptions{ArchivePrefetchWindows: -1})
	if err == nil {
		t.Fatal("expected negative archive prefetch windows error")
	}
	if err.Error() != "archive prefetch windows cannot be negative: -1" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyNodeOptionDefaultsUsesServiceDefaults(t *testing.T) {
	opts := applyNodeOptionDefaults(NodeOptions{})

	if opts.NextCheckpointBlocks != service.DefaultNextBlockCheckpointBlocks {
		t.Fatalf("next checkpoint blocks = %d, want %d", opts.NextCheckpointBlocks, service.DefaultNextBlockCheckpointBlocks)
	}
	if opts.ArchiveCheckpointBlocks != service.DefaultArchiveCatchUpCheckpointBlocks {
		t.Fatalf("archive checkpoint blocks = %d, want %d", opts.ArchiveCheckpointBlocks, service.DefaultArchiveCatchUpCheckpointBlocks)
	}
	if opts.CheckpointBytes != service.DefaultCheckpointBytes {
		t.Fatalf("checkpoint bytes = %d, want %d", opts.CheckpointBytes, service.DefaultCheckpointBytes)
	}
	if opts.SyncBackpressureWindows != service.DefaultSyncBackpressureWindows {
		t.Fatalf("sync backpressure windows = %d, want %d", opts.SyncBackpressureWindows, service.DefaultSyncBackpressureWindows)
	}
	if opts.ArchiveCheckpointPeriod != service.DefaultArchiveCatchUpCheckpointPeriod {
		t.Fatalf("archive checkpoint period = %s, want %s", opts.ArchiveCheckpointPeriod, service.DefaultArchiveCatchUpCheckpointPeriod)
	}
	if opts.ArchivePrefetchWindows != service.DefaultArchiveCatchUpPrefetchWindows {
		t.Fatalf("archive prefetch windows = %d, want %d", opts.ArchivePrefetchWindows, service.DefaultArchiveCatchUpPrefetchWindows)
	}
	if opts.Storage.PersistentStateKeepRecent != service.DefaultPersistentStateKeepRecent {
		t.Fatalf("persistent states kept = %d, want %d", opts.Storage.PersistentStateKeepRecent, service.DefaultPersistentStateKeepRecent)
	}
}
