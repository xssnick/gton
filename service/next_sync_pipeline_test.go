package service

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestDownloadElapsedExcludingInlinePrepare(t *testing.T) {
	tests := []struct {
		name    string
		elapsed time.Duration
		prepare time.Duration
		want    time.Duration
	}{
		{
			name:    "no inline prepare",
			elapsed: 20 * time.Millisecond,
			want:    20 * time.Millisecond,
		},
		{
			name:    "subtract inline prepare",
			elapsed: 20 * time.Millisecond,
			prepare: 7 * time.Millisecond,
			want:    13 * time.Millisecond,
		},
		{
			name:    "clamp negative duration",
			elapsed: 5 * time.Millisecond,
			prepare: 7 * time.Millisecond,
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := downloadElapsedExcludingInlinePrepare(tt.elapsed, tt.prepare)
			if got != tt.want {
				t.Fatalf("downloadElapsedExcludingInlinePrepare(%s, %s) = %s, want %s", tt.elapsed, tt.prepare, got, tt.want)
			}
		})
	}
}

func TestShardPrefetchForwardsParsedTargets(t *testing.T) {
	runner := &nextSyncRunner{
		ctx:     context.Background(),
		service: &Service{},
	}
	target := testBlockID(0, topShard, 100)
	targets := []ton.BlockIDExt{target}

	applied := make(chan nextAppliedMaster, 1)
	applied <- nextAppliedMaster{
		master:               &storage.BlockState{Block: testBlockID(-1, topShard, 99)},
		shardTargets:         targets,
		shardTargetsParsed:   true,
		shardTargetParse:     time.Millisecond,
		shardPrefetchTargets: len(targets),
	}
	close(applied)

	out := runner.startShardPrefetch(applied)
	item, ok := <-out
	if !ok {
		t.Fatal("startShardPrefetch closed without forwarding applied master")
	}
	if item.err != nil {
		t.Fatalf("startShardPrefetch returned error: %v", item.err)
	}
	if !item.shardTargetsParsed {
		t.Fatal("expected startShardPrefetch to preserve parsed shard targets flag")
	}
	if item.shardTargetParse != time.Millisecond {
		t.Fatalf("shard target parse duration = %s, want %s", item.shardTargetParse, time.Millisecond)
	}
	if item.shardPrefetchTargets != len(targets) {
		t.Fatalf("shard prefetch target count = %d, want %d", item.shardPrefetchTargets, len(targets))
	}
	if len(item.shardTargets) != len(targets) || !item.shardTargets[0].Equals(&target) {
		t.Fatalf("shard targets = %v, want %v", item.shardTargets, targets)
	}
}
