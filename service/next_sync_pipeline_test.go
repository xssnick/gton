package service

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
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

func TestShardPrefetchParsesTargetsFromMasterBlockExtra(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)
	prepared := PreparedBlock{
		ID:        downloaded.ID,
		BlockRoot: downloaded.Block,
		BlockBOC:  downloaded.BlockBOC,
	}

	runner := &nextSyncRunner{
		ctx:     context.Background(),
		service: &Service{},
	}

	applied := make(chan nextAppliedMaster, 1)
	applied <- nextAppliedMaster{
		block:  prepared,
		master: &storage.BlockState{Block: downloaded.ID},
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
	if len(item.shardTargets) == 0 {
		t.Fatal("expected shard targets from master block extra")
	}
}
