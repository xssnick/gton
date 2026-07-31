package service

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

func TestLogArchiveWindowApplyProgress(t *testing.T) {
	var out bytes.Buffer
	svc := &Service{log: zerolog.New(&out).Level(zerolog.InfoLevel)}
	started := time.Now().Add(-10 * time.Second)
	runner := &archiveCatchUpRunner{
		service:    svc,
		current:    &storage.CurrentState{Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 100)}},
		target:     testBlockID(-1, topShard, 200),
		started:    started,
		startSeqno: 100,
	}
	masterState := &storage.BlockState{Block: testBlockID(-1, topShard, 125)}
	window := &shardClientArchiveWindow{
		startSeqno:         101,
		masterStates:       map[uint32]*storage.BlockState{101: {}, 102: {}},
		shardBlocksApplied: 7,
		shardBlocksReused:  2,
	}

	now := started.Add(6 * time.Second)
	runner.logArchiveWindowApplyProgress(now, started, masterState, window, 3, 5, 4, 1)

	got := out.String()
	for _, want := range []string{
		`"message":"archive shard-client catch-up progress"`,
		`"catchup_method":"archive_shard_client"`,
		`"stage":"apply_shard_targets"`,
		`"processed_masterchain_blocks":25`,
		`"total_masterchain_blocks":100`,
		`"progress":"25.0%"`,
		`"window_start_seqno":101`,
		`"window_master_blocks":2`,
		`"completed_shard_targets":3`,
		`"total_shard_targets":5`,
		`"window_shard_blocks_applied":11`,
		`"window_shard_blocks_reused":3`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("log entry missing %s: %s", want, got)
		}
	}
}
