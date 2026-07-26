package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func liveBatchTestRecords(count int, dataLen int) []storage.EncodedCellRecord {
	records := make([]storage.EncodedCellRecord, count)
	for i := range records {
		binary.BigEndian.PutUint64(records[i].Hash[:8], uint64(i)*0x9e3779b97f4a7c15)
		binary.BigEndian.PutUint64(records[i].Hash[24:], uint64(i))
		data := make([]byte, dataLen)
		binary.BigEndian.PutUint64(data, uint64(i))
		records[i].Data = data
	}
	return records
}

func TestLiveStateCellBatchInitialSize(t *testing.T) {
	bulkPerShard := cellShardBatchInitialSize(stateCellImportBatchTargetBytes)

	tests := []struct {
		name  string
		count int
		size  int
		want  int
	}{
		{name: "empty", count: 0, size: 0, want: liveStateCellBatchMinInitialBytes},
		{name: "single record", count: 1, size: 92, want: liveStateCellBatchMinInitialBytes},
		{name: "real state update", count: 314, size: 108, want: liveStateCellBatchMinInitialBytes},
		{
			// 4096 * (92 + 64) = 638976, spread over 8 shards.
			name:  "typical prewrite",
			count: 4096,
			size:  92,
			want:  79872,
		},
		{name: "bulk sized set", count: 1 << 20, size: 256, want: bulkPerShard},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := storage.NewStateCellRecords(liveBatchTestRecords(tt.count, tt.size))
			got := liveStateCellBatchInitialSize(records)
			if got != tt.want {
				t.Fatalf("liveStateCellBatchInitialSize(%d records x %d B) = %d, want %d", tt.count, tt.size, got, tt.want)
			}
			if got > bulkPerShard {
				t.Fatalf("initial size %d exceeds the bulk import per-shard target %d", got, bulkPerShard)
			}
			if got < liveStateCellBatchMinInitialBytes {
				t.Fatalf("initial size %d is below the floor %d", got, liveStateCellBatchMinInitialBytes)
			}
		})
	}
}

// TestLiveStateCellBatchInitialSizeCoversPayload pins the sizing invariant the
// per-shard estimate exists for: the reservation must cover the payload plus
// framing that any single shard can actually receive, so the common case never
// pays a grow-and-copy round.
func TestLiveStateCellBatchInitialSizeCoversPayload(t *testing.T) {
	for _, count := range []int{1, 111, 314, 1000, 4095} {
		records := liveBatchTestRecords(count, 92)
		perShard := liveStateCellBatchInitialSize(storage.NewStateCellRecords(records))

		var shardBytes [cellDBShardCount]int
		for _, record := range records {
			shardBytes[cellShardIndex(record.Hash)] += len(record.Data) + 32
		}
		for shard, size := range shardBytes {
			if size > perShard {
				t.Fatalf("count=%d shard=%d needs %d B but the batch reserves %d B", count, shard, size, perShard)
			}
		}
	}
}

// TestLiveStateCellRecordSaveRoundTrip stores a live-sized record set through
// both save paths with the payload-derived batch size and reads every record
// back byte for byte.
func TestLiveStateCellRecordSaveRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "single-writer", count: 314},
		{name: "sharded", count: stateCellSaveShardedMinRecords},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := Open(Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer func() {
				if err := store.Close(); err != nil {
					t.Fatalf("close store: %v", err)
				}
			}()

			generation, err := store.activeCellGenerationID()
			if err != nil {
				t.Fatalf("active cell generation: %v", err)
			}

			records := liveBatchTestRecords(tt.count, 92)
			if _, err := store.saveCellRecordSet(ctx, storage.NewStateCellRecords(records), true, generation, false); err != nil {
				t.Fatalf("save cell records: %v", err)
			}

			seenShards := map[int]struct{}{}
			for i := range records {
				raw, err := store.getCellCopyFromGeneration(ctx, generation, records[i].Hash[:])
				if err != nil {
					t.Fatalf("read cell %d back: %v", i, err)
				}
				if !bytes.Equal(raw, records[i].Data) {
					t.Fatalf("cell %d data mismatch: got=%x want=%x", i, raw, records[i].Data)
				}
				seenShards[cellShardIndex(records[i].Hash)] = struct{}{}
			}
			if len(seenShards) != cellDBShardCount {
				t.Fatalf("readback covered %d shards, want %d", len(seenShards), cellDBShardCount)
			}
		})
	}
}

// TestCellShardBatchRetainedSize pins that the pooled-buffer cap is decoupled
// from the initial size: a payload-sized live batch must survive a growth round
// instead of discarding its buffer, while bulk import keeps its own cap.
func TestCellShardBatchRetainedSize(t *testing.T) {
	bulkPerShard := cellShardBatchInitialSize(stateCellImportBatchTargetBytes)

	if got := cellShardBatchRetainedSize(bulkPerShard); got != bulkPerShard {
		t.Fatalf("bulk import retained size = %d, want it unchanged at %d", got, bulkPerShard)
	}

	live := liveStateCellBatchInitialSize(storage.NewStateCellRecords(liveBatchTestRecords(314, 108)))
	retained := cellShardBatchRetainedSize(live)
	if retained <= live {
		t.Fatalf("live retained size = %d, want more than the initial size %d", retained, live)
	}
	if retained > bulkPerShard {
		t.Fatalf("live retained size = %d, want at most the bulk per-shard target %d", retained, bulkPerShard)
	}
}

// TestLiveStateCellBatchAllocatesPayloadSizedBuffer measures the actual heap
// reserved by the batch writer for a live-sized record set. The previous
// behaviour reserved cellShardBatchInitialSize per touched shard regardless of
// the payload; this asserts the reservation now tracks the payload.
func TestLiveStateCellBatchAllocatesPayloadSizedBuffer(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	records := liveBatchTestRecords(314, 108)
	set := storage.NewStateCellRecords(records)
	initial := liveStateCellBatchInitialSize(set)

	writer := store.cells.newBatchWriter(initial)
	defer writer.close()

	touched := map[int]struct{}{}
	for i := range records {
		if err := writer.set(records[i].Hash[:], records[i].Data); err != nil {
			t.Fatalf("stage record %d: %v", i, err)
		}
		touched[cellShardIndex(records[i].Hash)] = struct{}{}
	}
	if len(touched) != cellDBShardCount {
		t.Fatalf("record set touched %d shards, want all %d", len(touched), cellDBShardCount)
	}

	reserved := 0
	for _, batch := range writer.batches {
		if batch == nil {
			continue
		}
		reserved += cap(batch.Repr())
	}

	previous := cellShardBatchInitialSize(stateCellImportBatchTargetBytes) * cellDBShardCount
	if reserved >= previous {
		t.Fatalf("reserved %d B across %d shards, want well below the previous %d B", reserved, len(touched), previous)
	}
	if reserved > 4*initial*cellDBShardCount {
		t.Fatalf("reserved %d B, want close to %d B per shard", reserved, initial)
	}

	if _, err := writer.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}
