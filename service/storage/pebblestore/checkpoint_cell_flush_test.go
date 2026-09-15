package pebblestore

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func activeCellDBFlushCount(t *testing.T, store *Store) int64 {
	t.Helper()

	cells, err := store.acquireActiveCellStore(context.Background())
	if err != nil {
		t.Fatalf("acquire active cell store: %v", err)
	}
	defer cells.release()

	var flushes int64
	for _, shard := range cells.shards {
		flushes += shard.db.Metrics().Flush.Count
	}
	return flushes
}

// TestCheckpointCellFlushRunsOutsideArtifactPublish pins that a checkpoint
// carrying its cells inline flushes celldb before it takes artifactPublishMu,
// so a stalled flush cannot park the artifact prewriter, while the hot metadata
// commit still waits for the lock.
func TestCheckpointCellFlushRunsOutsideArtifactPublish(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     1,
	}
	child := cell.BeginCell().MustStoreUInt(0x33, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x44, 8).MustStoreRef(child).EndCell()
	state := blockStateWithRoot(block, root)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      storage.BlockStateWithoutCells(state),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	records := storage.NewStateCellRecords([]storage.EncodedCellRecord{
		mustEncodedCellRecord(t, root),
		mustEncodedCellRecord(t, child),
	})
	flushesBefore := activeCellDBFlushCount(t, store)

	type saveResult struct {
		timing storage.StateCheckpointTiming
		err    error
	}
	store.artifactPublishMu.Lock()
	done := make(chan saveResult, 1)
	go func() {
		timing, saveErr := store.SaveStateCheckpointEntries(ctx, checkpointEntries(state), records, current)
		done <- saveResult{timing: timing, err: saveErr}
	}()

	flushed := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if activeCellDBFlushCount(t, store) > flushesBefore {
			flushed = true
			break
		}
	}
	returnedUnderLock := false
	select {
	case result := <-done:
		returnedUnderLock = true
		done <- result
	default:
	}
	store.artifactPublishMu.Unlock()
	result := <-done

	if !flushed {
		t.Fatal("checkpoint cell flush waited for artifactPublishMu")
	}
	if returnedUnderLock {
		t.Fatalf("checkpoint returned while artifactPublishMu was held: %v", result.err)
	}
	if result.err != nil {
		t.Fatalf("save checkpoint with prepared cells: %v", result.err)
	}
	if result.timing.CellsFlush <= 0 {
		t.Fatalf("checkpoint cells flush timing = %v, want the flush measured", result.timing.CellsFlush)
	}

	loaded, err := store.LoadStateCellTree(ctx, state.Block, state.StateRootHash)
	if err != nil {
		t.Fatalf("load saved state: %v", err)
	}
	if loaded.HashKey() != root.HashKey() {
		t.Fatalf("loaded root hash = %x, want %x", loaded.HashKey(), root.HashKey())
	}
}
