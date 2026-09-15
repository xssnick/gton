package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

type flushBarrierFS struct {
	vfs.FS
	active  atomic.Bool
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (f *flushBarrierFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if f.active.Load() && strings.HasSuffix(name, ".sst") {
		f.once.Do(func() {
			close(f.started)
			<-f.release
		})
	}
	return f.FS.Create(name, category)
}

type pebbleCompactionAllowedTestDB struct {
	pebbleCompactionTestDB
	allowed int
}

func (d *pebbleCompactionAllowedTestDB) GetAllowedWithoutPermission() int {
	return d.allowed
}

func TestDroppedPendingCellGenerationKeepsShardsOpenWhileReferenced(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	origin := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x20}, 32),
		FileHash:  bytes.Repeat([]byte{0x21}, 32),
	}
	generation, err := store.BeginCellGeneration(ctx, origin)
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	heldCells, err := store.acquireCellStore(ctx, generation)
	if err != nil {
		t.Fatalf("hold pending generation ref: %v", err)
	}

	hash := bytes.Repeat([]byte{0x42}, 32)
	writer := heldCells.newBatchWriter(1 << 20)
	if err = writer.set(hash, []byte{0x01}); err != nil {
		t.Fatalf("set cell: %v", err)
	}
	if _, err = writer.flush(); err != nil {
		t.Fatalf("commit cell batch: %v", err)
	}
	writer.close()

	if err = store.DropPendingCellGeneration(ctx, generation); err != nil {
		t.Fatalf("drop pending generation: %v", err)
	}
	time.Sleep(2 * cellStoreDrainWarnAfter)

	// The holder acquired the generation before the drop, like a migration
	// worker inside SaveEncoded, so its shards must stay open past the grace.
	exists, err := heldCells.has(hash)
	if err != nil {
		t.Fatalf("read held generation after close grace: %v", err)
	}
	if !exists {
		t.Fatal("cell committed before drop is missing in held generation")
	}
	if err = heldCells.flush(); err != nil {
		t.Fatalf("flush held generation after close grace: %v", err)
	}
	for shard := 0; shard < cellDBShardCount; shard++ {
		if _, err = os.Stat(cellGenerationShardDir(store.dir, generation, shard)); err != nil {
			t.Fatalf("stat held generation shard %d dir: %v", shard, err)
		}
	}

	heldCells.release()

	deadline := time.Now().Add(2 * time.Second)
	for {
		removed := true
		for shard := 0; shard < cellDBShardCount; shard++ {
			if _, err = os.Stat(cellGenerationShardDir(store.dir, generation, shard)); err == nil {
				removed = false
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stat dropped generation shard dir: %v", err)
			}
		}
		if removed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dropped pending generation dirs were not removed after the last ref was released")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCellStoreFlushWaitsForInFlightFlush(t *testing.T) {
	fs := &flushBarrierFS{
		FS:      vfs.Default,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	cells, err := openCellStore(t.TempDir(), 1, fs, 1<<20, 1<<20, 2, 0, false, zerolog.Nop())
	if err != nil {
		t.Fatalf("open cell store: %v", err)
	}
	defer func() { _ = cells.close() }()

	hash := make([]byte, 32)
	hash[31] = 0x01
	writer := cells.newBatchWriter(1 << 20)
	if err = writer.set(hash, []byte{0x01}); err != nil {
		t.Fatalf("set cell: %v", err)
	}
	if _, err = writer.flush(); err != nil {
		t.Fatalf("commit cell batch: %v", err)
	}
	writer.close()

	fs.active.Store(true)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cells.flush()
	}()
	select {
	case <-fs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first flush did not start writing the memtable")
	}

	// The second caller committed its cell before the first flush took the
	// dirty snapshot, so it may return only after that memtable is flushed.
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- cells.flush()
	}()
	select {
	case err = <-secondDone:
		close(fs.release)
		t.Fatalf("second flush returned while the first flush was still writing its cells: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(fs.release)
	for name, done := range map[string]chan error{"first": firstDone, "second": secondDone} {
		select {
		case err = <-done:
			if err != nil {
				t.Fatalf("%s flush: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s flush did not finish after releasing the memtable write", name)
		}
	}
}

func TestPebbleCompactionSchedulerTryScheduleRespectsAllowedWithoutPermission(t *testing.T) {
	controller := newPebbleCompactionController(4)
	scheduler := controller.newScheduler()
	db := &pebbleCompactionAllowedTestDB{allowed: 1}
	scheduler.Register(1, db)
	<-controller.grantPokes

	controller.mu.Lock()
	controller.paused = false
	controller.mu.Unlock()

	granted, handle := scheduler.TrySchedule()
	if !granted {
		t.Fatal("first compaction was not granted")
	}
	if granted, _ = scheduler.TrySchedule(); granted {
		t.Fatal("second compaction was granted above the DB allowed-without-permission limit")
	}
	handle.Done()

	if granted, handle = scheduler.TrySchedule(); !granted {
		t.Fatal("compaction was not granted after the running one finished")
	}
	handle.Done()
	scheduler.Unregister()
}

func TestPebbleCompactionControllerGrantRespectsAllowedWithoutPermission(t *testing.T) {
	controller := newPebbleCompactionController(8)
	scheduler := controller.newScheduler()

	var handles []pebble.CompactionGrantHandle
	db := &pebbleCompactionAllowedTestDB{
		pebbleCompactionTestDB: pebbleCompactionTestDB{
			waiting: func() (bool, pebble.WaitingCompaction) {
				return true, pebble.WaitingCompaction{}
			},
			schedule: func(handle pebble.CompactionGrantHandle) bool {
				handles = append(handles, handle)
				return true
			},
		},
		allowed: 2,
	}
	scheduler.Register(1, db)
	<-controller.grantPokes

	controller.mu.Lock()
	controller.paused = false
	controller.mu.Unlock()
	controller.tryGrant()

	if len(handles) != db.allowed {
		t.Fatalf("granted compactions = %d, want DB allowed-without-permission limit %d", len(handles), db.allowed)
	}
	for _, handle := range handles {
		handle.Done()
	}
	scheduler.Unregister()
}
