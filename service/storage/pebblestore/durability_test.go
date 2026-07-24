package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

type testPebbleFailure struct {
	toggle  *errorfs.Toggle
	counter *errorfs.Counter
	mu      sync.Mutex
	before  func()
}

func (f *testPebbleFailure) setBeforeFailure(fn func()) {
	f.mu.Lock()
	f.before = fn
	f.mu.Unlock()
}

func (f *testPebbleFailure) runBeforeFailure() {
	f.mu.Lock()
	fn := f.before
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
}

type testPanicPebbleLogger struct{}

func (testPanicPebbleLogger) Infof(string, ...interface{})  {}
func (testPanicPebbleLogger) Errorf(string, ...interface{}) {}
func (testPanicPebbleLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func recoverTestPanic(fn func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	fn()
	return nil
}

func newTestPebbleFailureStore(t *testing.T, dir string) (*Store, *testPebbleFailure) {
	t.Helper()

	failure := &testPebbleFailure{}
	counter := &errorfs.Counter{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		fail := op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData || op.Kind == errorfs.OpFileSyncTo
		if fail {
			failure.runBeforeFailure()
			return errorfs.ErrInjected
		}
		return nil
	})}
	toggle := &errorfs.Toggle{Injector: counter}
	failure.toggle = toggle
	failure.counter = counter
	fs := errorfs.Wrap(vfs.NewMem(), toggle)
	db, err := pebble.Open("metadb", &pebble.Options{
		FS:     fs,
		Logger: testPanicPebbleLogger{},
	})
	if err != nil {
		t.Fatalf("open test metadb: %v", err)
	}
	t.Cleanup(func() {
		toggle.Off()
		_ = db.Close()
	})

	return &Store{
		log:             zerolog.Nop(),
		hot:             db,
		cellGenerations: make(map[uint64]*cellStore),
		dir:             dir,
		hotDrained:      make(chan struct{}),
	}, failure
}

func TestDropPendingCellGenerationRequiresDurableMetadataRemoval(t *testing.T) {
	const (
		activeGeneration  = uint64(1)
		pendingGeneration = uint64(2)
	)

	store, failure := newTestPebbleFailureStore(t, t.TempDir())
	origin := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x20}, 32),
		FileHash:  bytes.Repeat([]byte{0x21}, 32),
	}
	store.activeCellGeneration = activeGeneration
	store.nextCellGeneration = pendingGeneration + 1
	store.pendingCellMigration = &cellGenerationPendingMigration{
		generation: pendingGeneration,
		origin:     origin,
	}
	store.cellGenerations[activeGeneration] = nil
	store.cellGenerations[pendingGeneration] = nil

	manifest := store.manifestLocked()
	if err := store.hot.Set(hotKeyCellGenerationManifest(), encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		t.Fatalf("save pending generation manifest: %v", err)
	}
	if err := store.hot.Set(hotKeyCellGenerationCurrent(pendingGeneration), []byte("progress"), pebble.Sync); err != nil {
		t.Fatalf("save pending generation progress: %v", err)
	}

	var pendingAtSync atomic.Bool
	failure.setBeforeFailure(func() {
		store.mu.RLock()
		pendingAtSync.Store(store.pendingCellMigration != nil && store.pendingCellMigration.generation == pendingGeneration)
		store.mu.RUnlock()
	})
	failure.toggle.On()
	panicked := recoverTestPanic(func() {
		_ = store.DropPendingCellGeneration(context.Background(), pendingGeneration)
	})
	failure.toggle.Off()
	if panicked == nil {
		t.Fatal("drop pending generation did not fail at the injected sync boundary")
	}
	if failure.counter.Load() == 0 {
		t.Fatal("drop pending generation did not attempt to sync metadata")
	}
	if !pendingAtSync.Load() {
		t.Fatal("pending generation detached before metadata sync")
	}
	if store.pendingCellMigration == nil || store.pendingCellMigration.generation != pendingGeneration {
		t.Fatal("pending generation detached after metadata sync failure")
	}
	if _, ok := store.cellGenerations[pendingGeneration]; !ok {
		t.Fatal("pending generation cell store detached after metadata sync failure")
	}
}

func TestDroppedCellGenerationDoesNotReopenFromStaleShardDirs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	origin := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x20}, 32),
		FileHash:  bytes.Repeat([]byte{0x21}, 32),
	}
	generation, err := store.BeginCellGeneration(context.Background(), origin)
	if err != nil {
		t.Fatalf("begin pending generation: %v", err)
	}
	if err = store.DropPendingCellGeneration(context.Background(), generation); err != nil {
		t.Fatalf("drop pending generation: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allRemoved := true
		for shard := 0; shard < cellDBShardCount; shard++ {
			_, statErr := os.Stat(cellGenerationShardDir(dir, generation, shard))
			if statErr == nil {
				allRemoved = false
				break
			}
			if !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("stat dropped generation shard %d: %v", shard, statErr)
			}
		}
		if allRemoved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dropped generation shard directories were not removed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	staleDir := cellGenerationShardDir(dir, generation, 0)
	if err = os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatalf("recreate stale generation shard dir: %v", err)
	}
	if err = os.WriteFile(filepath.Join(staleDir, "stale"), []byte("partial cleanup"), 0o644); err != nil {
		t.Fatalf("write stale generation marker: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store with stale generation dir: %v", err)
	}
	defer func() { _ = store.Close() }()

	if _, err = store.PendingCellGenerationMigration(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("pending generation after reopen = %v, want ErrNotFound", err)
	}
	if _, err = os.Stat(staleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale generation dir after recovery = %v, want not exist", err)
	}
	nextGeneration, err := store.BeginCellGeneration(context.Background(), origin)
	if err != nil {
		t.Fatalf("begin generation after reopen: %v", err)
	}
	if nextGeneration <= generation {
		t.Fatalf("generation after stale dir = %d, want above dropped %d", nextGeneration, generation)
	}
}

func TestDeletePersistentStateFileDoesNotSyncMetadata(t *testing.T) {
	dir := t.TempDir()
	store, failure := newTestPebbleFailureStore(t, dir)
	observer := &testArtifactMetricsObserver{}
	store.SetArtifactMetricsObserver(observer)

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     79,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     80,
		RootHash:  bytes.Repeat([]byte{0x13}, 32),
		FileHash:  bytes.Repeat([]byte{0x14}, 32),
	}
	effectiveShard := block.Shard
	data := []byte{1, 2, 3, 4}

	path, err := store.persistentStateArtifactPath(block, master, effectiveShard)
	if err != nil {
		t.Fatalf("persistent state path: %v", err)
	}
	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create persistent state dir: %v", err)
	}
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write persistent state file: %v", err)
	}
	file := &storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   effectiveShard,
		Ref:              &storage.ArtifactRef{Size: int64(len(data))},
		FileHash:         bytes.Repeat([]byte{0x15}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x16}, 32),
	}
	key := hotKeyPersistentStateFile(block, master, effectiveShard)
	if err = store.hot.Set(key, encodePersistentStateFileRecord(file), pebble.Sync); err != nil {
		t.Fatalf("save persistent state metadata: %v", err)
	}
	observer.persistentStateBytes = int64(len(data))

	failure.toggle.On()
	err = store.DeletePersistentStateFile(context.Background(), block, master, effectiveShard)
	failure.toggle.Off()
	if err != nil {
		t.Fatalf("delete persistent state file: %v", err)
	}
	if failure.counter.Load() != 0 {
		t.Fatal("persistent state metadata delete unexpectedly synced Pebble")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("persistent state file after delete: %v, want not exist", statErr)
	}
	if observer.persistentStateBytes != 0 {
		t.Fatalf("persistent state metric after durable unlink = %d, want 0", observer.persistentStateBytes)
	}
}

func TestDeletePersistentStateFileKeepsMetadataWhenUnlinkFails(t *testing.T) {
	dir := t.TempDir()
	store, _ := newTestPebbleFailureStore(t, dir)

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     79,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     80,
		RootHash:  bytes.Repeat([]byte{0x23}, 32),
		FileHash:  bytes.Repeat([]byte{0x24}, 32),
	}
	effectiveShard := block.Shard

	path, err := store.persistentStateArtifactPath(block, master, effectiveShard)
	if err != nil {
		t.Fatalf("persistent state path: %v", err)
	}
	if err = os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("create non-empty directory at persistent state path: %v", err)
	}
	if err = os.WriteFile(filepath.Join(path, "child"), []byte{1}, 0o644); err != nil {
		t.Fatalf("make persistent state path non-empty: %v", err)
	}

	file := &storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   effectiveShard,
		Ref:              &storage.ArtifactRef{Size: 1},
		FileHash:         bytes.Repeat([]byte{0x25}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x26}, 32),
	}
	key := hotKeyPersistentStateFile(block, master, effectiveShard)
	if err = store.hot.Set(key, encodePersistentStateFileRecord(file), pebble.Sync); err != nil {
		t.Fatalf("save persistent state metadata: %v", err)
	}

	if err = store.DeletePersistentStateFile(context.Background(), block, master, effectiveShard); err == nil {
		t.Fatal("delete persistent state file succeeded despite unlink failure")
	}
	if _, recordErr := store.persistentStateFileRecord(context.Background(), block, master, effectiveShard); recordErr != nil {
		t.Fatalf("persistent state metadata was lost after unlink failure: %v", recordErr)
	}
}

func TestDeletePersistentStateFileRetriesAfterDurableUnlink(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     79,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     80,
		RootHash:  bytes.Repeat([]byte{0x13}, 32),
		FileHash:  bytes.Repeat([]byte{0x14}, 32),
	}
	data := []byte{1, 2, 3, 4}
	path, err := store.persistentStateArtifactPath(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state path: %v", err)
	}
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write persistent state file: %v", err)
	}
	if err = syncFile(path); err != nil {
		t.Fatalf("sync persistent state file: %v", err)
	}
	if err = syncDir(filepath.Dir(path)); err != nil {
		t.Fatalf("sync persistent state dir: %v", err)
	}
	if err = store.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		Ref:              &storage.ArtifactRef{Path: path, Size: int64(len(data))},
		FileHash:         bytes.Repeat([]byte{0x15}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x16}, 32),
	}); err != nil {
		t.Fatalf("save persistent state metadata: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store before simulated crash: %v", err)
	}

	if err = os.Remove(path); err != nil {
		t.Fatalf("simulate durable persistent state unlink: %v", err)
	}
	if err = syncDir(filepath.Dir(path)); err != nil {
		t.Fatalf("sync simulated persistent state unlink: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store after simulated crash: %v", err)
	}
	if err = store.DeletePersistentStateFile(context.Background(), block, master, 0); err != nil {
		t.Fatalf("retry persistent state delete: %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close store after retry: %v", err)
	}

	store, err = Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen store after retry: %v", err)
	}
	defer func() { _ = store.Close() }()
	if _, err = store.PersistentStateSize(context.Background(), block, master, 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("persistent state after retry = %v, want ErrNotFound", err)
	}
}

func TestSyncFileAndDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "archive.pack")
	if err := os.WriteFile(path, []byte("pack"), 0o644); err != nil {
		t.Fatalf("write pack: %v", err)
	}

	if err := syncFile(path); err != nil {
		t.Fatalf("sync file: %v", err)
	}
	if err := syncDir(dir); err != nil {
		t.Fatalf("sync directory: %v", err)
	}
}
