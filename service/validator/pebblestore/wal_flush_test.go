package pebblestore

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	walChildEnv = "GTON_WAL_FLUSH_CHILD"
	walChildDir = "GTON_WAL_FLUSH_DIR"
)

// THE PROPERTY the relaxed class rests on beyond the quorum argument: an unsynced
// write lands in the SAME write-ahead log as the commitments around it, so the next
// SYNCED commit flushes that log past it and makes it durable for free. If it did
// not hold, the risk would have to be described differently — an unsynced candidate
// would stay at risk until pebble happened to rotate the log.
//
// It is verified from two directions, because neither alone is enough:
//
//   - a HARD KILL of a live process, below. It proves the unsynced record is in the
//     log FILE — that the write reached the operating system rather than sitting in
//     the process's own buffer — and that reopening finds it. It does NOT prove the
//     bytes reached the platter: SIGKILL does not clear the page cache, so only a
//     machine crash could show that.
//   - the fsync ACCOUNTING in the second test: the WAL's own SyncData is called by
//     the synced commit at a file offset PAST the unsynced record, so the one fsync
//     the commitment pays for covers the payload written before it. That is the half
//     the kill cannot show.
//
// Together: the payload is in the log, and the commitment's fsync flushes the log
// through it. Which is what "durable for free" means.
func TestUnsyncedWriteSurvivesAHardKillWhenALaterSyncedWriteFollows(t *testing.T) {
	if os.Getenv(walChildEnv) == "1" {
		walFlushChild(t)

		return
	}

	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the test binary: %v", err)
	}
	child := exec.Command(executable, "-test.run", "TestUnsyncedWriteSurvivesAHardKillWhenALaterSyncedWriteFollows")
	child.Env = append(os.Environ(), walChildEnv+"=1", walChildDir+"="+dir)
	child.Stdout = os.Stderr
	child.Stderr = os.Stderr
	err = child.Run()

	// The child SIGKILLs itself after its writes, so there is no Close, no flush and
	// no clean shutdown of any kind — which is the point.
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("child exit = %v, want a signal: it must not have shut down cleanly", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child exit status = %v, want death by SIGKILL", exitErr)
	}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("reopen the database the killed process left behind: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()

	get := func(key string) ([]byte, error) {
		value, closer, err := db.Get([]byte(key))
		if err != nil {
			return nil, err
		}
		out := append([]byte(nil), value...)

		return out, closer.Close()
	}
	// The commitment must be there: that is the whole meaning of pebble.Sync.
	if _, err = get("commitment"); err != nil {
		t.Fatalf("the SYNCED record did not survive the kill, so this environment cannot show anything: %v", err)
	}
	// And the payload written before it, without an fsync of its own.
	payload, err := get("payload")
	if err != nil {
		t.Fatalf(
			"the unsynced payload did not survive a kill followed by a synced commit: the "+
				"WAL-flush property does NOT hold here, and the risk of the relaxed class has to "+
				"be described as lasting until pebble rotates the log: %v",
			err,
		)
	}
	if len(payload) != measureWireBytes {
		t.Fatalf("recovered payload = %d bytes, want the %d written", len(payload), measureWireBytes)
	}
	// THE CONTROL that makes the result above mean something. The child also wrote a
	// payload AFTER the last synced commit, which nothing flushes the log past. On
	// this machine it did NOT survive the kill — so the survival above is the synced
	// commit's doing and not the page cache saving everything indiscriminately. It is
	// logged rather than asserted: what pebble's log writer has already handed to the
	// operating system by the time of the kill is a timing property, and a test that
	// demanded its loss would be pinning the wrong thing.
	if _, err = get("trailing"); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("unexpected error reading the trailing record: %v", err)
	} else {
		t.Logf("trailing unsynced record after the last synced commit: survived=%v", err == nil)
	}
}

// walFlushChild is the process that dies. It writes the payload unsynced, a
// commitment synced, one more payload unsynced, and then kills itself.
func walFlushChild(t *testing.T) {
	dir := os.Getenv(walChildDir)
	if dir == "" {
		t.Fatal("child started without a database directory")
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("child open: %v", err)
	}

	commit := func(key string, value []byte, options *pebble.WriteOptions) {
		batch := db.NewIndexedBatch()
		if err := batch.Set([]byte(key), value, nil); err != nil {
			t.Fatalf("child set %s: %v", key, err)
		}
		if err := batch.Commit(options); err != nil {
			t.Fatalf("child commit %s: %v", key, err)
		}
		if err := batch.Close(); err != nil {
			t.Fatalf("child close %s: %v", key, err)
		}
	}

	commit("payload", make([]byte, measureWireBytes), pebble.NoSync)
	commit("commitment", []byte{1}, pebble.Sync)
	commit("trailing", []byte{2}, pebble.NoSync)

	// No Close, no flush, no deferred anything.
	if err = syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		t.Fatalf("child failed to kill itself: %v", err)
	}
	// Unreachable; SIGKILL is not deliverable-and-ignorable.
	t.Fatal("child survived SIGKILL")
}

// offsetTrackingFS records, for each fsync of a file, how many bytes had been
// written to that file by then. That is what shows one fsync covering the bytes of
// a preceding unsynced commit.
type offsetTrackingFS struct {
	vfs.FS
	mu      sync.Mutex
	written map[string]int64
	syncsAt map[string][]int64
}

func newOffsetTrackingFS() *offsetTrackingFS {
	return &offsetTrackingFS{
		FS:      vfs.Default,
		written: map[string]int64{},
		syncsAt: map[string][]int64{},
	}
}

func (f *offsetTrackingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, category)
	if err != nil {
		return nil, err
	}

	return &offsetTrackingFile{File: file, fs: f, name: name}, nil
}

func (f *offsetTrackingFS) note(name string, written int64) {
	f.mu.Lock()
	f.written[name] += written
	f.mu.Unlock()
}

func (f *offsetTrackingFS) noteSync(name string) {
	f.mu.Lock()
	f.syncsAt[name] = append(f.syncsAt[name], f.written[name])
	f.mu.Unlock()
}

// walSyncedBytes is how many write-ahead-log bytes have been covered by an fsync:
// the furthest synced offset of each log file, summed. Summing across files is what
// makes the answer independent of a rotation, which syncs the old log and starts a
// new one — either way the bytes are flushed.
func (f *offsetTrackingFS) walSyncedBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	var total int64
	for name, at := range f.syncsAt {
		if filepath.Ext(name) != ".log" || len(at) == 0 {
			continue
		}
		furthest := int64(0)
		for _, offset := range at {
			if offset > furthest {
				furthest = offset
			}
		}
		total += furthest
	}

	return total
}

func (f *offsetTrackingFS) walSyncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := 0
	for name, at := range f.syncsAt {
		if filepath.Ext(name) == ".log" {
			count += len(at)
		}
	}

	return count
}

type offsetTrackingFile struct {
	vfs.File
	fs   *offsetTrackingFS
	name string
}

func (f *offsetTrackingFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	f.fs.note(f.name, int64(n))

	return n, err
}

func (f *offsetTrackingFile) Sync() error {
	f.fs.noteSync(f.name)

	return f.File.Sync()
}

func (f *offsetTrackingFile) SyncData() error {
	f.fs.noteSync(f.name)

	return f.File.SyncData()
}

// The accounting half of the property: the commitment's fsync of the write-ahead
// log happens at an offset PAST the unsynced payload, so the one fsync it pays for
// is what makes the payload durable.
func TestTheSyncedCommitsFsyncCoversTheUnsyncedPayload(t *testing.T) {
	tracking := newOffsetTrackingFS()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{FS: tracking})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})

	payload := func() {
		batch := db.NewIndexedBatch()
		if err := batch.Set([]byte("payload"), make([]byte, measureWireBytes), nil); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
	}
	commitment := func() {
		batch := db.NewIndexedBatch()
		if err := batch.Set([]byte("commitment"), []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
	}

	payload()
	syncsBefore := tracking.walSyncCount()
	syncedBefore := tracking.walSyncedBytes()
	commitment()
	if tracking.walSyncCount() <= syncsBefore {
		t.Fatal("the synced commit did not fsync the write-ahead log at all")
	}
	// The bytes an fsync has covered now include the payload written before it. The
	// unsynced commit did not pay for that; the commitment did, and it would have
	// paid for its own bytes anyway.
	synced := tracking.walSyncedBytes()
	if synced < int64(measureWireBytes) {
		t.Fatalf(
			"the log has %d fsynced bytes after the commitment, less than the %d-byte payload "+
				"written before it (%d before): the unsynced payload is NOT made durable by the "+
				"next synced commit",
			synced,
			measureWireBytes,
			syncedBefore,
		)
	}
}
