package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"
)

// stallTimeout bounds work that must not wait for the write queue at all.
const stallTimeout = 5 * time.Second

// A write waiting for room in a full queue must not hold the locks that work
// outside the queue takes: a snapshot read never enters the queue, and opening
// another session's journal only schedules its initialization.
func TestFullQueueDoesNotStallReadsOrJournalOpen(t *testing.T) {
	store := openTestStoreWithOptions(t, Options{Dir: t.TempDir(), QueueSize: 1})
	defer closeTestStore(t, store)

	session := testSession(21)
	id := testCandidateID(1, 21)
	wire := []byte("stored before the stall")
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: wire}, done)
	})

	releaseWriter, blockerDone := blockTestWriter(t, store)
	released := false
	defer func() {
		if !released {
			close(releaseWriter)
		}
	}()
	fillerDone := make(chan error, 1)
	if err := store.submit(writeRequest{
		apply: func(*pebble.Batch) error { return nil },
		done:  func(err error) { fillerDone <- err },
	}); err != nil {
		t.Fatal(err)
	}
	finalizedDone := make(chan error, 1)
	go store.Validator().MarkFinalized(session, id, func(err error) { finalizedDone <- err })
	waitForOutstanding(t, store, 3)

	readResult := make(chan error, 1)
	go func() {
		record, err := store.Validator().Candidate(context.Background(), session, id)
		if err == nil && !bytes.Equal(record.Wire, wire) {
			err = errors.New("candidate wire changed")
		}
		readResult <- err
	}()
	select {
	case err := <-readResult:
		if err != nil {
			t.Fatalf("candidate read behind a full queue: %v", err)
		}
	case <-time.After(stallTimeout):
		t.Fatal("candidate read waited for a full write queue")
	}

	opened := make(chan simplex.Journal, 1)
	go func() { opened <- store.Validator().Journal(testSession(22), 4) }()
	var other simplex.Journal
	select {
	case other = <-opened:
	case <-time.After(stallTimeout):
		t.Fatal("journal open waited for a full write queue")
	}

	close(releaseWriter)
	released = true
	for _, result := range []chan error{blockerDone, fillerDone, finalizedDone} {
		if err := receiveTestResult(t, result); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := other.Bootstrap(); err != nil {
		t.Fatalf("journal opened during the stall: %v", err)
	}
}

// A session write sends to the queue without namespaceMu, so a deletion must not
// overtake a write its gate already admitted: that write would recreate the
// namespace behind the tombstones.
func TestDeleteSessionQueuesBehindAdmittedWrites(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	session := testSession(27)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().MarkFinalized(session, testCandidateID(1, 27), done)
	})
	namespace, err := namespaceForSession(session)
	if err != nil {
		t.Fatal(err)
	}

	// Holding stateMu exclusively parks the next write between its gate check
	// and its send.
	store.stateMu.Lock()
	stateLocked := true
	defer func() {
		if stateLocked {
			store.stateMu.Unlock()
		}
	}()
	finalizedDone := make(chan error, 1)
	go store.Validator().MarkFinalized(session, testCandidateID(2, 27), func(err error) {
		finalizedDone <- err
	})
	waitForAdmitting(t, store, namespace)

	deleteResult := make(chan error, 1)
	go func() { deleteResult <- store.Validator().DeleteSession(context.Background(), session) }()
	waitForDeleting(t, store, namespace)

	store.stateMu.Unlock()
	stateLocked = false
	if err = receiveTestResult(t, finalizedDone); err != nil {
		t.Fatalf("admitted write: %v", err)
	}
	if err = receiveTestResult(t, deleteResult); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	if _, err = store.Validator().LoadSession(context.Background(), session); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("session after delete = %v, want ErrNotFound", err)
	}
}

// DeleteSession removes candidate packs only after the records naming them are
// committed. A deletion whose batch fails leaves the session intact and readable,
// and the retried deletion removes the packs.
func TestFailedDeleteSessionKeepsCandidatePacks(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	session := testSession(23)
	id := testCandidateID(2, 23)
	wire := []byte("survives a failed delete")
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: wire}, done)
	})
	namespace, err := namespaceForSession(session)
	if err != nil {
		t.Fatal(err)
	}

	releaseWriter, blockerDone := blockTestWriter(t, store)
	released := false
	defer func() {
		if !released {
			close(releaseWriter)
		}
	}()
	deleteResult := make(chan error, 1)
	go func() { deleteResult <- store.Validator().DeleteSession(context.Background(), session) }()
	waitForQueued(t, store, 1)

	// A commitment queued right behind the deletion joins its batch and fails it.
	batchFailure := errors.New("injected batch failure")
	failedDone := make(chan error, 1)
	if err = store.submit(writeRequest{
		apply: func(*pebble.Batch) error { return batchFailure },
		done:  func(err error) { failedDone <- err },
	}); err != nil {
		t.Fatal(err)
	}

	close(releaseWriter)
	released = true
	if err = receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err = receiveTestResult(t, deleteResult); !errors.Is(err, batchFailure) {
		t.Fatalf("delete in a failed batch = %v, want the batch failure", err)
	}
	if err = receiveTestResult(t, failedDone); !errors.Is(err, batchFailure) {
		t.Fatalf("failing request = %v, want the batch failure", err)
	}

	record, err := store.Validator().Candidate(context.Background(), session, id)
	if err != nil {
		t.Fatalf("candidate after a failed delete: %v", err)
	}
	if !bytes.Equal(record.Wire, wire) {
		t.Fatalf("candidate after a failed delete = %q, want %q", record.Wire, wire)
	}

	if err = store.Validator().DeleteSession(context.Background(), session); err != nil {
		t.Fatalf("retried delete: %v", err)
	}
	if _, err = os.Stat(store.Validator().candidatePacks.sessionDir(namespace)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate pack dir after retried delete: %v", err)
	}
}

// A candidate pack I/O failure belongs to the SaveCandidate that hit it: the
// unrelated requests batched with it must still commit.
func TestCandidatePackIOErrorRejectsOnlyItsRequest(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	packs := store.Validator().candidatePacks

	// A stored candidate whose segment path is now a directory: saving it again
	// finds its pointer and fails to read the pack with an error other than
	// not-found.
	unreadable := testSession(24)
	unreadableRecord := validator.CandidateRecord{ID: testCandidateID(1, 24), Wire: []byte("unreadable pack")}
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(unreadable, unreadableRecord, done)
	})
	unreadableNamespace, err := namespaceForSession(unreadable)
	if err != nil {
		t.Fatal(err)
	}
	pointer := candidatePointer(t, store, unreadableNamespace, unreadableRecord.ID)
	packPath := packs.packPath(unreadableNamespace, pointer.segment)
	if err = os.Remove(packPath); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(packPath, 0o755); err != nil {
		t.Fatal(err)
	}

	// A new session whose pack directory path is a regular file: its first
	// append cannot create the directory.
	unwritable := testSession(25)
	unwritableNamespace, err := namespaceForSession(unwritable)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(packs.root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(packs.sessionDir(unwritableNamespace), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	healthy := testSession(26)
	finalized := testCandidateID(3, 26)

	// The three requests are queued behind a held writer, so they share a batch.
	releaseWriter, blockerDone := blockTestWriter(t, store)
	released := false
	defer func() {
		if !released {
			close(releaseWriter)
		}
	}()
	unreadableDone := make(chan error, 1)
	store.Validator().SaveCandidate(unreadable, unreadableRecord, func(err error) { unreadableDone <- err })
	unwritableDone := make(chan error, 1)
	store.Validator().SaveCandidate(
		unwritable,
		validator.CandidateRecord{ID: testCandidateID(2, 25), Wire: []byte("unwritable pack")},
		func(err error) { unwritableDone <- err },
	)
	finalizedDone := make(chan error, 1)
	store.Validator().MarkFinalized(healthy, finalized, func(err error) { finalizedDone <- err })

	close(releaseWriter)
	released = true
	if err = receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err = receiveTestResult(t, unreadableDone); err == nil || errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("save over an unreadable pack = %v, want an I/O error", err)
	}
	if err = receiveTestResult(t, unwritableDone); err == nil {
		t.Fatal("save into an uncreatable pack directory succeeded")
	}
	if err = receiveTestResult(t, finalizedDone); err != nil {
		t.Fatalf("unrelated finality marker failed with the pack errors: %v", err)
	}

	state, err := store.Validator().LoadSession(context.Background(), healthy)
	if err != nil {
		t.Fatalf("healthy session was not committed: %v", err)
	}
	if !slices.Equal(state.Finalized, []simplex.CandidateID{finalized}) {
		t.Fatalf("healthy finality = %+v, want %+v", state.Finalized, finalized)
	}
}

// blockTestWriter parks the writer goroutine inside a request of its own until
// the returned channel is closed, so the requests submitted meanwhile queue up.
func blockTestWriter(t *testing.T, store *Store) (chan struct{}, chan error) {
	t.Helper()

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	blockerDone := make(chan error, 1)
	if err := store.submit(writeRequest{
		apply: func(*pebble.Batch) error {
			close(writerEntered)
			<-releaseWriter

			return nil
		},
		done: func(err error) { blockerDone <- err },
	}); err != nil {
		t.Fatal(err)
	}
	<-writerEntered

	return releaseWriter, blockerDone
}

func waitForAdmitting(t *testing.T, store *Store, namespace storageNamespace) {
	t.Helper()
	validatorStore := store.Validator()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		validatorStore.namespaceMu.Lock()
		admitting := validatorStore.admitting[namespace]
		validatorStore.namespaceMu.Unlock()
		if admitting > 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("session write was not admitted")
}

func waitForQueued(t *testing.T, store *Store, want int) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.queue) >= want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("queued writes = %d, want at least %d", len(store.queue), want)
}
