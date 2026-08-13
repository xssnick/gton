package pebblestore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	crashDurabilityHelperEnv = "GTON_VALIDATOR_STORE_CRASH_HELPER"
	crashDurabilityDirEnv    = "GTON_VALIDATOR_STORE_CRASH_DIR"
)

type crashDurabilityFixture struct {
	session       validator.SessionStorageID
	candidate     validator.CandidateRecord
	authorization validator.DelegationAuthorization
	vote          simplex.Vote
	cert          *simplex.Certificate
	window        uint32
}

func TestDurabilityWithoutClose(t *testing.T) {
	if os.Getenv(crashDurabilityHelperEnv) == "1" {
		runCrashDurabilityHelper(t, os.Getenv(crashDurabilityDirEnv))
		os.Exit(0)
	}

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDurabilityWithoutClose$", "-test.count=1")
	cmd.Env = append(
		os.Environ(),
		crashDurabilityHelperEnv+"=1",
		crashDurabilityDirEnv+"="+dir,
	)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("crash helper timed out: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}

	fixture := newCrashDurabilityFixture()
	store := openTestStore(t, dir)
	defer closeTestStore(t, store)
	bootstrap, err := store.Validator().Journal(fixture.session, 4).Bootstrap()
	if err != nil {
		t.Fatalf("bootstrap after unclean exit: %v", err)
	}
	if !slices.Equal(bootstrap.OurVotes, []simplex.Vote{fixture.vote}) {
		t.Fatalf("bootstrap votes = %#v, want %#v", bootstrap.OurVotes, fixture.vote)
	}
	if len(bootstrap.Certificates) != 1 || bootstrap.Certificates[0].Vote != fixture.cert.Vote {
		t.Fatalf("bootstrap certificates = %#v", bootstrap.Certificates)
	}
	if bootstrap.FirstNonAnnouncedWindow != fixture.window {
		t.Fatalf("bootstrap window = %d, want %d", bootstrap.FirstNonAnnouncedWindow, fixture.window)
	}

	candidate, err := store.Validator().Candidate(context.Background(), fixture.session, fixture.candidate.ID)
	if err != nil {
		t.Fatalf("candidate after unclean exit: %v", err)
	}
	if !bytes.Equal(candidate.Wire, fixture.candidate.Wire) {
		t.Fatalf("candidate wire = %q, want %q", candidate.Wire, fixture.candidate.Wire)
	}
	authorization, err := store.Validator().DelegationAuthorization(
		context.Background(),
		fixture.session,
		fixture.authorization.StartSlot,
	)
	if err != nil {
		t.Fatalf("delegation authorization after unclean exit: %v", err)
	}
	assertDelegationAuthorization(t, authorization, fixture.authorization)
	state, err := store.Validator().LoadSession(context.Background(), fixture.session)
	if err != nil {
		t.Fatalf("load session after unclean exit: %v", err)
	}
	if !slices.Equal(state.CandidateIDs, []simplex.CandidateID{fixture.candidate.ID}) {
		t.Fatalf("candidate IDs = %#v", state.CandidateIDs)
	}
	if !slices.Equal(state.Finalized, []simplex.CandidateID{fixture.candidate.ID}) {
		t.Fatalf("finalized IDs = %#v", state.Finalized)
	}
	sessions, err := store.Validator().Sessions(context.Background())
	if err != nil {
		t.Fatalf("enumerate sessions after unclean exit: %v", err)
	}
	if !slices.Equal(sessions, []validator.SessionStorageID{fixture.session}) {
		t.Fatalf("enumerated sessions = %+v, want %+v", sessions, fixture.session)
	}

	status, err := store.Validator().Status(context.Background())
	if err != nil {
		t.Fatalf("status after unclean exit: %v", err)
	}
	if len(status.Sessions) != 1 {
		t.Fatalf("status sessions = %d, want 1", len(status.Sessions))
	}
	summary := status.Sessions[0]
	if summary.ID != fixture.session || summary.Candidates != 1 || summary.Finalized != 1 ||
		summary.Votes != 1 || summary.Certificates != 1 || summary.LeaderWindows != 0 ||
		summary.FirstNonAnnouncedWindow != fixture.window {
		t.Fatalf("status summary = %+v", summary)
	}
	if summary.LastFinalized == nil || *summary.LastFinalized != fixture.candidate.ID {
		t.Fatalf("last finalized = %+v, want %+v", summary.LastFinalized, fixture.candidate.ID)
	}
	if summary.LastLeaderWindow != nil {
		t.Fatalf("last leader window = %+v, want nil", summary.LastLeaderWindow)
	}
	if status.PendingWrites != 0 {
		t.Fatalf("pending writes after reopen = %d, want 0", status.PendingWrites)
	}
}

func runCrashDurabilityHelper(t *testing.T, dir string) {
	t.Helper()
	if dir == "" {
		t.Fatal("crash helper database directory is empty")
	}

	fixture := newCrashDurabilityFixture()
	store := openTestStore(t, dir)
	journal := store.Validator().Journal(fixture.session, 4)
	awaitTestSave(t, func(done func(error)) { journal.SaveOurVote(fixture.vote, done) })
	awaitTestSave(t, func(done func(error)) { journal.SaveCertificate(fixture.cert, done) })
	awaitTestSave(t, func(done func(error)) {
		journal.SaveFirstNonAnnouncedWindow(fixture.window, done)
	})
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(fixture.session, fixture.candidate, done)
	})
	awaitTestSave(t, func(done func(error)) {
		store.Validator().MarkFinalized(fixture.session, fixture.candidate.ID, done)
	})
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveDelegationAuthorization(
			context.Background(),
			fixture.session,
			fixture.authorization,
			done,
		)
	})
	waitForNoOutstanding(t, store)
}

func newCrashDurabilityFixture() crashDurabilityFixture {
	candidate := validator.CandidateRecord{
		ID:   testCandidateID(17, 12),
		Wire: []byte("crash-durable candidate"),
	}

	return crashDurabilityFixture{
		session:       testSession(11),
		candidate:     candidate,
		authorization: testDelegationAuthorization(20, 0x21),
		vote:          simplex.NotarizeVote(candidate.ID),
		cert:          &simplex.Certificate{Vote: simplex.SkipVote(19)},
		window:        7,
	}
}

func TestJournalReopenBootstrapOrderingAndDedup(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	session := testSession(1)
	journal := store.Validator().Journal(session, 4)

	first := simplex.NotarizeVote(testCandidateID(9, 1))
	firstResult := make(chan error, 1)
	duplicateResult := make(chan error, 1)
	journal.SaveOurVote(first, func(err error) { firstResult <- err })
	journal.SaveOurVote(first, func(err error) { duplicateResult <- err })
	if err := receiveTestResult(t, duplicateResult); !errors.Is(err, simplex.ErrAlreadySaved) {
		t.Fatalf("inflight duplicate error = %v, want ErrAlreadySaved", err)
	}
	if err := receiveTestResult(t, firstResult); err != nil {
		t.Fatalf("save first vote: %v", err)
	}

	second := simplex.SkipVote(3)
	awaitTestSave(t, func(done func(error)) { journal.SaveOurVote(second, done) })
	certificate := &simplex.Certificate{Vote: simplex.SkipVote(12)}
	awaitTestSave(t, func(done func(error)) { journal.SaveCertificate(certificate, done) })
	awaitTestSave(t, func(done func(error)) { journal.SaveFirstNonAnnouncedWindow(2, done) })
	awaitTestSave(t, func(done func(error)) { journal.SaveFirstNonAnnouncedWindow(7, done) })

	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	store = openTestStore(t, dir)
	journal = store.Validator().Journal(session, 4)
	bootstrap, err := journal.Bootstrap()
	if err != nil {
		t.Fatalf("bootstrap reopened journal: %v", err)
	}
	if !slices.Equal(bootstrap.OurVotes, []simplex.Vote{first, second}) {
		t.Fatalf("bootstrap votes = %#v, want submission order", bootstrap.OurVotes)
	}
	if len(bootstrap.Certificates) != 1 || bootstrap.Certificates[0].Vote != certificate.Vote {
		t.Fatalf("bootstrap certificates = %#v", bootstrap.Certificates)
	}
	if bootstrap.FirstNonAnnouncedWindow != 7 {
		t.Fatalf("pool window = %d, want 7", bootstrap.FirstNonAnnouncedWindow)
	}

	third := simplex.FinalizeVote(testCandidateID(4, 3))
	awaitTestSave(t, func(done func(error)) { journal.SaveOurVote(third, done) })
	if err = store.Close(); err != nil {
		t.Fatalf("close second store: %v", err)
	}
	store = openTestStore(t, dir)
	bootstrap, err = store.Validator().Journal(session, 4).Bootstrap()
	if err != nil {
		t.Fatalf("bootstrap second reopen: %v", err)
	}
	if !slices.Equal(bootstrap.OurVotes, []simplex.Vote{first, second, third}) {
		t.Fatalf("bootstrap votes after append = %#v", bootstrap.OurVotes)
	}
	if err = store.Close(); err != nil {
		t.Fatalf("close final store: %v", err)
	}
}

func TestJournalReturnsBeforeInitializationCompletes(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseWriter)
		}
	}()
	blockerDone := make(chan error, 1)
	err := store.submit(writeRequest{
		apply: func(*pebble.Batch) error {
			close(writerEntered)
			<-releaseWriter

			return nil
		},
		done: func(err error) { blockerDone <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	<-writerEntered

	journalResult := make(chan simplex.Journal, 1)
	go func() { journalResult <- store.Validator().Journal(testSession(2), 4) }()
	var journal simplex.Journal
	select {
	case journal = <-journalResult:
	case <-time.After(time.Second):
		t.Fatal("Journal waited for its durable initialization")
	}

	waitForOutstanding(t, store, 2)
	bootstrapResult := make(chan error, 1)
	go func() {
		_, err := journal.Bootstrap()
		bootstrapResult <- err
	}()
	select {
	case err = <-bootstrapResult:
		t.Fatalf("Bootstrap returned before initialization completed: %v", err)
	default:
	}

	close(releaseWriter)
	released = true
	if err = receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err = receiveTestResult(t, bootstrapResult); err != nil {
		t.Fatalf("Bootstrap after initialization: %v", err)
	}
}

func TestWriteBatchByteLimit(t *testing.T) {
	tests := []struct {
		name       string
		batchBytes int
		nextBytes  int
		want       bool
	}{
		{name: "request after oversized batch", batchBytes: maxWriteBatchBytes + 1},
		{name: "oversized request", nextBytes: maxWriteBatchBytes + 1},
		{name: "within cap", batchBytes: maxWriteBatchBytes / 2, nextBytes: maxWriteBatchBytes / 2, want: true},
		{name: "crosses cap", batchBytes: maxWriteBatchBytes / 2, nextBytes: maxWriteBatchBytes/2 + 1},
		{name: "zero-size request", batchBytes: maxWriteBatchBytes / 2, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := writeBatchCanAppend(test.batchBytes, writeRequest{sizeHint: test.nextBytes})
			if got != test.want {
				t.Fatalf("writeBatchCanAppend() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestWriterSplitsLargeBatchesWithoutReordering(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	store := &Store{
		db:         db,
		queue:      make(chan writeRequest, 3),
		writerDone: make(chan struct{}),
	}
	applied := make([]byte, 0, 3)
	batchWasEmpty := make([]bool, 0, 3)
	done := make(chan error, 3)
	for i, size := range []int{maxWriteBatchBytes / 2, maxWriteBatchBytes / 2, 1} {
		index := byte(i + 1)
		store.queue <- writeRequest{
			sizeHint: size,
			apply: func(batch *pebble.Batch) error {
				applied = append(applied, index)
				batchWasEmpty = append(batchWasEmpty, batch.Empty())

				return batch.Set([]byte{index}, []byte{index}, nil)
			},
			done: func(err error) { done <- err },
		}
	}
	close(store.queue)
	store.outstanding.Add(3)
	store.runWriter()
	for range 3 {
		if err = receiveTestResult(t, done); err != nil {
			t.Fatal(err)
		}
	}
	store.callbackWG.Wait()

	if !bytes.Equal(applied, []byte{1, 2, 3}) {
		t.Fatalf("apply order = %v, want FIFO", applied)
	}
	if !slices.Equal(batchWasEmpty, []bool{true, false, true}) {
		t.Fatalf("batch boundaries = %v, want [new same new]", batchWasEmpty)
	}
}

func TestCloseWaitsForJournalInitialization(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerReleased := false
	journalLocked := false
	storeClosed := false
	var j *journal
	defer func() {
		if journalLocked {
			j.mu.Unlock()
		}
		if !writerReleased {
			close(releaseWriter)
		}
		if !storeClosed {
			_ = store.Close()
		}
	}()

	blockerDone := make(chan error, 1)
	err := store.submit(writeRequest{
		apply: func(*pebble.Batch) error {
			close(writerEntered)
			<-releaseWriter

			return nil
		},
		done: func(err error) { blockerDone <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	<-writerEntered

	j = store.Validator().Journal(testSession(3), 4).(*journal)
	waitForOutstanding(t, store, 2)
	j.mu.Lock()
	journalLocked = true

	closeResult := make(chan error, 1)
	go func() { closeResult <- store.Close() }()
	waitForClosed(t, store)
	close(releaseWriter)
	writerReleased = true
	if err = receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.writerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not drain during Close")
	}
	select {
	case err = <-closeResult:
		t.Fatalf("Close returned before journal initializer exited: %v", err)
	default:
	}

	j.mu.Unlock()
	journalLocked = false
	if err = receiveTestResult(t, closeResult); err != nil {
		t.Fatal(err)
	}
	storeClosed = true
	if _, err = j.Bootstrap(); !errors.Is(err, validator.ErrStorageClosed) {
		t.Fatalf("journal initialized during Close with %v, want ErrStorageClosed", err)
	}
}

func TestPoolWindowMustAdvance(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	journal := store.Validator().Journal(testSession(2), 4)

	if err := awaitTestSaveError(t, func(done func(error)) {
		journal.SaveFirstNonAnnouncedWindow(0, done)
	}); err == nil {
		t.Fatal("initial zero pool window was accepted")
	}

	firstResult := make(chan error, 1)
	regressionResult := make(chan error, 1)
	journal.SaveFirstNonAnnouncedWindow(5, func(err error) { firstResult <- err })
	journal.SaveFirstNonAnnouncedWindow(4, func(err error) { regressionResult <- err })
	if err := receiveTestResult(t, firstResult); err != nil {
		t.Fatalf("save first pool window: %v", err)
	}
	if err := receiveTestResult(t, regressionResult); err == nil {
		t.Fatal("pool window regression was accepted")
	}
	if err := awaitTestSaveError(t, func(done func(error)) {
		journal.SaveFirstNonAnnouncedWindow(5, done)
	}); err == nil {
		t.Fatal("duplicate pool window was accepted")
	}

	awaitTestSave(t, func(done func(error)) { journal.SaveFirstNonAnnouncedWindow(6, done) })
	bootstrap, err := journal.Bootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.FirstNonAnnouncedWindow != 6 {
		t.Fatalf("pool window = %d, want 6", bootstrap.FirstNonAnnouncedWindow)
	}
}

func TestProtocolDescriptorConflictOnColdReopen(t *testing.T) {
	dir := t.TempDir()
	session := testSession(3)
	store := openTestStore(t, dir)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().Journal(session, 4).SaveFirstNonAnnouncedWindow(1, done)
	})
	closeTestStore(t, store)

	changed := session
	changed.Protocol.SlotsPerLeaderWindow++
	originalNamespace, err := session.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	changedNamespace, err := changed.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	if originalNamespace != changedNamespace {
		t.Fatal("protocol unexpectedly changed the dbId namespace")
	}

	store = openTestStore(t, dir)
	defer closeTestStore(t, store)
	if _, err = store.Validator().Journal(changed, 4).Bootstrap(); !errors.Is(err, validator.ErrSessionConflict) {
		t.Fatalf("changed protocol bootstrap error = %v, want ErrSessionConflict", err)
	}
	bootstrap, err := store.Validator().Journal(session, 4).Bootstrap()
	if err != nil {
		t.Fatalf("bootstrap original protocol after conflict: %v", err)
	}
	if bootstrap.FirstNonAnnouncedWindow != 1 {
		t.Fatalf("reopened pool window = %d, want 1", bootstrap.FirstNonAnnouncedWindow)
	}
}

func TestNamespaceSeparatesCXXPathComponents(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	master := testSession(2)
	shard := master
	shard.Shard = groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	shard.CatchainSeqno++

	masterDBID, err := master.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	shardDBID, err := shard.Namespace()
	if err != nil {
		t.Fatal(err)
	}
	if masterDBID != shardDBID {
		t.Fatal("dbId hash unexpectedly includes path components")
	}

	id := testCandidateID(1, 9)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(master, validator.CandidateRecord{ID: id, Wire: []byte("master")}, done)
	})
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(shard, validator.CandidateRecord{ID: id, Wire: []byte("shard")}, done)
	})

	gotMaster, err := store.Validator().Candidate(context.Background(), master, id)
	if err != nil {
		t.Fatal(err)
	}
	gotShard, err := store.Validator().Candidate(context.Background(), shard, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotMaster.Wire) != "master" || string(gotShard.Wire) != "shard" {
		t.Fatalf("namespace records = %q/%q", gotMaster.Wire, gotShard.Wire)
	}
}

func TestCandidateAtomicIdempotentConflictAndNotFound(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(3)
	id := testCandidateID(5, 7)

	wire := []byte("canonical candidate wire")
	want := append([]byte(nil), wire...)
	result := make(chan error, 1)
	store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: wire}, func(err error) {
		result <- err
	})
	for i := range wire {
		wire[i] = 0xff
	}
	if err := receiveTestResult(t, result); err != nil {
		t.Fatalf("save candidate: %v", err)
	}

	got, err := store.Validator().Candidate(context.Background(), session, id)
	if err != nil {
		t.Fatalf("load candidate: %v", err)
	}
	if !bytes.Equal(got.Wire, want) {
		t.Fatalf("candidate wire = %x, want %x", got.Wire, want)
	}
	got.Wire[0] ^= 0xff
	again, err := store.Validator().Candidate(context.Background(), session, id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again.Wire, want) {
		t.Fatal("Candidate returned storage-owned wire")
	}

	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: want}, done)
	})
	conflict := awaitTestSaveError(t, func(done func(error)) {
		store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: []byte("different")}, done)
	})
	if !errors.Is(conflict, validator.ErrCandidateConflict) {
		t.Fatalf("candidate conflict = %v, want ErrCandidateConflict", conflict)
	}
	if _, err = store.Validator().Candidate(
		context.Background(),
		session,
		testCandidateID(6, 8),
	); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing candidate error = %v, want storage.ErrNotFound", err)
	}

	namespace, err := namespaceForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	contentHash := candidateContentHash(t, store, namespace, id)
	if _, closer, err := store.db.Get(candidateContentKey(namespace, contentHash)); err != nil {
		t.Fatalf("candidate content is not atomic with index: %v", err)
	} else if err = closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDelegationAuthorizationOwnedIdempotentAndImmutable(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	session := testSession(13)
	authorization := testDelegationAuthorization(24, 0x31)
	want := authorization
	want.Signature = append([]byte(nil), authorization.Signature...)

	result := make(chan error, 1)
	store.Validator().SaveDelegationAuthorization(context.Background(), session, authorization, func(err error) {
		result <- err
	})
	for i := range authorization.Signature {
		authorization.Signature[i] = 0xff
	}
	if err := receiveTestResult(t, result); err != nil {
		t.Fatalf("save delegation authorization: %v", err)
	}

	got, err := store.Validator().DelegationAuthorization(context.Background(), session, want.StartSlot)
	if err != nil {
		t.Fatalf("load delegation authorization: %v", err)
	}
	assertDelegationAuthorization(t, got, want)
	got.Signature[0] ^= 0xff

	again, err := store.Validator().DelegationAuthorization(context.Background(), session, want.StartSlot)
	if err != nil {
		t.Fatalf("reload delegation authorization: %v", err)
	}
	assertDelegationAuthorization(t, again, want)

	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveDelegationAuthorization(context.Background(), session, want, done)
	})
	closeTestStore(t, store)

	store = openTestStore(t, dir)
	defer closeTestStore(t, store)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveDelegationAuthorization(context.Background(), session, want, done)
	})

	conflicts := []validator.DelegationAuthorization{
		testDelegationAuthorization(want.StartSlot, 0x41),
		{
			StartSlot: want.StartSlot,
			Collator:  want.Collator,
			Signature: bytes.Repeat([]byte{0x42}, ed25519.SignatureSize),
		},
	}
	for _, conflict := range conflicts {
		err = awaitTestSaveError(t, func(done func(error)) {
			store.Validator().SaveDelegationAuthorization(context.Background(), session, conflict, done)
		})
		if !errors.Is(err, validator.ErrDelegationConflict) {
			t.Fatalf("delegation conflict = %v, want ErrDelegationConflict", err)
		}
	}

	again, err = store.Validator().DelegationAuthorization(context.Background(), session, want.StartSlot)
	if err != nil {
		t.Fatalf("load delegation authorization after conflicts: %v", err)
	}
	assertDelegationAuthorization(t, again, want)
	if _, err = store.Validator().DelegationAuthorization(
		context.Background(),
		session,
		want.StartSlot+1,
	); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing delegation authorization = %v, want storage.ErrNotFound", err)
	}
}

func TestDelegationAuthorizationValidationAndCorruption(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(14)
	valid := testDelegationAuthorization(28, 0x51)

	invalid := []validator.DelegationAuthorization{
		{StartSlot: valid.StartSlot, Signature: append([]byte(nil), valid.Signature...)},
		{StartSlot: valid.StartSlot, Collator: valid.Collator},
		{StartSlot: valid.StartSlot, Collator: valid.Collator, Signature: make([]byte, ed25519.SignatureSize-1)},
		{StartSlot: valid.StartSlot, Collator: valid.Collator, Signature: make([]byte, ed25519.SignatureSize+1)},
	}
	for _, authorization := range invalid {
		if err := awaitTestSaveError(t, func(done func(error)) {
			store.Validator().SaveDelegationAuthorization(context.Background(), session, authorization, done)
		}); err == nil {
			t.Fatalf("invalid delegation authorization was accepted: %+v", authorization)
		}
	}
	if _, err := store.Validator().LoadSession(
		context.Background(),
		session,
	); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("invalid authorizations created a session: %v", err)
	}

	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveDelegationAuthorization(context.Background(), session, valid, done)
	})
	namespace, err := namespaceForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	key := delegationAuthorizationKey(namespace, valid.StartSlot)
	if err = store.db.Set(key, []byte{delegationAuthorizationVersion}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Validator().DelegationAuthorization(
		context.Background(),
		session,
		valid.StartSlot,
	); err == nil {
		t.Fatal("truncated delegation authorization was accepted")
	}

	mismatched := valid
	mismatched.StartSlot++
	value, err := encodeDelegationAuthorization(mismatched)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.db.Set(key, value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Validator().DelegationAuthorization(
		context.Background(),
		session,
		valid.StartSlot,
	); err == nil {
		t.Fatal("delegation authorization with a mismatched key was accepted")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = store.Validator().DelegationAuthorization(
		canceled,
		session,
		valid.StartSlot,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delegation lookup = %v, want context.Canceled", err)
	}
}

func TestDelegationAuthorizationNamespaceIsolationAndDelete(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	deletedSession := testSession(15)
	keptSession := deletedSession
	keptSession.Shard = groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	keptSession.CatchainSeqno++
	deletedAuthorization := testDelegationAuthorization(32, 0x61)
	keptAuthorization := testDelegationAuthorization(32, 0x71)

	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveDelegationAuthorization(
			context.Background(),
			deletedSession,
			deletedAuthorization,
			done,
		)
	})
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveDelegationAuthorization(
			context.Background(),
			keptSession,
			keptAuthorization,
			done,
		)
	})

	got, err := store.Validator().DelegationAuthorization(
		context.Background(),
		deletedSession,
		deletedAuthorization.StartSlot,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDelegationAuthorization(t, got, deletedAuthorization)
	got, err = store.Validator().DelegationAuthorization(
		context.Background(),
		keptSession,
		keptAuthorization.StartSlot,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDelegationAuthorization(t, got, keptAuthorization)

	if err = store.Validator().DeleteSession(context.Background(), deletedSession); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	if _, err = store.Validator().DelegationAuthorization(
		context.Background(),
		deletedSession,
		deletedAuthorization.StartSlot,
	); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted authorization = %v, want storage.ErrNotFound", err)
	}
	got, err = store.Validator().DelegationAuthorization(
		context.Background(),
		keptSession,
		keptAuthorization.StartSlot,
	)
	if err != nil {
		t.Fatalf("load kept authorization: %v", err)
	}
	assertDelegationAuthorization(t, got, keptAuthorization)

	deletedNamespace, err := namespaceForSession(deletedSession)
	if err != nil {
		t.Fatal(err)
	}
	if _, closer, getErr := store.db.Get(delegationAuthorizationKey(
		deletedNamespace,
		deletedAuthorization.StartSlot,
	)); !errors.Is(getErr, pebble.ErrNotFound) {
		if getErr == nil {
			_ = closer.Close()
		}
		t.Fatalf("deleted raw authorization = %v, want pebble.ErrNotFound", getErr)
	}
}

func TestDelegationAuthorizationClosedStore(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	session := testSession(16)
	authorization := testDelegationAuthorization(36, 0x81)
	closeTestStore(t, store)

	err := awaitTestSaveError(t, func(done func(error)) {
		store.Validator().SaveDelegationAuthorization(context.Background(), session, authorization, done)
	})
	if !errors.Is(err, validator.ErrStorageClosed) {
		t.Fatalf("save delegation authorization after close = %v, want ErrStorageClosed", err)
	}
	if _, err = store.Validator().DelegationAuthorization(
		context.Background(),
		session,
		authorization.StartSlot,
	); !errors.Is(err, validator.ErrStorageClosed) {
		t.Fatalf("load delegation authorization after close = %v, want ErrStorageClosed", err)
	}
}

func TestDelegationAuthorizationAdmissionHonorsContext(t *testing.T) {
	store := openTestStoreWithOptions(t, Options{Dir: t.TempDir(), QueueSize: 1})
	defer closeTestStore(t, store)

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseWriter)
		}
	}()
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

	fillerDone := make(chan error, 1)
	if err := store.submit(writeRequest{
		apply: func(*pebble.Batch) error { return nil },
		done:  func(err error) { fillerDone <- err },
	}); err != nil {
		t.Fatal(err)
	}

	session := testSession(17)
	authorization := testDelegationAuthorization(40, 0x91)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go store.Validator().SaveDelegationAuthorization(ctx, session, authorization, func(err error) {
		result <- err
	})
	waitForOutstanding(t, store, 3)
	cancel()
	if err := receiveTestResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled authorization admission = %v, want context.Canceled", err)
	}
	if _, err := store.Validator().DelegationAuthorization(
		context.Background(),
		session,
		authorization.StartSlot,
	); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cancelled authorization was admitted: %v", err)
	}

	close(releaseWriter)
	released = true
	if err := receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err := receiveTestResult(t, fillerDone); err != nil {
		t.Fatal(err)
	}
}

func TestLoadSessionEnumeratesCandidateMetadataOnly(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(4)
	id := testCandidateID(8, 5)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: []byte("candidate")}, done)
	})

	namespace, err := namespaceForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	contentHash := candidateContentHash(t, store, namespace, id)
	if err = store.db.Delete(candidateContentKey(namespace, contentHash), pebble.Sync); err != nil {
		t.Fatal(err)
	}

	state, err := store.Validator().LoadSession(context.Background(), session)
	if err != nil {
		t.Fatalf("load candidate metadata: %v", err)
	}
	if !slices.Equal(state.CandidateIDs, []simplex.CandidateID{id}) {
		t.Fatalf("candidate IDs = %#v", state.CandidateIDs)
	}
	if _, err = store.Validator().Candidate(context.Background(), session, id); err == nil {
		t.Fatal("Candidate unexpectedly loaded missing payload")
	}
}

func TestFinalLeaderStatusAndLoadSession(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(4)
	journal := store.Validator().Journal(session, 4)
	awaitTestSave(t, func(done func(error)) { journal.SaveOurVote(simplex.SkipVote(1), done) })
	awaitTestSave(t, func(done func(error)) {
		journal.SaveCertificate(&simplex.Certificate{Vote: simplex.SkipVote(2)}, done)
	})
	awaitTestSave(t, func(done func(error)) { journal.SaveFirstNonAnnouncedWindow(8, done) })

	ids := []simplex.CandidateID{testCandidateID(9, 1), testCandidateID(2, 2)}
	for _, id := range ids {
		awaitTestSave(t, func(done func(error)) {
			store.Validator().SaveCandidate(
				session,
				validator.CandidateRecord{ID: id, Wire: []byte{byte(id.Slot)}},
				done,
			)
		})
		awaitTestSave(t, func(done func(error)) { store.Validator().MarkFinalized(session, id, done) })
	}
	observed := time.Unix(123, 456).UTC()
	windows := []validator.LeaderWindowRecord{
		{Base: simplex.Parent(ids[0]), StartSlot: 12, EndSlot: 16, ObservedAt: observed.Add(time.Second)},
		{Base: simplex.Genesis(), StartSlot: 4, EndSlot: 8, ObservedAt: observed},
	}
	for _, window := range windows {
		awaitTestSave(t, func(done func(error)) {
			store.Validator().RecordLeaderWindow(session, window, done)
		})
	}

	state, err := store.Validator().LoadSession(context.Background(), session)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	if len(state.CandidateIDs) != 2 || state.CandidateIDs[0].Slot != 2 || state.CandidateIDs[1].Slot != 9 {
		t.Fatalf("candidate ordering = %#v", state.CandidateIDs)
	}
	if len(state.Finalized) != 2 || state.Finalized[0].Slot != 2 || state.Finalized[1].Slot != 9 {
		t.Fatalf("finalized ordering = %#v", state.Finalized)
	}
	// The two leader windows recorded above are telemetry, so LoadSession does
	// not decode them; they are asserted through the session summary below.

	status, err := store.Validator().Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(status.Sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(status.Sessions))
	}
	s := status.Sessions[0]
	if s.Candidates != 2 || s.Finalized != 2 || s.Votes != 1 || s.Certificates != 1 || s.LeaderWindows != 2 {
		t.Fatalf("session counts = %+v", s)
	}
	if s.FirstNonAnnouncedWindow != 8 || s.LastFinalized == nil || s.LastFinalized.Slot != 9 {
		t.Fatalf("session final/pool status = %+v", s)
	}
	if s.LastLeaderWindow == nil || s.LastLeaderWindow.StartSlot != 12 || !s.LastLeaderWindow.ObservedAt.Equal(observed.Add(time.Second)) {
		t.Fatalf("last leader window = %+v", s.LastLeaderWindow)
	}
	if status.PendingWrites != 0 {
		t.Fatalf("pending writes = %d, want 0", status.PendingWrites)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = store.Validator().Status(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled status error = %v", err)
	}
}

func TestSessionSummaryPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	session := testSession(5)
	id1 := testCandidateID(2, 1)
	id2 := testCandidateID(9, 2)

	for _, id := range []simplex.CandidateID{id1, id2} {
		awaitTestSave(t, func(done func(error)) {
			store.Validator().SaveCandidate(
				session,
				validator.CandidateRecord{ID: id, Wire: []byte{byte(id.Slot)}},
				done,
			)
		})
		awaitTestSave(t, func(done func(error)) { store.Validator().MarkFinalized(session, id, done) })
	}
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(
			session,
			validator.CandidateRecord{ID: id1, Wire: []byte{byte(id1.Slot)}},
			done,
		)
	})
	awaitTestSave(t, func(done func(error)) { store.Validator().MarkFinalized(session, id1, done) })

	journal := store.Validator().Journal(session, 4)
	awaitTestSave(t, func(done func(error)) { journal.SaveOurVote(simplex.SkipVote(1), done) })
	awaitTestSave(t, func(done func(error)) {
		journal.SaveCertificate(&simplex.Certificate{Vote: simplex.SkipVote(2)}, done)
	})
	awaitTestSave(t, func(done func(error)) { journal.SaveFirstNonAnnouncedWindow(3, done) })

	observed := time.Unix(500, 0).UTC()
	for _, window := range []validator.LeaderWindowRecord{
		{Base: simplex.Genesis(), StartSlot: 10, EndSlot: 14, ObservedAt: observed},
		{Base: simplex.Genesis(), StartSlot: 4, EndSlot: 8, ObservedAt: observed.Add(time.Second)},
		{Base: simplex.Genesis(), StartSlot: 10, EndSlot: 14, ObservedAt: observed.Add(2 * time.Second)},
	} {
		awaitTestSave(t, func(done func(error)) {
			store.Validator().RecordLeaderWindow(session, window, done)
		})
	}

	closeTestStore(t, store)
	store = openTestStore(t, dir)
	defer closeTestStore(t, store)
	status, err := store.Validator().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(status.Sessions))
	}
	got := status.Sessions[0]
	if got.ID != session {
		t.Fatalf("reopened session descriptor = %+v, want %+v", got.ID, session)
	}
	if got.Candidates != 2 || got.Finalized != 2 || got.Votes != 1 || got.Certificates != 1 || got.LeaderWindows != 2 {
		t.Fatalf("reopened summary counts = %+v", got)
	}
	if got.FirstNonAnnouncedWindow != 3 || got.LastFinalized == nil || *got.LastFinalized != id2 {
		t.Fatalf("reopened final/pool summary = %+v", got)
	}
	if got.LastLeaderWindow == nil || got.LastLeaderWindow.StartSlot != 10 ||
		!got.LastLeaderWindow.ObservedAt.Equal(observed.Add(2*time.Second)) {
		t.Fatalf("reopened leader summary = %+v", got.LastLeaderWindow)
	}
}

func TestMissingSessionSummaryIsCorruption(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(6)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(
			session,
			validator.CandidateRecord{ID: testCandidateID(1, 1), Wire: []byte("wire")},
			done,
		)
	})
	namespace, err := namespaceForSession(session)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.db.Delete(summaryKey(namespace), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Validator().Status(context.Background()); !errors.Is(err, errSessionSummaryMissing) {
		t.Fatalf("Status error = %v, want missing-summary corruption", err)
	}
	sessions, err := store.Validator().Sessions(context.Background())
	if err != nil {
		t.Fatalf("descriptor-only Sessions failed on missing summary: %v", err)
	}
	if !slices.Equal(sessions, []validator.SessionStorageID{session}) {
		t.Fatalf("Sessions = %+v, want %+v", sessions, session)
	}
}

func TestDeleteSessionIsolationAndGate(t *testing.T) {
	store := openTestStoreWithOptions(t, Options{Dir: t.TempDir(), QueueSize: 1})
	defer closeTestStore(t, store)
	deletedSession := testSession(5)
	keptSession := testSession(6)
	deletedJournal := store.Validator().Journal(deletedSession, 4)
	keptJournal := store.Validator().Journal(keptSession, 4)
	awaitTestSave(t, func(done func(error)) { keptJournal.SaveOurVote(simplex.SkipVote(2), done) })
	awaitTestSave(t, func(done func(error)) { deletedJournal.SaveOurVote(simplex.SkipVote(1), done) })

	// Hold submission after DeleteSession installs its namespace gate so the
	// rejection behavior can be observed deterministically.
	store.stateMu.Lock()
	stateLocked := true
	defer func() {
		if stateLocked {
			store.stateMu.Unlock()
		}
	}()
	deleteResult := make(chan error, 1)
	go func() {
		deleteResult <- store.Validator().DeleteSession(context.Background(), deletedSession)
	}()
	namespace, err := namespaceForSession(deletedSession)
	if err != nil {
		t.Fatal(err)
	}
	waitForDeleting(t, store, namespace)

	staleSave := make(chan error, 1)
	deletedJournal.SaveOurVote(simplex.SkipVote(3), func(err error) { staleSave <- err })
	if err = receiveTestResult(t, staleSave); !errors.Is(err, validator.ErrSessionClosed) {
		t.Fatalf("stale save during delete = %v, want ErrSessionClosed", err)
	}
	if _, err = store.Validator().Journal(
		deletedSession,
		4,
	).Bootstrap(); !errors.Is(err, validator.ErrSessionClosed) {
		t.Fatalf("journal open during delete = %v, want ErrSessionClosed", err)
	}

	store.stateMu.Unlock()
	stateLocked = false
	if err = receiveTestResult(t, deleteResult); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	if _, err = store.Validator().LoadSession(
		context.Background(),
		deletedSession,
	); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted session load = %v, want ErrNotFound", err)
	}
	staleSave = make(chan error, 1)
	deletedJournal.SaveOurVote(simplex.SkipVote(4), func(err error) { staleSave <- err })
	if err = receiveTestResult(t, staleSave); !errors.Is(err, validator.ErrSessionClosed) {
		t.Fatalf("stale save after delete = %v, want ErrSessionClosed", err)
	}
	directSave := make(chan error, 1)
	store.Validator().MarkFinalized(deletedSession, testCandidateID(7, 7), func(err error) {
		directSave <- err
	})
	if err = receiveTestResult(t, directSave); !errors.Is(err, validator.ErrSessionClosed) {
		t.Fatalf("direct save after delete = %v, want ErrSessionClosed", err)
	}
	if _, err = keptJournal.Bootstrap(); err != nil {
		t.Fatalf("kept session bootstrap: %v", err)
	}

	// Journal is the explicit operation that reopens a completed namespace.
	reopened := store.Validator().Journal(deletedSession, 4)
	awaitTestSave(t, func(done func(error)) { reopened.SaveOurVote(simplex.SkipVote(5), done) })
	if _, err = reopened.Bootstrap(); err != nil {
		t.Fatalf("reopened journal: %v", err)
	}
}

func TestDeleteSessionCanceledBeforeSubmission(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(7)
	journal := store.Validator().Journal(session, 4)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Validator().DeleteSession(ctx, session); !errors.Is(err, context.Canceled) {
		t.Fatalf("delete canceled error = %v", err)
	}
	awaitTestSave(t, func(done func(error)) { journal.SaveOurVote(simplex.SkipVote(1), done) })
}

func TestCandidateConcurrentDeleteUsesSnapshot(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(8)
	id := testCandidateID(1, 1)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: []byte("wire")}, done)
	})

	start := make(chan struct{})
	readResult := make(chan error, 1)
	go func() {
		<-start
		_, err := store.Validator().Candidate(context.Background(), session, id)
		readResult <- err
	}()
	close(start)
	deleteErr := store.Validator().DeleteSession(context.Background(), session)
	if deleteErr != nil {
		t.Fatalf("delete: %v", deleteErr)
	}
	err := receiveTestResult(t, readResult)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("candidate concurrent delete returned false corruption: %v", err)
	}
}

func TestCloseDrainsAndRejects(t *testing.T) {
	dir := t.TempDir()
	store := openTestStoreWithOptions(t, Options{Dir: dir, QueueSize: 1})
	session := testSession(9)
	id := testCandidateID(1, 2)

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: []byte("durable")}, func(err error) {
		if err != nil {
			t.Errorf("save before close: %v", err)
		}
		close(callbackEntered)
		<-releaseCallback
	})
	<-callbackEntered

	closeResult := make(chan error, 1)
	go func() { closeResult <- store.Close() }()
	waitForClosed(t, store)

	rejected := make(chan error, 1)
	store.Validator().MarkFinalized(session, id, func(err error) { rejected <- err })
	if err := receiveTestResult(t, rejected); !errors.Is(err, validator.ErrStorageClosed) {
		t.Fatalf("write after close = %v, want ErrStorageClosed", err)
	}
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before callback drained: %v", err)
	default:
	}

	close(releaseCallback)
	if err := receiveTestResult(t, closeResult); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	reopened := openTestStore(t, dir)
	defer closeTestStore(t, reopened)
	got, err := reopened.Validator().Candidate(context.Background(), session, id)
	if err != nil || string(got.Wire) != "durable" {
		t.Fatalf("drained candidate = %q, %v", got.Wire, err)
	}
}

func TestCallbackCanSynchronouslyReenterStorage(t *testing.T) {
	store := openTestStoreWithOptions(t, Options{Dir: t.TempDir(), QueueSize: 1})
	defer closeTestStore(t, store)
	session := testSession(10)
	id := testCandidateID(1, 3)

	outerResult := make(chan error, 1)
	store.Validator().SaveCandidate(session, validator.CandidateRecord{ID: id, Wire: []byte("candidate")}, func(err error) {
		if err != nil {
			outerResult <- err

			return
		}

		nestedResult := make(chan error, 1)
		store.Validator().MarkFinalized(session, id, func(err error) { nestedResult <- err })
		outerResult <- <-nestedResult
	})
	if err := receiveTestResult(t, outerResult); err != nil {
		t.Fatalf("reentrant storage callback: %v", err)
	}

	state, err := store.Validator().LoadSession(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Finalized) != 1 || state.Finalized[0] != id {
		t.Fatalf("nested finalized records = %#v", state.Finalized)
	}
}

func TestStoreValidatorViewIsStable(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	validatorStore := store.Validator()
	if validatorStore == nil {
		t.Fatal("Validator returned nil")
	}
	if store.Validator() != validatorStore {
		t.Fatal("Validator returned a different view")
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if store.Validator() != validatorStore {
		t.Fatal("Validator returned a different view after Close")
	}
	if _, err := validatorStore.Status(context.Background()); !errors.Is(err, validator.ErrStorageClosed) {
		t.Fatalf("Status after Close = %v, want ErrStorageClosed", err)
	}
}

func TestSchemaValidation(t *testing.T) {
	t.Run("new database is versioned", func(t *testing.T) {
		dir := t.TempDir()
		store := openTestStore(t, dir)
		closeTestStore(t, store)

		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		value, closer, err := db.Get(schemaKey)
		if err != nil {
			t.Fatal(err)
		}
		if len(value) != 4 || binary.LittleEndian.Uint32(value) != schemaVersion {
			t.Fatalf("schema marker = %x", value)
		}
		if err = closer.Close(); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("nonempty unversioned database is refused", func(t *testing.T) {
		dir := t.TempDir()
		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Set([]byte("foreign"), []byte("record"), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}

		store, err := Open(Options{Dir: dir})
		if store != nil {
			_ = store.Close()
		}
		if !errors.Is(err, ErrUnversionedDatabase) {
			t.Fatalf("Open error = %v, want ErrUnversionedDatabase", err)
		}
	})

	t.Run("unsupported schema is refused", func(t *testing.T) {
		dir := t.TempDir()
		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		marker := binary.LittleEndian.AppendUint32(nil, schemaVersion+1)
		if err = db.Set(schemaKey, marker, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}

		store, err := Open(Options{Dir: dir})
		if store != nil {
			_ = store.Close()
		}
		if !errors.Is(err, ErrUnsupportedSchema) {
			t.Fatalf("Open error = %v, want ErrUnsupportedSchema", err)
		}
	})
}

func openTestStore(t *testing.T, dir string) *Store {
	t.Helper()

	return openTestStoreWithOptions(t, Options{Dir: dir})
}

func openTestStoreWithOptions(t *testing.T, opts Options) *Store {
	t.Helper()

	store, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	return store
}

func closeTestStore(t *testing.T, store *Store) {
	t.Helper()

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func testSession(fill byte) validator.SessionStorageID {
	var sessionID [32]byte
	var keyID [32]byte
	var adnlID [32]byte
	for i := range sessionID {
		sessionID[i] = fill
		keyID[i] = fill + 1
		adnlID[i] = fill + 2
	}

	return validator.SessionStorageID{
		SessionID:      sessionID,
		Shard:          groups.ShardID{Workchain: -1, Shard: math.MinInt64},
		CatchainSeqno:  uint32(fill),
		IsValidator:    true,
		ValidatorKeyID: keyID,
		LocalADNLID:    adnlID,
		ValidatorIndex: int(fill),
		Protocol: validator.SessionProtocol{
			Version:              2,
			Flags:                fill,
			ProtocolVersion:      3,
			UseQUIC:              true,
			SlotsPerLeaderWindow: 4,
		},
	}
}

func testCandidateID(slot uint32, fill byte) simplex.CandidateID {
	id := simplex.CandidateID{Slot: slot}
	for i := range id.Hash {
		id.Hash[i] = fill
	}

	return id
}

func testDelegationAuthorization(startSlot uint32, fill byte) validator.DelegationAuthorization {
	var collator [32]byte
	for i := range collator {
		collator[i] = fill
	}

	return validator.DelegationAuthorization{
		StartSlot: startSlot,
		Collator:  collator,
		Signature: bytes.Repeat([]byte{fill + 1}, ed25519.SignatureSize),
	}
}

func assertDelegationAuthorization(
	t *testing.T,
	got validator.DelegationAuthorization,
	want validator.DelegationAuthorization,
) {
	t.Helper()

	if got.StartSlot != want.StartSlot || got.Collator != want.Collator || !bytes.Equal(got.Signature, want.Signature) {
		t.Fatalf("delegation authorization = %+v, want %+v", got, want)
	}
}

func awaitTestSave(t *testing.T, submit func(func(error))) {
	t.Helper()

	if err := awaitTestSaveError(t, submit); err != nil {
		t.Fatal(err)
	}
}

func awaitTestSaveError(t *testing.T, submit func(func(error))) error {
	t.Helper()

	result := make(chan error, 1)
	submit(func(err error) { result <- err })

	return receiveTestResult(t, result)
}

func receiveTestResult(t *testing.T, result <-chan error) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for asynchronous storage result")

		return nil
	}
}

func candidateContentHash(
	t *testing.T,
	store *Store,
	namespace storageNamespace,
	id simplex.CandidateID,
) [32]byte {
	t.Helper()

	value, closer, err := store.db.Get(candidateIndexKey(namespace, id))
	if err != nil {
		t.Fatal(err)
	}
	if len(value) != 32 {
		_ = closer.Close()
		t.Fatalf("content hash length = %d", len(value))
	}

	var hash [32]byte
	copy(hash[:], value)
	if err = closer.Close(); err != nil {
		t.Fatal(err)
	}

	return hash
}

func waitForDeleting(t *testing.T, store *Store, namespace storageNamespace) {
	t.Helper()
	validatorStore := store.Validator()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		validatorStore.namespaceMu.Lock()
		_, deleting := validatorStore.deleting[namespace]
		validatorStore.namespaceMu.Unlock()
		if deleting {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("session did not enter deleting state")
}

func waitForOutstanding(t *testing.T, store *Store, want int64) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if store.outstanding.Load() >= want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("outstanding writes = %d, want at least %d", store.outstanding.Load(), want)
}

func waitForNoOutstanding(t *testing.T, store *Store) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if store.outstanding.Load() == 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("outstanding writes = %d, want 0", store.outstanding.Load())
}

func waitForClosed(t *testing.T, store *Store) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		store.stateMu.Lock()
		closed := store.isClosed
		store.stateMu.Unlock()
		if closed {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("store did not enter closed state")
}

// A single request larger than the batch cap must still be written: the first
// request of a batch is appended before the cap is consulted. Without this the
// cap would silently drop any write bigger than itself.
func TestWriterCommitsSingleOversizedRequest(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	store := &Store{
		db:         db,
		queue:      make(chan writeRequest, 2),
		writerDone: make(chan struct{}),
	}
	done := make(chan error, 2)
	for i, size := range []int{maxWriteBatchBytes + 1, 1} {
		index := byte(i + 1)
		store.queue <- writeRequest{
			sizeHint: size,
			apply: func(batch *pebble.Batch) error {
				return batch.Set([]byte{index}, []byte{index}, nil)
			},
			done: func(err error) { done <- err },
		}
	}
	close(store.queue)

	go store.runWriter()
	for range 2 {
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	}
	<-store.writerDone

	for _, key := range []byte{1, 2} {
		value, closer, getErr := db.Get([]byte{key})
		if getErr != nil {
			t.Fatalf("key %d was not written: %v", key, getErr)
		}
		if len(value) != 1 || value[0] != key {
			t.Fatalf("key %d holds %x", key, value)
		}
		if err = closer.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// One batch carries up to maxWriteBatch requests from unrelated namespaces and
// from both the validator and the collator view, and a journal write error is
// fatal to a consensus session. A corrupt stored record must therefore fail
// only the request that read it: the batch it happened to share must still
// commit. The requests are applied through commitRequests directly because the
// batching of independently submitted writes is otherwise timing dependent.
func TestBatchIsolatesPerRecordCorruption(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	poisoned := testSession(7)
	healthy := testSession(8)
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(
			poisoned,
			validator.CandidateRecord{ID: testCandidateID(1, 1), Wire: []byte("wire")},
			done,
		)
	})
	poisonedNamespace, err := namespaceForSession(poisoned)
	if err != nil {
		t.Fatal(err)
	}
	healthyNamespace, err := namespaceForSession(healthy)
	if err != nil {
		t.Fatal(err)
	}
	// The descriptor survives, its summary does not: every validator write to
	// this namespace now fails inside ensureSession.
	if err = store.db.Delete(summaryKey(poisonedNamespace), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	// The collator view of the same physical store gets its own poison pill.
	corruptCollatorSession := [32]byte{9}
	if err = store.db.Set(collatorSessionKey(corruptCollatorSession), []byte("not a record"), pebble.Sync); err != nil {
		t.Fatal(err)
	}

	finalized := testCandidateID(4, 2)
	results := make([]error, 3)
	completed := make(chan int, 3)
	requests := []writeRequest{{
		apply: func(batch *pebble.Batch) error {
			_, applyErr := ensureSession(batch, poisoned, poisonedNamespace)

			return applyErr
		},
		done: func(err error) { results[0] = err; completed <- 0 },
	}, {
		apply: func(batch *pebble.Batch) error {
			_, applyErr := collatorSessionFromBatch(batch, corruptCollatorSession)

			return applyErr
		},
		done: func(err error) { results[1] = err; completed <- 1 },
	}, {
		// The body of MarkFinalized: an unrelated healthy consensus write.
		apply: func(batch *pebble.Batch) error {
			summary, applyErr := ensureSession(batch, healthy, healthyNamespace)
			if applyErr != nil {
				return applyErr
			}
			if applyErr = batch.Set(finalizedKey(healthyNamespace, finalized), []byte{}, nil); applyErr != nil {
				return applyErr
			}
			if applyErr = incrementSummaryCount("finalized", &summary.finalized); applyErr != nil {
				return applyErr
			}
			copyID := finalized
			summary.lastFinalized = &copyID

			return saveSessionSummary(batch, healthyNamespace, summary)
		},
		done: func(err error) { results[2] = err; completed <- 2 },
	}}

	// commitRequests decrements the counter its submitters incremented.
	store.outstanding.Add(int64(len(requests)))
	store.commitRequests(requests)
	for range requests {
		select {
		case <-completed:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for batch callbacks")
		}
	}
	store.callbackWG.Wait()

	if !errors.Is(results[0], errSessionSummaryMissing) {
		t.Fatalf("poisoned validator request error = %v, want missing summary", results[0])
	}
	if results[1] == nil {
		t.Fatal("corrupt collator session record did not fail its own request")
	}
	if errors.Is(results[1], errSessionSummaryMissing) {
		t.Fatalf("collator request inherited the validator request error: %v", results[1])
	}
	if results[2] != nil {
		t.Fatalf("healthy request failed alongside corrupt records: %v", results[2])
	}

	state, err := store.Validator().LoadSession(context.Background(), healthy)
	if err != nil {
		t.Fatalf("healthy session was not committed: %v", err)
	}
	if !slices.Equal(state.Finalized, []simplex.CandidateID{finalized}) {
		t.Fatalf("healthy finality = %+v, want %+v", state.Finalized, finalized)
	}
}

// Session start reads the journal three times with no write in between, so the
// scan is cached. The cache must stay a live read: a save invalidates it, and a
// closed store must not be served from it.
func TestJournalBootstrapCachesOneScan(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	closed := false
	defer func() {
		if !closed {
			closeTestStore(t, store)
		}
	}()
	session := testSession(11)
	j := store.Validator().Journal(session, 4).(*journal)
	awaitTestSave(t, func(done func(error)) { j.SaveOurVote(simplex.SkipVote(1), done) })

	first, err := j.Bootstrap()
	if err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}
	if !slices.Equal(first.OurVotes, []simplex.Vote{simplex.SkipVote(1)}) {
		t.Fatalf("first bootstrap votes = %#v", first.OurVotes)
	}
	second, err := j.Bootstrap()
	if err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	// Pointer identity is the observable form of "scanned once": Engine.Start
	// reads the same state the runtime constructor already read.
	if second != first {
		t.Fatal("second bootstrap rescanned the journal")
	}

	// A completed save must be visible to the next bootstrap.
	awaitTestSave(t, func(done func(error)) { j.SaveOurVote(simplex.SkipVote(2), done) })
	third, err := j.Bootstrap()
	if err != nil {
		t.Fatalf("bootstrap after save: %v", err)
	}
	if !slices.Equal(third.OurVotes, []simplex.Vote{simplex.SkipVote(1), simplex.SkipVote(2)}) {
		t.Fatalf("bootstrap after save votes = %#v", third.OurVotes)
	}
	awaitTestSave(t, func(done func(error)) { j.SaveFirstNonAnnouncedWindow(3, done) })
	fourth, err := j.Bootstrap()
	if err != nil {
		t.Fatalf("bootstrap after window save: %v", err)
	}
	if fourth.FirstNonAnnouncedWindow != 3 {
		t.Fatalf("bootstrap window = %d, want 3", fourth.FirstNonAnnouncedWindow)
	}

	closeTestStore(t, store)
	closed = true
	if _, err = j.Bootstrap(); !errors.Is(err, validator.ErrStorageClosed) {
		t.Fatalf("bootstrap on closed store = %v, want ErrStorageClosed", err)
	}
}

// A deleted namespace must not be served from the cache either.
func TestJournalBootstrapCacheFailsClosedAfterDelete(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	session := testSession(12)
	j := store.Validator().Journal(session, 4)
	awaitTestSave(t, func(done func(error)) { j.SaveOurVote(simplex.SkipVote(1), done) })
	if _, err := j.Bootstrap(); err != nil {
		t.Fatalf("bootstrap before delete: %v", err)
	}
	if err := store.Validator().DeleteSession(context.Background(), session); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	if _, err := j.Bootstrap(); !errors.Is(err, validator.ErrSessionClosed) {
		t.Fatalf("bootstrap after delete = %v, want ErrSessionClosed", err)
	}
}
