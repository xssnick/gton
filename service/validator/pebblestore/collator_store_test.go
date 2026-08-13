package pebblestore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	collatorCrashHelperEnv = "GTON_COLLATOR_STORE_CRASH_HELPER"
	collatorCrashDirEnv    = "GTON_COLLATOR_STORE_CRASH_DIR"
)

func TestCollatorDurabilityWithoutClose(t *testing.T) {
	if os.Getenv(collatorCrashHelperEnv) == "1" {
		runCollatorCrashHelper(t, os.Getenv(collatorCrashDirEnv))
		os.Exit(0)
	}

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCollatorDurabilityWithoutClose$", "-test.count=1")
	cmd.Env = append(os.Environ(), collatorCrashHelperEnv+"=1", collatorCrashDirEnv+"="+dir)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("collator crash helper timed out: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("collator crash helper: %v\n%s", err, output)
	}

	session := testCollatorSessionRecord(1)
	candidate := testDelegatedCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart, 1)
	store := openTestStore(t, dir)
	defer closeTestStore(t, store)

	gotSession, err := store.Collator().Session(context.Background(), session.Session.ID)
	if err != nil {
		t.Fatalf("session after unclean exit: %v", err)
	}
	if !reflect.DeepEqual(gotSession, session) {
		t.Fatalf("session after unclean exit = %#v, want %#v", gotSession, session)
	}

	gotCandidate, err := store.Collator().Candidate(context.Background(), candidate.WindowID, candidate.ID.Slot)
	if err != nil {
		t.Fatalf("candidate after unclean exit: %v", err)
	}
	if !reflect.DeepEqual(gotCandidate, candidate) {
		t.Fatalf("candidate after unclean exit = %#v, want %#v", gotCandidate, candidate)
	}
}

func runCollatorCrashHelper(t *testing.T, dir string) {
	t.Helper()
	if dir == "" {
		t.Fatal("collator crash helper database directory is empty")
	}

	session := testCollatorSessionRecord(1)
	candidate := testDelegatedCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart, 1)
	store := openTestStore(t, dir)
	awaitTestSave(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), session, done)
	})
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(candidate, done) })
	waitForNoOutstanding(t, store)
}

func TestCollatorStableViewsKeyspaceCodecsAndOwnership(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	if store.Collator() != store.Collator() || store.Validator() != store.Validator() {
		t.Fatal("Store returned unstable persistence views")
	}

	session := testCollatorSessionRecord(2)
	wantSessionValue, err := encodeCollatorSessionRecord(session)
	if err != nil {
		t.Fatal(err)
	}
	wantSession, err := decodeCollatorSessionRecord(wantSessionValue)
	if err != nil {
		t.Fatal(err)
	}

	sessionResult := make(chan error, 1)
	store.Collator().SaveSession(context.Background(), session, func(err error) { sessionResult <- err })
	session.Session.Validators[0].Weight++
	session.Update.MasterchainBlock.RootHash[0] ^= 0xff
	session.Update.Registered[0].Block.FileHash[0] ^= 0xff
	if err = receiveTestResult(t, sessionResult); err != nil {
		t.Fatal(err)
	}

	gotSession, err := store.Collator().Session(context.Background(), wantSession.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotSession, wantSession) {
		t.Fatalf("stored session = %#v, want %#v", gotSession, wantSession)
	}
	gotSession.Session.Validators[0].Weight++
	gotSession.Update.MasterchainBlock.RootHash[0] ^= 0xff
	againSession, err := store.Collator().Session(context.Background(), wantSession.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(againSession, wantSession) {
		t.Fatal("Session returned storage-owned mutable data")
	}

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	blockerDone := make(chan error, 1)
	if err = store.submit(writeRequest{
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

	duplicate := cloneCollatorSessionRecord(t, wantSession)
	duplicateResult := make(chan error, 1)
	store.Collator().SaveSession(context.Background(), duplicate, func(err error) { duplicateResult <- err })
	duplicate.Session.Validators[0].Weight++
	duplicate.Update.MasterchainBlock.RootHash[0] ^= 0xff
	close(releaseWriter)
	if err = receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err = receiveTestResult(t, duplicateResult); err != nil {
		t.Fatalf("queued duplicate observed caller mutation: %v", err)
	}

	candidate := testDelegatedCollatorCandidate(t, wantSession, wantSession.Update.CurrentWindowStart, wantSession.Update.CurrentWindowStart, 3)
	wantCandidateValue, err := encodeCollatorCandidateRecord(candidate)
	if err != nil {
		t.Fatal(err)
	}
	wantCandidate, err := decodeCollatorCandidateRecord(wantCandidateValue)
	if err != nil {
		t.Fatal(err)
	}

	candidateResult := make(chan error, 1)
	store.Collator().SaveCandidate(candidate, func(err error) { candidateResult <- err })
	candidate.Signature[0] ^= 0xff
	candidate.Block.RootHash[0] ^= 0xff
	if err = receiveTestResult(t, candidateResult); err != nil {
		t.Fatal(err)
	}

	gotCandidate, err := store.Collator().Candidate(context.Background(), wantCandidate.WindowID, wantCandidate.ID.Slot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotCandidate, wantCandidate) {
		t.Fatalf("stored candidate = %#v, want %#v", gotCandidate, wantCandidate)
	}
	gotCandidate.Signature[0] ^= 0xff
	gotCandidate.Block.RootHash[0] ^= 0xff
	againCandidate, err := store.Collator().Candidate(context.Background(), wantCandidate.WindowID, wantCandidate.ID.Slot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(againCandidate, wantCandidate) {
		t.Fatal("Candidate returned storage-owned mutable data")
	}

	value, closer, err := store.db.Get(collatorSessionKey(wantSession.Session.ID))
	if err != nil {
		t.Fatal(err)
	}
	storedSessionValue := append([]byte(nil), value...)
	if err = closer.Close(); err != nil {
		t.Fatal(err)
	}
	if storedSessionValue[0] != collatorSessionRecordVersion {
		t.Fatalf("session codec version = %d", storedSessionValue[0])
	}
	corrupted := append([]byte(nil), storedSessionValue...)
	corrupted[0]++
	if _, err = decodeCollatorSessionRecord(corrupted); err == nil {
		t.Fatal("unsupported session record version was accepted")
	}

	validatorSession := testSession(31)
	validatorCandidate := validator.CandidateRecord{ID: testCandidateID(2, 7), Wire: []byte("validator")}
	awaitTestSave(t, func(done func(error)) {
		store.Validator().SaveCandidate(validatorSession, validatorCandidate, done)
	})
	validatorStatus, err := store.Validator().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(validatorStatus.Sessions) != 1 || validatorStatus.Sessions[0].ID != validatorSession {
		t.Fatalf("validator keyspace status = %#v", validatorStatus.Sessions)
	}

	collatorStatus, err := store.Collator().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if collatorStatus.Sessions != 1 || collatorStatus.Candidates != 1 {
		t.Fatalf("collator keyspace status = %+v", collatorStatus)
	}
}

func TestCollatorPhysicalStoresAreIsolated(t *testing.T) {
	local := openTestStore(t, filepath.Join(t.TempDir(), "validator"))
	defer closeTestStore(t, local)
	standalone := openTestStore(t, filepath.Join(t.TempDir(), "collator"))
	defer closeTestStore(t, standalone)

	record := testCollatorSessionRecord(14)
	awaitTestSave(t, func(done func(error)) {
		local.Collator().SaveSession(context.Background(), record, done)
	})
	if _, err := standalone.Collator().Session(context.Background(), record.Session.ID); !errors.Is(err, collator.ErrNotFound) {
		t.Fatalf("standalone database observed local session: %v", err)
	}

	awaitTestSave(t, func(done func(error)) {
		standalone.Collator().SaveSession(context.Background(), record, done)
	})
	localStatus, err := local.Collator().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	standaloneStatus, err := standalone.Collator().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if localStatus.Sessions != 1 || standaloneStatus.Sessions != 1 {
		t.Fatalf("isolated session counts = local:%d standalone:%d", localStatus.Sessions, standaloneStatus.Sessions)
	}
}

func TestCollatorSessionAndCandidateIdempotencyConflictsAndOrdering(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	second := testCollatorSessionRecord(4)
	first := testCollatorSessionRecord(3)
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), second, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), first, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), second, done) })

	advanced := cloneCollatorSessionRecord(t, second)
	advanced.Update.MasterchainBlock = testCollatorBlock(-1, math.MinInt64, second.Update.MasterchainBlock.SeqNo+1, 41)
	advanced.Update.FinalizedBlock = testCollatorBlock(
		second.Session.Shard.Workchain,
		second.Session.Shard.Shard,
		second.Update.FinalizedBlock.SeqNo+1,
		42,
	)
	advanced.Update.CurrentWindowStart += second.Session.SlotsPerLeaderWindow
	advanced.Update.CurrentWindowObservedSlot = advanced.Update.CurrentWindowStart
	advanced.Update.CurrentWindowStartAt = advanced.Update.CurrentWindowStartAt.Add(
		time.Duration(second.Session.SlotsPerLeaderWindow) * second.Update.TargetRate,
	)
	advanced.Update.TargetRate += 25 * time.Millisecond
	advanced.Update.CurrentBase = simplex.Parent(testCollatorCandidateID(
		advanced.Update.CurrentWindowStart-1,
		43,
	))
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), advanced, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), advanced, done) })

	retimed := cloneCollatorSessionRecord(t, advanced)
	retimed.Update.TargetRate += 25 * time.Millisecond
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), retimed, done) })
	advanced = retimed

	storedAdvanced, err := store.Collator().Session(context.Background(), advanced.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedAdvanced.Update.TargetRate != advanced.Update.TargetRate {
		t.Fatalf("stored target rate = %s, want %s", storedAdvanced.Update.TargetRate, advanced.Update.TargetRate)
	}

	for _, targetRate := range []time.Duration{0, -time.Nanosecond} {
		invalid := cloneCollatorSessionRecord(t, advanced)
		invalid.Update.TargetRate = targetRate
		if err = awaitTestSaveError(t, func(done func(error)) {
			store.Collator().SaveSession(context.Background(), invalid, done)
		}); err == nil {
			t.Fatalf("target rate %s was accepted", targetRate)
		}
	}

	if err := awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), second, done)
	}); !errors.Is(err, collator.ErrSessionConflict) {
		t.Fatalf("session rollback error = %v, want ErrSessionConflict", err)
	}

	for _, field := range []string{
		"validator set hash",
		"consensus version",
		"consensus flags",
		"protocol version",
		"QUIC policy",
		"leader window size",
	} {
		changedImmutable := cloneCollatorSessionRecord(t, advanced)
		switch field {
		case "validator set hash":
			changedImmutable.Session.ValidatorSetHash++
		case "consensus version":
			changedImmutable.Session.ConsensusVersion++
		case "consensus flags":
			changedImmutable.Session.ConsensusFlags++
		case "protocol version":
			changedImmutable.Session.ProtocolVersion++
		case "QUIC policy":
			changedImmutable.Session.UseQUIC = !changedImmutable.Session.UseQUIC
		case "leader window size":
			changedImmutable.Session.SlotsPerLeaderWindow++
		}
		if err := awaitTestSaveError(t, func(done func(error)) {
			store.Collator().SaveSession(context.Background(), changedImmutable, done)
		}); !errors.Is(err, collator.ErrSessionConflict) {
			t.Fatalf("immutable %s conflict = %v, want ErrSessionConflict", field, err)
		}
	}

	changedMasterHash := cloneCollatorSessionRecord(t, advanced)
	changedMasterHash.Update.MasterchainBlock.RootHash[0] ^= 1
	if err := awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), changedMasterHash, done)
	}); !errors.Is(err, collator.ErrSessionConflict) {
		t.Fatalf("same-height masterchain conflict = %v, want ErrSessionConflict", err)
	}

	sessions, err := store.Collator().Sessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 || sessions[0].Session.ID != first.Session.ID || sessions[1].Session.ID != second.Session.ID {
		t.Fatalf("session order = %#v", sessions)
	}

	start := advanced.Update.CurrentWindowStart
	candidate := testDelegatedCollatorCandidate(t, advanced, start, start, 5)
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(candidate, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(candidate, done) })

	sameWindowNextSlot := testDelegatedCollatorCandidate(t, advanced, start, start+1, 5)
	sameWindowNextSlot.Parent = simplex.Parent(candidate.ID)
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(sameWindowNextSlot, done) })

	changedCandidate := cloneCollatorCandidate(t, candidate)
	changedCandidate.Signature[0] ^= 1
	if err = awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveCandidate(changedCandidate, done)
	}); !errors.Is(err, collator.ErrCandidateConflict) {
		t.Fatalf("candidate conflict = %v, want ErrCandidateConflict", err)
	}

	if _, err = store.Collator().Candidate(context.Background(), candidate.WindowID, candidate.ID.Slot+3); !errors.Is(err, collator.ErrNotFound) {
		t.Fatalf("missing candidate error = %v, want ErrNotFound", err)
	}
}

func TestCollatorDelegatedCandidateUsesDurableSessionBoundary(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	session := testCollatorSessionRecord(21)
	start := session.Update.CurrentWindowStart
	first := testDelegatedCollatorCandidate(t, session, start, start, 21)
	second := testDelegatedCollatorCandidate(t, session, start, start+1, 21)
	second.Parent = simplex.Parent(first.ID)

	awaitTestSave(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), session, done)
	})
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(first, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(first, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(second, done) })

	stored, err := store.Collator().Candidate(context.Background(), first.WindowID, first.ID.Slot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, first) {
		t.Fatalf("stored delegated candidate = %#v, want %#v", stored, first)
	}
}

func TestCollatorDelegatedCandidateRejectsInvalidAuthorityState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*collator.SessionRecord, *collator.CandidateRecord)
	}{
		{
			name: "activation missing",
			mutate: func(session *collator.SessionRecord, _ *collator.CandidateRecord) {
				session.Activation = nil
			},
		},
		{
			name: "window is not current",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.WindowID.StartSlot -= 4
				candidate.ID.Slot -= 4
			},
		},
		{
			name: "consensus is mid window",
			mutate: func(session *collator.SessionRecord, _ *collator.CandidateRecord) {
				session.Update.CurrentWindowObservedSlot++
			},
		},
		{
			name: "candidate slot is outside window",
			mutate: func(session *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.ID.Slot += session.Session.SlotsPerLeaderWindow
			},
		},
		{
			name: "leader is not scheduled",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.Leader++
			},
		},
		{
			name: "delegation key does not match signature",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.DelegationKey[0] ^= 1
			},
		},
		{
			name: "delegation signature is invalid",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.DelegationSignature[0] ^= 1
			},
		},
		{
			name: "collator signature is invalid",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.Signature[0] ^= 1
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t, t.TempDir())
			defer closeTestStore(t, store)

			session := testCollatorSessionRecord(31)
			candidate := testDelegatedCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart, 31)
			if test.mutate != nil {
				test.mutate(&session, &candidate)
			}

			awaitTestSave(t, func(done func(error)) {
				store.Collator().SaveSession(context.Background(), session, done)
			})
			err := awaitTestSaveError(t, func(done func(error)) {
				store.Collator().SaveCandidate(candidate, done)
			})
			if !errors.Is(err, collator.ErrCandidateConflict) {
				t.Fatalf("SaveCandidate error = %v, want ErrCandidateConflict", err)
			}
		})
	}

	t.Run("mixed authority across slots", func(t *testing.T) {
		store := openTestStore(t, t.TempDir())
		defer closeTestStore(t, store)

		session := testCollatorSessionRecord(32)
		start := session.Update.CurrentWindowStart
		delegated := testDelegatedCollatorCandidate(t, session, start, start, 32)
		self := testSelfCollatorCandidate(t, session, start, start+1)
		self.Parent = simplex.Parent(delegated.ID)

		awaitTestSave(t, func(done func(error)) {
			store.Collator().SaveSession(context.Background(), session, done)
		})
		awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(delegated, done) })
		err := awaitTestSaveError(t, func(done func(error)) {
			store.Collator().SaveCandidate(self, done)
		})
		if !errors.Is(err, collator.ErrCandidateConflict) {
			t.Fatalf("mixed authority after delegated candidate = %v, want ErrCandidateConflict", err)
		}
	})

	t.Run("delegated key must stay exact across slots", func(t *testing.T) {
		store := openTestStore(t, t.TempDir())
		defer closeTestStore(t, store)

		session := testCollatorSessionRecord(33)
		start := session.Update.CurrentWindowStart
		first := testDelegatedCollatorCandidate(t, session, start, start, 33)
		second := testDelegatedCollatorCandidate(t, session, start, start+1, 34)
		second.Parent = simplex.Parent(first.ID)

		awaitTestSave(t, func(done func(error)) {
			store.Collator().SaveSession(context.Background(), session, done)
		})
		awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(first, done) })
		err := awaitTestSaveError(t, func(done func(error)) {
			store.Collator().SaveCandidate(second, done)
		})
		if !errors.Is(err, collator.ErrCandidateConflict) {
			t.Fatalf("different delegation across slots = %v, want ErrCandidateConflict", err)
		}
	})
}

func TestCollatorSelfCandidateUsesDurableSessionBoundary(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	session := testCollatorSessionRecord(41)
	candidate := testSelfCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart)
	awaitTestSave(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), session, done)
	})
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(candidate, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(candidate, done) })

	stored, err := store.Collator().Candidate(context.Background(), candidate.WindowID, candidate.ID.Slot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, candidate) {
		t.Fatalf("stored self candidate = %#v, want %#v", stored, candidate)
	}
}

func TestCollatorSelfCandidateRejectsInvalidAuthorityState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*collator.SessionRecord, *collator.CandidateRecord)
	}{
		{
			name: "activation missing",
			mutate: func(session *collator.SessionRecord, _ *collator.CandidateRecord) {
				session.Activation = nil
			},
		},
		{
			name: "window is not current",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.WindowID.StartSlot -= 4
				candidate.ID.Slot -= 4
			},
		},
		{
			name: "consensus is mid window",
			mutate: func(session *collator.SessionRecord, _ *collator.CandidateRecord) {
				session.Update.CurrentWindowObservedSlot++
			},
		},
		{
			name: "candidate slot is outside window",
			mutate: func(session *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.ID.Slot += session.Session.SlotsPerLeaderWindow
			},
		},
		{
			name: "leader is not scheduled",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.Leader++
			},
		},
		{
			name: "delegation key is set",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.DelegationKey[0] = 1
			},
		},
		{
			name: "delegation signature is set",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.DelegationSignature = []byte{1}
			},
		},
		{
			name: "validator signature is invalid",
			mutate: func(_ *collator.SessionRecord, candidate *collator.CandidateRecord) {
				candidate.Signature[0] ^= 1
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t, t.TempDir())
			defer closeTestStore(t, store)

			session := testCollatorSessionRecord(51)
			candidate := testSelfCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart)
			if test.mutate != nil {
				test.mutate(&session, &candidate)
			}

			awaitTestSave(t, func(done func(error)) {
				store.Collator().SaveSession(context.Background(), session, done)
			})
			err := awaitTestSaveError(t, func(done func(error)) {
				store.Collator().SaveCandidate(candidate, done)
			})
			if !errors.Is(err, collator.ErrCandidateConflict) {
				t.Fatalf("SaveCandidate error = %v, want ErrCandidateConflict", err)
			}
		})
	}

	t.Run("mixed authority across slots", func(t *testing.T) {
		store := openTestStore(t, t.TempDir())
		defer closeTestStore(t, store)

		session := testCollatorSessionRecord(52)
		start := session.Update.CurrentWindowStart
		self := testSelfCollatorCandidate(t, session, start, start)
		delegated := testDelegatedCollatorCandidate(t, session, start, start+1, 52)
		delegated.Parent = simplex.Parent(self.ID)

		awaitTestSave(t, func(done func(error)) {
			store.Collator().SaveSession(context.Background(), session, done)
		})
		awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(self, done) })
		err := awaitTestSaveError(t, func(done func(error)) {
			store.Collator().SaveCandidate(delegated, done)
		})
		if !errors.Is(err, collator.ErrCandidateConflict) {
			t.Fatalf("mixed authority after self candidate = %v, want ErrCandidateConflict", err)
		}
	})

	_, candidate := testSelfCollatorSessionAndCandidate(t, 53)
	candidate.Authority = 0
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)
	if err := awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveCandidate(candidate, done)
	}); !errors.Is(err, collator.ErrCandidateConflict) {
		t.Fatalf("invalid candidate authority = %v, want ErrCandidateConflict", err)
	}
}

func TestCollatorDeleteSessionGateAndIsolation(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	target := testCollatorSessionRecord(7)
	other := testCollatorSessionRecord(8)
	candidate := testDelegatedCollatorCandidate(t, target, target.Update.CurrentWindowStart, target.Update.CurrentWindowStart, 7)
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), target, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), other, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(candidate, done) })

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

	deleteResult := make(chan error, 1)
	go func() {
		deleteResult <- store.Collator().DeleteSession(context.Background(), target.Session.ID)
	}()
	waitForCollatorDeleting(t, store.Collator(), target.Session.ID)
	if err := awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), target, done)
	}); !errors.Is(err, collator.ErrSessionRetired) {
		t.Fatalf("save during delete = %v, want ErrSessionRetired", err)
	}

	close(releaseWriter)
	if err := receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err := receiveTestResult(t, deleteResult); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Collator().Session(context.Background(), target.Session.ID); !errors.Is(err, collator.ErrNotFound) {
		t.Fatalf("deleted session read = %v, want ErrNotFound", err)
	}
	if err := awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveCandidate(candidate, done)
	}); !errors.Is(err, collator.ErrSessionRetired) {
		t.Fatalf("candidate save after delete = %v, want ErrSessionRetired", err)
	}
	if err := awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), target, done)
	}); err != nil {
		t.Fatalf("open replacement session generation: %v", err)
	}

	reopened, err := store.Collator().Session(context.Background(), target.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.Session.Equal(target.Session) || !reopened.Update.Equal(target.Update) {
		t.Fatal("reopened session generation differs from the requested record")
	}
	if _, err := store.Collator().Candidate(context.Background(), candidate.WindowID, candidate.ID.Slot); !errors.Is(err, collator.ErrNotFound) {
		t.Fatalf("deleted candidate read = %v, want ErrNotFound", err)
	}
	if _, err := store.Collator().Session(context.Background(), other.Session.ID); err != nil {
		t.Fatalf("unrelated session was deleted: %v", err)
	}
	if hasCollatorPrefix(t, store, collatorCandidateSessionPrefix(target.Session.ID)) {
		t.Fatal("DeleteSession left child records")
	}
}

func TestCollatorCancelledSessionAdmissionKeepsRetiredFence(t *testing.T) {
	store := openTestStoreWithOptions(t, Options{Dir: t.TempDir(), QueueSize: 1})
	defer closeTestStore(t, store)

	record := testCollatorSessionRecord(71)
	awaitTestSave(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), record, done)
	})
	if err := store.Collator().DeleteSession(context.Background(), record.Session.ID); err != nil {
		t.Fatal(err)
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	saveResult := make(chan error, 1)
	saveReturned := make(chan struct{})
	go func() {
		store.Collator().SaveSession(ctx, record, func(err error) { saveResult <- err })
		close(saveReturned)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if !store.Collator().sessionMu.TryLock() {
			break
		}
		store.Collator().sessionMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("session save did not block in queue admission")
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	if err := receiveTestResult(t, saveResult); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled session admission = %v, want context canceled", err)
	}
	select {
	case <-saveReturned:
	case <-time.After(time.Second):
		t.Fatal("cancelled session save did not return")
	}
	if _, err := store.Collator().Session(context.Background(), record.Session.ID); !errors.Is(err, collator.ErrNotFound) {
		t.Fatalf("cancelled admission reopened retired session: %v", err)
	}

	candidate := testDelegatedCollatorCandidate(t, record, record.Update.CurrentWindowStart, record.Update.CurrentWindowStart, 71)
	if err := awaitTestSaveError(t, func(done func(error)) {
		store.Collator().SaveCandidate(candidate, done)
	}); !errors.Is(err, collator.ErrSessionRetired) {
		t.Fatalf("candidate write after cancelled reopen = %v, want ErrSessionRetired", err)
	}

	close(releaseWriter)
	released = true
	if err := receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err := receiveTestResult(t, fillerDone); err != nil {
		t.Fatal(err)
	}
	waitForNoOutstanding(t, store)
}

func TestCollatorStatusGlobalBacklogAndCloseDrain(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	first := testCollatorSessionRecord(9)
	second := testCollatorSessionRecord(10)
	firstCandidate := testDelegatedCollatorCandidate(t, first, first.Update.CurrentWindowStart, first.Update.CurrentWindowStart, 9)
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), first, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveSession(context.Background(), second, done) })
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(firstCandidate, done) })

	status, err := store.Collator().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Sessions != 2 || status.Candidates != 1 || status.PendingWrites != 0 {
		t.Fatalf("collator status = %+v", status)
	}
	if status.DB.DiskSize == 0 || status.DB.WALSize == 0 {
		t.Fatalf("physical database metrics = %+v", status.DB)
	}

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	blockerDone := make(chan error, 1)
	if err = store.submit(writeRequest{
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

	validatorDone := make(chan error, 1)
	store.Validator().SaveCandidate(
		testSession(55),
		validator.CandidateRecord{ID: testCandidateID(1, 55), Wire: []byte("pending validator")},
		func(err error) { validatorDone <- err },
	)
	third := testCollatorSessionRecord(11)
	collatorDone := make(chan error, 1)
	store.Collator().SaveSession(context.Background(), third, func(err error) { collatorDone <- err })
	waitForOutstanding(t, store, 3)

	status, err = store.Collator().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.PendingWrites != 3 {
		t.Fatalf("global pending writes = %d, want 3", status.PendingWrites)
	}

	close(releaseWriter)
	if err = receiveTestResult(t, blockerDone); err != nil {
		t.Fatal(err)
	}
	if err = receiveTestResult(t, validatorDone); err != nil {
		t.Fatal(err)
	}
	if err = receiveTestResult(t, collatorDone); err != nil {
		t.Fatal(err)
	}
	waitForNoOutstanding(t, store)

	closeWriterEntered := make(chan struct{})
	releaseCloseWriter := make(chan struct{})
	closeBlockerDone := make(chan error, 1)
	if err = store.submit(writeRequest{
		apply: func(*pebble.Batch) error {
			close(closeWriterEntered)
			<-releaseCloseWriter

			return nil
		},
		done: func(err error) { closeBlockerDone <- err },
	}); err != nil {
		t.Fatal(err)
	}
	<-closeWriterEntered

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	closeCallbackResult := make(chan error, 1)
	store.Collator().SaveSession(context.Background(), testCollatorSessionRecord(12), func(err error) {
		close(callbackEntered)
		<-releaseCallback
		closeCallbackResult <- err
	})

	closeResult := make(chan error, 1)
	go func() { closeResult <- store.Close() }()
	waitForClosed(t, store)
	select {
	case err = <-closeResult:
		t.Fatalf("Close returned before accepted write drained: %v", err)
	default:
	}

	close(releaseCloseWriter)
	if err = receiveTestResult(t, closeBlockerDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-callbackEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("accepted collator write did not drain during Close")
	}
	select {
	case err = <-closeResult:
		t.Fatalf("Close returned before accepted callback: %v", err)
	default:
	}

	close(releaseCallback)
	if err = receiveTestResult(t, closeCallbackResult); err != nil {
		t.Fatal(err)
	}
	if err = receiveTestResult(t, closeResult); err != nil {
		t.Fatal(err)
	}

	closedResult := make(chan error, 1)
	store.Collator().SaveSession(
		context.Background(),
		testCollatorSessionRecord(13),
		func(err error) { closedResult <- err },
	)
	if err = receiveTestResult(t, closedResult); !errors.Is(err, collator.ErrClosed) {
		t.Fatalf("save after close = %v, want ErrClosed", err)
	}
	if _, err = store.Collator().Sessions(context.Background()); !errors.Is(err, collator.ErrClosed) {
		t.Fatalf("read after close = %v, want ErrClosed", err)
	}
}

func testCollatorSessionRecord(seed byte) collator.SessionRecord {
	var sessionID [32]byte
	var adnlID [32]byte
	for i := range sessionID {
		sessionID[i] = seed
		adnlID[i] = seed + 2
	}

	validatorPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	var publicKey [ed25519.PublicKeySize]byte
	copy(publicKey[:], validatorPrivate.Public().(ed25519.PublicKey))

	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	master := testCollatorBlock(-1, math.MinInt64, uint32(seed)+100, seed+3)
	finalized := testCollatorBlock(shard.Workchain, shard.Shard, uint32(seed)+20, seed+4)

	return collator.SessionRecord{
		Session: collator.Session{
			ID:                   sessionID,
			Shard:                shard,
			CatchainSeqno:        uint32(seed) + 7,
			ValidatorSetHash:     uint32(seed) + 17,
			ConsensusVersion:     2,
			ConsensusFlags:       3,
			ProtocolVersion:      3,
			UseQUIC:              true,
			SlotsPerLeaderWindow: 4,
			Validators: []collator.SessionValidator{{
				PublicKey: publicKey,
				ADNLID:    adnlID,
				Weight:    10,
			}},
		},
		Activation: &collator.SessionActivation{
			SessionID:      sessionID,
			Genesis:        []ton.BlockIDExt{finalized},
			MinMasterchain: master,
		},
		Update: collator.SessionUpdate{
			SessionID:         sessionID,
			TargetRate:        200 * time.Millisecond,
			MasterchainBlock:  master,
			HasFinalizedBlock: true,
			FinalizedBlock:    finalized,
			Registered: []groups.ShardDescription{{
				Shard:             shard,
				Block:             finalized,
				NextCatchainSeqno: uint32(seed) + 8,
				FSM: groups.ShardFSM{
					Kind:     groups.ShardFSMSplit,
					UTime:    uint32(seed) + 1_000,
					Interval: 60,
				},
			}},
			HasCurrentWindow:          true,
			CurrentWindowStart:        12,
			CurrentWindowObservedSlot: 12,
			CurrentWindowStartAt:      time.Unix(1_800_000_000+int64(seed), 123_456_789).UTC(),
			CurrentBase:               simplex.Genesis(),
		},
	}
}

func testCollatorSessionAndCandidate(t *testing.T, seed byte) (collator.SessionRecord, collator.CandidateRecord) {
	t.Helper()
	session := testCollatorSessionRecord(seed)
	candidate := testDelegatedCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart, seed)
	return session, candidate
}

func testSelfCollatorSessionAndCandidate(t *testing.T, seed byte) (collator.SessionRecord, collator.CandidateRecord) {
	t.Helper()
	session := testCollatorSessionRecord(seed)
	candidate := testSelfCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart)
	return session, candidate
}

func testDelegatedCollatorCandidate(
	t *testing.T,
	session collator.SessionRecord,
	start uint32,
	slot uint32,
	seed byte,
) collator.CandidateRecord {
	t.Helper()

	var collatedHash [32]byte
	for i := range collatedHash {
		collatedHash[i] = seed + byte(i) + 1
	}

	collatorPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed + 64}, ed25519.SeedSize))
	var delegationKey [ed25519.PublicKeySize]byte
	copy(delegationKey[:], collatorPrivate.Public().(ed25519.PublicKey))

	leaderPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{session.Session.ID[0]}, ed25519.SeedSize))
	record := collator.CandidateRecord{
		WindowID: collator.WindowID{
			SessionID: session.Session.ID,
			StartSlot: start,
		},
		Authority:        collator.CandidateAuthorityDelegated,
		ID:               testCollatorCandidateID(slot, seed),
		Parent:           simplex.Genesis(),
		Leader:           0,
		Block:            testCollatorBlock(session.Session.Shard.Workchain, session.Session.Shard.Shard, slot+100, seed+10),
		CollatedFileHash: collatedHash,
		DelegationKey:    delegationKey,
	}

	var err error
	record.DelegationSignature, err = simplex.SignDelegation(
		collatorStoreTestSigner(leaderPrivate),
		session.Session.ID,
		start,
		simplex.KeyNodeIDShort(delegationKey[:]),
	)
	if err != nil {
		t.Fatal(err)
	}
	record.Signature, err = simplex.SignCandidate(
		collatorStoreTestSigner(collatorPrivate),
		session.Session.ID,
		record.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	return record
}

func testSelfCollatorCandidate(
	t *testing.T,
	session collator.SessionRecord,
	start uint32,
	slot uint32,
) collator.CandidateRecord {
	t.Helper()

	var collatedHash [32]byte
	for i := range collatedHash {
		collatedHash[i] = session.Session.ID[0] + byte(i) + 11
	}

	record := collator.CandidateRecord{
		WindowID: collator.WindowID{
			SessionID: session.Session.ID,
			StartSlot: start,
		},
		Authority:        collator.CandidateAuthoritySelf,
		ID:               testCollatorCandidateID(slot, session.Session.ID[0]),
		Parent:           simplex.Genesis(),
		Leader:           0,
		Block:            testCollatorBlock(session.Session.Shard.Workchain, session.Session.Shard.Shard, slot+100, session.Session.ID[0]+10),
		CollatedFileHash: collatedHash,
	}

	validatorPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{session.Session.ID[0]}, ed25519.SeedSize))
	signature, err := simplex.SignCandidate(
		collatorStoreTestSigner(validatorPrivate),
		session.Session.ID,
		record.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	record.Signature = signature

	return record
}

type collatorStoreTestSigner ed25519.PrivateKey

func (s collatorStoreTestSigner) Sign(data []byte) ([]byte, error) {
	return ed25519.Sign(ed25519.PrivateKey(s), data), nil
}

func testCollatorCandidateID(slot uint32, seed byte) simplex.CandidateID {
	var hash [32]byte
	for i := range hash {
		hash[i] = seed + byte(i)
	}

	return simplex.CandidateID{Slot: slot, Hash: hash}
}

func testCollatorBlock(workchain int32, shard int64, seqno uint32, seed byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{seed}, 32),
		FileHash:  bytes.Repeat([]byte{seed + 1}, 32),
	}
}

func cloneCollatorSessionRecord(t *testing.T, record collator.SessionRecord) collator.SessionRecord {
	t.Helper()
	value, err := encodeCollatorSessionRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := decodeCollatorSessionRecord(value)
	if err != nil {
		t.Fatal(err)
	}

	return cloned
}

func cloneCollatorCandidate(t *testing.T, record collator.CandidateRecord) collator.CandidateRecord {
	t.Helper()
	value, err := encodeCollatorCandidateRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := decodeCollatorCandidateRecord(value)
	if err != nil {
		t.Fatal(err)
	}

	return cloned
}

func waitForCollatorDeleting(t *testing.T, store *CollatorStore, sessionID [32]byte) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		store.sessionMu.Lock()
		_, deleting := store.deleting[sessionID]
		store.sessionMu.Unlock()
		if deleting {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatal("collator session did not enter deletion gate")
}

func hasCollatorPrefix(t *testing.T, store *Store, prefix []byte) bool {
	t.Helper()
	lower, upper := prefixBounds(prefix)
	iter, err := store.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		t.Fatal(err)
	}
	result := iter.First()
	if err = errors.Join(iter.Error(), iter.Close()); err != nil {
		t.Fatal(err)
	}

	return result
}

func TestCollatorCodecPreservesEmptyCandidate(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer closeTestStore(t, store)

	session := testCollatorSessionRecord(14)
	record := testDelegatedCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart, 14)
	record.Empty = true
	record.Parent = simplex.Parent(testCollatorCandidateID(record.ID.Slot-1, 13))
	record.CollatedFileHash = [32]byte{}

	value, err := encodeCollatorCandidateRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCollatorCandidateRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.Empty || !reflect.DeepEqual(decoded, record) {
		t.Fatalf("empty candidate roundtrip = %#v, want %#v", decoded, record)
	}

	awaitTestSave(t, func(done func(error)) {
		store.Collator().SaveSession(context.Background(), session, done)
	})
	awaitTestSave(t, func(done func(error)) { store.Collator().SaveCandidate(record, done) })
	stored, err := store.Collator().Candidate(context.Background(), record.WindowID, record.ID.Slot)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Empty || !reflect.DeepEqual(stored, record) {
		t.Fatalf("stored empty candidate = %#v, want %#v", stored, record)
	}
}

func TestCollatorCandidateRecordStaysBounded(t *testing.T) {
	session := testCollatorSessionRecord(15)
	record := testDelegatedCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart, 15)

	value, err := encodeCollatorCandidateRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if cap(value) != len(value) {
		t.Fatalf("candidate encoding capacity = %d, want exact %d", cap(value), len(value))
	}
	if len(value) > collatorCandidateFixedSize+len(record.Signature)+len(record.DelegationSignature) {
		t.Fatalf("candidate marker encodes to %d bytes; it must not carry payloads", len(value))
	}

	decoded, err := decodeCollatorCandidateRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	for i := range value {
		value[i] ^= 0xff
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatal("decoded candidate aliases the storage value instead of owning its bytes")
	}
}

func TestCollatorCandidateCodecRejectsInvalidAuthority(t *testing.T) {
	session := testCollatorSessionRecord(16)
	record := testDelegatedCollatorCandidate(t, session, session.Update.CurrentWindowStart, session.Update.CurrentWindowStart, 16)

	value, err := encodeCollatorCandidateRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if value[0] != collatorCandidateRecordVersion ||
		value[len(value)-1] != byte(collator.CandidateAuthorityDelegated) {
		t.Fatalf("candidate authority = %d", value[len(value)-1])
	}
	invalid := append([]byte(nil), value...)
	invalid[len(invalid)-1] = 0
	if _, err = decodeCollatorCandidateRecord(invalid); err == nil {
		t.Fatal("invalid candidate authority was accepted")
	}
}
