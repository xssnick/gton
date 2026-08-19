package validator

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func reaperTestNamespace(session byte, shard groups.ShardID, identity [32]byte, seqno uint32) SessionStorageID {
	return SessionStorageID{
		SessionID:      supervisorTestID(session),
		Shard:          shard,
		CatchainSeqno:  seqno,
		IsValidator:    true,
		ValidatorKeyID: identity,
		LocalADNLID:    identity,
		ValidatorIndex: 0,
	}
}

func reaperTestDesired(ids ...SessionStorageID) map[sessionActorID]desiredSession {
	desired := make(map[sessionActorID]desiredSession, len(ids))
	for _, id := range ids {
		desired[sessionActorID{
			SessionID:   id.SessionID,
			LocalIndex:  id.ValidatorIndex,
			LocalADNLID: id.LocalADNLID,
		}] = desiredSession{config: SessionConfig{StorageID: id}}
	}

	return desired
}

// reaperTestClaims builds the claimed set through the supervisor's own builder,
// so every reaper test also pins that the two agree on what identity a claim is
// keyed by. Keyed by anything finer than the namespace, a live session stops
// protecting its own storage.
func reaperTestClaims(t *testing.T, ids ...SessionStorageID) map[sessionNamespaceKey]struct{} {
	t.Helper()

	claimed, err := claimedNamespaces(reaperTestDesired(ids...), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	return claimed
}

func newReaperTest(t *testing.T) (*sessionReaper, *validatorTestStorage) {
	t.Helper()

	storage := newValidatorTestStorage()
	log := zerolog.Nop()

	return newSessionReaper(storage, &log), storage
}

// TestReaperDeletesSupersededValidatorNamespaces is the regression test for the
// bug: nothing ever deleted a validator session's durable namespace, so every
// session this node had ever run stayed on disk in full.
func TestReaperDeletesSupersededValidatorNamespaces(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x41)
	current := reaperTestNamespace(0xa3, shard, identity, 9)
	older := reaperTestNamespace(0xa1, shard, identity, 7)
	oldest := reaperTestNamespace(0xa2, shard, identity, 8)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(older, oldest, current)
	reaper.reap(
		context.Background(),
		reaperTestDesired(current),
		reaperTestClaims(t, current),
		time.Now(),
	)

	remaining := storage.storedNamespaces()
	if len(remaining) != 1 || remaining[0] != current {
		t.Fatalf("remaining namespaces = %+v, want only the desired generation", remaining)
	}
	if deleted := storage.deletedNamespaces(); len(deleted) != 2 {
		t.Fatalf("deleted namespaces = %+v, want both superseded generations", deleted)
	}
}

// TestReaperKeepsTheLiveGeneration is the safety test. A namespace holds the
// vote journal, and losing a live journal loses double-vote protection, so the
// reaper must never delete a namespace it cannot prove superseded. The three
// shapes here are the ones the supervisor actually produces: the desired
// generation itself, a lineage whose shard has momentarily vanished from the
// snapshot, and a generation newer than anything desired.
func TestReaperKeepsTheLiveGeneration(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x42)
	live := reaperTestNamespace(0xb1, shard, identity, 7)
	newer := reaperTestNamespace(0xb2, shard, identity, 8)

	t.Run("equal seqno is not superseded", func(t *testing.T) {
		reaper, storage := newReaperTest(t)
		storage.setNamespaces(live)
		// Same lineage, same catchain seqno, different session id: a rotation
		// that reused the generation must not take the running journal with it.
		desired := reaperTestNamespace(0xb3, shard, identity, 7)
		reaper.reap(
			context.Background(),
			reaperTestDesired(desired),
			reaperTestClaims(t, desired),
			time.Now(),
		)
		if len(storage.deletedNamespaces()) != 0 {
			t.Fatalf("deleted an equal generation: %+v", storage.deletedNamespaces())
		}
	})

	t.Run("empty desired set deletes nothing", func(t *testing.T) {
		reaper, storage := newReaperTest(t)
		storage.setNamespaces(live, newer)
		// desiredSessions returns an empty map whenever the group snapshot is not
		// ready or lifecycle is disabled, and every managed session is retired in
		// that pass. Retirement is therefore not evidence of supersession.
		reaper.reap(
			context.Background(),
			reaperTestDesired(),
			reaperTestClaims(t),
			time.Now(),
		)
		if len(storage.deletedNamespaces()) != 0 {
			t.Fatalf("empty desired set deleted %+v", storage.deletedNamespaces())
		}
		if storage.scanCount() != 0 {
			t.Fatal("empty desired set scanned the namespace index")
		}
	})

	t.Run("stored generation newer than desired survives", func(t *testing.T) {
		reaper, storage := newReaperTest(t)
		storage.setNamespaces(newer)
		desired := reaperTestNamespace(0xb4, shard, identity, 7)
		reaper.reap(
			context.Background(),
			reaperTestDesired(desired),
			reaperTestClaims(t, desired),
			time.Now(),
		)
		if len(storage.deletedNamespaces()) != 0 {
			t.Fatalf("deleted a newer generation: %+v", storage.deletedNamespaces())
		}
	})
}

// TestReaperKeepsClaimedAndForeignNamespaces covers the two exclusions that are
// not about the catchain seqno: a namespace a live actor still owns, and one
// belonging to another lineage or role entirely.
func TestReaperKeepsClaimedAndForeignNamespaces(t *testing.T) {
	shard := supervisorTestShard()
	masterchain := groups.ShardID{Workchain: -1, Shard: math.MinInt64}
	identity := supervisorTestID(0x43)
	other := supervisorTestID(0x44)

	desired := reaperTestNamespace(0xc9, shard, identity, 9)
	// Superseded by catchain seqno, but a session that has not finished closing
	// is still writing to it.
	claimed := reaperTestNamespace(0xc1, shard, identity, 8)
	// The consensus observer owns observer namespaces; the supervisor cannot see
	// which of them are still desired.
	observer := SessionStorageID{
		SessionID:      supervisorTestID(0xc2),
		Shard:          shard,
		CatchainSeqno:  8,
		IsValidator:    false,
		LocalADNLID:    identity,
		ValidatorIndex: -1,
	}
	otherShard := reaperTestNamespace(0xc3, masterchain, identity, 8)
	otherIdentity := reaperTestNamespace(0xc4, shard, other, 8)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(claimed, observer, otherShard, otherIdentity)
	reaper.reap(
		context.Background(),
		reaperTestDesired(desired),
		reaperTestClaims(t, desired, claimed),
		time.Now(),
	)

	if deleted := storage.deletedNamespaces(); len(deleted) != 0 {
		t.Fatalf("deleted namespaces = %+v, want none", deleted)
	}
}

// TestReaperScansOncePerGeneration keeps the namespace index scan off every
// reconciliation. The supervisor reconciles on every masterchain block; a
// rotation happens on the order of once per catchain lifetime.
func TestReaperScansOncePerGeneration(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x45)
	desired := reaperTestNamespace(0xd1, shard, identity, 7)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(reaperTestNamespace(0xd0, shard, identity, 6))
	claimed := reaperTestClaims(t, desired)
	for range 3 {
		reaper.reap(context.Background(), reaperTestDesired(desired), claimed, time.Now())
	}
	if storage.scanCount() != 1 {
		t.Fatalf("namespace scans = %d, want 1 for one generation", storage.scanCount())
	}

	next := reaperTestNamespace(0xd2, shard, identity, 8)
	storage.setNamespaces(desired)
	reaper.reap(
		context.Background(),
		reaperTestDesired(next),
		reaperTestClaims(t, next),
		time.Now(),
	)
	if storage.scanCount() != 2 {
		t.Fatalf("namespace scans after a rotation = %d, want 2", storage.scanCount())
	}
	if deleted := storage.deletedNamespaces(); len(deleted) != 2 {
		t.Fatalf("deleted namespaces = %+v, want both superseded generations", deleted)
	}
}

// TestReaperRetriesAfterAFailedPass pins that a failure does not consume the
// generation: the namespaces would otherwise leak until the next rotation, and
// a persistently failing store would leak forever.
func TestReaperRetriesAfterAFailedPass(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x46)
	desired := reaperTestNamespace(0xe1, shard, identity, 7)
	stale := reaperTestNamespace(0xe0, shard, identity, 6)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(stale)
	storage.deleteErr = errors.New("write queue is closed")
	claimed := reaperTestClaims(t, desired)
	now := time.Now()
	reaper.reap(context.Background(), reaperTestDesired(desired), claimed, now)
	if len(storage.deletedNamespaces()) != 0 || storage.scanCount() != 1 {
		t.Fatalf("failed pass deleted %+v after %d scans", storage.deletedNamespaces(), storage.scanCount())
	}

	// The backoff holds the retry off the next reconciliation.
	reaper.reap(context.Background(), reaperTestDesired(desired), claimed, now.Add(time.Second))
	if storage.scanCount() != 1 {
		t.Fatalf("retried within the backoff: scans = %d", storage.scanCount())
	}

	storage.deleteErr = nil
	reaper.reap(context.Background(), reaperTestDesired(desired), claimed, now.Add(2*sessionReapRetryDelay))
	if deleted := storage.deletedNamespaces(); len(deleted) != 1 || deleted[0] != stale {
		t.Fatalf("retried pass deleted %+v, want the stale namespace", deleted)
	}
}

// TestSupervisorReapsStaleValidatorNamespacesOnStartup drives the whole path a
// crash leaves behind: namespaces orphaned between a rotation and a teardown
// that never ran. The first reconciliation with a ready snapshot is what reaps
// them, because every lineage is new to the watermark then.
func TestSupervisorReapsStaleValidatorNamespacesOnStartup(t *testing.T) {
	localKey := supervisorTestID(0x47)
	shard := supervisorTestShard()
	keys := &supervisorTestKeys{ids: [][32]byte{localKey}}
	storage := newValidatorTestStorage()
	preparer := newSupervisorTestPreparer()

	// supervisorTestGroup pins catchain seqno 7, so these two are strictly
	// older generations of the same lineage, and the third is another shard.
	stale := reaperTestNamespace(0xf1, shard, localKey, 5)
	staler := reaperTestNamespace(0xf2, shard, localKey, 6)
	foreign := reaperTestNamespace(
		0xf3,
		groups.ShardID{Workchain: -1, Shard: math.MinInt64},
		localKey,
		5,
	)
	storage.setNamespaces(stale, staler, foreign)

	supervisor := newSessionSupervisor(zerolog.Nop(), keys, storage, preparer.prepare)
	defer supervisor.Close()

	active := supervisorTestGroup(0xf0, shard, localKey)
	supervisor.Reconcile(supervisorTestSnapshot(90, []groups.Session{active}, nil))
	supervisor.Start(context.Background())

	waitFor(t, func() bool {
		return len(storage.deletedNamespaces()) == 2
	}, "stale validator namespaces were not reaped")

	remaining := storage.storedNamespaces()
	if len(remaining) != 1 || remaining[0] != foreign {
		t.Fatalf("remaining namespaces = %+v, want only the untouched masterchain lineage", remaining)
	}
}

// TestReaperRescansWhileASupersededNamespaceIsStillClaimed pins that a pass
// which could not finish its work does not record itself as done. A session
// whose Close failed keeps its claim and is retried a second later; without
// this, the namespace it holds would wait for the next rotation — a whole
// catchain generation — before any pass looked at it again.
func TestReaperRescansWhileASupersededNamespaceIsStillClaimed(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x48)
	desired := reaperTestNamespace(0xa9, shard, identity, 9)
	stuck := reaperTestNamespace(0xa8, shard, identity, 8)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(stuck)
	stillClosing := reaperTestClaims(t, desired, stuck)
	reaper.reap(context.Background(), reaperTestDesired(desired), stillClosing, time.Now())
	if len(storage.deletedNamespaces()) != 0 || storage.scanCount() != 1 {
		t.Fatalf("claimed namespace deleted %+v after %d scans", storage.deletedNamespaces(), storage.scanCount())
	}

	// The session finally closed. The very next reconciliation must reap it
	// rather than treat this lineage as already handled.
	reaper.reap(
		context.Background(),
		reaperTestDesired(desired),
		reaperTestClaims(t, desired),
		time.Now(),
	)
	if deleted := storage.deletedNamespaces(); len(deleted) != 1 || deleted[0] != stuck {
		t.Fatalf("deleted = %+v after the claim was released, want the stuck namespace", deleted)
	}

	// With nothing left blocked, the lineage is finally recorded as done.
	reaper.reap(
		context.Background(),
		reaperTestDesired(desired),
		reaperTestClaims(t, desired),
		time.Now(),
	)
	// Two scans: the blocked pass and the one that finished the work. The third
	// call finds the lineage recorded as done and does not touch the index.
	if storage.scanCount() != 2 {
		t.Fatalf("namespace scans = %d, want the completed pass to stop rescanning", storage.scanCount())
	}
}

// TestReaperKeepsALiveNamespaceWhoseDescriptorProtocolDiffers is the regression
// test for the identity mismatch. Protocol is excluded from Namespace by
// design, so a stored descriptor and a live claim that differ only in it are
// one namespace with one vote journal — while, keyed by SessionStorageID, they
// are two different map entries and the live one looks unclaimed.
//
// The trigger is ordinary: StorageID.Protocol is rebuilt from config 29/30 on
// every reconciliation, and the descriptor keeps whatever the session was
// opened with, so any change to the consensus parameters while a rotation
// overlap is running produces exactly this pair.
func TestReaperKeepsALiveNamespaceWhoseDescriptorProtocolDiffers(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x51)

	// The running session, as its descriptor was written.
	stored := reaperTestNamespace(0x52, shard, identity, 7)
	stored.Protocol = SessionProtocol{Version: 2, ProtocolVersion: 1, SlotsPerLeaderWindow: 4}
	// The very same namespace, as this reconciliation describes it.
	live := stored
	live.Protocol.SlotsPerLeaderWindow = 8
	if storedKey, liveKey := mustNamespaceKey(t, stored), mustNamespaceKey(t, live); storedKey != liveKey {
		t.Fatalf("protocol changed the namespace key: %+v vs %+v", storedKey, liveKey)
	}
	// A newer generation of the same lineage is what makes the stored one look
	// superseded, and it is also the pass's real work.
	next := reaperTestNamespace(0x53, shard, identity, 8)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(stored, reaperTestNamespace(0x54, shard, identity, 6))
	storage.Journal(stored, 100).SaveOurVote(simplex.NotarizeVote(simplex.CandidateID{Slot: 12}), func(error) {})
	journal := storage.journalFor(stored)

	reaper.reap(
		context.Background(),
		reaperTestDesired(live, next),
		reaperTestClaims(t, live, next),
		time.Now(),
	)

	for _, deleted := range storage.deletedNamespaces() {
		if deleted.SessionID == stored.SessionID {
			t.Fatal("deleted the namespace of a live session that reports a different protocol")
		}
	}
	votes, wiped := journal.recordedVotes()
	if wiped || len(votes) != 1 {
		t.Fatalf("live vote journal: votes=%d wiped=%t, want the recorded vote intact", len(votes), wiped)
	}
}

// TestReaperKeepsALiveGenerationsVoteJournal is the property the identity rule
// exists for. The pass below does real work — it reaps a genuinely superseded
// generation — and the live generation's journal must come through it holding
// the vote it already cast, because a node that reopens a slot with an empty
// journal can vote in it twice.
//
// The live descriptor differs from the desired one in both fields Namespace
// excludes, which is the shape a validator set rotation plus a config change
// produces at the same time.
func TestReaperKeepsALiveGenerationsVoteJournal(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x55)

	stored := reaperTestNamespace(0x56, shard, identity, 9)
	stored.ValidatorIndex = 3
	stored.Protocol = SessionProtocol{Version: 2, ProtocolVersion: 2, SlotsPerLeaderWindow: 4}
	live := stored
	live.ValidatorIndex = 11
	live.Protocol.UseQUIC = true

	superseded := reaperTestNamespace(0x57, shard, identity, 8)
	newer := reaperTestNamespace(0x58, shard, identity, 10)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(superseded, stored)
	vote := simplex.NotarizeVote(simplex.CandidateID{Slot: 41, Hash: [32]byte{0x9a}})
	storage.Journal(stored, 100).SaveOurVote(vote, func(error) {})
	journal := storage.journalFor(stored)

	reaper.reap(
		context.Background(),
		reaperTestDesired(live, newer),
		reaperTestClaims(t, live, newer),
		time.Now(),
	)

	deleted := storage.deletedNamespaces()
	if len(deleted) != 1 || deleted[0] != superseded {
		t.Fatalf("deleted = %+v, want only the superseded generation", deleted)
	}
	votes, wiped := journal.recordedVotes()
	if wiped || len(votes) != 1 || votes[0] != vote {
		t.Fatalf("live vote journal: votes=%+v wiped=%t, want the cast vote intact", votes, wiped)
	}
}

// TestReaperReapsPastAnUndeletableNamespace pins that one namespace the store
// refuses does not abandon the rest of the pass. DeleteSession rejects a
// namespace whose summary row is missing or undecodable, and a single such
// leftover used to end the loop and take every other superseded lineage on the
// node with it — silently restoring the unbounded growth this reaper removes.
func TestReaperReapsPastAnUndeletableNamespace(t *testing.T) {
	shard := supervisorTestShard()
	masterchain := groups.ShardID{Workchain: -1, Shard: math.MinInt64}
	identity := supervisorTestID(0x59)

	poison := reaperTestNamespace(0x5a, shard, identity, 6)
	reapable := reaperTestNamespace(0x5b, shard, identity, 7)
	otherLineage := reaperTestNamespace(0x5c, masterchain, identity, 4)

	desiredShard := reaperTestNamespace(0x5d, shard, identity, 8)
	desiredMaster := reaperTestNamespace(0x5e, masterchain, identity, 5)

	reaper, storage := newReaperTest(t)
	// Enumeration order puts the poison first, which is the only order in which
	// the old shape could be observed at all.
	storage.setNamespaces(poison, reapable, otherLineage)
	storage.failDelete(poison, errors.New("validator pebblestore: session summary is missing"))

	reaper.reap(
		context.Background(),
		reaperTestDesired(desiredShard, desiredMaster),
		reaperTestClaims(t, desiredShard, desiredMaster),
		time.Now(),
	)

	deleted := storage.deletedNamespaces()
	if len(deleted) != 2 {
		t.Fatalf("deleted = %+v, want the two deletable namespaces behind the poison one", deleted)
	}
	remaining := storage.storedNamespaces()
	if len(remaining) != 1 || remaining[0] != poison {
		t.Fatalf("remaining = %+v, want only the undeletable namespace", remaining)
	}
}

// TestReaperAbandonsAPermanentlyUndeletableNamespace is the poison-record
// policy: a namespace the store will never accept is retried a bounded number
// of times and then left alone with an Error, rather than holding its lineage's
// watermark down and rescanning the whole index once per backoff forever.
func TestReaperAbandonsAPermanentlyUndeletableNamespace(t *testing.T) {
	shard := supervisorTestShard()
	identity := supervisorTestID(0x5f)
	poison := reaperTestNamespace(0x60, shard, identity, 6)
	desired := reaperTestNamespace(0x61, shard, identity, 7)

	reaper, storage := newReaperTest(t)
	storage.setNamespaces(poison)
	storage.failDelete(poison, errors.New("validator pebblestore: session summary is missing"))

	now := time.Now()
	claims := reaperTestClaims(t, desired)
	for attempt := range sessionReapMaxDeleteAttempts {
		reaper.reap(context.Background(), reaperTestDesired(desired), claims, now)
		if got := storage.deleteAttempts(); got != attempt+1 {
			t.Fatalf("delete attempts after pass %d = %d", attempt+1, got)
		}
		// Every failing pass keeps the lineage unfinished, so the next
		// reconciliation past the backoff scans again instead of waiting a whole
		// catchain generation.
		now = now.Add(2 * sessionReapRetryDelay)
	}

	// The pass that exhausts the attempts is also the one that gives up, so the
	// lineage is recorded as done and an unchanged generation stops rescanning.
	reaper.reap(context.Background(), reaperTestDesired(desired), claims, now)
	if scans := storage.scanCount(); scans != sessionReapMaxDeleteAttempts {
		t.Fatalf("namespace scans = %d, want the abandoned lineage to stop rescanning", scans)
	}

	// A later rotation scans the index again and still must not re-offer it.
	rotated := reaperTestNamespace(0x62, shard, identity, 8)
	reaper.reap(
		context.Background(),
		reaperTestDesired(rotated),
		reaperTestClaims(t, rotated),
		now.Add(4*sessionReapRetryDelay),
	)
	if scans := storage.scanCount(); scans != sessionReapMaxDeleteAttempts+1 {
		t.Fatalf("namespace scans after a rotation = %d, want one more", scans)
	}
	if got := storage.deleteAttempts(); got != sessionReapMaxDeleteAttempts {
		t.Fatalf("delete attempts after abandonment = %d, want %d", got, sessionReapMaxDeleteAttempts)
	}
	if remaining := storage.storedNamespaces(); len(remaining) != 1 || remaining[0] != poison {
		t.Fatalf("remaining = %+v, want the abandoned namespace left on disk", remaining)
	}
}

func mustNamespaceKey(t *testing.T, id SessionStorageID) sessionNamespaceKey {
	t.Helper()

	key, err := sessionNamespaceKeyOf(id)
	if err != nil {
		t.Fatal(err)
	}

	return key
}

// TestSessionNamespaceKeyCollapsesExactlyTheDescriptorOnlyFields is the
// validator-side half of the identity statement the reaper depends on.
// pebblestore's TestStoredNamespaceCollapsesExactlyTheDescriptorOnlyFields makes
// the same claim about namespaceForSession; keeping them equal is what makes a
// claim neither finer nor coarser than the namespace it protects.
func TestSessionNamespaceKeyCollapsesExactlyTheDescriptorOnlyFields(t *testing.T) {
	base := reaperTestNamespace(0x63, supervisorTestShard(), supervisorTestID(0x64), 5)
	base.ValidatorIndex = 2
	base.Protocol = SessionProtocol{Version: 2, ProtocolVersion: 1, SlotsPerLeaderWindow: 4}
	baseKey := mustNamespaceKey(t, base)

	for _, testCase := range []struct {
		field     string
		mutate    func(*SessionStorageID)
		addresses bool
	}{
		{"SessionID", func(id *SessionStorageID) { id.SessionID[0] ^= 0xff }, true},
		{"Shard", func(id *SessionStorageID) {
			id.Shard = groups.ShardID{Workchain: -1, Shard: math.MinInt64}
		}, true},
		{"CatchainSeqno", func(id *SessionStorageID) { id.CatchainSeqno++ }, true},
		{"IsValidator", func(id *SessionStorageID) {
			id.IsValidator = false
			id.ValidatorKeyID = [32]byte{}
			id.ValidatorIndex = simplex.ObserverIndex
		}, true},
		{"ValidatorKeyID", func(id *SessionStorageID) { id.ValidatorKeyID[0] ^= 0xff }, true},
		{"LocalADNLID", func(id *SessionStorageID) { id.LocalADNLID[0] ^= 0xff }, true},
		{"ValidatorIndex", func(id *SessionStorageID) { id.ValidatorIndex += 7 }, false},
		{"Protocol", func(id *SessionStorageID) { id.Protocol.SlotsPerLeaderWindow += 4 }, false},
	} {
		mutated := base
		testCase.mutate(&mutated)
		if changed := mustNamespaceKey(t, mutated) != baseKey; changed != testCase.addresses {
			t.Fatalf("%s changed the namespace key = %t, want %t", testCase.field, changed, testCase.addresses)
		}
	}
}
