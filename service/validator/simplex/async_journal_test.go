package simplex

import (
	"bytes"
	"errors"
	"testing"
)

var errWriteInjected = errors.New("injected write failure")

// deferredJournal wraps MemoryJournal and captures completions of selected
// record kinds so a test can release them by hand, in any order. The
// underlying write (and its dedup/fault gates) still runs at call time — only
// the completion delivery is deferred, which is exactly the shape of a
// WAL-backed journal waiting for fsync.
type deferredJournal struct {
	inner   *MemoryJournal
	deferOn map[string]bool
	pending []func()
}

func newDeferredJournal(inner *MemoryJournal, kinds ...string) *deferredJournal {
	d := &deferredJournal{inner: inner, deferOn: map[string]bool{}}
	for _, k := range kinds {
		d.deferOn[k] = true
	}
	return d
}

func (d *deferredJournal) wrap(kind string, done func(error)) func(error) {
	if !d.deferOn[kind] {
		return done
	}
	return func(err error) {
		d.pending = append(d.pending, func() { done(err) })
	}
}

func (d *deferredJournal) Bootstrap() (*BootstrapState, error) { return d.inner.Bootstrap() }

func (d *deferredJournal) SaveOurVote(v Vote, done func(error)) {
	d.inner.SaveOurVote(v, d.wrap("vote", done))
}

func (d *deferredJournal) SaveCertificate(c *Certificate, done func(error)) {
	d.inner.SaveCertificate(c, d.wrap("cert", done))
}

func (d *deferredJournal) SaveFirstNonAnnouncedWindow(w uint32, done func(error)) {
	d.inner.SaveFirstNonAnnouncedWindow(w, d.wrap("pool", done))
}

// releaseAt delivers the i-th captured completion.
func (d *deferredJournal) releaseAt(t *testing.T, i int) {
	t.Helper()
	if i >= len(d.pending) {
		t.Fatalf("no pending completion %d (have %d)", i, len(d.pending))
	}
	f := d.pending[i]
	d.pending = append(d.pending[:i], d.pending[i+1:]...)
	f()
}

// TestVoteAppliedOnlyAfterDurable: an own vote is signed, applied and
// broadcast strictly after its journal record became durable; the engine keeps
// accepting input meanwhile.
func TestVoteAppliedOnlyAfterDurable(t *testing.T) {
	var dj *deferredJournal
	env := newTestEnv(t, withLocal(1), func(env *testEnv, cfg *Config) {
		dj = newDeferredJournal(env.journal, "vote")
		cfg.Journal = dj
	})
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x21)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.completeValidation(cand.ID, nil)

	// The intent is recorded in the voter, the pool and the wire see nothing.
	requireEqual(t, env.eng.voter.slots.at(0).votedNotar != nil, true, "voter intent recorded")
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "no broadcast before durability")
	requireEqual(t, env.eng.Stats().OwnVotesCast, uint64(0), "not applied before durability")
	requireEqual(t, env.eng.slots.at(0).votes[1].notarize.set, false, "no pool record before durability")

	// The engine keeps serving other inputs while the write is in flight.
	env.deliverVote(2, NotarizeVote(cand.ID))
	env.deliverVote(3, NotarizeVote(cand.ID))
	requireEqual(t, len(env.hooks.notarized), 0, "2 of 4 votes: no quorum without ours")

	dj.releaseAt(t, 0)
	requireEqual(t, env.trans.countVotes(VoteNotarize), 1, "broadcast after durability")
	requireEqual(t, env.eng.slots.at(0).votes[1].notarize.set, true, "applied after durability")
	// Our completed vote closes the quorum built while the write was pending.
	requireEqual(t, len(env.hooks.notarized), 1, "certificate after our vote landed")
	env.requireNoFatal()
}

// TestVoteSaveFailureFatalOnCompletion: a failed vote write kills the session
// when the completion arrives, and the vote is never signed or sent.
func TestVoteSaveFailureFatalOnCompletion(t *testing.T) {
	var dj *deferredJournal
	env := newTestEnv(t, withLocal(1), func(env *testEnv, cfg *Config) {
		dj = newDeferredJournal(env.journal, "vote")
		cfg.Journal = dj
	})
	env.start()
	env.journal.OnBeforeWrite(func(kind string) error {
		if kind == "vote" {
			return errWriteInjected
		}
		return nil
	})

	cand := env.makeCandidate(0, Genesis(), 0x22)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.completeValidation(cand.ID, nil)
	requireEqual(t, len(env.hooks.fatals), 0, "no fatal before completion")

	dj.releaseAt(t, 0)
	requireEqual(t, len(env.hooks.fatals), 1, "write failure is fatal on completion")
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "vote never sent")
}

// TestCertificateAppliedOnlyAfterDurable: a certificate is stored, gossiped
// and announced downstream only after its record became durable; the saving
// flag suppresses duplicates in flight.
func TestCertificateAppliedOnlyAfterDurable(t *testing.T) {
	var dj *deferredJournal
	env := newTestEnv(t, withLocal(1), func(env *testEnv, cfg *Config) {
		dj = newDeferredJournal(env.journal, "cert")
		cfg.Journal = dj
	})
	env.start()

	id := candID(0, 0x31)
	env.deliverVote(0, NotarizeVote(id))
	env.deliverVote(2, NotarizeVote(id))
	env.deliverVote(3, NotarizeVote(id))

	requireEqual(t, len(env.hooks.notarized), 0, "no notarization before durability")
	requireEqual(t, len(env.trans.gossips), 0, "no gossip before durability")
	requireEqual(t, env.eng.slots.at(0).notarCert == nil, true, "no stored cert before durability")

	// A network copy of the same certificate is ignored while the save runs.
	env.eng.HandleMessage(peer(2), 2, env.buildCert(NotarizeVote(id), 0, 2, 3).Serialize())
	requireEqual(t, len(dj.pending), 1, "duplicate did not start a second save")

	dj.releaseAt(t, 0)
	requireEqual(t, len(env.hooks.notarized), 1, "notarization after durability")
	requireEqual(t, len(env.trans.gossips), 1, "gossip after durability")
	env.requireNoFatal()
}

// TestCertificateObsoletedByFinalizationInFlight: a finalization that prunes
// the slot while its certificate is still persisting must not corrupt state —
// the late completion is dropped and the slot is re-resolved afterwards.
func TestCertificateObsoletedByFinalizationInFlight(t *testing.T) {
	var dj *deferredJournal
	env := newTestEnv(t, withLocal(1), func(env *testEnv, cfg *Config) {
		dj = newDeferredJournal(env.journal, "cert")
		cfg.Journal = dj
	})
	env.start()

	// Slot 0 notarization quorum: save 0 in flight.
	id0 := candID(0, 0x41)
	env.deliverVote(0, NotarizeVote(id0))
	env.deliverVote(2, NotarizeVote(id0))
	env.deliverVote(3, NotarizeVote(id0))
	// Slot 1 finalization certificate from the network: save 1 in flight.
	id1 := candID(1, 0x42)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(FinalizeVote(id1), 0, 2, 3).Serialize())
	requireEqual(t, len(dj.pending), 2, "two saves in flight")

	// The finalization completes first and prunes slot 0.
	dj.releaseAt(t, 1)
	requireEqual(t, len(env.hooks.finalized), 1, "slot 1 finalized")
	requireEqual(t, env.eng.slots.firstNonFinalized, uint32(2), "horizon advanced")

	// The obsolete slot-0 completion is dropped without touching anything.
	dj.releaseAt(t, 0)
	requireEqual(t, len(env.hooks.notarized), 0, "pruned slot certificate dropped")
	requireEqual(t, env.eng.Stats().CertificatesStored, uint64(1), "only the finalization stored")
	env.requireNoFatal()
}

// TestWindowAnnouncesWaitForDurabilityAndStayOrdered: consensus windows are
// announced only after their pool-state record is durable, and always in
// window order even when journal completions invert.
func TestWindowAnnouncesWaitForDurabilityAndStayOrdered(t *testing.T) {
	var dj *deferredJournal
	env := newTestEnv(t, withLocal(0), func(env *testEnv, cfg *Config) {
		dj = newDeferredJournal(env.journal, "pool")
		cfg.Journal = dj
	})
	env.start()

	// Window 0 (ours) is pending on its record.
	requireEqual(t, len(env.hooks.windows), 0, "no announce before durability")
	requireEqual(t, env.eng.voter.alarmArmed, false, "no voter timers before announce")

	// Settle slots 0..3 with notarization certificates: window 1 queues too.
	parent := Genesis()
	for s := uint32(0); s < 4; s++ {
		id := candID(s, byte(0x50+s))
		_ = parent
		env.eng.HandleMessage(peer(2), 2, env.buildCert(NotarizeVote(id), 0, 2, 3).Serialize())
		parent = Parent(id)
	}
	requireEqual(t, len(dj.pending), 2, "two window records in flight")
	requireEqual(t, len(env.hooks.windows), 0, "still nothing announced")

	// Window 1's record completes first: order must hold, nothing fires.
	dj.releaseAt(t, 1)
	requireEqual(t, len(env.hooks.windows), 0, "younger window waits for the older")

	// Window 0's record completes: both announce, in order.
	dj.releaseAt(t, 0)
	requireEqual(t, len(env.hooks.windows), 2, "durable windows announced")
	requireEqual(t, env.hooks.windows[0].StartSlot, uint32(0), "window 0 first")
	requireEqual(t, env.hooks.windows[0].LocalLeader, true, "window 0 is local")
	requireEqual(t, env.hooks.windows[1].StartSlot, uint32(4), "window 1 second")
	requireEqual(t, env.hooks.windows[1].LocalLeader, false, "window 1 is remote")
	requireEqual(t, env.eng.voter.currentWindow, uint32(1), "window 1 observed after window 0")
	env.requireNoFatal()
}

// TestVotePayloadCache: the memoized dataToSign payload is always identical to
// a fresh serialization, across kinds, slots and evictions.
func TestVotePayloadCache(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	votes := []Vote{
		NotarizeVote(candID(0, 0x61)),
		FinalizeVote(candID(0, 0x61)),
		SkipVote(0),
		NotarizeVote(candID(1, 0x62)), // evicts the notarize entry
		NotarizeVote(candID(0, 0x61)), // recomputed after eviction
		SkipVote(1),
	}
	for i, v := range votes {
		got := env.eng.votePayload(v)
		want := DataToSign(env.session, VoteBytes(v))
		if !bytes.Equal(got, want) {
			t.Fatalf("payload %d (%s) diverged from fresh serialization", i, v)
		}
		// Hit the cache again immediately — same bytes.
		if !bytes.Equal(env.eng.votePayload(v), want) {
			t.Fatalf("payload %d (%s) cache hit diverged", i, v)
		}
	}

	// A wrong signature keeps failing on the cached payload, a right one keeps
	// passing for a second validator.
	v := NotarizeVote(candID(2, 0x63))
	env.deliverVote(2, v)
	requireEqual(t, env.eng.slots.at(2).votes[2].notarize.set, true, "vote verified")
	sig := make([]byte, 64)
	env.eng.HandleMessage(peer(3), 3, (&SignedVote{ValidatorIndex: 3, Vote: v, Signature: sig}).Serialize())
	requireEqual(t, env.eng.slots.at(2).votes[3].notarize.set, false, "forged vote still rejected on cached payload")
	env.requireNoFatal()
}

// TestSlotMapPruneShapes covers both prune branches of notifyFinalized: the
// dense range delete and the sparse far-jump sweep.
func TestSlotMapPruneShapes(t *testing.T) {
	m := newSlotMap(func() *struct{} { return &struct{}{} })
	for i := uint32(0); i < 10; i++ {
		m.at(i)
	}
	m.notifyFinalized(4) // dense: range branch
	requireEqual(t, len(m.slots), 5, "slots 0..4 pruned")
	requireEqual(t, m.peek(4) == nil, true, "pruned slot inaccessible")
	requireEqual(t, m.peek(5) != nil, true, "live slot kept")

	m.notifyFinalized(1_000_000) // sparse: sweep branch
	requireEqual(t, len(m.slots), 0, "far jump prunes everything")
	requireEqual(t, m.at(999_999) == nil, true, "below horizon")
	requireEqual(t, m.at(1_000_001) != nil, true, "above horizon allocates")

	begin, end := m.interval()
	requireEqual(t, begin, uint32(1_000_001), "interval begin")
	requireEqual(t, end, uint32(1_000_002), "interval end")
}
