package simplex

import (
	"errors"
	"testing"
)

// restartEnv builds a fresh engine over an existing journal, simulating a
// process restart. Transport/hooks/clock are new (the network is gone).
func restartEnv(t *testing.T, prev *testEnv, local int) *testEnv {
	t.Helper()
	env := &testEnv{
		t: t, vals: prev.vals, keys: prev.keys, session: prev.session, spw: prev.spw,
		clock: newFakeClock(), trans: &fakeTransport{}, hooks: newRecHooks(t),
		journal: prev.journal, params: prev.params,
	}
	cfg := Config{
		SessionID: env.session, ProtocolVersion: 3, Validators: env.vals, LocalIndex: local,
		SlotsPerLeaderWindow: env.spw, Params: &env.params,
		Journal: env.journal, Transport: env.trans, Clock: env.clock,
		Hooks: env.hooks,
	}
	if local != ObserverIndex {
		cfg.Signer = newTestSigner(env.keys[local])
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	env.eng = eng
	return env
}

// TestRestartReplaysVotesWithoutRebroadcast: journaled own votes are
// re-signed and re-applied on restart, but never re-broadcast; the
// interrupted window is skip-voted.
func TestRestartReplaysVotesWithoutRebroadcast(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x21)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.completeValidation(cand.ID, nil)
	requireEqual(t, env.trans.countVotes(VoteNotarize), 1, "notarize before crash")

	// Crash. Restart over the same journal.
	env2 := restartEnv(t, env, 1)
	env2.start()

	// The notarize vote is applied to the pool without a second broadcast.
	slot := env2.eng.slots.at(0)
	requireEqual(t, slot.votes[1].notarize.set, true, "vote restored into pool")
	requireEqual(t, slot.notarizeWeight[cand.ID], uint64(1), "weight restored")
	requireEqual(t, env2.trans.countVotes(VoteNotarize), 0, "no notarize re-broadcast")

	// The last announced window (0) gets protective skip votes for all
	// non-finalized slots.
	requireEqual(t, env2.trans.countVotes(VoteSkip), int(env2.spw), "startup skips for announced window")
	// notarize+skip may coexist; voter state reflects both for slot 0.
	vslot := env2.eng.voter.slots.at(0)
	requireEqual(t, vslot.votedNotar != nil, true, "votedNotar restored")
	requireEqual(t, vslot.votedSkip, true, "votedSkip set on recovery")

	// The window is not announced (and produced) a second time.
	requireEqual(t, len(env2.hooks.windows), 0, "window 0 not re-announced")

	// Re-submitting the same candidate is a no-op (votedNotar set).
	if err := env2.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(env2.hooks.validations), 0, "no second validation")
	env2.requireNoFatal()
}

// TestCrashBetweenPersistAndSend: the vote intent is durable but the send
// never happened (crash right after the journal fsync). After restart the
// vote is in the pool, the journal dedups a re-cast, and no conflicting vote
// can be produced.
func TestCrashBetweenPersistAndSend(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x33)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}

	// Crash exactly between the successful persist and the broadcast: the
	// journal write goes through, then the process dies (panic unwinds
	// through castVote before signing/applying/sending).
	env.journal.OnBeforeWrite(func(kind string) error {
		if kind == "vote" {
			panic("simulated crash after fsync scheduling")
		}
		return nil
	})
	func() {
		defer func() { _ = recover() }()
		env.completeValidation(cand.ID, nil)
	}()
	env.journal.OnBeforeWrite(nil)
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "vote never sent")

	// Note: with OnBeforeWrite panicking before the map insert the record is
	// absent — that models a crash before fsync completion. Also model the
	// crash after a completed write:
	if err := env.journal.saveOurVote(NotarizeVote(cand.ID)); err != nil {
		t.Fatal(err)
	}

	env2 := restartEnv(t, env, 1)
	env2.start()

	slot := env2.eng.slots.at(0)
	requireEqual(t, slot.votes[1].notarize.set, true, "persisted intent applied on restart")
	requireEqual(t, env2.trans.countVotes(VoteNotarize), 0, "replay does not broadcast")

	// A duplicate cast attempt hits the journal dedup.
	requireEqual(t, errors.Is(env2.journal.saveOurVote(NotarizeVote(cand.ID)), ErrAlreadySaved), true, "journal dedup")

	// The startup skips coexist with the notarize; the engine never produces
	// a finalize for that slot now (skip+finalize is forbidden).
	vslot := env2.eng.voter.slots.at(0)
	requireEqual(t, vslot.votedSkip, true, "protective skip")
	env2.eng.HandleMessage(peer(2), 2, env2.buildCert(NotarizeVote(cand.ID), 0, 2, 3).Serialize())
	requireEqual(t, env2.trans.countVotes(VoteFinalize), 0, "no finalize after protective skip")
	env2.requireNoFatal()
}

// TestRestartReplaysCertificatesAndFinality: certificates are replayed
// without gossip, downstream notifications are re-emitted, and the finality
// horizon is restored.
func TestRestartReplaysCertificatesAndFinality(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	id0 := candID(0, 0x71)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(NotarizeVote(id0), 0, 2, 3).Serialize())
	env.eng.HandleMessage(peer(2), 2, env.buildCert(FinalizeVote(id0), 0, 2, 3).Serialize())
	id1 := candID(1, 0x72)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(NotarizeVote(id1), 0, 2, 3).Serialize())
	requireEqual(t, len(env.hooks.finalized), 1, "finalized before crash")

	env2 := restartEnv(t, env, 1)
	env2.start()

	// Journal replay order is unspecified — it comes from a prefix scan: the
	// slot-0 notarization is re-emitted only when it replays before the
	// finalization prunes the slot. Slot 1 must always resurface.
	found := false
	for _, id := range env2.hooks.notarized {
		if id == id1 {
			found = true
		}
	}
	requireEqual(t, found, true, "slot 1 notarization re-emitted")
	requireEqual(t, len(env2.hooks.finalized), 1, "finalization re-emitted")
	requireEqual(t, env2.eng.slots.firstNonFinalized, uint32(1), "finality horizon restored")
	requireEqual(t, env2.eng.lastFinalized, Parent(id0), "last finalized restored")
	requireEqual(t, len(env2.trans.gossips), 0, "no gossip during replay")

	// Slot 1 is already notarized: incoming duplicates are ignored, and the
	// available base for slot 2 was restored.
	requireEqual(t, env2.eng.slots.at(2).availableBase != nil, true, "base propagated")
	requireEqual(t, env2.eng.slots.at(2).availableBase.ID, id1, "base is slot 1 candidate")
	env2.requireNoFatal()
}

// TestRecoveryToleratesCorruptVoteLog: conflicting own votes in the journal
// (pre-crash bug) are tolerated during replay and dropped from the voter
// flags without corrupting invariants.
func TestRecoveryToleratesCorruptVoteLog(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	// Manually forge a corrupt log: two different notarize votes and a
	// finalize conflicting with a skip.
	idA := candID(0, 0x81)
	idB := candID(0, 0x82)
	if err := env.journal.saveOurVote(NotarizeVote(idA)); err != nil {
		t.Fatal(err)
	}
	if err := env.journal.saveOurVote(NotarizeVote(idB)); err != nil {
		t.Fatal(err)
	}
	if err := env.journal.saveOurVote(SkipVote(1)); err != nil {
		t.Fatal(err)
	}
	if err := env.journal.saveOurVote(FinalizeVote(candID(1, 0x83))); err != nil {
		t.Fatal(err)
	}

	env.start()

	// Pool: first notarize applied, second rejected quietly (tolerated).
	slot := env.eng.slots.at(0)
	requireEqual(t, slot.votes[1].notarize.set, true, "first notarize applied")
	requireEqual(t, slot.votes[1].notarize.vote.ID, idA, "first wins")
	// Voter flags follow the same drop rules.
	vslot := env.eng.voter.slots.at(0)
	requireEqual(t, vslot.votedNotar != nil && *vslot.votedNotar == idA, true, "voter keeps first notarize")
	vslot1 := env.eng.voter.slots.at(1)
	requireEqual(t, vslot1.votedSkip, true, "skip kept")
	requireEqual(t, vslot1.votedFinal, false, "conflicting finalize dropped")
	requireEqual(t, len(env.hooks.fatals), 0, "corrupt log must not be fatal on replay")
}

// TestWindowAnnouncePersistence: a crash after entering a window does not
// re-trigger candidate production for it after restart.
func TestWindowAnnouncePersistence(t *testing.T) {
	env := newTestEnv(t) // local 0 leads window 0
	env.start()
	requireEqual(t, len(env.hooks.windows), 1, "window announced")

	env2 := restartEnv(t, env, 0)
	env2.start()
	requireEqual(t, len(env2.hooks.windows), 0, "window not re-announced after restart")

	// Once the window is skipped through, the next window announces normally.
	for slot := uint32(0); slot < env2.spw; slot++ {
		env2.eng.HandleMessage(peer(2), 2, env2.buildCert(SkipVote(slot), 1, 2, 3).Serialize())
	}
	requireEqual(t, env2.eng.now, env2.spw, "advanced past window 0")
	// Window 1 leader is validator 1, so no local window; but the journal
	// must have moved on.
	bs, err := env2.journal.Bootstrap()
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, bs.FirstNonAnnouncedWindow, uint32(2), "window 1 announced and persisted")
	env2.requireNoFatal()
}

// TestBootstrapRejectsForgedCertificate: journal integrity is verified on
// start; a corrupt certificate fails the boot instead of poisoning the pool.
func TestBootstrapRejectsForgedCertificate(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	forged := env.buildCert(NotarizeVote(candID(0, 0x99)), 0, 2, 3)
	forged.Signatures[1].Signature = make([]byte, 64)
	if err := env.journal.saveCertificate(forged); err != nil {
		t.Fatal(err)
	}
	if err := env.eng.Start(); err == nil {
		t.Fatal("start must fail on a corrupt journaled certificate")
	}
}
