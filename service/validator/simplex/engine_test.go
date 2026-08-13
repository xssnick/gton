package simplex

import (
	"errors"
	"testing"
	"time"
)

// TestStartAnnouncesFirstWindow: on a fresh session the pool publishes window
// 0 with a genesis base after its journal record is durable.
func TestStartAnnouncesFirstWindow(t *testing.T) {
	env := newTestEnv(t) // local 0 is the round-robin leader of window 0
	env.start()

	requireEqual(t, len(env.hooks.windows), 1, "leader windows")
	w := env.hooks.windows[0]
	requireEqual(t, w.ObservedSlot, uint32(0), "observed slot")
	requireEqual(t, w.StartSlot, uint32(0), "window start")
	requireEqual(t, w.EndSlot, env.spw, "window end")
	requireEqual(t, w.Leader, uint32(0), "window leader")
	requireEqual(t, w.LocalLeader, true, "local leader")
	requireEqual(t, w.ObservedAt.Equal(env.clock.Now()), true, "window observation time")
	requireEqual(t, w.Base.Exists, false, "window base is genesis")
	env.requireNoFatal()

	// The window announcement must already be durable.
	bs, err := env.journal.Bootstrap()
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, bs.FirstNonAnnouncedWindow, uint32(1), "persisted window")
}

// TestRecoveryObservesAlignedWindowFromMidWindow covers recovery with enough
// journaled certificates to enter an already-running leader window. The base
// belongs to the first unsettled slot, while the declared range stays aligned
// to the actual leader boundaries and must not spill into the next leader.
func TestRecoveryObservesAlignedWindowFromMidWindow(t *testing.T) {
	env := newTestEnv(t, withLocal(1))

	var last CandidateID
	for slot := uint32(0); slot < 6; slot++ {
		last = candID(slot, byte(0x30+slot))
		if err := env.journal.saveCertificate(env.buildCert(NotarizeVote(last), 0, 2, 3)); err != nil {
			t.Fatal(err)
		}
	}
	if err := env.journal.saveWindow(1); err != nil {
		t.Fatal(err)
	}

	env.start()

	requireEqual(t, len(env.hooks.windows), 1, "recovered window observations")
	w := env.hooks.windows[0]
	requireEqual(t, w.ObservedSlot, uint32(6), "first unsettled slot")
	requireEqual(t, w.StartSlot, uint32(4), "aligned window start")
	requireEqual(t, w.EndSlot, uint32(8), "aligned window end")
	requireEqual(t, w.Leader, uint32(1), "window leader")
	requireEqual(t, w.LocalLeader, true, "local leader")
	requireEqual(t, w.Base, Parent(last), "base for first unsettled slot")
	env.requireNoFatal()
}

// TestFullSlotLifecycle drives one slot from candidate to finalization on a
// single engine with three remote validators.
func TestFullSlotLifecycle(t *testing.T) {
	env := newTestEnv(t, withLocal(1)) // leader of window 0 is validator 0
	env.start()
	requireEqual(t, len(env.hooks.windows), 1, "window observation")
	requireEqual(t, env.hooks.windows[0].LocalLeader, false, "remote leader window")

	cand := env.makeCandidate(0, Genesis(), 0x77)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	// Parent chain is settled (genesis), so validation starts immediately.
	requireEqual(t, len(env.hooks.validations), 1, "validation requests")
	requireEqual(t, env.hooks.validations[0].ID, cand.ID, "validated candidate")
	accepted := env.hooks.validations[0]
	wantRootByte := accepted.Block.RootHash[0]
	wantFileByte := accepted.Block.FileHash[0]
	cand.Block.RootHash[0] ^= 0xff
	cand.Block.FileHash[0] ^= 0xff
	requireEqual(t, accepted.Block.RootHash[0], wantRootByte, "owned candidate root hash")
	requireEqual(t, accepted.Block.FileHash[0], wantFileByte, "owned candidate file hash")
	requireEqual(t, len(env.trans.broadcasts), 0, "no votes before validation")

	env.completeValidation(cand.ID, nil)
	requireEqual(t, len(env.hooks.stored), 1, "stored candidates")
	requireEqual(t, env.trans.countVotes(VoteNotarize), 1, "own notarize broadcast")

	// Two more notarize votes reach the 3-of-4 quorum: certificate is built,
	// persisted, gossiped, and the finalize vote follows automatically.
	env.deliverVote(2, NotarizeVote(cand.ID))
	requireEqual(t, len(env.hooks.notarized), 0, "no cert on 2 votes")
	env.deliverVote(3, NotarizeVote(cand.ID))

	requireEqual(t, len(env.hooks.notarized), 1, "notarization observed")
	requireEqual(t, env.hooks.notarized[0], cand.ID, "notarized id")
	requireEqual(t, len(env.trans.gossips), 1, "certificate gossip")
	requireEqual(t, env.trans.gossips[0].count, env.params.CertificateGossipNeighbors, "gossip fanout")
	requireEqual(t, env.trans.countVotes(VoteFinalize), 1, "own finalize broadcast")

	env.deliverVote(2, FinalizeVote(cand.ID))
	env.deliverVote(3, FinalizeVote(cand.ID))

	requireEqual(t, len(env.hooks.finalized), 1, "finalization observed")
	requireEqual(t, env.hooks.finalized[0], cand.ID, "finalized id")
	requireEqual(t, env.eng.slots.firstNonFinalized, uint32(1), "finalized horizon")
	env.requireNoFatal()

	// The journal now holds our two votes and both certificates.
	bs, err := env.journal.Bootstrap()
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(bs.OurVotes), 2, "journaled votes")
	requireEqual(t, len(bs.Certificates), 2, "journaled certificates")
	requireEqual(t, bs.OurVotes[0].Kind, VoteNotarize, "first journaled vote")
	requireEqual(t, bs.OurVotes[1].Kind, VoteFinalize, "second journaled vote")
}

// TestValidationRejectAbstains: a rejected candidate never produces a
// notarize vote, but a later timeout still skips the slot.
func TestValidationRejectAbstains(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x10)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.completeValidation(cand.ID, errors.New("bad state root"))

	// The store is started on acceptance and runs concurrently with the
	// validation gate, so a rejected candidate is still persisted: the store is
	// started before the parent wait. What must not happen is the vote.
	requireEqual(t, len(env.hooks.stored), 1, "store started on acceptance")
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "no notarize on reject")
	requireEqual(t, env.eng.Stats().CandidatesRejected, uint64(1), "reject counter")

	// Timeout skips the window.
	env.clock.advance(env.params.FirstBlockTimeout + env.params.TargetRate + time.Millisecond)
	env.eng.Advance()
	requireEqual(t, env.trans.countVotes(VoteSkip), int(env.spw), "window skipped")
	env.requireNoFatal()
}

// TestSkipTimeoutsAndAdaptiveTimeout verifies the whole-window skip on alarm
// and the multiplicative first-block timeout growth.
func TestSkipTimeoutsAndAdaptiveTimeout(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	// Window 0: nothing happens, alarm fires at fbt+rate.
	wake := env.eng.NextWakeup()
	wantWake := env.clock.Now().Add(env.params.FirstBlockTimeout + env.params.TargetRate)
	requireEqual(t, wake.Equal(wantWake), true, "voter alarm deadline")

	env.clock.set(wantWake)
	env.eng.Advance()
	requireEqual(t, env.trans.countVotes(VoteSkip), int(env.spw), "skip votes for window 0")

	// Remote skip votes complete the certificates and open window 1.
	for slot := uint32(0); slot < env.spw; slot++ {
		env.deliverVote(2, SkipVote(slot))
		env.deliverVote(3, SkipVote(slot))
	}
	requireEqual(t, env.eng.now, env.spw, "present slot after skips")

	// Window 1 announcement re-arms the alarm with the increased timeout.
	v := env.eng.voter
	scaled := time.Duration(float64(env.params.FirstBlockTimeout) * env.params.FirstBlockTimeoutMultiplier)
	requireEqual(t, v.firstBlockTimeout, scaled, "adaptive first block timeout")
	env.requireNoFatal()
}

// TestParentWaitOnSkipRun: a candidate that jumps over skipped slots is
// released only when the skip run covers the whole gap.
func TestParentWaitOnSkipRun(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	// Slot 0 candidate notarized (without our vote to keep it simple).
	cand0 := env.makeCandidate(0, Genesis(), 0x21)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(NotarizeVote(cand0.ID), 0, 2, 3).Serialize())
	requireEqual(t, len(env.hooks.notarized), 1, "slot 0 notarized")

	// Candidate for slot 2 building on slot 0: needs slot 1 skipped.
	cand2 := env.makeCandidate(2, Parent(cand0.ID), 0x22)
	if err := env.eng.SubmitCandidate(cand2); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(env.hooks.validations), 0, "validation must wait for skip cert")

	env.eng.HandleMessage(peer(3), 3, env.buildCert(SkipVote(1), 0, 2, 3).Serialize())
	requireEqual(t, len(env.hooks.validations), 1, "validation released by skip run")
	requireEqual(t, env.hooks.validations[0].ID, cand2.ID, "released candidate")
	env.requireNoFatal()
}

// TestConflictingCandidateMisbehavior: a candidate contradicting an existing
// notarization certificate is reported against the leader.
func TestConflictingCandidateMisbehavior(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	candA := env.makeCandidate(0, Genesis(), 0x31)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(NotarizeVote(candA.ID), 0, 2, 3).Serialize())

	candB := env.makeCandidate(0, Genesis(), 0x32)
	if err := env.eng.SubmitCandidate(candB); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(env.hooks.misbehaviors), 1, "misbehavior reports")
	requireEqual(t, env.hooks.misbehaviors[0].Validator, candB.Leader, "reported leader")
	requireEqual(t, env.hooks.misbehaviors[0].M.Kind, MisbehaviorConflictingCandidateAndCertificate, "kind")
	requireEqual(t, len(env.hooks.validations), 0, "no validation for conflicting candidate")
	env.requireNoFatal()
}

// TestEquivocatingVotes: a second direct vote of the same kind is silently
// deduplicated (first wins, no double counting); the
// provable conflict surfaces when a certificate carries the other vote.
func TestEquivocatingVotes(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	idA := candID(0, 0xa1)
	idB := candID(0, 0xb2)
	env.deliverVote(2, NotarizeVote(idA))
	env.deliverVote(2, NotarizeVote(idB))

	// Direct path: dropped by the wants() dedup before signature checks.
	requireEqual(t, len(env.hooks.misbehaviors), 0, "no report from direct dedup")
	slot := env.eng.slots.at(0)
	requireEqual(t, slot.notarizeWeight[idA], uint64(1), "weight for idA")
	requireEqual(t, slot.notarizeWeight[idB], uint64(0), "weight for idB")

	// Certificate path: a valid idB certificate carries validator 2's
	// conflicting vote — the equivocation becomes provable and is reported.
	env.eng.HandleMessage(peer(3), 3, env.buildCert(NotarizeVote(idB), 0, 2, 3).Serialize())
	requireEqual(t, len(env.hooks.misbehaviors), 1, "conflict reported via certificate")
	requireEqual(t, env.hooks.misbehaviors[0].Validator, uint32(2), "equivocator index")
	m := env.hooks.misbehaviors[0].M
	requireEqual(t, m.Kind, MisbehaviorConflictingVotes, "kind")
	if len(m.Proof1) == 0 || len(m.Proof2) == 0 {
		t.Fatal("conflict proofs must be attached")
	}
	requireEqual(t, len(env.hooks.notarized), 1, "certificate still applied")
	env.requireNoFatal()
}

// TestFinalizeSkipConflictInvariant: finalize after skip from the same
// validator violates the invariant and is rejected.
func TestFinalizeSkipInvariant(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	env.deliverVote(2, SkipVote(0))
	env.deliverVote(2, FinalizeVote(candID(0, 0xcc)))

	requireEqual(t, len(env.hooks.misbehaviors), 1, "misbehavior reports")
	slot := env.eng.slots.at(0)
	requireEqual(t, slot.votes[2].finalize.set, false, "finalize rolled back")
	requireEqual(t, slot.votes[2].skip.set, true, "skip stays")
	// Notarize + skip is a legal combination.
	env.deliverVote(2, NotarizeVote(candID(0, 0xcd)))
	requireEqual(t, slot.votes[2].notarize.set, true, "notarize accepted after skip")
	requireEqual(t, len(env.hooks.misbehaviors), 1, "no extra reports")
	env.requireNoFatal()
}

// TestTooNewVotesDroppedCertsAccepted: votes beyond the desync horizon are
// dropped while valid certificates from the future are applied (catch-up).
func TestTooNewVotesDroppedCertsAccepted(t *testing.T) {
	p := DefaultParams()
	p.MaxLeaderWindowDesync = 2
	env := newTestEnv(t, withLocal(1), withParams(p))
	env.start()

	farSlot := (env.eng.now/env.spw + p.MaxLeaderWindowDesync + 1) * env.spw
	env.deliverVote(2, SkipVote(farSlot))
	if s := env.eng.slots.peek(farSlot); s != nil && s.votes[2].skip.set {
		t.Fatal("too new vote must be dropped")
	}

	farID := candID(farSlot, 0x99)
	env.eng.HandleMessage(peer(3), 3, env.buildCert(NotarizeVote(farID), 0, 2, 3).Serialize())
	requireEqual(t, len(env.hooks.notarized), 1, "future certificate accepted")
	requireEqual(t, env.hooks.notarized[0], farID, "future cert id")
	env.requireNoFatal()
}

// TestBadSignatureBans: an invalid vote signature bans the transport peer
// until the ban expires.
func TestBadSignatureBans(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	v := NotarizeVote(candID(0, 0x42))
	sv := SignedVote{ValidatorIndex: 2, Vote: v, Signature: make([]byte, 64)}
	env.eng.HandleMessage(peer(2), 2, sv.Serialize())
	requireEqual(t, env.eng.Stats().Bans, uint64(1), "ban issued")

	// A valid vote from the banned peer is dropped...
	env.deliverVote(2, v)
	slot := env.eng.slots.at(0)
	requireEqual(t, slot.votes[2].notarize.set, false, "vote from banned peer dropped")

	// ...until the ban expires.
	env.clock.advance(env.params.BadSignatureBanDuration + time.Second)
	env.deliverVote(2, v)
	requireEqual(t, slot.votes[2].notarize.set, true, "vote accepted after ban expiry")
	env.requireNoFatal()
}

// TestVoteFromNonValidatorBans: votes require an authenticated validator
// sender.
func TestVoteFromNonValidatorBans(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	env.eng.HandleMessage(peer(9), ObserverIndex, env.signedVoteWire(2, SkipVote(0)))
	requireEqual(t, env.eng.Stats().Bans, uint64(1), "non-validator vote source banned")

	// Certificates are accepted from anyone (observers gossip them too).
	env.eng.HandleMessage(peer(10), ObserverIndex, env.buildCert(SkipVote(0), 0, 2, 3).Serialize())
	slot := env.eng.slots.at(0)
	requireEqual(t, slot.skipCert != nil, true, "cert from observer accepted")
	env.requireNoFatal()
}

// TestBadCertificatesRejected covers the certificate verification paths.
func TestBadCertificatesRejected(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	id := candID(0, 0x55)

	under := env.buildCert(NotarizeVote(id), 2, 3) // weight 2 < threshold 3
	if err := env.eng.verifyCertificate(under); err == nil {
		t.Fatal("under-quorum certificate must fail")
	}

	dup := env.buildCert(NotarizeVote(id), 2, 3, 2)
	if err := env.eng.verifyCertificate(dup); err == nil {
		t.Fatal("duplicate index certificate must fail")
	}

	oob := env.buildCert(NotarizeVote(id), 1, 2, 3)
	oob.Signatures[0].ValidatorIndex = 7
	if err := env.eng.verifyCertificate(oob); err == nil {
		t.Fatal("out-of-bounds index certificate must fail")
	}

	forged := env.buildCert(NotarizeVote(id), 1, 2, 3)
	forged.Signatures[0].Signature = make([]byte, 64)
	if err := env.eng.verifyCertificate(forged); err == nil {
		t.Fatal("forged signature certificate must fail")
	}
	env.eng.HandleMessage(peer(6), ObserverIndex, forged.Serialize())
	requireEqual(t, env.eng.Stats().Bans, uint64(1), "forged cert source banned")

	good := env.buildCert(NotarizeVote(id), 1, 2, 3)
	if err := env.eng.verifyCertificate(good); err != nil {
		t.Fatal(err)
	}
	env.requireNoFatal()
}

// TestQuorumIsWeighted: quorum counts stake, not heads.
func TestQuorumIsWeighted(t *testing.T) {
	vals, keys := testValidators(4, 70, 10, 10, 10) // total 100, threshold 67
	env := &testEnv{
		t: t, vals: vals, keys: keys, spw: 4,
		clock: newFakeClock(), trans: &fakeTransport{}, hooks: newRecHooks(t),
		journal: NewMemoryJournal(4), params: DefaultParams(),
	}
	cfg := Config{
		SessionID: [32]byte{1}, ProtocolVersion: 3,
		Validators: vals, LocalIndex: 1, SlotsPerLeaderWindow: 4,
		Params: &env.params, Journal: env.journal, Transport: env.trans,
		Hooks: env.hooks,
		Clock: env.clock, Signer: newTestSigner(keys[1]),
	}
	env.session = cfg.SessionID
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	env.eng = eng
	env.start()

	requireEqual(t, eng.threshold, uint64(100*2/3+1), "threshold")

	id := candID(0, 0x66)
	// Validators 1+2+3 = weight 30 < 67: no certificate.
	env.deliverVote(2, NotarizeVote(id))
	env.deliverVote(3, NotarizeVote(id))
	// No validation was requested for this unknown candidate ID.
	requireEqual(t, len(env.hooks.notarized), 0, "no cert below weighted quorum")

	// Heavyweight validator 0 pushes it over: 30+70 >= 67.
	env.deliverVote(0, NotarizeVote(id))
	requireEqual(t, len(env.hooks.notarized), 1, "cert after weighted quorum")
	env.requireNoFatal()
}

// TestObserverFollowsFinality: an observer engine applies certificates and
// reports finality without ever voting.
func TestObserverFollowsFinality(t *testing.T) {
	env := newTestEnv(t, withLocal(ObserverIndex))
	env.start()

	id := candID(0, 0x88)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(NotarizeVote(id), 0, 2, 3).Serialize())
	env.eng.HandleMessage(peer(2), 2, env.buildCert(FinalizeVote(id), 0, 2, 3).Serialize())

	requireEqual(t, len(env.hooks.notarized), 1, "observer sees notarization")
	requireEqual(t, len(env.hooks.finalized), 1, "observer sees finalization")
	requireEqual(t, env.trans.countVotes(VoteNotarize)+env.trans.countVotes(VoteFinalize)+env.trans.countVotes(VoteSkip), 0, "observer never votes")
	requireEqual(t, len(env.hooks.windows), 1, "observer sees window")
	requireEqual(t, env.hooks.windows[0].LocalLeader, false, "observer is not leader")
	env.requireNoFatal()
}

func TestObserverNeverEmitsTimeoutOrStandstillTraffic(t *testing.T) {
	env := newTestEnv(t, withLocal(ObserverIndex))
	env.start()

	env.clock.advance(env.params.FirstBlockTimeout + env.params.TargetRate + time.Millisecond)
	env.eng.Advance()
	if len(env.trans.broadcasts) != 0 || len(env.trans.gossips) != 0 {
		t.Fatal("observer emitted timeout traffic")
	}

	env.clock.set(env.eng.standstillAt)
	env.eng.Advance()
	if env.eng.Stats().Standstills != 1 {
		t.Fatalf("standstill count = %d, want 1", env.eng.Stats().Standstills)
	}
	if env.eng.drain != nil || len(env.trans.broadcasts) != 0 || len(env.trans.gossips) != 0 {
		t.Fatal("observer emitted standstill-resolution traffic")
	}
	env.requireNoFatal()
}

// TestFinalizeSkipQuorumIsFatal: certificates proving that a slot is both
// skipped and finalized mean >1/3 faulty weight; the session fails closed.
func TestFinalizeSkipQuorumIsFatal(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	id := candID(0, 0xde)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(SkipVote(0), 0, 1, 2, 3).Serialize())
	env.eng.HandleMessage(peer(2), 2, env.buildCert(FinalizeVote(id), 0, 2, 3).Serialize())

	requireEqual(t, len(env.hooks.fatals), 1, "fatal reported")
	if env.eng.Err() == nil {
		t.Fatal("engine must be failed")
	}
	// Engine is halted: further inputs are ignored.
	env.deliverVote(3, SkipVote(1))
	if s := env.eng.slots.peek(1); s != nil && s.votes[3].skip.set {
		t.Fatal("failed engine must drop inputs")
	}
}

// TestJournalFailureStopsSigning: a failed vote persist prevents signing,
// application and broadcast (persist-before-sign).
func TestJournalFailureStopsSigning(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x11)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.journal.FailNextWrites(1)
	env.completeValidation(cand.ID, nil)

	requireEqual(t, len(env.hooks.fatals), 1, "journal failure is fatal")
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "no broadcast after failed persist")
	slot := env.eng.slots.peek(0)
	requireEqual(t, slot.votes[1].notarize.set, false, "vote not applied after failed persist")

	bs, err := env.journal.Bootstrap()
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(bs.OurVotes), 0, "nothing persisted")
}

// TestPrecheckCandidateBroadcast covers the broadcast dedup gate.
func TestPrecheckCandidateBroadcast(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	idA := [32]byte{0xaa}
	idB := [32]byte{0xbb}

	if err := env.eng.PrecheckCandidateBroadcast(0, idA, false); err != nil {
		t.Fatal(err)
	}
	if err := env.eng.PrecheckCandidateBroadcast(0, idA, true); err != nil {
		t.Fatal(err)
	}
	if err := env.eng.PrecheckCandidateBroadcast(0, idB, true); err == nil {
		t.Fatal("second broadcast id for the slot must be rejected")
	}
	if err := env.eng.PrecheckCandidateBroadcast(0, idA, false); err != nil {
		t.Fatal("same broadcast id must pass")
	}
	far := env.eng.now + env.params.MaxLeaderWindowDesync*env.spw + 1
	if err := env.eng.PrecheckCandidateBroadcast(far, idA, false); err == nil {
		t.Fatal("far future slot must be rejected")
	}
}

// TestStandstillResolutionRebroadcast: after the standstill timeout the
// engine re-broadcasts certificates and its own uncovered votes, and a new
// finalization aborts the drain.
func TestStandstillResolutionRebroadcast(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	// Local votes skip on timeout, one remote skip vote — no certificate yet.
	env.clock.advance(env.params.FirstBlockTimeout + env.params.TargetRate + time.Millisecond)
	env.eng.Advance()
	requireEqual(t, env.trans.countVotes(VoteSkip), int(env.spw), "own skips cast")
	env.trans.reset()

	// Standstill fires (armed at Start).
	env.clock.set(env.eng.standstillAt)
	env.eng.Advance()
	requireEqual(t, env.eng.Stats().Standstills, uint64(1), "standstill fired")
	// All 4 own skip votes must be re-broadcast (no certs cover them).
	requireEqual(t, env.trans.countVotes(VoteSkip), int(env.spw), "own votes re-broadcast")
	requireEqual(t, len(env.trans.validatorBroadcasts), len(env.trans.broadcasts), "standstill targets validators only")
	env.requireNoFatal()
}

// TestStandstillDrainPacing: with a tiny egress budget the drain is spread
// over multiple wakeups instead of bursting.
func TestStandstillDrainPacing(t *testing.T) {
	p := DefaultParams()
	p.StandstillMaxEgressBytesPerS = 64 // bytes/s; every message costs len*4
	p.StandstillMinEgressBytesPerS = 1  // keep the per-shard floor below the tiny budget
	env := newTestEnv(t, withLocal(1), withParams(p))
	testStandstillDrainPacing(t, env)
}

// TestStandstillEgressPerShardSplit: the same tiny effective budget reached
// through the per-shard divisor paces identically, while a floor above the
// divided budget removes the pacing.
func TestStandstillEgressPerShardSplit(t *testing.T) {
	p := DefaultParams()
	p.StandstillMaxEgressBytesPerS = 64 << 4 // divided by 2^4 back to 64
	p.StandstillMinEgressBytesPerS = 1
	env := newTestEnv(t, withLocal(1), withParams(p), withShardPrefix(4))
	testStandstillDrainPacing(t, env)

	floor := DefaultParams()
	floor.StandstillMaxEgressBytesPerS = 64 << 4
	floor.StandstillMinEgressBytesPerS = 1 << 20 // the floor wins over the divided budget
	fenv := newTestEnv(t, withLocal(1), withParams(floor), withShardPrefix(4))
	fenv.start()
	fenv.clock.advance(floor.FirstBlockTimeout + floor.TargetRate + time.Millisecond)
	fenv.eng.Advance()
	fenv.trans.reset()
	fenv.clock.set(fenv.eng.standstillAt)
	fenv.eng.Advance()
	if fenv.eng.drain != nil {
		t.Fatal("a budget above the message volume must drain in one step")
	}
}

func testStandstillDrainPacing(t *testing.T, env *testEnv) {
	t.Helper()
	p := env.params
	env.start()

	env.clock.advance(p.FirstBlockTimeout + p.TargetRate + time.Millisecond)
	env.eng.Advance()
	env.trans.reset()

	env.clock.set(env.eng.standstillAt)
	env.eng.Advance()
	sentAtOnce := len(env.trans.broadcasts)
	if sentAtOnce >= int(env.spw) {
		t.Fatalf("drain must be paced, sent %d at once", sentAtOnce)
	}
	if env.eng.drain == nil {
		t.Fatal("drain must stay active")
	}
	// Walk virtual time until everything is out; a standstill firing
	// mid-drain must not restart the queue.
	for i := 0; i < 100 && env.eng.drain != nil; i++ {
		next := env.eng.NextWakeup()
		env.clock.set(next)
		env.eng.Advance()
	}
	requireEqual(t, env.trans.countVotes(VoteSkip), int(env.spw), "all messages drained")

	// A finalization aborts a fresh drain immediately.
	env.clock.set(env.eng.standstillAt)
	env.eng.Advance()
	if env.eng.drain == nil {
		t.Fatal("second drain must start")
	}
	id := candID(0, 0x77)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(FinalizeVote(id), 0, 2, 3).Serialize())
	if env.eng.drain != nil {
		t.Fatal("finalization must abort the drain")
	}
	env.requireNoFatal()
}

// TestForgedCandidateRejected covers candidate verification.
func TestForgedCandidateRejected(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	// Wrong leader.
	cand := env.makeCandidate(0, Genesis(), 0x61)
	cand.Leader = 2
	if err := env.eng.SubmitCandidate(cand); err == nil {
		t.Fatal("wrong leader must be rejected")
	}

	// Broken signature.
	cand = env.makeCandidate(0, Genesis(), 0x62)
	cand.Signature[0] ^= 1
	if err := env.eng.SubmitCandidate(cand); err == nil {
		t.Fatal("bad signature must be rejected")
	}

	// Parent slot >= own slot.
	bad := env.makeCandidate(0, Genesis(), 0x63)
	bad.Parent = Parent(candID(5, 1))
	bad.ID.Slot = 3
	if err := env.eng.SubmitCandidate(bad); err == nil {
		t.Fatal("parent after slot must be rejected")
	}
	requireEqual(t, len(env.hooks.validations), 0, "no pipeline for rejected candidates")
	env.requireNoFatal()
}
