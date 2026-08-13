package simplex

import (
	"crypto/ed25519"
	"testing"
)

// TestConflictProofSurvivesInvariantRollback: a notarize and a finalize for
// different candidates from one validator break the invariant, so add() wipes
// the record it just inserted before returning. The misbehavior payload is
// built after that rollback, so both halves must still be complete and
// verifiable — losing one costs us the ability to prove the equivocation.
func TestConflictProofSurvivesInvariantRollback(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	idA := candID(0, 0xa1)
	idB := candID(0, 0xb2)
	env.deliverVote(2, NotarizeVote(idA))
	env.deliverVote(2, FinalizeVote(idB))

	requireEqual(t, len(env.hooks.misbehaviors), 1, "conflict reported")
	requireEqual(t, env.hooks.misbehaviors[0].Validator, uint32(2), "equivocator index")
	m := env.hooks.misbehaviors[0].M
	requireEqual(t, m.Kind, MisbehaviorConflictingVotes, "kind")

	// The offending finalize was rolled back: this is the path under test.
	vv := &env.eng.slots.at(0).votes[2]
	requireEqual(t, vv.finalize.set, false, "finalize rolled back")
	requireEqual(t, vv.notarize.set, true, "notarize kept")

	// Proof1 is the notarize that stayed, Proof2 the finalize that was wiped.
	for i, proof := range [][]byte{m.Proof1, m.Proof2} {
		want := NotarizeVote(idA)
		if i == 1 {
			want = FinalizeVote(idB)
		}
		v, sig, err := parseSignedVote(proof)
		if err != nil {
			t.Fatalf("proof %d does not parse: %v", i+1, err)
		}
		requireEqual(t, v, want, "proof vote")
		if !ed25519.Verify(env.vals[2].PublicKey, DataToSign(env.session, VoteBytes(v)), sig) {
			t.Fatalf("proof %d carries no valid signature of the equivocator", i+1)
		}
	}
	env.requireNoFatal()
}

// TestStandstillMessagesDoNotMaterializeGapSlots: certificate slots are
// unbounded above — a node returning from an outage accepts a valid certificate
// from far ahead as proof of session progress — so the tracked slot range is
// sparse and its interval spans the whole hole. The standstill snapshot walks
// that interval and must not create a slot per hole entry: a poolSlot carries a
// per-validator vote array plus two maps, so materializing the gap costs
// hundreds of megabytes and a multi-second stall, on every standstill tick.
func TestStandstillMessagesDoNotMaterializeGapSlots(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	const farSlot = 1_000_000
	farID := candID(farSlot, 0x99)
	env.eng.HandleMessage(peer(3), 3, env.buildCert(NotarizeVote(farID), 0, 2, 3).Serialize())
	requireEqual(t, len(env.hooks.notarized), 1, "future certificate accepted")

	tracked := len(env.eng.slots.slots)
	msgs := env.eng.buildStandstillMessages()
	requireEqual(t, len(env.eng.slots.slots), tracked, "standstill snapshot allocated gap slots")
	if len(msgs) == 0 {
		t.Fatal("standstill snapshot must still carry the far certificate")
	}
	env.requireNoFatal()
}
