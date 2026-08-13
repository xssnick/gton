package simplex

import "testing"

// deliverVoteBatched admits a vote the way Runner does — without the inline
// verification flush Engine.HandleMessage performs.
func (env *testEnv) deliverVoteBatched(i int, v Vote) {
	env.eng.handleMessageBatched(peer(i), i, env.signedVoteWire(i, v))
}

// TestFlushVerifyBatch pins the two properties the deferred vote verification
// has to preserve: nothing is applied before the flush, and the flush replays
// the admission gates in arrival order, so a forged signature still bans its
// sender and still takes that sender's later entries of the same batch with it.
func TestFlushVerifyBatch(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	v := NotarizeVote(candID(0, 0x42))
	env.deliverVoteBatched(2, v)
	forged := SignedVote{ValidatorIndex: 3, Vote: v, Signature: make([]byte, 64)}
	env.eng.handleMessageBatched(peer(3), 3, forged.Serialize())
	env.eng.handleMessageBatched(peer(3), 3, env.signedVoteWire(3, SkipVote(0)))
	env.deliverVoteBatched(0, v)

	slot := env.eng.slots.at(0)
	requireEqual(t, slot.votes[2].notarize.set, false, "vote applied before the flush")
	requireEqual(t, env.eng.Stats().Bans, uint64(0), "peer banned before the flush")

	env.eng.FlushVerify()
	requireEqual(t, slot.votes[2].notarize.set, true, "good vote applied")
	requireEqual(t, slot.votes[0].notarize.set, true, "good vote behind the forged one applied")
	requireEqual(t, env.eng.Stats().Bans, uint64(1), "forged signature banned its sender")
	requireEqual(t, slot.votes[3].skip.set, false, "later vote from the sender banned by this batch")
	env.requireNoFatal()
}

// TestFlushVerifyDropsVotesForASlotTheBatchFinalized covers the ordering hazard
// of deferring: entries admitted against a live slot may be applied after the
// same batch finalized it, which prunes the slot state they were gated on.
func TestFlushVerifyDropsVotesForASlotTheBatchFinalized(t *testing.T) {
	env := newTestEnv(t, withLocal(ObserverIndex))
	env.start()

	id := candID(0, 0x7c)
	for _, i := range []int{1, 2, 3} {
		env.deliverVoteBatched(i, NotarizeVote(id))
	}
	for _, i := range []int{1, 2, 3} {
		env.deliverVoteBatched(i, FinalizeVote(id))
	}
	// Admitted while slot 0 was live, applied after it is gone.
	env.deliverVoteBatched(0, NotarizeVote(id))

	env.eng.FlushVerify()
	requireEqual(t, env.eng.slots.firstNonFinalized, uint32(1), "slot finalized by the batch")
	requireEqual(t, len(env.hooks.finalized), 1, "finalization reported once")
	env.requireNoFatal()
}
