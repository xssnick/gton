package simplex

import "time"

// Where the gap between "our block left" and "the committee agreed" is spent.
//
// Measured on the test stand (2026-09-04, 15 validators, one loaded shard,
// 1.0-1.35 MB blocks): the certificate for our candidate arrived 331 ms after
// emission at the first slot of a window and about 1.9 s from the sixth slot
// on, growing ~250 ms per slot in between. That shape is a queue — the
// committee needs longer per block than the 400 ms slot gives it — and the
// window's tail is skipped once the backlog eats the voters' one-second grace.
//
// The interval that grows is not attributable from outside: emission to
// certificate covers our fan-out, every voter's own validation, and the spread
// between the fastest voter and the quorum. This splits it into the two parts
// a leader can act on:
//
//   - first_vote: emission to the first notarize vote from anyone else. Our
//     delivery plus the fastest validator's replay of the block. It is the
//     floor: no committee can be quicker than its quickest member.
//   - quorum: emission to the certificate, and the difference between the two
//     is how long the rest of the committee took to catch up with the first.
//
// A wide first_vote says the candidate is expensive to ship or to replay; a
// narrow first_vote with a wide spread says the committee is queued behind its
// own backlog and a smaller block would not help the leader that produced it.
//
// Only our own candidates are tracked: the entries are keyed by candidate id
// and dropped when the notarize certificate closes them, so the map holds at
// most the slots of one leader window.
type ownCandidateTiming struct {
	acceptedAt  time.Time
	firstVoteAt time.Time
	firstVoter  uint32
}

// ownCandidateTimingLimit bounds the map against a leader that never gets its
// certificates: one leader window is sixteen slots, and a session that keeps
// producing without notarizing has a bigger problem than this map.
const ownCandidateTimingLimit = 64

// noteOwnCandidate starts the clock for a candidate this node produced.
func (e *Engine) noteOwnCandidate(id CandidateID) {
	if e.ownCandidates == nil {
		e.ownCandidates = make(map[CandidateID]*ownCandidateTiming, ownCandidateTimingLimit)
	}
	if len(e.ownCandidates) >= ownCandidateTimingLimit {
		// Oldest by slot: the map is small and this runs once per own slot.
		var oldest CandidateID
		first := true
		for key := range e.ownCandidates {
			if first || key.Slot < oldest.Slot {
				oldest, first = key, false
			}
		}
		delete(e.ownCandidates, oldest)
	}
	e.ownCandidates[id] = &ownCandidateTiming{acceptedAt: e.clock.Now()}
}

// noteOwnCandidateVote records the first notarize vote another validator cast
// for one of our candidates.
func (e *Engine) noteOwnCandidateVote(idx uint32, v Vote) {
	if v.Kind != VoteNotarize || int(idx) == e.localIndex || len(e.ownCandidates) == 0 {
		return
	}
	timing := e.ownCandidates[v.ID]
	if timing == nil || !timing.firstVoteAt.IsZero() {
		return
	}
	timing.firstVoteAt = e.clock.Now()
	timing.firstVoter = idx
}

// reportOwnCandidateNotarized closes out one of our candidates when its
// notarize certificate is stored, and is a no-op for everyone else's.
func (e *Engine) reportOwnCandidateNotarized(cert *Certificate) {
	if cert.Vote.Kind != VoteNotarize || len(e.ownCandidates) == 0 {
		return
	}
	timing := e.ownCandidates[cert.Vote.ID]
	if timing == nil {
		return
	}
	delete(e.ownCandidates, cert.Vote.ID)
	now := e.clock.Now()
	event := e.log.Info()
	if event == nil {
		return
	}
	event.
		Uint32("slot", cert.Vote.ID.Slot).
		Dur("quorum", now.Sub(timing.acceptedAt)).
		Int("signatures", len(cert.Signatures))
	if timing.firstVoteAt.IsZero() {
		// A certificate that arrived whole from a peer, with no individual vote
		// seen first. Reported without the split rather than with a made-up one.
		event.Msg("own candidate notarized")

		return
	}
	event.
		Dur("first_vote", timing.firstVoteAt.Sub(timing.acceptedAt)).
		Dur("spread", now.Sub(timing.firstVoteAt)).
		Uint32("first_voter", timing.firstVoter).
		Msg("own candidate notarized")
}
