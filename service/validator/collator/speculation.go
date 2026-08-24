package collator

import (
	"sync"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

// speculativeBase is the predecessor of a build started before its leader
// window was observed.
//
// It exists for the same reason SuccessorOffer does — the state the chain
// resolution would look up is not installed yet — but for the other boundary.
// A pipelined successor bets on a predecessor this node has built and not yet
// committed; this one bets on a candidate this node has validated and the
// network has not yet notarized. In both cases the predecessor travels by
// value, and in both cases the producer decides afterwards whether the bet
// described the block it actually goes on to extend.
//
// The difference that matters is what is NOT here: a speculative build installs
// nothing. It does not advance the session update, it does not re-root the
// acquisition's candidate map, and it does not retain the message branch. That
// is deliberate and it is what makes a lost bet free. Two rules in
// ValidateSessionUpdateAdvance say why it has to be that way: a window start
// that is already fixed may never change its StartAt, and a base may never move
// to an earlier slot. Installing a guess would therefore make the real window —
// which arrives with the true StartAt, and sometimes with an earlier base —
// unrepresentable.
type speculativeBase struct {
	// state is the same capability AdvanceConsensusBase takes, bound to the
	// candidate this build extends.
	state *SelectedBaseState
	// at is the instant the block's header is stamped with. The observed window
	// computes this from the parent's generation time; a speculative build has
	// to be told, because the value it would derive from the session belongs to
	// the window it is still in.
	at time.Time
}

// speculativeHandoff is where a speculative build's own successor offer waits.
//
// A pipelined build hands its successor straight to the producer, because by
// then there is one. A speculative build finishes before its window opens — that
// is the whole point of it — so at the instant it hands over, the producer that
// would start slot 1 does not exist yet. Without somewhere to put the offer it
// was simply dropped, and the one slot boundary the bet was supposed to buy time
// for, slot 0 into slot 1, was the only unpipelined one in the window.
//
// So the offer is parked here, and the producer collects it in the same motion
// as the build. Ownership moves with it: until a producer adopts, a withdrawal
// only has to drop what is parked, because nothing has been started from it and
// no queue node exists yet; after adoption the producer's own revoke is
// installed and every later withdrawal goes there, to the successor it really
// started.
type speculativeHandoff struct {
	mu     sync.Mutex
	offer  *SuccessorOffer
	revoke func([32]byte, PipelineHandoffOutcome)
}

// park stores the offer a speculative build made before any producer existed.
// A build that offers twice — it cannot today, but the port is one-shot by
// convention rather than by construction — keeps the later offer, which is the
// one describing the block it went on to produce.
func (h *speculativeHandoff) park(offer SuccessorOffer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.revoke != nil {
		// A producer is already collecting from this handoff, so this offer
		// belongs to it rather than to the park. It cannot happen today; parking
		// it anyway would strand it.
		return
	}
	h.offer = &offer
}

// adopt hands whatever is parked to the producer and makes the producer's revoke
// the one every later withdrawal reaches. Returns nil when the build made no
// offer — a last-slot build, a build that failed before its handoff, or one
// whose offer was already withdrawn.
func (h *speculativeHandoff) adopt(revoke func([32]byte, PipelineHandoffOutcome)) *SuccessorOffer {
	h.mu.Lock()
	defer h.mu.Unlock()
	offer := h.offer
	h.offer, h.revoke = nil, revoke

	return offer
}

// withdraw is the revoke the speculative build's port was given. Before a
// producer adopts it only has to drop what is parked: nothing has been started
// from the offer, so there is no successor to stop and no queue node to discard.
// After adoption it forwards to the producer, which owns both.
func (h *speculativeHandoff) withdraw(root [32]byte, outcome PipelineHandoffOutcome) {
	h.mu.Lock()
	revoke := h.revoke
	if revoke == nil {
		h.offer = nil
	}
	h.mu.Unlock()
	if revoke != nil {
		revoke(root, outcome)
	}
}

// speculativeProduction is a bet in flight: the build, the window it is for and
// the base it was started on. The producer for that window takes it if the
// window opened on that base, and every other path drops it.
//
// expiry is how long the bet waits to be collected, and it is deliberately NOT
// the build's deadline. The build runs under the hard deadline the slot it is
// for would have had — a producer that adopts it inherits the future as it is,
// context and all, and a build bounded by the bet's short lifetime would turn a
// slow window into a hard-deadline error, which the producer treats as terminal
// and loses the whole window over. The expiry only ever takes down a bet nobody
// has come for: it is checked at adoption, and a timer armed at install fires
// it while the bet is still parked and is a no-op once it is not.
type speculativeProduction struct {
	startSlot uint32
	base      simplex.CandidateID
	future    *candidateBuildFuture
	// handoff is where this build parked its own successor offer, for the
	// producer that collects the bet to start slot 1 from. See speculativeHandoff.
	handoff *speculativeHandoff
	expiry  time.Time
	expire  *time.Timer
	// report accounts for this bet exactly once, on whichever path takes it out
	// of the slot. It travels with the bet because three of those paths have no
	// caller to report for them: the expiry timer, the producer that refuses a
	// bet it cannot use, and session retirement.
	report func(PipelineHandoffOutcome)
}

// settle reports the bet's outcome once. Every exit path calls it, so
// speculative_started equals the sum of the outcomes below and a bet that
// vanishes without one is a bug rather than a gap in the accounting.
func (s *speculativeProduction) settle(outcome PipelineHandoffOutcome) {
	if s.report != nil {
		s.report(outcome)
	}
}

// collectable reports whether a parked bet may still be handed to a producer:
// its lifetime has not run out and its build can still serve.
func (s *speculativeProduction) collectable(now time.Time) bool {
	if !s.expiry.IsZero() && !now.Before(s.expiry) {
		return false
	}

	return s.future.viable(now)
}

// disarm stops the expiry timer of a bet that has left the slot. Best effort:
// a timer that already fired finds the slot empty and does nothing.
func (s *speculativeProduction) disarm() {
	if s.expire != nil {
		s.expire.Stop()
	}
}

// speculationSlot is where a bet waits for the producer that may collect it.
// One mutex-guarded cell, in the shape successorSlot already established: the
// producer arrives from one place, but the paths that invalidate a bet — a
// window opening on another base, a session retiring, the service closing — are
// several, and each of them has to be able to take it down exactly once.
type speculationSlot struct {
	mu      sync.Mutex
	current *speculativeProduction
	closed  bool
}

// install parks a started build. It reports false when the slot is closed or
// already holds a bet, and the caller must then stop what it started.
//
// A bet with an expiry is armed to abandon itself when it runs out while still
// parked. Nothing else bounds a bet whose window never comes: the paths that
// settle bets are driven by consensus progress, and a stalled session delivers
// none.
func (s *speculationSlot) install(spec *speculativeProduction) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.current != nil {
		return false
	}
	s.current = spec
	if !spec.expiry.IsZero() {
		spec.expire = time.AfterFunc(time.Until(spec.expiry), func() { s.expire(spec) })
	}

	return true
}

// expire abandons spec if it is still the parked bet. It runs from the expiry
// timer, so by the time it fires the bet may already have been adopted or
// replaced; both leave the slot not holding spec, and both are no-ops here.
func (s *speculationSlot) expire(spec *speculativeProduction) {
	s.mu.Lock()
	if s.current != spec {
		s.mu.Unlock()

		return
	}
	s.current = nil
	s.mu.Unlock()

	spec.settle(PipelineHandoffSpeculativeMissed)
	spec.abandon()
}

// pending reports the window a bet is in flight for. The service reads it to
// refuse a second bet on the same window without taking the build down.
func (s *speculationSlot) pending() (uint32, simplex.CandidateID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return 0, simplex.CandidateID{}, false
	}

	return s.current.startSlot, s.current.base, true
}

// takeMatching removes and returns the parked build only when it was started
// for this window and on this base. Both halves are load-bearing. The window
// start alone would hand a producer a build whose predecessor consensus did not
// select — the block would be signed for a parent the network skipped. The base
// alone would survive a session that moved on to a later window whose base
// happens to be the same candidate, which cannot happen today only because a
// base is always inside the preceding window.
//
// A parked build that does not match is stopped here rather than left running:
// the CPU it would spend is worth more to the collation the producer is about
// to start on the real base.
func (s *speculationSlot) takeMatching(
	startSlot uint32,
	base simplex.ParentID,
) (*candidateBuildFuture, *speculativeHandoff, bool) {
	s.mu.Lock()
	spec := s.current
	if spec == nil {
		s.mu.Unlock()

		return nil, nil, false
	}
	// Collectability is checked with the same authority as the base: a build
	// that failed, or a bet whose lifetime ran out while the window it was for
	// had not opened, is not this slot's build. Adopting it would make its
	// error the slot's error — see candidateBuildFuture.viable.
	now := time.Now()
	sameWindow := spec.startSlot == startSlot && base.Exists && base.ID == spec.base
	matched := sameWindow && spec.collectable(now)
	s.current = nil
	s.mu.Unlock()
	spec.disarm()

	if !matched {
		// A bet refused for the window that did open on its base was not lost to
		// consensus: its build could not serve. That is the one refusal worth
		// telling apart, because it is the failure mode that hides — the build
		// error is deliberately kept off the collation series.
		outcome := PipelineHandoffSpeculativeMissed
		if sameWindow && !spec.future.viable(now) {
			outcome = PipelineHandoffSpeculativeFailed
		}
		spec.settle(outcome)
		spec.abandon()

		return nil, nil, false
	}

	return spec.future, spec.handoff, true
}

// dropOutdated takes down a bet the newly observed window has settled against,
// and is what makes a lost bet cost nothing beyond its own CPU. It runs on
// every observed window, not only the ones this node leads, because a bet is
// placed on the window after the one the session is in and the producer that
// would collect it may never start.
//
// It must run BEFORE that producer does, which is where it is called from: the
// consensus progress that installs the window is also what makes production
// eligible for it. A bet that matches the window and its base is left parked
// for the producer; everything else is stopped here.
func (s *speculationSlot) dropOutdated(startSlot uint32, base simplex.ParentID) bool {
	s.mu.Lock()
	spec := s.current
	if spec == nil {
		s.mu.Unlock()

		return false
	}
	if spec.startSlot == startSlot && base.Exists && base.ID == spec.base {
		s.mu.Unlock()

		return false
	}
	s.current = nil
	s.mu.Unlock()
	spec.disarm()

	spec.settle(PipelineHandoffSpeculativeMissed)
	spec.abandon()

	return true
}

// abandon cancels a bet without waiting for it to unwind. See
// candidateBuildFuture.abandon, which now says the same thing for the pipelined
// successor: both paths that take a bet down — the consensus progress that
// observed another base, and the producer that refused it — run in front of the
// collation that replaces it, and a join there would put an arbitrary stall
// between the window opening and its first block starting.
func (s *speculativeProduction) abandon() {
	s.future.abandon()
}

// close stops any resident bet and refuses every later one.
func (s *speculationSlot) close() {
	s.mu.Lock()
	spec := s.current
	s.current, s.closed = nil, true
	s.mu.Unlock()
	if spec != nil {
		spec.disarm()
		// Counted with the bets consensus lost, so the accounting closes: the
		// session going away is one more window this bet never served.
		spec.settle(PipelineHandoffSpeculativeMissed)
		// Cancelled here and joined elsewhere: this runs from RetireSession with
		// the session's control lock held, and a bet inside a TVM execution
		// would hold that lock for as long as it takes to notice.
		spec.future.abandon()
	}
}
