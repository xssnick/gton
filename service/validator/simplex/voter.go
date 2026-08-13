package simplex

import (
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

// voterSlot is the per-slot voting-policy state.
type voterSlot struct {
	pending    *Candidate
	votedNotar *CandidateID
	notarCert  *CandidateID
	votedSkip  bool
	votedFinal bool

	// notarization pipeline of the pending candidate: the durable store runs
	// concurrently with the parent wait and the validation gate, and both must
	// succeed before the notarize vote is cast.
	storeInFlight      bool
	stored             bool
	validationInFlight bool
	validated          bool
}

// voterState is the voting policy: when to notarize, finalize and skip. It
// exists only on validators.
type voterState struct {
	slots *slotMap[voterSlot]

	timeoutBase           time.Time
	timeoutSlot           uint32
	alarmAt               time.Time
	alarmArmed            bool
	firstBlockTimeout     time.Duration
	previousWindowHadSkip bool
	currentWindow         uint32
}

func newVoterState(firstBlockTimeout time.Duration) *voterState {
	return &voterState{
		slots:             newSlotMap(func() *voterSlot { return &voterSlot{} }),
		firstBlockTimeout: firstBlockTimeout,
	}
}

// bootstrapVoter restores per-slot voting flags from the journaled own votes,
// dropping records that violate local invariants (a pre-crash bug may have
// stored conflicting votes; recovery must not corrupt slot invariants).
func (e *Engine) bootstrapVoter(votes []Vote) {
	v := e.voter
	for _, vote := range votes {
		slot := v.slots.at(vote.Slot())
		if slot == nil {
			continue
		}
		switch vote.Kind {
		case VoteNotarize:
			if slot.votedNotar != nil && *slot.votedNotar != vote.ID {
				e.log.Warn().Msgf("dropping corrupted %s during recovery", vote)
				continue
			}
			id := vote.ID
			slot.votedNotar = &id
		case VoteFinalize:
			if slot.votedSkip || (slot.votedNotar != nil && *slot.votedNotar != vote.ID) {
				e.log.Warn().Msgf("dropping corrupted %s during recovery", vote)
				continue
			}
			slot.votedFinal = true
		case VoteSkip:
			if slot.votedFinal {
				e.log.Warn().Msgf("dropping corrupted %s during recovery", vote)
				continue
			}
			slot.votedSkip = true
		}
	}
}

// castStartupWindowSkips re-casts skip votes for every non-finalized slot of
// the last announced leader window. This is the crash-recovery move that
// lets the network get past a window whose production we may have
// interrupted; already-journaled votes deduplicate in the journal.
func (e *Engine) castStartupWindowSkips() {
	if e.firstNonAnnouncedWindow == 0 {
		return
	}
	v := e.voter
	window := e.firstNonAnnouncedWindow - 1
	for i := window * e.spw; i < (window+1)*e.spw; i++ {
		slot := v.slots.at(i)
		if slot == nil || slot.votedFinal {
			continue
		}
		slot.votedSkip = true
		e.castVote(SkipVote(i), false)
	}
}

// voterWindowObserved updates validator-only voting timers after the pool
// enters a new leader window. The shared HandleWindow hook is emitted by
// windowObserved for validators and observers alike.
func (e *Engine) voterWindowObserved(startSlot uint32) {
	v := e.voter
	newWindow := startSlot / e.spw
	v.currentWindow = newWindow

	if v.previousWindowHadSkip {
		scaled := time.Duration(float64(v.firstBlockTimeout) * e.params.FirstBlockTimeoutMultiplier)
		if scaled > e.params.FirstBlockTimeoutCap {
			scaled = e.params.FirstBlockTimeoutCap
		}
		v.firstBlockTimeout = scaled
	} else {
		v.firstBlockTimeout = e.params.FirstBlockTimeout
	}

	if startSlot%e.spw == 0 {
		v.previousWindowHadSkip = false
	}

	if v.timeoutSlot <= startSlot {
		v.timeoutSlot = startSlot + 1
		v.timeoutBase = e.clock.Now().Add(v.firstBlockTimeout)
		v.alarmAt = v.timeoutBase.Add(e.params.TargetRate)
		v.alarmArmed = true
	}
}

// voterNotarizationObserved advances the per-window notarization schedule and
// tries to finalize.
func (e *Engine) voterNotarizationObserved(id CandidateID) {
	v := e.voter
	slot := v.slots.at(id.Slot)
	if slot == nil {
		return
	}

	if v.timeoutSlot <= id.Slot+1 {
		if (id.Slot+1)%e.spw == 0 {
			// At the end of the window the next timeout is set by the coming
			// LeaderWindowObserved.
			v.timeoutSlot = id.Slot + 1
		} else {
			v.timeoutSlot = id.Slot + 2
		}
		offset := v.timeoutSlot - v.currentWindow*e.spw
		v.alarmAt = v.timeoutBase.Add(time.Duration(offset) * e.params.TargetRate)
		v.alarmArmed = true
	}

	slot.notarCert = &id
	e.tryVoteFinal(slot)
}

func (e *Engine) voterFinalizationObserved(id CandidateID) {
	e.voter.slots.notifyFinalized(id.Slot)
}

// fireVoterAlarm skips every not-yet-finalized slot from the overdue one to
// the end of its leader window.
func (e *Engine) fireVoterAlarm() {
	v := e.voter
	v.alarmArmed = false
	rangeStart := v.timeoutSlot - 1
	windowStart := rangeStart - rangeStart%e.spw
	windowEnd := windowStart + e.spw
	for i := rangeStart; i < windowEnd; i++ {
		slot := v.slots.at(i)
		if slot != nil && !slot.votedFinal {
			e.castVote(SkipVote(i), false)
			slot.votedSkip = true
			v.previousWindowHadSkip = true
		}
	}
	v.timeoutSlot = windowEnd
}

// acceptCandidate is the voter part of candidate ingestion.
func (e *Engine) acceptCandidate(c *Candidate) {
	v := e.voter
	firstTooNew := (v.currentWindow + e.params.MaxLeaderWindowDesync + 1) * e.spw
	if c.ID.Slot >= firstTooNew {
		e.log.Warn().Msgf("dropping too new candidate from validator %d: slot=%d, current_window=%d",
			c.Leader, c.ID.Slot, v.currentWindow*e.spw)
		return
	}
	slot := v.slots.at(c.ID.Slot)
	if slot == nil {
		return
	}
	if slot.votedNotar != nil {
		return
	}
	if slot.pending != nil {
		// Only the first candidate per slot is considered; a different second
		// one is the leader's problem; no misbehavior report is produced.
		return
	}
	slot.pending = c
	e.stats.CandidatesAccepted++
	if c.Leader != uint32(e.localIndex) {
		block := c.Block
		var blockPtr *ton.BlockIDExt
		if !c.Empty {
			blockPtr = &block
		}
		e.trace(TraceCandidateReceived{ID: c.ID, Parent: c.Parent, Block: blockPtr})
	}
	// The durable store starts the moment the candidate is accepted, so the
	// write overlaps the parent wait and the validation gate. Both must finish
	// before we vote.
	slot.storeInFlight = true
	storeDone := e.candidateDone(c.ID, e.completeStore)
	e.enqueue(func() { e.hooks.StoreCandidate(c, storeDone) })
	e.registerParentWait(c)
}

// onParentWaitDone continues the notarization pipeline after the parent
// chain of the pending candidate is settled.
func (e *Engine) onParentWaitDone(c *Candidate, out parentWaitOutcome) {
	if e.voter == nil {
		return
	}
	if out.misb != nil {
		e.reportMisbehavior(c.Leader, *out.misb)
		return
	}
	if out.err != nil {
		e.log.Debug().Msgf("candidate %s parent wait aborted: %v", c.ID, out.err)
		return
	}
	slot := e.voter.slots.at(c.ID.Slot)
	if slot == nil || slot.pending == nil || slot.pending.ID != c.ID || slot.votedNotar != nil {
		return
	}
	if slot.validationInFlight {
		return
	}
	slot.validationInFlight = true
	validationDone := e.candidateDone(c.ID, e.completeValidation)
	e.enqueue(func() { e.hooks.ValidateCandidate(c, validationDone) })
}

// completeValidation resumes the pipeline with the validation verdict.
func (e *Engine) completeValidation(id CandidateID, verdict error) {
	if e.voter == nil {
		return
	}
	slot := e.voter.slots.at(id.Slot)
	if slot == nil || slot.pending == nil || slot.pending.ID != id || !slot.validationInFlight {
		return
	}
	slot.validationInFlight = false
	if verdict != nil {
		e.log.Warn().Msgf("candidate %s is rejected: %v", id, verdict)
		e.stats.CandidatesRejected++
		return
	}
	slot.validated = true
	e.maybeVoteNotar(id, slot)
}

// completeStore resumes the pipeline with the outcome of the durable store.
func (e *Engine) completeStore(id CandidateID, err error) {
	if e.voter == nil {
		return
	}
	slot := e.voter.slots.at(id.Slot)
	if slot == nil || slot.pending == nil || slot.pending.ID != id || !slot.storeInFlight {
		return
	}
	slot.storeInFlight = false
	if err != nil {
		e.fatal(fmt.Errorf("simplex: candidate store failed for %s: %w", id, err))
		return
	}
	slot.stored = true
	e.maybeVoteNotar(id, slot)
}

// maybeVoteNotar casts the notarize vote once the candidate is both validated
// and durably stored.
func (e *Engine) maybeVoteNotar(id CandidateID, slot *voterSlot) {
	if !slot.validated || !slot.stored || slot.votedNotar != nil {
		return
	}
	cp := id
	slot.votedNotar = &cp
	e.castVote(NotarizeVote(id), false)
	// If the notarization certificate was observed before our own vote, the
	// finalize vote may become possible right now.
	e.tryVoteFinal(slot)
}

// tryVoteFinal casts the finalize vote when our notarize vote matches the
// observed notarization certificate and no skip was cast for the slot.
func (e *Engine) tryVoteFinal(slot *voterSlot) {
	if !slot.votedSkip && !slot.votedFinal &&
		slot.votedNotar != nil && slot.notarCert != nil && *slot.votedNotar == *slot.notarCert {
		e.castVote(FinalizeVote(*slot.votedNotar), false)
		slot.votedFinal = true
	}
}
