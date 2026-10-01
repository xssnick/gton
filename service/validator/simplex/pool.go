package simplex

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// proven is a vote we hold evidence for: either a directly signed vote or a
// vote extracted from a certificate (cert, which then carries the evidence).
// The serialized misbehavior evidence is not kept: it is fully derivable from
// what is stored here and equivocation is far too rare to pay one TL encode
// and ~5 heap objects per inbound vote, retained until the slot is pruned.
type proven struct {
	set      bool
	fromCert bool
	vote     Vote
	sig      []byte
	cert     *Certificate
}

// proofBytes rebuilds the canonical wire evidence for the vote of validator
// idx: the certificate that proved it, or the signed vote message itself.
// Both encoders are deterministic (Certificate.Serialize is the memoized
// canonical re-encode, never the received frame), so the bytes are exactly
// the ones the pool used to cache.
func (p proven) proofBytes(idx uint32) []byte {
	if p.cert != nil {
		return p.cert.Serialize()
	}
	sv := SignedVote{ValidatorIndex: idx, Vote: p.vote, Signature: p.sig}
	return sv.Serialize()
}

// conflictEvidence is the pair of contradicting votes behind a
// MisbehaviorConflictingVotes report. It holds the two records BY VALUE:
// validatorVotes.add rolls the just-inserted record back (*slot = proven{})
// after checkInvariants returns, so a pointer into the record would yield a
// zeroed proven and one half of the proof would come out empty.
type conflictEvidence struct {
	a, b proven
}

// misbehavior renders the deferred evidence for validator idx.
func (c conflictEvidence) misbehavior(idx uint32) Misbehavior {
	return Misbehavior{
		Kind:   MisbehaviorConflictingVotes,
		Proof1: c.a.proofBytes(idx),
		Proof2: c.b.proofBytes(idx),
	}
}

// validatorVotes is the per-validator per-slot voting record with the
// Simplex conflict rules:
//   - at most one notarize, one finalize, one skip;
//   - a second, different notarize/finalize is a provable conflict;
//   - notarize and finalize must reference the same candidate;
//   - finalize and skip are mutually exclusive;
//   - notarize and skip may coexist.
type validatorVotes struct {
	notarize proven
	skip     proven
	finalize proven
}

func (vv *validatorVotes) byKind(k VoteKind) *proven {
	switch k {
	case VoteNotarize:
		return &vv.notarize
	case VoteFinalize:
		return &vv.finalize
	case VoteSkip:
		return &vv.skip
	}
	panic(fmt.Sprintf("simplex: invalid vote kind %d", k))
}

// wants reports whether a vote of this kind would be new for the validator —
// the cheap dedup gate placed before signature verification.
func (vv *validatorVotes) wants(v Vote) bool {
	return !vv.byKind(v.Kind).set
}

// add applies a proven vote. It returns whether the vote was newly applied
// and, on a provable conflict, the two contradicting records; on conflict the
// vote is rolled back and nothing is applied.
func (vv *validatorVotes) add(p proven) (bool, *conflictEvidence) {
	slot := vv.byKind(p.vote.Kind)
	if slot.set {
		if (p.vote.Kind == VoteNotarize || p.vote.Kind == VoteFinalize) && slot.vote != p.vote {
			return false, &conflictEvidence{a: p, b: *slot}
		}
		return false, nil
	}
	*slot = p
	if c := vv.checkInvariants(); c != nil {
		*slot = proven{}
		return false, c
	}
	return true, nil
}

func (vv *validatorVotes) checkInvariants() *conflictEvidence {
	if vv.notarize.set && vv.finalize.set && vv.notarize.vote.ID != vv.finalize.vote.ID {
		return &conflictEvidence{a: vv.notarize, b: vv.finalize}
	}
	if vv.finalize.set && vv.skip.set {
		return &conflictEvidence{a: vv.finalize, b: vv.skip}
	}
	return nil
}

// poolSlot is the per-slot consensus state of the pool.
type poolSlot struct {
	votes []validatorVotes

	notarCert, skipCert, finalCert       *Certificate
	notarSaving, skipSaving, finalSaving bool

	skipWeight     uint64
	notarizeWeight map[CandidateID]uint64
	finalizeWeight map[CandidateID]uint64

	availableBase *ParentID
}

func newPoolSlot(validators int) *poolSlot {
	return &poolSlot{
		votes:          make([]validatorVotes, validators),
		notarizeWeight: map[CandidateID]uint64{},
		finalizeWeight: map[CandidateID]uint64{},
	}
}

func (s *poolSlot) certByKind(k VoteKind) (**Certificate, *bool) {
	switch k {
	case VoteNotarize:
		return &s.notarCert, &s.notarSaving
	case VoteFinalize:
		return &s.finalCert, &s.finalSaving
	case VoteSkip:
		return &s.skipCert, &s.skipSaving
	}
	panic(fmt.Sprintf("simplex: invalid vote kind %d", k))
}

// needs reports whether the slot still lacks a certificate of the vote's kind.
func (s *poolSlot) needs(v Vote) bool {
	cert, saving := s.certByKind(v.Kind)
	return *cert == nil && !*saving
}

func (s *poolSlot) willBe(k VoteKind) bool {
	cert, saving := s.certByKind(k)
	return *cert != nil || *saving
}

// notarizedBlock returns the candidate this slot is notarized for, from the
// notarization or finalization certificate.
func (s *poolSlot) notarizedBlock() *CandidateID {
	if s.notarCert != nil {
		id := s.notarCert.Vote.ID
		return &id
	}
	if s.finalCert != nil {
		id := s.finalCert.Vote.ID
		return &id
	}
	return nil
}

// addAvailableBase keeps the best (highest) known base for building on top of
// this slot, preferring parents from higher slots for forward progress.
func (s *poolSlot) addAvailableBase(p ParentID) {
	if s.availableBase == nil || p.Compare(*s.availableBase) >= 0 {
		cp := p
		s.availableBase = &cp
	}
}

// parentWaitOutcome is the resolution of a parent wait.
type parentWaitOutcome struct {
	misb *Misbehavior // leader misbehavior proof; pipeline aborts
	err  error        // benign abort (e.g. slot already finalized)
}

// evaluateParentWait decides whether a pending parent request can be resolved.
// It returns done=false when the request must keep waiting for more
// certificates.
func (e *Engine) evaluateParentWait(cand *Candidate) (bool, parentWaitOutcome) {
	id := cand.ID
	parent := cand.Parent

	nextSlotAfterParent := uint32(0)
	if parent.Exists {
		nextSlotAfterParent = parent.ID.Slot + 1
	}

	if id.Slot < e.slots.firstNonFinalized {
		return true, parentWaitOutcome{err: fmt.Errorf("candidate's slot is already finalized")}
	}
	if nextSlotAfterParent < e.slots.firstNonFinalized {
		return true, parentWaitOutcome{misb: e.conflictingCandidateAndCert(cand, e.lastFinalCert)}
	}

	slot := e.slots.at(id.Slot)
	if nb := slot.notarizedBlock(); nb != nil && *nb != id {
		return true, parentWaitOutcome{misb: e.conflictingCandidateAndCert(cand, slot.notarCert)}
	}

	if nextSlotAfterParent == e.slots.firstNonFinalized {
		if e.lastFinalized != parent {
			// first_nonfinalized == 0 implies parent == genesis == lastFinalized,
			// so reaching this branch means at least one slot is finalized.
			return true, parentWaitOutcome{misb: e.conflictingCandidateAndCert(cand, e.lastFinalCert)}
		}
	} else {
		// The next slot after the parent is above the finalized horizon,
		// so a parent exists.
		parentSlot := e.slots.at(parent.ID.Slot)
		if parentSlot.notarCert != nil {
			if nb := parentSlot.notarizedBlock(); nb == nil || *nb != parent.ID {
				return true, parentWaitOutcome{misb: e.conflictingCandidateAndCert(cand, parentSlot.notarCert)}
			}
		} else {
			// Parent is not yet notarized, try again later.
			return false, parentWaitOutcome{}
		}
	}

	if nextSlotAfterParent == id.Slot {
		return true, parentWaitOutcome{}
	}

	next := e.slots.at(nextSlotAfterParent)
	if next.skipCert == nil {
		// Not enough skip certificates yet.
		return false, parentWaitOutcome{}
	}
	runEnd, err := e.skipRuns.lowerBound(nextSlotAfterParent)
	if err != nil {
		e.fatal(err)
		return true, parentWaitOutcome{err: fmt.Errorf("internal error")}
	}
	if runEnd >= id.Slot {
		return true, parentWaitOutcome{}
	}
	return false, parentWaitOutcome{}
}

// conflictingCandidateAndCert builds the misbehavior proof for a candidate
// that contradicts an existing certificate. The proof payload is unspecified;
// we attach the candidate hash data and the certificate when available.
func (e *Engine) conflictingCandidateAndCert(c *Candidate, cert *Certificate) *Misbehavior {
	m := &Misbehavior{Kind: MisbehaviorConflictingCandidateAndCertificate, Proof1: c.HashDataBytes()}
	if cert != nil {
		m.Proof2 = cert.Serialize()
	}
	return m
}

func (e *Engine) registerParentWait(c *Candidate) {
	if done, out := e.evaluateParentWait(c); done {
		e.enqueue(func() { e.onParentWaitDone(c, out) })
		return
	}
	e.parentWaits = append(e.parentWaits, c)
}

func (e *Engine) resolveParentWaits() {
	for i := 0; i < len(e.parentWaits); {
		cand := e.parentWaits[i]
		done, out := e.evaluateParentWait(cand)
		if !done {
			i++
			continue
		}
		last := len(e.parentWaits) - 1
		e.parentWaits[i] = e.parentWaits[last]
		e.parentWaits = e.parentWaits[:last]
		e.enqueue(func() { e.onParentWaitDone(cand, out) })
	}
}

// ---- vote application and weight accounting ----

// applyVote adds a signed vote to the pool. The misbehavior evidence is not
// passed in: it is rebuilt from the stored record only if the validator turns
// out to equivocate.
func (e *Engine) applyVote(idx uint32, v Vote, sig []byte, tolerateConflicts bool) bool {
	slot := e.slots.at(v.Slot())
	if slot == nil {
		if (v.Kind == VoteNotarize || v.Kind == VoteFinalize) && e.lastFinalized.Exists && v.ID == e.lastFinalized.ID {
			return false
		}
		e.log.Warn().Msgf("dropping %s from validator %d which references a finalized slot", v, idx)
		return false
	}

	applied, ev := slot.votes[idx].add(proven{set: true, vote: v, sig: sig})
	if ev != nil {
		if int(idx) == e.localIndex && !tolerateConflicts {
			e.fatal(fmt.Errorf("simplex: we produced conflicting votes, conflict occurred for %s", v))
			return false
		}
		e.reportMisbehavior(idx, ev.misbehavior(idx))
		return false
	}
	if !applied {
		return false
	}
	e.noteOwnCandidateVote(idx, v)
	e.onVoteApplied(idx, v, slot)
	return true
}

// onVoteApplied accumulates quorum weight and builds a certificate when the
// 2/3+1 threshold is crossed. Each validator is counted at most once per
// (slot, kind, candidate), so sums stay below the 2^61-capped total weight.
func (e *Engine) onVoteApplied(idx uint32, v Vote, slot *poolSlot) {
	w := e.validators()[idx].Weight
	var newWeight uint64
	switch v.Kind {
	case VoteNotarize:
		slot.notarizeWeight[v.ID] += w
		newWeight = slot.notarizeWeight[v.ID]
	case VoteFinalize:
		slot.finalizeWeight[v.ID] += w
		newWeight = slot.finalizeWeight[v.ID]
	case VoteSkip:
		slot.skipWeight += w
		newWeight = slot.skipWeight
	}
	if newWeight >= e.threshold() && !slot.willBe(v.Kind) {
		// Attested, not verified — see Engine.seal for the evidence chain that
		// makes every signature in this certificate already checked.
		e.handleCertificate(e.seal(e.buildCertificate(slot, v)))
	}
}

// buildCertificate assembles a certificate from the directly signed votes
// stored in the slot that match the given vote exactly.
func (e *Engine) buildCertificate(slot *poolSlot, v Vote) *Certificate {
	cert := &Certificate{Vote: v}
	for i := range slot.votes {
		p := slot.votes[i].byKind(v.Kind)
		if p.set && !p.fromCert && p.vote == v {
			cert.Signatures = append(cert.Signatures, VoteSignature{ValidatorIndex: uint32(i), Signature: p.sig})
		}
	}
	e.stats.CertificatesBuilt++
	return cert
}

// ---- certificate persistence and application ----

// handleCertificate starts persisting a fresh certificate. This is the single
// entry point for both network certificates and locally assembled ones; the
// saving flag makes the persist single-flight per (slot, kind) while the engine
// keeps serving other inputs.
func (e *Engine) handleCertificate(vc VerifiedCertificate) {
	cert := vc.certificate()
	slot := e.slots.at(cert.Vote.Slot())
	if slot == nil || !slot.needs(cert.Vote) {
		return
	}
	_, saving := slot.certByKind(cert.Vote.Kind)
	*saving = true

	// Journal is injected code and may retain its argument. Hand it a copy so
	// it cannot mutate the certificate carried by vc after this callback seals
	// the persistence transition.
	e.journal.SaveCertificate(vc.Certificate(), e.journalDone(func(err error) {
		e.finishCertificateSave(vc, err)
	}))
}

// finishCertificateSave resumes certificate application once the persistence
// callback succeeds. The pool may have moved on while the write was in flight:
// a finalization can prune the slot, in which case the certificate is obsolete
// and dropped, which is why the slot is re-resolved after the write.
func (e *Engine) finishCertificateSave(vc VerifiedCertificate, err error) {
	cert := vc.certificate()
	if err != nil && !errors.Is(err, ErrAlreadySaved) {
		e.fatal(fmt.Errorf("simplex: certificate journal write: %w", err))
		return
	}
	slot := e.slots.at(cert.Vote.Slot())
	if slot == nil {
		return
	}

	// Extract the individual votes proven by the certificate into the
	// per-validator records; the certificate itself is the conflict evidence.
	for _, s := range cert.Signatures {
		p := proven{set: true, fromCert: true, vote: cert.Vote, sig: s.Signature, cert: cert}
		if _, ev := slot.votes[s.ValidatorIndex].add(p); ev != nil {
			e.reportMisbehavior(s.ValidatorIndex, ev.misbehavior(s.ValidatorIndex))
		}
	}

	e.storeSavedCertificate(slot, vc)
}

// storeSavedCertificate records the accepted certificate in the slot, gossips
// it and runs the typed transition.
func (e *Engine) storeSavedCertificate(slot *poolSlot, vc VerifiedCertificate) {
	cert := vc.certificate()
	certSlot, saving := slot.certByKind(cert.Vote.Kind)
	if *certSlot != nil {
		e.fatal(fmt.Errorf("simplex: duplicate %s certificate for slot %d", cert.Vote.Kind, cert.Vote.Slot()))
		return
	}
	*certSlot = cert
	*saving = false
	e.stats.CertificatesStored++
	if kind := cert.Vote.Kind; kind < VoteKindCount {
		e.stats.CertificatesByKind[kind]++
		e.stats.SignaturesByKind[kind] += uint64(len(cert.Signatures))
	}

	voters := make([]byte, len(e.validators()))
	for i := range voters {
		voters[i] = '.'
	}
	if e.stats.LastSignedSlot == nil {
		e.stats.LastSignedSlot = make([]uint32, len(voters))
	}
	slotNumber := cert.Vote.Slot()
	for _, s := range cert.Signatures {
		voters[s.ValidatorIndex] = 'V'
		// Highest slot this validator has been seen signing, which is what says
		// a voter has gone quiet before the session stops finalizing.
		if int(s.ValidatorIndex) < len(e.stats.LastSignedSlot) &&
			slotNumber > e.stats.LastSignedSlot[s.ValidatorIndex] {
			e.stats.LastSignedSlot[s.ValidatorIndex] = slotNumber
		}
	}
	e.log.Info().Msgf("obtained certificate for %s: %s", cert.Vote, voters)
	e.reportOwnCandidateNotarized(cert)

	if !e.suppressCertBroadcast {
		e.transport.BroadcastToRandom(e.params.CertificateGossipNeighbors, cert.Serialize())
		e.trace(TraceCertObserved{Vote: cert.Vote})
	}

	switch cert.Vote.Kind {
	case VoteNotarize:
		e.onNotarCertificate(vc)
	case VoteSkip:
		e.onSkipCertificate(slot, cert)
	case VoteFinalize:
		e.onFinalCertificate(slot, vc)
	}
	if e.failed() {
		return
	}
	e.resolveParentWaits()
}

func (e *Engine) onNotarCertificate(vc VerifiedCertificate) {
	id := vc.Vote().ID

	e.enqueue(func() { e.hooks.OnNotarized(id, vc) })
	if e.voter != nil {
		e.enqueue(func() { e.voterNotarizationObserved(id) })
	}

	next, err := e.nextNonSkippedSlotAfter(id.Slot)
	if err != nil {
		e.fatal(err)
		return
	}
	e.slots.at(next).addAvailableBase(Parent(id))
	e.advancePresent()
}

func (e *Engine) onSkipCertificate(slot *poolSlot, cert *Certificate) {
	i := cert.Vote.Slot()

	next, err := e.nextNonSkippedSlotAfter(i)
	if err != nil {
		e.fatal(err)
		return
	}

	e.skipRuns.erase(i)
	if next == i+1 {
		e.skipRuns.insert(i + 1)
	}

	if slot.availableBase != nil {
		e.slots.at(next).addAvailableBase(*slot.availableBase)
	}
	e.advancePresent()
}

func (e *Engine) onFinalCertificate(slot *poolSlot, vc VerifiedCertificate) {
	cert := vc.certificate()
	id := cert.Vote.ID

	// A finalize quorum over a skipped slot (or over two different
	// candidates) proves that more than 1/3 of the weight is faulty. The
	// reference aborts the process; we fail the session closed instead.
	if slot.skipCert != nil {
		e.fatal(fmt.Errorf("simplex: consensus violation: slot %d has both finalize and skip certificates", id.Slot))
		return
	}
	if nb := slot.notarizedBlock(); nb != nil && *nb != id {
		e.fatal(fmt.Errorf("simplex: consensus violation: slot %d finalized %s but notarized %s", id.Slot, id, *nb))
		return
	}
	if slot.notarCert == nil {
		next, err := e.nextNonSkippedSlotAfter(id.Slot)
		if err != nil {
			e.fatal(err)
			return
		}
		e.slots.at(next).addAvailableBase(Parent(id))
	}

	e.lastFinalized = Parent(id)
	e.lastFinalCert = cert
	e.slots.notifyFinalized(id.Slot)
	e.stats.SlotsFinalized++
	e.stats.LastFinalizedAt = e.clock.Now()

	e.enqueue(func() { e.hooks.OnFinalized(id, vc) })
	if e.voter != nil {
		e.enqueue(func() { e.voterFinalizationObserved(id) })
	}

	if e.now <= id.Slot {
		e.now = id.Slot + 1
		e.advancePresent()
	}

	e.skipRuns.eraseUpTo(id.Slot)
	for s := range e.seenBroadcasts {
		if s < e.slots.firstNonFinalized {
			delete(e.seenBroadcasts, s)
		}
	}

	// A new finalization both reschedules the standstill alarm and aborts an
	// in-flight standstill resolution broadcast.
	if !e.suppressCertBroadcast {
		e.standstillAt = e.clock.Now().Add(e.params.StandstillTimeout)
	}
	e.drain = nil
}

// nextNonSkippedSlotAfter returns the first slot after i that has no skip
// certificate, jumping over a whole skip run in O(log n). A missing run end
// is an internal invariant error.
func (e *Engine) nextNonSkippedSlotAfter(i uint32) (uint32, error) {
	j := i + 1
	if e.slots.at(j).skipCert != nil {
		end, err := e.skipRuns.lowerBound(j)
		if err != nil {
			return 0, err
		}
		j = end
	}
	return j, nil
}

// advancePresent moves the present slot past settled (notarized or skipped)
// slots and announces newly entered leader windows.
func (e *Engine) advancePresent() {
	if !e.started || e.failed() {
		return
	}
	for {
		s := e.slots.at(e.now)
		if s == nil {
			e.fatal(fmt.Errorf("simplex: internal: present slot %d below finalized horizon %d", e.now, e.slots.firstNonFinalized))
			return
		}
		if s.notarCert != nil || s.skipCert != nil {
			e.now++
			continue
		}
		break
	}

	window := e.now / e.spw
	if e.firstNonAnnouncedWindow > window {
		return
	}
	e.firstNonAnnouncedWindow = window + 1

	base := Genesis()
	if e.now != 0 {
		s := e.slots.at(e.now)
		if s.availableBase == nil {
			e.fatal(fmt.Errorf("simplex: internal: no available base at slot %d", e.now))
			return
		}
		base = *s.availableBase
	}
	startSlot := e.now
	// Match the reference bus flow: consumers start resolving the parent and
	// collating as soon as the window is observed, while DB persists its pool
	// cursor independently. The cursor is restart progress, not authority; a
	// failed write is still fatal when its callback returns.
	e.windowObserved(startSlot, base)
	e.journal.SaveFirstNonAnnouncedWindow(e.firstNonAnnouncedWindow, e.journalDone(func(err error) {
		if err != nil {
			e.fatal(fmt.Errorf("simplex: pool state journal write: %w", err))
		}
	}))
}

func (e *Engine) windowObserved(startSlot uint32, base ParentID) {
	firstBlockTimeout := e.params.FirstBlockTimeout
	if e.voter != nil {
		e.voterWindowObserved(startSlot)
		firstBlockTimeout = e.voter.firstBlockTimeout
	}

	windowStart := startSlot - startSlot%e.spw
	windowEnd := windowStart + e.spw
	leader := e.schedule.ExpectedLeader(windowStart)
	window := Window{
		Base:              base,
		ObservedSlot:      startSlot,
		StartSlot:         windowStart,
		EndSlot:           windowEnd,
		Leader:            leader,
		LocalLeader:       e.localIndex >= 0 && leader == uint32(e.localIndex),
		ObservedAt:        e.clock.Now(),
		FirstBlockTimeout: firstBlockTimeout,
	}
	e.enqueue(func() { e.hooks.HandleWindow(window) })
}

// ---- standstill detection and resolution ----

type drainState struct {
	msgs             [][]byte
	idx              int
	quota            float64
	quotaTime        time.Time
	nextAt           time.Time
	finalCertAtStart *Certificate
}

func (e *Engine) fireStandstill(now time.Time) {
	e.log.Warn().Msgf("standstill detected, current pool state:\n%s", e.DebugDump())
	e.stats.Standstills++
	e.standstillAt = now.Add(e.params.StandstillTimeout)

	// Piggyback periodic maintenance on the standstill tick: expired bans
	// would otherwise only be dropped when the same peer talks again.
	for p, until := range e.bans {
		if now.After(until) {
			delete(e.bans, p)
		}
	}
	// Standstill resolution runs only for voting identities.
	// Collator/observer Pools still track and log the alarm, but never emit
	// certificates or relay votes from that maintenance path.
	if e.localIndex == ObserverIndex {
		return
	}

	// A resolution task that finishes its current paced broadcast before
	// reacting to a new trigger does not lose the trigger: it lands on the
	// bridge recreated after every pass, so a standstill that fires mid-drain
	// queues another full pass.
	//
	// We deliberately drop it instead. The queued pass only ever fires when a
	// drain outlives the standstill timeout, i.e. exactly when the egress
	// budget is already saturated, and queuing then degenerates into a
	// permanent back-to-back rebroadcast of the whole pool. Dropping the
	// trigger costs nothing: the alarm was just rescheduled, so the next pass
	// starts one timeout after the drain instead of immediately.
	if e.drain != nil {
		return
	}
	e.startDrain(now)
}

func (e *Engine) startDrain(now time.Time) {
	e.drain = &drainState{
		msgs:             e.buildStandstillMessages(),
		quota:            0,
		quotaTime:        now.Add(-10 * time.Millisecond),
		nextAt:           now,
		finalCertAtStart: e.lastFinalCert,
	}
	e.stepDrain(now)
}

// buildStandstillMessages snapshots everything a lagging peer may need: the
// last finalization certificate, all per-slot certificates and our own votes
// that are not yet covered by a certificate.
func (e *Engine) buildStandstillMessages() [][]byte {
	var msgs [][]byte
	if e.lastFinalCert != nil {
		msgs = append(msgs, e.lastFinalCert.Serialize())
	}
	begin, end := e.slots.interval()
	for i := begin; i < end; i++ {
		// peek(), never at(): certificates from far ahead are accepted as proof
		// of session progress, so the tracked range is sparse after a catch-up
		// and at() would materialize a full poolSlot for every slot in the gap —
		// hundreds of megabytes for a single far certificate, re-paid on every
		// standstill tick. An untracked slot has neither certificates nor our
		// own votes, so skipping it yields exactly the same message set.
		s := e.slots.peek(i)
		if s == nil {
			continue
		}
		for _, cert := range []*Certificate{s.notarCert, s.skipCert, s.finalCert} {
			if cert != nil {
				msgs = append(msgs, cert.Serialize())
			}
		}
		if e.localIndex >= 0 {
			own := &s.votes[e.localIndex]
			for _, k := range []VoteKind{VoteNotarize, VoteSkip, VoteFinalize} {
				p := own.byKind(k)
				cert, _ := s.certByKind(k)
				if p.set && !p.fromCert && *cert == nil {
					sv := SignedVote{ValidatorIndex: uint32(e.localIndex), Vote: p.vote, Signature: p.sig}
					msgs = append(msgs, sv.Serialize())
				}
			}
		}
	}
	return msgs
}

// stepDrain sends queued standstill messages under the egress budget
// (bytes * validator count per second) using token accounting.
func (e *Engine) stepDrain(now time.Time) {
	d := e.drain
	if d == nil {
		return
	}
	if e.lastFinalCert != d.finalCertAtStart {
		e.drain = nil
		return
	}
	// The egress budget splits across sibling shards and never drops below
	// the floor: max(min, max / 2^pfx_len).
	rate := float64(e.params.StandstillMaxEgressBytesPerS) / float64(uint64(1)<<e.shardPfxLen)
	if minRate := float64(e.params.StandstillMinEgressBytesPerS); rate < minRate {
		rate = minRate
	}
	// Params are validated on ingress, but a large shard prefix can still
	// round the split budget down to zero; never let it become a divisor of 0
	// and wedge the drain forever.
	if rate < 1 {
		rate = 1
	}
	for d.idx < len(d.msgs) {
		msg := d.msgs[d.idx]
		cost := float64(len(msg) * len(e.validators()))
		if d.quota < cost {
			delay := (cost - d.quota) / rate
			at := d.quotaTime.Add(time.Duration(delay * float64(time.Second)))
			if at.After(now) {
				d.nextAt = at
				return
			}
			refilled := d.quota + now.Sub(d.quotaTime).Seconds()*rate
			if refilled > rate {
				refilled = rate
			}
			d.quota = refilled
			d.quotaTime = now
		}
		d.quota -= cost
		e.transport.BroadcastToValidators(msg)
		d.idx++
	}
	e.drain = nil
}

// DebugDump renders the pool state in the canonical debug format: one line per
// tracked slot, one character per validator (F finalize, N notarize, S skip,
// I notarize+skip, . nothing), plus slot certificate flags.
func (e *Engine) DebugDump() string {
	var sb strings.Builder
	if e.lastFinalCert != nil {
		fmt.Fprintf(&sb, "Last final cert is for %s\n", e.lastFinalCert.Vote.ID)
	}
	begin, end := e.slots.interval()
	for i := begin; i < end; i++ {
		s := e.slots.peek(i)
		if s == nil {
			continue
		}
		fmt.Fprintf(&sb, "%d: ", i)
		for v := range s.votes {
			vv := &s.votes[v]
			switch {
			case vv.finalize.set:
				sb.WriteByte('F')
			case vv.notarize.set && vv.skip.set:
				sb.WriteByte('I')
			case vv.notarize.set:
				sb.WriteByte('N')
			case vv.skip.set:
				sb.WriteByte('S')
			default:
				sb.WriteByte('.')
			}
		}
		if s.notarCert != nil {
			sb.WriteString(" notar")
		}
		if s.skipCert != nil {
			sb.WriteString(" skip")
		}
		if s.finalCert != nil {
			sb.WriteString(" final")
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}
