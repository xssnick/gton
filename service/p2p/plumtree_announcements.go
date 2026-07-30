package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"slices"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
)

// Buffered IHAVEs whose signature is checked only if we end up asking that peer
// for the part: measured, 99.98% of IHAVE verifications were never used.
//
// Unverified input reaches nothing shared - a flooding peer evicts only its own
// entries, and e.missing and fecState.parts stay signature-gated.

const (
	// Must exceed plumtreeFECTotalParts: a relay announces every part it holds, so
	// a smaller cap would make a peer evict its own earlier announcements.
	plumtreeAnnouncementPeerLimit = 2 * int(plumtreeFECTotalParts)
	// Above plumtreeRepairDelay so an announcement outlives its own deadline, well
	// below plumtreeBroadcastLifetime so fabricated ids drain on their own.
	plumtreeAnnouncementTTL = 3 * time.Second
	// One ed25519 verify each on the run-loop goroutine, which also dispatches
	// sends. Leftovers keep their deadline and are picked up on the next turn.
	plumtreeCandidatesPerAlarm = 32
)

// One buffered IHAVE. Fixed-size arrays, not the wire slices: IHAVE is parsed
// with ParseNoCopy, so a retained slice would pin the QUIC frame for the TTL.
type plumtreeAnnouncement struct {
	key              plumtreePartKey
	peer             PeerID
	source           [ed25519.PublicKeySize]byte
	signature        [ed25519.SignatureSize]byte
	dataHash         [sha256.Size]byte
	payloadTimestamp float64
	dataSize         int32
	certificate      plumtreeCertificate
	announcedAt      time.Time

	previous *plumtreeAnnouncement
	next     *plumtreeAnnouncement
}

// An announcement handed out for verification: a value copy, so ed25519 runs off
// the engine lock.
type plumtreeRepairCandidate struct {
	Key              plumtreePartKey
	Peer             PeerID
	Source           keys.PublicKeyED25519
	Certificate      plumtreeCertificate
	Signature        []byte
	DataHash         []byte
	PayloadTimestamp float64
	DataSize         int32
}

// One peer's queue, oldest first: map for lookup by part, list for eviction and
// TTL draining from the front.
type plumtreePeerAnnouncements struct {
	byKey  map[plumtreePartKey]*plumtreeAnnouncement
	oldest *plumtreeAnnouncement
	newest *plumtreeAnnouncement
	count  int
}

// The caller has already checked, under this lock, that the peer has not
// announced this part and that the per-part candidate cap leaves room.
func (e *plumtreeEngine) bufferAnnouncementLocked(
	key plumtreePartKey,
	from PeerID,
	source keys.PublicKeyED25519,
	certificate plumtreeCertificate,
	message BroadcastPlumtreeIHave,
	now time.Time,
) {
	queue := e.announcements[from]
	if queue == nil {
		queue = &plumtreePeerAnnouncements{
			byKey: make(map[plumtreePartKey]*plumtreeAnnouncement),
		}
		e.announcements[from] = queue
	}

	announcement := &plumtreeAnnouncement{
		key:              key,
		peer:             from,
		payloadTimestamp: message.PayloadTimestamp,
		dataSize:         message.DataSize,
		announcedAt:      now,
	}
	copy(announcement.source[:], source.Key)
	copy(announcement.signature[:], message.Signature)
	copy(announcement.dataHash[:], message.DataHash)
	announcement.certificate = ownPlumtreeCertificate(certificate)

	queue.push(announcement)
	for queue.count > plumtreeAnnouncementPeerLimit {
		queue.remove(queue.oldest)
	}
}

// The issuer key is copied by the parser, but the signature aliases the inbound
// QUIC frame and would pin it for the whole TTL.
func ownPlumtreeCertificate(certificate plumtreeCertificate) plumtreeCertificate {
	switch certificate.kind {
	case plumtreeCertificateLegacy:
		certificate.legacy.Signature = bytes.Clone(certificate.legacy.Signature)
	case plumtreeCertificateV2:
		certificate.v2.Signature = bytes.Clone(certificate.v2.Signature)
	}
	return certificate
}

// Takes up to plumtreeRepairTargetLimit announcers of a part. Consuming the
// entries is what stops a peer being asked twice for the same part.
func (e *plumtreeEngine) takeCandidatesLocked(
	key plumtreePartKey,
	into []plumtreeRepairCandidate,
) []plumtreeRepairCandidate {
	// Eager-path targets count against the same limit.
	budget := plumtreeRepairTargetLimit
	if missing := e.missing[key]; missing != nil {
		budget -= int(missing.targetCount)
	}
	if budget <= 0 {
		return into
	}

	// Earliest announcer first, not map order: a flood of late garbage must not
	// crowd honest announcers out of the budget.
	matched := e.matchedAnnouncements(key)
	slices.SortFunc(matched, func(a, b *plumtreeAnnouncement) int {
		return a.announcedAt.Compare(b.announcedAt)
	})
	if len(matched) > budget {
		matched = matched[:budget]
	}

	for _, announcement := range matched {
		into = append(into, plumtreeRepairCandidate{
			Key:              announcement.key,
			Peer:             announcement.peer,
			Source:           keys.PublicKeyED25519{Key: announcement.source[:]},
			Certificate:      announcement.certificate,
			Signature:        announcement.signature[:],
			DataHash:         announcement.dataHash[:],
			PayloadTimestamp: announcement.payloadTimestamp,
			DataSize:         announcement.dataSize,
		})
		e.removeAnnouncementLocked(announcement)
	}
	return into
}

func (e *plumtreeEngine) matchedAnnouncements(key plumtreePartKey) []*plumtreeAnnouncement {
	var matched []*plumtreeAnnouncement
	for _, queue := range e.announcements {
		if announcement := queue.byKey[key]; announcement != nil {
			matched = append(matched, announcement)
		}
	}
	return matched
}

func (e *plumtreeEngine) removeAnnouncementLocked(announcement *plumtreeAnnouncement) {
	queue := e.announcements[announcement.peer]
	if queue == nil {
		return
	}
	queue.remove(announcement)
	if queue.count == 0 {
		delete(e.announcements, announcement.peer)
	}
}

func (e *plumtreeEngine) hasAnnouncementLocked(key plumtreePartKey, from PeerID) bool {
	queue := e.announcements[from]
	return queue != nil && queue.byKey[key] != nil
}

// Drains entries past the TTL. Called from Alarm, so there is no separate sweep.
func (e *plumtreeEngine) expireAnnouncementsLocked(now time.Time) {
	cutoff := now.Add(-plumtreeAnnouncementTTL)
	for peer, queue := range e.announcements {
		queue.expire(cutoff)
		if queue.count == 0 {
			delete(e.announcements, peer)
		}
	}
}

// Schedules a revisit of the broadcast once the repair delay expires, measured
// from the announcement so buffering does not postpone the repair.
//
// Simple broadcasts are never armed: their state exists only once the whole
// payload is in hand, so the eager path repairs them immediately instead.
func (e *plumtreeEngine) armFECRepairLocked(key plumtreePartKey, announcedAt time.Time) {
	state := e.fecStates[key.broadcastID]
	if state == nil || state.parts[key.partIndex] != nil {
		return
	}
	e.setFECRepairAtLocked(state, key.partIndex, announcedAt.Add(plumtreeRepairDelay))
}

// Arms a broadcast whose state has just appeared, from the earliest announcement
// of each part it lacks - announcements routinely precede the first payload part.
//
// One pass over the buffers, not one lookup per part index: this runs under e.mu
// for every new broadcast.
func (e *plumtreeEngine) armFECRepairForStateLocked(state *plumtreeFECState) {
	for _, queue := range e.announcements {
		for announcement := queue.oldest; announcement != nil; announcement = announcement.next {
			key := announcement.key
			if key.broadcastID != state.broadcastID ||
				key.treeIndex == plumtreeSimpleTree ||
				state.parts[key.partIndex] != nil {
				continue
			}
			e.setFECRepairAtLocked(
				state,
				key.partIndex,
				announcement.announcedAt.Add(plumtreeRepairDelay),
			)
		}
	}
}

func (e *plumtreeEngine) setFECRepairAtLocked(
	state *plumtreeFECState,
	partIndex int32,
	at time.Time,
) {
	current := state.repairAt[partIndex]
	if current.IsZero() || at.Before(current) {
		state.repairAt[partIndex] = at
		current = at
	}
	if e.fecRepairNext.IsZero() || current.Before(e.fecRepairNext) {
		e.fecRepairNext = current
	}
}

// Collects candidates for every part still missing from a broadcast whose
// deadline passed. Verified state decides what is wanted, announcements only whom
// to ask.
func (e *plumtreeEngine) dueFECRepairsLocked(
	now time.Time,
	into []plumtreeRepairCandidate,
) []plumtreeRepairCandidate {
	e.fecRepairNext = time.Time{}
	for _, state := range e.fecStates {
		usable := !state.isDelivered && !state.hasDecodeFailed
		for partIndex := range state.repairAt {
			at := state.repairAt[partIndex]
			if at.IsZero() {
				continue
			}
			// Leave the deadline so the next turn picks this part up.
			if at.After(now) || len(into) >= plumtreeCandidatesPerAlarm {
				if e.fecRepairNext.IsZero() || at.Before(e.fecRepairNext) {
					e.fecRepairNext = at
				}
				continue
			}

			state.repairAt[partIndex] = time.Time{}
			if !usable || state.parts[partIndex] != nil {
				continue
			}
			into = e.takeCandidatesLocked(plumtreeFECPartKey(
				state.broadcastID,
				int32(partIndex),
			), into)
		}
	}
	return into
}

func plumtreeFECPartKey(broadcastID [sha256.Size]byte, partIndex int32) plumtreePartKey {
	return plumtreePartKey{
		broadcastID: broadcastID,
		partIndex:   partIndex,
		treeIndex:   partIndex + plumtreeFECTreeOffset,
	}
}

// StartVerifiedRepair registers a repair query for a verified candidate, if the
// part is still wanted. Takes the lock itself.
func (e *plumtreeEngine) StartVerifiedRepair(
	now time.Time,
	candidate plumtreeRepairCandidate,
) (plumtreeRepairAction, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed || len(e.repairs) >= plumtreeActiveRepairLimit {
		return plumtreeRepairAction{}, false
	}
	if e.getPartLocked(candidate.Key) != nil {
		return plumtreeRepairAction{}, false
	}
	if !e.hasStateLocked(candidate.Key.broadcastID) {
		return plumtreeRepairAction{}, false
	}

	return e.newRepairLocked(now, candidate.Key, candidate.Peer, candidate.DataSize), true
}

func (e *plumtreeEngine) DropPeerAnnouncements(peer PeerID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.announcements, peer)
}

// CandidateSignedData rebuilds the preimage the announcer signed.
func (c plumtreeRepairCandidate) CandidateSignedData() []byte {
	if c.Key.treeIndex == plumtreeSimpleTree {
		return serializeValidatedPlumtreeSimpleToSign(BroadcastPlumtreeSimpleToSign{
			ID:        c.Key.broadcastID[:],
			Timestamp: c.PayloadTimestamp,
			TreeIndex: c.Key.treeIndex,
			DataSize:  c.DataSize,
			DataHash:  c.DataHash,
		})
	}
	return serializeValidatedPlumtreeFECToSign(BroadcastPlumtreeFECToSign{
		ID:        c.Key.broadcastID[:],
		Timestamp: c.PayloadTimestamp,
		PartIndex: c.Key.partIndex,
		TreeIndex: c.Key.treeIndex,
		DataSize:  c.DataSize,
		DataHash:  c.DataHash,
	})
}

func (q *plumtreePeerAnnouncements) push(announcement *plumtreeAnnouncement) {
	announcement.previous = q.newest
	if q.newest != nil {
		q.newest.next = announcement
	} else {
		q.oldest = announcement
	}
	q.newest = announcement
	q.byKey[announcement.key] = announcement
	q.count++
}

func (q *plumtreePeerAnnouncements) remove(announcement *plumtreeAnnouncement) {
	if announcement.previous != nil {
		announcement.previous.next = announcement.next
	} else {
		q.oldest = announcement.next
	}
	if announcement.next != nil {
		announcement.next.previous = announcement.previous
	} else {
		q.newest = announcement.previous
	}
	delete(q.byKey, announcement.key)
	q.count--
}

func (q *plumtreePeerAnnouncements) expire(cutoff time.Time) {
	for q.oldest != nil && !q.oldest.announcedAt.After(cutoff) {
		q.remove(q.oldest)
	}
}
