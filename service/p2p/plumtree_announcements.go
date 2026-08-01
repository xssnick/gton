package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
)

// Buffered IHAVEs for parts we are still waiting on.
//
// Only the first IHAVE of a broadcast is verified on arrival, and only while no
// payload part of it has been verified yet: one signature proves the broadcast
// exists and that its source is authorised, and every part of a real FEC
// broadcast exists by construction. The remaining IHAVEs are taken on trust and
// checked at repair time, if we end up asking that peer for the part - measured,
// 99.98% of IHAVE verifications were never used. Simple broadcasts always verify
// on arrival: there is one part, so there is no "rest" to defer.
//
// Every announcement belongs to a live e.missing entry and dies with it, so the
// buffers are bounded by plumtreeMissingLimit x plumtreeRepairTargetLimit and
// need no sweep of their own. A peer that announces garbage occupies one of the
// five slots of a part until the repair verifies it, fails, and bans it.

// One ed25519 verify per candidate, on the run-loop goroutine that also
// dispatches sends, so a single Alarm hands out no more than this.
const plumtreeCandidatesPerAlarm = 32

// Announcements per peer. Only a bound on how much one peer may hold across all
// parts; the per-part bound that decides who gets asked is announcerCount.
const plumtreeAnnouncementPeerLimit = 8 * plumtreeRepairTargetLimit

// One buffered IHAVE. Fixed-size arrays, not the wire slices: IHAVE is parsed
// with ParseNoCopy, so a retained slice would pin the QUIC frame.
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

// One peer's queue, oldest first: map for lookup by part, list for eviction from
// the front. It exists so banning a peer drops its announcements without walking
// every part we are waiting on.
type plumtreePeerAnnouncements struct {
	byKey  map[plumtreePartKey]*plumtreeAnnouncement
	oldest *plumtreeAnnouncement
	newest *plumtreeAnnouncement
	count  int
}

// A broadcast whose existence one verified signature has already established.
func (e *plumtreeEngine) provenLocked(broadcastID [sha256.Size]byte) bool {
	return e.proven.contains(broadcastID) || e.fecStates[broadcastID] != nil
}

// Records an IHAVE and arms the repair timer for its part. The caller has
// already checked, under this lock, that the part is still wanted and that this
// peer has not announced it.
func (e *plumtreeEngine) recordAnnouncementLocked(
	key plumtreePartKey,
	from PeerID,
	source keys.PublicKeyED25519,
	certificate plumtreeCertificate,
	message BroadcastPlumtreeIHave,
	now time.Time,
) {
	missing := e.missing[key]
	if missing == nil {
		// Refuse the newest rather than evict the oldest: the list is ordered by
		// deadline, so the oldest entry is the one about to fire.
		if len(e.missing) >= plumtreeMissingLimit {
			return
		}
		missing = &plumtreeMissingPart{
			key:      key,
			repairAt: now.Add(plumtreeRepairDelay),
		}
		e.addMissingLocked(missing)
	} else if int(missing.announcerCount) >= plumtreeRepairTargetLimit {
		return
	}

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
	missing.announcers[missing.announcerCount] = announcement
	missing.announcerCount++

	for queue.count > plumtreeAnnouncementPeerLimit {
		e.dropAnnouncementLocked(queue.oldest)
	}
}

// The issuer key is copied by the parser, but the signature aliases the inbound
// QUIC frame and would pin it for as long as the announcement lives.
func ownPlumtreeCertificate(certificate plumtreeCertificate) plumtreeCertificate {
	switch certificate.kind {
	case plumtreeCertificateLegacy:
		certificate.legacy.Signature = bytes.Clone(certificate.legacy.Signature)
	case plumtreeCertificateV2:
		certificate.v2.Signature = bytes.Clone(certificate.v2.Signature)
	}
	return certificate
}

// Drops an announcement from both the peer queue and its part, taking the part
// with it once nothing is left to ask.
func (e *plumtreeEngine) dropAnnouncementLocked(announcement *plumtreeAnnouncement) {
	e.detachAnnouncementLocked(announcement)

	missing := e.missing[announcement.key]
	if missing == nil {
		return
	}
	for i := range int(missing.announcerCount) {
		if missing.announcers[i] != announcement {
			continue
		}
		last := int(missing.announcerCount) - 1
		missing.announcers[i] = missing.announcers[last]
		missing.announcers[last] = nil
		missing.announcerCount--
		break
	}
	if missing.announcerCount == 0 {
		e.eraseMissingLocked(missing.key)
	}
}

// Removes an announcement from its peer queue only. Callers that are discarding
// the part itself use this, so erasing a part cannot recurse back into it.
func (e *plumtreeEngine) detachAnnouncementLocked(announcement *plumtreeAnnouncement) {
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

// Hands out every announcer of a part whose deadline has passed. The caller
// erases the part right after, which releases the announcements.
func (e *plumtreeEngine) takeCandidatesLocked(
	missing *plumtreeMissingPart,
	into []plumtreeRepairCandidate,
) []plumtreeRepairCandidate {
	if state := e.fecStates[missing.key.broadcastID]; state != nil &&
		(state.isDelivered || state.hasDecodeFailed) {
		return into
	}

	for i := range int(missing.announcerCount) {
		announcement := missing.announcers[i]
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
	}
	return into
}

// StartVerifiedRepair registers a repair query for a verified candidate, if the
// part is still wanted. Takes the lock itself.
//
// DataSize comes from the announcement the caller has just verified, so the RLDP
// answer budget is attested by the source rather than claimed by the announcer.
func (e *plumtreeEngine) StartVerifiedRepair(
	now time.Time,
	candidate plumtreeRepairCandidate,
) (plumtreeRepairAction, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed || len(e.repairs) >= plumtreeActiveRepairLimit {
		return plumtreeRepairAction{}, false
	}
	if e.isSettledBroadcastLocked(candidate.Key.broadcastID) {
		return plumtreeRepairAction{}, false
	}
	if e.getPartLocked(candidate.Key) != nil {
		return plumtreeRepairAction{}, false
	}

	return e.newRepairLocked(now, candidate.Key, candidate.Peer, candidate.DataSize), true
}

func (e *plumtreeEngine) DropPeerAnnouncements(peer PeerID) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.dropPeerAnnouncementsLocked(peer)
}

func (e *plumtreeEngine) dropPeerAnnouncementsLocked(peer PeerID) {
	queue := e.announcements[peer]
	if queue == nil {
		return
	}
	for queue.oldest != nil {
		e.dropAnnouncementLocked(queue.oldest)
	}
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
	announcement.previous = nil
	announcement.next = nil
	delete(q.byKey, announcement.key)
	q.count--
}
