package p2p

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
)

// The fields are already range-checked by the message validators.
func plumtreeControlKey(broadcastID []byte, partIndex, treeIndex int32) plumtreePartKey {
	var key plumtreePartKey
	copy(key.broadcastID[:], broadcastID)
	key.partIndex = partIndex
	key.treeIndex = treeIndex
	return key
}

func (e *plumtreeEngine) HandleIHave(
	ctx context.Context,
	now time.Time,
	from PeerID,
	message BroadcastPlumtreeIHave,
) (plumtreeActions, error) {
	if e.isOriginalSender {
		return plumtreeActions{}, nil
	}
	if from.IsZero() {
		return plumtreeActions{}, fmt.Errorf("missing Plumtree immediate sender")
	}
	if err := validateBroadcastPlumtreeIHave(message, now); err != nil {
		return plumtreeActions{}, err
	}

	key := plumtreeControlKey(message.BroadcastID, message.PartIndex, message.TreeIndex)

	source, certificate, err := decodePlumtreeIdentity(message.Source, message.Certificate)
	if err != nil {
		return plumtreeActions{}, err
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return plumtreeActions{}, errPlumtreeClosed
	}
	if e.ignoreIHaveLocked(key, from) {
		e.mu.Unlock()
		return plumtreeActions{}, nil
	}
	// A proven broadcast is one whose existence a signature has already
	// established, so the rest of its FEC announcements are taken on trust and
	// checked only if we end up asking that peer for the part. Simple broadcasts
	// have a single part and so never have a "rest" to defer.
	if key.treeIndex != plumtreeSimpleTree && e.provenLocked(key.broadcastID) {
		actions := e.recordIHaveLocked(
			key,
			from,
			source,
			certificate,
			message,
			now,
		)
		e.mu.Unlock()
		return actions, nil
	}
	e.mu.Unlock()

	var signedData []byte
	if message.TreeIndex == plumtreeSimpleTree {
		signedData = serializeValidatedPlumtreeSimpleToSign(
			BroadcastPlumtreeSimpleToSign{
				ID:        message.BroadcastID,
				Timestamp: message.PayloadTimestamp,
				TreeIndex: message.TreeIndex,
				DataSize:  message.DataSize,
				DataHash:  message.DataHash,
			},
		)
	} else {
		signedData = serializeValidatedPlumtreeFECToSign(
			BroadcastPlumtreeFECToSign{
				ID:        message.BroadcastID,
				Timestamp: message.PayloadTimestamp,
				PartIndex: message.PartIndex,
				TreeIndex: message.TreeIndex,
				DataSize:  message.DataSize,
				DataHash:  message.DataHash,
			},
		)
	}

	plumtreeIHaveVerifiableTotal.Add(1)
	_, err = e.verifier.VerifyPlumtree(ctx, plumtreeVerification{
		From:        from,
		Source:      source,
		Certificate: certificate,
		DataSize:    uint32(message.DataSize),
		SignedData:  signedData,
		Signature:   message.Signature,
	})
	if err != nil {
		return plumtreeActions{}, err
	}
	// Only the timestamp windows can go stale during verification.
	commitNow := e.now()
	if err = validatePlumtreeTimestamp(message.Timestamp, commitNow); err != nil {
		return plumtreeActions{}, err
	}
	if err = validatePlumtreeTimestamp(message.PayloadTimestamp, commitNow); err != nil {
		return plumtreeActions{}, fmt.Errorf("invalid payload timestamp: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return plumtreeActions{}, errPlumtreeClosed
	}
	if e.ignoreIHaveLocked(key, from) {
		return plumtreeActions{}, nil
	}

	if key.treeIndex != plumtreeSimpleTree {
		e.proven.add(key.broadcastID)
	}
	return e.recordIHaveLocked(
		key,
		from,
		source,
		certificate,
		message,
		commitNow,
	), nil
}

func (e *plumtreeEngine) recordIHaveLocked(
	key plumtreePartKey,
	from PeerID,
	source keys.PublicKeyED25519,
	certificate plumtreeCertificate,
	message BroadcastPlumtreeIHave,
	now time.Time,
) plumtreeActions {
	e.recordAnnouncementLocked(
		key,
		from,
		source,
		certificate,
		message,
		now,
	)

	missing := e.missing[key]
	if missing == nil || e.slots[key.treeIndex].eagerCount != 0 {
		return plumtreeActions{}
	}

	// With no eager peer there is no payload path worth waiting for. Hand the
	// announcers to the same verification boundary as delayed repairs; a freshly
	// verified first IHAVE hits the verifier tuple cache there.
	actions := plumtreeActions{
		Candidates: e.takeCandidatesLocked(missing, nil),
	}
	e.eraseMissingLocked(key)
	return actions
}

func (e *plumtreeEngine) ignoreIHaveLocked(
	key plumtreePartKey,
	from PeerID,
) bool {
	if e.isSettledBroadcastLocked(key.broadcastID) {
		return true
	}
	if e.getPartLocked(key) != nil {
		return true
	}

	if e.hasAnnouncementLocked(key, from) {
		return true
	}

	missing := e.missing[key]
	return missing != nil && int(missing.announcerCount) >= plumtreeRepairTargetLimit
}

func (e *plumtreeEngine) HandlePrune(
	now time.Time,
	from PeerID,
	message BroadcastPlumtreePrune,
) error {
	if from.IsZero() {
		return fmt.Errorf("missing Plumtree immediate sender")
	}
	if err := validateBroadcastPlumtreePrune(message, now); err != nil {
		return err
	}

	key := plumtreeControlKey(message.BroadcastID, message.PartIndex, message.TreeIndex)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errPlumtreeClosed
	}
	if e.isSettledBroadcastLocked(key.broadcastID) {
		return nil
	}
	part := e.getPartLocked(key)
	if part == nil || !part.wasSentTo(from) {
		return nil
	}

	e.noteActiveLocked(from, now)
	e.removeEagerLocked(&e.slots[key.treeIndex], from)
	return nil
}

func (e *plumtreeEngine) HandleUseful(
	now time.Time,
	from PeerID,
	message BroadcastPlumtreeUseful,
) error {
	if from.IsZero() {
		return fmt.Errorf("missing Plumtree immediate sender")
	}
	if err := validateBroadcastPlumtreeUseful(message, now); err != nil {
		return err
	}

	key := plumtreeControlKey(message.BroadcastID, message.PartIndex, message.TreeIndex)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errPlumtreeClosed
	}
	if e.isSettledBroadcastLocked(key.broadcastID) {
		return nil
	}
	part := e.getPartLocked(key)
	if part == nil || !part.wasSentTo(from) {
		return nil
	}

	slot := &e.slots[key.treeIndex]
	e.noteActiveLocked(from, now)
	if slot.removePending(from) {
		e.promoteEagerLocked(slot, from, false, now)
	}
	return nil
}

func (e *plumtreeEngine) HandleRepairQuery(
	now time.Time,
	from PeerID,
	request RepairPlumtreePart,
) ([]byte, error) {
	if from.IsZero() {
		return nil, fmt.Errorf("missing Plumtree repair requester")
	}
	if err := validateRepairPlumtreePart(request, now); err != nil {
		return nil, err
	}

	key := plumtreeControlKey(request.BroadcastID, request.PartIndex, request.TreeIndex)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, errPlumtreeClosed
	}
	if !e.hasStateLocked(key.broadcastID) {
		if e.delivered.contains(key.broadcastID) {
			return e.notFoundBody, nil
		}
		return nil, fmt.Errorf("unknown Plumtree repair broadcast")
	}

	part := e.getPartLocked(key)
	if part == nil {
		return nil, fmt.Errorf("Plumtree repair for unknown part")
	}
	if !part.wasAdvertisedTo(from) {
		return nil, fmt.Errorf("Plumtree repair without IHAVE")
	}
	if part.wasSentTo(from) {
		return nil, fmt.Errorf("duplicate Plumtree repair")
	}

	slot := &e.slots[key.treeIndex]
	if !e.canReserveFeedbackLocked(slot, from, now) ||
		int(part.fullSentCount) >= e.localEagerLimit {
		return e.notFoundBody, nil
	}
	if !e.peers.PlumtreePeerReceivesBroadcasts(from) {
		return nil, fmt.Errorf("peer does not receive Plumtree broadcasts")
	}

	e.registerFullSendLocked(slot, part, from, now)
	return part.wire.Body, nil
}

func (e *plumtreeEngine) Alarm(now time.Time) plumtreeActions {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return plumtreeActions{}
	}

	// Scanned here rather than in NextAlarm, which runs after every message.
	e.pendingExpireNext = time.Time{}
	for i := range e.slots {
		slot := &e.slots[i]
		slot.expirePending(now)
		for j := range int(slot.pendingCount) {
			e.notePendingDeadlineLocked(
				slot.pending[j].sent.Add(plumtreePendingFeedbackTTL),
			)
		}
	}
	// Sweeping here, not while forwarding a payload, is what makes the inactivity
	// TTL apply to a neighbour that stopped sending - the case that matters.
	e.expireInactiveEagerLocked(now)

	var actions plumtreeActions
	// Before draining repairs: expiry moves ids into e.delivered, and a repair for
	// a part erased two statements later would be dead on arrival.
	e.expireBroadcastsLocked(now)

	// Leftovers keep their deadline, which is already in the past, so NextAlarm
	// brings us straight back for them.
	for e.missingOldest != nil && len(actions.Candidates) < plumtreeCandidatesPerAlarm {
		missing := e.missingOldest
		if missing.repairAt.After(now) {
			break
		}
		actions.Candidates = e.takeCandidatesLocked(missing, actions.Candidates)
		e.eraseMissingLocked(missing.key)
	}

	return actions
}

// An assembled broadcast expires sooner than one still collecting parts, so the
// firstSeen-ordered list is not deadline-ordered and has to be walked whole. It is
// bounded by the state budget.
func (e *plumtreeEngine) expireBroadcastsLocked(now time.Time) {
	e.stateExpireNext = time.Time{}

	for state := e.fecOldest; state != nil; {
		next := state.next
		if deadline := plumtreeStateDeadline(state.firstSeen, state.isDelivered); !now.Before(deadline) {
			if state.isDelivered && !e.isOriginalSender {
				e.stats.NoteFECPartsCollected(uint32(state.partCount))
			}
			e.removeFECStateLocked(state)
			e.delivered.add(state.broadcastID)
		} else {
			e.noteStateDeadlineLocked(deadline)
		}
		state = next
	}

	for state := e.simpleOldest; state != nil; {
		next := state.next
		if deadline := plumtreeStateDeadline(state.firstSeen, true); !now.Before(deadline) {
			e.removeSimpleStateLocked(state)
			e.delivered.add(state.broadcastID)
		} else {
			e.noteStateDeadlineLocked(deadline)
		}
		state = next
	}
}

func plumtreeStateDeadline(firstSeen time.Time, delivered bool) time.Time {
	if delivered {
		return firstSeen.Add(plumtreeDeliveredStateTTL)
	}
	return firstSeen.Add(plumtreePendingStateTTL)
}

func (e *plumtreeEngine) noteStateDeadlineLocked(deadline time.Time) {
	if e.stateExpireNext.IsZero() || deadline.Before(e.stateExpireNext) {
		e.stateExpireNext = deadline
	}
}

func (e *plumtreeEngine) NextAlarm() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return time.Time{}, false
	}

	var next time.Time
	hasNext := false

	if e.missingOldest != nil {
		next = e.missingOldest.repairAt
		hasNext = true
	}
	if !e.stateExpireNext.IsZero() && (!hasNext || e.stateExpireNext.Before(next)) {
		next = e.stateExpireNext
		hasNext = true
	}
	if !e.pendingExpireNext.IsZero() && (!hasNext || e.pendingExpireNext.Before(next)) {
		next = e.pendingExpireNext
		hasNext = true
	}

	return next, hasNext
}

// Keeps pendingExpireNext a lower bound on the earliest pending deadline: a stale
// value costs one extra wake-up, never a missed deadline.
func (e *plumtreeEngine) notePendingDeadlineLocked(deadline time.Time) {
	if e.pendingExpireNext.IsZero() || deadline.Before(e.pendingExpireNext) {
		e.pendingExpireNext = deadline
	}
}

// The size comes from the validated announcement and becomes the RLDP answer
// budget.
func (e *plumtreeEngine) newRepairLocked(
	now time.Time,
	key plumtreePartKey,
	peer PeerID,
	dataSize int32,
) plumtreeRepairAction {
	repairID := e.nextRepairIDLocked()
	plumtreeRepairRequestsTotal.Add(1)
	e.repairs[repairID] = plumtreeRepairAttempt{from: peer, key: key}
	return plumtreeRepairAction{
		ID: repairID,
		To: peer,
		Request: RepairPlumtreePart{
			BroadcastID: key.broadcastID[:],
			Timestamp:   plumtreeUnixSeconds(now),
			PartIndex:   key.partIndex,
			TreeIndex:   key.treeIndex,
		},
		Timeout:       plumtreeRepairTimeout,
		MaxAnswerSize: uint64(dataSize) + plumtreeRepairMTUOverhead,
	}
}

func (e *plumtreeEngine) nextRepairIDLocked() uint64 {
	e.nextRepairID++
	return e.nextRepairID
}

func (e *plumtreeEngine) FinishRepair(repairID uint64) error {
	_, err := e.takeRepairAttempt(repairID)
	return err
}

func (e *plumtreeEngine) takeRepairAttempt(
	repairID uint64,
) (plumtreeRepairAttempt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return plumtreeRepairAttempt{}, errPlumtreeClosed
	}

	attempt, ok := e.repairs[repairID]
	if !ok {
		return plumtreeRepairAttempt{}, fmt.Errorf(
			"unknown Plumtree repair attempt %d",
			repairID,
		)
	}
	delete(e.repairs, repairID)
	return attempt, nil
}

// No state of our own and already delivered: control messages about it are noise
// from peers that have not caught up yet.
func (e *plumtreeEngine) isSettledBroadcastLocked(id [sha256.Size]byte) bool {
	return !e.hasStateLocked(id) && e.delivered.contains(id)
}

func (e *plumtreeEngine) hasStateLocked(id [sha256.Size]byte) bool {
	_, hasFEC := e.fecStates[id]
	_, hasSimple := e.simpleStates[id]
	return hasFEC || hasSimple
}

func (e *plumtreeEngine) getPartLocked(
	key plumtreePartKey,
) *plumtreePartState {
	if key.treeIndex == plumtreeSimpleTree {
		state := e.simpleStates[key.broadcastID]
		if state == nil || key.partIndex != 0 {
			return nil
		}
		return &state.part
	}

	state := e.fecStates[key.broadcastID]
	if state == nil {
		return nil
	}
	return state.parts[key.partIndex]
}

func (e *plumtreeEngine) addMissingLocked(missing *plumtreeMissingPart) {
	missing.previous = e.missingNewest
	if e.missingNewest != nil {
		e.missingNewest.next = missing
	} else {
		e.missingOldest = missing
	}
	e.missingNewest = missing
	e.missing[missing.key] = missing
}

func (e *plumtreeEngine) eraseMissingLocked(key plumtreePartKey) {
	missing := e.missing[key]
	if missing == nil {
		return
	}

	if missing.previous != nil {
		missing.previous.next = missing.next
	} else {
		e.missingOldest = missing.next
	}
	if missing.next != nil {
		missing.next.previous = missing.previous
	} else {
		e.missingNewest = missing.previous
	}
	delete(e.missing, key)

	for i := range int(missing.announcerCount) {
		e.detachAnnouncementLocked(missing.announcers[i])
	}
}

func (e *plumtreeEngine) removeFECStateLocked(state *plumtreeFECState) {
	if state.previous != nil {
		state.previous.next = state.next
	} else {
		e.fecOldest = state.next
	}
	if state.next != nil {
		state.next.previous = state.previous
	} else {
		e.fecNewest = state.previous
	}
	delete(e.fecStates, state.broadcastID)
	e.budget.release(
		1,
		state.retainedWireBytes+state.decoderEstimatedBytes,
	)
	// So a decode finishing concurrently cannot release the same bytes twice.
	state.retainedWireBytes = 0
	state.decoderEstimatedBytes = 0
}

func (e *plumtreeEngine) removeSimpleStateLocked(state *plumtreeSimpleState) {
	if state.previous != nil {
		state.previous.next = state.next
	} else {
		e.simpleOldest = state.next
	}
	if state.next != nil {
		state.next.previous = state.previous
	} else {
		e.simpleNewest = state.previous
	}
	delete(e.simpleStates, state.broadcastID)
	e.budget.release(1, state.retainedWireBytes)
}
