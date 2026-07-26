package p2p

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"
)

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

	var broadcastID [sha256.Size]byte
	copy(broadcastID[:], message.BroadcastID)
	key := plumtreePartKey{
		broadcastID: broadcastID,
		partIndex:   message.PartIndex,
		treeIndex:   message.TreeIndex,
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
	e.mu.Unlock()

	source, certificate, err := decodePlumtreeIdentity(message.Source, message.Certificate)
	if err != nil {
		return plumtreeActions{}, err
	}

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
	// Only the two timestamp windows can go stale during verification; the
	// remaining fields were fully validated at entry and are value copies.
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

	missing := e.missing[key]
	if missing == nil {
		missing = &plumtreeMissingPart{
			key:      key,
			dataSize: message.DataSize,
			repairAt: commitNow.Add(plumtreeRepairDelay),
		}
		e.addMissingLocked(missing)
		for len(e.missing) > plumtreeMissingLimit {
			e.eraseMissingLocked(e.missingOldest.key)
		}
	} else if message.DataSize > missing.dataSize {
		missing.dataSize = message.DataSize
	}

	missing.targets[missing.targetCount] = from
	missing.targetCount++

	var actions plumtreeActions
	if e.slots[key.treeIndex].eagerCount == 0 {
		e.startRepairRequestsLocked(commitNow, missing, &actions)
	}
	return actions, nil
}

func (e *plumtreeEngine) ignoreIHaveLocked(
	key plumtreePartKey,
	from PeerID,
) bool {
	if !e.hasStateLocked(key.broadcastID) && e.delivered.contains(key.broadcastID) {
		return true
	}
	if e.getPartLocked(key) != nil {
		return true
	}

	missing := e.missing[key]
	if missing == nil {
		return false
	}
	for i := range int(missing.targetCount) {
		if missing.targets[i] == from {
			return true
		}
	}
	return int(missing.targetCount) >= plumtreeRepairTargetLimit
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

	var broadcastID [sha256.Size]byte
	copy(broadcastID[:], message.BroadcastID)
	key := plumtreePartKey{
		broadcastID: broadcastID,
		partIndex:   message.PartIndex,
		treeIndex:   message.TreeIndex,
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errPlumtreeClosed
	}
	if !e.hasStateLocked(key.broadcastID) && e.delivered.contains(key.broadcastID) {
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

	var broadcastID [sha256.Size]byte
	copy(broadcastID[:], message.BroadcastID)
	key := plumtreePartKey{
		broadcastID: broadcastID,
		partIndex:   message.PartIndex,
		treeIndex:   message.TreeIndex,
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errPlumtreeClosed
	}
	if !e.hasStateLocked(key.broadcastID) && e.delivered.contains(key.broadcastID) {
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

	var broadcastID [sha256.Size]byte
	copy(broadcastID[:], request.BroadcastID)
	key := plumtreePartKey{
		broadcastID: broadcastID,
		partIndex:   request.PartIndex,
		treeIndex:   request.TreeIndex,
	}

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

	for i := range e.slots {
		e.slots[i].expirePending(now)
	}

	var actions plumtreeActions
	for e.missingOldest != nil {
		missing := e.missingOldest
		if missing.repairAt.After(now) {
			break
		}
		e.startRepairRequestsLocked(now, missing, &actions)
		e.eraseMissingLocked(missing.key)
	}

	expiredBefore := now.Add(-plumtreeBroadcastLifetime)
	for e.fecOldest != nil && !e.fecOldest.firstSeen.After(expiredBefore) {
		state := e.fecOldest
		if state.isDelivered && !e.isOriginalSender {
			e.stats.NoteFECPartsCollected(uint32(state.partCount))
		}
		e.removeFECStateLocked(state)
		e.delivered.add(state.broadcastID)
	}
	for e.simpleOldest != nil && !e.simpleOldest.firstSeen.After(expiredBefore) {
		state := e.simpleOldest
		e.removeSimpleStateLocked(state)
		e.delivered.add(state.broadcastID)
	}
	return actions
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
	if e.fecOldest != nil {
		deadline := e.fecOldest.firstSeen.Add(plumtreeBroadcastLifetime)
		if !hasNext || deadline.Before(next) {
			next = deadline
			hasNext = true
		}
	}
	if e.simpleOldest != nil {
		deadline := e.simpleOldest.firstSeen.Add(plumtreeBroadcastLifetime)
		if !hasNext || deadline.Before(next) {
			next = deadline
			hasNext = true
		}
	}
	for i := range e.slots {
		slot := &e.slots[i]
		for j := range int(slot.pendingCount) {
			deadline := slot.pending[j].sent.Add(plumtreePendingFeedbackTTL)
			if !hasNext || deadline.Before(next) {
				next = deadline
				hasNext = true
			}
		}
	}

	return next, hasNext
}

func (e *plumtreeEngine) startRepairRequestsLocked(
	now time.Time,
	missing *plumtreeMissingPart,
	actions *plumtreeActions,
) {
	for missing.sentTargets < missing.targetCount {
		peer := missing.targets[missing.sentTargets]
		missing.sentTargets++
		if len(e.repairs) >= plumtreeActiveRepairLimit {
			continue
		}

		repairID := e.nextRepairIDLocked()
		e.repairs[repairID] = plumtreeRepairAttempt{
			from: peer,
			key:  missing.key,
		}
		actions.Repairs = append(actions.Repairs, plumtreeRepairAction{
			ID: repairID,
			To: peer,
			Request: RepairPlumtreePart{
				BroadcastID: missing.key.broadcastID[:],
				Timestamp:   plumtreeUnixSeconds(now),
				PartIndex:   missing.key.partIndex,
				TreeIndex:   missing.key.treeIndex,
			},
			Timeout:       plumtreeRepairTimeout,
			MaxAnswerSize: uint64(missing.dataSize) + plumtreeRepairMTUOverhead,
		})
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
	// Zero the accounting so a decode finishing concurrently with this eviction
	// cannot release the same bytes twice; budget.release panics on underflow.
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
