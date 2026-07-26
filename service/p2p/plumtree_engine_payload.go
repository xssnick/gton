package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/xssnick/raptorq"
	"github.com/xssnick/tonutils-go/adnl/keys"
)

const (
	plumtreeFECIDConstructorID        = 0x06a25034
	plumtreeSimpleToSignConstructorID = 0x38f3338e
	plumtreeFECToSignConstructorID    = 0xc3e1c094

	plumtreeFECIDWireSize        = 48
	plumtreeSimpleToSignWireSize = 84
	plumtreeFECToSignWireSize    = 88
)

type plumtreePreparedSimple struct {
	message     BroadcastPlumtreeSimple
	wire        plumtreePayloadWire
	key         plumtreePartKey
	source      keys.PublicKeyED25519
	certificate plumtreeCertificate
	dataHash    [sha256.Size]byte
	signedData  []byte
}

type plumtreePreparedFEC struct {
	message      BroadcastPlumtreeFEC
	wire         plumtreePayloadWire
	key          plumtreePartKey
	source       keys.PublicKeyED25519
	certificate  plumtreeCertificate
	fullDataHash [sha256.Size]byte
	dataHash     [sha256.Size]byte
	signedData   []byte
}

func (e *plumtreeEngine) HandleSimple(
	ctx context.Context,
	now time.Time,
	from PeerID,
	envelope plumtreeSimpleEnvelope,
) (plumtreeActions, error) {
	payloadContext := plumtreePayloadContext{
		from:   from,
		origin: plumtreePayloadOriginPush,
	}
	return e.handleSimple(ctx, now, payloadContext, envelope)
}

func (e *plumtreeEngine) HandleFEC(
	ctx context.Context,
	now time.Time,
	from PeerID,
	envelope plumtreeFECEnvelope,
) (plumtreeActions, error) {
	payloadContext := plumtreePayloadContext{
		from:   from,
		origin: plumtreePayloadOriginPush,
	}
	return e.handleFEC(ctx, now, payloadContext, envelope)
}

func (e *plumtreeEngine) HandleRepairSimple(
	ctx context.Context,
	now time.Time,
	repairID uint64,
	envelope plumtreeSimpleEnvelope,
) (plumtreeActions, error) {
	attempt, err := e.takeRepairAttempt(repairID)
	if err != nil {
		return plumtreeActions{}, err
	}

	payloadContext := plumtreePayloadContext{
		from:        attempt.from,
		origin:      plumtreePayloadOriginRepair,
		expectedKey: attempt.key,
	}
	return e.handleSimple(ctx, now, payloadContext, envelope)
}

func (e *plumtreeEngine) HandleRepairFEC(
	ctx context.Context,
	now time.Time,
	repairID uint64,
	envelope plumtreeFECEnvelope,
) (plumtreeActions, error) {
	attempt, err := e.takeRepairAttempt(repairID)
	if err != nil {
		return plumtreeActions{}, err
	}

	payloadContext := plumtreePayloadContext{
		from:        attempt.from,
		origin:      plumtreePayloadOriginRepair,
		expectedKey: attempt.key,
	}
	return e.handleFEC(ctx, now, payloadContext, envelope)
}

func preparePlumtreeSimple(
	now time.Time,
	envelope plumtreeSimpleEnvelope,
) (plumtreePreparedSimple, error) {
	message := envelope.Message
	if err := validateBroadcastPlumtreeSimple(message, now); err != nil {
		return plumtreePreparedSimple{}, err
	}

	var broadcastID [sha256.Size]byte
	copy(broadcastID[:], message.BroadcastID)

	return plumtreePreparedSimple{
		message: message,
		wire:    envelope.Wire,
		key: plumtreePartKey{
			broadcastID: broadcastID,
			partIndex:   0,
			treeIndex:   message.TreeIndex,
		},
	}, nil
}

func preparePlumtreeFEC(
	now time.Time,
	envelope plumtreeFECEnvelope,
) (plumtreePreparedFEC, error) {
	message := envelope.Message
	if err := validateBroadcastPlumtreeFEC(message, now); err != nil {
		return plumtreePreparedFEC{}, err
	}

	partSize := int32(len(message.Data))
	broadcastID := computeValidatedPlumtreeFECID(message, partSize)
	if broadcastID == ([sha256.Size]byte{}) {
		return plumtreePreparedFEC{}, fmt.Errorf("empty Plumtree FEC broadcast id")
	}

	var fullDataHash [sha256.Size]byte
	copy(fullDataHash[:], message.FullDataHash)

	return plumtreePreparedFEC{
		message: message,
		wire:    envelope.Wire,
		key: plumtreePartKey{
			broadcastID: broadcastID,
			partIndex:   message.PartIndex,
			treeIndex:   message.TreeIndex,
		},
		fullDataHash: fullDataHash,
	}, nil
}

func completePlumtreeSimpleVerification(
	prepared *plumtreePreparedSimple,
) error {
	source, certificate, err := decodePlumtreeIdentity(
		prepared.message.Source,
		prepared.message.Certificate,
	)
	if err != nil {
		return err
	}

	dataHash := sha256.Sum256(prepared.message.Data)
	signedData := serializeValidatedPlumtreeSimpleToSign(
		BroadcastPlumtreeSimpleToSign{
			ID:        prepared.message.BroadcastID,
			Timestamp: prepared.message.Timestamp,
			TreeIndex: prepared.message.TreeIndex,
			DataSize:  int32(len(prepared.message.Data)),
			DataHash:  dataHash[:],
		},
	)

	prepared.source = source
	prepared.certificate = certificate
	prepared.dataHash = dataHash
	prepared.signedData = signedData
	return nil
}

func completePlumtreeFECVerification(
	prepared *plumtreePreparedFEC,
) error {
	source, certificate, err := decodePlumtreeIdentity(
		prepared.message.Source,
		prepared.message.Certificate,
	)
	if err != nil {
		return err
	}

	dataHash := sha256.Sum256(prepared.message.Data)
	signedData := serializeValidatedPlumtreeFECToSign(
		BroadcastPlumtreeFECToSign{
			ID:        prepared.key.broadcastID[:],
			Timestamp: prepared.message.Timestamp,
			PartIndex: prepared.message.PartIndex,
			TreeIndex: prepared.message.TreeIndex,
			DataSize:  int32(len(prepared.message.Data)),
			DataHash:  dataHash[:],
		},
	)

	prepared.source = source
	prepared.certificate = certificate
	prepared.dataHash = dataHash
	prepared.signedData = signedData
	return nil
}

func computeValidatedPlumtreeFECID(
	message BroadcastPlumtreeFEC,
	partSize int32,
) [sha256.Size]byte {
	var scratch [plumtreeFECIDWireSize]byte
	binary.LittleEndian.PutUint32(scratch[0:4], plumtreeFECIDConstructorID)
	binary.LittleEndian.PutUint32(scratch[4:8], uint32(message.Flags))
	copy(scratch[8:40], message.FullDataHash)
	binary.LittleEndian.PutUint32(scratch[40:44], uint32(message.FullDataSize))
	binary.LittleEndian.PutUint32(scratch[44:48], uint32(partSize))
	return sha256.Sum256(scratch[:])
}

func serializeValidatedPlumtreeSimpleToSign(
	value BroadcastPlumtreeSimpleToSign,
) []byte {
	boxed := make([]byte, plumtreeSimpleToSignWireSize)
	binary.LittleEndian.PutUint32(boxed[0:4], plumtreeSimpleToSignConstructorID)
	copy(boxed[4:36], value.ID)
	binary.LittleEndian.PutUint64(boxed[36:44], math.Float64bits(value.Timestamp))
	binary.LittleEndian.PutUint32(boxed[44:48], uint32(value.TreeIndex))
	binary.LittleEndian.PutUint32(boxed[48:52], uint32(value.DataSize))
	copy(boxed[52:84], value.DataHash)
	return boxed
}

func serializeValidatedPlumtreeFECToSign(
	value BroadcastPlumtreeFECToSign,
) []byte {
	boxed := make([]byte, plumtreeFECToSignWireSize)
	binary.LittleEndian.PutUint32(boxed[0:4], plumtreeFECToSignConstructorID)
	copy(boxed[4:36], value.ID)
	binary.LittleEndian.PutUint64(boxed[36:44], math.Float64bits(value.Timestamp))
	binary.LittleEndian.PutUint32(boxed[44:48], uint32(value.PartIndex))
	binary.LittleEndian.PutUint32(boxed[48:52], uint32(value.TreeIndex))
	binary.LittleEndian.PutUint32(boxed[52:56], uint32(value.DataSize))
	copy(boxed[56:88], value.DataHash)
	return boxed
}

func (e *plumtreeEngine) handleSimple(
	ctx context.Context,
	now time.Time,
	payloadContext plumtreePayloadContext,
	envelope plumtreeSimpleEnvelope,
) (plumtreeActions, error) {
	if payloadContext.from.IsZero() {
		return plumtreeActions{}, fmt.Errorf("missing Plumtree simple immediate sender")
	}

	prepared, err := preparePlumtreeSimple(now, envelope)
	if err != nil {
		return plumtreeActions{}, err
	}

	e.mu.Lock()
	actions, err := e.precheckSimpleLocked(now, payloadContext, prepared.key)
	e.mu.Unlock()
	if err != nil {
		return actions, err
	}

	if err = completePlumtreeSimpleVerification(&prepared); err != nil {
		return plumtreeActions{}, err
	}

	source, err := e.verifier.VerifyPlumtree(ctx, plumtreeVerification{
		From:        payloadContext.from,
		Source:      prepared.source,
		Certificate: prepared.certificate,
		DataSize:    uint32(len(prepared.message.Data)),
		SignedData:  prepared.signedData,
		Signature:   prepared.message.Signature,
	})
	if err != nil {
		return plumtreeActions{}, err
	}
	// Every field but the timestamp window was fully validated at entry and is
	// an immutable value copy, so only the window can go stale during
	// verification.
	commitNow := e.now()
	if err = validatePlumtreeTimestamp(prepared.message.Timestamp, commitNow); err != nil {
		return plumtreeActions{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	actions, err = e.recheckSimpleLocked(
		commitNow,
		payloadContext.from,
		prepared.key,
	)
	if err != nil {
		return actions, err
	}

	wireBytes := uint64(len(prepared.wire.Message))
	if !e.budget.reserve(1, wireBytes) {
		return plumtreeActions{}, errPlumtreeMemoryCapacity
	}

	part := plumtreePartState{
		partIndex:   prepared.key.partIndex,
		treeIndex:   prepared.key.treeIndex,
		timestamp:   prepared.message.Timestamp,
		dataSize:    int32(len(prepared.message.Data)),
		dataHash:    prepared.dataHash,
		sourceKey:   prepared.source,
		certificate: prepared.certificate,
		signature:   prepared.message.Signature,
		wire:        prepared.wire,
	}
	state := &plumtreeSimpleState{
		broadcastID:       prepared.key.broadcastID,
		source:            source,
		data:              prepared.message.Data,
		firstSeen:         commitNow,
		part:              part,
		retainedWireBytes: wireBytes,
	}
	e.addSimpleStateLocked(state)
	e.eraseMissingLocked(prepared.key)
	e.noteActiveLocked(payloadContext.from, commitNow)

	slot := &e.slots[prepared.key.treeIndex]
	e.promoteEagerLocked(slot, payloadContext.from, true, commitNow)
	e.forwardPayloadLocked(
		commitNow,
		payloadContext.from,
		state.broadcastID,
		&state.part,
		&actions,
	)

	e.stats.NoteDeliveredBroadcast()
	e.stats.NoteUsefulDelivery(time.Now(), state.part.timestamp)
	e.delivered.add(state.broadcastID)
	actions.Deliveries = append(actions.Deliveries, plumtreeDelivery{
		Source: state.source,
		Data:   state.data,
	})
	return actions, nil
}

func (e *plumtreeEngine) handleFEC(
	ctx context.Context,
	now time.Time,
	payloadContext plumtreePayloadContext,
	envelope plumtreeFECEnvelope,
) (plumtreeActions, error) {
	if payloadContext.from.IsZero() {
		return plumtreeActions{}, fmt.Errorf("missing Plumtree immediate sender")
	}

	prepared, err := preparePlumtreeFEC(now, envelope)
	if err != nil {
		return plumtreeActions{}, err
	}

	e.mu.Lock()
	actions, err := e.precheckFECLocked(now, payloadContext, prepared)
	e.mu.Unlock()
	if err != nil {
		return actions, err
	}

	if err = completePlumtreeFECVerification(&prepared); err != nil {
		return plumtreeActions{}, err
	}

	source, err := e.verifier.VerifyPlumtree(ctx, plumtreeVerification{
		From:        payloadContext.from,
		Source:      prepared.source,
		Certificate: prepared.certificate,
		DataSize:    uint32(prepared.message.FullDataSize),
		SignedData:  prepared.signedData,
		Signature:   prepared.message.Signature,
	})
	if err != nil {
		return plumtreeActions{}, err
	}

	commitNow := e.now()
	if err = validatePlumtreeTimestamp(prepared.message.Timestamp, commitNow); err != nil {
		return plumtreeActions{}, err
	}

	e.mu.Lock()

	state, actions, err := e.recheckFECLocked(
		commitNow,
		payloadContext.from,
		prepared,
		source,
	)
	if err != nil {
		e.mu.Unlock()
		return actions, err
	}

	part := &plumtreePartState{
		partIndex:   prepared.key.partIndex,
		treeIndex:   prepared.key.treeIndex,
		timestamp:   prepared.message.Timestamp,
		dataSize:    int32(len(prepared.message.Data)),
		dataHash:    prepared.dataHash,
		sourceKey:   prepared.source,
		certificate: prepared.certificate,
		signature:   prepared.message.Signature,
		wire:        prepared.wire,
	}
	state.parts[prepared.key.partIndex] = part
	state.partCount++
	e.eraseMissingLocked(prepared.key)
	e.noteActiveLocked(payloadContext.from, commitNow)

	slot := &e.slots[prepared.key.treeIndex]
	e.promoteEagerLocked(slot, payloadContext.from, true, commitNow)
	e.forwardPayloadLocked(
		commitNow,
		payloadContext.from,
		state.broadcastID,
		part,
		&actions,
	)

	if state.isDelivered || state.hasDecodeFailed {
		e.mu.Unlock()
		return actions, nil
	}
	e.mu.Unlock()

	// RaptorQ decode and the payload hash cost milliseconds on a full-size
	// broadcast, so they run outside the engine lock. decodeMu serialises them
	// per broadcast; other broadcasts, repair queries and the alarm loop keep
	// making progress meanwhile. Signature verification and FEC origination
	// already stay off the engine lock for the same reason.
	state.decodeMu.Lock()
	defer state.decodeMu.Unlock()

	// decoderReleased is set only under both decodeMu and mu, so a set flag
	// here means a concurrent handler already completed or failed this
	// broadcast. A nil decoder just has not been built yet: construction waits
	// for the first symbol so the multi-hundred-KB buffer zeroing happens here
	// instead of under the engine lock in recheckFECLocked.
	if state.decoderReleased {
		return actions, nil
	}
	decoder := state.decoder
	if decoder == nil {
		raptor := raptorq.NewRaptorQ(uint32(state.partSize))
		decoder, err = raptor.CreateDecoder(uint32(state.fullDataSize))
		if err != nil {
			e.mu.Lock()
			state.hasDecodeFailed = true
			e.releaseFECDecoderLocked(state)
			e.mu.Unlock()
			return actions, fmt.Errorf("create Plumtree FEC decoder: %w", err)
		}
		state.decoder = decoder
	}

	usefulPart := state.decoderPartCount < uint8(plumtreeFECDataParts)
	canDecode, err := decoder.AddSymbol(
		uint32(prepared.message.PartIndex),
		prepared.message.Data,
	)
	if err != nil {
		return actions, fmt.Errorf("add Plumtree FEC symbol: %w", err)
	}
	state.decoderPartCount++
	if !canDecode {
		if usefulPart {
			e.stats.NoteUsefulDelivery(time.Now(), prepared.message.Timestamp)
		}
		return actions, nil
	}

	if state.decodeBuffer == nil {
		state.decodeBuffer = make([]byte, state.fullDataSize)
	}
	decoded, err := decoder.DecodeInto(state.decodeBuffer)
	if err != nil {
		return actions, fmt.Errorf("decode Plumtree FEC payload: %w", err)
	}
	if !decoded {
		if usefulPart {
			e.stats.NoteUsefulDelivery(time.Now(), prepared.message.Timestamp)
		}
		return actions, nil
	}
	payloadHash := sha256.Sum256(state.decodeBuffer)

	e.mu.Lock()
	defer e.mu.Unlock()

	// Close zeroes e.delivered while a decode may still be inside decodeMu;
	// eviction does not set decoderReleased, so this is the first point where
	// shutdown becomes visible to a decode that straddled it.
	if e.closed {
		return actions, nil
	}

	if payloadHash != state.fullDataHash {
		state.hasDecodeFailed = true
		e.releaseFECDecoderLocked(state)
		return actions, fmt.Errorf("Plumtree decoded data hash mismatch")
	}

	if usefulPart {
		e.stats.NoteUsefulDelivery(time.Now(), prepared.message.Timestamp)
	}
	e.stats.NoteDeliveredBroadcast()
	state.isDelivered = true
	e.delivered.add(state.broadcastID)
	data := state.decodeBuffer
	e.releaseFECDecoderLocked(state)
	actions.Deliveries = append(actions.Deliveries, plumtreeDelivery{
		Source: state.firstSource,
		Data:   data,
	})
	return actions, nil
}

func (e *plumtreeEngine) checkPayloadOriginLocked(
	now time.Time,
	payloadContext plumtreePayloadContext,
	key plumtreePartKey,
) (plumtreeActions, error) {
	slot := &e.slots[key.treeIndex]
	if e.isOriginalSender {
		e.removeEagerLocked(slot, payloadContext.from)
		return e.pruneActions(now, payloadContext.from, key),
			fmt.Errorf("original sender ignores Plumtree payload")
	}

	switch payloadContext.origin {
	case plumtreePayloadOriginPush:
		if !slot.hasEager(payloadContext.from) {
			return e.pruneActions(now, payloadContext.from, key),
				fmt.Errorf("unsolicited Plumtree payload from non-eager peer")
		}
	case plumtreePayloadOriginRepair:
		if payloadContext.expectedKey != key {
			return plumtreeActions{}, fmt.Errorf("Plumtree repair response key mismatch")
		}
	default:
		panic("invalid Plumtree payload origin")
	}

	return plumtreeActions{}, nil
}

func (e *plumtreeEngine) precheckSimpleLocked(
	now time.Time,
	payloadContext plumtreePayloadContext,
	key plumtreePartKey,
) (plumtreeActions, error) {
	if e.closed {
		return plumtreeActions{}, errPlumtreeClosed
	}

	actions, err := e.checkPayloadOriginLocked(now, payloadContext, key)
	if err != nil {
		return actions, err
	}

	if _, ok := e.simpleStates[key.broadcastID]; !ok && e.delivered.contains(key.broadcastID) {
		return plumtreeActions{}, fmt.Errorf("known Plumtree simple broadcast")
	}
	return e.checkDuplicateSimpleLocked(now, payloadContext.from, key)
}

func (e *plumtreeEngine) recheckSimpleLocked(
	now time.Time,
	from PeerID,
	key plumtreePartKey,
) (plumtreeActions, error) {
	if e.closed {
		return plumtreeActions{}, errPlumtreeClosed
	}

	if !e.hasStateLocked(key.broadcastID) && e.delivered.contains(key.broadcastID) {
		return plumtreeActions{}, fmt.Errorf("known Plumtree simple broadcast")
	}
	return e.checkDuplicateSimpleLocked(now, from, key)
}

func (e *plumtreeEngine) checkDuplicateSimpleLocked(
	now time.Time,
	from PeerID,
	key plumtreePartKey,
) (plumtreeActions, error) {
	if _, ok := e.simpleStates[key.broadcastID]; ok {
		e.removeEagerLocked(&e.slots[key.treeIndex], from)
		return e.pruneActions(now, from, key),
			fmt.Errorf("duplicate Plumtree simple payload")
	}
	return plumtreeActions{}, nil
}

func (e *plumtreeEngine) precheckFECLocked(
	now time.Time,
	payloadContext plumtreePayloadContext,
	prepared plumtreePreparedFEC,
) (plumtreeActions, error) {
	if e.closed {
		return plumtreeActions{}, errPlumtreeClosed
	}

	actions, err := e.checkPayloadOriginLocked(now, payloadContext, prepared.key)
	if err != nil {
		return actions, err
	}

	state, ok := e.fecStates[prepared.key.broadcastID]
	if !ok && e.delivered.contains(prepared.key.broadcastID) {
		return plumtreeActions{}, fmt.Errorf("known Plumtree broadcast")
	}
	if ok {
		if err := checkPlumtreeFECMetadata(state, prepared); err != nil {
			return plumtreeActions{}, err
		}
		if state.parts[prepared.key.partIndex] != nil {
			e.removeEagerLocked(&e.slots[prepared.key.treeIndex], payloadContext.from)
			return e.pruneActions(now, payloadContext.from, prepared.key),
				fmt.Errorf("duplicate Plumtree part")
		}
	}

	return plumtreeActions{}, nil
}

func (e *plumtreeEngine) recheckFECLocked(
	now time.Time,
	from PeerID,
	prepared plumtreePreparedFEC,
	source PeerID,
) (*plumtreeFECState, plumtreeActions, error) {
	if e.closed {
		return nil, plumtreeActions{}, errPlumtreeClosed
	}

	state, ok := e.fecStates[prepared.key.broadcastID]
	if !ok && e.delivered.contains(prepared.key.broadcastID) {
		return nil, plumtreeActions{}, fmt.Errorf("known Plumtree broadcast")
	}
	if ok {
		if err := checkPlumtreeFECMetadata(state, prepared); err != nil {
			return nil, plumtreeActions{}, err
		}
		if state.parts[prepared.key.partIndex] != nil {
			e.removeEagerLocked(&e.slots[prepared.key.treeIndex], from)
			return nil, e.pruneActions(now, from, prepared.key),
				fmt.Errorf("duplicate Plumtree part")
		}

		wireBytes := uint64(len(prepared.wire.Message))
		if !e.budget.reserve(0, wireBytes) {
			return nil, plumtreeActions{}, errPlumtreeMemoryCapacity
		}
		state.retainedWireBytes += wireBytes
		return state, plumtreeActions{}, nil
	}

	wireBytes := uint64(len(prepared.wire.Message))
	decoderEstimatedBytes := plumtreeFECDecoderBufferEstimate(
		prepared.message.FullDataSize,
		int32(len(prepared.message.Data)),
	)
	if !e.budget.reserve(1, wireBytes+decoderEstimatedBytes) {
		return nil, plumtreeActions{}, errPlumtreeMemoryCapacity
	}

	state = &plumtreeFECState{
		broadcastID:           prepared.key.broadcastID,
		firstSource:           source,
		fullDataHash:          prepared.fullDataHash,
		firstSeen:             now,
		flags:                 prepared.message.Flags,
		fullDataSize:          prepared.message.FullDataSize,
		partSize:              int32(len(prepared.message.Data)),
		retainedWireBytes:     wireBytes,
		decoderEstimatedBytes: decoderEstimatedBytes,
	}
	e.addFECStateLocked(state)
	return state, plumtreeActions{}, nil
}

func (e *plumtreeEngine) releaseFECDecoderLocked(
	state *plumtreeFECState,
) {
	state.decoder = nil
	state.decoderReleased = true
	state.decodeBuffer = nil
	e.budget.release(0, state.decoderEstimatedBytes)
	state.decoderEstimatedBytes = 0
}

func checkPlumtreeFECMetadata(
	state *plumtreeFECState,
	prepared plumtreePreparedFEC,
) error {
	message := prepared.message
	isCollision := state.flags != message.Flags ||
		state.fullDataHash != prepared.fullDataHash ||
		state.fullDataSize != message.FullDataSize ||
		state.partSize != int32(len(message.Data))
	if isCollision {
		return fmt.Errorf("Plumtree broadcast id collision")
	}
	return nil
}

func (e *plumtreeEngine) forwardPayloadLocked(
	now time.Time,
	from PeerID,
	broadcastID [sha256.Size]byte,
	part *plumtreePartState,
	actions *plumtreeActions,
) {
	active := e.activePlumtreePeers()
	verdicts := e.slotReceiveVerdictsLocked(&e.slots[part.treeIndex])
	e.forwardPayloadToActiveLocked(
		now,
		from,
		broadcastID,
		part,
		active,
		&verdicts,
		actions,
	)
}

func (e *plumtreeEngine) activePlumtreePeers() []PeerID {
	active := e.peers.PlumtreeActivePeers(plumtreeActiveNeighbourLimit)
	if len(active) > plumtreeActiveNeighbourLimit {
		active = active[:plumtreeActiveNeighbourLimit]
	}
	return active
}

func (e *plumtreeEngine) forwardPayloadToActiveLocked(
	now time.Time,
	from PeerID,
	broadcastID [sha256.Size]byte,
	part *plumtreePartState,
	active []PeerID,
	verdicts *plumtreeReceiveVerdicts,
	actions *plumtreeActions,
) {
	slot := &e.slots[part.treeIndex]
	e.removeInactiveEagerLocked(slot, now, verdicts)

	controlCount := 0
	if !from.IsZero() {
		controlCount = 1
	}
	actions.Outbounds = slices.Grow(
		actions.Outbounds,
		controlCount+int(slot.eagerCount)+len(active),
	)
	if !from.IsZero() {
		actions.Outbounds = append(
			actions.Outbounds,
			plumtreeUsefulOutbound(now, from, broadcastID, part),
		)
	}
	iHaveControl := plumtreeIHaveControl(now, broadcastID, part)

	for i := range int(slot.eagerCount) {
		peer := slot.eager[i]
		if peer == from {
			continue
		}
		e.sendPayloadLocked(now, peer, slot, part, verdicts, actions)
	}

	for _, peer := range active {
		if peer == e.localID || peer == from || slot.hasEager(peer) {
			continue
		}
		e.sendIHaveLocked(now, peer, slot, part, iHaveControl, actions)
	}
}

// sendPayloadLocked needs a roster verdict because eager peers are promoted
// from inbound senders and may be absent from the active snapshot.
func (e *plumtreeEngine) sendPayloadLocked(
	now time.Time,
	peer PeerID,
	slot *plumtreeSlot,
	part *plumtreePartState,
	verdicts *plumtreeReceiveVerdicts,
	actions *plumtreeActions,
) bool {
	if peer.IsZero() || peer == e.localID {
		return false
	}
	if int(part.fullSentCount) >= e.localEagerLimit || part.wasSentTo(peer) {
		return false
	}
	if !e.canReserveFeedbackLocked(slot, peer, now) {
		return false
	}
	if !verdicts.receivesBroadcasts(peer) {
		return false
	}

	e.registerFullSendLocked(slot, part, peer, now)
	actions.Outbounds = append(
		actions.Outbounds,
		plumtreePayloadOutbound(peer, part.wire.Message),
	)
	return true
}

// sendIHaveLocked trusts the active snapshot: it was already filtered by the
// same roster predicate plus QUIC readiness within the accepted snapshot TTL.
func (e *plumtreeEngine) sendIHaveLocked(
	now time.Time,
	peer PeerID,
	slot *plumtreeSlot,
	part *plumtreePartState,
	control plumtreeOutboundControl,
	actions *plumtreeActions,
) {
	if peer.IsZero() || peer == e.localID ||
		part.wasSentTo(peer) || part.wasAdvertisedTo(peer) {
		return
	}
	if !e.canReserveFeedbackLocked(slot, peer, now) {
		return
	}
	if int(part.fullSentCount) >= e.localEagerLimit || part.dataSize == 0 {
		return
	}

	actions.Outbounds = append(
		actions.Outbounds,
		plumtreeOutboundAction{
			Kind:    plumtreeOutboundIHave,
			To:      peer,
			Control: control,
		},
	)
	part.addAdvertised(peer)
}

func (e *plumtreeEngine) pruneActions(
	now time.Time,
	peer PeerID,
	key plumtreePartKey,
) plumtreeActions {
	if peer.IsZero() || peer == e.localID {
		return plumtreeActions{}
	}
	return plumtreeActions{
		Outbounds: []plumtreeOutboundAction{
			plumtreePruneOutbound(now, peer, key),
		},
	}
}

func plumtreePayloadOutbound(
	peer PeerID,
	wire []byte,
) plumtreeOutboundAction {
	return plumtreeOutboundAction{
		Kind: plumtreeOutboundPayload,
		To:   peer,
		Wire: wire,
	}
}

func plumtreeIHaveControl(
	now time.Time,
	broadcastID [sha256.Size]byte,
	part *plumtreePartState,
) plumtreeOutboundControl {
	return plumtreeOutboundControl{
		key: plumtreePartKey{
			broadcastID: broadcastID,
			partIndex:   part.partIndex,
			treeIndex:   part.treeIndex,
		},
		timestamp: plumtreeUnixSeconds(now),
		part:      part,
	}
}

func plumtreePruneOutbound(
	now time.Time,
	peer PeerID,
	key plumtreePartKey,
) plumtreeOutboundAction {
	return plumtreeOutboundAction{
		Kind: plumtreeOutboundPrune,
		To:   peer,
		Control: plumtreeOutboundControl{
			key:       key,
			timestamp: plumtreeUnixSeconds(now),
		},
	}
}

func plumtreeUsefulOutbound(
	now time.Time,
	peer PeerID,
	broadcastID [sha256.Size]byte,
	part *plumtreePartState,
) plumtreeOutboundAction {
	return plumtreeOutboundAction{
		Kind: plumtreeOutboundUseful,
		To:   peer,
		Control: plumtreeOutboundControl{
			key: plumtreePartKey{
				broadcastID: broadcastID,
				partIndex:   part.partIndex,
				treeIndex:   part.treeIndex,
			},
			timestamp: plumtreeUnixSeconds(now),
		},
	}
}

func plumtreeUnixSeconds(now time.Time) float64 {
	return float64(now.Unix()) + float64(now.Nanosecond())/float64(time.Second)
}

func (e *plumtreeEngine) addFECStateLocked(state *plumtreeFECState) {
	state.previous = e.fecNewest
	if e.fecNewest != nil {
		e.fecNewest.next = state
	} else {
		e.fecOldest = state
	}
	e.fecNewest = state
	e.fecStates[state.broadcastID] = state
}

func (e *plumtreeEngine) addSimpleStateLocked(state *plumtreeSimpleState) {
	state.previous = e.simpleNewest
	if e.simpleNewest != nil {
		e.simpleNewest.next = state
	} else {
		e.simpleOldest = state
	}
	e.simpleNewest = state
	e.simpleStates[state.broadcastID] = state
}
