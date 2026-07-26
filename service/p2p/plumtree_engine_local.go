package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"runtime"
	"sync"

	"github.com/xssnick/raptorq"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// The fixed boxed source, certificate, hashes, indices, and signature fit in
// this reserve, so TL appends the variable payload directly into its final wire.
const plumtreeLocalWireOverhead = 256

// plumtreeLocalOrigin owns the signing key and the local QUIC envelope shared
// by every payload produced for one overlay.
type plumtreeLocalOrigin struct {
	privateKey ed25519.PrivateKey
	source     keys.PublicKeyED25519
	sourceID   PeerID
	envelope   *quicOverlayEnvelope
}

func newPlumtreeLocalOrigin(
	privateKey ed25519.PrivateKey,
	envelope *quicOverlayEnvelope,
) (plumtreeLocalOrigin, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return plumtreeLocalOrigin{}, fmt.Errorf(
			"invalid local Plumtree private key size %d",
			len(privateKey),
		)
	}

	ownedKey := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if !bytes.Equal(ownedKey, privateKey) {
		return plumtreeLocalOrigin{}, fmt.Errorf(
			"local Plumtree private key has inconsistent public half",
		)
	}

	publicKey := ed25519.PublicKey(ownedKey[ed25519.SeedSize:])
	sourceID, err := peerIDFromED25519PublicKey(publicKey)
	if err != nil {
		return plumtreeLocalOrigin{}, err
	}

	return plumtreeLocalOrigin{
		privateKey: ownedKey,
		source: keys.PublicKeyED25519{
			Key: publicKey,
		},
		sourceID: sourceID,
		envelope: envelope,
	}, nil
}

func (o plumtreeLocalOrigin) serialize(
	message tl.Serializable,
	payloadSize int,
) (plumtreePayloadWire, error) {
	wire, bodyOffset := o.envelope.messageBuffer(
		payloadSize + plumtreeLocalWireOverhead,
	)
	wire, err := tl.Append(wire, message, true)
	if err != nil {
		return plumtreePayloadWire{}, err
	}
	return plumtreePayloadWire{
		Message: wire,
		Body:    wire[bodyOffset:],
	}, nil
}

// OriginateSimple signs and retains one already serialized TON broadcast.
// The caller keeps data immutable only until this method returns.
func (e *plumtreeEngine) OriginateSimple(
	origin plumtreeLocalOrigin,
	flags int32,
	broadcastID [sha256.Size]byte,
	data []byte,
) (plumtreeActions, error) {
	if err := e.validateLocalOrigin(origin); err != nil {
		return plumtreeActions{}, err
	}
	if broadcastID == ([sha256.Size]byte{}) {
		return plumtreeActions{}, fmt.Errorf("empty local Plumtree simple broadcast id")
	}
	if len(data) == 0 || len(data) > int(plumtreeMaxPayloadSize) {
		return plumtreeActions{}, fmt.Errorf(
			"invalid local Plumtree simple payload size %d",
			len(data),
		)
	}
	if err := e.checkLocalBroadcastID(broadcastID); err != nil {
		return plumtreeActions{}, err
	}

	dataHash := sha256.Sum256(data)
	now := e.now()
	timestamp := plumtreeUnixSeconds(now)
	signedData := serializeValidatedPlumtreeSimpleToSign(
		BroadcastPlumtreeSimpleToSign{
			ID:        broadcastID[:],
			Timestamp: timestamp,
			TreeIndex: plumtreeSimpleTree,
			DataSize:  int32(len(data)),
			DataHash:  dataHash[:],
		},
	)
	signature := ed25519.Sign(origin.privateKey, signedData)
	wire, err := origin.serialize(
		BroadcastPlumtreeSimple{
			Flags:       flags,
			Timestamp:   timestamp,
			Source:      origin.source,
			Certificate: overlay.CertificateEmpty{},
			BroadcastID: broadcastID[:],
			TreeIndex:   plumtreeSimpleTree,
			Data:        data,
			Signature:   signature,
		},
		len(data),
	)
	if err != nil {
		return plumtreeActions{}, fmt.Errorf(
			"serialize local Plumtree simple payload: %w",
			err,
		)
	}

	part := plumtreePartState{
		partIndex:   0,
		treeIndex:   plumtreeSimpleTree,
		timestamp:   timestamp,
		dataSize:    int32(len(data)),
		dataHash:    dataHash,
		sourceKey:   origin.source,
		certificate: plumtreeCertificate{kind: plumtreeCertificateEmpty},
		signature:   signature,
		wire:        wire,
	}
	state := &plumtreeSimpleState{
		broadcastID: broadcastID,
		source:      origin.sourceID,
		firstSeen:   now,
		part:        part,
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err = e.checkLocalBroadcastIDLocked(broadcastID); err != nil {
		return plumtreeActions{}, err
	}

	wireBytes := uint64(len(state.part.wire.Message))
	if !e.budget.reserve(1, wireBytes) {
		return plumtreeActions{}, errPlumtreeMemoryCapacity
	}
	state.retainedWireBytes = wireBytes
	e.addSimpleStateLocked(state)
	e.delivered.add(broadcastID)

	var actions plumtreeActions
	e.forwardPayloadLocked(
		now,
		PeerID{},
		broadcastID,
		&state.part,
		&actions,
	)
	return actions, nil
}

// OriginateFEC signs and retains all 45 RaptorQ symbols for one already
// serialized TON broadcast. The caller keeps data immutable only until this
// method returns.
func (e *plumtreeEngine) OriginateFEC(
	origin plumtreeLocalOrigin,
	flags int32,
	data []byte,
) (plumtreeActions, error) {
	if err := e.validateLocalOrigin(origin); err != nil {
		return plumtreeActions{}, err
	}
	if len(data) == 0 || len(data) > int(plumtreeMaxPayloadSize) {
		return plumtreeActions{}, fmt.Errorf(
			"invalid local Plumtree FEC payload size %d",
			len(data),
		)
	}

	fullDataSize := int32(len(data))
	partSize := plumtreeFECPartSize(fullDataSize)
	fullDataHash := sha256.Sum256(data)
	broadcastID := computeLocalPlumtreeFECID(
		flags,
		fullDataHash,
		fullDataSize,
		partSize,
	)
	if broadcastID == ([sha256.Size]byte{}) {
		return plumtreeActions{}, fmt.Errorf("empty local Plumtree FEC broadcast id")
	}
	if err := e.checkLocalBroadcastID(broadcastID); err != nil {
		return plumtreeActions{}, err
	}

	raptor := raptorq.NewRaptorQ(uint32(partSize))
	encoder, err := raptor.CreateEncoder(data)
	if err != nil {
		return plumtreeActions{}, fmt.Errorf(
			"create local Plumtree FEC encoder: %w",
			err,
		)
	}

	// One timestamp for all parts: each is signed independently, so a shared
	// value is wire-valid and saves 44 clock reads.
	timestamp := plumtreeUnixSeconds(e.now())
	var parts [plumtreeFECTotalParts]plumtreePartState
	buildPart := func(partIndex int32) error {
		symbol := encoder.GenSymbol(uint32(partIndex))
		treeIndex := partIndex + plumtreeFECTreeOffset
		dataHash := sha256.Sum256(symbol)
		signedData := serializeValidatedPlumtreeFECToSign(
			BroadcastPlumtreeFECToSign{
				ID:        broadcastID[:],
				Timestamp: timestamp,
				PartIndex: partIndex,
				TreeIndex: treeIndex,
				DataSize:  partSize,
				DataHash:  dataHash[:],
			},
		)
		signature := ed25519.Sign(origin.privateKey, signedData)
		wire, serializeErr := origin.serialize(
			BroadcastPlumtreeFEC{
				Flags:        flags,
				Timestamp:    timestamp,
				Source:       origin.source,
				Certificate:  overlay.CertificateEmpty{},
				FullDataHash: fullDataHash[:],
				FullDataSize: fullDataSize,
				PartIndex:    partIndex,
				TreeIndex:    treeIndex,
				Data:         symbol,
				Signature:    signature,
			},
			len(symbol),
		)
		if serializeErr != nil {
			return fmt.Errorf(
				"serialize local Plumtree FEC part %d: %w",
				partIndex,
				serializeErr,
			)
		}

		parts[partIndex] = plumtreePartState{
			partIndex:   partIndex,
			treeIndex:   treeIndex,
			timestamp:   timestamp,
			dataSize:    partSize,
			dataHash:    dataHash,
			sourceKey:   origin.source,
			certificate: plumtreeCertificate{kind: plumtreeCertificateEmpty},
			signature:   signature,
			wire:        wire,
		}
		return nil
	}
	if err = buildPlumtreeFECParts(buildPart); err != nil {
		return plumtreeActions{}, err
	}

	commitNow := e.now()
	state := &plumtreeFECState{
		broadcastID:  broadcastID,
		firstSource:  origin.sourceID,
		fullDataHash: fullDataHash,
		firstSeen:    commitNow,
		flags:        flags,
		fullDataSize: fullDataSize,
		partSize:     partSize,
		isDelivered:  true,
		partCount:    uint8(plumtreeFECTotalParts),
	}
	for partIndex := int32(0); partIndex < plumtreeFECTotalParts; partIndex++ {
		state.parts[partIndex] = &parts[partIndex]
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err = e.checkLocalBroadcastIDLocked(broadcastID); err != nil {
		return plumtreeActions{}, err
	}

	var wireBytes uint64
	for partIndex := int32(0); partIndex < plumtreeFECTotalParts; partIndex++ {
		wireBytes += uint64(len(state.parts[partIndex].wire.Message))
	}
	if !e.budget.reserve(1, wireBytes) {
		return plumtreeActions{}, errPlumtreeMemoryCapacity
	}
	state.retainedWireBytes = wireBytes
	e.addFECStateLocked(state)
	e.delivered.add(broadcastID)

	active := e.activePlumtreePeers()
	verdicts := e.fecEagerReceiveVerdictsLocked()
	outboundCapacity := int(plumtreeFECTotalParts) * len(active)
	for partIndex := int32(0); partIndex < plumtreeFECTotalParts; partIndex++ {
		treeIndex := partIndex + plumtreeFECTreeOffset
		outboundCapacity += int(e.slots[treeIndex].eagerCount)
	}
	actions := plumtreeActions{
		Outbounds: make([]plumtreeOutboundAction, 0, outboundCapacity),
	}
	for partIndex := int32(0); partIndex < plumtreeFECTotalParts; partIndex++ {
		e.forwardPayloadToActiveLocked(
			commitNow,
			PeerID{},
			broadcastID,
			state.parts[partIndex],
			active,
			&verdicts,
			&actions,
		)
	}
	return actions, nil
}

// buildPlumtreeFECParts fans one part builder out over contiguous partIndex
// ranges. Safe because GenSymbol supports concurrent calls on one encoder,
// ed25519.Sign is pure, origin.serialize is an atomic load plus a fresh
// buffer, and every part lands in its own slot. The first error wins.
func buildPlumtreeFECParts(buildPart func(partIndex int32) error) error {
	workers := int32(runtime.NumCPU())
	if workers > plumtreeFECTotalParts {
		workers = plumtreeFECTotalParts
	}
	if workers <= 1 {
		for partIndex := int32(0); partIndex < plumtreeFECTotalParts; partIndex++ {
			if err := buildPart(partIndex); err != nil {
				return err
			}
		}
		return nil
	}

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		buildErr error
	)
	partsPerWorker := (plumtreeFECTotalParts + workers - 1) / workers
	for start := int32(0); start < plumtreeFECTotalParts; start += partsPerWorker {
		end := min(start+partsPerWorker, plumtreeFECTotalParts)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partIndex := start; partIndex < end; partIndex++ {
				if err := buildPart(partIndex); err != nil {
					errOnce.Do(func() { buildErr = err })
					return
				}
			}
		}()
	}
	wg.Wait()
	return buildErr
}

func (e *plumtreeEngine) validateLocalOrigin(
	origin plumtreeLocalOrigin,
) error {
	if !e.isOriginalSender {
		return fmt.Errorf("local Plumtree origin requires original-sender engine")
	}
	if origin.sourceID != e.localID {
		return fmt.Errorf("local Plumtree source does not match engine identity")
	}
	return nil
}

func (e *plumtreeEngine) checkLocalBroadcastIDLocked(
	broadcastID [sha256.Size]byte,
) error {
	if e.closed {
		return errPlumtreeClosed
	}

	if e.hasStateLocked(broadcastID) || e.delivered.contains(broadcastID) {
		return fmt.Errorf("known local Plumtree broadcast")
	}
	return nil
}

func (e *plumtreeEngine) checkLocalBroadcastID(
	broadcastID [sha256.Size]byte,
) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.checkLocalBroadcastIDLocked(broadcastID)
}

func computeLocalPlumtreeFECID(
	flags int32,
	fullDataHash [sha256.Size]byte,
	fullDataSize int32,
	partSize int32,
) [sha256.Size]byte {
	return computeValidatedPlumtreeFECID(
		BroadcastPlumtreeFEC{
			Flags:        flags,
			FullDataHash: fullDataHash[:],
			FullDataSize: fullDataSize,
		},
		partSize,
	)
}
