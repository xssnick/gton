package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/raptorq"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type plumtreeEngineTestPeers struct {
	active   []PeerID
	receives map[PeerID]bool
}

func (p *plumtreeEngineTestPeers) PlumtreeActivePeers(limit int) []PeerID {
	if len(p.active) <= limit {
		return p.active
	}
	return p.active[:limit]
}

func (p *plumtreeEngineTestPeers) PlumtreePeerReceivesBroadcasts(peer PeerID) bool {
	return p.receives[peer]
}

func (p *plumtreeEngineTestPeers) PlumtreePeersReceiveBroadcasts(
	peers []PeerID,
	out []bool,
) {
	for i, peer := range peers {
		out[i] = p.receives[peer]
	}
}

type plumtreeEngineTestVerifier struct {
	calls   atomic.Int64
	entered chan struct{}
	release <-chan struct{}
	exited  chan struct{}
}

func (v *plumtreeEngineTestVerifier) VerifyPlumtree(
	ctx context.Context,
	request plumtreeVerification,
) (PeerID, error) {
	v.calls.Add(1)
	if v.entered != nil {
		v.entered <- struct{}{}
	}
	if v.release != nil {
		select {
		case <-ctx.Done():
			return PeerID{}, ctx.Err()
		case <-v.release:
		}
	}
	if v.exited != nil {
		v.exited <- struct{}{}
	}
	return PeerID(sha256.Sum256(request.Source.Key)), nil
}

func TestPlumtreeEngineSimpleRepairRelayAndDuplicate(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_000, 123_000_000)
	local := plumtreeEngineTestPeer(1)
	sourcePeer := plumtreeEngineTestPeer(2)
	relayPeer := plumtreeEngineTestPeer(3)
	peers := &plumtreeEngineTestPeers{
		active: []PeerID{relayPeer},
		receives: map[PeerID]bool{
			sourcePeer: true,
			relayPeer:  true,
		},
	}
	verifier := &plumtreeEngineTestVerifier{}
	engine := plumtreeEngineTestNew(t, local, false, peers, verifier, func() time.Time {
		return now
	})
	source := plumtreeEngineTestSource(0x31)

	firstID := plumtreeEngineTestID(1)
	firstData := []byte("first Plumtree payload")
	firstMessage := plumtreeEngineTestSimple(now, firstID, source, firstData)
	firstIHave := plumtreeEngineTestIHaveSimple(now, firstMessage)

	actions, err := engine.HandleIHave(t.Context(), now, sourcePeer, firstIHave)
	if err != nil {
		t.Fatalf("handle first IHAVE: %v", err)
	}
	if len(actions.Repairs) != 1 {
		t.Fatalf("repair actions = %d, want 1", len(actions.Repairs))
	}

	firstEnvelope := plumtreeEngineTestSimpleEnvelope(t, firstMessage, 0xa1)
	actions, err = engine.HandleRepairSimple(
		t.Context(),
		now,
		actions.Repairs[0].ID,
		firstEnvelope,
	)
	if err != nil {
		t.Fatalf("handle first repair: %v", err)
	}
	if len(actions.Deliveries) != 1 || !bytes.Equal(actions.Deliveries[0].Data, firstData) {
		t.Fatalf("unexpected first delivery: %+v", actions.Deliveries)
	}
	if len(actions.Outbounds) != 2 ||
		actions.Outbounds[0].Kind != plumtreeOutboundUseful ||
		actions.Outbounds[1].Kind != plumtreeOutboundIHave {
		t.Fatalf("first outbounds = %+v, want Useful then IHAVE", actions.Outbounds)
	}

	answer, err := engine.HandleRepairQuery(now, relayPeer, RepairPlumtreePart{
		BroadcastID: firstID[:],
		Timestamp:   plumtreeUnixSeconds(now),
		PartIndex:   0,
		TreeIndex:   0,
	})
	if err != nil {
		t.Fatalf("answer relay repair: %v", err)
	}
	if &answer[0] != &firstEnvelope.Wire.Body[0] {
		t.Fatal("repair answer did not reuse the retained body")
	}
	if err = engine.HandleUseful(now, relayPeer, BroadcastPlumtreeUseful{
		BroadcastID: firstID[:],
		Timestamp:   plumtreeUnixSeconds(now),
		PartIndex:   0,
		TreeIndex:   0,
	}); err != nil {
		t.Fatalf("handle Useful: %v", err)
	}

	secondID := plumtreeEngineTestID(2)
	secondMessage := plumtreeEngineTestSimple(
		now.Add(time.Millisecond),
		secondID,
		source,
		[]byte("second Plumtree payload"),
	)
	secondEnvelope := plumtreeEngineTestSimpleEnvelope(t, secondMessage, 0xa2)
	actions, err = engine.HandleSimple(
		t.Context(),
		now.Add(time.Millisecond),
		sourcePeer,
		secondEnvelope,
	)
	if err != nil {
		t.Fatalf("handle direct simple payload: %v", err)
	}
	if len(actions.Outbounds) != 2 ||
		actions.Outbounds[0].Kind != plumtreeOutboundUseful ||
		actions.Outbounds[1].Kind != plumtreeOutboundPayload ||
		actions.Outbounds[1].To != relayPeer {
		t.Fatalf("outbounds = %+v, want Useful then relay payload", actions.Outbounds)
	}
	if &actions.Outbounds[1].Wire[0] != &secondEnvelope.Wire.Message[0] {
		t.Fatal("relay did not reuse the immutable message wire")
	}
	if err = engine.HandlePrune(now.Add(time.Millisecond), relayPeer, BroadcastPlumtreePrune{
		BroadcastID: secondID[:],
		Timestamp:   plumtreeUnixSeconds(now.Add(time.Millisecond)),
		PartIndex:   0,
		TreeIndex:   0,
	}); err != nil {
		t.Fatalf("handle Prune: %v", err)
	}
	engine.mu.Lock()
	relayIsEager := engine.slots[0].hasEager(relayPeer)
	engine.mu.Unlock()
	if relayIsEager {
		t.Fatal("Prune did not remove the eager relay")
	}

	callsBeforeDuplicate := verifier.calls.Load()
	actions, err = engine.HandleSimple(
		t.Context(),
		now.Add(2*time.Millisecond),
		sourcePeer,
		secondEnvelope,
	)
	if err == nil {
		t.Fatal("duplicate simple payload was accepted")
	}
	if len(actions.Outbounds) != 1 ||
		actions.Outbounds[0].Kind != plumtreeOutboundPrune {
		t.Fatalf("duplicate outbounds = %+v, want Prune", actions.Outbounds)
	}
	if verifier.calls.Load() != callsBeforeDuplicate {
		t.Fatal("duplicate payload reached the verifier")
	}
}

func TestPlumtreeEngineOriginalSenderUsesOneEagerPeerAndPrunesPayloads(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_050, 0)
	verifier := &plumtreeEngineTestVerifier{}
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(6),
		true,
		&plumtreeEngineTestPeers{receives: make(map[PeerID]bool)},
		verifier,
		func() time.Time { return now },
	)
	if engine.localEagerLimit != plumtreeOriginalEagerLimit ||
		len(engine.slots) != int(plumtreeTreeSlots) {
		t.Fatalf(
			"original limits = eager %d, slots %d",
			engine.localEagerLimit,
			len(engine.slots),
		)
	}

	first := plumtreeEngineTestPeer(7)
	second := plumtreeEngineTestPeer(8)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[0], first, true, now)
	engine.promoteEagerLocked(&engine.slots[0], second, true, now)
	eagerCount := engine.slots[0].eagerCount
	hasSecond := engine.slots[0].hasEager(second)
	engine.mu.Unlock()
	if eagerCount != 1 || !hasSecond {
		t.Fatalf("original eager state = count %d, second %v", eagerCount, hasSecond)
	}

	id := plumtreeEngineTestID(9)
	message := BroadcastPlumtreeSimple{
		Timestamp:   plumtreeUnixSeconds(now),
		Source:      &keys.PublicKeyED25519{},
		Certificate: (*overlay.Certificate)(nil),
		BroadcastID: id[:],
		TreeIndex:   0,
		Data:        []byte{1},
	}
	actions, err := engine.HandleSimple(
		t.Context(),
		now,
		second,
		plumtreeSimpleEnvelope{
			Message: message,
			Wire: plumtreePayloadWire{
				Message: []byte{0xa0},
				Body:    []byte{0xa1},
			},
		},
	)
	if err == nil || len(actions.Outbounds) != 1 ||
		actions.Outbounds[0].Kind != plumtreeOutboundPrune {
		t.Fatalf("original payload result = outbounds %+v, err %v", actions.Outbounds, err)
	}
	if verifier.calls.Load() != 0 {
		t.Fatal("original sender verified a payload it must ignore")
	}

	if _, err = engine.HandleIHave(
		t.Context(),
		now,
		PeerID{},
		BroadcastPlumtreeIHave{},
	); err != nil {
		t.Fatalf("original sender validated ignored IHAVE: %v", err)
	}
}

func TestPlumtreeEngineAcceptsOnlyDecodedValueIdentityVariants(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_075, 0)
	from := plumtreeEngineTestPeer(10)
	verifier := &plumtreeEngineTestVerifier{}
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(9),
		false,
		&plumtreeEngineTestPeers{
			receives: map[PeerID]bool{from: true},
		},
		verifier,
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[0], from, true, now)
	engine.mu.Unlock()

	id := plumtreeEngineTestID(10)
	source := plumtreeEngineTestSource(0x41)
	tests := []struct {
		name        string
		source      any
		certificate any
	}{
		{
			name:        "source pointer",
			source:      &source,
			certificate: overlay.CertificateEmpty{},
		},
		{
			name:        "certificate pointer",
			source:      source,
			certificate: &overlay.CertificateEmpty{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := BroadcastPlumtreeSimple{
				Timestamp:   plumtreeUnixSeconds(now),
				Source:      test.source,
				Certificate: test.certificate,
				BroadcastID: id[:],
				TreeIndex:   0,
				Data:        []byte{1},
			}
			_, err := engine.HandleSimple(
				t.Context(),
				now,
				from,
				plumtreeSimpleEnvelope{
					Message: message,
					Wire: plumtreePayloadWire{
						Message: []byte{1},
						Body:    []byte{2},
					},
				},
			)
			if err == nil {
				t.Fatal("pointer identity variant was accepted")
			}
		})
	}
	if verifier.calls.Load() != 0 {
		t.Fatal("unsupported identity reached the verifier")
	}
}

func TestPlumtreeEngineCompactSignatureSerializationMatchesProtocol(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_090, 0)
	id := plumtreeEngineTestID(14)
	hash := plumtreeEngineTestID(15)

	simpleExpected, err := plumtreeSimpleToSignBytes(
		id[:],
		plumtreeUnixSeconds(now),
		0,
		32,
		hash[:],
	)
	if err != nil {
		t.Fatalf("serialize reference simple signature: %v", err)
	}
	simple := serializeValidatedPlumtreeSimpleToSign(
		BroadcastPlumtreeSimpleToSign{
			ID:        id[:],
			Timestamp: plumtreeUnixSeconds(now),
			TreeIndex: 0,
			DataSize:  32,
			DataHash:  hash[:],
		},
	)
	if !bytes.Equal(simple, simpleExpected) ||
		len(simple) != plumtreeSimpleToSignWireSize {
		t.Fatal("compact simple signature payload differs from protocol helper")
	}

	fecExpected, err := plumtreeFECToSignBytes(
		id[:],
		plumtreeUnixSeconds(now),
		3,
		4,
		32,
		hash[:],
	)
	if err != nil {
		t.Fatalf("serialize reference FEC signature: %v", err)
	}
	fec := serializeValidatedPlumtreeFECToSign(
		BroadcastPlumtreeFECToSign{
			ID:        id[:],
			Timestamp: plumtreeUnixSeconds(now),
			PartIndex: 3,
			TreeIndex: 4,
			DataSize:  32,
			DataHash:  hash[:],
		},
	)
	if !bytes.Equal(fec, fecExpected) || len(fec) != plumtreeFECToSignWireSize {
		t.Fatal("compact FEC signature payload differs from protocol helper")
	}
}

func TestPlumtreeEngineFECDecodesRaptorQThirtyOfFortyFive(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_100, 0)
	local := plumtreeEngineTestPeer(11)
	sourcePeer := plumtreeEngineTestPeer(12)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{sourcePeer: true},
	}
	engine := plumtreeEngineTestNew(
		t,
		local,
		false,
		peers,
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	source := plumtreeEngineTestSource(0x42)
	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i)
	}

	partSize := plumtreeFECPartSize(int32(len(data)))
	raptor := raptorq.NewRaptorQ(uint32(partSize))
	encoder, err := raptor.CreateEncoder(data)
	if err != nil {
		t.Fatalf("create FEC encoder: %v", err)
	}
	if encoder.BaseSymbolsNum() != uint32(plumtreeFECDataParts) {
		t.Fatalf(
			"base symbols = %d, want %d",
			encoder.BaseSymbolsNum(),
			plumtreeFECDataParts,
		)
	}

	fullHash := sha256.Sum256(data)
	broadcastID, err := plumtreeFECBroadcastID(
		0,
		fullHash[:],
		int32(len(data)),
		partSize,
	)
	if err != nil {
		t.Fatalf("compute FEC broadcast id: %v", err)
	}

	partOrder := make([]int32, 0, plumtreeFECTotalParts)
	for i := int32(15); i < plumtreeFECTotalParts; i++ {
		partOrder = append(partOrder, i)
	}
	for i := int32(0); i < 15; i++ {
		partOrder = append(partOrder, i)
	}

	var delivered []byte
	partsUsed := 0
	for _, partIndex := range partOrder {
		symbol := encoder.GenSymbol(uint32(partIndex))
		message := BroadcastPlumtreeFEC{
			Flags:        0,
			Timestamp:    plumtreeUnixSeconds(now),
			Source:       source,
			Certificate:  overlay.CertificateEmpty{},
			FullDataHash: fullHash[:],
			FullDataSize: int32(len(data)),
			PartIndex:    partIndex,
			TreeIndex:    partIndex + plumtreeFECTreeOffset,
			Data:         symbol,
			Signature:    []byte{0x91},
		}
		partHash := sha256.Sum256(symbol)
		repairActions, handleErr := engine.HandleIHave(
			t.Context(),
			now,
			sourcePeer,
			BroadcastPlumtreeIHave{
				BroadcastID:      broadcastID[:],
				Timestamp:        plumtreeUnixSeconds(now),
				PartIndex:        partIndex,
				TreeIndex:        partIndex + plumtreeFECTreeOffset,
				Source:           source,
				Certificate:      overlay.CertificateEmpty{},
				PayloadTimestamp: plumtreeUnixSeconds(now),
				DataSize:         int32(len(symbol)),
				DataHash:         partHash[:],
				Signature:        message.Signature,
			},
		)
		if handleErr != nil {
			t.Fatalf("handle FEC IHAVE part %d: %v", partIndex, handleErr)
		}
		if len(repairActions.Repairs) != 1 {
			t.Fatalf(
				"part %d repair actions = %d, want 1",
				partIndex,
				len(repairActions.Repairs),
			)
		}

		envelope := plumtreeEngineTestFECEnvelope(t, message, byte(partIndex))
		actions, handleErr := engine.HandleRepairFEC(
			t.Context(),
			now,
			repairActions.Repairs[0].ID,
			envelope,
		)
		if handleErr != nil {
			t.Fatalf("handle FEC repair part %d: %v", partIndex, handleErr)
		}
		partsUsed++
		if len(actions.Deliveries) == 1 {
			delivered = actions.Deliveries[0].Data
			break
		}
	}

	if !bytes.Equal(delivered, data) {
		t.Fatal("decoded FEC payload does not match input")
	}
	if partsUsed > int(plumtreeFECTotalParts) {
		t.Fatalf("decoded after %d parts, limit is %d", partsUsed, plumtreeFECTotalParts)
	}
}

func TestPlumtreeEngineRepairDelayTargetsAndActiveCap(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_200, 0)
	local := plumtreeEngineTestPeer(21)
	eager := plumtreeEngineTestPeer(22)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{eager: true},
	}
	verifier := &plumtreeEngineTestVerifier{}
	engine := plumtreeEngineTestNew(t, local, false, peers, verifier, func() time.Time {
		return now
	})
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[1], eager, true, now)
	engine.mu.Unlock()

	broadcastID := plumtreeEngineTestID(23)
	source := plumtreeEngineTestSource(0x53)
	dataHash := sha256.Sum256([]byte{1})
	for i := byte(0); i < plumtreeRepairTargetLimit+1; i++ {
		from := plumtreeEngineTestPeer(30 + i)
		actions, err := engine.HandleIHave(
			t.Context(),
			now,
			from,
			BroadcastPlumtreeIHave{
				BroadcastID:      broadcastID[:],
				Timestamp:        plumtreeUnixSeconds(now),
				PartIndex:        0,
				TreeIndex:        1,
				Source:           source,
				Certificate:      overlay.CertificateEmpty{},
				PayloadTimestamp: plumtreeUnixSeconds(now),
				DataSize:         1,
				DataHash:         dataHash[:],
				Signature:        []byte{1},
			},
		)
		if err != nil {
			t.Fatalf("handle target %d: %v", i, err)
		}
		if len(actions.Repairs) != 0 {
			t.Fatalf("target %d repaired before delay", i)
		}
	}
	if verifier.calls.Load() != plumtreeRepairTargetLimit {
		t.Fatalf(
			"verifier calls = %d, want %d",
			verifier.calls.Load(),
			plumtreeRepairTargetLimit,
		)
	}
	if actions := engine.Alarm(now.Add(plumtreeRepairDelay - time.Nanosecond)); len(actions.Repairs) != 0 {
		t.Fatal("repair started before 200 ms")
	}
	actions := engine.Alarm(now.Add(plumtreeRepairDelay))
	if len(actions.Repairs) != plumtreeRepairTargetLimit {
		t.Fatalf(
			"repair actions = %d, want %d",
			len(actions.Repairs),
			plumtreeRepairTargetLimit,
		)
	}
	for _, action := range actions.Repairs {
		if action.Timeout != 5*time.Second {
			t.Fatalf("repair timeout = %v, want 5s", action.Timeout)
		}
		if action.MaxAnswerSize != 1+plumtreeRepairMTUOverhead {
			t.Fatalf("repair max answer = %d", action.MaxAnswerSize)
		}
	}

	engine.mu.Lock()
	var dropped plumtreeMissingPart
	dropped.dataSize = 1
	dropped.targetCount = 1
	dropped.targets[0] = plumtreeEngineTestPeer(99)
	for len(engine.repairs) < plumtreeActiveRepairLimit {
		engine.nextRepairID++
		engine.repairs[engine.nextRepairID] = plumtreeRepairAttempt{}
	}
	var cappedActions plumtreeActions
	engine.startRepairRequestsLocked(now, &dropped, &cappedActions)
	engine.mu.Unlock()
	if len(cappedActions.Repairs) != 0 || dropped.sentTargets != 1 {
		t.Fatal("active-cap target was not consumed without a query")
	}

	if err := engine.FinishRepair(actions.Repairs[0].ID); err != nil {
		t.Fatalf("finish repair: %v", err)
	}
	engine.mu.Lock()
	engine.startRepairRequestsLocked(now, &dropped, &cappedActions)
	engine.mu.Unlock()
	if len(cappedActions.Repairs) != 0 {
		t.Fatal("consumed target retried after capacity became available")
	}
}

func TestPlumtreeEngineBoundedCachesAndInactivity(t *testing.T) {
	t.Parallel()

	var delivered plumtreeDeliveredCache
	delivered.values = make(map[[sha256.Size]byte]struct{}, plumtreeDeliveredLimit)
	for i := 0; i < plumtreeDeliveredLimit; i++ {
		delivered.add(plumtreeEngineTestID(uint32(i + 1)))
	}
	first := plumtreeEngineTestID(1)
	second := plumtreeEngineTestID(2)
	delivered.add(first)
	delivered.add(plumtreeEngineTestID(plumtreeDeliveredLimit + 1))
	if delivered.contains(first) {
		t.Fatal("duplicate refreshed delivered FIFO position")
	}
	if !delivered.contains(second) || delivered.size != plumtreeDeliveredLimit {
		t.Fatal("delivered FIFO evicted the wrong entry")
	}

	now := time.Unix(1_800_000_300, 0)
	peer := plumtreeEngineTestPeer(61)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{peer: true},
	}
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(60),
		false,
		peers,
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	slot := &engine.slots[0]
	engine.promoteEagerLocked(slot, peer, true, now.Add(-plumtreeEagerInactivityTTL))
	activity := engine.activities[peer]
	activity.sentSinceActive = plumtreeMaxSentWithoutActivity
	engine.activities[peer] = activity
	verdicts := engine.slotReceiveVerdictsLocked(slot)
	engine.removeInactiveEagerLocked(slot, now, &verdicts)
	if !slot.hasEager(peer) {
		t.Fatal("peer was removed at exactly 50 sends")
	}
	activity = engine.activities[peer]
	activity.sentSinceActive++
	engine.activities[peer] = activity
	engine.removeInactiveEagerLocked(slot, now, &verdicts)
	if slot.hasEager(peer) {
		t.Fatal("peer remained eager after more than 50 inactive sends")
	}

	slot.reservePending(peer, now.Add(-plumtreePendingFeedbackTTL))
	slot.expirePending(now)
	if slot.pendingCount != 0 {
		t.Fatal("pending feedback survived at the 5 second boundary")
	}
	expiredID := plumtreeEngineTestID(63)
	if !engine.budget.reserve(1, 0) {
		engine.mu.Unlock()
		t.Fatal("reserve expired simple state")
	}
	engine.addSimpleStateLocked(&plumtreeSimpleState{
		broadcastID: expiredID,
		firstSeen:   now.Add(-plumtreeBroadcastLifetime),
	})
	engine.mu.Unlock()

	engine.Alarm(now)
	answer, err := engine.HandleRepairQuery(now, peer, RepairPlumtreePart{
		BroadcastID: expiredID[:],
		Timestamp:   plumtreeUnixSeconds(now),
		PartIndex:   0,
		TreeIndex:   0,
	})
	if err != nil {
		t.Fatalf("repair expired broadcast: %v", err)
	}
	var notFound BroadcastNotFound
	if err = parsePlumtreeMessage(&notFound, answer); err != nil {
		t.Fatalf("expired repair answer is not a not-found: %v", err)
	}
}

func TestPlumtreeEngineNextAlarm(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_325, 0)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(64),
		false,
		&plumtreeEngineTestPeers{},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	if _, ok := engine.NextAlarm(); ok {
		t.Fatal("empty engine has an alarm")
	}

	pendingAt := now
	simple := &plumtreeSimpleState{
		broadcastID: plumtreeEngineTestID(641),
		firstSeen:   now.Add(-14 * time.Second),
	}
	fec := &plumtreeFECState{
		broadcastID: plumtreeEngineTestID(642),
		firstSeen:   now.Add(-17 * time.Second),
	}
	missing := &plumtreeMissingPart{
		key: plumtreePartKey{
			broadcastID: plumtreeEngineTestID(643),
		},
		repairAt: now.Add(2 * time.Second),
	}

	engine.mu.Lock()
	engine.slots[plumtreeTreeSlots-1].reservePending(
		plumtreeEngineTestPeer(65),
		pendingAt,
	)
	if !engine.budget.reserve(2, 0) {
		engine.mu.Unlock()
		t.Fatal("reserve alarm states")
	}
	engine.addSimpleStateLocked(simple)
	engine.addFECStateLocked(fec)
	engine.addMissingLocked(missing)
	engine.mu.Unlock()

	plumtreeEngineTestRequireNextAlarm(t, engine, missing.repairAt)

	engine.mu.Lock()
	engine.eraseMissingLocked(missing.key)
	engine.mu.Unlock()
	plumtreeEngineTestRequireNextAlarm(
		t,
		engine,
		fec.firstSeen.Add(plumtreeBroadcastLifetime),
	)

	engine.mu.Lock()
	engine.removeFECStateLocked(fec)
	engine.mu.Unlock()
	plumtreeEngineTestRequireNextAlarm(
		t,
		engine,
		pendingAt.Add(plumtreePendingFeedbackTTL),
	)

	engine.mu.Lock()
	engine.slots[plumtreeTreeSlots-1].removePending(plumtreeEngineTestPeer(65))
	engine.mu.Unlock()
	plumtreeEngineTestRequireNextAlarm(
		t,
		engine,
		simple.firstSeen.Add(plumtreeBroadcastLifetime),
	)

	engine.mu.Lock()
	engine.removeSimpleStateLocked(simple)
	engine.mu.Unlock()
	if _, ok := engine.NextAlarm(); ok {
		t.Fatal("cleared engine still has an alarm")
	}
}

func TestPlumtreeEngineMissingPartFIFOIsBounded(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_350, 0)
	local := plumtreeEngineTestPeer(65)
	from := plumtreeEngineTestPeer(66)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{from: true},
	}
	engine := plumtreeEngineTestNew(
		t,
		local,
		false,
		peers,
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[0], from, true, now)
	engine.mu.Unlock()

	source := plumtreeEngineTestSource(0x65)
	dataHash := sha256.Sum256([]byte{1})
	for i := uint32(1); i <= plumtreeMissingLimit+1; i++ {
		id := plumtreeEngineTestID(i)
		actions, err := engine.HandleIHave(
			t.Context(),
			now,
			from,
			BroadcastPlumtreeIHave{
				BroadcastID:      id[:],
				Timestamp:        plumtreeUnixSeconds(now),
				PartIndex:        0,
				TreeIndex:        0,
				Source:           source,
				Certificate:      overlay.CertificateEmpty{},
				PayloadTimestamp: plumtreeUnixSeconds(now),
				DataSize:         1,
				DataHash:         dataHash[:],
				Signature:        []byte{1},
			},
		)
		if err != nil {
			t.Fatalf("handle IHAVE %d: %v", i, err)
		}
		if len(actions.Repairs) != 0 {
			t.Fatalf("IHAVE %d started an immediate repair", i)
		}
	}

	firstKey := plumtreePartKey{
		broadcastID: plumtreeEngineTestID(1),
		partIndex:   0,
		treeIndex:   0,
	}
	secondKey := plumtreePartKey{
		broadcastID: plumtreeEngineTestID(2),
		partIndex:   0,
		treeIndex:   0,
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.missing) != plumtreeMissingLimit {
		t.Fatalf("missing parts = %d, want %d", len(engine.missing), plumtreeMissingLimit)
	}
	if engine.missing[firstKey] != nil || engine.missing[secondKey] == nil {
		t.Fatal("missing part FIFO evicted the wrong entry")
	}
}

func TestPlumtreeEngineConcurrentVerificationRechecksDuplicate(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_400, 0)
	local := plumtreeEngineTestPeer(71)
	from := plumtreeEngineTestPeer(72)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{from: true},
	}
	release := make(chan struct{})
	verifier := &plumtreeEngineTestVerifier{
		entered: make(chan struct{}, 2),
		release: release,
	}
	engine := plumtreeEngineTestNew(t, local, false, peers, verifier, func() time.Time {
		return now
	})
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[0], from, true, now)
	engine.mu.Unlock()

	message := plumtreeEngineTestSimple(
		now,
		plumtreeEngineTestID(73),
		plumtreeEngineTestSource(0x64),
		[]byte("concurrent"),
	)
	envelope := plumtreeEngineTestSimpleEnvelope(t, message, 0xb1)

	type result struct {
		actions plumtreeActions
		err     error
	}
	results := make(chan result, 2)
	var calls sync.WaitGroup
	calls.Add(2)
	for range 2 {
		go func() {
			defer calls.Done()
			actions, err := engine.HandleSimple(t.Context(), now, from, envelope)
			results <- result{actions: actions, err: err}
		}()
	}
	<-verifier.entered
	close(release)
	calls.Wait()
	close(results)

	deliveries := 0
	failures := 0
	for result := range results {
		deliveries += len(result.actions.Deliveries)
		if result.err != nil {
			failures++
		}
	}
	if deliveries != 1 || failures != 1 {
		t.Fatalf("deliveries/failures = %d/%d, want 1/1", deliveries, failures)
	}
}

func TestPlumtreeEngineConcurrentFECDecoderCreatedAtCommit(t *testing.T) {
	const (
		concurrentParts = 8
		fullDataSize    = int32(4 << 20)
	)

	now := time.Unix(1_800_000_450, 0)
	local := plumtreeEngineTestPeer(74)
	from := plumtreeEngineTestPeer(75)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{from: true},
	}
	release := make(chan struct{})
	verifier := &plumtreeEngineTestVerifier{
		entered: make(chan struct{}, concurrentParts),
		release: release,
		exited:  make(chan struct{}, concurrentParts),
	}
	engine := plumtreeEngineTestNew(t, local, false, peers, verifier, func() time.Time {
		return now
	})
	source := plumtreeEngineTestSource(0x65)
	fullHash := sha256.Sum256([]byte("concurrent FEC decoder allocation"))
	partSize := plumtreeFECPartSize(fullDataSize)
	partData := make([]byte, partSize)

	engine.mu.Lock()
	for partIndex := int32(0); partIndex < concurrentParts; partIndex++ {
		engine.promoteEagerLocked(
			&engine.slots[partIndex+plumtreeFECTreeOffset],
			from,
			true,
			now,
		)
	}
	engine.mu.Unlock()

	results := make(chan error, concurrentParts)
	for partIndex := int32(0); partIndex < concurrentParts; partIndex++ {
		message := BroadcastPlumtreeFEC{
			Timestamp:    plumtreeUnixSeconds(now),
			Source:       source,
			Certificate:  overlay.CertificateEmpty{},
			FullDataHash: fullHash[:],
			FullDataSize: fullDataSize,
			PartIndex:    partIndex,
			TreeIndex:    partIndex + plumtreeFECTreeOffset,
			Data:         partData,
			Signature:    []byte{0x92},
		}
		envelope := plumtreeFECEnvelope{
			Message: message,
			Wire: plumtreePayloadWire{
				Message: []byte{byte(partIndex)},
				Body:    []byte{byte(partIndex)},
			},
		}
		go func() {
			_, err := engine.HandleFEC(t.Context(), now, from, envelope)
			results <- err
		}()
	}

	for range concurrentParts {
		<-verifier.entered
	}

	engine.mu.Lock()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	close(release)
	for range concurrentParts {
		<-verifier.exited
	}
	plumtreeEngineTestWaitForFECCommitLock(t, concurrentParts)

	runtime.GC()
	var blocked runtime.MemStats
	runtime.ReadMemStats(&blocked)
	engine.mu.Unlock()

	for range concurrentParts {
		if err := <-results; err != nil {
			t.Fatalf("handle concurrent FEC part: %v", err)
		}
	}

	var heapGrowth uint64
	if blocked.HeapAlloc > before.HeapAlloc {
		heapGrowth = blocked.HeapAlloc - before.HeapAlloc
	}
	if heapGrowth > uint64(fullDataSize)*2 {
		t.Fatalf(
			"heap grew by %d bytes before FEC commit; decoder was allocated before the locked state recheck",
			heapGrowth,
		)
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()

	if len(engine.fecStates) != 1 {
		t.Fatalf("FEC states = %d, want 1", len(engine.fecStates))
	}
	for _, state := range engine.fecStates {
		if state.decoder == nil {
			t.Fatal("committed FEC state has no decoder")
		}
		if state.partCount != concurrentParts {
			t.Fatalf(
				"committed FEC parts = %d, want %d",
				state.partCount,
				concurrentParts,
			)
		}
	}
}

func TestPlumtreeEngineRechecksTimestampAfterVerification(t *testing.T) {
	t.Parallel()

	receivedAt := time.Unix(1_800_000_500, 0)
	commitAt := receivedAt
	local := plumtreeEngineTestPeer(81)
	from := plumtreeEngineTestPeer(82)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{from: true},
	}
	release := make(chan struct{})
	verifier := &plumtreeEngineTestVerifier{
		entered: make(chan struct{}, 1),
		release: release,
	}
	engine := plumtreeEngineTestNew(
		t,
		local,
		false,
		peers,
		verifier,
		func() time.Time { return commitAt },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(&engine.slots[plumtreeSimpleTree], from, true, receivedAt)
	engine.mu.Unlock()
	message := plumtreeEngineTestSimple(
		receivedAt,
		plumtreeEngineTestID(83),
		plumtreeEngineTestSource(0x66),
		[]byte("expires during verification"),
	)
	envelope := plumtreeEngineTestSimpleEnvelope(t, message, 0xc1)

	result := make(chan error, 1)
	go func() {
		_, err := engine.HandleSimple(
			t.Context(),
			receivedAt,
			from,
			envelope,
		)
		result <- err
	}()

	select {
	case <-verifier.entered:
	case err := <-result:
		t.Fatalf("message failed before verification: %v", err)
	}
	commitAt = receivedAt.Add(plumtreeBroadcastLifetime + time.Second)
	close(release)

	err := <-result
	if err == nil || !strings.Contains(err.Error(), "too old Plumtree timestamp") {
		t.Fatalf("verification completion error = %v", err)
	}
}

func BenchmarkPlumtreeDeliveredCacheExactCount(b *testing.B) {
	cache := plumtreeDeliveredCache{
		values: make(map[[sha256.Size]byte]struct{}, plumtreeDeliveredLimit),
	}
	var value uint32

	b.ReportAllocs()
	for b.Loop() {
		value++
		cache.add(plumtreeEngineTestID(value))
	}
}

func BenchmarkPlumtreeForwardPayloadActions(b *testing.B) {
	engine, active := plumtreeForwardBenchmarkEngine()
	from := plumtreeEngineTestPeer(0xff)
	broadcastID := plumtreeEngineTestID(1)
	part := plumtreePartState{
		treeIndex: 0,
		dataSize:  1,
	}
	now := time.Unix(1_000_000, 0)

	b.ReportAllocs()
	for b.Loop() {
		engine.slots[0] = plumtreeSlot{}
		part.fullSentCount = 0
		part.advertisedCount = 0
		actions := plumtreeActions{}

		verdicts := engine.slotReceiveVerdictsLocked(&engine.slots[0])
		engine.forwardPayloadToActiveLocked(
			now,
			from,
			broadcastID,
			&part,
			active,
			&verdicts,
			&actions,
		)
		if len(actions.Outbounds) != 1+plumtreeActiveNeighbourLimit {
			b.Fatalf("outbounds = %d", len(actions.Outbounds))
		}
	}
}

func BenchmarkPlumtreeForwardAllFECActions(b *testing.B) {
	engine, active := plumtreeForwardBenchmarkEngine()
	broadcastID := plumtreeEngineTestID(1)
	now := time.Unix(1_000_000, 0)
	var parts [plumtreeFECTotalParts]plumtreePartState
	for index := range parts {
		parts[index].treeIndex = int32(index) + plumtreeFECTreeOffset
		parts[index].dataSize = 1
	}

	b.ReportAllocs()
	for b.Loop() {
		actions := plumtreeActions{
			Outbounds: make(
				[]plumtreeOutboundAction,
				0,
				int(plumtreeFECTotalParts)*len(active),
			),
		}
		verdicts := engine.fecEagerReceiveVerdictsLocked()
		for index := range parts {
			parts[index].advertisedCount = 0
			engine.forwardPayloadToActiveLocked(
				now,
				PeerID{},
				broadcastID,
				&parts[index],
				active,
				&verdicts,
				&actions,
			)
		}
		if len(actions.Outbounds) !=
			int(plumtreeFECTotalParts)*plumtreeActiveNeighbourLimit {
			b.Fatalf("outbounds = %d", len(actions.Outbounds))
		}
	}
}

func plumtreeForwardBenchmarkEngine() (*plumtreeEngine, []PeerID) {
	active := make([]PeerID, plumtreeActiveNeighbourLimit)
	receives := make(map[PeerID]bool, len(active))
	for index := range active {
		peer := plumtreeEngineTestPeer(byte(index + 1))
		active[index] = peer
		receives[peer] = true
	}

	engine := &plumtreeEngine{
		localID:         plumtreeEngineTestPeer(0xfe),
		peers:           &plumtreeEngineTestPeers{receives: receives},
		localEagerLimit: plumtreeRegularEagerLimit,
	}
	return engine, active
}

func plumtreeEngineTestNew(
	t *testing.T,
	local PeerID,
	isOriginal bool,
	peers plumtreePeerSet,
	verifier plumtreeVerifier,
	now func() time.Time,
) *plumtreeEngine {
	t.Helper()

	engine, err := newPlumtreeEngine(plumtreeEngineConfig{
		LocalID:          local,
		IsOriginalSender: isOriginal,
		Now:              now,
	}, &plumtreeMemoryBudget{}, peers, verifier, &plumtreeStats{})
	if err != nil {
		t.Fatalf("create Plumtree engine: %v", err)
	}
	return engine
}

func plumtreeEngineTestPeer(value byte) PeerID {
	var peer PeerID
	peer[0] = value
	return peer
}

func plumtreeEngineTestID(value uint32) [sha256.Size]byte {
	var id [sha256.Size]byte
	binary.LittleEndian.PutUint32(id[:], value)
	return id
}

func plumtreeEngineTestSource(value byte) keys.PublicKeyED25519 {
	return keys.PublicKeyED25519{
		Key: ed25519.PublicKey(bytes.Repeat([]byte{value}, ed25519.PublicKeySize)),
	}
}

func plumtreeEngineTestSimple(
	now time.Time,
	id [sha256.Size]byte,
	source keys.PublicKeyED25519,
	data []byte,
) BroadcastPlumtreeSimple {
	return BroadcastPlumtreeSimple{
		Timestamp:   plumtreeUnixSeconds(now),
		Source:      source,
		Certificate: overlay.CertificateEmpty{},
		BroadcastID: id[:],
		TreeIndex:   0,
		Data:        data,
		Signature:   []byte{0x81},
	}
}

func plumtreeEngineTestIHaveSimple(
	now time.Time,
	message BroadcastPlumtreeSimple,
) BroadcastPlumtreeIHave {
	dataHash := sha256.Sum256(message.Data)
	return BroadcastPlumtreeIHave{
		BroadcastID:      message.BroadcastID,
		Timestamp:        plumtreeUnixSeconds(now),
		PartIndex:        0,
		TreeIndex:        0,
		Source:           message.Source,
		Certificate:      message.Certificate,
		PayloadTimestamp: message.Timestamp,
		DataSize:         int32(len(message.Data)),
		DataHash:         dataHash[:],
		Signature:        message.Signature,
	}
}

func plumtreeEngineTestSimpleEnvelope(
	t *testing.T,
	message BroadcastPlumtreeSimple,
	prefix byte,
) plumtreeSimpleEnvelope {
	t.Helper()

	body, err := tl.Serialize(message, true)
	if err != nil {
		t.Fatalf("serialize simple payload: %v", err)
	}
	wire := make([]byte, len(body)+1)
	wire[0] = prefix
	copy(wire[1:], body)
	var decoded BroadcastPlumtreeSimple
	rest, err := tl.ParseNoCopy(&decoded, wire[1:], true)
	if err != nil || len(rest) != 0 {
		t.Fatalf(
			"parse retained simple payload: rest=%d err=%v",
			len(rest),
			err,
		)
	}
	return plumtreeSimpleEnvelope{
		Message: decoded,
		Wire: plumtreePayloadWire{
			Message: wire,
			Body:    wire[1:],
		},
	}
}

func plumtreeEngineTestFECEnvelope(
	t *testing.T,
	message BroadcastPlumtreeFEC,
	prefix byte,
) plumtreeFECEnvelope {
	t.Helper()

	body, err := tl.Serialize(message, true)
	if err != nil {
		t.Fatalf("serialize FEC payload: %v", err)
	}
	wire := make([]byte, len(body)+1)
	wire[0] = prefix
	copy(wire[1:], body)
	var decoded BroadcastPlumtreeFEC
	rest, err := tl.ParseNoCopy(&decoded, wire[1:], true)
	if err != nil || len(rest) != 0 {
		t.Fatalf(
			"parse retained FEC payload: rest=%d err=%v",
			len(rest),
			err,
		)
	}
	return plumtreeFECEnvelope{
		Message: decoded,
		Wire: plumtreePayloadWire{
			Message: wire,
			Body:    wire[1:],
		},
	}
}

func plumtreeEngineTestRequireNextAlarm(
	t *testing.T,
	engine *plumtreeEngine,
	want time.Time,
) {
	t.Helper()

	got, ok := engine.NextAlarm()
	if !ok {
		t.Fatalf("NextAlarm() has no deadline, want %v", want)
	}
	if !got.Equal(want) {
		t.Fatalf("NextAlarm() = %v, want %v", got, want)
	}
}

func plumtreeEngineTestWaitForFECCommitLock(t *testing.T, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		size := runtime.Stack(stack, true)
		blocked := 0
		for _, goroutine := range bytes.Split(stack[:size], []byte("\n\n")) {
			if bytes.Contains(
				goroutine,
				[]byte("(*plumtreeEngine).handleFEC"),
			) && bytes.Contains(goroutine, []byte("sync.(*Mutex).Lock")) {
				blocked++
			}
		}
		if blocked == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("%d concurrent FEC handlers did not reach the commit lock", want)
}
