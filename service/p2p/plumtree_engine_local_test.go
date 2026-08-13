package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/xssnick/raptorq"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type plumtreeLocalTestPeers struct {
	active      []PeerID
	receives    map[PeerID]bool
	activeCalls int
}

func (p *plumtreeLocalTestPeers) PlumtreeActivePeers(limit int) []PeerID {
	p.activeCalls++
	if len(p.active) > limit {
		return p.active[:limit]
	}
	return p.active
}

func (p *plumtreeLocalTestPeers) PlumtreePeerReceivesBroadcasts(
	peer PeerID,
) bool {
	return p.receives[peer]
}

func (p *plumtreeLocalTestPeers) PlumtreePeersReceiveBroadcasts(
	peers []PeerID,
	out []bool,
) {
	for i, peer := range peers {
		out[i] = p.receives[peer]
	}
}

func TestPlumtreeEngineOriginateSimpleSignsRetainsAndFansOut(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_900_000_000, 125_000_000)
	overlayID := bytes.Repeat([]byte{0x41}, sha256.Size)
	origin := plumtreeLocalTestOrigin(t, 0x11, overlayID)
	eager := plumtreeEngineTestPeer(0x21)
	lazyOne := plumtreeEngineTestPeer(0x22)
	lazyTwo := plumtreeEngineTestPeer(0x23)
	peers := &plumtreeLocalTestPeers{
		active: []PeerID{eager, lazyOne, lazyTwo},
		receives: map[PeerID]bool{
			eager:   true,
			lazyOne: true,
			lazyTwo: true,
		},
	}
	engine := plumtreeEngineTestNew(
		t,
		origin.sourceID,
		true,
		peers,
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(
		&engine.slots[plumtreeSimpleTree],
		eager,
		true,
		now,
	)
	engine.mu.Unlock()

	broadcastID := plumtreeEngineTestID(0x31415926)
	data := []byte("already serialized inner TON finality broadcast")
	expectedData := bytes.Clone(data)
	actions, err := engine.OriginateSimple(
		origin,
		0x10203040,
		broadcastID,
		data,
	)
	if err != nil {
		t.Fatalf("originate simple: %v", err)
	}
	if len(actions.Deliveries) != 0 || len(actions.Candidates) != 0 {
		t.Fatalf("unexpected local simple actions: %+v", actions)
	}
	if len(actions.Outbounds) != 1 {
		t.Fatalf("simple outbounds = %d, want one eager payload", len(actions.Outbounds))
	}

	full := actions.Outbounds[0]
	if full.Kind != plumtreeOutboundPayload || full.To != eager {
		t.Fatalf("first simple outbound = %+v, want eager payload", full)
	}
	if peers.activeCalls != 1 {
		t.Fatalf("active peer snapshots = %d, want 1", peers.activeCalls)
	}

	message := plumtreeLocalTestSimpleMessage(t, full.Wire, overlayID)
	if message.Flags != 0x10203040 ||
		message.Timestamp != plumtreeUnixSeconds(now) ||
		!bytes.Equal(message.BroadcastID, broadcastID[:]) ||
		message.TreeIndex != plumtreeSimpleTree ||
		!bytes.Equal(message.Data, expectedData) {
		t.Fatalf("unexpected simple payload: %+v", message)
	}
	source, ok := message.Source.(keys.PublicKeyED25519)
	if !ok || !bytes.Equal(source.Key, origin.source.Key) {
		t.Fatalf("simple source = %T %+v", message.Source, message.Source)
	}
	if _, ok = message.Certificate.(overlay.CertificateEmpty); !ok {
		t.Fatalf("simple certificate = %T, want empty value", message.Certificate)
	}

	dataHash := sha256.Sum256(expectedData)
	signedData := serializeValidatedPlumtreeSimpleToSign(
		BroadcastPlumtreeSimpleToSign{
			ID:        broadcastID[:],
			Timestamp: message.Timestamp,
			TreeIndex: message.TreeIndex,
			DataSize:  int32(len(expectedData)),
			DataHash:  dataHash[:],
		},
	)
	if !ed25519.Verify(origin.source.Key, signedData, message.Signature) {
		t.Fatal("simple payload signature is invalid")
	}
	plumtreeLocalTestRequireWireHash(
		t,
		full.Wire,
		"edf421c994b0653a36f3c5aefd4060683f8dc68ea168c73c7fce3acaad1ade2b",
	)

	if state := engine.simpleStates[broadcastID]; state == nil ||
		!engine.delivered.contains(broadcastID) {
		t.Fatal("local eager simple broadcast was not retained as delivered")
	}

	coldPeers := &plumtreeLocalTestPeers{
		active: []PeerID{lazyOne, lazyTwo},
		receives: map[PeerID]bool{
			lazyOne: true,
			lazyTwo: true,
		},
	}
	coldEngine := plumtreeEngineTestNew(
		t,
		origin.sourceID,
		true,
		coldPeers,
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	coldID := plumtreeEngineTestID(0x31415927)
	coldData := bytes.Clone(expectedData)
	coldActions, err := coldEngine.OriginateSimple(
		origin,
		0x10203040,
		coldID,
		coldData,
	)
	if err != nil {
		t.Fatalf("originate cold simple: %v", err)
	}
	if len(coldActions.Outbounds) != 2 {
		t.Fatalf("cold simple outbounds = %d, want two IHAVE", len(coldActions.Outbounds))
	}
	for i, peer := range []PeerID{lazyOne, lazyTwo} {
		action := coldActions.Outbounds[i]
		if action.Kind != plumtreeOutboundIHave || action.To != peer {
			t.Fatalf("cold simple outbound %d = %+v, want IHAVE to %v", i, action, peer)
		}
	}
	if coldPeers.activeCalls != 1 {
		t.Fatalf("cold active peer snapshots = %d, want 1", coldPeers.activeCalls)
	}

	coldData[0] ^= 0xff
	answer, err := coldEngine.HandleRepairQuery(now, lazyOne, RepairPlumtreePart{
		BroadcastID: coldID[:],
		Timestamp:   plumtreeUnixSeconds(now),
		PartIndex:   0,
		TreeIndex:   plumtreeSimpleTree,
	})
	if err != nil {
		t.Fatalf("answer local simple repair: %v", err)
	}
	state := coldEngine.simpleStates[coldID]
	if state == nil || !coldEngine.delivered.contains(coldID) {
		t.Fatal("local simple broadcast was not retained as delivered")
	}
	if len(answer) == 0 || &answer[0] != &state.part.wire.Body[0] {
		t.Fatal("simple repair did not reuse retained wire body")
	}
	var repairedSimple BroadcastPlumtreeSimple
	if err = parsePlumtreeMessage(&repairedSimple, answer); err != nil {
		t.Fatalf("parse simple repair: %v", err)
	}
	if !bytes.Equal(repairedSimple.Data, expectedData) {
		t.Fatalf("unexpected repaired simple payload: %+v", repairedSimple)
	}

	if _, err = engine.OriginateSimple(
		origin,
		0x10203040,
		broadcastID,
		expectedData,
	); err == nil {
		t.Fatal("duplicate local simple broadcast was accepted")
	}
}

func TestPlumtreeEngineOriginateFECMatchesRaptorQAndRepairWire(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_900_000_100, 0)
	now := start
	overlayID := bytes.Repeat([]byte{0x42}, sha256.Size)
	origin := plumtreeLocalTestOrigin(t, 0x12, overlayID)
	firstPeer := plumtreeEngineTestPeer(0x31)
	secondPeer := plumtreeEngineTestPeer(0x32)
	peers := &plumtreeLocalTestPeers{
		active: []PeerID{firstPeer, secondPeer},
		receives: map[PeerID]bool{
			firstPeer:  true,
			secondPeer: true,
		},
	}
	engine := plumtreeEngineTestNew(
		t,
		origin.sourceID,
		true,
		peers,
		&plumtreeEngineTestVerifier{},
		func() time.Time {
			result := now
			now = now.Add(time.Millisecond)
			return result
		},
	)

	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i)
	}
	expectedData := bytes.Clone(data)
	const flags = int32(0x11223344)
	actions, err := engine.OriginateFEC(origin, flags, data)
	if err != nil {
		t.Fatalf("originate FEC: %v", err)
	}
	if len(actions.Deliveries) != 0 || len(actions.Candidates) != 0 {
		t.Fatalf("unexpected local FEC actions: %+v", actions)
	}
	if len(actions.Outbounds) != int(plumtreeFECTotalParts)*len(peers.active) {
		t.Fatalf(
			"FEC outbounds = %d, want %d IHAVE",
			len(actions.Outbounds),
			int(plumtreeFECTotalParts)*len(peers.active),
		)
	}
	for _, action := range actions.Outbounds {
		if action.Kind != plumtreeOutboundIHave || action.To.IsZero() {
			t.Fatalf("unexpected local FEC outbound: %+v", action)
		}
	}
	if peers.activeCalls != 1 {
		t.Fatalf("active peer snapshots = %d, want 1", peers.activeCalls)
	}

	fullDataHash := sha256.Sum256(expectedData)
	partSize := plumtreeFECPartSize(int32(len(expectedData)))
	broadcastID := computeLocalPlumtreeFECID(
		flags,
		fullDataHash,
		int32(len(expectedData)),
		partSize,
	)
	state := engine.fecStates[broadcastID]
	if state == nil ||
		!state.isDelivered ||
		state.partCount != uint8(plumtreeFECTotalParts) ||
		!engine.delivered.contains(broadcastID) {
		t.Fatalf("unexpected local FEC state: %+v", state)
	}
	var retainedWireBytes uint64
	for partIndex := int32(0); partIndex < plumtreeFECTotalParts; partIndex++ {
		retainedWireBytes += uint64(len(state.parts[partIndex].wire.Message))
	}
	if state.decoderEstimatedBytes != 0 {
		t.Fatalf(
			"local FEC decoder estimate = %d, want 0",
			state.decoderEstimatedBytes,
		)
	}
	plumtreeMemoryBudgetRequireCounters(
		t,
		engine.budget,
		1,
		retainedWireBytes,
	)

	raptor := raptorq.NewRaptorQ(uint32(partSize))
	decoder, err := raptor.CreateDecoder(uint32(len(expectedData)))
	if err != nil {
		t.Fatalf("create FEC decoder: %v", err)
	}
	for partIndex := int32(0); partIndex < plumtreeFECDataParts; partIndex++ {
		part := state.parts[partIndex]
		if part == nil {
			t.Fatalf("missing retained FEC part %d", partIndex)
		}
		message := plumtreeLocalTestFECMessage(t, part.wire.Message, overlayID)
		if message.Flags != flags ||
			message.FullDataSize != int32(len(expectedData)) ||
			message.PartIndex != partIndex ||
			message.TreeIndex != partIndex+plumtreeFECTreeOffset ||
			!bytes.Equal(message.FullDataHash, fullDataHash[:]) ||
			len(message.Data) != int(partSize) {
			t.Fatalf("unexpected FEC part %d: %+v", partIndex, message)
		}
		source, ok := message.Source.(keys.PublicKeyED25519)
		if !ok || !bytes.Equal(source.Key, origin.source.Key) {
			t.Fatalf("FEC source %d = %T %+v", partIndex, message.Source, message.Source)
		}
		if _, ok = message.Certificate.(overlay.CertificateEmpty); !ok {
			t.Fatalf("FEC certificate %d = %T", partIndex, message.Certificate)
		}

		partHash := sha256.Sum256(message.Data)
		signedData := serializeValidatedPlumtreeFECToSign(
			BroadcastPlumtreeFECToSign{
				ID:        broadcastID[:],
				Timestamp: message.Timestamp,
				PartIndex: partIndex,
				TreeIndex: message.TreeIndex,
				DataSize:  int32(len(message.Data)),
				DataHash:  partHash[:],
			},
		)
		if !ed25519.Verify(origin.source.Key, signedData, message.Signature) {
			t.Fatalf("FEC signature %d is invalid", partIndex)
		}
		if _, err = decoder.AddSymbol(uint32(partIndex), message.Data); err != nil {
			t.Fatalf("add FEC symbol %d: %v", partIndex, err)
		}
	}

	decoded := make([]byte, len(expectedData))
	ok, err := decoder.DecodeInto(decoded)
	if err != nil {
		t.Fatalf("decode FEC symbols: %v", err)
	}
	if !ok || !bytes.Equal(decoded, expectedData) {
		t.Fatal("serialized local FEC symbols do not reconstruct the payload")
	}
	plumtreeLocalTestRequireWireHash(
		t,
		state.parts[0].wire.Message,
		"39f9d7ffc0de45ee64b0f98021e52d2864aab6ffcb1821c5169296553e7d8bc4",
	)

	data[0] ^= 0xff
	answer, err := engine.HandleRepairQuery(start, firstPeer, RepairPlumtreePart{
		BroadcastID: broadcastID[:],
		Timestamp:   plumtreeUnixSeconds(start),
		PartIndex:   0,
		TreeIndex:   plumtreeFECTreeOffset,
	})
	if err != nil {
		t.Fatalf("answer local FEC repair: %v", err)
	}
	if len(answer) == 0 || &answer[0] != &state.parts[0].wire.Body[0] {
		t.Fatal("FEC repair did not reuse retained wire body")
	}
	var repairedFEC BroadcastPlumtreeFEC
	if err = parsePlumtreeMessage(&repairedFEC, answer); err != nil {
		t.Fatalf("parse FEC repair: %v", err)
	}
	if repairedFEC.Data[0] != expectedData[0] {
		t.Fatalf("unexpected repaired FEC payload: %+v", repairedFEC)
	}
}

func TestPlumtreeLocalOriginRejectsInvalidBoundaries(t *testing.T) {
	t.Parallel()

	overlayID := bytes.Repeat([]byte{0x43}, sha256.Size)
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat(
		[]byte{0x13},
		ed25519.SeedSize,
	))
	envelope, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		t.Fatalf("create local envelope: %v", err)
	}
	if _, err := newPlumtreeLocalOrigin(
		privateKey[:ed25519.PrivateKeySize-1],
		envelope,
	); err == nil {
		t.Fatal("short local private key was accepted")
	}

	inconsistent := bytes.Clone(privateKey)
	inconsistent[len(inconsistent)-1] ^= 0xff
	if _, err := newPlumtreeLocalOrigin(inconsistent, envelope); err == nil {
		t.Fatal("inconsistent local private key was accepted")
	}
	if _, err := newQUICOverlayEnvelope(overlayID[:31], nil); err == nil {
		t.Fatal("short local overlay id was accepted")
	}

	origin := plumtreeLocalTestOrigin(t, 0x13, overlayID)
	nonOriginal := plumtreeEngineTestNew(
		t,
		origin.sourceID,
		false,
		&plumtreeLocalTestPeers{receives: make(map[PeerID]bool)},
		&plumtreeEngineTestVerifier{},
		time.Now,
	)
	if _, err := nonOriginal.OriginateSimple(
		origin,
		0,
		plumtreeEngineTestID(1),
		[]byte{1},
	); err == nil {
		t.Fatal("non-original Plumtree engine originated a payload")
	}

	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(0x44),
		true,
		&plumtreeLocalTestPeers{receives: make(map[PeerID]bool)},
		&plumtreeEngineTestVerifier{},
		time.Now,
	)
	if _, err := engine.OriginateSimple(
		origin,
		0,
		plumtreeEngineTestID(2),
		[]byte{1},
	); err != nil {
		t.Fatalf("per-broadcast signer with another ADNL identity was rejected: %v", err)
	}

	if _, err := engine.OriginateSimple(
		origin,
		0,
		[sha256.Size]byte{},
		[]byte{1},
	); err == nil {
		t.Fatal("empty local simple id was accepted")
	}
	if _, err := engine.OriginateSimple(
		origin,
		0,
		plumtreeEngineTestID(3),
		nil,
	); err == nil {
		t.Fatal("empty local simple payload was accepted")
	}
	if _, err := engine.OriginateFEC(origin, 0, nil); err == nil {
		t.Fatal("empty local FEC payload was accepted")
	}
}

func plumtreeLocalTestOrigin(
	t *testing.T,
	seed byte,
	overlayID []byte,
) plumtreeLocalOrigin {
	t.Helper()

	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat(
		[]byte{seed},
		ed25519.SeedSize,
	))
	envelope, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		t.Fatalf("create local Plumtree envelope: %v", err)
	}
	origin, err := newPlumtreeLocalOrigin(privateKey, envelope)
	if err != nil {
		t.Fatalf("create local Plumtree origin: %v", err)
	}
	return origin
}

func plumtreeLocalTestSimpleMessage(
	t *testing.T,
	wire []byte,
	overlayID []byte,
) BroadcastPlumtreeSimple {
	t.Helper()

	header, body, err := parseQUICMessageEnvelope(wire)
	if err != nil {
		t.Fatalf("parse local simple envelope: %v", err)
	}
	if !bytes.Equal(header.overlay, overlayID) ||
		header.certificateKind != quicMembershipCertificateOmitted {
		t.Fatalf("unexpected local simple envelope: %+v", header)
	}
	var message BroadcastPlumtreeSimple
	if err = parsePlumtreeMessage(&message, body); err != nil {
		t.Fatalf("parse local simple payload: %v", err)
	}
	return message
}

func plumtreeLocalTestFECMessage(
	t *testing.T,
	wire []byte,
	overlayID []byte,
) BroadcastPlumtreeFEC {
	t.Helper()

	header, body, err := parseQUICMessageEnvelope(wire)
	if err != nil {
		t.Fatalf("parse local FEC envelope: %v", err)
	}
	if !bytes.Equal(header.overlay, overlayID) ||
		header.certificateKind != quicMembershipCertificateOmitted {
		t.Fatalf("unexpected local FEC envelope: %+v", header)
	}
	var message BroadcastPlumtreeFEC
	if err = parsePlumtreeMessage(&message, body); err != nil {
		t.Fatalf("parse local FEC payload: %v", err)
	}
	return message
}

func plumtreeLocalTestRequireWireHash(
	t *testing.T,
	wire []byte,
	expected string,
) {
	t.Helper()

	sum := sha256.Sum256(wire)
	if got := hex.EncodeToString(sum[:]); got != expected {
		t.Fatalf("wire SHA256 = %s, want %s", got, expected)
	}
}
