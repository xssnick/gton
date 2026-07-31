package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
)

func TestNewOverlaySubscriptionCreatesPlumtreeOnlyForPublicOverlay(
	t *testing.T,
) {
	node := newTestNode(t)
	overlayID := testPeerID("public-plumtree-runtime")
	publicSub, err := node.newOverlaySubscription(overlaySpec{
		Name:      "public-plumtree",
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatalf("create public subscription: %v", err)
	}
	t.Cleanup(publicSub.close)
	if publicSub.plumtree == nil {
		t.Fatal("public subscription has no Plumtree runtime")
	}

	customID := testPeerID("custom-without-plumtree-runtime")
	customSub, err := node.newOverlaySubscription(overlaySpec{
		Name:         "custom-plumtree",
		Kind:         overlayKindCustomFixed,
		ShortID:      customID[:],
		FixedNodeIDs: map[PeerID]struct{}{},
	})
	if err != nil {
		t.Fatalf("create custom subscription: %v", err)
	}
	t.Cleanup(customSub.close)
	if customSub.plumtree != nil {
		t.Fatal("custom subscription unexpectedly has a Plumtree runtime")
	}
}

func TestPlumtreeQUICReachabilityDoesNotGrantOverlayMembership(t *testing.T) {
	t.Parallel()

	peerID := testPeerID("plumtree-quic-only-peer")
	node := &Node{
		quicPeers: map[PeerID]*authenticatedQUICPeer{
			peerID: {peer: &adnlquic.Peer{}, id: peerID},
		},
	}
	sub := &overlaySubscription{
		node:  node,
		peers: map[PeerID]*overlayPeer{},
	}

	if sub.PlumtreePeerReceivesBroadcasts(peerID) {
		t.Fatal("node-global QUIC route granted overlay membership")
	}

	sub.peers[peerID] = &overlayPeer{id: peerID}
	if !sub.PlumtreePeerReceivesBroadcasts(peerID) {
		t.Fatal("active overlay roster peer was rejected")
	}
}

func TestPlumtreeRuntimeRetainsInboundWireWithoutPayloadCopy(t *testing.T) {
	node := newTestNode(t)
	overlayID := testPeerID("plumtree-runtime-wire")
	sub, err := node.newOverlaySubscription(overlaySpec{
		Name:      "plumtree-runtime-wire",
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	t.Cleanup(sub.close)

	sourcePrivate := ed25519.NewKeyFromSeed(bytes.Repeat(
		[]byte{0x51},
		ed25519.SeedSize,
	))
	sourcePublic := sourcePrivate.Public().(ed25519.PublicKey)
	sourceID, err := peerIDFromED25519PublicKey(sourcePublic)
	if err != nil {
		t.Fatalf("compute source id: %v", err)
	}
	node.SetPlumtreePolicy(NewPlumtreePolicy(2, 2, []PeerID{sourceID}))

	from := testPeerID("plumtree-runtime-source")
	sub.plumtree.engine.mu.Lock()
	sub.plumtree.engine.promoteEagerLocked(
		&sub.plumtree.engine.slots[plumtreeSimpleTree],
		from,
		true,
		time.Now(),
	)
	sub.plumtree.engine.mu.Unlock()

	data, err := tl.Serialize(overlay.Ping{}, true)
	if err != nil {
		t.Fatalf("serialize inner broadcast: %v", err)
	}
	now := time.Now()
	broadcastID := sha256.Sum256([]byte("plumtree-runtime-broadcast"))
	dataHash := sha256.Sum256(data)
	signedData := serializeValidatedPlumtreeSimpleToSign(
		BroadcastPlumtreeSimpleToSign{
			ID:        broadcastID[:],
			Timestamp: plumtreeUnixSeconds(now),
			TreeIndex: plumtreeSimpleTree,
			DataSize:  int32(len(data)),
			DataHash:  dataHash[:],
		},
	)
	message := BroadcastPlumtreeSimple{
		Timestamp:   plumtreeUnixSeconds(now),
		Source:      keys.PublicKeyED25519{Key: sourcePublic},
		Certificate: overlay.CertificateEmpty{},
		BroadcastID: broadcastID[:],
		TreeIndex:   plumtreeSimpleTree,
		Data:        data,
		Signature:   ed25519.Sign(sourcePrivate, signedData),
	}
	body, err := tl.Serialize(message, true)
	if err != nil {
		t.Fatalf("serialize Plumtree message: %v", err)
	}
	wire, retainedBody := sub.quicEnvelope.WrapMessageBody(body)
	var decoded BroadcastPlumtreeSimple
	rest, err := tl.ParseNoCopy(&decoded, retainedBody, true)
	if err != nil || len(rest) != 0 {
		t.Fatalf("parse retained message: rest=%d err=%v", len(rest), err)
	}

	if err = sub.plumtree.HandleMessage(
		context.Background(),
		from,
		wire,
		retainedBody,
	); err != nil {
		t.Fatalf("handle Plumtree message: %v", err)
	}

	sub.plumtree.engine.mu.Lock()
	state := sub.plumtree.engine.simpleStates[broadcastID]
	if state == nil {
		sub.plumtree.engine.mu.Unlock()
		t.Fatal("simple state was not retained")
	}
	retainedWire := state.part.wire.Message
	retainedData := state.data
	sub.plumtree.engine.mu.Unlock()

	if &retainedWire[0] != &wire[0] {
		t.Fatal("relay wire was copied")
	}
	if !bytes.Equal(retainedData, data) {
		t.Fatal("retained payload changed")
	}
	if &retainedData[0] != &decoded.Data[0] {
		t.Fatal("decoded payload was copied before retention")
	}

	sub.plumtree.stats.mu.Lock()
	delivered := sub.plumtree.stats.counters.deliveredBroadcast
	useful := sub.plumtree.stats.counters.usefulCount
	sub.plumtree.stats.mu.Unlock()
	if delivered != 1 || useful != 1 {
		t.Fatalf(
			"stats delivered/useful = %d/%d, want 1/1",
			delivered,
			useful,
		)
	}
}

func TestPlumtreeRuntimeRejectsMessagesWhilePolicyDisabled(t *testing.T) {
	node := newTestNode(t)
	overlayID := testPeerID("disabled-plumtree-runtime")
	sub, err := node.newOverlaySubscription(overlaySpec{
		Name:      "disabled-plumtree",
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	t.Cleanup(sub.close)

	body := make([]byte, 4)
	body[0] = byte(broadcastPlumtreeSimpleConstructorID)
	if err = sub.plumtree.HandleMessage(
		context.Background(),
		testPeerID("disabled-plumtree-peer"),
		body,
		body,
	); !errors.Is(err, errPlumtreeDisabled) {
		t.Fatalf("disabled Plumtree result = %v", err)
	}
}

func TestPlumtreeRuntimeDispatchGatesConstructors(t *testing.T) {
	node := newTestNode(t)
	overlayID := testPeerID("plumtree-dispatch-runtime")
	sub, err := node.newOverlaySubscription(overlaySpec{
		Name:      "plumtree-dispatch",
		Kind:      overlayKindPublicShard,
		Workchain: 0,
		ShortID:   overlayID[:],
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	t.Cleanup(sub.close)
	node.SetPlumtreePolicy(NewPlumtreePolicy(2, 2, nil))

	from := testPeerID("plumtree-dispatch-peer")
	pruneBytes, err := tl.Serialize(BroadcastPlumtreePrune{
		BroadcastID: bytes.Repeat([]byte{1}, sha256.Size),
		Timestamp:   plumtreeUnixSeconds(time.Now()),
		PartIndex:   0,
		TreeIndex:   1,
	}, true)
	if err != nil {
		t.Fatalf("serialize prune: %v", err)
	}
	notFoundBytes, err := tl.Serialize(BroadcastNotFound{}, true)
	if err != nil {
		t.Fatalf("serialize not found: %v", err)
	}

	ctx := context.Background()
	if _, err = sub.plumtree.processMessage(ctx, from, pruneBytes, pruneBytes); err != nil {
		t.Fatalf("process valid prune: %v", err)
	}
	trailing := append(append([]byte{}, pruneBytes...), 0)
	if _, err = sub.plumtree.processMessage(ctx, from, trailing, trailing); err == nil {
		t.Fatal("broadcast dispatch accepted trailing data")
	}
	unknown := []byte{0, 0, 0, 0}
	if _, err = sub.plumtree.processMessage(ctx, from, unknown, unknown); err == nil {
		t.Fatal("broadcast dispatch accepted an unknown constructor")
	}
	if _, err = sub.plumtree.processMessage(ctx, from, notFoundBytes, notFoundBytes); err == nil {
		t.Fatal("broadcast dispatch accepted a not-found answer")
	}
	if _, err = sub.plumtree.processMessage(ctx, from, nil, []byte{1, 2}); err == nil {
		t.Fatal("broadcast dispatch accepted a short body")
	}

	action := plumtreeRepairAction{ID: 1}
	actions, err := sub.plumtree.processRepairAnswer(ctx, action, notFoundBytes)
	if err != nil || len(actions.Outbounds) != 0 || len(actions.Deliveries) != 0 {
		t.Fatalf("not-found repair answer = %+v, %v", actions, err)
	}
	if _, err = sub.plumtree.processRepairAnswer(ctx, action, pruneBytes); err == nil {
		t.Fatal("repair dispatch accepted PRUNE")
	}
	if _, err = sub.plumtree.processRepairAnswer(ctx, action, []byte{1, 2}); err == nil {
		t.Fatal("repair dispatch accepted a short answer")
	}

	if _, err = sub.plumtree.HandleRepairQuery(
		ctx,
		from,
		RepairPlumtreePart{},
	); err == nil {
		t.Fatal("invalid repair query was accepted")
	}

	telemetry := sub.plumtree.stats.telemetry.snapshot()
	if got := telemetry.receivedMessages[plumtreeMessagePrune]; got != 1 {
		t.Fatalf("parsed PRUNE messages = %d, want 1", got)
	}
	if got := telemetry.receivedMessages[plumtreeMessageRepairQuery]; got != 1 {
		t.Fatalf("parsed repair queries = %d, want 1", got)
	}
	if got := telemetry.receivedMessages[plumtreeMessageSimple]; got != 0 {
		t.Fatalf("malformed simple messages counted = %d, want 0", got)
	}
}

func TestQUICOverlayEnvelopeWrapMessageBodyOwnsOneContiguousWire(t *testing.T) {
	overlayID := testPeerID("plumtree-envelope")
	body := []byte{1, 2, 3, 4, 5}

	envelope, err := newQUICOverlayEnvelope(overlayID[:], nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	wire, retainedBody := envelope.WrapMessageBody(body)
	if !bytes.Equal(retainedBody, body) {
		t.Fatalf("body = %x, want %x", retainedBody, body)
	}
	if &retainedBody[0] != &wire[len(wire)-len(body)] {
		t.Fatal("body is not a view into the final wire")
	}
}

func TestIsPlumtreeBroadcastRecognizesOnlyDirectMessageTypes(t *testing.T) {
	constructors := []uint32{
		broadcastPlumtreeSimpleConstructorID,
		broadcastPlumtreeFECConstructorID,
		broadcastPlumtreeIHaveConstructorID,
		broadcastPlumtreePruneConstructorID,
		broadcastPlumtreeUsefulConstructorID,
		plumtreeStatsPushConstructorID,
	}
	for _, constructor := range constructors {
		body := []byte{
			byte(constructor),
			byte(constructor >> 8),
			byte(constructor >> 16),
			byte(constructor >> 24),
		}
		if _, ok := plumtreeBroadcastConstructor(body); !ok {
			t.Fatalf("constructor 0x%08x was not recognized", constructor)
		}
	}

	notMessages := [][]byte{
		nil,
		{1, 2, 3},
		{
			byte(broadcastNotFoundConstructorID),
			byte(broadcastNotFoundConstructorID >> 8),
			byte(broadcastNotFoundConstructorID >> 16),
			byte(broadcastNotFoundConstructorID >> 24),
		},
	}
	for _, body := range notMessages {
		if _, ok := plumtreeBroadcastConstructor(body); ok {
			t.Fatalf("non-message body %x was recognized", body)
		}
	}
}

func TestPlumtreeStatusSnapshotAggregatesOverlayTelemetry(t *testing.T) {
	t.Parallel()

	firstStats := &plumtreeStats{}
	firstStats.telemetry.notePartReceived(plumtreePartSourceDirect)
	firstStats.telemetry.noteMessageReceived(plumtreeMessageFEC)
	firstStats.telemetry.noteMessageReceived(plumtreeMessageIHave)

	secondStats := &plumtreeStats{}
	secondStats.telemetry.notePartReceived(plumtreePartSourceRecovery)
	secondStats.telemetry.noteMessageReceived(plumtreeMessageIHave)
	secondStats.telemetry.noteMessageReceived(plumtreeMessagePrune)

	subscriptions := []*overlaySubscription{
		{
			spec:     overlaySpec{Name: "masterchain"},
			plumtree: &plumtreeRuntime{stats: firstStats},
		},
		{
			spec:     overlaySpec{Name: "masterchain"},
			plumtree: &plumtreeRuntime{stats: secondStats},
		},
		{
			spec: overlaySpec{Name: "custom"},
		},
	}

	snapshot := plumtreeStatusSnapshot(subscriptions)
	if len(snapshot) != 1 {
		t.Fatalf("Plumtree overlay snapshots = %d, want 1", len(snapshot))
	}

	got := snapshot[0]
	if got.Overlay != "masterchain" ||
		got.DirectParts != 1 ||
		got.RecoveryParts != 1 ||
		got.FECMessages != 1 ||
		got.IHaveMessages != 2 ||
		got.PruneMessages != 1 {
		t.Fatalf("aggregated Plumtree telemetry = %+v", got)
	}
}
