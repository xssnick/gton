package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/fastsync"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func TestFastSyncPlumtreeFlagPreservesOrdinaryShardDescriptions(t *testing.T) {
	for _, delivery := range []Delivery{DeliverySimple, DeliveryFEC, DeliveryTwoStep} {
		t.Run(string(delivery), func(t *testing.T) {
			node := newTestNode(t)
			sub := testOverlaySubscription(&overlaySubscription{
				node: node,
				spec: overlaySpec{Name: "fast-sync", Kind: overlayKindFastSync, ShortID: []byte{1, 2, 3}},
				fastSync: &fastSyncOverlayRuntime{spec: fastSyncOverlaySpec{
					localValidator: true, plumtreeEnabled: true,
				}},
				log: discardLogger(),
			})
			msg := tonnodeapi.NewShardBlockBroadcast{Block: tonnodeapi.NewShardBlock{
				ID: testBlockID(0, topShard, 202), CCSeqno: 7, Data: testShardDescriptionData(1),
			}}
			payload, err := tl.Serialize(msg, true)
			if err != nil {
				t.Fatal(err)
			}
			result := sub.classifyBroadcast(nil, msg, payload, delivery, true, testPeerID("validator"))
			if result == nil || result.event == nil || result.event.ShardDescription == nil {
				t.Fatal("ordinary shard-top description was not admitted by the Plumtree decliner")
			}
		})
	}
}

func TestFastSyncPlumtreeFlagFiltersOnlyPlumtreeTargets(t *testing.T) {
	peer := testRebroadcastQueuePeer("validator")
	peer.route = newTestPeerRoute("127.0.0.1:3000")
	sub := testOverlaySubscription(&overlaySubscription{
		node: newTestNode(t),
		spec: overlaySpec{Kind: overlayKindFastSync},
		fastSync: &fastSyncOverlayRuntime{membership: fastsync.NewMembership(
			[]PeerID{peer.id}, []PeerID{peer.id}, overlayMemberDoNotReceivePlumtreeBroadcasts,
		)},
		peers: map[PeerID]*overlayPeer{peer.id: peer},
	})
	targets := sub.buildBroadcastTargetsSnapshot()
	if len(targets.peers) != 1 || len(targets.broadcast) != 1 || len(targets.simpleRelay) != 1 || len(targets.relay) != 1 {
		t.Fatal("ordinary gossip omitted the Plumtree decliner")
	}
	if len(targets.plumtree) != 0 || sub.PlumtreePeerReceivesBroadcasts(peer.id) {
		t.Fatal("Plumtree gossip included the decliner")
	}
}

func TestFastSyncDeclinesPlumtreeBeforeEngineWork(t *testing.T) {
	node, sub, _ := newFastSyncExternalTestSubscription(t)
	for _, constructor := range []uint32{
		broadcastPlumtreeSimpleConstructorID,
		broadcastPlumtreeFECConstructorID,
		broadcastPlumtreeIHaveConstructorID,
	} {
		body := binary.LittleEndian.AppendUint32(nil, constructor)
		if err := sub.plumtree.HandleMessage(t.Context(), node.localID, body, body); err != nil {
			t.Fatalf("declined payload reached parser: %v", err)
		}
	}
	body := binary.LittleEndian.AppendUint32(nil, broadcastPlumtreePruneConstructorID)
	if err := sub.plumtree.HandleMessage(t.Context(), node.localID, body, body); err == nil {
		t.Fatal("Plumtree control message skipped its parser")
	}
}

func TestFastSyncLegacyOverlaysDoNotDeclinePlumtreeInMemberFlags(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, 32)
	if err := node.SetFastSyncOverlays(FastSyncState{
		Roster: NewFastSyncValidatorRoster(nil, []FastSyncValidator{
			fastSyncOverlayTestValidator(0x11, node.localID),
		}, nil),
		Shards: []FastSyncShard{{Workchain: 0, Shard: topShard}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, sub := range node.fastSyncSubscriptions {
		if sub.fastSync.spec.permanentFlags != 0 || fastSyncLocalMemberFlags(sub.fastSync.spec.receivePlumtreeBroadcasts) != 0 {
			t.Fatal("legacy FastSync overlay advertises a Plumtree decline flag")
		}
	}
}

func TestFastSyncCertifiedExternalBroadcastIngress(t *testing.T) {
	for _, fec := range []bool{false, true} {
		name := "simple"
		if fec {
			name = "fec"
		}
		t.Run(name, func(t *testing.T) {
			node, sub, issuer := newFastSyncExternalTestSubscription(t)
			sender := newTestNode(t)
			admission := &testExternalMessageAdmission{}
			node.externalMessageAdmission = admission
			certificate := fastSyncMembershipTestCertificate(t, issuer, sender.localID, 1, int32(time.Now().Add(time.Hour).Unix()))
			envelope, err := newQUICOverlayEnvelope(sub.spec.ShortID, &certificate)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{
				Message: tonnodeapi.ExternalMessage{Data: testExternalMessageBOC(t)},
			}, true)
			if err != nil {
				t.Fatal(err)
			}
			peer := &authenticatedQUICPeer{id: sender.localID}
			messages := fastSyncExternalTestWire(t, sender, payload, fec)
			for _, message := range messages {
				wire, err := envelope.Message(message)
				if err != nil {
					t.Fatal(err)
				}
				if err := node.handleQUICMessage(context.Background(), peer, wire); err != nil && !errors.Is(err, overlay.ErrBroadcastRejected) {
					t.Fatal(err)
				}
			}
			if len(admission.events) != 1 || admission.events[0].Priority != 0 || admission.events[0].IsLocal {
				t.Fatalf("external admission = %+v, want one remote priority-0 message", admission.events)
			}
			if sub.fastSync.permanent(sender.localID) {
				t.Fatal("test sender must be a certified, non-validator FastSync member")
			}
		})
	}
}

func TestFastSyncExternalBroadcastRejectsInvalidAuthorityAndApplication(t *testing.T) {
	t.Run("unknown member", func(t *testing.T) {
		node, sub, _ := newFastSyncExternalTestSubscription(t)
		sender := newTestNode(t)
		admission := &testExternalMessageAdmission{}
		node.externalMessageAdmission = admission
		envelope, err := newQUICOverlayEnvelope(sub.spec.ShortID, nil)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{Message: tonnodeapi.ExternalMessage{Data: testExternalMessageBOC(t)}}, true)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := envelope.Message(fastSyncExternalTestWire(t, sender, payload, false)[0])
		if err != nil {
			t.Fatal(err)
		}
		if err = node.handleQUICMessage(t.Context(), &authenticatedQUICPeer{id: sender.localID}, wire); err == nil {
			t.Fatal("uncertified transport peer was accepted")
		}
		if len(admission.events) != 0 {
			t.Fatal("uncertified broadcast reached message admission")
		}
	})

	for _, name := range []string{"untrusted block", "rejected external", "oversized external"} {
		t.Run(name, func(t *testing.T) {
			node, sub, _ := newFastSyncExternalTestSubscription(t)
			admission := &testExternalMessageAdmission{}
			node.externalMessageAdmission = admission
			var msg any = tonnodeapi.NewExternalMessageBroadcast{Message: tonnodeapi.ExternalMessage{Data: testExternalMessageBOC(t)}}
			wantAdmission := 0
			switch name {
			case "untrusted block":
				msg = tonnodeapi.NewShardBlockBroadcast{Block: tonnodeapi.NewShardBlock{
					ID: testBlockID(0, topShard, 202), CCSeqno: 7, Data: testShardDescriptionData(1),
				}}
			case "rejected external":
				admission.err = errors.New("admission rejected")
				wantAdmission = 1
			case "oversized external":
				msg = tonnodeapi.NewExternalMessageBroadcast{Message: tonnodeapi.ExternalMessage{Data: make([]byte, maxOverlayPayloadSize+1)}}
			}
			result, err := sub.classifyBroadcastPayload(nil, msg, newKnownBroadcastPayload([]byte{1}), DeliverySimple, false, testPeerID("nonvalidator"))
			if err != nil || result.disposition != broadcastDispositionIgnore {
				t.Fatalf("invalid FastSync broadcast result = %+v, %v", result, err)
			}
			if len(admission.events) != wantAdmission {
				t.Fatalf("admission calls = %d, want %d", len(admission.events), wantAdmission)
			}
		})
	}
}

func TestFastSyncExternalSendUsesOrdinaryDeliveryAndKeepsPublicIsolation(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary node", true: "validator or collator"}[disabled], func(t *testing.T) {
			node, public := newSendExternalMessageTestNode(t)
			node.disablePublicExternalMessages = disabled
			publicPeer := testRebroadcastQueuePeer("public")
			public.peers[publicPeer.id] = publicPeer
			fastPeer := testRebroadcastQueuePeer("fast-sync")
			fast := testOverlaySubscription(&overlaySubscription{
				node:     node,
				spec:     overlaySpec{Kind: overlayKindFastSync, ShortID: testPeerID("fast-overlay").Bytes()},
				fastSync: &fastSyncOverlayRuntime{membership: fastsync.NewMembership(nil, []PeerID{fastPeer.id}, overlayMemberDoNotReceivePlumtreeBroadcasts)},
				peers:    map[PeerID]*overlayPeer{fastPeer.id: fastPeer},
			})
			node.fastSyncSubscriptions[FastSyncShard{Workchain: 0, Shard: topShard}] = fast
			if err := sendTestExternalMessage(t, node, testExternalMessageBOC(t)); err != nil {
				t.Fatal(err)
			}
			req, ok := fastPeer.localRebroadcastQueue.TryPop()
			if !ok || req.simple == nil || req.fec != nil || fast.rebroadcastDelivery(req) != DeliverySimple {
				t.Fatalf("FastSync external was not sent through ordinary simple broadcast: %+v", req)
			}
			if _, sent := publicPeer.localRebroadcastQueue.TryPop(); sent == disabled {
				t.Fatalf("public send = %t with disablePublic=%t", sent, disabled)
			}
		})
	}
}

func TestFastSyncExternalSendWithoutMembershipStillAdmitsLocally(t *testing.T) {
	node, _ := newSendExternalMessageTestNode(t)
	node.disablePublicExternalMessages = true
	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission
	if err := sendTestExternalMessage(t, node, testExternalMessageBOC(t)); err != nil {
		t.Fatal(err)
	}
	if len(admission.events) != 1 || !admission.events[0].IsLocal {
		t.Fatal("isolated validator lost local admission while FastSync membership is unavailable")
	}
}

func TestFastSyncExternalFECPlanPinsNeighboursAndDisablesTwoStep(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{spec: overlaySpec{Kind: overlayKindFastSync}})
	for _, size := range []int{ordinarySimpleBroadcastMaxSize, ordinarySimpleBroadcastMaxSize + 1} {
		plan := sub.rebroadcastPlan("tonNode.externalMessageBroadcast", size)
		if sub.usesTwoStepRebroadcast(plan) {
			t.Fatal("FastSync external unexpectedly uses two-step delivery")
		}
		if size <= ordinarySimpleBroadcastMaxSize {
			if plan.mode != rebroadcastModeSimple || plan.flags != 0 {
				t.Fatalf("simple plan = %+v", plan)
			}
		} else if plan.mode != rebroadcastModeFEC || plan.flags != broadcastFlagFixedNeighbours|overlay.BroadcastFlagNoTwoStep {
			t.Fatalf("FEC plan = %+v", plan)
		}
	}
}

func newFastSyncExternalTestSubscription(t *testing.T) (*Node, *overlaySubscription, fastSyncMembershipTestIssuer) {
	t.Helper()
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, 32)
	issuer := newFastSyncMembershipTestIssuer(t, 0x85)
	roster := peerRuntimeTestRoster(issuer.public.Key,
		FastSyncValidator{ADNLID: node.localID},
		FastSyncValidator{ADNLID: issuer.id},
	)
	if err := node.SetFastSyncOverlays(FastSyncState{Roster: roster, MasterchainPlumtreeEnabled: true}); err != nil {
		t.Fatal(err)
	}
	sub := node.fastSyncSubscriptions[FastSyncShard{Workchain: -1, Shard: topShard}]
	return node, sub, issuer
}

func fastSyncExternalTestWire(t *testing.T, sender *Node, payload []byte, fec bool) []tl.Serializable {
	t.Helper()
	if !fec {
		message, err := sender.buildSimpleBroadcast(payload, 0)
		if err != nil {
			t.Fatal(err)
		}
		return []tl.Serializable{message}
	}
	encoder, err := overlay.NewBroadcastFECSender(sender.privKey, overlay.CertificateEmpty{}, payload,
		broadcastFlagFixedNeighbours, overlay.WithBroadcastFECSymbolSize(32))
	if err != nil {
		t.Fatal(err)
	}
	messages := make([]tl.Serializable, 0, encoder.TotalParts())
	for i := uint32(0); i < encoder.TotalParts(); i++ {
		part, err := encoder.Part(i)
		if err != nil {
			t.Fatal(err)
		}
		var message overlay.BroadcastFEC
		if _, err = tl.Parse(&message, part.FullWire, true); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, message)
	}
	return messages
}

func TestFastSyncBroadcastWireOmitsInternalTwoStepFlag(t *testing.T) {
	for _, kind := range []string{"tonNode.newShardBlockBroadcast", "tonNode.externalMessageBroadcast"} {
		for _, size := range []int{ordinarySimpleBroadcastMaxSize, ordinarySimpleBroadcastMaxSize + 1} {
			node := newTestNode(t)
			peer := testRebroadcastQueuePeer("wire-target")
			sub := testOverlaySubscription(&overlaySubscription{
				node:     node,
				spec:     overlaySpec{Kind: overlayKindFastSync},
				fastSync: &fastSyncOverlayRuntime{membership: fastsync.NewMembership(nil, []PeerID{peer.id}, 0)},
				peers:    map[PeerID]*overlayPeer{peer.id: peer},
			})
			if !sub.enqueueRebroadcast(rebroadcastRequest{subscription: sub, kind: kind, payload: make([]byte, size), local: true}) {
				t.Fatal("enqueue broadcast failed")
			}
			req, ok := peer.localRebroadcastQueue.TryPop()
			if !ok {
				t.Fatal("broadcast was not queued for ordinary delivery")
			}
			flags := int32(0)
			wantFlags := int32(0)
			if size <= ordinarySimpleBroadcastMaxSize {
				if req.simple == nil {
					t.Fatal("missing simple broadcast")
				}
				flags = req.simple.Flags
			} else {
				if req.fec == nil {
					t.Fatal("missing FEC broadcast")
				}
				part, err := req.fec.Part(0)
				if err != nil {
					t.Fatal(err)
				}
				var message overlay.BroadcastFEC
				if _, err = tl.Parse(&message, part.FullWire, true); err != nil {
					t.Fatal(err)
				}
				flags = message.Flags
				wantFlags = broadcastFlagFixedNeighbours
				if kind == "tonNode.newShardBlockBroadcast" {
					wantFlags = overlay.BroadcastFlagAnySender
				}
			}
			if flags != wantFlags {
				t.Fatalf("%s size %d wire flags = %d, want %d", kind, size, flags, wantFlags)
			}
		}
	}
}

func TestFastSyncOwnExternalRelaysOnceWithoutPoolReadmission(t *testing.T) {
	node, sub, issuer := newFastSyncExternalTestSubscription(t)
	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission
	data := testExternalMessageBOC(t)
	node.myExternalMessages.Mark(externalMessageFingerprint(sub.spec.ShortID, data), time.Now())
	payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{Message: tonnodeapi.ExternalMessage{Data: data}}, true)
	if err != nil {
		t.Fatal(err)
	}
	original, err := node.buildSimpleBroadcast(payload, 0)
	if err != nil {
		t.Fatal(err)
	}
	relay := attachFastSyncSimpleRelayTestPeer(sub, issuer.id)
	sender := newTestNode(t)
	certificate := fastSyncMembershipTestCertificate(t, issuer, sender.localID, 1, int32(time.Now().Add(time.Hour).Unix()))
	envelope, err := newQUICOverlayEnvelope(sub.spec.ShortID, &certificate)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := envelope.Message(original)
	if err != nil {
		t.Fatal(err)
	}
	peer := &authenticatedQUICPeer{id: sender.localID}
	if err := node.handleQUICMessage(t.Context(), peer, wire); err != nil {
		t.Fatal(err)
	}
	requireFastSyncSimpleRelayEnvelope(t, relay, original)
	if err := node.handleQUICMessage(t.Context(), peer, wire); err != nil {
		t.Fatal(err)
	}
	select {
	case <-relay.sent:
		t.Fatal("reflected message was relayed twice")
	case <-time.After(20 * time.Millisecond):
	}
	if len(admission.events) != 0 {
		t.Fatal("reflected local message was admitted twice")
	}
}

func TestFastSyncSimpleRelayWaitsForApplicationAdmission(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		for _, rejected := range []bool{false, true} {
			node, sub, issuer := newFastSyncExternalTestSubscription(t)
			relay := attachFastSyncSimpleRelayTestPeer(sub, issuer.id)
			admission := &fastSyncRelayAdmission{
				entered: make(chan struct{}), release: make(chan struct{}),
			}
			if rejected {
				admission.err = errors.New("external rejected")
			}
			node.externalMessageAdmission = admission
			sender := newTestNode(t)
			certificate := fastSyncMembershipTestCertificate(t, issuer, sender.localID, 1, int32(time.Now().Add(time.Hour).Unix()))
			envelope, err := newQUICOverlayEnvelope(sub.spec.ShortID, &certificate)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{Message: tonnodeapi.ExternalMessage{Data: testExternalMessageBOC(t)}}, true)
			if err != nil {
				t.Fatal(err)
			}
			signer := sender
			if trusted {
				signer = node
			}
			original, err := signer.buildSimpleBroadcast(payload, 0)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := envelope.Message(original)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- node.handleQUICMessage(t.Context(), &authenticatedQUICPeer{id: sender.localID}, wire)
			}()
			select {
			case <-admission.entered:
			case <-time.After(5 * time.Second):
				close(admission.release)
				t.Fatal("broadcast did not reach application admission")
			}
			select {
			case <-relay.sent:
				close(admission.release)
				<-done
				t.Fatal("broadcast relayed before application admission completed")
			default:
			}
			close(admission.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if rejected {
				select {
				case <-relay.sent:
					t.Fatalf("rejected external relayed with trusted=%t", trusted)
				case <-time.After(20 * time.Millisecond):
				}
			} else {
				requireFastSyncSimpleRelayEnvelope(t, relay, original)
			}
		}
	}
}

type fastSyncRelayAdmission struct {
	entered chan struct{}
	release chan struct{}
	err     error
}

func (a *fastSyncRelayAdmission) AcceptExternalMessage(context.Context, ExternalMessageEvent) error {
	close(a.entered)
	<-a.release
	return a.err
}

func (a *fastSyncRelayAdmission) AcceptCheckedExternalMessage(ctx context.Context, event ExternalMessageEvent) error {
	return a.AcceptExternalMessage(ctx, event)
}

func attachFastSyncSimpleRelayTestPeer(sub *overlaySubscription, id PeerID) *testSimpleRelayPeer {
	relay := &testSimpleRelayPeer{id: id.Bytes(), sent: make(chan tl.Serializable, 2)}
	peer := testRebroadcastQueuePeer("native-relay")
	peer.id = id
	peer.broadcastPeer = relay
	sub.mx.Lock()
	sub.peers[id] = peer
	sub.neighbours = append(sub.neighbours, id)
	sub.notifyPeersChangedLocked()
	sub.mx.Unlock()
	return relay
}

func requireFastSyncSimpleRelayEnvelope(t *testing.T, relay *testSimpleRelayPeer, original overlay.Broadcast) {
	t.Helper()
	var message tl.Serializable
	select {
	case message = <-relay.sent:
	case <-time.After(5 * time.Second):
		t.Fatal("FastSync simple broadcast did not reach native relay")
	}
	got, err := tl.Serialize(message, true)
	if err != nil {
		t.Fatal(err)
	}
	want, err := tl.Serialize(original, true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("FastSync relay changed the broadcast source, signature or payload")
	}
}
