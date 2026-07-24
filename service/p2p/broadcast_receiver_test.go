package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

func TestInboundPeerReceivesPublicBroadcastWithoutJoiningRosterAndAttachesFixedOverlay(t *testing.T) {
	peerPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	peerHash, err := tl.Hash(keys.PublicKeyED25519{Key: peerPub})
	if err != nil {
		t.Fatalf("hash peer key: %v", err)
	}
	peerID, err := NewPeerID(peerHash)
	if err != nil {
		t.Fatalf("parse peer id: %v", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	node := newTestNode(t)
	node.runCtx = runCtx

	publicOverlayID := testPeerID("public-overlay")
	publicSub := mustGetOrCreateSubscription(t, node, overlaySpec{
		Name:    "public",
		Kind:    overlayKindPublicShard,
		ShortID: publicOverlayID[:],
	})
	customOverlayID := testPeerID("custom-overlay")
	customSub := mustGetOrCreateSubscription(t, node, overlaySpec{
		Name:         "custom.fixed",
		Kind:         overlayKindCustomFixed,
		ShortID:      customOverlayID[:],
		FixedNodes:   []PeerID{peerID},
		FixedNodeIDs: map[PeerID]struct{}{peerID: {}},
	})
	t.Cleanup(func() {
		publicSub.close()
		customSub.close()
		node.wg.Wait()
		for _, pooled := range node.pool.snapshot() {
			node.pool.closeIfUnused(pooled)
		}
	})

	base := newTestOverlayADNL()
	base.id = peerID.Bytes()
	base.pub = peerPub
	base.remoteAddr = "127.0.0.1:19001"
	if err = node.handleInboundPeer(base); err != nil {
		t.Fatalf("handle inbound peer: %v", err)
	}

	if peer := publicSub.peerByID(peerID); peer != nil {
		t.Fatal("unlisted inbound peer joined the public overlay roster")
	}
	customPeer := customSub.peerByID(peerID)
	if customPeer == nil || customPeer.overlay == nil {
		t.Fatal("configured fixed inbound peer was not attached to its custom overlay")
	}

	answered := false
	base.answerResponder = func(context.Context, []byte, tl.Serializable) error {
		answered = true
		return nil
	}
	if err = base.queryHandler(&adnl.MessageQuery{
		ID: bytes.Repeat([]byte{0x01}, 32),
		Data: []tl.Serializable{
			overlay.Query{Overlay: customOverlayID[:]},
			overlay.Ping{},
		},
	}); err != nil {
		t.Fatalf("dispatch fixed overlay query: %v", err)
	}
	if !answered {
		t.Fatal("configured fixed inbound peer did not reach the overlay query handler")
	}

	delivered := false
	var signerID []byte
	publicSub.broadcastReceiver.SetBroadcastHandlerWithInfo(func(msg tl.Serializable, info overlay.BroadcastInfo) overlay.BroadcastDisposition {
		if !bytes.Equal(info.ImmediatePeerID, peerID[:]) {
			t.Fatalf("immediate peer id = %x, want %x", info.ImmediatePeerID, peerID[:])
		}
		if !bytes.Equal(info.SourceID, signerID) {
			t.Fatalf("signer id = %x, want %x", info.SourceID, signerID)
		}
		delivered = true
		return publicSub.handleReceivedBroadcast(msg, info)
	})
	msg := signedTestSimpleBroadcast(t, IhrMessageBroadcast{
		Message: IhrMessage{Data: []byte{0x01}},
	})
	signerID, err = tl.Hash(msg.Source)
	if err != nil {
		t.Fatalf("hash broadcast signer: %v", err)
	}
	pooled := node.pool.snapshot()[0]
	oldTouch := time.Now().Add(-time.Minute)
	pooled.touch(oldTouch)
	unknownOverlayID := testPeerID("unknown-public-overlay")
	if err = base.customHandler(&adnl.MessageCustom{Data: []tl.Serializable{
		overlay.Message{Overlay: unknownOverlayID[:]},
		msg,
	}}); err != nil {
		t.Fatalf("handle unknown overlay broadcast: %v", err)
	}
	if !pooled.lastUsed().Equal(oldTouch) {
		t.Fatal("unknown overlay resolver lookup refreshed the transport idle deadline")
	}
	invalid := msg
	invalid.Signature = append([]byte(nil), msg.Signature...)
	invalid.Signature[0] ^= 0xff
	_ = base.customHandler(&adnl.MessageCustom{Data: []tl.Serializable{
		overlay.Message{Overlay: publicOverlayID[:]},
		invalid,
	}})
	if !pooled.lastUsed().Equal(oldTouch) {
		t.Fatal("invalid broadcast refreshed the transport idle deadline before verification")
	}
	if err = base.customHandler(&adnl.MessageCustom{Data: []tl.Serializable{
		overlay.Message{Overlay: publicOverlayID[:]},
		msg,
	}}); err != nil {
		t.Fatalf("handle public broadcast from unlisted peer: %v", err)
	}
	if !delivered {
		t.Fatal("public broadcast from unlisted peer was not delivered")
	}
	if !pooled.lastUsed().After(oldTouch) {
		t.Fatal("verified public broadcast did not refresh the inbound transport idle deadline")
	}
	if peer := publicSub.peerByID(peerID); peer != nil {
		t.Fatal("receiving a public broadcast promoted an unlisted peer into the roster")
	}
}

func signedTestSimpleBroadcast(tb testing.TB, payload tl.Serializable) overlay.Broadcast {
	tb.Helper()

	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatalf("generate broadcast key: %v", err)
	}
	data, err := tl.Serialize(payload, true)
	if err != nil {
		tb.Fatalf("serialize broadcast payload: %v", err)
	}
	source := keys.PublicKeyED25519{Key: pub}
	sourceID, err := tl.Hash(source)
	if err != nil {
		tb.Fatalf("hash broadcast source: %v", err)
	}
	broadcastID, err := tl.Hash(OverlayBroadcastID{
		Source:   sourceID,
		DataHash: hashSimpleBroadcastPayload(data),
	})
	if err != nil {
		tb.Fatalf("hash broadcast id: %v", err)
	}
	date := uint32(time.Now().Unix())
	toSign, err := tl.Serialize(overlay.BroadcastToSign{Hash: broadcastID, Date: date}, true)
	if err != nil {
		tb.Fatalf("serialize broadcast signature payload: %v", err)
	}

	return overlay.Broadcast{
		Source:      source,
		Certificate: overlay.CertificateEmpty{},
		Data:        data,
		Date:        int32(date),
		Signature:   ed25519.Sign(key, toSign),
	}
}

func TestUnlistedPublicBroadcastArrivingAsRLDPMessageUsesResolver(t *testing.T) {
	peerID := testPeerID("unlisted-rldp-peer")
	localID := testPeerID("rldp-local")
	node := &Node{
		log:           discardLogger(),
		localID:       localID,
		subscriptions: map[string]*overlaySubscription{},
	}
	node.pool = newPeerPool(nil, node.resolvePublicBroadcastReceiver)
	publicOverlayID := testPeerID("rldp-public-overlay")
	sub := mustGetOrCreateSubscription(t, node, overlaySpec{
		Name:    "public.rldp",
		Kind:    overlayKindPublicShard,
		ShortID: publicOverlayID[:],
	})
	t.Cleanup(sub.close)

	delivered := false
	sub.broadcastReceiver.SetBroadcastHandlerWithInfo(func(_ tl.Serializable, info overlay.BroadcastInfo) overlay.BroadcastDisposition {
		if !bytes.Equal(info.ImmediatePeerID, peerID[:]) {
			t.Fatalf("RLDP immediate peer id = %x, want %x", info.ImmediatePeerID, peerID[:])
		}
		delivered = true
		return overlay.BroadcastDispositionAcceptAndRelay
	})

	base := newTestOverlayADNL()
	base.id = peerID.Bytes()
	adnlWrapper := overlay.CreateExtendedADNL(base)
	adnlWrapper.SetBroadcastReceiverResolver(node.resolvePublicBroadcastReceiver)
	rldpTransport := &testBroadcastRLDP{adnl: adnlWrapper}
	overlay.CreateExtendedRLDP(rldpTransport)
	if rldpTransport.onMessage == nil {
		t.Fatal("RLDP wrapper did not install its message ingress handler")
	}

	msg := signedTestSimpleBroadcast(t, IhrMessageBroadcast{
		Message: IhrMessage{Data: []byte{0x02}},
	})
	data, err := tl.Serialize(overlay.WrapMessage(publicOverlayID[:], msg), true)
	if err != nil {
		t.Fatalf("serialize RLDP broadcast payload: %v", err)
	}
	if err = rldpTransport.onMessage(bytes.Repeat([]byte{0x03}, 32), data); err != nil {
		t.Fatalf("route completed RLDP message: %v", err)
	}
	if !delivered {
		t.Fatal("public broadcast completed over RLDP was not delivered through the resolver")
	}
	if peer := sub.peerByID(peerID); peer != nil {
		t.Fatal("RLDP public broadcast promoted an unlisted peer into the roster")
	}
}

func TestPublicBroadcastReceiverResolverLifecycleAndCustomIsolation(t *testing.T) {
	node := &Node{
		log:           discardLogger(),
		localID:       testPeerID("resolver-lifecycle-local"),
		subscriptions: map[string]*overlaySubscription{},
	}
	node.pool = newPeerPool(nil, node.resolvePublicBroadcastReceiver)
	publicOverlayID := testPeerID("resolver-lifecycle-public")
	publicSpec := overlaySpec{
		Name:    "public.lifecycle",
		Kind:    overlayKindPublicShard,
		ShortID: publicOverlayID[:],
	}
	publicSub := mustGetOrCreateSubscription(t, node, publicSpec)
	customOverlayID := testPeerID("resolver-lifecycle-custom")
	customSub := mustGetOrCreateSubscription(t, node, overlaySpec{
		Name:    "custom.lifecycle",
		Kind:    overlayKindCustomFixed,
		ShortID: customOverlayID[:],
	})
	t.Cleanup(func() {
		publicSub.close()
		customSub.close()
	})

	got, err := node.resolvePublicBroadcastReceiver(publicOverlayID[:])
	if err != nil || got != publicSub.broadcastReceiver {
		t.Fatalf("resolve active public receiver = %p, %v", got, err)
	}
	if _, err = node.resolvePublicBroadcastReceiver(customOverlayID[:]); !errors.Is(err, overlay.ErrBroadcastReceiverNotFound) {
		t.Fatalf("custom fixed receiver resolved publicly: %v", err)
	}

	deleteAt := time.Now().Add(-time.Second)
	publicSub.setActive(false, deleteAt)
	if _, err = node.resolvePublicBroadcastReceiver(publicOverlayID[:]); !errors.Is(err, overlay.ErrBroadcastReceiverNotFound) {
		t.Fatalf("inactive public receiver resolved: %v", err)
	}
	publicSub.setActive(true, time.Time{})
	got, err = node.resolvePublicBroadcastReceiver(publicOverlayID[:])
	if err != nil || got != publicSub.broadcastReceiver {
		t.Fatalf("resolve reactivated public receiver = %p, %v", got, err)
	}

	publicSub.setActive(false, deleteAt)
	if !node.deleteInactiveSubscription(overlaySpecKey(publicSpec), publicSub, time.Now()) {
		t.Fatal("expired inactive public subscription was not deleted")
	}
	if _, err = node.resolvePublicBroadcastReceiver(publicOverlayID[:]); !errors.Is(err, overlay.ErrBroadcastReceiverNotFound) {
		t.Fatalf("deleted public receiver resolved: %v", err)
	}
}

func TestReceivedBroadcastMetricsClassifyImmediatePeerRosterMembership(t *testing.T) {
	node := newTestNode(t)
	rosterPeer := testRebroadcastQueuePeer("metric-roster-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "public.metrics",
			Kind:    overlayKindPublicShard,
			ShortID: testPeerID("metrics-overlay").Bytes(),
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{rosterPeer.id: rosterPeer},
	})
	signerID := testPeerID("metric-signer")
	msg := IhrMessageBroadcast{Message: IhrMessage{Data: []byte{0x01}}}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize metric broadcast: %v", err)
	}

	if disposition := sub.handleReceivedBroadcast(msg, overlay.BroadcastInfo{
		SourceID:        signerID[:],
		ImmediatePeerID: rosterPeer.id[:],
		BroadcastID:     testPeerID("metric-roster-broadcast").Bytes(),
		Delivery:        overlay.BroadcastDeliverySimple,
		Payload:         payload,
	}); disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("handle roster broadcast disposition: %v", disposition)
	}
	unlistedPeerID := testPeerID("metric-unlisted-peer")
	if disposition := sub.handleReceivedBroadcast(msg, overlay.BroadcastInfo{
		SourceID:        signerID[:],
		ImmediatePeerID: unlistedPeerID[:],
		BroadcastID:     testPeerID("metric-unlisted-broadcast").Bytes(),
		Delivery:        overlay.BroadcastDeliverySimple,
		Payload:         payload,
	}); disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("handle unlisted broadcast disposition: %v", disposition)
	}

	if got := testBroadcastStatCount(node, "received_roster", sub.spec.Name, "tonNode.ihrMessageBroadcast"); got != 1 {
		t.Fatalf("received roster metric = %d, want 1", got)
	}
	if got := testBroadcastStatCount(node, "received_unlisted", sub.spec.Name, "tonNode.ihrMessageBroadcast"); got != 1 {
		t.Fatalf("received unlisted metric = %d, want 1", got)
	}
}

func TestReceivedSimpleBroadcastKeepsRawPayloadAndExactQueueWeight(t *testing.T) {
	node := newTestNode(t)
	target := testRebroadcastQueuePeer("raw-payload-target")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "public.raw-payload",
			Kind:    overlayKindPublicShard,
			ShortID: testPeerID("raw-payload-overlay").Bytes(),
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{target.id: target},
	})
	msg := IhrMessageBroadcast{Message: IhrMessage{Data: bytes.Repeat([]byte{0xA5}, 512)}}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize broadcast payload: %v", err)
	}

	disposition := sub.handleReceivedBroadcast(msg, overlay.BroadcastInfo{
		SourceID:        testPeerID("raw-payload-signer").Bytes(),
		ImmediatePeerID: testPeerID("raw-payload-unlisted-peer").Bytes(),
		BroadcastID:     testPeerID("raw-payload-broadcast").Bytes(),
		Delivery:        overlay.BroadcastDeliverySimple,
		Payload:         payload,
	})
	if disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("broadcast disposition = %v, want accept", disposition)
	}

	queued, ok := target.rebroadcastQueue.TryPop()
	if !ok {
		t.Fatal("accepted simple broadcast was not queued")
	}
	if queued.payloadSource != nil || !bytes.Equal(queued.payload, payload) {
		t.Fatal("queued broadcast did not retain the exact incoming payload")
	}
	wantBytes := int64(len(payload) + 256)
	if got := rebroadcastRequestBytes(queued); got != wantBytes {
		t.Fatalf("queued broadcast weight = %d, want %d", got, wantBytes)
	}
}

type testBroadcastRLDP struct {
	adnl      rldp.ADNL
	onQuery   func([]byte, *rldp.Query) error
	onMessage func([]byte, []byte) error
}

func (r *testBroadcastRLDP) GetADNL() rldp.ADNL {
	return r.adnl
}

func (*testBroadcastRLDP) GetRateInfo() (int64, int64) {
	return 0, 0
}

func (*testBroadcastRLDP) Stats() rldp.Stats {
	return rldp.Stats{}
}

func (*testBroadcastRLDP) Close() {}

func (*testBroadcastRLDP) DoQuery(context.Context, uint64, tl.Serializable, tl.Serializable) error {
	return nil
}

func (*testBroadcastRLDP) DoQueryAsync(context.Context, uint64, []byte, tl.Serializable, chan<- rldp.AsyncQueryResult) error {
	return nil
}

func (r *testBroadcastRLDP) SetOnQuery(handler func([]byte, *rldp.Query) error) {
	r.onQuery = handler
}

func (r *testBroadcastRLDP) SetOnMessage(handler func([]byte, []byte) error) {
	r.onMessage = handler
}

func (*testBroadcastRLDP) SetOnDisconnect(func()) {}

func (*testBroadcastRLDP) SendAnswer(context.Context, uint64, uint32, []byte, []byte, tl.Serializable) error {
	return nil
}

func BenchmarkResolvePublicBroadcastReceiver(b *testing.B) {
	overlayID := testPeerID("benchmark-public-overlay")
	receiver, err := overlay.NewBroadcastReceiver(overlayID[:], maxOverlayPayloadSize, true, false)
	if err != nil {
		b.Fatalf("create broadcast receiver: %v", err)
	}
	b.Cleanup(receiver.Close)

	node := &Node{}
	node.publicBroadcastReceivers.Store(&publicBroadcastReceiverSnapshot{
		receivers: map[string]*overlay.BroadcastReceiver{
			string(overlayID[:]): receiver,
		},
	})

	var failures atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			got, resolveErr := node.resolvePublicBroadcastReceiver(overlayID[:])
			if resolveErr != nil || got != receiver {
				failures.Add(1)
			}
		}
	})
	if failures.Load() != 0 {
		b.Fatalf("resolver failures: %d", failures.Load())
	}
}
