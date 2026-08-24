package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
)

type twoStepSendOutcome struct {
	result overlay.BroadcastTwoStepSendResult
	err    error
}

func TestQUICDialContenderHonorsContextDuringPrewarm(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)
	dht := &blockingOutboundRouteDHT{
		pub:     remotePub,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	node := newTestNode(t)
	node.dht = dht
	runCtx, cancelRun := context.WithCancel(context.Background())
	node.runCtx = runCtx
	peer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remotePub,
		route: newTestPeerRoute(""),
	}
	t.Cleanup(func() {
		cancelRun()
		node.wg.Wait()
	})

	peer.requestBackgroundQUICDial()
	select {
	case <-dht.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background prewarm did not enter DHT resolution")
	}

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelDial()
	dialDone := make(chan error, 1)
	go func() {
		_, dialErr := peer.dialQUIC(dialCtx)
		dialDone <- dialErr
	}()

	var dialErr error
	select {
	case dialErr = <-dialDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("candidate dial blocked behind the prewarm transport mutex")
	}
	if !errors.Is(dialErr, context.DeadlineExceeded) {
		t.Fatalf("candidate dial error = %v, want context deadline", dialErr)
	}
	if !peer.route.QUICDialInFlight() {
		t.Fatal("candidate deadline released the prewarm owner's route claim")
	}

	cancelRun()
	node.wg.Wait()
	if peer.route.QUICDialInFlight() {
		t.Fatal("cancelled prewarm retained the route dial claim")
	}
}

func TestTwoStepQUICFastPeerIsNotHeldBySlowPrewarm(t *testing.T) {
	fastGateway, err := adnlquic.NewGateway(quicOutboundTestKey(t))
	if err != nil {
		t.Fatalf("create fast peer gateway: %v", err)
	}
	fastMessages := make(chan struct{}, 1)
	fastGateway.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		peer.SetMessageHandler(func(context.Context, []byte) {
			fastMessages <- struct{}{}
		})
		return nil
	})
	fastAddr := startQUICOutboundTestGateway(t, fastGateway)
	fastID, err := NewPeerID(fastGateway.ID())
	if err != nil {
		t.Fatalf("parse fast peer id: %v", err)
	}

	slowPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate slow peer key: %v", err)
	}
	slowID := peerIDForQUICOutboundTest(t, slowPub)
	dht := &blockingOutboundRouteDHT{
		pub:     slowPub,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	node := newTestNode(t)
	node.dht = dht
	startQUICOutboundTestGateway(t, node.quicGateway)
	runCtx, cancelRun := context.WithCancel(context.Background())
	node.runCtx = runCtx
	t.Cleanup(func() {
		cancelRun()
		node.wg.Wait()
	})

	slowPeer := &overlayPeer{
		node:  node,
		id:    slowID,
		pub:   slowPub,
		route: newTestPeerRoute(""),
	}
	fastPeer := &overlayPeer{
		node:  node,
		id:    fastID,
		pub:   fastGateway.PublicKey(),
		route: newTestPeerRoute(fastAddr),
	}
	slowPeer.requestBackgroundQUICDial()
	select {
	case <-dht.started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow peer prewarm did not enter DHT resolution")
	}

	overlayID := testPeerID("parallel-two-step-quic-send")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "custom.parallel-two-step-quic-send",
			Kind:    overlayKindCustomFixed,
			ShortID: overlayID[:],
			UseQUIC: true,
		},
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			slowID: slowPeer,
			fastID: fastPeer,
		},
	})
	t.Cleanup(sub.broadcastReceiver.Close)
	sub.broadcastTargets.Store(&broadcastTargetsSnapshot{
		generation: sub.broadcastTargetsGen.Load(),
		builtAt:    time.Now(),
		peers:      []*overlayPeer{slowPeer, fastPeer},
	})

	peerSet, failed := sub.resolveTwoStepPeerSet(context.Background(), PeerID{})
	if len(failed) != 0 || len(peerSet) != 2 {
		t.Fatalf("resolved peers = %d, failures = %+v; want two deferred send peers", len(peerSet), failed)
	}
	if _, err = node.quicGateway.OutboundPeerDefaultID(fastID[:]); err == nil {
		t.Fatal("fast peer was already connected before two-step send")
	}

	sendCtx, cancelSend := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelSend()
	sendDone := make(chan twoStepSendOutcome, 1)
	go func() {
		result, sendErr := overlay.SendBroadcastTwoStep(sendCtx, overlay.BroadcastTwoStepSendRequest{
			Key:         node.privKey,
			Certificate: overlay.CertificateEmpty{},
			LocalADNLID: node.localID.Bytes(),
			Payload:     []byte("independent cold dial"),
			PeerSet:     peerSet,
		},
			overlay.WithBroadcastTwoStepDate(123),
			overlay.WithBroadcastTwoStepSendConcurrency(2),
			overlay.WithBroadcastTwoStepPeerSendTimeout(2*time.Second),
		)
		sendDone <- twoStepSendOutcome{result: result, err: sendErr}
	}()

	select {
	case <-fastMessages:
		if !slowPeer.route.QUICDialInFlight() {
			t.Fatal("fast send arrived only after the slow prewarm completed")
		}
	case outcome := <-sendDone:
		t.Fatalf("two-step send completed before reaching the fast peer: result=%+v err=%v", outcome.result, outcome.err)
	case <-sendCtx.Done():
		t.Fatalf("fast peer did not receive while slow peer was blocked: %v", sendCtx.Err())
	}

	var outcome twoStepSendOutcome
	select {
	case outcome = <-sendDone:
	case <-sendCtx.Done():
		t.Fatalf("bounded two-step send did not finish: %v", sendCtx.Err())
	}
	if outcome.result.Sent != 1 || len(outcome.result.Failed) != 1 {
		t.Fatalf("two-step result = %+v, want one fast success and one slow failure", outcome.result)
	}
	if outcome.err == nil || !errors.Is(outcome.result.Failed[0].Err, context.DeadlineExceeded) {
		t.Fatalf("slow peer outcome = %+v, err=%v; want bounded deadline failure", outcome.result.Failed, outcome.err)
	}

	cancelRun()
	node.wg.Wait()
}

func TestAttachPooledPeerPrewarmsMissingQUICRoute(t *testing.T) {
	node := newTestNode(t)
	runCtx, cancel := context.WithCancel(context.Background())
	node.runCtx = runCtx
	// Hold the only transport-setup slot so the prewarm claim remains
	// observable without relying on QUIC handshake timing.
	node.quicOutboundDialSlots = make(chan struct{}, 1)
	node.quicOutboundDialSlots <- struct{}{}

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	id := peerIDForQUICOutboundTest(t, pub)
	route := newTestPeerRoute("")
	peer := &overlayPeer{
		node:  node,
		id:    id,
		pub:   pub,
		route: route,
	}
	overlayID := testPeerID("attach-prewarm-quic-route")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.attach-prewarm-quic-route",
			Kind:         overlayKindCustomFixed,
			ShortID:      overlayID[:],
			FixedNodeIDs: map[PeerID]struct{}{id: {}},
			UseQUIC:      true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{id: peer},
	})
	if _, ok := sub.setRunCancel(func() {}); !ok {
		t.Fatal("mark subscription running")
	}
	t.Cleanup(func() {
		sub.stopRun()
		sub.broadcastReceiver.Close()
	})

	if sub.attachPooledPeer(&pooledPeer{id: id, route: route}, nil) {
		t.Fatal("existing peer was attached twice")
	}
	if route.ClaimBackgroundQUICDial() {
		route.ReleaseBackgroundQUICDial()
		t.Fatal("missing QUIC route did not schedule DHT and connection prewarm")
	}

	cancel()
	node.wg.Wait()
	if !route.ClaimBackgroundQUICDial() {
		t.Fatal("cancelled QUIC prewarm retained the background dial claim")
	}
	route.ReleaseBackgroundQUICDial()
}
