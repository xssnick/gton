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

// The source fan-out must never dial: a peer without an established
// connection fails immediately with a background dial requested, so one cold
// peer costs its own part and nothing else. See quicRouteBroadcastPeer.
func TestTwoStepQUICSendSkipsUnconnectedPeerWithoutDialing(t *testing.T) {
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

	warmCtx, cancelWarm := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err = fastPeer.dialQUIC(warmCtx); err != nil {
		t.Fatalf("warm the fast peer: %v", err)
	}
	cancelWarm()

	peerSet := sub.resolveTwoStepPeerSet(PeerID{})
	if len(peerSet) != 2 {
		t.Fatalf("resolved peers = %d, want two", len(peerSet))
	}

	// No peer send budget at all, exactly as the source path sends: if the
	// fan-out waited for the cold peer it would hang on the blocked DHT until
	// this context expires.
	sendCtx, cancelSend := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelSend()
	result, sendErr := overlay.SendBroadcastTwoStep(sendCtx, overlay.BroadcastTwoStepSendRequest{
		Key:         node.privKey,
		Certificate: overlay.CertificateEmpty{},
		LocalADNLID: node.localID.Bytes(),
		Payload:     []byte("no inline dial"),
		PeerSet:     peerSet,
	}, overlay.WithBroadcastTwoStepDate(123))

	if !slowPeer.route.QUICDialInFlight() {
		t.Fatal("two-step send waited for the cold peer's background dial")
	}
	if result.Sent != 1 || len(result.Failed) != 1 {
		t.Fatalf("two-step result = %+v, want one delivery and one skipped peer", result)
	}
	if !errors.Is(result.Failed[0].Err, errQUICPeerOffline) {
		t.Fatalf("cold peer error = %v, want %v", result.Failed[0].Err, errQUICPeerOffline)
	}
	if sendErr == nil {
		t.Fatal("skipped peer was not reported to the caller")
	}

	outcome := sub.twoStepSendOutcome(result)
	if outcome.Sent != 1 || outcome.Faults[TwoStepSendFaultOffline] != 1 {
		t.Fatalf("classified outcome = %+v, want one sent and one offline", outcome)
	}
	for fault, count := range outcome.Faults {
		if TwoStepSendFault(fault) != TwoStepSendFaultOffline && count != 0 {
			t.Fatalf("outcome charged fault %d to the peer: %+v", fault, outcome)
		}
	}
	if sub.peerByID(slowID) == nil {
		t.Fatal("a peer this node never connected to was evicted by its own send")
	}

	select {
	case <-fastMessages:
	case <-time.After(2 * time.Second):
		t.Fatal("connected peer did not receive the two-step broadcast")
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
