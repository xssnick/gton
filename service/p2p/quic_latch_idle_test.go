package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
)

// Every overlay of the node writes to a peer over one QUIC connection, so a
// relay write attached through one overlay defers to this node's candidate
// symbol written to the same peer through another: a shard session's candidate
// is not reordered behind the masterchain session's relays. Both attachments
// are built the production way, from the one pooled transport of the peer.
func TestRelayWriteDefersToPriorityWriteOfAnotherOverlay(t *testing.T) {
	limits := adnlquic.DefaultLimits()
	limits.MaxIncomingStreams = 1
	hold := make(chan struct{})
	fx := newPrioritySendTestPeerWithRemote(t, prioritySendTestRemote{limits: limits, hold: hold})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold) }) }
	t.Cleanup(release)

	transport := newTestOverlayADNL()
	transport.id = fx.peer.id.Bytes()
	transport.pub = fx.peer.pub
	pooled, _, err := fx.node.pool.wrap(transport)
	if err != nil {
		t.Fatalf("wrap the peer's ADNL transport: %v", err)
	}
	t.Cleanup(pooled.close)

	attach := func(name string) (*overlaySubscription, *overlayPeer) {
		overlayID := testPeerID(name)
		sub := testOverlaySubscription(&overlaySubscription{
			node: fx.node,
			spec: overlaySpec{
				Name:    name,
				Kind:    overlayKindPrivate,
				ShortID: overlayID[:],
				UseQUIC: true,
			},
			log:   discardLogger(),
			peers: map[PeerID]*overlayPeer{},
		})
		t.Cleanup(sub.broadcastReceiver.Close)

		peer, attachErr := sub.newOverlayPeer(pooled, nil, true)
		if attachErr != nil {
			t.Fatalf("attach the peer to %s: %v", name, attachErr)
		}
		t.Cleanup(peer.release)
		return sub, peer
	}
	shard, shardPeer := attach("private.priority-send-shard-session")
	master, masterPeer := attach("private.priority-send-master-session")
	priority := quicPriorityBroadcastPeer{route: quicRouteBroadcastPeer{peer: shardPeer, envelope: shard.quicEnvelope}}
	relay := quicRouteBroadcastPeer{peer: masterPeer, envelope: master.quicEnvelope}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	occupier := []byte("message the remote is still handling")
	if err = relay.SendPreparedCustomMessage(ctx, occupier); err != nil {
		t.Fatalf("write occupying the remote's stream credit: %v", err)
	}
	fx.expectSilence(t, 20*time.Millisecond, "the remote let the held message through")

	// The shard session's candidate write waits for stream credit with its
	// latch raised.
	candidateBody := []byte("own candidate symbol of the shard session")
	candidateDone := make(chan error, 1)
	go func() { candidateDone <- priority.SendPreparedCustomMessage(ctx, candidateBody) }()
	deadline := time.Now().Add(2 * time.Second)
	for !shardPeer.prioritySend.raised.Load() {
		if time.Now().After(deadline) {
			t.Fatal("candidate write waiting for stream credit did not raise the latch")
		}
		time.Sleep(time.Millisecond)
	}

	// The masterchain session relays to the same peer meanwhile and defers.
	relayBody := []byte("relayed symbol of the masterchain leader")
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.SendPreparedCustomMessage(ctx, relayBody) }()
	waits := fx.awaitWaits(t, 1)
	if waits[0].Result != prioritySendWaitBounded || waits[0].Duration < quicPrioritySendWaitBound {
		t.Fatalf("relay wait behind the other overlay's candidate write = %+v, want %q of at least %v", waits[0], prioritySendWaitBounded, quicPrioritySendWaitBound)
	}

	release()
	fx.expectMessage(t, occupier, "held message after the remote was released")
	fx.expectMessage(t, candidateBody, "candidate write after the stream credit returned")
	if err = <-candidateDone; err != nil {
		t.Fatalf("candidate write after the stream credit returned: %v", err)
	}
	fx.expectMessage(t, relayBody, "deferred relay write of the other overlay")
	if err = <-relayDone; err != nil {
		t.Fatalf("deferred relay write of the other overlay: %v", err)
	}
	if shardPeer.prioritySend.raised.Load() || masterPeer.prioritySend.raised.Load() {
		t.Fatal("latch stayed raised after the candidate write returned")
	}
}

// A path this node dialed is idle only when nothing crosses it in either
// direction. The remote's own messages arrive on the same managed peer as our
// sends, and closing it would drop a peer that is actively feeding us.
func TestSweepKeepsDialedPathTheRemoteIsSendingOver(t *testing.T) {
	remote, err := adnlquic.NewGateway(quicOutboundTestKey(t))
	if err != nil {
		t.Fatalf("create remote QUIC gateway: %v", err)
	}
	remotePaths := make(chan *adnlquic.Peer, 1)
	remote.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		remotePaths <- peer
		return nil
	})
	remoteAddr := startQUICOutboundTestGateway(t, remote)
	remoteID, err := NewPeerID(remote.ID())
	if err != nil {
		t.Fatalf("parse remote peer id: %v", err)
	}

	node := newTestNode(t)
	node.quicGateway.SetConnectionHandler(node.handleInboundQUICPeer)
	startQUICOutboundTestGateway(t, node.quicGateway)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remote.PublicKey(),
		route: newTestPeerRoute(remoteAddr),
	}
	if _, err = dialer.dialQUIC(ctx); err != nil {
		t.Fatalf("dial the remote: %v", err)
	}
	path, err := node.authenticatedQUICPeer(remoteID)
	if err != nil {
		t.Fatalf("lookup the dialed path: %v", err)
	}
	if !path.outbound {
		t.Fatal("dialed path is not marked outbound")
	}

	// Dialed long ago and never used by this node since.
	path.registeredAt = time.Now().Add(-quicIdleTTL - time.Minute)
	if victims := selectIdleQUICPaths([]*authenticatedQUICPeer{path}, 1, time.Now()); len(victims) != 1 {
		t.Fatal("a dialed path nothing crossed is not idle")
	}

	var remotePath *adnlquic.Peer
	select {
	case remotePath = <-remotePaths:
	case <-ctx.Done():
		t.Fatal("remote did not register the dialed path")
	}
	overlayID := testPeerID("remote-feeds-dialed-path")
	envelope, err := newQUICOverlayEnvelope(overlayID[:], nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	payload, err := envelope.Message(overlay.Ping{})
	if err != nil {
		t.Fatalf("build the remote's message: %v", err)
	}
	if err = remotePath.SendMessage(ctx, payload); err != nil {
		t.Fatalf("remote sends over the path we dialed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(selectIdleQUICPaths([]*authenticatedQUICPeer{path}, 1, time.Now())) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the remote's message did not make the dialed path active")
		}
		time.Sleep(time.Millisecond)
	}
	if closed := node.sweepIdleQUICPaths(time.Now()); closed != 0 {
		t.Fatalf("sweep closed %d paths the remote is sending over", closed)
	}
	if _, err = node.quicGateway.OutboundPeerDefaultID(remoteID[:]); err != nil {
		t.Fatalf("dialed path after the sweep: %v", err)
	}
}
