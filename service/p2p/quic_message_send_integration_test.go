//go:build integration

package p2p

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
)

func TestPrivateQUICMessageDeadlineDoesNotAbortSharedDial(t *testing.T) {
	remote, err := adnlquic.NewGateway(quicOutboundTestKey(t))
	if err != nil {
		t.Fatal(err)
	}
	messages := make(chan []byte, 2)
	remote.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		peer.SetMessageHandler(func(_ context.Context, payload []byte) {
			messages <- bytes.Clone(payload)
		})
		return nil
	})
	addr := startQUICOutboundTestGateway(t, remote)
	endpoint, err := netip.ParseAddrPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remote.PublicKey())

	node := newTestNode(t)
	startQUICOutboundTestGateway(t, node.quicGateway)
	runCtx, cancelRun := context.WithCancel(t.Context())
	node.runCtx = runCtx
	t.Cleanup(func() {
		cancelRun()
		node.wg.Wait()
	})
	routeDHT := &blockingOutboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{adnladdr.QUIC{
				IP:   net.IP(endpoint.Addr().AsSlice()),
				Port: int32(endpoint.Port()),
			}},
			ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
		},
		pub:     remote.PublicKey(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	node.dht = routeDHT
	peer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remote.PublicKey(),
		route: newTestPeerRoute(""),
	}
	overlayID := testPeerID("private-message-cold-dial")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "private-message-cold-dial",
			Kind:    overlayKindCustomFixed,
			ShortID: overlayID[:],
			UseQUIC: true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{remoteID: peer},
	})
	t.Cleanup(sub.broadcastReceiver.Close)
	handle := &PrivateOverlay{sub: sub}

	shortCtx, cancelShort := context.WithTimeout(t.Context(), time.Second)
	defer cancelShort()
	first := make(chan error, 1)
	go func() { first <- handle.SendMessageRaw(shortCtx, remoteID, []byte{1, 2, 3, 4}) }()
	select {
	case <-routeDHT.started:
	case <-time.After(3 * time.Second):
		t.Fatal("cold message did not start route discovery")
	}
	if err := <-first; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first message = %v, want deadline exceeded", err)
	}
	if !peer.route.QUICDialInFlight() {
		t.Fatal("message deadline aborted the shared connection attempt")
	}
	if peer.route.QUICAddressStale() {
		t.Fatal("message deadline marked the peer route stale")
	}

	// A newer vote can use the same attempt, even after the first vote expired.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	second := make(chan error, 1)
	body := []byte{5, 6, 7, 8}
	go func() { second <- handle.SendMessageRaw(ctx, remoteID, body) }()
	close(routeDHT.release)
	if err := <-second; err != nil {
		t.Fatalf("second message on shared connection: %v", err)
	}
	select {
	case got := <-messages:
		if !bytes.HasSuffix(got, body) {
			t.Fatalf("delivered message = %x, want newer body %x", got, body)
		}
	case <-ctx.Done():
		t.Fatalf("second message was not delivered: %v", ctx.Err())
	}
	if calls := routeDHT.calls.Load(); calls != 1 {
		t.Fatalf("DHT lookups = %d, want one shared dial", calls)
	}
}
