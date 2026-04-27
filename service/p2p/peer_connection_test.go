package p2p

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type testOverlayADNL struct {
	closerCtx         context.Context
	closeFn           context.CancelFunc
	customHandler     func(msg *adnl.MessageCustom) error
	queryHandler      func(msg *adnl.MessageQuery) error
	disconnectHandler func(addr string, key ed25519.PublicKey)
}

func newTestOverlayADNL() *testOverlayADNL {
	ctx, cancel := context.WithCancel(context.Background())
	return &testOverlayADNL{
		closerCtx: ctx,
		closeFn:   cancel,
	}
}

func (m *testOverlayADNL) SetCustomMessageHandler(handler func(msg *adnl.MessageCustom) error) {
	m.customHandler = handler
}

func (m *testOverlayADNL) SetQueryHandler(handler func(msg *adnl.MessageQuery) error) {
	m.queryHandler = handler
}

func (m *testOverlayADNL) SetDisconnectHandler(handler func(addr string, key ed25519.PublicKey)) {
	m.disconnectHandler = handler
}

func (m *testOverlayADNL) GetDisconnectHandler() func(addr string, key ed25519.PublicKey) {
	return m.disconnectHandler
}

func (m *testOverlayADNL) SendCustomMessage(context.Context, tl.Serializable) error {
	return nil
}

func (m *testOverlayADNL) Query(context.Context, tl.Serializable, tl.Serializable) error {
	return nil
}

func (m *testOverlayADNL) Answer(context.Context, []byte, tl.Serializable) error {
	return nil
}

func (m *testOverlayADNL) GetCloserCtx() context.Context {
	return m.closerCtx
}

func (m *testOverlayADNL) RemoteAddr() string {
	return "127.0.0.1:17555"
}

func (m *testOverlayADNL) GetID() []byte {
	return []byte("test-peer")
}

func (m *testOverlayADNL) Close() {
	m.closeFn()
}

func newTestOverlayWrapper() (*overlay.ADNLOverlayWrapper, *testOverlayADNL) {
	base := newTestOverlayADNL()
	wrapper := overlay.CreateExtendedADNL(base).CreateOverlayWithSettings([]byte{1}, 0, true, false)
	return wrapper, base
}

func TestQueryCandidatesSkipClosedPeers(t *testing.T) {
	now := int32(time.Now().Unix())

	openOverlay, _ := newTestOverlayWrapper()
	closedOverlay, closedConn := newTestOverlayWrapper()
	closedConn.Close()
	fallbackOverlay, _ := newTestOverlayWrapper()

	sub := &overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[string]*overlayPeer{
			"peer-1": {id: "peer-1", overlay: openOverlay, announced: &overlay.Node{Version: now}, alive: true},
			"peer-2": {id: "peer-2", overlay: closedOverlay, announced: &overlay.Node{Version: now}, alive: true},
			"peer-3": {id: "peer-3", overlay: fallbackOverlay, announced: &overlay.Node{Version: now}, alive: true},
		},
		neighbours: []string{"peer-1", "peer-2"},
	}

	got := sub.queryCandidates(0, 0)
	if len(got) != 2 {
		t.Fatalf("unexpected candidate count: got %d want 2", len(got))
	}
	if got[0].id != "peer-1" {
		t.Fatalf("expected open neighbour first, got %q", got[0].id)
	}
	if got[1].id != "peer-3" {
		t.Fatalf("expected open fallback peer second, got %q", got[1].id)
	}
}

func TestHandlePeerQueryFailureRemovesClosedPeer(t *testing.T) {
	now := int32(time.Now().Unix())

	peerOverlay, _ := newTestOverlayWrapper()
	peer := &overlayPeer{
		id:        "peer-1",
		overlay:   peerOverlay,
		announced: &overlay.Node{Version: now},
		alive:     true,
	}

	sub := &overlaySubscription{
		log:        discardLogger(),
		peers:      map[string]*overlayPeer{"peer-1": peer},
		neighbours: []string{"peer-1"},
	}

	sub.handlePeerQueryFailure(peer, adnl.ErrPeerConnClosed)

	if _, ok := sub.peers["peer-1"]; ok {
		t.Fatal("closed peer must be removed from subscription")
	}
	if len(sub.neighbours) != 0 {
		t.Fatalf("closed peer must be removed from neighbours, got %d", len(sub.neighbours))
	}
}
