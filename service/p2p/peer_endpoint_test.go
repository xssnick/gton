package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type recordingNetManager struct {
	reads  chan *adnl.UDPPacket
	writes chan string
}

func newRecordingNetManager() *recordingNetManager {
	return &recordingNetManager{
		reads:  make(chan *adnl.UDPPacket),
		writes: make(chan string, 16),
	}
}

func (*recordingNetManager) Free(*adnl.UDPPacket) {}

func (m *recordingNetManager) GetReaderChan(*adnl.Gateway) <-chan *adnl.UDPPacket {
	return m.reads
}

func (*recordingNetManager) InitConnection(*adnl.Gateway, string) error { return nil }

func (*recordingNetManager) CloseConnection(*adnl.Gateway) {}

func (m *recordingNetManager) WritePacket(_ *adnl.Gateway, packet []byte, addr net.Addr) (int, error) {
	m.writes <- addr.String()
	return len(packet), nil
}

func (*recordingNetManager) Close() {}

type peerEndpointFixture struct {
	node    *Node
	gateway *adnl.Gateway
	manager *recordingNetManager
	peer    *pooledPeer
	pub     ed25519.PublicKey
}

func newPeerEndpointFixture(t *testing.T, endpoint peerEndpoint) *peerEndpointFixture {
	t.Helper()

	_, gatewayKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate gateway key: %v", err)
	}
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}

	manager := newRecordingNetManager()
	gateway := adnl.NewGatewayWithNetManager(gatewayKey, manager)
	gateway.SetAddressList(nil)
	node := &Node{
		pool:    newPeerPool(gateway, nil, nil, nil),
		peerUse: map[PeerID]peerUse{},
	}

	rawID, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		t.Fatalf("hash peer key: %v", err)
	}
	peerID, err := NewPeerID(rawID)
	if err != nil {
		t.Fatalf("parse peer id: %v", err)
	}
	peer, release, err := node.acquirePeerEndpoint(peerID, endpoint, pub)
	if err != nil {
		t.Fatalf("acquire initial endpoint: %v", err)
	}
	if !node.pool.retain(peer) {
		t.Fatal("retain initial endpoint")
	}
	release()

	fixture := &peerEndpointFixture{
		node:    node,
		gateway: gateway,
		manager: manager,
		peer:    peer,
		pub:     pub,
	}
	t.Cleanup(func() {
		node.pool.releaseRetained(peer)
		_ = gateway.Close()
	})
	return fixture
}

func (f *peerEndpointFixture) sendRoute(t *testing.T) string {
	t.Helper()

	if err := f.peer.adnl.SendCustomMessage(context.Background(), overlay.Ping{}); err != nil {
		t.Fatalf("send endpoint probe: %v", err)
	}
	select {
	case endpoint := <-f.manager.writes:
		return endpoint
	case <-time.After(time.Second):
		t.Fatal("endpoint probe was not written")
		return ""
	}
}

func TestAcquirePeerEndpointKeepsCanonicalAddress(t *testing.T) {
	canonical := peerEndpoint{
		adnlAddr: "127.0.0.1:21001",
		quicAddr: "127.0.0.1:22001",
	}
	announced := peerEndpoint{
		adnlAddr: "127.0.0.1:21002",
		quicAddr: "127.0.0.1:22002",
	}
	fixture := newPeerEndpointFixture(t, canonical)

	if route := fixture.sendRoute(t); route != canonical.adnlAddr {
		t.Fatalf("initial route = %s, want %s", route, canonical.adnlAddr)
	}

	peer, release, err := fixture.node.acquirePeerEndpoint(fixture.peer.id, announced, fixture.pub)
	if err != nil {
		t.Fatalf("reuse canonical endpoint: %v", err)
	}
	if peer != fixture.peer {
		t.Fatal("same PeerID returned a different pooled peer")
	}
	release()
	release()

	if peer.addr != canonical.adnlAddr {
		t.Fatalf("committed ADNL endpoint = %s, want %s", peer.addr, canonical.adnlAddr)
	}
	if peer.route.QUICAddress() != canonical.quicAddr {
		t.Fatalf("committed QUIC endpoint = %s, want %s", peer.route.QUICAddress(), canonical.quicAddr)
	}
	if route := fixture.sendRoute(t); route != canonical.adnlAddr {
		t.Fatalf("route after alternate announcement = %s, want %s", route, canonical.adnlAddr)
	}
}

func TestAcquirePeerEndpointReusesCanonicalAddressWhileDownloading(t *testing.T) {
	fixture := newPeerEndpointFixture(t, peerEndpoint{
		adnlAddr: "127.0.0.1:21101",
		quicAddr: "127.0.0.1:22101",
	})
	overlayPeer := &overlayPeer{id: fixture.peer.id}
	releaseDownload := fixture.node.acquireDownloadPeer(overlayPeer)
	defer releaseDownload()

	peer, release, err := fixture.node.acquirePeerEndpoint(fixture.peer.id, peerEndpoint{
		adnlAddr: "127.0.0.1:21102",
		quicAddr: "127.0.0.1:22102",
	}, fixture.pub)
	if err != nil {
		t.Fatalf("reuse endpoint during download: %v", err)
	}
	if peer != fixture.peer {
		t.Fatal("download-time reuse returned a different pooled peer")
	}
	release()

	fixture.node.peerUseMx.RLock()
	use := fixture.node.peerUse[fixture.peer.id]
	fixture.node.peerUseMx.RUnlock()
	if use.downloads != 1 || use.queries != 0 || use.endpointPending != nil {
		t.Fatalf("peer use after endpoint release = %+v", use)
	}
}

func TestAcquireArchivePeerEndpointDoesNotEnterLiveAccounting(t *testing.T) {
	fixture := newPeerEndpointFixture(t, peerEndpoint{
		adnlAddr: "127.0.0.1:21151",
		quicAddr: "127.0.0.1:22151",
	})

	peer, release, err := fixture.node.acquireArchivePeerEndpoint(fixture.peer.id, "127.0.0.1:21152", fixture.pub)
	if err != nil {
		t.Fatalf("acquire archive endpoint: %v", err)
	}
	defer release()
	if peer != fixture.peer {
		t.Fatal("archive endpoint did not reuse the canonical pooled transport")
	}

	fixture.node.peerUseMx.RLock()
	_, tracked := fixture.node.peerUse[fixture.peer.id]
	fixture.node.peerUseMx.RUnlock()
	if tracked {
		t.Fatal("archive endpoint entered ordinary live peer accounting")
	}
	if _, protected := fixture.node.protectedPeerIDs()[fixture.peer.id]; protected {
		t.Fatal("archive endpoint protected the peer in live selection")
	}
}

func TestNewOverlayPeerSharesPooledImmutableRoutingData(t *testing.T) {
	fixture := newPeerEndpointFixture(t, peerEndpoint{
		adnlAddr: "127.0.0.1:21201",
		quicAddr: "127.0.0.1:22201",
	})
	overlayID := testPeerID("shared-pooled-routing-overlay")
	sub := testOverlaySubscription(&overlaySubscription{
		node: fixture.node,
		spec: overlaySpec{
			ShortID: overlayID[:],
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})

	peer, err := sub.newOverlayPeer(fixture.peer, nil, false)
	if err != nil {
		t.Fatalf("create overlay peer: %v", err)
	}
	t.Cleanup(func() {
		peer.close()
		sub.broadcastReceiver.Close()
	})

	if len(peer.pub) != ed25519.PublicKeySize {
		t.Fatalf("overlay peer public key size = %d", len(peer.pub))
	}
	if &peer.pub[0] != &fixture.peer.pub[0] {
		t.Fatal("overlay peer duplicated the pooled immutable public key")
	}
	if peer.route != fixture.peer.route {
		t.Fatal("overlay peer did not share the pooled peer route")
	}
	if peer.route.QUICAddress() != fixture.peer.route.QUICAddress() {
		t.Fatalf("overlay QUIC route = %q, want %q", peer.route.QUICAddress(), fixture.peer.route.QUICAddress())
	}
	if &peer.overlayID[0] != &sub.spec.ShortID[0] {
		t.Fatal("overlay peer did not retain the canonical subscription overlay id")
	}
}

func TestPeerPoolStaleDisconnectKeepsReplacement(t *testing.T) {
	peerID := testPeerID("reconnected-peer")
	oldPeer := &pooledPeer{id: peerID}
	replacement := &pooledPeer{id: peerID}
	pool := &peerPool{peers: map[PeerID]*pooledPeer{peerID: replacement}}

	pool.removeDisconnected(peerID, oldPeer)
	if pool.peers[peerID] != replacement {
		t.Fatal("stale disconnect removed replacement transport")
	}

	pool.removeDisconnected(peerID, replacement)
	if pool.peers[peerID] != nil {
		t.Fatal("current disconnect left transport registered")
	}
}
