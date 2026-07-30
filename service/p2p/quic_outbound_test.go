package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
)

type outboundRouteDHT struct {
	dhtBackend

	addresses *adnladdr.List
	pub       ed25519.PublicKey
	calls     atomic.Int32
}

type blockingOutboundRouteDHT struct {
	dhtBackend

	addresses *adnladdr.List
	pub       ed25519.PublicKey
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	calls     atomic.Int32
}

func (d *blockingOutboundRouteDHT) FindAddresses(
	ctx context.Context,
	_ []byte,
) (*adnladdr.List, ed25519.PublicKey, error) {
	d.calls.Add(1)
	d.startOnce.Do(func() {
		close(d.started)
	})

	select {
	case <-d.release:
		return d.addresses, d.pub, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (d *outboundRouteDHT) FindAddresses(
	context.Context,
	[]byte,
) (*adnladdr.List, ed25519.PublicKey, error) {
	d.calls.Add(1)
	return d.addresses, d.pub, nil
}

func TestStaleQUICRouteRefreshesTheSignedAddressList(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)
	dht := &outboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.UDP{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 24002,
				},
				adnladdr.QUIC{
					IP:   net.IPv4(127, 0, 0, 2),
					Port: 25003,
				},
			},
		},
		pub: remotePub,
	}
	node := newTestNode(t)
	node.dht = dht
	route := newPeerRoute("127.0.0.1:25001")

	addr, err := resolveQUICRoute(
		context.Background(),
		node,
		remoteID,
		remotePub,
		route,
	)
	if err != nil {
		t.Fatalf("reuse current QUIC route: %v", err)
	}
	if addr != "127.0.0.1:25001" || dht.calls.Load() != 0 {
		t.Fatalf("current route = %q with %d DHT lookups", addr, dht.calls.Load())
	}

	route.markQUICAddrStale()
	addr, err = resolveQUICRoute(
		context.Background(),
		node,
		remoteID,
		remotePub,
		route,
	)
	if err != nil {
		t.Fatalf("refresh stale QUIC route: %v", err)
	}
	if addr != "127.0.0.2:25003" {
		t.Fatalf("refreshed QUIC route = %q, want explicit address", addr)
	}
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("stale route DHT lookups = %d, want 1", got)
	}
	if route.quicAddrStale() {
		t.Fatal("validated DHT refresh left the QUIC route stale")
	}

	route.markQUICAddrStale()
	addr, err = resolveQUICRoute(
		context.Background(),
		node,
		remoteID,
		remotePub,
		route,
	)
	if err != nil {
		t.Fatalf("reuse recently checked DHT route: %v", err)
	}
	if addr != "127.0.0.2:25003" {
		t.Fatalf("recent DHT route = %q, want explicit address", addr)
	}
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("recent stale route DHT lookups = %d, want 1", got)
	}

	node.peerAddresses.mx.Lock()
	entry := node.peerAddresses.cache[remoteID]
	entry.cachedAt = time.Now().Add(-quicAddressCacheMaxAge)
	node.peerAddresses.cache[remoteID] = entry
	node.peerAddresses.mx.Unlock()

	route.markQUICAddrStale()
	if _, err = resolveQUICRoute(
		context.Background(),
		node,
		remoteID,
		remotePub,
		route,
	); err != nil {
		t.Fatalf("refresh old cached DHT route: %v", err)
	}
	if got := dht.calls.Load(); got != 2 {
		t.Fatalf("old stale route DHT lookups = %d, want 2", got)
	}
}

func TestDHTAddressLookupSuppliesBothPeerRoutes(t *testing.T) {
	remotePub, remoteKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)
	addresses := &adnladdr.List{
		Addresses: []adnladdr.Address{
			adnladdr.UDP{
				IP:   net.IPv4(127, 0, 0, 1),
				Port: 24001,
			},
			adnladdr.QUIC{
				IP:   net.IPv4(127, 0, 0, 1),
				Port: 25001,
			},
		},
	}

	tests := []struct {
		name    string
		connect func(*overlaySubscription) (bool, error)
		spec    func() overlaySpec
	}{
		{
			name: "public overlay node",
			spec: func() overlaySpec {
				spec, buildErr := buildOverlaySpec(
					make([]byte, PeerIDSize),
					-1,
					topShard,
					"quic-route-public",
				)
				if buildErr != nil {
					t.Fatalf("build public overlay: %v", buildErr)
				}
				return spec
			},
			connect: func(sub *overlaySubscription) (bool, error) {
				node, nodeErr := overlay.NewNode(sub.spec.FullID, remoteKey)
				if nodeErr != nil {
					return false, nodeErr
				}
				return sub.connectOverlayNodeV1(context.Background(), *node)
			},
		},
		{
			name: "fixed overlay node",
			spec: func() overlaySpec {
				shortID := testPeerID("quic-route-fixed-overlay")
				return overlaySpec{
					Name:         "quic-route-fixed",
					Kind:         overlayKindCustomFixed,
					ShortID:      shortID[:],
					FixedNodes:   []PeerID{remoteID},
					FixedNodeIDs: map[PeerID]struct{}{remoteID: {}},
				}
			},
			connect: func(sub *overlaySubscription) (bool, error) {
				return sub.connectFixedNode(context.Background(), remoteID)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dht := &outboundRouteDHT{
				addresses: addresses,
				pub:       remotePub,
			}
			node := newQUICRouteDiscoveryNode(t, dht)
			sub := testOverlaySubscription(&overlaySubscription{
				node:  node,
				spec:  test.spec(),
				log:   discardLogger(),
				peers: map[PeerID]*overlayPeer{},
			})
			t.Cleanup(func() {
				sub.close()
				node.wg.Wait()
			})

			attached, err := test.connect(sub)
			if err != nil {
				t.Fatalf("connect peer: %v", err)
			}
			if !attached {
				t.Fatal("peer was not attached")
			}
			if got := dht.calls.Load(); got != 1 {
				t.Fatalf("DHT address lookups = %d, want 1", got)
			}

			peer := sub.peerByID(remoteID)
			if peer == nil {
				t.Fatal("attached peer is missing")
			}
			if peer.addr != "127.0.0.1:24001" {
				t.Fatalf("ADNL route = %q", peer.addr)
			}
			if peer.route.quicAddr() != "127.0.0.1:25001" {
				t.Fatalf("QUIC route = %q", peer.route.quicAddr())
			}
		})
	}
}

func TestFixedQUICOverlayDiscoversRouteForAttachedInboundPeerOnce(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)
	dht := &blockingOutboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.UDP{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 24011,
				},
				adnladdr.QUIC{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 25011,
				},
			},
		},
		pub:     remotePub,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	node := newQUICRouteDiscoveryNode(t, dht)
	overlayID := testPeerID("fixed-inbound-quic-route")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "custom.fixed-inbound-quic-route",
			Kind:         overlayKindCustomFixed,
			ShortID:      overlayID[:],
			FixedNodes:   []PeerID{remoteID},
			FixedNodeIDs: map[PeerID]struct{}{remoteID: {}},
			UseQUIC:      true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	t.Cleanup(func() {
		sub.close()
		node.wg.Wait()
	})

	inbound := newTestOverlayADNL()
	inbound.id = remoteID.Bytes()
	inbound.pub = remotePub
	inbound.remoteAddr = "198.51.100.20:31000"
	pooled, fresh, err := node.pool.wrap(inbound)
	if err != nil {
		t.Fatalf("wrap inbound ADNL peer: %v", err)
	}
	if !fresh {
		t.Fatal("inbound ADNL peer unexpectedly reused an existing transport")
	}
	if !sub.attachPooledPeer(pooled, nil) {
		t.Fatal("attach inbound fixed peer")
	}

	peer := sub.peerByID(remoteID)
	if peer == nil {
		t.Fatal("attached inbound peer is missing")
	}
	if peer.route != pooled.route {
		t.Fatal("attached peer does not share its pooled route")
	}
	if peer.route.quicAddr() != "" {
		t.Fatalf("inbound peer unexpectedly derived QUIC route %q", peer.route.quicAddr())
	}

	sub.startSeedFromFixedNodes(context.Background())
	sub.startSeedFromFixedNodes(context.Background())
	select {
	case <-dht.started:
	case <-time.After(time.Second):
		t.Fatal("fixed route DHT lookup did not start")
	}
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("concurrent DHT address lookups = %d, want 1", got)
	}
	close(dht.release)
	node.wg.Wait()

	if peer.addr != inbound.remoteAddr {
		t.Fatalf("attached ADNL route changed to %q, want inbound route %q", peer.addr, inbound.remoteAddr)
	}
	if got := peer.route.quicAddr(); got != "127.0.0.1:25011" {
		t.Fatalf("discovered QUIC route = %q, want DHT route", got)
	}

	sub.startSeedFromFixedNodes(context.Background())
	node.wg.Wait()
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("DHT address lookups after route discovery = %d, want 1", got)
	}
}

func TestFixedNodeSeedResolvesPeersInParallel(t *testing.T) {
	const peerCount = dhtSeedConnectParallelism

	dht := &blockingOutboundRouteDHT{
		addresses: &adnladdr.List{},
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	node := newQUICRouteDiscoveryNode(t, dht)

	fixedNodes := make([]PeerID, 0, peerCount)
	fixedNodeIDs := make(map[PeerID]struct{}, peerCount)
	for i := range peerCount {
		id := testPeerID(fmt.Sprintf("parallel-fixed-seed-%d", i))
		fixedNodes = append(fixedNodes, id)
		fixedNodeIDs[id] = struct{}{}
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "parallel-fixed-seed",
			Kind:         overlayKindCustomFixed,
			FixedNodes:   fixedNodes,
			FixedNodeIDs: fixedNodeIDs,
			UseQUIC:      true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	t.Cleanup(func() {
		sub.close()
		node.wg.Wait()
	})

	done := make(chan struct{})
	go func() {
		sub.seedFromFixedNodes(context.Background())
		close(done)
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for dht.calls.Load() < peerCount {
		select {
		case <-ticker.C:
		case <-deadline.C:
			close(dht.release)
			<-done
			t.Fatalf(
				"concurrent DHT lookups = %d, want %d",
				dht.calls.Load(),
				peerCount,
			)
		}
	}

	close(dht.release)
	<-done
}

func TestResolveQUICRouteUsesCachedAddressWithoutDHT(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)
	dht := &outboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.QUIC{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 25012,
				},
			},
		},
		pub: remotePub,
	}
	peer := &overlayPeer{
		node:  &Node{dht: dht},
		id:    remoteID,
		pub:   remotePub,
		route: newPeerRoute("127.0.0.1:25011"),
	}

	addr, err := resolveQUICRoute(
		context.Background(),
		peer.node,
		peer.id,
		peer.pub,
		peer.route,
	)
	if err != nil {
		t.Fatalf("resolve QUIC route: %v", err)
	}
	if addr != "127.0.0.1:25011" {
		t.Fatalf("resolved QUIC route = %q", addr)
	}
	if got := dht.calls.Load(); got != 0 {
		t.Fatalf("DHT address lookups = %d, want 0", got)
	}
}

func TestResolveQUICRouteRejectsDifferentPeerIdentity(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate other key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)
	dht := &outboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.QUIC{
					IP:   net.IPv4(127, 0, 0, 1),
					Port: 25012,
				},
			},
		},
		pub: otherPub,
	}
	peer := &overlayPeer{
		node:  &Node{dht: dht},
		id:    remoteID,
		pub:   remotePub,
		route: newPeerRoute(""),
	}

	if _, err = resolveQUICRoute(
		context.Background(),
		peer.node,
		peer.id,
		peer.pub,
		peer.route,
	); err == nil {
		t.Fatal("resolution accepted a different peer identity")
	}
	if got := peer.route.quicAddr(); got != "" {
		t.Fatalf("failed resolution stored QUIC route %q", got)
	}
}

func TestPeerEndpointRoutesAreSelectedIndependently(t *testing.T) {
	_, err := peerEndpointFromAddresses([]adnladdr.Address{
		adnladdr.QUIC{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 25002,
		},
	})
	if err == nil {
		t.Fatal("endpoint without an ADNL route was accepted")
	}

	addresses := []adnladdr.Address{
		adnladdr.UDP{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 24002,
		},
		adnladdr.QUIC{
			IP:   net.IP{1, 2, 3},
			Port: 25002,
		},
	}
	endpoint, err := peerEndpointFromAddresses(addresses)
	if err != nil {
		t.Fatalf("select ADNL endpoint independently of malformed QUIC route: %v", err)
	}
	if endpoint.adnlAddr != "127.0.0.1:24002" || endpoint.quicAddr != "" {
		t.Fatalf("selected endpoint = %+v", endpoint)
	}

	_, err = peerQUICRouteFromAddresses(addresses)
	if err == nil {
		t.Fatal("malformed QUIC route was accepted")
	}

	quicAddr, err := peerQUICRouteFromAddresses([]adnladdr.Address{
		adnladdr.UDP{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 24002,
		},
	})
	if err != nil {
		t.Fatalf("derive QUIC route from ADNL UDP endpoint: %v", err)
	}
	if quicAddr != "127.0.0.1:25002" {
		t.Fatalf("derived QUIC route = %q, want %q", quicAddr, "127.0.0.1:25002")
	}
}

func TestMalformedQUICRouteOnlyRejectsCustomQUICOverlay(t *testing.T) {
	remotePub, remoteKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)
	addresses := &adnladdr.List{
		Addresses: []adnladdr.Address{
			adnladdr.UDP{
				IP:   net.IPv4(127, 0, 0, 1),
				Port: 24003,
			},
			adnladdr.QUIC{
				IP:   net.IP{1, 2, 3},
				Port: 25003,
			},
		},
	}

	tests := []struct {
		name    string
		spec    func() overlaySpec
		connect func(*overlaySubscription) (bool, error)
		wantErr bool
	}{
		{
			name: "public overlay",
			spec: func() overlaySpec {
				spec, buildErr := buildOverlaySpec(
					make([]byte, PeerIDSize),
					-1,
					topShard,
					"optional-malformed-quic-public",
				)
				if buildErr != nil {
					t.Fatalf("build public overlay: %v", buildErr)
				}
				return spec
			},
			connect: func(sub *overlaySubscription) (bool, error) {
				node, nodeErr := overlay.NewNode(sub.spec.FullID, remoteKey)
				if nodeErr != nil {
					return false, nodeErr
				}
				return sub.connectOverlayNodeV1(context.Background(), *node)
			},
		},
		{
			name: "custom RLDP2 overlay",
			spec: func() overlaySpec {
				shortID := testPeerID("optional-malformed-quic-rldp")
				return overlaySpec{
					Name:         "custom.optional-malformed-quic-rldp",
					Kind:         overlayKindCustomFixed,
					ShortID:      shortID[:],
					FixedNodes:   []PeerID{remoteID},
					FixedNodeIDs: map[PeerID]struct{}{remoteID: {}},
				}
			},
			connect: func(sub *overlaySubscription) (bool, error) {
				return sub.connectFixedNode(context.Background(), remoteID)
			},
		},
		{
			name: "custom QUIC overlay",
			spec: func() overlaySpec {
				shortID := testPeerID("required-malformed-quic")
				return overlaySpec{
					Name:         "custom.required-malformed-quic",
					Kind:         overlayKindCustomFixed,
					ShortID:      shortID[:],
					FixedNodes:   []PeerID{remoteID},
					FixedNodeIDs: map[PeerID]struct{}{remoteID: {}},
					UseQUIC:      true,
				}
			},
			connect: func(sub *overlaySubscription) (bool, error) {
				return sub.connectFixedNode(context.Background(), remoteID)
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dht := &outboundRouteDHT{
				addresses: addresses,
				pub:       remotePub,
			}
			node := newQUICRouteDiscoveryNode(t, dht)
			sub := testOverlaySubscription(&overlaySubscription{
				node:  node,
				spec:  test.spec(),
				log:   discardLogger(),
				peers: map[PeerID]*overlayPeer{},
			})
			t.Cleanup(func() {
				sub.close()
				node.wg.Wait()
			})

			attached, connectErr := test.connect(sub)
			if test.wantErr {
				if connectErr == nil {
					t.Fatal("custom QUIC overlay accepted malformed QUIC route")
				}
				if attached {
					t.Fatal("custom QUIC overlay attached peer with malformed QUIC route")
				}
				return
			}
			if connectErr != nil {
				t.Fatalf("connect with optional malformed QUIC route: %v", connectErr)
			}
			if !attached {
				t.Fatal("peer was not attached")
			}

			peer := sub.peerByID(remoteID)
			if peer == nil {
				t.Fatal("attached peer is missing")
			}
			if peer.addr != "127.0.0.1:24003" {
				t.Fatalf("ADNL route = %q", peer.addr)
			}
			if peer.route.quicAddr() != "" {
				t.Fatalf("malformed optional QUIC route was retained as %q", peer.route.quicAddr())
			}
		})
	}
}

func TestQUICOutboundOperationsReuseGatewayPathAndFrameOverlay(t *testing.T) {
	overlayID := testPeerID("quic-outbound-overlay")
	var connections atomic.Int32
	var queries atomic.Int32
	messages := make(chan error, 2)

	serverKey := quicOutboundTestKey(t)
	server, err := adnlquic.NewGateway(serverKey)
	if err != nil {
		t.Fatalf("create QUIC server: %v", err)
	}
	server.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		connections.Add(1)
		peer.SetQueryHandler(func(_ context.Context, payload []byte) ([]byte, error) {
			var envelope overlay.Query
			req, parseErr := parseQUICOverlayObject(payload, &envelope)
			if parseErr != nil {
				return nil, parseErr
			}
			if !bytes.Equal(envelope.Overlay, overlayID[:]) {
				return nil, errors.New("unexpected query overlay id")
			}
			if _, ok := req.(overlay.Ping); !ok {
				return nil, fmt.Errorf("unexpected query type %T", req)
			}

			answer, serializeErr := tl.Serialize(overlay.Pong{}, true)
			if serializeErr != nil {
				return nil, serializeErr
			}
			if queries.Add(1) == 2 {
				answer = append(answer, 0, 0, 0, 0)
			}
			return answer, nil
		})
		peer.SetMessageHandler(func(_ context.Context, payload []byte) {
			var envelope overlay.Message
			req, parseErr := parseQUICOverlayObject(payload, &envelope)
			if parseErr == nil && !bytes.Equal(envelope.Overlay, overlayID[:]) {
				parseErr = errors.New("unexpected message overlay id")
			}
			if parseErr == nil {
				if _, ok := req.(overlay.Ping); !ok {
					parseErr = fmt.Errorf("unexpected message type %T", req)
				}
			}
			messages <- parseErr
		})
		return nil
	})
	serverAddr := startQUICOutboundTestGateway(t, server)

	node := newTestNode(t)
	t.Cleanup(func() {
		if err := node.quicGateway.Close(); err != nil {
			t.Errorf("close outbound QUIC gateway: %v", err)
		}
	})
	serverID, err := NewPeerID(server.ID())
	if err != nil {
		t.Fatalf("parse server id: %v", err)
	}
	peer := &overlayPeer{
		node:        node,
		id:          serverID,
		route:       newPeerRoute(serverAddr),
		pub:         server.PublicKey(),
		overlayID:   overlayID[:],
		fixedMember: true,
		alive:       true,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "custom.quic-outbound",
			Kind:    overlayKindCustomFixed,
			ShortID: overlayID[:],
			UseQUIC: true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{serverID: peer},
	})
	t.Cleanup(sub.broadcastReceiver.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queryTransport := quicPeerQueryTransport{
		peer:     peer,
		envelope: sub.quicEnvelope,
	}
	var result tl.Serializable
	if err = queryTransport.Query(ctx, 1024, overlay.Ping{}, &result); err != nil {
		t.Fatalf("typed QUIC query: %v", err)
	}
	if _, ok := result.(overlay.Pong); !ok {
		t.Fatalf("typed answer = %T, want overlay.Pong value", result)
	}

	result = nil
	if err = queryTransport.Query(ctx, 1024, overlay.Ping{}, &result); err == nil {
		t.Fatal("typed QUIC query accepted trailing answer bytes")
	}

	raw, err := queryTransport.QueryRaw(ctx, 1024, overlay.Ping{})
	if err != nil {
		t.Fatalf("raw QUIC query: %v", err)
	}
	var rawResult tl.Serializable
	rest, err := tl.ParseNoCopy(&rawResult, raw, true)
	if err != nil {
		t.Fatalf("parse raw answer: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("raw answer has %d trailing bytes", len(rest))
	}
	if _, ok := rawResult.(overlay.Pong); !ok {
		t.Fatalf("raw answer = %T, want overlay.Pong value", rawResult)
	}

	peerSet, failed := sub.resolveTwoStepPeerSet(ctx, PeerID{})
	if len(failed) != 0 {
		t.Fatalf("resolve QUIC two-step peers: %+v", failed)
	}
	if len(peerSet) != 1 {
		t.Fatalf("resolved QUIC two-step peers = %d, want 1", len(peerSet))
	}
	broadcastPeer, ok := peerSet[0].(quicRouteBroadcastPeer)
	if !ok {
		t.Fatalf("resolved two-step peer = %T, want concrete QUIC peer", peerSet[0])
	}
	if !bytes.Equal(broadcastPeer.ID(), serverID[:]) {
		t.Fatalf("broadcast peer id = %x, want %x", broadcastPeer.ID(), serverID)
	}
	if err = broadcastPeer.SendCustomMessage(ctx, overlay.Ping{}); err != nil {
		t.Fatalf("send QUIC broadcast message: %v", err)
	}
	select {
	case messageErr := <-messages:
		if messageErr != nil {
			t.Fatalf("parse QUIC broadcast message: %v", messageErr)
		}
	case <-ctx.Done():
		t.Fatal("QUIC broadcast message was not received")
	}

	if err = (quicRouteBroadcastPeer{
		peer:     peer,
		envelope: sub.quicEnvelope,
	}).SendCustomMessage(ctx, overlay.Ping{}); err != nil {
		t.Fatalf("send one-off QUIC message: %v", err)
	}
	select {
	case messageErr := <-messages:
		if messageErr != nil {
			t.Fatalf("parse one-off QUIC message: %v", messageErr)
		}
	case <-ctx.Done():
		t.Fatal("one-off QUIC message was not received")
	}

	if got := connections.Load(); got != 1 {
		t.Fatalf("QUIC path generations = %d, want 1", got)
	}
}

func TestOverlayPeerDialQUICDoesNotReuseInboundOnlyPath(t *testing.T) {
	node := newTestNode(t)
	inboundPath := make(chan *adnlquic.Peer, 1)
	node.quicGateway.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		inboundPath <- peer
		return nil
	})
	nodeAddr := startQUICOutboundTestGateway(t, node.quicGateway)

	remote, err := adnlquic.NewGateway(quicOutboundTestKey(t))
	if err != nil {
		t.Fatalf("create remote QUIC gateway: %v", err)
	}
	remote.SetConnectionHandler(func(*adnlquic.Peer) error {
		return nil
	})
	remoteAddr := startQUICOutboundTestGateway(t, remote)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err = remote.DialDefault(ctx, node.quicPublicKey, nodeAddr); err != nil {
		t.Fatalf("create node inbound path: %v", err)
	}

	var inbound *adnlquic.Peer
	select {
	case inbound = <-inboundPath:
	case <-ctx.Done():
		t.Fatalf("wait for node inbound path: %v", ctx.Err())
	}
	if got, lookupErr := node.quicGateway.OutboundPeer(
		node.quicPublicKey,
		remote.PublicKey(),
	); got != nil || !errors.Is(lookupErr, adnlquic.ErrOutboundPeerNotFound) {
		t.Fatalf(
			"outbound lookup = (%p, %v), want not found",
			got,
			lookupErr,
		)
	}

	remoteID, err := NewPeerID(remote.ID())
	if err != nil {
		t.Fatalf("parse remote peer id: %v", err)
	}
	peer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remote.PublicKey(),
		route: newPeerRoute(remoteAddr),
	}

	outbound, err := peer.dialQUIC(ctx)
	if err != nil {
		t.Fatalf("dial node outbound path: %v", err)
	}
	if outbound != inbound {
		t.Fatal("outbound connection did not attach to the existing managed peer")
	}
	got, lookupErr := node.quicGateway.OutboundPeer(
		node.quicPublicKey,
		remote.PublicKey(),
	)
	if lookupErr != nil {
		t.Fatalf("lookup outbound path: %v", lookupErr)
	}
	if got != outbound {
		t.Fatalf("outbound lookup = %p, want %p", got, outbound)
	}
}

func TestOverlayPeerDialQUICRecoversChangedEndpointThroughDHT(t *testing.T) {
	node := newTestNode(t)
	startQUICOutboundTestGateway(t, node.quicGateway)

	remoteKey := quicOutboundTestKey(t)
	remote, err := adnlquic.NewGateway(remoteKey)
	if err != nil {
		t.Fatalf("create remote QUIC gateway: %v", err)
	}
	remote.SetConnectionHandler(func(*adnlquic.Peer) error {
		return nil
	})
	remoteAddr := startQUICOutboundTestGateway(t, remote)
	remoteEndpoint, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		t.Fatalf("parse remote QUIC endpoint: %v", err)
	}

	deadConn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	})
	if err != nil {
		t.Fatalf("reserve dead QUIC endpoint: %v", err)
	}
	deadAddr := deadConn.LocalAddr().String()
	if err = deadConn.Close(); err != nil {
		t.Fatalf("close dead QUIC endpoint: %v", err)
	}

	remoteID := peerIDForQUICOutboundTest(t, remote.PublicKey())
	dht := &outboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.QUIC{
					IP:   net.IP(remoteEndpoint.Addr().AsSlice()),
					Port: int32(remoteEndpoint.Port()),
				},
			},
			ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
		},
		pub: remote.PublicKey(),
	}
	node.dht = dht
	route := newPeerRoute(deadAddr)
	peer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remote.PublicKey(),
		route: route,
	}

	failedCtx, cancelFailed := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = peer.dialQUIC(failedCtx)
	cancelFailed()
	if err == nil {
		t.Fatal("dial to dead QUIC endpoint unexpectedly succeeded")
	}
	if !route.quicAddrStale() {
		t.Fatal("failed post-resolution QUIC dial did not mark the route stale")
	}
	if route.quicReady(time.Now()) {
		t.Fatal("failed post-resolution QUIC dial did not enter cooldown")
	}
	if got := dht.calls.Load(); got != 0 {
		t.Fatalf("DHT lookups before stale recovery = %d, want 0", got)
	}

	route.quicRetryState.Store(time.Now().Add(-time.Second).UnixNano())
	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), 5*time.Second)
	outbound, err := peer.dialQUIC(recoveryCtx)
	cancelRecovery()
	if err != nil {
		t.Fatalf("recover changed QUIC endpoint: %v", err)
	}
	if outbound == nil {
		t.Fatal("recovered QUIC endpoint returned no peer")
	}
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("DHT lookups during stale recovery = %d, want 1", got)
	}
	if got := route.quicAddr(); got != remoteAddr {
		t.Fatalf("recovered QUIC route = %q, want %q", got, remoteAddr)
	}
	if route.quicAddrStale() {
		t.Fatal("successful changed-endpoint dial left route stale")
	}
	if got := route.quicRetryState.Load(); got != 0 {
		t.Fatalf("retry state after changed-endpoint recovery = %d, want 0", got)
	}
	route.quicRetryMx.Lock()
	failures := route.quicDialFailures
	route.quicRetryMx.Unlock()
	if failures != 0 {
		t.Fatalf("failures after changed-endpoint recovery = %d, want 0", failures)
	}
}

func TestPlumtreeRuntimeDialsOutboundForAuthenticatedPeer(t *testing.T) {
	remoteKey := quicOutboundTestKey(t)
	var ingressQueries atomic.Int32
	var targetConnections atomic.Int32
	var targetQueries atomic.Int32

	ingress, err := adnlquic.NewGateway(remoteKey)
	if err != nil {
		t.Fatalf("create ingress QUIC gateway: %v", err)
	}
	ingress.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		peer.SetQueryHandler(func(context.Context, []byte) ([]byte, error) {
			ingressQueries.Add(1)
			return []byte("ingress"), nil
		})
		return nil
	})
	startQUICOutboundTestGateway(t, ingress)

	target, err := adnlquic.NewGateway(remoteKey)
	if err != nil {
		t.Fatalf("create target QUIC gateway: %v", err)
	}
	target.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		targetConnections.Add(1)
		peer.SetQueryHandler(func(context.Context, []byte) ([]byte, error) {
			targetQueries.Add(1)
			return []byte("target"), nil
		})
		return nil
	})
	targetAddr := startQUICOutboundTestGateway(t, target)
	targetEndpoint, err := netip.ParseAddrPort(targetAddr)
	if err != nil {
		t.Fatalf("parse target QUIC address: %v", err)
	}
	if targetEndpoint.Port() <= 1000 {
		t.Fatalf("target QUIC port %d cannot be derived from ADNL", targetEndpoint.Port())
	}

	node := newTestNode(t)
	inboundRegistered := make(chan struct{})
	node.quicGateway.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		if err := node.handleInboundQUICPeer(peer); err != nil {
			return err
		}
		close(inboundRegistered)
		return nil
	})
	nodeAddr := startQUICOutboundTestGateway(t, node.quicGateway)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = ingress.DialDefault(
		ctx,
		node.quicPublicKey,
		nodeAddr,
	); err != nil {
		t.Fatalf("create node inbound path: %v", err)
	}

	remoteID, err := NewPeerID(ingress.ID())
	if err != nil {
		t.Fatalf("parse remote peer id: %v", err)
	}
	select {
	case <-inboundRegistered:
	case <-ctx.Done():
		t.Fatalf("wait for authenticated inbound path: %v", ctx.Err())
	}
	if _, err = node.authenticatedQUICPeer(remoteID); err != nil {
		t.Fatalf("lookup authenticated inbound path: %v", err)
	}
	if peer, lookupErr := node.quicGateway.OutboundPeer(
		node.quicPublicKey,
		ingress.PublicKey(),
	); peer != nil || !errors.Is(lookupErr, adnlquic.ErrOutboundPeerNotFound) {
		t.Fatalf(
			"outbound peer before Plumtree send = (%p, %v), want not found",
			peer,
			lookupErr,
		)
	}

	dht := &blockingOutboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.UDP{
					IP:   net.IP(targetEndpoint.Addr().AsSlice()),
					Port: int32(targetEndpoint.Port() - 1000),
				},
			},
		},
		pub:     ingress.PublicKey(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer func() {
		select {
		case <-dht.release:
		default:
			close(dht.release)
		}
	}()
	node.dht = dht

	sub := &overlaySubscription{
		node:  node,
		peers: map[PeerID]*overlayPeer{},
	}
	if sub.peerByID(remoteID) != nil {
		t.Fatal("authenticated public QUIC peer unexpectedly joined the overlay roster")
	}
	runtime := &plumtreeRuntime{
		sub: sub,
	}
	results := make(chan *adnlquic.Peer, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			path, pathErr := runtime.sub.quicPeerPath(remoteID)
			if pathErr != nil {
				errs <- pathErr
				return
			}
			peer, peerErr := path.dialGated(ctx)
			if peerErr != nil {
				errs <- peerErr
				return
			}
			results <- peer
		}()
	}

	select {
	case <-dht.started:
	case <-ctx.Done():
		t.Fatal("Plumtree outbound route did not start DHT resolution")
	}
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("concurrent DHT address lookups = %d, want 1", got)
	}
	close(dht.release)

	var peers [2]*adnlquic.Peer
	for i := range peers {
		select {
		case peers[i] = <-results:
		case peerErr := <-errs:
			t.Fatalf("dial Plumtree outbound path: %v", peerErr)
		case <-ctx.Done():
			t.Fatalf("wait for Plumtree outbound path: %v", ctx.Err())
		}
	}
	if peers[0] != peers[1] {
		t.Fatal("concurrent Plumtree dials returned different managed peers")
	}

	answer, err := peers[0].QueryOutbound(ctx, []byte("repair"), 1024)
	if err != nil {
		t.Fatalf("query Plumtree outbound path: %v", err)
	}
	if string(answer) != "target" {
		t.Fatalf("query answer = %q, want target", answer)
	}
	if got := ingressQueries.Load(); got != 0 {
		t.Fatalf("queries sent over inbound connection = %d, want 0", got)
	}
	if got := targetQueries.Load(); got != 1 {
		t.Fatalf("queries sent over DHT-resolved outbound connection = %d, want 1", got)
	}
	if got := targetConnections.Load(); got != 1 {
		t.Fatalf("DHT-resolved outbound connections = %d, want 1", got)
	}
	if got := dht.calls.Load(); got != 1 {
		t.Fatalf("total DHT address lookups = %d, want 1", got)
	}
}

func TestQUICOutboundOperationsRequireRoute(t *testing.T) {
	overlayID := testPeerID("quic-missing-route-overlay")
	peerPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	envelope, err := newQUICOverlayEnvelope(
		overlayID[:],
		nil,
	)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	peer := &overlayPeer{
		node:  newTestNode(t),
		pub:   peerPublicKey,
		route: newPeerRoute(""),
	}

	if _, err = (quicPeerQueryTransport{
		peer:     peer,
		envelope: envelope,
	}).QueryRaw(context.Background(), 1024, overlay.Ping{}); !errors.Is(err, errQUICRouteMissing) {
		t.Fatalf("raw query error = %v, want missing route", err)
	}
	typedPeer := &overlayPeer{
		node:  peer.node,
		pub:   peerPublicKey,
		route: newPeerRoute(""),
	}
	if err = (quicPeerQueryTransport{
		peer:     typedPeer,
		envelope: envelope,
	}).Query(context.Background(), 1024, overlay.Ping{}, &overlay.Pong{}); !errors.Is(err, errQUICRouteMissing) {
		t.Fatalf("typed query error = %v, want missing route", err)
	}
	// Broadcast sends never dial inline: without a live outbound connection
	// the part is dropped and errQUICPeerOffline is returned so the caller can
	// stop early and count the drop, while a background dial is kicked off.
	if err = (quicRouteBroadcastPeer{
		peer:     peer,
		envelope: envelope,
	}).SendCustomMessage(context.Background(), overlay.Ping{}); !errors.Is(err, errQUICPeerOffline) {
		t.Fatalf("message error = %v, want errQUICPeerOffline", err)
	}
}

func TestCustomTwoStepQUICDoesNotFallbackWithoutRoute(t *testing.T) {
	overlayID := testPeerID("custom-quic-no-fallback")
	peerPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	peerID := peerIDForQUICOutboundTest(t, peerPublicKey)
	adnlOverlay, base := newTestOverlayWrapper()
	t.Cleanup(base.Close)
	rldpTransport := &testBroadcastRLDP{adnl: base}
	node := newTestNode(t)
	peer := &overlayPeer{
		node:          node,
		id:            peerID,
		pub:           peerPublicKey,
		route:         newPeerRoute(""),
		overlayID:     overlayID[:],
		fixedMember:   true,
		overlay:       adnlOverlay,
		rldpOverlay:   overlay.CreateExtendedRLDP(rldpTransport).CreateOverlay(overlayID[:]),
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "custom.quic-no-fallback",
			Kind:    overlayKindCustomFixed,
			ShortID: overlayID[:],
			UseQUIC: true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peerID: peer},
	})
	t.Cleanup(sub.broadcastReceiver.Close)

	peerSet, failed := sub.resolveTwoStepPeerSet(context.Background(), PeerID{})
	if len(peerSet) != 0 {
		t.Fatalf("resolved peers without QUIC route = %d, want 0", len(peerSet))
	}
	if len(failed) != 1 || !errors.Is(failed[0].Err, errQUICRouteMissing) {
		t.Fatalf("QUIC route failures = %+v, want one missing-route error", failed)
	}
	if len(rldpTransport.sent) != 0 || len(base.sent) != 0 {
		t.Fatal("missing QUIC route fell back to RLDP or ADNL")
	}
	if relayPeers := (twoStepPeerSet{sub: sub}).Peers(); len(relayPeers) != 0 {
		t.Fatalf("receive relay peers without QUIC route = %d, want 0", len(relayPeers))
	}

	peer.route.setQUICAddr("127.0.0.1:25021")
	relayPeers := (twoStepPeerSet{sub: sub}).Peers()
	if len(relayPeers) != 1 {
		t.Fatalf("receive relay peers with QUIC route = %d, want 1", len(relayPeers))
	}
	if _, ok := relayPeers[0].(quicRouteBroadcastPeer); !ok {
		t.Fatalf("receive relay peer = %T, want route-bound QUIC peer", relayPeers[0])
	}
}

func newQUICRouteDiscoveryNode(t *testing.T, dht dhtBackend) *Node {
	t.Helper()

	node := newTestNode(t)
	_, gatewayKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ADNL gateway key: %v", err)
	}
	gateway := adnl.NewGatewayWithNetManager(gatewayKey, newRecordingNetManager())
	gateway.SetAddressList(nil)
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Errorf("close ADNL gateway: %v", err)
		}
	})

	node.dht = dht
	node.pool = newPeerPool(gateway, nil, nil, nil)
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	node.runCtx = runCtx
	return node
}

func peerIDForQUICOutboundTest(t *testing.T, pub ed25519.PublicKey) PeerID {
	t.Helper()

	raw, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		t.Fatalf("hash peer public key: %v", err)
	}
	id, err := NewPeerID(raw)
	if err != nil {
		t.Fatalf("parse peer id: %v", err)
	}
	return id
}

func quicOutboundTestKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()

	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate QUIC key: %v", err)
	}
	return key
}

func startQUICOutboundTestGateway(t *testing.T, gateway *adnlquic.Gateway) string {
	t.Helper()

	packetConn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	})
	if err != nil {
		t.Fatalf("listen QUIC test gateway: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- gateway.Serve(packetConn)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = gateway.WaitReady(ctx); err != nil {
		_ = gateway.Close()
		_ = packetConn.Close()
		t.Fatalf("wait for QUIC test gateway: %v", err)
	}

	t.Cleanup(func() {
		_ = gateway.Close()
		_ = packetConn.Close()
		if err := <-serveErr; err != nil {
			t.Errorf("serve QUIC test gateway: %v", err)
		}
	})
	return packetConn.LocalAddr().String()
}
