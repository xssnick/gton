package p2p

import (
	"context"
	"crypto/ed25519"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/peerroute"
)

// reopenedRouteLogMessage starts the Info line reopenFailingRoute writes when
// it reopens a route.
const reopenedRouteLogMessage = "inbound QUIC path from a fixed member reopened its failing route"

type inboundReopenScopeCase struct {
	name    string
	kind    overlayKind
	useQUIC bool
	want    bool
}

func TestInboundQUICReopensFailingRouteScope(t *testing.T) {
	for _, tc := range []inboundReopenScopeCase{
		{name: "custom fixed over QUIC", kind: overlayKindCustomFixed, useQUIC: true, want: true},
		{name: "private over QUIC", kind: overlayKindPrivate, useQUIC: true, want: true},
		{name: "custom fixed over ADNL", kind: overlayKindCustomFixed},
		{name: "private over ADNL", kind: overlayKindPrivate},
		{name: "FastSync over QUIC", kind: overlayKindFastSync, useQUIC: true},
		{name: "public shard over QUIC", kind: overlayKindPublicShard, useQUIC: true},
	} {
		spec := overlaySpec{Kind: tc.kind, UseQUIC: tc.useQUIC}
		if got := spec.inboundQUICReopensFailingRoute(); got != tc.want {
			t.Errorf("%s: inboundQUICReopensFailingRoute = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Only the roster of an overlay in scope counts. The public shard and the
// FastSync subscription below accept every peer id (a FastSync subscription
// built without its membership runtime falls through to "unrestricted" in
// acceptsPeerID), so they also check that the kind filter runs first.
func TestInboundReopensQUICRouteMembership(t *testing.T) {
	node := newTestNode(t)
	customMember := testPeerID("inbound-reopen-custom-member")
	privateMember := testPeerID("inbound-reopen-private-member")
	adnlMember := testPeerID("inbound-reopen-adnl-member")
	stranger := testPeerID("inbound-reopen-stranger")

	register := func(name string, kind overlayKind, useQUIC bool, roster ...PeerID) {
		overlayID := testPeerID("inbound-reopen-overlay:" + name)
		fixed := make(map[PeerID]struct{}, len(roster))
		for _, id := range roster {
			fixed[id] = struct{}{}
		}
		sub := testOverlaySubscription(&overlaySubscription{
			node: node,
			spec: overlaySpec{
				Name:         name,
				Kind:         kind,
				ShortID:      overlayID[:],
				UseQUIC:      useQUIC,
				FixedNodeIDs: fixed,
			},
			log:   discardLogger(),
			peers: map[PeerID]*overlayPeer{},
		})
		node.subscriptionsMx.Lock()
		node.subscriptions[string(overlayID[:])] = sub
		node.subscriptionsMx.Unlock()
	}
	register("custom", overlayKindCustomFixed, true, customMember)
	register("private", overlayKindPrivate, true, privateMember)
	register("custom-adnl", overlayKindCustomFixed, false, adnlMember)
	register("fast-sync", overlayKindFastSync, true)
	register("public", overlayKindPublicShard, true)

	for id, want := range map[PeerID]bool{
		customMember:  true,
		privateMember: true,
		adnlMember:    false,
		stranger:      false,
	} {
		if got := node.inboundReopensQUICRoute(id); got != want {
			t.Errorf("inboundReopensQUICRoute(%s) = %v, want %v", id, got, want)
		}
	}
}

// A reachable peer on no fixed QUIC roster - it is on the roster of a custom
// overlay over ADNL - dials us while its route is failing: the gate stays
// armed and no dial starts, so its connection churn cannot buy handshakes.
func TestInboundQUICPathFromNonMemberLeavesRouteGated(t *testing.T) {
	h := newQUICRestartHarness(t, false)
	stranger := startLocalSendTestGateway(t)
	overlayID := testPeerID("inbound-reopen-adnl-overlay")
	adnlOverlay := testOverlaySubscription(&overlaySubscription{
		node: h.node,
		spec: overlaySpec{
			Name:         "custom.adnl",
			Kind:         overlayKindCustomFixed,
			ShortID:      overlayID[:],
			FixedNodeIDs: map[PeerID]struct{}{stranger.id: {}},
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	h.node.subscriptionsMx.Lock()
	h.node.subscriptions[string(overlayID[:])] = adnlOverlay
	h.node.subscriptionsMx.Unlock()

	// The learned endpoint is live, so a dial would connect at once.
	route := h.node.peerRoutes.Get(stranger.id)
	route.SetQUICAddress(stranger.addr)
	failQUICRouteDial(t, route)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := stranger.gateway.DialDefault(ctx, h.node.quicPublicKey, h.nodeAddr); err != nil {
		t.Fatalf("stranger dial to A: %v", err)
	}
	// The handler may still be on its way to the check; running it here as
	// well keeps the assertions below from passing before it has.
	awaitQUICPathRegistered(t, h.node, stranger.gateway.PublicKey(), stranger.id).reopenFailingRoute()

	quicRestartHolds(t, 500*time.Millisecond, "the stranger's route to stay failing and gated", func() bool {
		_, outboundErr := h.node.quicGateway.OutboundPeerDefaultID(stranger.id[:])
		return outboundErr != nil &&
			route.QUICDialFailing() &&
			!route.QUICDialInFlight() &&
			!route.QUICDialPermitted(time.Now())
	})
	if strings.Contains(h.logs.String(), reopenedRouteLogMessage) {
		t.Fatalf("a non-member's path reopened its route\nlogs:\n%s", h.logs.String())
	}
}

// The gateway runs the connection handler for the path our own dial creates
// too. That path already has our outbound connection, so it is never taken for
// proof of life: not when the handler runs for it, and not even if a failure
// verdict were on the route while the connection is up.
func TestInboundQUICReopenIgnoresPathWithOwnOutbound(t *testing.T) {
	h := newQUICRestartHarness(t, true)
	route := h.peer.route
	// The gateway publishes the outbound connection before the prewarm dial
	// records its success on the route, so wait for both.
	quicRestartWait(t, quicRelayDialTimeout, "subscription prewarm to open A->B", func() bool {
		return h.hasOutbound() && !route.QUICDialInFlight()
	})
	authenticated := awaitQUICPathRegistered(t, h.node, h.remotePub, h.remoteID)

	failQUICRouteDial(t, route)
	authenticated.reopenFailingRoute()

	if !route.QUICDialFailing() || route.QUICDialPermitted(time.Now()) {
		t.Fatal("a path with our own outbound connection reopened the route")
	}
	if strings.Contains(h.logs.String(), reopenedRouteLogMessage) {
		t.Fatalf("a path with our own outbound connection logged a reopen\nlogs:\n%s", h.logs.String())
	}
}

// A member that dials us while a dial of ours is in flight leaves that dial
// alone, since it owns the gate until its verdict. When the dial then fails the
// route stays gated: the handler has already run, so the member waits out the
// gate (the known gap reopenFailingRoute documents for a peer that returns in
// the last seconds of our dial).
func TestInboundQUICPathLeavesDialInFlightAlone(t *testing.T) {
	h := newQUICRestartHarness(t, false)
	route := h.peer.route

	failedAt := time.Now().Add(-peerRouteRetryPolicy.MinDelay - time.Second)
	if !route.BeginQUICDial(failedAt) {
		t.Fatal("claim the dial that is about to fail")
	}
	route.FailQUICDial(failedAt)
	if !route.BeginQUICDial(time.Now()) {
		t.Fatal("claim the next dial after the retry gate expired")
	}

	h.remote.dialNode(t, h.node.quicPublicKey, h.nodeAddr)
	h.awaitInboundPath(t).reopenFailingRoute()

	quicRestartHolds(t, 300*time.Millisecond, "the in-flight dial to keep the route", func() bool {
		return route.QUICDialInFlight() && route.QUICDialFailing()
	})

	route.FailQUICDial(time.Now())
	quicRestartHolds(t, 300*time.Millisecond, "the failed dial to leave the route gated", func() bool {
		return route.QUICDialFailing() &&
			!route.QUICDialInFlight() &&
			!route.QUICDialPermitted(time.Now())
	})
	if strings.Contains(h.logs.String(), reopenedRouteLogMessage) {
		t.Fatalf("a path that arrived during a dial logged a reopen\nlogs:\n%s", h.logs.String())
	}
}

// A member that is alive but not dialable from here - its advertised endpoint
// is a black hole - reopens its failing route with its first new path only.
// The reopen dial fails and backs off from where the streak was, and the next
// new path leaves the gate armed, so reconnects cannot turn the retry gate into
// back-to-back handshakes that each hold the two-step worker.
func TestInboundQUICPathReopensOncePerFailingStreak(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a 5s QUIC handshake to time out")
	}

	h := newQUICRestartHarness(t, false)
	blackHole := listenQUICBlackHole(t)
	dht := h.resolveRemoteThroughDHT(t, blackHole)
	route := h.peer.route
	failQUICRouteDial(t, route)
	route.MarkQUICAddressStale()

	h.remote.dialNode(t, h.node.quicPublicKey, h.nodeAddr)
	firstPath := h.awaitInboundPath(t)
	quicRestartWait(t, quicRelayDialTimeout, "the reopen dial to the black hole to fail", func() bool {
		return dht.calls.Load() > 0 && route.QUICDialFailing() && !route.QUICDialInFlight()
	})
	// The streak's first failure gated 10 s; the reopen dial is its second
	// and gates 20 s.
	if route.QUICDialPermitted(time.Now().Add(peerRouteRetryPolicy.MinDelay + time.Second)) {
		t.Fatal("the failed reopen dial restarted the backoff")
	}

	// B reconnects: a new gateway path, and no second reopen.
	h.remote.stop(true)
	h.returnRemote(t)
	var secondPath *authenticatedQUICPeer
	quicRestartWait(t, 5*time.Second, "B's second path to be registered", func() bool {
		current, err := h.node.authenticatedQUICPeer(h.remoteID)
		if err != nil ||
			current == firstPath ||
			current.peer != h.node.quicGateway.Peer(h.node.quicPublicKey, h.remotePub) {
			return false
		}
		secondPath = current
		return true
	})
	secondPath.reopenFailingRoute()
	quicRestartHolds(t, 500*time.Millisecond, "B's second path to leave the route gated", func() bool {
		return route.QUICDialFailing() &&
			!route.QUICDialInFlight() &&
			!route.QUICDialPermitted(time.Now())
	})
	if got := strings.Count(h.logs.String(), reopenedRouteLogMessage); got != 1 {
		t.Fatalf("logged %d reopens, want 1\nlogs:\n%s", got, h.logs.String())
	}
}

// listenQUICBlackHole returns a loopback UDP endpoint that never answers, so a
// QUIC dial to it fails on the handshake idle timeout.
func listenQUICBlackHole(t *testing.T) string {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen black hole: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn.LocalAddr().String()
}

// failQUICRouteDial records one failed dial now, arming the route's retry gate
// for its first backoff step.
func failQUICRouteDial(t *testing.T, route *peerroute.Route) {
	t.Helper()

	now := time.Now()
	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected the dial that is about to fail")
	}
	route.FailQUICDial(now)
	if !route.QUICDialFailing() || route.QUICDialPermitted(now) {
		t.Fatal("precondition: the failed dial left the route healthy")
	}
}

// awaitQUICPathRegistered waits until node holds a gateway path to the peer and
// the connection handler has registered it.
func awaitQUICPathRegistered(t *testing.T, node *Node, pub ed25519.PublicKey, id PeerID) *authenticatedQUICPeer {
	t.Helper()

	var authenticated *authenticatedQUICPeer
	quicRestartWait(t, 5*time.Second, "the QUIC path to be registered", func() bool {
		path := node.quicGateway.Peer(node.quicPublicKey, pub)
		if path == nil {
			return false
		}
		current, err := node.authenticatedQUICPeer(id)
		if err != nil || current.peer != path {
			return false
		}
		authenticated = current
		return true
	})
	return authenticated
}

// quicRestartHolds fails unless cond holds throughout the period.
func quicRestartHolds(t *testing.T, period time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(period)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("expected %s for %s", what, period)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
