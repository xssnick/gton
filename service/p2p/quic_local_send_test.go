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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/peerroute"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
)

const localSendTestKind = "tonNode.externalMessageBroadcast"

// localSendTestGateway is a reachable remote QUIC peer counting the overlay
// messages it receives.
type localSendTestGateway struct {
	gateway  *adnlquic.Gateway
	addr     string
	id       PeerID
	messages atomic.Int64
	received chan struct{}
}

func startLocalSendTestGateway(t *testing.T) *localSendTestGateway {
	t.Helper()

	gateway, err := adnlquic.NewGateway(quicOutboundTestKey(t))
	if err != nil {
		t.Fatalf("create remote QUIC gateway: %v", err)
	}
	remote := &localSendTestGateway{
		gateway:  gateway,
		received: make(chan struct{}, 256),
	}
	gateway.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		peer.SetMessageHandler(func(context.Context, []byte) {
			remote.messages.Add(1)
			select {
			case remote.received <- struct{}{}:
			default:
			}
		})
		return nil
	})
	remote.addr = startQUICOutboundTestGateway(t, gateway)
	remote.id, err = NewPeerID(gateway.ID())
	if err != nil {
		t.Fatalf("parse remote peer id: %v", err)
	}

	return remote
}

// addressList is the DHT record that resolves the remote's QUIC endpoint.
func (g *localSendTestGateway) addressList(t *testing.T) *adnladdr.List {
	t.Helper()

	endpoint, err := netip.ParseAddrPort(g.addr)
	if err != nil {
		t.Fatalf("parse remote QUIC endpoint: %v", err)
	}
	return &adnladdr.List{
		Addresses: []adnladdr.Address{
			adnladdr.QUIC{
				IP:   net.IP(endpoint.Addr().AsSlice()),
				Port: int32(endpoint.Port()),
			},
		},
		ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
	}
}

func (g *localSendTestGateway) awaitMessage(t *testing.T, what string) {
	t.Helper()

	select {
	case <-g.received:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not receive the broadcast", what)
	}
}

// localTwoStepFixture is a custom QUIC overlay of two members: a warm one this
// node is connected to, and a reachable cold one whose route resolves through
// a DHT lookup that blocks until the test releases it.
type localTwoStepFixture struct {
	node     *Node
	sub      *overlaySubscription
	warm     *localSendTestGateway
	cold     *localSendTestGateway
	coldPeer *overlayPeer
	dht      *blockingOutboundRouteDHT
}

func newLocalTwoStepFixture(t *testing.T, coldRoute *peerroute.Route) *localTwoStepFixture {
	t.Helper()

	warm := startLocalSendTestGateway(t)
	cold := startLocalSendTestGateway(t)
	dht := &blockingOutboundRouteDHT{
		addresses: cold.addressList(t),
		pub:       cold.gateway.PublicKey(),
		started:   make(chan struct{}),
		release:   make(chan struct{}),
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

	warmPeer := &overlayPeer{
		node:  node,
		id:    warm.id,
		pub:   warm.gateway.PublicKey(),
		route: newTestPeerRoute(warm.addr),
	}
	coldPeer := &overlayPeer{
		node:  node,
		id:    cold.id,
		pub:   cold.gateway.PublicKey(),
		route: coldRoute,
	}
	overlayID := testPeerID("local-two-step-quic:" + t.Name())
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "custom.local-two-step-quic",
			Kind:    overlayKindCustomFixed,
			ShortID: overlayID[:],
			UseQUIC: true,
		},
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			warm.id: warmPeer,
			cold.id: coldPeer,
		},
	})
	t.Cleanup(sub.broadcastReceiver.Close)
	sub.broadcastTargets.Store(&broadcastTargetsSnapshot{
		generation: sub.broadcastTargetsGen.Load(),
		builtAt:    time.Now(),
		peers:      []*overlayPeer{warmPeer, coldPeer},
	})

	warmCtx, cancelWarm := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWarm()
	if _, err := warmPeer.dialQUIC(warmCtx); err != nil {
		t.Fatalf("warm the connected member: %v", err)
	}

	return &localTwoStepFixture{
		node:     node,
		sub:      sub,
		warm:     warm,
		cold:     cold,
		coldPeer: coldPeer,
		dht:      dht,
	}
}

func (fx *localTwoStepFixture) request(local bool) rebroadcastRequest {
	return rebroadcastRequest{
		subscription: fx.sub,
		kind:         localSendTestKind,
		payload:      []byte(fmt.Sprintf("two-step broadcast, local=%t", local)),
		local:        local,
	}
}

func (fx *localTwoStepFixture) coldConnected() bool {
	_, err := fx.node.quicGateway.OutboundPeerDefaultID(fx.cold.id[:])
	return err == nil
}

func (fx *localTwoStepFixture) localDrops(reason string) uint64 {
	return testBroadcastDropStatCount(fx.node, fx.sub.spec.Name, localSendTestKind, reason)
}

func (fx *localTwoStepFixture) requireNoLocalDrops(t *testing.T) {
	t.Helper()

	for _, reason := range localSendDropReasons {
		if got := fx.localDrops(reason); got != 0 {
			t.Fatalf("local send drops %q = %d, want 0", reason, got)
		}
	}
}

// An own broadcast reaches a member this node has no outbound connection to:
// the send joins the route's shared dial and waits for it within its budget,
// as C++ QuicSender does, instead of dropping the member's part.
func TestLocalTwoStepQUICSendWaitsForColdPeerDial(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		fx := newLocalTwoStepFixture(t, newTestPeerRoute(""))
		releaseAfter := 100 * time.Millisecond
		go func() {
			<-fx.dht.started
			time.Sleep(releaseAfter)
			close(fx.dht.release)
		}()

		start := time.Now()
		if !fx.sub.sendTwoStepRebroadcast(context.Background(), fx.request(true)) {
			t.Fatal("own two-step broadcast reported no delivery")
		}
		if elapsed := time.Since(start); elapsed < releaseAfter {
			t.Fatalf("own broadcast returned after %s, before the cold member's dial could finish", elapsed)
		}

		fx.warm.awaitMessage(t, "connected member")
		fx.cold.awaitMessage(t, "cold member")
		// Every failed recipient of an own broadcast is counted, so no count
		// means both recipients took their part: Sent == Attempted == 2.
		fx.requireNoLocalDrops(t)
		if !fx.coldConnected() {
			t.Fatal("cold member has no outbound connection after the own broadcast")
		}
		if fx.coldPeer.route.QUICDialFailing() {
			t.Fatal("a successful shared dial left the route failing")
		}
	})

	t.Run("relay", func(t *testing.T) {
		fx := newLocalTwoStepFixture(t, newTestPeerRoute(""))
		req := fx.request(false)

		// The DHT is never released, so a relay that waited for the cold
		// member would spend the whole send budget.
		start := time.Now()
		if !fx.sub.sendTwoStepRebroadcast(context.Background(), req) {
			t.Fatal("relayed two-step broadcast reported no delivery to the connected member")
		}
		if elapsed := time.Since(start); elapsed >= peerRebroadcastTimeout {
			t.Fatalf("relayed broadcast took %s, the whole send budget", elapsed)
		}
		if fx.coldConnected() {
			t.Fatal("relay waited for the cold member's dial")
		}
		select {
		case <-fx.dht.started:
		case <-time.After(5 * time.Second):
			t.Fatal("relay did not request a background dial for the cold member")
		}
		if !fx.coldPeer.route.QUICDialInFlight() {
			t.Fatal("the cold member's background dial is not in flight")
		}
		fx.warm.awaitMessage(t, "connected member")
		fx.requireNoLocalDrops(t)

		// The same fan-out, observed at the tonutils result: the plain set is
		// what sendTwoStepRebroadcast hands over for a relayed request.
		result, err := overlay.SendBroadcastTwoStep(context.Background(), overlay.BroadcastTwoStepSendRequest{
			Key:         fx.node.privKey,
			Certificate: overlay.CertificateEmpty{},
			LocalADNLID: fx.node.localID.Bytes(),
			Payload:     req.payload,
			PeerSet:     fx.sub.resolveTwoStepPeerSet(PeerID{}),
		}, overlay.WithBroadcastTwoStepDate(123))
		if err == nil {
			t.Fatal("skipped cold member was not reported")
		}
		if result.Sent != 1 || len(result.Failed) != 1 {
			t.Fatalf("relay result = %+v, want one delivery and one skipped member", result)
		}
		if !bytes.Equal(result.Failed[0].PeerID, fx.cold.id[:]) || !errors.Is(result.Failed[0].Err, errQUICPeerOffline) {
			t.Fatalf("relay failure = %x: %v, want cold member offline", result.Failed[0].PeerID, result.Failed[0].Err)
		}
	})
}

// A member whose last dial failed is not waited for: one unreachable member of
// the single-worker two-step queue must not hold every member's broadcast for
// a handshake timeout on each attempt.
func TestLocalTwoStepQUICSendSkipsWaitOnFailingRoute(t *testing.T) {
	route := newTestPeerRouteWithRetry("", time.Millisecond, time.Millisecond)
	failedAt := time.Now().Add(-time.Second)
	if !route.BeginQUICDial(failedAt) {
		t.Fatal("claim the dial that is about to fail")
	}
	route.FailQUICDial(failedAt)
	fx := newLocalTwoStepFixture(t, route)
	if !route.QUICDialFailing() || !route.QUICDialPermitted(time.Now()) {
		t.Fatal("precondition: the route failed and its retry gate expired")
	}

	// The DHT is never released: a wait on the cold member would last the
	// whole 5 s send budget.
	start := time.Now()
	if !fx.sub.sendTwoStepRebroadcast(context.Background(), fx.request(true)) {
		t.Fatal("own two-step broadcast did not reach the connected member")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("own broadcast waited %s on a member whose route is failing", elapsed)
	}

	if got := fx.localDrops("local_send_peer_offline"); got != 1 {
		t.Fatalf("local_send_peer_offline drops = %d, want 1 for the cold member", got)
	}
	select {
	case <-fx.dht.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the skipped member got no background dial")
	}
	if !route.QUICDialInFlight() {
		t.Fatal("the skipped member's background dial is not in flight")
	}
	fx.warm.awaitMessage(t, "connected member")
	if got := fx.cold.messages.Load(); got != 0 {
		t.Fatalf("cold member received %d messages, want 0", got)
	}
}

// The send budget only ends the wait: the shared dial keeps running on the
// node context, and the route is not marked failing by our own deadline.
func TestLocalTwoStepQUICWaitEndsWithSendBudgetWithoutAbortingDial(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	dht := &blockingOutboundRouteDHT{
		pub:     remotePub,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	node := newTestNode(t)
	node.dht = dht
	runCtx, cancelRun := context.WithCancel(context.Background())
	node.runCtx = runCtx
	t.Cleanup(func() {
		cancelRun()
		node.wg.Wait()
	})

	peer := &overlayPeer{
		node:  node,
		id:    peerIDForQUICOutboundTest(t, remotePub),
		pub:   remotePub,
		route: newTestPeerRoute(""),
	}
	overlayID := testPeerID("local-send-budget")
	envelope, err := newQUICOverlayEnvelope(overlayID[:], nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	local := quicLocalBroadcastPeer{route: quicRouteBroadcastPeer{peer: peer, envelope: envelope}}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = local.SendCustomMessage(ctx, overlay.Ping{})
	if !errors.Is(err, errQUICPeerOffline) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send error = %v, want offline wrapping the send deadline", err)
	}
	// The wait ran out our own budget: reported as such, and not a peer fault.
	if fault := classifyTwoStepSendFault(err); fault != TwoStepSendFaultContextDeadline {
		t.Fatalf("send fault = %d, want the expired send budget", fault)
	}

	select {
	case <-dht.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the shared dial never started")
	}
	if !peer.route.QUICDialInFlight() {
		t.Fatal("the send deadline aborted the shared dial")
	}
	if peer.route.QUICDialFailing() {
		t.Fatal("the send deadline marked the route failing")
	}
}

// localPeerQueueFixture is a QUIC overlay of the given kind with one reachable
// member whose route knows its address but has no outbound connection yet.
type localPeerQueueFixture struct {
	node   *Node
	sub    *overlaySubscription
	remote *localSendTestGateway
	peer   *overlayPeer
}

func newLocalPeerQueueFixture(t *testing.T, kind overlayKind) *localPeerQueueFixture {
	t.Helper()

	remote := startLocalSendTestGateway(t)
	node := newTestNode(t)
	startQUICOutboundTestGateway(t, node.quicGateway)
	runCtx, cancelRun := context.WithCancel(context.Background())
	node.runCtx = runCtx
	t.Cleanup(func() {
		cancelRun()
		node.wg.Wait()
	})

	peer := testRebroadcastQueuePeer("local-peer-queue")
	peer.node = node
	peer.id = remote.id
	peer.pub = remote.gateway.PublicKey()
	peer.route = newTestPeerRoute(remote.addr)
	peer.fixedMember = true
	overlayID := testPeerID("local-peer-queue-overlay")
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "local-peer-queue",
			Kind:    kind,
			ShortID: overlayID[:],
			UseQUIC: true,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{remote.id: peer},
	})
	t.Cleanup(sub.broadcastReceiver.Close)
	relay := quicRouteBroadcastPeer{peer: peer, envelope: sub.quicEnvelope}
	peer.broadcastPeer = relay
	peer.localBroadcastPeer = quicLocalBroadcastPeer{route: relay}

	return &localPeerQueueFixture{
		node:   node,
		sub:    sub,
		remote: remote,
		peer:   peer,
	}
}

// resolveThroughDHT makes the member's route forget its address, so a dial
// has to resolve it through a DHT lookup that blocks until the test releases
// it.
func (fx *localPeerQueueFixture) resolveThroughDHT(t *testing.T) *blockingOutboundRouteDHT {
	t.Helper()

	dht := &blockingOutboundRouteDHT{
		addresses: fx.remote.addressList(t),
		pub:       fx.remote.gateway.PublicKey(),
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	fx.node.dht = dht
	fx.peer.route = newTestPeerRoute("")

	return dht
}

// fecRequest is an FEC-sized request with its sender built, so the test knows
// how many parts a complete delivery is.
func (fx *localPeerQueueFixture) fecRequest(t *testing.T, local bool) (rebroadcastRequest, uint32) {
	t.Helper()

	payload := bytes.Repeat([]byte{0x5a}, 5000)
	sender, err := overlay.NewBroadcastFECSender(
		fx.node.privKey,
		nil,
		payload,
		0,
		overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
	)
	if err != nil {
		t.Fatalf("create FEC sender: %v", err)
	}
	return rebroadcastRequest{
		subscription: fx.sub,
		kind:         localSendTestKind,
		fec:          sender,
		payloadSize:  len(payload),
		local:        local,
	}, sender.TotalParts()
}

func (fx *localPeerQueueFixture) connected() bool {
	_, err := fx.node.quicGateway.OutboundPeerDefaultID(fx.remote.id[:])
	return err == nil
}

func (fx *localPeerQueueFixture) failedQueries() uint64 {
	fx.peer.statsMx.Lock()
	defer fx.peer.statsMx.Unlock()

	return fx.peer.failedQueries
}

func (fx *localPeerQueueFixture) awaitParts(t *testing.T, parts uint32) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for fx.remote.messages.Load() < int64(parts) {
		if time.Now().After(deadline) {
			t.Fatalf("peer received %d of %d FEC parts", fx.remote.messages.Load(), parts)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (fx *localPeerQueueFixture) localDrops(kind, reason string) uint64 {
	return testBroadcastDropStatCount(fx.node, fx.sub.spec.Name, kind, reason)
}

// The per-peer queues (FastSync and custom per-peer FEC/simple sends) wait for
// a missing connection for an own broadcast too; the wait holds only this
// peer's queue. A relayed request keeps failing fast without faulting the peer.
func TestLocalPeerRebroadcastWaitsForColdQUICPath(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		fx := newLocalPeerQueueFixture(t, overlayKindCustomFixed)
		req, parts := fx.fecRequest(t, true)
		if plan := fx.sub.rebroadcastPlan(req.kind, req.payloadLen()); plan.mode != rebroadcastModeFEC {
			t.Fatalf("request plan = %+v, want FEC", plan)
		}

		if !fx.sub.rebroadcastToPeer(context.Background(), fx.peer, req) {
			t.Fatal("own FEC broadcast to a cold but reachable peer was dropped")
		}
		fx.awaitParts(t, parts)
		if got := fx.localDrops(req.kind, "local_send_peer_offline"); got != 0 {
			t.Fatalf("local_send_peer_offline drops = %d, want 0", got)
		}
	})

	t.Run("relay", func(t *testing.T) {
		fx := newLocalPeerQueueFixture(t, overlayKindCustomFixed)
		req, _ := fx.fecRequest(t, false)

		if fx.sub.rebroadcastToPeer(context.Background(), fx.peer, req) {
			t.Fatal("relayed FEC broadcast to a peer without an outbound connection reported delivery")
		}

		// The drop requested a background dial; once it has landed, nothing of
		// the dropped request may have reached the peer.
		deadline := time.Now().Add(quicRelayDialTimeout)
		for !fx.connected() {
			if time.Now().After(deadline) {
				t.Fatal("relayed drop did not bring the connection up in the background")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if got := fx.remote.messages.Load(); got != 0 {
			t.Fatalf("peer received %d parts of a dropped relay, want 0", got)
		}
		if fx.sub.peerByID(fx.peer.id) != fx.peer {
			t.Fatal("relayed drop evicted the peer")
		}
		if got := fx.failedQueries(); got != 0 {
			t.Fatalf("relayed drop faulted the peer: failed queries = %d", got)
		}
		if got := testBroadcastDropStatCount(fx.node, fx.sub.spec.Name, req.kind, "local_send_peer_offline"); got != 0 {
			t.Fatalf("relayed drop counted as local send drop: %d", got)
		}
	})
}

// While the node lags, per-peer FEC sends share a few node-wide slots. An own
// send waits for its connection before it takes one, so a cold peer's dial
// cannot hold back every other FEC rebroadcast of its class.
func TestLocalPeerDialWaitHoldsNoFECBackpressureSlot(t *testing.T) {
	fx := newLocalPeerQueueFixture(t, overlayKindCustomFixed)
	fx.node.syncLag = &testSyncLagProvider{lag: rebroadcastFECLagThreshold + 1}
	dht := fx.resolveThroughDHT(t)
	req, parts := fx.fecRequest(t, true)
	req.queuedAt = time.Now()

	done := make(chan bool, 1)
	go func() {
		done <- fx.sub.rebroadcastToPeer(context.Background(), fx.peer, req)
	}()

	select {
	case <-dht.started:
	case <-time.After(5 * time.Second):
		t.Fatal("own send did not start the dial")
	}
	slots := fx.node.rebroadcastFECSlotLimit(rebroadcastFECBackpressureClass(req.kind))
	if held := len(slots); held != 0 {
		t.Fatalf("own send holds %d node-wide FEC slots while it waits for its dial", held)
	}

	close(dht.release)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("own FEC broadcast was dropped after its dial landed")
		}
	case <-time.After(2 * peerRebroadcastTimeout):
		t.Fatal("own FEC broadcast did not finish")
	}
	fx.awaitParts(t, parts)
}

// An own FEC burst gets its whole budget after waiting for the connection: a
// dial that lands late in the wait cannot truncate the burst into a peer
// fault, as it did while the wait and the burst shared one budget.
func TestLocalPeerFECBurstKeepsItsBudgetAfterDialWait(t *testing.T) {
	if testing.Short() {
		t.Skip("spends more than one send budget on a dial wait and a paced burst")
	}

	fx := newLocalPeerQueueFixture(t, overlayKindFastSync)
	dht := fx.resolveThroughDHT(t)
	req, parts := fx.fecRequest(t, true)
	if plan := fx.sub.rebroadcastPlan(req.kind, req.payloadLen()); plan.mode != rebroadcastModeFEC {
		t.Fatalf("request plan = %+v, want FEC", plan)
	}

	// The dial lands half a budget into the wait and the paced burst takes
	// another 0.6 of one: each fits a budget, together they overrun it.
	dialAfter := peerRebroadcastTimeout / 2
	burst := peerRebroadcastTimeout * 3 / 5
	pauses := (parts - 1) / overlay.DefaultBroadcastFECBurstSize
	if pauses == 0 {
		t.Fatalf("%d parts leave no pause to pace the burst with", parts)
	}
	fx.node.fastSyncBroadcastFECPace = burst / time.Duration(pauses)
	go func() {
		<-dht.started
		time.Sleep(dialAfter)
		close(dht.release)
	}()

	start := time.Now()
	if !fx.sub.rebroadcastToPeer(context.Background(), fx.peer, req) {
		t.Fatalf("own FEC burst after a late dial failed after %s", time.Since(start))
	}
	if elapsed := time.Since(start); elapsed <= peerRebroadcastTimeout {
		t.Fatalf("wait and burst took %s, want more than one send budget", elapsed)
	}
	fx.awaitParts(t, parts)
	if got := fx.failedQueries(); got != 0 {
		t.Fatalf("own send faulted the peer: failed queries = %d", got)
	}
	for _, reason := range localSendDropReasons {
		if got := fx.localDrops(req.kind, reason); got != 0 {
			t.Fatalf("local send drops %q = %d, want 0", reason, got)
		}
	}
}

// A wait that ends without a connection is this node's own budget running
// out, not the peer's fault: the peer stays attached and unfaulted, the shared
// dial keeps running, and the miss is counted as an expired send budget.
func TestLocalPeerDialWaitExpiryDoesNotFaultPeer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    overlayKind
		payload int
		mode    rebroadcastMode
	}{
		{name: "simple", kind: overlayKindFastSync, payload: 100, mode: rebroadcastModeSimple},
		{name: "fec", kind: overlayKindCustomFixed, payload: 5000, mode: rebroadcastModeFEC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newLocalPeerQueueFixture(t, tc.kind)
			dht := fx.resolveThroughDHT(t)
			req := rebroadcastRequest{
				subscription: fx.sub,
				kind:         localSendTestKind,
				payload:      bytes.Repeat([]byte{0x5a}, tc.payload),
				local:        true,
			}
			if plan := fx.sub.rebroadcastPlan(req.kind, req.payloadLen()); plan.mode != tc.mode {
				t.Fatalf("request plan = %+v, want mode %d", plan, tc.mode)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if fx.sub.rebroadcastToPeer(ctx, fx.peer, req) {
				t.Fatal("own send without a connection reported delivery")
			}

			if fx.sub.peerByID(fx.peer.id) != fx.peer {
				t.Fatal("expired dial wait evicted the peer")
			}
			if got := fx.failedQueries(); got != 0 {
				t.Fatalf("expired dial wait faulted the peer: failed queries = %d", got)
			}
			if got := fx.localDrops(req.kind, "local_send_context_deadline"); got != 1 {
				t.Fatalf("local_send_context_deadline drops = %d, want 1", got)
			}
			select {
			case <-dht.started:
			case <-time.After(5 * time.Second):
				t.Fatal("the wait did not start the shared dial")
			}
			if !fx.peer.route.QUICDialInFlight() || fx.peer.route.QUICDialFailing() {
				t.Fatal("the expired wait aborted the shared dial or marked the route failing")
			}
		})
	}
}

func TestNewOverlayPeerLocalBroadcastPeer(t *testing.T) {
	for _, useQUIC := range []bool{true, false} {
		t.Run(fmt.Sprintf("quic=%t", useQUIC), func(t *testing.T) {
			pool, pooled, _ := newTestLeasedPooledPeer(fmt.Sprintf("local-broadcast-peer-%t", useQUIC))
			overlayID := testPeerID(fmt.Sprintf("local-broadcast-peer-overlay-%t", useQUIC))
			sub := testOverlaySubscription(&overlaySubscription{
				node: &Node{pool: pool},
				log:  discardLogger(),
				spec: overlaySpec{
					Name:    "custom.local-broadcast-peer",
					Kind:    overlayKindCustomFixed,
					ShortID: overlayID[:],
					UseQUIC: useQUIC,
				},
				peers: map[PeerID]*overlayPeer{},
			})
			t.Cleanup(sub.broadcastReceiver.Close)

			peer := mustNewTestOverlayPeer(t, sub, pooled, nil, true)
			t.Cleanup(peer.release)

			if !useQUIC {
				if peer.broadcastPeer != overlay.BroadcastPeer(peer.overlay) || peer.localBroadcastPeer != overlay.BroadcastPeer(peer.overlay) {
					t.Fatalf("ADNL peer broadcast identities = %T / %T, want the peer's overlay for both", peer.broadcastPeer, peer.localBroadcastPeer)
				}
				return
			}

			relay, ok := peer.broadcastPeer.(quicRouteBroadcastPeer)
			if !ok {
				t.Fatalf("QUIC relay identity = %T, want quicRouteBroadcastPeer", peer.broadcastPeer)
			}
			if relay.peer != peer || relay.envelope != sub.quicEnvelope {
				t.Fatal("QUIC relay identity is not bound to the peer and the overlay envelope")
			}
			local, ok := peer.localBroadcastPeer.(quicLocalBroadcastPeer)
			if !ok {
				t.Fatalf("QUIC local identity = %T, want quicLocalBroadcastPeer", peer.localBroadcastPeer)
			}
			if local.route != relay {
				t.Fatal("QUIC local identity does not share the relay identity's route")
			}
		})
	}
}

func TestLocalTwoStepPeerSetMarksOnlyQUICPeers(t *testing.T) {
	quicPeer := quicRouteBroadcastPeer{peer: &overlayPeer{id: testPeerID("local-set-quic")}}
	rldpPeer := customRLDPBroadcastPeer{id: testPeerID("local-set-rldp")}

	peers := localTwoStepPeerSet(overlay.StaticBroadcastPeerSet{quicPeer, rldpPeer})
	if local, ok := peers[0].(quicLocalBroadcastPeer); !ok || local.route != quicPeer {
		t.Fatalf("QUIC peer = %T, want quicLocalBroadcastPeer over the same route", peers[0])
	}
	if peers[1] != overlay.BroadcastPeer(rldpPeer) {
		t.Fatalf("RLDP peer = %T, want it unchanged", peers[1])
	}
}

func TestLocalSendDropReasonsCoverEveryFault(t *testing.T) {
	seen := make(map[string]TwoStepSendFault, len(localSendDropReasons))
	for fault, reason := range localSendDropReasons {
		if !strings.HasPrefix(reason, "local_send_") || len(reason) == len("local_send_") {
			t.Fatalf("fault %d reason = %q, want a local_send_ reason", fault, reason)
		}
		if previous, ok := seen[reason]; ok {
			t.Fatalf("faults %d and %d share reason %q", previous, fault, reason)
		}
		seen[reason] = TwoStepSendFault(fault)
	}
}

// failingBroadcastPeer fails every send with the same error.
type failingBroadcastPeer struct {
	id  []byte
	err error
}

func (p failingBroadcastPeer) ID() []byte {
	return p.id
}

func (p failingBroadcastPeer) SendCustomMessage(context.Context, tl.Serializable) error {
	return p.err
}

// Every recipient an own broadcast misses is counted by reason on the per-peer
// paths; a relayed miss is not.
func TestLocalPeerSendFailureCountedByReason(t *testing.T) {
	const kind = "tonNode.newShardBlockBroadcast"

	for _, tc := range []struct {
		name    string
		payload int
		err     error
		reason  string
	}{
		{name: "simple", payload: 100, err: errQUICPeerOffline, reason: "local_send_peer_offline"},
		{name: "fec", payload: 2000, err: context.DeadlineExceeded, reason: "local_send_context_deadline"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newTestNode(t)
			peer := testRebroadcastQueuePeer("local-drop-" + tc.name)
			failing := failingBroadcastPeer{id: peer.id[:], err: tc.err}
			peer.broadcastPeer = failing
			peer.localBroadcastPeer = failing
			overlayID := testPeerID("local-drop-overlay-" + tc.name)
			sub := testOverlaySubscription(&overlaySubscription{
				node: node,
				log:  discardLogger(),
				spec: overlaySpec{
					Name:    "custom.local-drop",
					Kind:    overlayKindCustomFixed,
					ShortID: overlayID[:],
				},
				peers: map[PeerID]*overlayPeer{peer.id: peer},
			})
			t.Cleanup(sub.broadcastReceiver.Close)

			request := func(local bool) rebroadcastRequest {
				return rebroadcastRequest{
					subscription: sub,
					kind:         kind,
					payload:      bytes.Repeat([]byte{0x11}, tc.payload),
					local:        local,
				}
			}
			count := func() uint64 {
				return testBroadcastDropStatCount(node, sub.spec.Name, kind, tc.reason)
			}

			if sub.rebroadcastToPeer(context.Background(), peer, request(false)) {
				t.Fatal("failing relay send reported delivery")
			}
			if got := count(); got != 0 {
				t.Fatalf("relayed miss counted as %q: %d", tc.reason, got)
			}

			if sub.rebroadcastToPeer(context.Background(), peer, request(true)) {
				t.Fatal("failing own send reported delivery")
			}
			if got := count(); got != 1 {
				t.Fatalf("own miss %q count = %d, want 1", tc.reason, got)
			}
		})
	}
}
