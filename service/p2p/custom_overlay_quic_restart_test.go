package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/address"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Regression for "the first external message after a custom-overlay peer
// restarts is silently lost (use_quic: true)".
//
// Node A (a real Node, real QUIC gateway with the production connection
// handler) is msg_sender of the custom fixed overlay "node-conn" with use_quic
// and skip_public_msg_send. Peer B is the only other member and is
// block_sender. B is a real tonutils QUIC gateway that decodes what it receives
// with an overlay.BroadcastReceiver configured as gton configures a custom
// two-step receiver, so "delivered" means B reconstructed the external.
//
// The external enters exactly as the liteserver hands it over:
// Node.SendExternalMessage -> custom two-step queue -> the subscription's
// two-step worker (runTwoStepRebroadcastLoop/sendTwoStepRebroadcast). After a
// restart A holds only the connection B opened, and a broadcast A originates
// must wait for A's own dial rather than drop B's part.
//
// An external A sends while B is down for longer than the QUIC handshake
// timeout fails the shared dial, and A's route to B is then failing behind a
// 10-30 s retry gate. B's new connection to A once it is back reopens the
// route (authenticatedQUICPeer.reopenFailingRoute), so the next external waits
// for A's redial instead of being dropped until the gate expires
// (TestCustomQUICInboundPathReopensFailingRoute).
//
// Not covered, and not asserted here (see reopenFailingRoute): externals
// written to the zombie outbound connection a crash leaves behind; a crash
// while A held only B's inbound connection, where B's new connection joins
// that live path without a handler and A's failed dial keeps the route gated;
// B coming back in the last ~2 s of an in-flight dial of A's, where the
// handler runs before that dial's failure is recorded; and a B that never
// dials A.

// TestCustomQUICFirstLocalBroadcastAfterPeerRestartIsDelivered: B restarts
// gracefully (same key, same UDP port) and re-establishes its own connection to
// A; A's first local external still reaches B.
func TestCustomQUICFirstLocalBroadcastAfterPeerRestartIsDelivered(t *testing.T) {
	h := newQUICRestartHarness(t, true)
	h.assertWarmPathDelivers(t)

	h.restartRemote(t, true)
	h.assertFirstDelivered(t)
}

// TestCustomQUICFirstLocalBroadcastAfterPeerCrashIsDelivered is the kill -9
// flavour: B's socket disappears without a CONNECTION_CLOSE, B comes back on
// the same port and dials A at once. A's own outbound connection is a zombie
// until the QUIC idle timeout (15s, hard-coded in tonutils defaultQUICConfig)
// closes it; the first external after that still reaches B. Externals sent
// inside that window go over the zombie, count as sent and are lost - this
// fix does not cover them, so the test sends only once the zombie is gone.
func TestCustomQUICFirstLocalBroadcastAfterPeerCrashIsDelivered(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the 15s QUIC idle timeout")
	}

	h := newQUICRestartHarness(t, true)
	h.assertWarmPathDelivers(t)

	h.restartRemote(t, false)
	h.assertFirstDelivered(t)
}

// TestCustomQUICInboundPathReopensFailingRoute: B stays down long enough for
// the dial A's external starts to fail on the handshake idle timeout, so A's
// route to B is failing behind the 10 s retry gate and B's address is stale. B
// comes back on the same port and dials A; that new path reopens the route,
// and A's next external, sent at once, reaches B. Without the reopen it is
// dropped until the gate expires.
func TestCustomQUICInboundPathReopensFailingRoute(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a 5s QUIC handshake to time out")
	}

	h := newQUICRestartHarness(t, true)
	dht := h.resolveRemoteThroughDHT(t, h.remote.addr)
	h.assertWarmPathDelivers(t)
	route := h.peer.route

	h.stopRemote(t, true)
	if _, sent := h.sendLocalExternal(t, h.seq()); sent {
		t.Fatal("external sent while B was down was counted as sent")
	}
	quicRestartWait(t, quicRelayDialTimeout, "A's dial to the stopped B to fail", func() bool {
		return route.QUICDialFailing() && !route.QUICDialInFlight()
	})
	failedAt := time.Now()
	if route.QUICDialPermitted(failedAt) || !route.QUICAddressStale() {
		t.Fatal("precondition: the failed dial armed no retry gate or left B's address fresh")
	}

	h.returnRemote(t)
	h.awaitInboundPath(t)
	// The handler registers the path before it reopens the route; an external
	// sent in between would meet the failing route and be dropped.
	quicRestartWait(t, time.Second, "B's new path to reopen A's route", func() bool {
		return !route.QUICDialFailing()
	})

	sentBefore, droppedBefore := h.counters()
	payload, sent := h.sendLocalExternal(t, h.seq())
	if !sent {
		t.Fatalf("first external after B came back and dialed A was dropped\nlogs:\n%s", h.logs.String())
	}
	if !h.remote.awaitDelivery(payload, 5*time.Second) {
		t.Fatalf("first external after B came back did not reach B\nlogs:\n%s", h.logs.String())
	}
	if elapsed := time.Since(failedAt); elapsed >= peerRouteRetryPolicy.MinDelay {
		t.Fatalf("external delivered %s after the failed dial, past the retry gate", elapsed)
	}

	if route.QUICDialFailing() {
		t.Fatal("route still failing after the redial")
	}
	if !h.hasOutbound() {
		t.Fatal("the delivered external left A without its own connection to B")
	}
	if dht.calls.Load() == 0 {
		t.Fatal("the redial did not re-resolve B's stale address")
	}
	if !strings.Contains(h.logs.String(), reopenedRouteLogMessage) {
		t.Fatalf("no reopen logged\nlogs:\n%s", h.logs.String())
	}
	sentAfter, droppedAfter := h.counters()
	if sentAfter-sentBefore != 1 || droppedAfter != droppedBefore {
		t.Fatalf("local rebroadcast counters moved by sent=%d dropped=%d, want 1 and 0", sentAfter-sentBefore, droppedAfter-droppedBefore)
	}
}

// TestCustomQUICLocalBroadcastOverInboundOnlyPathIsDelivered is the reduced
// form: the only QUIC connection between A and B is the one B opened, which is
// the state a restart of B leaves behind.
func TestCustomQUICLocalBroadcastOverInboundOnlyPathIsDelivered(t *testing.T) {
	h := newQUICRestartHarness(t, false)
	h.remote.dialNode(t, h.node.quicPublicKey, h.nodeAddr)
	h.awaitInboundPath(t)

	h.assertFirstDelivered(t)
}

type quicRestartHarness struct {
	node       *Node
	nodeAddr   string
	sub        *overlaySubscription
	peer       *overlayPeer
	spec       overlaySpec
	remoteKey  ed25519.PrivateKey
	remotePub  ed25519.PublicKey
	remoteID   PeerID
	remotePort int
	remote     *quicRestartRemote
	logs       *quicRestartLog
	nextSeq    byte
}

// newQUICRestartHarness builds node A with the "node-conn" subscription and
// starts the first incarnation of B. With prewarm the subscription starts the
// way Node.startSubscription does (prewarmQUICPeers, then the two-step worker);
// sub.run is not started because its discovery loop needs DHT and ADNL, and
// its fixed-peer probe goes over ADNL anyway.
func newQUICRestartHarness(t *testing.T, prewarm bool) *quicRestartHarness {
	t.Helper()

	node := newTestNode(t)
	node.zeroStateFileHash = bytes.Repeat([]byte{0x11}, 32)
	logs := &quicRestartLog{}
	node.log = zerolog.New(logs).Level(zerolog.DebugLevel)
	runCtx, cancelRun := context.WithCancel(context.Background())
	node.runCtx = runCtx
	// Production wiring from node_lifecycle.go: the gateway calls this for
	// every new (local, peer) path, inbound or outbound.
	node.quicGateway.SetConnectionHandler(node.handleInboundQUICPeer)
	nodeAddr := startQUICOutboundTestGateway(t, node.quicGateway)
	t.Cleanup(func() {
		cancelRun()
		node.wg.Wait()
	})

	remoteKey := quicOutboundTestKey(t)
	remotePub := remoteKey.Public().(ed25519.PublicKey)
	remoteID := peerIDForQUICOutboundTest(t, remotePub)

	spec, localMember, err := buildCustomOverlaySpec(node.zeroStateFileHash, CustomOverlayConfig{
		Name: "node-conn",
		Nodes: []CustomOverlayNodeConfig{
			{ADNLID: node.localID, MsgSender: true},
			{ADNLID: remoteID, BlockSender: true},
		},
		SkipPublicMsgSend: true,
		UseQUIC:           true,
	}, node.localID)
	if err != nil {
		t.Fatalf("build custom overlay spec: %v", err)
	}
	if !localMember {
		t.Fatal("node A is not a member of its own custom overlay")
	}

	remote := startQUICRestartRemote(t, remoteKey, 0, spec)

	sub := mustGetOrCreateSubscription(t, node, spec)
	sub.setActive(true, time.Time{})
	sub.log = node.log.With().Str("overlay", spec.Name).Logger()

	// The route is the node-wide one handleInboundQUICPeer also uses, as for a
	// pooled peer; connectFixedNode files B's QUIC address into it.
	route := node.peerRoutes.Get(remoteID)
	route.SetQUICAddress(remote.addr)
	// Mirrors newOverlayPeer for UseQUIC without the ADNL/RLDP transports: a
	// fixed member that stays attached.
	peer := &overlayPeer{
		node:         node,
		id:           remoteID,
		addr:         remote.addr,
		route:        route,
		pub:          remotePub,
		overlayID:    spec.ShortID,
		fixedMember:  true,
		overlay:      &overlay.ADNLOverlayWrapper{},
		release:      func() {},
		attachedAt:   time.Now(),
		prioritySend: &quicPrioritySendLatch{},
	}
	peer.queryTransport = quicPeerQueryTransport{peer: peer, envelope: sub.quicEnvelope}
	relay := quicRouteBroadcastPeer{peer: peer, envelope: sub.quicEnvelope}
	peer.broadcastPeer = relay
	peer.localBroadcastPeer = quicLocalBroadcastPeer{route: relay}
	peer.initRebroadcastQueues()
	sub.mx.Lock()
	sub.peers[remoteID] = peer
	sub.notifyPeersChangedLocked()
	sub.mx.Unlock()

	subCtx, cancelSub := context.WithCancel(runCtx)
	if _, ok := sub.setRunCancel(cancelSub); !ok {
		cancelSub()
		t.Fatal("mark subscription running")
	}
	if prewarm {
		sub.prewarmQUICPeers()
	}
	sub.startTwoStepRebroadcastWorker(subCtx)

	return &quicRestartHarness{
		node:       node,
		nodeAddr:   nodeAddr,
		sub:        sub,
		peer:       peer,
		spec:       spec,
		remoteKey:  remoteKey,
		remotePub:  remotePub,
		remoteID:   remoteID,
		remotePort: remote.port,
		remote:     remote,
		logs:       logs,
	}
}

// resolveRemoteThroughDHT gives A a DHT record advertising addr as B's QUIC
// endpoint, which a dial after a failed one re-resolves; B always comes back on
// the same port.
func (h *quicRestartHarness) resolveRemoteThroughDHT(t *testing.T, addr string) *outboundRouteDHT {
	t.Helper()

	endpoint, err := netip.ParseAddrPort(addr)
	if err != nil {
		t.Fatalf("parse B's advertised QUIC endpoint: %v", err)
	}
	dht := &outboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.QUIC{
					IP:   net.IP(endpoint.Addr().AsSlice()),
					Port: int32(endpoint.Port()),
				},
			},
			ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
		},
		pub: h.remotePub,
	}
	h.node.dht = dht
	return dht
}

func (h *quicRestartHarness) hasOutbound() bool {
	_, err := h.node.quicGateway.OutboundPeerDefaultID(h.remoteID[:])
	return err == nil
}

func (h *quicRestartHarness) counters() (sent, dropped uint64) {
	return h.node.localRebroadcastSent.Load(), h.node.localRebroadcastDropped.Load()
}

// submitLocalExternal submits a new external exactly as the liteserver does
// and returns the tonNode.externalMessageBroadcast payload B must reconstruct.
func (h *quicRestartHarness) submitLocalExternal(t *testing.T, seq byte) []byte {
	t.Helper()

	body := quicRestartExternalBOC(t, seq)
	parsed, err := parseExternalMessageData(body)
	if err != nil {
		t.Fatalf("parse external %d: %v", seq, err)
	}
	// nil here is what the liteserver turns into sendMsgStatus status:1.
	if err = h.node.SendExternalMessage(context.Background(), body, parsed.message.DstAddr); err != nil {
		t.Fatalf("SendExternalMessage %d: %v", seq, err)
	}

	payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: body},
	}, true)
	if err != nil {
		t.Fatalf("serialize expected payload: %v", err)
	}
	return payload
}

// sendLocalExternal submits an external and waits for the two-step worker to
// account for it; the bool is whether the worker counted it as sent.
func (h *quicRestartHarness) sendLocalExternal(t *testing.T, seq byte) ([]byte, bool) {
	t.Helper()

	sentBefore, droppedBefore := h.counters()
	payload := h.submitLocalExternal(t, seq)
	quicRestartWait(t, 2*peerRebroadcastTimeout, fmt.Sprintf("two-step worker outcome for external %d", seq), func() bool {
		sent, dropped := h.counters()
		return sent+dropped > sentBefore+droppedBefore
	})
	sent, dropped := h.counters()
	if sent+dropped != sentBefore+droppedBefore+1 {
		t.Fatalf("external %d produced %d worker outcomes, want 1", seq, sent+dropped-sentBefore-droppedBefore)
	}
	return payload, sent > sentBefore
}

func (h *quicRestartHarness) assertWarmPathDelivers(t *testing.T) {
	t.Helper()

	quicRestartWait(t, quicRelayDialTimeout, "subscription prewarm to open A->B", h.hasOutbound)

	payload, sent := h.sendLocalExternal(t, h.seq())
	if !sent {
		t.Fatalf("warm external was dropped\nlogs:\n%s", h.logs.String())
	}
	if !h.remote.awaitDelivery(payload, 5*time.Second) {
		t.Fatal("warm external did not reach B")
	}
}

// restartRemote stops B and starts it again with the same key on the same UDP
// port; the new B immediately opens its own connection to A, as a block_sender
// does. graceful=true is systemctl restart (Gateway.Close sends
// CONNECTION_CLOSE); graceful=false is a crash (socket closed first).
func (h *quicRestartHarness) restartRemote(t *testing.T, graceful bool) {
	t.Helper()

	h.stopRemote(t, graceful)
	h.returnRemote(t)

	if !graceful {
		quicRestartWait(t, 45*time.Second, "A's zombie outbound connection to hit the QUIC idle timeout", func() bool {
			return !h.hasOutbound()
		})
	}

	authenticated := h.awaitInboundPath(t)
	h.node.quicPeersMx.RLock()
	markedOutbound := authenticated.outbound
	h.node.quicPeersMx.RUnlock()
	if graceful && markedOutbound {
		t.Fatal("handleInboundQUICPeer for B's new path should have registered it as inbound-only")
	}
	if h.sub.peerByID(h.remoteID) != h.peer {
		t.Fatal("B is no longer the attached fixed member")
	}
}

// stopRemote stops B; after a graceful stop it waits until A has dropped its
// outbound connection.
func (h *quicRestartHarness) stopRemote(t *testing.T, graceful bool) {
	t.Helper()

	if !h.hasOutbound() {
		t.Fatal("precondition: A has no outbound connection to B before the stop")
	}

	h.remote.stop(graceful)
	if graceful {
		quicRestartWait(t, 30*time.Second, "A to drop its outbound connection after B's shutdown", func() bool {
			return !h.hasOutbound()
		})
	}
}

// returnRemote starts B again with the same key on the same UDP port and has
// it open its own connection to A at once.
func (h *quicRestartHarness) returnRemote(t *testing.T) {
	t.Helper()

	h.remote = startQUICRestartRemote(t, h.remoteKey, h.remotePort, h.spec)
	h.remote.dialNode(t, h.node.quicPublicKey, h.nodeAddr)
}

// awaitInboundPath waits until A holds the shared gateway path for B and the
// production handler has registered it.
func (h *quicRestartHarness) awaitInboundPath(t *testing.T) *authenticatedQUICPeer {
	t.Helper()

	return awaitQUICPathRegistered(t, h.node, h.remotePub, h.remoteID)
}

// assertFirstDelivered sends the first external while A holds only the
// connection B opened: the worker must count it as sent, B must reconstruct
// it, and no recipient may be counted as missed.
func (h *quicRestartHarness) assertFirstDelivered(t *testing.T) {
	t.Helper()

	// Nothing on A brings its own direction back while B keeps feeding it,
	// so the send below is the one that has to wait for A's dial.
	if h.hasOutbound() {
		t.Fatal("precondition: A already has an outbound connection to B")
	}

	sentBefore, droppedBefore := h.counters()
	payload, sent := h.sendLocalExternal(t, h.seq())
	if !sent {
		t.Fatalf("first external over the inbound-only path was dropped\nlogs:\n%s", h.logs.String())
	}
	if !h.remote.awaitDelivery(payload, 5*time.Second) {
		t.Fatalf("first external over the inbound-only path did not reach B\nlogs:\n%s", h.logs.String())
	}
	if !h.hasOutbound() {
		t.Fatal("the delivered external left A without its own connection to B")
	}

	sentAfter, droppedAfter := h.counters()
	if sentAfter-sentBefore != 1 || droppedAfter != droppedBefore {
		t.Fatalf("local rebroadcast counters moved by sent=%d dropped=%d, want 1 and 0", sentAfter-sentBefore, droppedAfter-droppedBefore)
	}
	for _, reason := range localSendDropReasons {
		if got := testBroadcastDropStatCount(h.node, h.spec.Name, "tonNode.externalMessageBroadcast", reason); got != 0 {
			t.Fatalf("local send drops %q = %d, want 0", reason, got)
		}
	}
}

func (h *quicRestartHarness) seq() byte {
	h.nextSeq++
	return h.nextSeq
}

// quicRestartRemote is one process lifetime of B.
type quicRestartRemote struct {
	gateway   *adnlquic.Gateway
	conn      *net.UDPConn
	addr      string
	port      int
	serveDone chan struct{}
	receiver  *overlay.BroadcastReceiver
	delivered chan []byte
	stopped   atomic.Bool
}

func startQUICRestartRemote(t *testing.T, key ed25519.PrivateKey, port int, spec overlaySpec) *quicRestartRemote {
	t.Helper()

	gateway, err := adnlquic.NewGatewayWithLimits(nodeQUICLimits(), key)
	if err != nil {
		t.Fatalf("create B gateway: %v", err)
	}
	localID, err := NewPeerID(gateway.ID())
	if err != nil {
		t.Fatalf("parse B id: %v", err)
	}
	overlayID := spec.ShortID
	// B runs the same overlay config, so it receives the way
	// newOverlaySubscription sets up a custom fixed receiver: no FEC relay,
	// configured senders authorized, two-step enabled (it relays to nobody: A
	// is the only other member and the source).
	receiver, err := overlay.NewBroadcastReceiver(overlayID, spec.unauthorizedBroadcastLimit(), spec.relaysFECBroadcasts(), false)
	if err != nil {
		t.Fatalf("create B broadcast receiver: %v", err)
	}
	receiver.SetAuthorizedKeys(spec.AuthorizedKeys)
	receiver.EnableBroadcastTwoStep(localID.Bytes(), overlay.StaticBroadcastPeerSet{})

	r := &quicRestartRemote{
		gateway:   gateway,
		serveDone: make(chan struct{}),
		receiver:  receiver,
		delivered: make(chan []byte, 16),
	}
	receiver.SetBroadcastHandlerWithInfo(func(_ tl.Serializable, info overlay.BroadcastInfo) overlay.BroadcastDisposition {
		select {
		case r.delivered <- bytes.Clone(info.Payload):
		default:
		}
		return overlay.BroadcastDispositionAcceptAndRelay
	})
	gateway.SetConnectionHandler(func(peer *adnlquic.Peer) error {
		var from PeerID
		copy(from[:], peer.PeerID())
		peer.SetMessageHandler(func(_ context.Context, payload []byte) {
			header, body, parseErr := parseQUICMessageEnvelope(payload)
			if parseErr != nil || !bytes.Equal(header.overlay, overlayID) || !isTwoStepBroadcast(body) {
				return
			}
			msg, parseErr := parseOneQUICOverlayObject(body)
			if parseErr != nil {
				return
			}
			_ = receiver.HandleMessage(quicBroadcastSource{id: &from}, msg)
		})
		return nil
	})

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		receiver.Close()
		t.Fatalf("listen B on port %d: %v", port, err)
	}
	r.conn = conn
	r.addr = conn.LocalAddr().String()
	r.port = conn.LocalAddr().(*net.UDPAddr).Port
	go func() {
		_ = gateway.Serve(conn)
		close(r.serveDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = gateway.WaitReady(ctx); err != nil {
		r.stop(true)
		t.Fatalf("wait for B gateway: %v", err)
	}
	t.Cleanup(func() { r.stop(true) })
	return r
}

func (r *quicRestartRemote) stop(graceful bool) {
	if !r.stopped.CompareAndSwap(false, true) {
		return
	}
	if graceful {
		_ = r.gateway.Close()
		_ = r.conn.Close()
	} else {
		_ = r.conn.Close()
		_ = r.gateway.Close()
	}
	<-r.serveDone
	r.receiver.Close()
}

// dialNode opens B's own connection to A. A retry covers the short window in
// which A's gateway still holds the closed path of the previous incarnation.
func (r *quicRestartRemote) dialNode(t *testing.T, nodePub ed25519.PublicKey, nodeAddr string) {
	t.Helper()

	quicRestartWait(t, 5*time.Second, "B to dial A", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := r.gateway.DialDefault(ctx, nodePub, nodeAddr)
		return err == nil
	})
}

func (r *quicRestartRemote) awaitDelivery(payload []byte, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case got := <-r.delivered:
			if bytes.Equal(got, payload) {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

type quicRestartLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *quicRestartLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *quicRestartLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// quicRestartExternalBOC builds a distinct external per sequence number, so
// neither the local dedupe nor the per-address limiter interferes.
func quicRestartExternalBOC(t *testing.T, seq byte) []byte {
	t.Helper()

	dst := address.MustParseRawAddr(fmt.Sprintf("0:%s%02x", strings.Repeat("11", 31), seq))
	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   dst,
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().MustStoreUInt(uint64(seq), 8).EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	return root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
}

// quicRestartWait polls cond until it holds or the timeout expires.
func quicRestartWait(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
