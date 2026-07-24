package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type testOverlayADNL struct {
	closerCtx         context.Context
	closeFn           context.CancelFunc
	customHandler     func(msg *adnl.MessageCustom) error
	queryHandler      func(msg *adnl.MessageQuery) error
	queryResponder    func(req tl.Serializable, result tl.Serializable) error
	answerResponder   func(ctx context.Context, queryID []byte, result tl.Serializable) error
	nopResponder      func(ctx context.Context) error
	disconnectHandler func(addr string, key ed25519.PublicKey)
	statsFn           func() adnl.PeerStats
	sent              []tl.Serializable
	reinits           atomic.Int64
	nops              atomic.Int64
	id                []byte
	pub               ed25519.PublicKey
	remoteAddr        string
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

func (m *testOverlayADNL) GetQueryHandler() func(msg *adnl.MessageQuery) error {
	return m.queryHandler
}

func (m *testOverlayADNL) SendCustomMessage(_ context.Context, req tl.Serializable) error {
	m.sent = append(m.sent, req)
	return nil
}

func (m *testOverlayADNL) Query(_ context.Context, req tl.Serializable, result tl.Serializable) error {
	if m.queryResponder != nil {
		return m.queryResponder(req, result)
	}
	return nil
}

func (m *testOverlayADNL) SendNop(ctx context.Context) error {
	m.nops.Add(1)
	if m.nopResponder != nil {
		return m.nopResponder(ctx)
	}
	return nil
}

func (m *testOverlayADNL) Ping(context.Context) (time.Duration, error) {
	return 0, nil
}

func (m *testOverlayADNL) Reinit() {
	m.reinits.Add(1)
}

func (m *testOverlayADNL) Answer(ctx context.Context, queryID []byte, result tl.Serializable) error {
	if m.answerResponder != nil {
		return m.answerResponder(ctx, queryID, result)
	}
	return nil
}

func (m *testOverlayADNL) GetCloserCtx() context.Context {
	return m.closerCtx
}

func (m *testOverlayADNL) RemoteAddr() string {
	if m.remoteAddr != "" {
		return m.remoteAddr
	}
	return "127.0.0.1:17555"
}

func (m *testOverlayADNL) GetID() []byte {
	if len(m.id) > 0 {
		return m.id
	}
	return []byte("test-peer")
}

func (m *testOverlayADNL) GetPubKey() ed25519.PublicKey {
	return m.pub
}

func (*testOverlayADNL) SetAddresses(adnladdr.List) {}

func (m *testOverlayADNL) Stats() adnl.PeerStats {
	if m.statsFn != nil {
		return m.statsFn()
	}
	return adnl.PeerStats{}
}

func (m *testOverlayADNL) Close() {
	m.closeFn()
}

func newTestOverlayWrapper() (*overlay.ADNLOverlayWrapper, *testOverlayADNL) {
	base := newTestOverlayADNL()
	overlayID := testPeerID("test-overlay")
	receiver, err := overlay.NewBroadcastReceiver(overlayID[:], maxOverlayPayloadSize, true, false)
	if err != nil {
		panic(err)
	}
	closeBase := base.closeFn
	base.closeFn = func() {
		receiver.Close()
		closeBase()
	}
	wrapper, err := overlay.CreateExtendedADNL(base).AttachOverlay(receiver)
	if err != nil {
		panic(err)
	}
	return wrapper, base
}

func testIsGetRandomPeersQuery(req tl.Serializable) bool {
	_, ok := testOverlayQueryPayload(req).(overlay.GetRandomPeers)
	return ok
}

func testOverlayQueryPayload(req tl.Serializable) tl.Serializable {
	arr, ok := req.([]tl.Serializable)
	if !ok || len(arr) != 2 {
		return nil
	}
	return arr[1]
}

func waitForTestPeerWarmupIdle(t *testing.T, peer *overlayPeer) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		peer.statsMx.Lock()
		running := peer.warmupRunning
		peer.statsMx.Unlock()
		if !running {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("peer warmup did not finish")
}

func waitForTestAliveKnownPeerCount(t *testing.T, sub *overlaySubscription, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := sub.aliveKnownPeerCount(); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("alive known peer count = %d, want %d", sub.aliveKnownPeerCount(), want)
}

func TestQueryCandidatesSkipClosedPeers(t *testing.T) {
	now := int32(time.Now().Unix())

	openOverlay, _ := newTestOverlayWrapper()
	closedOverlay, closedConn := newTestOverlayWrapper()
	closedConn.Close()
	fallbackOverlay, _ := newTestOverlayWrapper()

	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[PeerID]*overlayPeer{
			testPeerID("peer-1"): {id: testPeerID("peer-1"), overlay: openOverlay, announced: &overlay.Node{Version: now}, alive: true},
			testPeerID("peer-2"): {id: testPeerID("peer-2"), overlay: closedOverlay, announced: &overlay.Node{Version: now}, alive: true},
			testPeerID("peer-3"): {id: testPeerID("peer-3"), overlay: fallbackOverlay, announced: &overlay.Node{Version: now}, alive: true},
		},
		neighbours: []PeerID{testPeerID("peer-1"), testPeerID("peer-2")},
	})

	got := sub.queryCandidates(0, 0)
	if len(got) != 2 {
		t.Fatalf("unexpected candidate count: got %d want 2", len(got))
	}
	if got[0].id != testPeerID("peer-1") {
		t.Fatalf("expected open neighbour first, got %q", got[0].id)
	}
	if got[1].id != testPeerID("peer-3") {
		t.Fatalf("expected open fallback peer second, got %q", got[1].id)
	}
}

func TestHandlePeerQueryFailureRemovesClosedPeer(t *testing.T) {
	now := int32(time.Now().Unix())

	peerOverlay, _ := newTestOverlayWrapper()
	peer := &overlayPeer{
		id:        testPeerID("peer-1"),
		overlay:   peerOverlay,
		announced: &overlay.Node{Version: now},
		alive:     true,
	}

	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{testPeerID("peer-1"): peer},
		neighbours: []PeerID{testPeerID("peer-1")},
	})

	sub.handlePeerQueryFailure(peer, adnl.ErrPeerConnClosed)

	if _, ok := sub.peers[testPeerID("peer-1")]; ok {
		t.Fatal("closed peer must be removed from subscription")
	}
	if len(sub.neighbours) != 0 {
		t.Fatalf("closed peer must be removed from neighbours, got %d", len(sub.neighbours))
	}
}

func TestAttachPublicInboundPeerStartsRandomPeerWarmup(t *testing.T) {
	_, selfKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate self key: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerPool, pooled, base := newTestLeasedPooledPeer("public-inbound")
	fullID := make([]byte, 32)
	fullID[0] = 0x11
	shortID := make([]byte, 32)
	shortID[0] = 0x22

	node := &Node{
		log:     discardLogger(),
		privKey: selfKey,
		runCtx:  runCtx,
		pool:    peerPool,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		log:  discardLogger(),
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      fullID,
			ShortID:     shortID,
			RandomPeers: true,
		},
		peers:      map[PeerID]*overlayPeer{},
		peerNotify: make(chan struct{}, 1),
	})

	warmed := make(chan struct{}, 1)
	base.queryResponder = func(req tl.Serializable, _ tl.Serializable) error {
		arr, ok := req.([]tl.Serializable)
		if !ok || len(arr) != 2 {
			return nil
		}
		if _, ok = arr[1].(overlay.GetRandomPeers); !ok {
			return nil
		}
		select {
		case warmed <- struct{}{}:
		default:
		}
		return nil
	}

	ensureTestBroadcastReceiver(t, sub)
	if !sub.attachPooledPeer(pooled, nil) {
		t.Fatal("public inbound peer was not attached")
	}

	select {
	case <-warmed:
	case <-time.After(time.Second):
		t.Fatal("public inbound peer was not warmed with overlay.getRandomPeers")
	}

	cancel()
	node.wg.Wait()
}

func TestAttachPooledPeerRejectsClosedSubscription(t *testing.T) {
	pool, pooled, _ := newTestLeasedPooledPeer("closed-subscription")
	t.Cleanup(pooled.close)

	sub := testOverlaySubscription(&overlaySubscription{
		node: &Node{
			log:  discardLogger(),
			pool: pool,
		},
		log: discardLogger(),
		spec: overlaySpec{
			Kind:    overlayKindPublicShard,
			ShortID: testPeerID("closed-subscription-overlay").Bytes(),
		},
		peers: map[PeerID]*overlayPeer{},
	})
	ensureTestBroadcastReceiver(t, sub)
	sub.close()

	if sub.attachPooledPeer(pooled, nil) {
		t.Fatal("closed subscription accepted a peer")
	}
	if len(sub.peers) != 0 || pooled.refs != 0 || len(pooled.adnlOverlayRefs) != 0 || len(pooled.rldpOverlayRefs) != 0 {
		t.Fatal("closed subscription retained the rejected peer")
	}
}

func TestAttachCustomFixedPeerWarmsADNLWithNop(t *testing.T) {
	_, selfKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate self key: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerPool, pooled, base := newTestLeasedPooledPeer("custom-fixed")
	nopped := make(chan struct{})
	base.nopResponder = func(context.Context) error {
		select {
		case <-nopped:
		default:
			close(nopped)
		}
		return nil
	}

	node := &Node{
		log:     discardLogger(),
		privKey: selfKey,
		runCtx:  runCtx,
		pool:    peerPool,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		log:  discardLogger(),
		spec: overlaySpec{
			Kind:         overlayKindCustomFixed,
			ShortID:      []byte{0x42},
			FixedNodeIDs: map[PeerID]struct{}{pooled.id: {}},
		},
		peers:      map[PeerID]*overlayPeer{},
		peerNotify: make(chan struct{}, 1),
	})

	ensureTestBroadcastReceiver(t, sub)
	if !sub.attachPooledPeer(pooled, nil) {
		t.Fatal("custom fixed peer was not attached")
	}

	select {
	case <-nopped:
	case <-time.After(time.Second):
		t.Fatal("custom fixed peer ADNL nop warmup did not run")
	}
	peer := sub.peers[pooled.id]
	waitForTestPeerWarmupIdle(t, peer)
	if stats := peer.statsSnapshot(); !stats.lastReceiveAt.IsZero() || !stats.lastSuccessAt.IsZero() {
		t.Fatalf("nop warmup marked peer receive/success at %v/%v", stats.lastReceiveAt, stats.lastSuccessAt)
	}
	if sub.aliveKnownPeerCount() != 0 {
		t.Fatal("nop warmup should not mark custom fixed peer alive")
	}

	cancel()
	node.wg.Wait()
}

func TestCustomFixedPeerOnlyAnswersOverlayPingInOverlayLayer(t *testing.T) {
	_, selfKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate self key: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerPool, pooled, base := newTestLeasedPooledPeer("custom-query")
	answered := make(chan tl.Serializable, 1)
	base.answerResponder = func(_ context.Context, _ []byte, result tl.Serializable) error {
		select {
		case answered <- result:
		default:
		}
		return nil
	}

	shortID := testPeerID("custom-query-overlay").Bytes()
	node := &Node{
		log:     discardLogger(),
		privKey: selfKey,
		runCtx:  runCtx,
		pool:    peerPool,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		log:  discardLogger(),
		spec: overlaySpec{
			Kind:         overlayKindCustomFixed,
			ShortID:      shortID,
			FixedNodeIDs: map[PeerID]struct{}{pooled.id: {}},
		},
		peers:      map[PeerID]*overlayPeer{},
		peerNotify: make(chan struct{}, 1),
	})

	ensureTestBroadcastReceiver(t, sub)
	if !sub.attachPooledPeer(pooled, nil) {
		t.Fatal("custom fixed peer was not attached")
	}
	if base.queryHandler == nil {
		t.Fatal("base ADNL query handler was not installed")
	}
	if err = base.queryHandler(&adnl.MessageQuery{
		ID:   make([]byte, 32),
		Data: overlay.WrapQuery(shortID, GetCapabilities{}),
	}); err == nil || !strings.Contains(err.Error(), "overlay query is not supported in private overlay") {
		t.Fatalf("custom fixed getCapabilities error = %v, want private overlay reject", err)
	}

	select {
	case <-answered:
		t.Fatal("custom fixed app-level overlay query was answered")
	default:
	}

	if err = base.queryHandler(&adnl.MessageQuery{
		ID:   make([]byte, 32),
		Data: overlay.WrapQuery(shortID, overlay.Ping{}),
	}); err != nil {
		t.Fatalf("custom fixed overlay ping error = %v", err)
	}
	select {
	case result := <-answered:
		if _, ok := result.(overlay.Pong); !ok {
			t.Fatalf("custom fixed overlay ping answer = %T, want overlay.Pong", result)
		}
	case <-time.After(time.Second):
		t.Fatal("custom fixed overlay ping was not answered")
	}

	cancel()
	node.wg.Wait()
}

func TestAttachPublicAdvertisedPeerWaitsForPromotion(t *testing.T) {
	peerPool, pooled, _ := newTestLeasedPooledPeer("pending-public")
	shortID := []byte{0x31}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := &Node{
		log:    discardLogger(),
		runCtx: runCtx,
		pool:   peerPool,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		log:  discardLogger(),
		spec: overlaySpec{
			Kind:    overlayKindPublicShard,
			ShortID: shortID,
		},
		peers: map[PeerID]*overlayPeer{},
	})
	announcedPub := testPeerID("pending-public-key")
	announced := &overlay.Node{
		ID:      keys.PublicKeyED25519{Key: ed25519.PublicKey(announcedPub[:])},
		Overlay: shortID,
		Version: int32(time.Now().Unix()),
	}

	ensureTestBroadcastReceiver(t, sub)
	if !sub.attachPooledPeer(pooled, announced) {
		t.Fatal("public advertised peer was not attached")
	}

	peer := sub.peers[pooled.id]
	if peer == nil {
		t.Fatal("attached peer missing")
	}
	if stats := peer.statsSnapshot(); !stats.pending || stats.alive {
		t.Fatalf("attached public peer stats = pending %v alive %v, want pending true alive false", stats.pending, stats.alive)
	}
	if got := sub.aliveKnownPeerCount(); got != 0 {
		t.Fatalf("pending public peer counted as alive known: %d", got)
	}
	sub.reloadNeighbours()
	if len(sub.neighbours) != 0 {
		t.Fatalf("pending public peer entered neighbours: %d", len(sub.neighbours))
	}
	if got := len(sub.broadcastTargetsSnapshot().peers); got != 0 {
		t.Fatalf("pending public peer entered rebroadcast candidates: %d", got)
	}
	if got := len(sub.overlayNodesSnapshot()); got != 0 {
		t.Fatalf("pending public peer was advertised: %d", got)
	}

	if !peer.noteReceive() {
		t.Fatal("expected first receive to promote pending peer")
	}
	sub.peerPromoted(peer)

	if stats := peer.statsSnapshot(); stats.pending || !stats.alive {
		t.Fatalf("promoted public peer stats = pending %v alive %v, want pending false alive true", stats.pending, stats.alive)
	}
	if got := sub.aliveKnownPeerCount(); got != 1 {
		t.Fatalf("promoted public peer alive known count = %d, want 1", got)
	}
	if len(sub.neighbours) != 1 || sub.neighbours[0] != pooled.id {
		t.Fatalf("promoted public peer neighbours = %#v, want %s", sub.neighbours, pooled.id.String())
	}
	if got := len(sub.broadcastTargetsSnapshot().peers); got != 1 {
		t.Fatalf("promoted public peer rebroadcast candidates = %d, want 1", got)
	}
	if got := len(sub.overlayNodesSnapshot()); got != 1 {
		t.Fatalf("promoted public peer advertised nodes = %d, want 1", got)
	}
}

func TestExistingPendingPublicPeerRediscoveryRetriesWarmup(t *testing.T) {
	_, selfKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate self key: %v", err)
	}
	peerPub, peerKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}

	fullID := make([]byte, 32)
	fullID[0] = 0x41
	shortID, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		t.Fatalf("build overlay short id: %v", err)
	}
	announced, err := overlay.NewNode(fullID, peerKey)
	if err != nil {
		t.Fatalf("build announced overlay node: %v", err)
	}
	rawPeerID, err := tl.Hash(keys.PublicKeyED25519{Key: peerPub})
	if err != nil {
		t.Fatalf("hash peer id: %v", err)
	}
	peerID, err := NewPeerID(rawPeerID)
	if err != nil {
		t.Fatalf("parse peer id: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerPool, pooled, base := newTestLeasedPooledPeer("pending-rediscovery")
	delete(peerPool.peers, pooled.id)
	pooled.id = peerID
	pooled.pub = append(ed25519.PublicKey(nil), peerPub...)
	peerPool.peers[peerID] = pooled

	node := &Node{
		log:     discardLogger(),
		privKey: selfKey,
		runCtx:  runCtx,
		pool:    peerPool,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		log:  discardLogger(),
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      fullID,
			ShortID:     shortID,
			RandomPeers: true,
		},
		peers:      map[PeerID]*overlayPeer{},
		peerNotify: make(chan struct{}, 1),
	})

	var warmupCalls atomic.Int32
	firstWarmupDone := make(chan struct{})
	retryStarted := make(chan struct{}, 1)
	releaseRetry := make(chan struct{})
	base.queryResponder = func(req tl.Serializable, _ tl.Serializable) error {
		if !testIsGetRandomPeersQuery(req) {
			return nil
		}

		call := warmupCalls.Add(1)
		if call == 1 {
			close(firstWarmupDone)
			return errors.New("warmup failed")
		}

		select {
		case retryStarted <- struct{}{}:
		default:
		}
		<-releaseRetry
		return nil
	}

	ensureTestBroadcastReceiver(t, sub)
	if !sub.attachPooledPeer(pooled, announced) {
		t.Fatal("public advertised peer was not attached")
	}
	peer := sub.peers[peerID]
	if peer == nil {
		t.Fatal("attached peer missing")
	}

	select {
	case <-firstWarmupDone:
	case <-time.After(time.Second):
		t.Fatal("initial public peer warmup did not run")
	}
	waitForTestPeerWarmupIdle(t, peer)
	if stats := peer.statsSnapshot(); !stats.pending || stats.alive {
		t.Fatalf("failed warmup stats = pending %v alive %v, want pending true alive false", stats.pending, stats.alive)
	}
	if got := sub.aliveKnownPeerCount(); got != 0 {
		t.Fatalf("failed warmup public peer counted as alive known: %d", got)
	}

	attached, err := sub.connectOverlayNodeV1(context.Background(), *announced)
	if err != nil {
		t.Fatalf("rediscover existing pending peer: %v", err)
	}
	if attached {
		t.Fatal("rediscovered existing pending peer should not attach a duplicate")
	}
	select {
	case <-retryStarted:
	case <-time.After(time.Second):
		t.Fatal("rediscovered pending peer did not retry warmup")
	}

	attached, err = sub.connectOverlayNodeV1(context.Background(), *announced)
	if err != nil {
		t.Fatalf("rediscover pending peer while warmup runs: %v", err)
	}
	if attached {
		t.Fatal("rediscovered running pending peer should not attach a duplicate")
	}
	time.Sleep(50 * time.Millisecond)
	if got := warmupCalls.Load(); got != 2 {
		t.Fatalf("warmup calls while retry runs = %d, want 2", got)
	}

	close(releaseRetry)
	waitForTestAliveKnownPeerCount(t, sub, 1)
	if got := warmupCalls.Load(); got != 2 {
		t.Fatalf("warmup calls after retry = %d, want 2", got)
	}
	if stats := peer.statsSnapshot(); stats.pending || !stats.alive {
		t.Fatalf("retried warmup stats = pending %v alive %v, want pending false alive true", stats.pending, stats.alive)
	}

	cancel()
	node.wg.Wait()
}

func TestPingPeersRunsPeerQueriesConcurrently(t *testing.T) {
	const peerCount = 3
	const queryDelay = 150 * time.Millisecond

	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		spec:  overlaySpec{Kind: overlayKindPublicShard, QueryCapabilities: true},
		peers: map[PeerID]*overlayPeer{},
	})
	now := int32(time.Now().Unix())
	for i := 0; i < peerCount; i++ {
		id := testPeerID(string(rune('a' + i)))
		wrapper, base := newTestOverlayWrapper()
		base.queryResponder = func(tl.Serializable, tl.Serializable) error {
			time.Sleep(queryDelay)
			return nil
		}
		sub.peers[id] = &overlayPeer{
			id:            id,
			overlay:       wrapper,
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		sub.neighbours = append(sub.neighbours, id)
	}

	startedAt := time.Now()
	sub.pingPeers(context.Background())
	elapsed := time.Since(startedAt)
	if elapsed >= queryDelay*2 {
		t.Fatalf("pingPeers took %s for %d peers with %s query delay, expected concurrent execution", elapsed, peerCount, queryDelay)
	}
}

func TestStartPingPeersDoesNotBlockCaller(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := &Node{
		log:    discardLogger(),
		runCtx: runCtx,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		spec:  overlaySpec{Kind: overlayKindPublicShard, QueryCapabilities: true},
		peers: map[PeerID]*overlayPeer{},
	})
	id := testPeerID("slow")
	wrapper, base := newTestOverlayWrapper()
	base.queryResponder = func(tl.Serializable, tl.Serializable) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	sub.peers[id] = &overlayPeer{
		id:            id,
		overlay:       wrapper,
		announced:     &overlay.Node{Version: int32(time.Now().Unix())},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.neighbours = append(sub.neighbours, id)

	startedAt := time.Now()
	sub.startPingPeers(runCtx)
	if elapsed := time.Since(startedAt); elapsed > 50*time.Millisecond {
		t.Fatalf("startPingPeers blocked caller for %s", elapsed)
	}

	cancel()
	node.wg.Wait()
}

func TestStaleMaintenanceDoesNotRemoveReplacementPeer(t *testing.T) {
	id := testPeerID("maintenance-replacement")
	oldWrapper, oldBase := newTestOverlayWrapper()
	freshWrapper, freshBase := newTestOverlayWrapper()
	oldPeer := &overlayPeer{id: id, overlay: oldWrapper}
	freshPeer := &overlayPeer{id: id, overlay: freshWrapper, release: freshBase.Close}
	sub := testOverlaySubscription(&overlaySubscription{
		peers: map[PeerID]*overlayPeer{id: freshPeer},
	})

	oldBase.Close()
	if sub.peerReadyForMaintenance(context.Background(), oldPeer) {
		t.Fatal("closed stale peer reported ready for maintenance")
	}

	sub.mx.Lock()
	current := sub.peers[id]
	sub.mx.Unlock()
	if current != freshPeer {
		t.Fatal("stale maintenance removed the replacement peer")
	}
	select {
	case <-freshBase.GetCloserCtx().Done():
		t.Fatal("stale maintenance closed the replacement transport")
	default:
	}
}

func TestStaleForgetDoesNotRemoveReplacementPeer(t *testing.T) {
	id := testPeerID("forget-replacement")
	oldWrapper, _ := newTestOverlayWrapper()
	freshWrapper, freshBase := newTestOverlayWrapper()
	oldPeer := &overlayPeer{id: id, overlay: oldWrapper}
	freshPeer := &overlayPeer{id: id, overlay: freshWrapper, release: freshBase.Close}
	sub := testOverlaySubscription(&overlaySubscription{
		peers: map[PeerID]*overlayPeer{id: freshPeer},
	})

	sub.sendForgetPeer(context.Background(), oldPeer)

	sub.mx.Lock()
	current := sub.peers[id]
	sub.mx.Unlock()
	if current != freshPeer {
		t.Fatal("stale forget removed the replacement peer")
	}
	select {
	case <-freshBase.GetCloserCtx().Done():
		t.Fatal("stale forget closed the replacement transport")
	default:
	}
}
