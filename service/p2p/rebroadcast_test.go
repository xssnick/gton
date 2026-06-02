package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type mockRebroadcastPeer struct {
	id   []byte
	sent []tl.Serializable
}

func (m *mockRebroadcastPeer) ID() []byte {
	return append([]byte(nil), m.id...)
}

func (m *mockRebroadcastPeer) SendCustomMessage(_ context.Context, req tl.Serializable) error {
	m.sent = append(m.sent, req)
	return nil
}

func TestPlanRebroadcastMatchesCppNodeRouting(t *testing.T) {
	tests := []struct {
		name       string
		kind       string
		payloadLen int
		mode       rebroadcastMode
		flags      int32
	}{
		{name: "block broadcast always fec", kind: "tonNode.blockBroadcast", payloadLen: 32, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
		{name: "compressed block broadcast always fec", kind: "tonNode.blockBroadcastCompressedV2", payloadLen: 256, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
		{name: "small shard info stays simple", kind: "tonNode.newShardBlockBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize, mode: rebroadcastModeSimple},
		{name: "large shard info switches to fec anysender", kind: "tonNode.newShardBlockBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize + 1, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
		{name: "small external stays simple", kind: "tonNode.externalMessageBroadcast", payloadLen: 128, mode: rebroadcastModeSimple},
		{name: "large external switches to fec", kind: "tonNode.externalMessageBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize + 1, mode: rebroadcastModeFEC},
		{name: "small ihr stays simple", kind: "tonNode.ihrMessageBroadcast", payloadLen: 128, mode: rebroadcastModeSimple},
		{name: "large ihr switches to fec", kind: "tonNode.ihrMessageBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize + 1, mode: rebroadcastModeFEC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planRebroadcast(tt.kind, tt.payloadLen)
			if plan.mode != tt.mode || plan.flags != tt.flags {
				t.Fatalf("unexpected plan: got mode=%d flags=%d want mode=%d flags=%d", plan.mode, plan.flags, tt.mode, tt.flags)
			}
		})
	}
}

func TestPlanCustomRebroadcastMatchesCppNodeRouting(t *testing.T) {
	tests := []struct {
		name       string
		kind       string
		payloadLen int
		mode       rebroadcastMode
		flags      int32
	}{
		{name: "small shard info stays ordinary simple", kind: "tonNode.newShardBlockBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize, mode: rebroadcastModeSimple},
		{name: "large shard info uses two-step path with anysender", kind: "tonNode.newShardBlockBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize + 1, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
		{name: "small external uses two-step path", kind: "tonNode.externalMessageBroadcast", payloadLen: 128, mode: rebroadcastModeFEC},
		{name: "small ihr uses two-step path", kind: "tonNode.ihrMessageBroadcast", payloadLen: 128, mode: rebroadcastModeFEC},
		{name: "block broadcast uses two-step path with anysender", kind: "tonNode.blockBroadcastCompressedV2", payloadLen: 256, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planCustomRebroadcast(tt.kind, tt.payloadLen)
			if plan.mode != tt.mode || plan.flags != tt.flags {
				t.Fatalf("unexpected custom plan: got mode=%d flags=%d want mode=%d flags=%d", plan.mode, plan.flags, tt.mode, tt.flags)
			}
		})
	}
}

func TestSelectRebroadcastQueueTargetsSamplesFullPool(t *testing.T) {
	const totalPeers = 20
	const limit = 5

	peers := make([]*overlayPeer, 0, totalPeers)
	prefixIDs := make(map[PeerID]struct{}, limit)
	for i := 0; i < totalPeers; i++ {
		id := testPeerID(string(rune('a' + i)))
		peers = append(peers, &overlayPeer{id: id})
		if i < limit {
			prefixIDs[id] = struct{}{}
		}
	}

	sawOutsidePrefix := false
	for attempt := 0; attempt < 200 && !sawOutsidePrefix; attempt++ {
		targets := selectRebroadcastQueueTargets(peers, nil, limit)
		if len(targets) != limit {
			t.Fatalf("got %d targets, want %d", len(targets), limit)
		}

		seen := make(map[PeerID]struct{}, len(targets))
		for _, peer := range targets {
			if _, ok := seen[peer.id]; ok {
				t.Fatalf("duplicate target %x", peer.id)
			}
			seen[peer.id] = struct{}{}

			if _, ok := prefixIDs[peer.id]; !ok {
				sawOutsidePrefix = true
			}
		}
	}

	if !sawOutsidePrefix {
		t.Fatal("expected rebroadcast target selection to sample beyond the first limit peers")
	}
}

func TestAllowRebroadcastDoesNotThrottleLocalRequests(t *testing.T) {
	node := newTestNode(t)
	node.SetRebroadcastQuiet(true)

	req := &rebroadcastRequest{
		kind:  "tonNode.externalMessageBroadcast",
		local: true,
	}
	if !node.allowRebroadcast(req) {
		t.Fatalf("local rebroadcast must bypass quiet throttle")
	}
	if !node.allowRebroadcast(req) {
		t.Fatalf("duplicate local rebroadcast must bypass quiet throttle")
	}

	peerReq := &rebroadcastRequest{
		kind: "tonNode.externalMessageBroadcast",
	}
	if !node.allowRebroadcast(peerReq) {
		t.Fatalf("first peer rebroadcast should pass quiet throttle")
	}
	if node.allowRebroadcast(peerReq) {
		t.Fatalf("peer rebroadcast should be throttled in quiet mode")
	}
}

func TestEnqueueRebroadcastAdmissionClosedDropsRemoteBlock(t *testing.T) {
	node := newTestNode(t)
	node.SetBroadcastAdmission(testBroadcastAdmission(false))
	peer := testRebroadcastQueuePeer("peer")
	sub := &overlaySubscription{
		node:  node,
		spec:  overlaySpec{Name: "basechain"},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peer.id: peer},
	}

	if sub.enqueueRebroadcast(rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.blockBroadcast",
		payload:      []byte{0x01},
	}) {
		t.Fatal("closed admission accepted remote block rebroadcast")
	}
	if _, ok := peer.rebroadcastQueue.TryPop(); ok {
		t.Fatal("remote block rebroadcast reached peer queue")
	}
	if got := node.peerRebroadcastDropped.Load(); got != 1 {
		t.Fatalf("peer rebroadcast dropped = %d, want 1", got)
	}
}

func TestEnqueueRebroadcastAdmissionClosedKeepsLocalExternal(t *testing.T) {
	node := newTestNode(t)
	node.SetBroadcastAdmission(testBroadcastAdmission(false))
	peer := testRebroadcastQueuePeer("peer")
	sub := &overlaySubscription{
		node:  node,
		spec:  overlaySpec{Name: "basechain"},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peer.id: peer},
	}

	if !sub.enqueueRebroadcast(rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.externalMessageBroadcast",
		payload:      []byte{0x01},
		local:        true,
	}) {
		t.Fatal("closed admission rejected local external rebroadcast")
	}
	if got, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatal("local external rebroadcast did not reach peer queue")
	} else if got.kind != "tonNode.externalMessageBroadcast" || !got.local {
		t.Fatalf("unexpected local rebroadcast request: %#v", got)
	}
}

func TestEnqueueRebroadcastAdmissionClosedDropsLocalBlockFanout(t *testing.T) {
	node := newTestNode(t)
	node.SetBroadcastAdmission(testBroadcastAdmission(false))
	peer := testRebroadcastQueuePeer("peer")
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name: "custom.private-a",
			Kind: overlayKindCustomFixed,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peer.id: peer},
	}

	if sub.enqueueRebroadcast(rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.blockBroadcast",
		payload:      []byte{0x01},
		local:        true,
	}) {
		t.Fatal("closed admission accepted local block fanout")
	}
	if _, ok := peer.localRebroadcastQueue.TryPop(); ok {
		t.Fatal("local block fanout reached peer queue")
	}
	if got := node.localRebroadcastDropped.Load(); got != 1 {
		t.Fatalf("local rebroadcast dropped = %d, want 1", got)
	}
}

type testSyncLagProvider struct {
	lag int64
	ok  bool
}

func (p *testSyncLagProvider) SyncLagSeconds() (int64, bool) {
	return p.lag, p.ok
}

func TestRebroadcastFECBackpressureLimitsExternalSlots(t *testing.T) {
	node := newTestNode(t)
	node.syncLag = &testSyncLagProvider{lag: rebroadcastFECLagThreshold + 1, ok: true}

	req := rebroadcastRequest{
		kind:     "tonNode.externalMessageBroadcast",
		queuedAt: time.Now(),
	}
	releases := make([]func(), 0, externalFECBackpressureSlots)
	for i := 0; i < externalFECBackpressureSlots; i++ {
		release, ok := node.waitRebroadcastFECBackpressure(context.Background(), &overlayPeer{id: testPeerID(string(rune('a' + i)))}, req)
		if !ok {
			t.Fatalf("acquire external FEC slot %d", i)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if release, ok := node.waitRebroadcastFECBackpressure(ctx, &overlayPeer{id: testPeerID("extra")}, req); ok {
		release()
		t.Fatal("expected extra external FEC sender to wait while all slots are busy")
	}

	releases[0]()
	releases = releases[1:]
	release, ok := node.waitRebroadcastFECBackpressure(context.Background(), &overlayPeer{id: testPeerID("extra")}, req)
	if !ok {
		t.Fatal("expected external FEC sender to acquire a released slot")
	}
	releases = append(releases, release)
}

func TestRebroadcastFECBackpressureLimitsOneStreamPerPeer(t *testing.T) {
	node := newTestNode(t)
	node.syncLag = &testSyncLagProvider{lag: rebroadcastFECLagThreshold + 1, ok: true}
	peer := &overlayPeer{id: testPeerID("peer")}
	req := rebroadcastRequest{
		kind:     "tonNode.externalMessageBroadcast",
		queuedAt: time.Now(),
	}

	release, ok := node.waitRebroadcastFECBackpressure(context.Background(), peer, req)
	if !ok {
		t.Fatal("expected first peer FEC sender to acquire slot")
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if secondRelease, ok := node.waitRebroadcastFECBackpressure(ctx, peer, req); ok {
		secondRelease()
		t.Fatal("expected second FEC sender for the same peer to wait")
	}
}

func TestRebroadcastFECBackpressureUsesSeparateBlockAndExternalSlots(t *testing.T) {
	node := newTestNode(t)
	node.syncLag = &testSyncLagProvider{lag: rebroadcastFECLagThreshold + 1, ok: true}
	blockReq := rebroadcastRequest{
		kind:     "tonNode.blockBroadcastCompressedV2",
		queuedAt: time.Now(),
	}
	externalReq := rebroadcastRequest{
		kind:     "tonNode.externalMessageBroadcast",
		queuedAt: time.Now(),
	}

	var blockReleases []func()
	for i := 0; i < blockFECBackpressureSlots; i++ {
		release, ok := node.waitRebroadcastFECBackpressure(context.Background(), &overlayPeer{id: testPeerID(string(rune('b' + i)))}, blockReq)
		if !ok {
			t.Fatalf("acquire block FEC slot %d", i)
		}
		blockReleases = append(blockReleases, release)
	}
	defer func() {
		for _, release := range blockReleases {
			release()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if release, ok := node.waitRebroadcastFECBackpressure(ctx, &overlayPeer{id: testPeerID("block-extra")}, blockReq); ok {
		release()
		t.Fatal("expected block FEC sender to wait while block slots are busy")
	}

	release, ok := node.waitRebroadcastFECBackpressure(context.Background(), &overlayPeer{id: testPeerID("external")}, externalReq)
	if !ok {
		t.Fatal("expected external FEC sender to use independent slots")
	}
	release()
}

func TestRebroadcastFECBackpressurePassesWhenLagClears(t *testing.T) {
	node := newTestNode(t)
	lag := &testSyncLagProvider{lag: rebroadcastFECLagThreshold + 1, ok: true}
	node.syncLag = lag
	req := rebroadcastRequest{
		kind:     "tonNode.blockBroadcastCompressedV2",
		queuedAt: time.Now(),
	}

	var releases []func()
	for i := 0; i < blockFECBackpressureSlots; i++ {
		release, ok := node.waitRebroadcastFECBackpressure(context.Background(), &overlayPeer{id: testPeerID(string(rune('c' + i)))}, req)
		if !ok {
			t.Fatalf("acquire block FEC slot %d", i)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	lag.lag = rebroadcastFECLagThreshold
	release, ok := node.waitRebroadcastFECBackpressure(context.Background(), &overlayPeer{id: testPeerID("after-lag")}, req)
	if !ok {
		t.Fatal("expected FEC sender to bypass slots after lag clears")
	}
	release()
}

func TestSendCustomTwoStepRebroadcastUsesTonutilsGroupSender(t *testing.T) {
	node := newTestNode(t)
	overlayA, connA := newTestOverlayWrapper()
	overlayB, connB := newTestOverlayWrapper()
	now := int32(time.Now().Unix())

	peerA := &overlayPeer{
		id:            testPeerID("peer-a"),
		addr:          "peer-a",
		overlay:       overlayA,
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	peerB := &overlayPeer{
		id:            testPeerID("peer-b"),
		addr:          "peer-b",
		overlay:       overlayB,
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name: "custom.private-a",
			Kind: overlayKindCustomFixed,
		},
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peerA.id: peerA,
			peerB.id: peerB,
		},
	}

	ok := sub.sendCustomTwoStepRebroadcast(context.Background(), rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.externalMessageBroadcast",
		payload:      []byte{0x01, 0x02, 0x03},
		local:        true,
	})
	if !ok {
		t.Fatal("expected custom two-step send to succeed")
	}

	for name, conn := range map[string]*testOverlayADNL{"peer-a": connA, "peer-b": connB} {
		if len(conn.sent) != 1 {
			t.Fatalf("%s sent messages = %d, want 1", name, len(conn.sent))
		}
		msg := sentOverlayPayload(conn.sent[0])
		if _, ok := msg.(*overlay.BroadcastTwoStepSimple); !ok {
			t.Fatalf("%s sent unexpected message type %T", name, msg)
		}
	}
}

func TestCustomSmallShardRebroadcastUsesOrdinarySimpleQueue(t *testing.T) {
	node := newTestNode(t)
	peer := testRebroadcastQueuePeer("peer")
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name: "custom.private-a",
			Kind: overlayKindCustomFixed,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peer.id: peer},
	}

	if !sub.enqueueRebroadcast(rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.newShardBlockBroadcast",
		payload:      []byte{0x01},
		local:        true,
	}) {
		t.Fatal("expected ordinary custom shard rebroadcast enqueue")
	}

	got, ok := peer.localRebroadcastQueue.TryPop()
	if !ok {
		t.Fatal("expected small shard broadcast to use ordinary peer queue")
	}
	if got.kind != "tonNode.newShardBlockBroadcast" || !got.local {
		t.Fatalf("unexpected queued rebroadcast: %#v", got)
	}
	if snapshot, ok := sub.customTwoStepQueueStatusSnapshot(); ok && snapshot.Items != 0 {
		t.Fatalf("custom two-step queue items = %d, want 0", snapshot.Items)
	}
}

func TestBuildSimpleBroadcastSupportsAnySender(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: newTestPeerStore(),
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	payload := []byte{0xAA, 0xBB, 0xCC}
	msg, err := node.buildSimpleBroadcast(payload, overlay.BroadcastFlagAnySender)
	if err != nil {
		t.Fatalf("build simple broadcast: %v", err)
	}

	if msg.Flags != overlay.BroadcastFlagAnySender {
		t.Fatalf("unexpected flags: %d", msg.Flags)
	}

	toSign, err := tl.Serialize(overlay.BroadcastToSign{
		Hash: mustHashSimpleBroadcastID(t, msg.Data, msg.Flags),
		Date: uint32(msg.Date),
	}, true)
	if err != nil {
		t.Fatalf("serialize toSign: %v", err)
	}

	pub := node.privKey.Public().(ed25519.PublicKey)
	if !ed25519.Verify(pub, toSign, msg.Signature) {
		t.Fatalf("signature should verify with zero source hash when any-sender is enabled")
	}
}

func sentOverlayPayload(msg tl.Serializable) tl.Serializable {
	wrapped, ok := msg.([]tl.Serializable)
	if ok && len(wrapped) == 2 {
		return wrapped[1]
	}
	return msg
}

func TestRebroadcastFECToPeerUsesTonutilsBroadcaster(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	peer := &mockRebroadcastPeer{id: bytes.Repeat([]byte{0x11}, 32)}
	payload := bytes.Repeat([]byte{0xAB}, 2000)
	sender, err := overlay.NewBroadcastFECSender(
		priv,
		overlay.CertificateEmpty{},
		payload,
		overlay.BroadcastFlagAnySender,
		overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
		overlay.WithBroadcastFECDate(100),
	)
	if err != nil {
		t.Fatalf("create fec sender: %v", err)
	}
	if err = runFECBroadcasterToPeer(context.Background(), sender, peer, time.Second); err != nil {
		t.Fatalf("run fec broadcaster: %v", err)
	}

	want := int(sender.TotalParts())
	if len(peer.sent) != want {
		t.Fatalf("unexpected sent part count: got %d want %d", len(peer.sent), want)
	}

	for i, msg := range peer.sent {
		if _, ok := msg.(*overlay.BroadcastFEC); !ok {
			t.Fatalf("message %d has unexpected type %T", i, msg)
		}
	}
}

func TestTonutilsFECRebroadcastReusesPartsAcrossPeerWorkers(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	payload := bytes.Repeat([]byte{0xAB}, 2000)
	sender, err := overlay.NewBroadcastFECSender(
		priv,
		overlay.CertificateEmpty{},
		payload,
		overlay.BroadcastFlagAnySender,
		overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
		overlay.WithBroadcastFECDate(100),
	)
	if err != nil {
		t.Fatalf("create fec sender: %v", err)
	}

	peerA := &mockRebroadcastPeer{id: bytes.Repeat([]byte{0x11}, 32)}
	peerB := &mockRebroadcastPeer{id: bytes.Repeat([]byte{0x22}, 32)}
	if err = runFECBroadcasterToPeer(context.Background(), sender, peerA, time.Second); err != nil {
		t.Fatalf("run peer A fec broadcaster: %v", err)
	}
	if err = runFECBroadcasterToPeer(context.Background(), sender, peerB, time.Second); err != nil {
		t.Fatalf("run peer B fec broadcaster: %v", err)
	}

	if len(peerA.sent) != int(sender.TotalParts()) || len(peerB.sent) != int(sender.TotalParts()) {
		t.Fatalf("expected both peers to receive all FEC parts, got peerA=%d peerB=%d", len(peerA.sent), len(peerB.sent))
	}
	if peerA.sent[0] != peerB.sent[0] {
		t.Fatal("expected peers to reuse the same cached FEC part")
	}
}

func TestEnqueueRebroadcastSkipsSourcePeer(t *testing.T) {
	node := newTestNode(t)
	sourcePeer := testRebroadcastQueuePeer("source")
	targetPeer := testRebroadcastQueuePeer("target")
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{Name: "basechain"},
		log:  discardLogger(),
		peers: map[PeerID]*overlayPeer{
			sourcePeer.id: sourcePeer,
			targetPeer.id: targetPeer,
		},
	}

	req := rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.externalMessageBroadcast",
		payload:      []byte{0x01},
		sourcePeerID: sourcePeer.id,
	}
	if !sub.enqueueRebroadcast(req) {
		t.Fatal("expected rebroadcast enqueue")
	}

	if _, ok := sub.peers[sourcePeer.id].rebroadcastQueue.TryPop(); ok {
		t.Fatal("source peer should not receive its own rebroadcast")
	}

	got, ok := sub.peers[targetPeer.id].rebroadcastQueue.TryPop()
	if !ok {
		t.Fatal("expected target peer rebroadcast")
	}
	if got.sourcePeerID != sourcePeer.id {
		t.Fatalf("source peer id = %q, want source", got.sourcePeerID)
	}
}

func TestEnqueueInboundBlockRebroadcastUsesCppNodeFanout(t *testing.T) {
	node := newTestNode(t)
	sourcePeer := testRebroadcastQueuePeer("source")
	peers := map[PeerID]*overlayPeer{
		sourcePeer.id: sourcePeer,
	}
	for i := 0; i < 8; i++ {
		peer := testRebroadcastQueuePeer(string(rune('a' + i)))
		peers[peer.id] = peer
	}
	sub := &overlaySubscription{
		node:  node,
		spec:  overlaySpec{Name: "basechain"},
		log:   discardLogger(),
		peers: peers,
	}

	req := rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.blockBroadcastCompressedV2",
		payload:      []byte{0x01},
		sourcePeerID: sourcePeer.id,
	}
	if !sub.enqueueRebroadcast(req) {
		t.Fatal("expected block rebroadcast enqueue")
	}

	if _, ok := peers[sourcePeer.id].rebroadcastQueue.TryPop(); ok {
		t.Fatal("source peer should not receive its own block rebroadcast")
	}
	if got := countQueuedRebroadcasts(peers, false); got != rebroadcastFanout {
		t.Fatalf("queued block rebroadcasts = %d, want %d", got, rebroadcastFanout)
	}
}

func TestEnqueueInboundBlockRebroadcastSharesFECSource(t *testing.T) {
	node := newTestNode(t)
	sourcePeer := testRebroadcastQueuePeer("source")
	peers := map[PeerID]*overlayPeer{
		sourcePeer.id: sourcePeer,
	}
	for i := 0; i < 8; i++ {
		peer := testRebroadcastQueuePeer(string(rune('a' + i)))
		peers[peer.id] = peer
	}
	sub := &overlaySubscription{
		node:  node,
		spec:  overlaySpec{Name: "basechain"},
		log:   discardLogger(),
		peers: peers,
	}

	req := rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.blockBroadcastCompressedV2",
		payload:      bytes.Repeat([]byte{0x01}, 2000),
		sourcePeerID: sourcePeer.id,
	}
	if !sub.enqueueRebroadcast(req) {
		t.Fatal("expected block rebroadcast enqueue")
	}

	var shared *overlay.BroadcastFECSender
	queued := 0
	for id, peer := range peers {
		if id == sourcePeer.id {
			continue
		}
		got, ok := peer.rebroadcastQueue.TryPop()
		if !ok {
			continue
		}
		queued++
		if len(got.payload) != 0 {
			t.Fatal("expected queued FEC rebroadcast to drop payload slice")
		}
		if got.fec == nil {
			t.Fatal("expected queued FEC rebroadcast to carry shared source")
		}
		if shared == nil {
			shared = got.fec
		} else if shared != got.fec {
			t.Fatal("expected all queued peers to share one FEC source")
		}
	}
	if queued != rebroadcastFanout {
		t.Fatalf("queued block rebroadcasts = %d, want %d", queued, rebroadcastFanout)
	}
}

func TestEnqueueRebroadcastPrefersNeighbours(t *testing.T) {
	node := newTestNode(t)
	peers := map[PeerID]*overlayPeer{}
	for i := 0; i < 8; i++ {
		peer := testRebroadcastQueuePeer(string(rune('a' + i)))
		peers[peer.id] = peer
	}
	sub := &overlaySubscription{
		node:       node,
		spec:       overlaySpec{Name: "basechain"},
		log:        discardLogger(),
		peers:      peers,
		neighbours: []PeerID{testPeerID("g"), testPeerID("h")},
	}

	req := rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.externalMessageBroadcast",
		payload:      []byte{0x01},
	}
	if !sub.enqueueRebroadcast(req) {
		t.Fatal("expected rebroadcast enqueue")
	}

	for _, id := range sub.neighbours {
		if _, ok := peers[id].rebroadcastQueue.TryPop(); !ok {
			t.Fatalf("expected neighbour %q to receive rebroadcast before non-neighbours", id)
		}
	}
}

func TestEnqueueLocalExternalRebroadcastUsesFullFanout(t *testing.T) {
	node := newTestNode(t)
	peers := map[PeerID]*overlayPeer{}
	for i := 0; i < 8; i++ {
		peer := testRebroadcastQueuePeer(string(rune('a' + i)))
		peers[peer.id] = peer
	}
	sub := &overlaySubscription{
		node:  node,
		spec:  overlaySpec{Name: "basechain"},
		log:   discardLogger(),
		peers: peers,
	}

	req := rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.externalMessageBroadcast",
		payload:      []byte{0x01},
		local:        true,
	}
	if !sub.enqueueRebroadcast(req) {
		t.Fatal("expected local external rebroadcast enqueue")
	}

	if got := countQueuedRebroadcasts(peers, true); got != externalRebroadcastFanout {
		t.Fatalf("queued local external rebroadcasts = %d, want %d", got, externalRebroadcastFanout)
	}
}

func TestEnqueueLocalRebroadcastRecordsDropWhenPeerQueuesAreFull(t *testing.T) {
	node := newTestNode(t)
	peer := testRebroadcastQueuePeer("peer")
	sub := &overlaySubscription{
		node:  node,
		spec:  overlaySpec{Name: "basechain"},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peer.id: peer},
	}

	for i := 0; i < peerRebroadcastQueueItems; i++ {
		if !peer.localRebroadcastQueue.Push(rebroadcastRequest{
			kind:    "tonNode.externalMessageBroadcast",
			payload: []byte{byte(i)},
			local:   true,
		}) {
			t.Fatalf("fill local rebroadcast queue at item %d", i)
		}
	}

	if sub.enqueueRebroadcast(rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.externalMessageBroadcast",
		payload:      []byte{0x02},
		local:        true,
	}) {
		t.Fatal("expected full local rebroadcast queues to reject request")
	}
	if got := node.localRebroadcastDropped.Load(); got != 1 {
		t.Fatalf("local dropped counter = %d, want 1", got)
	}
}

func TestPeerRebroadcastWorkerDropsStaleQueuedRequest(t *testing.T) {
	node := newTestNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	node.runCtx = ctx

	peer := testRebroadcastQueuePeer("peer")
	sub := &overlaySubscription{
		node: node,
		log:  discardLogger(),
	}
	workers := sub.peerRebroadcastWorkerCount()

	for i := 0; i < workers; i++ {
		if !peer.localRebroadcastQueue.Push(rebroadcastRequest{
			kind:     "tonNode.externalMessageBroadcast",
			payload:  []byte{byte(i + 1)},
			local:    true,
			queuedAt: time.Now().Add(-rebroadcastQueueMaxAge - time.Second),
		}) {
			t.Fatalf("enqueue stale local rebroadcast %d", i)
		}
	}

	sub.startPeerRebroadcastWorker(peer)

	deadline := time.After(time.Second)
	for node.localRebroadcastDropped.Load() < uint64(workers) {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("stale rebroadcast dropped=%d want %d", node.localRebroadcastDropped.Load(), workers)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	done := make(chan struct{})
	go func() {
		node.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rebroadcast worker did not stop")
	}
}

func TestPeerRebroadcastWorkerCountByOverlay(t *testing.T) {
	master := &overlaySubscription{spec: overlaySpec{Workchain: -1, Shard: topShard}}
	if got := master.peerRebroadcastWorkerCount(); got != masterPeerRebroadcastWorkers {
		t.Fatalf("masterchain workers = %d, want %d", got, masterPeerRebroadcastWorkers)
	}

	base := &overlaySubscription{spec: overlaySpec{Workchain: 0, Shard: topShard}}
	if got := base.peerRebroadcastWorkerCount(); got != basePeerRebroadcastWorkers {
		t.Fatalf("basechain workers = %d, want %d", got, basePeerRebroadcastWorkers)
	}
}

func mustHashSimpleBroadcastID(t *testing.T, payload []byte, flags int32) []byte {
	t.Helper()

	hash, err := tl.Hash(OverlayBroadcastID{
		Source:   make([]byte, 32),
		DataHash: hashSimpleBroadcastPayload(payload),
		Flags:    flags,
	})
	if err != nil {
		t.Fatalf("hash broadcast id: %v", err)
	}
	return hash
}

func countQueuedRebroadcasts(peers map[PeerID]*overlayPeer, local bool) int {
	count := 0
	for _, peer := range peers {
		var queue *boundedQueue[rebroadcastRequest]
		if local {
			queue = peer.localRebroadcastQueue
		} else {
			queue = peer.rebroadcastQueue
		}
		for {
			if _, ok := queue.TryPop(); !ok {
				break
			}
			count++
		}
	}
	return count
}
