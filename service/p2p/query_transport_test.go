package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func TestNewOverlayPeerBindsQueryTransport(t *testing.T) {
	tests := []struct {
		name     string
		spec     overlaySpec
		wantQUIC bool
	}{
		{
			name: "public RLDP",
			spec: overlaySpec{Kind: overlayKindPublicShard},
		},
		{
			name: "custom RLDP",
			spec: overlaySpec{Kind: overlayKindCustomFixed},
		},
		{
			name:     "custom QUIC",
			spec:     overlaySpec{Kind: overlayKindCustomFixed, UseQUIC: true},
			wantQUIC: true,
		},
		{
			name: "FastSync QUIC",
			spec: overlaySpec{
				Kind:     overlayKindFastSync,
				UseQUIC:  true,
				FastSync: &fastSyncOverlaySpec{},
			},
			wantQUIC: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool, pooled, _ := newTestLeasedPooledPeer(test.name)
			node := &Node{pool: pool}
			test.spec.ShortID = testPeerID(test.name + "-overlay").Bytes()
			sub := testOverlaySubscription(&overlaySubscription{
				node:  node,
				spec:  test.spec,
				log:   discardLogger(),
				peers: map[PeerID]*overlayPeer{},
			})
			peer := mustNewTestOverlayPeer(t, sub, pooled, nil, test.spec.Kind == overlayKindCustomFixed)
			t.Cleanup(func() {
				peer.close()
				sub.broadcastReceiver.Close()
			})

			_, isQUIC := peer.queryTransport.(quicPeerQueryTransport)
			if isQUIC != test.wantQUIC {
				t.Fatalf("QUIC query transport = %v, want %v", isQUIC, test.wantQUIC)
			}
			if !test.wantQUIC {
				if _, ok := peer.queryTransport.(rldpPeerQueryTransport); !ok {
					t.Fatalf("query transport = %T, want RLDP", peer.queryTransport)
				}
			}
		})
	}
}

func TestQUICOverlayEnvelopeUsesCertificateForQueryAndMessage(t *testing.T) {
	certificate := overlay.MemberCertificate{
		IssuedBy: newFastSyncMembershipTestIssuer(t, 0x41).public,
		Flags:    7,
		Slot:     2,
		ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
	}
	overlayID := testPeerID("member-query-envelope-overlay").Bytes()
	envelope, err := newQUICOverlayEnvelope(overlayID, &certificate)
	if err != nil {
		t.Fatalf("create member envelope: %v", err)
	}
	payload, err := envelope.Query(overlay.Ping{})
	if err != nil {
		t.Fatalf("serialize member query: %v", err)
	}

	header, body, err := parseQUICQueryEnvelope(payload)
	if err != nil {
		t.Fatalf("parse member query envelope: %v", err)
	}
	if header.certificateKind != quicMembershipCertificateMember {
		t.Fatalf(
			"certificate kind = %d, want member",
			header.certificateKind,
		)
	}
	if header.certificate.Slot != certificate.Slot ||
		header.certificate.Flags != certificate.Flags ||
		header.certificate.ExpireAt != certificate.ExpireAt {
		t.Fatalf(
			"decoded certificate = %+v, want %+v",
			header.certificate,
			certificate,
		)
	}
	if _, err = parseOneQUICOverlayObject(body); err != nil {
		t.Fatalf("parse member query body: %v", err)
	}

	payload, err = envelope.Message(ForgetPeer{})
	if err != nil {
		t.Fatalf("serialize member message: %v", err)
	}
	header, body, err = parseQUICMessageEnvelope(payload)
	if err != nil {
		t.Fatalf("parse member message envelope: %v", err)
	}
	if header.certificateKind != quicMembershipCertificateMember ||
		header.certificate.Slot != certificate.Slot ||
		header.certificate.Flags != certificate.Flags ||
		header.certificate.ExpireAt != certificate.ExpireAt {
		t.Fatalf(
			"decoded message certificate = %+v, want %+v",
			header.certificate,
			certificate,
		)
	}
	if _, err = parseOneQUICOverlayObject(body); err != nil {
		t.Fatalf("parse member message body: %v", err)
	}
}

func TestQUICOverlayEnvelopeMessageReuseBoundary(t *testing.T) {
	overlayID := testPeerID("message-reuse-envelope").Bytes()
	plain, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		t.Fatalf("create plain envelope: %v", err)
	}
	if !plain.CanReuseMessage(quicOverlayHeader{
		certificateKind: quicMembershipCertificateOmitted,
	}) {
		t.Fatal("plain local envelope did not reuse a plain inbound message")
	}
	if plain.CanReuseMessage(quicOverlayHeader{
		certificateKind: quicMembershipCertificateMember,
	}) {
		t.Fatal("plain local envelope reused a certified inbound message")
	}

	certificate := overlay.MemberCertificate{
		IssuedBy: newFastSyncMembershipTestIssuer(t, 0x43).public,
		Slot:     1,
		ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
	}
	certified, err := newQUICOverlayEnvelope(overlayID, &certificate)
	if err != nil {
		t.Fatalf("create certified envelope: %v", err)
	}
	if certified.CanReuseMessage(quicOverlayHeader{
		certificateKind: quicMembershipCertificateOmitted,
	}) {
		t.Fatal("certified local envelope reused a remote sender envelope")
	}
}

func TestQUICOverlayEnvelopeConcurrentRotationPublishesWholePrefixes(
	t *testing.T,
) {
	overlayID := testPeerID("concurrent-envelope-rotation").Bytes()
	issuer := newFastSyncMembershipTestIssuer(t, 0x44)
	certificates := [2]overlay.MemberCertificate{
		{
			IssuedBy: issuer.public,
			Slot:     1,
			ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
		},
		{
			IssuedBy: issuer.public,
			Slot:     2,
			ExpireAt: int32(time.Now().Add(2 * time.Hour).Unix()),
		},
	}
	envelope, err := newQUICOverlayEnvelope(
		overlayID,
		&certificates[0],
	)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}

	start := make(chan struct{})
	failures := make(chan error, 4)
	var readers sync.WaitGroup
	for reader := range 4 {
		readQuery := reader%2 == 0
		readers.Go(func() {
			<-start
			for range 500 {
				var header quicOverlayHeader
				if readQuery {
					payload, serializeErr := envelope.Query(overlay.Ping{})
					if serializeErr != nil {
						failures <- serializeErr
						return
					}
					var parseErr error
					header, _, parseErr = parseQUICQueryEnvelope(payload)
					if parseErr != nil {
						failures <- parseErr
						return
					}
				} else {
					payload, serializeErr := envelope.Message(ForgetPeer{})
					if serializeErr != nil {
						failures <- serializeErr
						return
					}
					var parseErr error
					header, _, parseErr = parseQUICMessageEnvelope(payload)
					if parseErr != nil {
						failures <- parseErr
						return
					}
				}
				if header.certificateKind !=
					quicMembershipCertificateMember ||
					header.certificate.Slot < 1 ||
					header.certificate.Slot > 2 ||
					header.certificate.ExpireAt !=
						certificates[header.certificate.Slot-1].ExpireAt {
					failures <- fmt.Errorf(
						"partial envelope state: %+v",
						header,
					)
					return
				}
			}
		})
	}

	close(start)
	var rotateErr error
	for i := range 500 {
		var state *quicOverlayEnvelopeState
		state, rotateErr = envelope.prepareCertificate(
			&certificates[i%len(certificates)],
		)
		if rotateErr != nil {
			break
		}
		envelope.state.Store(state)
	}
	readers.Wait()
	close(failures)

	if rotateErr != nil {
		t.Fatalf("rotate envelope: %v", rotateErr)
	}
	for failure := range failures {
		t.Error(failure)
	}
}

func BenchmarkQUICOverlayEnvelopeWrapMessageBody(b *testing.B) {
	overlayID := testPeerID("message-body-envelope-benchmark").Bytes()
	envelope, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		b.Fatalf("create envelope: %v", err)
	}
	body := make([]byte, 1024)

	b.ReportAllocs()
	for b.Loop() {
		quicOverlayEnvelopeBenchmarkWire, _ = envelope.WrapMessageBody(body)
	}
}

func BenchmarkQUICOverlayEnvelopeMessage(b *testing.B) {
	overlayID := testPeerID("message-envelope-benchmark").Bytes()
	envelope, err := newQUICOverlayEnvelope(overlayID, nil)
	if err != nil {
		b.Fatalf("create envelope: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		quicOverlayEnvelopeBenchmarkWire, err = envelope.Message(ForgetPeer{})
		if err != nil {
			b.Fatalf("serialize message: %v", err)
		}
	}
}

var quicOverlayEnvelopeBenchmarkWire []byte

func TestRLDPPeerQueryTransportTypedRawAndStrictParsing(t *testing.T) {
	answer := Capabilities{VersionMajor: 3, VersionMinor: 1, Flags: 7}
	encoded, err := tl.Serialize(answer, true)
	if err != nil {
		t.Fatalf("serialize answer: %v", err)
	}

	rldpClient := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		asyncResult: encoded,
	}
	overlayID := []byte{1, 2, 3}
	transport := rldpPeerQueryTransport{
		overlay:   overlay.CreateExtendedRLDP(rldpClient).CreateOverlay(overlayID),
		overlayID: overlayID,
	}

	var decoded Capabilities
	if err = transport.Query(context.Background(), 1024, GetCapabilities{}, &decoded); err != nil {
		t.Fatalf("typed query: %v", err)
	}
	if decoded != answer {
		t.Fatalf("typed answer = %+v, want %+v", decoded, answer)
	}
	raw, err := transport.QueryRaw(context.Background(), 1024, GetCapabilities{})
	if err != nil {
		t.Fatalf("raw query: %v", err)
	}
	if string(raw) != string(encoded) {
		t.Fatal("raw query changed the answer bytes")
	}

	rldpClient.mu.Lock()
	rldpClient.asyncResult = append(encoded, 0, 0, 0, 0)
	rldpClient.mu.Unlock()
	if err = transport.Query(context.Background(), 1024, GetCapabilities{}, &decoded); err == nil ||
		!strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("trailing answer error = %v", err)
	}
}

func TestQUICQueryTransportDoesNotFallbackWithoutRoute(t *testing.T) {
	overlayID := testPeerID("query-transport-missing-route")
	envelope, err := newQUICOverlayEnvelope(overlayID[:], nil)
	if err != nil {
		t.Fatalf("create envelope: %v", err)
	}
	peerPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	peer := &overlayPeer{
		node:      newTestNode(t),
		pub:       peerPublicKey,
		route:     newTestPeerRoute(""),
		overlayID: []byte{1},
	}
	transport := quicPeerQueryTransport{
		peer:     peer,
		envelope: envelope,
	}

	_, err = transport.QueryRaw(context.Background(), 1024, GetCapabilities{})
	if !errors.Is(err, errQUICRouteMissing) {
		t.Fatalf("QUIC query error = %v, want missing route", err)
	}
}

func TestCustomQuerySelectionReadinessCooldownAndScope(t *testing.T) {
	node := &Node{
		localID:       testPeerID("local-query-node"),
		subscriptions: map[string]*overlaySubscription{},
	}
	first, firstPeer := testCustomQuerySubscription(node, "custom.alpha", "alpha-peer")
	second, secondPeer := testCustomQuerySubscription(node, "custom.beta", "beta-peer")
	unlisted := testReadyQueryPeer("unlisted")
	first.peers[unlisted.id] = unlisted
	node.subscriptions["beta"] = second
	node.subscriptions["alpha"] = first

	selected, err := node.querySubscriptionForBlock(ton.BlockIDExt{Workchain: 0, Shard: topShard})
	if err != nil {
		t.Fatalf("select custom query overlay: %v", err)
	}
	if selected != first {
		t.Fatalf("selected overlay = %q, want %q", selected.spec.Name, first.spec.Name)
	}
	historical, err := node.querySubscriptionForHistoricalBlock(ton.BlockIDExt{Workchain: 0, Shard: topShard})
	if err != nil {
		t.Fatalf("select custom historical query overlay: %v", err)
	}
	if historical != first {
		t.Fatalf("historical overlay = %q, want custom %q", historical.spec.Name, first.spec.Name)
	}

	candidates := first.queryCandidates(0, 0)
	if len(candidates) != 1 || candidates[0] != firstPeer {
		t.Fatalf("custom candidate count = %d, want only configured acceptor", len(candidates))
	}

	startedAt := time.Now()
	first.beginPeerQueryOperation(firstPeer).finish(context.DeadlineExceeded)
	stats := firstPeer.statsSnapshot()
	if !stats.queryResponds {
		t.Fatal("application failure cleared capabilities readiness")
	}
	if delay := stats.queryIgnoreUntil.Sub(startedAt); delay < customQueryFailureCooldown ||
		delay > customQueryFailureCooldown+100*time.Millisecond {
		t.Fatalf("query cooldown = %s, want %s", delay, customQueryFailureCooldown)
	}

	selected, err = node.querySubscriptionForBlock(ton.BlockIDExt{Workchain: 0, Shard: topShard})
	if err != nil {
		t.Fatalf("select second custom query overlay: %v", err)
	}
	if selected != second || selected.queryCandidates(0, 0)[0] != secondPeer {
		t.Fatalf("selected overlay after cooldown = %q, want %q", selected.spec.Name, second.spec.Name)
	}
}

// Archive, zero state and persistent state must not be served from a FastSync
// overlay: its pool holds a single validator, which disables hedging, ranking by
// throughput and rotation - everything the archive pool exists for.
func TestHistoricalQuerySelectionSkipsFastSync(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, PeerIDSize)
	node.SetMonitorMinSplitDepth(0, 0)

	shard := int64(0x4000000000000000)
	remoteID := testPeerID("historical-fast-sync")
	roster := NewFastSyncValidatorRoster(
		nil,
		[]FastSyncValidator{fastSyncOverlayTestValidator(0x42, remoteID)},
		nil,
	)
	fastSync := &overlaySubscription{
		spec: overlaySpec{
			Kind:      overlayKindFastSync,
			Workchain: 0,
			Shard:     shard,
		},
		fastSync: &fastSyncOverlayRuntime{
			spec:       fastSyncOverlaySpec{roster: roster},
			aliveRoots: []PeerID{remoteID},
		},
	}
	node.fastSyncSubscriptions[FastSyncShard{
		Workchain: 0,
		Shard:     shard,
	}] = fastSync

	selected, err := node.querySubscriptionForHistoricalBlock(ton.BlockIDExt{
		Workchain: 0,
		Shard:     shard,
	})
	if err != nil {
		t.Fatalf("select historical query overlay: %v", err)
	}
	if selected == fastSync {
		t.Fatal("historical query selected the FastSync overlay for the archive path")
	}
	if selected.spec.Kind != overlayKindPublicShard {
		t.Fatalf(
			"historical query selected kind %d (%q), want public shard",
			selected.spec.Kind,
			selected.spec.Name,
		)
	}
}

func TestPublicQueryCandidatesUseOneHourAliveRandomFallback(t *testing.T) {
	now := time.Now()
	retained := testReadyQueryPeer("retained-public-fallback")
	retained.announced = &overlay.Node{Version: int32(now.Add(-30 * time.Minute).Unix())}
	retained.lastReceiveAt = now

	expired := testReadyQueryPeer("expired-public-fallback")
	expired.announced = &overlay.Node{Version: int32(now.Add(-publicRandomQueryFallbackTTL - time.Minute).Unix())}
	expired.lastReceiveAt = now

	dead := testReadyQueryPeer("dead-public-fallback")
	dead.announced = &overlay.Node{Version: int32(now.Add(-30 * time.Minute).Unix())}
	dead.alive = false

	pending := testReadyQueryPeer("pending-public-fallback")
	pending.announced = &overlay.Node{Version: int32(now.Add(-30 * time.Minute).Unix())}
	pending.pending = true

	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		spec: overlaySpec{Kind: overlayKindPublicShard},
		peers: map[PeerID]*overlayPeer{
			retained.id: retained,
			expired.id:  expired,
			dead.id:     dead,
			pending.id:  pending,
		},
	})

	candidates := sub.queryCandidates(0, 0)
	if len(candidates) != 1 || candidates[0] != retained {
		t.Fatalf("public random fallback candidates = %#v, want retained alive peer", candidates)
	}
	ensureCtx, cancelEnsure := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelEnsure()
	if err := sub.ensurePeers(ensureCtx); err != nil {
		t.Fatalf("ensure peers rejected retained public fallback: %v", err)
	}

	fresh := testReadyQueryPeer("fresh-public-query")
	fresh.announced = &overlay.Node{Version: int32(now.Unix())}
	fresh.lastReceiveAt = now
	sub.peers[fresh.id] = fresh

	candidates = sub.queryCandidates(0, 0)
	if len(candidates) != 1 || candidates[0] != fresh {
		t.Fatalf("ordinary public candidates = %#v, want fresh peer without stale fallback", candidates)
	}

	otherRetained := testReadyQueryPeer("other-retained-public-fallback")
	otherRetained.announced = &overlay.Node{Version: int32(now.Add(-45 * time.Minute).Unix())}
	otherRetained.lastReceiveAt = now
	sub.peers[otherRetained.id] = otherRetained
	delete(sub.peers, fresh.id)

	selected := make(map[PeerID]struct{})
	for range 64 {
		candidates = sub.queryCandidates(0, 0)
		if len(candidates) != 1 {
			t.Fatalf("public random fallback candidate count = %d, want 1", len(candidates))
		}
		selected[candidates[0].id] = struct{}{}
	}
	if len(selected) != 2 {
		t.Fatalf("public random fallback did not vary across retained peers: %v", selected)
	}

	boundaryNow := now.Truncate(time.Second)
	retained.announced.Version = int32(boundaryNow.Add(-publicRandomQueryFallbackTTL).Unix())
	if !retained.publicRandomQueryFallbackReady(boundaryNow, 0, 0) {
		t.Fatal("public random fallback rejected announcement exactly one hour old")
	}
	retained.announced.Version--
	if retained.publicRandomQueryFallbackReady(boundaryNow, 0, 0) {
		t.Fatal("public random fallback accepted announcement older than one hour")
	}
}

func TestCustomArchivePoolUsesOnlyReadyQueryAcceptors(t *testing.T) {
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dhtClient := &fakeDHTClient{}
	node := &Node{
		runCtx:  runCtx,
		localID: testPeerID("local-archive-node"),
		dht:     dhtClient,
	}
	listed := testReadyQueryPeer("listed-archive-peer")
	listed.fixedMember = true
	listed.lastReceiveAt = time.Now()
	unlisted := testReadyQueryPeer("unlisted-archive-peer")
	var listedClosed int
	listed.release = func() {
		listedClosed++
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:           "custom.archive",
			Kind:           overlayKindCustomFixed,
			SendQueries:    true,
			QueryAcceptors: []PeerID{listed.id},
		},
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			listed.id:   listed,
			unlisted.id: unlisted,
		},
	})

	pool := newArchivePeerPool(sub)
	if done := pool.refill(context.Background(), true); done != nil {
		t.Fatal("custom archive pool started discovery")
	}
	if calls := dhtClient.findOverlayNodesCallCount(); calls != 0 {
		t.Fatalf("custom archive pool made %d DHT queries", calls)
	}
	candidates := pool.candidates(archiveShardFromBlock(ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
	}))
	if len(candidates) != 1 || candidates[0] != listed {
		t.Fatalf("custom archive candidates = %d, want listed acceptor only", len(candidates))
	}

	listed.ignoreApplicationQueries(time.Now().Add(customQueryFailureCooldown))
	if candidates = pool.candidates(archiveShardFromBlock(ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
	})); len(candidates) != 0 {
		t.Fatalf("custom archive candidates during cooldown = %d, want 0", len(candidates))
	}

	pool.Close()
	if listedClosed != 0 {
		t.Fatal("archive pool closed a subscription-owned custom peer")
	}
}

func testCustomQuerySubscription(
	node *Node,
	name,
	peerLabel string,
) (*overlaySubscription, *overlayPeer) {
	peer := testReadyQueryPeer(peerLabel)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:           name,
			Kind:           overlayKindCustomFixed,
			SendQueries:    true,
			QueryAcceptors: []PeerID{node.localID, peer.id},
		},
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	})
	return sub, peer
}

func testReadyQueryPeer(label string) *overlayPeer {
	return &overlayPeer{
		id:            testPeerID(label),
		addr:          label,
		overlay:       &overlay.ADNLOverlayWrapper{},
		alive:         true,
		queryResponds: true,
		release:       func() {},
	}
}
