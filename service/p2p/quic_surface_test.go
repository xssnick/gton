package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/rs/zerolog"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type quicZeroStatePeerStore struct {
	PeerStorage
	data []byte
}

var (
	quicMessageEnvelopeBenchmarkHeader quicOverlayHeader
	quicMessageEnvelopeBenchmarkBody   []byte
)

func (s *quicZeroStatePeerStore) ZeroState(context.Context, ton.BlockIDExt) ([]byte, error) {
	return s.data, nil
}

func BenchmarkParseQUICMessageEnvelopePlain(b *testing.B) {
	overlayID := testPeerID("quic-message-envelope-benchmark").Bytes()
	payload, err := tl.Serialize(overlay.Message{Overlay: overlayID}, true)
	if err != nil {
		b.Fatalf("serialize overlay message: %v", err)
	}
	payload, err = tl.Append(payload, overlay.Ping{}, true)
	if err != nil {
		b.Fatalf("serialize message body: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		quicMessageEnvelopeBenchmarkHeader,
			quicMessageEnvelopeBenchmarkBody,
			err = parseQUICMessageEnvelope(payload)
		if err != nil {
			b.Fatalf("parse message envelope: %v", err)
		}
	}
}

func TestQUICHotPathConstructorIDs(t *testing.T) {
	tests := []struct {
		schema string
		id     uint32
	}{
		{
			schema: broadcastTwoStepSimpleSchema,
			id:     broadcastTwoStepSimpleConstructorID,
		},
		{
			schema: broadcastTwoStepFECSchema,
			id:     broadcastTwoStepFECConstructorID,
		},
	}

	for _, test := range tests {
		if actual := tl.CRC(test.schema); actual != test.id {
			t.Fatalf("constructor id for %q = 0x%08x, want 0x%08x", test.schema, actual, test.id)
		}
	}
}

func TestParseQUICOverlayObject(t *testing.T) {
	overlayID := testPeerID("quic-parser-overlay").Bytes()
	envelopePayload, err := tl.Serialize(overlay.Query{Overlay: overlayID}, true)
	if err != nil {
		t.Fatalf("serialize overlay envelope: %v", err)
	}
	validPayload := testQUICOverlayQueryPayload(t, overlayID, overlay.Ping{})

	t.Run("decoded TL constructors are values", func(t *testing.T) {
		var envelope overlay.Query
		obj, err := parseQUICOverlayObject(validPayload, &envelope)
		if err != nil {
			t.Fatalf("parse overlay query: %v", err)
		}
		if !bytes.Equal(envelope.Overlay, overlayID) {
			t.Fatalf("overlay id = %x, want %x", envelope.Overlay, overlayID)
		}
		if _, ok := obj.(overlay.Ping); !ok {
			t.Fatalf("payload type = %T, want overlay.Ping value", obj)
		}
	})

	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "missing payload",
			payload: envelopePayload,
		},
		{
			name:    "malformed envelope",
			payload: []byte{0, 0, 0, 0},
		},
		{
			name:    "malformed payload",
			payload: append(bytes.Clone(envelopePayload), 0, 0, 0, 0),
		},
		{
			name:    "trailing bytes",
			payload: append(bytes.Clone(validPayload), 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var envelope overlay.Query
			if _, err := parseQUICOverlayObject(tt.payload, &envelope); err == nil {
				t.Fatal("parse succeeded")
			}
		})
	}
}

func TestHandleQUICQueryChecksPrivateMembershipBeforePing(t *testing.T) {
	node := newTestNode(t)
	memberID := testPeerID("quic-private-member")
	outsiderID := testPeerID("quic-private-outsider")
	overlayID := testPeerID("quic-private-overlay").Bytes()

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "quic-private",
			Kind:         overlayKindCustomFixed,
			ShortID:      overlayID,
			FixedNodeIDs: map[PeerID]struct{}{memberID: {}},
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	payload := testQUICOverlayQueryPayload(t, overlayID, overlay.Ping{})
	if answer, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: outsiderID},
		payload,
	); !errors.Is(err, errQUICOverlayNotFound) || answer != nil {
		t.Fatalf("outsider result = (%x, %v), want nil overlay-not-found", answer, err)
	}

	envelope, err := tl.Serialize(overlay.Query{Overlay: overlayID}, true)
	if err != nil {
		t.Fatalf("serialize private overlay envelope: %v", err)
	}
	malformed := append(envelope, 0, 0, 0, 0)
	if answer, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: outsiderID},
		malformed,
	); !errors.Is(err, errQUICOverlayNotFound) || answer != nil {
		t.Fatalf(
			"outsider malformed result = (%x, %v), want nil overlay-not-found",
			answer,
			err,
		)
	}

	answer, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: memberID},
		payload,
	)
	if err != nil {
		t.Fatalf("member ping: %v", err)
	}
	var response any
	rest, err := tl.ParseNoCopy(&response, answer, true)
	if err != nil {
		t.Fatalf("parse ping response: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("ping response has %d trailing bytes", len(rest))
	}
	if _, ok := response.(overlay.Pong); !ok {
		t.Fatalf("ping response type = %T, want overlay.Pong value", response)
	}
}

func TestHandleQUICQueryChecksFastSyncCertificate(t *testing.T) {
	now := time.Now()
	issuer := newFastSyncMembershipTestIssuer(t, 0x61)
	permanent := testPeerID("quic-fast-sync-permanent")
	member := testPeerID("quic-fast-sync-certified")
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		member,
		0,
		int32(now.Add(time.Hour).Unix()),
	)
	membership := newFastSyncTestMembership(
		fastSyncMembershipTestRoster(
			[]PeerID{issuer.id},
			[]PeerID{permanent},
		),
		0,
	)

	node := newTestNode(t)
	overlayID := testPeerID("quic-fast-sync-overlay").Bytes()
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "quic-fast-sync",
			Kind:    overlayKindFastSync,
			ShortID: overlayID,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
		fastSync: &fastSyncOverlayRuntime{
			membership: membership,
		},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	if _, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: permanent},
		testQUICOverlayQueryPayload(t, overlayID, overlay.Ping{}),
	); err != nil {
		t.Fatalf("permanent peer query: %v", err)
	}

	memberPayload := testQUICOverlayQueryWithExtraPayload(
		t,
		overlayID,
		overlay.MessageExtra{
			Flags:       1,
			Certificate: certificate,
		},
		overlay.Ping{},
	)
	if _, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: member},
		memberPayload,
	); err != nil {
		t.Fatalf("certified peer query: %v", err)
	}

	if _, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: member},
		testQUICOverlayQueryPayload(t, overlayID, overlay.Ping{}),
	); !errors.Is(err, errQUICOverlayNotFound) {
		t.Fatalf("omitted unknown member certificate error = %v", err)
	}
	if _, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: testPeerID("quic-fast-sync-unknown")},
		testQUICOverlayQueryWithExtraPayload(
			t,
			overlayID,
			overlay.MessageExtra{
				Flags:       1,
				Certificate: overlay.EmptyMemberCertificate{},
			},
			overlay.Ping{},
		),
	); !errors.Is(err, errQUICOverlayNotFound) {
		t.Fatalf("empty unknown member certificate error = %v", err)
	}
}

func TestParseQUICPlainMessageEnvelope(t *testing.T) {
	overlayID := testPeerID("quic-plain-message-overlay").Bytes()
	payload, err := tl.Serialize(overlay.Message{Overlay: overlayID}, true)
	if err != nil {
		t.Fatalf("serialize message envelope: %v", err)
	}
	payload, err = tl.Append(payload, overlay.Ping{}, true)
	if err != nil {
		t.Fatalf("serialize message body: %v", err)
	}

	header, body, err := parseQUICMessageEnvelope(payload)
	if err != nil {
		t.Fatalf("parse plain message: %v", err)
	}
	if !bytes.Equal(header.overlay, overlayID) {
		t.Fatalf("overlay id = %x, want %x", header.overlay, overlayID)
	}
	if cap(header.overlay) != PeerIDSize {
		t.Fatalf("overlay id capacity = %d, want %d", cap(header.overlay), PeerIDSize)
	}
	if &header.overlay[0] != &payload[4] {
		t.Fatal("plain message overlay id does not alias the input")
	}
	if &body[0] != &payload[plainOverlayMessageEnvelopeSize] {
		t.Fatal("plain message body does not alias the input")
	}

	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "short overlay id",
			payload: payload[:plainOverlayMessageEnvelopeSize-1],
		},
		{
			name:    "missing body",
			payload: payload[:plainOverlayMessageEnvelopeSize],
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotHeader, _, parseErr := parseQUICMessageEnvelope(test.payload)
			if parseErr == nil {
				t.Fatal("parse succeeded")
			}
			if test.name == "missing body" &&
				!bytes.Equal(gotHeader.overlay, overlayID) {
				t.Fatalf(
					"overlay id on missing body = %x, want %x",
					gotHeader.overlay,
					overlayID,
				)
			}
		})
	}
}

func TestParseQUICMessageWithMemberCertificate(t *testing.T) {
	overlayID := testPeerID("quic-member-message-overlay").Bytes()
	certificate := overlay.MemberCertificate{
		IssuedBy: newFastSyncMembershipTestIssuer(t, 0x62).public,
		Flags:    3,
		Slot:     1,
		ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
	}
	payload, err := tl.Serialize(overlay.MessageWithExtra{
		Overlay: overlayID,
		Extra: overlay.MessageExtra{
			Flags:       1,
			Certificate: certificate,
		},
	}, true)
	if err != nil {
		t.Fatalf("serialize message with extra: %v", err)
	}
	payload, err = tl.Append(payload, overlay.Ping{}, true)
	if err != nil {
		t.Fatalf("serialize message body: %v", err)
	}

	header, body, err := parseQUICMessageEnvelope(payload)
	if err != nil {
		t.Fatalf("parse message with extra: %v", err)
	}
	if header.certificateKind != quicMembershipCertificateMember ||
		header.certificate.Slot != certificate.Slot {
		t.Fatalf("parsed member certificate = %+v", header)
	}
	message, err := parseOneQUICOverlayObject(body)
	if err != nil {
		t.Fatalf("parse message body: %v", err)
	}
	if _, ok := message.(overlay.Ping); !ok {
		t.Fatalf("message body type = %T, want overlay.Ping", message)
	}
}

func TestHandleQUICQueryAllowsAnswerAboveDefaultMessageLimit(t *testing.T) {
	store := &quicZeroStatePeerStore{
		PeerStorage: newTestPeerStore(),
		data: make(
			[]byte,
			adnlquic.MaxPlumtreePayloadSize+1,
		),
	}
	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		PeerStorage:   store,
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(node.closeSubscriptions)

	overlayID := testPeerID("quic-answer-size-overlay").Bytes()
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "quic-answer-size",
			ShortID: overlayID,
		},
		log: discardLogger(),
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	payload := testQUICOverlayQueryPayload(t, overlayID, DownloadZeroState{
		Block: testStoredMasterBlockID(0),
	})
	store.data = store.data[:adnlquic.MaxPlumtreePayloadSize]
	answer, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: testPeerID("quic-answer-size-peer")},
		payload,
	)
	if err != nil {
		t.Fatalf("maximum-size answer: %v", err)
	}
	if len(answer) != adnlquic.MaxPlumtreePayloadSize {
		t.Fatalf(
			"default-limit answer = %d bytes, want %d",
			len(answer),
			adnlquic.MaxPlumtreePayloadSize,
		)
	}

	store.data = store.data[:adnlquic.MaxPlumtreePayloadSize+1]
	answer, err = node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: testPeerID("quic-answer-size-peer")},
		payload,
	)
	if err != nil {
		t.Fatalf("answer above default message limit: %v", err)
	}
	if len(answer) != adnlquic.MaxPlumtreePayloadSize+1 {
		t.Fatalf(
			"answer above default message limit = %d bytes, want %d",
			len(answer),
			adnlquic.MaxPlumtreePayloadSize+1,
		)
	}
}

func TestHandleQUICQuerySlotWaitCancelsWithoutReleasingOccupiedSlot(t *testing.T) {
	node := newTestNode(t)
	if cap(node.quicQuerySlots) != inboundQUICQueryParallelism {
		t.Fatalf(
			"QUIC query parallelism = %d, want %d",
			cap(node.quicQuerySlots),
			inboundQUICQueryParallelism,
		)
	}
	for range inboundQUICQueryParallelism {
		node.quicQuerySlots <- struct{}{}
	}
	t.Cleanup(func() {
		for len(node.quicQuerySlots) > 0 {
			<-node.quicQuerySlots
		}
	})

	overlayID := testPeerID("quic-query-slot-overlay").Bytes()
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "quic-query-slot",
			ShortID: overlayID,
		},
		log: discardLogger(),
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	payload := testQUICOverlayQueryPayload(t, overlayID, overlay.Ping{})
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := node.handleQUICQuery(
			ctx,
			&authenticatedQUICPeer{id: testPeerID("quic-query-slot-peer")},
			payload,
		)
		result <- err
	}()
	<-started

	select {
	case err := <-result:
		t.Fatalf("query returned before cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("query error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("query did not stop after context cancellation")
	}
	if len(node.quicQuerySlots) != inboundQUICQueryParallelism {
		t.Fatalf(
			"occupied QUIC query slots = %d, want %d",
			len(node.quicQuerySlots),
			inboundQUICQueryParallelism,
		)
	}
}

func TestNewQUICGatewayUsesADNLIdentity(t *testing.T) {
	node := newTestNode(t)

	if !bytes.Equal(node.quicGateway.ID(), node.gateway.GetID()) {
		t.Fatal("QUIC and ADNL gateways use different identities")
	}
}

func TestStaleQUICDisconnectDoesNotRemoveReplacementPath(t *testing.T) {
	node := newTestNode(t)
	peerID := testPeerID("replaced-quic-path")
	stale := &adnlquic.Peer{}
	replacement := &adnlquic.Peer{}
	replacementPath := &authenticatedQUICPeer{
		peer: replacement,
		id:   peerID,
	}

	node.quicPeersMx.Lock()
	node.quicPeers[peerID] = replacementPath
	node.quicPeersMx.Unlock()

	if got := node.StatusSnapshot().QUICPeers; got != 1 {
		t.Fatalf("QUIC peer count = %d, want 1", got)
	}

	node.removeQUICPeer(peerID, stale)
	current, err := node.authenticatedQUICPeer(peerID)
	if err != nil {
		t.Fatalf("lookup replacement QUIC path: %v", err)
	}
	if current != replacementPath {
		t.Fatal("stale disconnect removed the replacement QUIC path")
	}

	node.removeQUICPeer(peerID, replacement)
	if _, err = node.authenticatedQUICPeer(peerID); !errors.Is(err, errAuthenticatedQUICPeerNotFound) {
		t.Fatalf("lookup removed QUIC path: %v", err)
	}
	if got := node.StatusSnapshot().QUICPeers; got != 0 {
		t.Fatalf("QUIC peer count after disconnect = %d, want 0", got)
	}
}

func TestNewRejectsInconsistentADNLPrivateKey(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	privateKey[ed25519.SeedSize] ^= 1
	logger := zerolog.Nop()

	if _, err := New(Options{
		Logger:        &logger,
		PrivateKey:    privateKey,
		PeerStorage:   newTestPeerStore(),
		StateFilesDir: t.TempDir(),
	}); err == nil {
		t.Fatal("New accepted an Ed25519 private key with an inconsistent public half")
	}
}

func TestClientOnlyGatewayDoesNotStartQUICListener(t *testing.T) {
	node := newTestNode(t)

	if err := node.startGateway(); err != nil {
		t.Fatalf("start client gateway: %v", err)
	}
	t.Cleanup(func() {
		_ = node.gateway.Close()
		_ = node.closeQUICGateway()
	})

	if node.quicPacketConn != nil || node.quicServeDone != nil {
		t.Fatal("client-only node started a public QUIC listener")
	}
}

func TestStartQUICServerBindsWildcardAtADNLPortPlus1000(t *testing.T) {
	adnlEndpoint, allocatedQUICEndpoint := testFreeQUICDerivedEndpoint(t)
	wantQUICEndpoint := netip.AddrPortFrom(
		netip.IPv4Unspecified(),
		allocatedQUICEndpoint.Port(),
	)
	node := newTestNode(t)
	node.listenAddr = adnlEndpoint.String()

	if err := node.startQUICServer(); err != nil {
		t.Fatalf("start QUIC server: %v", err)
	}
	t.Cleanup(func() {
		if err := node.closeQUICGateway(); err != nil {
			t.Errorf("close QUIC gateway: %v", err)
		}
	})

	if node.quicPacketConn == nil {
		t.Fatal("QUIC packet listener was not created")
	}
	got, err := netip.ParseAddrPort(node.quicPacketConn.LocalAddr().String())
	if err != nil {
		t.Fatalf("parse QUIC listener address: %v", err)
	}
	if got != wantQUICEndpoint {
		t.Fatalf("QUIC listener = %s, want %s", got, wantQUICEndpoint)
	}
}

func TestQUICListenEndpointMatchesCppnodePortAndWildcard(t *testing.T) {
	tests := []struct {
		name        string
		adnl        netip.AddrPort
		wantNetwork string
		want        netip.AddrPort
	}{
		{
			name:        "IPv4",
			adnl:        netip.MustParseAddrPort("127.0.0.1:30303"),
			wantNetwork: "udp4",
			want:        netip.MustParseAddrPort("0.0.0.0:31303"),
		},
		{
			name:        "IPv6 ADNL still uses the IPv4 QUIC wildcard",
			adnl:        netip.MustParseAddrPort("[2001:db8::1]:30303"),
			wantNetwork: "udp4",
			want:        netip.MustParseAddrPort("0.0.0.0:31303"),
		},
		{
			name:        "uint16 wrap",
			adnl:        netip.MustParseAddrPort("127.0.0.1:64536"),
			wantNetwork: "udp4",
			want:        netip.MustParseAddrPort("0.0.0.0:0"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network, endpoint := quicListenEndpoint(tt.adnl)
			if network != tt.wantNetwork {
				t.Fatalf("network = %q, want %q", network, tt.wantNetwork)
			}
			if endpoint != tt.want {
				t.Fatalf("endpoint = %s, want %s", endpoint, tt.want)
			}
		})
	}
}

func TestStartQUICServerRejectsUnsupportedEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
	}{
		{
			name:     "hostname",
			endpoint: "localhost:30303",
		},
		{
			name:     "zero ADNL port",
			endpoint: "127.0.0.1:0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newTestNode(t)
			node.listenAddr = tt.endpoint

			if err := node.startQUICServer(); err == nil {
				t.Cleanup(func() {
					_ = node.closeQUICGateway()
				})
				t.Fatal("start succeeded")
			}
		})
	}
}

func testQUICOverlayQueryPayload(
	tb testing.TB,
	overlayID []byte,
	query tl.Serializable,
) []byte {
	tb.Helper()

	payload, err := tl.Serialize(overlay.Query{Overlay: overlayID}, true)
	if err != nil {
		tb.Fatalf("serialize QUIC overlay query envelope: %v", err)
	}
	payload, err = tl.Append(payload, query, true)
	if err != nil {
		tb.Fatalf("serialize QUIC overlay query: %v", err)
	}
	return payload
}

func testQUICOverlayQueryWithExtraPayload(
	tb testing.TB,
	overlayID []byte,
	extra overlay.MessageExtra,
	query tl.Serializable,
) []byte {
	tb.Helper()

	payload, err := tl.Serialize(overlay.QueryWithExtra{
		Overlay: overlayID,
		Extra:   extra,
	}, true)
	if err != nil {
		tb.Fatalf("serialize QUIC overlay query with extra: %v", err)
	}
	payload, err = tl.Append(payload, query, true)
	if err != nil {
		tb.Fatalf("serialize QUIC overlay query body: %v", err)
	}
	return payload
}

func testFreeQUICDerivedEndpoint(tb testing.TB) (netip.AddrPort, netip.AddrPort) {
	tb.Helper()

	for range 16 {
		packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			tb.Fatalf("allocate QUIC test port: %v", err)
		}
		quicEndpoint, parseErr := netip.ParseAddrPort(packetConn.LocalAddr().String())
		closeErr := packetConn.Close()
		if parseErr != nil {
			tb.Fatalf("parse allocated QUIC test port: %v", parseErr)
		}
		if closeErr != nil {
			tb.Fatalf("release allocated QUIC test port: %v", closeErr)
		}
		if quicEndpoint.Port() > 1000 {
			return netip.AddrPortFrom(
				quicEndpoint.Addr(),
				quicEndpoint.Port()-1000,
			), quicEndpoint
		}
	}
	tb.Fatal("failed to allocate a QUIC port above 1000")
	return netip.AddrPort{}, netip.AddrPort{}
}

// An inbound query arrives on a connection the peer already holds, so the
// inactive-overlay notice must go back down that connection. Deriving an
// outbound sender from the peer id instead dials, and for a peer this node has
// never dialled itself that dial resolves through the DHT - all while the
// handler still holds one of the 64 inbound query slots, which is how a handful
// of unresolvable peers could stall every inbound QUIC query.
//
// The DHT stub blocks, so a regression shows up as both a non-zero call count
// and a handler that only returns once the query deadline expires.
func TestHandleQUICQueryDoesNotDialWhenOverlayIsInactive(t *testing.T) {
	memberPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate member key: %v", err)
	}
	memberID, err := peerIDFromPublicKey(memberPub)
	if err != nil {
		t.Fatalf("member id: %v", err)
	}
	overlayID := testPeerID("quic-inactive-overlay").Bytes()

	stub := &blockingOutboundRouteDHT{
		// A non-nil list keeps the teardown of a failing run clean: the blocked
		// resolve is released by the deferred close and must not dereference nil.
		addresses: &adnladdr.List{},
		pub:       memberPub,
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	defer close(stub.release)

	node := newTestNode(t)
	node.dht = stub

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:         "quic-inactive",
			Kind:         overlayKindCustomFixed,
			ShortID:      overlayID,
			FixedNodeIDs: map[PeerID]struct{}{memberID: {}},
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	sub.setActive(false, time.Time{})

	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	// Registering the peer with its key is what makes the outbound dial - and
	// with it the DHT resolve - reachable from the inbound handler.
	inbound := &authenticatedQUICPeer{
		node:      node,
		id:        memberID,
		publicKey: memberPub,
		route:     node.peerRoutes.Get(memberID),
	}
	node.quicPeersMx.Lock()
	node.quicPeers[memberID] = inbound
	node.quicPeersMx.Unlock()

	payload := testQUICOverlayQueryPayload(t, overlayID, overlay.Ping{})

	done := make(chan struct{})
	var (
		answer   []byte
		queryErr error
	)
	go func() {
		defer close(done)
		answer, queryErr = node.handleQUICQuery(context.Background(), inbound, payload)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("inbound query blocked, so it went out to the network")
	}

	if !errors.Is(queryErr, errOverlayInactive) || answer != nil {
		t.Fatalf("inactive overlay result = (%x, %v), want nil overlay-inactive", answer, queryErr)
	}
	if calls := stub.calls.Load(); calls != 0 {
		t.Fatalf("inbound query made %d DHT lookups, want 0", calls)
	}
	if addr := inbound.route.QUICAddress(); addr != "" {
		t.Fatalf("inbound query resolved a dial route = %q, want none", addr)
	}
}
