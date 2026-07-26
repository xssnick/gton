package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func TestSetFastSyncOverlaysReconcilesValidatorShards(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, PeerIDSize)

	local := fastSyncOverlayTestValidator(0x11, node.localID)
	remoteID := testPeerID("fast-sync-overlay-remote")
	remote := fastSyncOverlayTestValidator(0x22, remoteID)
	roster := NewFastSyncValidatorRoster(
		nil,
		[]FastSyncValidator{local, remote},
		nil,
	)
	state := FastSyncState{
		Roster: roster,
		Shards: []FastSyncShard{{
			Workchain: 0,
			Shard:     topShard,
		}},
		MasterchainPlumtreeEnabled: true,
		ShardPlumtreeEnabled:       true,
	}

	if err := node.SetFastSyncOverlays(state); err != nil {
		t.Fatalf("set FastSync overlays: %v", err)
	}
	if len(node.fastSyncSubscriptions) != 2 {
		t.Fatalf(
			"FastSync subscription count = %d, want 2",
			len(node.fastSyncSubscriptions),
		)
	}

	master := node.fastSyncSubscriptions[FastSyncShard{
		Workchain: -1,
		Shard:     topShard,
	}]
	shard := node.fastSyncSubscriptions[FastSyncShard{
		Workchain: 0,
		Shard:     topShard,
	}]
	for name, sub := range map[string]*overlaySubscription{
		"masterchain": master,
		"shardchain":  shard,
	} {
		if sub == nil {
			t.Fatalf("%s FastSync subscription is missing", name)
		}
		if !sub.spec.UseQUIC || !sub.spec.SendQueries ||
			!sub.spec.AcceptQueries || !sub.spec.RandomPeers {
			t.Fatalf("%s FastSync transport flags = %+v", name, sub.spec)
		}
		if sub.spec.ProtoVersionMajor != 3 ||
			sub.spec.ProtoVersionMinor != 2 {
			t.Fatalf(
				"%s FastSync protocol version = %d.%d, want 3.2",
				name,
				sub.spec.ProtoVersionMajor,
				sub.spec.ProtoVersionMinor,
			)
		}
		if sub.fastSync == nil || !sub.fastSync.spec.localValidator ||
			sub.fastSync.spec.receiveBroadcasts {
			t.Fatalf("%s FastSync local policy = %+v", name, sub.fastSync)
		}
		if sub.plumtree == nil {
			t.Fatalf("%s FastSync Plumtree runtime is missing", name)
		}
		if got := sub.peerLimit(); got != maxPeersPerOverlay {
			t.Fatalf(
				"%s peer limit = %d, want %d",
				name,
				got,
				maxPeersPerOverlay,
			)
		}
	}

	originalMaster := master
	if err := node.SetFastSyncOverlays(state); err != nil {
		t.Fatalf("repeat FastSync reconciliation: %v", err)
	}
	if node.fastSyncSubscriptions[FastSyncShard{
		Workchain: -1,
		Shard:     topShard,
	}] != originalMaster {
		t.Fatal("unchanged FastSync state replaced the subscription")
	}

	node.zeroStateFileHash = make([]byte, PeerIDSize-1)
	if err := node.SetFastSyncOverlays(FastSyncState{
		Roster: roster,
		Shards: append(state.Shards, FastSyncShard{
			Workchain: 1,
			Shard:     topShard,
		}),
		MasterchainPlumtreeEnabled: true,
		ShardPlumtreeEnabled:       true,
	}); err == nil {
		t.Fatal("invalid zero-state file hash was accepted")
	}
	if node.fastSyncSubscriptions[FastSyncShard{
		Workchain: -1,
		Shard:     topShard,
	}] != originalMaster || len(node.fastSyncSubscriptions) != 2 {
		t.Fatal("failed reconciliation changed active FastSync subscriptions")
	}

	node.zeroStateFileHash = make([]byte, PeerIDSize)
	if err := node.SetFastSyncOverlays(FastSyncState{
		Roster: NewFastSyncValidatorRoster(
			nil,
			[]FastSyncValidator{remote},
			nil,
		),
		Shards: state.Shards,
	}); err != nil {
		t.Fatalf("remove unauthorized local FastSync overlays: %v", err)
	}
	if len(node.fastSyncSubscriptions) != 0 {
		t.Fatalf(
			"unauthorized local node retained %d FastSync subscriptions",
			len(node.fastSyncSubscriptions),
		)
	}
}

func TestFastSyncPeerLimitIncludesCertificateSlots(t *testing.T) {
	roots := make([]FastSyncValidator, 0, 5)
	for i := range 5 {
		roots = append(roots, fastSyncOverlayTestValidator(
			byte(i+1),
			testPeerID(string(rune('a'+i))),
		))
	}
	roster := NewFastSyncValidatorRoster(nil, roots, nil)
	sub := &overlaySubscription{
		spec: overlaySpec{
			Kind:       overlayKindFastSync,
			FixedNodes: roster.adnlIDs,
		},
		fastSync: &fastSyncOverlayRuntime{
			spec: fastSyncOverlaySpec{roster: roster},
		},
	}

	want := len(roster.adnlIDs) +
		len(roster.rootPublicKeyIDs)*FastSyncMemberSlotCount
	if got := sub.peerLimit(); got != want {
		t.Fatalf("FastSync peer limit = %d, want %d", got, want)
	}
}

func TestFastSyncWarmupPromotesLearnedPeerBeforeExchange(t *testing.T) {
	peers, remoteID := newFastSyncLearnedPeerRuntime(t)
	runtime := &fastSyncOverlayRuntime{
		membership:     peers.membership,
		peers:          peers,
		aliveRootIndex: make(map[PeerID]int),
	}

	var queryOrder []string
	peer := testReadyQueryPeer("fast-sync-learned-peer")
	peer.id = remoteID
	adnlOverlay, adnlPeer := newTestOverlayWrapper()
	t.Cleanup(adnlOverlay.Close)
	peer.overlay = adnlOverlay
	peer.queryTransport = fastSyncLivenessQueryTransport{
		query: func(
			_ context.Context,
			maxAnswerSize uint64,
			req tl.Serializable,
			result tl.Serializable,
		) error {
			if _, ok := req.(GetCapabilities); !ok {
				return fmt.Errorf("unexpected FastSync query %T", req)
			}
			if maxAnswerSize != fastSyncPingMaxAnswer {
				return fmt.Errorf(
					"capabilities max answer = %d, want %d",
					maxAnswerSize,
					fastSyncPingMaxAnswer,
				)
			}

			queryOrder = append(queryOrder, "capabilities")
			capabilities, ok := result.(*Capabilities)
			if !ok {
				return fmt.Errorf(
					"capabilities destination is %T",
					result,
				)
			}
			*capabilities = Capabilities{
				VersionMajor: 3,
				VersionMinor: 2,
			}
			return nil
		},
	}
	adnlPeer.queryResponder = func(
		req tl.Serializable,
		result tl.Serializable,
	) error {
		if _, ok := testOverlayQueryPayload(req).(overlay.GetRandomPeersV2); !ok {
			return fmt.Errorf("unexpected ADNL overlay query %T", req)
		}

		queryOrder = append(queryOrder, "random peers")
		if alive := peers.Counts().AliveNonPermanent; alive != 1 {
			return fmt.Errorf(
				"alive clients before exchange = %d",
				alive,
			)
		}
		nodes, ok := result.(*overlay.NodesV2)
		if !ok {
			return fmt.Errorf(
				"random peers destination is %T",
				result,
			)
		}
		*nodes = overlay.NodesV2{}
		return nil
	}
	sub := &overlaySubscription{
		node:     &Node{},
		spec:     overlaySpec{Kind: overlayKindFastSync},
		log:      discardLogger(),
		peers:    map[PeerID]*overlayPeer{remoteID: peer},
		fastSync: runtime,
	}

	sub.warmupFastSyncPeer(context.Background(), peer)

	if len(queryOrder) != 2 ||
		queryOrder[0] != "capabilities" ||
		queryOrder[1] != "random peers" {
		t.Fatalf("FastSync warmup query order = %v", queryOrder)
	}
	response, err := peers.RandomPeers(time.Now(), 1)
	if err != nil {
		t.Fatalf("random peers after warmup: %v", err)
	}
	if len(response.Nodes) != 2 {
		t.Fatalf(
			"random peers after warmup = %d nodes, want local and learned",
			len(response.Nodes),
		)
	}
	if got := peerRuntimeTestNodeID(t, response.Nodes[1]); got != remoteID {
		t.Fatalf("propagated peer = %v, want %v", got, remoteID)
	}
}

func TestFastSyncRandomPeersRejectsOversizedResponseBeforeLearning(t *testing.T) {
	peers, remoteID := newFastSyncLearnedPeerRuntime(t)
	runtime := &fastSyncOverlayRuntime{
		membership:     peers.membership,
		peers:          peers,
		aliveRootIndex: make(map[PeerID]int),
	}
	before := peers.Counts()

	peer := testReadyQueryPeer("fast-sync-oversized-random-peers")
	peer.id = remoteID
	adnlOverlay, adnlPeer := newTestOverlayWrapper()
	t.Cleanup(adnlOverlay.Close)
	peer.overlay = adnlOverlay
	adnlPeer.queryResponder = func(
		req tl.Serializable,
		result tl.Serializable,
	) error {
		if _, ok := testOverlayQueryPayload(req).(overlay.GetRandomPeersV2); !ok {
			return fmt.Errorf("unexpected ADNL overlay query %T", req)
		}

		nodes, ok := result.(*overlay.NodesV2)
		if !ok {
			return fmt.Errorf("random peers destination is %T", result)
		}
		nodes.Nodes = make(
			[]overlay.NodeV2,
			fastSyncRandomPeerResultLimit+1,
		)
		return nil
	}
	sub := &overlaySubscription{
		node:     &Node{},
		spec:     overlaySpec{Kind: overlayKindFastSync},
		log:      discardLogger(),
		peers:    map[PeerID]*overlayPeer{remoteID: peer},
		fastSync: runtime,
	}

	sub.exchangeFastSyncRandomPeers(context.Background(), peer)

	if after := peers.Counts(); after != before {
		t.Fatalf("oversized response changed peer runtime: before=%+v after=%+v", before, after)
	}
	if sub.advertisedPeerLearning.Load() {
		t.Fatal("oversized response entered peer learning")
	}
}

func TestFastSyncQueryTimeoutCoalescesCapabilityPing(t *testing.T) {
	peers, remoteID := newFastSyncLearnedPeerRuntime(t)
	runtime := &fastSyncOverlayRuntime{
		membership:     peers.membership,
		peers:          peers,
		aliveRootIndex: make(map[PeerID]int),
	}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var queries atomic.Int32
	peer := testReadyQueryPeer("fast-sync-timeout-peer")
	peer.id = remoteID
	peer.queryTransport = fastSyncLivenessQueryTransport{
		query: func(
			ctx context.Context,
			_ uint64,
			req tl.Serializable,
			result tl.Serializable,
		) error {
			if _, ok := req.(GetCapabilities); !ok {
				return fmt.Errorf("unexpected FastSync query %T", req)
			}
			capabilities, ok := result.(*Capabilities)
			if !ok {
				return fmt.Errorf(
					"capabilities destination is %T",
					result,
				)
			}

			queries.Add(1)
			started <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
			*capabilities = Capabilities{
				VersionMajor: 3,
				VersionMinor: 2,
			}
			return nil
		},
	}
	node := &Node{
		runCtx: context.Background(),
	}
	sub := &overlaySubscription{
		node:     node,
		spec:     overlaySpec{Kind: overlayKindFastSync},
		log:      discardLogger(),
		peers:    map[PeerID]*overlayPeer{remoteID: peer},
		fastSync: runtime,
	}
	timeoutErr := fmt.Errorf("application query: %w", context.DeadlineExceeded)

	sub.finishPeerQueryOperation(peer, timeoutErr)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("FastSync timeout did not start capabilities query")
	}

	sub.finishPeerQueryOperation(peer, timeoutErr)
	if got := queries.Load(); got != 1 {
		t.Fatalf("concurrent timeout pings = %d, want 1", got)
	}

	close(release)
	node.wg.Wait()

	if got := queries.Load(); got != 1 {
		t.Fatalf("completed timeout pings = %d, want 1", got)
	}
	if alive := peers.Counts().AliveNonPermanent; alive != 1 {
		t.Fatalf("alive clients after timeout ping = %d, want 1", alive)
	}
}

type fastSyncLivenessQueryTransport struct {
	query func(
		context.Context,
		uint64,
		tl.Serializable,
		tl.Serializable,
	) error
}

func (t fastSyncLivenessQueryTransport) Query(
	ctx context.Context,
	maxAnswerSize uint64,
	req tl.Serializable,
	result tl.Serializable,
) error {
	return t.query(ctx, maxAnswerSize, req, result)
}

func (fastSyncLivenessQueryTransport) QueryRaw(
	context.Context,
	uint64,
	tl.Serializable,
) ([]byte, error) {
	return nil, errors.New("raw query is not supported")
}

func newFastSyncLearnedPeerRuntime(
	t *testing.T,
) (*fastSyncPeerRuntime, PeerID) {
	t.Helper()

	now := time.Now().Truncate(time.Second)
	overlayID := peerRuntimeTestOverlayID(0xe1)
	peers, issuerKey := peerRuntimeTestRootRuntime(t, overlayID, now)
	remoteKey := peerRuntimeTestKey(0xe2)
	remoteID := peerRuntimeTestPeerID(
		remoteKey.Public().(ed25519.PublicKey),
	)
	certificate := peerRuntimeTestCertificate(
		t,
		issuerKey,
		remoteID,
		0,
		0,
		int32(now.Add(time.Hour).Unix()),
	)
	node := peerRuntimeTestNode(
		t,
		remoteKey,
		overlayID,
		0,
		int32(now.Unix()),
		certificate,
	)
	if _, err := peers.EnrollNode(node, now); err != nil {
		t.Fatalf("enroll learned FastSync peer: %v", err)
	}
	return peers, remoteID
}

func TestSetFastSyncOverlaysRotatesCertificateInPlace(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, PeerIDSize)

	issuer := newFastSyncMembershipTestIssuer(t, 0x51)
	var issuerKey FastSyncValidatorPublicKey
	copy(issuerKey[:], issuer.public.Key)
	roster := NewFastSyncValidatorRoster(
		nil,
		[]FastSyncValidator{{
			PublicKey: issuerKey,
			ADNLID:    testPeerID("fast-sync-certificate-issuer"),
		}},
		nil,
	)
	now := time.Now()
	first := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node.localID,
		0,
		0,
		int32(now.Add(time.Hour).Unix()),
	)
	node.fastSyncCertificates = []overlay.MemberCertificate{first}

	state := FastSyncState{Roster: roster}
	if err := node.SetFastSyncOverlays(state); err != nil {
		t.Fatalf("set FastSync overlay: %v", err)
	}
	shard := FastSyncShard{Workchain: -1, Shard: topShard}
	sub := node.fastSyncSubscriptions[shard]
	if sub == nil {
		t.Fatal("masterchain FastSync subscription is missing")
	}

	second := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node.localID,
		1,
		0,
		int32(now.Add(2*time.Hour).Unix()),
	)
	node.fastSyncCertificates = []overlay.MemberCertificate{second}
	if err := node.SetFastSyncOverlays(state); err != nil {
		t.Fatalf("rotate FastSync certificate: %v", err)
	}
	if node.fastSyncSubscriptions[shard] != sub {
		t.Fatal("certificate rotation replaced the FastSync subscription")
	}

	payload, err := sub.quicEnvelope.Query(overlay.Ping{})
	if err != nil {
		t.Fatalf("serialize query with rotated certificate: %v", err)
	}
	header, _, err := parseQUICQueryEnvelope(payload)
	if err != nil {
		t.Fatalf("parse query with rotated certificate: %v", err)
	}
	if header.certificateKind != quicMembershipCertificateMember ||
		header.certificate.Slot != second.Slot ||
		header.certificate.ExpireAt != second.ExpireAt {
		t.Fatalf("query uses stale certificate: %+v", header)
	}

	payload, err = sub.quicEnvelope.Message(ForgetPeer{})
	if err != nil {
		t.Fatalf("serialize message with rotated certificate: %v", err)
	}
	header, _, err = parseQUICMessageEnvelope(payload)
	if err != nil {
		t.Fatalf("parse message with rotated certificate: %v", err)
	}
	if header.certificateKind != quicMembershipCertificateMember ||
		header.certificate.Slot != second.Slot ||
		header.certificate.ExpireAt != second.ExpireAt {
		t.Fatalf("message uses stale certificate: %+v", header)
	}
}

func fastSyncOverlayTestValidator(
	key byte,
	adnlID PeerID,
) FastSyncValidator {
	var publicKey FastSyncValidatorPublicKey
	for i := range publicKey {
		publicKey[i] = key
	}
	return FastSyncValidator{
		PublicKey: publicKey,
		ADNLID:    adnlID,
	}
}
