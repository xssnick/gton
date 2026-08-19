package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func TestPrivateOverlayRegistryBuildsRawFullIDAndOwnsConfig(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	fullID := []byte("raw private overlay id")
	source := testPeerID("private-broadcast-source")
	cfg := PrivateOverlayConfig{
		Name:                       "session",
		FullID:                     fullID,
		Members:                    []PeerID{node.localID, node.localID},
		AuthorizedBroadcastSources: map[PeerID]uint32{source: 1234},
		EnableTwoStep:              true,
		TwoStepIntermediateMembers: []PeerID{node.localID},
	}

	handle, err := node.PrivateOverlays().Open(cfg, PrivateOverlayCallbacks{})
	if err != nil {
		t.Fatalf("open private overlay: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })

	wantShortID, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		t.Fatalf("hash short id: %v", err)
	}
	handleID := handle.ID()
	if !bytes.Equal(handleID[:], wantShortID) {
		t.Fatalf("short id = %x, want %x", handleID, wantShortID)
	}
	if len(handle.sub.spec.FixedNodes) != 1 || handle.sub.spec.FixedNodes[0] != node.localID {
		t.Fatalf("fixed members = %v, want one local member", handle.sub.spec.FixedNodes)
	}

	fullID[0] ^= 0xff
	cfg.AuthorizedBroadcastSources[source] = 9
	if bytes.Equal(handle.sub.spec.FullID, fullID) {
		t.Fatal("private overlay retained caller-owned full id")
	}
	if got := handle.sub.spec.AuthorizedKeys[string(source[:])]; got != 1234 {
		t.Fatalf("authorized size = %d, want 1234", got)
	}
	if handle.sub.plumtree != nil {
		t.Fatal("private overlay created an unsupported Plumtree runtime")
	}
}

func TestPrivateOverlayTwoStepRequiresExplicitIntermediates(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	remote := testPeerID("private-overlay-intermediate")

	_, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:        []byte("missing two-step intermediates"),
		Members:       []PeerID{node.localID, remote},
		EnableTwoStep: true,
	}, PrivateOverlayCallbacks{})
	if err == nil {
		t.Fatal("two-step overlay accepted an implicit intermediate set")
	}

	handle, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:                     []byte("foreign two-step intermediate"),
		Members:                    []PeerID{node.localID},
		EnableTwoStep:              true,
		TwoStepIntermediateMembers: []PeerID{remote},
	}, PrivateOverlayCallbacks{})
	if err != nil {
		t.Fatalf("two-step overlay rejected an intermediate allow-set wider than its members: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	if _, ok := handle.sub.spec.PrivateTwoStepIntermediateIDs[remote]; !ok {
		t.Fatal("two-step overlay lost the configured current-validator intermediate")
	}
}

func TestPrivateOverlayTwoStepTargetsOnlyConfiguredIntermediates(t *testing.T) {
	validator := testPeerID("two-step-validator")
	observer := testPeerID("two-step-observer")
	validatorPeer := &overlayPeer{id: validator}
	observerPeer := &overlayPeer{id: observer}
	sub := &overlaySubscription{spec: overlaySpec{
		Kind:                          overlayKindPrivate,
		PrivateTwoStep:                true,
		PrivateTwoStepIntermediateIDs: map[PeerID]struct{}{validator: {}},
	}}
	sub.broadcastTargets.Store(&broadcastTargetsSnapshot{
		builtAt: time.Now(),
		peers:   []*overlayPeer{validatorPeer, observerPeer},
	})

	got := sub.twoStepIntermediateCandidates()
	if len(got) != 1 || got[0] != validatorPeer {
		t.Fatalf("two-step candidates = %#v, want only current validator", got)
	}
	if got = sub.twoStepCandidates(validator); len(got) != 1 || got[0] != observerPeer {
		t.Fatalf("two-step relay candidates = %#v, want every peer except the source", got)
	}
}

func TestPrivateOverlayReusesLaterDiscoveredPooledPeer(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	remoteID := testPeerID("private-overlay-later-peer")
	handle, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:  []byte("private-overlay-later-peer"),
		Members: []PeerID{node.localID, remoteID},
	}, PrivateOverlayCallbacks{})
	if err != nil {
		t.Fatalf("open private overlay: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })

	transport := newTestOverlayADNL()
	transport.id = remoteID.Bytes()
	transport.pub = ed25519.PublicKey(bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize))
	pooled, fresh, err := node.pool.wrap(transport)
	if err != nil {
		t.Fatalf("wrap discovered peer: %v", err)
	}
	if !fresh {
		t.Fatal("discovered peer unexpectedly reused a pool entry")
	}

	node.attachPrivateOverlayPeer(pooled)
	if peer := handle.sub.peerByID(remoteID); peer == nil {
		t.Fatal("private overlay did not reuse later discovered roster peer")
	}
}

func TestPrivateOverlayPolicyIsFixedAndSeparateFromTONCustomPolicy(t *testing.T) {
	spec := overlaySpec{
		Kind:                         overlayKindPrivate,
		PrivateAllowLegacyBroadcasts: true,
		PrivateTwoStep:               true,
	}
	for name, predicate := range map[string]func(*overlaySpec) bool{
		"seeds fixed nodes":       (*overlaySpec).seedsFromFixedNodes,
		"restricts peer ids":      (*overlaySpec).restrictsPeerIDs,
		"adopts roster ingress":   (*overlaySpec).adoptsInboundPeers,
		"keeps members permanent": (*overlaySpec).membersArePermanent,
		"sizes roster limit":      (*overlaySpec).rosterSizesPeerLimit,
		"hides private roster":    (*overlaySpec).privatePeerRoster,
		"supports fixed probes":   (*overlaySpec).runsFixedPeerProbes,
		"uses two-step":           (*overlaySpec).usesTwoStepDelivery,
		"relays legacy FEC":       (*overlaySpec).relaysFECBroadcasts,
	} {
		if !predicate(&spec) {
			t.Errorf("private overlay policy %q is disabled", name)
		}
	}
	for name, predicate := range map[string]func(*overlaySpec) bool{
		"TON sender roles":        (*overlaySpec).authorizesBroadcastSenders,
		"TON IHR drop":            (*overlaySpec).dropsIHRBroadcasts,
		"TON local rebroadcast":   (*overlaySpec).originatesLocalBroadcasts,
		"public ingress":          (*overlaySpec).servesPublicIngress,
		"public directory tier":   (*overlaySpec).hasDirectoryTier,
		"shard lifecycle":         (*overlaySpec).followsShardLifecycle,
		"TON two-step queue":      (*overlaySpec).runsTwoStepRebroadcastWorker,
		"preferred block queries": (*overlaySpec).preferredAsQuerySource,
		"Plumtree":                (*overlaySpec).usesPlumtree,
	} {
		if predicate(&spec) {
			t.Errorf("private overlay inherited unrelated policy %q", name)
		}
	}
}

func TestPrivateOverlayRegistryValidatesMembershipAndDuplicates(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	registry := node.PrivateOverlays()

	if _, err := registry.Open(PrivateOverlayConfig{
		FullID:  []byte("missing local"),
		Members: []PeerID{testPeerID("other")},
	}, PrivateOverlayCallbacks{}); err == nil {
		t.Fatal("overlay without local ADNL member was accepted")
	}

	cfg := PrivateOverlayConfig{
		FullID:  []byte("duplicate overlay"),
		Members: []PeerID{node.localID},
	}
	first, err := registry.Open(cfg, PrivateOverlayCallbacks{})
	if err != nil {
		t.Fatalf("open first overlay: %v", err)
	}
	defer first.Close()
	if _, err = registry.Open(cfg, PrivateOverlayCallbacks{}); !errors.Is(err, ErrPrivateOverlayExists) {
		t.Fatalf("duplicate open error = %v, want ErrPrivateOverlayExists", err)
	}
}

func TestPrivateOverlayCloseWaitsCallbacksAndPreventsABA(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	registry := node.PrivateOverlays()
	entered := make(chan struct{})
	release := make(chan struct{})
	cfg := PrivateOverlayConfig{
		FullID:  []byte("callback lifecycle"),
		Members: []PeerID{node.localID},
	}
	handle, err := registry.Open(cfg, PrivateOverlayCallbacks{
		Message: func(context.Context, PeerID, tl.Serializable) {
			close(entered)
			<-release
		},
	})
	if err != nil {
		t.Fatalf("open private overlay: %v", err)
	}

	callbackDone := make(chan error, 1)
	go func() {
		callbackDone <- handle.sub.handlePrivateOverlayMessage(
			context.Background(),
			testPeerID("callback peer"),
			tl.Raw{1, 2, 3, 4},
		)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("message callback did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- handle.Close() }()
	waitPrivateOverlayClosing(t, registry, overlaySpecKey(handle.sub.spec))
	if _, err = registry.Open(cfg, PrivateOverlayCallbacks{}); !errors.Is(err, ErrPrivateOverlayClosing) {
		t.Fatalf("open while closing error = %v, want ErrPrivateOverlayClosing", err)
	}
	select {
	case err = <-closeDone:
		t.Fatalf("Close returned before callback exited: %v", err)
	default:
	}

	close(release)
	if err = <-callbackDone; err != nil {
		t.Fatalf("message callback path: %v", err)
	}
	if err = <-closeDone; err != nil {
		t.Fatalf("close private overlay: %v", err)
	}
	if err = handle.sub.handlePrivateOverlayMessage(
		context.Background(),
		testPeerID("late callback peer"),
		tl.Raw{5, 6, 7, 8},
	); !errors.Is(err, ErrPrivateOverlayClosed) {
		t.Fatalf("late callback error = %v, want ErrPrivateOverlayClosed", err)
	}

	reopened, err := registry.Open(cfg, PrivateOverlayCallbacks{})
	if err != nil {
		t.Fatalf("reopen private overlay: %v", err)
	}
	defer reopened.Close()
	if reopened == handle || !reopened.sub.isActive() {
		t.Fatal("reopen did not create an independent active generation")
	}
	if err = handle.Close(); err != nil {
		t.Fatalf("repeat old Close: %v", err)
	}
	if !reopened.sub.isActive() {
		t.Fatal("old handle Close removed the reopened generation")
	}
}

func TestPrivateOverlayQUICRawMessageAndQueryBypassTONDispatch(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	remote := testPeerID("private quic remote")
	messageBody := []byte{0xef, 0xbe, 0xad, 0xde, 1, 2, 3}
	queryBody := []byte{0xce, 0xfa, 0xed, 0xfe, 4, 5, 6}
	answerBody := []byte{0x44, 0x33, 0x22, 0x11, 7, 8}

	messageReceived := make(chan tl.Raw, 1)
	queryReceived := make(chan tl.Raw, 1)
	handle, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:  []byte("raw quic ingress"),
		Members: []PeerID{node.localID, remote},
		UseQUIC: true,
	}, PrivateOverlayCallbacks{
		Message: func(_ context.Context, source PeerID, message tl.Serializable) {
			if source != remote {
				t.Errorf("message source = %s, want %s", source, remote)
			}
			raw, ok := message.(tl.Raw)
			if !ok {
				t.Errorf("message type = %T, want tl.Raw", message)
				return
			}
			messageReceived <- raw
		},
		Query: func(_ context.Context, source PeerID, request tl.Serializable) (tl.Serializable, error) {
			if source != remote {
				t.Errorf("query source = %s, want %s", source, remote)
			}
			raw, ok := request.(tl.Raw)
			if !ok {
				t.Errorf("query type = %T, want tl.Raw", request)
				return nil, errors.New("query was not raw")
			}
			queryReceived <- raw
			return tl.Raw(answerBody), nil
		},
	})
	if err != nil {
		t.Fatalf("open private overlay: %v", err)
	}
	defer handle.Close()

	peer := &authenticatedQUICPeer{id: remote, addr: "private-quic-peer"}
	messageWire, err := handle.sub.quicEnvelope.Message(tl.Raw(messageBody))
	if err != nil {
		t.Fatalf("wrap raw message: %v", err)
	}
	if err = node.handleQUICMessage(context.Background(), peer, messageWire); err != nil {
		t.Fatalf("handle raw QUIC message: %v", err)
	}
	if got := <-messageReceived; !bytes.Equal(got, messageBody) {
		t.Fatalf("raw message = %x, want %x", got, messageBody)
	}

	queryWire, err := handle.sub.quicEnvelope.Query(tl.Raw(queryBody))
	if err != nil {
		t.Fatalf("wrap raw query: %v", err)
	}
	answer, err := node.handleQUICQuery(context.Background(), peer, queryWire)
	if err != nil {
		t.Fatalf("handle raw QUIC query: %v", err)
	}
	if got := <-queryReceived; !bytes.Equal(got, queryBody) {
		t.Fatalf("raw query = %x, want %x", got, queryBody)
	}
	if !bytes.Equal(answer, answerBody) {
		t.Fatalf("raw answer = %x, want %x", answer, answerBody)
	}

}

func TestPrivateOverlayBroadcastCallbacksBypassTONClassifier(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	source := testPeerID("private broadcast source")
	sourceADNL := testPeerID("private broadcast source ADNL")
	immediate := testPeerID("private immediate peer")
	broadcastID := sha256.Sum256([]byte("private broadcast"))
	prechecked := 0
	delivered := 0
	handle, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:                     []byte("generic broadcast callbacks"),
		Members:                    []PeerID{node.localID, immediate},
		AuthorizedBroadcastSources: map[PeerID]uint32{source: 1024},
		EnableTwoStep:              true,
		TwoStepIntermediateMembers: []PeerID{immediate},
	}, PrivateOverlayCallbacks{
		BroadcastPrecheck: func(_ context.Context, request PrivateOverlayBroadcastPrecheck) error {
			prechecked++
			if request.Source != source || !bytes.Equal(request.SourceADNL, sourceADNL[:]) ||
				request.ImmediatePeer != immediate || request.Delivery != DeliveryTwoStep {
				t.Errorf("unexpected precheck: %+v", request)
			}
			return nil
		},
		Broadcast: func(_ context.Context, broadcast PrivateOverlayBroadcast) PrivateOverlayBroadcastDisposition {
			delivered++
			if broadcast.Source != source || broadcast.ID != broadcastID {
				t.Errorf("unexpected broadcast: %+v", broadcast)
			}
			if broadcast.Delivery == DeliveryTwoStep && !bytes.Equal(broadcast.SourceADNL, sourceADNL[:]) {
				t.Errorf("two-step source ADNL = %x, want %x", broadcast.SourceADNL, sourceADNL)
			}
			if broadcast.Delivery != DeliveryTwoStep && broadcast.SourceADNL != nil {
				t.Errorf("non-two-step source ADNL = %x, want nil", broadcast.SourceADNL)
			}
			return PrivateOverlayBroadcastAcceptAndRelay
		},
	})
	if err != nil {
		t.Fatalf("open private overlay: %v", err)
	}
	defer handle.Close()

	precheckInfo := overlay.BroadcastPrecheckInfo{
		SourceID:         source[:],
		SourceADNL:       sourceADNL[:],
		ImmediatePeerID:  immediate[:],
		BroadcastID:      broadcastID[:],
		Delivery:         overlay.BroadcastDeliveryTwoStepSimple,
		SignatureChecked: true,
	}
	if err = handle.sub.precheckPrivateOverlayBroadcast(precheckInfo); err != nil {
		t.Fatalf("precheck two-step broadcast: %v", err)
	}
	disposition := handle.sub.handlePrivateOverlayBroadcast(tl.Raw{1, 2, 3, 4}, overlay.BroadcastInfo{
		SourceID:        source[:],
		SourceADNL:      sourceADNL[:],
		ImmediatePeerID: immediate[:],
		BroadcastID:     broadcastID[:],
		Payload:         []byte{1, 2, 3, 4},
		Delivery:        overlay.BroadcastDeliveryTwoStepSimple,
	})
	if disposition != overlay.BroadcastDispositionAcceptAndRelay {
		t.Fatalf("two-step disposition = %v, want accept-and-relay", disposition)
	}
	if prechecked != 1 || delivered != 1 {
		t.Fatalf("callback counts = precheck %d delivery %d, want 1/1", prechecked, delivered)
	}

	legacy := precheckInfo
	legacy.Delivery = overlay.BroadcastDeliverySimple
	legacy.SourceADNL = nil
	legacyRequest, err := privateOverlayBroadcastPrecheck(legacy)
	if err != nil {
		t.Fatalf("convert legacy precheck: %v", err)
	}
	if legacyRequest.SourceADNL != nil {
		t.Fatalf("legacy source ADNL = %x, want nil", legacyRequest.SourceADNL)
	}
	if err = handle.sub.precheckPrivateOverlayBroadcast(legacy); err == nil {
		t.Fatal("disabled legacy simple broadcast passed precheck")
	}
	disposition = handle.sub.handlePrivateOverlayBroadcast(tl.Raw{1, 2, 3, 4}, overlay.BroadcastInfo{
		SourceID:        source[:],
		ImmediatePeerID: immediate[:],
		BroadcastID:     broadcastID[:],
		Payload:         []byte{1, 2, 3, 4},
		Delivery:        overlay.BroadcastDeliverySimple,
	})
	if disposition != overlay.BroadcastDispositionIgnore || delivered != 1 {
		t.Fatalf("legacy disposition/count = %v/%d, want ignore/1", disposition, delivered)
	}

}

func TestPrivateOverlayQUICIngressRoutesTwoStepBroadcast(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	immediate := testPeerID("private two-step immediate peer")
	sourceKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x53}, ed25519.SeedSize))
	sourceID, err := peerIDFromED25519PublicKey(sourceKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatalf("derive broadcast source id: %v", err)
	}

	delivered := make(chan PrivateOverlayBroadcast, 1)
	handle, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:                     []byte("private two-step QUIC ingress"),
		Members:                    []PeerID{node.localID, immediate},
		AuthorizedBroadcastSources: map[PeerID]uint32{sourceID: 1024},
		UseQUIC:                    true,
		EnableTwoStep:              true,
		TwoStepIntermediateMembers: []PeerID{immediate},
	}, PrivateOverlayCallbacks{
		BroadcastPrecheck: func(_ context.Context, request PrivateOverlayBroadcastPrecheck) error {
			if request.Source != sourceID || request.ImmediatePeer != immediate ||
				!bytes.Equal(request.SourceADNL, immediate[:]) || request.Delivery != DeliveryTwoStep {
				t.Errorf("unexpected broadcast precheck: %+v", request)
			}
			return nil
		},
		Broadcast: func(_ context.Context, broadcast PrivateOverlayBroadcast) PrivateOverlayBroadcastDisposition {
			delivered <- broadcast
			return PrivateOverlayBroadcastAcceptAndRelay
		},
	})
	if err != nil {
		t.Fatalf("open private overlay: %v", err)
	}
	defer handle.Close()

	payload, err := tl.Serialize(overlay.Pong{}, true)
	if err != nil {
		t.Fatalf("serialize broadcast payload: %v", err)
	}
	extra := []byte{0xfe, 0xca, 0xde, 0x01}
	message := overlay.BroadcastTwoStepSimple{
		Date:        uint32(time.Now().Unix()),
		Source:      keys.PublicKeyED25519{Key: sourceKey.Public().(ed25519.PublicKey)},
		SourceADNL:  immediate[:],
		Certificate: overlay.CertificateEmpty{},
		Data:        payload,
		Extra:       extra,
	}
	if err = message.Sign(sourceKey); err != nil {
		t.Fatalf("sign two-step broadcast: %v", err)
	}
	boxed, err := tl.Serialize(message, true)
	if err != nil {
		t.Fatalf("serialize two-step broadcast: %v", err)
	}
	wire, err := handle.sub.quicEnvelope.Message(tl.Raw(boxed))
	if err != nil {
		t.Fatalf("wrap two-step broadcast: %v", err)
	}
	if err = node.handleQUICMessage(context.Background(), &authenticatedQUICPeer{id: immediate}, wire); err != nil {
		t.Fatalf("handle two-step private broadcast: %v", err)
	}

	select {
	case broadcast := <-delivered:
		if broadcast.Source != sourceID || !bytes.Equal(broadcast.Payload, payload) || !bytes.Equal(broadcast.Extra, extra) {
			t.Fatalf("delivered broadcast = %+v", broadcast)
		}
	case <-time.After(time.Second):
		t.Fatal("private two-step broadcast was not delivered")
	}
}

func TestPrivateOverlayTwoStepUsesPerCallSignerAndRawExtra(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	configuredKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x41}, ed25519.SeedSize))
	configuredSigner := privateOverlayTestSigner{key: configuredKey}
	handle, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:                     []byte("two-step per-call signer"),
		Members:                    []PeerID{node.localID},
		EnableTwoStep:              true,
		TwoStepIntermediateMembers: []PeerID{node.localID},
		BroadcastSigner:            configuredSigner,
	}, PrivateOverlayCallbacks{})
	if err != nil {
		t.Fatalf("open private overlay: %v", err)
	}
	defer handle.Close()

	payload := []byte("candidate wire")
	first, err := handle.BroadcastTwoStep(
		context.Background(),
		nil,
		payload,
		tl.Raw{0xef, 0xbe, 0xad, 0xde},
		0,
	)
	if err != nil {
		t.Fatalf("broadcast with configured signer: %v", err)
	}
	second, err := handle.BroadcastTwoStep(
		context.Background(),
		privateOverlayTestSigner{key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))},
		payload,
		tl.Raw{0xef, 0xbe, 0xad, 0xde},
		0,
	)
	if err != nil {
		t.Fatalf("broadcast with per-call signer: %v", err)
	}
	if bytes.Equal(first.BroadcastID, second.BroadcastID) {
		t.Fatal("per-call signer did not change the authenticated broadcast source")
	}

	third, err := handle.BroadcastTwoStep(
		context.Background(),
		configuredSigner,
		payload,
		tl.Raw{0xef, 0xbe, 0xad, 0xdf},
		0,
	)
	if err != nil {
		t.Fatalf("broadcast with different raw extra: %v", err)
	}
	if bytes.Equal(first.BroadcastID, third.BroadcastID) {
		t.Fatal("raw extra did not participate in the two-step broadcast ID")
	}
}

func TestPublicPlumtreeOriginationUsesPerCallSignerWithoutReceiveOriginalRole(t *testing.T) {
	node := newTestNode(t)
	overlayID := testPeerID("public per-call signer overlay")
	signerKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	signer := privateOverlayTestSigner{key: signerKey}
	sourceID, err := peerIDFromED25519PublicKey(signer.PublicKey())
	if err != nil {
		t.Fatalf("derive signer id: %v", err)
	}
	node.SetPlumtreePolicy(NewPlumtreePolicy([]PeerID{sourceID}))

	sub, err := node.newOverlaySubscription(overlaySpec{
		Name:    "public-per-call-signer",
		Kind:    overlayKindPublicShard,
		ShortID: overlayID[:],
	})
	if err != nil {
		t.Fatalf("create public subscription: %v", err)
	}
	t.Cleanup(sub.close)
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID[:])] = sub
	node.subscriptionsMx.Unlock()

	broadcastID := sha256.Sum256([]byte("public per-call signed broadcast"))
	if err = node.OriginatePlumtreeSimple(
		context.Background(),
		overlayID,
		signer,
		0,
		broadcastID,
		[]byte{1, 2, 3, 4},
	); err != nil {
		t.Fatalf("originate public Plumtree broadcast: %v", err)
	}

	sub.plumtree.engine.mu.Lock()
	state := sub.plumtree.engine.simpleStates[broadcastID]
	isOriginalSender := sub.plumtree.engine.isOriginalSender
	canOriginate := sub.plumtree.engine.canOriginate
	sub.plumtree.engine.mu.Unlock()
	if state == nil || state.source != sourceID {
		t.Fatalf("originated state = %+v, want source %s", state, sourceID)
	}
	if isOriginalSender {
		t.Fatal("per-call origination changed public receive-side original-sender role")
	}
	if !canOriginate {
		t.Fatal("public Plumtree engine did not expose origination capability")
	}
}

func newPrivateOverlayTestNode(t *testing.T) *Node {
	t.Helper()
	node := newTestNode(t)
	node.networkStarted.Store(true)
	t.Cleanup(func() {
		node.closeSubscriptions()
		node.wg.Wait()
	})
	return node
}

func waitPrivateOverlayClosing(t *testing.T, registry *PrivateOverlayRegistry, key string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		registry.node.subscriptionsMx.RLock()
		closing := registry.closing[key] != nil
		registry.node.subscriptionsMx.RUnlock()
		if closing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("private overlay did not enter closing state")
		}
		time.Sleep(time.Millisecond)
	}
}

type privateOverlayTestSigner struct {
	key ed25519.PrivateKey
}

func (s privateOverlayTestSigner) PublicKey() ed25519.PublicKey {
	return s.key.Public().(ed25519.PublicKey)
}

func (s privateOverlayTestSigner) Sign(payload []byte) ([]byte, error) {
	return ed25519.Sign(s.key, payload), nil
}
