package network

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	adnloverlay "github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

type testPrivateOverlay struct {
	callbacks p2p.PrivateOverlayCallbacks

	mu              sync.Mutex
	closed          int
	queryPeers      []p2p.PeerID
	queryAnswer     []byte
	queryErr        error
	broadcastSigner adnloverlay.BroadcastSigner
	broadcastExtra  []byte
	broadcastErr    error
	broadcast       func(context.Context) error
	broadcasts      int
	send            func(context.Context, p2p.PeerID, []byte) error
}

type testCandidateTransportObserver struct {
	mu         sync.Mutex
	queueItems int
	queueAges  []time.Duration
	drops      [candidateOutboundDropReasonCount]int
	sends      []CandidateTransportSendObservation
}

func (o *testCandidateTransportObserver) AddCandidateOutboundQueue(
	_ collator.MetricChain,
	delta int,
) {
	o.mu.Lock()
	o.queueItems += delta
	o.mu.Unlock()
}

func (o *testCandidateTransportObserver) ObserveCandidateOutboundQueueAge(
	_ collator.MetricChain,
	age time.Duration,
) {
	o.mu.Lock()
	o.queueAges = append(o.queueAges, age)
	o.mu.Unlock()
}

func (o *testCandidateTransportObserver) AddCandidateOutboundDrop(
	_ collator.MetricChain,
	reason CandidateOutboundDropReason,
) {
	o.mu.Lock()
	o.drops[reason]++
	o.mu.Unlock()
}

func (o *testCandidateTransportObserver) ObserveCandidateTransportSend(
	observation CandidateTransportSendObservation,
) {
	o.mu.Lock()
	o.sends = append(o.sends, observation)
	o.mu.Unlock()
}

func (o *testCandidateTransportObserver) snapshot() (
	int,
	[]time.Duration,
	[candidateOutboundDropReasonCount]int,
	[]CandidateTransportSendObservation,
) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.queueItems,
		append([]time.Duration(nil), o.queueAges...),
		o.drops,
		append([]CandidateTransportSendObservation(nil), o.sends...)
}

func (o *testPrivateOverlay) Close() error {
	o.mu.Lock()
	o.closed++
	o.mu.Unlock()
	return nil
}

func (o *testPrivateOverlay) SendMessageRaw(ctx context.Context, peer p2p.PeerID, wire []byte) error {
	o.mu.Lock()
	send := o.send
	o.mu.Unlock()
	if send == nil {
		return nil
	}
	return send(ctx, peer, wire)
}

func (o *testPrivateOverlay) QueryRaw(
	_ context.Context,
	peer p2p.PeerID,
	_ uint64,
	_ []byte,
) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queryPeers = append(o.queryPeers, peer)
	return append([]byte(nil), o.queryAnswer...), o.queryErr
}

func (o *testPrivateOverlay) BroadcastTwoStep(
	ctx context.Context,
	signer adnloverlay.BroadcastSigner,
	_ []byte,
	extra tl.Raw,
	_ int32,
) (adnloverlay.BroadcastTwoStepSendResult, error) {
	o.mu.Lock()
	o.broadcastSigner = signer
	o.broadcastExtra = append([]byte(nil), extra...)
	o.broadcasts++
	broadcast := o.broadcast
	err := o.broadcastErr
	o.mu.Unlock()
	if broadcast != nil {
		err = broadcast(ctx)
	}
	return adnloverlay.BroadcastTwoStepSendResult{}, err
}

func (o *testPrivateOverlay) broadcastSnapshot() (int, adnloverlay.BroadcastSigner) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.broadcasts, o.broadcastSigner
}

type testOverlayOpener struct {
	mu       sync.Mutex
	overlays []*testPrivateOverlay
	configs  []p2p.PrivateOverlayConfig
	failNext error
}

func (o *testOverlayOpener) open(
	config p2p.PrivateOverlayConfig,
	callbacks p2p.PrivateOverlayCallbacks,
) (privateOverlay, error) {
	o.mu.Lock()
	if o.failNext != nil {
		err := o.failNext
		o.failNext = nil
		o.mu.Unlock()
		return nil, err
	}
	handle := &testPrivateOverlay{callbacks: callbacks}
	o.overlays = append(o.overlays, handle)
	o.configs = append(o.configs, config)
	o.mu.Unlock()
	return handle, nil
}

func (o *testOverlayOpener) fail(err error) {
	o.mu.Lock()
	o.failNext = err
	o.mu.Unlock()
}

func (o *testOverlayOpener) latest() *testPrivateOverlay {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.overlays[len(o.overlays)-1]
}

type testBlockPublisher struct {
	mu                    sync.Mutex
	accepted              []p2p.AcceptedBlockPublication
	candidates            []p2p.BlockCandidatePublication
	cachedCandidates      []p2p.TrustedBlockCandidate
	producerRegistrations int
	activeProducers       int
}

func (p *testBlockPublisher) TryPublishAccepted(publication p2p.AcceptedBlockPublication) bool {
	p.mu.Lock()
	p.accepted = append(p.accepted, publication)
	p.mu.Unlock()
	return true
}

func (p *testBlockPublisher) TryRelayCandidate(publication p2p.BlockCandidatePublication) bool {
	p.mu.Lock()
	p.candidates = append(p.candidates, publication)
	p.mu.Unlock()
	return true
}

func (p *testBlockPublisher) TryCacheCandidate(candidate p2p.TrustedBlockCandidate) bool {
	p.mu.Lock()
	p.cachedCandidates = append(p.cachedCandidates, candidate)
	p.mu.Unlock()
	return true
}

func (p *testBlockPublisher) RegisterPlumtreeProducer() io.Closer {
	p.mu.Lock()
	p.producerRegistrations++
	p.activeProducers++
	p.mu.Unlock()

	return &testProducerLease{close: func() {
		p.mu.Lock()
		p.activeProducers--
		p.mu.Unlock()
	}}
}

type testProducerLease struct {
	once  sync.Once
	close func()
}

func (l *testProducerLease) Close() error {
	l.once.Do(l.close)
	return nil
}

type testSessionReceiver struct {
	mu          sync.Mutex
	messages    int
	prechecks   int
	candidates  int
	serves      int
	precheckErr error
	receiveErr  error
	serve       func(validator.CandidateRequest) (validator.CandidateResponse, error)
	payload     []byte
	delegation  *simplex.Delegation
}

func (r *testSessionReceiver) ReceiveConsensusMessage(simplex.PeerID, int, []byte) {
	r.mu.Lock()
	r.messages++
	r.mu.Unlock()
}

func (r *testSessionReceiver) PrecheckCandidateBroadcast(uint32, [32]byte, bool) error {
	r.mu.Lock()
	r.prechecks++
	err := r.precheckErr
	r.mu.Unlock()
	return err
}

func (r *testSessionReceiver) ReceiveCandidate(
	_ context.Context,
	_ uint32,
	payload []byte,
	delegation *simplex.Delegation,
) (validator.CandidateArtifact, error) {
	r.mu.Lock()
	r.candidates++
	r.payload = payload
	r.delegation = delegation
	err := r.receiveErr
	r.mu.Unlock()
	if err != nil {
		return validator.CandidateArtifact{}, err
	}
	return validator.CandidateArtifact{Candidate: simplex.Candidate{Empty: true}}, nil
}

func (r *testSessionReceiver) ServeCandidate(
	_ context.Context,
	_ simplex.PeerID,
	request validator.CandidateRequest,
) (validator.CandidateResponse, error) {
	r.mu.Lock()
	r.serves++
	serve := r.serve
	r.mu.Unlock()
	if serve == nil {
		return validator.CandidateResponse{}, nil
	}
	return serve(request)
}

// testRelayingReceiver accepts candidates with a non-empty artifact so the
// received-candidate path actually reaches the node-owned publication queue.
type testRelayingReceiver struct {
	testSessionReceiver
}

func (r *testRelayingReceiver) ReceiveCandidate(
	_ context.Context,
	expectedSlot uint32,
	_ []byte,
	_ *simplex.Delegation,
) (validator.CandidateArtifact, error) {
	r.mu.Lock()
	r.candidates++
	r.mu.Unlock()
	return validator.CandidateArtifact{
		Candidate: simplex.Candidate{ID: simplex.CandidateID{Slot: expectedSlot}},
		BlockBOC:  []byte{7},
	}, nil
}

type testOverlaySigner struct {
	key ed25519.PrivateKey
}

func (s testOverlaySigner) PublicKey() ed25519.PublicKey {
	return s.key.Public().(ed25519.PublicKey)
}

func (s testOverlaySigner) Sign(payload []byte) ([]byte, error) {
	return ed25519.Sign(s.key, payload), nil
}

func TestSessionRejectsSecondLocalRole(t *testing.T) {
	manager, opener, validatorSpec, observerSpec := testSessionManager(t)
	validatorEndpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	standalone, err := manager.prepare(context.Background(), observerSpec)
	if !errors.Is(err, ErrSessionConflict) || standalone != nil {
		t.Fatalf("second role prepare = (%T, %v), want nil conflict", standalone, err)
	}
	if len(manager.sessions) != 1 ||
		manager.sessions[validatorSpec.id].endpoint(sessionKindValidator) != validatorEndpoint ||
		manager.sessions[validatorSpec.id].endpoint(sessionKindObserver) != nil {
		t.Fatal("second role changed the existing session")
	}
	if len(opener.overlays) != 1 || opener.overlays[0].closed != 0 {
		t.Fatalf("second role changed physical overlays: opens=%d closes=%d", len(opener.overlays), opener.overlays[0].closed)
	}
}

func TestProtocolOneRoutesConsensusAndCandidatesToSeparateOverlays(t *testing.T) {
	manager, opener, validatorSpec, _ := testProtocolV1SharedManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	if len(opener.overlays) != 2 {
		t.Fatalf("protocol 1 opened %d overlays, want consensus and block-sync", len(opener.overlays))
	}
	consensus := opener.overlays[0]
	blockSync := opener.overlays[1]
	if consensus.callbacks.Message == nil || consensus.callbacks.Query == nil ||
		consensus.callbacks.BroadcastPrecheck == nil {
		t.Fatal("consensus overlay callbacks are incomplete")
	}
	if blockSync.callbacks.Message != nil || blockSync.callbacks.Query != nil ||
		blockSync.callbacks.BroadcastPrecheck == nil {
		t.Fatal("block-sync overlay installed consensus callbacks")
	}
	receiver := &testSessionReceiver{}
	startTestEndpoint(t, endpoint, receiver)

	extra, err := (&simplex.BroadcastExtra{Slot: 0}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	source := firstMapKey(validatorSpec.validatorSource)
	sourceADNL := validatorSpec.candidateADNL[source]
	precheck := p2p.PrivateOverlayBroadcastPrecheck{
		Source: source, SourceADNL: sourceADNL[:], ID: [32]byte{1}, Extra: extra,
		Delivery: p2p.DeliveryTwoStep, SignatureChecked: true,
	}
	if err = consensus.callbacks.BroadcastPrecheck(context.Background(), precheck); !errors.Is(err, ErrUnsupportedBroadcastMode) {
		t.Fatalf("consensus candidate precheck = %v, want explicit protocol rejection", err)
	}
	if err = blockSync.callbacks.BroadcastPrecheck(context.Background(), precheck); err != nil {
		t.Fatal(err)
	}

	if err = endpoint.BroadcastCandidate(context.Background(), simplex.CandidateBroadcast{
		Data: []byte{1}, Extra: extra,
	}, validator.CandidateArtifact{Candidate: simplex.Candidate{Empty: true}}); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		broadcasts, _ := blockSync.broadcastSnapshot()
		return broadcasts == 1
	})
	if broadcasts, _ := consensus.broadcastSnapshot(); broadcasts != 0 {
		t.Fatal("protocol 1 candidate was sent on the consensus overlay")
	}
	if _, signer := blockSync.broadcastSnapshot(); signer != nil {
		t.Fatal("protocol 1 validator candidate did not select the node ADNL signer")
	}

	request := validator.CandidateRequest{
		SessionID: validatorSpec.id, ID: simplex.CandidateID{Slot: 0},
		WantCandidate: true, MaximumReplyBytes: validatorSpec.maxReplyBytes,
	}
	_, _ = endpoint.RequestCandidate(context.Background(), request)
	consensus.mu.Lock()
	consensusQueries := len(consensus.queryPeers)
	consensus.mu.Unlock()
	blockSync.mu.Lock()
	blockSyncQueries := len(blockSync.queryPeers)
	blockSync.mu.Unlock()
	if consensusQueries != 1 || blockSyncQueries != 0 {
		t.Fatalf("candidate request queries = consensus %d block-sync %d", consensusQueries, blockSyncQueries)
	}
}

func TestCandidateSourceMustMatchRoundRobinLeader(t *testing.T) {
	manager, _, spec, _ := testSessionManager(t)
	firstSource := p2p.PeerID{0x61}
	secondSource := p2p.PeerID{0x62}
	firstADNL := p2p.PeerID{0x71}
	secondADNL := p2p.PeerID{0x72}
	spec.validatorCount = 2
	spec.validatorKeys = append(spec.validatorKeys, [32]byte{0x63})
	spec.slotsPerLeaderWindow = 2
	spec.validatorSource = map[p2p.PeerID]int{firstSource: 0, secondSource: 1}
	spec.candidateADNL = map[p2p.PeerID]p2p.PeerID{firstSource: firstADNL, secondSource: secondADNL}
	hub := newSession(manager, spec)
	extra, err := (&simplex.BroadcastExtra{Slot: 2}).Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if _, err = hub.validateCandidateSource(firstSource, firstADNL[:], extra, true); err == nil {
		t.Fatal("candidate from the wrong round-robin leader was accepted")
	}
	if _, err = hub.validateCandidateSource(secondSource, secondADNL[:], extra, true); err != nil {
		t.Fatalf("candidate from the scheduled leader was rejected: %v", err)
	}
}

func TestDelegatedCandidateSourceChecksLeaderAuthorizationBeforeCommit(t *testing.T) {
	manager, _, spec, _ := testSessionManager(t)
	collatorKey := ed25519.NewKeyFromSeed(bytesOf(0xd1, ed25519.SeedSize))
	collatorPublic := collatorKey.Public().(ed25519.PublicKey)
	collatorID, err := publicKeyID(collatorPublic)
	if err != nil {
		t.Fatal(err)
	}
	source := p2p.PeerID(collatorID)
	spec.candidateADNL[source] = source
	hub := newSession(manager, spec)

	invalidExtra, err := (&simplex.BroadcastExtra{
		Slot: 0,
		Delegation: &simplex.Delegation{
			CollatorKey: collatorPublic,
			Signature:   make([]byte, ed25519.SignatureSize),
		},
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.validateCandidateSource(source, source[:], invalidExtra, false); err != nil {
		t.Fatalf("unchecked delegation was rejected before the overlay verified its signature: %v", err)
	}
	if _, err = hub.validateCandidateSource(source, source[:], invalidExtra, true); err == nil {
		t.Fatal("invalid checked delegation was accepted")
	}

	signature, err := simplex.SignDelegation(spec.signer, spec.id, 0, collatorID)
	if err != nil {
		t.Fatal(err)
	}
	validExtra, err := (&simplex.BroadcastExtra{
		Slot: 0,
		Delegation: &simplex.Delegation{
			CollatorKey: collatorPublic,
			Signature:   signature,
		},
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.validateCandidateSource(source, source[:], validExtra, true); err != nil {
		t.Fatalf("valid checked delegation was rejected: %v", err)
	}
}

func TestDelegatedCandidateDeliveryPassesBarePayloadWithoutCopy(t *testing.T) {
	manager, opener, spec, _ := testSessionManager(t)
	collatorKey := ed25519.NewKeyFromSeed(bytesOf(0xd2, ed25519.SeedSize))
	collatorPublic := collatorKey.Public().(ed25519.PublicKey)
	collatorID, err := publicKeyID(collatorPublic)
	if err != nil {
		t.Fatal(err)
	}
	source := p2p.PeerID(collatorID)
	spec.candidateADNL[source] = source
	endpoint, err := manager.prepare(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	receiver := &testSessionReceiver{}
	startTestEndpoint(t, endpoint, receiver)

	delegationSignature, err := simplex.SignDelegation(spec.signer, spec.id, 0, collatorID)
	if err != nil {
		t.Fatal(err)
	}
	extra, err := (&simplex.BroadcastExtra{
		Slot: 0,
		Delegation: &simplex.Delegation{
			CollatorKey: collatorPublic,
			Signature:   delegationSignature,
		},
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x7a}, 700<<10)
	disposition := opener.latest().callbacks.Broadcast(context.Background(), p2p.PrivateOverlayBroadcast{
		Source:     source,
		SourceADNL: source[:],
		Payload:    payload,
		Extra:      extra,
		Delivery:   p2p.DeliveryTwoStep,
	})
	if disposition != p2p.PrivateOverlayBroadcastAcceptAndRelay {
		t.Fatalf("candidate disposition = %d", disposition)
	}

	receiver.mu.Lock()
	receivedPayload := receiver.payload
	receivedDelegation := receiver.delegation
	receiver.mu.Unlock()
	if len(receivedPayload) != len(payload) || &receivedPayload[0] != &payload[0] {
		t.Fatal("candidate transport copied the bare candidate payload")
	}
	if receivedDelegation == nil ||
		!bytes.Equal(receivedDelegation.CollatorKey, collatorPublic) ||
		!bytes.Equal(receivedDelegation.Signature, delegationSignature) {
		t.Fatalf("candidate transport delegation = %+v", receivedDelegation)
	}

	// ParseBroadcastExtra owns its byte fields. The runtime may retain them
	// until lazy canonical materialization, so they must not alias the overlay
	// extra buffer even though the large payload deliberately does.
	clear(extra)
	if !bytes.Equal(receivedDelegation.CollatorKey, collatorPublic) ||
		!bytes.Equal(receivedDelegation.Signature, delegationSignature) {
		t.Fatal("candidate delegation aliases the overlay extra buffer")
	}
}

func TestRemoteCollatorClientDistinguishesVerdictFromDelivery(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	client, err := NewRemoteCollatorClient(manager, manager.localADNLID)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	query := collator.AuthenticatedQuery{SessionID: validatorSpec.id, SourceADNL: manager.localADNLID}
	handle := opener.latest()
	handle.queryAnswer = EncodeRequestError()
	err = client.Probe(context.Background(), query, simplex.ConsensusPleaseCollatePrepare{WindowStartSlot: 3})
	if !errors.Is(err, ErrRequestRejected) || errors.Is(err, collator.ErrUnavailable) {
		t.Fatalf("requestError classification = %v", err)
	}
	handle.queryAnswer = []byte{1, 2, 3}
	if err = client.Probe(context.Background(), query, simplex.ConsensusPleaseCollatePrepare{WindowStartSlot: 3}); err != nil {
		t.Fatalf("non-requestError answer = %v", err)
	}
	handle.queryErr = errors.New("delivery failed")
	err = client.Probe(context.Background(), query, simplex.ConsensusPleaseCollatePrepare{WindowStartSlot: 3})
	if !errors.Is(err, collator.ErrUnavailable) {
		t.Fatalf("delivery classification = %v", err)
	}
}

func TestCandidateRelaySurvivesPrivateBroadcastFailure(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	broadcastErr := errors.New("partial private fanout")
	opener.latest().broadcastErr = broadcastErr
	extra, err := (&simplex.BroadcastExtra{Slot: 1}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	err = endpoint.BroadcastCandidate(context.Background(), simplex.CandidateBroadcast{
		Data: []byte{1}, Extra: extra,
	}, validator.CandidateArtifact{
		Candidate: simplex.Candidate{},
		BlockBOC:  []byte{2},
	})
	if err != nil {
		t.Fatalf("private broadcast failure escaped handoff: %v", err)
	}
	waitForCondition(t, func() bool {
		broadcasts, _ := opener.latest().broadcastSnapshot()
		return broadcasts == 1
	})
	publisher := manager.broadcasts.(*testBlockPublisher)
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.candidates) != 1 {
		t.Fatalf("candidate publications = %d, want 1", len(publisher.candidates))
	}
	if !signerPublicKeysEqual(
		publisher.candidates[0].CertificateSigner,
		validatorSpec.signer,
	) {
		t.Fatal("candidate publication lost the validator certificate signer")
	}
}

func TestAcceptedBlockPublicationResolvesValidatorCertificateSigner(t *testing.T) {
	manager, _, validatorSpec, _ := testSessionManager(t)
	if _, err := manager.prepare(context.Background(), validatorSpec); err != nil {
		t.Fatal(err)
	}

	manager.PublishAcceptedBlock(validator.AcceptedBlockPublication{
		SessionID: validatorSpec.id,
	})
	publisher := manager.broadcasts.(*testBlockPublisher)
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.accepted) != 1 {
		t.Fatalf("accepted publications = %d, want 1", len(publisher.accepted))
	}
	if !signerPublicKeysEqual(
		publisher.accepted[0].CertificateSigner,
		validatorSpec.signer,
	) {
		t.Fatal("accepted publication lost the validator certificate signer")
	}
	wantFinalityMode := p2p.BlockBroadcastModePublic |
		p2p.BlockBroadcastModeFastSync |
		p2p.BlockBroadcastModeCustom
	if publisher.accepted[0].FinalityMode != wantFinalityMode ||
		publisher.accepted[0].BlockMode != p2p.BlockBroadcastModeCustom ||
		!publisher.accepted[0].Plumtree {
		t.Fatalf("protocol 3 accepted publication routes = %#v", publisher.accepted[0])
	}
}

func TestProtocolOneUsesLegacyRoutesAndCachesInboundCandidate(t *testing.T) {
	manager, opener, validatorSpec, _ := testProtocolV1SharedManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testRelayingReceiver{})
	publisher := manager.broadcasts.(*testBlockPublisher)
	publisher.mu.Lock()
	registrations := publisher.producerRegistrations
	publisher.mu.Unlock()
	if registrations != 0 {
		t.Fatal("protocol 1 endpoint registered a Plumtree producer")
	}

	manager.PublishAcceptedBlock(validator.AcceptedBlockPublication{SessionID: validatorSpec.id})
	publisher.mu.Lock()
	if len(publisher.accepted) != 1 ||
		publisher.accepted[0].BlockMode != p2p.BlockBroadcastModeCustom ||
		publisher.accepted[0].FinalityMode != 0 || publisher.accepted[0].Plumtree ||
		publisher.accepted[0].CertificateSigner != nil {
		publisher.mu.Unlock()
		t.Fatalf("protocol 1 accepted publication = %#v", publisher.accepted)
	}
	publisher.mu.Unlock()

	extra, err := (&simplex.BroadcastExtra{Slot: 0}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err = endpoint.BroadcastCandidate(context.Background(), simplex.CandidateBroadcast{
		Data: []byte{1}, Extra: extra,
	}, validator.CandidateArtifact{Candidate: simplex.Candidate{}, BlockBOC: []byte{2}}); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		publisher.mu.Lock()
		defer publisher.mu.Unlock()
		return len(publisher.candidates) == 1
	})
	publisher.mu.Lock()
	wantCandidateMode := p2p.BlockBroadcastModeCustom | p2p.BlockBroadcastModeFastSync
	if publisher.candidates[0].Mode != wantCandidateMode || publisher.candidates[0].Plumtree ||
		publisher.candidates[0].CertificateSigner != nil {
		publisher.mu.Unlock()
		t.Fatalf("protocol 1 local candidate routes = %#v", publisher.candidates[0])
	}
	publisher.mu.Unlock()

	receiveTestCandidate(t, opener.overlays[1].callbacks, validatorSpec, 0)
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.candidates) != 1 {
		t.Fatal("protocol 1 inbound candidate was re-originated on the public/custom relay")
	}
	if len(publisher.cachedCandidates) != 1 {
		t.Fatalf("protocol 1 cached candidates = %d, want 1", len(publisher.cachedCandidates))
	}
}

func TestReceivedCandidateRelayResolvesValidatorCertificateSigner(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	validatorEndpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, validatorEndpoint, &testRelayingReceiver{})

	receiveTestCandidate(t, opener.latest().callbacks, validatorSpec, 1)
	publisher := manager.broadcasts.(*testBlockPublisher)
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.candidates) != 1 {
		t.Fatalf("candidate publications = %d, want 1", len(publisher.candidates))
	}
	if !signerPublicKeysEqual(
		publisher.candidates[0].CertificateSigner,
		validatorSpec.signer,
	) {
		t.Fatal("received candidate relay lost the validator certificate signer")
	}
	if publisher.candidates[0].CatchainSeqno != validatorSpec.catchainSeqno ||
		publisher.candidates[0].ValidatorSetHash != validatorSpec.validatorSetHash {
		t.Fatal("received candidate relay lost the session catchain identity")
	}
}

func TestReceivedCandidateRelayUsesStandaloneCollatorSpec(t *testing.T) {
	manager, opener, _, observerSpec := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), observerSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testRelayingReceiver{})

	receiveTestCandidate(t, opener.latest().callbacks, observerSpec, 2)
	publisher := manager.broadcasts.(*testBlockPublisher)
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.candidates) != 1 {
		t.Fatalf("candidate publications = %d, want 1", len(publisher.candidates))
	}
	// A standalone collator relays without a validator certificate.
	if publisher.candidates[0].CertificateSigner != nil {
		t.Fatal("standalone collator hub relayed with a certificate signer")
	}
	if publisher.candidates[0].CatchainSeqno != observerSpec.catchainSeqno ||
		publisher.candidates[0].ValidatorSetHash != observerSpec.validatorSetHash {
		t.Fatal("received candidate relay lost the session catchain identity")
	}
}

func receiveTestCandidate(
	t *testing.T,
	callbacks p2p.PrivateOverlayCallbacks,
	spec sessionSpec,
	slot uint32,
) {
	t.Helper()
	extra, err := (&simplex.BroadcastExtra{Slot: slot}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	source := firstMapKey(spec.validatorSource)
	sourceADNL := spec.candidateADNL[source]
	if disposition := callbacks.Broadcast(context.Background(), p2p.PrivateOverlayBroadcast{
		Source: source, SourceADNL: sourceADNL[:], Payload: []byte{9}, Extra: extra,
		Delivery: p2p.DeliveryTwoStep,
	}); disposition != p2p.PrivateOverlayBroadcastAcceptAndRelay {
		t.Fatalf("candidate disposition = %d", disposition)
	}
}

func TestCandidateRelayDoesNotWaitForPrivateWorkerAndRetireCancels(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	handle := opener.latest()
	started := make(chan struct{}, candidateSenderWorkerCount)
	stopped := make(chan struct{}, candidateSenderWorkerCount)
	handle.mu.Lock()
	handle.broadcast = func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		stopped <- struct{}{}

		return ctx.Err()
	}
	handle.mu.Unlock()
	extra, err := (&simplex.BroadcastExtra{Slot: 1}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	broadcast := simplex.CandidateBroadcast{Data: []byte{1}, Extra: extra}
	artifact := validator.CandidateArtifact{
		Candidate: simplex.Candidate{},
		BlockBOC:  []byte{2},
	}

	handoff := make(chan error, 1)
	go func() {
		handoff <- endpoint.BroadcastCandidate(context.Background(), broadcast, artifact)
	}()
	select {
	case err = <-handoff:
		if err != nil {
			t.Fatalf("candidate handoff: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		_ = manager.RetireValidatorSession(context.Background(), validatorSpec.id)
		t.Fatal("candidate handoff waited for private delivery")
	}
	publisher := manager.broadcasts.(*testBlockPublisher)
	publisher.mu.Lock()
	publications := len(publisher.candidates)
	publisher.mu.Unlock()
	if publications != 1 {
		t.Fatalf("immediate public candidate publications = %d, want 1", publications)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("private candidate worker did not start")
	}
	for range candidateSenderWorkerCount - 1 {
		if err = endpoint.BroadcastCandidate(context.Background(), broadcast, artifact); err != nil {
			t.Fatalf("candidate worker handoff: %v", err)
		}
	}
	for range candidateSenderWorkerCount - 1 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("private candidate worker did not start")
		}
	}

	const extraCandidates = 4
	for range candidateOutboundQueueSize + extraCandidates {
		if err = endpoint.BroadcastCandidate(context.Background(), broadcast, artifact); err != nil {
			t.Fatalf("queued candidate handoff: %v", err)
		}
	}
	if got := len(endpoint.candidateOutbound); got != candidateOutboundQueueSize {
		t.Fatalf("private candidate queue depth = %d, want bounded depth %d", got, candidateOutboundQueueSize)
	}
	publisher.mu.Lock()
	publications = len(publisher.candidates)
	publisher.mu.Unlock()
	if want := candidateSenderWorkerCount + candidateOutboundQueueSize + extraCandidates; publications != want {
		t.Fatalf("public candidate publications = %d, want %d", publications, want)
	}

	if err = manager.RetireValidatorSession(context.Background(), validatorSpec.id); err != nil {
		t.Fatal(err)
	}
	for range candidateSenderWorkerCount {
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("retire did not cancel private candidate workers")
		}
	}
	if got := len(endpoint.candidateOutbound); got != 0 {
		t.Fatalf("retired private candidate queue retained %d messages", got)
	}
}

func TestCandidatePrivateSendDeadlineReleasesWorkerAndObservesQueueAge(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	observer := &testCandidateTransportObserver{}
	manager.candidateMetrics = observer
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if retireErr := manager.RetireValidatorSession(context.Background(), validatorSpec.id); retireErr != nil {
			t.Error(retireErr)
		}
	}()
	startTestEndpoint(t, endpoint, &testSessionReceiver{})

	started := make(chan struct{}, 1)
	handle := opener.latest()
	handle.mu.Lock()
	handle.broadcast = func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()

		return ctx.Err()
	}
	handle.mu.Unlock()

	extra, err := (&simplex.BroadcastExtra{Slot: 1}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err = endpoint.BroadcastCandidate(
		context.Background(),
		simplex.CandidateBroadcast{Data: []byte{1}, Extra: extra},
		validator.CandidateArtifact{BlockBOC: []byte{2}},
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("private candidate send did not start")
	}

	waitForCondition(t, func() bool {
		_, _, _, sends := observer.snapshot()
		return len(sends) == 1
	})
	queueItems, queueAges, drops, sends := observer.snapshot()
	if queueItems != 0 {
		t.Fatalf("candidate queue items = %d, want 0", queueItems)
	}
	if len(queueAges) != 1 {
		t.Fatalf("candidate queue age samples = %d, want 1", len(queueAges))
	}
	if drops[CandidateOutboundDropQueueFull] != 0 || drops[CandidateOutboundDropUnavailable] != 0 {
		t.Fatalf("candidate queue drops = %v, want none", drops)
	}
	if sends[0].Result != CandidateTransportSendDeadline {
		t.Fatalf("candidate send result = %d, want deadline", sends[0].Result)
	}
	if sends[0].Duration < candidateTransportSendTimeout/2 || sends[0].Duration > 2*candidateTransportSendTimeout {
		t.Fatalf("candidate send duration = %s, want bounded near %s", sends[0].Duration, candidateTransportSendTimeout)
	}
}

func TestCandidatePrivateWorkersDoNotSerializeBehindOneSlowSend(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	handle := opener.latest()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	laterCompleted := make(chan struct{}, 1)
	var callsMu sync.Mutex
	calls := 0
	handle.mu.Lock()
	handle.broadcast = func(ctx context.Context) error {
		callsMu.Lock()
		calls++
		call := calls
		callsMu.Unlock()
		if call == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}
		laterCompleted <- struct{}{}

		return nil
	}
	handle.mu.Unlock()
	extra, err := (&simplex.BroadcastExtra{Slot: 1}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	broadcast := simplex.CandidateBroadcast{Data: []byte{1}, Extra: extra}
	artifact := validator.CandidateArtifact{Candidate: simplex.Candidate{Empty: true}}
	if err = endpoint.BroadcastCandidate(context.Background(), broadcast, artifact); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first private candidate send did not start")
	}
	if err = endpoint.BroadcastCandidate(context.Background(), broadcast, artifact); err != nil {
		t.Fatal(err)
	}
	select {
	case <-laterCompleted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("slow private send serialized a later candidate")
	}
	close(releaseFirst)
}

func TestConsensusFanoutDoesNotSerializeSlowPeers(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	handle := opener.latest()
	delivered := make(chan p2p.PeerID, 2)
	blocked := validatorSpec.peers[0]
	handle.send = func(ctx context.Context, peer p2p.PeerID, _ []byte) error {
		if peer == blocked {
			<-ctx.Done()
			return ctx.Err()
		}
		delivered <- peer
		return nil
	}

	endpoint.BroadcastToAll([]byte{1, 2, 3, 4})
	select {
	case peer := <-delivered:
		if peer != validatorSpec.peers[1] {
			t.Fatalf("delivered peer = %x", peer)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked peer delayed an independent consensus delivery")
	}
}

func TestConsensusTransportIsReadyWhenStartReturns(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err = endpoint.Start(ctx, &testSessionReceiver{}); err != nil {
		t.Fatal(err)
	}

	delivered := make(chan p2p.PeerID, len(validatorSpec.peers))
	handle := opener.latest()
	handle.mu.Lock()
	handle.send = func(_ context.Context, peer p2p.PeerID, _ []byte) error {
		delivered <- peer

		return nil
	}
	handle.mu.Unlock()

	// Engine.Start emits crash-recovery votes before the caller enters Run.
	// They must already have a live transport path at this boundary.
	endpoint.BroadcastToAll([]byte{1, 2, 3, 4})
	for range validatorSpec.peers {
		select {
		case <-delivered:
		case <-time.After(time.Second):
			t.Fatal("consensus message emitted after Start was not delivered")
		}
	}

	runResult := make(chan error, 1)
	go func() { runResult <- endpoint.Run(ctx) }()
	cancel()
	select {
	case err = <-runResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not join transport workers")
	}
}

func TestConsensusPeerSenderDropsFailedMessageAndContinues(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})

	handle := opener.latest()
	target := validatorSpec.peers[0]
	delivered := make(chan []byte, 1)
	var attemptsMu sync.Mutex
	attempts := 0
	handle.mu.Lock()
	handle.send = func(_ context.Context, peer p2p.PeerID, wire []byte) error {
		if peer != target {
			return nil
		}
		attemptsMu.Lock()
		attempts++
		attempt := attempts
		attemptsMu.Unlock()
		if attempt == 1 {
			return errors.New("private peer is not attached yet")
		}
		delivered <- append([]byte(nil), wire...)
		return nil
	}
	handle.mu.Unlock()

	failed := []byte{1, 2, 3, 4}
	deliverable := []byte{5, 6, 7, 8}
	endpoint.BroadcastToAll(failed)
	endpoint.BroadcastToAll(deliverable)
	select {
	case got := <-delivered:
		if string(got) != string(deliverable) {
			t.Fatalf("delivered wire = %x, want newer %x", got, deliverable)
		}
	case <-time.After(time.Second):
		t.Fatal("failed consensus message blocked newer traffic")
	}
	attemptsMu.Lock()
	gotAttempts := attempts
	attemptsMu.Unlock()
	if gotAttempts != 2 {
		t.Fatalf("send attempts = %d, want one per message", gotAttempts)
	}
}

func TestConsensusPeerQueueBoundsStalledFanout(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	handle := opener.latest()
	blocked := validatorSpec.peers[0]
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	fastDelivered := make(chan struct{}, consensusPeerQueueSize+2)
	var callsMu sync.Mutex
	slowCalls := 0
	handle.mu.Lock()
	handle.send = func(_ context.Context, peer p2p.PeerID, _ []byte) error {
		if peer != blocked {
			select {
			case fastDelivered <- struct{}{}:
			default:
			}

			return nil
		}

		callsMu.Lock()
		slowCalls++
		call := slowCalls
		callsMu.Unlock()
		if call == 1 {
			close(slowStarted)
			<-releaseSlow
		}

		return nil
	}
	handle.mu.Unlock()

	endpoint.BroadcastToAll([]byte{0})
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("stalled peer worker did not start")
	}
	select {
	case <-fastDelivered:
	case <-time.After(time.Second):
		t.Fatal("stalled peer blocked an independent peer")
	}
	for i := range consensusPeerQueueSize + 10 {
		endpoint.BroadcastToAll([]byte{byte(i + 1)})
	}
	waitForCondition(t, func() bool { return len(endpoint.outbound) == 0 })
	callsMu.Lock()
	gotSlowCalls := slowCalls
	callsMu.Unlock()
	if gotSlowCalls != 1 {
		t.Fatalf("concurrent sends to stalled peer = %d, want 1", gotSlowCalls)
	}

	close(releaseSlow)
	waitForCondition(t, func() bool {
		callsMu.Lock()
		defer callsMu.Unlock()
		return slowCalls == 1+consensusPeerQueueSize
	})
}

func TestConsensusPeerWorkersStopOnRunCancellation(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err = endpoint.Start(context.Background(), &testSessionReceiver{}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- endpoint.Run(runCtx) }()
	waitForCondition(t, endpoint.active)
	started := make(chan p2p.PeerID, len(validatorSpec.peers))
	handle := opener.latest()
	handle.mu.Lock()
	handle.send = func(ctx context.Context, peer p2p.PeerID, _ []byte) error {
		started <- peer
		<-ctx.Done()

		return ctx.Err()
	}
	handle.mu.Unlock()

	endpoint.BroadcastToAll([]byte{1})
	for range validatorSpec.peers {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("peer worker did not start")
		}
	}
	cancelRun()
	select {
	case err = <-runResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("run cancellation did not join peer workers")
	}
	if err = manager.RetireValidatorSession(context.Background(), validatorSpec.id); err != nil {
		t.Fatal(err)
	}
}

func TestStandstillFanoutTargetsOnlyValidators(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	handle := opener.latest()
	delivered := make(chan p2p.PeerID, 2)
	handle.send = func(_ context.Context, peer p2p.PeerID, _ []byte) error {
		delivered <- peer
		return nil
	}

	endpoint.BroadcastToValidators([]byte{4, 3, 2, 1})
	select {
	case peer := <-delivered:
		if _, validatorPeer := validatorSpec.validatorByADNL[peer]; !validatorPeer {
			t.Fatalf("standstill delivered to non-validator %x", peer)
		}
	case <-time.After(time.Second):
		t.Fatal("standstill did not reach validator")
	}
	select {
	case peer := <-delivered:
		t.Fatalf("standstill also reached non-validator %x", peer)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRandomFanoutTargetsRequestedPeerCount(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	delivered := make(chan p2p.PeerID, len(validatorSpec.peers))
	handle := opener.latest()
	handle.mu.Lock()
	handle.send = func(_ context.Context, peer p2p.PeerID, _ []byte) error {
		delivered <- peer

		return nil
	}
	handle.mu.Unlock()

	endpoint.BroadcastToRandom(1, []byte{1, 2, 3, 4})
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("random fanout did not reach a peer")
	}
	select {
	case peer := <-delivered:
		t.Fatalf("random fanout also reached peer %x", peer)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestProtocolOneBlockSyncOpenFailureClosesNewConsensusHandle(t *testing.T) {
	manager, _, validatorSpec, _ := testProtocolV1SharedManager(t)
	consensus := &testPrivateOverlay{}
	opens := 0
	manager.openOverlay = func(
		p2p.PrivateOverlayConfig,
		p2p.PrivateOverlayCallbacks,
	) (privateOverlay, error) {
		opens++
		if opens == 2 {
			return nil, errors.New("block-sync open failed")
		}
		return consensus, nil
	}

	if _, err := manager.prepare(context.Background(), validatorSpec); err == nil {
		t.Fatal("partial protocol 1 open failure was hidden")
	}
	consensus.mu.Lock()
	closed := consensus.closed
	consensus.mu.Unlock()
	if closed != 1 || len(manager.sessions) != 0 {
		t.Fatalf("partial open cleanup = closes %d sessions %d", closed, len(manager.sessions))
	}
}

// A candidate fetch may only be sent to a peer which is required to hold the
// candidate. Standalone collators and persistent observers are ordinary overlay
// members, and drawing them burns a full resolve timeout on the path to voting.
func TestCandidateRequestTargetsOnlyValidatorPeers(t *testing.T) {
	manager, opener, validatorSpec, _ := testSessionManager(t)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, endpoint, &testSessionReceiver{})
	handle := opener.latest()

	request := validator.CandidateRequest{
		SessionID:         validatorSpec.id,
		ID:                simplex.CandidateID{Slot: 1},
		WantCandidate:     true,
		MaximumReplyBytes: validatorSpec.maxReplyBytes,
	}
	for range 32 {
		// The empty answer fails to decode; only the chosen target matters here.
		_, _ = endpoint.RequestCandidate(context.Background(), request)
	}

	handle.mu.Lock()
	defer handle.mu.Unlock()
	if len(handle.queryPeers) != 32 {
		t.Fatalf("candidate fetches reached the overlay %d times, want 32", len(handle.queryPeers))
	}
	for _, peer := range handle.queryPeers {
		if _, validatorPeer := validatorSpec.validatorByADNL[peer]; !validatorPeer {
			t.Fatalf("candidate fetch targeted non-validator member %x", peer)
		}
	}
}

// An overlay whose validator peers are all local or absent must still fetch.
func TestCandidateRequestFallsBackToFullMembership(t *testing.T) {
	_, _, validatorSpec, _ := testSessionManager(t)
	validatorSpec.validatorByADNL = map[p2p.PeerID]int{}

	target, ok := candidateRequestTarget(validatorSpec)
	if !ok {
		t.Fatal("candidate fetch has no target although the overlay has members")
	}
	if !slices.Contains(validatorSpec.peers, target) {
		t.Fatalf("candidate fetch fell back to %x, which is not an overlay member", target)
	}

	validatorSpec.peers = nil
	if _, ok = candidateRequestTarget(validatorSpec); ok {
		t.Fatal("candidate fetch drew a target from an empty overlay")
	}
}

// Standstill drain fans one message out to every shard validator, so its target
// selection runs per message on a roster-sized membership.
func BenchmarkConsensusValidatorFanoutDispatch(b *testing.B) {
	manager, _, validatorSpec, _ := testSessionManager(b)
	spec := validatorSpec
	spec.members = []p2p.PeerID{p2p.PeerID(manager.localADNLID)}
	spec.peers = nil
	spec.validatorByADNL = make(map[p2p.PeerID]int, 50)
	for i := range 100 {
		peer := p2p.PeerID{0xA0, byte(i)}
		spec.members = append(spec.members, peer)
		spec.peers = append(spec.peers, peer)
		// Half the membership is standalone collators and persistent observers,
		// which the standstill path must exclude.
		if i%2 == 0 {
			spec.validatorByADNL[peer] = i
		}
	}

	hub := newSession(manager, spec)
	endpoint := hub.endpoint(sessionKindValidator)
	senders := &consensusPeerSenders{
		endpoint: endpoint,
		ctx:      context.Background(),
		peers:    make(map[p2p.PeerID]consensusPeerSender, len(spec.peers)),
	}
	// Seed the sender map before reconcile so it adopts the spec generation
	// without starting real per-peer workers: this measures target selection and
	// hand-off, not the transport underneath it.
	for _, peer := range spec.peers {
		senders.peers[peer] = consensusPeerSender{cancel: func() {}, queue: make(chan []byte, 1)}
	}
	senders.reconcile()
	targets := make([]p2p.PeerID, 0, len(spec.validatorByADNL))
	for _, peer := range spec.peers {
		if _, validatorPeer := spec.validatorByADNL[peer]; validatorPeer {
			targets = append(targets, peer)
		}
	}
	message := outboundConsensusMessage{wire: make([]byte, 64), validatorsOnly: true}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		senders.dispatch(message)
		for _, peer := range targets {
			<-senders.peers[peer].queue
		}
	}
}

// receiveConsensusMessage is the highest-frequency callback in the package: one
// invocation per inbound vote and per gossiped certificate from every peer.
func BenchmarkReceiveConsensusMessage(b *testing.B) {
	manager, _, validatorSpec, _ := testSessionManager(b)
	endpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		b.Fatal(err)
	}
	startTestEndpoint(b, endpoint, &testSessionReceiver{})
	hub := manager.sessions[validatorSpec.id]
	source := validatorSpec.peers[0]
	// Box the payload once: the transport hands the callback an already
	// constructed tl.Serializable, so boxing it per iteration would only measure
	// the benchmark's own conversion.
	var message tl.Serializable = tl.Raw(make([]byte, 64))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		hub.receiveConsensusMessage(ctx, source, message)
	}
}

func testSessionManager(tb testing.TB) (*Manager, *testOverlayOpener, sessionSpec, sessionSpec) {
	tb.Helper()
	local := [32]byte{0x10}
	validatorADNL := p2p.PeerID{0x20}
	validatorSource := p2p.PeerID{0x30}
	remote := p2p.PeerID{0x40}
	signerKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	var validatorKey [ed25519.PublicKeySize]byte
	copy(validatorKey[:], signerKey.Public().(ed25519.PublicKey))
	opener := &testOverlayOpener{}
	manager := &Manager{
		openOverlay: opener.open,
		broadcasts:  &testBlockPublisher{},
		localADNLID: local,
		sessions:    make(map[[32]byte]*session),
	}
	authorized := map[p2p.PeerID]uint32{
		validatorSource:   1 << 20,
		p2p.PeerID(local): 1 << 20,
	}
	base := sessionSpec{
		id:                   [32]byte{1},
		protocolVersion:      3,
		useQUIC:              true,
		slotsPerLeaderWindow: 1,
		openConsensus:        true,
		workchain:            0,
		shard:                -1 << 63,
		fullOverlayID:        []byte{1, 2, 3},
		members:              []p2p.PeerID{p2p.PeerID(local), validatorADNL, remote},
		peers:                []p2p.PeerID{validatorADNL, remote},
		validatorByADNL:      map[p2p.PeerID]int{validatorADNL: 0},
		validatorKeys:        [][32]byte{validatorKey},
		validatorCount:       1,
		catchainSeqno:        9,
		validatorSetHash:     10,
		maxReplyBytes:        1 << 20,
		consensusAuthorized:  authorized,
		authorized:           authorized,
		candidateADNL:        map[p2p.PeerID]p2p.PeerID{validatorSource: validatorADNL, p2p.PeerID(local): p2p.PeerID(local)},
		validatorSource:      map[p2p.PeerID]int{validatorSource: 0},
	}
	validatorSpec := base
	validatorSpec.kind = sessionKindValidator
	validatorSpec.signer = testOverlaySigner{key: signerKey}
	observerSpec := base
	observerSpec.kind = sessionKindObserver
	observerSpec.role = collator.OverlayRoleCollator
	observerSpec.signer = nil
	return manager, opener, validatorSpec, observerSpec
}

func testProtocolV1SharedManager(tb testing.TB) (*Manager, *testOverlayOpener, sessionSpec, sessionSpec) {
	manager, opener, validatorSpec, _ := testSessionManager(tb)
	validatorADNL := firstMapKey(validatorSpec.validatorByADNL)
	localCollator := p2p.PeerID(manager.localADNLID)
	validatorSpec.protocolVersion = 1
	validatorSpec.blockSyncFullID = []byte{4, 5, 6}
	validatorSpec.blockSyncMembers = validatorSpec.members
	validatorSpec.authorized = map[p2p.PeerID]uint32{
		validatorADNL: 1 << 20,
		localCollator: 1 << 20,
	}
	validatorSpec.candidateADNL = map[p2p.PeerID]p2p.PeerID{
		validatorADNL: validatorADNL,
		localCollator: localCollator,
	}
	validatorSpec.validatorSource = map[p2p.PeerID]int{validatorADNL: 0}

	observerSpec := validatorSpec
	observerSpec.kind = sessionKindObserver
	observerSpec.role = collator.OverlayRoleCollator
	observerSpec.signer = nil

	return manager, opener, validatorSpec, observerSpec
}

func TestValidatorEndpointOwnsPlumtreeProducerLease(t *testing.T) {
	manager, _, validatorSpec, _ := testSessionManager(t)
	publisher := manager.broadcasts.(*testBlockPublisher)
	hub := newSession(manager, validatorSpec)
	hub.installInitialHandles(&testPrivateOverlay{}, nil, validatorSpec)
	endpoint := hub.endpoint(sessionKindValidator)

	if err := endpoint.Start(t.Context(), &testSessionReceiver{}); err != nil {
		t.Fatal(err)
	}
	publisher.mu.Lock()
	if publisher.activeProducers != 1 ||
		publisher.producerRegistrations != 1 {
		publisher.mu.Unlock()
		t.Fatal("validator endpoint did not register its public Plumtree producer role")
	}
	publisher.mu.Unlock()

	endpoint.retire()
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.activeProducers != 0 {
		t.Fatal("validator endpoint retained its Plumtree producer role after retirement")
	}
}

func startTestEndpoint(t testing.TB, endpoint *sessionEndpoint, receiver validator.SessionReceiver) {
	t.Helper()
	if err := endpoint.Start(context.Background(), receiver); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- endpoint.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-runResult:
			if err != nil {
				t.Errorf("run endpoint: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("endpoint workers did not stop")
		}
	})
	waitForCondition(t, endpoint.active)
}

func waitForCondition(t testing.TB, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not satisfied")
		}
		time.Sleep(time.Millisecond)
	}
}

func assertReceiverCounts(t *testing.T, receiver *testSessionReceiver, messages, prechecks, candidates, serves int) {
	t.Helper()
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	if receiver.messages != messages || receiver.prechecks != prechecks ||
		receiver.candidates != candidates || receiver.serves != serves {
		t.Fatalf("receiver counts = (%d,%d,%d,%d), want (%d,%d,%d,%d)",
			receiver.messages, receiver.prechecks, receiver.candidates, receiver.serves,
			messages, prechecks, candidates, serves,
		)
	}
}

func firstMapKey[V any](values map[p2p.PeerID]V) p2p.PeerID {
	for value := range values {
		return value
	}
	return p2p.PeerID{}
}
