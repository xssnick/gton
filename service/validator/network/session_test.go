package network

import (
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
	mu         sync.Mutex
	messages   int
	prechecks  int
	candidates int
	serves     int
	serve      func(validator.CandidateRequest) (validator.CandidateResponse, error)
}

func (r *testSessionReceiver) ReceiveConsensusMessage(simplex.PeerID, int, []byte) {
	r.mu.Lock()
	r.messages++
	r.mu.Unlock()
}

func (r *testSessionReceiver) PrecheckCandidateBroadcast(uint32, [32]byte, bool) error {
	r.mu.Lock()
	r.prechecks++
	r.mu.Unlock()
	return nil
}

func (r *testSessionReceiver) ReceiveCandidate(
	context.Context,
	[]byte,
) (validator.CandidateArtifact, error) {
	r.mu.Lock()
	r.candidates++
	r.mu.Unlock()
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
	context.Context,
	[]byte,
) (validator.CandidateArtifact, error) {
	r.mu.Lock()
	r.candidates++
	r.mu.Unlock()
	return validator.CandidateArtifact{BlockBOC: []byte{7}}, nil
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

func TestSharedSessionMultiplexesConsumersAndRetiresByRole(t *testing.T) {
	manager, opener, validatorSpec, observerSpec := testSharedManager(t)
	validatorEndpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	observerEndpoint, err := manager.prepare(context.Background(), observerSpec)
	if err != nil {
		t.Fatal(err)
	}
	if validatorEndpoint == observerEndpoint {
		t.Fatal("shared hub returned one lifecycle endpoint for both consumers")
	}
	if len(manager.sessions) != 1 || len(opener.overlays) != 2 || opener.overlays[0].closed != 1 {
		t.Fatalf("shared open state = sessions %d overlays %d first closes %d", len(manager.sessions), len(opener.overlays), opener.overlays[0].closed)
	}

	validatorReceiver := &testSessionReceiver{serve: func(validator.CandidateRequest) (validator.CandidateResponse, error) {
		return validator.CandidateResponse{}, validator.ErrCandidateUnavailable
	}}
	observerReceiver := &testSessionReceiver{}
	startTestEndpoint(t, validatorEndpoint, validatorReceiver)
	startTestEndpoint(t, observerEndpoint, observerReceiver)

	callbacks := opener.latest().callbacks
	callbacks.Message(context.Background(), p2p.PeerID{0x91}, tl.Raw{1, 2, 3, 4})
	extra, err := (&simplex.BroadcastExtra{Slot: 7}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	source := firstMapKey(validatorSpec.validatorSource)
	sourceADNL := validatorSpec.candidateADNL[source]
	if err = callbacks.BroadcastPrecheck(context.Background(), p2p.PrivateOverlayBroadcastPrecheck{
		Source: source, SourceADNL: sourceADNL[:], ID: [32]byte{3}, Extra: extra,
		Delivery: p2p.DeliveryTwoStep, SignatureChecked: true,
	}); err != nil {
		t.Fatal(err)
	}
	if disposition := callbacks.Broadcast(context.Background(), p2p.PrivateOverlayBroadcast{
		Source: source, SourceADNL: sourceADNL[:], Payload: []byte{9}, Extra: extra,
		Delivery: p2p.DeliveryTwoStep,
	}); disposition != p2p.PrivateOverlayBroadcastAcceptAndRelay {
		t.Fatalf("candidate disposition = %d", disposition)
	}

	request := validator.CandidateRequest{
		SessionID: validatorSpec.id, ID: simplex.CandidateID{Slot: 8},
		WantCandidate: true, MaximumReplyBytes: validatorSpec.maxReplyBytes,
	}
	requestWire, err := EncodeCandidateRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := callbacks.Query(context.Background(), p2p.PeerID{4}, tl.Raw(requestWire))
	if err != nil {
		t.Fatal(err)
	}
	if IsRequestError(answer.(tl.Raw)) {
		t.Fatal("observer fallback returned requestError")
	}

	assertReceiverCounts(t, validatorReceiver, 1, 1, 1, 1)
	assertReceiverCounts(t, observerReceiver, 1, 1, 1, 1)
	if err = manager.RetireValidatorSession(context.Background(), validatorSpec.id); err != nil {
		t.Fatal(err)
	}
	if len(manager.sessions) != 1 || manager.sessions[validatorSpec.id].endpoint(sessionKindObserver) == nil {
		t.Fatal("retiring validator closed the shared observer hub")
	}
	opener.latest().callbacks.Message(context.Background(), p2p.PeerID{5}, tl.Raw{1, 2, 3, 4})
	assertReceiverCounts(t, validatorReceiver, 1, 1, 1, 1)
	assertReceiverCounts(t, observerReceiver, 2, 1, 1, 1)
	if err = manager.RetireSession(context.Background(), validatorSpec.id); err != nil {
		t.Fatal(err)
	}
	if len(manager.sessions) != 0 || opener.latest().closed != 1 {
		t.Fatal("last consumer did not close and remove the shared hub")
	}
}

func TestSharedSessionUsesPerConsumerCandidateSigner(t *testing.T) {
	manager, opener, validatorSpec, observerSpec := testSharedManager(t)
	validatorEndpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	observerEndpoint, err := manager.prepare(context.Background(), observerSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, validatorEndpoint, &testSessionReceiver{})
	startTestEndpoint(t, observerEndpoint, &testSessionReceiver{})

	legacyExtra, err := (&simplex.BroadcastExtra{Slot: 1}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err = validatorEndpoint.BroadcastCandidate(context.Background(), simplex.CandidateBroadcast{
		Data: []byte{1}, Extra: legacyExtra,
	}, validator.CandidateArtifact{Candidate: simplex.Candidate{Empty: true}}); err != nil {
		t.Fatal(err)
	}
	handle := opener.latest()
	waitForCondition(t, func() bool {
		broadcasts, _ := handle.broadcastSnapshot()
		return broadcasts == 1
	})
	_, signer := handle.broadcastSnapshot()
	if signer == nil || signer.PublicKey()[0] != validatorSpec.signer.PublicKey()[0] {
		t.Fatal("validator candidate did not use the session validator signer")
	}

	collatorKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	collatorID, err := publicKeyID(collatorKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	manager.localADNLID = collatorID
	delegatedExtra, err := (&simplex.BroadcastExtra{
		Slot: 2,
		Delegation: &simplex.Delegation{
			CollatorKey: collatorKey.Public().(ed25519.PublicKey),
			Signature:   make([]byte, ed25519.SignatureSize),
		},
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err = observerEndpoint.BroadcastCandidate(context.Background(), simplex.CandidateBroadcast{
		Data: []byte{2}, Extra: delegatedExtra,
	}, validator.CandidateArtifact{Candidate: simplex.Candidate{Empty: true}}); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		broadcasts, _ := handle.broadcastSnapshot()
		return broadcasts == 2
	})
	_, signer = handle.broadcastSnapshot()
	if signer != nil {
		t.Fatal("standalone collator candidate did not select the node ADNL signer")
	}
}

func TestRemoteCollatorClientDistinguishesVerdictFromDelivery(t *testing.T) {
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, _, validatorSpec, _ := testSharedManager(t)
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
}

func TestReceivedCandidateRelayResolvesValidatorCertificateSigner(t *testing.T) {
	manager, opener, validatorSpec, observerSpec := testSharedManager(t)
	validatorEndpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	// Attaching the standalone collator consumer clears the signer in the merged
	// effective spec, which is exactly the state that used to silence the public
	// re-origination of every candidate received from another validator.
	observerEndpoint, err := manager.prepare(context.Background(), observerSpec)
	if err != nil {
		t.Fatal(err)
	}
	startTestEndpoint(t, validatorEndpoint, &testRelayingReceiver{})
	startTestEndpoint(t, observerEndpoint, &testRelayingReceiver{})

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

func TestReceivedCandidateRelayFallsBackToEffectiveSpec(t *testing.T) {
	manager, opener, _, observerSpec := testSharedManager(t)
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
	// A pure standalone-collator hub has no validator contribution, so the relay
	// keeps the effective spec and stays a plain relay without a certificate.
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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

func TestCandidatePrivateWorkersDoNotSerializeBehindOneSlowSend(t *testing.T) {
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	manager, opener, validatorSpec, _ := testSharedManager(t)
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

// hubHasHandle reads the installed overlay handle under the same lock the
// session itself uses. Production never asks this question: it compares the
// handle against a spec through handleMatches instead.
func hubHasHandle(s *session) bool {
	s.handleMu.RLock()
	defer s.handleMu.RUnlock()
	return s.handle != nil
}

func TestFailedSharedAttachRestoresExistingOverlay(t *testing.T) {
	manager, opener, validatorSpec, observerSpec := testSharedManager(t)
	validatorEndpoint, err := manager.prepare(context.Background(), validatorSpec)
	if err != nil {
		t.Fatal(err)
	}
	opener.fail(errors.New("open failed"))
	if _, err = manager.prepare(context.Background(), observerSpec); err == nil {
		t.Fatal("shared attach failure was hidden")
	}
	hub := manager.sessions[validatorSpec.id]
	if hub.endpoint(sessionKindValidator) != validatorEndpoint ||
		hub.endpoint(sessionKindObserver) != nil || !hubHasHandle(hub) {
		t.Fatal("failed attach did not restore the existing validator overlay")
	}
	if _, err = manager.prepare(context.Background(), observerSpec); err != nil {
		t.Fatalf("shared attach retry: %v", err)
	}
}

func TestFailedDetachCanRepairRemainingRoleOnRetry(t *testing.T) {
	manager, opener, validatorSpec, observerSpec := testSharedManager(t)
	if _, err := manager.prepare(context.Background(), validatorSpec); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.prepare(context.Background(), observerSpec); err != nil {
		t.Fatal(err)
	}
	opener.fail(errors.New("open failed"))
	if err := manager.RetireValidatorSession(context.Background(), validatorSpec.id); err == nil {
		t.Fatal("detach reopen failure was hidden")
	}
	hub := manager.sessions[validatorSpec.id]
	if hub.endpoint(sessionKindValidator) != nil || hub.endpoint(sessionKindObserver) == nil || hubHasHandle(hub) {
		t.Fatal("failed detach did not retain a retryable remaining role")
	}
	if err := manager.RetireValidatorSession(context.Background(), validatorSpec.id); err != nil {
		t.Fatalf("repair on idempotent retire retry: %v", err)
	}
	if !hubHasHandle(hub) {
		t.Fatal("idempotent retire retry did not repair the remaining overlay")
	}
}

// Detach must hand the surviving contribution back UNCHANGED. mergeSessionSpecs
// clears kind, role and signer, so folding a lone contribution through it would
// leave the remaining role relaying candidates without a certificate signer.
func TestDetachRestoresSurvivingContributionUnchanged(t *testing.T) {
	manager, _, validatorSpec, observerSpec := testSharedManager(t)
	if _, err := manager.prepare(context.Background(), validatorSpec); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.prepare(context.Background(), observerSpec); err != nil {
		t.Fatal(err)
	}
	hub := manager.sessions[validatorSpec.id]
	if merged := hub.currentSpec(); merged.kind != 0 || merged.signer != nil {
		t.Fatal("shared effective spec kept a role-scoped field")
	}

	if err := manager.RetireSession(context.Background(), observerSpec.id); err != nil {
		t.Fatal(err)
	}
	effective := hub.currentSpec()
	if effective.signer == nil {
		t.Fatal("detach dropped the surviving validator candidate signer")
	}
	if !effective.equal(validatorSpec) {
		t.Fatal("detach altered the surviving validator contribution")
	}
}

// A candidate fetch may only be sent to a peer which is required to hold the
// candidate. Standalone collators and persistent observers are ordinary overlay
// members, and drawing them burns a full resolve timeout on the path to voting.
func TestCandidateRequestTargetsOnlyValidatorPeers(t *testing.T) {
	manager, opener, validatorSpec, _ := testSharedManager(t)
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
	_, _, validatorSpec, _ := testSharedManager(t)
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
	manager, _, validatorSpec, _ := testSharedManager(b)
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
	manager, _, validatorSpec, _ := testSharedManager(b)
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

func testSharedManager(tb testing.TB) (*Manager, *testOverlayOpener, sessionSpec, sessionSpec) {
	tb.Helper()
	local := [32]byte{0x10}
	validatorADNL := p2p.PeerID{0x20}
	validatorSource := p2p.PeerID{0x30}
	remote := p2p.PeerID{0x40}
	signerKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	opener := &testOverlayOpener{}
	manager := &Manager{
		openOverlay: opener.open,
		broadcasts:  &testBlockPublisher{},
		localADNLID: local,
		sessions:    make(map[[32]byte]*session),
	}
	base := sessionSpec{
		id:               [32]byte{1},
		workchain:        0,
		shard:            -1 << 63,
		fullOverlayID:    []byte{1, 2, 3},
		members:          []p2p.PeerID{p2p.PeerID(local), validatorADNL, remote},
		peers:            []p2p.PeerID{validatorADNL, remote},
		validatorByADNL:  map[p2p.PeerID]int{validatorADNL: 0},
		validatorCount:   1,
		catchainSeqno:    9,
		validatorSetHash: 10,
		maxReplyBytes:    1 << 20,
		authorized:       map[p2p.PeerID]uint32{validatorSource: 1 << 20, p2p.PeerID(local): 1 << 20},
		candidateADNL:    map[p2p.PeerID]p2p.PeerID{validatorSource: validatorADNL, p2p.PeerID(local): p2p.PeerID(local)},
		validatorSource:  map[p2p.PeerID]struct{}{validatorSource: {}},
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

func TestValidatorEndpointOwnsPlumtreeProducerLease(t *testing.T) {
	manager, _, validatorSpec, _ := testSharedManager(t)
	publisher := manager.broadcasts.(*testBlockPublisher)
	hub := newSession(manager, validatorSpec)
	hub.installInitialHandle(&testPrivateOverlay{}, validatorSpec)
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

func firstMapKey(values map[p2p.PeerID]struct{}) p2p.PeerID {
	for value := range values {
		return value
	}
	return p2p.PeerID{}
}
