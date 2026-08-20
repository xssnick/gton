package network

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	adnloverlay "github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	consensusOutboundQueueSize = 256
	consensusPeerQueueSize     = 256
	// C++ ADNL get_peer_node keeps an outbound message pending for up to ten
	// seconds while peer discovery completes. The per-peer worker provides the
	// same bound without creating an unbounded goroutine per vote.
	consensusPeerSendTimeout   = 10 * time.Second
	candidateOutboundQueueSize = 8
	candidateSenderWorkerCount = 4
)

type privateOverlay interface {
	Close() error
	SendMessageRaw(context.Context, p2p.PeerID, []byte) error
	QueryRaw(context.Context, p2p.PeerID, uint64, []byte) ([]byte, error)
	BroadcastTwoStep(
		context.Context,
		adnloverlay.BroadcastSigner,
		[]byte,
		tl.Raw,
		int32,
	) (adnloverlay.BroadcastTwoStepSendResult, error)
}

type privateOverlayOpener func(
	p2p.PrivateOverlayConfig,
	p2p.PrivateOverlayCallbacks,
) (privateOverlay, error)

// session owns the physical overlays for exactly one local identity and role.
// C++ resolves validator/collator/observer precedence before creating a session;
// the Manager enforces the same boundary instead of merging role runtimes.
type session struct {
	manager *Manager
	id      [32]byte

	controlMu   sync.Mutex
	specMu      sync.RWMutex
	spec        sessionSpec
	specVersion uint64
	consumer    *sessionEndpoint

	handleMu        sync.RWMutex
	consensusHandle privateOverlay
	blockSyncHandle privateOverlay
	handleContext   context.Context
	handleCancel    context.CancelFunc
	handleSpec      sessionSpec
}

type sessionEndpoint struct {
	hub  *session
	kind sessionKind

	stateMu          sync.Mutex
	receiver         validator.SessionReceiver
	started          bool
	runStarted       bool
	running          bool
	callbacksActive  bool
	retired          bool
	retiredSignal    chan struct{}
	callbackWG       sync.WaitGroup
	runCancel        context.CancelFunc
	runDone          chan struct{}
	sendWG           sync.WaitGroup
	plumtreeProducer io.Closer

	outbound          chan outboundConsensusMessage
	candidateOutbound chan outboundCandidateMessage
	peersChanged      chan struct{}
}

// One enqueue copy separates the caller-owned consensus buffer from the
// endpoint. Peer queues share that immutable copy; transport sends must not
// mutate it.
type outboundConsensusMessage struct {
	wire           []byte
	count          uint32
	validatorsOnly bool
}

// Candidate buffers are already immutable runtime-owned encodings. Enqueueing
// transfers references, not payload copies, to the endpoint worker.
type outboundCandidateMessage struct {
	signer adnloverlay.BroadcastSigner
	data   []byte
	extra  []byte
}

type consensusPeerSender struct {
	cancel context.CancelFunc
	queue  chan []byte
}

type consensusPeerSenders struct {
	endpoint *sessionEndpoint
	ctx      context.Context
	peers    map[p2p.PeerID]consensusPeerSender
	spec     sessionSpec
	// validatorPeers is the standstill-drain fan-out target set, derived once
	// per spec generation instead of on every message.
	validatorPeers []p2p.PeerID
	version        uint64
}

type receiverBinding struct {
	receiver validator.SessionReceiver
	endpoint *sessionEndpoint
}

var _ validator.SessionNetwork = (*sessionEndpoint)(nil)

func newSession(manager *Manager, spec sessionSpec) *session {
	hub := &session{
		manager:     manager,
		id:          spec.id,
		spec:        spec,
		specVersion: 1,
	}
	hub.consumer = newSessionEndpoint(hub, spec.kind)
	return hub
}

func newSessionEndpoint(hub *session, kind sessionKind) *sessionEndpoint {
	return &sessionEndpoint{
		hub:               hub,
		kind:              kind,
		retiredSignal:     make(chan struct{}),
		outbound:          make(chan outboundConsensusMessage, consensusOutboundQueueSize),
		candidateOutbound: make(chan outboundCandidateMessage, candidateOutboundQueueSize),
		peersChanged:      make(chan struct{}, 1),
	}
}

func (s *session) consensusCallbacks(protocolVersion uint8) p2p.PrivateOverlayCallbacks {
	callbacks := p2p.PrivateOverlayCallbacks{
		Message:           s.receiveConsensusMessage,
		Query:             s.receiveQuery,
		BroadcastPrecheck: s.precheckCandidate,
		Broadcast:         s.receiveCandidate,
	}
	if protocolVersion == 1 {
		callbacks.BroadcastPrecheck = s.rejectCandidatePrecheck
		callbacks.Broadcast = s.rejectCandidate
	}

	return callbacks
}

func (s *session) blockSyncCallbacks() p2p.PrivateOverlayCallbacks {
	return p2p.PrivateOverlayCallbacks{
		BroadcastPrecheck: s.precheckCandidate,
		Broadcast:         s.receiveCandidate,
	}
}

func (s *session) installInitialHandles(
	consensus privateOverlay,
	blockSync privateOverlay,
	spec sessionSpec,
) {
	s.handleMu.Lock()
	s.consensusHandle = consensus
	s.blockSyncHandle = blockSync
	s.handleContext, s.handleCancel = newHandleLifetime()
	s.handleSpec = spec
	s.handleMu.Unlock()
}

func (s *session) endpoint(kind sessionKind) *sessionEndpoint {
	s.specMu.RLock()
	defer s.specMu.RUnlock()
	if s.consumer == nil || s.spec.kind != kind {
		return nil
	}

	return s.consumer
}

func (s *session) signalPeersChanged() {
	s.specMu.RLock()
	endpoint := s.consumer
	s.specMu.RUnlock()
	if endpoint != nil {
		endpoint.signalPeersChanged()
	}
}

func (s *session) attach(ctx context.Context, spec sessionSpec) (*sessionEndpoint, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	s.specMu.RLock()
	current := s.spec
	endpoint := s.consumer
	s.specMu.RUnlock()
	if endpoint == nil || current.kind != spec.kind || !current.equal(spec) {
		return nil, fmt.Errorf("%w: %x", ErrSessionConflict, spec.id)
	}
	if !s.handlesMatch(current) {
		if err := s.replaceHandles(ctx, current); err != nil {
			return nil, err
		}
	}

	return endpoint, nil
}

func (s *session) update(ctx context.Context, spec sessionSpec) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	s.specMu.RLock()
	current := s.spec
	if s.consumer == nil || current.kind != spec.kind {
		s.specMu.RUnlock()
		return fmt.Errorf("%w: %x", ErrSessionNotFound, spec.id)
	}
	if current.equal(spec) {
		s.specMu.RUnlock()
		if !s.handlesMatch(current) {
			return s.replaceHandles(ctx, current)
		}
		return nil
	}
	s.specMu.RUnlock()

	s.specMu.Lock()
	s.spec = spec
	s.specVersion++
	s.specMu.Unlock()
	s.signalPeersChanged()
	if s.handlesMatch(spec) {
		return nil
	}
	if err := s.replaceHandles(ctx, spec); err != nil {
		// Update is optional control-plane work. Repair the old immutable
		// snapshot so its already running consumer remains available.
		restoreErr := s.restoreSpec(current)
		return errors.Join(err, restoreErr)
	}

	return nil
}

func (s *session) detach(_ context.Context, kind sessionKind) (bool, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	s.specMu.RLock()
	endpoint := s.consumer
	currentKind := s.spec.kind
	s.specMu.RUnlock()
	if endpoint == nil || currentKind != kind {
		return endpoint == nil, nil
	}
	endpoint.retire()

	s.specMu.Lock()
	s.consumer = nil
	s.specMu.Unlock()

	return true, s.closeHandles()
}

func (s *session) retireAll() error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	s.specMu.RLock()
	endpoint := s.consumer
	s.specMu.RUnlock()
	if endpoint != nil {
		endpoint.retire()
	}

	s.specMu.Lock()
	s.consumer = nil
	s.specMu.Unlock()
	return s.closeHandles()
}

// restoreSpec republishes a previously live spec after a failed handle
// replacement. An update is optional control-plane work on a running consumer,
// so failure must leave the previous overlay open rather than no overlay at all.
func (s *session) restoreSpec(prev sessionSpec) error {
	s.specMu.Lock()
	s.spec = prev
	s.specVersion++
	s.specMu.Unlock()
	s.signalPeersChanged()

	return s.openHandles(prev)
}

func (s *session) replaceHandles(ctx context.Context, spec sessionSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.closeHandles(); err != nil {
		return fmt.Errorf("validator network: close session %x overlays: %w", s.id, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.openHandles(spec)
}

func (s *session) openHandles(spec sessionSpec) error {
	var consensus privateOverlay
	var err error
	if spec.openConsensus {
		consensus, err = s.manager.openOverlay(
			spec.consensusOverlayConfig(),
			s.consensusCallbacks(spec.protocolVersion),
		)
		if err != nil {
			return fmt.Errorf("validator network: open session %x consensus overlay: %w", s.id, err)
		}
	}

	var blockSync privateOverlay
	if spec.hasBlockSync() {
		blockSync, err = s.manager.openOverlay(spec.blockSyncOverlayConfig(), s.blockSyncCallbacks())
		if err != nil {
			var closeErr error
			if consensus != nil {
				closeErr = consensus.Close()
			}
			return errors.Join(
				fmt.Errorf("validator network: open session %x block-sync overlay: %w", s.id, err),
				closeErr,
			)
		}
	}

	s.installInitialHandles(consensus, blockSync, spec)
	return nil
}

func (s *session) ensureHandle(ctx context.Context) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	spec := s.currentSpec()
	if s.handlesMatch(spec) {
		return nil
	}
	return s.replaceHandles(ctx, spec)
}

func (s *session) handlesMatch(spec sessionSpec) bool {
	s.handleMu.RLock()
	defer s.handleMu.RUnlock()
	return (s.consensusHandle != nil) == spec.openConsensus &&
		(s.blockSyncHandle != nil) == spec.hasBlockSync() &&
		s.handleContext != nil && s.handleSpec.overlayFieldsEqual(spec)
}

func (s *session) closeHandles() error {
	s.handleMu.RLock()
	cancel := s.handleCancel
	s.handleMu.RUnlock()
	if cancel != nil {
		cancel()
	}

	s.handleMu.Lock()
	consensus := s.consensusHandle
	blockSync := s.blockSyncHandle
	s.consensusHandle = nil
	s.blockSyncHandle = nil
	s.handleContext = nil
	s.handleCancel = nil
	s.handleSpec = sessionSpec{}
	s.handleMu.Unlock()

	var blockSyncErr error
	if blockSync != nil {
		blockSyncErr = blockSync.Close()
	}
	var consensusErr error
	if consensus != nil {
		consensusErr = consensus.Close()
	}

	return errors.Join(blockSyncErr, consensusErr)
}

func (s *session) currentSpec() sessionSpec {
	s.specMu.RLock()
	defer s.specMu.RUnlock()
	return s.spec
}

func (s *session) currentSpecVersion() (sessionSpec, uint64) {
	s.specMu.RLock()
	defer s.specMu.RUnlock()

	return s.spec, s.specVersion
}

func (s *session) contribution(kind sessionKind) (sessionSpec, error) {
	s.specMu.RLock()
	defer s.specMu.RUnlock()
	if s.consumer == nil || s.spec.kind != kind {
		return sessionSpec{}, ErrSessionNotFound
	}
	return s.spec, nil
}

func (e *sessionEndpoint) Start(ctx context.Context, receiver validator.SessionReceiver) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.stateMu.Lock()
	if e.retired {
		e.stateMu.Unlock()
		return ErrManagerClosed
	}
	if e.started {
		e.stateMu.Unlock()
		return errors.New("validator network: session receiver already started")
	}
	spec, err := e.hub.contribution(e.kind)
	if err != nil {
		e.stateMu.Unlock()
		return err
	}
	var producer io.Closer
	if spec.protocolVersion >= 2 && spec.signer != nil {
		producer = e.hub.manager.broadcasts.RegisterPlumtreeProducer()
	}
	runCtx, runCancel := context.WithCancel(ctx)
	e.receiver = receiver
	e.started = true
	e.running = true
	e.runCancel = runCancel
	e.runDone = make(chan struct{})
	e.plumtreeProducer = producer

	// The transport must be ready when Start returns. Simplex recovery emits
	// startup skip votes from Engine.Start, before SessionNetwork.Run gets its
	// goroutine scheduled. C++ has the private-overlay actor attached to the bus
	// at that point; deferring these workers to Run would silently drop those
	// recovery votes and leave every restarted validator with only its own vote.
	senders := &consensusPeerSenders{
		endpoint: e,
		ctx:      runCtx,
		peers:    make(map[p2p.PeerID]consensusPeerSender),
	}
	senders.reconcile()
	if spec.canOriginateCandidate() {
		e.sendWG.Add(candidateSenderWorkerCount)
		for range candidateSenderWorkerCount {
			go e.runCandidateSender(runCtx)
		}
	}
	e.callbacksActive = true
	runDone := e.runDone
	go e.runTransport(runCtx, runCancel, runDone, senders)
	e.stateMu.Unlock()

	return nil
}

func (e *sessionEndpoint) Run(ctx context.Context) error {
	e.stateMu.Lock()
	if e.retired {
		e.stateMu.Unlock()
		return nil
	}
	if !e.started {
		e.stateMu.Unlock()
		return errors.New("validator network: session receiver is not started")
	}
	if e.runStarted {
		e.stateMu.Unlock()
		return errors.New("validator network: session network already ran")
	}
	e.runStarted = true
	runDone := e.runDone
	if !e.running {
		e.stateMu.Unlock()
		<-runDone

		return nil
	}
	runCancel := e.runCancel
	e.stateMu.Unlock()

	select {
	case <-ctx.Done():
		runCancel()
	case <-runDone:
	}
	<-runDone

	return nil
}

func (e *sessionEndpoint) runTransport(
	runCtx context.Context,
	runCancel context.CancelFunc,
	runDone chan struct{},
	senders *consensusPeerSenders,
) {
	defer func() {
		runCancel()
		e.stateMu.Lock()
		producer := e.plumtreeProducer
		e.plumtreeProducer = nil
		e.callbacksActive = false
		e.running = false
		e.runCancel = nil
		e.stateMu.Unlock()
		if producer != nil {
			_ = producer.Close()
		}
		senders.stop()
		e.sendWG.Wait()
		e.drainOutbound()
		e.callbackWG.Wait()
		close(runDone)
	}()

	for {
		select {
		case <-runCtx.Done():
			return
		case <-e.retiredSignal:
			return
		case <-e.peersChanged:
			senders.reconcile()
		case message := <-e.outbound:
			senders.dispatch(message)
		}
	}
}

func (e *sessionEndpoint) retire() {
	e.stateMu.Lock()
	if e.retired {
		e.stateMu.Unlock()
		return
	}
	e.retired = true
	e.callbacksActive = false
	runCancel := e.runCancel
	runDone := e.runDone
	close(e.retiredSignal)
	e.stateMu.Unlock()
	if runCancel != nil {
		runCancel()
	}
	if runDone != nil {
		<-runDone
	}
	e.callbackWG.Wait()
}

func (e *sessionEndpoint) beginCallback() (validator.SessionReceiver, bool) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if !e.callbacksActive || e.retired || e.receiver == nil {
		return nil, false
	}
	e.callbackWG.Add(1)
	return e.receiver, true
}

func (e *sessionEndpoint) endCallback() {
	e.callbackWG.Done()
}

func (e *sessionEndpoint) signalPeersChanged() {
	select {
	case e.peersChanged <- struct{}{}:
	default:
	}
}

func (e *sessionEndpoint) BroadcastToAll(message []byte) {
	e.enqueueConsensus(0, false, message)
}

func (e *sessionEndpoint) BroadcastToRandom(count uint32, message []byte) {
	if count != 0 {
		e.enqueueConsensus(count, false, message)
	}
}

// BroadcastToValidators sends standstill-drain traffic only to shard
// validators. Persistent observers and collators remain ordinary overlay
// members but are deliberately excluded from this protocol path.
func (e *sessionEndpoint) BroadcastToValidators(message []byte) {
	e.enqueueConsensus(0, true, message)
}

func (e *sessionEndpoint) enqueueConsensus(count uint32, validatorsOnly bool, message []byte) {
	// stateMu is the same mutex every ingress callback takes through
	// beginCallback, so the defensive copy of the caller-owned buffer is made
	// before entering that critical section. The running/retired check and the
	// non-blocking send must stay together inside it: separating them lets a
	// message land in e.outbound after drainOutbound has already run.
	outbound := outboundConsensusMessage{
		wire:           append([]byte(nil), message...),
		count:          count,
		validatorsOnly: validatorsOnly,
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if !e.running || e.retired {
		return
	}
	select {
	case e.outbound <- outbound:
	default:
		e.hub.warn("dropping consensus message because the outbound queue is full", nil)
	}
}

func (s *consensusPeerSenders) dispatch(message outboundConsensusMessage) {
	s.reconcile()
	peers := s.spec.peers
	if message.validatorsOnly {
		peers = s.validatorPeers
	}
	if message.count != 0 && uint64(message.count) < uint64(len(peers)) {
		peers = append([]p2p.PeerID(nil), peers...)
		rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
		peers = peers[:message.count]
	}
	for _, peer := range peers {
		sender, ok := s.peers[peer]
		if !ok {
			// reconcile above rebuilds the target sets and the sender map from
			// the same spec generation, so this cannot happen. Without the check
			// a mismatch would report a zero-value sender's nil queue as a full
			// queue forever.
			s.endpoint.warnPeer("consensus fan-out target has no sender", peer, nil)
			continue
		}
		select {
		case sender.queue <- message.wire:
		default:
			s.endpoint.warnPeer("dropping consensus message because the peer queue is full", peer, nil)
		}
	}
}

func (s *consensusPeerSenders) reconcile() {
	spec, version := s.endpoint.hub.currentSpecVersion()
	if version == s.version {
		return
	}
	if !spec.openConsensus {
		spec.peers = nil
	}
	s.spec = spec
	s.version = version
	// spec.peers is members minus the local ID, so filtering it by
	// validatorByADNL selects exactly the standstill-drain targets.
	validatorPeers := make([]p2p.PeerID, 0, len(spec.validatorByADNL))
	active := make(map[p2p.PeerID]struct{}, len(spec.peers))
	for _, peer := range spec.peers {
		active[peer] = struct{}{}
		if _, validatorPeer := spec.validatorByADNL[peer]; validatorPeer {
			validatorPeers = append(validatorPeers, peer)
		}
		if _, exists := s.peers[peer]; exists {
			continue
		}

		peerCtx, cancel := context.WithCancel(s.ctx)
		sender := consensusPeerSender{
			cancel: cancel,
			queue:  make(chan []byte, consensusPeerQueueSize),
		}
		s.peers[peer] = sender
		s.endpoint.sendWG.Add(1)
		go s.endpoint.runConsensusPeerSender(peerCtx, peer, sender.queue)
	}
	s.validatorPeers = validatorPeers
	for peer, sender := range s.peers {
		if _, exists := active[peer]; exists {
			continue
		}
		sender.cancel()
		delete(s.peers, peer)
	}
}

func (s *consensusPeerSenders) stop() {
	for peer, sender := range s.peers {
		sender.cancel()
		delete(s.peers, peer)
	}
}

func (e *sessionEndpoint) runConsensusPeerSender(
	ctx context.Context,
	peer p2p.PeerID,
	queue <-chan []byte,
) {
	defer e.sendWG.Done()

	var lastFailureLog time.Time
	for {
		var message []byte
		select {
		case <-ctx.Done():
			return
		case message = <-queue:
		}

		// C++ detaches every fire-and-forget quic.message and accounts a failed
		// send as a drop; it does not retry an old vote ahead of newer protocol
		// traffic. Keep one bounded sender per peer in Go, but preserve that
		// failure policy so an unavailable route cannot head-of-line block the
		// session indefinitely.
		sendCtx, cancel := context.WithTimeout(ctx, consensusPeerSendTimeout)
		err := e.hub.sendMessageRaw(sendCtx, peer, message)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil && time.Since(lastFailureLog) >= time.Second {
			e.hub.manager.log.Debug().
				Err(err).
				Hex("session_id", e.hub.id[:]).
				Hex("peer_id", peer[:]).
				Msg("send consensus message")
			lastFailureLog = time.Now()
		}
	}
}

func (e *sessionEndpoint) BroadcastCandidate(
	ctx context.Context,
	broadcast simplex.CandidateBroadcast,
	artifact validator.CandidateArtifact,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !e.active() {
		return ErrSessionInactive
	}
	spec, err := e.hub.contribution(e.kind)
	if err != nil {
		return err
	}
	if e.kind == sessionKindValidator &&
		(spec.role == collator.OverlayRoleObserver || spec.signer == nil) {
		return errors.New("validator network: persistent observer cannot originate a candidate")
	}
	extra, err := simplex.ParseBroadcastExtra(broadcast.Extra)
	if err != nil {
		return err
	}
	if e.kind == sessionKindValidator {
		if extra.Delegation != nil {
			return errors.New("validator network: validator signer cannot originate a delegated candidate")
		}
	} else {
		if spec.role != collator.OverlayRoleCollator {
			return errors.New("validator network: observer cannot originate a candidate")
		}
		if extra.Delegation == nil {
			return errors.New("validator network: standalone collator candidate has no delegation")
		}
		collatorID, err := publicKeyID(extra.Delegation.CollatorKey)
		if err != nil {
			return err
		}
		if collatorID != e.hub.manager.localADNLID {
			return errors.New("validator network: delegation is bound to another collator")
		}
	}

	// CandidateGenerated independently feeds public relay and the private
	// transport. Both retain the runtime-owned immutable buffers by reference.
	e.hub.manager.publishCandidate(spec, artifact)
	candidateSigner := spec.signer
	if spec.hasBlockSync() {
		// Protocol v1 signs block-sync broadcasts with the node ADNL key. A nil
		// per-send signer selects the private-overlay handle's node signer.
		candidateSigner = nil
	}
	if !e.enqueueCandidate(outboundCandidateMessage{
		signer: candidateSigner,
		data:   broadcast.Data,
		extra:  broadcast.Extra,
	}) {
		e.hub.warn("dropping private candidate broadcast because the outbound queue is unavailable", nil)
	}

	return nil
}

func (e *sessionEndpoint) enqueueCandidate(message outboundCandidateMessage) bool {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if !e.running || e.retired {
		return false
	}

	select {
	case e.candidateOutbound <- message:
		return true
	default:
		return false
	}
}

func (e *sessionEndpoint) runCandidateSender(ctx context.Context) {
	defer e.sendWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-e.candidateOutbound:
			if ctx.Err() != nil {
				return
			}
			if err := e.hub.broadcastTwoStep(ctx, message.signer, message.data, message.extra); err != nil && ctx.Err() == nil {
				e.hub.warn("send private candidate broadcast", err)
			}
		}
	}
}

func (e *sessionEndpoint) drainOutbound() {
	drainQueue(e.outbound)
	drainQueue(e.candidateOutbound)
}

func drainQueue[T any](queue <-chan T) {
	for {
		select {
		case <-queue:
		default:
			return
		}
	}
}

func (e *sessionEndpoint) RequestCandidate(
	ctx context.Context,
	request validator.CandidateRequest,
) (validator.CandidateResponse, error) {
	if !e.active() {
		return validator.CandidateResponse{}, ErrSessionInactive
	}
	spec := e.hub.currentSpec()
	if request.SessionID != spec.id {
		return validator.CandidateResponse{}, errors.New("validator network: candidate request belongs to another session")
	}
	wire, err := EncodeCandidateRequest(request)
	if err != nil {
		return validator.CandidateResponse{}, err
	}
	target, ok := candidateRequestTarget(spec)
	if !ok {
		return validator.CandidateResponse{}, ErrSessionNotFound
	}
	answer, err := e.hub.queryRaw(ctx, target, uint64(request.MaximumReplyBytes), wire)
	if err != nil {
		return validator.CandidateResponse{}, err
	}

	return DecodeCandidateResponse(request, answer, spec.validatorCount)
}

// candidateRequestTarget draws the peer asked for a candidate. The uniform draw
// with replacement is deliberate and matches validator-session.cpp, which
// redraws td::Random::fast over get_block_approvers on every retry. Only the
// pool differs: spec.peers is the whole overlay membership, including standalone
// collators and persistent observers which are not required to hold the
// candidate at all, so the pool is narrowed to session validators. Candidate
// resolution is on the critical path to voting, and each retry that lands on a
// peer which cannot answer costs a full resolve timeout.
//
// A session whose validator peers are all local or absent — a single-validator
// shard, or an observer overlay built from non-validator members — falls back to
// the full membership rather than failing the fetch outright.
func candidateRequestTarget(spec sessionSpec) (p2p.PeerID, bool) {
	targets := make([]p2p.PeerID, 0, len(spec.peers))
	for _, peer := range spec.peers {
		if _, validatorPeer := spec.validatorByADNL[peer]; validatorPeer {
			targets = append(targets, peer)
		}
	}
	if len(targets) == 0 {
		targets = spec.peers
	}
	if len(targets) == 0 {
		return p2p.PeerID{}, false
	}

	return targets[rand.IntN(len(targets))], true
}

func (e *sessionEndpoint) active() bool {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	return e.running && !e.retired
}

func (s *session) activeSessionBinding() (receiverBinding, bool) {
	return s.activeBinding(s.currentSpec().kind)
}

func (s *session) activeBinding(kind sessionKind) (receiverBinding, bool) {
	endpoint := s.endpoint(kind)
	if endpoint == nil {
		return receiverBinding{}, false
	}
	receiver, ok := endpoint.beginCallback()
	if !ok {
		return receiverBinding{}, false
	}
	return receiverBinding{receiver: receiver, endpoint: endpoint}, true
}

func (s *session) receiveConsensusMessage(
	_ context.Context,
	source p2p.PeerID,
	message tl.Serializable,
) {
	binding, ok := s.activeSessionBinding()
	if !ok {
		return
	}
	defer binding.endpoint.endCallback()
	wire, err := serializedTL(message)
	if err != nil {
		s.warn("decode consensus message", err)
		return
	}
	spec := s.currentSpec()
	sourceValidator := simplex.ObserverIndex
	if index, ok := spec.validatorByADNL[source]; ok {
		sourceValidator = index
	}
	binding.receiver.ReceiveConsensusMessage(simplex.PeerID(source), sourceValidator, wire)
}

func (s *session) receiveQuery(
	ctx context.Context,
	source p2p.PeerID,
	message tl.Serializable,
) (tl.Serializable, error) {
	wire, err := serializedTL(message)
	if err != nil {
		return requestErrorResponse(), nil
	}
	if IsCandidateRequest(wire) {
		return s.serveCandidate(ctx, source, wire), nil
	}
	if IsPleaseCollatePrepare(wire) {
		return s.servePleaseCollatePrepare(ctx, source, wire), nil
	}
	if IsPleaseCollate(wire) {
		return s.servePleaseCollate(ctx, source, wire), nil
	}
	return requestErrorResponse(), nil
}

func (s *session) serveCandidate(ctx context.Context, source p2p.PeerID, wire []byte) tl.Serializable {
	binding, ok := s.activeSessionBinding()
	if !ok {
		return requestErrorResponse()
	}
	defer binding.endpoint.endCallback()
	spec := s.currentSpec()
	request, err := DecodeCandidateRequest(wire, spec.id, spec.maxReplyBytes)
	if err != nil {
		return requestErrorResponse()
	}
	response, err := binding.receiver.ServeCandidate(ctx, simplex.PeerID(source), request)
	if err != nil && !errors.Is(err, validator.ErrCandidateUnavailable) {
		return requestErrorResponse()
	}
	answer, err := EncodeCandidateResponse(request, response)
	if err != nil {
		return requestErrorResponse()
	}
	return tl.Raw(answer)
}

func (s *session) servePleaseCollatePrepare(
	ctx context.Context,
	source p2p.PeerID,
	wire []byte,
) tl.Serializable {
	binding, ok := s.activeBinding(sessionKindObserver)
	if !ok {
		return requestErrorResponse()
	}
	defer binding.endpoint.endCallback()
	spec, err := s.contribution(sessionKindObserver)
	if err != nil || spec.role != collator.OverlayRoleCollator {
		return requestErrorResponse()
	}
	request, err := DecodePleaseCollatePrepare(wire)
	if err != nil {
		return requestErrorResponse()
	}
	handlers, lifetime, ok := s.manager.delegatedHandlers()
	if !ok {
		return requestErrorResponse()
	}
	handlerCtx, cancel := contextWithPeerLifetime(ctx, lifetime)
	err = handlers.Probe(handlerCtx, collator.AuthenticatedQuery{
		SessionID: s.id, SourceADNL: [32]byte(source),
	}, request)
	cancel()
	if err != nil {
		return requestErrorResponse()
	}
	return p2p.Success{}
}

func (s *session) servePleaseCollate(
	ctx context.Context,
	source p2p.PeerID,
	wire []byte,
) tl.Serializable {
	binding, ok := s.activeBinding(sessionKindObserver)
	if !ok {
		return requestErrorResponse()
	}
	defer binding.endpoint.endCallback()
	spec, err := s.contribution(sessionKindObserver)
	if err != nil || spec.role != collator.OverlayRoleCollator {
		return requestErrorResponse()
	}
	request, err := DecodePleaseCollate(wire)
	if err != nil {
		return requestErrorResponse()
	}
	handlers, lifetime, ok := s.manager.delegatedHandlers()
	if !ok {
		return requestErrorResponse()
	}
	handlerCtx, cancel := contextWithPeerLifetime(ctx, lifetime)
	err = handlers.Commit(handlerCtx, collator.AuthenticatedQuery{
		SessionID: s.id, SourceADNL: [32]byte(source),
	}, request)
	cancel()
	if err != nil {
		return requestErrorResponse()
	}
	return p2p.Success{}
}

func (s *session) precheckCandidate(
	_ context.Context,
	request p2p.PrivateOverlayBroadcastPrecheck,
) error {
	if request.Delivery != p2p.DeliveryTwoStep {
		return ErrUnsupportedBroadcastMode
	}
	extra, err := s.validateCandidateSource(
		request.Source,
		request.SourceADNL,
		request.Extra,
		request.SignatureChecked,
	)
	if err != nil {
		return err
	}
	binding, ok := s.activeSessionBinding()
	if !ok {
		return ErrSessionInactive
	}
	defer binding.endpoint.endCallback()
	return binding.receiver.PrecheckCandidateBroadcast(
		extra.Slot,
		request.ID,
		request.SignatureChecked,
	)
}

func (s *session) rejectCandidatePrecheck(
	context.Context,
	p2p.PrivateOverlayBroadcastPrecheck,
) error {
	return ErrUnsupportedBroadcastMode
}

func (s *session) rejectCandidate(
	context.Context,
	p2p.PrivateOverlayBroadcast,
) p2p.PrivateOverlayBroadcastDisposition {
	return p2p.PrivateOverlayBroadcastIgnore
}

func (s *session) receiveCandidate(
	ctx context.Context,
	broadcast p2p.PrivateOverlayBroadcast,
) p2p.PrivateOverlayBroadcastDisposition {
	if broadcast.Delivery != p2p.DeliveryTwoStep {
		return p2p.PrivateOverlayBroadcastIgnore
	}
	extra, err := s.validateCandidateSource(
		broadcast.Source,
		broadcast.SourceADNL,
		broadcast.Extra,
		true,
	)
	if err != nil {
		return p2p.PrivateOverlayBroadcastIgnore
	}
	binding, ok := s.activeSessionBinding()
	if !ok {
		return p2p.PrivateOverlayBroadcastRetry
	}
	defer binding.endpoint.endCallback()
	wire, err := (simplex.CandidateBroadcast{
		Data: broadcast.Payload, Extra: broadcast.Extra,
	}).CandidateWire(extra.Delegation)
	if err != nil {
		return p2p.PrivateOverlayBroadcastIgnore
	}
	artifact, err := binding.receiver.ReceiveCandidate(ctx, extra.Slot, wire)
	if err != nil {
		return p2p.PrivateOverlayBroadcastIgnore
	}
	spec := s.currentSpec()
	if spec.protocolVersion == 1 {
		s.manager.cacheCandidate(spec, artifact)
	}
	if spec.protocolVersion < 2 {
		return p2p.PrivateOverlayBroadcastAcceptAndRelay
	}

	s.manager.publishCandidate(spec, artifact)
	return p2p.PrivateOverlayBroadcastAcceptAndRelay
}

func (s *session) validateCandidateSource(
	source p2p.PeerID,
	sourceADNL []byte,
	extraWire []byte,
	signatureChecked bool,
) (*simplex.BroadcastExtra, error) {
	if len(sourceADNL) != len(p2p.PeerID{}) {
		return nil, errors.New("validator network: candidate source ADNL has invalid size")
	}
	spec := s.currentSpec()
	expected, authorized := spec.candidateADNL[source]
	if !authorized || !bytes.Equal(expected[:], sourceADNL) {
		return nil, errors.New("validator network: candidate source ADNL does not match signer")
	}
	extra, err := simplex.ParseBroadcastExtra(extraWire)
	if err != nil {
		return nil, err
	}
	if spec.slotsPerLeaderWindow == 0 || spec.validatorCount == 0 ||
		len(spec.validatorKeys) != spec.validatorCount {
		return nil, errors.New("validator network: candidate leader schedule is invalid")
	}
	expectedLeader := int(
		extra.Slot / spec.slotsPerLeaderWindow % uint32(spec.validatorCount),
	)
	if extra.Delegation == nil {
		validatorIndex, validatorSource := spec.validatorSource[source]
		if !validatorSource {
			return nil, errors.New("validator network: non-delegated candidate has a collator source")
		}
		if validatorIndex != expectedLeader {
			return nil, errors.New("validator network: candidate source is not the scheduled leader")
		}
		return extra, nil
	}
	collatorID, err := publicKeyID(extra.Delegation.CollatorKey)
	if err != nil {
		return nil, err
	}
	if p2p.PeerID(collatorID) != source {
		return nil, errors.New("validator network: delegated candidate signer differs from delegation")
	}
	if signatureChecked {
		windowStart := extra.Slot - extra.Slot%spec.slotsPerLeaderWindow
		leaderKey := spec.validatorKeys[expectedLeader]
		if !simplex.VerifyDelegationSignature(
			ed25519.PublicKey(leaderKey[:]),
			spec.id,
			windowStart,
			collatorID,
			extra.Delegation.Signature,
		) {
			return nil, errors.New("validator network: delegation signature is not valid")
		}
	}
	return extra, nil
}

func (s *session) sendMessageRaw(ctx context.Context, peer p2p.PeerID, wire []byte) error {
	s.handleMu.RLock()
	defer s.handleMu.RUnlock()
	if s.consensusHandle == nil || s.handleContext == nil {
		return ErrSessionInactive
	}
	requestCtx, cancel := contextWithPeerLifetime(ctx, s.handleContext)
	defer cancel()
	return s.consensusHandle.SendMessageRaw(requestCtx, peer, wire)
}

func (s *session) queryRaw(
	ctx context.Context,
	peer p2p.PeerID,
	maxAnswerSize uint64,
	wire []byte,
) ([]byte, error) {
	s.handleMu.RLock()
	defer s.handleMu.RUnlock()
	if s.consensusHandle == nil || s.handleContext == nil {
		return nil, ErrSessionInactive
	}
	requestCtx, cancel := contextWithPeerLifetime(ctx, s.handleContext)
	defer cancel()
	return s.consensusHandle.QueryRaw(requestCtx, peer, maxAnswerSize, wire)
}

func (s *session) broadcastTwoStep(
	ctx context.Context,
	signer adnloverlay.BroadcastSigner,
	data []byte,
	extra []byte,
) error {
	s.handleMu.RLock()
	defer s.handleMu.RUnlock()
	if s.handleContext == nil {
		return ErrSessionInactive
	}
	handle := s.consensusHandle
	if s.handleSpec.hasBlockSync() {
		handle = s.blockSyncHandle
	}
	if handle == nil {
		return ErrSessionInactive
	}
	requestCtx, cancel := contextWithPeerLifetime(ctx, s.handleContext)
	defer cancel()
	_, err := handle.BroadcastTwoStep(requestCtx, signer, data, tl.Raw(extra), 0)
	return err
}

func newHandleLifetime() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

func contextWithPeerLifetime(ctx, peer context.Context) (context.Context, context.CancelFunc) {
	combined, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(peer, cancel)
	return combined, func() {
		stop()
		cancel()
	}
}

func serializedTL(message tl.Serializable) ([]byte, error) {
	if raw, ok := message.(tl.Raw); ok {
		return raw, nil
	}
	wire, err := tl.Serialize(message, true)
	if err != nil {
		return nil, fmt.Errorf("validator network: serialize TL value: %w", err)
	}
	return wire, nil
}

func requestErrorResponse() tl.Raw {
	return tl.Raw(EncodeRequestError())
}

func (s *session) warn(message string, err error) {
	event := s.manager.log.Warn().Hex("session_id", s.id[:])
	if err != nil {
		event = event.Err(err)
	}
	event.Msg(message)
}

func (e *sessionEndpoint) warnPeer(message string, peer p2p.PeerID, err error) {
	event := e.hub.manager.log.Warn().
		Hex("session_id", e.hub.id[:]).
		Hex("peer_id", peer[:])
	if err != nil {
		event = event.Err(err)
	}
	event.Msg(message)
}
