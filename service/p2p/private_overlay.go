package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

var (
	ErrPrivateOverlayExists             = errors.New("private overlay already exists")
	ErrPrivateOverlayClosing            = errors.New("private overlay is closing")
	ErrPrivateOverlayClosed             = errors.New("private overlay is closed")
	ErrPrivateOverlayPeerNotFound       = errors.New("private overlay peer not found")
	ErrPrivateOverlayHandlerUnavailable = errors.New("private overlay handler is unavailable")
)

// PrivateOverlayConfig defines one fixed-membership overlay. FullID is the raw
// overlay identifier used to derive the wire short id as pub.overlay(FullID).
// Members contains ADNL ids; the node's current LocalID must be present.
type PrivateOverlayConfig struct {
	Name                            string
	FullID                          []byte
	Members                         []PeerID
	AuthorizedBroadcastSources      map[PeerID]uint32
	MaxUnauthenticatedBroadcastSize uint32
	UseQUIC                         bool
	AllowLegacyBroadcasts           bool
	EnableTwoStep                   bool
	TwoStepIntermediateMembers      []PeerID
	BroadcastSigner                 overlay.BroadcastSigner
}

// PrivateOverlayCallbacks are invoked synchronously on authenticated overlay
// ingress. Payloads held by callback arguments are borrowed for the duration
// of the callback and must be copied before retaining them. Callbacks must not
// call PrivateOverlay.Close synchronously: Close waits for active callbacks.
// Schedule Close only after the callback returns.
type PrivateOverlayCallbacks struct {
	Message           func(context.Context, PeerID, tl.Serializable)
	Query             func(context.Context, PeerID, tl.Serializable) (tl.Serializable, error)
	BroadcastPrecheck func(context.Context, PrivateOverlayBroadcastPrecheck) error
	Broadcast         func(context.Context, PrivateOverlayBroadcast) PrivateOverlayBroadcastDisposition
}

type PrivateOverlayBroadcastPrecheck struct {
	Source           PeerID
	SourceKey        ed25519.PublicKey
	SourceADNL       []byte
	ImmediatePeer    PeerID
	ID               [sha256.Size]byte
	Extra            []byte
	Delivery         Delivery
	Trusted          bool
	SignatureChecked bool
}

type PrivateOverlayBroadcast struct {
	Source        PeerID
	SourceKey     ed25519.PublicKey
	SourceADNL    []byte
	ImmediatePeer PeerID
	ID            [sha256.Size]byte
	Message       tl.Serializable
	Payload       []byte
	Extra         []byte
	Delivery      Delivery
	Trusted       bool
}

type PrivateOverlayBroadcastDisposition uint8

const (
	PrivateOverlayBroadcastIgnore PrivateOverlayBroadcastDisposition = iota
	PrivateOverlayBroadcastAcceptAndRelay
	PrivateOverlayBroadcastRetry
)

// PrivateOverlayRegistry owns dynamic private overlays for one Node. Its state
// is protected by Node.subscriptionsMx so overlay ids cannot race the shared
// subscription registry.
type PrivateOverlayRegistry struct {
	node    *Node
	handles map[string]*PrivateOverlay
	closing map[string]*overlaySubscription
	sealed  bool
}

// PrivateOverlay is the concrete lifecycle and transport handle returned by
// PrivateOverlayRegistry.Open.
type PrivateOverlay struct {
	registry *PrivateOverlayRegistry
	sub      *overlaySubscription
	id       PeerID
	signer   overlay.BroadcastSigner

	closeOnce sync.Once
	closeErr  error
}

type privateOverlayRuntime struct {
	mx        sync.Mutex
	closing   bool
	callbacks PrivateOverlayCallbacks
	wg        sync.WaitGroup
}

type nodeBroadcastSigner struct {
	key ed25519.PrivateKey
}

func (s nodeBroadcastSigner) PublicKey() ed25519.PublicKey {
	return s.key.Public().(ed25519.PublicKey)
}

func (s nodeBroadcastSigner) Sign(payload []byte) ([]byte, error) {
	return ed25519.Sign(s.key, payload), nil
}

func newPrivateOverlayRegistry(node *Node) *PrivateOverlayRegistry {
	return &PrivateOverlayRegistry{
		node:    node,
		handles: make(map[string]*PrivateOverlay),
		closing: make(map[string]*overlaySubscription),
	}
}

func (n *Node) PrivateOverlays() *PrivateOverlayRegistry {
	return n.privateOverlays
}

func (r *PrivateOverlayRegistry) LocalID() PeerID {
	return r.node.localID
}

func (r *PrivateOverlayRegistry) Open(
	cfg PrivateOverlayConfig,
	callbacks PrivateOverlayCallbacks,
) (*PrivateOverlay, error) {
	spec, signer, err := r.buildSpec(cfg)
	if err != nil {
		return nil, err
	}
	key := overlaySpecKey(spec)
	runtime := &privateOverlayRuntime{callbacks: callbacks}

	r.node.subscriptionsMx.Lock()
	if r.sealed || !r.node.networkStarted.Load() || r.node.offline.Load() || r.node.runCtx.Err() != nil {
		r.node.subscriptionsMx.Unlock()
		return nil, ErrOffline
	}
	if r.closing[key] != nil {
		r.node.subscriptionsMx.Unlock()
		return nil, ErrPrivateOverlayClosing
	}
	if r.node.subscriptions[key] != nil {
		r.node.subscriptionsMx.Unlock()
		return nil, ErrPrivateOverlayExists
	}

	sub, err := r.node.newPrivateOverlaySubscription(spec, runtime)
	if err != nil {
		r.node.subscriptionsMx.Unlock()
		return nil, err
	}
	id, err := NewPeerID(spec.ShortID)
	if err != nil {
		r.node.subscriptionsMx.Unlock()
		sub.close()
		return nil, err
	}
	handle := &PrivateOverlay{
		registry: r,
		sub:      sub,
		id:       id,
		signer:   signer,
	}
	r.node.subscriptions[key] = sub
	r.handles[key] = handle
	r.node.publishPublicBroadcastReceiversLocked()
	r.node.subscriptionsMx.Unlock()

	r.node.attachSubscriptionPeers(sub)
	r.node.startSubscription(sub)
	return handle, nil
}

func (r *PrivateOverlayRegistry) buildSpec(
	cfg PrivateOverlayConfig,
) (overlaySpec, overlay.BroadcastSigner, error) {
	if len(cfg.FullID) == 0 {
		return overlaySpec{}, nil, errors.New("private overlay full id is empty")
	}
	if len(cfg.Members) == 0 {
		return overlaySpec{}, nil, errors.New("private overlay has no members")
	}

	members := make([]PeerID, 0, len(cfg.Members))
	memberIDs := make(map[PeerID]struct{}, len(cfg.Members))
	localMember := false
	for _, id := range cfg.Members {
		if id.IsZero() {
			return overlaySpec{}, nil, errors.New("private overlay has an empty member id")
		}
		if _, exists := memberIDs[id]; exists {
			continue
		}
		memberIDs[id] = struct{}{}
		members = append(members, id)
		localMember = localMember || id == r.node.localID
	}
	if !localMember {
		return overlaySpec{}, nil, errors.New("local ADNL id is not a private overlay member")
	}

	fullID := append([]byte(nil), cfg.FullID...)
	shortID, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		return overlaySpec{}, nil, fmt.Errorf("build private overlay short id: %w", err)
	}
	authorized := make(map[string]uint32, len(cfg.AuthorizedBroadcastSources))
	for id, maxSize := range cfg.AuthorizedBroadcastSources {
		if id.IsZero() {
			return overlaySpec{}, nil, errors.New("private overlay has an empty broadcast source id")
		}
		authorized[string(id[:])] = maxSize
	}
	var twoStepIntermediateIDs map[PeerID]struct{}
	if cfg.EnableTwoStep {
		if len(cfg.TwoStepIntermediateMembers) == 0 {
			return overlaySpec{}, nil, errors.New("private overlay has no two-step intermediate members")
		}

		twoStepIntermediateIDs = make(map[PeerID]struct{}, len(cfg.TwoStepIntermediateMembers))
		for _, id := range cfg.TwoStepIntermediateMembers {
			if id.IsZero() {
				return overlaySpec{}, nil, errors.New("private overlay has an empty two-step intermediate member id")
			}
			twoStepIntermediateIDs[id] = struct{}{}
		}
	}

	signer := cfg.BroadcastSigner
	if signer == nil {
		signer = nodeBroadcastSigner{key: r.node.privKey}
	}
	if len(signer.PublicKey()) != ed25519.PublicKeySize {
		return overlaySpec{}, nil, errors.New("private overlay broadcast signer has an invalid public key")
	}

	name := cfg.Name
	if name == "" {
		name = PeerID(shortID).String()
	}
	return overlaySpec{
		Name:                            "private." + name,
		Kind:                            overlayKindPrivate,
		FullID:                          fullID,
		ShortID:                         shortID,
		FixedNodes:                      members,
		FixedNodeIDs:                    memberIDs,
		AuthorizedKeys:                  authorized,
		UseQUIC:                         cfg.UseQUIC,
		PrivateAllowLegacyBroadcasts:    cfg.AllowLegacyBroadcasts,
		PrivateTwoStep:                  cfg.EnableTwoStep,
		PrivateTwoStepIntermediateIDs:   twoStepIntermediateIDs,
		PrivateUnauthenticatedBroadcast: cfg.MaxUnauthenticatedBroadcastSize,
	}, signer, nil
}

func (r *PrivateOverlayRegistry) close(handle *PrivateOverlay) error {
	key := overlaySpecKey(handle.sub.spec)

	r.node.subscriptionsMx.Lock()
	if r.handles[key] != handle || r.node.subscriptions[key] != handle.sub {
		r.node.subscriptionsMx.Unlock()
		return nil
	}
	delete(r.handles, key)
	delete(r.node.subscriptions, key)
	r.closing[key] = handle.sub
	handle.sub.mx.Lock()
	handle.sub.removed = true
	handle.sub.inactive = true
	handle.sub.broadcastReceiver.SetActive(false)
	handle.sub.mx.Unlock()
	r.node.publishPublicBroadcastReceiversLocked()
	r.node.subscriptionsMx.Unlock()

	handle.sub.close()

	r.node.subscriptionsMx.Lock()
	if r.closing[key] == handle.sub {
		delete(r.closing, key)
	}
	r.node.subscriptionsMx.Unlock()
	return nil
}

func (o *PrivateOverlay) ID() PeerID {
	return o.id
}

// Close removes the overlay and waits for callbacks already in progress. It is
// not re-entrant from a PrivateOverlayCallbacks invocation; schedule it after
// the callback returns.
func (o *PrivateOverlay) Close() error {
	o.closeOnce.Do(func() {
		o.closeErr = o.registry.close(o)
	})
	return o.closeErr
}

func (o *PrivateOverlay) SendMessage(
	ctx context.Context,
	peerID PeerID,
	message tl.Serializable,
) error {
	peer, err := o.peer(peerID)
	if err != nil {
		return err
	}
	if !o.sub.spec.UseQUIC {
		return peer.overlay.SendCustomMessage(ctx, message)
	}

	quicPeer, err := peer.dialQUIC(ctx)
	if err != nil {
		return err
	}
	payload, err := o.sub.quicEnvelope.Message(message)
	if err != nil {
		return err
	}
	if err = quicPeer.SendOutboundMessage(ctx, payload); err != nil {
		return fmt.Errorf("send private overlay QUIC message: %w", err)
	}
	return nil
}

func (o *PrivateOverlay) SendMessageRaw(
	ctx context.Context,
	peerID PeerID,
	boxed []byte,
) error {
	return o.SendMessage(ctx, peerID, tl.Raw(boxed))
}

func (o *PrivateOverlay) SendRLDPMessage(
	ctx context.Context,
	peerID PeerID,
	message tl.Serializable,
) error {
	peer, err := o.peer(peerID)
	if err != nil {
		return err
	}
	return peer.rldpOverlay.SendCustomMessage(ctx, message)
}

func (o *PrivateOverlay) SendRLDPMessageRaw(
	ctx context.Context,
	peerID PeerID,
	boxed []byte,
) error {
	return o.SendRLDPMessage(ctx, peerID, tl.Raw(boxed))
}

func (o *PrivateOverlay) Query(
	ctx context.Context,
	peerID PeerID,
	maxAnswerSize uint64,
	request tl.Serializable,
	result tl.Serializable,
) error {
	peer, err := o.peer(peerID)
	if err != nil {
		return err
	}
	return peer.queryTransport.Query(ctx, maxAnswerSize, request, result)
}

func (o *PrivateOverlay) QueryRaw(
	ctx context.Context,
	peerID PeerID,
	maxAnswerSize uint64,
	boxed []byte,
) ([]byte, error) {
	peer, err := o.peer(peerID)
	if err != nil {
		return nil, err
	}
	return peer.queryTransport.QueryRaw(ctx, maxAnswerSize, tl.Raw(boxed))
}

func (o *PrivateOverlay) BroadcastTwoStep(
	ctx context.Context,
	signer overlay.BroadcastSigner,
	payload []byte,
	extra tl.Raw,
	flags int32,
) (overlay.BroadcastTwoStepSendResult, error) {
	if !o.sub.spec.PrivateTwoStep {
		return overlay.BroadcastTwoStepSendResult{}, errors.New("private overlay two-step broadcast is disabled")
	}
	if !o.sub.isActive() {
		return overlay.BroadcastTwoStepSendResult{}, ErrPrivateOverlayClosed
	}
	if signer == nil {
		signer = o.signer
	}

	peerSet, resolveFailed := o.sub.resolveTwoStepPeerSet(ctx, PeerID{})
	result, err := overlay.SendBroadcastTwoStep(ctx, overlay.BroadcastTwoStepSendRequest{
		Signer:      signer,
		Certificate: overlay.CertificateEmpty{},
		LocalADNLID: o.sub.node.localID.Bytes(),
		Payload:     payload,
		Extra:       []byte(extra),
		Flags:       flags,
		PeerSet:     peerSet,
	}, overlay.WithBroadcastTwoStepPeerSendTimeout(twoStepPeerSendTimeout))
	result.Attempted += len(resolveFailed)
	result.Failed = append(result.Failed, resolveFailed...)
	o.sub.markTwoStepPeerFailures(result.Failed)
	return result, err
}

func (o *PrivateOverlay) peer(id PeerID) (*overlayPeer, error) {
	if !o.sub.isActive() {
		return nil, ErrPrivateOverlayClosed
	}
	peer := o.sub.peerByID(id)
	if peer == nil {
		return nil, ErrPrivateOverlayPeerNotFound
	}
	return peer, nil
}

func (r *privateOverlayRuntime) begin() bool {
	r.mx.Lock()
	defer r.mx.Unlock()

	if r.closing {
		return false
	}
	r.wg.Add(1)
	return true
}

func (r *privateOverlayRuntime) done() {
	r.wg.Done()
}

func (r *privateOverlayRuntime) close() {
	r.mx.Lock()
	r.closing = true
	r.mx.Unlock()
	r.wg.Wait()
}

func (s *overlaySubscription) handlePrivateOverlayMessage(
	ctx context.Context,
	source PeerID,
	message tl.Serializable,
) error {
	if s.private == nil {
		return nil
	}
	if !s.isActive() || !s.private.begin() {
		return ErrPrivateOverlayClosed
	}
	defer s.private.done()

	if s.private.callbacks.Message != nil {
		s.private.callbacks.Message(ctx, source, message)
	}
	return nil
}
