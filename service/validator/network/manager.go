package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/overlay"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
)

var (
	// ErrManagerClosed reports an operation attempted after Manager.Close.
	ErrManagerClosed = errors.New("validator network: manager closed")
	// ErrSessionNotFound reports a session which has not been prepared or was
	// already retired.
	ErrSessionNotFound = errors.New("validator network: session not found")
	// ErrSessionConflict reports incompatible contributions for a shared
	// session ID. Validator and observer roles may coexist when their immutable
	// consensus and candidate-authorization inputs agree.
	ErrSessionConflict = errors.New("validator network: conflicting session descriptor")
	// ErrSessionInactive reports ingress or an operation outside SessionNetwork.Run.
	ErrSessionInactive = errors.New("validator network: session is inactive")
	// ErrLocalADNLUnavailable reports a session which does not contain the one
	// ADNL identity configured for this node.
	ErrLocalADNLUnavailable = errors.New("validator network: local ADNL identity is unavailable")
	// ErrUnsupportedBroadcastMode rejects an inbound candidate which did not
	// arrive over the one delivery Delegated-v3 defines for candidates:
	// private-overlay two-step FEC.
	ErrUnsupportedBroadcastMode = errors.New("validator network: candidate broadcast mode is unsupported")
)

// ManagerOptions binds the validator protocol endpoint to the node's one ADNL
// identity and its dynamic private-overlay registry.
type ManagerOptions struct {
	PrivateOverlays *p2p.PrivateOverlayRegistry
	BlockBroadcasts *p2p.BlockBroadcasts
	Logger          zerolog.Logger
}

// Manager owns validator and standalone-collator private overlays. A session
// handle is fixed-membership; UpdateSession closes and reopens it atomically
// from the Manager's point of view.
type Manager struct {
	openOverlay privateOverlayOpener
	broadcasts  blockBroadcastPublisher
	localADNLID [32]byte
	log         zerolog.Logger

	operationMu     sync.Mutex
	mu              sync.RWMutex
	sessions        map[[32]byte]*session
	remoteHandlers  collator.RemoteHandlers
	remoteContext   context.Context
	observerStarted bool
	closed          bool
}

var _ validator.ConsensusObserverNetwork = (*Manager)(nil)
var _ validator.BlockPublisher = (*Manager)(nil)

// NewManager validates the single configured ADNL identity before any overlay
// is opened.
func NewManager(options ManagerOptions) (*Manager, error) {
	if options.PrivateOverlays == nil {
		return nil, errors.New("validator network: private overlay registry is required")
	}
	if options.BlockBroadcasts == nil {
		return nil, errors.New("validator network: block broadcast capability is required")
	}
	localADNLID := [32]byte(options.PrivateOverlays.LocalID())
	if localADNLID == ([32]byte{}) {
		return nil, errors.New("validator network: local ADNL ID is zero")
	}

	return &Manager{
		openOverlay: func(
			config p2p.PrivateOverlayConfig,
			callbacks p2p.PrivateOverlayCallbacks,
		) (privateOverlay, error) {
			return options.PrivateOverlays.Open(config, callbacks)
		},
		broadcasts:  options.BlockBroadcasts,
		localADNLID: localADNLID,
		log:         options.Logger,
		sessions:    make(map[[32]byte]*session),
	}, nil
}

// LocalADNLID returns the one configured node identity shared by ordinary
// validator sessions and standalone collation.
func (m *Manager) LocalADNLID() [32]byte {
	return m.localADNLID
}

// Start installs the process-wide Delegated-v3 server handlers. Session
// overlays become active when their SessionNetwork.Start completes.
func (m *Manager) Start(ctx context.Context, handlers collator.RemoteHandlers) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if handlers.Probe == nil || handlers.Commit == nil {
		return errors.New("validator network: remote collator handlers are required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrManagerClosed
	}
	if m.observerStarted {
		return nil
	}

	m.remoteHandlers = handlers
	m.remoteContext = ctx
	m.observerStarted = true
	return nil
}

// PrepareValidatorSession opens an inactive private consensus overlay using
// the session validator key as its candidate broadcast signer.
func (m *Manager) PrepareValidatorSession(
	ctx context.Context,
	config validator.SessionConfig,
) (validator.SessionNetwork, error) {
	spec, err := m.validatorSessionSpec(config)
	if err != nil {
		return nil, err
	}

	return m.prepare(ctx, spec)
}

// PreparePersistentObserverSession opens an inactive validator-service
// endpoint for one configured persistent-overlay identity. It shares the
// physical session hub with a standalone observer endpoint, but has no voting
// or candidate-signing authority.
func (m *Manager) PreparePersistentObserverSession(
	ctx context.Context,
	config validator.SessionConfig,
) (validator.SessionNetwork, error) {
	spec, err := m.persistentObserverSessionSpec(config)
	if err != nil {
		return nil, err
	}

	return m.prepare(ctx, spec)
}

// PrepareSession opens an inactive standalone observer/collator overlay.
func (m *Manager) PrepareSession(
	ctx context.Context,
	descriptor collator.OverlaySession,
) (validator.SessionNetwork, error) {
	if err := m.requireObserverStarted(); err != nil {
		return nil, err
	}
	spec, err := m.observerSessionSpec(descriptor)
	if err != nil {
		return nil, err
	}

	return m.prepare(ctx, spec)
}

func (m *Manager) prepare(ctx context.Context, spec sessionSpec) (*sessionEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return nil, ErrManagerClosed
	}
	current := m.sessions[spec.id]
	m.mu.RUnlock()
	if current != nil {
		return current.attach(ctx, spec)
	}

	s := newSession(m, spec)
	handle, err := m.openOverlay(spec.overlayConfig(), s.callbacks())
	if err != nil {
		return nil, fmt.Errorf("validator network: open session %x overlay: %w", spec.id, err)
	}
	s.installInitialHandle(handle, spec)
	m.mu.Lock()
	m.sessions[spec.id] = s
	m.mu.Unlock()

	return s.endpoint(spec.kind), nil
}

// UpdateSession replaces the fixed private-overlay membership and policy while
// preserving the session-scoped SessionNetwork value held by the runtime.
func (m *Manager) UpdateSession(ctx context.Context, descriptor collator.OverlaySession) error {
	if err := m.requireObserverStarted(); err != nil {
		return err
	}
	spec, err := m.observerSessionSpec(descriptor)
	if err != nil {
		return err
	}

	m.mu.RLock()
	s := m.sessions[spec.id]
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return ErrManagerClosed
	}
	if s == nil {
		return fmt.Errorf("%w: %x", ErrSessionNotFound, spec.id)
	}
	if s.endpoint(sessionKindObserver) == nil {
		return fmt.Errorf("%w: validator session %x cannot be updated as an observer", ErrSessionConflict, spec.id)
	}

	return s.update(ctx, spec)
}

// RetireValidatorSession idempotently retires one validator-service endpoint,
// whether it owns a voting validator or a persistent observer runtime.
func (m *Manager) RetireValidatorSession(ctx context.Context, sessionID [32]byte) error {
	return m.retire(ctx, sessionID, sessionKindValidator)
}

// RetireSession idempotently retires one standalone observer overlay.
func (m *Manager) RetireSession(ctx context.Context, sessionID [32]byte) error {
	return m.retire(ctx, sessionID, sessionKindObserver)
}

func (m *Manager) retire(ctx context.Context, sessionID [32]byte, kind sessionKind) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	m.mu.RLock()
	s := m.sessions[sessionID]
	m.mu.RUnlock()
	if s == nil {
		return nil
	}
	if s.endpoint(kind) == nil {
		return s.ensureHandle(ctx)
	}
	empty, err := s.detach(ctx, kind)
	if empty {
		m.mu.Lock()
		if m.sessions[sessionID] == s {
			delete(m.sessions, sessionID)
		}
		m.mu.Unlock()
	}

	return err
}

// Close idempotently retires every session and disables Delegated-v3 handlers.
func (m *Manager) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.remoteHandlers = collator.RemoteHandlers{}
	m.remoteContext = nil
	sessions := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	clear(m.sessions)
	m.mu.Unlock()

	var result error
	for _, s := range sessions {
		result = errors.Join(result, s.retireAll())
	}
	return result
}

func (m *Manager) requireObserverStarted() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrManagerClosed
	}
	if !m.observerStarted || m.remoteContext == nil || m.remoteContext.Err() != nil {
		return ErrSessionInactive
	}

	return nil
}

func (m *Manager) delegatedHandlers() (collator.RemoteHandlers, context.Context, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed || !m.observerStarted || m.remoteContext == nil || m.remoteContext.Err() != nil {
		return collator.RemoteHandlers{}, nil, false
	}

	return m.remoteHandlers, m.remoteContext, true
}

func (m *Manager) session(id [32]byte) (*session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrManagerClosed
	}
	s := m.sessions[id]
	if s == nil {
		return nil, fmt.Errorf("%w: %x", ErrSessionNotFound, id)
	}

	return s, nil
}

type sessionKind uint8

const (
	sessionKindValidator sessionKind = iota + 1
	sessionKindObserver
)

// sessionKindOrder fixes the iteration order over the kinds one shared hub can
// hold. Go map iteration is randomized, and two behaviours depend on this order
// being stable: the effective spec folds contributions with the order-sensitive
// mergeSessionSpecs, and the callback bindings are consumed validator-first.
var sessionKindOrder = [...]sessionKind{sessionKindValidator, sessionKindObserver}

type sessionSpec struct {
	id               [32]byte
	kind             sessionKind
	role             collator.OverlayRole
	workchain        int32
	shard            int64
	fullOverlayID    []byte
	members          []p2p.PeerID
	peers            []p2p.PeerID
	validatorByADNL  map[p2p.PeerID]int
	validatorCount   int
	catchainSeqno    uint32
	validatorSetHash uint32
	maxReplyBytes    uint32
	authorized       map[p2p.PeerID]uint32
	candidateADNL    map[p2p.PeerID]p2p.PeerID
	validatorSource  map[p2p.PeerID]struct{}
	signer           overlay.BroadcastSigner
}

// mergeableFieldsEqual holds the immutable consensus and candidate-authorization
// inputs which two contributions to one shared session must agree on. It is the
// mergeSessionSpecs precondition, and equal is defined on top of it so a new
// sessionSpec field cannot be added to one predicate and silently omitted from
// the other.
func (s sessionSpec) mergeableFieldsEqual(other sessionSpec) bool {
	return s.id == other.id && bytes.Equal(s.fullOverlayID, other.fullOverlayID) &&
		s.workchain == other.workchain && s.shard == other.shard &&
		s.validatorCount == other.validatorCount && s.catchainSeqno == other.catchainSeqno &&
		s.validatorSetHash == other.validatorSetHash && s.maxReplyBytes == other.maxReplyBytes &&
		peerIndexMapsEqual(s.validatorByADNL, other.validatorByADNL) &&
		peerSizeMapsEqual(s.authorized, other.authorized) &&
		peerPeerMapsEqual(s.candidateADNL, other.candidateADNL) &&
		peerSetsEqual(s.validatorSource, other.validatorSource)
}

// equal is a TOTAL comparison of every sessionSpec field which is not derived
// from another one. session.handleMatches uses it against merged effective
// specs whose kind, role and signer mergeSessionSpecs deliberately zeroes, so a
// shared-fields-only comparison would accept a stale handle and leave a
// validator transport running with the wrong signer after a role change. peers
// is the one exception: it is remoteMembers(members, localADNLID) at every
// construction site, so comparing members already covers it.
func (s sessionSpec) equal(other sessionSpec) bool {
	return s.mergeableFieldsEqual(other) &&
		s.kind == other.kind && s.role == other.role &&
		slices.Equal(s.members, other.members) &&
		signerPublicKeysEqual(s.signer, other.signer)
}

func signerPublicKeysEqual(left, right overlay.BroadcastSigner) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}

	return bytes.Equal(left.PublicKey(), right.PublicKey())
}

func (s sessionSpec) overlayConfig() p2p.PrivateOverlayConfig {
	return p2p.PrivateOverlayConfig{
		Name:                            fmt.Sprintf("consensus.%x", s.id[:6]),
		FullID:                          s.fullOverlayID,
		Members:                         s.members,
		AuthorizedBroadcastSources:      s.authorized,
		MaxUnauthenticatedBroadcastSize: 0,
		UseQUIC:                         true,
		AllowLegacyBroadcasts:           false,
		EnableTwoStep:                   true,
		// The shared handle has two possible origins. Each two-step send binds
		// its signer explicitly; nil at open keeps the node ADNL signer as the
		// standalone-collator default without exposing its private key.
		BroadcastSigner: nil,
	}
}

func peerIndexMapsEqual(left, right map[p2p.PeerID]int) bool {
	if len(left) != len(right) {
		return false
	}
	for id, index := range left {
		other, ok := right[id]
		if !ok || other != index {
			return false
		}
	}
	return true
}

func peerSizeMapsEqual(left, right map[p2p.PeerID]uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for id, size := range left {
		other, ok := right[id]
		if !ok || other != size {
			return false
		}
	}
	return true
}

func peerPeerMapsEqual(left, right map[p2p.PeerID]p2p.PeerID) bool {
	if len(left) != len(right) {
		return false
	}
	for id, peer := range left {
		other, ok := right[id]
		if !ok || other != peer {
			return false
		}
	}
	return true
}

func peerSetsEqual(left, right map[p2p.PeerID]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for id := range left {
		if _, ok := right[id]; !ok {
			return false
		}
	}
	return true
}

func mergeSessionSpecs(
	left sessionSpec,
	right sessionSpec,
	localADNLID [32]byte,
) (sessionSpec, error) {
	if !left.mergeableFieldsEqual(right) {
		return sessionSpec{}, fmt.Errorf("%w: incompatible shared session %x", ErrSessionConflict, left.id)
	}

	merged := left
	merged.kind = 0
	merged.role = 0
	merged.signer = nil
	memberSet := make(map[p2p.PeerID]struct{}, len(left.members)+len(right.members))
	merged.members = make([]p2p.PeerID, 0, len(left.members)+len(right.members))
	for _, members := range [][]p2p.PeerID{left.members, right.members} {
		for _, member := range members {
			if _, exists := memberSet[member]; exists {
				continue
			}
			memberSet[member] = struct{}{}
			merged.members = append(merged.members, member)
		}
	}
	merged.peers = remoteMembers(merged.members, localADNLID)

	return merged, nil
}
