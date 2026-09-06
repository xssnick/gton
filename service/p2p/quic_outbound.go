package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/peerroute"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
)

var errQUICRouteMissing = errors.New("quic peer route is missing")
var errAuthenticatedQUICPeerNotFound = errors.New("authenticated quic peer is not connected")
var errQUICDialDeferred = errors.New("QUIC dial retry is deferred")

const (
	quicAddressCacheMaxAge      = 30 * time.Second
	quicPeerDiscoveryRetryDelay = 100 * time.Millisecond
	// outboundQUICDialParallelism bounds transport setup for the background
	// route prewarm, so simultaneous overlays cannot multiply the handshake and
	// DHT load. No send path waits behind it: broadcasts go out over an
	// established connection or not at all.
	outboundQUICDialParallelism = 16
)

// errQUICPeerOffline reports that a broadcast could not be sent because the peer
// has no live outbound QUIC connection. Callers treat it as a dropped part, not
// a peer failure: a background dial is already requested, and eviction of a
// genuinely dead peer is the ping path's job.
var errQUICPeerOffline = errors.New("quic peer has no live outbound connection")

// quicRelayDialTimeout bounds the background dial (DHT resolve plus QUIC
// handshake) started when a broadcast send finds no live outbound connection.
const quicRelayDialTimeout = 10 * time.Second

// quicBroadcastSource adapts an authenticated source to BroadcastPeer. C++
// ignores FEC feedback, so sending a response is intentionally a no-op.
type quicBroadcastSource struct {
	// HandleMessage consumes the borrowed immutable ID synchronously.
	id *PeerID
}

// quicRouteBroadcastPeer is the only outbound broadcast peer: receive-side FEC
// relay, source fan-out and queued rebroadcast all send through it, so none of
// them can dial inline. C++ hands every recipient to the transport actor and
// returns (cppnode/ton/overlay/broadcast-twostep.cpp:263-265); a source that
// instead resolved and handshook a cold peer inside its own send budget spent
// that budget on transport setup and then reported the expiry as a peer fault.
// Connection setup belongs to the paths that can afford to wait for it: the
// attach-time prewarm, requestBackgroundQUICDial, and the per-peer consensus
// senders, which dial under their own budget every slot and so keep a live
// validator's path warm.
type quicRouteBroadcastPeer struct {
	peer     *overlayPeer
	envelope *quicOverlayEnvelope
}

var _ overlay.BroadcastPeer = quicBroadcastSource{}
var _ overlay.BroadcastPeer = quicRouteBroadcastPeer{}
var _ overlay.PreparedBroadcastPeer = quicBroadcastSource{}
var _ overlay.PreparedBroadcastPeer = quicRouteBroadcastPeer{}

func (p quicBroadcastSource) ID() []byte {
	return p.id[:]
}

func (quicBroadcastSource) SendCustomMessage(
	context.Context,
	tl.Serializable,
) error {
	return nil
}

func (quicBroadcastSource) SendPreparedCustomMessage(context.Context, []byte) error {
	return nil
}

// sendForgetPeerOverInboundPath answers on the path the query arrived on.
// Deriving an outbound sender from the peer id instead would dial - and, for a
// peer this node has never dialled itself, resolve through the DHT, since an
// inbound connection never populates the route - all while the inbound handler
// still holds one of its query slots. Peer.SendMessage falls back to the
// inbound client, so the connection the peer already holds carries the notice.
func (s *overlaySubscription) sendForgetPeerOverInboundPath(
	ctx context.Context,
	peer *authenticatedQUICPeer,
) {
	if peer.peer == nil {
		return
	}
	payload, err := s.quicEnvelope.Message(ForgetPeer{})
	if err != nil {
		return
	}
	_ = peer.peer.SendMessage(ctx, payload)
}

func (p quicRouteBroadcastPeer) ID() []byte {
	return p.peer.id[:]
}

// SendCustomMessage never dials inline: parts go out only over an existing
// outbound connection, and at most one background dial per retry window brings
// a missing connection up. An offline peer yields errQUICPeerOffline so the
// caller can stop early and count the drop honestly rather than mistaking it
// for a delivery; dropping parts for an unreachable peer is acceptable FEC
// redundancy loss.
//
// Every write through this type is relay traffic - somebody else's broadcast
// forwarded to the peer - and defers to this node's own candidate symbol in
// flight to the same peer (see quicPrioritySendLatch); the candidate itself
// goes out through quicPriorityBroadcastPeer.
func (p quicRouteBroadcastPeer) SendCustomMessage(ctx context.Context, req tl.Serializable) error {
	peer, err := p.outbound()
	if err != nil {
		return err
	}
	p.peer.awaitPrioritySend(ctx)
	return p.writeMessage(ctx, peer, req)
}

func (p quicRouteBroadcastPeer) SendPreparedCustomMessage(ctx context.Context, body []byte) error {
	peer, err := p.outbound()
	if err != nil {
		return err
	}
	p.peer.awaitPrioritySend(ctx)
	return p.writePrepared(ctx, peer, body)
}

// outbound is the live outbound connection to the peer or errQUICPeerOffline
// with a background dial requested; it never waits on anything.
func (p quicRouteBroadcastPeer) outbound() (*adnlquic.Peer, error) {
	peer, err := p.peer.node.quicGateway.OutboundPeerDefaultID(p.peer.id[:])
	if err != nil {
		p.peer.requestBackgroundQUICDial()
		return nil, errQUICPeerOffline
	}
	return peer, nil
}

// writeMessage and writePrepared are the route-bound writes shared by relay
// and priority sends; the caller decides how the peer's latch applies before
// opening the stream.
func (p quicRouteBroadcastPeer) writeMessage(ctx context.Context, peer *adnlquic.Peer, req tl.Serializable) error {
	payload, err := p.envelope.Message(req)
	if err != nil {
		return err
	}
	if err = peer.SendOutboundMessage(ctx, payload); err != nil {
		return fmt.Errorf("send quic overlay message: %w", err)
	}
	return nil
}

func (p quicRouteBroadcastPeer) writePrepared(ctx context.Context, peer *adnlquic.Peer, body []byte) error {
	prefix := p.envelope.state.Load().messagePrefix
	if err := peer.SendOutboundMessageParts(ctx, prefix, body); err != nil {
		return fmt.Errorf("send prepared quic overlay message: %w", err)
	}
	return nil
}

// requestBackgroundQUICDial starts at most one asynchronous dial per retry
// window for this peer's route; once the dial lands, subsequent sends find the
// live peer through the gateway.
func (p *overlayPeer) requestBackgroundQUICDial() {
	route := p.route
	if !route.QUICDialPermitted(time.Now()) {
		return
	}
	if !route.ClaimBackgroundQUICDial() {
		return
	}
	node := p.node
	path := p.quicPath()
	spawned := node.runAsync(func() {
		defer route.ReleaseBackgroundQUICDial()
		ctx, cancel := context.WithTimeout(node.runCtx, quicRelayDialTimeout)
		defer cancel()
		_, _ = path.dialBounded(ctx)
	})
	if !spawned {
		route.ReleaseBackgroundQUICDial()
	}
}

// prewarmQUICPeers resolves missing routes and opens connections for peers
// attached before subscription startup. Later peer and route arrivals are
// handled by prewarmQUICPeer from attachPooledPeer.
func (s *overlaySubscription) prewarmQUICPeers() {
	if !s.spec.UseQUIC {
		return
	}

	for _, peer := range s.peersSnapshot() {
		s.prewarmQUICPeer(peer)
	}
}

func (s *overlaySubscription) prewarmQUICPeer(peer *overlayPeer) {
	if !s.spec.UseQUIC {
		return
	}

	s.mx.Lock()
	current := s.cancel != nil && !s.removed && !s.inactive && s.peers[peer.id] == peer
	s.mx.Unlock()
	if !current {
		return
	}

	if _, err := s.node.quicGateway.OutboundPeerDefaultID(peer.id[:]); err == nil {
		return
	}
	peer.requestBackgroundQUICDial()
}

func (p *overlayPeer) dialQUIC(ctx context.Context) (*adnlquic.Peer, error) {
	return p.quicPath().dialGated(ctx)
}

func (p *overlayPeer) quicPath() quicPeerPath {
	return quicPeerPath{
		node:      p.node,
		id:        p.id,
		publicKey: p.pub,
		route:     p.route,
	}
}

func (p *authenticatedQUICPeer) quicPath() quicPeerPath {
	return quicPeerPath{
		node:      p.node,
		id:        p.id,
		publicKey: p.publicKey,
		route:     p.route,
	}
}

func resolveQUICRoute(
	ctx context.Context,
	node *Node,
	id PeerID,
	publicKey ed25519.PublicKey,
	route *peerroute.Route,
) (string, error) {
	if addr := route.QUICAddress(); addr != "" && !route.QUICAddressStale() {
		return addr, nil
	}
	if node.dht == nil {
		return "", errQUICRouteMissing
	}

	var (
		addresses         *adnladdr.List
		resolvedPublicKey ed25519.PublicKey
		err               error
	)
	for {
		addresses, resolvedPublicKey, err = node.resolvePeerAddressesFresh(
			ctx,
			id,
			quicAddressCacheMaxAge,
		)
		if err == nil {
			break
		}
		if !errors.Is(err, dht.ErrDHTValueIsNotFound) {
			return "", fmt.Errorf("resolve QUIC peer: %w", err)
		}

		timer := time.NewTimer(quicPeerDiscoveryRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", fmt.Errorf("resolve QUIC peer: %w", ctx.Err())
		case <-timer.C:
		}
	}
	if !bytes.Equal(resolvedPublicKey, publicKey) {
		return "", errors.New("QUIC peer public key changed in DHT")
	}
	addr, err := peerQUICRouteFromAddresses(addresses.Addresses)
	if err != nil {
		return "", err
	}
	route.RefreshQUICAddress(addr)
	return addr, nil
}

// quicPeerPath is the single owner of outbound QUIC dialing for a peer. id is
// the peer's precomputed 32-byte ADNL short id (every roster and handshake
// identity is derived that way), so dials skip re-hashing the public key.
type quicPeerPath struct {
	node      *Node
	id        PeerID
	publicKey ed25519.PublicKey
	route     *peerroute.Route
}

// quicDialTurn is always one of two valid states: an established peer the
// caller can use immediately, or ownership of the route dial claim.
type quicDialTurn struct {
	peer  *adnlquic.Peer
	owner bool
}

// dialBounded runs the background route prewarm under a node-wide
// transport-setup budget. Its only caller is requestBackgroundQUICDial, whose
// context is the reachability probe itself (quicRelayDialTimeout), which is
// what makes dialClaimed's retry gate honest: a broadcast that entered here
// under its own send budget would arm a 10-30s per-peer gate for a peer that
// was merely slower than one candidate. A nil limiter is retained for
// hand-built nodes in narrow package tests; every node created through New has
// the bound.
func (p quicPeerPath) dialBounded(ctx context.Context) (*adnlquic.Peer, error) {
	turn, err := p.awaitQUICDialTurn(ctx)
	if err != nil {
		return nil, err
	}
	if !turn.owner {
		return turn.peer, nil
	}

	slots := p.node.quicOutboundDialSlots
	if slots == nil {
		return p.dialClaimed(ctx)
	}

	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		p.route.FinishQUICDial()
		return nil, fmt.Errorf("wait for QUIC dial slot: %w", ctx.Err())
	}

	return p.dialClaimed(ctx)
}

func (s *overlaySubscription) quicPeerPath(id PeerID) (quicPeerPath, error) {
	if peer := s.peerByID(id); peer != nil {
		return peer.quicPath(), nil
	}

	peer, err := s.node.authenticatedQUICPeer(id)
	if err != nil {
		return quicPeerPath{}, err
	}
	return peer.quicPath(), nil
}

// dialGated dials with the per-route retry gate: one in-flight attempt, and a
// failed attempt defers redials (including the DHT re-resolve) for the retry
// window.
func (p quicPeerPath) dialGated(ctx context.Context) (*adnlquic.Peer, error) {
	turn, err := p.awaitQUICDialTurn(ctx)
	if err != nil {
		return nil, err
	}
	if !turn.owner {
		return turn.peer, nil
	}

	return p.dialClaimed(ctx)
}

// awaitQUICDialTurn claims the route before entering tonutils Gateway, whose
// per-peer dial mutex is not context-aware. Contenders coalesce on the route's
// completion channel and therefore retain their own deadline while reusing a
// successful prewarm.
func (p quicPeerPath) awaitQUICDialTurn(ctx context.Context) (quicDialTurn, error) {
	for {
		if peer, err := p.node.quicGateway.OutboundPeerDefaultID(p.id[:]); err == nil {
			p.route.SucceedQUICDial(false)
			p.node.noteOutboundQUICPath(p.id)

			return quicDialTurn{peer: peer}, nil
		}
		if err := ctx.Err(); err != nil {
			return quicDialTurn{}, fmt.Errorf("dial QUIC peer: %w", err)
		}
		if p.route.BeginQUICDial(time.Now()) {
			return quicDialTurn{owner: true}, nil
		}
		if !p.route.QUICDialInFlight() {
			return quicDialTurn{}, fmt.Errorf("dial QUIC peer: %w", errQUICDialDeferred)
		}
		if err := p.route.WaitQUICDial(ctx); err != nil {
			return quicDialTurn{}, fmt.Errorf("wait for QUIC peer dial: %w", err)
		}
	}
}

// dialClaimed enters the transport only after this caller owns the route. No
// second caller for the same peer can now block on tonutils' non-context
// dialMu; it waits on the route completion channel instead.
//
// A failure here arms the per-peer retry gate, so every caller's context must
// be a genuine reachability budget rather than a latency budget of its own:
// broadcast fan-out reaches peers through quicRouteBroadcastPeer and never
// arrives here at all.
func (p quicPeerPath) dialClaimed(ctx context.Context) (*adnlquic.Peer, error) {
	endpointChosen := false
	peer, err := p.node.quicGateway.DialDefaultResolvedID(
		ctx,
		p.id[:],
		p.publicKey,
		func(ctx context.Context) (string, error) {
			addr, resolveErr := resolveQUICRoute(
				ctx,
				p.node,
				p.id,
				p.publicKey,
				p.route,
			)
			if resolveErr != nil {
				return "", resolveErr
			}
			endpointChosen = true
			return addr, nil
		},
	)
	if err == nil {
		p.route.SucceedQUICDial(true)
		// Queries, two-step broadcasts, Plumtree fanout, repairs and background
		// relay dials all meet here, so the idle sweep can account for every
		// outbound path consistently.
		p.node.noteOutboundQUICPath(p.id)
		return peer, nil
	}
	if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
		p.route.FinishQUICDial()
		return nil, fmt.Errorf("dial QUIC peer: %w", ctxErr)
	}
	if endpointChosen {
		p.route.MarkQUICAddressStale()
	}
	p.route.FailQUICDial(time.Now())
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("dial QUIC peer: %w", ctxErr)
	}
	return nil, fmt.Errorf("dial QUIC peer: %w", err)
}
