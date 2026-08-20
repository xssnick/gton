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

// SendCustomMessage runs on the FEC part-processing path, so it never dials
// inline: parts go out only over an existing outbound connection, and at most
// one background dial per retry window brings a missing connection up. An
// offline peer yields errQUICPeerOffline so the caller can stop early and count
// the drop honestly rather than mistaking it for a delivery; dropping parts for
// an unreachable peer is acceptable FEC redundancy loss.
func (p quicRouteBroadcastPeer) SendCustomMessage(ctx context.Context, req tl.Serializable) error {
	node := p.peer.node
	peer, err := node.quicGateway.OutboundPeerDefaultID(p.peer.id[:])
	if err != nil {
		p.peer.requestBackgroundQUICDial()
		return errQUICPeerOffline
	}
	payload, err := p.envelope.Message(req)
	if err != nil {
		return err
	}
	if err = peer.SendOutboundMessage(ctx, payload); err != nil {
		return fmt.Errorf("send quic overlay message: %w", err)
	}
	return nil
}

func (p quicRouteBroadcastPeer) SendPreparedCustomMessage(ctx context.Context, body []byte) error {
	node := p.peer.node
	peer, err := node.quicGateway.OutboundPeerDefaultID(p.peer.id[:])
	if err != nil {
		p.peer.requestBackgroundQUICDial()
		return errQUICPeerOffline
	}
	prefix := p.envelope.state.Load().messagePrefix
	if err = peer.SendOutboundMessageParts(ctx, prefix, body); err != nil {
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
		_, _ = path.dialGated(ctx)
	})
	if !spawned {
		route.ReleaseBackgroundQUICDial()
	}
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
	endpointChosen := false
	claimed := false
	peer, err := p.node.quicGateway.DialDefaultResolvedID(
		ctx,
		p.id[:],
		p.publicKey,
		func(ctx context.Context) (string, error) {
			if !p.route.BeginQUICDial(time.Now()) {
				return "", errQUICDialDeferred
			}
			claimed = true

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
		p.route.SucceedQUICDial(claimed)
		// Queries, two-step broadcasts, Plumtree fanout, repairs and background
		// relay dials all meet here, so the idle sweep can account for every
		// outbound path consistently.
		p.node.noteOutboundQUICPath(p.id)
		return peer, nil
	}
	if errors.Is(err, errQUICDialDeferred) {
		return nil, fmt.Errorf("dial QUIC peer: %w", err)
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		if claimed {
			p.route.FinishQUICDial()
		}
		return nil, fmt.Errorf("dial QUIC peer: %w", err)
	}
	if endpointChosen {
		p.route.MarkQUICAddressStale()
	}
	if claimed {
		p.route.FailQUICDial(time.Now())
	} else {
		p.route.DeferQUICDial(time.Now())
	}
	return nil, fmt.Errorf("dial QUIC peer: %w", err)
}
