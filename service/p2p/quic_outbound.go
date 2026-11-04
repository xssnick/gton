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
	// dials - the route prewarm, the redial a relay requests and the attempt
	// own messages share through dialForMessage - so simultaneous overlays
	// cannot multiply the handshake and DHT load. Relays never wait behind it;
	// an own broadcast or a consensus message waits for its shared attempt only
	// within its own send budget.
	outboundQUICDialParallelism = 16
)

// errQUICPeerOffline reports that a broadcast could not be sent because the peer
// has no live outbound QUIC connection: a relay found none, or an own broadcast
// found none and either skipped the wait on a failing route or did not get one
// from the shared dial within its send budget (the dial error is then wrapped
// too). Callers treat it as a dropped part, not a peer failure: a dial is
// already requested or in flight, and eviction of a genuinely dead peer is the
// ping path's job.
var errQUICPeerOffline = errors.New("quic peer has no live outbound connection")

// quicRelayDialTimeout bounds a background dial (DHT resolve plus QUIC
// handshake): the one a relay requests when it finds no live outbound
// connection, and the one dialForMessage starts for own broadcasts and
// consensus messages, which wait for it only within their own budgets.
const quicRelayDialTimeout = 10 * time.Second

// quicBroadcastSource adapts an authenticated source to BroadcastPeer. C++
// ignores FEC feedback, so sending a response is intentionally a no-op.
type quicBroadcastSource struct {
	// HandleMessage consumes the borrowed immutable ID synchronously.
	id *PeerID
}

// quicRouteBroadcastPeer is the relay identity of an outbound broadcast peer:
// receive-side FEC and simple relay, two-step relay and the queued rebroadcast
// of somebody else's broadcast all send through it, and it never dials inline.
// That is a deliberate deviation from C++, whose QuicSender makes every send,
// relayed or own, wait for the connection it creates. A relayed part is one of
// many copies other members forward too, so dropping it is redundancy loss,
// while waiting costs every relay path something it cannot spare:
// receive-side relays forward under short per-peer budgets on shared
// dispatchers (tonutils DefaultTwoStepRelayPeerTimeout = 750 ms), a queued
// two-step relay would hold its overlay's only queue worker, and a relay that
// resolved and handshook a cold peer inside its budget spent it on transport
// setup and then reported the expiry as a peer fault. Connection setup belongs
// to the paths that can afford to wait for it: the attach-time prewarm,
// requestBackgroundQUICDial, and the background dial shared through
// dialForMessage. Own broadcasts wait for that dial through
// quicLocalBroadcastPeer; this node's validator candidate goes out through
// quicPriorityBroadcastPeer. A vote's short delivery deadline never owns a dial.
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

// SendCustomMessage never dials inline: relayed parts go out only over an
// existing outbound connection, and at most one background dial per retry
// window brings a missing connection up. An offline peer yields
// errQUICPeerOffline so the caller can stop early and count the drop honestly
// rather than mistaking it for a delivery; dropping relayed parts for an
// unreachable peer is acceptable FEC redundancy loss.
//
// Every write through this type is relay traffic - somebody else's broadcast
// forwarded to the peer - and defers to this node's own candidate symbol in
// flight to the same peer (see quicPrioritySendLatch); the candidate itself
// goes out through quicPriorityBroadcastPeer and this node's other broadcasts
// through quicLocalBroadcastPeer.
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
		p.peer.quicPath().requestBackgroundQUICDial()
		return nil, errQUICPeerOffline
	}
	return peer, nil
}

// quicLocalBroadcastPeer is the send identity of a broadcast this node
// originates in a custom or FastSync overlay: a queued rebroadcast request
// marked local, such as an external accepted from a client or a block this
// node publishes; the validator candidate goes through
// quicPriorityBroadcastPeer instead. Unlike a relay it waits for a missing
// outbound connection, as C++ QuicSender does for every message
// (quic-sender.cpp send_message_coro / wait_connection_ready): the send joins
// or starts the route's shared dial (dialForMessage, which runs it on the node
// context under quicRelayDialTimeout) and waits for it within its own send
// budget; the per-peer queues wait before the send, under a budget of their
// own (rebroadcastToPeer). The idle sweep, an LRU drop of the path or a
// graceful remote restart no dial failed against therefore costs one
// handshake, not the broadcast. A connection the peer opened to us is never
// written to, as in C++.
//
// It does not wait on a route whose last dial that reached a verdict failed:
// the part is then dropped with errQUICPeerOffline and a gated background
// dial, exactly like a relay. The two-step queue of a custom or FastSync
// overlay is one worker and tonutils returns only when every recipient is
// done, so waiting on an unreachable member would stall the broadcast for
// every member for a whole send budget (5 s) and, with the retry gate, again
// every 10-30 s. Everything else is waited on - a never-dialed route, a dial
// in flight - so the skip bounds the stall to one dial attempt per outage, and
// to one more for a fixed member whose new connection reopens its route
// (reopenFailingRoute): usually the 5 s handshake when the address is known,
// up to quicRelayDialTimeout (two send budgets) when the route is resolved
// through the DHT (measured with a known address: a plain wait stalled every
// attempt by 5.0 s, with the skip only the first send stalled).
//
// A dial that failed while the peer was down leaves the route failing until a
// redial succeeds, and the retry gate lets one through only 10-30 s after the
// failure. A custom fixed or private overlay member that opens a new
// connection to us once it is back ends that early: reopenFailingRoute reopens
// the route and redials at once, and own broadcasts wait for that dial (its
// doc lists the restarts it does not reach). Otherwise - a FastSync member, a
// member that never dials us - the gate is waited out and the send that
// requests the redial is dropped too. After a crash the old outbound
// connection takes writes until the 15 s QUIC idle timeout closes it, so parts
// written to it count as sent and are lost.
//
// Writes defer to this node's own candidate symbol in flight to the same peer,
// like relay writes (see quicPrioritySendLatch).
type quicLocalBroadcastPeer struct {
	route quicRouteBroadcastPeer
}

var _ overlay.BroadcastPeer = quicLocalBroadcastPeer{}
var _ overlay.PreparedBroadcastPeer = quicLocalBroadcastPeer{}

func (p quicLocalBroadcastPeer) ID() []byte {
	return p.route.ID()
}

func (p quicLocalBroadcastPeer) SendCustomMessage(ctx context.Context, req tl.Serializable) error {
	peer, err := p.connection(ctx)
	if err != nil {
		return err
	}

	p.route.peer.awaitPrioritySend(ctx)
	return p.route.writeMessage(ctx, peer, req)
}

func (p quicLocalBroadcastPeer) SendPreparedCustomMessage(ctx context.Context, body []byte) error {
	peer, err := p.connection(ctx)
	if err != nil {
		return err
	}

	p.route.peer.awaitPrioritySend(ctx)
	return p.route.writePrepared(ctx, peer, body)
}

// connection returns the live outbound connection, waiting for the shared dial
// on a route that is not failing. Every error wraps errQUICPeerOffline, so
// callers drop the part without faulting the peer: a dial this node is still
// waiting for says nothing about the peer, and handlePeerQueryFailure would
// evict a peer with no open connection. A wait error also wraps why the wait
// ended, which classifyTwoStepSendFault reports in preference to the drop.
func (p quicLocalBroadcastPeer) connection(ctx context.Context) (*adnlquic.Peer, error) {
	member := p.route.peer

	// FEC bursts come here for every part, so the hit stays lock-free:
	// dialForMessage's own hit path takes the route and node-wide path locks.
	if peer, err := member.node.quicGateway.OutboundPeerDefaultID(member.id[:]); err == nil {
		return peer, nil
	}

	if member.route.QUICDialFailing() {
		return p.route.outbound()
	}

	peer, err := member.quicPath().dialForMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errQUICPeerOffline, err)
	}
	return peer, nil
}

// localTwoStepPeerSet marks the peers of this node's own two-step broadcast.
// The freshly resolved set is rewritten in place like prioritizeTwoStepPeerSet
// does; RLDP peers have no QUIC connection to wait for and stay as they are.
func localTwoStepPeerSet(peers overlay.StaticBroadcastPeerSet) overlay.StaticBroadcastPeerSet {
	for i, peer := range peers {
		if route, ok := peer.(quicRouteBroadcastPeer); ok {
			peers[i] = quicLocalBroadcastPeer{route: route}
		}
	}
	return peers
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
// window for this route; once the dial lands, subsequent sends find the live
// peer through the gateway.
func (p quicPeerPath) requestBackgroundQUICDial() {
	route := p.route
	if !route.QUICDialPermitted(time.Now()) {
		return
	}
	if !route.ClaimBackgroundQUICDial() {
		return
	}

	spawned := p.node.runAsync(func() {
		defer route.ReleaseBackgroundQUICDial()
		ctx, cancel := context.WithTimeout(p.node.runCtx, quicRelayDialTimeout)
		defer cancel()
		_, _ = p.dialBounded(ctx)
	})
	if !spawned {
		route.ReleaseBackgroundQUICDial()
	}
}

// reopenFailingRoute runs for every new gateway path to this peer, from the
// connection handler (handleInboundQUICPeer). An authenticated QUIC connection
// the peer opened to us is fresh proof that it is alive, so when the path
// belongs to a member of a custom fixed or private QUIC overlay whose route is
// failing, the route is reopened (peerroute.Route.ReopenFailingQUICDial) and
// one background dial starts at once. Own broadcasts and consensus messages
// that arrive while it is in flight wait for it (dialForMessage joins it)
// instead of being dropped.
//
// Without it, a member that was down long enough for one of our dials to fail
// (the 5 s QUIC handshake idle timeout) stays failing for the 10-30 s retry
// window after it is back and has dialed us: every own broadcast to it is
// dropped, the one whose send finally requests the redial included (measured:
// 11 externals lost over ~11 s after the member returned; 26 over 26 s after a
// 50 s outage). This is gton policy rather than C++ parity - C++ QuicSender has
// no retry gate at all and re-dials for every message after a failed init - and
// it narrows the gap only for fixed members (see
// inboundQUICReopensFailingRoute for why FastSync is left out).
//
// It reacts to a new path - what a returning member opens - never to inbound
// messages: those arrive on the hot path (every FEC part), and a peer that is
// alive but not dialable from here (NAT, a firewall, a wrong advertised
// address) keeps talking to us over its own connection, so reopening on its
// traffic would turn the gate into back-to-back 5 s handshakes, which is what
// it exists to prevent. Nothing bounds how often a member opens new paths
// either, so the route reopens only once per failing streak - until a dial of
// ours succeeds - and keeps its backoff count. Such a member then costs one
// more failed dial per streak: while it is in flight own broadcasts wait for
// it, which in a custom overlay holds the single two-step worker - every
// member's own broadcasts - for up to quicRelayDialTimeout (the kept stale
// flag makes the dial re-resolve through the DHT), and its failure arms the
// gate where the backoff left off. The dial is started, never run here: the
// gateway keeps the path not ready until the handler returns, and a dial for
// the same path waits on that.
//
// The route is left alone when the path already has our outbound connection
// (the gateway runs the handler for paths our own dial creates too), when it is
// not failing, already reopened in this streak or has a dial in flight, and
// when the peer is no such member. The learned address stays stale: the remote
// address of an inbound connection is the peer's listener only for a serving
// peer without NAT - behind NAT it is a mapping, and a client-mode peer dials
// from an ephemeral socket - and it is no signed advertisement, so the signed
// DHT record stays the authority and the dial re-resolves through it.
//
// Not covered:
//   - A crash of the peer leaves our old path to it alive until the 15 s QUIC
//     idle timeout, and its new connection joins that path without a handler:
//     the gateway runs it only for a path it creates, and an inbound connection
//     replacing the one of a live path has no callback. If the path carried our
//     outbound connection, own broadcasts went over the zombie and were lost
//     while no dial ran, so the route is normally not failing. If it held only
//     the peer's inbound connection - what the idle sweep or an earlier restart
//     of the peer leaves behind - an own broadcast during the outage dials,
//     fails and arms the gate, and the member waits it out as before. Closing
//     that needs a gateway hook for an inbound connection joining a live path.
//   - A connection the peer opens while our dial to it is in flight joins the
//     path that dial created, and the handler runs as the dial gives up, before
//     its failure is recorded: the dial is still in flight here, its failure
//     then arms the gate, and the member waits it out although its connection
//     is newer than the probes that failed. Our Initial is retransmitted with a
//     backoff from 200 ms - at about 0.2, 0.6, 1.4 and 3.0 s; the next, at
//     6.2 s, falls past the 5 s handshake idle timeout - so a peer whose
//     listener is back within the first ~3 s of our dial answers a probe and
//     the dial succeeds, while one back in the last ~2 s that dials us before
//     our dial gives up waits out the gate (measured on loopback: back at
//     2.5 s, our dial succeeded at 3.0 s; back at 3.3-4.5 s, it failed at 5.0 s
//     with the route gated). That is 2 s of every 15-35 s retry cycle while
//     own broadcasts keep dialing a member that is down.
//   - A member that never dials us (a pure receiver) gives no proof of life and
//     waits out the gate too.
func (p *authenticatedQUICPeer) reopenFailingRoute() {
	node := p.node
	if _, err := node.quicGateway.OutboundPeerDefaultID(p.id[:]); err == nil {
		return
	}

	// Every new path comes through here, so a healthy route - the common
	// case - costs one route lock and no subscription scan.
	if !p.route.QUICDialFailing() || !node.inboundReopensQUICRoute(p.id) {
		return
	}
	if !p.route.ReopenFailingQUICDial() {
		return
	}

	node.log.Info().
		Str("peer_id", p.id.String()).
		Str("peer", p.addr).
		Msg("inbound QUIC path from a fixed member reopened its failing route; redialing")
	// A background dial that has just recorded its failure may still hold the
	// single-flight claim, and then none starts here. The route is no longer
	// failing either way, so the next own broadcast starts the dial itself
	// through dialForMessage and waits for it.
	p.quicPath().requestBackgroundQUICDial()
}

// inboundReopensQUICRoute reports whether id is on the roster of an overlay
// whose members' failing routes a new inbound path reopens.
func (n *Node) inboundReopensQUICRoute(id PeerID) bool {
	n.subscriptionsMx.RLock()
	defer n.subscriptionsMx.RUnlock()

	for _, sub := range n.subscriptions {
		if sub.spec.inboundQUICReopensFailingRoute() && sub.acceptsPeerID(id) {
			return true
		}
	}
	return false
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
	peer.quicPath().requestBackgroundQUICDial()
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
// was merely slower than one candidate. Waiting senders get the same
// reachability budget through dialForMessage, whose attempt runs on the node
// context under quicRelayDialTimeout while they only wait for it. A nil
// limiter is retained for hand-built nodes in narrow package tests; every node
// created through New has the bound.
func (p quicPeerPath) dialBounded(ctx context.Context) (*adnlquic.Peer, error) {
	turn, err := p.awaitQUICDialTurn(ctx)
	if err != nil {
		return nil, err
	}
	if !turn.owner {
		return turn.peer, nil
	}

	return p.dialClaimedBounded(ctx)
}

func (p quicPeerPath) dialClaimedBounded(ctx context.Context) (*adnlquic.Peer, error) {
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

// dialForMessage lets independent latency-bounded messages - consensus messages
// and this node's own broadcasts - share one background reachability attempt.
// Expiring a vote only stops its wait: it must not abort a healthy cold
// handshake or arm the route's retry gate for later votes.
func (p quicPeerPath) dialForMessage(ctx context.Context) (*adnlquic.Peer, error) {
	for {
		if err := p.node.runCtx.Err(); err != nil {
			return nil, err
		}

		turn, err := p.awaitQUICDialTurn(ctx)
		if err != nil {
			return nil, err
		}
		if !turn.owner {
			return turn.peer, nil
		}

		spawned := p.node.runAsync(func() {
			dialCtx, cancel := context.WithTimeout(p.node.runCtx, quicRelayDialTimeout)
			defer cancel()
			_, _ = p.dialClaimedBounded(dialCtx)
		})
		if !spawned {
			p.route.FinishQUICDial()
			return nil, context.Canceled
		}
	}
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
// relays never arrive here, and own broadcasts and consensus messages arrive
// only through dialForMessage's background attempt, under quicRelayDialTimeout
// on the node context.
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
