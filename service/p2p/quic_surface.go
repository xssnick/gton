package p2p

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/tl"
)

const quicStartupTimeout = 5 * time.Second

const (
	overlayQueryConstructorID            = uint32(0xccfd8443)
	overlayQueryWithExtraConstructorID   = uint32(0x94ffc3e9)
	overlayMessageConstructorID          = uint32(0x75252420)
	overlayMessageWithExtraConstructorID = uint32(0xa232233d)
)

var errQUICOverlayNotFound = errors.New("QUIC overlay is not subscribed")

type authenticatedQUICPeer struct {
	peer      *adnlquic.Peer
	node      *Node
	route     *peerRoute
	id        PeerID
	publicKey ed25519.PublicKey
	addr      string
	// outbound marks a path this node dialed. Only those are swept: an inbound
	// path is the remote's resource and closing it would drop a peer that is
	// actively feeding us.
	outbound bool
	// registeredAt bounds the idle age of a path that has not carried anything
	// yet, so a freshly dialed peer is not swept before it is used.
	registeredAt time.Time
}

// lastActive reports when this path last carried an outbound payload. The stamp
// lives on the transport itself (adnlquic.Peer.LastOutbound), so every send and
// query updates it and no call site can be forgotten. QUIC keep-alive (5s) is
// shorter than the idle timeout (15s), so without this a path never expires.
func (p *authenticatedQUICPeer) lastActive() time.Time {
	if p.peer != nil {
		if at := p.peer.LastOutbound(); !at.IsZero() {
			return at
		}
	}
	return p.registeredAt
}

type quicMembershipCertificateKind uint8

const (
	quicMembershipCertificateOmitted quicMembershipCertificateKind = iota
	quicMembershipCertificateEmpty
	quicMembershipCertificateMember
)

type quicOverlayHeader struct {
	overlay         []byte
	certificateKind quicMembershipCertificateKind
	certificate     overlay.MemberCertificate
}

func (n *Node) startQUICServer() error {
	adnlEndpoint, err := netip.ParseAddrPort(n.listenAddr)
	if err != nil {
		return fmt.Errorf("parse numeric ADNL listen endpoint: %w", err)
	}
	if adnlEndpoint.Port() == 0 {
		return errors.New("ADNL listen port must not be zero")
	}

	network, listenEndpoint := quicListenEndpoint(adnlEndpoint)
	packetConn, err := net.ListenPacket(network, listenEndpoint.String())
	if err != nil {
		return fmt.Errorf("listen QUIC on %s: %w", listenEndpoint, err)
	}

	done := make(chan struct{})
	n.quicPacketConn = packetConn
	n.quicServeDone = done

	go func() {
		n.quicServeErr = n.quicGateway.Serve(packetConn)
		close(done)
	}()

	readyCtx, cancel := context.WithTimeout(n.runCtx, quicStartupTimeout)
	defer cancel()
	if err = n.quicGateway.WaitReady(readyCtx); err != nil {
		_ = n.closeQUICGateway()
		return fmt.Errorf("wait for QUIC gateway: %w", err)
	}

	select {
	case <-done:
		serveErr := n.quicServeErr
		_ = n.closeQUICGateway()
		if serveErr == nil {
			serveErr = errors.New("QUIC gateway stopped during startup")
		}
		return serveErr
	default:
		return nil
	}
}

func quicListenEndpoint(
	adnlEndpoint netip.AddrPort,
) (string, netip.AddrPort) {
	// QuicSender binds every local identity on the IPv4 wildcard and uses only
	// the ADNL endpoint's port to derive the QUIC port.
	return "udp4", netip.AddrPortFrom(
		netip.IPv4Unspecified(),
		adnlEndpoint.Port()+1000,
	)
}

func (n *Node) closeQUICGateway() error {
	gatewayErr := n.quicGateway.Close()

	var packetErr error
	if n.quicPacketConn != nil {
		packetErr = n.quicPacketConn.Close()
		if errors.Is(packetErr, net.ErrClosed) {
			packetErr = nil
		}
	}
	if n.quicServeDone != nil {
		<-n.quicServeDone
	}
	return errors.Join(gatewayErr, packetErr)
}

func (n *Node) checkQUICServer() error {
	if !n.isPublicServer() {
		return nil
	}

	done := n.quicServeDone
	select {
	case <-done:
		err := n.quicServeErr
		if err == nil {
			err = errors.New("QUIC gateway stopped unexpectedly")
		}
		return err
	default:
		return nil
	}
}

func (n *Node) startQUICMonitor(ctx context.Context) {
	done := n.quicServeDone
	if done == nil {
		return
	}

	n.runAsync(func() {
		select {
		case <-ctx.Done():
			return
		case <-done:
		}
		if ctx.Err() != nil {
			return
		}

		err := n.quicServeErr
		reason := "QUIC gateway stopped unexpectedly"
		if err != nil {
			reason = fmt.Sprintf("%s: %v", reason, err)
		}
		n.log.Error().
			Str("reason", reason).
			Msg("QUIC gateway exited while node was online")
		go n.enterOfflineFailure(reason)
	})
}

// noteOutboundQUICPath marks a path as one we dialed. Recorded at our own dial
// sites rather than derived in the connection handler, which the gateway calls
// for both directions.
func (n *Node) noteOutboundQUICPath(id PeerID) {
	n.quicPeersMx.Lock()
	if peer := n.quicPeers[id]; peer != nil {
		peer.outbound = true
	}
	n.quicPeersMx.Unlock()
}

func (n *Node) handleInboundQUICPeer(peer *adnlquic.Peer) error {
	if !n.beginInbound() {
		return context.Canceled
	}
	defer n.finishInbound()

	var peerID PeerID
	copy(peerID[:], peer.PeerID())
	authenticated := &authenticatedQUICPeer{
		peer: peer,
		node: n,
		// Shared with the pooled transport for the same peer: a separate route
		// would split the learned QUIC address and the single-dial gate.
		route:        n.peerRoutes.get(peerID),
		id:           peerID,
		publicKey:    peer.PeerKey(),
		addr:         peer.RemoteAddr(),
		registeredAt: time.Now(),
	}
	n.quicPeersMx.Lock()
	n.quicPeers[peerID] = authenticated
	n.quicPeersMx.Unlock()
	if n.quicPeersAccepted.Add(1) == 1 {
		n.log.Info().
			Str("peer_id", peerID.String()).
			Str("peer", peer.RemoteAddr()).
			Msg("accepted first inbound QUIC peer")
	}

	peer.SetDisconnectHandler(func(disconnected *adnlquic.Peer) {
		n.removeQUICPeer(peerID, disconnected)
	})
	peer.SetQueryHandler(func(ctx context.Context, payload []byte) ([]byte, error) {
		return n.handleQUICQuery(ctx, authenticated, payload)
	})
	peer.SetMessageHandler(func(ctx context.Context, payload []byte) {
		if err := n.handleQUICMessage(ctx, authenticated, payload); err != nil &&
			!errors.Is(err, overlay.ErrBroadcastRejected) &&
			!errors.Is(err, errQUICOverlayNotFound) &&
			!errors.Is(err, context.Canceled) {
			n.log.Debug().
				Err(err).
				Str("peer", peer.RemoteAddr()).
				Msg("rejected QUIC overlay message")
		}
	})
	return nil
}

func (n *Node) authenticatedQUICPeer(id PeerID) (*authenticatedQUICPeer, error) {
	n.quicPeersMx.RLock()
	peer := n.quicPeers[id]
	n.quicPeersMx.RUnlock()
	if peer == nil {
		return nil, errAuthenticatedQUICPeerNotFound
	}
	return peer, nil
}

func (n *Node) removeQUICPeer(id PeerID, disconnected *adnlquic.Peer) {
	n.quicPeersMx.Lock()
	if current := n.quicPeers[id]; current == nil || current.peer != disconnected {
		n.quicPeersMx.Unlock()
		return
	}
	delete(n.quicPeers, id)
	n.quicPeersMx.Unlock()

	n.removePlumtreePeer(id)
}

func (n *Node) handleQUICQuery(
	ctx context.Context,
	peer *authenticatedQUICPeer,
	payload []byte,
) ([]byte, error) {
	if !n.beginInbound() {
		return nil, context.Canceled
	}
	defer n.finishInbound()

	header, body, err := parseQUICQueryEnvelope(payload)
	if err != nil {
		return nil, fmt.Errorf("parse QUIC overlay query: %w", err)
	}
	now := time.Now()
	sub, err := n.quicSubscription(header, peer.id, now)
	if err != nil {
		return nil, err
	}
	req, err := parseOneQUICOverlayObject(body)
	if err != nil {
		return nil, fmt.Errorf("parse QUIC overlay query body: %w", err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, peerQueryTimeout)
	defer cancel()

	select {
	case n.quicQuerySlots <- struct{}{}:
		defer func() {
			<-n.quicQuerySlots
		}()
	case <-queryCtx.Done():
		return nil, queryCtx.Err()
	}

	if repair, ok := req.(RepairPlumtreePart); ok {
		if sub.plumtree == nil {
			return nil, errPlumtreeDisabled
		}
		return sub.plumtree.HandleRepairQuery(queryCtx, peer.id, repair)
	}

	resp, err := sub.handlePeerQuery(queryCtx, peer.addr, req)
	if err != nil {
		if errors.Is(err, errOverlayInactive) {
			sub.sendForgetPeerOverInboundPath(queryCtx, peer)
		}
		return nil, err
	}
	answer, err := tl.Serialize(resp, true)
	if err != nil {
		return nil, fmt.Errorf("serialize QUIC overlay answer: %w", err)
	}
	return answer, nil
}

func (n *Node) handleQUICMessage(
	ctx context.Context,
	peer *authenticatedQUICPeer,
	payload []byte,
) error {
	if !n.beginInbound() {
		return context.Canceled
	}
	defer n.finishInbound()

	header, body, err := parseQUICMessageEnvelope(payload)
	if err != nil {
		return fmt.Errorf("parse QUIC overlay message: %w", err)
	}
	sub, err := n.quicSubscription(header, peer.id, time.Now())
	if err != nil {
		return err
	}

	if constructor, isPlumtree := plumtreeBroadcastConstructor(body); isPlumtree {
		if sub.plumtree == nil {
			return errPlumtreeDisabled
		}
		// Only the two payload broadcasts keep their wire form for forwarding;
		// control messages never touch it, so re-wrapping is skipped for them.
		var wire []byte
		switch constructor {
		case broadcastPlumtreeSimpleConstructorID,
			broadcastPlumtreeFECConstructorID:
			wire = payload
			if !sub.quicEnvelope.CanReuseMessage(header) {
				wire, body = sub.quicEnvelope.WrapMessageBody(body)
			}
		}
		return sub.plumtree.HandleMessage(ctx, peer.id, wire, body)
	}

	msg, err := parseOneQUICOverlayObject(body)
	if err != nil {
		return fmt.Errorf("parse QUIC overlay message body: %w", err)
	}
	if _, ok := msg.(ForgetPeer); ok {
		if current := sub.peerByID(peer.id); current != nil {
			sub.removePeerIfCurrent(current)
		}
		return nil
	}
	return sub.broadcastReceiver.HandleMessage(quicBroadcastSource{
		id: &peer.id,
	}, msg)
}

func parseQUICOverlayBody(payload []byte, envelope tl.Serializable) ([]byte, error) {
	body, err := tl.ParseNoCopy(envelope, payload, true)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, errors.New("missing boxed overlay payload")
	}
	return body, nil
}

func parseQUICQueryEnvelope(
	payload []byte,
) (quicOverlayHeader, []byte, error) {
	if len(payload) < 4 {
		return quicOverlayHeader{}, nil, errors.New(
			"QUIC overlay query header is too short",
		)
	}

	switch binary.LittleEndian.Uint32(payload[:4]) {
	case overlayQueryConstructorID:
		var envelope overlay.Query
		body, err := parseQUICOverlayBody(payload, &envelope)
		return quicOverlayHeader{
			overlay:         envelope.Overlay,
			certificateKind: quicMembershipCertificateOmitted,
		}, body, err
	case overlayQueryWithExtraConstructorID:
		var envelope overlay.QueryWithExtra
		body, err := parseQUICOverlayBody(payload, &envelope)
		if err != nil {
			return quicOverlayHeader{}, nil, err
		}
		header, err := quicOverlayHeaderFromExtra(
			envelope.Overlay,
			envelope.Extra,
		)
		return header, body, err
	default:
		return quicOverlayHeader{}, nil, errors.New(
			"unsupported QUIC overlay query header",
		)
	}
}

func parseQUICMessageEnvelope(
	payload []byte,
) (quicOverlayHeader, []byte, error) {
	if len(payload) < 4 {
		return quicOverlayHeader{}, nil, errors.New(
			"QUIC overlay message header is too short",
		)
	}

	switch binary.LittleEndian.Uint32(payload[:4]) {
	case overlayMessageConstructorID:
		var envelope overlay.Message
		body, err := parseQUICOverlayBody(payload, &envelope)
		return quicOverlayHeader{
			overlay:         envelope.Overlay,
			certificateKind: quicMembershipCertificateOmitted,
		}, body, err
	case overlayMessageWithExtraConstructorID:
		var envelope overlay.MessageWithExtra
		body, err := parseQUICOverlayBody(payload, &envelope)
		if err != nil {
			return quicOverlayHeader{}, nil, err
		}
		header, err := quicOverlayHeaderFromExtra(
			envelope.Overlay,
			envelope.Extra,
		)
		return header, body, err
	default:
		return quicOverlayHeader{}, nil, errors.New(
			"unsupported QUIC overlay message header",
		)
	}
}

func quicOverlayHeaderFromExtra(
	overlayID []byte,
	extra overlay.MessageExtra,
) (quicOverlayHeader, error) {
	header := quicOverlayHeader{
		overlay:         overlayID,
		certificateKind: quicMembershipCertificateOmitted,
	}
	if extra.Flags&1 == 0 {
		return header, nil
	}

	switch certificate := extra.Certificate.(type) {
	case overlay.EmptyMemberCertificate:
		header.certificateKind = quicMembershipCertificateEmpty
	case overlay.MemberCertificate:
		header.certificateKind = quicMembershipCertificateMember
		header.certificate = certificate
	default:
		return quicOverlayHeader{}, fmt.Errorf(
			"unsupported member certificate type %T",
			extra.Certificate,
		)
	}
	return header, nil
}

func parseOneQUICOverlayObject(body []byte) (tl.Serializable, error) {
	var obj any
	rest, err := tl.ParseNoCopy(&obj, body, true)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%d trailing bytes after overlay payload", len(rest))
	}
	return obj, nil
}

func (n *Node) quicSubscription(
	header quicOverlayHeader,
	peerID PeerID,
	now time.Time,
) (*overlaySubscription, error) {
	n.subscriptionsMx.RLock()
	sub := n.subscriptions[string(header.overlay)]
	n.subscriptionsMx.RUnlock()
	if sub == nil {
		return nil, errQUICOverlayNotFound
	}
	if err := sub.authorizeQUICPeer(peerID, header, now); err != nil {
		return nil, errQUICOverlayNotFound
	}
	return sub, nil
}

func (s *overlaySubscription) authorizeQUICPeer(
	peerID PeerID,
	header quicOverlayHeader,
	now time.Time,
) error {
	if s.fastSync == nil {
		if s.acceptsPeerID(peerID) {
			return nil
		}
		return errQUICOverlayNotFound
	}

	if header.certificateKind == quicMembershipCertificateMember {
		return s.fastSync.membership.AuthorizeMember(
			peerID,
			header.certificate,
			now,
		)
	}
	// Empty and omitted are distinct TL states, but C++ resolves both through
	// the enrolled peer's last verified certificate.
	return s.fastSync.membership.AuthorizeOmitted(peerID, now)
}
