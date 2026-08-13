package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type overlayNodeIdentity struct {
	pub    ed25519.PublicKey
	peerID PeerID
	self   bool
}

func (s *overlaySubscription) connectDHTOverlayNode(ctx context.Context, node overlay.Node) (bool, error) {
	connectCtx, cancel := context.WithTimeout(ctx, dhtSeedPeerTimeout)
	defer cancel()
	return s.connectOverlayNodeV1(connectCtx, node)
}

func (s *overlaySubscription) connectOverlayNodeV1(ctx context.Context, node overlay.Node) (bool, error) {
	if !s.isActive() {
		return false, errors.New("shard is inactive")
	}
	identity, err := s.overlayNodeIdentity(node)
	if err != nil {
		return false, err
	}
	if identity.self {
		return false, nil
	}

	peerID := identity.peerID

	s.mx.Lock()
	if peer := s.peers[peerID]; peer != nil {
		peer.mergeAnnouncement(&node)
		s.mx.Unlock()
		s.reloadNeighbours()
		s.retryPendingPeerWarmup(peer)
		return false, nil
	}
	s.mx.Unlock()

	// Resolve rather than reuse the filed address. A DHT lookup is expensive,
	// but nothing else re-checks an endpoint: acquirePeerEndpoint does no I/O
	// (the ADNL handshake is lazy), so a dial to a peer that has moved fails
	// silently later, and attachPooledPeer writes the same dead address back
	// into the directory. Re-resolving on every gossip mention is what keeps a
	// moved peer reachable at all. The other discovery paths that dial straight
	// from a filed address - promotion and the peer cache - are seeded by this
	// one, so this is where the address has to stay honest.
	endpoint, err := s.resolvePeerEndpoint(ctx, peerID)
	if err != nil {
		return false, err
	}

	// File what we learned before deciding whether to dial: the address and the
	// announcement are the valuable part, and they cost nothing to keep. A full
	// live tier may still dial an eligible replacement; attachPooledPeer repeats
	// the selection under the same lock before it commits the swap.
	s.mx.Lock()
	// Proven: the endpoint came from this peer's own signed DHT record and the
	// dial is our decision, not something a gossip flood can drive.
	s.rememberDirectoryPeerLocked(
		identity.peerID,
		identity.pub,
		endpoint.adnlAddr,
		endpoint.quicAddr,
		&node,
		time.Now(),
		directoryProven,
	)
	canAttach := len(s.peers) < s.livePeerLimit()
	if !canAttach {
		protected := s.node.protectedPeerIDs()
		evictID := s.peerReplacementCandidateLocked(peerID, time.Now(), protected)
		canAttach = !evictID.IsZero()
	}
	s.mx.Unlock()
	if !canAttach {
		return false, nil
	}

	pooled, releaseEndpoint, err := s.node.acquirePeerEndpoint(identity.peerID, endpoint, identity.pub)
	if err != nil {
		return false, fmt.Errorf("connect overlay peer endpoint: %w", err)
	}
	attached := s.attachPooledPeer(pooled, &node)
	s.node.attachPrivateOverlayPeer(pooled)
	releaseEndpoint()
	if attached {
		event := s.log.Debug()
		if s.aliveKnownPeerCount() <= 3 {
			event = s.log.Info()
		}
		event.
			Str("peer", pooled.addr).
			Str("peer_id", pooled.id.String()).
			Msg("connected overlay peer")
	}
	return attached, nil
}

// resolvePeerEndpoint finds a peer's endpoint in the DHT.
func (s *overlaySubscription) resolvePeerEndpoint(ctx context.Context, id PeerID) (peerEndpoint, error) {
	addrList, _, err := s.node.resolvePeerAddresses(ctx, id)
	if err != nil {
		return peerEndpoint{}, fmt.Errorf("find ADNL addresses: %w", err)
	}
	if len(addrList.Addresses) == 0 {
		return peerEndpoint{}, fmt.Errorf("overlay node has no addresses")
	}

	endpoint, err := peerEndpointFromAddresses(addrList.Addresses)
	if err != nil {
		return peerEndpoint{}, err
	}
	if quicAddr, quicErr := peerQUICRouteFromAddresses(addrList.Addresses); quicErr == nil {
		endpoint.quicAddr = quicAddr
	}
	return endpoint, nil
}

func (s *overlaySubscription) connectFixedNode(ctx context.Context, nodeID PeerID) (bool, error) {
	addrList, pub, err := s.node.resolvePeerAddresses(ctx, nodeID)
	if err != nil {
		return false, fmt.Errorf("find ADNL addresses: %w", err)
	}
	if len(addrList.Addresses) == 0 {
		return false, fmt.Errorf("fixed node has no addresses")
	}

	pubID, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		return false, fmt.Errorf("hash fixed node public key: %w", err)
	}
	if !bytes.Equal(pubID, nodeID[:]) {
		return false, fmt.Errorf("fixed node public key does not match requested ADNL id")
	}

	endpoint, err := peerEndpointFromAddresses(addrList.Addresses)
	if err != nil {
		return false, err
	}
	quicAddr, quicErr := peerQUICRouteFromAddresses(addrList.Addresses)
	if quicErr != nil {
		if s.spec.UseQUIC {
			return false, quicErr
		}
	} else {
		endpoint.quicAddr = quicAddr
	}
	pooled, releaseEndpoint, err := s.node.acquirePeerEndpoint(nodeID, endpoint, pub)
	if err != nil {
		return false, fmt.Errorf("connect fixed node endpoint: %w", err)
	}
	reinitFreshFixedTransport(pooled)
	attached := s.attachPooledPeer(pooled, nil)
	s.node.attachPrivateOverlayPeer(pooled)
	releaseEndpoint()
	if attached {
		s.log.Debug().
			Str("peer", pooled.addr).
			Str("peer_id", pooled.id.String()).
			Msg("connected fixed custom overlay peer")
	}
	return attached, nil
}

// reinitFreshFixedTransport restores restart semantics for an
// outbound-created transport: registerClient copies the gateway address
// list, whose ReinitDate is the process start time, so without this a
// rebuilt connection would keep the previous reinit epoch and a remote that
// remembers our old sequence numbers would drop the fresh low-seqno packets
// as duplicates. Reinit is applied only before any traffic in either
// direction: inbound means the pair already works, outbound means a channel
// may be pending and resetting it mid-handshake could collide with another
// overlay attaching the same peer.
func reinitFreshFixedTransport(pooled *pooledPeer) {
	stats := pooled.adnl.Stats()
	if stats.Inbound.Packets != 0 || stats.Outbound.Packets != 0 {
		return
	}
	if reiniter, ok := pooled.adnl.ADNL.(adnlReiniter); ok {
		reiniter.Reinit()
	}
}

func firstADNLEndpoint(addresses []adnladdr.Address) (string, error) {
	for _, addr := range addresses {
		endpoint, err := adnladdr.DialString(addr)
		if err == nil {
			return endpoint, nil
		}
	}
	return "", errors.New("peer has no supported endpoint")
}

func peerEndpointFromAddresses(addresses []adnladdr.Address) (peerEndpoint, error) {
	adnlAddr, err := firstADNLEndpoint(addresses)
	if err != nil {
		return peerEndpoint{}, err
	}

	return peerEndpoint{adnlAddr: adnlAddr}, nil
}

func peerQUICRouteFromAddresses(addresses []adnladdr.Address) (string, error) {
	endpoint, err := adnlquic.PeerEndpoint(addresses)
	if err != nil {
		return "", fmt.Errorf("peer QUIC endpoint: %w", err)
	}
	return endpoint.String(), nil
}

func (s *overlaySubscription) overlayNodeIdentity(node overlay.Node) (overlayNodeIdentity, error) {
	pub, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		return overlayNodeIdentity{}, fmt.Errorf("unsupported overlay node key type %T", node.ID)
	}
	if len(pub.Key) != ed25519.PublicKeySize {
		return overlayNodeIdentity{}, fmt.Errorf("invalid overlay node key size %d", len(pub.Key))
	}

	if err := node.CheckSignature(); err != nil {
		return overlayNodeIdentity{}, fmt.Errorf("overlay node signature: %w", err)
	}
	now := time.Now()
	nodeTime := time.Unix(int64(node.Version), 0)
	if nodeTime.Before(now.Add(-overlayPeerTTL)) || nodeTime.After(now.Add(overlayFutureSkew)) {
		return overlayNodeIdentity{}, fmt.Errorf("stale overlay node version: %d", node.Version)
	}
	if !bytes.Equal(node.Overlay, s.spec.ShortID) {
		return overlayNodeIdentity{}, fmt.Errorf("overlay id mismatch")
	}

	if selfPub, ok := s.node.privKey.Public().(ed25519.PublicKey); ok && bytes.Equal(pub.Key, selfPub) {
		return overlayNodeIdentity{self: true}, nil
	}

	nodeID, err := tl.Hash(node.ID)
	if err != nil {
		return overlayNodeIdentity{}, fmt.Errorf("hash overlay node id: %w", err)
	}
	peerID, err := NewPeerID(nodeID)
	if err != nil {
		return overlayNodeIdentity{}, fmt.Errorf("parse overlay node id: %w", err)
	}

	return overlayNodeIdentity{
		pub:    append(ed25519.PublicKey(nil), pub.Key...),
		peerID: peerID,
	}, nil
}

func (s *overlaySubscription) attachPooledPeer(pooled *pooledPeer, announced *overlay.Node) bool {
	if !s.acceptsPeerID(pooled.id) {
		return false
	}

	s.mx.Lock()
	if s.removed {
		s.mx.Unlock()
		return false
	}
	if peer := s.peers[pooled.id]; peer != nil {
		peer.mergeAnnouncement(announced)
		s.mx.Unlock()
		return false
	}

	var evicted *overlayPeer
	var evictID PeerID
	if len(s.peers) >= s.livePeerLimit() {
		protected := s.node.protectedPeerIDs()
		evictID = s.attachPeerEvictionCandidateLocked(pooled.id, protected)
		if evictID.IsZero() {
			s.mx.Unlock()
			return false
		}
	}

	fixedMember := s.spec.membersArePermanent()
	if s.fastSync != nil {
		fixedMember = s.fastSync.permanent(pooled.id)
	}
	state, err := s.newOverlayPeer(pooled, announced, fixedMember)
	if err != nil {
		s.mx.Unlock()
		s.log.Warn().
			Err(err).
			Str("peer", pooled.addr).
			Msg("failed to attach peer to overlay")
		return false
	}
	if !evictID.IsZero() {
		evicted = s.peers[evictID]
		delete(s.peers, evictID)
		s.removeNeighbourLocked(evictID)
		// Demotion, not amnesia: the directory row stays, so the peer is still
		// advertised, still a promotion candidate, and still served through the
		// detached query path.
		s.markDirectoryLiveLocked(evictID, false)
	}
	if s.spec.peersStartPending() {
		state.markPending()
	}
	state.initRebroadcastQueues()
	s.peers[pooled.id] = state
	s.rememberDirectoryPeerLocked(
		pooled.id,
		pooled.pub,
		pooled.addr,
		pooled.route.QUICAddress(),
		announced,
		time.Now(),
		directoryProven,
	)
	s.markDirectoryLiveLocked(pooled.id, true)
	s.notifyPeersChangedLocked()
	s.mx.Unlock()

	if evicted != nil {
		evicted.close()
	}
	s.installHandlers(state)
	s.startPeerRebroadcastWorker(state)
	s.reloadNeighbours()
	s.startPeerWarmup(state)
	return true
}

func (s *overlaySubscription) newOverlayPeer(pooled *pooledPeer, announced *overlay.Node, fixedMember bool) (*overlayPeer, error) {
	adnlOverlay, rldpOverlay, release, err := s.node.pool.acquireOverlay(pooled, s.broadcastReceiver, fixedMember)
	if err != nil {
		return nil, err
	}

	peer := &overlayPeer{
		node:        s.node,
		id:          pooled.id,
		addr:        pooled.addr,
		route:       pooled.route,
		pub:         pooled.pub,
		overlayID:   s.spec.ShortID,
		announced:   cloneOverlayNode(announced),
		fixedMember: fixedMember,
		overlay:     adnlOverlay,
		rldp:        pooled.rldp,
		rldpOverlay: rldpOverlay,
		release:     release,
		alive:       !fixedMember,
		attachedAt:  time.Now(),
	}
	if s.spec.UseQUIC {
		peer.queryTransport = quicPeerQueryTransport{
			peer:     peer,
			envelope: s.quicEnvelope,
		}
		peer.broadcastPeer = quicRouteBroadcastPeer{
			peer:     peer,
			envelope: s.quicEnvelope,
		}
	} else {
		peer.queryTransport = rldpPeerQueryTransport{
			overlay:   rldpOverlay,
			overlayID: s.spec.ShortID,
		}
		peer.broadcastPeer = peer.overlay
	}
	return peer, nil
}

func (s *overlaySubscription) installHandlers(peer *overlayPeer) {
	peer.overlay.SetCustomMessageHandler(func(msg *adnl.MessageCustom) error {
		switch msg.Data.(type) {
		case ForgetPeer:
			s.removePeerIfCurrent(peer)
			return nil
		default:
			return s.handlePrivateOverlayMessage(s.node.runCtx, peer.id, msg.Data)
		}
	})
	peer.overlay.SetQueryHandler(func(msg *adnl.MessageQuery) error {
		return s.answerADNLQuery(peer, msg)
	})
	peer.rldpOverlay.SetOnQuery(func(transferID []byte, query *rldp.Query) error {
		return s.answerRLDPQuery(peer, transferID, query)
	})
	peer.overlay.SetDisconnectHandler(func(_ string, _ ed25519.PublicKey) {
		// Remove by pointer, not by id: the disconnect cascade runs
		// asynchronously and a recovery path may have already re-attached a
		// fresh peer object for the same id, which must survive this
		// stale callback.
		s.removePeerIfCurrent(peer)
	})
}
