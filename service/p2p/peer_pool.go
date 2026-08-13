package p2p

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/peerroute"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

type peerPool struct {
	gateway                   *adnl.Gateway
	broadcastReceiverResolver overlay.BroadcastReceiverResolver
	// customMessage receives ADNL messages that carry no overlay envelope.
	customMessage func(msg *adnl.MessageCustom) error
	detachedQuery *detachedQueryHandlers
	routes        *peerroute.Table[PeerID]
	mx            sync.RWMutex
	peers         map[PeerID]*pooledPeer
}

// detachedQueryHandlers serve overlay queries arriving on a transport that has
// no attachment for the target overlay.
type detachedQueryHandlers struct {
	adnl func(pooled *pooledPeer, msg *adnl.MessageQuery) error
	rldp func(pooled *pooledPeer, transferID []byte, query *rldp.Query) error
}

type peerEndpoint struct {
	adnlAddr string
	quicAddr string
}

type pooledPeer struct {
	id              PeerID
	addr            string
	route           *peerroute.Route
	pub             ed25519.PublicKey
	adnl            *overlay.ADNLWrapper
	baseRLDP        *rldp.RLDP
	rldp            *overlay.RLDPWrapper
	refs            int
	fixedMemberRefs int
	adnlOverlayRefs map[*overlay.ADNLOverlayWrapper]int
	rldpOverlayRefs map[*overlay.RLDPOverlayWrapper]int
	lastUsedAt      atomic.Int64
}

type idlePooledPeer struct {
	peer     *pooledPeer
	lastUsed time.Time
}

var errPeerEndpointBusy = errors.New("peer endpoint is in active use")
var errPooledPeerUnavailable = errors.New("pooled peer is no longer available")

// acquirePeerEndpoint treats the first ADNL connection for a PeerID as canonical.
func (n *Node) acquirePeerEndpoint(peerID PeerID, endpoint peerEndpoint, key ed25519.PublicKey) (*pooledPeer, func(), error) {
	if err := validatePeerEndpointIdentity(peerID, key); err != nil {
		return nil, nil, err
	}

	n.peerUseMx.Lock()
	use := n.peerUse[peerID]
	if use.endpointPending != nil {
		n.peerUseMx.Unlock()
		return nil, nil, errPeerEndpointBusy
	}

	n.pool.mx.Lock()
	pooled := n.pool.peers[peerID]
	if pooled != nil && pooledPeerClosed(pooled) {
		// The transport is already closed but the asynchronous disconnect
		// cascade has not pruned the pool entry yet; reusing it would attach
		// a dead connection. Prune like removeDisconnected and reconnect.
		delete(n.pool.peers, peerID)
		pooled = nil
	}
	if pooled != nil {
		pooled.route.SetQUICAddress(endpoint.quicAddr)
		pooled.refs++
		n.pool.mx.Unlock()

		use.queries++
		n.peerUse[peerID] = use
		n.peerUseMx.Unlock()
		return pooled, n.releasePeerEndpoint(peerID, pooled, nil), nil
	}
	n.pool.mx.Unlock()

	if use.downloads > 0 || use.queries > 0 {
		n.peerUseMx.Unlock()
		return nil, nil, errPeerEndpointBusy
	}

	pending := make(chan struct{})
	use.endpointPending = pending
	n.peerUse[peerID] = use
	n.peerUseMx.Unlock()

	connected, err := n.pool.Get(endpoint, key)
	if err != nil {
		n.cancelPeerEndpointPending(peerID, pending)
		return nil, nil, err
	}
	pooled = connected
	if pooled.id != peerID {
		n.pool.closeIfUnused(pooled)
		n.cancelPeerEndpointPending(peerID, pending)
		return nil, nil, fmt.Errorf("peer endpoint identity mismatch: got %s want %s", pooled.id.String(), peerID.String())
	}
	if !n.pool.retain(pooled) {
		n.cancelPeerEndpointPending(peerID, pending)
		return nil, nil, errors.New("peer endpoint disconnected during acquisition")
	}

	n.peerUseMx.Lock()
	use = n.peerUse[peerID]
	use.queries++
	n.peerUse[peerID] = use
	n.peerUseMx.Unlock()
	return pooled, n.releasePeerEndpoint(peerID, pooled, pending), nil
}

// acquireArchivePeerEndpoint retains the canonical transport without entering
// ordinary live query/download accounting. The returned overlay handle has its
// own lifetime and can therefore be rotated by the archive pool independently
// from the live overlay roster.
func (n *Node) acquireArchivePeerEndpoint(peerID PeerID, addr string, key ed25519.PublicKey) (*pooledPeer, func(), error) {
	if err := validatePeerEndpointIdentity(peerID, key); err != nil {
		return nil, nil, err
	}
	if pooled := n.pool.retainByID(peerID); pooled != nil {
		return pooled, releaseRetainedPeer(n.pool, pooled), nil
	}

	pooled, err := n.pool.getADNL(addr, key)
	if err != nil {
		return nil, nil, err
	}
	if pooled.id != peerID {
		n.pool.closeIfUnused(pooled)
		return nil, nil, fmt.Errorf("peer endpoint identity mismatch: got %s want %s", pooled.id.String(), peerID.String())
	}
	if !n.pool.retain(pooled) {
		return nil, nil, errors.New("peer endpoint disconnected during acquisition")
	}
	return pooled, releaseRetainedPeer(n.pool, pooled), nil
}

func validatePeerEndpointIdentity(peerID PeerID, key ed25519.PublicKey) error {
	if peerID.IsZero() {
		return errors.New("peer endpoint identity is empty")
	}
	if len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid peer endpoint public key size %d", len(key))
	}
	rawID, err := tl.Hash(keys.PublicKeyED25519{Key: key})
	if err != nil {
		return fmt.Errorf("hash peer endpoint public key: %w", err)
	}
	keyID, err := NewPeerID(rawID)
	if err != nil {
		return fmt.Errorf("parse peer endpoint identity: %w", err)
	}
	if keyID != peerID {
		return fmt.Errorf("peer endpoint public key does not match identity %s", peerID.String())
	}
	return nil
}

func releaseRetainedPeer(pool *peerPool, peer *pooledPeer) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			pool.releaseRetained(peer)
		})
	}
}

func (n *Node) releasePeerEndpoint(peerID PeerID, pooled *pooledPeer, pending chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			n.peerUseMx.Lock()
			use := n.peerUse[peerID]
			if use.queries > 0 {
				use.queries--
			}
			if pending != nil && use.endpointPending == pending {
				use.endpointPending = nil
				close(pending)
			}
			if use.downloads == 0 && use.queries == 0 && use.endpointPending == nil {
				delete(n.peerUse, peerID)
			} else {
				n.peerUse[peerID] = use
			}
			n.peerUseMx.Unlock()

			n.pool.releaseRetained(pooled)
		})
	}
}

func (n *Node) cancelPeerEndpointPending(peerID PeerID, pending chan struct{}) {
	n.peerUseMx.Lock()
	defer n.peerUseMx.Unlock()

	use := n.peerUse[peerID]
	if use.endpointPending != pending {
		return
	}
	use.endpointPending = nil
	close(pending)
	if use.downloads == 0 && use.queries == 0 {
		delete(n.peerUse, peerID)
		return
	}
	n.peerUse[peerID] = use
}

func newPeerPool(
	gateway *adnl.Gateway,
	resolver overlay.BroadcastReceiverResolver,
	customMessage func(msg *adnl.MessageCustom) error,
	routes *peerroute.Table[PeerID],
) *peerPool {
	if routes == nil {
		routes = peerroute.NewTable[PeerID](peerRouteRetryPolicy)
	}
	return &peerPool{
		gateway:                   gateway,
		broadcastReceiverResolver: resolver,
		customMessage:             customMessage,
		routes:                    routes,
		peers:                     map[PeerID]*pooledPeer{},
	}
}

func (p *peerPool) Get(endpoint peerEndpoint, key ed25519.PublicKey) (*pooledPeer, error) {
	peer, err := p.gateway.RegisterClient(endpoint.adnlAddr, key)
	if err != nil {
		return nil, err
	}
	pooled, _, err := p.wrapEndpoint(peer, endpoint)
	if err != nil {
		peer.Close()
		return nil, err
	}
	return pooled, nil
}

func (p *peerPool) getADNL(addr string, key ed25519.PublicKey) (*pooledPeer, error) {
	peer, err := p.gateway.RegisterClient(addr, key)
	if err != nil {
		return nil, err
	}
	pooled, _, err := p.wrap(peer)
	if err != nil {
		peer.Close()
		return nil, err
	}
	return pooled, nil
}

func (p *peerPool) wrap(peer adnl.Peer) (*pooledPeer, bool, error) {
	id, err := NewPeerID(peer.GetID())
	if err != nil {
		return nil, false, err
	}

	p.mx.Lock()
	pooled, fresh, stale := p.wrapPeerLocked(peer, id)
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
	return pooled, fresh, nil
}

func (p *peerPool) wrapEndpoint(peer adnl.Peer, endpoint peerEndpoint) (*pooledPeer, bool, error) {
	id, err := NewPeerID(peer.GetID())
	if err != nil {
		return nil, false, err
	}

	p.mx.Lock()
	pooled, fresh, stale := p.wrapPeerLocked(peer, id)
	// Inbound ADNL peers have no discovered QUIC route. The first outbound
	// address-list route learned for that live generation becomes canonical.
	pooled.route.SetQUICAddress(endpoint.quicAddr)
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
	return pooled, fresh, nil
}

func (p *peerPool) wrapPeerLocked(peer adnl.Peer, id PeerID) (*pooledPeer, bool, []*pooledPeer) {
	if pooled := p.peers[id]; pooled != nil {
		if !pooledPeerClosed(pooled) {
			pooled.touch(time.Now())
			return pooled, false, nil
		}
		delete(p.peers, id)
	}

	wrapper := overlay.CreateExtendedADNL(peer)
	wrapper.SetBroadcastReceiverResolver(p.broadcastReceiverResolver)
	if p.customMessage != nil {
		// Non-overlay ADNL messages fall through to this handler; without it
		// they are dropped before anything sees them.
		wrapper.SetCustomMessageHandler(p.customMessage)
	}
	baseRLDP := rldp.NewClientV2(wrapper)
	rldpClient := overlay.CreateExtendedRLDP(baseRLDP)

	pooled := &pooledPeer{
		id:              id,
		addr:            peer.RemoteAddr(),
		route:           p.routes.Get(id),
		pub:             append(ed25519.PublicKey(nil), peer.GetPubKey()...),
		adnl:            wrapper,
		baseRLDP:        baseRLDP,
		rldp:            rldpClient,
		adnlOverlayRefs: map[*overlay.ADNLOverlayWrapper]int{},
		rldpOverlayRefs: map[*overlay.RLDPOverlayWrapper]int{},
	}
	pooled.touch(time.Now())
	// Serve overlay queries from peers we hold no attachment for. Without this
	// the wrappers drop them as "unregistered overlay", so an unattached peer
	// gets neither blocks nor Pong and evicts us as unreliable.
	if p.detachedQuery != nil {
		wrapper.SetOnUnknownOverlayQuery(func(msg *adnl.MessageQuery) error {
			return p.detachedQuery.adnl(pooled, msg)
		})
		rldpClient.SetOnUnknownOverlayQuery(func(transferID []byte, query *rldp.Query) error {
			return p.detachedQuery.rldp(pooled, transferID, query)
		})
	}
	rldpClient.SetOnDisconnect(func() {
		p.removeDisconnected(id, pooled)
	})
	p.peers[id] = pooled
	stale := p.pruneOverCapLocked(time.Now())
	return pooled, true, stale
}

func (p *pooledPeer) touch(now time.Time) {
	p.lastUsedAt.Store(now.UnixNano())
}

func (p *pooledPeer) lastUsed() time.Time {
	lastUsed := time.Unix(0, p.lastUsedAt.Load())
	stats := p.adnl.Stats()
	return latestTime(lastUsed, stats.Inbound.LastPacketAt, stats.Outbound.LastPacketAt)
}

func (p *peerPool) touchByID(id PeerID, now time.Time) {
	p.mx.RLock()
	if pooled := p.peers[id]; pooled != nil {
		pooled.touch(now)
	}
	p.mx.RUnlock()
}

func (p *peerPool) removeDisconnected(id PeerID, disconnected *pooledPeer) {
	p.mx.Lock()
	if p.peers[id] == disconnected {
		delete(p.peers, id)
	}
	p.mx.Unlock()
}

func (p *peerPool) acquireOverlay(pooled *pooledPeer, receiver *overlay.BroadcastReceiver, fixedMember bool) (*overlay.ADNLOverlayWrapper, *overlay.RLDPOverlayWrapper, func(), error) {
	p.mx.Lock()
	if p.peers[pooled.id] != pooled || pooledPeerClosed(pooled) {
		p.mx.Unlock()
		return nil, nil, nil, errPooledPeerUnavailable
	}

	adnlOverlay, err := pooled.adnl.AttachOverlay(receiver)
	if err != nil {
		p.mx.Unlock()
		return nil, nil, nil, err
	}
	rldpOverlay := pooled.rldp.CreateOverlay(receiver.OverlayID())

	pooled.refs++
	pooled.adnlOverlayRefs[adnlOverlay]++
	pooled.rldpOverlayRefs[rldpOverlay]++
	if fixedMember {
		pooled.fixedMemberRefs++
		if pooled.fixedMemberRefs == 1 {
			// C++ raises the unexpected RLDP receive MTU only for private-overlay
			// peers. Public and unlisted peers keep tonutils-go's bounded default;
			// expected query answers use their per-request limits independently.
			pooled.baseRLDP.SetMaxUnexpectedTransferSize(maxRLDPTwoStepTransferSize)
		}
	}
	p.mx.Unlock()

	var once sync.Once
	return adnlOverlay, rldpOverlay, func() {
		once.Do(func() {
			p.releaseOverlay(pooled, adnlOverlay, rldpOverlay, fixedMember)
		})
	}, nil
}

func (p *peerPool) releaseOverlay(pooled *pooledPeer, adnlOverlay *overlay.ADNLOverlayWrapper, rldpOverlay *overlay.RLDPOverlayWrapper, fixedMember bool) {
	p.mx.Lock()
	// Detach zero-ref wrappers before releasing the pool lock. Otherwise a
	// concurrent acquire can reuse the same wrapper in the gap and a stale
	// Close would detach the live generation.
	if refs := pooled.adnlOverlayRefs[adnlOverlay]; refs <= 1 {
		delete(pooled.adnlOverlayRefs, adnlOverlay)
		adnlOverlay.Close()
	} else {
		pooled.adnlOverlayRefs[adnlOverlay] = refs - 1
	}
	if refs := pooled.rldpOverlayRefs[rldpOverlay]; refs <= 1 {
		delete(pooled.rldpOverlayRefs, rldpOverlay)
		rldpOverlay.Close()
	} else {
		pooled.rldpOverlayRefs[rldpOverlay] = refs - 1
	}

	if pooled.refs > 0 {
		pooled.refs--
	}
	if fixedMember {
		pooled.fixedMemberRefs--
		if pooled.fixedMemberRefs == 0 {
			// The transport stays pooled for reuse by public overlays, so the
			// raised private-overlay MTU is restored to the bounded default.
			pooled.baseRLDP.SetMaxUnexpectedTransferSize(0)
		}
	}
	var stale []*pooledPeer
	if pooledPeerIdle(pooled) && p.peers[pooled.id] == pooled {
		now := time.Now()
		pooled.touch(now)
		stale = p.pruneOverCapLocked(now)
	}
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
}

func (p *peerPool) closeIfUnused(pooled *pooledPeer) {
	var closeBase *pooledPeer

	p.mx.Lock()
	if pooled.refs == 0 && len(pooled.adnlOverlayRefs) == 0 && len(pooled.rldpOverlayRefs) == 0 && p.peers[pooled.id] == pooled {
		delete(p.peers, pooled.id)
		closeBase = pooled
	}
	p.mx.Unlock()

	if closeBase != nil {
		closeBase.close()
	}
}

func (p *peerPool) retain(pooled *pooledPeer) bool {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.peers[pooled.id] != pooled {
		return false
	}
	pooled.refs++
	return true
}

func (p *peerPool) retainByID(peerID PeerID) *pooledPeer {
	p.mx.Lock()
	defer p.mx.Unlock()

	pooled := p.peers[peerID]
	if pooled == nil {
		return nil
	}
	if pooledPeerClosed(pooled) {
		delete(p.peers, peerID)
		return nil
	}
	pooled.refs++
	return pooled
}

// pooledPeerClosed reports whether the pooled transport has already been
// closed underneath (the disconnect cascade prunes the pool asynchronously,
// so a closed entry can linger in the map for a moment).
func pooledPeerClosed(pooled *pooledPeer) bool {
	select {
	case <-pooled.adnl.GetCloserCtx().Done():
		return true
	default:
		return false
	}
}

func (p *peerPool) releaseRetained(pooled *pooledPeer) {
	p.mx.Lock()
	if pooled.refs > 0 {
		pooled.refs--
	}
	var stale []*pooledPeer
	if pooledPeerIdle(pooled) && p.peers[pooled.id] == pooled {
		now := time.Now()
		pooled.touch(now)
		stale = p.pruneOverCapLocked(now)
	}
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
}

func (p *peerPool) size() int {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return len(p.peers)
}

// hasTransport reports whether the pool already holds an open transport for a
// peer. Such a peer needs no address: acquirePeerEndpoint reuses a pooled
// transport by id and never looks at the endpoint it was handed. This is what
// makes an address-less directory row reachable - inbound peers on a public
// overlay never join the roster, so their row carries whatever gossip said
// about them and no address at all.
func (p *peerPool) hasTransport(id PeerID) bool {
	p.mx.RLock()
	defer p.mx.RUnlock()

	pooled := p.peers[id]
	return pooled != nil && !pooledPeerClosed(pooled)
}

func (p *peerPool) pruneIdle(now time.Time) int {
	p.mx.Lock()
	stale := p.sweepIdleLocked(now)
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
	return len(stale)
}

// pruneOverCapLocked is the release-path variant: it only enforces the hard
// cap, and only once the pool is over it, so releasing a peer never pays for a
// per-peer Stats() scan. Expiring by TTL is the periodic sweep's job.
func (p *peerPool) pruneOverCapLocked(now time.Time) []*pooledPeer {
	if len(p.peers) <= peerPoolMaxIdle {
		return nil
	}
	return p.sweepIdleLocked(now)
}

// sweepIdleLocked drops pooled transports nothing references any more: first
// everything idle past the TTL, then the least recently used above the cap.
//
// The TTL used to be gated behind "pool larger than the cap", which made it
// inert in practice: the pool only had to stay under peerPoolMaxIdle, so
// unreferenced peers accumulated for as long as the node ran. Re-dialling a
// peer when it is needed again is cheaper than holding its ADNL channel, its
// RLDP client and that client's two goroutines indefinitely. This mirrors
// sweepIdleQUICPaths, so both transports of a peer now decay together the way
// quicIdleTTL already promises.
//
// Only peers with no refs and no overlay attachments are candidates, and
// lastUsed also reflects real ADNL packet activity, so a transport still
// carrying traffic is kept even when nothing holds a reference to it.
func (p *peerPool) sweepIdleLocked(now time.Time) []*pooledPeer {
	cutoff := now.Add(-peerPoolIdleTTL)
	var stale []*pooledPeer
	idle := make([]idlePooledPeer, 0, len(p.peers))

	for id, pooled := range p.peers {
		if pooledPeerClosed(pooled) {
			delete(p.peers, id)
			continue
		}
		if !pooledPeerIdle(pooled) {
			continue
		}
		lastUsed := pooled.lastUsed()
		if !lastUsed.After(cutoff) {
			delete(p.peers, id)
			stale = append(stale, pooled)
			continue
		}
		idle = append(idle, idlePooledPeer{peer: pooled, lastUsed: lastUsed})
	}

	if excess := len(idle) - peerPoolMaxIdle; excess > 0 {
		sort.Slice(idle, func(i, j int) bool {
			return idle[i].lastUsed.Before(idle[j].lastUsed)
		})
		for _, candidate := range idle[:excess] {
			delete(p.peers, candidate.peer.id)
			stale = append(stale, candidate.peer)
		}
	}
	return stale
}

func pooledPeerIdle(pooled *pooledPeer) bool {
	return pooled.refs == 0 && len(pooled.adnlOverlayRefs) == 0 && len(pooled.rldpOverlayRefs) == 0
}

func (p *pooledPeer) close() {
	if p.rldp != nil {
		p.rldp.Close()
		return
	}
	if p.adnl != nil {
		p.adnl.Close()
	}
}

func (p *peerPool) overlaySnapshot() []*pooledPeer {
	p.mx.RLock()
	defer p.mx.RUnlock()

	list := make([]*pooledPeer, 0, len(p.peers))
	for _, peer := range p.peers {
		if len(peer.adnlOverlayRefs) == 0 {
			continue
		}
		list = append(list, peer)
	}
	return list
}
