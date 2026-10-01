package p2p

import (
	"context"
	"sort"
	"time"
)

// Outbound QUIC paths never expired on their own: the transport keep-alive (5s)
// is shorter than its idle timeout (15s), and nothing in the node closed a path
// explicitly, so the set converged to "every peer we ever sent to" — measured at
// ~1300 live paths (~5.2k goroutines) on a production node. The sweep bounds it
// to the working set, mirroring the ADNL side (C++ MARK_IDLE_TIMEOUT / peer-pair
// LRU) rather than inventing a new policy.
const (
	quicIdleSweepInterval = 30 * time.Second
	// quicIdleTTL matches peerPoolIdleTTL so the QUIC and ADNL transports of a
	// peer decay together.
	quicIdleTTL = peerPoolIdleTTL
	// maxOutboundQUICPaths bounds the dialed set even when everything looks
	// recently used: plumtree needs at most plumtreeActiveNeighbourLimit peers
	// per subscribed overlay, so this leaves a wide margin.
	maxOutboundQUICPaths = 256
)

func (n *Node) runQUICIdleSweepLoop(ctx context.Context) {
	ticker := time.NewTicker(quicIdleSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n.sweepIdleQUICPaths(now)
			// Rides the same ticker: it reclaims per-peer bookkeeping nothing
			// else frees, and is too cheap to deserve a goroutine of its own.
			n.sweepPeerRoutes()
		}
	}
}

// sweepIdleQUICPaths closes outbound paths that no subsystem is using: idle past
// the TTL, or the least recently used above the cap. Pinned peers (plumtree
// members that still carry broadcasts for us) are never swept — dropping those
// is exactly the broadcast-famine mechanism this node was suffering from.
func (n *Node) sweepIdleQUICPaths(now time.Time) int {
	pinned := n.pinnedQUICPeers()

	n.quicPeersMx.RLock()
	candidates := make([]*authenticatedQUICPeer, 0, len(n.quicPeers))
	outbound := 0
	for id, peer := range n.quicPeers {
		if !peer.outbound {
			continue
		}
		outbound++
		if _, ok := pinned[id]; ok {
			continue
		}
		candidates = append(candidates, peer)
	}
	n.quicPeersMx.RUnlock()

	victims := selectIdleQUICPaths(candidates, outbound, now)
	if len(victims) == 0 {
		return 0
	}

	// Closed outside every lock: the close cascades through the gateway
	// disconnect handler back into removeQUICPeer (quicPeersMx) and on into the
	// plumtree engine and subscription locks.
	closed := 0
	for _, victim := range victims {
		if n.dropQUICPath(victim) {
			closed++
		}
	}
	if closed > 0 {
		n.log.Debug().
			Int("closed", closed).
			Int("outbound_paths", outbound).
			Msg("closed idle outbound QUIC paths")
	}
	return closed
}

// selectIdleQUICPaths returns the paths to close: everything idle past the TTL,
// plus the least recently used ones needed to get back under the cap.
func selectIdleQUICPaths(candidates []*authenticatedQUICPeer, outbound int, now time.Time) []*authenticatedQUICPeer {
	cutoff := now.Add(-quicIdleTTL)
	victims := make([]*authenticatedQUICPeer, 0, len(candidates))
	remaining := make([]*authenticatedQUICPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer.lastActive().Before(cutoff) {
			victims = append(victims, peer)
			continue
		}
		remaining = append(remaining, peer)
	}

	over := outbound - len(victims) - maxOutboundQUICPaths
	if over <= 0 {
		return victims
	}
	sortQUICPathsByIdle(remaining)
	if over > len(remaining) {
		over = len(remaining)
	}
	return append(victims, remaining[:over]...)
}

func sortQUICPathsByIdle(peers []*authenticatedQUICPeer) {
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].lastActive().Before(peers[j].lastActive())
	})
}

func (n *Node) dropQUICPath(peer *authenticatedQUICPeer) bool {
	n.quicPeersMx.Lock()
	if current := n.quicPeers[peer.id]; current != peer {
		n.quicPeersMx.Unlock()
		return false
	}
	delete(n.quicPeers, peer.id)
	n.quicPeersMx.Unlock()

	peer.peer.Close()
	return true
}

// pinnedQUICPeers are the peers some subsystem still needs a QUIC path to:
// plumtree members of any subscription. They are exempt from the sweep.
func (n *Node) pinnedQUICPeers() map[PeerID]struct{} {
	pinned := map[PeerID]struct{}{}
	for _, sub := range n.subscriptionsSnapshot() {
		for _, id := range sub.plumtreePinnedPeers() {
			pinned[id] = struct{}{}
		}
	}
	return pinned
}

// maxPeerRoutes bounds the node-wide route table. A route is ~150 bytes and one
// is filed for every peer id that ever connected - inbound included, where the
// only cost to the other side is an ADNL or QUIC handshake from a fresh key - so
// without a bound the table is a slow, unbounded leak with a cheap accelerator.
// The bound is far above any real working set: a node knows maxPeersPerOverlay
// per overlay and dials a few hundred paths.
const maxPeerRoutes = 20000

// sweepPeerRoutes drops routes no live transport holds, once the table is over
// its bound. Liveness is derived from the transports themselves rather than
// reference-counted: routes are handed out in two places and released in a
// dozen, and a single missed release would pin a route forever while a double
// release would drop one out from under a live peer.
func (n *Node) sweepPeerRoutes() int {
	if n.peerRoutes.Size() <= maxPeerRoutes {
		return 0
	}

	held := n.routesHeldByTransports()
	return n.peerRoutes.Sweep(held, maxPeerRoutes)
}

// routesHeldByTransports takes the two locks one after the other, never nested,
// so it cannot participate in a lock order at all.
func (n *Node) routesHeldByTransports() map[PeerID]struct{} {
	n.pool.mx.RLock()
	held := make(map[PeerID]struct{}, len(n.pool.peers))
	for id := range n.pool.peers {
		held[id] = struct{}{}
	}
	n.pool.mx.RUnlock()

	n.quicPeersMx.RLock()
	for id := range n.quicPeers {
		held[id] = struct{}{}
	}
	n.quicPeersMx.RUnlock()

	return held
}
