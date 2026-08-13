package p2p

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"sort"
	"time"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/ton"
)

const publicRandomQueryFallbackTTL = time.Hour

func (n *Node) querySubscriptionForBlock(block ton.BlockIDExt) (*overlaySubscription, error) {
	if err := sharddomain.Validate(block.Shard); err != nil {
		return nil, err
	}
	if selected := n.readyCustomQuerySubscription(time.Now()); selected != nil {
		return selected, nil
	}
	selected, err := n.readyFastSyncQuerySubscription(block)
	if err != nil {
		return nil, err
	}
	if selected != nil {
		return selected, nil
	}

	return n.subscriptionForBlock(block)
}

// querySubscriptionForHistoricalBlock picks the overlay for bulk historical
// downloads: archive packages, zero state and persistent state.
//
// FastSync is deliberately not a candidate here. Its pool holds exactly one
// validator at a time, which disables everything the archive pool exists for -
// hedging across peers, ranking by measured throughput, and rotating away from
// a slow source. Historical traffic is bulk rather than latency-critical, so
// the public pool's DHT-discovered peer set is the better source even though it
// costs a few seconds of cold start on the first session.
func (n *Node) querySubscriptionForHistoricalBlock(block ton.BlockIDExt) (*overlaySubscription, error) {
	if err := sharddomain.Validate(block.Shard); err != nil {
		return nil, err
	}
	if selected := n.readyCustomQuerySubscription(time.Now()); selected != nil {
		return selected, nil
	}

	historical, err := n.historicalOverlayBlockForDownload(block)
	if err != nil {
		return nil, err
	}
	return n.subscriptionForOverlayBlock(historical)
}

func (n *Node) readyCustomQuerySubscription(now time.Time) *overlaySubscription {
	n.subscriptionsMx.RLock()
	var selected *overlaySubscription
	for _, sub := range n.subscriptions {
		if !sub.spec.preferredAsQuerySource() {
			continue
		}
		if selected != nil && sub.spec.Name >= selected.spec.Name {
			continue
		}
		if !sub.hasReadyQueryPeer(now) {
			continue
		}
		selected = sub
	}
	n.subscriptionsMx.RUnlock()

	return selected
}

func (n *Node) historicalOverlayBlockForDownload(block ton.BlockIDExt) (ton.BlockIDExt, error) {
	if block.Workchain == -1 {
		block.Shard = topShard
		return block, nil
	}

	// Historical state is available through every monitored ancestor overlay.
	// Varying the level matches C++ and avoids concentrating all snapshots on
	// the deepest monitored shard overlay.
	maxDepth := n.monitorMinSplitDepthForWorkchain(block.Workchain)
	selectedDepth := uint32(rand.Int64N(int64(maxDepth) + 1))
	prefixLen, err := sharddomain.PrefixLength(block.Shard)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if prefixLen > selectedDepth {
		block.Shard, err = sharddomain.Ancestor(block.Shard, selectedDepth)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
	}
	return block, nil
}

func (s *overlaySubscription) hasReadyQueryPeer(now time.Time) bool {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.removed || s.inactive {
		return false
	}
	for _, id := range s.spec.QueryAcceptors {
		if id == s.node.localID {
			continue
		}
		peer := s.peers[id]
		if peer != nil && peer.hasOpenConnection() && peer.queryReady(now, 0, 0) {
			return true
		}
	}
	return false
}

func (s *overlaySubscription) customQueryCandidates(
	requiredVersionMajor,
	requiredVersionMinor int32,
) []*overlayPeer {
	now := time.Now()
	s.mx.Lock()
	if s.removed || s.inactive {
		s.mx.Unlock()
		return nil
	}
	peers := make([]*overlayPeer, 0, len(s.spec.QueryAcceptors))
	for _, id := range s.spec.QueryAcceptors {
		if id == s.node.localID {
			continue
		}
		peer := s.peers[id]
		if peer != nil {
			peers = append(peers, peer)
		}
	}
	s.mx.Unlock()

	candidates := peers[:0]
	for _, peer := range peers {
		if peer.hasOpenConnection() &&
			peer.queryReady(now, requiredVersionMajor, requiredVersionMinor) {
			candidates = append(candidates, peer)
		}
	}
	if len(candidates) > 1 {
		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})
	}
	return candidates
}

func (s *overlaySubscription) publicRandomQueryFallback(
	requiredVersionMajor,
	requiredVersionMinor int32,
) []*overlayPeer {
	selected := s.publicRandomQueryFallbackPeer(requiredVersionMajor, requiredVersionMinor)
	if selected == nil {
		return nil
	}
	return []*overlayPeer{selected}
}

func (s *overlaySubscription) publicRandomQueryFallbackPeer(
	requiredVersionMajor,
	requiredVersionMinor int32,
) *overlayPeer {
	if !s.spec.drawsRandomQueryFallback() {
		return nil
	}

	now := time.Now()
	var selected *overlayPeer
	eligible := 0

	s.mx.Lock()
	for _, peer := range s.peers {
		if !peer.hasOpenConnection() ||
			!peer.publicRandomQueryFallbackReady(now, requiredVersionMajor, requiredVersionMinor) {
			continue
		}

		eligible++
		if rand.IntN(eligible) == 0 {
			selected = peer
		}
	}
	s.mx.Unlock()

	return selected
}

func (p *overlayPeer) publicRandomQueryFallbackReady(
	now time.Time,
	requiredVersionMajor,
	requiredVersionMinor int32,
) bool {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	if p.pending || !p.alive ||
		!peerEligibleVersion(p.versionMajor, p.versionMinor, requiredVersionMajor, requiredVersionMinor) {
		return false
	}
	if p.fixedMember {
		return true
	}
	if p.announced == nil {
		return false
	}

	// Public announcements enter the peer roster only after signature
	// verification. pending=false plus alive=true means the peer subsequently
	// answered traffic, matching C++'s verified-alive fallback requirement.
	nodeVersion := int64(p.announced.Version)
	nowUnix := now.Unix()
	return nodeVersion+int64(publicRandomQueryFallbackTTL/time.Second) >= nowUnix &&
		nodeVersion <= nowUnix+int64(overlayFutureSkew/time.Second)
}

func (s *overlaySubscription) startQueryPeerDiscovery(ctx context.Context) {
	if s.spec.seedsFromFixedNodes() {
		s.startSeedFromFixedNodes(ctx)
		return
	}
	s.startSeedFromDHTTarget(ctx, bootstrapDiscoveryTarget)
}

func (s *overlaySubscription) probeCustomQueryPeers(ctx context.Context) {
	if !s.isActive() || !s.spec.SendQueries {
		return
	}
	s.runPeerMaintenance(
		ctx,
		s.customQueryProbeTargets(),
		customQueryProbeMaxPeers,
		s.probeCustomQueryPeer,
	)
}

func (s *overlaySubscription) customQueryProbeTargets() []*overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	acceptors := s.spec.QueryAcceptors
	if len(acceptors) == 0 {
		return nil
	}

	start := 0
	if !s.lastQueryProbed.IsZero() {
		start = sort.Search(len(acceptors), func(i int) bool {
			return bytes.Compare(acceptors[i][:], s.lastQueryProbed[:]) > 0
		})
		if start == len(acceptors) {
			start = 0
		}
	}

	peers := make([]*overlayPeer, 0, minInt(customQueryProbeMaxPeers, len(acceptors)))
	for scanned := 0; scanned < len(acceptors) && len(peers) < customQueryProbeMaxPeers; scanned++ {
		id := acceptors[(start+scanned)%len(acceptors)]
		s.lastQueryProbed = id
		if id == s.node.localID {
			continue
		}
		if peer := s.peers[id]; peer != nil {
			peers = append(peers, peer)
		}
	}
	return peers
}

func (s *overlaySubscription) probeCustomQueryPeer(ctx context.Context, peer *overlayPeer) {
	queryCtx, cancel := context.WithTimeout(ctx, customQueryProbeTimeout)
	defer cancel()

	startedAt := time.Now()
	var capabilities Capabilities
	err := peer.queryTransport.Query(
		queryCtx,
		customQueryProbeMaxAnswer,
		GetCapabilities{},
		&capabilities,
	)
	if err != nil {
		changed := peer.queryProbeFailed()
		if errors.Is(err, adnl.ErrPeerConnClosed) || !peer.hasOpenConnection() {
			s.removePeerIfCurrent(peer)
			return
		}
		if changed {
			s.notifyQueryReadinessChanged(peer)
		}
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Msg("custom overlay capabilities query failed")
		return
	}

	promoted, becameReady := peer.queryProbeSuccess(capabilities, time.Since(startedAt))
	if promoted || becameReady {
		s.peerPromoted(peer)
	}
}

func (s *overlaySubscription) notifyQueryReadinessChanged(peer *overlayPeer) {
	s.mx.Lock()
	if s.peers[peer.id] == peer {
		s.notifyPeersChangedLocked()
	}
	s.mx.Unlock()
}
