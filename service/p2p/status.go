package p2p

import (
	"sort"
	"time"

	storage2 "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

type StatusSnapshot struct {
	ListenAddr            string
	LatestMasterchain     *ton.BlockIDExt
	LatestBasechain       *ton.BlockIDExt
	LatestBasechainShards []ton.BlockIDExt
	Overlays              []OverlayStatusSnapshot
	FECReceivers          []FECReceiverStatusSnapshot
	Queues                []QueueStatusSnapshot
	Rebroadcast           []RebroadcastStatusSnapshot
	Broadcasts            []BroadcastStatusSnapshot
}

type OverlayStatusSnapshot struct {
	Name             string
	KnownPeers       int
	AliveKnownPeers  int
	ActiveNeighbours int
	AliveNeighbours  int
	Neighbours       []NeighbourStatusSnapshot
}

type NeighbourStatusSnapshot struct {
	ID            string
	Addr          string
	Alive         bool
	LastSuccessAt time.Time
	FailedQueries uint64
	Unreliability float64
}

type QueueStatusSnapshot struct {
	Name     string
	Items    int
	Bytes    int64
	MaxItems int
	MaxBytes int64
	Pushed   uint64
	Dropped  uint64
}

type RebroadcastStatusSnapshot struct {
	Queue   string
	Sent    uint64
	Dropped uint64
}

type FECReceiverStatusSnapshot struct {
	Overlay                 string
	ActiveStreams           int
	ActiveBytes             int64
	DeliveredBroadcasts     int
	DroppedTotal            uint64
	EvictedTotal            uint64
	CompletedTotal          uint64
	DeliveredCacheHitsTotal uint64
}

type BroadcastStatusSnapshot struct {
	Direction string
	Overlay   string
	Kind      string
	Count     uint64
}

type broadcastStatKey struct {
	direction string
	overlay   string
	kind      string
}

type fecReceiverPeerKey struct {
	overlay string
	peer    string
}

type fecReceiverCounterSnapshot struct {
	dropped            uint64
	evicted            uint64
	completed          uint64
	deliveredCacheHits uint64
}

func (n *Node) StatusSnapshot() StatusSnapshot {
	subscriptions := n.subscriptionsSnapshot()
	snapshot := StatusSnapshot{
		ListenAddr: n.listenAddr,
		Overlays:   make([]OverlayStatusSnapshot, 0, len(subscriptions)),
	}

	n.latestBlocksMx.RLock()
	if n.observedMasterchain != nil {
		block := *n.observedMasterchain
		snapshot.LatestMasterchain = &block
	}
	if n.latestBasechain != nil {
		block := *n.latestBasechain
		snapshot.LatestBasechain = &block
	}
	for _, block := range n.latestBasechainShards {
		snapshot.LatestBasechainShards = append(snapshot.LatestBasechainShards, block)
	}
	n.latestBlocksMx.RUnlock()

	sort.Slice(snapshot.LatestBasechainShards, func(i, j int) bool {
		left := storage2.ShardKeyFromBlock(snapshot.LatestBasechainShards[i])
		right := storage2.ShardKeyFromBlock(snapshot.LatestBasechainShards[j])
		if left.Workchain != right.Workchain {
			return left.Workchain < right.Workchain
		}
		return uint64(left.Shard) < uint64(right.Shard)
	})

	for _, sub := range subscriptions {
		snapshot.Overlays = append(snapshot.Overlays, sub.statusSnapshot())
	}
	sort.SliceStable(snapshot.Overlays, func(i, j int) bool {
		return snapshot.Overlays[i].Name < snapshot.Overlays[j].Name
	})
	snapshot.FECReceivers = n.fecReceiverStatusSnapshot(subscriptions)
	snapshot.Queues = n.queueStatusSnapshot()
	snapshot.Rebroadcast = n.rebroadcastStatusSnapshot()
	snapshot.Broadcasts = n.broadcastStatusSnapshot()

	return snapshot
}

func (n *Node) noteBroadcast(direction, overlay, kind string) {
	if direction == "" {
		direction = "unknown"
	}
	if overlay == "" {
		overlay = "unknown"
	}
	if kind == "" {
		kind = "unknown"
	}

	key := broadcastStatKey{
		direction: direction,
		overlay:   overlay,
		kind:      kind,
	}

	n.broadcastStatsMx.Lock()
	if n.broadcastStats == nil {
		n.broadcastStats = map[broadcastStatKey]uint64{}
	}
	n.broadcastStats[key]++
	n.broadcastStatsMx.Unlock()
}

func (n *Node) broadcastStatusSnapshot() []BroadcastStatusSnapshot {
	n.broadcastStatsMx.Lock()
	defer n.broadcastStatsMx.Unlock()

	stats := make([]BroadcastStatusSnapshot, 0, len(n.broadcastStats))
	for key, count := range n.broadcastStats {
		stats = append(stats, BroadcastStatusSnapshot{
			Direction: key.direction,
			Overlay:   key.overlay,
			Kind:      key.kind,
			Count:     count,
		})
	}

	sort.SliceStable(stats, func(i, j int) bool {
		if stats[i].Direction != stats[j].Direction {
			return stats[i].Direction < stats[j].Direction
		}
		if stats[i].Overlay != stats[j].Overlay {
			return stats[i].Overlay < stats[j].Overlay
		}
		return stats[i].Kind < stats[j].Kind
	})
	return stats
}

func (n *Node) queueStatusSnapshot() []QueueStatusSnapshot {
	queues := make([]QueueStatusSnapshot, 0, 3)
	if n.eventQueue != nil {
		queues = append(queues, n.eventQueue.StatusSnapshot("broadcast"))
	}
	local, regular := n.peerRebroadcastQueueStatusSnapshot()
	queues = append(queues, regular, local)
	return queues
}

func (n *Node) peerRebroadcastQueueStatusSnapshot() (QueueStatusSnapshot, QueueStatusSnapshot) {
	regular := QueueStatusSnapshot{Name: "rebroadcast"}
	local := QueueStatusSnapshot{Name: "local_rebroadcast"}

	for _, sub := range n.subscriptionsSnapshot() {
		for _, peer := range sub.peersSnapshot() {
			localPeer, regularPeer, ok := peer.rebroadcastQueueSnapshots()
			if !ok {
				continue
			}
			addQueueStatus(&local, localPeer)
			addQueueStatus(&regular, regularPeer)
		}
	}

	return local, regular
}

func addQueueStatus(total *QueueStatusSnapshot, next QueueStatusSnapshot) {
	total.Items += next.Items
	total.Bytes += next.Bytes
	total.MaxItems += next.MaxItems
	total.MaxBytes += next.MaxBytes
	total.Pushed += next.Pushed
	total.Dropped += next.Dropped
}

func (n *Node) rebroadcastStatusSnapshot() []RebroadcastStatusSnapshot {
	return []RebroadcastStatusSnapshot{
		{
			Queue:   "rebroadcast",
			Sent:    n.peerRebroadcastSent.Load(),
			Dropped: n.peerRebroadcastDropped.Load(),
		},
		{
			Queue:   "local_rebroadcast",
			Sent:    n.localRebroadcastSent.Load(),
			Dropped: n.localRebroadcastDropped.Load(),
		},
	}
}

func (n *Node) fecReceiverStatusSnapshot(subscriptions []*overlaySubscription) []FECReceiverStatusSnapshot {
	byOverlay := map[string]*FECReceiverStatusSnapshot{}
	seen := map[fecReceiverPeerKey]struct{}{}

	for _, sub := range subscriptions {
		sub.mx.Lock()
		overlayName := sub.spec.Name
		peers := make([]*overlayPeer, 0, len(sub.peers))
		for _, peer := range sub.peers {
			peers = append(peers, peer)
		}
		sub.mx.Unlock()

		snapshot := byOverlay[overlayName]
		if snapshot == nil {
			snapshot = &FECReceiverStatusSnapshot{Overlay: overlayName}
			byOverlay[overlayName] = snapshot
		}

		for _, peer := range peers {
			if peer.overlay == nil {
				continue
			}

			stats := peer.overlay.FECBroadcastStats()
			snapshot.ActiveStreams += stats.ActiveStreams
			snapshot.ActiveBytes += stats.ActiveBytes
			snapshot.DeliveredBroadcasts += stats.DeliveredBroadcasts

			key := fecReceiverPeerKey{overlay: overlayName, peer: peer.id}
			seen[key] = struct{}{}
			n.addFECReceiverCounterDeltas(key, stats.DroppedTotal, stats.EvictedTotal, stats.CompletedTotal, stats.DeliveredCacheHitsTotal)
		}
	}

	n.fecReceiverStatsMx.Lock()
	for key := range n.fecReceiverLast {
		if _, ok := seen[key]; !ok {
			delete(n.fecReceiverLast, key)
		}
	}
	for overlay, totals := range n.fecReceiverTotals {
		snapshot := byOverlay[overlay]
		if snapshot == nil {
			snapshot = &FECReceiverStatusSnapshot{Overlay: overlay}
			byOverlay[overlay] = snapshot
		}
		snapshot.DroppedTotal = totals.dropped
		snapshot.EvictedTotal = totals.evicted
		snapshot.CompletedTotal = totals.completed
		snapshot.DeliveredCacheHitsTotal = totals.deliveredCacheHits
	}
	n.fecReceiverStatsMx.Unlock()

	stats := make([]FECReceiverStatusSnapshot, 0, len(byOverlay))
	for _, snapshot := range byOverlay {
		stats = append(stats, *snapshot)
	}
	sort.SliceStable(stats, func(i, j int) bool {
		return stats[i].Overlay < stats[j].Overlay
	})
	return stats
}

func (n *Node) addFECReceiverCounterDeltas(key fecReceiverPeerKey, dropped, evicted, completed, deliveredCacheHits uint64) {
	n.fecReceiverStatsMx.Lock()
	defer n.fecReceiverStatsMx.Unlock()

	if n.fecReceiverLast == nil {
		n.fecReceiverLast = map[fecReceiverPeerKey]fecReceiverCounterSnapshot{}
	}
	if n.fecReceiverTotals == nil {
		n.fecReceiverTotals = map[string]fecReceiverCounterSnapshot{}
	}

	current := fecReceiverCounterSnapshot{
		dropped:            dropped,
		evicted:            evicted,
		completed:          completed,
		deliveredCacheHits: deliveredCacheHits,
	}
	last := n.fecReceiverLast[key]
	totals := n.fecReceiverTotals[key.overlay]
	totals.dropped += counterDelta(last.dropped, current.dropped)
	totals.evicted += counterDelta(last.evicted, current.evicted)
	totals.completed += counterDelta(last.completed, current.completed)
	totals.deliveredCacheHits += counterDelta(last.deliveredCacheHits, current.deliveredCacheHits)

	n.fecReceiverLast[key] = current
	n.fecReceiverTotals[key.overlay] = totals
}

func counterDelta(previous, current uint64) uint64 {
	if current >= previous {
		return current - previous
	}
	return current
}

func (s *overlaySubscription) statusSnapshot() OverlayStatusSnapshot {
	s.mx.Lock()
	defer s.mx.Unlock()

	now := time.Now()
	snapshot := OverlayStatusSnapshot{
		Name:       s.spec.Name,
		Neighbours: make([]NeighbourStatusSnapshot, 0, len(s.neighbours)),
	}

	for _, peer := range s.peers {
		if !peer.isKnownOverlayPeer(now) {
			continue
		}
		snapshot.KnownPeers++
		if peer.isAliveKnownOverlayPeer(now) {
			snapshot.AliveKnownPeers++
		}
	}

	for _, id := range s.neighbours {
		peer := s.peers[id]
		stats := peer.statsSnapshot()
		snapshot.Neighbours = append(snapshot.Neighbours, NeighbourStatusSnapshot{
			ID:            peer.id,
			Addr:          peer.addr,
			Alive:         stats.alive,
			LastSuccessAt: stats.lastSuccessAt,
			FailedQueries: stats.failedQueries,
			Unreliability: stats.unreliability,
		})
		if stats.alive {
			snapshot.AliveNeighbours++
		}
	}
	sort.SliceStable(snapshot.Neighbours, func(i, j int) bool {
		return snapshot.Neighbours[i].Addr < snapshot.Neighbours[j].Addr
	})
	snapshot.ActiveNeighbours = len(snapshot.Neighbours)
	return snapshot
}
