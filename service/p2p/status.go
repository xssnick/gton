package p2p

import (
	"sort"
	"time"

	storage2 "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/ton"
)

// statusPeersLocked lists the peers rendered in the status table. Custom
// fixed overlays list every member: a probed peer that never delivered
// anything (frozen from the start) is never promoted into neighbours and
// would otherwise be invisible exactly when its diagnostics matter most.
func (s *overlaySubscription) statusPeersLocked() []*overlayPeer {
	if s.spec.statusListsWholeRoster() {
		peers := make([]*overlayPeer, 0, len(s.peers))
		for _, peer := range s.peers {
			peers = append(peers, peer)
		}
		return peers
	}

	peers := make([]*overlayPeer, 0, len(s.neighbours))
	for _, id := range s.neighbours {
		// A neighbour whose roster row is gone must not reach the callers, who
		// dereference these entries; see the dangling-neighbour handling in
		// pruneNeighboursLocked.
		if peer := s.peers[id]; peer != nil {
			peers = append(peers, peer)
		}
	}
	return peers
}

func adnlChannelStateLabel(state adnl.PeerChannelState) string {
	switch state {
	case adnl.PeerChannelStateNone:
		return "none"
	case adnl.PeerChannelStatePending:
		return "pending"
	case adnl.PeerChannelStateReady:
		return "ready"
	default:
		return "unknown"
	}
}

type StatusSnapshot struct {
	ListenAddr            string
	QUICPeers             int
	QUICPeersAccepted     uint64
	Offline               bool
	OfflineReason         string
	LatestMasterchain     *ton.BlockIDExt
	LatestBasechain       *ton.BlockIDExt
	LatestBasechainShards []ton.BlockIDExt
	Overlays              []OverlayStatusSnapshot
	FECReceivers          []FECReceiverStatusSnapshot
	Queues                []QueueStatusSnapshot
	Rebroadcast           []RebroadcastStatusSnapshot
	Broadcasts            []BroadcastStatusSnapshot
	BroadcastDrops        []BroadcastDropStatusSnapshot
	Plumtree              []PlumtreeStatusSnapshot
}

type OverlayStatusSnapshot struct {
	Name             string
	KnownPeers       int
	AliveKnownPeers  int
	ActiveNeighbours int
	AliveNeighbours  int
	FixedProbes      bool
	SoftRecoveries   uint64
	HardRecoveries   uint64
	Neighbours       []NeighbourStatusSnapshot
}

type NeighbourStatusSnapshot struct {
	ID               string
	Addr             string
	Alive            bool
	LastReceiveAt    time.Time
	LastSuccessAt    time.Time
	FailedQueries    uint64
	Unreliability    float64
	LastPongAt       time.Time
	ProbeFailures    uint32
	ADNLLastInAt     time.Time
	ADNLChannelState string
}

type QueueStatusSnapshot struct {
	Name     string
	Items    int
	Bytes    int64
	MaxItems int
	MaxBytes int64
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
	SimpleRelaySentTotal    uint64
	SimpleRelayFailedTotal  uint64
	FECRelaySentTotal       uint64
	FECRelayFailedTotal     uint64
}

type BroadcastStatusSnapshot struct {
	Direction string
	Overlay   string
	Kind      string
	Delivery  Delivery
	Reason    string
	Count     uint64
}

type BroadcastDropStatusSnapshot struct {
	Overlay string
	Kind    string
	Reason  string
	Count   uint64
}

type PlumtreeStatusSnapshot struct {
	Overlay string

	DirectParts   uint64
	RecoveryParts uint64

	SimpleMessages      uint64
	FECMessages         uint64
	IHaveMessages       uint64
	PruneMessages       uint64
	UsefulMessages      uint64
	StatsPushMessages   uint64
	RepairQueryMessages uint64
}

type broadcastStatKey struct {
	direction string
	overlay   string
	kind      string
	delivery  Delivery
	reason    string
}

type fecReceiverCounterSnapshot struct {
	dropped            uint64
	evicted            uint64
	completed          uint64
	deliveredCacheHits uint64
	simpleRelaySent    uint64
	simpleRelayFailed  uint64
	fecRelaySent       uint64
	fecRelayFailed     uint64
}

func (n *Node) StatusSnapshot() StatusSnapshot {
	subscriptions := n.subscriptionsSnapshot()
	snapshot := StatusSnapshot{
		ListenAddr:    n.listenAddr,
		Offline:       n.IsOffline(),
		OfflineReason: n.OfflineReason(),
		Overlays:      make([]OverlayStatusSnapshot, 0, len(subscriptions)),
	}

	n.quicPeersMx.RLock()
	snapshot.QUICPeers = len(n.quicPeers)
	n.quicPeersMx.RUnlock()
	snapshot.QUICPeersAccepted = n.quicPeersAccepted.Load()

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
	snapshot.BroadcastDrops = n.broadcastDropStatusSnapshot()
	snapshot.Plumtree = plumtreeStatusSnapshot(subscriptions)

	return snapshot
}

func plumtreeStatusSnapshot(
	subscriptions []*overlaySubscription,
) []PlumtreeStatusSnapshot {
	byOverlay := make(
		map[string]*PlumtreeStatusSnapshot,
		len(subscriptions),
	)
	for _, sub := range subscriptions {
		if sub.plumtree == nil {
			continue
		}

		overlay := sub.spec.Name
		snapshot := byOverlay[overlay]
		if snapshot == nil {
			snapshot = &PlumtreeStatusSnapshot{Overlay: overlay}
			byOverlay[overlay] = snapshot
		}

		telemetry := sub.plumtree.stats.telemetry.snapshot()
		snapshot.DirectParts += telemetry.receivedParts[plumtreePartSourceDirect]
		snapshot.RecoveryParts += telemetry.receivedParts[plumtreePartSourceRecovery]
		snapshot.SimpleMessages += telemetry.receivedMessages[plumtreeMessageSimple]
		snapshot.FECMessages += telemetry.receivedMessages[plumtreeMessageFEC]
		snapshot.IHaveMessages += telemetry.receivedMessages[plumtreeMessageIHave]
		snapshot.PruneMessages += telemetry.receivedMessages[plumtreeMessagePrune]
		snapshot.UsefulMessages += telemetry.receivedMessages[plumtreeMessageUseful]
		snapshot.StatsPushMessages += telemetry.receivedMessages[plumtreeMessageStatsPush]
		snapshot.RepairQueryMessages += telemetry.receivedMessages[plumtreeMessageRepairQuery]
	}

	result := make([]PlumtreeStatusSnapshot, 0, len(byOverlay))
	for _, snapshot := range byOverlay {
		result = append(result, *snapshot)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Overlay < result[j].Overlay
	})

	return result
}

func (n *Node) noteBroadcast(
	direction string,
	overlay string,
	kind string,
	delivery Delivery,
) {
	n.noteBroadcastWithReason(direction, overlay, kind, delivery, "")
}

func (n *Node) noteBroadcastDrop(overlay, kind, reason string) {
	n.noteBroadcastWithReason("dropped", overlay, kind, "", reason)
}

func (n *Node) noteBroadcastWithReason(
	direction string,
	overlay string,
	kind string,
	delivery Delivery,
	reason string,
) {
	if direction == "" {
		direction = "unknown"
	}
	if overlay == "" {
		overlay = "unknown"
	}
	if kind == "" {
		kind = "unknown"
	}
	if reason == "" && direction == "dropped" {
		reason = "unknown"
	}

	key := broadcastStatKey{
		direction: direction,
		overlay:   overlay,
		kind:      kind,
		delivery:  delivery,
		reason:    reason,
	}

	n.broadcastStatsMx.Lock()
	n.broadcastStats[key]++
	n.broadcastStatsMx.Unlock()
}

func (n *Node) broadcastStatusSnapshot() []BroadcastStatusSnapshot {
	n.broadcastStatsMx.Lock()
	defer n.broadcastStatsMx.Unlock()

	stats := make([]BroadcastStatusSnapshot, 0, len(n.broadcastStats))
	for key, count := range n.broadcastStats {
		if key.direction == "dropped" {
			continue
		}
		stats = append(stats, BroadcastStatusSnapshot{
			Direction: key.direction,
			Overlay:   key.overlay,
			Kind:      key.kind,
			Delivery:  key.delivery,
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
		if stats[i].Delivery != stats[j].Delivery {
			return stats[i].Delivery < stats[j].Delivery
		}
		return stats[i].Kind < stats[j].Kind
	})
	return stats
}

func (n *Node) broadcastDropStatusSnapshot() []BroadcastDropStatusSnapshot {
	n.broadcastStatsMx.Lock()
	defer n.broadcastStatsMx.Unlock()

	stats := make([]BroadcastDropStatusSnapshot, 0, len(n.broadcastStats))
	for key, count := range n.broadcastStats {
		if key.direction != "dropped" {
			continue
		}
		stats = append(stats, BroadcastDropStatusSnapshot{
			Overlay: key.overlay,
			Kind:    key.kind,
			Reason:  key.reason,
			Count:   count,
		})
	}

	sort.SliceStable(stats, func(i, j int) bool {
		if stats[i].Overlay != stats[j].Overlay {
			return stats[i].Overlay < stats[j].Overlay
		}
		if stats[i].Kind != stats[j].Kind {
			return stats[i].Kind < stats[j].Kind
		}
		return stats[i].Reason < stats[j].Reason
	})
	return stats
}

func (n *Node) queueStatusSnapshot() []QueueStatusSnapshot {
	queues := make([]QueueStatusSnapshot, 0, 4)
	queues = append(queues, n.eventQueue.StatusSnapshot("broadcast"))
	local, regular := n.peerRebroadcastQueueStatusSnapshot()
	queues = append(queues, regular, local)
	if twoStep, ok := n.twoStepQueueStatusSnapshot(); ok {
		queues = append(queues, twoStep)
	}
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

func (n *Node) twoStepQueueStatusSnapshot() (QueueStatusSnapshot, bool) {
	total := QueueStatusSnapshot{Name: twoStepRebroadcastQueueName}
	found := false
	for _, sub := range n.subscriptionsSnapshot() {
		next, ok := sub.twoStepQueueStatusSnapshot()
		if !ok {
			continue
		}
		found = true
		addQueueStatus(&total, next)
	}
	return total, found
}

func addQueueStatus(total *QueueStatusSnapshot, next QueueStatusSnapshot) {
	total.Items += next.Items
	total.Bytes += next.Bytes
	total.MaxItems += next.MaxItems
	total.MaxBytes += next.MaxBytes
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
	seen := map[string]struct{}{}

	for _, sub := range subscriptions {
		overlayName := sub.spec.Name

		snapshot := byOverlay[overlayName]
		if snapshot == nil {
			snapshot = &FECReceiverStatusSnapshot{Overlay: overlayName}
			byOverlay[overlayName] = snapshot
		}

		stats := sub.broadcastReceiver.FECBroadcastStats()
		addFECReceiverSnapshotStats(snapshot, stats)

		seen[overlayName] = struct{}{}
		n.addFECReceiverCounterDeltas(overlayName, stats)
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
		snapshot.SimpleRelaySentTotal = totals.simpleRelaySent
		snapshot.SimpleRelayFailedTotal = totals.simpleRelayFailed
		snapshot.FECRelaySentTotal = totals.fecRelaySent
		snapshot.FECRelayFailedTotal = totals.fecRelayFailed
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

func addFECReceiverSnapshotStats(snapshot *FECReceiverStatusSnapshot, stats overlay.FECBroadcastStats) {
	snapshot.ActiveStreams += stats.ActiveStreams
	snapshot.ActiveBytes += stats.ActiveBytes
	snapshot.DeliveredBroadcasts += stats.DeliveredBroadcasts
}

func (n *Node) addFECReceiverCounterDeltas(overlayName string, stats overlay.FECBroadcastStats) {
	n.fecReceiverStatsMx.Lock()
	defer n.fecReceiverStatsMx.Unlock()

	current := fecReceiverCounterSnapshot{
		dropped:            stats.DroppedTotal,
		evicted:            stats.EvictedTotal,
		completed:          stats.CompletedTotal,
		deliveredCacheHits: stats.DeliveredCacheHitsTotal,
		simpleRelaySent:    stats.SimpleRelaySentTotal,
		simpleRelayFailed:  stats.SimpleRelayFailedTotal,
		fecRelaySent:       stats.FECRelaySentTotal,
		fecRelayFailed:     stats.FECRelayFailedTotal,
	}
	last := n.fecReceiverLast[overlayName]
	totals := n.fecReceiverTotals[overlayName]
	totals.dropped += counterDelta(last.dropped, current.dropped)
	totals.evicted += counterDelta(last.evicted, current.evicted)
	totals.completed += counterDelta(last.completed, current.completed)
	totals.deliveredCacheHits += counterDelta(last.deliveredCacheHits, current.deliveredCacheHits)
	totals.simpleRelaySent += counterDelta(last.simpleRelaySent, current.simpleRelaySent)
	totals.simpleRelayFailed += counterDelta(last.simpleRelayFailed, current.simpleRelayFailed)
	totals.fecRelaySent += counterDelta(last.fecRelaySent, current.fecRelaySent)
	totals.fecRelayFailed += counterDelta(last.fecRelayFailed, current.fecRelayFailed)

	n.fecReceiverLast[overlayName] = current
	n.fecReceiverTotals[overlayName] = totals
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
		Name:           s.spec.Name,
		FixedProbes:    s.spec.runsFixedPeerProbes(),
		SoftRecoveries: s.softRecoveries,
		HardRecoveries: s.hardRecoveries,
		Neighbours:     make([]NeighbourStatusSnapshot, 0, len(s.neighbours)),
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

	for _, peer := range s.statusPeersLocked() {
		stats := peer.statsSnapshot()
		neighbour := NeighbourStatusSnapshot{
			ID:            peer.id.String(),
			Addr:          peer.addr,
			Alive:         stats.alive,
			LastReceiveAt: stats.lastReceiveAt,
			LastSuccessAt: stats.lastSuccessAt,
			FailedQueries: stats.failedQueries,
			Unreliability: stats.unreliability,
			LastPongAt:    stats.lastPongAt,
			ProbeFailures: stats.probeFailures,
		}
		if adnlStats, ok := peer.adnlPairStats(); ok {
			neighbour.ADNLLastInAt = adnlStats.Inbound.LastPacketAt
			neighbour.ADNLChannelState = adnlChannelStateLabel(adnlStats.Channel.State)
		}
		snapshot.Neighbours = append(snapshot.Neighbours, neighbour)
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
