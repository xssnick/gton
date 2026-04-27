package p2p

import (
	"sort"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

type StatusSnapshot struct {
	ListenAddr        string
	LatestMasterchain *ton.BlockIDExt
	LatestBasechain   *ton.BlockIDExt
	Overlays          []OverlayStatusSnapshot
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

func (n *Node) StatusSnapshot() StatusSnapshot {
	subscriptions := n.subscriptionsSnapshot()
	snapshot := StatusSnapshot{
		ListenAddr: n.listenAddr,
		Overlays:   make([]OverlayStatusSnapshot, 0, len(subscriptions)),
	}

	n.latestBlocksMx.RLock()
	if n.latestMasterchain != nil {
		block := *n.latestMasterchain
		snapshot.LatestMasterchain = &block
	}
	if n.latestBasechain != nil {
		block := *n.latestBasechain
		snapshot.LatestBasechain = &block
	}
	n.latestBlocksMx.RUnlock()

	for _, sub := range subscriptions {
		snapshot.Overlays = append(snapshot.Overlays, sub.statusSnapshot())
	}
	sort.SliceStable(snapshot.Overlays, func(i, j int) bool {
		return snapshot.Overlays[i].Name < snapshot.Overlays[j].Name
	})

	return snapshot
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
