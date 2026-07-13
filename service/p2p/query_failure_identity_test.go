package p2p

import (
	"testing"

	"github.com/xssnick/tonutils-go/adnl"
)

func TestHandlePeerQueryFailureClosedConnectionUsesPeerIdentity(t *testing.T) {
	tests := []struct {
		name        string
		replacement bool
		wantRemoved bool
	}{
		{
			name:        "current subscription peer",
			wantRemoved: true,
		},
		{
			name:        "replaced peer with same ID",
			replacement: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := testPeerID(tt.name)
			failed := &overlayPeer{id: id}
			current := failed
			if tt.replacement {
				current = &overlayPeer{id: id}
			}

			closed := 0
			current.release = func() {
				closed++
			}
			sub := &overlaySubscription{
				peers:      map[PeerID]*overlayPeer{id: current},
				neighbours: []PeerID{id},
			}

			sub.handlePeerQueryFailure(failed, adnl.ErrPeerConnClosed)

			if tt.wantRemoved {
				if sub.peers[id] != nil {
					t.Fatal("closed current peer survived in subscription")
				}
				if sub.hasNeighbourLocked(id) {
					t.Fatal("closed current peer survived in neighbours")
				}
				if closed != 1 {
					t.Fatalf("current peer close count = %d, want 1", closed)
				}
				return
			}

			if sub.peers[id] != current {
				t.Fatal("failure from replaced peer removed current subscription peer")
			}
			if !sub.hasNeighbourLocked(id) {
				t.Fatal("failure from replaced peer removed current neighbour")
			}
			if closed != 0 {
				t.Fatalf("replacement peer close count = %d, want 0", closed)
			}
		})
	}
}

func TestMarkPeerQueryFailedUsesPeerIdentity(t *testing.T) {
	tests := []struct {
		name        string
		current     bool
		replacement bool
		wantRemoved bool
	}{
		{
			name:        "current subscription peer",
			current:     true,
			wantRemoved: true,
		},
		{
			name:        "replaced peer with same ID",
			current:     true,
			replacement: true,
		},
		{
			name: "non-owned peer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := testPeerID(tt.name)
			failed := &overlayPeer{
				id:            id,
				unreliability: peerStopUnreliability,
			}
			peers := map[PeerID]*overlayPeer{}
			var current *overlayPeer
			if tt.current {
				current = failed
				if tt.replacement {
					current = &overlayPeer{id: id}
				}
				peers[id] = current
			}
			sub := &overlaySubscription{
				peers:      peers,
				neighbours: []PeerID{id},
			}

			sub.markPeerQueryFailed(failed)

			wantPresent := !tt.wantRemoved
			if got := sub.hasNeighbourLocked(id); got != wantPresent {
				t.Fatalf("neighbour present = %v, want %v", got, wantPresent)
			}
			if !tt.wantRemoved && sub.peers[id] != current {
				t.Fatal("subscription peer identity changed")
			}
			stats := failed.statsSnapshot()
			if stats.failedQueries != 1 {
				t.Fatalf("failed peer query count = %d, want 1", stats.failedQueries)
			}
		})
	}
}
