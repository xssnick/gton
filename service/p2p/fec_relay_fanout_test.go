package p2p

import (
	"fmt"
	"testing"
)

// The FEC relay forwards every received part to every peer this set returns,
// and it is consulted per part. Returning the whole roster made a 300-peer node
// emit ~40k UDP sends/s (~550 Mbit/s) of pure relay egress at head rates, which
// saturates the uplink and costs the node its own inbound broadcasts. The set
// must stay bounded no matter how large the roster grows.
func newRelayFanoutSubscription(t *testing.T, peers, neighbours int) *overlaySubscription {
	t.Helper()

	roster := make(map[PeerID]*overlayPeer, peers)
	ids := make([]PeerID, 0, peers)
	for i := 0; i < peers; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("relay-%03d", i))
		roster[peer.id] = peer
		ids = append(ids, peer.id)
	}
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: roster,
	})
	if neighbours > len(ids) {
		neighbours = len(ids)
	}
	sub.neighbours = append(sub.neighbours, ids[:neighbours]...)
	return sub
}

func TestBroadcastRelayTargetsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name       string
		peers      int
		neighbours int
	}{
		{name: "large roster with neighbours", peers: 300, neighbours: 16},
		{name: "large roster without neighbours", peers: 300, neighbours: 0},
		{name: "roster smaller than the fanout", peers: 3, neighbours: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := newRelayFanoutSubscription(t, tc.peers, tc.neighbours)
			if got := len(sub.broadcastTargetsSnapshot().broadcast); got != tc.peers {
				t.Fatalf("broadcast targets = %d, want the whole roster %d", got, tc.peers)
			}

			// Asserted through the interface the FEC relay actually calls, so
			// re-wiring it back to the full roster fails here.
			relay := overlayFECRelayPeerSet{sub: sub}.Peers()
			want := broadcastFECRelayFanout
			if tc.peers < want {
				want = tc.peers
			}
			if len(relay) != want {
				t.Fatalf("relay targets = %d, want %d (roster %d)", len(relay), want, tc.peers)
			}
		})
	}
}

// The relay sample must rotate, otherwise the same handful of peers carries all
// relayed traffic for as long as the roster is stable.
func TestBroadcastRelayTargetsRotate(t *testing.T) {
	sub := newRelayFanoutSubscription(t, 300, 0)

	first := sub.buildBroadcastTargetsSnapshot().relay
	differs := false
	for i := 0; i < 20 && !differs; i++ {
		next := sub.buildBroadcastTargetsSnapshot().relay
		for j := range next {
			if j >= len(first) || next[j] != first[j] {
				differs = true
				break
			}
		}
	}
	if !differs {
		t.Fatalf("relay sample never changed across rebuilds")
	}
}
