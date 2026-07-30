package p2p

import (
	"sync"
	"testing"
)

// The route carries the learned QUIC endpoint and the single-dial gate. Before
// the table existed it was minted per transport (pool wrap) and again per
// inbound QUIC peer, so the same peer had two gates that each claimed nothing,
// and a pool prune threw the learned address away.
func TestPeerRouteTableReturnsStablePointer(t *testing.T) {
	table := newPeerRouteTable()

	var id PeerID
	id[0] = 0x7A
	first := table.get(id)
	if first == nil {
		t.Fatal("route must be created on first use")
	}
	if second := table.get(id); second != first {
		t.Fatal("the same peer must always get the same route pointer")
	}

	var other PeerID
	other[0] = 0x7B
	if table.get(other) == first {
		t.Fatal("different peers must not share a route")
	}
	if got := table.size(); got != 2 {
		t.Fatalf("table size = %d, want 2", got)
	}
}

func TestPeerRouteTableSurvivesTransportChurn(t *testing.T) {
	table := newPeerRouteTable()

	var id PeerID
	id[0] = 0x11
	// A transport learns the peer's QUIC endpoint...
	table.get(id).setQUICAddr("10.0.0.7:31303")
	// ...the transport is pruned and a new one is wrapped for the same peer.
	if got := table.get(id).quicAddr(); got != "10.0.0.7:31303" {
		t.Fatalf("quic addr = %q, want it to survive transport churn", got)
	}
}

func TestPeerRouteTableDialGateIsSharedAcrossPaths(t *testing.T) {
	table := newPeerRouteTable()

	var id PeerID
	id[0] = 0x22
	// The pooled transport claims the single-dial gate.
	poolRoute := table.get(id)
	if !poolRoute.quicDialSpawned.CompareAndSwap(false, true) {
		t.Fatal("first claim must succeed")
	}
	// The inbound QUIC path for the same peer must observe that claim, which is
	// only true because both resolve through the table.
	inboundRoute := table.get(id)
	if inboundRoute.quicDialSpawned.CompareAndSwap(false, true) {
		t.Fatal("the dial gate must be shared between the pool and inbound paths")
	}
}

func TestPeerRouteTableConcurrentGet(t *testing.T) {
	table := newPeerRouteTable()

	var id PeerID
	id[0] = 0x33
	var wg sync.WaitGroup
	routes := make([]*peerRoute, 32)
	for i := range routes {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			routes[idx] = table.get(id)
		}(i)
	}
	wg.Wait()

	for i, route := range routes {
		if route != routes[0] {
			t.Fatalf("concurrent get %d returned a different route", i)
		}
	}
	if got := table.size(); got != 1 {
		t.Fatalf("concurrent get created %d routes, want 1", got)
	}
}
