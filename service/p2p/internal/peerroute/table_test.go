package peerroute

import (
	"sync"
	"testing"
	"time"
)

type testPeerID [32]byte

func testRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MinDelay: 10 * time.Second,
		MaxDelay: 30 * time.Second,
	}
}

func TestTableReturnsStablePointer(t *testing.T) {
	table := NewTable[testPeerID](testRetryPolicy())

	var id testPeerID
	id[0] = 0x7A
	first := table.Get(id)
	if first == nil {
		t.Fatal("route must be created on first use")
	}
	if second := table.Get(id); second != first {
		t.Fatal("the same peer must always get the same route pointer")
	}

	var other testPeerID
	other[0] = 0x7B
	if table.Get(other) == first {
		t.Fatal("different peers must not share a route")
	}
	if got := table.Size(); got != 2 {
		t.Fatalf("table size = %d, want 2", got)
	}
}

func TestTableSurvivesTransportChurn(t *testing.T) {
	table := NewTable[testPeerID](testRetryPolicy())

	var id testPeerID
	id[0] = 0x11
	table.Get(id).SetQUICAddress("10.0.0.7:31303")
	if got := table.Get(id).QUICAddress(); got != "10.0.0.7:31303" {
		t.Fatalf("quic address = %q, want it to survive transport churn", got)
	}
}

func TestTableDialGateIsSharedAcrossPaths(t *testing.T) {
	table := NewTable[testPeerID](testRetryPolicy())

	var id testPeerID
	id[0] = 0x22
	poolRoute := table.Get(id)
	if !poolRoute.ClaimBackgroundQUICDial() {
		t.Fatal("first claim must succeed")
	}

	inboundRoute := table.Get(id)
	if inboundRoute.ClaimBackgroundQUICDial() {
		t.Fatal("the dial gate must be shared between the pool and inbound paths")
	}
}

func TestTableConcurrentGet(t *testing.T) {
	table := NewTable[testPeerID](testRetryPolicy())

	var id testPeerID
	id[0] = 0x33
	routes := make([]*Route, 32)

	var wait sync.WaitGroup
	for i := range routes {
		wait.Go(func() {
			routes[i] = table.Get(id)
		})
	}
	wait.Wait()

	for i, route := range routes {
		if route != routes[0] {
			t.Fatalf("concurrent get %d returned a different route", i)
		}
	}
	if got := table.Size(); got != 1 {
		t.Fatalf("concurrent get created %d routes, want 1", got)
	}
}

func TestTableSweepKeepsHeldAndUsefulRoutes(t *testing.T) {
	table := NewTable[testPeerID](testRetryPolicy())

	var heldID testPeerID
	heldID[0] = 1
	heldRoute := table.Get(heldID)

	var usefulID testPeerID
	usefulID[0] = 2
	usefulRoute := table.Get(usefulID)
	usefulRoute.SetQUICAddress("1.2.3.4:4278")

	var disposableID testPeerID
	disposableID[0] = 3
	table.Get(disposableID)

	held := map[testPeerID]struct{}{heldID: {}}
	if dropped := table.Sweep(held, 2); dropped != 1 {
		t.Fatalf("dropped routes = %d, want 1", dropped)
	}
	if table.Get(heldID) != heldRoute {
		t.Fatal("held route was swept")
	}
	if table.Get(usefulID) != usefulRoute {
		t.Fatal("useful route was swept before an address-less route")
	}
}
