package p2p

import (
	"testing"
	"time"
)

// Outbound QUIC paths used to accumulate without bound (measured ~1300 live on a
// production node, ~5.2k goroutines) because keep-alive outlives the idle
// timeout and nothing ever closed one. These pin the sweep policy.

func testQUICPath(seed byte, outbound bool, lastActive time.Time) *authenticatedQUICPeer {
	var id PeerID
	id[0] = seed
	return &authenticatedQUICPeer{
		id:           id,
		outbound:     outbound,
		registeredAt: lastActive,
	}
}

func TestSelectIdleQUICPathsClosesIdleOutbound(t *testing.T) {
	now := time.Now()
	fresh := testQUICPath(1, true, now.Add(-time.Second))
	idle := testQUICPath(2, true, now.Add(-quicIdleTTL-time.Minute))

	victims := selectIdleQUICPaths([]*authenticatedQUICPeer{fresh, idle}, 2, now)
	if len(victims) != 1 || victims[0] != idle {
		t.Fatalf("only the idle path must be closed, got %d victims", len(victims))
	}
}

func TestSelectIdleQUICPathsKeepsEverythingBusy(t *testing.T) {
	now := time.Now()
	paths := []*authenticatedQUICPeer{
		testQUICPath(1, true, now.Add(-time.Second)),
		testQUICPath(2, true, now.Add(-quicIdleTTL+time.Second)),
	}
	if victims := selectIdleQUICPaths(paths, len(paths), now); len(victims) != 0 {
		t.Fatalf("busy paths must survive, got %d victims", len(victims))
	}
}

func TestSelectIdleQUICPathsEnforcesCapByLRU(t *testing.T) {
	now := time.Now()
	// All recently used, but far above the cap: the least recently used excess
	// must go so the dialed set stays bounded.
	total := maxOutboundQUICPaths + 10
	paths := make([]*authenticatedQUICPeer, 0, total)
	for i := 0; i < total; i++ {
		// All well inside the idle TTL, so only the cap rule can fire.
		paths = append(paths, testQUICPath(byte(i%251), true, now.Add(-time.Duration(i)*time.Millisecond)))
	}

	victims := selectIdleQUICPaths(paths, total, now)
	if len(victims) != 10 {
		t.Fatalf("expected the 10 over-cap paths to be closed, got %d", len(victims))
	}
	// Victims must be the least recently used, i.e. the largest offsets.
	oldestKept := now.Add(-time.Duration(total-11) * time.Millisecond)
	for _, victim := range victims {
		if victim.lastActive().After(oldestKept) {
			t.Fatalf("cap eviction must pick the least recently used paths")
		}
	}
}

func TestSelectIdleQUICPathsCapAccountsForIdleVictims(t *testing.T) {
	now := time.Now()
	// Idle paths already reduce the count, so the cap must not double-evict.
	paths := []*authenticatedQUICPeer{
		testQUICPath(1, true, now.Add(-quicIdleTTL-time.Hour)),
		testQUICPath(2, true, now.Add(-time.Second)),
	}
	victims := selectIdleQUICPaths(paths, maxOutboundQUICPaths+1, now)
	if len(victims) != 1 {
		t.Fatalf("the idle path alone brings the count under the cap, got %d victims", len(victims))
	}
}

// Inbound paths belong to the remote and may be the very connection feeding us
// broadcasts; they are never swept.
func TestSweepIgnoresInboundPaths(t *testing.T) {
	node := &Node{quicPeers: map[PeerID]*authenticatedQUICPeer{}}
	inbound := testQUICPath(9, false, time.Now().Add(-24*time.Hour))
	node.quicPeers[inbound.id] = inbound

	if closed := node.sweepIdleQUICPaths(time.Now()); closed != 0 {
		t.Fatalf("inbound path must not be swept, closed %d", closed)
	}
	if len(node.quicPeers) != 1 {
		t.Fatalf("inbound path was removed from the registry")
	}
}

// The route table is filed from inbound connections too, so without a bound it
// grows with every peer id that ever handshaked with us and never shrinks.
func TestPeerRouteSweepBoundsTheTable(t *testing.T) {
	node := newTestNode(t)
	now := time.Now()

	for i := range maxPeerRoutes + 500 {
		var id PeerID
		id[0], id[1], id[2] = byte(i), byte(i>>8), byte(i>>16)
		node.peerRoutes.get(id)
	}
	if got := node.peerRoutes.size(); got != maxPeerRoutes+500 {
		t.Fatalf("filed %d routes, want %d", got, maxPeerRoutes+500)
	}

	if dropped := node.sweepPeerRoutes(now); dropped != 500 {
		t.Fatalf("dropped %d routes, want 500", dropped)
	}
	if got := node.peerRoutes.size(); got != maxPeerRoutes {
		t.Fatalf("table holds %d routes after the sweep, want %d", got, maxPeerRoutes)
	}
	if dropped := node.sweepPeerRoutes(now); dropped != 0 {
		t.Fatalf("sweep dropped %d routes while at the bound", dropped)
	}
}

// A route carries the learned QUIC address and the single-dial gate of a peer we
// are talking to right now: taking it away loses the address and leaves the gate
// claiming nothing. Garbage left by one-shot inbound connections goes instead.
func TestPeerRouteSweepKeepsHeldAndUsefulRoutes(t *testing.T) {
	node := newTestNode(t)

	held := testPeerID("held-by-a-live-transport")
	node.peerRoutes.get(held)
	node.pool.mx.Lock()
	node.pool.peers[held] = &pooledPeer{id: held}
	node.pool.mx.Unlock()

	useful := testPeerID("knows-a-quic-address")
	node.peerRoutes.get(useful).setQUICAddr("1.2.3.4:4278")

	for i := range maxPeerRoutes + 100 {
		var id PeerID
		id[0], id[1], id[2], id[3] = byte(i), byte(i>>8), byte(i>>16), 0xAA
		node.peerRoutes.get(id)
	}

	if dropped := node.sweepPeerRoutes(time.Now()); dropped == 0 {
		t.Fatal("sweep dropped nothing while over the bound")
	}

	node.peerRoutes.mx.RLock()
	_, keptHeld := node.peerRoutes.routes[held]
	_, keptUseful := node.peerRoutes.routes[useful]
	node.peerRoutes.mx.RUnlock()

	if !keptHeld {
		t.Fatal("a route held by a live transport was swept")
	}
	if !keptUseful {
		t.Fatal("a route with a learned QUIC address was swept before address-less ones")
	}
}
