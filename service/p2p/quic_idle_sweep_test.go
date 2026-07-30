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
