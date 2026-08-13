package p2p

import (
	"sync"
	"testing"
	"time"
)

func TestBroadcastTargetsExpiredRefreshIsSingleflight(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		peers: map[PeerID]*overlayPeer{},
	})
	sub.broadcastTargets.Store(&broadcastTargetsSnapshot{
		builtAt: time.Now().Add(-broadcastTargetsTTL - time.Second),
	})

	const callers = 64
	start := make(chan struct{})
	results := make(chan *broadcastTargetsSnapshot, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	sub.mx.Lock()
	for i := 0; i < callers; i++ {
		go func() {
			ready.Done()
			<-start
			results <- sub.broadcastTargetsSnapshot()
		}()
	}
	ready.Wait()
	close(start)
	time.Sleep(10 * time.Millisecond)
	sub.mx.Unlock()

	first := <-results
	for i := 1; i < callers; i++ {
		if got := <-results; got != first {
			t.Fatal("concurrent expired-cache callers rebuilt different snapshots")
		}
	}
}

func TestBroadcastTargetsExcludeDeferredQUICRouteOnlyFromPlumtree(t *testing.T) {
	peerID := testPeerID("deferred-plumtree-route")
	route := newTestPeerRoute("127.0.0.1:3000")
	peer := &overlayPeer{
		id:          peerID,
		route:       route,
		fixedMember: true,
		alive:       true,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		peers: map[PeerID]*overlayPeer{peerID: peer},
	})

	snapshot := sub.buildBroadcastTargetsSnapshot()
	if len(snapshot.peers) != 1 ||
		len(snapshot.broadcast) != 1 ||
		len(snapshot.plumtree) != 1 {
		t.Fatalf(
			"eligible targets = peers %d, broadcasts %d, Plumtree %d; want 1, 1, 1",
			len(snapshot.peers),
			len(snapshot.broadcast),
			len(snapshot.plumtree),
		)
	}

	route.DeferQUICDial(time.Now())
	snapshot = sub.buildBroadcastTargetsSnapshot()
	if len(snapshot.peers) != 1 ||
		len(snapshot.broadcast) != 1 ||
		len(snapshot.plumtree) != 0 {
		t.Fatalf(
			"deferred targets = peers %d, broadcasts %d, Plumtree %d; want 1, 1, 0",
			len(snapshot.peers),
			len(snapshot.broadcast),
			len(snapshot.plumtree),
		)
	}
	if !sub.PlumtreePeerReceivesBroadcasts(peerID) {
		t.Fatal("QUIC retry delay changed Plumtree roster membership")
	}
}

func BenchmarkBroadcastTargetsSnapshotCached(b *testing.B) {
	sub := testOverlaySubscription(&overlaySubscription{
		peers: map[PeerID]*overlayPeer{},
	})
	sub.broadcastTargets.Store(&broadcastTargetsSnapshot{
		builtAt: time.Now().Add(time.Hour),
	})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = sub.broadcastTargetsSnapshot()
	}
}
