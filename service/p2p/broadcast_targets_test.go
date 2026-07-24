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
