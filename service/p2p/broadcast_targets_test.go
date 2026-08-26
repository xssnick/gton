package p2p

import (
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
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
	route := newPeerRoute("127.0.0.1:3000")
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

	route.deferQUICDial(time.Now())
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

func TestBuildBroadcastTargetsSnapshotKeepsWholeCustomRoster(t *testing.T) {
	tests := []struct {
		name      string
		kind      overlayKind
		wantPeers int
	}{
		{
			name:      "custom fixed keeps silent member",
			kind:      overlayKindCustomFixed,
			wantPeers: 2,
		},
		{
			name:      "public shard keeps alive subset",
			kind:      overlayKindPublicShard,
			wantPeers: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			aliveID := testPeerID(tt.name + ":alive")
			silentID := testPeerID(tt.name + ":silent")
			alive := &overlayPeer{
				id:            aliveID,
				fixedMember:   tt.kind == overlayKindCustomFixed,
				alive:         true,
				lastReceiveAt: now,
				announced:     &overlay.Node{Version: int32(now.Unix())},
			}
			silent := &overlayPeer{
				id:          silentID,
				fixedMember: tt.kind == overlayKindCustomFixed,
				announced:   &overlay.Node{Version: int32(now.Unix())},
			}
			sub := testOverlaySubscription(&overlaySubscription{
				spec: overlaySpec{Kind: tt.kind},
				peers: map[PeerID]*overlayPeer{
					aliveID:  alive,
					silentID: silent,
				},
			})

			snapshot := sub.buildBroadcastTargetsSnapshot()
			if len(snapshot.peers) != tt.wantPeers {
				t.Fatalf("broadcast target peers = %d, want %d", len(snapshot.peers), tt.wantPeers)
			}
			if tt.wantPeers == 1 && snapshot.peers[0].id != aliveID {
				t.Fatalf("public broadcast target = %s, want alive peer %s", snapshot.peers[0].id, aliveID)
			}
			if tt.wantPeers == 2 {
				got := map[PeerID]struct{}{}
				for _, peer := range snapshot.peers {
					got[peer.id] = struct{}{}
				}
				if _, ok := got[aliveID]; !ok {
					t.Fatal("custom broadcast targets omitted alive member")
				}
				if _, ok := got[silentID]; !ok {
					t.Fatal("custom broadcast targets omitted silent member")
				}
			}
		})
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
