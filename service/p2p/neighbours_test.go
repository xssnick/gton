package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func TestOverlayPeerLivenessRecoversAfterInboundTraffic(t *testing.T) {
	peer := &overlayPeer{
		alive:         true,
		lastReceiveAt: time.Now().Add(-20 * time.Second),
	}

	peer.queryFailed()
	peer.queryFailed()
	peer.queryFailed()

	if peer.statsSnapshot().alive {
		t.Fatalf("expected peer to become dead after repeated missed pings")
	}

	peer.noteReceive()
	stats := peer.statsSnapshot()
	if !stats.alive {
		t.Fatalf("expected inbound traffic to mark peer alive again")
	}
	if stats.lastReceiveAt.IsZero() {
		t.Fatalf("expected inbound traffic to refresh last receive timestamp")
	}
}

func TestFixedOverlayPeerKeepsFailureStatsWithoutEviction(t *testing.T) {
	peer := &overlayPeer{
		fixedMember:   true,
		alive:         true,
		lastReceiveAt: time.Now().Add(-20 * time.Second),
	}

	for i := 0; i < 8; i++ {
		peer.queryFailed()
	}

	stats := peer.statsSnapshot()
	if !stats.alive {
		t.Fatal("fixed overlay peer with prior traffic should stay alive after failures")
	}
	if stats.failedQueries != 8 || stats.unreliability != 8 {
		t.Fatalf("fixed overlay failures should be visible in stats: failed=%d unreliability=%f", stats.failedQueries, stats.unreliability)
	}
}

func TestFixedOverlayPeerNeedsTrafficBeforeAlive(t *testing.T) {
	peer := &overlayPeer{
		fixedMember: true,
		overlay:     &overlay.ADNLOverlayWrapper{},
		alive:       true,
	}

	if peer.isAliveKnownOverlayPeer(time.Now()) {
		t.Fatal("fixed overlay peer without receive or success should not be alive known")
	}

	peer.noteReceive()
	if !peer.isAliveKnownOverlayPeer(time.Now()) {
		t.Fatal("fixed overlay peer should become alive known after inbound traffic")
	}
}

func TestReloadNeighboursReplacesWorstPeer(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{},
		neighbours: make([]PeerID, 0, maxQueryNeighbours),
	})

	now := int32(time.Now().Unix())
	for i := 0; i < maxQueryNeighbours; i++ {
		id := testPeerID(string(rune('a' + i)))
		peer := &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		if i == 0 {
			peer.unreliability = peerStopUnreliability + 1
		}
		sub.peers[id] = peer
		sub.neighbours = append(sub.neighbours, id)
	}

	fresh := &overlayPeer{
		id:            testPeerID("fresh"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.peers[fresh.id] = fresh

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked(testPeerID("a")) {
		t.Fatalf("expected worst neighbour to be replaced")
	}
	if !sub.hasNeighbourLocked(fresh.id) {
		t.Fatalf("expected fresh peer to be added to neighbours")
	}
}

func TestReloadNeighboursKeepsLeasedWorstPeer(t *testing.T) {
	leasedID := testPeerID("a")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			peerUse: map[PeerID]peerUse{leasedID: {downloads: 1}},
		},
		peers:      map[PeerID]*overlayPeer{},
		neighbours: make([]PeerID, 0, maxQueryNeighbours),
	})

	now := int32(time.Now().Unix())
	for i := 0; i < maxQueryNeighbours; i++ {
		id := testPeerID(string(rune('a' + i)))
		peer := &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		if id == leasedID {
			peer.unreliability = peerStopUnreliability + 1
		}
		sub.peers[id] = peer
		sub.neighbours = append(sub.neighbours, id)
	}

	fresh := &overlayPeer{
		id:            testPeerID("fresh"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.peers[fresh.id] = fresh

	sub.reloadNeighbours()

	if !sub.hasNeighbourLocked(leasedID) {
		t.Fatalf("leased neighbour was rotated")
	}
}

func TestReloadNeighboursDoesNotProtectArchiveSelection(t *testing.T) {
	pinnedID := testPeerID("a")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		node:       node,
		peers:      map[PeerID]*overlayPeer{},
		neighbours: make([]PeerID, 0, maxQueryNeighbours),
	})
	session := node.BeginArchiveSession()
	defer session.Close()

	now := int32(time.Now().Unix())
	for i := 0; i < maxQueryNeighbours; i++ {
		id := testPeerID(string(rune('a' + i)))
		peer := &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		if id == pinnedID {
			peer.unreliability = peerStopUnreliability + 1
		}
		sub.peers[id] = peer
		sub.neighbours = append(sub.neighbours, id)
	}
	session.selectArchivePeer(archive.ShardID{Workchain: -1, Shard: topShard}, sub.peers[pinnedID])
	if _, protected := node.protectedPeerIDs()[pinnedID]; protected {
		t.Fatal("archive selection entered live neighbour protection")
	}

	fresh := &overlayPeer{
		id:            testPeerID("fresh"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.peers[fresh.id] = fresh

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked(pinnedID) {
		t.Fatalf("archive-selected unreliable neighbour was protected from normal rotation")
	}
}

func TestReloadNeighboursDoesNotRandomRotateLeasedNeighbours(t *testing.T) {
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		node:       node,
		peers:      map[PeerID]*overlayPeer{},
		neighbours: make([]PeerID, 0, maxQueryNeighbours),
	})

	now := int32(time.Now().Unix())
	for i := 0; i < maxQueryNeighbours; i++ {
		id := testPeerID(string(rune('a' + i)))
		sub.peers[id] = &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		sub.neighbours = append(sub.neighbours, id)
		node.peerUse[id] = peerUse{downloads: 1}
	}

	fresh := &overlayPeer{
		id:            testPeerID("fresh"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.peers[fresh.id] = fresh

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked(fresh.id) {
		t.Fatalf("fresh peer replaced a leased neighbour")
	}
	for id := range node.peerUse {
		if !sub.hasNeighbourLocked(id) {
			t.Fatalf("leased neighbour %q was rotated", id)
		}
	}
}

func TestReloadNeighboursPrunesDeadLeasedPeer(t *testing.T) {
	deadID := testPeerID("dead")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			peerUse: map[PeerID]peerUse{deadID: {downloads: 1}},
		},
		peers:      map[PeerID]*overlayPeer{},
		neighbours: []PeerID{deadID},
	})

	now := int32(time.Now().Unix())
	sub.peers[deadID] = &overlayPeer{
		id:            deadID,
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         false,
		lastReceiveAt: time.Now().Add(-time.Minute),
	}

	fresh := &overlayPeer{
		id:            testPeerID("fresh"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.peers[fresh.id] = fresh

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked(deadID) {
		t.Fatalf("dead leased neighbour was not pruned")
	}
	if !sub.hasNeighbourLocked(fresh.id) {
		t.Fatalf("fresh peer did not replace dead leased neighbour")
	}
}

func TestReloadNeighboursPrunesDeadSessionPinnedArchivePeer(t *testing.T) {
	deadID := testPeerID("dead")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		node:       node,
		peers:      map[PeerID]*overlayPeer{},
		neighbours: []PeerID{deadID},
	})
	session := node.BeginArchiveSession()
	defer session.Close()

	now := int32(time.Now().Unix())
	sub.peers[deadID] = &overlayPeer{
		id:            deadID,
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         false,
		lastReceiveAt: time.Now().Add(-time.Minute),
	}
	session.selectArchivePeer(archive.ShardID{Workchain: -1, Shard: topShard}, sub.peers[deadID])

	fresh := &overlayPeer{
		id:            testPeerID("fresh"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.peers[fresh.id] = fresh

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked(deadID) {
		t.Fatalf("dead session-pinned archive neighbour was not pruned")
	}
	if !sub.hasNeighbourLocked(fresh.id) {
		t.Fatalf("fresh peer did not replace dead session-pinned archive neighbour")
	}
}

func TestReloadNeighboursPrefersAliveKnownPeers(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{},
		neighbours: []PeerID{testPeerID("dead")},
	})

	now := int32(time.Now().Unix())
	deadID := testPeerID("dead")
	aliveID := testPeerID("alive")
	sub.peers[deadID] = &overlayPeer{
		id:            testPeerID("dead"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         false,
		lastReceiveAt: time.Now().Add(-time.Minute),
	}
	sub.peers[aliveID] = &overlayPeer{
		id:            testPeerID("alive"),
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked(deadID) {
		t.Fatalf("expected dead known peer to be dropped from neighbours")
	}
	if !sub.hasNeighbourLocked(aliveID) {
		t.Fatalf("expected alive known peer to occupy neighbour slot")
	}
}

func TestAttachPeerEvictionRejectsHealthyFullPool(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})

	now := int32(time.Now().Unix())
	for i := 0; i < maxPeersPerOverlay; i++ {
		id := testPeerID(string(rune('a' + i)))
		sub.peers[id] = &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
	}

	sub.mx.Lock()
	got := sub.attachPeerEvictionCandidateLocked(testPeerID("new"), nil)
	sub.mx.Unlock()
	if !got.IsZero() {
		t.Fatalf("healthy full pool eviction candidate = %q, want none", got)
	}
}

func TestAttachPeerEvictionAllowsBadPeerReplacement(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})

	now := int32(time.Now().Unix())
	for i := 0; i < maxPeersPerOverlay; i++ {
		id := testPeerID(string(rune('a' + i)))
		peer := &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		if i == 3 {
			peer.unreliability = peerStopUnreliability + 1
		}
		sub.peers[id] = peer
	}

	sub.mx.Lock()
	got := sub.attachPeerEvictionCandidateLocked(testPeerID("new"), nil)
	sub.mx.Unlock()
	if got != testPeerID("d") {
		t.Fatalf("bad peer eviction candidate = %q, want d", got)
	}
}

func TestAttachPeerEvictionAllowsSlowPeerReplacement(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})

	now := int32(time.Now().Unix())
	for i := 0; i < maxPeersPerOverlay; i++ {
		id := testPeerID(string(rune('a' + i)))
		peer := &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		if i == 3 {
			peer.downloadSlowUntil = time.Now().Add(time.Minute)
		}
		sub.peers[id] = peer
	}

	sub.mx.Lock()
	got := sub.attachPeerEvictionCandidateLocked(testPeerID("new"), nil)
	sub.mx.Unlock()
	if got != testPeerID("d") {
		t.Fatalf("slow peer eviction candidate = %q, want d", got)
	}
}

func TestAttachPooledPeerDoesNotEvictProtectedPeer(t *testing.T) {
	tests := []struct {
		name string
		use  peerUse
	}{
		{name: "download", use: peerUse{downloads: 1}},
		{name: "query", use: peerUse{queries: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peerPool, pooled, _ := newTestLeasedPooledPeer("candidate-" + tt.name)
			protectedID := testPeerID("protected-" + tt.name)
			sub := testOverlaySubscription(&overlaySubscription{
				log: discardLogger(),
				node: &Node{
					pool: peerPool,
					peerUse: map[PeerID]peerUse{
						protectedID: tt.use,
					},
				},
				spec:  overlaySpec{ShortID: []byte{0x01}, Kind: overlayKindPublicShard},
				peers: map[PeerID]*overlayPeer{},
			})

			now := int32(time.Now().Unix())
			protected := &overlayPeer{
				id:            protectedID,
				overlay:       &overlay.ADNLOverlayWrapper{},
				announced:     &overlay.Node{Version: now},
				alive:         true,
				lastReceiveAt: time.Now(),
				unreliability: peerStopUnreliability + 1,
			}
			sub.peers[protectedID] = protected
			for i := 1; i < maxPeersPerOverlay; i++ {
				id := testPeerID(tt.name + string(rune('a'+i)))
				sub.peers[id] = &overlayPeer{
					id:            id,
					overlay:       &overlay.ADNLOverlayWrapper{},
					announced:     &overlay.Node{Version: now},
					alive:         true,
					lastReceiveAt: time.Now(),
				}
			}

			if sub.attachPooledPeer(pooled, nil) {
				t.Fatal("protected full pool accepted replacement peer")
			}
			if sub.peers[protectedID] != protected {
				t.Fatal("protected peer was evicted")
			}
			if sub.peers[pooled.id] != nil {
				t.Fatal("replacement peer was attached despite protected eviction candidate")
			}
		})
	}
}

func TestDHTRefreshReplacementKeepsPeerUntilAttach(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})

	now := int32(time.Now().Unix())
	for i := 0; i < maxPeersPerOverlay; i++ {
		id := testPeerID(string(rune('a' + i)))
		peer := &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		if i == 3 {
			peer.downloadSlowUntil = time.Now().Add(time.Minute)
		}
		sub.peers[id] = peer
	}

	if !sub.hasPeerReplacementCandidate(testPeerID("new")) {
		t.Fatal("expected slow peer to allow DHT refresh replacement")
	}
	if sub.peers[testPeerID("d")] == nil {
		t.Fatal("DHT refresh should not evict before candidate is attached")
	}
}

func TestPingTargetsRotateNeighbours(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{},
		neighbours: []PeerID{},
	})

	now := int32(time.Now().Unix())
	for i := 0; i < 8; i++ {
		id := testPeerID(string(rune('a' + i)))
		sub.peers[id] = &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
			versionMajor:  3,
		}
		sub.neighbours = append(sub.neighbours, id)
	}

	first := sub.pingTargets()
	second := sub.pingTargets()
	if len(first) != peerPingFanout || len(second) != peerPingFanout {
		t.Fatalf("unexpected ping target count: first=%d second=%d", len(first), len(second))
	}
	if first[0].id == second[0].id {
		t.Fatalf("expected round-robin ping selection to advance, got %q twice", first[0].id)
	}
}

func TestEnsurePeersReturnsWhenFirstPeerArrives(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{},
		peerNotify: make(chan struct{}, 1),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sub.ensurePeers(ctx)
	}()

	time.Sleep(10 * time.Millisecond)
	sub.mx.Lock()
	peerID := testPeerID("peer-1")
	sub.peers[peerID] = &overlayPeer{
		id:            peerID,
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: int32(time.Now().Unix())},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.notifyPeersChangedLocked()
	sub.mx.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ensure peers: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for ensurePeers")
	}
}

func TestStartSeedFromDHTSetsCooldownAfterSearch(t *testing.T) {
	node := newTestNode(t)
	fake := &fakeDHTClient{}
	node.dht = fake

	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})

	before := time.Now()
	sub.startSeedFromDHT(context.Background())
	node.wg.Wait()

	if calls := fake.findOverlayNodesCallCount(); calls != 1 {
		t.Fatalf("expected one DHT search, got %d", calls)
	}

	sub.seedMx.Lock()
	nextSeedAt := sub.nextSeedAt
	sub.seedMx.Unlock()
	delay := nextSeedAt.Sub(before)
	if delay < dhtSeedCooldownMinDelay || delay > dhtSeedCooldownMinDelay+dhtSeedCooldownJitter+time.Second {
		t.Fatalf("unexpected DHT seed cooldown: %s", delay)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sub.startSeedFromDHT(ctx)
	node.wg.Wait()

	if calls := fake.findOverlayNodesCallCount(); calls != 1 {
		t.Fatalf("cooldown should block immediate DHT search, got %d calls", calls)
	}
}

func TestStartSeedFromDHTRefreshesWhenPeerPoolIsFull(t *testing.T) {
	node := newTestNode(t)
	fake := &fakeDHTClient{}
	node.dht = fake

	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	for i := 0; i < maxPeersPerOverlay; i++ {
		id := testPeerID(string(rune('a' + i)))
		sub.peers[id] = &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: int32(time.Now().Unix())},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
	}

	sub.startSeedFromDHT(context.Background())
	node.wg.Wait()

	if calls := fake.findOverlayNodesCallCount(); calls != 1 {
		t.Fatalf("expected DHT refresh search with full peer pool, got %d", calls)
	}
}

func TestAnnounceSelfRetriesAfterDHTWarmup(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		ListenAddr:    "127.0.0.1:30303",
		PeerStorage:   newTestPeerStore(),
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.externalIP = net.ParseIP("127.0.0.1").To4()
	node.gateway.SetAddressList([]adnladdr.Address{&adnladdr.UDP{IP: []byte{127, 0, 0, 1}, Port: 30303}})

	spec, err := buildOverlaySpec(make([]byte, 32), -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}
	node.subscriptions = map[string]*overlaySubscription{
		"master": {
			node: node,
			spec: spec,
			log:  discardLogger(),
		},
	}

	fake := &fakeDHTClient{
		storeAddressErrs: []error{
			errNoAliveStore,
			nil,
		},
		storeOverlayErrs: []error{
			errNoAliveStore,
			nil,
		},
	}
	node.dht = fake

	if err := node.announceSelf(context.Background()); err != nil {
		t.Fatalf("announce self: %v", err)
	}
	if fake.storeAddressCalls != 2 {
		t.Fatalf("expected 2 address store attempts, got %d", fake.storeAddressCalls)
	}
	if fake.storeOverlayCalls != 2 {
		t.Fatalf("expected 2 overlay store attempts, got %d", fake.storeOverlayCalls)
	}
	if fake.findAddressesCalls != 1 {
		t.Fatalf("expected address DHT warmup before retry, got %d", fake.findAddressesCalls)
	}
	if fake.findOverlayNodesCalls != 1 {
		t.Fatalf("expected overlay DHT warmup before retry, got %d", fake.findOverlayNodesCalls)
	}
	if got := timeoutDuration(t, fake.storeAddressDeadline); got < dhtStoreTimeout-time.Second {
		t.Fatalf("store address timeout too short: %s", got)
	}
	if got := timeoutDuration(t, fake.storeOverlayDeadline); got < dhtStoreTimeout-time.Second {
		t.Fatalf("store overlay timeout too short: %s", got)
	}
	if got := timeoutDuration(t, fake.findAddressesDeadline); got < dhtFindTimeout-time.Second {
		t.Fatalf("find address timeout too short: %s", got)
	}
	if got := timeoutDuration(t, fake.findOverlayNodesDeadline); got < dhtFindTimeout-time.Second {
		t.Fatalf("find overlay timeout too short: %s", got)
	}
}

func TestAnnounceSelfSkipsDHTStoreWhenOffline(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		ListenAddr:    "127.0.0.1:30303",
		PeerStorage:   newTestPeerStore(),
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.gateway.SetAddressList([]adnladdr.Address{&adnladdr.UDP{IP: []byte{127, 0, 0, 1}, Port: 30303}})
	fake := &fakeDHTClient{}
	node.dht = fake
	node.EnterOffline("test")

	if err = node.announceSelf(context.Background()); !errors.Is(err, ErrOffline) {
		t.Fatalf("announce self error = %v, want %v", err, ErrOffline)
	}
	if fake.storeAddressCalls != 0 {
		t.Fatalf("offline announce stored address %d times", fake.storeAddressCalls)
	}
	if fake.storeOverlayCalls != 0 {
		t.Fatalf("offline announce stored overlay %d times", fake.storeOverlayCalls)
	}
	if fake.findAddressesCalls != 0 || fake.findOverlayNodesCalls != 0 {
		t.Fatalf("offline announce warmed DHT addresses=%d overlays=%d", fake.findAddressesCalls, fake.findOverlayNodesCalls)
	}
}

func TestDHTServerPublishSkipsStoreWhenOffline(t *testing.T) {
	node := newTestNode(t)
	node.EnterOffline("test")

	if err := node.publishDHTServerAddress(context.Background(), adnladdr.List{}); !errors.Is(err, ErrOffline) {
		t.Fatalf("publish DHT server address error = %v, want %v", err, ErrOffline)
	}
}

var errNoAliveStore = errors.New("no alive nodes found to store this key")

type fakeDHTClient struct {
	mx                           sync.Mutex
	storeAddressCalls            int
	storeOverlayCalls            int
	findAddressesCalls           int
	findOverlayNodesCalls        int
	storeAddressErrs             []error
	storeOverlayErrs             []error
	findAddressesErr             error
	findOverlayNodesWait         <-chan struct{}
	findOverlayNodesWaitAt       int
	findOverlayNodesStarted      chan<- struct{}
	findOverlayNodesContinuation *dht.Continuation
	storeAddressDeadline         time.Time
	storeOverlayDeadline         time.Time
	findOverlayNodesDeadline     time.Time
	findAddressesDeadline        time.Time
}

func (f *fakeDHTClient) findOverlayNodesCallCount() int {
	f.mx.Lock()
	defer f.mx.Unlock()

	return f.findOverlayNodesCalls
}

func (f *fakeDHTClient) FindOverlayNodes(ctx context.Context, _ []byte, _ ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error) {
	f.mx.Lock()
	f.findOverlayNodesCalls++
	call := f.findOverlayNodesCalls
	f.findOverlayNodesDeadline, _ = ctx.Deadline()
	wait := f.findOverlayNodesWait
	waitAt := f.findOverlayNodesWaitAt
	started := f.findOverlayNodesStarted
	continuation := f.findOverlayNodesContinuation
	f.mx.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}

	if wait != nil && (waitAt == 0 || call == waitAt) {
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	return &overlay.NodesList{}, continuation, nil
}

func (f *fakeDHTClient) FindAddresses(ctx context.Context, _ []byte) (*adnladdr.List, ed25519.PublicKey, error) {
	f.mx.Lock()
	f.findAddressesCalls++
	f.findAddressesDeadline, _ = ctx.Deadline()
	err := f.findAddressesErr
	f.mx.Unlock()

	if err != nil {
		return nil, nil, err
	}
	return &adnladdr.List{}, nil, nil
}

func (f *fakeDHTClient) FindValue(context.Context, *dht.Key, ...*dht.Continuation) (*dht.Value, *dht.Continuation, error) {
	return nil, nil, nil
}

func (f *fakeDHTClient) StoreAddress(ctx context.Context, _ adnladdr.List, _ time.Duration, _ ed25519.PrivateKey) (int, []byte, error) {
	f.mx.Lock()
	defer f.mx.Unlock()

	f.storeAddressCalls++
	f.storeAddressDeadline, _ = ctx.Deadline()
	return 1, nil, popErr(&f.storeAddressErrs)
}

func (f *fakeDHTClient) StoreOverlayNodes(ctx context.Context, _ []byte, _ *overlay.NodesList, _ time.Duration) (int, []byte, error) {
	f.mx.Lock()
	defer f.mx.Unlock()

	f.storeOverlayCalls++
	f.storeOverlayDeadline, _ = ctx.Deadline()
	return 1, nil, popErr(&f.storeOverlayErrs)
}

func timeoutDuration(tb testing.TB, deadline time.Time) time.Duration {
	tb.Helper()
	if deadline.IsZero() {
		tb.Fatal("expected context deadline")
	}
	return time.Until(deadline)
}

func popErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}
