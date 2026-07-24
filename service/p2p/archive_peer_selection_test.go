package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func testArchiveCandidate(label string) *overlayPeer {
	id := testPeerID(label)
	return &overlayPeer{
		id:        id,
		addr:      label,
		overlay:   &overlay.ADNLOverlayWrapper{},
		announced: &overlay.Node{Version: int32(time.Now().Unix())},
		alive:     true,
		release:   func() {},
	}
}

func testArchivePool(tb testing.TB, sub *overlaySubscription) *archivePeerPool {
	tb.Helper()

	if sub.peers == nil {
		sub.peers = map[PeerID]*overlayPeer{}
	}
	if sub.node.runCtx == nil || sub.node.runCtx.Err() != nil {
		ctx, cancel := context.WithCancel(context.Background())
		sub.node.runCtx = ctx
		tb.Cleanup(cancel)
	}

	pool := newArchivePeerPool(sub)
	tb.Cleanup(pool.Close)
	return pool
}

func testArchiveOnlyPoolPeer(tb testing.TB, pool *archivePeerPool, label string) *overlayPeer {
	tb.Helper()

	overlayWrapper, _ := newTestOverlayWrapper()
	peer := testArchiveCandidate(label)
	peer.overlay = overlayWrapper
	if !addTestArchiveOnlyPeer(pool, peer) {
		tb.Fatalf("failed to add archive-only peer %s", label)
	}
	return peer
}

func addTestArchiveOnlyPeer(pool *archivePeerPool, peer *overlayPeer) bool {
	if peer == nil || peer.id.IsZero() {
		return false
	}
	if peer.release == nil {
		peer.release = func() {}
	}

	pool.pruneClosedPeers()
	pool.mx.Lock()
	defer pool.mx.Unlock()
	if pool.closed || pool.peers[peer.id] != nil || pool.recentlyRejectedLocked(peer.id, time.Now()) || pool.archiveOnlySizeLocked() >= archivePeerRosterLimit {
		return false
	}
	pool.peers[peer.id] = &archivePeer{
		peer:    peer,
		addedAt: time.Now(),
	}
	return true
}

func testArchivePoolHasPeer(pool *archivePeerPool, peerID PeerID) bool {
	pool.mx.Lock()
	defer pool.mx.Unlock()
	return pool.peers[peerID] != nil
}

func beginTestArchiveRequest(tb testing.TB, pool *archivePeerPool, shard archive.ShardID, seqno uint32) archivePeerProbe {
	tb.Helper()
	probe, release, err := pool.beginArchiveRequest(shard, seqno)
	if err != nil {
		tb.Fatalf("begin archive demand: %v", err)
	}
	tb.Cleanup(release)
	return probe
}

func beginTestZeroStateRequest(tb testing.TB, pool *archivePeerPool, shard archive.ShardID, block ton.BlockIDExt) archivePeerProbe {
	tb.Helper()
	probe, release, err := pool.beginZeroStateRequest(shard, block)
	if err != nil {
		tb.Fatalf("begin zero-state demand: %v", err)
	}
	tb.Cleanup(release)
	return probe
}

func newTestLeasedPooledPeer(label string) (*peerPool, *pooledPeer, *testOverlayADNL) {
	base := newTestOverlayADNL()
	adnlWrapper := overlay.CreateExtendedADNL(base)
	baseRLDP := rldp.NewClientV2(adnlWrapper)
	rldpWrapper := overlay.CreateExtendedRLDP(baseRLDP)
	id := testPeerID(label)
	pooled := &pooledPeer{
		id:              id,
		addr:            label,
		adnl:            adnlWrapper,
		baseRLDP:        baseRLDP,
		rldp:            rldpWrapper,
		adnlOverlayRefs: map[*overlay.ADNLOverlayWrapper]int{},
		rldpOverlayRefs: map[*overlay.RLDPOverlayWrapper]int{},
	}
	pooled.touch(time.Now())
	pool := &peerPool{peers: map[PeerID]*pooledPeer{id: pooled}}
	return pool, pooled, base
}

func mustNewTestOverlayPeer(tb testing.TB, sub *overlaySubscription, pooled *pooledPeer, announced *overlay.Node, fixedMember bool) *overlayPeer {
	tb.Helper()
	ensureTestBroadcastReceiver(tb, sub)

	peer, err := sub.newOverlayPeer(pooled, announced, fixedMember)
	if err != nil {
		tb.Fatalf("create test overlay peer: %v", err)
	}
	return peer
}

func TestArchiveScoutAddressLookupUsesArchiveTimeout(t *testing.T) {
	if dhtSeedPeerTimeout != 5*time.Second {
		t.Fatalf("live peer discovery timeout = %s, want 5s", dhtSeedPeerTimeout)
	}

	_, selfKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate self key: %v", err)
	}
	_, peerKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	peerNode, err := overlay.NewNode([]byte{0x01}, peerKey)
	if err != nil {
		t.Fatalf("create peer overlay node: %v", err)
	}

	fake := &fakeDHTClient{findAddressesErr: context.DeadlineExceeded}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht:     fake,
			privKey: selfKey,
		},
		spec: overlaySpec{ShortID: peerNode.Overlay},
	})
	pool := testArchivePool(t, sub)
	defer pool.Close()
	beginTestArchiveRequest(t, pool, archive.ShardID{Workchain: -1, Shard: topShard}, 1)

	if status := pool.offerArchiveNode(*peerNode); status != archivePeerOfferQueued {
		t.Fatalf("offer status = %d, want queued", status)
	}
	deadline := time.Now().Add(time.Second)
	for {
		fake.mx.Lock()
		lookupDeadline := fake.findAddressesDeadline
		fake.mx.Unlock()
		if !lookupDeadline.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("archive scout did not start address lookup")
		}
		time.Sleep(time.Millisecond)
	}

	fake.mx.Lock()
	lookupDeadline := fake.findAddressesDeadline
	fake.mx.Unlock()
	got := timeoutDuration(t, lookupDeadline)
	timeoutTooShort := got < archiveDHTAddressTimeout-time.Second
	timeoutTooLong := got > archiveDHTAddressTimeout
	if timeoutTooShort || timeoutTooLong {
		t.Fatalf("archive address lookup timeout = %s, want about %s", got, archiveDHTAddressTimeout)
	}
	identity, err := sub.overlayNodeIdentity(*peerNode)
	if err != nil {
		t.Fatalf("resolve peer identity: %v", err)
	}
	if !pool.scout.retry.peerBlocked(identity.peerID, time.Now()) {
		t.Fatal("unreachable archive peer was not added to the retry cache")
	}
}

func TestArchivePeerCooldownFiltersOnlyArchivePool(t *testing.T) {
	peerA := testArchiveCandidate("peer-a")
	peerB := testArchiveCandidate("peer-b")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peerA.id: peerA,
			peerB.id: peerB,
		},
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peerA)
	addTestArchiveOnlyPeer(pool, peerB)
	basechain := archive.ShardID{Workchain: 0, Shard: topShard}
	masterchain := archive.ShardID{Workchain: -1, Shard: topShard}

	pool.noteFailure(basechain, peerA, archivePeerRejectNotAvailable)

	got := pool.candidates(basechain)
	if len(got) != 1 || got[0] != peerB {
		t.Fatalf("unexpected basechain peers after cooldown: %#v", got)
	}

	got = pool.candidates(masterchain)
	if len(got) != 2 {
		t.Fatalf("cooldown leaked into masterchain pool: %#v", got)
	}

	pool.mx.Lock()
	state := pool.shards[archivePeerPoolKey(basechain)]
	state.peers[downloadPeerID(peerA)].cooldownUntil = time.Now().Add(-time.Second)
	pool.mx.Unlock()

	got = pool.candidates(basechain)
	if len(got) != 2 {
		t.Fatalf("expired cooldown entry was not restored: %#v", got)
	}
}

func TestRejectArchivePeerClearsArchiveSelectionWithoutLivePin(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("selected")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger(), node: node})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeerFromPool(shard, peer, pool)
	if _, protected := node.protectedPeerIDs()[peer.id]; protected {
		t.Fatal("archive selection entered live peer protection")
	}

	session.rejectArchivePeer(context.Background(), pool, shard, peer, archivePeerRejectNotAvailable)
	if selected := session.selectedArchivePeerID(shard); !selected.IsZero() {
		t.Fatalf("failed archive peer stayed selected: %s", selected.String())
	}
	if _, protected := node.protectedPeerIDs()[peer.id]; protected {
		t.Fatal("archive rejection changed live peer protection")
	}
}

func TestArchiveSessionSelectionIsShardLocal(t *testing.T) {
	shardA := archive.ShardID{Workchain: 0, Shard: topShard}
	shardB := archive.ShardID{Workchain: 1, Shard: topShard}
	peerA := testArchiveCandidate("peer-a")
	peerB := testArchiveCandidate("peer-b")
	session := (&Node{runCtx: context.Background()}).BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeer(shardA, peerA)
	session.selectArchivePeer(shardB, peerA)
	session.selectArchivePeer(shardA, peerB)

	if selected := session.selectedArchivePeerID(shardA); selected != peerB.id {
		t.Fatalf("shard A selected peer = %s, want %s", selected.String(), peerB.id.String())
	}
	if selected := session.selectedArchivePeerID(shardB); selected != peerA.id {
		t.Fatalf("shard B selected peer = %s, want %s", selected.String(), peerA.id.String())
	}
	session.clearSelectedArchivePeerID(shardB, peerA.id)
	if selected := session.selectedArchivePeerID(shardB); !selected.IsZero() {
		t.Fatalf("cleared shard B selection = %s", selected.String())
	}
}

func TestArchiveSessionCloseDetachesOnlyArchiveOnlyPeers(t *testing.T) {
	liveOverlay, liveConn := newTestOverlayWrapper()
	livePeer := testArchiveCandidate("live")
	livePeer.overlay = liveOverlay
	archivePool, pooledArchive, archiveConn := newTestLeasedPooledPeer("archive-only")
	node := &Node{peerUse: map[PeerID]peerUse{}, pool: archivePool}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
		peers: map[PeerID]*overlayPeer{
			livePeer.id: livePeer,
		},
	})
	archivePeer := mustNewTestOverlayPeer(t, sub, pooledArchive, nil, false)
	session := node.BeginArchiveSession()

	pool, err := session.archivePeerPool(sub)
	if err != nil {
		t.Fatalf("create archive peer pool: %v", err)
	}
	addTestArchiveOnlyPeer(pool, archivePeer)
	session.Close()

	select {
	case <-archiveConn.GetCloserCtx().Done():
		t.Fatal("archive session close skipped the idle transport grace period")
	default:
	}
	select {
	case <-liveConn.GetCloserCtx().Done():
		t.Fatal("borrowed live peer connection was closed by archive session")
	default:
	}
	if _, ok := sub.peers[livePeer.id]; !ok {
		t.Fatal("borrowed live peer was removed from live pool")
	}
	if _, ok := sub.peers[archivePeer.id]; ok {
		t.Fatal("archive-only peer leaked into live pool")
	}
	pooledArchive.touch(time.Now().Add(-peerPoolIdleTTL - time.Second))
	if got := archivePool.pruneIdleLockedForTest(time.Now(), 0); got != 1 {
		t.Fatalf("pruned idle archive transports = %d, want 1", got)
	}
	select {
	case <-archiveConn.GetCloserCtx().Done():
	default:
		t.Fatal("expired archive-only transport survived idle pruning")
	}
}

func TestArchivePoolKeepsOwnedPeerSeparateFromSameLivePeer(t *testing.T) {
	basePool, pooledPeer, sharedConn := newTestLeasedPooledPeer("same-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		node:  &Node{pool: basePool},
		log:   discardLogger(),
		spec:  overlaySpec{ShortID: []byte{0x01}, Kind: overlayKindPublicShard},
		peers: map[PeerID]*overlayPeer{},
	})
	announced := &overlay.Node{Version: int32(time.Now().Unix())}
	archivePeer := mustNewTestOverlayPeer(t, sub, pooledPeer, announced, false)
	livePeer := mustNewTestOverlayPeer(t, sub, pooledPeer, announced, false)
	sub.peers[livePeer.id] = livePeer
	pool := testArchivePool(t, sub)

	if !addTestArchiveOnlyPeer(pool, archivePeer) {
		t.Fatal("expected archive-only peer to be added")
	}
	select {
	case <-sharedConn.GetCloserCtx().Done():
		t.Fatal("archive-only replacement closed shared pooled ADNL")
	default:
	}
	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 1 || got[0] != archivePeer {
		t.Fatalf("archive pool adopted the live peer pointer: %#v", got)
	}
	if sub.peerByID(livePeer.id) != livePeer {
		t.Fatal("archive pool changed the live roster entry")
	}
}

func TestArchivePoolLeaseDoesNotEnterLivePeerAccounting(t *testing.T) {
	basePool, pooledPeer, sharedConn := newTestLeasedPooledPeer("leased-same-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		node:  &Node{pool: basePool},
		log:   discardLogger(),
		spec:  overlaySpec{ShortID: []byte{0x01}, Kind: overlayKindPublicShard},
		peers: map[PeerID]*overlayPeer{},
	})
	announced := &overlay.Node{Version: int32(time.Now().Unix())}
	archivePeer := mustNewTestOverlayPeer(t, sub, pooledPeer, announced, false)
	pool := testArchivePool(t, sub)

	if !addTestArchiveOnlyPeer(pool, archivePeer) {
		t.Fatal("expected archive-only peer to be added")
	}
	release, ok := pool.acquire(archivePeer)
	if !ok {
		t.Fatal("failed to lease archive-only peer")
	}
	if got := sub.node.downloadPeerLeaseCount(archivePeer); got != 0 {
		t.Fatalf("archive lease entered live download accounting: %d", got)
	}
	if _, protected := sub.node.protectedPeerIDs()[archivePeer.id]; protected {
		t.Fatal("archive lease protected the peer in the live roster")
	}
	select {
	case <-sharedConn.GetCloserCtx().Done():
		t.Fatal("leased archive-only replacement closed pooled ADNL")
	default:
	}

	release()
}

func TestEnsureArchivePeersBoundsDHTDiscoveryWait(t *testing.T) {
	waitDHT := make(chan struct{})
	fake := &fakeDHTClient{findOverlayNodesWait: waitDHT}
	livePeer := &overlayPeer{
		id:        testPeerID("overlay-peer"),
		addr:      "overlay-peer",
		announced: &overlay.Node{Version: int32(time.Now().Unix())},
		alive:     true,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht: fake,
		},
		spec: overlaySpec{
			FullID:  []byte{0x01},
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
		peers: map[PeerID]*overlayPeer{
			livePeer.id: livePeer,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}

	done := make(chan error, 1)
	go func() {
		done <- sub.ensureArchivePeers(context.Background(), pool, 1, shard)
	}()

	select {
	case err := <-done:
		t.Fatalf("ensureArchivePeers returned before DHT discovery completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ensureArchivePeers returned error at bounded deadline: %v", err)
		}
	case <-time.After(archiveDiscoveryWait + 500*time.Millisecond):
		t.Fatal("ensureArchivePeers waited for the entire DHT walk")
	}
	close(waitDHT)
}

func TestEnsureArchivePeersRefreshesReadyPoolWithoutWaiting(t *testing.T) {
	waitDHT := make(chan struct{})
	fake := &fakeDHTClient{findOverlayNodesWait: waitDHT}
	livePeer := testArchiveCandidate("ready-overlay-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht: fake,
		},
		spec: overlaySpec{
			FullID:  []byte{0x01},
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
		peers: map[PeerID]*overlayPeer{
			livePeer.id: livePeer,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	pool.markSuccess(shard, livePeer)

	if err := sub.ensureArchivePeers(context.Background(), pool, 1, shard); err != nil {
		close(waitDHT)
		t.Fatalf("ensure ready archive peers: %v", err)
	}

	pool.discoveryMx.Lock()
	discoveryRunning := pool.discoveryRunning
	pool.discoveryMx.Unlock()
	if !discoveryRunning {
		close(waitDHT)
		t.Fatal("ready archive pool did not start background DHT top-up")
	}

	close(waitDHT)
	deadline := time.Now().Add(time.Second)
	for {
		pool.discoveryMx.Lock()
		discoveryRunning = pool.discoveryRunning
		pool.discoveryMx.Unlock()
		if !discoveryRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background DHT top-up did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestArchiveOnlyPeerCloseDoesNotCloseSharedPooledADNL(t *testing.T) {
	pool, pooled, base := newTestLeasedPooledPeer("shared")
	sub := testOverlaySubscription(&overlaySubscription{
		node: &Node{pool: pool},
		spec: overlaySpec{
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	archivePeer := mustNewTestOverlayPeer(t, sub, pooled, nil, false)
	archivePeer.initRebroadcastQueues()
	livePeer := mustNewTestOverlayPeer(t, sub, pooled, nil, false)
	liveOverlay := livePeer.overlay

	closeArchiveOnlyPeer(archivePeer)

	archivePeer.rebroadcastMx.Lock()
	rebroadcastClosed := archivePeer.rebroadcastClosed
	archivePeer.rebroadcastMx.Unlock()
	if !rebroadcastClosed {
		t.Fatal("archive-only close left rebroadcast queues open")
	}
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("archive-only close closed shared pooled ADNL")
	default:
	}
	got, err := pooled.adnl.AttachOverlay(sub.broadcastReceiver)
	if err != nil {
		t.Fatalf("reattach shared live overlay: %v", err)
	}
	if got != liveOverlay {
		t.Fatal("archive-only close unregistered shared live overlay")
	}

	livePeer.close()
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("last overlay release skipped the idle transport grace period")
	default:
	}
	pooled.touch(time.Now().Add(-peerPoolIdleTTL - time.Second))
	pool.pruneIdleLockedForTest(time.Now(), 0)
	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("expired pooled ADNL survived idle pruning")
	}
}

func TestArchiveOnlyPeerCloseRetainsTransportUntilIdleCapEviction(t *testing.T) {
	pool, pooled, base := newTestLeasedPooledPeer("archive-only")
	sub := testOverlaySubscription(&overlaySubscription{
		node: &Node{pool: pool},
		spec: overlaySpec{
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	archivePeer := mustNewTestOverlayPeer(t, sub, pooled, nil, false)

	closeArchiveOnlyPeer(archivePeer)

	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("unused archive-only transport skipped the idle grace period")
	default:
	}
	if len(pool.snapshot()) != 1 {
		t.Fatal("idle archive-only pooled peer was removed immediately")
	}

	pooled.touch(time.Now().Add(-peerPoolIdleTTL - time.Second))
	pool.pruneIdleLockedForTest(time.Now(), 0)
	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("expired archive-only pooled ADNL survived pruning")
	}
	if len(pool.snapshot()) != 0 {
		t.Fatal("expired archive-only pooled peer survived in pool")
	}
}

func TestClosedArchivePoolIgnoresLateSuccess(t *testing.T) {
	peer := testArchiveCandidate("late-success")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}

	pool.Close()
	pool.markSuccess(shard, peer)

	if testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("closed archive pool accepted late peer success")
	}
}

func TestRotateUselessArchivePeersRemovesNotAvailablePeers(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peerA := testArchiveCandidate("archive-miss-a")
	peerB := testArchiveCandidate("archive-miss-b")
	leasedPeer := testArchiveCandidate("leased")
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: &Node{peerUse: map[PeerID]peerUse{}},
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peerA)
	addTestArchiveOnlyPeer(pool, peerB)
	addTestArchiveOnlyPeer(pool, leasedPeer)
	release, ok := pool.acquire(leasedPeer)
	if !ok {
		t.Fatal("failed to lease archive-only peer")
	}
	defer release()

	if pool.noteFailure(shard, peerA, archivePeerRejectNotAvailable).useless {
		t.Fatal("single not-available should not make peer useless")
	}
	if !pool.noteFailure(shard, peerA, archivePeerRejectNotAvailable).useless {
		t.Fatal("second not-available should make unproven peer A useless")
	}
	pool.noteFailure(shard, peerB, archivePeerRejectNotAvailable)
	if !pool.noteFailure(shard, peerB, archivePeerRejectNotAvailable).useless {
		t.Fatal("second not-available should make unproven peer B useless")
	}
	pool.noteFailure(shard, leasedPeer, archivePeerRejectNotAvailable)
	if !pool.noteFailure(shard, leasedPeer, archivePeerRejectNotAvailable).useless {
		t.Fatal("second not-available should make leased peer useless")
	}

	if rotated := pool.rotateUseless(shard); rotated != 2 {
		t.Fatalf("unexpected rotated peer count: got %d want 2", rotated)
	}
	if testArchivePoolHasPeer(pool, peerA.id) {
		t.Fatal("not-available peer A was not removed")
	}
	if testArchivePoolHasPeer(pool, peerB.id) {
		t.Fatal("not-available peer B was not removed")
	}
	if !testArchivePoolHasPeer(pool, leasedPeer.id) {
		t.Fatal("leased peer was removed")
	}
}

func TestRotateUselessArchivePeersMovesProvenPeerToReserve(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	uselessPeer := testArchiveCandidate("useless")
	provenPeer := testArchiveCandidate("proven")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, uselessPeer)
	addTestArchiveOnlyPeer(pool, provenPeer)
	pool.markSuccess(shard, provenPeer)

	for strike := 1; strike <= archivePeerNotAvailableRotateThreshold; strike++ {
		pool.noteFailure(shard, uselessPeer, archivePeerRejectNotAvailable)
		verdict := pool.noteFailure(shard, provenPeer, archivePeerRejectNotAvailable)
		if verdict.useless != (strike == archivePeerNotAvailableRotateThreshold) {
			t.Fatalf("proven peer strike %d useless=%v", strike, verdict.useless)
		}
	}

	if rotated := pool.rotateUseless(shard); rotated != 2 {
		t.Fatalf("unexpected rotated peer count: got %d want 2", rotated)
	}
	if testArchivePoolHasPeer(pool, uselessPeer.id) {
		t.Fatal("useless peer was not removed")
	}
	if testArchivePoolHasPeer(pool, provenPeer.id) {
		t.Fatal("temporarily useless proven peer kept occupying an active slot")
	}
	pool.mx.Lock()
	valuable, reserved := pool.valuable[provenPeer.id]
	pool.mx.Unlock()
	if !reserved || valuable.nextTryAt.IsZero() {
		t.Fatal("proven peer was not retained in the valuable retry reserve")
	}
}

func TestRotateValuableArchivePeerKeepsOtherShardActive(t *testing.T) {
	shardA := archive.ShardID{Workchain: 0, Shard: topShard}
	shardB := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("valuable-reserve")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	if !addTestArchiveOnlyPeer(pool, peer) {
		t.Fatal("failed to add archive-only peer")
	}
	pool.markSuccess(shardA, peer)
	pool.markSuccess(shardB, peer)

	for range archivePeerErrorRotateThreshold {
		pool.noteFailure(shardA, peer, archivePeerRejectDownloadFailed)
	}
	if rotated := pool.rotateUseless(shardA); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}

	pool.mx.Lock()
	_, active := pool.peers[peer.id]
	_, reserved := pool.valuable[peer.id]
	stateA := pool.shards[archivePeerPoolKey(shardA)].peers[peer.id]
	stateB := pool.shards[archivePeerPoolKey(shardB)].peers[peer.id]
	pool.mx.Unlock()
	if !active {
		t.Fatal("failure on one shard removed a peer still useful for another shard")
	}
	if !reserved {
		t.Fatal("valuable peer was not retained in the retry reserve")
	}
	if stateA == nil || stateA.archiveDownloads == 0 || stateB == nil || stateB.archiveDownloads == 0 {
		t.Fatal("rotating a valuable peer erased independent shard history")
	}
	if !pool.coolingDown(shardA, peer) {
		t.Fatal("failed shard route did not enter cooldown")
	}
	if pool.coolingDown(shardB, peer) {
		t.Fatal("failed shard route cooldown leaked into another shard")
	}
}

func TestProvenPeerNotAvailableCooldownEscalates(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("proven-backoff")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)
	pool.markSuccess(shard, peer)

	for strike, want := range archiveNotAvailableCooldowns {
		verdict := pool.noteFailure(shard, peer, archivePeerRejectNotAvailable)
		wantUseless := strike+1 >= archivePeerNotAvailableRotateThreshold
		if verdict.useless != wantUseless {
			t.Fatalf("strike %d useless=%v, want %v", strike+1, verdict.useless, wantUseless)
		}
		if verdict.cooldown != want {
			t.Fatalf("strike %d cooldown = %s, want %s", strike+1, verdict.cooldown, want)
		}
	}
	if verdict := pool.noteFailure(shard, peer, archivePeerRejectNotAvailable); verdict.cooldown != archiveNotAvailableCooldowns[len(archiveNotAvailableCooldowns)-1] {
		t.Fatalf("cooldown after ladder end = %s, want max", verdict.cooldown)
	}

	// ArchiveInfo alone intentionally changes no archive health state.
	if verdict := pool.noteFailure(shard, peer, archivePeerRejectNotAvailable); verdict.cooldown != archiveNotAvailableCooldowns[len(archiveNotAvailableCooldowns)-1] {
		t.Fatalf("cooldown after archive info = %s, want max", verdict.cooldown)
	}

	pool.markSuccess(shard, peer)
	if verdict := pool.noteFailure(shard, peer, archivePeerRejectNotAvailable); verdict.cooldown != archiveNotAvailableCooldowns[0] {
		t.Fatalf("cooldown after archive bytes = %s, want first step", verdict.cooldown)
	}
}

func TestNoteFailureSuccessClearsErrorsButKeepsBadImports(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("decay")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete)
	pool.markSuccess(shard, peer)

	pool.mx.Lock()
	failure := pool.shards[archivePeerPoolKey(shard)].peers[peer.id].failure
	pool.mx.Unlock()
	if got := archivePeerFailureErrors(failure); got != 0 {
		t.Fatalf("errors after success = %d, want 0", got)
	}
	if failure.badImports != 1 {
		t.Fatalf("bad imports after success = %d, want 1 (sticky)", failure.badImports)
	}

	if !pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete).useless {
		t.Fatal("second bad import should make peer useless despite success in between")
	}
}

func TestArchivePoolPrunesClosedArchiveOnlyPeers(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	conns := make([]*testOverlayADNL, 0, archivePeerRosterLimit)

	for i := 0; i < archivePeerRosterLimit; i++ {
		overlayWrapper, conn := newTestOverlayWrapper()
		peer := testArchiveCandidate(fmt.Sprintf("closed-%d", i))
		peer.overlay = overlayWrapper
		if !addTestArchiveOnlyPeer(pool, peer) {
			t.Fatalf("failed to add archive-only peer %d", i)
		}
		pool.noteFailure(shard, peer, archivePeerRejectNotAvailable)
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		conn.Close()
	}

	if size := pool.size(); size != 0 {
		t.Fatalf("closed archive-only peers should not count towards pool size, got %d", size)
	}
	if got := pool.candidates(shard); len(got) != 0 {
		t.Fatalf("closed archive-only peers should not remain candidates, got %#v", got)
	}

	pool.mx.Lock()
	shards := len(pool.shards)
	pool.mx.Unlock()
	if shards != 0 {
		t.Fatalf("closed archive-only peers left shard state: %d", shards)
	}
}

func TestArchivePoolClosedPeersDoNotBlockRefreshLimit(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	conns := make([]*testOverlayADNL, 0, archivePeerRosterLimit)

	for i := 0; i < archivePeerRosterLimit; i++ {
		overlayWrapper, conn := newTestOverlayWrapper()
		peer := testArchiveCandidate(fmt.Sprintf("closed-hard-limit-%d", i))
		peer.overlay = overlayWrapper
		if !addTestArchiveOnlyPeer(pool, peer) {
			t.Fatalf("failed to add archive-only peer %d", i)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		conn.Close()
	}

	replacement := testArchiveCandidate("replacement")
	if !addTestArchiveOnlyPeer(pool, replacement) {
		t.Fatal("closed archive-only peers blocked refresh-limit replacement")
	}
	if size := pool.size(); size != 1 {
		t.Fatalf("unexpected pool size after hard limit cleanup: got %d want 1", size)
	}
}

func TestArchivePoolPrunesClosedBorrowedPeerWithoutClosingConnection(t *testing.T) {
	overlayWrapper, conn := newTestOverlayWrapper()
	peer := testArchiveCandidate("borrowed-closed")
	peer.overlay = overlayWrapper
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	})
	pool := testArchivePool(t, sub)
	conn.Close()

	if size := pool.size(); size != 0 {
		t.Fatalf("closed borrowed peer should not count towards archive pool size, got %d", size)
	}
	if _, ok := sub.peers[peer.id]; !ok {
		t.Fatal("closed borrowed peer was removed from live subscription")
	}
}

func TestArchivePoolKeepsLeasedClosedPeerUntilRelease(t *testing.T) {
	overlayWrapper, conn := newTestOverlayWrapper()
	peer := testArchiveCandidate("leased-closed")
	peer.overlay = overlayWrapper
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	if !addTestArchiveOnlyPeer(pool, peer) {
		t.Fatal("failed to add archive-only peer")
	}
	release, ok := pool.acquire(peer)
	if !ok {
		t.Fatal("failed to lease archive-only peer")
	}
	conn.Close()

	pool.pruneClosedPeers()
	if !testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("leased closed peer was removed before release")
	}

	release()
	pool.pruneClosedPeers()
	if testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("closed peer survived release and cleanup")
	}
}

func TestArchivePoolPrunesDeadUnprovenArchiveOnlyPeers(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	peers := make([]*overlayPeer, 0, 3)

	for i := 0; i < cap(peers); i++ {
		peers = append(peers, testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("dead-unproven-%d", i)))
	}
	for _, peer := range peers {
		peer.alive = false
	}

	pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now())
	if usable := pool.usableSize(time.Now()); usable != 0 {
		t.Fatalf("dead unproven archive-only peers should not be usable, got %d", usable)
	}
	if size := pool.size(); size != 0 {
		t.Fatalf("dead unproven archive-only peers should be pruned, got size %d", size)
	}
	for _, peer := range peers {
		if testArchivePoolHasPeer(pool, peer.id) {
			t.Fatalf("dead unproven archive-only peer %s survived prune", peer.addr)
		}
	}
}

func TestArchivePoolKeepsLeasedAndProvenDeadArchiveOnlyPeers(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	leased := testArchiveOnlyPoolPeer(t, pool, "leased-dead-unproven")
	proven := testArchiveOnlyPoolPeer(t, pool, "proven-dead")
	release, ok := pool.acquire(leased)
	if !ok {
		t.Fatal("failed to lease archive-only peer")
	}
	pool.markSuccess(shard, proven)
	leased.alive = false
	proven.alive = false

	pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now())
	if !testArchivePoolHasPeer(pool, leased.id) {
		t.Fatal("leased dead archive-only peer was removed")
	}
	if !testArchivePoolHasPeer(pool, proven.id) {
		t.Fatal("proven dead archive-only peer was removed")
	}
	if usable := pool.usableSize(time.Now()); usable != 1 {
		t.Fatalf("only proven dead archive-only peer should remain usable, got %d", usable)
	}

	release()
	pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now())
	if testArchivePoolHasPeer(pool, leased.id) {
		t.Fatal("released dead unproven archive-only peer survived prune")
	}
	if !testArchivePoolHasPeer(pool, proven.id) {
		t.Fatal("proven dead archive-only peer was pruned after leased peer release")
	}
}

func TestArchivePoolRefillStartsDHTWhenPoolFullOfUnprovenPeers(t *testing.T) {
	fake := &fakeDHTClient{}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht: fake,
		},
		spec: overlaySpec{
			FullID:  []byte{0x01},
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 100)

	for i := 0; i < archivePeerRosterLimit; i++ {
		testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("junk-%d", i))
	}

	done := pool.refill(context.Background(), false)
	if done == nil {
		t.Fatal("unproven junk peers muted archive DHT refill")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("archive DHT refill did not finish")
	}
	if fake.findOverlayNodesCallCount() == 0 {
		t.Fatal("archive DHT refill did not query DHT")
	}
}

func TestArchivePoolRefillRefreshesDHTWithEnoughProvenPeers(t *testing.T) {
	fake := &fakeDHTClient{findOverlayNodesContinuation: &dht.Continuation{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht: fake,
		},
		spec: overlaySpec{
			FullID:  []byte{0x01},
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 100)

	for i := 0; i < 4; i++ {
		peer := testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("proven-%d", i))
		pool.markSuccess(shard, peer)
	}

	done := pool.refill(context.Background(), false)
	if done == nil {
		t.Fatal("proven-peer target suppressed periodic DHT refresh")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("periodic archive DHT refresh did not finish")
	}
	if calls := fake.findOverlayNodesCallCount(); calls != 1 {
		t.Fatalf("periodic archive DHT seed calls = %d, want 1", calls)
	}

	if done := pool.refill(context.Background(), false); done != nil {
		t.Fatal("periodic archive DHT refresh ignored its cooldown")
	}
	if calls := fake.findOverlayNodesCallCount(); calls != 1 {
		t.Fatalf("DHT calls during refresh cooldown = %d, want 1", calls)
	}
}

func TestArchivePoolRefillUsesOneDHTSeedPage(t *testing.T) {
	fake := &fakeDHTClient{findOverlayNodesContinuation: &dht.Continuation{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht: fake,
		},
		spec: overlaySpec{
			FullID:  []byte{0x01},
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 100)

	done := pool.refill(context.Background(), false)
	if done == nil {
		t.Fatal("archive pool did not start its DHT seed lookup")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("archive DHT top-up did not finish")
	}
	if calls := fake.findOverlayNodesCallCount(); calls != 1 {
		t.Fatalf("DHT seed calls = %d, want 1 even with a continuation", calls)
	}
}

func TestArchivePoolDiscoveryUsesOneMinuteInterval(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	now := time.Now()
	pool.discoveryMx.Lock()
	pool.nextDiscoveryAt = now.Add(archiveDiscoveryInterval)
	pool.discoveryMx.Unlock()

	if pool.shouldDiscoverDHT(now, false) {
		t.Fatal("archive pool ignored the discovery interval")
	}
	pool.discoveryMx.Lock()
	nextDiscoveryAt := pool.nextDiscoveryAt
	pool.discoveryMx.Unlock()
	if got := nextDiscoveryAt; !got.Equal(now.Add(archiveDiscoveryInterval)) {
		t.Fatalf("next discovery = %s, want %s", got.Sub(now), archiveDiscoveryInterval)
	}
	if !pool.shouldDiscoverDHT(now.Add(archiveDiscoveryInterval+time.Nanosecond), false) {
		t.Fatal("archive pool did not reopen discovery after one minute")
	}
}

func TestArchivePoolUrgentRefillBypassesCalmDiscoveryInterval(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)

	now := time.Now()
	pool.discoveryMx.Lock()
	pool.nextDiscoveryAt = now.Add(archiveDiscoveryInterval)
	pool.lastDiscoveryAt = now.Add(-archiveDiscoveryUrgentInterval)
	pool.discoveryMx.Unlock()

	if !pool.shouldDiscoverDHT(now, true) {
		t.Fatal("urgent refill did not bypass the calm one-minute DHT interval")
	}
	pool.discoveryMx.Lock()
	pool.lastDiscoveryAt = now
	pool.discoveryMx.Unlock()
	if pool.shouldDiscoverDHT(now.Add(time.Nanosecond), true) {
		t.Fatal("urgent DHT refill ignored its short anti-spin interval")
	}
}

func TestArchivePoolCloseCancelsDHTDiscovery(t *testing.T) {
	waitDHT := make(chan struct{})
	started := make(chan struct{}, 1)
	fake := &fakeDHTClient{
		findOverlayNodesWait:    waitDHT,
		findOverlayNodesStarted: started,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht: fake,
		},
		spec: overlaySpec{
			FullID:  []byte{0x01},
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 100)

	done := pool.refill(context.Background(), false)
	if done == nil {
		t.Fatal("archive DHT discovery did not start")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		close(waitDHT)
		t.Fatal("archive DHT discovery did not reach lookup")
	}

	pool.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		close(waitDHT)
		t.Fatal("closing archive pool did not cancel DHT discovery")
	}
}

func TestArchivePoolKeepsValuablePeersUsableAfterAnnouncementExpires(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: 0, Shard: topShard}

	const valuablePeers = 4
	for i := 0; i < valuablePeers; i++ {
		peer := testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("stale-proven-%d", i))
		pool.markSuccess(shard, peer)
		peer.announced = &overlay.Node{Version: int32(time.Now().Add(-overlayPeerTTL - time.Second).Unix())}
		peer.alive = false
	}

	if got := pool.provenUsableSize(time.Now()); got != valuablePeers {
		t.Fatalf("valuable peers usable after announcement expiry = %d, want %d", got, valuablePeers)
	}
	if got := len(pool.candidates(shard)); got != valuablePeers {
		t.Fatalf("valuable peers were removed from archive candidates: %d", got)
	}
}

func TestArchivePoolKeepsStaleStandbyPeersUntilUsefulReplacement(t *testing.T) {
	const borrowedPeers = 10
	peers := make(map[PeerID]*overlayPeer, borrowedPeers)
	for i := 0; i < borrowedPeers; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("borrowed-%d", i))
		peers[peer.id] = peer
	}
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: peers,
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: 0, Shard: topShard}

	for i := 0; i < archivePeerRosterLimit; i++ {
		peer := testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("stale-standby-%d", i))
		pool.markSuccess(shard, peer)
	}

	pool.pruneStaleArchiveOnlyPeers()
	if got := pool.archiveOnlySize(); got != archivePeerRosterLimit {
		t.Fatalf("archive-only peers after stale standby prune = %d, want %d", got, archivePeerRosterLimit)
	}
	if got := pool.size(); got != archivePeerRosterLimit {
		t.Fatalf("archive peers after standby prune = %d, want %d", got, archivePeerRosterLimit)
	}
}

func TestArchivePoolArchiveOnlyRefreshLimitIsAtomic(t *testing.T) {
	const candidates = archivePeerRosterLimit * 4

	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	peers := make([]*overlayPeer, 0, candidates)
	for i := 0; i < candidates; i++ {
		overlayWrapper, _ := newTestOverlayWrapper()
		peer := testArchiveCandidate(fmt.Sprintf("concurrent-candidate-%d", i))
		peer.overlay = overlayWrapper
		peers = append(peers, peer)
	}

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Go(func() {
			addTestArchiveOnlyPeer(pool, peer)
		})
	}
	wg.Wait()

	if got := pool.archiveOnlySize(); got != archivePeerRosterLimit {
		t.Fatalf("archive-only peers after concurrent adds = %d, want %d", got, archivePeerRosterLimit)
	}
}

func TestArchivePoolForcedRefillStartsDHTWhenDue(t *testing.T) {
	fake := &fakeDHTClient{}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		node: &Node{
			dht: fake,
		},
		spec: overlaySpec{
			FullID:  []byte{0x01},
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 100)

	done := pool.refill(context.Background(), true)
	if done == nil {
		t.Fatal("forced refill did not start a due DHT seed lookup")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forced archive DHT refill did not finish")
	}
	if fake.findOverlayNodesCallCount() == 0 {
		t.Fatal("forced archive DHT refill did not query DHT")
	}
}

func TestArchivePoolDoesNotChangeLiveRosterLimit(t *testing.T) {
	if maxPeersPerOverlay != 20 {
		t.Fatalf("live overlay roster limit = %d, want 20", maxPeersPerOverlay)
	}
	if archivePeerRosterLimit != 40 {
		t.Fatalf("archive roster limit = %d, want 40", archivePeerRosterLimit)
	}

	peers := make(map[PeerID]*overlayPeer, maxPeersPerOverlay)
	for i := 0; i < maxPeersPerOverlay; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("live-%d", i))
		peers[peer.id] = peer
	}
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: peers,
	})
	pool := testArchivePool(t, sub)

	for i := 0; i < archivePeerRosterLimit; i++ {
		testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("archive-%d", i))
	}
	if got := len(sub.peersSnapshot()); got != maxPeersPerOverlay {
		t.Fatalf("archive roster changed live roster size = %d, want %d", got, maxPeersPerOverlay)
	}
	if got := pool.archiveOnlySize(); got != archivePeerRosterLimit {
		t.Fatalf("archive-only roster size = %d, want %d", got, archivePeerRosterLimit)
	}
}

func TestArchiveNotAvailableBackoffIsExactDemandOnly(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	peer := testArchiveCandidate("distance-specific-archive")
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)
	oldProbe, releaseOld, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin old archive demand: %v", err)
	}

	pool.recordDemandNotAvailable(oldProbe, peer.id, archivePeerNotAvailableTTL)
	releaseOld()
	_, releaseOldRetry, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("restart old archive demand: %v", err)
	}
	defer releaseOldRetry()
	_, releaseOther, err := pool.beginArchiveRequest(shard, 200)
	if err != nil {
		t.Fatalf("begin other archive demand: %v", err)
	}
	defer releaseOther()

	if got := pool.candidatesForArchive(shard, 100); len(got) != 0 {
		t.Fatalf("old archive miss returned %d candidates after demand restart, want 0", len(got))
	}
	session := (&Node{runCtx: context.Background()}).BeginArchiveSession()
	defer session.Close()
	session.selectArchivePeerFromPool(shard, peer, pool)
	if got := pool.downloadCandidatesForArchive(session, shard, 100, pool.candidates(shard)); len(got) != 0 {
		t.Fatalf("sticky peer bypassed exact archive backoff: %#v", got)
	}
	got := pool.candidatesForArchive(shard, 200)
	if len(got) != 1 || got[0] != peer {
		t.Fatalf("old archive miss blocked another distance: %#v", got)
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("exact archive miss leaked into shard-wide cooldown")
	}
	pool.recordArchiveDemandEvidence(shard, 100, peer, archivePeerDemandProven)
	if got := pool.candidatesForArchive(shard, 100); len(got) != 1 || got[0] != peer {
		t.Fatalf("real archive evidence did not clear exact backoff: %#v", got)
	}
}

func TestArchiveAvailabilityDoesNotProvePeerOrResetDownloadFailures(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("info-only")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	// ArchiveInfo alone intentionally does not reset download failures.

	pool.mx.Lock()
	state := pool.shards[archivePeerPoolKey(shard)]
	var failure archivePeerFailure
	if state != nil && state.peers[peer.id] != nil {
		failure = state.peers[peer.id].failure
	}
	pool.mx.Unlock()
	if failure.downloadErrors != 1 {
		t.Fatalf("download failures after archive info = %d, want 1", failure.downloadErrors)
	}
	if got := pool.provenUsableSize(time.Now()); got != 0 {
		t.Fatalf("proven peers after archive info = %d, want 0", got)
	}

	pool.markSuccess(shard, peer)

	pool.mx.Lock()
	failure = pool.shards[archivePeerPoolKey(shard)].peers[peer.id].failure
	pool.mx.Unlock()
	if failure.downloadErrors != 0 {
		t.Fatal("real archive bytes did not clear download failures")
	}
	if got := pool.provenUsableSize(time.Now()); got != 1 {
		t.Fatalf("proven peers after archive bytes = %d, want 1", got)
	}
}

func TestRotateUselessArchivePeersWaitsForRepeatedErrors(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("flaky")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	for i := 0; i < archivePeerErrorRotateThreshold-1; i++ {
		if pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed).useless {
			t.Fatalf("download error %d made peer useless before threshold", i+1)
		}
	}
	if rotated := pool.rotateUseless(shard); rotated != 0 {
		t.Fatalf("unexpected early rotated peer count: got %d want 0", rotated)
	}
	if !testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("peer was rotated before repeated error threshold")
	}

	if !pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed).useless {
		t.Fatal("peer should become useless after repeated errors")
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("peer survived repeated archive errors")
	}
}

func TestRotatedArchivePeerNegativeCacheBlocksReconnect(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("rediscovered")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	for i := 0; i < archivePeerNotAvailableRotateThreshold; i++ {
		pool.noteFailure(shard, peer, archivePeerRejectNotAvailable)
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}

	if !pool.recentlyRejected(peer.id, time.Now()) {
		t.Fatal("rotated junk peer missing from negative cache")
	}
	if pool.scout.retry.peerBlocked(peer.id, time.Now()) {
		t.Fatal("archive-only rejection leaked into getRandomPeers transport backoff")
	}

	// A stale pointer cannot clear the cache after its roster entry was removed.
	pool.markSuccess(shard, peer)
	if !pool.recentlyRejected(peer.id, time.Now()) {
		t.Fatal("stale success cleared rotated peer negative cache")
	}

	// A successful in-flight scout result contains real bytes and may admit a
	// fresh connection even if the earlier roster entry was just rotated.
	fresh := testArchiveCandidate("rediscovered")
	result := pool.admitArchiveOnlyPeer(fresh, provenArchiveScoutResult(t, pool, shard, 2<<20))
	if !result.admitted {
		t.Fatal("real archive bytes did not admit a recovered peer")
	}
	if pool.recentlyRejected(peer.id, time.Now()) {
		t.Fatal("real archive bytes did not clear recovered peer negative cache")
	}
}

func TestRotatedProvenArchivePeerMovesToValuableReserve(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("proven-rotated")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)
	pool.markSuccess(shard, peer)

	for i := 0; i < archivePeerErrorRotateThreshold; i++ {
		pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	pool.mx.Lock()
	valuable, reserved := pool.valuable[peer.id]
	pool.mx.Unlock()
	if pool.recentlyRejected(peer.id, time.Now()) {
		t.Fatal("valuable peer entered ordinary archive rejection cache")
	}
	if !reserved || valuable.nextTryAt.IsZero() {
		t.Fatalf("valuable reserve after rotation = %+v reserved=%v", valuable, reserved)
	}
}

func TestBadArchiveImportMakesPeerUseless(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("bad-import")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	verdict := pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete)
	if verdict.useless {
		t.Fatal("first bad archive import should not make peer useless")
	}
	if verdict.cooldown != archiveFailureCooldown {
		t.Fatalf("first bad import cooldown = %s, want %s", verdict.cooldown, archiveFailureCooldown)
	}
	if rotated := pool.rotateUseless(shard); rotated != 0 {
		t.Fatalf("peer rotated after single bad import: got %d", rotated)
	}

	if !pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete).useless {
		t.Fatal("second bad archive import should make peer useless")
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("bad import peer survived rotation")
	}
}

func TestArchiveQueryCandidatesUseAllAliveKnownPeers(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[PeerID]*overlayPeer{
			testPeerID("peer-1"): {id: testPeerID("peer-1"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			testPeerID("peer-2"): {id: testPeerID("peer-2"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			testPeerID("peer-3"): {id: testPeerID("peer-3"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
		},
		neighbours: []PeerID{testPeerID("peer-1"), testPeerID("peer-2")},
	})
	pool := testArchivePool(t, sub)
	for _, peer := range sub.peersSnapshot() {
		addTestArchiveOnlyPeer(pool, peer)
	}

	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 3 {
		t.Fatalf("archive candidates should use all alive known peers, got %d", len(got))
	}
	seen := map[PeerID]struct{}{
		got[0].id: {},
		got[1].id: {},
		got[2].id: {},
	}
	if _, ok := seen[testPeerID("peer-1")]; !ok {
		t.Fatalf("expected peer-1 in archive candidates, got %#v", got)
	}
	if _, ok := seen[testPeerID("peer-2")]; !ok {
		t.Fatalf("expected peer-2 in archive candidates, got %#v", got)
	}
	if _, ok := seen[testPeerID("peer-3")]; !ok {
		t.Fatalf("expected peer-3 in archive candidates, got %#v", got)
	}
}

func TestArchiveDownloadCandidatesPutSelectedPeerFirst(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	selected := testArchiveCandidate("selected")
	fast := testArchiveCandidate("fast")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			selected.id: selected,
			fast.id:     fast,
		},
	})
	session := node.BeginArchiveSession()
	defer session.Close()
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, selected)
	addTestArchiveOnlyPeer(pool, fast)

	session.selectArchivePeer(shard, selected)
	got := pool.downloadCandidates(session, shard, []*overlayPeer{fast, selected})
	if len(got) != 2 || got[0] != selected {
		t.Fatalf("selected archive peer should stay first, got %#v", got)
	}
}

func TestArchiveSessionComparativeHedgeCadence(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	selected := testArchiveCandidate("selected")
	node := &Node{
		runCtx:  context.Background(),
		peerUse: map[PeerID]peerUse{},
	}
	session := node.BeginArchiveSession()
	defer session.Close()
	now := time.Now()

	if session.shouldHedgeArchiveDownload(shard, true, now) {
		t.Fatal("archive session without selected peer should not hedge")
	}

	session.selectArchivePeer(shard, selected)
	if session.shouldHedgeArchiveDownload(shard, false, now) {
		t.Fatal("archive session should not hedge without alternatives")
	}
	if !session.shouldHedgeArchiveDownload(shard, true, now) {
		t.Fatal("archive session should hedge first selected peer download with alternatives")
	}
	if session.shouldHedgeArchiveDownload(shard, true, now.Add(archiveSessionComparativeHedgeGap-time.Millisecond)) {
		t.Fatal("archive session hedged again before comparative hedge gap")
	}
	if !session.shouldHedgeArchiveDownload(shard, true, now.Add(archiveSessionComparativeHedgeGap)) {
		t.Fatal("archive session should hedge again after comparative hedge gap")
	}
}

func TestArchiveDownloadCandidatesDropMissingSelectedPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	selected := testArchiveCandidate("selected")
	fast := testArchiveCandidate("fast")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			fast.id: fast,
		},
	})
	session := node.BeginArchiveSession()
	defer session.Close()
	pool := testArchivePool(t, sub)

	session.selectArchivePeer(shard, selected)
	if _, ok := node.protectedPeerIDs()[selected.id]; ok {
		t.Fatal("archive selection entered live peer protection")
	}

	got := pool.downloadCandidates(session, shard, []*overlayPeer{fast})
	if len(got) != 1 || got[0] != fast {
		t.Fatalf("missing selected peer should not stay in candidates, got %#v", got)
	}
	if selectedID := session.selectedArchivePeerID(shard); !selectedID.IsZero() {
		t.Fatalf("missing selected archive peer was not cleared: %s", selectedID.String())
	}
	if _, ok := node.protectedPeerIDs()[selected.id]; ok {
		t.Fatal("missing selected archive peer stayed pinned")
	}
}

func TestArchiveQueryCandidatesKeepProvenArchivePeerAfterAnnouncementExpires(t *testing.T) {
	now := int32(time.Now().Add(-overlayPeerTTL - time.Second).Unix())
	peer := testArchiveCandidate("archive-retained")
	peer.announced = &overlay.Node{Version: now}
	peer.alive = false
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	addTestArchiveOnlyPeer(pool, peer)
	pool.markSuccess(shard, peer)

	got := pool.candidates(shard)
	if len(got) != 1 || got[0] != peer {
		t.Fatalf("recent archive peer should remain an archive candidate, got %#v", got)
	}
}

func TestArchivePeerProvenForWorkchainCanServeAnotherShard(t *testing.T) {
	peer := testArchiveCandidate("workchain-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	})
	pool := testArchivePool(t, sub)
	firstShard := archive.ShardID{Workchain: 0, Shard: topShard}
	otherShard := archive.ShardID{Workchain: 0, Shard: topShard >> 1}
	addTestArchiveOnlyPeer(pool, peer)

	pool.markSuccess(firstShard, peer)

	got := pool.candidates(otherShard)
	if len(got) == 0 || got[0] != peer {
		t.Fatalf("workchain-proven peer should be candidate for another shard, got %#v", got)
	}
}

func TestArchiveNotAvailableIsShardLocalForProvenWorkchainPeer(t *testing.T) {
	peer := testArchiveCandidate("workchain-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	})
	pool := testArchivePool(t, sub)
	firstShard := archive.ShardID{Workchain: 0, Shard: topShard}
	otherShard := archive.ShardID{Workchain: 0, Shard: topShard >> 1}
	addTestArchiveOnlyPeer(pool, peer)

	pool.markSuccess(firstShard, peer)
	pool.noteFailure(firstShard, peer, archivePeerRejectNotAvailable)

	if got := pool.candidates(firstShard); len(got) != 0 {
		t.Fatalf("not-available shard should be cooled down, got %#v", got)
	}
	got := pool.candidates(otherShard)
	if len(got) == 0 || got[0] != peer {
		t.Fatalf("shard-local not_available should not ban workchain peer, got %#v", got)
	}
}

func TestArchiveShardParallelismPrefersUnleasedPeer(t *testing.T) {
	busy := testArchiveCandidate("busy")
	free := testArchiveCandidate("free")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			busy.id: busy,
			free.id: free,
		},
	})
	pool := testArchivePool(t, sub)
	shardA := archive.ShardID{Workchain: 0, Shard: topShard}
	shardB := archive.ShardID{Workchain: 0, Shard: topShard >> 1}
	addTestArchiveOnlyPeer(pool, busy)
	addTestArchiveOnlyPeer(pool, free)
	pool.markSuccess(shardA, busy)
	pool.markSuccess(shardA, free)
	release, ok := pool.acquire(busy)
	if !ok {
		t.Fatal("failed to lease busy archive peer")
	}
	defer release()

	session := sub.node.BeginArchiveSession()
	defer session.Close()
	session.selectArchivePeer(shardB, busy)

	got := pool.downloadCandidates(session, shardB, []*overlayPeer{busy, free})
	if len(got) == 0 || got[0] != free {
		t.Fatalf("leased archive peer should not be first for parallel shard, got %#v", got)
	}
}

func TestArchiveDownloadCandidatesKeepSelectedProvenPeerAfterAnnouncementExpires(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	now := int32(time.Now().Add(-overlayPeerTTL - time.Second).Unix())
	selected := testArchiveCandidate("selected")
	selected.announced = &overlay.Node{Version: now}
	selected.alive = false
	fast := testArchiveCandidate("fast")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			selected.id: selected,
			fast.id:     fast,
		},
	})
	session := node.BeginArchiveSession()
	defer session.Close()
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, selected)
	addTestArchiveOnlyPeer(pool, fast)
	pool.markSuccess(shard, selected)

	session.selectArchivePeer(shard, selected)
	got := pool.downloadCandidates(session, shard, []*overlayPeer{fast, selected})
	if len(got) != 2 || got[0] != selected {
		t.Fatalf("recent selected archive peer should stay first, got %#v", got)
	}
	if _, ok := node.protectedPeerIDs()[selected.id]; ok {
		t.Fatal("recent archive selection entered live peer protection")
	}
}

func TestArchiveQueryCandidatesUseKnownPeersWithoutNeighbours(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[PeerID]*overlayPeer{
			testPeerID("peer-1"): {id: testPeerID("peer-1"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			testPeerID("peer-2"): {id: testPeerID("peer-2"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
		},
	})
	pool := testArchivePool(t, sub)
	for _, peer := range sub.peersSnapshot() {
		addTestArchiveOnlyPeer(pool, peer)
	}

	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 2 {
		t.Fatalf("archive candidates should use known peers without neighbours, got %d", len(got))
	}
}

func TestArchiveQueryCandidatesSkipDeadKnownPeers(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[PeerID]*overlayPeer{
			testPeerID("alive"): {id: testPeerID("alive"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			testPeerID("dead"):  {id: testPeerID("dead"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: false},
		},
		neighbours: []PeerID{testPeerID("dead"), testPeerID("alive")},
	})
	pool := testArchivePool(t, sub)
	for _, peer := range sub.peersSnapshot() {
		addTestArchiveOnlyPeer(pool, peer)
	}

	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 1 {
		t.Fatalf("unexpected archive candidate count: got %d want 1", len(got))
	}
	if got[0].id != testPeerID("alive") {
		t.Fatalf("expected only alive peer, got %q", got[0].id)
	}
}

func TestArchiveTrafficUsesOnlyPoolLocalPerformance(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("archive-local-performance")
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)
	before := peer.statsSnapshot()

	pool.noteArchiveSeedSuccess(shard, peer, int64(archiveSliceProbeSize), time.Second)
	pool.markProven(shard, peer)
	pool.noteArchiveDownload(shard, peer, 100<<10, 2*time.Second)
	pool.markSuccess(shard, peer)

	if after := peer.statsSnapshot(); after != before {
		t.Fatalf("archive traffic changed live peer stats: before=%+v after=%+v", before, after)
	}
	performance, ok := pool.peerPerformance(shard, peer.id)
	if !ok || performance.probeSuccesses == 0 || performance.archiveDownloads != 1 {
		t.Fatalf("archive-local performance = %+v ok=%v", performance, ok)
	}
}

func TestArchivePerformanceUsesRelativeRatesAtAnyLinkSpeed(t *testing.T) {
	tests := []struct {
		name     string
		slowRate int64
		fastRate int64
	}{
		{name: "small link", slowRate: 8 << 20, fastRate: 10 << 20},
		{name: "large link", slowRate: 50 << 20, fastRate: 70 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slow := testArchiveCandidate(tt.name + "-slow")
			fast := testArchiveCandidate(tt.name + "-fast")
			performance := map[PeerID]archivePeerPerformance{
				slow.id: {
					archiveDownloads: 1,
					bytes:            tt.slowRate,
					downloadElapsed:  time.Second,
				},
				fast.id: {
					archiveDownloads: 1,
					bytes:            tt.fastRate,
					downloadElapsed:  time.Second,
				},
			}

			ordered := prioritizeArchivePeersWithPerformance(
				archive.ShardID{Workchain: -1, Shard: topShard},
				[]*overlayPeer{slow, fast},
				nil,
				performance,
			)
			if ordered[0] != fast {
				t.Fatalf("relative ranking selected %q, want %q", ordered[0].addr, fast.addr)
			}
		})
	}
}

func TestArchiveCompletedRateIsWeightedByBytesAndTime(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	peer := testArchiveCandidate("weighted-rate")
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	pool.noteArchiveDownload(shard, peer, 100<<10, time.Second)
	pool.noteArchiveDownload(shard, peer, 400<<20, 40*time.Second)

	performance, ok := pool.peerPerformance(shard, peer.id)
	if !ok {
		t.Fatal("archive performance missing")
	}
	want := float64((100<<10)+(400<<20)) / 41
	if got := performance.bytesPerSecond(); got != want {
		t.Fatalf("weighted archive rate = %f, want %f", got, want)
	}
}

func TestArchiveFreshProbeOverridesHistoricalCompletedRate(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	peer := testArchiveCandidate("fresh-probe-rate")
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	pool.noteArchiveDownload(shard, peer, 70<<20, time.Second)
	pool.noteArchiveSeedSuccess(shard, peer, int64(archiveSliceProbeSize), 32*time.Millisecond)

	performance, ok := pool.peerPerformance(shard, peer.id)
	if !ok {
		t.Fatal("archive performance missing")
	}
	want := float64(archiveSliceProbeSize) / (32 * time.Millisecond).Seconds()
	if got := performance.bytesPerSecond(); got != want {
		t.Fatalf("fresh archive probe rate = %f, want %f", got, want)
	}
}

func TestArchiveFailureDoesNotMutateLiveRosterOrProtection(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	live := testArchiveCandidate("same-identity")
	archivePeer := testArchiveCandidate("same-identity")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{live.id: live},
	})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, archivePeer)
	session := node.BeginArchiveSession()
	defer session.Close()
	before := live.statsSnapshot()

	session.selectArchivePeerFromPool(shard, archivePeer, pool)
	sub.noteArchiveDownloadError(context.Background(), session, pool, 1, shard, archivePeer, context.DeadlineExceeded)

	if sub.peerByID(live.id) != live {
		t.Fatal("archive failure changed the live roster entry")
	}
	if after := live.statsSnapshot(); after != before {
		t.Fatalf("archive failure changed live stats: before=%+v after=%+v", before, after)
	}
	if _, protected := node.protectedPeerIDs()[live.id]; protected {
		t.Fatal("archive selection or failure protected the peer in live policy")
	}
	if !pool.transportBlocked(archivePeer.id, time.Now()) {
		t.Fatal("archive timeout did not enter the archive transport backoff")
	}
	if got := pool.candidates(shard); len(got) != 0 {
		t.Fatalf("archive timeout peer remained a candidate: %#v", got)
	}
	if !pool.coolingDown(shard, archivePeer) {
		t.Fatal("first archive timeout did not cool down the archive route")
	}
}

func TestArchiveInfoDoesNotKeepDeadPeerActive(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "info-only-dead")

	peer.statsMx.Lock()
	peer.announced = &overlay.Node{Version: int32(time.Now().Add(-overlayPeerTTL - time.Second).Unix())}
	peer.alive = false
	peer.statsMx.Unlock()

	pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now())
	if got := pool.candidates(shard); len(got) != 0 {
		t.Fatalf("ArchiveInfo-only dead peer remained a candidate: %#v", got)
	}
}

func TestArchiveValuableReserveRequiresCompletedDownload(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "valuable-lifecycle")

	pool.noteArchiveSeedSuccess(shard, peer, int64(archiveSliceProbeSize), time.Second)
	pool.markProven(shard, peer)
	if got := pool.valuableSize(); got != 0 {
		t.Fatalf("probe-only peer entered valuable reserve: %d", got)
	}

	pool.noteArchiveDownload(shard, peer, 100<<10, time.Second)
	pool.markSuccess(shard, peer)
	if got := pool.valuableSize(); got != 1 {
		t.Fatalf("completed archive did not create valuable reserve entry: %d", got)
	}

	for range archivePeerErrorRotateThreshold {
		pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("valuable peer rotation count = %d, want 1", rotated)
	}
	if testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("failed valuable peer remained active")
	}
	if pool.recentlyRejected(peer.id, time.Now()) {
		t.Fatal("valuable peer entered the ordinary archive rejection cache")
	}
	pool.mx.Lock()
	valuable, ok := pool.valuable[peer.id]
	pool.mx.Unlock()
	if !ok || valuable.nextTryAt.IsZero() {
		t.Fatalf("valuable reserve entry after rotation = %+v ok=%v", valuable, ok)
	}
}

func TestArchivePeerProbeRequiresRealSliceBytes(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		spec: overlaySpec{ShortID: []byte{0x01}},
	})
	pool := testArchivePool(t, sub)
	beginTestArchiveRequest(t, pool, shard, 100)
	probe, ok := pool.probeSnapshot()
	if !ok {
		t.Fatal("archive probe was not recorded")
	}

	junkOverlay, junkBase := newTestOverlayWrapper()
	junk := testArchiveCandidate("classify-junk")
	junk.overlay = junkOverlay
	junkBase.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		if out, ok := result.(*tl.Serializable); ok {
			*out = ArchiveNotFound{}
		}
		return nil
	}
	if _, err := pool.probeArchivePeerEvidence(context.Background(), junk, probe); !errors.Is(err, archive.ErrNotAvailable) {
		t.Fatalf("junk probe error = %v, want archive not available", err)
	}

	serving := testArchiveDownloadPeer(t, "classify-serving", 42, testArchivePackBytes("classify-serving"), 0)
	result, err := pool.probeArchivePeerEvidence(context.Background(), serving, probe)
	if err != nil {
		t.Fatalf("serving peer probe: %v", err)
	}
	if result.evidence != archivePeerEvidenceProven || result.bytes == 0 {
		t.Fatalf("serving peer evidence = %+v, want real archive bytes", result)
	}
	if admission := pool.admitArchiveOnlyPeer(serving, result); !admission.admitted {
		t.Fatal("proven serving peer was not admitted")
	}

	erring := testArchiveCandidate("classify-error")
	erringOverlay, erringBase := newTestOverlayWrapper()
	erring.overlay = erringOverlay
	erringBase.queryResponder = func(tl.Serializable, tl.Serializable) error {
		return context.DeadlineExceeded
	}
	if _, err = pool.probeArchivePeerEvidence(context.Background(), erring, probe); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("erring peer probe error = %v, want deadline", err)
	}

	canceled := testArchiveCandidate("classify-canceled")
	canceledOverlay, canceledBase := newTestOverlayWrapper()
	canceled.overlay = canceledOverlay
	canceledBase.queryResponder = func(tl.Serializable, tl.Serializable) error {
		return context.Canceled
	}
	if _, err = pool.probeArchivePeerEvidence(context.Background(), canceled, probe); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled peer probe error = %v, want cancellation", err)
	}
}

func TestArchivePeerZeroStateProbeRecordsAvailabilityOnly(t *testing.T) {
	block := testBlockID(-1, topShard, 0)
	shard := archiveShardFromBlock(block)
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		spec: overlaySpec{ShortID: []byte{0x01}},
	})
	pool := testArchivePool(t, sub)
	beginTestZeroStateRequest(t, pool, shard, block)
	probe, ok := pool.probeSnapshot()
	if !ok {
		t.Fatal("zero-state probe was not recorded")
	}

	serving := &overlayPeer{
		id:        testPeerID("zero-serving"),
		addr:      "zero-serving",
		overlay:   &overlay.ADNLOverlayWrapper{},
		announced: &overlay.Node{Version: int32(time.Now().Unix())},
		alive:     true,
		rldpOverlay: overlay.CreateExtendedRLDP(&testArchiveRLDP{
			adnl:        newTestOverlayADNL(),
			queryResult: PreparedState{},
		}).CreateOverlay([]byte{0x01}),
	}
	result, err := pool.probeArchivePeerEvidence(context.Background(), serving, probe)
	if err != nil {
		t.Fatalf("zero-state serving probe: %v", err)
	}
	if result.evidence != archivePeerEvidenceAvailable {
		t.Fatalf("zero-state evidence = %d, want available only", result.evidence)
	}

	missing := &overlayPeer{
		id:        testPeerID("zero-missing"),
		addr:      "zero-missing",
		overlay:   &overlay.ADNLOverlayWrapper{},
		announced: &overlay.Node{Version: int32(time.Now().Unix())},
		alive:     true,
		rldpOverlay: overlay.CreateExtendedRLDP(&testArchiveRLDP{
			adnl:        newTestOverlayADNL(),
			queryResult: NotFoundState{},
		}).CreateOverlay([]byte{0x01}),
	}
	if _, err = pool.probeArchivePeerEvidence(context.Background(), missing, probe); !errors.Is(err, ErrStateNotAvailable) {
		t.Fatalf("missing zero-state probe error = %v, want not available", err)
	}
}
