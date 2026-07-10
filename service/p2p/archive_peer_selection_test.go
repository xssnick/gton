package p2p

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

func testArchiveCandidate(label string) *overlayPeer {
	id := testPeerID(label)
	return &overlayPeer{
		id:        id,
		addr:      label,
		overlay:   &overlay.ADNLOverlayWrapper{},
		announced: &overlay.Node{Version: int32(time.Now().Unix())},
		alive:     true,
	}
}

func testArchivePool(sub *overlaySubscription) *archivePeerPool {
	if sub.peers == nil {
		sub.peers = map[PeerID]*overlayPeer{}
	}
	return newArchivePeerPool(sub)
}

func testArchiveOnlyPoolPeer(tb testing.TB, pool *archivePeerPool, label string) *overlayPeer {
	tb.Helper()

	overlayWrapper, _ := newTestOverlayWrapper()
	peer := testArchiveCandidate(label)
	peer.overlay = overlayWrapper
	if !pool.addArchiveOnlyPeer(peer) {
		tb.Fatalf("failed to add archive-only peer %s", label)
	}
	return peer
}

func newTestLeasedPooledPeer(label string) (*peerPool, *pooledPeer, *testOverlayADNL) {
	base := newTestOverlayADNL()
	adnlWrapper := overlay.CreateExtendedADNL(base)
	baseRLDP := rldp.NewClientV2(adnlWrapper)
	rldpWrapper := overlay.CreateExtendedRLDP(baseRLDP)
	id := testPeerID(label)
	pooled := &pooledPeer{
		id:          id,
		addr:        label,
		adnl:        adnlWrapper,
		rldp:        rldpWrapper,
		overlayRefs: map[string]int{},
	}
	pool := &peerPool{peers: map[PeerID]*pooledPeer{id: pooled}}
	return pool, pooled, base
}

func TestArchivePeerPoolConnectSeedNodeUsesPeerTimeout(t *testing.T) {
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
	pool := testArchivePool(sub)

	_, _ = pool.connectArchiveSeedNode(context.Background(), *peerNode)

	if got := timeoutDuration(t, fake.findAddressesDeadline); got < dhtSeedPeerTimeout-time.Second {
		t.Fatalf("archive seed address lookup timeout too short: %s", got)
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
	pool := testArchivePool(sub)
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
	state.cooldownUntil[downloadPeerID(peerA)] = time.Now().Add(-time.Second)
	pool.mx.Unlock()

	got = pool.candidates(basechain)
	if len(got) != 2 {
		t.Fatalf("expired cooldown entry was not restored: %#v", got)
	}
}

func TestRejectArchivePeerKeepsSelectedPeerBeforeErrorThreshold(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer"}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
	})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeer(shard, peer)
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.rejectArchivePeer(context.Background(), pool, shard, peer, "test")
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("selected archive peer was unpinned before error threshold")
	}
	if selected := session.selectedArchivePeerID(shard); selected != peer.id {
		t.Fatalf("selected archive peer changed after single generic error: %s", selected.String())
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("single generic archive error should not cool down peer")
	}
}

func TestRejectArchiveNotAvailableUnpinsSessionPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("peer")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeer(shard, peer)
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.rejectArchivePeer(context.Background(), pool, shard, peer, archivePeerRejectNotAvailable)
	if _, ok := node.pinnedPeerIDs()[peer.id]; ok {
		t.Fatal("archive not available reject should unpin archive peer")
	}
	if selected := session.selectedArchivePeerID(shard); !selected.IsZero() {
		t.Fatalf("archive peer selection survived not-available reject: %s", selected.String())
	}
	if _, ok := sub.peers[peer.id]; !ok {
		t.Fatal("borrowed live peer should not be removed from live pool")
	}
}

func TestArchiveSessionCloseReleasesPinnedPeers(t *testing.T) {
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer"}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	session := node.BeginArchiveSession()

	session.noteArchivePeerSuccess(peer)
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.Close()
	if _, ok := node.pinnedPeerIDs()[peer.id]; ok {
		t.Fatal("archive session pin survived close")
	}
}

func TestArchiveSessionCloseClosesOnlyArchiveOnlyPeers(t *testing.T) {
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
	archivePeer := sub.newOverlayPeer(pooledArchive, nil, false, true)
	session := node.BeginArchiveSession()

	pool := session.archivePeerPool(sub)
	pool.addArchiveOnlyPeer(archivePeer)
	session.Close()

	select {
	case <-archiveConn.GetCloserCtx().Done():
	default:
		t.Fatal("archive-only peer connection survived archive session close")
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
}

func TestArchivePoolBorrowedPeerReplacesAndClosesArchiveOnlyPeer(t *testing.T) {
	basePool, pooledPeer, sharedConn := newTestLeasedPooledPeer("same-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		node:  &Node{pool: basePool},
		log:   discardLogger(),
		spec:  overlaySpec{ShortID: []byte{0x01}, Kind: overlayKindPublicShard},
		peers: map[PeerID]*overlayPeer{},
	})
	announced := &overlay.Node{Version: int32(time.Now().Unix())}
	archivePeer := sub.newOverlayPeer(pooledPeer, announced, false, true)
	livePeer := sub.newOverlayPeer(pooledPeer, announced, false, true)
	pool := testArchivePool(sub)

	if !pool.addArchiveOnlyPeer(archivePeer) {
		t.Fatal("expected archive-only peer to be added")
	}
	pool.addBorrowedPeer(livePeer)

	select {
	case <-sharedConn.GetCloserCtx().Done():
		t.Fatal("archive-only replacement closed shared pooled ADNL")
	default:
	}
	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 1 || got[0] != livePeer {
		t.Fatalf("expected borrowed live peer after replacement, got %#v", got)
	}
}

func TestArchivePoolBorrowedPeerDoesNotReplaceLeasedArchiveOnlyPeer(t *testing.T) {
	basePool, pooledPeer, sharedConn := newTestLeasedPooledPeer("leased-same-peer")
	sub := testOverlaySubscription(&overlaySubscription{
		node:  &Node{pool: basePool},
		log:   discardLogger(),
		spec:  overlaySpec{ShortID: []byte{0x01}, Kind: overlayKindPublicShard},
		peers: map[PeerID]*overlayPeer{},
	})
	announced := &overlay.Node{Version: int32(time.Now().Unix())}
	archivePeer := sub.newOverlayPeer(pooledPeer, announced, false, true)
	livePeer := sub.newOverlayPeer(pooledPeer, announced, false, true)
	pool := testArchivePool(sub)

	if !pool.addArchiveOnlyPeer(archivePeer) {
		t.Fatal("expected archive-only peer to be added")
	}
	release := pool.acquire(archivePeer)
	pool.addBorrowedPeer(livePeer)

	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 1 || got[0] != archivePeer {
		t.Fatalf("leased archive-only peer was replaced: %#v", got)
	}
	select {
	case <-sharedConn.GetCloserCtx().Done():
		t.Fatal("leased archive-only replacement closed pooled ADNL")
	default:
	}

	release()
	pool.addBorrowedPeer(livePeer)
	got = pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 1 || got[0] != livePeer {
		t.Fatalf("released archive-only peer was not replaced by live peer: %#v", got)
	}
}

func TestEnsureArchivePeersWaitsForDHTDiscoveryCompletion(t *testing.T) {
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
	pool := testArchivePool(sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}

	done := make(chan error, 1)
	go func() {
		done <- sub.ensureArchivePeers(context.Background(), pool, shard)
	}()

	select {
	case err := <-done:
		t.Fatalf("ensureArchivePeers returned before DHT discovery completed: %v", err)
	case <-time.After(archiveDiscoveryWait + 250*time.Millisecond):
	}

	close(waitDHT)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ensureArchivePeers returned error after DHT discovery: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ensureArchivePeers did not finish after DHT discovery completed")
	}
}

func TestArchiveOnlyPeerCloseDoesNotCloseSharedPooledADNL(t *testing.T) {
	pool, pooled, base := newTestLeasedPooledPeer("shared")
	overlayID := []byte{0x01}
	sub := testOverlaySubscription(&overlaySubscription{
		node: &Node{pool: pool},
		spec: overlaySpec{
			ShortID: overlayID,
			Kind:    overlayKindPublicShard,
		},
	})
	archivePeer := sub.newOverlayPeer(pooled, nil, false, true)
	archivePeer.initRebroadcastQueues()
	livePeer := sub.newOverlayPeer(pooled, nil, false, true)
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
	if got := pooled.adnl.CreateOverlayWithSettings(overlayID, maxOverlayPayloadSize, true, false); got != liveOverlay {
		t.Fatal("archive-only close unregistered shared live overlay")
	}

	livePeer.close()
	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("pooled ADNL survived last overlay release")
	}
}

func TestArchiveOnlyPeerCloseClosesUnusedPooledADNL(t *testing.T) {
	pool, pooled, base := newTestLeasedPooledPeer("archive-only")
	sub := testOverlaySubscription(&overlaySubscription{
		node: &Node{pool: pool},
		spec: overlaySpec{
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	})
	archivePeer := sub.newOverlayPeer(pooled, nil, false, true)

	closeArchiveOnlyPeer(archivePeer)

	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("unused archive-only pooled ADNL survived close")
	}
	if len(pool.snapshot()) != 0 {
		t.Fatal("unused archive-only pooled peer survived in pool")
	}
}

func TestClosedArchivePoolIgnoresLateSuccess(t *testing.T) {
	peer := testArchiveCandidate("late-success")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}

	pool.Close()
	pool.markSuccess(shard, peer)
	pool.markAvailable(shard, peer)

	if pool.hasPeer(peer.id) {
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
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peerA)
	pool.addArchiveOnlyPeer(peerB)
	pool.addArchiveOnlyPeer(leasedPeer)
	release := pool.acquire(leasedPeer)
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
	if pool.hasPeer(peerA.id) {
		t.Fatal("not-available peer A was not removed")
	}
	if pool.hasPeer(peerB.id) {
		t.Fatal("not-available peer B was not removed")
	}
	if !pool.hasPeer(leasedPeer.id) {
		t.Fatal("leased peer was removed")
	}
}

func TestRotateUselessArchivePeersKeepsProvenPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	uselessPeer := testArchiveCandidate("useless")
	provenPeer := testArchiveCandidate("proven")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(uselessPeer)
	pool.addArchiveOnlyPeer(provenPeer)
	pool.markSuccess(shard, provenPeer)

	for i := 0; i < archivePeerNotAvailableRotateThreshold+1; i++ {
		pool.noteFailure(shard, uselessPeer, archivePeerRejectNotAvailable)
		if pool.noteFailure(shard, provenPeer, archivePeerRejectNotAvailable).useless {
			t.Fatal("not-available should never make proven archive peer useless")
		}
	}

	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if pool.hasPeer(uselessPeer.id) {
		t.Fatal("useless peer was not removed")
	}
	if !pool.hasPeer(provenPeer.id) {
		t.Fatal("proven peer was removed over not-available answers")
	}
}

func TestProvenPeerNotAvailableCooldownEscalates(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("proven-backoff")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)
	pool.markSuccess(shard, peer)

	for strike, want := range archiveNotAvailableCooldowns {
		verdict := pool.noteFailure(shard, peer, archivePeerRejectNotAvailable)
		if verdict.useless {
			t.Fatalf("strike %d made proven peer useless", strike+1)
		}
		if verdict.cooldown != want {
			t.Fatalf("strike %d cooldown = %s, want %s", strike+1, verdict.cooldown, want)
		}
	}
	if verdict := pool.noteFailure(shard, peer, archivePeerRejectNotAvailable); verdict.cooldown != archiveNotAvailableCooldowns[len(archiveNotAvailableCooldowns)-1] {
		t.Fatalf("cooldown after ladder end = %s, want max", verdict.cooldown)
	}

	pool.markAvailable(shard, peer)
	if verdict := pool.noteFailure(shard, peer, archivePeerRejectNotAvailable); verdict.cooldown != archiveNotAvailableCooldowns[0] {
		t.Fatalf("cooldown after availability reset = %s, want first step", verdict.cooldown)
	}
}

func TestNoteFailureSuccessDecaysErrorsButKeepsBadImports(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("decay")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete)
	pool.markSuccess(shard, peer)

	pool.mx.Lock()
	failure := pool.shards[archivePeerPoolKey(shard)].failures[peer.id]
	pool.mx.Unlock()
	if failure.errors != 1 {
		t.Fatalf("errors after success = %d, want 1 (decayed)", failure.errors)
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
	pool := testArchivePool(sub)
	conns := make([]*testOverlayADNL, 0, bootstrapDiscoveryTarget)

	for i := 0; i < bootstrapDiscoveryTarget; i++ {
		overlayWrapper, conn := newTestOverlayWrapper()
		peer := testArchiveCandidate(fmt.Sprintf("closed-%d", i))
		peer.overlay = overlayWrapper
		if !pool.addArchiveOnlyPeer(peer) {
			t.Fatalf("failed to add archive-only peer %d", i)
		}
		pool.markAvailable(shard, peer)
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
	workchains := len(pool.workchains)
	shards := len(pool.shards)
	pool.mx.Unlock()
	if workchains != 0 {
		t.Fatalf("closed archive-only peers left workchain indexes: %d", workchains)
	}
	if shards != 0 {
		t.Fatalf("closed archive-only peers left shard state: %d", shards)
	}
}

func TestArchivePoolClosedPeersDoNotBlockHardLimit(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	conns := make([]*testOverlayADNL, 0, archivePeerHardLimit)

	for i := 0; i < archivePeerHardLimit; i++ {
		overlayWrapper, conn := newTestOverlayWrapper()
		peer := testArchiveCandidate(fmt.Sprintf("closed-hard-limit-%d", i))
		peer.overlay = overlayWrapper
		if !pool.addArchiveOnlyPeer(peer) {
			t.Fatalf("failed to add archive-only peer %d", i)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		conn.Close()
	}

	replacement := testArchiveCandidate("replacement")
	if !pool.addArchiveOnlyPeer(replacement) {
		t.Fatal("closed archive-only peers blocked hard limit replacement")
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
	pool := testArchivePool(sub)
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
	pool := testArchivePool(sub)
	if !pool.addArchiveOnlyPeer(peer) {
		t.Fatal("failed to add archive-only peer")
	}
	release := pool.acquire(peer)
	conn.Close()

	if pruned := pool.pruneClosedPeers(); pruned != 0 {
		t.Fatalf("leased closed peer was pruned before release: %d", pruned)
	}
	if !pool.hasPeer(peer.id) {
		t.Fatal("leased closed peer was removed before release")
	}

	release()
	if pruned := pool.pruneClosedPeers(); pruned != 1 {
		t.Fatalf("closed peer was not pruned after release: %d", pruned)
	}
	if pool.hasPeer(peer.id) {
		t.Fatal("closed peer survived release and cleanup")
	}
}

func TestArchivePoolPrunesDeadUnprovenArchiveOnlyPeers(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	peers := make([]*overlayPeer, 0, 3)

	for i := 0; i < cap(peers); i++ {
		peers = append(peers, testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("dead-unproven-%d", i)))
	}
	for _, peer := range peers {
		peer.alive = false
	}

	if pruned := pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now()); pruned != len(peers) {
		t.Fatalf("unexpected dead unproven archive-only prune count: got %d want %d", pruned, len(peers))
	}
	if usable := pool.usableSize(time.Now()); usable != 0 {
		t.Fatalf("dead unproven archive-only peers should not be usable, got %d", usable)
	}
	if size := pool.size(); size != 0 {
		t.Fatalf("dead unproven archive-only peers should be pruned, got size %d", size)
	}
	for _, peer := range peers {
		if pool.hasPeer(peer.id) {
			t.Fatalf("dead unproven archive-only peer %s survived prune", peer.addr)
		}
	}
}

func TestArchivePoolKeepsLeasedAndProvenDeadArchiveOnlyPeers(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	leased := testArchiveOnlyPoolPeer(t, pool, "leased-dead-unproven")
	proven := testArchiveOnlyPoolPeer(t, pool, "proven-dead")
	release := pool.acquire(leased)
	pool.markSuccess(shard, proven)
	leased.alive = false
	proven.alive = false

	if pruned := pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now()); pruned != 0 {
		t.Fatalf("leased/proven dead archive-only peers were pruned: %d", pruned)
	}
	if !pool.hasPeer(leased.id) {
		t.Fatal("leased dead archive-only peer was removed")
	}
	if !pool.hasPeer(proven.id) {
		t.Fatal("proven dead archive-only peer was removed")
	}
	if usable := pool.usableSize(time.Now()); usable != 1 {
		t.Fatalf("only proven dead archive-only peer should remain usable, got %d", usable)
	}

	release()
	if pruned := pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now()); pruned != 1 {
		t.Fatalf("released dead unproven archive-only peer was not pruned: %d", pruned)
	}
	if pool.hasPeer(leased.id) {
		t.Fatal("released dead unproven archive-only peer survived prune")
	}
	if !pool.hasPeer(proven.id) {
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
	pool := testArchivePool(sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	pool.noteArchiveRequest(shard, 100)

	for i := 0; i < bootstrapDiscoveryTarget; i++ {
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

func TestArchivePoolRefillSkipsDHTWithEnoughProvenPeers(t *testing.T) {
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
	pool := testArchivePool(sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	pool.noteArchiveRequest(shard, 100)

	for i := 0; i < archiveProvenPeerTarget; i++ {
		peer := testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("proven-%d", i))
		pool.markSuccess(shard, peer)
	}

	if done := pool.refill(context.Background(), false); done != nil {
		t.Fatal("proven-peer target reached but refill still started DHT discovery")
	}
	if fake.findOverlayNodesCallCount() != 0 {
		t.Fatal("DHT was queried despite enough proven archive peers")
	}
}

func TestRotateUselessArchivePeersWaitsForRepeatedErrors(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("flaky")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	for i := 0; i < archivePeerErrorRotateThreshold-1; i++ {
		if pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed).useless {
			t.Fatalf("download error %d made peer useless before threshold", i+1)
		}
	}
	if rotated := pool.rotateUseless(shard); rotated != 0 {
		t.Fatalf("unexpected early rotated peer count: got %d want 0", rotated)
	}
	if !pool.hasPeer(peer.id) {
		t.Fatal("peer was rotated before repeated error threshold")
	}

	if !pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed).useless {
		t.Fatal("peer should become useless after repeated errors")
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if pool.hasPeer(peer.id) {
		t.Fatal("peer survived repeated archive errors")
	}
}

func TestRotatedArchivePeerNegativeCacheBlocksReconnect(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("rediscovered")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	for i := 0; i < archivePeerNotAvailableRotateThreshold; i++ {
		pool.noteFailure(shard, peer, archivePeerRejectNotAvailable)
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}

	if pool.addArchiveOnlyPeer(peer) {
		t.Fatal("negative-cached junk peer was reconnected")
	}
	if !pool.recentlyRejected(peer.id, time.Now()) {
		t.Fatal("rotated junk peer missing from negative cache")
	}

	// A later availability answer (for example via the borrowed live path)
	// clears the negative cache so the peer can be pooled again.
	pool.markAvailable(shard, peer)
	pool.mx.Lock()
	delete(pool.peers, peer.id)
	pool.mx.Unlock()
	if !pool.addArchiveOnlyPeer(peer) {
		t.Fatal("peer stayed blocked after availability cleared negative cache")
	}
}

func TestRotatedProvenArchivePeerIsNotNegativeCached(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("proven-rotated")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)
	pool.markSuccess(shard, peer)

	for i := 0; i < archivePeerErrorRotateThreshold; i++ {
		pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if pool.recentlyRejected(peer.id, time.Now()) {
		t.Fatal("proven peer rotated on errors must not be negative-cached")
	}
	if !pool.addArchiveOnlyPeer(peer) {
		t.Fatal("proven rotated peer could not reconnect")
	}
}

func TestBadArchiveImportMakesPeerUseless(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("bad-import")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	verdict := pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete)
	if verdict.useless {
		t.Fatal("first bad archive import should not make peer useless")
	}
	if verdict.cooldown != archiveSlowPeerPenalty {
		t.Fatalf("first bad import cooldown = %s, want %s", verdict.cooldown, archiveSlowPeerPenalty)
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
	if pool.hasPeer(peer.id) {
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
	pool := testArchivePool(sub)

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
	fast.downloadCount = 2
	fast.downloadBytesSec = float64(16 << 20)
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
	pool := testArchivePool(sub)

	session.selectArchivePeer(shard, selected)
	got := pool.downloadCandidates(session, shard, []*overlayPeer{fast, selected})
	if len(got) != 2 || got[0] != selected {
		t.Fatalf("selected archive peer should stay first, got %#v", got)
	}
}

func TestArchiveSessionComparativeHedgeCadence(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	selected := testArchiveCandidate("selected")
	node := &Node{peerUse: map[PeerID]peerUse{}}
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
	pool := testArchivePool(sub)

	session.selectArchivePeer(shard, selected)
	if _, ok := node.pinnedPeerIDs()[selected.id]; !ok {
		t.Fatal("expected selected archive peer to be pinned")
	}

	got := pool.downloadCandidates(session, shard, []*overlayPeer{fast})
	if len(got) != 1 || got[0] != fast {
		t.Fatalf("missing selected peer should not stay in candidates, got %#v", got)
	}
	if selectedID := session.selectedArchivePeerID(shard); !selectedID.IsZero() {
		t.Fatalf("missing selected archive peer was not cleared: %s", selectedID.String())
	}
	if _, ok := node.pinnedPeerIDs()[selected.id]; ok {
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
	pool := testArchivePool(sub)
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
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
	pool := testArchivePool(sub)
	firstShard := archive.ShardID{Workchain: 0, Shard: topShard}
	otherShard := archive.ShardID{Workchain: 0, Shard: topShard >> 1}

	pool.markAvailable(firstShard, peer)

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
	pool := testArchivePool(sub)
	firstShard := archive.ShardID{Workchain: 0, Shard: topShard}
	otherShard := archive.ShardID{Workchain: 0, Shard: topShard >> 1}

	pool.markAvailable(firstShard, peer)
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
	busy.downloadCount = 2
	busy.downloadBytesSec = float64(12 << 20)
	free.downloadCount = 2
	free.downloadBytesSec = float64(12 << 20)
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			busy.id: busy,
			free.id: free,
		},
	})
	pool := testArchivePool(sub)
	shardA := archive.ShardID{Workchain: 0, Shard: topShard}
	shardB := archive.ShardID{Workchain: 0, Shard: topShard >> 1}
	pool.markAvailable(shardA, busy)
	pool.markAvailable(shardA, free)
	release := pool.acquire(busy)
	defer release()

	session := sub.node.BeginArchiveSession()
	defer session.Close()

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
	pool := testArchivePool(sub)
	pool.markSuccess(shard, selected)

	session.selectArchivePeer(shard, selected)
	got := pool.downloadCandidates(session, shard, []*overlayPeer{fast, selected})
	if len(got) != 2 || got[0] != selected {
		t.Fatalf("recent selected archive peer should stay first, got %#v", got)
	}
	if _, ok := node.pinnedPeerIDs()[selected.id]; !ok {
		t.Fatal("recent selected archive peer should stay pinned")
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
	pool := testArchivePool(sub)

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
	pool := testArchivePool(sub)

	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 1 {
		t.Fatalf("unexpected archive candidate count: got %d want 1", len(got))
	}
	if got[0].id != testPeerID("alive") {
		t.Fatalf("expected only alive peer, got %q", got[0].id)
	}
}

func TestArchiveSmallSeedDoesNotUpdatePeerSpeed(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}

	noteArchivePeerSeedSuccess(shard, peer, archiveSpeedSampleMinBytes/4, time.Second)

	if stats := peer.statsSnapshot(); stats.downloadCount != 0 || stats.downloadBytesSec != 0 {
		t.Fatalf("small archive seed should not update peer speed: %#v", stats)
	}

	noteArchivePeerSeedSuccess(shard, peer, archiveSpeedSampleMinBytes, time.Second)

	stats := peer.statsSnapshot()
	if stats.downloadCount != 1 || stats.downloadBytesSec == 0 {
		t.Fatalf("reliable archive seed should update peer speed: %#v", stats)
	}
}

func TestArchiveDownloadSuccessDoesNotUseFixedSlowThreshold(t *testing.T) {
	tests := []struct {
		name    string
		bytes   int64
		elapsed time.Duration
	}{
		{
			name:    "small pack",
			bytes:   int64(310 << 10),
			elapsed: 10 * time.Second,
		},
		{
			name:    "reliable sample",
			bytes:   archiveSpeedSampleMinBytes,
			elapsed: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &overlayPeer{id: testPeerID(tt.name), addr: tt.name, alive: true}

			noteArchivePeerDownload(archive.ShardID{Workchain: -1, Shard: topShard}, peer, tt.bytes, tt.elapsed)

			stats := peer.statsSnapshot()
			if archiveSpeedSampleReliable(tt.bytes) && stats.downloadCount != 1 {
				t.Fatalf("reliable archive download should update speed score: %#v", stats)
			}
			if !archiveSpeedSampleReliable(tt.bytes) && (stats.downloadCount != 0 || stats.downloadBytesSec != 0) {
				t.Fatalf("small archive download should not update speed score: %#v", stats)
			}
			if stats.downloadSlowUntil.After(time.Now()) {
				t.Fatalf("successful archive download should not set fixed slow penalty: %#v", stats)
			}
		})
	}
}

func TestArchiveDeadlineAfterPinnedSuccessKeepsSessionPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{node: node, log: discardLogger()})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)

	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("deadline after successful response should keep archive session pin")
	}
	if peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("deadline after successful response should not set slow penalty")
	}
}

func TestPinnedArchiveDeadlineGraceKeepsSessionPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{node: node, log: discardLogger()})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	for i := 0; i < archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)
	}

	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("deadline grace should keep archive session pin")
	}
	if peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("deadline grace should not set slow penalty")
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("deadline grace should not cool down archive peer")
	}
}

func TestPinnedArchiveTimeoutsScaleWithDeadlineGrace(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{node: node, log: discardLogger()})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	if got := session.archivePeerInfoTimeout(peer); got != archiveInfoTimeout {
		t.Fatalf("initial archive info timeout = %s, want %s", got, archiveInfoTimeout)
	}
	if got := session.archivePeerSliceProbeTimeout(peer); got != archiveSliceProbeTimeout {
		t.Fatalf("initial archive probe timeout = %s, want %s", got, archiveSliceProbeTimeout)
	}
	if got := session.archivePeerSliceTimeout(peer); got != archiveSliceTimeout {
		t.Fatalf("initial archive slice timeout = %s, want %s", got, archiveSliceTimeout)
	}

	lastInfo := archiveInfoTimeout
	lastProbe := archiveSliceProbeTimeout
	lastSlice := archiveSliceTimeout
	for i := 0; i < archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)

		infoTimeout := session.archivePeerInfoTimeout(peer)
		probeTimeout := session.archivePeerSliceProbeTimeout(peer)
		sliceTimeout := session.archivePeerSliceTimeout(peer)
		if infoTimeout <= lastInfo {
			t.Fatalf("archive info timeout did not grow: previous=%s current=%s", lastInfo, infoTimeout)
		}
		if probeTimeout <= lastProbe {
			t.Fatalf("archive probe timeout did not grow: previous=%s current=%s", lastProbe, probeTimeout)
		}
		if sliceTimeout <= lastSlice {
			t.Fatalf("archive slice timeout did not grow: previous=%s current=%s", lastSlice, sliceTimeout)
		}
		lastInfo = infoTimeout
		lastProbe = probeTimeout
		lastSlice = sliceTimeout
	}

	if lastInfo != archiveInfoPinnedMaxTimeout {
		t.Fatalf("archive info timeout after grace = %s, want %s", lastInfo, archiveInfoPinnedMaxTimeout)
	}
	if lastProbe != archiveSliceProbePinnedMaxTimeout {
		t.Fatalf("archive probe timeout after grace = %s, want %s", lastProbe, archiveSliceProbePinnedMaxTimeout)
	}
	if lastSlice != archiveSlicePinnedMaxTimeout {
		t.Fatalf("archive slice timeout after grace = %s, want %s", lastSlice, archiveSlicePinnedMaxTimeout)
	}
}

func TestArchiveInfoDoesNotResetPinnedDeadlineGrace(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{node: node, log: discardLogger()})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerAvailable(peer)
	for i := 0; i < archiveSessionPinnedDeadlineGrace; i++ {
		if !session.archivePeerDeadlineGrace(peer, context.DeadlineExceeded) {
			t.Fatalf("deadline grace stopped early at failure %d", i+1)
		}
		session.noteArchivePeerAvailable(peer)
	}

	failures, pinned := session.archivePeerDeadlineFailures(peer)
	if !pinned || failures != archiveSessionPinnedDeadlineGrace {
		t.Fatalf("deadline failures after repeated archive info = %d pinned=%v, want %d and pinned", failures, pinned, archiveSessionPinnedDeadlineGrace)
	}

	sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)

	if _, ok := node.pinnedPeerIDs()[peer.id]; ok {
		t.Fatal("deadline after repeated archive info should clear archive session pin")
	}
	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("deadline after repeated archive info should set slow penalty")
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("single post-grace deadline should not cool down archive peer")
	}
}

func TestArchiveDataSuccessResetsPinnedDeadlineGrace(t *testing.T) {
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerAvailable(peer)
	if !session.archivePeerDeadlineGrace(peer, context.DeadlineExceeded) {
		t.Fatal("first deadline should stay in grace")
	}

	session.noteArchivePeerSuccess(peer)

	failures, pinned := session.archivePeerDeadlineFailures(peer)
	if !pinned || failures != 0 {
		t.Fatalf("deadline failures after archive data success = %d pinned=%v, want 0 and pinned", failures, pinned)
	}
}

func TestArchiveDeadlineAfterPinnedGraceMarksPeerSlow(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{node: node, log: discardLogger()})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	for i := 0; i <= archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)
	}

	if _, ok := node.pinnedPeerIDs()[peer.id]; ok {
		t.Fatal("deadline after grace should clear archive session pin")
	}
	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("deadline after grace should set slow penalty")
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("single post-grace deadline should not cool down archive peer")
	}
}

func TestSelectedArchiveDeadlineAfterPinnedGraceKeepsPeerUntilErrorThreshold(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("selected-deadline")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	})
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeer(shard, peer)
	for i := 0; i <= archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)
	}

	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("selected archive peer should stay pinned after first post-grace deadline")
	}
	if selected := session.selectedArchivePeerID(shard); selected != peer.id {
		t.Fatalf("selected archive peer changed before error threshold: %s", selected.String())
	}
	if _, ok := sub.peers[peer.id]; !ok {
		t.Fatal("selected archive peer was rotated before error threshold")
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("selected archive peer should not cool down before error threshold")
	}

	for i := 1; i < archivePeerErrorRotateThreshold; i++ {
		sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)
	}

	if _, ok := node.pinnedPeerIDs()[peer.id]; ok {
		t.Fatal("selected archive peer pin survived repeated errors")
	}
	if selected := session.selectedArchivePeerID(shard); !selected.IsZero() {
		t.Fatalf("selected archive peer survived repeated errors: %s", selected.String())
	}
	if _, ok := sub.peers[peer.id]; !ok {
		t.Fatal("borrowed live peer should survive archive rotation")
	}
}

func TestArchiveDeadlineWithoutSuccessMarksPeerSlow(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(sub)
	session := sub.node.BeginArchiveSession()
	defer session.Close()

	sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)

	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("first deadline without archive success should set slow penalty")
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("first deadline without archive success should not cool down archive peer")
	}
}

func TestRepeatedArchiveDeadlineErrorsRotatePeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("deadline-flaky")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)
	session := sub.node.BeginArchiveSession()
	defer session.Close()

	for i := 0; i < archivePeerErrorRotateThreshold; i++ {
		sub.noteArchiveDownloadError(context.Background(), session, pool, shard, peer, context.DeadlineExceeded)
	}

	if pool.hasPeer(peer.id) {
		t.Fatal("peer survived repeated archive deadline errors")
	}
}

func TestArchiveLargeDownloadUpdatesLargePackSpeed(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}

	noteArchivePeerDownload(shard, peer, archiveSpeedSampleMinBytes, time.Second)
	if stats := peer.statsSnapshot(); stats.archiveLargeDownloads != 0 || stats.archiveLargeBytesSec != 0 {
		t.Fatalf("regular archive sample should not update large-pack speed: %#v", stats)
	}

	noteArchivePeerDownload(shard, peer, archiveLargeSpeedSampleMinBytes, time.Second)
	stats := peer.statsSnapshot()
	if stats.archiveLargeDownloads != 1 || stats.archiveLargeBytesSec == 0 {
		t.Fatalf("large archive sample should update large-pack speed: %#v", stats)
	}
}

func TestArchiveLargePackSpeedHasPriorityOverSmallPackSpeed(t *testing.T) {
	node := &Node{}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	largePackFast := &overlayPeer{
		id:                    testPeerID("large-pack-fast"),
		addr:                  "large-pack-fast",
		alive:                 true,
		downloadCount:         2,
		downloadBytesSec:      float64(5 << 20),
		archiveLargeBytesSec:  float64(60 << 20),
		archiveLargeDownloads: 1,
	}
	probeFast := &overlayPeer{
		id:               testPeerID("small-pack-fast"),
		addr:             "small-pack-fast",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: float64(100 << 20),
	}

	ordered := node.prioritizeArchivePeers(shard, []*overlayPeer{probeFast, largePackFast})
	if ordered[0] != largePackFast {
		t.Fatalf("large-pack speed should outrank small-pack speed, got %q", ordered[0].addr)
	}
}

func TestArchiveLargePackPeerCanUseParallelCapacity(t *testing.T) {
	node := &Node{
		peerUse: map[PeerID]peerUse{testPeerID("large-pack-fast"): {downloads: 2}},
	}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	largePackFast := &overlayPeer{
		id:                    testPeerID("large-pack-fast"),
		addr:                  "large-pack-fast",
		alive:                 true,
		downloadCount:         3,
		downloadBytesSec:      float64(20 << 20),
		archiveLargeBytesSec:  float64(24 << 20),
		archiveLargeDownloads: 2,
	}
	freeMedium := &overlayPeer{
		id:               testPeerID("free-medium"),
		addr:             "free-medium",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: float64(10 << 20),
	}

	ordered := node.prioritizeArchivePeers(shard, []*overlayPeer{freeMedium, largePackFast})
	if ordered[0] != largePackFast {
		t.Fatalf("large-pack peer should keep priority within parallel capacity, got %q", ordered[0].addr)
	}
}

func TestArchiveLargePackPriorityStopsAfterParallelCapacity(t *testing.T) {
	node := &Node{
		peerUse: map[PeerID]peerUse{testPeerID("large-pack-fast"): {downloads: 3}},
	}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	largePackFast := &overlayPeer{
		id:                    testPeerID("large-pack-fast"),
		addr:                  "large-pack-fast",
		alive:                 true,
		downloadCount:         3,
		downloadBytesSec:      float64(18 << 20),
		archiveLargeBytesSec:  float64(18 << 20),
		archiveLargeDownloads: 2,
	}
	freeProbeFast := &overlayPeer{
		id:               testPeerID("free-probe-fast"),
		addr:             "free-probe-fast",
		alive:            true,
		downloadCount:    2,
		downloadBytesSec: float64(12 << 20),
	}

	ordered := node.prioritizeArchivePeers(shard, []*overlayPeer{largePackFast, freeProbeFast})
	if ordered[0] != freeProbeFast {
		t.Fatalf("large-pack peer at capacity should not have absolute priority, got %q", ordered[0].addr)
	}
}

func TestArchivePoolKeepsRecentlyAvailableArchiveOnlyPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "standby-available")
	pool.markAvailable(shard, peer)

	// Expired announcement and no pings: previously this counted as dead.
	peer.statsMx.Lock()
	peer.announced = &overlay.Node{Version: int32(time.Now().Add(-overlayPeerTTL - time.Second).Unix())}
	peer.alive = false
	peer.statsMx.Unlock()

	if pruned := pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now()); pruned != 0 {
		t.Fatalf("recently available archive-only peer was pruned: %d", pruned)
	}
	if got := pool.candidates(shard); len(got) != 1 || got[0] != peer {
		t.Fatalf("recently available archive-only peer should stay usable, got %#v", got)
	}

	// Once availability ages out, the peer is prunable again.
	pool.mx.Lock()
	pool.peers[peer.id].lastAvailableAt = time.Now().Add(-archiveAvailablePeerTTL - time.Second)
	pool.mx.Unlock()
	if pruned := pool.pruneUnprovenDeadArchiveOnlyPeers(time.Now()); pruned != 1 {
		t.Fatalf("stale available archive-only peer was not pruned: %d", pruned)
	}
}

func TestArchivePoolRefreshKnownPeerMergesAnnouncement(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "refresh-announcement")
	stale := int32(time.Now().Add(-overlayPeerTTL - time.Second).Unix())
	peer.statsMx.Lock()
	peer.announced = &overlay.Node{Version: stale}
	peer.statsMx.Unlock()

	fresh := &overlay.Node{Version: int32(time.Now().Unix())}
	if !pool.refreshKnownPeer(peer.id, fresh) {
		t.Fatal("known archive-only peer was not recognized")
	}
	peer.statsMx.Lock()
	got := peer.announced.Version
	peer.statsMx.Unlock()
	if got != fresh.Version {
		t.Fatalf("announcement version = %d, want refreshed %d", got, fresh.Version)
	}

	if pool.refreshKnownPeer(testPeerID("unknown"), fresh) {
		t.Fatal("unknown peer reported as known")
	}
}

func TestArchivePoolKeepaliveTargetsSelectProvenArchiveOnlyPeers(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	borrowed := testArchiveCandidate("keepalive-borrowed")
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			borrowed.id: borrowed,
		},
	})
	pool := testArchivePool(sub)
	proven := testArchiveOnlyPoolPeer(t, pool, "keepalive-proven")
	available := testArchiveOnlyPoolPeer(t, pool, "keepalive-available")
	junk := testArchiveOnlyPoolPeer(t, pool, "keepalive-junk")
	pool.markSuccess(shard, proven)
	pool.markAvailable(shard, available)
	pool.markSuccess(shard, borrowed)

	targets := pool.keepaliveTargets(time.Now())
	got := map[PeerID]struct{}{}
	for _, peer := range targets {
		got[peer.id] = struct{}{}
	}
	if _, ok := got[proven.id]; !ok {
		t.Fatal("proven archive-only peer missing from keepalive targets")
	}
	if _, ok := got[available.id]; !ok {
		t.Fatal("recently available archive-only peer missing from keepalive targets")
	}
	if _, ok := got[junk.id]; ok {
		t.Fatal("unproven junk peer must not be kept alive")
	}
	if _, ok := got[borrowed.id]; ok {
		t.Fatal("borrowed live peer must not be pinged by the archive pool")
	}
}

func TestClassifyNewArchivePeerDropsNotAvailableAndKeepsServing(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		spec: overlaySpec{ShortID: []byte{0x01}},
	})
	pool := testArchivePool(sub)
	pool.noteArchiveRequest(shard, 100)

	junkOverlay, junkBase := newTestOverlayWrapper()
	junk := testArchiveCandidate("classify-junk")
	junk.overlay = junkOverlay
	junkBase.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		if out, ok := result.(*tl.Serializable); ok {
			*out = ArchiveNotFound{}
		}
		return nil
	}
	if !pool.addArchiveOnlyPeer(junk) {
		t.Fatal("failed to add junk peer")
	}
	if pool.classifyNewArchivePeer(context.Background(), junk) {
		t.Fatal("junk peer passed archive classification")
	}
	if pool.hasPeer(junk.id) {
		t.Fatal("junk peer survived classification")
	}
	if !pool.recentlyRejected(junk.id, time.Now()) {
		t.Fatal("classified junk peer missing from negative cache")
	}

	servingOverlay, servingBase := newTestOverlayWrapper()
	serving := testArchiveCandidate("classify-serving")
	serving.overlay = servingOverlay
	servingBase.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		if out, ok := result.(*tl.Serializable); ok {
			*out = ArchiveInfo{ID: 42}
		}
		return nil
	}
	if !pool.addArchiveOnlyPeer(serving) {
		t.Fatal("failed to add serving peer")
	}
	if !pool.classifyNewArchivePeer(context.Background(), serving) {
		t.Fatal("serving peer failed archive classification")
	}
	if !pool.hasPeer(serving.id) {
		t.Fatal("serving peer missing after classification")
	}
	if pool.provenUsableSize(time.Now()) != 1 {
		t.Fatalf("proven usable size = %d, want 1", pool.provenUsableSize(time.Now()))
	}

	erring := testArchiveCandidate("classify-error")
	erringOverlay, erringBase := newTestOverlayWrapper()
	erring.overlay = erringOverlay
	erringBase.queryResponder = func(tl.Serializable, tl.Serializable) error {
		return context.DeadlineExceeded
	}
	if !pool.addArchiveOnlyPeer(erring) {
		t.Fatal("failed to add erring peer")
	}
	if pool.classifyNewArchivePeer(context.Background(), erring) {
		t.Fatal("erring peer passed archive classification")
	}
	if pool.recentlyRejected(erring.id, time.Now()) {
		t.Fatal("query error must not negative-cache the peer")
	}
}

func TestClassifyNewArchivePeerWithoutProbeKeepsPeer(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
	})
	pool := testArchivePool(sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "no-probe")

	if !pool.classifyNewArchivePeer(context.Background(), peer) {
		t.Fatal("peer dropped without a probe target")
	}
	if !pool.hasPeer(peer.id) {
		t.Fatal("peer missing after no-probe classification")
	}
}

func TestClassifyNewArchivePeerZeroStateProbe(t *testing.T) {
	block := testBlockID(-1, topShard, 0)
	shard := archiveShardFromBlock(block)
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		spec: overlaySpec{ShortID: []byte{0x01}},
	})
	pool := testArchivePool(sub)
	pool.noteZeroStateRequest(shard, block)

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
	if !pool.addArchiveOnlyPeer(serving) {
		t.Fatal("failed to add zero-state serving peer")
	}
	if !pool.classifyNewArchivePeer(context.Background(), serving) {
		t.Fatal("zero-state serving peer failed classification")
	}
	if pool.provenUsableSize(time.Now()) != 1 {
		t.Fatalf("proven usable size = %d, want 1", pool.provenUsableSize(time.Now()))
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
	if !pool.addArchiveOnlyPeer(missing) {
		t.Fatal("failed to add zero-state missing peer")
	}
	if pool.classifyNewArchivePeer(context.Background(), missing) {
		t.Fatal("peer without zero state passed classification")
	}
	if !pool.recentlyRejected(missing.id, time.Now()) {
		t.Fatal("peer without zero state missing from negative cache")
	}
}
