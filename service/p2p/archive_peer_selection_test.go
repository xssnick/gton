package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
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

func TestArchivePeerCooldownFiltersOnlyArchivePool(t *testing.T) {
	peerA := testArchiveCandidate("peer-a")
	peerB := testArchiveCandidate("peer-b")
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peerA.id: peerA,
			peerB.id: peerB,
		},
	}
	pool := testArchivePool(sub)
	basechain := archive.ShardID{Workchain: 0, Shard: topShard}
	masterchain := archive.ShardID{Workchain: -1, Shard: topShard}

	pool.cooldown(basechain, peerA, "test")

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
	state.cooldownUntil[archivePeerID(peerA)] = time.Now().Add(-time.Second)
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
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
	}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeer(shard, peer)
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.rejectArchivePeer(pool, shard, peer, "test")
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
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeer(shard, peer)
	if _, ok := node.pinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.rejectArchivePeer(pool, shard, peer, archivePeerRejectNotAvailable)
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
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
		peers: map[PeerID]*overlayPeer{
			livePeer.id: livePeer,
		},
	}
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
	sub := &overlaySubscription{
		node:  &Node{pool: basePool},
		log:   discardLogger(),
		spec:  overlaySpec{ShortID: []byte{0x01}, Kind: overlayKindPublicShard},
		peers: map[PeerID]*overlayPeer{},
	}
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

func TestArchiveOnlyPeerCloseDoesNotCloseSharedPooledADNL(t *testing.T) {
	pool, pooled, base := newTestLeasedPooledPeer("shared")
	overlayID := []byte{0x01}
	sub := &overlaySubscription{
		node: &Node{pool: pool},
		spec: overlaySpec{
			ShortID: overlayID,
			Kind:    overlayKindPublicShard,
		},
	}
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
	sub := &overlaySubscription{
		node: &Node{pool: pool},
		spec: overlaySpec{
			ShortID: []byte{0x01},
			Kind:    overlayKindPublicShard,
		},
	}
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
	sub := &overlaySubscription{
		log: discardLogger(),
	}
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
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: &Node{peerUse: map[PeerID]peerUse{}},
	}
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peerA)
	pool.addArchiveOnlyPeer(peerB)
	pool.addArchiveOnlyPeer(leasedPeer)
	release := pool.acquire(leasedPeer)
	defer release()

	if !pool.noteFailure(shard, peerA, archivePeerRejectNotAvailable) {
		t.Fatal("not-available peer A should be useless immediately")
	}
	if !pool.noteFailure(shard, peerB, archivePeerRejectNotAvailable) {
		t.Fatal("not-available peer B should be useless immediately")
	}
	if !pool.noteFailure(shard, leasedPeer, archivePeerRejectNotAvailable) {
		t.Fatal("leased not-available peer should be useless immediately")
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

func TestRotateUselessArchivePeersKeepsSessionPinnedPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	uselessPeer := testArchiveCandidate("useless")
	pinnedPeer := testArchiveCandidate("pinned")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
	}
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(uselessPeer)
	pool.addArchiveOnlyPeer(pinnedPeer)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(pinnedPeer)
	release := pool.acquire(pinnedPeer)
	defer release()
	pool.noteFailure(shard, uselessPeer, archivePeerRejectNotAvailable)
	pool.noteFailure(shard, pinnedPeer, archivePeerRejectNotAvailable)

	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if pool.hasPeer(uselessPeer.id) {
		t.Fatal("useless peer was not removed")
	}
	if !pool.hasPeer(pinnedPeer.id) {
		t.Fatal("session-pinned peer was removed")
	}
}

func TestRotateUselessArchivePeersWaitsForRepeatedErrors(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("flaky")
	sub := &overlaySubscription{
		log: discardLogger(),
	}
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	for i := 0; i < archivePeerErrorRotateThreshold-1; i++ {
		if pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed) {
			t.Fatalf("download error %d made peer useless before threshold", i+1)
		}
	}
	if rotated := pool.rotateUseless(shard); rotated != 0 {
		t.Fatalf("unexpected early rotated peer count: got %d want 0", rotated)
	}
	if !pool.hasPeer(peer.id) {
		t.Fatal("peer was rotated before repeated error threshold")
	}

	if !pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed) {
		t.Fatal("peer should become useless after repeated errors")
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if pool.hasPeer(peer.id) {
		t.Fatal("peer survived repeated archive errors")
	}
}

func TestRotatedArchivePeerKeepsCooldownIfRediscovered(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("rediscovered")
	sub := &overlaySubscription{
		log: discardLogger(),
	}
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	pool.noteFailure(shard, peer, archivePeerRejectNotAvailable)
	pool.cooldown(shard, peer, archivePeerRejectNotAvailable)
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}

	pool.addArchiveOnlyPeer(peer)
	if available := pool.candidates(shard); len(available) != 0 {
		t.Fatalf("rediscovered useless peer should stay on archive cooldown: %#v", available)
	}
	if rotated := pool.rotateUseless(shard); rotated != 0 {
		t.Fatalf("rediscovered cooldown peer should not rotate again without a new failure: got %d", rotated)
	}
}

func TestBadArchiveImportMakesPeerUseless(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("bad-import")
	sub := &overlaySubscription{
		log: discardLogger(),
	}
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	if !pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete) {
		t.Fatal("bad archive import should make peer useless immediately")
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
	sub := &overlaySubscription{
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
	}
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
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			selected.id: selected,
			fast.id:     fast,
		},
	}
	session := node.BeginArchiveSession()
	defer session.Close()
	pool := testArchivePool(sub)

	session.selectArchivePeer(shard, selected)
	got := pool.downloadCandidates(session, shard, []*overlayPeer{fast, selected})
	if len(got) != 2 || got[0] != selected {
		t.Fatalf("selected archive peer should stay first, got %#v", got)
	}
}

func TestArchiveDownloadCandidatesDropMissingSelectedPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	selected := testArchiveCandidate("selected")
	fast := testArchiveCandidate("fast")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			fast.id: fast,
		},
	}
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
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}
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
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}
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
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}
	pool := testArchivePool(sub)
	firstShard := archive.ShardID{Workchain: 0, Shard: topShard}
	otherShard := archive.ShardID{Workchain: 0, Shard: topShard >> 1}

	pool.markAvailable(firstShard, peer)
	pool.noteFailure(firstShard, peer, archivePeerRejectNotAvailable)
	pool.cooldown(firstShard, peer, archivePeerRejectNotAvailable)

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
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[PeerID]*overlayPeer{
			busy.id: busy,
			free.id: free,
		},
	}
	pool := testArchivePool(sub)
	shardA := archive.ShardID{Workchain: 0, Shard: topShard}
	shardB := archive.ShardID{Workchain: 0, Shard: topShard >> 1}
	pool.markAvailable(shardA, busy)
	pool.markAvailable(shardA, free)
	release := pool.acquire(busy)
	defer release()

	got := pool.downloadCandidates(nil, shardB, []*overlayPeer{busy, free})
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
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			selected.id: selected,
			fast.id:     fast,
		},
	}
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
	sub := &overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[PeerID]*overlayPeer{
			testPeerID("peer-1"): {id: testPeerID("peer-1"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			testPeerID("peer-2"): {id: testPeerID("peer-2"), overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
		},
	}
	pool := testArchivePool(sub)

	got := pool.candidates(archive.ShardID{Workchain: -1, Shard: topShard})
	if len(got) != 2 {
		t.Fatalf("archive candidates should use known peers without neighbours, got %d", len(got))
	}
}

func TestArchiveQueryCandidatesSkipDeadKnownPeers(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}
	sub := &overlaySubscription{
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
	}
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

func TestArchiveSmallDownloadCanMarkPeerSlow(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}

	if noteArchivePeerDownload(shard, peer, archiveSpeedSampleMinBytes/4, time.Second) {
		t.Fatal("fast small archive download should not mark peer slow")
	}
	if stats := peer.statsSnapshot(); stats.downloadCount != 0 || stats.downloadBytesSec != 0 {
		t.Fatalf("small archive download should not update speed score: %#v", stats)
	}

	if !noteArchivePeerDownload(shard, peer, archiveSpeedSampleMinBytes/4, archiveSmallPackSlowElapsed+time.Second) {
		t.Fatal("slow small archive download should mark peer slow")
	}
	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("slow small archive download should set slow penalty")
	}
}

func TestArchiveDeadlineAfterPinnedSuccessKeepsSessionPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{log: discardLogger()}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	sub.noteArchiveDownloadError(session, pool, shard, peer, context.DeadlineExceeded)

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
	sub := &overlaySubscription{log: discardLogger()}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	for i := 0; i < archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(session, pool, shard, peer, context.DeadlineExceeded)
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
	sub := &overlaySubscription{log: discardLogger()}
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
		sub.noteArchiveDownloadError(session, pool, shard, peer, context.DeadlineExceeded)

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
	sub := &overlaySubscription{log: discardLogger()}
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

	sub.noteArchiveDownloadError(session, pool, shard, peer, context.DeadlineExceeded)

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
	sub := &overlaySubscription{log: discardLogger()}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	for i := 0; i <= archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(session, pool, shard, peer, context.DeadlineExceeded)
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
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()

	session.selectArchivePeer(shard, peer)
	for i := 0; i <= archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(session, pool, shard, peer, context.DeadlineExceeded)
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
		sub.noteArchiveDownloadError(session, pool, shard, peer, context.DeadlineExceeded)
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
	sub := &overlaySubscription{log: discardLogger()}
	pool := testArchivePool(sub)

	sub.noteArchiveDownloadError(nil, pool, shard, peer, context.DeadlineExceeded)

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
	sub := &overlaySubscription{
		log: discardLogger(),
	}
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)

	for i := 0; i < archivePeerErrorRotateThreshold; i++ {
		sub.noteArchiveDownloadError(nil, pool, shard, peer, context.DeadlineExceeded)
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
