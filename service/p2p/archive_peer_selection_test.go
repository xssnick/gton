package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/adnl/overlay"
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

func TestArchivePeerCooldownFiltersOnlyArchivePool(t *testing.T) {
	sub := &overlaySubscription{
		log:          discardLogger(),
		archivePeers: map[string]*archivePeerPoolState{},
	}
	peerA := &overlayPeer{id: testPeerID("peer-a"), addr: "peer-a"}
	peerB := &overlayPeer{id: testPeerID("peer-b"), addr: "peer-b"}
	basechain := archive.ShardID{Workchain: 0, Shard: topShard}
	masterchain := archive.ShardID{Workchain: -1, Shard: topShard}

	sub.cooldownArchivePeer(basechain, peerA, "test")

	got := sub.availableArchivePeers(basechain, []*overlayPeer{peerA, peerB})
	if len(got) != 1 || got[0] != peerB {
		t.Fatalf("unexpected basechain peers after cooldown: %#v", got)
	}

	got = sub.availableArchivePeers(masterchain, []*overlayPeer{peerA, peerB})
	if len(got) != 2 {
		t.Fatalf("cooldown leaked into masterchain pool: %#v", got)
	}

	state := sub.archivePeers[archivePeerPoolKey(basechain)]
	state.cooldownUntil[archivePeerID(peerA)] = time.Now().Add(-time.Second)

	got = sub.availableArchivePeers(basechain, []*overlayPeer{peerA, peerB})
	if len(got) != 2 {
		t.Fatalf("expired cooldown entry was not restored: %#v", got)
	}
}

func TestRejectArchivePeerUnpinsSessionPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer"}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         node,
		archivePeers: map[string]*archivePeerPoolState{},
	}
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.rejectArchivePeer(sub, shard, peer, "test")
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; ok {
		t.Fatal("archive peer pin survived reject")
	}
	if !sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("rejected archive peer was not cooled down")
	}
}

func TestRejectArchiveNotAvailableUnpinsSessionPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer"}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         node,
		archivePeers: map[string]*archivePeerPoolState{},
	}
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.rejectArchivePeer(sub, shard, peer, archivePeerRejectNotAvailable)
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; ok {
		t.Fatal("archive not available reject should unpin archive peer")
	}
	if !sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("archive not available peer was not cooled down")
	}
}

func TestArchiveSessionCloseReleasesPinnedPeers(t *testing.T) {
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer"}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	session := node.BeginArchiveSession()

	session.noteArchivePeerSuccess(peer)
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("expected pinned archive peer")
	}

	session.Close()
	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; ok {
		t.Fatal("archive session pin survived close")
	}
}

func TestRotateUnavailableArchivePeersRemovesCooldownPeers(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peerA := testArchiveCandidate("cooldown-a")
	peerB := testArchiveCandidate("cooldown-b")
	leasedPeer := testArchiveCandidate("leased")
	sub := &overlaySubscription{
		log: discardLogger(),
		node: &Node{
			peerUse: map[PeerID]peerUse{leasedPeer.id: {downloads: 1}},
		},
		peers: map[PeerID]*overlayPeer{
			peerA.id:      peerA,
			peerB.id:      peerB,
			leasedPeer.id: leasedPeer,
		},
		neighbours: []PeerID{peerA.id, peerB.id, leasedPeer.id},
	}

	sub.cooldownArchivePeer(shard, peerA, "test")
	sub.cooldownArchivePeer(shard, peerB, "test")
	sub.cooldownArchivePeer(shard, leasedPeer, "test")

	if rotated := sub.rotateUnavailableArchivePeers(shard); rotated != 2 {
		t.Fatalf("unexpected rotated peer count: got %d want 2", rotated)
	}
	if _, ok := sub.peers[peerA.id]; ok {
		t.Fatal("cooled down peer A was not removed")
	}
	if _, ok := sub.peers[peerB.id]; ok {
		t.Fatal("cooled down peer B was not removed")
	}
	if _, ok := sub.peers[leasedPeer.id]; !ok {
		t.Fatal("leased peer was removed")
	}
	if len(sub.neighbours) != 1 || sub.neighbours[0] != leasedPeer.id {
		t.Fatalf("unexpected neighbours after rotation: %#v", sub.neighbours)
	}
}

func TestRotateUnavailableArchivePeersKeepsSessionPinnedPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	cooldownPeer := testArchiveCandidate("cooldown")
	pinnedPeer := testArchiveCandidate("pinned")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			cooldownPeer.id: cooldownPeer,
			pinnedPeer.id:   pinnedPeer,
		},
		neighbours: []PeerID{cooldownPeer.id, pinnedPeer.id},
	}
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(pinnedPeer)
	sub.cooldownArchivePeer(shard, cooldownPeer, "test")
	sub.cooldownArchivePeer(shard, pinnedPeer, "test")

	if rotated := sub.rotateUnavailableArchivePeers(shard); rotated != 1 {
		t.Fatalf("unexpected rotated peer count: got %d want 1", rotated)
	}
	if _, ok := sub.peers[cooldownPeer.id]; ok {
		t.Fatal("cooled down peer was not removed")
	}
	if _, ok := sub.peers[pinnedPeer.id]; !ok {
		t.Fatal("session-pinned peer was removed")
	}
}

func TestRotateUnavailableArchivePeersKeepsPoolWhenAnyPeerAvailable(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	cooldownPeer := testArchiveCandidate("cooldown")
	availablePeer := testArchiveCandidate("available")
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: &Node{},
		peers: map[PeerID]*overlayPeer{
			cooldownPeer.id:  cooldownPeer,
			availablePeer.id: availablePeer,
		},
		neighbours: []PeerID{cooldownPeer.id, availablePeer.id},
	}

	sub.cooldownArchivePeer(shard, cooldownPeer, "test")

	if rotated := sub.rotateUnavailableArchivePeers(shard); rotated != 0 {
		t.Fatalf("unexpected rotated peer count: got %d want 0", rotated)
	}
	if len(sub.peers) != 2 {
		t.Fatalf("pool should stay intact while an archive peer is available, got %d peers", len(sub.peers))
	}
	if len(sub.neighbours) != 2 {
		t.Fatalf("neighbours should stay intact while an archive peer is available, got %d", len(sub.neighbours))
	}
}

func TestArchiveQueryCandidatesUseNeighboursBeforeKnownPeers(t *testing.T) {
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

	got := sub.archiveQueryCandidates()
	if len(got) != 2 {
		t.Fatalf("archive candidates should use only neighbours when present, got %d", len(got))
	}
	seen := map[PeerID]struct{}{
		got[0].id: {},
		got[1].id: {},
	}
	if _, ok := seen[testPeerID("peer-1")]; !ok {
		t.Fatalf("expected peer-1 in archive neighbours, got %#v", got)
	}
	if _, ok := seen[testPeerID("peer-2")]; !ok {
		t.Fatalf("expected peer-2 in archive neighbours, got %#v", got)
	}
}

func TestArchiveQueryCandidatesDoNotUseKnownPeersWithoutNeighbours(t *testing.T) {
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

	got := sub.archiveQueryCandidates()
	if len(got) != 0 {
		t.Fatalf("archive candidates should use only neighbours, got %d known peers", len(got))
	}
}

func TestRememberedArchivePeersFallbackUsesKnownPeerWithoutNeighbours(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("remembered")
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         &Node{},
		archivePeers: map[string]*archivePeerPoolState{},
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}

	sub.rememberArchivePeer(shard, peer, int64(4<<20), time.Second)
	got, fallback := sub.availableArchivePeersWithFallback(shard)
	if !fallback {
		t.Fatal("expected remembered archive fallback")
	}
	if len(got) != 1 || got[0] != peer {
		t.Fatalf("unexpected remembered archive peers: %#v", got)
	}
}

func TestRememberedArchivePeersFallbackPrioritizesSpeedWithAttemptFairness(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	fast := testArchiveCandidate("fast")
	slow := testArchiveCandidate("slow")
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         &Node{},
		archivePeers: map[string]*archivePeerPoolState{},
		peers: map[PeerID]*overlayPeer{
			fast.id: fast,
			slow.id: slow,
		},
	}

	sub.rememberArchivePeer(shard, slow, int64(1<<20), time.Second)
	sub.rememberArchivePeer(shard, fast, int64(4<<20), time.Second)

	got, fallback := sub.availableArchivePeersWithFallback(shard)
	if !fallback {
		t.Fatal("expected remembered archive fallback")
	}
	if len(got) != 2 || got[0] != fast {
		t.Fatalf("fast remembered peer should be first initially: %#v", got)
	}

	for i := 0; i < 4; i++ {
		sub.noteRememberedArchivePeerAttempt(shard, fast)
	}

	got, fallback = sub.availableArchivePeersWithFallback(shard)
	if !fallback {
		t.Fatal("expected remembered archive fallback after attempts")
	}
	if len(got) != 2 || got[0] != slow {
		t.Fatalf("attempt fairness should give slower peer a retry slot: %#v", got)
	}
}

func TestRememberedArchivePeersFallbackRespectsArchiveCooldown(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("remembered")
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         &Node{},
		archivePeers: map[string]*archivePeerPoolState{},
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}

	sub.rememberArchivePeer(shard, peer, int64(4<<20), time.Second)
	sub.cooldownArchivePeer(shard, peer, "test")

	got, fallback := sub.availableArchivePeersWithFallback(shard)
	if fallback {
		t.Fatal("cooling down peer should not be used as remembered fallback")
	}
	if len(got) != 0 {
		t.Fatalf("cooling down remembered peer should not be available: %#v", got)
	}

	state := sub.archivePeers[archivePeerPoolKey(shard)]
	state.cooldownUntil[archivePeerID(peer)] = time.Now().Add(-time.Second)

	got, fallback = sub.availableArchivePeersWithFallback(shard)
	if !fallback {
		t.Fatal("expected remembered archive fallback after cooldown")
	}
	if len(got) != 1 || got[0] != peer {
		t.Fatalf("unexpected remembered archive peers after cooldown: %#v", got)
	}
}

func TestRememberedArchivePeersFallbackDropsAfterConsecutiveFailures(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("remembered")
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         &Node{},
		archivePeers: map[string]*archivePeerPoolState{},
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}

	sub.rememberArchivePeer(shard, peer, int64(4<<20), time.Second)
	for i := 0; i < archiveRememberedPeerMaxFailures; i++ {
		sub.noteRememberedArchivePeerFailure(shard, peer)
	}

	if state := sub.archivePeers[archivePeerPoolKey(shard)]; state != nil && len(state.remembered) != 0 {
		t.Fatalf("remembered peer survived consecutive failures: %#v", state.remembered)
	}
}

func TestRememberArchivePeerResetsConsecutiveFailures(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("remembered")
	sub := &overlaySubscription{
		log:          discardLogger(),
		node:         &Node{},
		archivePeers: map[string]*archivePeerPoolState{},
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}

	sub.rememberArchivePeer(shard, peer, int64(4<<20), time.Second)
	sub.noteRememberedArchivePeerFailure(shard, peer)
	sub.rememberArchivePeer(shard, peer, int64(4<<20), time.Second)

	got, fallback := sub.availableArchivePeersWithFallback(shard)
	if !fallback {
		t.Fatal("expected remembered archive fallback after success reset")
	}
	if len(got) != 1 || got[0] != peer {
		t.Fatalf("unexpected remembered archive peers after success reset: %#v", got)
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

	got := sub.archiveQueryCandidates()
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
	sub := &overlaySubscription{log: discardLogger(), archivePeers: map[string]*archivePeerPoolState{}}
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	sub.noteArchiveDownloadError(session, shard, peer, context.DeadlineExceeded)

	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; !ok {
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
	sub := &overlaySubscription{log: discardLogger(), archivePeers: map[string]*archivePeerPoolState{}}
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	for i := 0; i < archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(session, shard, peer, context.DeadlineExceeded)
	}

	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; !ok {
		t.Fatal("deadline grace should keep archive session pin")
	}
	if peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("deadline grace should not set slow penalty")
	}
	if sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("deadline grace should not cool down archive peer")
	}
}

func TestPinnedArchiveTimeoutsScaleWithDeadlineGrace(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{log: discardLogger(), archivePeers: map[string]*archivePeerPoolState{}}
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
		sub.noteArchiveDownloadError(session, shard, peer, context.DeadlineExceeded)

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
	sub := &overlaySubscription{log: discardLogger(), archivePeers: map[string]*archivePeerPoolState{}}
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

	sub.noteArchiveDownloadError(session, shard, peer, context.DeadlineExceeded)

	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; ok {
		t.Fatal("deadline after repeated archive info should clear archive session pin")
	}
	if !sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("deadline after repeated archive info should cool down archive peer")
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
	sub := &overlaySubscription{log: discardLogger(), archivePeers: map[string]*archivePeerPoolState{}}
	session := node.BeginArchiveSession()
	defer session.Close()

	session.noteArchivePeerSuccess(peer)
	for i := 0; i <= archiveSessionPinnedDeadlineGrace; i++ {
		sub.noteArchiveDownloadError(session, shard, peer, context.DeadlineExceeded)
	}

	if _, ok := node.archiveSessionPinnedPeerIDs()[peer.id]; ok {
		t.Fatal("deadline after grace should clear archive session pin")
	}
	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("deadline after grace should set slow penalty")
	}
	if !sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("deadline after grace should cool down archive peer")
	}
}

func TestArchiveDeadlineWithoutSuccessMarksPeerSlow(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := &overlayPeer{id: testPeerID("peer"), addr: "peer", alive: true}
	sub := &overlaySubscription{log: discardLogger(), archivePeers: map[string]*archivePeerPoolState{}}

	sub.noteArchiveDownloadError(nil, shard, peer, context.DeadlineExceeded)

	if !peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("first deadline without archive success should set slow penalty")
	}
	if !sub.archivePeerCoolingDown(shard, peer) {
		t.Fatal("first deadline without archive success should cool down archive peer")
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
