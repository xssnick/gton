package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func fillTestArchiveRoster(t *testing.T, pool *archivePeerPool, shard archive.ShardID, speed float64) []*overlayPeer {
	t.Helper()

	peers := make([]*overlayPeer, 0, archivePeerRosterLimit)
	for i := 0; i < archivePeerRosterLimit; i++ {
		peer := testArchiveOnlyPoolPeer(t, pool, fmt.Sprintf("roster-%02d", i))
		pool.noteArchiveDownload(shard, peer, int64(speed), time.Second)
		pool.markSuccess(shard, peer)
		peers = append(peers, peer)
	}
	return peers
}

func provenArchiveScoutResult(t testing.TB, pool *archivePeerPool, shard archive.ShardID, speed float64) archivePeerProbeResult {
	t.Helper()
	probe, ok := pool.probeSnapshot()
	if !ok || probe.shard != shard || probe.zeroState {
		beginTestArchiveRequest(t, pool, shard, 1)
		probe, _ = pool.probeSnapshot()
	}
	return archivePeerProbeResult{
		probe:          probe,
		evidence:       archivePeerEvidenceProven,
		at:             time.Now(),
		bytes:          archiveSliceProbeSize,
		elapsed:        time.Second,
		bytesPerSecond: speed,
	}
}

func testArchiveOfferPool(t testing.TB, sub *overlaySubscription) *archivePeerPool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &archivePeerPool{
		sub:                   sub,
		log:                   discardLogger(),
		ctx:                   ctx,
		cancel:                cancel,
		scout:                 newArchiveScoutShared(),
		closeDone:             make(chan struct{}),
		peers:                 map[PeerID]*archivePeer{},
		shards:                map[string]*archiveShardPeerState{},
		valuable:              map[PeerID]archiveValuablePeer{},
		demands:               map[uint64]*archivePeerDemand{},
		demandKeys:            map[string]uint64{},
		demandBlockedUntil:    map[archivePeerDemandRetryKey]time.Time{},
		rejectedUntil:         map[PeerID]time.Time{},
		transportBlockedUntil: map[PeerID]time.Time{},
		randomExpandedUntil:   map[PeerID]time.Time{},
		scouting:              map[PeerID]struct{}{},
		offers:                make(chan archivePeerOffer, archivePeerPendingLimit),
	}
}

func TestArchiveScoutReplacesWorstPeerInFullRoster(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peers := fillTestArchiveRoster(t, pool, shard, 4<<20)

	pool.mx.Lock()
	health := pool.shardPeerLocked(archivePeerPoolKey(shard), peers[0].id)
	health.bytes = 1 << 20
	health.downloadElapsed = time.Second
	pool.mx.Unlock()
	candidate := testArchiveCandidate("faster-newcomer")
	result := pool.admitArchiveOnlyPeer(candidate, provenArchiveScoutResult(t, pool, shard, 8<<20))
	if !result.admitted || !result.replaced {
		t.Fatalf("admission = %+v, want admitted replacement", result)
	}
	if result.evicted != peers[0] {
		t.Fatalf("evicted peer = %p, want slowest %p", result.evicted, peers[0])
	}
	closeArchiveOnlyPeer(result.evicted)
	if got := pool.archiveOnlySize(); got != archivePeerRosterLimit {
		t.Fatalf("archive roster size = %d, want %d", got, archivePeerRosterLimit)
	}
	if !testArchivePoolHasPeer(pool, candidate.id) || testArchivePoolHasPeer(pool, peers[0].id) {
		t.Fatal("atomic roster replacement did not install newcomer and remove worst peer")
	}
	pool.mx.Lock()
	valuable, reserved := pool.valuable[peers[0].id]
	pool.mx.Unlock()
	if !reserved || valuable.nextTryAt.IsZero() {
		t.Fatal("evicted valuable peer was not moved to reserve")
	}
	if pool.recentlyRejected(peers[0].id, time.Now()) || pool.scout.retry.peerBlocked(peers[0].id, time.Now()) {
		t.Fatal("evicted valuable peer entered the junk retry cache")
	}
}

func TestArchiveScoutCurrentBytesReplaceHistoricallyProvenUnknownPeer(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	fillTestArchiveRoster(t, pool, shard, 4<<20)

	failed := testArchiveCandidate("failed-newcomer")
	if got := pool.admitArchiveOnlyPeer(failed, archivePeerProbeResult{}); got.admitted {
		t.Fatal("candidate without archive evidence entered full roster")
	}
	unmeasured := testArchiveCandidate("short-sample-newcomer")
	got := pool.admitArchiveOnlyPeer(unmeasured, provenArchiveScoutResult(t, pool, shard, 0))
	if !got.admitted || !got.replaced {
		t.Fatal("current real bytes did not replace a peer with unknown current coverage")
	}
	closeArchiveOnlyPeer(got.evicted)
	if got := pool.archiveOnlySize(); got != archivePeerRosterLimit {
		t.Fatalf("archive roster size = %d, want %d", got, archivePeerRosterLimit)
	}
}

func TestArchiveScoutUnmeasuredCandidateDoesNotEvictCurrentHealthyRoster(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peers := fillTestArchiveRoster(t, pool, shard, 4<<20)
	result := provenArchiveScoutResult(t, pool, shard, 0)

	pool.mx.Lock()
	demand := pool.demands[result.probe.demandID]
	for _, peer := range peers {
		demand.peers[peer.id] = archivePeerDemandPeer{
			evidence: archivePeerDemandProven,
			at:       time.Now(),
		}
	}
	pool.mx.Unlock()

	candidate := testArchiveCandidate("unmeasured-current-newcomer")
	if got := pool.admitArchiveOnlyPeer(candidate, result); got.admitted {
		t.Fatal("unmeasured candidate evicted a peer from a fully covered healthy demand")
	}
}

func TestArchiveScoutEvictionProtectsPeerLease(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peers := fillTestArchiveRoster(t, pool, shard, 4<<20)

	pool.mx.Lock()
	health := pool.shardPeerLocked(archivePeerPoolKey(shard), peers[0].id)
	health.bytes = 1 << 20
	health.downloadElapsed = time.Second
	pool.mx.Unlock()
	release, ok := pool.acquire(peers[0])
	if !ok {
		t.Fatal("failed to lease slow roster peer")
	}
	defer release()

	candidate := testArchiveCandidate("leased-replacement")
	result := pool.admitArchiveOnlyPeer(candidate, provenArchiveScoutResult(t, pool, shard, 8<<20))
	if !result.admitted || !result.replaced {
		t.Fatalf("admission = %+v, want replacement of an unleased peer", result)
	}
	if result.evicted == peers[0] || !testArchivePoolHasPeer(pool, peers[0].id) {
		t.Fatal("leased archive peer was evicted")
	}
	closeArchiveOnlyPeer(result.evicted)
}

func TestArchiveScoutRejectsDisconnectedCandidateBeforeEviction(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	fillTestArchiveRoster(t, pool, shard, 4<<20)

	wrapper, conn := newTestOverlayWrapper()
	candidate := testArchiveCandidate("closed-newcomer")
	candidate.overlay = wrapper
	conn.Close()
	if got := pool.admitArchiveOnlyPeer(candidate, provenArchiveScoutResult(t, pool, shard, 8<<20)); got.admitted {
		t.Fatal("disconnected candidate entered roster")
	}
	if got := pool.archiveOnlySize(); got != archivePeerRosterLimit {
		t.Fatalf("archive roster changed after disconnected candidate: %d", got)
	}
}

func TestArchiveRosterIsIndependentFromLiveRoster(t *testing.T) {
	live := make(map[PeerID]*overlayPeer, maxPeersPerOverlay)
	for i := 0; i < maxPeersPerOverlay; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("live-%03d", i))
		live[peer.id] = peer
	}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger(), peers: live})
	pool := testArchivePool(t, sub)

	if got := pool.size(); got != 0 {
		t.Fatalf("archive roster adopted %d live peers", got)
	}
	if got := len(sub.peersSnapshot()); got != maxPeersPerOverlay {
		t.Fatalf("live roster size = %d, want %d", got, maxPeersPerOverlay)
	}
}

func TestArchiveScoutConcurrencyIsSharedAcrossPools(t *testing.T) {
	shared := newArchiveScoutShared()
	releases := make([]func(), 0, archiveConcurrentScoutLimit)
	for i := 0; i < archiveConcurrentScoutLimit; i++ {
		release, err := shared.acquire(context.Background(), testPeerID(fmt.Sprintf("slot-%d", i)))
		if err != nil {
			t.Fatalf("acquire scout slot %d: %v", i, err)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := shared.acquire(ctx, testPeerID("slot-overflow")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overflow scout acquire error = %v, want deadline", err)
	}
}

func TestTransientArchiveScoutQueriesRandomPeersWithoutArchive(t *testing.T) {
	fullID := []byte{1, 2, 3}
	_, advertisedKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate advertised peer key: %v", err)
	}
	advertised, err := overlay.NewNode(fullID, advertisedKey)
	if err != nil {
		t.Fatalf("create advertised overlay node: %v", err)
	}

	node := newTestNode(t)
	node.dht = &fakeDHTClient{findAddressesErr: context.Canceled}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      fullID,
			ShortID:     advertised.Overlay,
			RandomPeers: true,
		},
		peers: map[PeerID]*overlayPeer{},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 1)

	wrapper, base := newTestOverlayWrapper()
	peer := testArchiveCandidate("transient-random-relay")
	peer.overlay = wrapper
	statsBefore := peer.statsSnapshot()

	randomStarted := make(chan struct{})
	archiveStarted := make(chan struct{})
	releaseRandom := make(chan struct{}, 1)
	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		switch testOverlayQueryPayload(req).(type) {
		case overlay.GetRandomPeers:
			close(randomStarted)
			<-releaseRandom
			*result.(*overlay.NodesList) = overlay.NodesList{List: []overlay.Node{*advertised}}
			return nil
		case GetArchiveInfo:
			close(archiveStarted)
			*result.(*tl.Serializable) = ArchiveNotFound{}
			return nil
		default:
			return fmt.Errorf("unexpected transient scout query %T", testOverlayQueryPayload(req))
		}
	}

	result := make(chan bool, 1)
	done := make(chan struct{})
	t.Cleanup(func() {
		select {
		case releaseRandom <- struct{}{}:
		default:
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("transient random relay goroutine did not stop during cleanup")
		}
		closeArchiveOnlyPeer(peer)
	})
	go func() {
		defer close(done)
		result <- pool.scoutConnectedTransientArchivePeer(peer, pool.probeSnapshots(4), false)
	}()

	select {
	case <-randomStarted:
	case <-time.After(time.Second):
		t.Fatal("transient DHT peer was not asked for random peers")
	}
	select {
	case <-archiveStarted:
	case <-time.After(time.Second):
		t.Fatal("archive probe did not run alongside random peer discovery")
	}
	releaseRandom <- struct{}{}

	select {
	case admitted := <-result:
		if admitted {
			t.Fatal("peer without the requested archive entered the archive roster")
		}
	case <-time.After(time.Second):
		t.Fatal("transient scout did not finish after random peer response")
	}
	stats := pool.scoutStats.snapshot()
	if stats.transientRandomQueries != 1 || stats.transientRandomResponses != 1 ||
		stats.transientRandomReceivedNodes != 1 || stats.transientRandomProcessedNodes != 1 ||
		stats.transientRandomQueued != 1 {
		t.Fatalf("transient random peer stats = %+v, want one successful query", stats)
	}
	if stats.proven != 0 || stats.available != 0 || stats.admitted != 0 || stats.notAvailable != 1 {
		t.Fatalf("archive evidence changed by random peer response: %+v", stats)
	}
	statsAfter := peer.statsSnapshot()
	if statsAfter.failedQueries != statsBefore.failedQueries || statsAfter.unreliability != statsBefore.unreliability {
		t.Fatalf("random relay changed source peer rating: before=%+v after=%+v", statsBefore, statsAfter)
	}
}

func TestArchiveDHTOfferDoesNotRefreshLiveAnnouncement(t *testing.T) {
	fullID := []byte{4, 5, 6}
	_, peerKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate advertised peer key: %v", err)
	}
	advertised, err := overlay.NewNode(fullID, peerKey)
	if err != nil {
		t.Fatalf("create advertised overlay node: %v", err)
	}

	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      fullID,
			ShortID:     advertised.Overlay,
			RandomPeers: true,
		},
		peers: map[PeerID]*overlayPeer{},
	})
	identity, err := sub.overlayNodeIdentity(*advertised)
	if err != nil {
		t.Fatalf("resolve advertised identity: %v", err)
	}
	original := cloneOverlayNode(advertised)
	original.Version--
	live := &overlayPeer{
		id:        identity.peerID,
		addr:      "127.0.0.1:29999",
		pub:       append(ed25519.PublicKey(nil), identity.pub...),
		overlay:   &overlay.ADNLOverlayWrapper{},
		announced: original,
		alive:     true,
	}
	sub.peers[live.id] = live
	pool := testArchiveOfferPool(t, sub)
	beginTestArchiveRequest(t, pool, archive.ShardID{Workchain: -1, Shard: topShard}, 1)

	if status := pool.offerArchiveNode(*advertised); status != archivePeerOfferQueued {
		t.Fatalf("archive DHT offer status = %d, want queued", status)
	}
	live.statsMx.Lock()
	version := live.announced.Version
	live.statsMx.Unlock()
	if version != original.Version {
		t.Fatalf("archive DHT changed live announcement version = %d, want %d", version, original.Version)
	}
}

func TestTransientArchiveRandomPeersFeedScoutQueue(t *testing.T) {
	fullID := []byte{1, 2, 3}
	advertised := make([]overlay.Node, 0, archiveTransientRandomReplyLimit+2)
	for i := 0; i < archiveTransientRandomReplyLimit+2; i++ {
		_, advertisedKey, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatalf("generate advertised peer key %d: %v", i, err)
		}
		candidate, err := overlay.NewNode(fullID, advertisedKey)
		if err != nil {
			t.Fatalf("create advertised overlay node %d: %v", i, err)
		}
		advertised = append(advertised, *candidate)
	}

	node := newTestNode(t)
	node.dht = &fakeDHTClient{findAddressesErr: context.Canceled}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      fullID,
			ShortID:     advertised[0].Overlay,
			RandomPeers: true,
		},
		peers: map[PeerID]*overlayPeer{},
	})
	pool := testArchivePool(t, sub)
	beginTestArchiveRequest(t, pool, archive.ShardID{Workchain: -1, Shard: topShard}, 1)

	wrapper, base := newTestOverlayWrapper()
	peer := testArchiveCandidate("transient-random-source")
	peer.overlay = wrapper
	defer closeArchiveOnlyPeer(peer)
	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		if _, ok := testOverlayQueryPayload(req).(overlay.GetRandomPeers); !ok {
			return fmt.Errorf("unexpected random source query %T", testOverlayQueryPayload(req))
		}
		*result.(*overlay.NodesList) = overlay.NodesList{List: advertised}
		return nil
	}

	pool.exchangeTransientRandomPeers(context.Background(), peer)
	stats := pool.scoutStats.snapshot()
	if stats.transientRandomReceivedNodes != uint64(len(advertised)) ||
		stats.transientRandomProcessedNodes != archiveTransientRandomReplyLimit ||
		stats.transientRandomQueued != archiveTransientRandomReplyLimit {
		t.Fatalf("transient random peer stats = %+v, want capped queued nodes", stats)
	}
}

func TestTransientRandomExpansionIsPeerTTLBounded(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	now := time.Now()
	first := testPeerID("transient-random-ttl")
	if !pool.reserveTransientRandomExpansion(first, now) {
		t.Fatal("first transient random expansion was rejected")
	}
	if pool.reserveTransientRandomExpansion(first, now.Add(time.Second)) {
		t.Fatal("same peer expanded again before its TTL expired")
	}
	if !pool.reserveTransientRandomExpansion(first, now.Add(archiveTransientRandomExpansionTTL+time.Nanosecond)) {
		t.Fatal("peer remained blocked after transient expansion TTL")
	}
}

func TestArchiveBackoffStillReoffersDisconnectedRandomRelay(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			RandomPeers: true,
		},
	})
	pool := testArchiveOfferPool(t, sub)
	probe := beginTestArchiveRequest(t, pool, archive.ShardID{Workchain: -1, Shard: topShard}, 1)
	peerID := testPeerID("blocked-random-relay")
	pool.recordDemandNotAvailable(probe, peerID, archivePeerNotAvailableTTL)
	pool.rememberRejected(peerID, archivePeerNoBenefitTTL)

	now := time.Now()
	pool.mx.Lock()
	pool.randomExpandedUntil[peerID] = now.Add(archiveTransientRandomExpansionTTL)
	pool.mx.Unlock()
	identity := overlayNodeIdentity{peerID: peerID, pub: make(ed25519.PublicKey, ed25519.PublicKeySize)}
	if status := pool.offerArchiveIdentity(nil, identity, "invalid-endpoint"); status != archivePeerOfferBackoff {
		t.Fatalf("relay offer before random TTL = %d, want backoff", status)
	}

	pool.mx.Lock()
	pool.randomExpandedUntil[peerID] = now.Add(-time.Nanosecond)
	pool.mx.Unlock()
	if status := pool.offerArchiveIdentity(nil, identity, "invalid-endpoint"); status != archivePeerOfferQueued {
		t.Fatalf("relay offer after random TTL = %d, want queued despite archive backoff", status)
	}
}

func TestArchivePoolReoffersRandomRelayBetweenDemands(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			RandomPeers: true,
		},
	})
	pool := testArchiveOfferPool(t, sub)
	peerID := testPeerID("between-demand-relay")
	identity := overlayNodeIdentity{peerID: peerID, pub: make(ed25519.PublicKey, ed25519.PublicKeySize)}

	if status := pool.offerArchiveIdentity(nil, identity, "invalid-endpoint"); status != archivePeerOfferQueued {
		t.Fatalf("between-demand relay offer = %d, want queued", status)
	}
}

func TestValuableScoutCanProbeDemandAfterNotAvailableBackoff(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      []byte{1},
			ShortID:     []byte{1},
			RandomPeers: true,
		},
	})
	pool := testArchivePool(t, sub)
	probe := beginTestArchiveRequest(t, pool, archive.ShardID{Workchain: -1, Shard: topShard}, 1)

	wrapper, base := newTestOverlayWrapper()
	peer := testArchiveCandidate("valuable-demand-retry")
	peer.overlay = wrapper
	defer closeArchiveOnlyPeer(peer)
	pool.recordDemandNotAvailable(probe, peer.id, archivePeerNotAvailableTTL)

	archiveQueries := 0
	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		switch testOverlayQueryPayload(req).(type) {
		case overlay.GetRandomPeers:
			*result.(*overlay.NodesList) = overlay.NodesList{}
			return nil
		case GetArchiveInfo:
			archiveQueries++
			*result.(*tl.Serializable) = ArchiveNotFound{}
			return nil
		default:
			return fmt.Errorf("unexpected valuable retry query %T", testOverlayQueryPayload(req))
		}
	}

	if pool.scoutConnectedTransientArchivePeer(peer, []archivePeerProbe{probe}, false) {
		t.Fatal("archive-blocked relay entered the archive roster")
	}
	if archiveQueries != 0 {
		t.Fatalf("ordinary scout issued %d archive queries during demand backoff", archiveQueries)
	}
	if pool.scoutConnectedTransientArchivePeer(peer, []archivePeerProbe{probe}, true) {
		t.Fatal("valuable peer without the requested archive entered the roster")
	}
	if archiveQueries != 1 {
		t.Fatalf("valuable retry issued %d archive queries, want 1", archiveQueries)
	}
}

func TestTransientRandomResponsePreventsUnreachableCache(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      []byte{1},
			ShortID:     []byte{1},
			RandomPeers: true,
		},
		peers: map[PeerID]*overlayPeer{},
	})
	pool := testArchivePool(t, sub)
	beginTestArchiveRequest(t, pool, archive.ShardID{Workchain: -1, Shard: topShard}, 1)

	wrapper, base := newTestOverlayWrapper()
	peer := testArchiveCandidate("transient-random-live-transport")
	peer.overlay = wrapper
	defer closeArchiveOnlyPeer(peer)
	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		switch testOverlayQueryPayload(req).(type) {
		case overlay.GetRandomPeers:
			*result.(*overlay.NodesList) = overlay.NodesList{}
			return nil
		case GetArchiveInfo:
			return errors.New("archive query failed")
		default:
			return fmt.Errorf("unexpected transient scout query %T", testOverlayQueryPayload(req))
		}
	}

	if pool.scoutConnectedTransientArchivePeer(peer, pool.probeSnapshots(4), false) {
		t.Fatal("peer with a failed archive query entered the archive roster")
	}
	if pool.scout.retry.peerBlocked(peer.id, time.Now()) || pool.scout.retry.endpointBlocked(peer.addr, time.Now()) {
		t.Fatal("successful random peer response was cached as an unreachable transport")
	}
	stats := pool.scoutStats.snapshot()
	if stats.transportFailure != 1 || stats.transientRandomResponses != 1 {
		t.Fatalf("transient transport stats = %+v, want archive failure and live random response", stats)
	}
}

func TestTransientArchiveScoutWaitsForRandomPeersBeforeAdmission(t *testing.T) {
	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{
			Kind:        overlayKindPublicShard,
			FullID:      []byte{1},
			ShortID:     []byte{1},
			RandomPeers: true,
		},
		peers: map[PeerID]*overlayPeer{},
	})
	pool := testArchivePool(t, sub)
	beginTestArchiveRequest(t, pool, archive.ShardID{Workchain: -1, Shard: topShard}, 1)

	wrapper, base := newTestOverlayWrapper()
	archiveSliceStarted := make(chan GetArchiveSlice, 1)
	archiveRLDP := &testArchiveRLDP{
		adnl:               newTestOverlayADNL(),
		asyncResult:        testArchivePackBytes("transient-admission"),
		asyncStarted:       archiveSliceStarted,
		asyncErrors:        map[int64]error{},
		asyncDelays:        map[int64]time.Duration{},
		asyncWaitForCancel: map[int64]bool{},
	}
	peer := testArchiveCandidate("transient-admission")
	peer.overlay = wrapper
	peer.rldpOverlay = overlay.CreateExtendedRLDP(archiveRLDP).CreateOverlay([]byte{1})

	randomStarted := make(chan struct{})
	releaseRandom := make(chan struct{}, 1)
	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		switch testOverlayQueryPayload(req).(type) {
		case overlay.GetRandomPeers:
			close(randomStarted)
			<-releaseRandom
			*result.(*overlay.NodesList) = overlay.NodesList{}
			return nil
		case GetArchiveInfo:
			*result.(*tl.Serializable) = ArchiveInfo{ID: 1}
			return nil
		default:
			return fmt.Errorf("unexpected transient scout query %T", testOverlayQueryPayload(req))
		}
	}

	result := make(chan bool, 1)
	done := make(chan struct{})
	t.Cleanup(func() {
		select {
		case releaseRandom <- struct{}{}:
		default:
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("transient admission goroutine did not stop during cleanup")
		}
		if !testArchivePoolHasPeer(pool, peer.id) {
			closeArchiveOnlyPeer(peer)
		}
	})
	go func() {
		defer close(done)
		result <- pool.scoutConnectedTransientArchivePeer(peer, pool.probeSnapshots(4), false)
	}()
	select {
	case <-randomStarted:
	case <-time.After(time.Second):
		t.Fatal("transient random peer query did not start")
	}
	waitArchiveSliceStarted(t, archiveSliceStarted)
	deadline := time.Now().Add(time.Second)
	for len(archiveRLDP.snapshot().asyncCompleted) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("transient archive probe did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	if testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("transient peer entered roster while getRandomPeers still used its overlay")
	}

	releaseRandom <- struct{}{}
	select {
	case admitted := <-result:
		if !admitted {
			t.Fatal("proven transient peer was not admitted after getRandomPeers completed")
		}
	case <-time.After(time.Second):
		t.Fatal("transient scout did not finish after random peer response")
	}
	if !testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("proven transient peer missing from archive roster")
	}
}

func TestArchiveHealthyRosterStillWalksDHT(t *testing.T) {
	fake := &fakeDHTClient{findOverlayNodesContinuation: &dht.Continuation{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: &Node{dht: fake},
		spec: overlaySpec{FullID: []byte{1}, ShortID: []byte{1}, Kind: overlayKindPublicShard},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 1)
	fillTestArchiveRoster(t, pool, shard, 4<<20)

	done := pool.refill(context.Background(), false)
	if done == nil {
		t.Fatal("healthy full roster suppressed DHT scouting")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("healthy DHT scouting did not finish")
	}
	if got := fake.findOverlayNodesCallCount(); got != 1 {
		t.Fatalf("healthy DHT seed queries = %d, want 1", got)
	}
}

func TestArchiveUrgentRefillJoinsRunningSeedQuery(t *testing.T) {
	gate := make(chan struct{})
	fake := &fakeDHTClient{
		findOverlayNodesWait:         gate,
		findOverlayNodesWaitAt:       1,
		findOverlayNodesContinuation: &dht.Continuation{},
	}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: &Node{dht: fake},
		spec: overlaySpec{FullID: []byte{1}, ShortID: []byte{1}, Kind: overlayKindPublicShard},
	})
	pool := testArchivePool(t, sub)
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 1)
	fillTestArchiveRoster(t, pool, shard, 4<<20)

	done := pool.refill(context.Background(), false)
	deadline := time.Now().Add(time.Second)
	for fake.findOverlayNodesCallCount() < 1 {
		if time.Now().After(deadline) {
			close(gate)
			t.Fatal("archive seed query did not start")
		}
		time.Sleep(time.Millisecond)
	}
	forcedDone := pool.refill(context.Background(), true)
	if forcedDone != done {
		close(gate)
		t.Fatal("forced request did not join the running discovery")
	}
	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queued forced discovery did not finish")
	}
	if got := fake.findOverlayNodesCallCount(); got != 1 {
		t.Fatalf("DHT seed queries after urgent refill = %d, want 1", got)
	}
}

func TestArchiveProbeBytesResetOnlyProbeFailures(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("split-failures")
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	pool.noteFailure(shard, peer, archivePeerRejectCandidateFailed)
	pool.noteFailure(shard, peer, archivePeerRejectNotAvailable)
	pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	pool.noteFailure(shard, peer, ArchivePeerRejectImportIncomplete)
	pool.markProven(shard, peer)

	pool.mx.Lock()
	state := pool.shards[archivePeerPoolKey(shard)]
	health := state.peers[peer.id]
	failure := health.failure
	cooling := !health.cooldownUntil.IsZero()
	pool.mx.Unlock()
	if failure.probeErrors != 0 || failure.notAvailable != 0 {
		t.Fatalf("probe-local failures survived real probe bytes: %+v", failure)
	}
	if failure.downloadErrors != 1 || failure.badImports != 1 {
		t.Fatalf("full-download failures were forgiven by probe: %+v", failure)
	}
	if !cooling {
		t.Fatal("bad-import cooldown was cleared by probe bytes")
	}
}

func TestArchiveRotationRechecksFailureSnapshot(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	peer := testArchiveCandidate("stale-rotation")
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	addTestArchiveOnlyPeer(pool, peer)

	var failure archivePeerFailure
	for i := 0; i < archivePeerErrorRotateThreshold; i++ {
		pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	}
	pool.mx.Lock()
	failure = pool.shards[archivePeerPoolKey(shard)].peers[peer.id].failure
	pool.mx.Unlock()
	pool.markSuccess(shard, peer)

	candidate := archivePeerRotationCandidate{peerID: peer.id, failure: failure}
	if pool.rotatePeer(archivePeerPoolKey(shard), candidate) {
		t.Fatal("stale failure snapshot rotated a recovered peer")
	}
	if !testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("recovered peer disappeared after stale rotation")
	}
}

func TestArchiveRetryCacheDoesNotShortenBackoff(t *testing.T) {
	now := time.Now()
	peerID := testPeerID("retry-max")
	cache := archivePeerRetryCache{
		peers:     map[PeerID]time.Time{},
		endpoints: map[string]time.Time{},
		inflight:  map[PeerID]struct{}{},
	}
	cache.rememberPeer(peerID, now.Add(20*time.Minute))
	cache.rememberPeer(peerID, now.Add(time.Minute))
	cache.rememberEndpoint("127.0.0.1:1", now.Add(20*time.Minute))
	cache.rememberEndpoint("127.0.0.1:1", now.Add(time.Minute))

	if !cache.peerBlocked(peerID, now.Add(10*time.Minute)) {
		t.Fatal("short retry shortened an existing peer backoff")
	}
	if !cache.endpointBlocked("127.0.0.1:1", now.Add(10*time.Minute)) {
		t.Fatal("short retry shortened an existing endpoint backoff")
	}
}

func TestArchiveDemandReferenceLifecycle(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)

	first, releaseFirst, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin first demand: %v", err)
	}
	second, releaseSecond, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin duplicate demand: %v", err)
	}
	if first.demandID != second.demandID {
		t.Fatalf("duplicate demand refs differ: %d != %d", first.demandID, second.demandID)
	}
	releaseFirst()
	pool.mx.Lock()
	_, retained := pool.demands[first.demandID]
	pool.mx.Unlock()
	if !retained {
		t.Fatal("demand disappeared before its last reference was released")
	}
	releaseSecond()
	pool.mx.Lock()
	_, retained = pool.demands[first.demandID]
	pool.mx.Unlock()
	if retained {
		t.Fatal("demand survived its last reference")
	}
}

func TestArchiveDemandFailuresAndNoBenefitAreIsolated(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	peerID := testPeerID("demand-isolation")
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	first, releaseFirst, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin first demand: %v", err)
	}
	defer releaseFirst()
	second, releaseSecond, err := pool.beginArchiveRequest(shard, 101)
	if err != nil {
		t.Fatalf("begin second demand: %v", err)
	}
	defer releaseSecond()

	pool.recordDemandNotAvailable(first, peerID, time.Minute)
	pool.recordDemandNoBenefit(first, peerID, time.Minute)
	if !pool.demandPeerBlocked(first, peerID, time.Now()) {
		t.Fatal("first demand did not retain its negative evidence")
	}
	if pool.demandPeerBlocked(second, peerID, time.Now()) {
		t.Fatal("first demand negative evidence leaked into second demand")
	}
	if pool.recentlyRejected(peerID, time.Now()) {
		t.Fatal("demand-local failure leaked into pool-wide rejection cache")
	}
}

func TestArchiveScoutNotAvailableBackoffIsDemandScoped(t *testing.T) {
	master := archive.ShardID{Workchain: -1, Shard: topShard}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "not-available-demand-scope")

	masterProbe, releaseMaster, err := pool.beginArchiveRequest(master, 100)
	if err != nil {
		t.Fatalf("begin master demand: %v", err)
	}
	defer releaseMaster()
	shardProbe, releaseShard, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin shard demand: %v", err)
	}
	defer releaseShard()

	pool.noteScoutFailure(masterProbe, peer, archive.ErrNotAvailable)
	now := time.Now()
	if !pool.demandPeerBlocked(masterProbe, peer.id, now) {
		t.Fatal("not-available response did not block its exact master demand")
	}
	if pool.demandPeerBlocked(shardProbe, peer.id, now) {
		t.Fatal("master not-available response blocked shard archive demand")
	}
	if pool.transportBlocked(peer.id, now) {
		t.Fatal("not-available response leaked into peer-wide transport backoff")
	}
}

func TestArchiveScoutInvalidResponseBackoffIsDemandScoped(t *testing.T) {
	master := archive.ShardID{Workchain: -1, Shard: topShard}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "invalid-response-demand-scope")

	masterProbe, releaseMaster, err := pool.beginArchiveRequest(master, 100)
	if err != nil {
		t.Fatalf("begin master demand: %v", err)
	}
	defer releaseMaster()
	shardProbe, releaseShard, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin shard demand: %v", err)
	}
	defer releaseShard()

	pool.noteScoutFailure(masterProbe, peer, fmt.Errorf("%w: test", errArchivePeerInvalidResponse))
	now := time.Now()
	if !pool.demandPeerBlocked(masterProbe, peer.id, now) {
		t.Fatal("invalid response did not block its exact master demand")
	}
	if pool.demandPeerBlocked(shardProbe, peer.id, now) {
		t.Fatal("master invalid response blocked shard archive demand")
	}
	if pool.transportBlocked(peer.id, now) {
		t.Fatal("invalid response leaked into peer-wide transport backoff")
	}
}

func TestArchiveScoutTransportBackoffIsPeerScoped(t *testing.T) {
	master := archive.ShardID{Workchain: -1, Shard: topShard}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "transport-peer-scope")

	masterProbe, releaseMaster, err := pool.beginArchiveRequest(master, 100)
	if err != nil {
		t.Fatalf("begin master demand: %v", err)
	}
	defer releaseMaster()
	shardProbe, releaseShard, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin shard demand: %v", err)
	}
	defer releaseShard()

	pool.noteScoutFailure(masterProbe, peer, context.DeadlineExceeded)
	now := time.Now()
	if !pool.transportBlocked(peer.id, now) {
		t.Fatal("transport timeout did not enter peer-wide transport backoff")
	}
	if pool.demandPeerBlocked(masterProbe, peer.id, now) || pool.demandPeerBlocked(shardProbe, peer.id, now) {
		t.Fatal("transport timeout was recorded as demand-local archive absence")
	}

	pool.markProven(master, peer)
	if pool.transportBlocked(peer.id, time.Now()) {
		t.Fatal("real archive bytes did not clear peer-wide transport backoff")
	}
}

func TestArchiveStaleDemandResultCannotMutateRoster(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	probe, release, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin demand: %v", err)
	}
	release()

	candidate := testArchiveCandidate("stale-demand-result")
	result := archivePeerProbeResult{
		probe:          probe,
		evidence:       archivePeerEvidenceProven,
		at:             time.Now(),
		bytes:          archiveSliceProbeSize,
		elapsed:        time.Second,
		bytesPerSecond: 8 << 20,
	}
	admission := pool.admitArchiveOnlyPeer(candidate, result)
	if admission.admitted || !admission.stale {
		t.Fatalf("stale admission = %+v, want stale no-op", admission)
	}
	if testArchivePoolHasPeer(pool, candidate.id) {
		t.Fatal("stale demand result entered roster")
	}
}

func TestArchiveZeroStatePreparedCandidateReplacesCurrentNegativeRoster(t *testing.T) {
	block := testBlockID(-1, topShard, 0)
	shard := archiveShardFromBlock(block)
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peers := fillTestArchiveRoster(t, pool, shard, 4<<20)
	probe, release, err := pool.beginZeroStateRequest(shard, block)
	if err != nil {
		t.Fatalf("begin zero-state demand: %v", err)
	}
	defer release()
	for _, peer := range peers {
		pool.recordDemandNotAvailable(probe, peer.id, time.Minute)
	}

	candidate := testArchiveCandidate("zero-state-prepared-newcomer")
	result := archivePeerProbeResult{
		probe:    probe,
		evidence: archivePeerEvidenceAvailable,
		at:       time.Now(),
	}
	admission := pool.admitArchiveOnlyPeer(candidate, result)
	if !admission.admitted || !admission.replaced {
		t.Fatalf("prepared zero-state admission = %+v, want current-useful replacement", admission)
	}
	closeArchiveOnlyPeer(admission.evicted)
	pool.mx.Lock()
	entry := pool.peers[candidate.id]
	pool.mx.Unlock()
	if entry == nil || entry.archiveDownloads != 0 {
		t.Fatalf("PreparedState candidate archive success = %v, want available but unproven", entry)
	}
}

func TestArchiveDemandCoverageProtectsUniquePeer(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peers := fillTestArchiveRoster(t, pool, shard, 4<<20)
	first, releaseFirst, err := pool.beginArchiveRequest(shard, 100)
	if err != nil {
		t.Fatalf("begin first demand: %v", err)
	}
	defer releaseFirst()
	second, releaseSecond, err := pool.beginArchiveRequest(shard, 101)
	if err != nil {
		t.Fatalf("begin second demand: %v", err)
	}
	defer releaseSecond()

	pool.mx.Lock()
	for i := 0; i < 4; i++ {
		pool.demands[first.demandID].peers[peers[i].id] = archivePeerDemandPeer{evidence: archivePeerDemandProven, at: time.Now()}
	}
	unique := peers[0]
	pool.demands[second.demandID].peers[unique.id] = archivePeerDemandPeer{evidence: archivePeerDemandProven, at: time.Now()}
	pool.mx.Unlock()

	candidate := testArchiveCandidate("coverage-balanced-newcomer")
	result := archivePeerProbeResult{
		probe:          first,
		evidence:       archivePeerEvidenceProven,
		at:             time.Now(),
		bytes:          archiveSliceProbeSize,
		elapsed:        time.Second,
		bytesPerSecond: 8 << 20,
	}
	admission := pool.admitArchiveOnlyPeer(candidate, result)
	if !admission.admitted || !admission.replaced {
		t.Fatalf("balanced coverage admission = %+v, want replacement", admission)
	}
	if admission.evicted == unique || !testArchivePoolHasPeer(pool, unique.id) {
		t.Fatal("candidate evicted the only peer covering another active demand")
	}
	closeArchiveOnlyPeer(admission.evicted)
}

func TestArchiveDemandCoverageNeverStarvesExistingDemand(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peers := fillTestArchiveRoster(t, pool, shard, 4<<20)

	releases := make([]func(), 0, len(peers)+1)
	for i, peer := range peers {
		probe, release, err := pool.beginArchiveRequest(shard, uint32(100+i))
		if err != nil {
			t.Fatalf("begin protected demand %d: %v", i, err)
		}
		releases = append(releases, release)
		pool.mx.Lock()
		pool.demands[probe.demandID].peers[peer.id] = archivePeerDemandPeer{
			evidence: archivePeerDemandProven,
			at:       time.Now(),
		}
		pool.mx.Unlock()
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	probe, release, err := pool.beginArchiveRequest(shard, 10_000)
	if err != nil {
		t.Fatalf("begin newcomer demand: %v", err)
	}
	releases = append(releases, release)
	candidate := testArchiveCandidate("starvation-guard-newcomer")
	admission := pool.admitArchiveOnlyPeer(candidate, archivePeerProbeResult{
		probe:          probe,
		evidence:       archivePeerEvidenceProven,
		at:             time.Now(),
		bytes:          archiveSliceProbeSize,
		elapsed:        time.Second,
		bytesPerSecond: 64 << 20,
	})
	if admission.admitted {
		closeArchiveOnlyPeer(admission.evicted)
		t.Fatal("newcomer displaced the sole provider of another active demand")
	}
	for _, peer := range peers {
		if !testArchivePoolHasPeer(pool, peer.id) {
			t.Fatalf("protected peer %s was evicted", peer.id.String())
		}
	}
}

func TestArchiveFullLeasedRosterDefersCandidateWithoutCachingNoBenefit(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peers := fillTestArchiveRoster(t, pool, shard, 4<<20)
	result := provenArchiveScoutResult(t, pool, shard, 8<<20)
	releases := make([]func(), 0, len(peers))
	for _, peer := range peers {
		release, ok := pool.acquire(peer)
		if !ok {
			t.Fatalf("lease roster peer %s", peer.addr)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	candidate := testArchiveCandidate("all-leased-newcomer")
	admission := pool.admitArchiveOnlyPeer(candidate, result)
	if admission.admitted || !admission.deferred {
		t.Fatalf("all-leased admission = %+v, want deferred", admission)
	}
	if pool.demandPeerBlocked(result.probe, candidate.id, time.Now()) {
		t.Fatal("deferred candidate was cached as no-benefit")
	}
}
