package p2p

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/ton"
)

func TestArchiveSessionClosePreventsPoolRecreation(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	runCtx, cancel := context.WithCancel(context.Background())
	sub.node.runCtx = runCtx
	defer cancel()
	session := sub.node.BeginArchiveSession()

	pool, err := session.archivePeerPool(sub)
	if err != nil {
		t.Fatalf("create archive pool: %v", err)
	}
	session.Close()

	if !pool.isClosed() {
		t.Fatal("archive pool survived session close")
	}
	got, err := session.archivePeerPool(sub)
	if got != nil {
		t.Fatal("closed archive session returned a peer pool")
	}
	if !errors.Is(err, errArchiveSessionClosed) {
		t.Fatalf("archive peer pool after close error = %v, want %v", err, errArchiveSessionClosed)
	}
	if release := session.tryAcquireArchiveHedge(); release != nil {
		release()
		t.Fatal("closed archive session acquired a hedge slot")
	}

	session.mx.Lock()
	poolCount := len(session.pools)
	session.mx.Unlock()
	if poolCount != 0 {
		t.Fatalf("closed archive session retained %d peer pools", poolCount)
	}
}

func TestArchiveSessionClosedCallsDoNotCreateSubscription(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, 32)
	session := node.BeginArchiveSession()
	session.Close()

	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	if _, err := session.DownloadArchive(context.Background(), 1, shard, ArchiveDownloadOptions{}); !errors.Is(err, errArchiveSessionClosed) {
		t.Fatalf("download archive after close error = %v, want %v", err, errArchiveSessionClosed)
	}
	block := ton.BlockIDExt{Workchain: shard.Workchain, Shard: shard.Shard}
	if _, err := session.DownloadZeroState(context.Background(), block); !errors.Is(err, errArchiveSessionClosed) {
		t.Fatalf("download zero state after close error = %v, want %v", err, errArchiveSessionClosed)
	}
	if session.RejectArchivePeer(shard, "peer", archivePeerRejectDownloadFailed) {
		t.Fatal("closed archive session accepted peer rejection")
	}

	node.subscriptionsMx.Lock()
	subscriptionCount := len(node.subscriptions)
	node.subscriptionsMx.Unlock()
	if subscriptionCount != 0 {
		t.Fatalf("closed archive session created %d overlay subscriptions", subscriptionCount)
	}
}

func TestArchiveSessionSharesRosterWithShardLocalHealth(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		spec:  overlaySpec{Name: "masterchain", Workchain: -1, Shard: topShard},
		peers: map[PeerID]*overlayPeer{},
	})
	runCtx, cancel := context.WithCancel(context.Background())
	sub.node.runCtx = runCtx
	defer cancel()
	session := sub.node.BeginArchiveSession()
	defer session.Close()

	pool, err := session.archivePeerPool(sub)
	if err != nil {
		t.Fatalf("create archive pool: %v", err)
	}
	samePool, err := session.archivePeerPool(sub)
	if err != nil {
		t.Fatalf("reuse archive pool: %v", err)
	}
	if pool != samePool {
		t.Fatal("one master subscription created more than one archive roster")
	}
	if !pool.shouldRunContinuousDiscovery() {
		t.Fatal("session-owned archive pool did not keep calm discovery active between downloads")
	}

	peer := testArchiveCandidate("same-archive-peer")
	addTestArchiveOnlyPeer(pool, peer)
	master := archive.ShardID{Workchain: -1, Shard: topShard}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	pool.noteArchiveDownload(master, peer, 2<<20, time.Second)
	pool.markSuccess(master, peer)

	for range archivePeerErrorRotateThreshold {
		pool.noteFailure(shard, peer, archivePeerRejectDownloadFailed)
	}
	if rotated := pool.rotateUseless(shard); rotated != 1 {
		t.Fatalf("shard-local rotation count = %d, want 1", rotated)
	}
	if !testArchivePoolHasPeer(pool, peer.id) {
		t.Fatal("shard-local failure removed a peer proven useful for master archives")
	}
	if !pool.coolingDown(shard, peer) {
		t.Fatal("shard-local failure did not cool down the shard route")
	}
	if pool.coolingDown(master, peer) {
		t.Fatal("shard cooldown leaked into master archive health")
	}
}

func TestArchiveSessionRejectsOnlyMatchingShard(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	runCtx, cancel := context.WithCancel(context.Background())
	sub.node.runCtx = runCtx
	defer cancel()
	session := sub.node.BeginArchiveSession()
	defer session.Close()
	pool, err := session.archivePeerPool(sub)
	if err != nil {
		t.Fatalf("create archive pool: %v", err)
	}

	peer := testArchiveCandidate("lane-peer")
	addTestArchiveOnlyPeer(pool, peer)
	master := archive.ShardID{Workchain: -1, Shard: topShard}
	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	session.selectArchivePeerFromPool(shard, peer, pool)

	if !session.RejectArchivePeer(shard, peer.addr, ArchivePeerRejectImportFailed) {
		t.Fatal("matching shard peer was not rejected")
	}
	if !pool.coolingDown(shard, peer) {
		t.Fatal("shard peer did not enter cooldown")
	}
	if pool.coolingDown(master, peer) {
		t.Fatal("shard rejection leaked into master archive health")
	}
}

func TestArchiveSelectionSurvivesProbeInDifferentPool(t *testing.T) {
	node := newTestNode(t)
	firstSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	secondSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	firstPool := testArchivePool(t, firstSub)
	secondPool := testArchivePool(t, secondSub)
	peer := testArchiveCandidate("selected-historical-route")
	if !addTestArchiveOnlyPeer(firstPool, peer) {
		t.Fatal("add selected historical peer")
	}

	session := node.BeginArchiveSession()
	defer session.Close()
	shard := archive.ShardID{Workchain: 0, Shard: int64(0x3800000000000000)}
	session.selectArchivePeerFromPool(shard, peer, firstPool)

	if candidates := secondPool.downloadCandidates(session, shard, nil); len(candidates) != 0 {
		t.Fatalf("different historical pool returned %d candidates, want 0", len(candidates))
	}
	if selected := session.selectedArchivePeerID(shard); selected != peer.id {
		t.Fatalf("different historical pool cleared selected peer: got %s want %s", selected.String(), peer.id.String())
	}
	if selected := firstPool.selectedPeer(session, shard); selected != peer {
		t.Fatal("original historical pool lost selected peer")
	}

	firstPool.Close()
	secondPool.downloadCandidates(session, shard, nil)
	if selected := session.selectedArchivePeerID(shard); !selected.IsZero() {
		t.Fatalf("closed historical pool retained selected peer: %s", selected.String())
	}
}

func TestArchiveSessionReplacesClosedPool(t *testing.T) {
	sub := testOverlaySubscription(&overlaySubscription{
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	session := sub.node.BeginArchiveSession()
	defer session.Close()

	closedPool, err := session.archivePeerPool(sub)
	if err != nil {
		t.Fatalf("create archive peer pool: %v", err)
	}
	closedPool.Close()

	replacement, err := session.archivePeerPool(sub)
	if err != nil {
		t.Fatalf("replace closed archive peer pool: %v", err)
	}
	if replacement == closedPool {
		t.Fatal("closed archive peer pool was reused")
	}
	if replacement.isClosed() {
		t.Fatal("replacement archive peer pool is closed")
	}
}

func TestArchiveSessionReapsInactiveIdlePool(t *testing.T) {
	node := &Node{}
	oldSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	activeSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	session := node.BeginArchiveSession()
	defer session.Close()

	oldPool, err := session.archivePeerPool(oldSub)
	if err != nil {
		t.Fatalf("create old archive peer pool: %v", err)
	}
	oldSub.setActive(false, time.Now().Add(time.Minute))
	oldPool.mx.Lock()
	oldPool.lastUsedAt = time.Now().Add(-archivePoolInactiveGrace - time.Second)
	oldPool.mx.Unlock()

	if _, err = session.archivePeerPool(activeSub); err != nil {
		t.Fatalf("create active archive peer pool: %v", err)
	}
	if !oldPool.isClosed() {
		t.Fatal("inactive idle archive peer pool was not closed")
	}

	session.mx.Lock()
	_, retained := session.pools[oldSub]
	session.mx.Unlock()
	if retained {
		t.Fatal("inactive idle archive peer pool remained in session")
	}
}

func TestArchiveSessionKeepsLeasedInactivePoolUntilRelease(t *testing.T) {
	node := &Node{}
	oldSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	activeSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	session := node.BeginArchiveSession()
	defer session.Close()

	oldPool, err := session.archivePeerPool(oldSub)
	if err != nil {
		t.Fatalf("create old archive peer pool: %v", err)
	}
	peer := testArchiveCandidate("leased-inactive")
	if !addTestArchiveOnlyPeer(oldPool, peer) {
		t.Fatal("add leased peer to old archive pool")
	}
	release, ok := oldPool.acquire(peer)
	if !ok {
		t.Fatal("lease peer from old archive pool")
	}
	oldSub.setActive(false, time.Now().Add(time.Minute))
	oldPool.mx.Lock()
	oldPool.lastUsedAt = time.Now().Add(-archivePoolInactiveGrace - time.Second)
	oldPool.mx.Unlock()

	if _, err = session.archivePeerPool(activeSub); err != nil {
		t.Fatalf("create active archive peer pool: %v", err)
	}
	if oldPool.isClosed() {
		t.Fatal("leased inactive archive peer pool was closed")
	}

	release()
	if _, err = session.archivePeerPool(activeSub); err != nil {
		t.Fatalf("reuse active archive peer pool: %v", err)
	}
	if !oldPool.isClosed() {
		t.Fatal("inactive archive peer pool was not closed after its lease ended")
	}
}

func TestArchiveSessionKeepsInactivePoolDuringActiveUse(t *testing.T) {
	node := &Node{}
	oldSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	activeSub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	session := node.BeginArchiveSession()
	defer session.Close()

	oldPool, releaseUse, err := session.useArchivePeerPool(oldSub)
	if err != nil {
		t.Fatalf("begin old archive peer pool use: %v", err)
	}
	oldSub.setActive(false, time.Now().Add(time.Minute))
	oldPool.mx.Lock()
	oldPool.lastUsedAt = time.Now().Add(-archivePoolInactiveGrace - time.Second)
	oldPool.mx.Unlock()

	if _, err = session.archivePeerPool(activeSub); err != nil {
		t.Fatalf("create active archive peer pool: %v", err)
	}
	if oldPool.isClosed() {
		t.Fatal("inactive archive peer pool was closed during active use")
	}

	releaseUse()
	oldPool.mx.Lock()
	oldPool.lastUsedAt = time.Now().Add(-archivePoolInactiveGrace - time.Second)
	oldPool.mx.Unlock()
	if _, err = session.archivePeerPool(activeSub); err != nil {
		t.Fatalf("reuse active archive peer pool: %v", err)
	}
	if !oldPool.isClosed() {
		t.Fatal("inactive archive peer pool was not closed after active use ended")
	}
}

func TestArchiveSessionCloseCancelsAndWaitsForActiveOperation(t *testing.T) {
	node := &Node{runCtx: context.Background()}
	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	session := node.BeginArchiveSession()
	opCtx, finishOperation, err := session.beginOperation(context.Background())
	if err != nil {
		t.Fatalf("begin archive session operation: %v", err)
	}
	finishOperation = sync.OnceFunc(finishOperation)
	defer finishOperation()
	pool, releasePool, err := session.useArchivePeerPool(sub)
	if err != nil {
		t.Fatalf("begin archive peer pool use: %v", err)
	}
	releasePool = sync.OnceFunc(releasePool)
	defer releasePool()
	peer := testArchiveCandidate("active-close")
	if !addTestArchiveOnlyPeer(pool, peer) {
		t.Fatal("add active archive peer")
	}
	releasePeer, ok := pool.acquire(peer)
	if !ok {
		t.Fatal("lease active archive peer")
	}
	releasePeer = sync.OnceFunc(releasePeer)
	defer releasePeer()

	firstCloseDone := make(chan struct{})
	go func() {
		session.Close()
		close(firstCloseDone)
	}()

	select {
	case <-opCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("session Close did not cancel the active operation")
	}
	select {
	case <-firstCloseDone:
		t.Fatal("session Close returned before the active operation ended")
	case <-time.After(50 * time.Millisecond):
	}
	if pool.isClosed() {
		t.Fatal("session Close closed the pool while an archive peer was leased")
	}
	secondCloseStarted := make(chan struct{})
	secondCloseDone := make(chan struct{})
	go func() {
		close(secondCloseStarted)
		session.Close()
		close(secondCloseDone)
	}()
	<-secondCloseStarted
	select {
	case <-secondCloseDone:
		t.Fatal("concurrent session Close returned before teardown completed")
	case <-time.After(50 * time.Millisecond):
	}

	releasePeer()
	releasePool()
	finishOperation()
	select {
	case <-firstCloseDone:
	case <-time.After(time.Second):
		t.Fatal("session Close did not finish after the active operation ended")
	}
	select {
	case <-secondCloseDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent session Close did not join completed teardown")
	}
	if !pool.isClosed() {
		t.Fatal("archive peer pool survived completed session Close")
	}
}

func TestArchiveSessionCloseRacingPoolCreationDoesNotLeakPool(t *testing.T) {
	for i := 0; i < 32; i++ {
		sub := testOverlaySubscription(&overlaySubscription{
			log:   discardLogger(),
			peers: map[PeerID]*overlayPeer{},
		})
		session := sub.node.BeginArchiveSession()
		start := make(chan struct{})

		var pool *archivePeerPool
		var poolErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			pool, poolErr = session.archivePeerPool(sub)
		}()
		go func() {
			defer wg.Done()
			<-start
			session.Close()
		}()

		close(start)
		wg.Wait()

		if poolErr != nil && !errors.Is(poolErr, errArchiveSessionClosed) {
			t.Fatalf("iteration %d: archive peer pool error = %v", i, poolErr)
		}
		if pool != nil && !pool.isClosed() {
			t.Fatalf("iteration %d: pool created during Close remained open", i)
		}

		session.mx.Lock()
		poolCount := len(session.pools)
		closed := session.closed
		session.mx.Unlock()
		if !closed || poolCount != 0 {
			t.Fatalf("iteration %d: closed=%v pools=%d, want true/0", i, closed, poolCount)
		}
	}
}
