package p2p

import (
	"context"
	"crypto/sha256"
	"github.com/xssnick/gton/service/storage"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestRunConcurrentBlockDownloadsHedgesPastHungPeers(t *testing.T) {
	peers := []*overlayPeer{
		{id: "peer-1", addr: "peer-1"},
		{id: "peer-2", addr: "peer-2"},
		{id: "peer-3", addr: "peer-3"},
	}
	want := &DownloadedBlock{ID: testBlockID(-1, topShard, 42)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	startedAt := time.Now()
	got, err := runConcurrentBlockDownloads(ctx, peers, 2, func(*overlayPeer, error) {}, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		switch peer.id {
		case "peer-3":
			return *want, nil
		default:
			<-ctx.Done()
			return DownloadedBlock{}, ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("runConcurrentBlockDownloads: %v", err)
	}
	if !got.ID.Equals(&want.ID) {
		t.Fatalf("unexpected block: got=%s want=%s", got.BlockRef(), want.BlockRef())
	}

	elapsed := time.Since(startedAt)
	if elapsed >= downloadQueryTimeout {
		t.Fatalf("hedged download should complete before query timeout, took %v", elapsed)
	}
}

func TestProbeNextFullFromPeersFansOutAfterFirstNotAvailable(t *testing.T) {
	peers := []*overlayPeer{
		{id: "peer-1", addr: "peer-1"},
		{id: "peer-2", addr: "peer-2"},
		{id: "peer-3", addr: "peer-3"},
	}
	want := &DownloadedBlock{ID: testBlockID(-1, topShard, 42)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := make(chan string, len(peers))
	release := make(chan struct{})
	gotCh := make(chan *DownloadedBlock, 1)
	errCh := make(chan error, 1)

	go func() {
		got, err := probeNextFullFromPeers(ctx, peers, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
			started <- peer.id
			switch peer.id {
			case "peer-1":
				return DownloadedBlock{}, ErrBlockNotAvailable
			case "peer-2":
				<-release
				return DownloadedBlock{}, ErrBlockNotAvailable
			case "peer-3":
				<-release
				return *want, nil
			default:
				return DownloadedBlock{}, context.Canceled
			}
		}, nil)
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- got
	}()

	seen := map[string]bool{}
	for len(seen) < len(peers) {
		select {
		case peer := <-started:
			seen[peer] = true
		case <-time.After(time.Second):
			t.Fatalf("probe did not start all expected peers, seen=%v", seen)
		}
	}
	close(release)

	select {
	case err := <-errCh:
		t.Fatalf("probeNextFullFromPeers: %v", err)
	case got := <-gotCh:
		if !got.ID.Equals(&want.ID) {
			t.Fatalf("unexpected block: got=%s want=%s", got.BlockRef(), want.BlockRef())
		}
	case <-time.After(time.Second):
		t.Fatal("probe did not complete")
	}
}

func TestProbeNextFullFromPeersRampsFanoutAfterDelay(t *testing.T) {
	peers := []*overlayPeer{
		{id: "peer-1", addr: "peer-1"},
		{id: "peer-2", addr: "peer-2"},
		{id: "peer-3", addr: "peer-3"},
		{id: "peer-4", addr: "peer-4"},
	}
	want := &DownloadedBlock{ID: testBlockID(-1, topShard, 42)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := make(chan string, len(peers))
	gotCh := make(chan *DownloadedBlock, 1)
	errCh := make(chan error, 1)

	go func() {
		got, err := probeNextFullFromPeersStaged(ctx, peers, 2, 4, 50*time.Millisecond, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
			started <- peer.id
			if peer.id == "peer-4" {
				return *want, nil
			}
			<-ctx.Done()
			return DownloadedBlock{}, ctx.Err()
		}, nil)
		if err != nil {
			errCh <- err
			return
		}
		gotCh <- got
	}()

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case peer := <-started:
			seen[peer] = true
		case <-time.After(time.Second):
			t.Fatalf("probe did not start initial peers, seen=%v", seen)
		}
	}
	if seen["peer-3"] || seen["peer-4"] {
		t.Fatal("staged peer started before delay")
	}

	select {
	case peer := <-started:
		t.Fatalf("unexpected staged peer before delay: %s", peer)
	case <-time.After(20 * time.Millisecond):
	}

	select {
	case peer := <-started:
		if peer != "peer-3" {
			t.Fatalf("unexpected first ramp peer: %s", peer)
		}
	case <-time.After(time.Second):
		t.Fatal("probe did not start first ramp peer")
	}

	select {
	case peer := <-started:
		t.Fatalf("unexpected second ramp peer before next delay: %s", peer)
	case <-time.After(20 * time.Millisecond):
	}

	select {
	case err := <-errCh:
		t.Fatalf("probeNextFullFromPeersStaged: %v", err)
	case got := <-gotCh:
		if !got.ID.Equals(&want.ID) {
			t.Fatalf("unexpected block: got=%s want=%s", got.BlockRef(), want.BlockRef())
		}
	case <-time.After(time.Second):
		t.Fatal("probe did not complete after staged fanout")
	}
}

func TestProbeNextFullFromPeersStopsAfterEarlyFailures(t *testing.T) {
	peers := []*overlayPeer{
		{id: "peer-1", addr: "peer-1"},
		{id: "peer-2", addr: "peer-2"},
		{id: "peer-3", addr: "peer-3"},
		{id: "peer-4", addr: "peer-4"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	startedAt := time.Now()
	_, err := probeNextFullFromPeersWithOptions(ctx, peers, probeFullPeerOptions{
		peerLimit:         2,
		stagedPeerLimit:   4,
		stageDelay:        10 * time.Millisecond,
		earlyFailureCount: 3,
		earlyFailureDelay: 40 * time.Millisecond,
	}, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		if peer.id == "peer-4" {
			<-ctx.Done()
			return DownloadedBlock{}, ctx.Err()
		}
		return DownloadedBlock{}, ErrBlockNotAvailable
	}, nil)
	if err == nil {
		t.Fatal("expected early failure error")
	}
	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("early failure took too long: %s", elapsed)
	}
}

func TestProbeNextFullFromPeersStopsAfterSoftTimeout(t *testing.T) {
	peers := []*overlayPeer{
		{id: "peer-1", addr: "peer-1"},
		{id: "peer-2", addr: "peer-2"},
		{id: "peer-3", addr: "peer-3"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	startedAt := time.Now()
	_, err := probeNextFullFromPeersWithOptions(ctx, peers, probeFullPeerOptions{
		peerLimit:       2,
		stagedPeerLimit: 3,
		stageDelay:      10 * time.Millisecond,
		maxElapsed:      50 * time.Millisecond,
	}, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
		<-ctx.Done()
		return DownloadedBlock{}, ctx.Err()
	}, nil)
	if err == nil {
		t.Fatal("expected soft timeout error")
	}
	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("soft timeout took too long: %s", elapsed)
	}
}

func TestPreferDownloadPeerMovesSourceFirst(t *testing.T) {
	peers := []*overlayPeer{
		{id: "peer-a", addr: "peer-a"},
		{id: "peer-b", addr: "peer-b"},
		{id: "peer-c", addr: "peer-c"},
	}

	got := preferDownloadPeer(peers, "peer-b")
	if len(got) != len(peers) {
		t.Fatalf("unexpected peers count: %d", len(got))
	}
	if got[0].id != "peer-b" || got[1].id != "peer-a" || got[2].id != "peer-c" {
		t.Fatalf("unexpected preferred order: %q, %q, %q", got[0].id, got[1].id, got[2].id)
	}
	if peers[0].id != "peer-a" || peers[1].id != "peer-b" || peers[2].id != "peer-c" {
		t.Fatal("preferDownloadPeer mutated original slice order")
	}
}

func TestChainBlockDownloadSuccessPinsPeer(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	fast := &overlayPeer{id: "fast", addr: "fast", alive: true}
	other := &overlayPeer{id: "other", addr: "other", alive: true}

	sub.noteChainBlockDownloadSuccess(chain, fast, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)

	got := sub.currentChainBlockPeer(chain, []*overlayPeer{other, fast})
	if got != fast {
		t.Fatalf("expected fast peer to stay sticky, got %v", got)
	}
}

func TestChainBlockDownloadSuccessPinsSmallBlock(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(0, topShard, 77)
	peer := &overlayPeer{id: "fast-small", addr: "fast-small", alive: true}

	sub.noteChainBlockDownloadSuccess(chain, peer, &DownloadedBlock{
		ID:       testBlockID(0, topShard, 78),
		BlockBOC: make([]byte, 6<<10),
	}, time.Second)

	if got := sub.currentChainBlockPeer(chain, []*overlayPeer{peer}); got != peer {
		t.Fatalf("expected small successful block to pin peer, got %v", got)
	}
	if peer.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("small successful block should not mark peer slow")
	}
}

func TestLargeSlowBlockDownloadDoesNotStayPinned(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(0, topShard, 77)
	peer := &overlayPeer{id: "slow-large", addr: "slow-large", alive: true}

	sub.noteChainBlockDownloadSuccess(chain, peer, &DownloadedBlock{
		ID:       testBlockID(0, topShard, 78),
		BlockBOC: make([]byte, 1<<20),
	}, 30*time.Second)

	if got := sub.currentChainBlockPeer(chain, []*overlayPeer{peer}); got != nil {
		t.Fatalf("expected large slow block to clear pinned peer, got %v", got)
	}
}

func TestChainBlockUnavailableDoesNotSlowUntilConfirmed(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	fast := &overlayPeer{id: "fast", addr: "fast", alive: true}

	sub.noteChainBlockDownloadSuccess(chain, fast, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)
	sub.noteChainBlockDownloadFailure(chain, fast, ErrBlockNotAvailable)

	got := sub.currentChainBlockPeer(chain, []*overlayPeer{fast})
	if got != fast {
		t.Fatalf("expected unconfirmed not-available response to keep sticky peer, got %v", got)
	}
	if fast.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("unconfirmed not-available response should not slow peer")
	}
}

func TestChainBlockUnavailableSlowsAfterAnotherPeerSucceeds(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	stale := &overlayPeer{id: "stale", addr: "stale", alive: true}
	fresh := &overlayPeer{id: "fresh", addr: "fresh", alive: true}

	sub.noteChainBlockDownloadFailure(chain, stale, ErrBlockNotAvailable)
	sub.noteChainBlockDownloadSuccess(chain, fresh, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)

	if !stale.statsSnapshot().downloadSlowUntil.After(time.Now()) {
		t.Fatal("confirmed not-available response should temporarily slow peer")
	}
	if got := sub.currentChainBlockPeer(chain, []*overlayPeer{stale, fresh}); got != fresh {
		t.Fatalf("expected successful peer to become sticky, got %v", got)
	}
}

func TestLiveNextUnavailablePenalizesAndClearsPinnedMasterPeer(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	stale := &overlayPeer{id: "stale", addr: "stale", alive: true}
	fresh := &overlayPeer{id: "fresh", addr: "fresh", alive: true}

	sub.noteChainBlockDownloadSuccess(chain, stale, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)
	if got := sub.currentChainBlockPeer(chain, []*overlayPeer{fresh, stale}); got != stale {
		t.Fatalf("expected stale peer to be sticky before live miss, got %v", got)
	}

	sub.noteLiveNextDownloadFailure(chain, stale, ErrBlockNotAvailable)
	if got := sub.currentChainBlockPeer(chain, []*overlayPeer{stale}); got != nil {
		t.Fatalf("expected live miss to clear sticky peer, got %v", got)
	}

	prioritized := sub.prioritizeLiveNextPeers([]*overlayPeer{stale, fresh}, "", time.Now())
	if prioritized[0] != fresh || prioritized[1] != stale {
		t.Fatalf("unexpected live next priority after miss: %q, %q", prioritized[0].id, prioritized[1].id)
	}
}

func TestLiveNextUnavailableDoesNotClearBasechainSticky(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(0, topShard, 77)
	peer := &overlayPeer{id: "base-fast", addr: "base-fast", alive: true}

	sub.noteChainBlockDownloadSuccess(chain, peer, &DownloadedBlock{
		ID:       testBlockID(0, topShard, 78),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)
	sub.noteLiveNextDownloadFailure(chain, peer, ErrBlockNotAvailable)

	if got := sub.currentChainBlockPeer(chain, []*overlayPeer{peer}); got != peer {
		t.Fatalf("basechain not-available should keep sticky peer, got %v", got)
	}
}

func TestLiveNextPeerScorePrefersLowerLatency(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	slow := &overlayPeer{id: "slow", addr: "slow", alive: true}
	fast := &overlayPeer{id: "fast", addr: "fast", alive: true}
	block := &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}

	sub.noteLiveNextDownloadSuccess(chain, slow, block, 400*time.Millisecond, 0)
	sub.noteLiveNextDownloadSuccess(chain, fast, block, 50*time.Millisecond, 0)

	prioritized := sub.prioritizeLiveNextPeers([]*overlayPeer{slow, fast}, "", time.Now())
	if prioritized[0] != fast || prioritized[1] != slow {
		t.Fatalf("unexpected live next latency order: %q, %q", prioritized[0].id, prioritized[1].id)
	}
}

func TestLiveNextPeerScoreKeepsSlowPeerBehindHealthy(t *testing.T) {
	now := time.Now()
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	slow := &overlayPeer{id: "slow", addr: "slow", alive: true, downloadSlowUntil: now.Add(time.Minute)}
	healthy := &overlayPeer{id: "healthy", addr: "healthy", alive: true}
	block := &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}

	sub.noteLiveNextDownloadSuccess(chain, slow, block, 10*time.Millisecond, 0)
	slow.downloadSlowUntil = now.Add(time.Minute)

	prioritized := sub.prioritizeLiveNextPeers([]*overlayPeer{slow, healthy}, "", now)
	if prioritized[0] != healthy || prioritized[1] != slow {
		t.Fatalf("unexpected live next health order: %q, %q", prioritized[0].id, prioritized[1].id)
	}
}

func TestLiveNextPeerScoreBeatsGenericStickyPeer(t *testing.T) {
	now := int32(time.Now().Unix())
	sticky := &overlayPeer{id: "sticky", addr: "sticky", alive: true, overlay: &overlay.ADNLOverlayWrapper{}, announced: &overlay.Node{Version: now}}
	fast := &overlayPeer{id: "fast", addr: "fast", alive: true, overlay: &overlay.ADNLOverlayWrapper{}, announced: &overlay.Node{Version: now}}
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[string]*overlayPeer{
			sticky.id: sticky,
			fast.id:   fast,
		},
	}
	chain := testBlockID(-1, topShard, 41)
	block := &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}

	sub.noteChainBlockDownloadSuccess(chain, sticky, block, time.Millisecond)
	sub.noteLiveNextDownloadSuccess(chain, sticky, block, 300*time.Millisecond, 0)
	sub.noteLiveNextDownloadSuccess(chain, fast, block, 20*time.Millisecond, 0)

	candidates := sub.liveNextBlockDownloadCandidates(chain, "")
	if len(candidates) != 2 {
		t.Fatalf("unexpected candidate count: %d", len(candidates))
	}
	if candidates[0] != fast {
		t.Fatalf("fast live next peer should beat generic sticky peer, got %q", candidates[0].id)
	}
}

func TestLiveNextPreferredSourceBeatsStickyPeer(t *testing.T) {
	now := int32(time.Now().Unix())
	sticky := &overlayPeer{id: "sticky", addr: "sticky", alive: true, overlay: &overlay.ADNLOverlayWrapper{}, announced: &overlay.Node{Version: now}}
	preferred := &overlayPeer{id: "preferred", addr: "preferred", alive: true, overlay: &overlay.ADNLOverlayWrapper{}, announced: &overlay.Node{Version: now}}
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[string]*overlayPeer{
			sticky.id:    sticky,
			preferred.id: preferred,
		},
	}
	chain := testBlockID(-1, topShard, 41)

	sub.noteChainBlockDownloadSuccess(chain, sticky, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)

	candidates := sub.liveNextBlockDownloadCandidates(chain, preferred.id)
	if len(candidates) != 2 {
		t.Fatalf("unexpected candidate count: %d", len(candidates))
	}
	if candidates[0] != preferred {
		t.Fatalf("preferred source should lead live next probe, got %q", candidates[0].id)
	}
}

func TestLiveNextPeerScoreAccountsForBlockSize(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	smallFast := &overlayPeer{id: "small-fast", addr: "small-fast", alive: true}
	bigFast := &overlayPeer{id: "big-fast", addr: "big-fast", alive: true}

	sub.noteLiveNextDownloadSuccess(chain, smallFast, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 64<<10),
	}, 40*time.Millisecond, 0)
	sub.noteLiveNextDownloadSuccess(chain, bigFast, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 43),
		BlockBOC: make([]byte, 4<<20),
	}, 800*time.Millisecond, 0)

	prioritized := sub.prioritizeLiveNextPeers([]*overlayPeer{smallFast, bigFast}, "", time.Now())
	if prioritized[0] != bigFast || prioritized[1] != smallFast {
		t.Fatalf("unexpected live next size-aware order: %q, %q", prioritized[0].id, prioritized[1].id)
	}
}

func TestLiveNextPeerScoreRewardsAvailabilityAfterMisses(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	alwaysFast := &overlayPeer{id: "always-fast", addr: "always-fast", alive: true}
	earlyAvailable := &overlayPeer{id: "early-available", addr: "early-available", alive: true}

	sub.noteLiveNextDownloadSuccess(chain, alwaysFast, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 4<<20),
	}, 300*time.Millisecond, 0)
	sub.noteLiveNextDownloadSuccess(chain, earlyAvailable, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 43),
		BlockBOC: make([]byte, 1<<20),
	}, time.Second, 4)

	prioritized := sub.prioritizeLiveNextPeers([]*overlayPeer{alwaysFast, earlyAvailable}, "", time.Now())
	if prioritized[0] != earlyAvailable || prioritized[1] != alwaysFast {
		t.Fatalf("unexpected live next availability order: %q, %q", prioritized[0].id, prioritized[1].id)
	}
}

func TestChainBlockFailureClearsPinnedPeer(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	chain := testBlockID(-1, topShard, 41)
	fast := &overlayPeer{id: "fast", addr: "fast", alive: true}

	sub.noteChainBlockDownloadSuccess(chain, fast, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)
	sub.noteChainBlockDownloadFailure(chain, fast, context.DeadlineExceeded)

	if got := sub.currentChainBlockPeer(chain, []*overlayPeer{fast}); got != nil {
		t.Fatalf("expected failed sticky peer to be cleared, got %v", got)
	}
}

func TestChainBlockPinnedPeerIsPerChain(t *testing.T) {
	sub := &overlaySubscription{log: discardLogger()}
	master := testBlockID(-1, topShard, 41)
	base := testBlockID(0, topShard, 77)
	masterPeer := &overlayPeer{id: "master-fast", addr: "master-fast", alive: true}
	basePeer := &overlayPeer{id: "base-fast", addr: "base-fast", alive: true}

	sub.noteChainBlockDownloadSuccess(master, masterPeer, &DownloadedBlock{
		ID:       testBlockID(-1, topShard, 42),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)
	sub.noteChainBlockDownloadSuccess(base, basePeer, &DownloadedBlock{
		ID:       testBlockID(0, topShard, 78),
		BlockBOC: make([]byte, 1<<20),
	}, time.Millisecond)

	if got := sub.currentChainBlockPeer(master, []*overlayPeer{basePeer, masterPeer}); got != masterPeer {
		t.Fatalf("unexpected master sticky peer %v", got)
	}
	if got := sub.currentChainBlockPeer(base, []*overlayPeer{masterPeer, basePeer}); got != basePeer {
		t.Fatalf("unexpected base sticky peer %v", got)
	}
}

func TestBlockDownloadParallelismGivesFastPeerExclusiveTry(t *testing.T) {
	fast := &overlayPeer{
		id:            "fast",
		addr:          "fast",
		alive:         true,
		roundtrip:     downloadQueryHedgeDelay / 2,
		unreliability: 0,
	}
	slow := &overlayPeer{id: "slow", addr: "slow", alive: true}

	if got := blockDownloadParallelism([]*overlayPeer{fast, slow}); got != 1 {
		t.Fatalf("fast peer should get exclusive first try, got parallelism %d", got)
	}
}

func TestBlockDownloadParallelismHedgesUnknownOrSlowPeerImmediately(t *testing.T) {
	unknown := &overlayPeer{id: "unknown", addr: "unknown", alive: true}
	if got := blockDownloadParallelism([]*overlayPeer{unknown}); got != downloadQueryParallelism {
		t.Fatalf("unknown peer should use default parallelism, got %d", got)
	}

	slow := &overlayPeer{
		id:        "slow",
		addr:      "slow",
		alive:     true,
		roundtrip: downloadQueryHedgeDelay + time.Millisecond,
	}
	if got := blockDownloadParallelism([]*overlayPeer{slow}); got != downloadQueryParallelism {
		t.Fatalf("slow peer should use default parallelism, got %d", got)
	}
}

func TestRunConcurrentOverlayQueriesHedgesPastHungPeers(t *testing.T) {
	peers := []*overlayPeer{
		{id: "peer-1", addr: "peer-1"},
		{id: "peer-2", addr: "peer-2"},
		{id: "peer-3", addr: "peer-3"},
	}
	want := testBlockID(-1, topShard, 42)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	startedAt := time.Now()
	got, err := runConcurrentOverlayQueries(ctx, peers, 2, 20*time.Millisecond, func(ctx context.Context, peer *overlayPeer) (tl.Serializable, error) {
		switch peer.id {
		case "peer-3":
			return KeyBlocks{Blocks: []ton.BlockIDExt{want}}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("runConcurrentOverlayQueries: %v", err)
	}

	keyBlocks, ok := got.(KeyBlocks)
	if !ok {
		t.Fatalf("unexpected response type %T", got)
	}
	if len(keyBlocks.Blocks) != 1 || !keyBlocks.Blocks[0].Equals(&want) {
		t.Fatalf("unexpected key blocks: %#v", keyBlocks.Blocks)
	}

	elapsed := time.Since(startedAt)
	if elapsed >= time.Second {
		t.Fatalf("hedged query should complete before key block lookup timeout, took %v", elapsed)
	}
}

func TestRunConcurrentKeyBlockQueriesPrefersNonEmptyBatch(t *testing.T) {
	peers := []*overlayPeer{
		{id: "empty", addr: "empty"},
		{id: "with-key", addr: "with-key"},
	}
	want := testBlockID(-1, topShard, 42)
	semanticFailures := 0

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got, err := queryKeyBlocksFromPeers(ctx, peers, 2, 20*time.Millisecond, func(ctx context.Context, peer *overlayPeer) (tl.Serializable, error) {
		switch peer.id {
		case "empty":
			return KeyBlocks{}, nil
		case "with-key":
			time.Sleep(10 * time.Millisecond)
			return KeyBlocks{Blocks: []ton.BlockIDExt{want}}, nil
		default:
			return nil, context.Canceled
		}
	}, func(*overlayPeer) {
		semanticFailures++
	})
	if err != nil {
		t.Fatalf("queryKeyBlocksFromPeers: %v", err)
	}

	keyBlocks, ok := got.(KeyBlocks)
	if !ok {
		t.Fatalf("unexpected response type %T", got)
	}
	if len(keyBlocks.Blocks) != 1 || !keyBlocks.Blocks[0].Equals(&want) {
		t.Fatalf("unexpected key blocks: %#v", keyBlocks.Blocks)
	}
	if semanticFailures != 1 {
		t.Fatalf("expected one semantic failure, got %d", semanticFailures)
	}
}

func TestQueryCandidatesKeepNeighboursFirstButFallBackToOtherPeers(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}

	sub := &overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		peers: map[string]*overlayPeer{
			"peer-1": {id: "peer-1", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			"peer-2": {id: "peer-2", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
			"peer-3": {id: "peer-3", overlay: overlayWrapper, announced: &overlay.Node{Version: now}, alive: true},
		},
		neighbours: []string{"peer-1", "peer-2"},
	}

	got := sub.queryCandidates(0, 0)
	if len(got) != 3 {
		t.Fatalf("unexpected candidate count: got %d want 3", len(got))
	}
	front := map[string]struct{}{
		got[0].id: {},
		got[1].id: {},
	}
	if _, ok := front["peer-1"]; !ok {
		t.Fatalf("expected peer-1 in neighbour prefix, got %q and %q", got[0].id, got[1].id)
	}
	if _, ok := front["peer-2"]; !ok {
		t.Fatalf("expected peer-2 in neighbour prefix, got %q and %q", got[0].id, got[1].id)
	}
	if got[2].id != "peer-3" {
		t.Fatalf("expected fallback peer at the end, got %q", got[2].id)
	}
}

func TestHedgedQueryCandidatesReserveSlotsForFastPeers(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}

	peers := map[string]*overlayPeer{}
	neighbours := make([]string, 0, 16)
	for i := 0; i < 16; i++ {
		id := "neighbour-" + string(rune('a'+i))
		peers[id] = &overlayPeer{
			id:           id,
			overlay:      overlayWrapper,
			announced:    &overlay.Node{Version: now},
			alive:        true,
			roundtrip:    500 * time.Millisecond,
			versionMajor: shardchainProtoVersionMajor,
			versionMinor: shardchainProtoVersionMinor,
		}
		neighbours = append(neighbours, id)
	}
	peers["fast"] = &overlayPeer{
		id:           "fast",
		overlay:      overlayWrapper,
		announced:    &overlay.Node{Version: now},
		alive:        true,
		roundtrip:    10 * time.Millisecond,
		versionMajor: shardchainProtoVersionMajor,
		versionMinor: shardchainProtoVersionMinor,
	}

	sub := &overlaySubscription{
		log:        discardLogger(),
		spec:       overlaySpec{ProtoVersionMajor: shardchainProtoVersionMajor, ProtoVersionMinor: shardchainProtoVersionMinor},
		peers:      peers,
		neighbours: neighbours,
	}

	got := sub.hedgedQueryCandidates(0, 0, 4)
	if len(got) != 4 {
		t.Fatalf("unexpected candidate count %d", len(got))
	}
	if !hasPeerID(got, "fast") {
		t.Fatalf("expected fast non-neighbour in bounded candidate window, got %#v", got)
	}
	for _, peer := range got[:2] {
		if _, ok := peers[peer.id]; !ok || peer.id == "fast" {
			t.Fatalf("expected neighbour prefix, got %q", peer.id)
		}
	}
}

func TestRunConcurrentKeyBlockQueriesSkipsErrorResponse(t *testing.T) {
	peers := []*overlayPeer{
		{id: "error", addr: "error"},
		{id: "with-key", addr: "with-key"},
	}
	want := testBlockID(-1, topShard, 42)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	got, err := queryKeyBlocksFromPeers(ctx, peers, 2, 20*time.Millisecond, func(ctx context.Context, peer *overlayPeer) (tl.Serializable, error) {
		switch peer.id {
		case "error":
			return KeyBlocks{Error: true}, nil
		case "with-key":
			return KeyBlocks{Blocks: []ton.BlockIDExt{want}}, nil
		default:
			return nil, context.Canceled
		}
	}, nil)
	if err != nil {
		t.Fatalf("queryKeyBlocksFromPeers: %v", err)
	}

	keyBlocks, ok := got.(KeyBlocks)
	if !ok {
		t.Fatalf("unexpected response type %T", got)
	}
	if len(keyBlocks.Blocks) != 1 || !keyBlocks.Blocks[0].Equals(&want) {
		t.Fatalf("unexpected key blocks: %#v", keyBlocks.Blocks)
	}
}

func hasPeerID(peers []*overlayPeer, id string) bool {
	for _, peer := range peers {
		if peer.id == id {
			return true
		}
	}
	return false
}

func TestAliveNeighbourPeersUseOnlyAliveNeighbours(t *testing.T) {
	now := int32(time.Now().Unix())
	overlayWrapper := &overlay.ADNLOverlayWrapper{}

	sub := &overlaySubscription{
		log: discardLogger(),
		spec: overlaySpec{
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		peers: map[string]*overlayPeer{
			"alive-neighbour": {
				id:            "alive-neighbour",
				overlay:       overlayWrapper,
				announced:     &overlay.Node{Version: now},
				alive:         true,
				versionMajor:  masterchainProtoVersionMajor,
				versionMinor:  masterchainProtoVersionMinor,
				lastReceiveAt: time.Now(),
			},
			"dead-neighbour": {
				id:            "dead-neighbour",
				overlay:       overlayWrapper,
				announced:     &overlay.Node{Version: now},
				alive:         false,
				versionMajor:  masterchainProtoVersionMajor,
				versionMinor:  masterchainProtoVersionMinor,
				lastReceiveAt: time.Now(),
			},
			"alive-non-neighbour": {
				id:            "alive-non-neighbour",
				overlay:       overlayWrapper,
				announced:     &overlay.Node{Version: now},
				alive:         true,
				versionMajor:  masterchainProtoVersionMajor,
				versionMinor:  masterchainProtoVersionMinor,
				lastReceiveAt: time.Now(),
			},
		},
		neighbours: []string{"dead-neighbour", "alive-neighbour"},
	}

	got := sub.aliveNeighbourPeers(masterchainProtoVersionMajor, masterchainProtoVersionMinor)
	if len(got) != 1 {
		t.Fatalf("unexpected alive neighbour count: got %d want 1", len(got))
	}
	if got[0].id != "alive-neighbour" {
		t.Fatalf("unexpected alive neighbour %q", got[0].id)
	}
}

func TestSortPeersByPreferenceUsesRoundtripAfterReliabilityAndVersion(t *testing.T) {
	peers := []*overlayPeer{
		{id: "unknown", versionMajor: shardchainProtoVersionMajor, versionMinor: shardchainProtoVersionMinor},
		{id: "slow", versionMajor: shardchainProtoVersionMajor, versionMinor: shardchainProtoVersionMinor, roundtrip: 500 * time.Millisecond},
		{id: "fast", versionMajor: shardchainProtoVersionMajor, versionMinor: shardchainProtoVersionMinor, roundtrip: 50 * time.Millisecond},
	}

	sortPeersByPreference(peers)

	if peers[0].id != "fast" || peers[1].id != "slow" || peers[2].id != "unknown" {
		t.Fatalf("unexpected peer order: %q, %q, %q", peers[0].id, peers[1].id, peers[2].id)
	}
}

func TestDownloadBlockFullUsesLocalCacheBeforeOverlay(t *testing.T) {
	store := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: store,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	blockCell := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	blockData := blockCell.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	rootHash := blockCell.HashKey()
	fileHash := sha256.Sum256(blockData)
	block := testBlockID(-1, topShard, 100)
	block.RootHash = append([]byte(nil), rootHash[:]...)
	block.FileHash = append([]byte(nil), fileHash[:]...)

	proofData := testBlockProofCell(t, block, testProofSignatureSet()).ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	if err = store.SaveBlockFull(&storage.ServedBlockFull{
		ID:     block,
		Block:  blockData,
		Proof:  proofData,
		IsLink: false,
	}); err != nil {
		t.Fatalf("save cached block: %v", err)
	}

	got, err := node.DownloadBlockFull(context.Background(), block)
	if err != nil {
		t.Fatalf("download cached block: %v", err)
	}
	if !got.ID.Equals(&block) || got.Kind != "local full block cache" {
		t.Fatalf("unexpected cached block: %#v", got)
	}
}
