package p2p

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
)

var fastSyncQueryLimiterSink bool

func TestFastSyncQueryCost(t *testing.T) {
	tests := []struct {
		name  string
		query tl.Serializable
		class inboundQueryClass
		cost  uint64
	}{
		{"archive", GetArchiveSlice{MaxSize: 2<<20 + 1}, inboundQueryHeavy, 2},
		{"persistent state", DownloadPersistentStateSliceV2{MaxSize: 2<<20 + 1}, inboundQueryHeavy, 2},
		{"zero state", DownloadZeroState{}, inboundQueryHeavy, 8},
		{"block", tonnodeapi.DownloadBlock{}, inboundQueryMedium, 1},
		{"block full", tonnodeapi.DownloadBlockFull{}, inboundQueryMedium, 1},
		{"next block full", DownloadNextBlockFull{}, inboundQueryMedium, 1},
		{"next blocks full", DownloadNextBlocksFull{}, inboundQueryMedium, 1},
		{"block proof", DownloadBlockProof{}, inboundQueryMedium, 1},
		{"block proof link", DownloadBlockProofLink{}, inboundQueryMedium, 1},
		{"key block proof", DownloadKeyBlockProof{}, inboundQueryMedium, 1},
		{"key block proof link", DownloadKeyBlockProofLink{}, inboundQueryMedium, 1},
		{"out queue proof", GetOutMsgQueueProof{}, inboundQueryMedium, 1},
		{"next block description", GetNextBlockDescription{}, inboundQuerySmall, 1},
		{"prepare block proof", PrepareBlockProof{}, inboundQuerySmall, 1},
		{"prepare key block proof", PrepareKeyBlockProof{}, inboundQuerySmall, 1},
		{"prepare block", PrepareBlock{}, inboundQuerySmall, 1},
		{"prepare zero state", PrepareZeroState{}, inboundQuerySmall, 1},
		{"next key blocks", GetNextKeyBlockIDs{}, inboundQuerySmall, 1},
		{"archive info", GetArchiveInfo{}, inboundQuerySmall, 1},
		{"shard archive info", GetShardArchiveInfo{}, inboundQuerySmall, 1},
		{"prepare persistent state", PreparePersistentState{}, inboundQuerySmall, 1},
		{"persistent state size", GetPersistentStateSizeV2{}, inboundQuerySmall, 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class, cost := inboundQueryCost(test.query)
			if class != test.class || cost != test.cost {
				t.Fatalf(
					"cost = (%d, %d), want (%d, %d)",
					class,
					cost,
					test.class,
					test.cost,
				)
			}
		})
	}

	if class, _ := inboundQueryCost(&PrepareBlock{}); class != inboundQueryUnlimited {
		t.Fatal("pointer query was classified despite value-only TL decoding")
	}
	if class, _ := inboundQueryCost(overlay.Ping{}); class != inboundQueryUnlimited {
		t.Fatal("uncategorized query was limited")
	}
}

func TestFastSyncHeavyQueryCost(t *testing.T) {
	tests := []struct {
		size uint64
		cost uint64
	}{
		{0, 1},
		{1, 1},
		{2 << 20, 1},
		{2<<20 + 1, 2},
		{math.MaxUint64, 1 << 43},
	}
	for _, test := range tests {
		if cost := inboundHeavyQueryCost(test.size); cost != test.cost {
			t.Fatalf("cost(%d) = %d, want %d", test.size, cost, test.cost)
		}
	}
}

func TestFastSyncQueryLimiterCategoryAndGlobalLimits(t *testing.T) {
	now := time.Unix(1_800_100_000, 0)

	tests := []struct {
		name  string
		query tl.Serializable
		limit int
	}{
		{"heavy", GetArchiveSlice{}, int(inboundQueryHeavyLimit)},
		{"medium", tonnodeapi.DownloadBlock{}, int(inboundQueryMediumLimit)},
		{"small", PrepareBlock{}, int(inboundQuerySmallLimit)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter := newInboundQueryLimiter()
			for i := 0; i < test.limit; i++ {
				if !limiter.Allow(test.query, now) {
					t.Fatalf("request %d was rejected", i+1)
				}
			}
			if limiter.Allow(test.query, now) {
				t.Fatalf("request %d was accepted", test.limit+1)
			}
		})
	}

	limiter := newInboundQueryLimiter()
	for i := 0; i < int(inboundQueryMediumLimit); i++ {
		if !limiter.Allow(tonnodeapi.DownloadBlock{}, now) {
			t.Fatalf("medium request %d was rejected", i+1)
		}
	}
	for i := 0; i < int(inboundQueryGlobalLimit-inboundQueryMediumLimit); i++ {
		if !limiter.Allow(GetArchiveSlice{}, now) {
			t.Fatalf("heavy request %d was rejected by shared global limit", i+1)
		}
	}
	if limiter.Allow(GetArchiveSlice{}, now) {
		t.Fatal("request beyond shared global limit was accepted")
	}
	if !limiter.Allow(PrepareBlock{}, now) {
		t.Fatal("small request was charged to the global window")
	}
	if !limiter.Allow(overlay.Ping{}, now) {
		t.Fatal("uncategorized request was limited")
	}
}

func TestFastSyncQueryLimiterWindowBoundaryAndHostileSize(t *testing.T) {
	now := time.Unix(1_800_100_100, 0)
	limiter := newInboundQueryLimiter()
	for i := 0; i < int(inboundQuerySmallLimit); i++ {
		if !limiter.Allow(PrepareBlock{}, now) {
			t.Fatalf("request %d was rejected", i+1)
		}
	}
	if limiter.Allow(
		PrepareBlock{},
		now.Add(inboundQueryRateWindowDuration),
	) {
		t.Fatal("request at the exact window boundary was accepted")
	}
	if !limiter.Allow(
		PrepareBlock{},
		now.Add(inboundQueryRateWindowDuration+time.Nanosecond),
	) {
		t.Fatal("request beyond the window boundary was rejected")
	}

	limiter = newInboundQueryLimiter()
	if limiter.Allow(
		DownloadPersistentStateSliceV2{MaxSize: math.MaxInt64},
		now,
	) {
		t.Fatal("hostile max_size was accepted")
	}
	if !limiter.Allow(GetArchiveSlice{}, now) {
		t.Fatal("rejected hostile size consumed limiter capacity")
	}
}

func TestFastSyncQueryLimiterStorageRemainsBounded(t *testing.T) {
	limiter := newInboundQueryLimiter()
	now := time.Unix(1_800_100_150, 0)

	for range 10_000 {
		if !limiter.Allow(PrepareBlock{}, now) {
			t.Fatal("spaced request was rejected")
		}
		now = now.Add(2 * inboundQueryRateWindowDuration)
	}

	if limiter.small.count > len(limiter.small.entries) ||
		len(limiter.small.entries) != int(inboundQuerySmallLimit) {
		t.Fatalf(
			"small window storage = count %d, capacity %d",
			limiter.small.count,
			len(limiter.small.entries),
		)
	}
}

func TestFastSyncQUICQueryRateLimitRunsBeforeConcurrencyGate(t *testing.T) {
	node := newTestNode(t)
	peerID := testPeerID("fast-sync-rate-limit-peer")
	overlayID := testPeerID("fast-sync-rate-limit-overlay").Bytes()
	membership := newFastSyncMembership(
		fastSyncMembershipTestRoster(nil, []PeerID{peerID}),
		0,
	)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "fast-sync-rate-limit",
			Kind:    overlayKindFastSync,
			ShortID: overlayID,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
		fastSync: &fastSyncOverlayRuntime{
			membership: membership,
		},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	now := time.Now()
	for i := 0; i < int(inboundQuerySmallLimit); i++ {
		if !sub.inboundQueryLimiter().Allow(PrepareBlock{}, now) {
			t.Fatalf("fill request %d was rejected", i+1)
		}
	}
	for range inboundQUICQueryParallelism {
		node.quicQuerySlots <- struct{}{}
	}
	t.Cleanup(func() {
		for len(node.quicQuerySlots) > 0 {
			<-node.quicQuerySlots
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := node.handleQUICQuery(
		ctx,
		&authenticatedQUICPeer{id: peerID},
		testQUICOverlayQueryPayload(t, overlayID, PrepareBlock{}),
	)
	if !errors.Is(err, errInboundQueryRateLimited) {
		t.Fatalf("query error = %v, want rate limit", err)
	}
}

func TestFastSyncInboundQueryLimitIsSharedAcrossTransports(t *testing.T) {
	node := newTestNode(t)
	peerID := testPeerID("fast-sync-shared-limit-peer")
	overlayID := testPeerID("fast-sync-shared-limit-overlay").Bytes()
	peer := &overlayPeer{
		id:   peerID,
		addr: "127.0.0.1:17555",
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "fast-sync-shared-limit",
			Kind:    overlayKindFastSync,
			ShortID: overlayID,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peerID: peer},
		fastSync: &fastSyncOverlayRuntime{
			membership: newFastSyncMembership(
				fastSyncMembershipTestRoster(nil, []PeerID{peerID}),
				0,
			),
		},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	req := GetArchiveSlice{
		ArchiveID: 1,
		MaxSize:   maxPeerSliceRequestSize,
	}
	for i := 0; i < 4; i++ {
		err := sub.answerADNLQuery(peer, &adnl.MessageQuery{Data: req})
		if err == nil {
			t.Fatalf("ADNL request %d did not reach storage", i+1)
		}
		if errors.Is(err, errInboundQueryRateLimited) {
			t.Fatalf("ADNL request %d was rate limited", i+1)
		}
	}
	for i := 0; i < 4; i++ {
		err := sub.answerRLDPQuery(peer, nil, &rldp.Query{Data: req})
		if err == nil {
			t.Fatalf("RLDP request %d did not reach storage", i+1)
		}
		if errors.Is(err, errInboundQueryRateLimited) {
			t.Fatalf("RLDP request %d was rate limited", i+1)
		}
	}

	_, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: peerID},
		testQUICOverlayQueryPayload(t, overlayID, req),
	)
	if !errors.Is(err, errInboundQueryRateLimited) {
		t.Fatalf("QUIC query error = %v, want rate limit", err)
	}

	publicOverlayID := testPeerID("public-shared-limit-overlay").Bytes()
	publicPeer := &overlayPeer{
		id:   peerID,
		addr: "127.0.0.1:17556",
	}
	publicSub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "public-shared-limit",
			Kind:    overlayKindPublicShard,
			ShortID: publicOverlayID,
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{peerID: publicPeer},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(publicOverlayID)] = publicSub
	node.subscriptionsMx.Unlock()

	err = publicSub.answerADNLQuery(
		publicPeer,
		&adnl.MessageQuery{Data: req},
	)
	if err == nil || errors.Is(err, errInboundQueryRateLimited) {
		t.Fatalf("public ADNL query error = %v, want storage error", err)
	}
	err = publicSub.answerRLDPQuery(
		publicPeer,
		nil,
		&rldp.Query{Data: req},
	)
	if err == nil || errors.Is(err, errInboundQueryRateLimited) {
		t.Fatalf("public RLDP query error = %v, want storage error", err)
	}
	_, err = node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: peerID},
		testQUICOverlayQueryPayload(t, publicOverlayID, req),
	)
	if err == nil || errors.Is(err, errInboundQueryRateLimited) {
		t.Fatalf("public QUIC query error = %v, want storage error", err)
	}
}

func BenchmarkFastSyncQueryLimiterAllow(b *testing.B) {
	limiter := newInboundQueryLimiter()
	now := time.Unix(1_800_100_200, 0)

	b.ReportAllocs()
	for b.Loop() {
		now = now.Add(2 * inboundQueryRateWindowDuration)
		fastSyncQueryLimiterSink = limiter.Allow(PrepareBlock{}, now)
	}
}

// TestInboundQueryLimitAppliesToEveryOverlayKind pins the reason this limiter
// stopped being FastSync-only: the reference node runs the same limiter on its
// public, fast-sync and custom overlays, and the public one is the overlay any
// stranger can reach, so it needs the cap most.
func TestInboundQueryLimitAppliesToEveryOverlayKind(t *testing.T) {
	node := newTestNode(t)

	for _, test := range []struct {
		name string
		kind overlayKind
	}{
		{"public", overlayKindPublicShard},
		{"customFixed", overlayKindCustomFixed},
		{"fastSync", overlayKindFastSync},
	} {
		t.Run(test.name, func(t *testing.T) {
			sub := &overlaySubscription{node: node, spec: overlaySpec{Kind: test.kind}}

			now := time.Now()
			for i := range int(inboundQuerySmallLimit) {
				if err := sub.admitInboundQuery(PrepareBlock{}, now); err != nil {
					t.Fatalf("fill request %d rejected: %v", i+1, err)
				}
			}
			if err := sub.admitInboundQuery(PrepareBlock{}, now); !errors.Is(err, errInboundQueryRateLimited) {
				t.Fatalf("request past the limit: got %v, want %v", err, errInboundQueryRateLimited)
			}
		})
	}
}

// TestInboundQueryLimitsAreIndependentPerKind checks the budgets do not share a
// window: a flood on one overlay must not starve queries arriving on another.
func TestInboundQueryLimitsAreIndependentPerKind(t *testing.T) {
	node := newTestNode(t)
	public := &overlaySubscription{node: node, spec: overlaySpec{Kind: overlayKindPublicShard}}
	fastSync := &overlaySubscription{node: node, spec: overlaySpec{Kind: overlayKindFastSync}}

	now := time.Now()
	for i := range int(inboundQuerySmallLimit) {
		if err := public.admitInboundQuery(PrepareBlock{}, now); err != nil {
			t.Fatalf("public fill request %d rejected: %v", i+1, err)
		}
	}
	if err := public.admitInboundQuery(PrepareBlock{}, now); !errors.Is(err, errInboundQueryRateLimited) {
		t.Fatalf("public request past the limit: got %v", err)
	}
	if err := fastSync.admitInboundQuery(PrepareBlock{}, now); err != nil {
		t.Fatalf("FastSync must keep its own budget: %v", err)
	}
}
