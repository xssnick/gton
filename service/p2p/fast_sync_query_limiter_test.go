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
		class fastSyncQueryClass
		cost  uint64
	}{
		{"archive", GetArchiveSlice{MaxSize: 2<<20 + 1}, fastSyncQueryHeavy, 2},
		{"persistent state", DownloadPersistentStateSliceV2{MaxSize: 2<<20 + 1}, fastSyncQueryHeavy, 2},
		{"zero state", DownloadZeroState{}, fastSyncQueryHeavy, 8},
		{"block", tonnodeapi.DownloadBlock{}, fastSyncQueryMedium, 1},
		{"block full", tonnodeapi.DownloadBlockFull{}, fastSyncQueryMedium, 1},
		{"next block full", DownloadNextBlockFull{}, fastSyncQueryMedium, 1},
		{"next blocks full", DownloadNextBlocksFull{}, fastSyncQueryMedium, 1},
		{"block proof", DownloadBlockProof{}, fastSyncQueryMedium, 1},
		{"block proof link", DownloadBlockProofLink{}, fastSyncQueryMedium, 1},
		{"key block proof", DownloadKeyBlockProof{}, fastSyncQueryMedium, 1},
		{"key block proof link", DownloadKeyBlockProofLink{}, fastSyncQueryMedium, 1},
		{"out queue proof", GetOutMsgQueueProof{}, fastSyncQueryMedium, 1},
		{"next block description", GetNextBlockDescription{}, fastSyncQuerySmall, 1},
		{"prepare block proof", PrepareBlockProof{}, fastSyncQuerySmall, 1},
		{"prepare key block proof", PrepareKeyBlockProof{}, fastSyncQuerySmall, 1},
		{"prepare block", PrepareBlock{}, fastSyncQuerySmall, 1},
		{"prepare zero state", PrepareZeroState{}, fastSyncQuerySmall, 1},
		{"next key blocks", GetNextKeyBlockIDs{}, fastSyncQuerySmall, 1},
		{"archive info", GetArchiveInfo{}, fastSyncQuerySmall, 1},
		{"shard archive info", GetShardArchiveInfo{}, fastSyncQuerySmall, 1},
		{"prepare persistent state", PreparePersistentState{}, fastSyncQuerySmall, 1},
		{"persistent state size", GetPersistentStateSizeV2{}, fastSyncQuerySmall, 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class, cost := fastSyncQueryCost(test.query)
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

	if class, _ := fastSyncQueryCost(&PrepareBlock{}); class != fastSyncQueryUnlimited {
		t.Fatal("pointer query was classified despite value-only TL decoding")
	}
	if class, _ := fastSyncQueryCost(overlay.Ping{}); class != fastSyncQueryUnlimited {
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
		if cost := fastSyncHeavyQueryCost(test.size); cost != test.cost {
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
		{"heavy", GetArchiveSlice{}, int(fastSyncQueryHeavyLimit)},
		{"medium", tonnodeapi.DownloadBlock{}, int(fastSyncQueryMediumLimit)},
		{"small", PrepareBlock{}, int(fastSyncQuerySmallLimit)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter := newFastSyncQueryLimiter()
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

	limiter := newFastSyncQueryLimiter()
	for i := 0; i < int(fastSyncQueryMediumLimit); i++ {
		if !limiter.Allow(tonnodeapi.DownloadBlock{}, now) {
			t.Fatalf("medium request %d was rejected", i+1)
		}
	}
	for i := 0; i < int(fastSyncQueryGlobalLimit-fastSyncQueryMediumLimit); i++ {
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
	limiter := newFastSyncQueryLimiter()
	for i := 0; i < int(fastSyncQuerySmallLimit); i++ {
		if !limiter.Allow(PrepareBlock{}, now) {
			t.Fatalf("request %d was rejected", i+1)
		}
	}
	if limiter.Allow(
		PrepareBlock{},
		now.Add(fastSyncQueryRateWindowDuration),
	) {
		t.Fatal("request at the exact window boundary was accepted")
	}
	if !limiter.Allow(
		PrepareBlock{},
		now.Add(fastSyncQueryRateWindowDuration+time.Nanosecond),
	) {
		t.Fatal("request beyond the window boundary was rejected")
	}

	limiter = newFastSyncQueryLimiter()
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
	limiter := newFastSyncQueryLimiter()
	now := time.Unix(1_800_100_150, 0)

	for range 10_000 {
		if !limiter.Allow(PrepareBlock{}, now) {
			t.Fatal("spaced request was rejected")
		}
		now = now.Add(2 * fastSyncQueryRateWindowDuration)
	}

	if limiter.small.count > len(limiter.small.entries) ||
		len(limiter.small.entries) != int(fastSyncQuerySmallLimit) {
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
	for i := 0; i < int(fastSyncQuerySmallLimit); i++ {
		if !node.fastSyncQueryLimiter.Allow(PrepareBlock{}, now) {
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
	if !errors.Is(err, errFastSyncQueryRateLimited) {
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
		if errors.Is(err, errFastSyncQueryRateLimited) {
			t.Fatalf("ADNL request %d was rate limited", i+1)
		}
	}
	for i := 0; i < 4; i++ {
		err := sub.answerRLDPQuery(peer, nil, &rldp.Query{Data: req})
		if err == nil {
			t.Fatalf("RLDP request %d did not reach storage", i+1)
		}
		if errors.Is(err, errFastSyncQueryRateLimited) {
			t.Fatalf("RLDP request %d was rate limited", i+1)
		}
	}

	_, err := node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: peerID},
		testQUICOverlayQueryPayload(t, overlayID, req),
	)
	if !errors.Is(err, errFastSyncQueryRateLimited) {
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
	if err == nil || errors.Is(err, errFastSyncQueryRateLimited) {
		t.Fatalf("public ADNL query error = %v, want storage error", err)
	}
	err = publicSub.answerRLDPQuery(
		publicPeer,
		nil,
		&rldp.Query{Data: req},
	)
	if err == nil || errors.Is(err, errFastSyncQueryRateLimited) {
		t.Fatalf("public RLDP query error = %v, want storage error", err)
	}
	_, err = node.handleQUICQuery(
		context.Background(),
		&authenticatedQUICPeer{id: peerID},
		testQUICOverlayQueryPayload(t, publicOverlayID, req),
	)
	if err == nil || errors.Is(err, errFastSyncQueryRateLimited) {
		t.Fatalf("public QUIC query error = %v, want storage error", err)
	}
}

func BenchmarkFastSyncQueryLimiterAllow(b *testing.B) {
	limiter := newFastSyncQueryLimiter()
	now := time.Unix(1_800_100_200, 0)

	b.ReportAllocs()
	for b.Loop() {
		now = now.Add(2 * fastSyncQueryRateWindowDuration)
		fastSyncQueryLimiterSink = limiter.Allow(PrepareBlock{}, now)
	}
}
