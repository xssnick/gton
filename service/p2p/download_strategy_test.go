package p2p

import (
	"context"
	"crypto/sha256"
	"flexserver/service/storage"
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

func TestChainBlockUnavailableDoesNotClearPinnedPeer(t *testing.T) {
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
		t.Fatalf("expected not-available response to keep sticky peer, got %v", got)
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

	proofData := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell().ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
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
