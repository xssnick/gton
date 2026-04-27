package p2p

import (
	"context"
	"crypto/sha256"
	"flexserver/service/storage"
	"flexserver/service/storage/memstore"
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
	got, err := runConcurrentBlockDownloads(ctx, peers, 2, func(ctx context.Context, peer *overlayPeer) (DownloadedBlock, error) {
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
	store := memstore.NewPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: store,
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	blockCell := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	blockData := blockCell.ToBOCWithOptions(cell.BOCOptions{WithCRC32C: false})
	rootHash := blockCell.HashKey()
	fileHash := sha256.Sum256(blockData)
	block := testBlockID(-1, topShard, 100)
	block.RootHash = append([]byte(nil), rootHash[:]...)
	block.FileHash = append([]byte(nil), fileHash[:]...)

	proofData := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell().ToBOCWithOptions(cell.BOCOptions{WithCRC32C: false})
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
