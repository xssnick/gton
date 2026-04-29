package p2p

import (
	"bytes"
	"context"
	"flexserver/service/archive"
	tnstore "flexserver/service/storage"
	"flexserver/service/storage/memstore"
	"os"
	"path/filepath"
	"testing"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func TestDispatchPeerQueryServesStoredBlockAndProofData(t *testing.T) {
	storage := memstore.NewPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	}

	block := testStoredBlockID(10)
	next := testStoredBlockID(11)
	full := &tnstore.ServedBlockFull{
		ID:     next,
		Proof:  []byte{0xAA, 0xBB},
		Block:  []byte{0xCC, 0xDD},
		IsLink: false,
	}
	if err := storage.SaveBlockFull(full); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	storage.LinkNextBlock(block, next)
	storage.SaveBlockProof(tnstore.ServedProofBlockLink, block, []byte{0x01, 0x02, 0x03}, nil)

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, tonnodeapi.DownloadBlockFull{Block: next})
	if err != nil {
		t.Fatalf("downloadBlockFull: %v", err)
	}
	downloaded, ok := resp.(tonnodeapi.DataFull)
	if !ok {
		t.Fatalf("unexpected downloadBlockFull response %T", resp)
	}
	if !bytes.Equal(downloaded.Block, full.Block) || !bytes.Equal(downloaded.Proof, full.Proof) {
		t.Fatalf("unexpected full block payload")
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, DownloadNextBlockFull{PrevBlock: block})
	if err != nil {
		t.Fatalf("downloadNextBlockFull: %v", err)
	}
	nextResp, ok := resp.(tonnodeapi.DataFull)
	if !ok {
		t.Fatalf("unexpected downloadNextBlockFull response %T", resp)
	}
	if !nextResp.ID.Equals(&next) {
		t.Fatalf("unexpected next block %v", nextResp.ID.SeqNo)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareBlock{Block: next})
	if err != nil {
		t.Fatalf("prepareBlock: %v", err)
	}
	if _, ok = resp.(Prepared); !ok {
		t.Fatalf("unexpected prepareBlock response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareBlockProof{Block: next, AllowPartial: false})
	if err != nil {
		t.Fatalf("prepareBlockProof: %v", err)
	}
	if _, ok = resp.(PreparedProof); !ok {
		t.Fatalf("unexpected prepareBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareBlockProof{Block: block, AllowPartial: true})
	if err != nil {
		t.Fatalf("prepareBlockProof partial: %v", err)
	}
	if _, ok = resp.(PreparedProofLink); !ok {
		t.Fatalf("unexpected partial prepareBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, tonnodeapi.DownloadBlock{Block: next})
	if err != nil {
		t.Fatalf("downloadBlock: %v", err)
	}
	blockData, ok := resp.(TonNodeData)
	if !ok {
		t.Fatalf("unexpected downloadBlock response %T", resp)
	}
	if !bytes.Equal(blockData.Data, full.Block) {
		t.Fatalf("unexpected raw block data")
	}
}

func TestDispatchPeerQueryDoesNotServeStateSnapshots(t *testing.T) {
	storage := memstore.NewPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "basechain",
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		log: discardLogger(),
	}

	block := testStoredBlockID(20)
	master := testStoredMasterBlockID(21)
	stateID := PersistentStateIDV2{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   block.Shard,
	}
	archivePath := filepath.Join(t.TempDir(), "archive.pack")
	if err = os.WriteFile(archivePath, []byte{9, 8, 7, 6}, 0o644); err != nil {
		t.Fatalf("write archive file: %v", err)
	}
	if _, err = storage.SaveArchiveFile(21, -1, topShard, 777, archivePath); err != nil {
		t.Fatalf("save archive file: %v", err)
	}
	shardArchivePath := filepath.Join(t.TempDir(), "shard-archive.pack")
	if err = os.WriteFile(shardArchivePath, []byte{6, 5, 4}, 0o644); err != nil {
		t.Fatalf("write shard archive file: %v", err)
	}
	if _, err = storage.SaveArchiveFile(21, 0, topShard, 778, shardArchivePath); err != nil {
		t.Fatalf("save shard archive file: %v", err)
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareZeroState{Block: block})
	if err != nil {
		t.Fatalf("prepareZeroState: %v", err)
	}
	if _, ok := resp.(NotFoundState); !ok {
		t.Fatalf("unexpected prepareZeroState response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, DownloadZeroState{Block: block})
	if err != nil {
		t.Fatalf("downloadZeroState: %v", err)
	}
	zero, ok := resp.(TonNodeData)
	if !ok || len(zero.Data) != 0 {
		t.Fatalf("unexpected zero state response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PreparePersistentState{
		Block:            block,
		MasterchainBlock: master,
	})
	if err != nil {
		t.Fatalf("preparePersistentState: %v", err)
	}
	if _, ok := resp.(NotFoundState); !ok {
		t.Fatalf("unexpected preparePersistentState response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetPersistentStateSizeV2{State: stateID})
	if err != nil {
		t.Fatalf("getPersistentStateSizeV2: %v", err)
	}
	if _, ok := resp.(PersistentStateSizeNotFound); !ok {
		t.Fatalf("unexpected persistent state size response %#v", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, DownloadPersistentStateSliceV2{
		State:   stateID,
		Offset:  0,
		MaxSize: 3,
	})
	if err != nil {
		t.Fatalf("downloadPersistentStateSliceV2: %v", err)
	}
	chunk, ok := resp.(TonNodeData)
	if !ok || len(chunk.Data) != 0 {
		t.Fatalf("unexpected persistent state chunk %#v", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetArchiveInfo{MasterchainSeqno: 21})
	if err != nil {
		t.Fatalf("getArchiveInfo: %v", err)
	}
	info, ok := resp.(ArchiveInfo)
	if !ok || info.ID != 777 {
		t.Fatalf("unexpected archive info %#v", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetShardArchiveInfo{
		MasterchainSeqno: 21,
		ShardPrefix:      archive.ShardID{Workchain: 0, Shard: topShard},
	})
	if err != nil {
		t.Fatalf("getShardArchiveInfo: %v", err)
	}
	info, ok = resp.(ArchiveInfo)
	if !ok || info.ID != 778 {
		t.Fatalf("unexpected shard archive info %#v", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetArchiveSlice{
		ArchiveID: 777,
		Offset:    0,
		MaxSize:   2,
	})
	if err != nil {
		t.Fatalf("getArchiveSlice: %v", err)
	}
	archiveChunk, ok := resp.(TonNodeData)
	if !ok || !bytes.Equal(archiveChunk.Data, []byte{9, 8}) {
		t.Fatalf("unexpected archive chunk %#v", resp)
	}
}

func TestAnswerPeerQueryStopsSilentlyAfterNodeContextCancel(t *testing.T) {
	storage := memstore.NewPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node.runCtx = ctx

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	}

	answered := false
	err = sub.answerPeerQuery(&overlayPeer{addr: "peer"}, GetCapabilities{}, func(context.Context, tl.Serializable) error {
		answered = true
		return nil
	})
	if err != nil {
		t.Fatalf("answer peer query: %v", err)
	}
	if answered {
		t.Fatal("query was answered after node context was canceled")
	}
}

func TestStatusSnapshotIncludesNeighbours(t *testing.T) {
	storage := memstore.NewPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{Name: "masterchain"},
		log:  discardLogger(),
		peers: map[string]*overlayPeer{
			"peer-1": {
				id:            "peer-1",
				addr:          "1.2.3.4:30303",
				alive:         true,
				lastSuccessAt: time.Now().Add(-2 * time.Second),
				lastReceiveAt: time.Now(),
				failedQueries: 3,
				unreliability: 1.5,
				announced:     &overlay.Node{Version: int32(time.Now().Unix())},
			},
		},
		neighbours: []string{"peer-1"},
	}
	node.subscriptions = map[string]*overlaySubscription{"master": sub}

	snapshot := node.StatusSnapshot()
	if len(snapshot.Overlays) != 1 {
		t.Fatalf("unexpected overlays count %d", len(snapshot.Overlays))
	}
	if snapshot.Overlays[0].ActiveNeighbours != 1 {
		t.Fatalf("unexpected active neighbours %d", snapshot.Overlays[0].ActiveNeighbours)
	}
	if snapshot.Overlays[0].AliveKnownPeers != 1 {
		t.Fatalf("unexpected alive known peers %d", snapshot.Overlays[0].AliveKnownPeers)
	}
	if snapshot.Overlays[0].AliveNeighbours != 1 {
		t.Fatalf("unexpected alive neighbours %d", snapshot.Overlays[0].AliveNeighbours)
	}
	neighbour := snapshot.Overlays[0].Neighbours[0]
	if neighbour.Addr != "1.2.3.4:30303" {
		t.Fatalf("unexpected neighbour addr %q", neighbour.Addr)
	}
	if neighbour.FailedQueries != 3 {
		t.Fatalf("unexpected failed queries %d", neighbour.FailedQueries)
	}
	if neighbour.LastSuccessAt.IsZero() {
		t.Fatal("expected last success timestamp")
	}
}

func testStoredBlockID(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
	}
}

func testStoredMasterBlockID(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
	}
}
