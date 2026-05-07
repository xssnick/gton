package p2p

import (
	"bytes"
	"context"
	"flexserver/service/archive"
	tnstore "flexserver/service/storage"
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
	storage := newTestPeerStore()
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

	block := testStoredMasterBlockID(10)
	next := testStoredMasterBlockID(11)
	shardBlock := testStoredBlockID(12)
	keyBlock := testStoredMasterBlockID(13)
	full := &tnstore.ServedBlockFull{
		ID:     next,
		Proof:  []byte{0xAA, 0xBB},
		Block:  []byte{0xCC, 0xDD},
		IsLink: false,
	}
	if err := storage.SaveBlockFull(full); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	if err = storage.LinkNextBlock(block, next); err != nil {
		t.Fatalf("link next block: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofBlockLink, block, []byte{0x01, 0x02, 0x03}, nil); err != nil {
		t.Fatalf("save block proof: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofBlockLink, shardBlock, []byte{0x04, 0x05}, nil); err != nil {
		t.Fatalf("save shard block proof link: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofKeyBlock, keyBlock, []byte{0x06, 0x07}, nil); err != nil {
		t.Fatalf("save key block proof: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofKeyBlockLink, keyBlock, []byte{0x08, 0x09}, nil); err != nil {
		t.Fatalf("save key block proof link: %v", err)
	}

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

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetNextBlockDescription{PrevBlock: block})
	if err != nil {
		t.Fatalf("getNextBlockDescription: %v", err)
	}
	desc, ok := resp.(BlockDescription)
	if !ok {
		t.Fatalf("unexpected getNextBlockDescription response %T", resp)
	}
	if !desc.ID.Equals(&next) {
		t.Fatalf("unexpected next description %v", desc.ID.SeqNo)
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

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareBlockProof{Block: shardBlock, AllowPartial: true})
	if err != nil {
		t.Fatalf("prepareBlockProof partial: %v", err)
	}
	if _, ok = resp.(PreparedProofLink); !ok {
		t.Fatalf("unexpected partial prepareBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareKeyBlockProof{Block: keyBlock, AllowPartial: true})
	if err != nil {
		t.Fatalf("prepareKeyBlockProof partial: %v", err)
	}
	if _, ok = resp.(PreparedProofLink); !ok {
		t.Fatalf("unexpected partial prepareKeyBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareKeyBlockProof{Block: keyBlock, AllowPartial: false})
	if err != nil {
		t.Fatalf("prepareKeyBlockProof full: %v", err)
	}
	if _, ok = resp.(PreparedProof); !ok {
		t.Fatalf("unexpected full prepareKeyBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareBlockProof{Block: testStoredMasterBlockID(0), AllowPartial: false})
	if err == nil {
		t.Fatal("expected prepareBlockProof zero state error")
	}
	if resp != nil {
		t.Fatalf("unexpected zero state prepareBlockProof response %T", resp)
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

func TestDispatchPeerQueryDirectDownloadsErrorWhenMissing(t *testing.T) {
	storage := newTestPeerStore()
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

	block := testStoredMasterBlockID(50)
	if resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, tonnodeapi.DownloadBlock{Block: block}); err == nil || resp != nil {
		t.Fatalf("downloadBlock missing resp=%T err=%v, want error", resp, err)
	}
	if resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, DownloadBlockProof{Block: block}); err == nil || resp != nil {
		t.Fatalf("downloadBlockProof missing resp=%T err=%v, want error", resp, err)
	}
	if resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, DownloadZeroState{Block: block}); err == nil || resp != nil {
		t.Fatalf("downloadZeroState missing resp=%T err=%v, want error", resp, err)
	}
	if resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetArchiveSlice{ArchiveID: 404, Offset: 0, MaxSize: 1}); err == nil || resp != nil {
		t.Fatalf("getArchiveSlice missing resp=%T err=%v, want error", resp, err)
	}
}

func TestDispatchPeerQueryShardNextDescriptionRequiresMasterchain(t *testing.T) {
	storage := newTestPeerStore()
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

	prev := testStoredBlockID(20)
	next := testStoredBlockID(21)
	if err = storage.SaveBlockFull(&tnstore.ServedBlockFull{ID: next, Proof: []byte{1}, Block: []byte{2}}); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	if err = storage.LinkNextBlock(prev, next); err != nil {
		t.Fatalf("link next block: %v", err)
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetNextBlockDescription{PrevBlock: prev})
	if err == nil {
		t.Fatal("expected getNextBlockDescription shard prev error")
	}
	if resp != nil {
		t.Fatalf("unexpected getNextBlockDescription response %T", resp)
	}
}

func TestDispatchPeerQueryServesZeroStateAndArchiveData(t *testing.T) {
	storage := newTestPeerStore()
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
	zeroBlock := testStoredBlockID(0)
	zeroState := []byte{5, 4, 3, 2, 1}
	if err = storage.SaveZeroState(zeroBlock, zeroState, nil); err != nil {
		t.Fatalf("save zero state: %v", err)
	}

	master := testStoredMasterBlockID(21)
	stateID := PersistentStateIDV2{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
	}
	stateData := []byte{7, 8, 9, 10, 11}
	statePath := filepath.Join(t.TempDir(), "state.boc")
	if err = os.WriteFile(statePath, stateData, 0o644); err != nil {
		t.Fatalf("write persistent state file: %v", err)
	}
	if err = storage.SavePersistentStateFile(&tnstore.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref:              &tnstore.ArtifactRef{Path: statePath, Size: int64(len(stateData))},
		FileHash:         bytes.Repeat([]byte{0x44}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x55}, 32),
		CellsCount:       3,
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
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

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PrepareZeroState{Block: zeroBlock})
	if err != nil {
		t.Fatalf("prepareZeroState: %v", err)
	}
	if _, ok := resp.(PreparedState); !ok {
		t.Fatalf("unexpected prepareZeroState response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, DownloadZeroState{Block: zeroBlock})
	if err != nil {
		t.Fatalf("downloadZeroState: %v", err)
	}
	zero, ok := resp.(TonNodeData)
	if !ok || !bytes.Equal(zero.Data, zeroState) {
		t.Fatalf("unexpected zero state response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, PreparePersistentState{
		Block:            block,
		MasterchainBlock: master,
	})
	if err != nil {
		t.Fatalf("preparePersistentState: %v", err)
	}
	if _, ok := resp.(PreparedState); !ok {
		t.Fatalf("unexpected preparePersistentState response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetPersistentStateSizeV2{State: stateID})
	if err != nil {
		t.Fatalf("getPersistentStateSizeV2: %v", err)
	}
	stateSize, ok := resp.(PersistentStateSize)
	if !ok || stateSize.Size != int64(len(stateData)) {
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
	if !ok || !bytes.Equal(chunk.Data, stateData[:3]) {
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

func TestDispatchPeerQueryServesNextKeyBlockIDs(t *testing.T) {
	store := newTestPebbleStore(t)
	logger := discardLogger()
	node, err := New(Options{
		Logger:  &logger,
		Storage: store,
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

	for seqno := uint32(11); seqno <= 14; seqno++ {
		block := testStoredMasterBlockID(seqno)
		meta := &tnstore.BlockMeta{ID: block}
		if seqno == 12 || seqno == 14 {
			meta.Mark(tnstore.BlockMetaIsKeyBlock)
		}
		if err = store.SaveBlockMeta(meta); err != nil {
			t.Fatalf("save block meta %d: %v", seqno, err)
		}
	}
	if err = store.SaveSeenMasterchainBlock(context.Background(), testStoredMasterBlockID(14)); err != nil {
		t.Fatalf("save seen masterchain: %v", err)
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetNextKeyBlockIDs{
		Block:   testStoredMasterBlockID(10),
		MaxSize: 2,
	})
	if err != nil {
		t.Fatalf("getNextKeyBlockIds: %v", err)
	}
	keyBlocks, ok := resp.(KeyBlocks)
	if !ok {
		t.Fatalf("unexpected getNextKeyBlockIds response %T", resp)
	}
	if keyBlocks.Error || keyBlocks.Incomplete || len(keyBlocks.Blocks) != 2 {
		t.Fatalf("unexpected key blocks response %#v", keyBlocks)
	}
	if keyBlocks.Blocks[0].SeqNo != 12 || keyBlocks.Blocks[1].SeqNo != 14 {
		t.Fatalf("unexpected key block seqnos %#v", keyBlocks.Blocks)
	}
}

func TestAnswerPeerQueryStopsSilentlyAfterNodeContextCancel(t *testing.T) {
	storage := newTestPeerStore()
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
	storage := newTestPeerStore()
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
