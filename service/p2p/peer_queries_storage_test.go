package p2p

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type prepareZeroStatePeerStore struct {
	tnstore.PeerServingStorage
	size int64
	err  error
}

func (s prepareZeroStatePeerStore) ZeroStateSize(context.Context, ton.BlockIDExt) (int64, error) {
	return s.size, s.err
}

func TestServePrepareZeroStateDistinguishesStorageErrors(t *testing.T) {
	storageFailure := errors.New("zero state storage failure")
	tests := []struct {
		name         string
		size         int64
		err          error
		wantPrepared bool
		wantErr      error
	}{
		{name: "available", size: 1, wantPrepared: true},
		{name: "not found", err: tnstore.ErrNotFound},
		{name: "canceled", err: context.Canceled, wantErr: context.Canceled},
		{name: "storage failure", err: storageFailure, wantErr: storageFailure},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := prepareZeroStatePeerStore{
				PeerServingStorage: newTestPeerStore(),
				size:               tt.size,
				err:                tt.err,
			}
			sub := testOverlaySubscription(&overlaySubscription{
				node: &Node{peerStorage: store},
				log:  discardLogger(),
			})

			response, err := sub.servePrepareZeroState(context.Background(), testStoredMasterBlockID(0))
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("prepare zero state error = %v, want %v", err, tt.wantErr)
				}
				if response != nil {
					t.Fatalf("prepare zero state response = %T, want nil", response)
				}
				return
			}
			if err != nil {
				t.Fatalf("prepare zero state: %v", err)
			}
			if tt.wantPrepared {
				if _, ok := response.(PreparedState); !ok {
					t.Fatalf("prepare zero state response = %T, want PreparedState", response)
				}
				return
			}
			if _, ok := response.(NotFoundState); !ok {
				t.Fatalf("prepare zero state response = %T, want NotFoundState", response)
			}
		})
	}
}

func TestDispatchPeerQueryServesStoredBlockAndProofData(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

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
	if err = storage.SaveBlockProof(tnstore.ServedProofBlockLink, shardBlock, []byte{0x04, 0x05}, nil); err != nil {
		t.Fatalf("save shard block proof link: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofKeyBlock, keyBlock, []byte{0x06, 0x07}, nil); err != nil {
		t.Fatalf("save key block proof: %v", err)
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), tonnodeapi.DownloadBlockFull{Block: next})
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

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadNextBlockFull{PrevBlock: block})
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

	resp, err = sub.dispatchPeerQuery(context.Background(), GetNextBlockDescription{PrevBlock: block})
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

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareBlock{Block: next})
	if err != nil {
		t.Fatalf("prepareBlock: %v", err)
	}
	if _, ok = resp.(Prepared); !ok {
		t.Fatalf("unexpected prepareBlock response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareBlockProof{Block: next, AllowPartial: false})
	if err != nil {
		t.Fatalf("prepareBlockProof: %v", err)
	}
	if _, ok = resp.(PreparedProof); !ok {
		t.Fatalf("unexpected prepareBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareBlockProof{Block: shardBlock, AllowPartial: true})
	if err != nil {
		t.Fatalf("prepareBlockProof partial: %v", err)
	}
	if _, ok = resp.(PreparedProofLink); !ok {
		t.Fatalf("unexpected partial prepareBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareKeyBlockProof{Block: keyBlock, AllowPartial: true})
	if err != nil {
		t.Fatalf("prepareKeyBlockProof partial: %v", err)
	}
	if _, ok = resp.(PreparedProofLink); !ok {
		t.Fatalf("unexpected partial prepareKeyBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareKeyBlockProof{Block: keyBlock, AllowPartial: false})
	if err != nil {
		t.Fatalf("prepareKeyBlockProof full: %v", err)
	}
	if _, ok = resp.(PreparedProof); !ok {
		t.Fatalf("unexpected full prepareKeyBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareBlockProof{Block: testStoredMasterBlockID(0), AllowPartial: false})
	if err == nil {
		t.Fatal("expected prepareBlockProof zero state error")
	}
	if resp != nil {
		t.Fatalf("unexpected zero state prepareBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), tonnodeapi.DownloadBlock{Block: next})
	if err != nil {
		t.Fatalf("downloadBlock: %v", err)
	}
	blockData, ok := resp.(tl.Raw)
	if !ok {
		t.Fatalf("unexpected downloadBlock response %T", resp)
	}
	if !bytes.Equal(blockData, full.Block) {
		t.Fatalf("unexpected raw block data")
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadBlockProofLink{Block: shardBlock})
	if err != nil {
		t.Fatalf("downloadBlockProofLink: %v", err)
	}
	proofLink, ok := resp.(tl.Raw)
	if !ok || !bytes.Equal(proofLink, []byte{0x04, 0x05}) {
		t.Fatalf("unexpected block proof link response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadKeyBlockProof{Block: keyBlock})
	if err != nil {
		t.Fatalf("downloadKeyBlockProof: %v", err)
	}
	keyProof, ok := resp.(tl.Raw)
	if !ok || !bytes.Equal(keyProof, []byte{0x06, 0x07}) {
		t.Fatalf("unexpected key block proof response %T", resp)
	}
}

func TestDispatchPeerQueryServesLiveBlockBeforeCheckpoint(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	prev := testStoredMasterBlockID(70)
	next := testStoredMasterBlockID(71)
	blockData := []byte{0x71, 0x01}
	proofData := []byte{0x71, 0x02}
	if err := node.liveBlockCache.PublishLiveBlockArtifacts(tnstore.LiveBlockCacheArtifacts{
		Block:     next,
		BlockData: blockData,
		Meta: &tnstore.BlockMeta{
			ID:       next,
			PrevRefs: []ton.BlockIDExt{prev},
		},
		Proofs: []tnstore.LiveBlockProofArtifact{
			{Kind: tnstore.ServedProofBlock, Data: proofData},
		},
	}); err != nil {
		t.Fatalf("publish live block: %v", err)
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), tonnodeapi.DownloadBlockFull{Block: next})
	if err != nil {
		t.Fatalf("downloadBlockFull: %v", err)
	}
	full, ok := resp.(tonnodeapi.DataFull)
	if !ok {
		t.Fatalf("unexpected downloadBlockFull response %T", resp)
	}
	if !bytes.Equal(full.Block, blockData) || !bytes.Equal(full.Proof, proofData) {
		t.Fatalf("unexpected live full block payload")
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadNextBlockFull{PrevBlock: prev})
	if err != nil {
		t.Fatalf("downloadNextBlockFull: %v", err)
	}
	nextFull, ok := resp.(tonnodeapi.DataFull)
	if !ok || !nextFull.ID.Equals(&next) {
		t.Fatalf("unexpected live next response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), GetNextBlockDescription{PrevBlock: prev})
	if err != nil {
		t.Fatalf("getNextBlockDescription: %v", err)
	}
	desc, ok := resp.(BlockDescription)
	if !ok || !desc.ID.Equals(&next) {
		t.Fatalf("unexpected live next description %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareBlock{Block: next})
	if err != nil {
		t.Fatalf("prepareBlock: %v", err)
	}
	if _, ok = resp.(Prepared); !ok {
		t.Fatalf("unexpected live prepareBlock response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareBlockProof{Block: next, AllowPartial: false})
	if err != nil {
		t.Fatalf("prepareBlockProof: %v", err)
	}
	if _, ok = resp.(PreparedProof); !ok {
		t.Fatalf("unexpected live prepareBlockProof response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), tonnodeapi.DownloadBlock{Block: next})
	if err != nil {
		t.Fatalf("downloadBlock: %v", err)
	}
	rawBlock, ok := resp.(tl.Raw)
	if !ok || !bytes.Equal(rawBlock, blockData) {
		t.Fatalf("unexpected live block data response %T", resp)
	}
}

func TestDispatchPeerQueryServesLiveKeyBlockProof(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	keyBlock := testStoredMasterBlockID(72)
	proofData := []byte{0x72, 0x01}
	if err := node.liveBlockCache.PublishLiveBlockArtifacts(tnstore.LiveBlockCacheArtifacts{
		Block: keyBlock,
		Proofs: []tnstore.LiveBlockProofArtifact{
			{Kind: tnstore.ServedProofBlock, Data: proofData},
			{Kind: tnstore.ServedProofKeyBlock, Data: proofData},
		},
	}); err != nil {
		t.Fatalf("publish live key proof: %v", err)
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), PrepareKeyBlockProof{Block: keyBlock, AllowPartial: false})
	if err != nil {
		t.Fatalf("prepareKeyBlockProof full: %v", err)
	}
	if _, ok := resp.(PreparedProof); !ok {
		t.Fatalf("unexpected live key proof prepare response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadKeyBlockProof{Block: keyBlock})
	if err != nil {
		t.Fatalf("downloadKeyBlockProof: %v", err)
	}
	rawProof, ok := resp.(tl.Raw)
	if !ok || !bytes.Equal(rawProof, proofData) {
		t.Fatalf("unexpected live key proof response %T", resp)
	}

	linkOnlyBlock := testStoredMasterBlockID(73)
	linkOnlyProof := []byte{0x73, 0x01}
	if err := node.liveBlockCache.PublishLiveBlockArtifacts(tnstore.LiveBlockCacheArtifacts{
		Block: linkOnlyBlock,
		Proofs: []tnstore.LiveBlockProofArtifact{
			{Kind: tnstore.ServedProofKeyBlockLink, Data: linkOnlyProof},
		},
	}); err != nil {
		t.Fatalf("publish live key proof link: %v", err)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PrepareKeyBlockProof{Block: linkOnlyBlock, AllowPartial: true})
	if err != nil {
		t.Fatalf("prepareKeyBlockProof link-only: %v", err)
	}
	if _, ok = resp.(PreparedProofLink); !ok {
		t.Fatalf("unexpected live key proof link prepare response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadKeyBlockProofLink{Block: linkOnlyBlock})
	if err != nil {
		t.Fatalf("downloadKeyBlockProofLink: %v", err)
	}
	rawProof, ok = resp.(tl.Raw)
	if !ok || !bytes.Equal(rawProof, linkOnlyProof) {
		t.Fatalf("unexpected live key proof link response %T", resp)
	}
}

func TestDispatchPeerQueryDirectDownloadsErrorWhenMissing(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	block := testStoredMasterBlockID(50)
	if resp, err := sub.dispatchPeerQuery(context.Background(), tonnodeapi.DownloadBlock{Block: block}); err == nil || resp != nil {
		t.Fatalf("downloadBlock missing resp=%T err=%v, want error", resp, err)
	}
	if resp, err := sub.dispatchPeerQuery(context.Background(), DownloadBlockProof{Block: block}); err == nil || resp != nil {
		t.Fatalf("downloadBlockProof missing resp=%T err=%v, want error", resp, err)
	}
	if resp, err := sub.dispatchPeerQuery(context.Background(), DownloadZeroState{Block: block}); err == nil || resp != nil {
		t.Fatalf("downloadZeroState missing resp=%T err=%v, want error", resp, err)
	}
	if resp, err := sub.dispatchPeerQuery(context.Background(), GetArchiveSlice{ArchiveID: 404, Offset: 0, MaxSize: 1}); err == nil || resp != nil {
		t.Fatalf("getArchiveSlice missing resp=%T err=%v, want error", resp, err)
	}
}

func TestDispatchPeerQueryShardNextDescriptionRequiresMasterchain(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "basechain",
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	prev := testStoredBlockID(20)
	next := testStoredBlockID(21)
	if err = storage.SaveBlockFull(&tnstore.ServedBlockFull{ID: next, Proof: []byte{1}, Block: []byte{2}}); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	if err = storage.LinkNextBlock(prev, next); err != nil {
		t.Fatalf("link next block: %v", err)
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), GetNextBlockDescription{PrevBlock: prev})
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
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "basechain",
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

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
	stateName, err := tnstore.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	statePath := filepath.Join(t.TempDir(), stateName)
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
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}
	archivePath, archivePtr := writePeerQueryArchivePack(t, []byte{9, 8, 7, 6})
	registerPeerQueryArchivePack(t, storage, 21, -1, topShard, 0, archivePath)
	shardArchivePath, _ := writePeerQueryArchivePack(t, []byte{6, 5, 4})
	registerPeerQueryArchivePack(t, storage, 21, 0, topShard, 1, shardArchivePath)

	resp, err := sub.dispatchPeerQuery(context.Background(), PrepareZeroState{Block: zeroBlock})
	if err != nil {
		t.Fatalf("prepareZeroState: %v", err)
	}
	if _, ok := resp.(PreparedState); !ok {
		t.Fatalf("unexpected prepareZeroState response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadZeroState{Block: zeroBlock})
	if err != nil {
		t.Fatalf("downloadZeroState: %v", err)
	}
	zero, ok := resp.(tl.Raw)
	if !ok || !bytes.Equal(zero, zeroState) {
		t.Fatalf("unexpected zero state response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), PreparePersistentState{
		Block:            block,
		MasterchainBlock: master,
	})

	if err != nil {
		t.Fatalf("preparePersistentState: %v", err)
	}
	if _, ok := resp.(PreparedState); !ok {
		t.Fatalf("unexpected preparePersistentState response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), GetPersistentStateSizeV2{State: stateID})
	if err != nil {
		t.Fatalf("getPersistentStateSizeV2: %v", err)
	}
	stateSize, ok := resp.(PersistentStateSize)
	if !ok || stateSize.Size != int64(len(stateData)) {
		t.Fatalf("unexpected persistent state size response %#v", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), DownloadPersistentStateSliceV2{
		State:   stateID,
		Offset:  0,
		MaxSize: 3,
	})

	if err != nil {
		t.Fatalf("downloadPersistentStateSliceV2: %v", err)
	}
	chunk, ok := resp.(tl.Raw)
	if !ok || !bytes.Equal(chunk, stateData[:3]) {
		t.Fatalf("unexpected persistent state chunk %#v", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), GetArchiveInfo{MasterchainSeqno: 21})
	if err != nil {
		t.Fatalf("getArchiveInfo: %v", err)
	}
	info, ok := resp.(ArchiveInfo)
	if !ok || info.ID != 0 {
		t.Fatalf("unexpected archive info %#v", resp)
	}
	masterArchiveID := info.ID

	resp, err = sub.dispatchPeerQuery(context.Background(), GetShardArchiveInfo{
		MasterchainSeqno: 21,
		ShardPrefix:      tonnodeapi.ShardID{Workchain: 0, Shard: topShard},
	})

	if err != nil {
		t.Fatalf("getShardArchiveInfo: %v", err)
	}
	info, ok = resp.(ArchiveInfo)
	if !ok || info.ID == masterArchiveID {
		t.Fatalf("unexpected shard archive info %#v", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), GetArchiveSlice{
		ArchiveID: masterArchiveID,
		Offset:    archivePtr.Offset,
		MaxSize:   2,
	})

	if err != nil {
		t.Fatalf("getArchiveSlice: %v", err)
	}
	archiveChunk, ok := resp.(tl.Raw)
	if !ok || !bytes.Equal(archiveChunk, []byte{9, 8}) {
		t.Fatalf("unexpected archive chunk %#v", resp)
	}
}

func TestAnswerPeerQuerySerializesDataMethodsAsRawBytes(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	block := testStoredMasterBlockID(60)
	shardBlock := testStoredBlockID(63)
	zeroBlock := testStoredMasterBlockID(0)
	master := testStoredMasterBlockID(62)
	blockData := []byte{0x10, 0x11, 0x12}
	blockProof := []byte{0x20, 0x21}
	keyBlock, keyProof, keyProofLink := testPeerMasterBlockProof(t, 61)
	masterLinkBlock, _, masterLinkProofLink := testPeerMasterBlockProof(t, 64)
	blockProofLink := []byte{0x30, 0x31}
	zeroState := []byte{0x40, 0x41, 0x42}
	stateData := []byte{0x50, 0x51, 0x52, 0x53}
	archiveData := []byte{0x60, 0x61, 0x62}

	if err = storage.SaveBlockFull(&tnstore.ServedBlockFull{ID: block, Proof: blockProof, Block: blockData}); err != nil {
		t.Fatalf("save block full: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofBlock, block, blockProof, nil); err != nil {
		t.Fatalf("save block proof: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofKeyBlock, keyBlock, keyProof, nil); err != nil {
		t.Fatalf("save key block proof: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofBlockLink, masterLinkBlock, masterLinkProofLink, nil); err != nil {
		t.Fatalf("save master block proof link: %v", err)
	}
	if err = storage.SaveBlockProof(tnstore.ServedProofBlockLink, shardBlock, blockProofLink, nil); err != nil {
		t.Fatalf("save block proof link: %v", err)
	}
	if err = storage.SaveZeroState(zeroBlock, zeroState, nil); err != nil {
		t.Fatalf("save zero state: %v", err)
	}

	stateName, err := tnstore.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	statePath := filepath.Join(t.TempDir(), stateName)
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
	}); err != nil {
		t.Fatalf("save persistent state file: %v", err)
	}

	archivePath, archivePtr := writePeerQueryArchivePack(t, archiveData)
	registerPeerQueryArchivePack(t, storage, 62, -1, topShard, 0, archivePath)
	archiveID, err := storage.ArchiveInfo(context.Background(), 62, -1, topShard)
	if err != nil {
		t.Fatalf("get archive info: %v", err)
	}

	cases := []struct {
		name string
		req  tl.Serializable
		want []byte
	}{
		{name: "downloadBlock", req: tonnodeapi.DownloadBlock{Block: block}, want: blockData},
		{name: "downloadBlockProof", req: DownloadBlockProof{Block: block}, want: blockProof},
		{name: "downloadKeyBlockProof", req: DownloadKeyBlockProof{Block: keyBlock}, want: keyProof},
		{name: "downloadBlockProofLink", req: DownloadBlockProofLink{Block: shardBlock}, want: blockProofLink},
		{name: "downloadMasterBlockProofLink", req: DownloadBlockProofLink{Block: masterLinkBlock}, want: masterLinkProofLink},
		{name: "downloadKeyBlockProofLink", req: DownloadKeyBlockProofLink{Block: keyBlock}, want: keyProofLink},
		{name: "downloadZeroState", req: DownloadZeroState{Block: zeroBlock}, want: zeroState},
		{name: "downloadPersistentStateSliceV2", req: DownloadPersistentStateSliceV2{
			State: PersistentStateIDV2{
				Block:            block,
				MasterchainBlock: master,
				EffectiveShard:   0,
			},
			Offset:  1,
			MaxSize: 2,
		}, want: stateData[1:3]},
		{name: "getArchiveSlice", req: GetArchiveSlice{
			ArchiveID: archiveID,
			Offset:    archivePtr.Offset + 1,
			MaxSize:   2,
		}, want: archiveData[1:3]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := sub.handlePeerQuery(context.Background(), "peer", tc.req)
			if err != nil {
				t.Fatalf("answer query: %v", err)
			}
			if _, ok := resp.(tl.Raw); !ok {
				t.Fatalf("response type = %T, want tl.Raw", resp)
			}
			got, err := tl.Serialize(resp, true)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("serialized answer %x, want raw %x", got, tc.want)
			}
		})
	}
}

func writePeerQueryArchivePack(t *testing.T, data []byte) (string, packfile.Pointer) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "archive.pack")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("create archive pack: %v", err)
	}
	appender, err := packfile.NewAppender(file)
	if err != nil {
		_ = file.Close()
		t.Fatalf("create archive appender: %v", err)
	}
	ptr, err := appender.Append("test", data)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("write archive pack: %v", err)
	}
	return path, ptr
}

func registerPeerQueryArchivePack(t *testing.T, store *testPeerStore, masterchainSeqno int32, workchain int32, shard int64, archiveID int64, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read archive pack: %v", err)
	}

	store.mu.Lock()
	store.archiveInfos[testArchiveInfoKey(masterchainSeqno, workchain, shard)] = archiveID
	store.archiveFiles[archiveID] = data
	store.mu.Unlock()
}

func TestDispatchPeerQueryServesNextKeyBlockIDs(t *testing.T) {
	store := newTestPebbleStore(t)
	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		Storage:       store,
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	anchor := testStoredMasterBlockID(10)
	saveTestServedMasterBlockMeta(t, store, anchor, true)
	for seqno := uint32(11); seqno <= 14; seqno++ {
		block := testStoredMasterBlockID(seqno)
		meta := &tnstore.BlockMeta{ID: block, GenUTime: seqno}
		if seqno == 12 || seqno == 14 {
			meta.Mark(tnstore.BlockMetaIsKeyBlock)
			saveTestServedMasterBlockMeta(t, store, block, true)
			continue
		}
		if err = store.SaveBlockMeta(meta); err != nil {
			t.Fatalf("save block meta %d: %v", seqno, err)
		}
	}
	node.RememberSeenMasterchainBlock(testStoredMasterBlockID(14))

	resp, err := sub.dispatchPeerQuery(context.Background(), GetNextKeyBlockIDs{
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

func TestDispatchPeerQueryNextKeyBlockIDsRejectsNonKeyAnchor(t *testing.T) {
	store := newTestPebbleStore(t)
	anchor := testStoredMasterBlockID(10)
	if err := store.SaveBlockMeta(&tnstore.BlockMeta{ID: anchor, GenUTime: 10}); err != nil {
		t.Fatalf("save anchor meta: %v", err)
	}
	keyBlock := testStoredMasterBlockID(12)
	meta := &tnstore.BlockMeta{ID: keyBlock}
	meta.Mark(tnstore.BlockMetaIsKeyBlock)
	if err := store.SaveBlockMeta(meta); err != nil {
		t.Fatalf("save key block meta: %v", err)
	}

	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		Storage:       store,
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	node.RememberSeenMasterchainBlock(keyBlock)

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	resp, err := sub.dispatchPeerQuery(context.Background(), GetNextKeyBlockIDs{
		Block:   anchor,
		MaxSize: 1,
	})

	if err != nil {
		t.Fatalf("getNextKeyBlockIds: %v", err)
	}
	keyBlocks, ok := resp.(KeyBlocks)
	if !ok {
		t.Fatalf("unexpected getNextKeyBlockIds response %T", resp)
	}
	if !keyBlocks.Error {
		t.Fatalf("expected error-shaped key blocks response, got %#v", keyBlocks)
	}
}

func TestDispatchPeerQueryNextKeyBlockIDsUsesKeyIndexForLargeGap(t *testing.T) {
	store := newTestPebbleStore(t)
	anchor := testStoredMasterBlockID(10)
	saveTestServedMasterBlockMeta(t, store, anchor, true)
	keyBlock := testStoredMasterBlockID(500_000)
	saveTestServedMasterBlockMeta(t, store, keyBlock, true)

	countingStore := &countingSeqNoLookupStore{Storage: store}
	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		Storage:       countingStore,
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	node.RememberSeenMasterchainBlock(keyBlock)

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	resp, err := sub.dispatchPeerQuery(context.Background(), GetNextKeyBlockIDs{
		Block:   testStoredMasterBlockID(10),
		MaxSize: 1,
	})

	if err != nil {
		t.Fatalf("getNextKeyBlockIds: %v", err)
	}
	keyBlocks, ok := resp.(KeyBlocks)
	if !ok {
		t.Fatalf("unexpected getNextKeyBlockIds response %T", resp)
	}
	if keyBlocks.Error || keyBlocks.Incomplete || len(keyBlocks.Blocks) != 1 || keyBlocks.Blocks[0].SeqNo != keyBlock.SeqNo {
		t.Fatalf("unexpected key blocks response %#v", keyBlocks)
	}
	if countingStore.lookupBlockBySeqNoCalls != 0 {
		t.Fatalf("LookupBlockBySeqNo calls = %d, want 0", countingStore.lookupBlockBySeqNoCalls)
	}
}

type countingSeqNoLookupStore struct {
	tnstore.Storage

	lookupBlockBySeqNoCalls int
}

func saveTestServedMasterBlockMeta(t *testing.T, store *pebblestore.Store, block ton.BlockIDExt, keyBlock bool) {
	t.Helper()

	meta := &tnstore.BlockMeta{
		ID:       block,
		GenUTime: block.SeqNo,
	}
	if keyBlock {
		meta.Mark(tnstore.BlockMetaIsKeyBlock)
	}
	if _, err := store.SaveStateCheckpointEntries(context.Background(), []tnstore.StateCheckpointBlock{{
		State: &tnstore.BlockState{
			Block:         block,
			StateRootHash: bytes.Repeat([]byte{0x01}, 32),
		},
		Artifact: &tnstore.ServedBlockFull{
			ID:    block,
			Block: []byte{0x01},
			Proof: []byte{0x02},
			Meta:  meta,
		},
	}}, tnstore.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save served master block meta %s: %v", tnstore.FormatBlockRef(block), err)
	}
}

func (s *countingSeqNoLookupStore) LookupBlockBySeqNo(ctx context.Context, ref tnstore.BlockSeqRef) (ton.BlockIDExt, error) {
	s.lookupBlockBySeqNoCalls++
	return ton.BlockIDExt{}, tnstore.ErrNotFound
}

func TestAnswerPeerQueryStopsSilentlyAfterNodeContextCancel(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node.runCtx = ctx

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	})

	_, err = sub.handlePeerQuery(node.runCtx, "peer", GetCapabilities{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handle peer query error = %v, want context cancellation", err)
	}
}

func TestStatusSnapshotIncludesNeighbours(t *testing.T) {
	storage := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: storage,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{Name: "masterchain"},
		log:  discardLogger(),
		peers: map[PeerID]*overlayPeer{
			testPeerID("peer-1"): {
				id:            testPeerID("peer-1"),
				addr:          "1.2.3.4:30303",
				alive:         true,
				lastSuccessAt: time.Now().Add(-2 * time.Second),
				lastReceiveAt: time.Now(),
				failedQueries: 3,
				unreliability: 1.5,
				announced:     &overlay.Node{Version: int32(time.Now().Unix())},
			},
		},
		neighbours: []PeerID{testPeerID("peer-1")},
	})
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

type testPeerBlockIDExtTLB struct {
	ShardID  tlb.ShardIdent `tlb:"."`
	SeqNo    uint32         `tlb:"## 32"`
	RootHash []byte         `tlb:"bits 256"`
	FileHash []byte         `tlb:"bits 256"`
}

type testPeerBlockProofEnvelope struct {
	_          tlb.Magic             `tlb:"#c3"`
	ProofFor   testPeerBlockIDExtTLB `tlb:"."`
	Root       *cell.Cell            `tlb:"^"`
	Signatures *cell.Cell            `tlb:"maybe ^"`
}

func testPeerMasterBlockProof(t *testing.T, seqno uint32) (ton.BlockIDExt, []byte, []byte) {
	t.Helper()

	root := testPeerBlockRoot(t, -1, topShard, seqno)
	rootHash := root.HashKey(0)
	id := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  bytes.Clone(rootHash[:]),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
	}
	full := testPeerBlockProofEnvelopeBOC(t, id, root, testPeerBlockProofSignatures())
	link, err := blockproof.LinkBOC(id, full)
	if err != nil {
		t.Fatalf("export proof link: %v", err)
	}
	return id, full, link
}

func testPeerBlockRoot(t *testing.T, workchain int32, shard int64, seqno uint32) *cell.Cell {
	t.Helper()

	stateRoot := cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell()
	var header tlb.BlockHeader
	header.Version = 1
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: workchain, ShardPrefix: uint64(shard)}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.MinRefMcSeqno = seqno
	header.PrevKeyBlockSeqno = seqno
	header.KeyBlock = workchain == -1
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    1,
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}

	root, err := tlb.ToCell(&tlb.Block{
		GlobalID:    -239,
		BlockInfo:   header,
		ValueFlow:   cell.BeginCell().EndCell(),
		StateUpdate: testPeerMerkleUpdateCell(t, cell.BeginCell().EndCell(), stateRoot),
		Extra: &tlb.BlockExtra{
			InMsgDesc:          cell.BeginCell().EndCell(),
			OutMsgDesc:         cell.BeginCell().EndCell(),
			ShardAccountBlocks: cell.BeginCell().EndCell(),
			RandSeed:           bytes.Repeat([]byte{0x01}, 32),
			CreatedBy:          bytes.Repeat([]byte{0x02}, 32),
		},
	})
	if err != nil {
		t.Fatalf("build block root: %v", err)
	}
	return root
}

func testPeerMerkleUpdateCell(t *testing.T, oldRoot *cell.Cell, newRoot *cell.Cell) *cell.Cell {
	t.Helper()

	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(oldRoot.Hash(0), 256).
		MustStoreSlice(newRoot.Hash(0), 256).
		MustStoreUInt(uint64(oldRoot.Depth(0)), 16).
		MustStoreUInt(uint64(newRoot.Depth(0)), 16).
		MustStoreRef(oldRoot).
		MustStoreRef(newRoot).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("build merkle update: %v", err)
	}
	return update
}

func testPeerBlockProofEnvelopeBOC(t *testing.T, id ton.BlockIDExt, root *cell.Cell, signatures *cell.Cell) []byte {
	t.Helper()

	envelope, err := tlb.ToCell(&testPeerBlockProofEnvelope{
		ProofFor: testPeerBlockIDExtTLB{
			ShardID: tlb.ShardIdent{
				PrefixBits:  0,
				WorkchainID: id.Workchain,
				ShardPrefix: uint64(id.Shard),
			},
			SeqNo:    id.SeqNo,
			RootHash: bytes.Clone(id.RootHash),
			FileHash: bytes.Clone(id.FileHash),
		},
		Root:       testPeerFullMerkleProof(t, root),
		Signatures: signatures,
	})
	if err != nil {
		t.Fatalf("serialize block proof envelope: %v", err)
	}
	return envelope.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
}

func testPeerFullMerkleProof(t *testing.T, root *cell.Cell) *cell.Cell {
	t.Helper()

	proofBuilder := cell.NewMerkleProofBuilder(root)
	testPeerMarkFullProofSubtree(t, proofBuilder.Root())
	proof, err := proofBuilder.CreateProof()
	if err != nil {
		t.Fatalf("create block proof: %v", err)
	}
	return proof
}

func testPeerMarkFullProofSubtree(t *testing.T, root *cell.Cell) {
	t.Helper()

	loader, err := root.BeginParse()
	if err != nil {
		t.Fatalf("begin proof subtree: %v", err)
	}
	for i := 0; i < loader.RefsNum(); i++ {
		ref, err := loader.PeekRefCellAt(i)
		if err != nil {
			t.Fatalf("proof subtree ref %d: %v", i, err)
		}
		testPeerMarkFullProofSubtree(t, ref)
	}
}

func testPeerBlockProofSignatures() *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 64).
		MustStoreDict(cell.NewDict(16)).
		EndCell()
}
