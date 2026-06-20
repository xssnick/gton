package service

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestStatusSnapshotIncludesLocalChainProgress(t *testing.T) {
	store := openTestPebbleStorage(t)
	node, err := p2p.New(p2p.Options{Storage: store, StateFilesDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	master := testBlockID(-1, topShard, 40)
	base := testBlockID(0, topShard, 77)
	masterState := &tnstore.BlockState{
		Block:         master,
		StateRootHash: master.RootHash,
		Parsed:        &tlb.ShardStateUnsplit{GenUTime: 100},
	}
	baseState := &tnstore.BlockState{
		Block:  base,
		Cell:   testShardStateCell(t, base),
		Parsed: &tlb.ShardStateUnsplit{GenUTime: 120},
	}
	err = store.SaveStateCheckpoint(context.Background(), []*tnstore.BlockState{
		masterState,
		baseState,
	}, &tnstore.CurrentState{
		Masterchain: *masterState,
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: topShard}: *baseState,
		},
	})
	if err != nil {
		t.Fatalf("save current state: %v", err)
	}

	svc := &Service{
		node:    node,
		storage: store,
	}
	snapshot := svc.StatusSnapshot()

	if !snapshot.LocalStateLoaded {
		t.Fatal("expected local state to be loaded")
	}
	if snapshot.LocalMasterchain == nil || snapshot.LocalMasterchain.SeqNo != 40 {
		t.Fatalf("unexpected local masterchain block %+v", snapshot.LocalMasterchain)
	}
	if snapshot.LocalBasechain == nil || snapshot.LocalBasechain.SeqNo != 77 {
		t.Fatalf("unexpected local basechain block %+v", snapshot.LocalBasechain)
	}
	if snapshot.LocalMasterchainUtime != 100 {
		t.Fatalf("unexpected local masterchain utime %d", snapshot.LocalMasterchainUtime)
	}
	if snapshot.LocalBasechainUtime != 120 {
		t.Fatalf("unexpected local basechain utime %d", snapshot.LocalBasechainUtime)
	}
}

func TestStatusSnapshotUsesLiveCurrentState(t *testing.T) {
	store := openTestPebbleStorage(t)
	node, err := p2p.New(p2p.Options{Storage: store, StateFilesDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	storedMaster := testBlockID(-1, topShard, 40)
	liveMaster := testBlockID(-1, topShard, 41)
	liveBase := testBlockID(0, topShard, 78)

	err = store.SaveStateCheckpoint(context.Background(), []*tnstore.BlockState{{
		Block:         storedMaster,
		StateRootHash: storedMaster.RootHash,
		Parsed:        &tlb.ShardStateUnsplit{GenUTime: 100},
	}}, &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:         storedMaster,
			StateRootHash: storedMaster.RootHash,
			Parsed:        &tlb.ShardStateUnsplit{GenUTime: 100},
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	})
	if err != nil {
		t.Fatalf("save current state: %v", err)
	}

	svc := &Service{
		node:    node,
		storage: store,
	}
	svc.publishLiveCurrentStateChanged(&tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:  liveMaster,
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 200},
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: topShard}: {
				Block:  liveBase,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 220},
			},
		},
	})

	snapshot := svc.StatusSnapshot()
	if snapshot.LocalMasterchain == nil || snapshot.LocalMasterchain.SeqNo != liveMaster.SeqNo {
		t.Fatalf("unexpected local masterchain block %+v", snapshot.LocalMasterchain)
	}
	if snapshot.LocalBasechain == nil || snapshot.LocalBasechain.SeqNo != liveBase.SeqNo {
		t.Fatalf("unexpected local basechain block %+v", snapshot.LocalBasechain)
	}
	if snapshot.LocalMasterchainUtime != 200 {
		t.Fatalf("unexpected local masterchain utime %d", snapshot.LocalMasterchainUtime)
	}
	if snapshot.LocalBasechainUtime != 220 {
		t.Fatalf("unexpected local basechain utime %d", snapshot.LocalBasechainUtime)
	}
}

func TestStatusSnapshotIncludesAppliedMasterchainProgress(t *testing.T) {
	store := openTestPebbleStorage(t)
	node, err := p2p.New(p2p.Options{Storage: store, StateFilesDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	currentMaster := testBlockID(-1, topShard, 100)
	appliedMaster := testBlockID(-1, topShard, 105)

	svc := &Service{
		node:    node,
		storage: store,
		currentStatus: &tnstore.CurrentState{
			Masterchain: tnstore.BlockState{
				Block:  currentMaster,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 1000},
			},
			Shards: map[tnstore.ShardKey]tnstore.BlockState{},
		},
	}
	svc.rememberAppliedMasterchainState(&tnstore.BlockState{
		Block:  appliedMaster,
		Parsed: &tlb.ShardStateUnsplit{GenUTime: 1005},
	})

	snapshot := svc.StatusSnapshot()
	if snapshot.LocalMasterchain == nil || snapshot.LocalMasterchain.SeqNo != currentMaster.SeqNo {
		t.Fatalf("local masterchain = %+v, want %s", snapshot.LocalMasterchain, tnstore.FormatBlockRef(currentMaster))
	}
	if snapshot.AppliedMasterchain == nil || snapshot.AppliedMasterchain.SeqNo != appliedMaster.SeqNo {
		t.Fatalf("applied masterchain = %+v, want %s", snapshot.AppliedMasterchain, tnstore.FormatBlockRef(appliedMaster))
	}
	if snapshot.LocalMasterchainUtime != 1000 {
		t.Fatalf("local masterchain utime = %d, want 1000", snapshot.LocalMasterchainUtime)
	}
	if snapshot.AppliedMasterchainUtime != 1005 {
		t.Fatalf("applied masterchain utime = %d, want 1005", snapshot.AppliedMasterchainUtime)
	}
}

func TestStatusSnapshotIncludesSplitBasechainShards(t *testing.T) {
	store := openTestPebbleStorage(t)
	node, err := p2p.New(p2p.Options{Storage: store, StateFilesDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	master := testBlockID(-1, topShard, 100)
	left := testBlockID(0, int64(0x4000000000000000), 200)
	right := testBlockID(0, int64(-0x4000000000000000), 201)

	svc := &Service{
		node:    node,
		storage: store,
	}
	svc.publishLiveCurrentStateChanged(&tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:  master,
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 300},
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: left.Shard}: {
				Block:  left,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 310},
			},
			{Workchain: 0, Shard: right.Shard}: {
				Block:  right,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 320},
			},
		},
	})

	snapshot := svc.StatusSnapshot()
	if snapshot.LocalBasechain != nil {
		t.Fatalf("unexpected unsplit basechain block %+v", snapshot.LocalBasechain)
	}
	if len(snapshot.LocalBasechainShards) != 2 {
		t.Fatalf("local basechain shards = %d, want 2", len(snapshot.LocalBasechainShards))
	}
	if !snapshot.LocalBasechainShards[0].Block.Equals(&left) || snapshot.LocalBasechainShards[0].Utime != 310 {
		t.Fatalf("unexpected left basechain shard %+v", snapshot.LocalBasechainShards[0])
	}
	if !snapshot.LocalBasechainShards[1].Block.Equals(&right) || snapshot.LocalBasechainShards[1].Utime != 320 {
		t.Fatalf("unexpected right basechain shard %+v", snapshot.LocalBasechainShards[1])
	}
}

func TestStatusSnapshotPrefersLocalShardClientBasechainLatest(t *testing.T) {
	staleBase := testBlockID(0, topShard, 71)
	localMaster := testBlockID(-1, topShard, 100)
	localBase := testBlockID(0, topShard, 82)
	snapshot := StatusSnapshot{
		StatusSnapshot: p2p.StatusSnapshot{
			LatestBasechain: &staleBase,
		},
	}
	svc := &Service{}
	svc.populateStatusLatestBasechain(context.Background(), &snapshot, &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{Block: localMaster},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: topShard}: {Block: localBase},
		},
	})

	if snapshot.LatestBasechain == nil || !snapshot.LatestBasechain.Equals(&localBase) {
		t.Fatalf("latest basechain = %+v, want local shard-client basechain %s", snapshot.LatestBasechain, tnstore.FormatBlockRef(localBase))
	}
	if len(snapshot.LatestBasechainShards) != 1 || !snapshot.LatestBasechainShards[0].Equals(&localBase) {
		t.Fatalf("latest basechain shards = %+v, want %s", snapshot.LatestBasechainShards, tnstore.FormatBlockRef(localBase))
	}
}

func TestCurrentBasechainLagSecondsUsesLocalShardUTime(t *testing.T) {
	baseFresh := testBlockID(0, topShard, 10)
	baseStale := testBlockID(0, topShard>>1, 11)
	master := testBlockID(-1, topShard, 20)

	lag, shards, ok := currentBasechainLagSeconds(time.Unix(1000, 0), &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:  master,
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 999},
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: topShard}: {
				Block:  baseFresh,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 990},
			},
			{Workchain: 0, Shard: topShard >> 1}: {
				Block:  baseStale,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 970},
			},
		},
	})
	if !ok {
		t.Fatal("expected basechain lag")
	}
	if shards != 2 {
		t.Fatalf("basechain shards = %d, want 2", shards)
	}
	if lag != 30 {
		t.Fatalf("basechain lag = %d, want 30", lag)
	}
}

func TestSyncLagSecondsUsesMaxCurrentStateLag(t *testing.T) {
	now := time.Now()
	master := testBlockID(-1, topShard, 20)
	baseFresh := testBlockID(0, topShard, 10)
	baseStale := testBlockID(0, topShard>>1, 11)
	svc := &Service{
		currentStatus: &tnstore.CurrentState{
			Masterchain: tnstore.BlockState{
				Block:  master,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(now.Add(-4 * time.Second).Unix())},
			},
			Shards: map[tnstore.ShardKey]tnstore.BlockState{
				{Workchain: 0, Shard: topShard}: {
					Block:  baseFresh,
					Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(now.Add(-2 * time.Second).Unix())},
				},
				{Workchain: 0, Shard: topShard >> 1}: {
					Block:  baseStale,
					Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(now.Add(-9 * time.Second).Unix())},
				},
			},
		},
	}

	lag, err := svc.SyncLagSeconds()
	if err != nil {
		t.Fatalf("sync lag: %v", err)
	}
	if lag < 8 || lag > 10 {
		t.Fatalf("sync lag = %d, want about 9", lag)
	}
}

func TestStatusSnapshotUsesLiveBlockCacheForTransactions(t *testing.T) {
	store := openTestPebbleStorage(t)
	node, err := p2p.New(p2p.Options{Storage: store, StateFilesDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create p2p node: %v", err)
	}
	cache := tnstore.NewLiveBlockCache(tnstore.DefaultLiveBlockCacheMaxBlocks)

	block, root, data, meta := mustStatusFixtureBlock(t)
	meta.GenUTime = 100
	if err = cache.PublishLiveBlockArtifacts(tnstore.LiveBlockArtifacts{
		Block:     block,
		Root:      root,
		BlockData: data,
		Meta:      meta,
		Proofs: []tnstore.LiveBlockProofArtifact{{
			Kind: tnstore.ServedProofBlock,
			Data: []byte{0x01},
		}},
	}); err != nil {
		t.Fatalf("publish live block: %v", err)
	}

	svc := &Service{
		node:           node,
		storage:        store,
		liveBlockCache: cache,
	}
	svc.publishLiveCurrentStateChanged(&tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:  block,
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 100},
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	})

	snapshot := svc.StatusSnapshot()
	if !snapshot.LocalMasterchainHasTx {
		t.Fatal("expected live masterchain transaction count")
	}
	if snapshot.LocalMasterchainTx == 0 {
		t.Fatal("expected positive live masterchain transaction count")
	}
	if _, err := store.BlockData(context.Background(), block); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("pebble block data error = %v, want ErrNotFound", err)
	}
}

func TestRecentTPSSnapshotUsesLastLiveBlockWithoutStorageHistory(t *testing.T) {
	store := openTestPebbleStorage(t)
	cache := tnstore.NewLiveBlockCache(tnstore.DefaultLiveBlockCacheMaxBlocks)

	block, root, data, meta := mustStatusFixtureBlock(t)
	meta.GenUTime = 100
	if err := cache.PublishLiveBlockArtifacts(tnstore.LiveBlockArtifacts{
		Block:     block,
		Root:      root,
		BlockData: data,
		Meta:      meta,
		Proofs: []tnstore.LiveBlockProofArtifact{{
			Kind: tnstore.ServedProofBlock,
			Data: []byte{0x01},
		}},
	}); err != nil {
		t.Fatalf("publish live block: %v", err)
	}

	svc := &Service{
		storage:        store,
		liveBlockCache: cache,
	}
	snapshot := svc.recentTPSSnapshot(context.Background(), &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{Block: block},
		Shards:      map[tnstore.ShardKey]tnstore.BlockState{},
	}, statusTPSMasterWindow)

	if !snapshot.Complete {
		t.Fatalf("expected complete live TPS snapshot: %+v", snapshot)
	}
	if snapshot.WindowMasters != 1 {
		t.Fatalf("window masters = %d, want 1", snapshot.WindowMasters)
	}
	if snapshot.Transactions == 0 {
		t.Fatal("expected live transactions")
	}
	if snapshot.DurationSeconds != 1 {
		t.Fatalf("duration = %d, want 1", snapshot.DurationSeconds)
	}
	if snapshot.TPS <= 0 {
		t.Fatalf("tps = %f, want positive", snapshot.TPS)
	}
	if _, err := store.LookupBlockBySeqNo(context.Background(), tnstore.BlockHistoryKey{Workchain: -1, Shard: topShard}, block.SeqNo); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("pebble seqno lookup error = %v, want ErrNotFound", err)
	}
}

func mustStatusFixtureBlock(t *testing.T) (ton.BlockIDExt, *cell.Cell, []byte, *tnstore.BlockMeta) {
	t.Helper()

	rawFixture, err := os.ReadFile("testdata/masterchain_block_fixture.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var fixture blockFixture
	if err = json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	data, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		t.Fatalf("decode block boc base64: %v", err)
	}
	root, err := cell.FromBOC(data)
	if err != nil {
		t.Fatalf("parse block boc: %v", err)
	}

	rootHash, err := hex.DecodeString(fixture.Block.RootHashHex)
	if err != nil {
		t.Fatalf("decode root hash: %v", err)
	}
	fileHash, err := hex.DecodeString(fixture.Block.FileHashHex)
	if err != nil {
		t.Fatalf("decode file hash: %v", err)
	}
	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		t.Fatalf("parse shard: %v", err)
	}

	block := ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}
	meta, err := tnstore.BuildBlockMetaFromBlockData(block, data)
	if err != nil {
		t.Fatalf("build block meta: %v", err)
	}
	return block, root, data, meta
}
