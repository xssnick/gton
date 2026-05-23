package service

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
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
	svc.publishLiveCurrentState(&tnstore.CurrentState{
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
	svc.publishLiveCurrentState(&tnstore.CurrentState{
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

func TestObserveCurrentSyncStateReportsMasterchainAndShardClientSeqno(t *testing.T) {
	observer := &fakeSyncObserver{}
	svc := &Service{sync: observer}

	svc.observeCurrentSyncState(&tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:  testBlockID(-1, topShard, 100),
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 990},
		},
		ShardClientSeqno: 90,
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: topShard}: {
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 980},
			},
			{Workchain: 0, Shard: topShard >> 1}: {
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 995},
			},
		},
	})

	if got := observer.current.MasterchainSeqno; got != 100 {
		t.Fatalf("masterchain seqno = %v, want 100", got)
	}
	if got := observer.current.ShardClientSeqno; got != 90 {
		t.Fatalf("shard client seqno = %v, want 90", got)
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

type fakeSyncObserver struct {
	current SyncCurrentStateObservation
}

func (o *fakeSyncObserver) ObserveSyncCurrentState(observation SyncCurrentStateObservation) {
	o.current = observation
}

func (o *fakeSyncObserver) ObserveSyncBlock(SyncBlockObservation) {}

func (o *fakeSyncObserver) ObserveSyncPersist(SyncPersistObservation) {}
