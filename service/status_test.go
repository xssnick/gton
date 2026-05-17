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
		StateCellHash: master.RootHash,
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
		StateCellHash: storedMaster.RootHash,
		Parsed:        &tlb.ShardStateUnsplit{GenUTime: 100},
	}}, &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:         storedMaster,
			StateRootHash: storedMaster.RootHash,
			StateCellHash: storedMaster.RootHash,
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

func TestObserveCurrentSyncLagReportsMasterchainAndSlowestShard(t *testing.T) {
	observer := &fakeSyncLagObserver{values: map[string]float64{}}
	svc := &Service{syncLag: observer}

	svc.observeCurrentSyncLag(context.Background(), &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 990},
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: topShard}: {
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 980},
			},
			{Workchain: 0, Shard: topShard >> 1}: {
				Parsed: &tlb.ShardStateUnsplit{GenUTime: 995},
			},
		},
	}, time.Unix(1000, 0))

	if got := observer.values[syncLagChainMasterchain]; got != 10 {
		t.Fatalf("masterchain lag = %v, want 10", got)
	}
	if got := observer.values[syncLagChainShardchain]; got != 20 {
		t.Fatalf("shardchain lag = %v, want 20", got)
	}
}

type fakeSyncLagObserver struct {
	values map[string]float64
}

func (o *fakeSyncLagObserver) SetSyncLag(chain string, seconds float64) {
	o.values[chain] = seconds
}
