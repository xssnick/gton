package service

import (
	"context"
	"testing"

	"flexserver/service/p2p"
	tnstore "flexserver/service/storage"
	"flexserver/service/storage/memstore"

	"github.com/xssnick/tonutils-go/ton"
)

func TestStatusSnapshotIncludesLocalChainProgress(t *testing.T) {
	store := memstore.New()
	node, err := p2p.New(p2p.Options{Storage: store})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 40}
	base := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 77}
	err = store.SaveCurrentState(context.Background(), &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{Block: master},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			{Workchain: 0, Shard: topShard}: {Block: base},
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
}
