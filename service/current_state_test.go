package service

import (
	"context"
	"testing"

	tnstore "flexserver/service/storage"
	"flexserver/service/storage/memstore"

	"github.com/xssnick/tonutils-go/ton"
)

func TestPersistShardBlockStateDoesNotMoveResumePointer(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	svc := &Service{storage: store}

	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 50}
	base := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 100}
	key := tnstore.ShardKeyFromBlock(base)
	err := store.SaveCurrentState(ctx, &tnstore.CurrentState{
		ShardClientSeqno: master.SeqNo,
		Masterchain:      tnstore.BlockState{Block: master},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			key: {Block: base},
		},
	})
	if err != nil {
		t.Fatalf("save current state: %v", err)
	}

	next := &tnstore.BlockState{Block: ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 101}}
	if err = svc.persistShardBlockState(ctx, next); err != nil {
		t.Fatalf("persist next shard: %v", err)
	}

	current, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current state: %v", err)
	}
	if got := current.Shards[key].Block.SeqNo; got != 100 {
		t.Fatalf("unexpected current shard seqno %d", got)
	}
	if current.ShardClientSeqno != master.SeqNo {
		t.Fatalf("unexpected shard client seqno %d", current.ShardClientSeqno)
	}
	if _, err = store.BlockState(ctx, next.Block); err != nil {
		t.Fatalf("load persisted next shard: %v", err)
	}
}
