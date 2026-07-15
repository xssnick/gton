package service

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestPruneShardDescriptionHintsDropsOverflowInOneBatch(t *testing.T) {
	now := time.Unix(1000, 0)
	svc := &Service{
		shardDescriptionHints: map[storage.BlockRootHash]shardDescriptionHint{},
	}

	total := shardDescriptionHintLimit + 3
	for i := 0; i < total; i++ {
		block := testBlockID(0, topShard, uint32(i+1))
		key := storage.BlockKey(block)
		svc.shardDescriptionOrder = append(svc.shardDescriptionOrder, key)
		svc.shardDescriptionHints[key] = shardDescriptionHint{
			Description: p2p.ShardBlockDescription{Block: block},
			ReceivedAt:  now,
		}
	}

	svc.pruneShardDescriptionHintsLocked(now)

	if len(svc.shardDescriptionOrder) != shardDescriptionHintLimit {
		t.Fatalf("order length = %d, want %d", len(svc.shardDescriptionOrder), shardDescriptionHintLimit)
	}
	if len(svc.shardDescriptionHints) != shardDescriptionHintLimit {
		t.Fatalf("hint count = %d, want %d", len(svc.shardDescriptionHints), shardDescriptionHintLimit)
	}

	for i := 0; i < 3; i++ {
		key := storage.BlockKey(testBlockID(0, topShard, uint32(i+1)))
		if _, ok := svc.shardDescriptionHints[key]; ok {
			t.Fatalf("overflow block #%d was retained", i+1)
		}
	}
	firstKept := storage.BlockKey(testBlockID(0, topShard, 4))
	if svc.shardDescriptionOrder[0] != firstKept {
		t.Fatalf("first retained key = %x, want %x", svc.shardDescriptionOrder[0], firstKept)
	}
}

func TestRememberShardDescriptionHintSkipsAfterSyncUntilFrozen(t *testing.T) {
	block := testBlockID(0, topShard, 10)
	svc := &Service{
		node:      newFrozenTestNode(t),
		syncUntil: 200,
	}

	svc.rememberShardDescriptionHint(p2p.BroadcastEvent{
		Block: block,
		Kind:  "tonNode.newShardBlockBroadcast",
		ShardDescription: &p2p.ShardBlockDescription{
			Block: block,
		},
	})

	if len(svc.shardDescriptionHints) != 0 {
		t.Fatalf("shard description hints = %d, want 0", len(svc.shardDescriptionHints))
	}
}

func TestCloneShardBlockDescriptionCopiesBlockIDs(t *testing.T) {
	block := testBlockID(0, topShard, 30)
	prev := testBlockID(0, topShard, 29)
	master := testBlockID(-1, topShard, 30)
	desc := &p2p.ShardBlockDescription{
		Block: block,
		Chain: []p2p.ShardDescriptionLink{{
			Block:          block,
			PrevRefs:       []ton.BlockIDExt{prev},
			MasterchainRef: &master,
		}},
	}

	cloned := cloneShardBlockDescription(desc)
	cloned.Block.RootHash[0] = 0xA1
	cloned.Chain[0].Block.RootHash[0] = 0xA2
	cloned.Chain[0].PrevRefs[0].RootHash[0] = 0xA3
	cloned.Chain[0].MasterchainRef.RootHash[0] = 0xA4

	if desc.Block.RootHash[0] == 0xA1 {
		t.Fatal("clone shares description block root hash backing array")
	}
	if desc.Chain[0].Block.RootHash[0] == 0xA2 {
		t.Fatal("clone shares link block root hash backing array")
	}
	if desc.Chain[0].PrevRefs[0].RootHash[0] == 0xA3 {
		t.Fatal("clone shares prev ref root hash backing array")
	}
	if desc.Chain[0].MasterchainRef.RootHash[0] == 0xA4 {
		t.Fatal("clone shares masterchain ref root hash backing array")
	}
}
