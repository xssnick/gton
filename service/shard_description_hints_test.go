package service

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
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
			Block:      block,
			ReceivedAt: now,
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
