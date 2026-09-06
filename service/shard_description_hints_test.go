package service

import (
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestPruneShardDescriptionHintsDropsOverflowInOneBatch(t *testing.T) {
	now := time.Unix(1000, 0)
	svc := &SyncCoordinator{
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

	gen := svc.pruneShardDescriptionHintsLocked(now)

	if gen.count != shardDescriptionHintLimit {
		t.Fatalf("generation count = %d, want %d", gen.count, shardDescriptionHintLimit)
	}
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

func TestPruneShardDescriptionHintsGenerationSurvivesEvictingNewestHint(t *testing.T) {
	now := time.Now()
	svc := &SyncCoordinator{
		shardDescriptionHints: map[storage.BlockRootHash]shardDescriptionHint{},
	}

	total := shardDescriptionHintLimit + 1
	for i := 0; i < total; i++ {
		block := testBlockID(0, topShard, uint32(i+1))
		key := storage.BlockKey(block)
		svc.shardDescriptionOrder = append(svc.shardDescriptionOrder, key)
		svc.shardDescriptionHints[key] = shardDescriptionHint{
			Description: p2p.ShardBlockDescription{Block: block},
			ReceivedAt:  now,
			seq:         uint64(i + 1),
		}
	}
	// The oldest block was re-described last: its hint carries the newest
	// sequence while keeping its place at the front of the eviction order.
	oldest := storage.BlockKey(testBlockID(0, topShard, 1))
	hint := svc.shardDescriptionHints[oldest]
	hint.seq = uint64(total + 1)
	svc.shardDescriptionHints[oldest] = hint

	gen := svc.pruneShardDescriptionHintsLocked(now)

	if _, ok := svc.shardDescriptionHints[oldest]; ok {
		t.Fatal("re-described oldest block survived the overflow eviction")
	}
	want := shardDescriptionHintGeneration{count: shardDescriptionHintLimit, maxSeq: uint64(total)}
	if gen != want {
		t.Fatalf("generation = %+v, want %+v", gen, want)
	}
}

func TestShardDescriptionHintSnapshotSkipsUnchangedTable(t *testing.T) {
	now := time.Now()
	svc := &SyncCoordinator{
		shardDescriptionHints: map[storage.BlockRootHash]shardDescriptionHint{},
	}
	first := testBlockID(0, topShard, 11)
	second := testBlockID(0, topShard, 12)
	svc.storeShardDescriptionHint(shardDescriptionHint{Description: p2p.ShardBlockDescription{Block: first}, ReceivedAt: now})
	svc.storeShardDescriptionHint(shardDescriptionHint{Description: p2p.ShardBlockDescription{Block: second}, ReceivedAt: now})

	hints, gen, changed := svc.shardDescriptionHintSnapshot(now, nil, shardDescriptionHintGeneration{})
	if !changed || len(hints) != 2 {
		t.Fatalf("first snapshot: changed=%v len=%d, want changed with 2 hints", changed, len(hints))
	}
	if gen.count != 2 || gen.maxSeq != hints[1].seq || hints[1].seq <= hints[0].seq {
		t.Fatalf("generation = %+v for seqs %d,%d", gen, hints[0].seq, hints[1].seq)
	}

	again, sameGen, changed := svc.shardDescriptionHintSnapshot(now, hints, gen)
	if changed || sameGen != gen {
		t.Fatalf("unchanged table: changed=%v gen=%+v, want unchanged %+v", changed, sameGen, gen)
	}
	if len(again) != 2 || &again[0] != &hints[0] {
		t.Fatal("snapshot of an unchanged table did not hand back the caller's slice")
	}

	// Re-describing a known block replaces its hint in place and is a change.
	svc.storeShardDescriptionHint(shardDescriptionHint{
		Description: p2p.ShardBlockDescription{Block: first, CatchainSeqno: 7},
		ReceivedAt:  now,
	})
	hints, gen, changed = svc.shardDescriptionHintSnapshot(now, hints, gen)
	if !changed || len(hints) != 2 || hints[0].Description.CatchainSeqno != 7 {
		t.Fatalf("replaced hint: changed=%v len=%d catchain=%d", changed, len(hints), hints[0].Description.CatchainSeqno)
	}
	if gen.count != 2 || gen.maxSeq != hints[0].seq {
		t.Fatalf("generation after replacement = %+v, want maxSeq %d", gen, hints[0].seq)
	}

	// Dropping a hint lowers the count, and the reused scratch must not keep
	// the dropped hint alive past the new length.
	svc.dropShardDescriptionHint(second)
	hints, gen, changed = svc.shardDescriptionHintSnapshot(now, hints, gen)
	if !changed || len(hints) != 1 || gen.count != 1 {
		t.Fatalf("after drop: changed=%v len=%d gen=%+v", changed, len(hints), gen)
	}
	if !hints[0].Description.Block.Equals(&first) {
		t.Fatalf("surviving hint = %s, want %s", storage.FormatBlockRef(hints[0].Description.Block), storage.FormatBlockRef(first))
	}
	for _, stale := range hints[len(hints):cap(hints)] {
		if stale.Description.Block.RootHash != nil {
			t.Fatal("scratch past the snapshot length still references a dropped hint")
		}
	}

	// Expiry empties the table; an empty table has the zero generation, and
	// stays unchanged afterwards.
	later := now.Add(shardDescriptionHintTTL + time.Second)
	hints, gen, changed = svc.shardDescriptionHintSnapshot(later, hints, gen)
	if !changed || len(hints) != 0 || gen != (shardDescriptionHintGeneration{}) {
		t.Fatalf("after expiry: changed=%v len=%d gen=%+v", changed, len(hints), gen)
	}
	if _, _, changed = svc.shardDescriptionHintSnapshot(later, hints, gen); changed {
		t.Fatal("empty table reported as changed")
	}
}

func TestRememberShardDescriptionHintSkipsAfterSyncUntilFrozen(t *testing.T) {
	block := testBlockID(0, topShard, 10)
	svc := &SyncCoordinator{
		node:      newFrozenTestNode(t),
		syncUntil: 200,
	}
	freezeSyncUntil(t, svc)

	svc.rememberShardDescriptionHint(p2p.BroadcastEvent{
		Block:                block,
		Kind:                 "tonNode.newShardBlockBroadcast",
		ShardDescriptionRoot: cell.BeginCell().EndCell(),
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

	cloned, err := cloneShardBlockDescription(desc)
	if err != nil {
		t.Fatal(err)
	}
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
