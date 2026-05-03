package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"flexserver/service/p2p"
	tnstore "flexserver/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

func TestSaveBlockStateDoesNotMoveCurrentState(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	master := testBlockID(-1, topShard, 50)
	base := testBlockID(0, topShard, 100)
	key := tnstore.ShardKeyFromBlock(base)
	masterState := &tnstore.BlockState{
		Block:         master,
		StateRootHash: master.RootHash,
		StateCellHash: master.RootHash,
	}
	baseState := &tnstore.BlockState{
		Block: base,
		Cell:  testShardStateCell(t, base),
	}
	err := store.SaveBlockStatesAndCurrentState(ctx, []*tnstore.BlockState{
		masterState,
		baseState,
	}, &tnstore.CurrentState{
		ShardClientSeqno: master.SeqNo,
		Masterchain:      *masterState,
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			key: *baseState,
		},
	})
	if err != nil {
		t.Fatalf("save current state: %v", err)
	}

	next := &tnstore.BlockState{
		Block: testBlockID(0, topShard, 101),
	}
	next.Cell = testShardStateCell(t, next.Block)
	if err = store.SaveBlockState(ctx, next); err != nil {
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

func TestCurrentStateForNextMasterStateUsesExactShardTarget(t *testing.T) {
	ctx := context.Background()
	master := &tnstore.BlockState{
		Block:         testBlockID(-1, topShard, 51),
		StateRootHash: bytes32(0x51),
		StateCellHash: bytes32(0x52),
	}
	target := testBlockID(0, topShard, 101)
	ahead := testBlockID(0, topShard, 102)
	key := tnstore.ShardKeyFromBlock(target)
	current := &tnstore.CurrentState{
		ShardClientSeqno: master.Block.SeqNo - 1,
		Masterchain: tnstore.BlockState{
			Block:         testBlockID(-1, topShard, 50),
			StateRootHash: bytes32(0x50),
			StateCellHash: bytes32(0x50),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			key: {
				Block: ahead,
				Cell:  testShardStateCell(t, ahead),
			},
		},
	}

	env := newFakeShardStateResolverEnv()
	env.addState(target)
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current:   current.Shards,
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})

	next, _, err := (&Service{log: zerolog.Nop()}).currentStateForNextMasterState(ctx, current, master, []ton.BlockIDExt{target}, resolver)
	if err != nil {
		t.Fatalf("build next current state: %v", err)
	}
	got := next.Shards[key].Block
	if !got.Equals(&target) {
		t.Fatalf("next shard = %s, want exact target %s", tnstore.FormatBlockRef(got), tnstore.FormatBlockRef(target))
	}
	if loads := env.stateLoads[tnstore.BlockKey(target)]; loads != 1 {
		t.Fatalf("target state loads = %d, want 1", loads)
	}
	if loads := env.blockLoads[tnstore.BlockKey(target)]; loads != 0 {
		t.Fatalf("target block loads = %d, want 0", loads)
	}
}

func TestVerifiedMasterchainQueueAcceptsBroadcastOutsideCatchUp(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	downloaded := p2p.DownloadedBlock{
		ID: next,
		Meta: &tnstore.BlockMeta{
			ID:       next,
			PrevRefs: []ton.BlockIDExt{prev},
		},
	}

	svc := &Service{}
	svc.queueVerifiedMasterchainBlock(downloaded)

	got, ok := svc.takeQueuedMasterchainBlock(prev, next)
	if !ok {
		t.Fatal("expected queued masterchain block to be available")
	}
	if !got.ID.Equals(&next) {
		t.Fatalf("queued block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(next))
	}
}

func TestVerifiedMasterchainQueueEvictsOldestPrevSeqno(t *testing.T) {
	svc := &Service{}
	for seqno := uint32(10); seqno < 10+nextMasterchainQueueLimit; seqno++ {
		prev := testMasterBlockID(seqno)
		next := testMasterBlockID(seqno + 1)
		svc.queueVerifiedMasterchainBlock(p2p.DownloadedBlock{
			ID: next,
			Meta: &tnstore.BlockMeta{
				ID:       next,
				PrevRefs: []ton.BlockIDExt{prev},
			},
		})
	}

	oldestPrev := testMasterBlockID(10)
	newPrev := testMasterBlockID(1000)
	newNext := testMasterBlockID(1001)
	svc.queueVerifiedMasterchainBlock(p2p.DownloadedBlock{
		ID: newNext,
		Meta: &tnstore.BlockMeta{
			ID:       newNext,
			PrevRefs: []ton.BlockIDExt{newPrev},
		},
	})

	if _, ok := svc.takeQueuedMasterchainBlock(oldestPrev, testMasterBlockID(11)); ok {
		t.Fatal("expected oldest queued masterchain block to be evicted")
	}
	if got, ok := svc.takeQueuedMasterchainBlock(newPrev, newNext); !ok || !got.ID.Equals(&newNext) {
		t.Fatalf("expected newest queued masterchain block, got ok=%v block=%s", ok, tnstore.FormatBlockRef(got.ID))
	}
}

func TestCurrentStateWakeInterruptsLivePollDelay(t *testing.T) {
	svc := &Service{currentStateWake: make(chan struct{}, 1)}
	svc.wakeCurrentStateSync()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()
	if !svc.waitCurrentStatePoll(ctx, time.Hour) {
		t.Fatal("expected current state poll wake")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("wake took %s", elapsed)
	}
}

func TestFlushStagedCurrentSyncPersistsAfterContextCancel(t *testing.T) {
	store := openTestPebbleStorage(t)
	current, baseKey, master, base := testCurrentStateForShutdownFlush(t, 70, 120)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	defer cancelShutdown()

	runner := &nextSyncRunner{
		service: &Service{
			log:             zerolog.Nop(),
			storage:         store,
			shutdownContext: shutdownCtx,
		},
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
	}

	if err := runner.flushStagedCurrentSync("test_shutdown"); err != nil {
		t.Fatalf("flush staged current after cancel: %v", err)
	}
	if runner.stagedBlocks != 0 {
		t.Fatalf("staged blocks = %d, want 0", runner.stagedBlocks)
	}

	persisted, err := store.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("load persisted current state: %v", err)
	}
	if !persisted.Masterchain.Block.Equals(&master) {
		t.Fatalf("persisted masterchain = %s, want %s", tnstore.FormatBlockRef(persisted.Masterchain.Block), tnstore.FormatBlockRef(master))
	}
	if persisted.ShardClientSeqno != master.SeqNo {
		t.Fatalf("persisted shard client seqno = %d, want %d", persisted.ShardClientSeqno, master.SeqNo)
	}
	if got := persisted.Shards[baseKey].Block; !got.Equals(&base) {
		t.Fatalf("persisted basechain = %s, want %s", tnstore.FormatBlockRef(got), tnstore.FormatBlockRef(base))
	}
}

func TestFlushStagedCurrentSyncStopsWhenShutdownContextCanceled(t *testing.T) {
	store := openTestPebbleStorage(t)
	current, _, _, _ := testCurrentStateForShutdownFlush(t, 71, 121)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	cancelShutdown()

	runner := &nextSyncRunner{
		service: &Service{
			log:             zerolog.Nop(),
			storage:         store,
			shutdownContext: shutdownCtx,
		},
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
	}

	err := runner.flushStagedCurrentSync("test_shutdown")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("flush staged current error = %v, want context canceled", err)
	}
	if runner.stagedBlocks != 1 {
		t.Fatalf("staged blocks = %d, want 1", runner.stagedBlocks)
	}
	if _, err = store.CurrentState(context.Background()); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("current state error = %v, want not found", err)
	}
}

func testCurrentStateForShutdownFlush(t *testing.T, masterSeqno, baseSeqno uint32) (*tnstore.CurrentState, tnstore.ShardKey, ton.BlockIDExt, ton.BlockIDExt) {
	t.Helper()

	master := testBlockID(-1, topShard, masterSeqno)
	base := testBlockID(0, topShard, baseSeqno)
	baseKey := tnstore.ShardKeyFromBlock(base)

	return &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: master.RootHash,
			StateCellHash: master.RootHash,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			baseKey: {
				Block: base,
				Cell:  testShardStateCell(t, base),
			},
		},
	}, baseKey, master, base
}
