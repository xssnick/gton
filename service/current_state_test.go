package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func testCurrentBlockStates(current *tnstore.CurrentState) []*tnstore.BlockState {
	if current == nil {
		return nil
	}

	states := make([]*tnstore.BlockState, 0, 1+len(current.Shards))
	states = append(states, tnstore.CloneBlockState(&current.Masterchain))
	for _, key := range tnstore.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		states = append(states, tnstore.CloneBlockState(&shard))
	}
	return states
}

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
	err := store.SaveStateCheckpoint(ctx, []*tnstore.BlockState{
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

func TestPublishCommittedCurrentStateDoesNotRegressStatus(t *testing.T) {
	svc := &Service{}

	newerMaster := testBlockID(-1, topShard, 101)
	olderMaster := testBlockID(-1, topShard, 100)
	newer := &tnstore.CurrentState{
		ShardClientSeqno: newerMaster.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: newerMaster,
		},
	}
	older := &tnstore.CurrentState{
		ShardClientSeqno: olderMaster.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: olderMaster,
		},
	}

	svc.publishCommittedCurrentState(newer)
	svc.publishCommittedCurrentState(older)

	svc.currentStatusMu.RLock()
	defer svc.currentStatusMu.RUnlock()
	if svc.currentStatus == nil {
		t.Fatal("current status is nil")
	}
	if got := svc.currentStatus.Masterchain.Block.SeqNo; got != newerMaster.SeqNo {
		t.Fatalf("current status regressed to seqno %d", got)
	}
}

func TestPersistArchiveCurrentStateReturnsSavedRoots(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)

	master := testBlockID(-1, topShard, 60)
	shard := testBlockID(0, topShard, 120)
	shardKey := tnstore.ShardKeyFromBlock(shard)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			shardKey: {
				Block:  shard,
				Cell:   testShardStateCell(t, shard),
				Parsed: &tlb.ShardStateUnsplit{},
			},
		},
	}
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store},
		ctx:     ctx,
	}

	persisted, err := runner.persistArchiveCurrentState(current, 1, 0, testCurrentBlockStates(current), nil, nil)
	if err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if persisted.Masterchain.Cell == nil {
		t.Fatal("persisted masterchain state lost saved root")
	}
	shardState := persisted.Shards[shardKey]
	if shardState.Cell == nil {
		t.Fatal("persisted shard state lost saved root")
	}
	if shardState.Parsed == nil {
		t.Fatal("persisted shard state lost parsed state")
	}
	if shardState.Parsed.Seqno != shard.SeqNo {
		t.Fatalf("persisted shard state was not reparsed from saved root, seqno=%d", shardState.Parsed.Seqno)
	}
}

func TestPersistArchiveCurrentStateStoresHistoricalAppliedStates(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)

	master := testBlockID(-1, topShard, 70)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	historical := &tnstore.BlockState{
		Block: testBlockID(0, topShard, 130),
		Cell:  testShardStateCell(t, testBlockID(0, topShard, 130)),
	}
	states := append([]*tnstore.BlockState{historical}, testCurrentBlockStates(current)...)
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store},
		ctx:     ctx,
	}

	if _, err := runner.persistArchiveCurrentState(current, 1, 0, states, nil, nil); err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if _, err := store.BlockState(ctx, historical.Block); err != nil {
		t.Fatalf("load historical archive state: %v", err)
	}
}

func TestPersistArchiveCurrentStateStoresImportedMetadataOnlyState(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)

	master := testBlockID(-1, topShard, 71)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	historicalBlock := testBlockID(0, topShard, 131)
	historicalRoot := testShardStateCell(t, historicalBlock)
	preparedCells, err := tnstore.PrepareReachableStateCells(historicalRoot)
	if err != nil {
		t.Fatalf("prepare historical cells: %v", err)
	}
	stateRootHash := historicalRoot.HashKey(0)
	stateCellHash := historicalRoot.HashKey()
	historical := &tnstore.BlockState{
		Block:         historicalBlock,
		StateRootHash: stateRootHash[:],
		StateCellHash: stateCellHash[:],
	}

	overlay := newArchiveStateCellOverlay(store.LazyCellLoader())
	currentCells, err := tnstore.PrepareReachableStateCells(current.Masterchain.Cell)
	if err != nil {
		t.Fatalf("prepare current cells: %v", err)
	}
	overlay.rememberPreparedCells(currentCells)
	overlay.rememberPreparedCells(preparedCells)
	checkpointCells := overlay.beginCheckpoint()
	states := append([]*tnstore.BlockState{historical}, testCurrentBlockStates(current)...)
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store},
		ctx:     ctx,
	}

	if _, err = runner.persistArchiveCurrentState(current, 1, 0, states, checkpointCells, nil); err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if _, err = store.BlockState(ctx, historical.Block); err != nil {
		t.Fatalf("load historical archive state: %v", err)
	}
	if _, err = store.LoadStateCellTree(ctx, historical.Block, historical.StateRootHash); err != nil {
		t.Fatalf("load historical archive state cells: %v", err)
	}
}

func TestArchiveCheckpointUsesAppliedStateCellsForRootValidation(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	overlayCells := mustPreparedReachableStateCells(t, root)
	delete(overlayCells, root.HashKey())

	overlay := newArchiveStateCellOverlay(nil)
	overlay.rememberPreparedCells(overlayCells)

	rootHash := root.HashKey(0)
	cellHash := root.HashKey()
	master := testBlockID(-1, topShard, 72)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			StateCellHash: cellHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	runner := &archiveCatchUpRunner{
		service:             svc,
		ctx:                 ctx,
		current:             current,
		lastCheckpointSeqno: master.SeqNo - 1,
		stateCells:          overlay,
	}
	runner.checkpointStates.rememberWithCells(&current.Masterchain, preparedCells)
	if _, err := runner.startCheckpoint("test"); err != nil {
		t.Fatalf("start checkpoint: %v", err)
	}
	if _, err := runner.finishCheckpoint(true); err != nil {
		t.Fatalf("finish checkpoint: %v", err)
	}
	svc.Wait()

	if _, err := store.LoadStateCellTree(ctx, master, rootHash[:]); err != nil {
		t.Fatalf("load persisted checkpoint state cells: %v", err)
	}
}

func TestNextBlockCheckpointUsesAppliedStateCellsForRootValidation(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store, shutdownContext: ctx}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	overlayCells := mustPreparedReachableStateCells(t, root)
	delete(overlayCells, root.HashKey())

	rootHash := root.HashKey(0)
	cellHash := root.HashKey()
	master := testBlockID(-1, topShard, 73)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			StateCellHash: cellHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	stateCells := newStateCellWindowCache(nil)
	stateCells.addPreparedRecords(overlayCells)
	runner := &nextSyncRunner{
		service:      svc,
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   stateCells,
	}
	runner.checkpointStates.rememberWithCells(&current.Masterchain, preparedCells)
	if err := runner.flushStagedCurrentSync("test"); err != nil {
		t.Fatalf("flush staged next-block current: %v", err)
	}

	if _, err := store.LoadStateCellTree(ctx, master, rootHash[:]); err != nil {
		t.Fatalf("load persisted next-block checkpoint state cells: %v", err)
	}
}

func TestFlushStagedCurrentAsyncFailureKeepsCheckpointStates(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store, shutdownContext: ctx}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	delete(preparedCells, root.HashKey())

	rootHash := root.HashKey(0)
	cellHash := root.HashKey()
	master := testBlockID(-1, topShard, 74)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			StateCellHash: cellHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	runner := &nextSyncRunner{
		service:      svc,
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   newStateCellWindowCache(nil),
	}
	runner.checkpointStates.rememberWithCells(&current.Masterchain, preparedCells)
	if err := runner.flushStagedCurrent(); err != nil {
		t.Fatalf("schedule staged current flush: %v", err)
	}
	svc.Wait()

	if err := svc.takeCurrentStatePersistError(); err == nil {
		t.Fatal("expected async checkpoint persist error")
	}
	remaining := runner.checkpointStates.clone()
	if len(remaining) != 1 {
		t.Fatalf("remaining checkpoint states = %d, want 1", len(remaining))
	}
	if !remaining[0].Block.Equals(&master) {
		t.Fatalf("remaining checkpoint block = %s, want %s", tnstore.FormatBlockRef(remaining[0].Block), tnstore.FormatBlockRef(master))
	}
}

func TestArchiveCheckpointReleasesRetainedCellLoaderOnPersistFailure(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	delete(preparedCells, root.HashKey())

	overlay := newArchiveStateCellOverlay(nil)
	overlay.rememberPreparedCells(preparedCells)

	rootHash := root.HashKey(0)
	cellHash := root.HashKey()
	master := testBlockID(-1, topShard, 72)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			StateCellHash: cellHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	runner := &archiveCatchUpRunner{
		service:             svc,
		ctx:                 ctx,
		current:             current,
		lastCheckpointSeqno: master.SeqNo - 1,
		stateCells:          overlay,
	}
	runner.checkpointStates.remember(&current.Masterchain)
	if _, err := runner.startCheckpoint("test"); err != nil {
		t.Fatalf("start checkpoint: %v", err)
	}
	if _, err := runner.finishCheckpoint(true); err == nil {
		t.Fatal("checkpoint with missing prepared root succeeded")
	}
	svc.Wait()

	svc.stateCellLoaderMu.RLock()
	defer svc.stateCellLoaderMu.RUnlock()
	if len(svc.stateCellLoaders) != 0 {
		t.Fatalf("failed archive checkpoint left %d retained state cell loaders", len(svc.stateCellLoaders))
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
	for _, state := range testCurrentBlockStates(current) {
		runner.checkpointStates.remember(state)
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
	for _, state := range testCurrentBlockStates(current) {
		runner.checkpointStates.remember(state)
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
