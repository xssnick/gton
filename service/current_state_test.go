package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func testPeerID(label string) p2p.PeerID {
	return p2p.PeerID(sha256.Sum256([]byte(label)))
}

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

func testStateCheckpointEntries(states []*tnstore.BlockState) []tnstore.StateCheckpointBlock {
	entries := make([]tnstore.StateCheckpointBlock, 0, len(states))
	for _, state := range states {
		if state == nil {
			continue
		}
		entries = append(entries, tnstore.StateCheckpointBlock{State: state})
	}
	return entries
}

func openManualTestPebbleStorage(t *testing.T) *pebblestore.Store {
	t.Helper()

	store, err := pebblestore.Open(pebblestore.Options{
		Dir: filepath.Join(t.TempDir(), "storage"),
	})
	if err != nil {
		t.Fatalf("open pebble storage: %v", err)
	}
	return store
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

func TestMarkLiveCheckpointStatesFlushedPublishesAllEntries(t *testing.T) {
	flusher := &testLiveCheckpointFlusher{}
	svc := &Service{liveState: flusher}
	master := testBlockID(-1, topShard, 102)
	shard := testBlockID(0, topShard, 103)

	svc.markLiveCheckpointStatesFlushed([]tnstore.StateCheckpointBlock{
		{State: &tnstore.BlockState{Block: master}},
		{},
		{State: &tnstore.BlockState{Block: shard}},
	})

	if len(flusher.blocks) != 2 {
		t.Fatalf("flushed blocks = %d, want 2", len(flusher.blocks))
	}
	if !flusher.blocks[0].Equals(&master) {
		t.Fatalf("first flushed block = %s, want %s", tnstore.FormatBlockRef(flusher.blocks[0]), tnstore.FormatBlockRef(master))
	}
	if !flusher.blocks[1].Equals(&shard) {
		t.Fatalf("second flushed block = %s, want %s", tnstore.FormatBlockRef(flusher.blocks[1]), tnstore.FormatBlockRef(shard))
	}
}

type testLiveCheckpointFlusher struct {
	current *tnstore.CurrentState
	blocks  []ton.BlockIDExt
}

func (f *testLiveCheckpointFlusher) SetLiveCurrentState(current *tnstore.CurrentState) {
	f.current = current
}

func (f *testLiveCheckpointFlusher) MarkLiveBlockStatesFlushed(blocks []ton.BlockIDExt) {
	f.blocks = append(f.blocks, blocks...)
}

func TestSyncBlockResultForError(t *testing.T) {
	timeout := &testTimeoutError{}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", want: "success"},
		{name: "canceled", err: context.Canceled, want: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "timeout"},
		{name: "timeout", err: timeout, want: "timeout"},
		{name: "block miss", err: p2p.ErrBlockNotAvailable, want: "miss"},
		{name: "state miss", err: p2p.ErrStateNotAvailable, want: "miss"},
		{name: "retry", err: errPersistentStateGCActive, want: "retry"},
		{name: "error", err: errors.New("boom"), want: "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := syncBlockResultForError(tc.err); got != tc.want {
				t.Fatalf("result = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSyncBlockSourceForDownloadedBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		want string
	}{
		{name: "plain overlay download", kind: "tonNode.dataFull", want: "next_block"},
		{name: "broadcast cache", kind: "tonNode.blockBroadcastCompressedV2", want: "broadcast_cache"},
		{name: "shard description broadcast hint", kind: "tonNode.newShardBlockBroadcast", want: "broadcast_hint"},
		{name: "stored full block", kind: "local full block cache", want: "stored"},
		{name: "stored next block", kind: "local next block cache", want: "stored"},
		{name: "stored block data", kind: "stored block", want: "stored"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := syncBlockSourceForDownloadedBlock("next_block", p2p.DownloadedBlock{Kind: tc.kind})
			if got != tc.want {
				t.Fatalf("source = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSyncBlockOriginTreatsBroadcastHintAsBroadcast(t *testing.T) {
	if got := syncBlockOriginForSource("broadcast_hint"); got != "broadcast" {
		t.Fatalf("origin = %q, want broadcast", got)
	}
}

func TestStateRootForCompressedBlockUsesLiveShardState(t *testing.T) {
	block := testBlockID(0, topShard, 123)
	root := testShardStateCell(t, block)
	svc := &Service{
		currentStatus: &tnstore.CurrentState{
			Shards: map[tnstore.ShardKey]tnstore.BlockState{
				tnstore.ShardKeyFromBlock(block): {
					Block: block,
					Cell:  root,
				},
			},
		},
	}

	got, err := svc.StateRootForCompressedBlock(context.Background(), block)
	if err != nil {
		t.Fatalf("state root error = %v", err)
	}
	if got != root {
		t.Fatal("state root was not taken from live shard state")
	}
}

func TestStateRootForCompressedBlockUsesRecentlyAppliedShardState(t *testing.T) {
	block := testBlockID(0, topShard, 124)
	root := testShardStateCell(t, block)
	svc := &Service{}

	if !svc.rememberCompressedBlockState(&tnstore.BlockState{
		Block: block,
		Cell:  root,
	}) {
		t.Fatal("state root was not remembered")
	}

	got, err := svc.StateRootForCompressedBlock(context.Background(), block)
	if err != nil {
		t.Fatalf("state root error = %v", err)
	}
	if got != root {
		t.Fatal("state root was not taken from recently applied shard state")
	}
}

func TestCurrentBroadcastValidatorConfigUsesLiveCurrentState(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	persistedMaster := testBlockID(-1, topShard, 120)
	liveMaster := testBlockID(-1, topShard, 121)

	if err := store.SaveCurrentState(ctx, &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{Block: persistedMaster},
		Shards:      map[tnstore.ShardKey]tnstore.BlockState{},
	}); err != nil {
		t.Fatalf("save persisted current state: %v", err)
	}

	svc := &Service{
		storage: store,
		currentStatus: &tnstore.CurrentState{
			Masterchain: tnstore.BlockState{Block: liveMaster},
			Shards:      map[tnstore.ShardKey]tnstore.BlockState{},
		},
		masterStateCache: map[tnstore.BlockRootHash]*tnstore.BlockState{
			tnstore.BlockKey(liveMaster): {Block: liveMaster},
		},
	}

	_, err := svc.currentBroadcastValidatorConfig(ctx)
	if err == nil {
		t.Fatal("validator config unexpectedly loaded from incomplete test state")
	}
	if !strings.Contains(err.Error(), tnstore.FormatBlockRef(liveMaster)) {
		t.Fatalf("validator config error = %q, want live master %s", err, tnstore.FormatBlockRef(liveMaster))
	}
	if strings.Contains(err.Error(), tnstore.FormatBlockRef(persistedMaster)) {
		t.Fatalf("validator config used persisted current state: %v", err)
	}
}

func TestShardPrefetchDoesNotMarkScheduledWhenSlotUnavailable(t *testing.T) {
	runner := &nextSyncRunner{
		service:            &Service{node: &p2p.Node{}},
		ctx:                context.Background(),
		shardPrefetchSlots: make(chan struct{}, 1),
	}
	runner.shardPrefetchSlots <- struct{}{}

	scheduled := map[tnstore.BlockRootHash]struct{}{}
	scheduledOrder := []tnstore.BlockRootHash{}
	master := testBlockID(-1, topShard, 125)
	target := testBlockID(0, topShard, 126)

	got := runner.scheduleShardPrefetch(scheduled, &scheduledOrder, master, []ton.BlockIDExt{target})
	if got != 0 {
		t.Fatalf("scheduled prefetch count = %d, want 0", got)
	}
	if _, ok := scheduled[tnstore.BlockKey(target)]; ok {
		t.Fatal("prefetch target was marked scheduled without an available worker slot")
	}
	if len(scheduledOrder) != 0 {
		t.Fatalf("scheduled order length = %d, want 0", len(scheduledOrder))
	}
}

func TestShardDescriptionPrefetchReportsUnavailableSlot(t *testing.T) {
	runner := &nextSyncRunner{
		service:            &Service{node: &p2p.Node{}},
		ctx:                context.Background(),
		shardPrefetchSlots: make(chan struct{}, 1),
	}
	runner.shardPrefetchSlots <- struct{}{}

	desc := &p2p.ShardBlockDescription{Block: testBlockID(0, topShard, 127)}
	if runner.prefetchShardDescriptionTarget(shardDescriptionHint{Overlay: "test"}, desc) {
		t.Fatal("description prefetch started without an available worker slot")
	}
}

type testTimeoutError struct{}

func (e *testTimeoutError) Error() string {
	return "timeout"
}

func (e *testTimeoutError) Timeout() bool {
	return true
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

	persisted, err := runner.persistArchiveCurrentState(current, 1, 0, testStateCheckpointEntries(testCurrentBlockStates(current)), nil)
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

	if _, err := runner.persistArchiveCurrentState(current, 1, 0, testStateCheckpointEntries(states), nil); err != nil {
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
	historical := &tnstore.BlockState{
		Block:         historicalBlock,
		StateRootHash: stateRootHash[:],
	}

	overlay := newArchiveStateCellOverlay(store.LazyCellLoader())
	currentCells, err := tnstore.PrepareReachableStateCells(current.Masterchain.Cell)
	if err != nil {
		t.Fatalf("prepare current cells: %v", err)
	}
	if err = overlay.rememberPrepared(current.Masterchain.Cell, currentCells, nil, 0); err != nil {
		t.Fatalf("remember current cells: %v", err)
	}
	if err = overlay.rememberPrepared(historicalRoot, preparedCells, nil, 0); err != nil {
		t.Fatalf("remember historical cells: %v", err)
	}
	checkpointCells := overlay.beginCheckpoint()
	entries := append([]tnstore.StateCheckpointBlock{{
		State: historical,
	}}, testStateCheckpointEntries(testCurrentBlockStates(current))...)
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store},
		ctx:     ctx,
	}

	if _, err = runner.persistArchiveCurrentState(current, 1, 0, entries, checkpointCells); err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if _, err = store.BlockState(ctx, historical.Block); err != nil {
		t.Fatalf("load historical archive state: %v", err)
	}
	if _, err = store.LoadStateCellTree(ctx, historical.Block, historical.StateRootHash); err != nil {
		t.Fatalf("load historical archive state cells: %v", err)
	}
}

func TestArchiveCheckpointPersistsEntryStateCellsBeforeMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	overlay := newArchiveStateCellOverlay(nil)
	if err := overlay.rememberPrepared(root, mustPreparedReachableStateCells(t, root), nil, 0); err != nil {
		t.Fatalf("remember checkpoint cells: %v", err)
	}

	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 72)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
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
	if _, err := runner.finishCheckpoint(true); err != nil {
		t.Fatalf("finish checkpoint: %v", err)
	}
	svc.Wait()

	if _, err := store.LoadStateCellTree(ctx, master, rootHash[:]); err != nil {
		t.Fatalf("load persisted checkpoint state cells: %v", err)
	}
}

func TestNextBlockCheckpointPersistsEntryStateCellsBeforeMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store, shutdownContext: ctx}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 73)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	stateCells := newStateCellWindowCache(nil)
	if err := stateCells.addPreparedStateRecords(rootHash, mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("remember checkpoint cells: %v", err)
	}
	runner := &nextSyncRunner{
		service:      svc,
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   stateCells,
	}
	runner.checkpointStates.remember(&current.Masterchain)
	if err := runner.flushStagedCurrentSync("test"); err != nil {
		t.Fatalf("flush staged next-block current: %v", err)
	}

	if _, err := store.LoadStateCellTree(ctx, master, rootHash[:]); err != nil {
		t.Fatalf("load persisted next-block checkpoint state cells: %v", err)
	}
}

func TestNextBlockAsyncCheckpointCompletesSnapshotBeforeUnlock(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store, shutdownContext: ctx}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 76)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	stateCells := newStateCellWindowCache(nil)
	if err := stateCells.addPreparedStateRecords(rootHash, mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("remember checkpoint cells: %v", err)
	}
	runner := &nextSyncRunner{
		service:      svc,
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   stateCells,
	}
	runner.checkpointStates.remember(&current.Masterchain)

	svc.currentStatePersistMu.Lock()
	checkpoint := runner.checkpoint()
	cells := runner.stateCells.beginCheckpoint()
	callbackDone := make(chan struct{})
	callbackSawUnlocked := false
	next, err := svc.persistNextBlockCurrentStateLocked(current, &runner.timing, checkpoint.entries, cells, func() {
		if svc.currentStatePersistMu.TryLock() {
			callbackSawUnlocked = true
			svc.currentStatePersistMu.Unlock()
		}
		runner.completeCheckpoint(checkpoint)
		close(callbackDone)
	}, nil, 0, time.Now())
	if err != nil {
		t.Fatalf("schedule async checkpoint: %v", err)
	}
	runner.current = next
	svc.Wait()

	select {
	case <-callbackDone:
	default:
		t.Fatal("async checkpoint did not complete checkpoint metadata callback")
	}
	if err = svc.takeCurrentStatePersistError(); err != nil {
		t.Fatalf("async checkpoint persist error: %v", err)
	}
	if callbackSawUnlocked {
		t.Fatal("checkpoint metadata callback ran after persist mutex was unlocked")
	}
	if stale := runner.stateCells.beginCheckpoint(); stale != nil {
		t.Fatal("async checkpoint left completed cell snapshot pending")
	}
	if remaining := runner.checkpointStates.cloneEntries(); len(remaining) != 0 {
		t.Fatalf("async checkpoint left %d completed state entries pending", len(remaining))
	}
}

func TestNextMasterApplyCellsArePrivateUntilCheckpointMetadata(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x52, 8).EndCell()
	records := mustPreparedReachableStateCells(t, root)
	shared := newStateCellWindowCache(nil)
	applyCells := newNextMasterApplyCellWindow(func(hash cell.Hash) (*cell.Cell, error) {
		return shared.loader()(hash)
	})

	block := testBlockID(-1, topShard, 75)
	applyCells.remember(block, records)
	if checkpoint := shared.beginCheckpoint(); checkpoint != nil {
		if hasCellRecord(checkpoint.records(), root.HashKey()) {
			t.Fatal("apply-ahead cells leaked into checkpoint before metadata")
		}
	}

	if err := shared.addPreparedStateRecords(root.HashKey(0), records); err != nil {
		t.Fatalf("commit master cells: %v", err)
	}
	applyCells.forget(block)
	checkpoint := shared.beginCheckpoint()
	if checkpoint == nil {
		t.Fatal("committed master cells did not enter checkpoint")
	}
	if !hasCellRecord(checkpoint.records(), root.HashKey()) {
		t.Fatal("checkpoint does not contain cells committed with metadata")
	}
}

func TestFlushStagedCurrentAsyncFailureKeepsCheckpointStates(t *testing.T) {
	ctx := context.Background()
	shutdownCtx, cancelShutdown := context.WithCancel(ctx)
	store := openTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store, shutdownContext: shutdownCtx}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	preparedCells = removePreparedCellRecord(preparedCells, root.HashKey())

	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 74)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
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
	runner.stateCells.addPreparedRecords(preparedCells)
	runner.checkpointStates.remember(&current.Masterchain)
	cancelShutdown()
	if err := runner.flushStagedCurrent(); err != nil {
		t.Fatalf("schedule staged current flush: %v", err)
	}
	svc.Wait()

	if err := svc.takeCurrentStatePersistError(); err == nil {
		t.Fatal("expected async checkpoint persist error")
	}
	remaining := runner.checkpointStates.cloneEntries()
	if len(remaining) != 1 {
		t.Fatalf("remaining checkpoint states = %d, want 1", len(remaining))
	}
	if !remaining[0].state.Block.Equals(&master) {
		t.Fatalf("remaining checkpoint block = %s, want %s", tnstore.FormatBlockRef(remaining[0].state.Block), tnstore.FormatBlockRef(master))
	}
}

func TestArchiveCheckpointReleasesRetainedCellLoaderOnPersistFailure(t *testing.T) {
	ctx := context.Background()
	store := openManualTestPebbleStorage(t)
	svc := &Service{log: zerolog.Nop(), storage: store}

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	preparedCells = removePreparedCellRecord(preparedCells, root.HashKey())

	overlay := newArchiveStateCellOverlay(nil)
	overlay.addPreparedRecords(preparedCells)

	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 72)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
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
	if err := store.Close(); err != nil {
		t.Fatalf("close store before checkpoint: %v", err)
	}
	if _, err := runner.startCheckpoint("test"); err != nil {
		t.Fatalf("start checkpoint: %v", err)
	}
	if _, err := runner.finishCheckpoint(true); err == nil {
		t.Fatal("checkpoint with closed storage succeeded")
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
	}
	target := testBlockID(0, topShard, 101)
	ahead := testBlockID(0, topShard, 102)
	key := tnstore.ShardKeyFromBlock(target)
	current := &tnstore.CurrentState{
		ShardClientSeqno: master.Block.SeqNo - 1,
		Masterchain: tnstore.BlockState{
			Block:         testBlockID(-1, topShard, 50),
			StateRootHash: bytes32(0x50),
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

func TestCurrentStateForNextMasterStatePreservesUnchangedShardMasterchainRef(t *testing.T) {
	ctx := context.Background()
	inclusionMaster := testBlockID(-1, topShard, 50)
	nextMaster := &tnstore.BlockState{
		Block:         testBlockID(-1, topShard, 51),
		StateRootHash: bytes32(0x51),
	}
	shard := testBlockID(0, topShard, 100)
	key := tnstore.ShardKeyFromBlock(shard)
	current := &tnstore.CurrentState{
		ShardClientSeqno: inclusionMaster.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         inclusionMaster,
			StateRootHash: bytes32(0x50),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			key: {
				Block:          shard,
				Cell:           testShardStateCell(t, shard),
				MasterchainRef: &inclusionMaster,
			},
		},
	}

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: current.Shards,
	})
	next, _, err := (&Service{log: zerolog.Nop()}).currentStateForNextMasterState(ctx, current, nextMaster, []ton.BlockIDExt{shard}, resolver)
	if err != nil {
		t.Fatalf("build next current state: %v", err)
	}

	got := next.Shards[key].MasterchainRef
	if got == nil {
		t.Fatal("unchanged shard masterchain ref was lost")
	}
	// Matches cppnode BlockHandle::masterchain_ref_block: the ref is the masterchain block
	// that first included this shard block, and unchanged shard blocks must not move it forward.
	if !got.Equals(&inclusionMaster) {
		t.Fatalf("unchanged shard masterchain ref = %s, want inclusion master %s", tnstore.FormatBlockRef(*got), tnstore.FormatBlockRef(inclusionMaster))
	}
	if got.Equals(&nextMaster.Block) {
		t.Fatalf("unchanged shard masterchain ref moved to next master %s", tnstore.FormatBlockRef(nextMaster.Block))
	}
}

func TestVerifiedMasterchainQueueAcceptsBroadcastOutsideCatchUp(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	downloaded := testPreparedMasterchainBlock(prev, next)

	svc := &Service{}
	svc.queueMasterchainBlockCandidateFromSource(downloaded, p2p.PeerID{})

	got, ok := svc.takeQueuedMasterchainBlock(prev, next)
	if !ok {
		t.Fatal("expected queued masterchain block to be available")
	}
	if !got.ID.Equals(&next) {
		t.Fatalf("queued block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(next))
	}
}

func TestMasterchainBroadcastCandidateWaitsForValidation(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	downloaded := testVerifiedMasterchainBlock(prev, next)
	downloaded.Kind = "tonNode.blockBroadcast"
	downloaded.BlockBOC = []byte{1}
	downloaded.ProofBOC = []byte{2}
	downloaded.consensus = &masterchainConsensusProof{
		block:   next,
		prevRef: prev,
	}

	svc := &Service{}
	svc.queueMasterchainBroadcastCandidateFromSource(downloaded, testPeerID("peer-a"))

	if _, ok := svc.takeQueuedMasterchainBlock(prev, next); ok {
		t.Fatal("unverified broadcast candidate must not be returned by verified fast path")
	}
	candidate, ok := svc.peekQueuedMasterchainCandidate(prev, next)
	if !ok {
		t.Fatal("expected queued masterchain broadcast candidate")
	}
	if candidate.sourcePeerID != testPeerID("peer-a") {
		t.Fatalf("candidate source = %q, want peer-a", candidate.sourcePeerID)
	}
	future, ok := svc.queuedMasterchainFuture(testMasterBlockID(9))
	if !ok || !future.block.Equals(&next) {
		t.Fatalf("candidate future = %v %s, want %s", ok, tnstore.FormatBlockRef(future.block), tnstore.FormatBlockRef(next))
	}
}

func TestVerifiedMasterchainQueueDropsFarFutureWhenFull(t *testing.T) {
	svc := &Service{}
	for seqno := uint32(10); seqno < 10+nextMasterchainQueueLimit; seqno++ {
		prev := testMasterBlockID(seqno)
		next := testMasterBlockID(seqno + 1)
		svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(prev, next), p2p.PeerID{})
	}

	oldestPrev := testMasterBlockID(10)
	farPrev := testMasterBlockID(1000)
	farNext := testMasterBlockID(1001)
	svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(farPrev, farNext), p2p.PeerID{})

	oldestNext := testMasterBlockID(11)
	if got, ok := svc.takeQueuedMasterchainBlock(oldestPrev, oldestNext); !ok || !got.ID.Equals(&oldestNext) {
		t.Fatalf("expected oldest queued masterchain block to stay available, got ok=%v block=%s", ok, tnstore.FormatBlockRef(got.ID))
	}
	if _, ok := svc.takeQueuedMasterchainBlock(farPrev, farNext); ok {
		t.Fatal("expected far future masterchain block to be dropped")
	}
}

func TestVerifiedMasterchainQueueSeqnoIndexDoesNotDeleteReplacement(t *testing.T) {
	oldPrev := testMasterBlockID(10)
	newPrev := testMasterBlockID(20)
	block := testMasterBlockID(21)
	svc := &Service{}

	svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(oldPrev, block), testPeerID("old"))
	svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(newPrev, block), testPeerID("new"))

	if _, ok := svc.takeQueuedMasterchainBlock(oldPrev, block); ok {
		t.Fatal("expected same-seqno replacement to remove old prev entry")
	}
	if got, ok := svc.takeQueuedMasterchainBlock(newPrev, block); !ok || !got.ID.Equals(&block) {
		t.Fatalf("expected replacement block, got ok=%v block=%s", ok, tnstore.FormatBlockRef(got.ID))
	}
}

func TestQueuedMasterchainBlockAheadReportsFutureBlock(t *testing.T) {
	current := testMasterBlockID(10)
	futurePrev := testMasterBlockID(12)
	future := testMasterBlockID(13)
	svc := &Service{}
	svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(futurePrev, future), p2p.PeerID{})

	got, ok := svc.queuedMasterchainFuture(current)
	if !ok {
		t.Fatal("expected queued future masterchain block")
	}
	if !got.block.Equals(&future) {
		t.Fatalf("queued future block = %s, want %s", tnstore.FormatBlockRef(got.block), tnstore.FormatBlockRef(future))
	}
}

func TestQueuedMasterchainFutureReportsMissingSeqnoAndSource(t *testing.T) {
	current := testMasterBlockID(10)
	futurePrev := testMasterBlockID(12)
	future := testMasterBlockID(13)
	svc := &Service{}
	svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(futurePrev, future), testPeerID("peer-a"))

	got, ok := svc.queuedMasterchainFuture(current)
	if !ok {
		t.Fatal("expected queued future masterchain block")
	}
	if !got.block.Equals(&future) {
		t.Fatalf("queued future block = %s, want %s", tnstore.FormatBlockRef(got.block), tnstore.FormatBlockRef(future))
	}
	if got.lowestMissingSeqno != current.SeqNo+1 {
		t.Fatalf("lowest missing seqno = %d, want %d", got.lowestMissingSeqno, current.SeqNo+1)
	}
	if got.sourcePeerID != testPeerID("peer-a") {
		t.Fatalf("source key = %q, want peer-a", got.sourcePeerID)
	}
}

func TestNextBlockBootstrapProbeDecisionWidensFanout(t *testing.T) {
	prev := testMasterBlockID(100)
	base, _ := (&Service{}).nextBlockBootstrapProbeDecision(prev, 0, nextBlockBootstrapProbeState{})
	if base.peerLimit != nextBlockBootstrapProbePeers {
		t.Fatalf("base probe peers = %d, want %d", base.peerLimit, nextBlockBootstrapProbePeers)
	}

	urgent, _ := (&Service{}).nextBlockBootstrapProbeDecision(prev, 0, nextBlockBootstrapProbeState{
		consecutiveMisses: nextBlockBootstrapUrgentMisses,
		liveTail:          true,
	})
	if urgent.peerLimit != nextBlockBootstrapUrgentPeers {
		t.Fatalf("urgent probe peers = %d, want %d", urgent.peerLimit, nextBlockBootstrapUrgentPeers)
	}

	wide, _ := (&Service{}).nextBlockBootstrapProbeDecision(prev, 0, nextBlockBootstrapProbeState{
		consecutiveMisses: nextBlockBootstrapWideMisses,
		liveTail:          true,
	})
	if wide.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("wide probe peers = %d, want %d", wide.peerLimit, nextBlockBootstrapWidePeers)
	}

	catchUpWide, _ := (&Service{}).nextBlockBootstrapProbeDecision(prev, 0, nextBlockBootstrapProbeState{
		consecutiveMisses: nextBlockBootstrapWideMisses,
	})
	if catchUpWide.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("catch-up wide probe peers = %d, want %d", catchUpWide.peerLimit, nextBlockBootstrapWidePeers)
	}
}

func TestNextBlockBootstrapProbeDecisionUsesFutureQueueAndLag(t *testing.T) {
	prev := testMasterBlockID(100)
	futurePrev := testMasterBlockID(101)
	future := testMasterBlockID(102)
	svc := &Service{}
	svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(futurePrev, future), testPeerID("peer-a"))

	queued, _ := svc.nextBlockBootstrapProbeDecision(prev, 0, nextBlockBootstrapProbeState{})
	if queued.peerLimit != nextBlockBootstrapProbePeers {
		t.Fatalf("cold queued future probe peers = %d, want %d", queued.peerLimit, nextBlockBootstrapProbePeers)
	}
	if !queued.preferredSourcePeerID.IsZero() {
		t.Fatalf("cold queued future preferred source = %q, want empty", queued.preferredSourcePeerID)
	}

	queued, _ = svc.nextBlockBootstrapProbeDecision(prev, 0, nextBlockBootstrapProbeState{liveTail: true})
	if queued.peerLimit != nextBlockBootstrapUrgentPeers {
		t.Fatalf("queued future probe peers = %d, want %d", queued.peerLimit, nextBlockBootstrapUrgentPeers)
	}
	if !queued.queuedFutureAhead || queued.aheadBlocks != future.SeqNo-prev.SeqNo {
		t.Fatalf("unexpected queued future decision %+v", queued)
	}
	if queued.preferredSourcePeerID != testPeerID("peer-a") {
		t.Fatalf("queued future preferred source = %q, want peer-a", queued.preferredSourcePeerID)
	}
	if queued.lowestMissingSeqno != prev.SeqNo+1 {
		t.Fatalf("queued future lowest missing = %d, want %d", queued.lowestMissingSeqno, prev.SeqNo+1)
	}

	baseLive, _ := (&Service{}).nextBlockBootstrapProbeDecision(prev, 0, nextBlockBootstrapProbeState{liveTail: true})
	if baseLive.probeTimeout() != nextBlockBootstrapLiveProbeTimeout {
		t.Fatalf("live probe timeout = %s, want %s", baseLive.probeTimeout(), nextBlockBootstrapLiveProbeTimeout)
	}
	if baseLive.stagedPeerLimit() != nextBlockBootstrapWidePeers {
		t.Fatalf("live staged probe peers = %d, want %d", baseLive.stagedPeerLimit(), nextBlockBootstrapWidePeers)
	}
	if !(nextBlockBootstrapProbeDecision{liveTail: true, rawBroadcastAhead: true}).shouldPreferBroadcastCandidate() {
		t.Fatal("live raw masterchain broadcast should prefer broadcast candidate")
	}
	if (nextBlockBootstrapProbeDecision{rawBroadcastAhead: true}).shouldPreferBroadcastCandidate() {
		t.Fatal("cold raw masterchain broadcast should not delay peer probe")
	}

	oldUTime := time.Now().Add(-time.Duration(nextBlockBootstrapWideLagSeconds+1) * time.Second).Unix()
	lagged, _ := (&Service{}).nextBlockBootstrapProbeDecision(prev, oldUTime, nextBlockBootstrapProbeState{liveTail: true})
	if lagged.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("lagged probe peers = %d, want %d", lagged.peerLimit, nextBlockBootstrapWidePeers)
	}

	lagged, _ = (&Service{}).nextBlockBootstrapProbeDecision(prev, oldUTime, nextBlockBootstrapProbeState{})
	if lagged.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("lagged catch-up probe peers = %d, want %d", lagged.peerLimit, nextBlockBootstrapWidePeers)
	}
}

func TestExactBlockDownloadProbeDecisionWidensFanout(t *testing.T) {
	base := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started: time.Now(),
	})
	if base.peerLimit != exactBlockDownloadProbePeers {
		t.Fatalf("base exact probe peers = %d, want %d", base.peerLimit, exactBlockDownloadProbePeers)
	}
	if base.stagedPeerLimit != exactBlockDownloadProbePeers {
		t.Fatalf("base exact staged probe peers = %d, want %d", base.stagedPeerLimit, exactBlockDownloadProbePeers)
	}

	urgent := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started:           time.Now(),
		consecutiveMisses: nextBlockBootstrapUrgentMisses,
	})
	if urgent.peerLimit != nextBlockBootstrapUrgentPeers {
		t.Fatalf("urgent exact probe peers = %d, want %d", urgent.peerLimit, nextBlockBootstrapUrgentPeers)
	}
	if urgent.stagedPeerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("urgent exact staged probe peers = %d, want %d", urgent.stagedPeerLimit, nextBlockBootstrapWidePeers)
	}

	wide := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started:           time.Now(),
		consecutiveMisses: nextBlockBootstrapWideMisses,
	})
	if wide.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("wide exact probe peers = %d, want %d", wide.peerLimit, nextBlockBootstrapWidePeers)
	}

	waited := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started: time.Now().Add(-time.Duration(nextBlockBootstrapWideLagSeconds+1) * time.Second),
	})
	if waited.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("waited exact probe peers = %d, want %d", waited.peerLimit, nextBlockBootstrapWidePeers)
	}
}

func TestNextSyncRunnerShouldYieldBootstrapToArchive(t *testing.T) {
	oldUTime := time.Now().Add(-time.Duration(nextToArchiveLagSeconds+1) * time.Second).Unix()
	freshUTime := time.Now().Add(-time.Duration(nextToArchiveLagSeconds-1) * time.Second).Unix()

	runner := &nextSyncRunner{mode: nextSyncBootstrap}
	if !runner.shouldYieldBootstrapToArchive(oldUTime) {
		t.Fatal("expected unlimited bootstrap to yield when archive lag threshold is crossed")
	}
	if runner.shouldYieldBootstrapToArchive(freshUTime) {
		t.Fatal("did not expect unlimited bootstrap to yield below archive lag threshold")
	}

	runner.maxBlocks = 1
	if runner.shouldYieldBootstrapToArchive(oldUTime) {
		t.Fatal("did not expect bounded bootstrap to yield to archive")
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

func TestPreferredMasterchainBroadcastWaitReturnsQueuedBlock(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	svc := &Service{currentStateWake: make(chan struct{}, 1)}

	go func() {
		time.Sleep(10 * time.Millisecond)
		svc.queueMasterchainBlockCandidateFromSource(testPreparedMasterchainBlock(prev, next), testPeerID("broadcast-peer"))
		svc.wakeCurrentStateSync()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	downloaded, source, _, ok, err := svc.waitPreferredMasterchainBroadcast(ctx, prev, masterchainSeqnoTarget(^uint32(0)), nil, time.Hour)
	if err != nil {
		t.Fatalf("wait preferred masterchain broadcast: %v", err)
	}
	if !ok {
		t.Fatal("expected preferred masterchain broadcast")
	}
	if !downloaded.ID.Equals(&next) {
		t.Fatalf("preferred block = %s, want %s", tnstore.FormatBlockRef(downloaded.ID), tnstore.FormatBlockRef(next))
	}
	if source != "broadcast_queue" {
		t.Fatalf("preferred source = %q, want broadcast_queue", source)
	}
}

func testPreparedMasterchainBlock(prev ton.BlockIDExt, block ton.BlockIDExt) PreparedBlock {
	return PreparedBlock{
		ID: block,
		Meta: &tnstore.BlockMeta{
			ID:       block,
			PrevRefs: []ton.BlockIDExt{prev},
		},
	}
}

func testVerifiedMasterchainBlock(prev ton.BlockIDExt, block ton.BlockIDExt) VerifiedBlock {
	return VerifiedBlock{
		ID: block,
		Meta: &tnstore.BlockMeta{
			ID:       block,
			PrevRefs: []ton.BlockIDExt{prev},
		},
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
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			baseKey: {
				Block: base,
				Cell:  testShardStateCell(t, base),
			},
		},
	}, baseKey, master, base
}
