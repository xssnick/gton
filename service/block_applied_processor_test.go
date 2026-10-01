package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errTestBlockAppliedProcessorRetry = errors.New("retry block applied processor")

type blockAppliedProcessorFunc func(context.Context, BlockAppliedEvent) error

func (f blockAppliedProcessorFunc) ProcessBlockApplied(ctx context.Context, event BlockAppliedEvent) error {
	return f(ctx, event)
}

func TestBlockAppliedProcessorRetriesUntilSuccess(t *testing.T) {
	calls := 0
	runner := &blockAppliedProcessorRunner{
		log: zerolog.Nop(),
		processor: blockAppliedProcessorFunc(func(context.Context, BlockAppliedEvent) error {
			calls++
			if calls < 3 {
				return errTestBlockAppliedProcessorRetry
			}
			return nil
		}),
		retryDelay: time.Millisecond,
	}

	if err := runner.run(context.Background(), BlockAppliedEvent{
		Meta: &storage.BlockMeta{},
	}); err != nil {
		t.Fatalf("run hook: %v", err)
	}
	if calls != 3 {
		t.Fatalf("hook calls = %d, want 3", calls)
	}
}

func TestBlockAppliedProcessorAllowsParallelCalls(t *testing.T) {
	allowHook := make(chan struct{})
	firstHookAttempted := make(chan struct{})
	secondHookAttempted := make(chan struct{})
	var firstHookAttemptOnce sync.Once
	runner := &blockAppliedProcessorRunner{
		log: zerolog.Nop(),
		processor: blockAppliedProcessorFunc(func(_ context.Context, event BlockAppliedEvent) error {
			if event.Meta.ID.SeqNo == 2 {
				close(secondHookAttempted)
				return nil
			}

			firstHookAttemptOnce.Do(func() {
				close(firstHookAttempted)
			})
			select {
			case <-allowHook:
				return nil
			default:
				return errTestBlockAppliedProcessorRetry
			}
		}),
		retryDelay: time.Millisecond,
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runner.run(context.Background(), BlockAppliedEvent{
			Meta: &storage.BlockMeta{ID: ton.BlockIDExt{SeqNo: 1}},
		})
	}()

	select {
	case <-firstHookAttempted:
	case <-time.After(time.Second):
		t.Fatal("first hook did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- runner.run(context.Background(), BlockAppliedEvent{
			Meta: &storage.BlockMeta{ID: ton.BlockIDExt{SeqNo: 2}},
		})
	}()

	select {
	case <-secondHookAttempted:
	case <-time.After(time.Second):
		t.Fatal("second hook did not start while first hook was retrying")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second apply: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second apply did not finish while first hook was retrying")
	}

	close(allowHook)

	if err := <-firstDone; err != nil {
		t.Fatalf("first apply: %v", err)
	}
}

func TestApplyBlockWithHooksPassesStateRoots(t *testing.T) {
	leftBlock := testBlockID(0, topShard, 10)
	rightBlock := testBlockID(0, topShard, 11)
	nextBlock := testBlockID(0, topShard, 12)
	leftRoot := testShardStateCell(t, leftBlock)
	rightRoot := testShardStateCell(t, rightBlock)
	nextRoot := testShardStateCell(t, nextBlock)
	masterBlock := testBlockID(-1, topShard, 9)
	masterRoot := testShardStateCell(t, masterBlock)
	mergeRoot := cell.BeginCell().
		MustStoreUInt(0x5f327da5, 32).
		MustStoreRef(leftRoot.Virtualize(0)).
		MustStoreRef(rightRoot.Virtualize(0)).
		EndCell()

	var event BlockAppliedEvent
	svc := &SyncCoordinator{
		status: newTestStatusTracker(nil, nil),
		blockAppliedProcessor: &blockAppliedProcessorRunner{
			log: zerolog.Nop(),
			processor: blockAppliedProcessorFunc(func(_ context.Context, e BlockAppliedEvent) error {
				event = e
				return nil
			}),
			retryDelay: time.Millisecond,
		},
	}

	next, err := svc.applyBlockAndObserve(
		context.Background(),
		[]*storage.BlockState{
			{Block: leftBlock, Cell: leftRoot},
			{Block: rightBlock, Cell: rightRoot},
		},
		PreparedBlock{
			ID:          nextBlock,
			Meta:        &storage.BlockMeta{ID: nextBlock},
			StateUpdate: mustMerkleUpdateCell(t, mergeRoot, nextRoot),
		},
		nil,
		&blockAppliedObserverMeta{
			InclusionMasterRef:   &masterBlock,
			InclusionMasterState: masterRoot,
		},
	)
	if err != nil {
		t.Fatalf("apply block with hooks: %v", err)
	}

	if event.PreviousState.HashKey(0) != mergeRoot.HashKey(0) {
		t.Fatalf("previous state root hash mismatch: got=%x want=%x", event.PreviousState.HashKey(0), mergeRoot.HashKey(0))
	}
	if event.CurrentState.HashKey(0) != nextRoot.HashKey(0) {
		t.Fatalf("current state root hash mismatch: got=%x want=%x", event.CurrentState.HashKey(0), nextRoot.HashKey(0))
	}
	if event.CurrentState.HashKey(0) != next.Cell.HashKey(0) {
		t.Fatalf("event current state differs from returned state: got=%x want=%x", event.CurrentState.HashKey(0), next.Cell.HashKey(0))
	}
	if event.InclusionMasterRef == nil || !event.InclusionMasterRef.Equals(&masterBlock) {
		t.Fatalf("inclusion master ref = %+v, want %s", event.InclusionMasterRef, storage.FormatBlockRef(masterBlock))
	}
	if event.InclusionMasterState.HashKey(0) != masterRoot.HashKey(0) {
		t.Fatalf("inclusion master state hash mismatch: got=%x want=%x", event.InclusionMasterState.HashKey(0), masterRoot.HashKey(0))
	}
}

func TestApplyBlockCanDeferBlockAppliedProcessor(t *testing.T) {
	previousBlock := testBlockID(0, topShard, 20)
	nextBlock := testBlockID(0, topShard, 21)
	previousRoot := testShardStateCell(t, previousBlock)
	nextRoot := testShardStateCell(t, nextBlock)

	processorCalls := 0
	svc := &SyncCoordinator{
		status: newTestStatusTracker(nil, nil),
		blockAppliedProcessor: &blockAppliedProcessorRunner{
			log: zerolog.Nop(),
			processor: blockAppliedProcessorFunc(func(_ context.Context, event BlockAppliedEvent) error {
				processorCalls++
				if !event.Meta.ID.Equals(&nextBlock) {
					t.Fatalf("processed block = %s, want %s", storage.FormatBlockRef(event.Meta.ID), storage.FormatBlockRef(nextBlock))
				}
				return nil
			}),
			retryDelay: time.Millisecond,
		},
	}

	var deferred BlockAppliedEvent
	next, err := svc.applyBlockAndObserve(
		context.Background(),
		[]*storage.BlockState{{Block: previousBlock, Cell: previousRoot}},
		PreparedBlock{
			ID:          nextBlock,
			Meta:        &storage.BlockMeta{ID: nextBlock},
			StateUpdate: mustMerkleUpdateCell(t, previousRoot, nextRoot),
		},
		nil,
		&blockAppliedObserverMeta{deferEvent: func(event BlockAppliedEvent) {
			deferred = event
		}},
	)
	if err != nil {
		t.Fatalf("apply block: %v", err)
	}
	if processorCalls != 0 {
		t.Fatalf("processor calls during speculative apply = %d, want 0", processorCalls)
	}
	if deferred.Meta == nil || !deferred.Meta.ID.Equals(&nextBlock) {
		t.Fatalf("deferred block = %+v, want %s", deferred.Meta, storage.FormatBlockRef(nextBlock))
	}
	if deferred.CurrentState.HashKey(0) != next.Cell.HashKey(0) {
		t.Fatalf("deferred state differs from returned state: got=%x want=%x", deferred.CurrentState.HashKey(0), next.Cell.HashKey(0))
	}

	if err = svc.blockAppliedProcessor.run(context.Background(), deferred); err != nil {
		t.Fatalf("process deferred event: %v", err)
	}
	if processorCalls != 1 {
		t.Fatalf("processor calls after commit dispatch = %d, want 1", processorCalls)
	}
}

func TestArchiveShardApplyCollectsWithoutCallingProcessor(t *testing.T) {
	previousBlock := testBlockID(0, topShard, 20)
	nextBlock := testBlockID(0, topShard, 21)
	masterBlock := testMasterBlockID(19)
	previousRoot := testShardStateCell(t, previousBlock)
	nextRoot := testShardStateCell(t, nextBlock)
	masterRoot := cell.BeginCell().MustStoreUInt(19, 8).EndCell()

	processorCalls := 0
	svc := &SyncCoordinator{
		status: newTestStatusTracker(nil, nil),
		blockAppliedProcessor: &blockAppliedProcessorRunner{
			log: zerolog.Nop(),
			processor: blockAppliedProcessorFunc(func(context.Context, BlockAppliedEvent) error {
				processorCalls++
				return nil
			}),
			retryDelay: time.Millisecond,
		},
	}
	window := &shardClientArchiveWindow{}
	enableArchiveBlockAppliedEvents(t, window)
	observerMeta := &blockAppliedObserverMeta{
		InclusionMasterRef:   &masterBlock,
		InclusionMasterState: masterRoot,
		deferEvent: func(event BlockAppliedEvent) {
			window.blockApplied.deferShard(masterBlock.SeqNo, event)
		},
	}

	next, err := svc.applyArchiveShardBlock(
		t.Context(),
		nextBlock,
		[]*storage.BlockState{{Block: previousBlock, Cell: previousRoot}},
		PreparedBlock{
			ID: nextBlock,
			Meta: &storage.BlockMeta{
				ID:       nextBlock,
				PrevRefs: []ton.BlockIDExt{previousBlock},
			},
			StateUpdate: mustMerkleUpdateCell(t, previousRoot, nextRoot),
		},
		nil,
		observerMeta,
	)
	if err != nil {
		t.Fatalf("apply archive shard block: %v", err)
	}
	if processorCalls != 0 {
		t.Fatalf("processor calls during archive shard apply = %d, want 0", processorCalls)
	}
	if next.Cell.HashKey(0) != nextRoot.HashKey(0) {
		t.Fatalf("applied archive shard state hash = %x, want %x", next.Cell.HashKey(0), nextRoot.HashKey(0))
	}

	window.blockApplied.mu.Lock()
	deferred := window.blockApplied.shards[masterBlock.SeqNo]
	window.blockApplied.mu.Unlock()
	if len(deferred) != 1 || deferred[0].Meta == nil || !deferred[0].Meta.ID.Equals(&nextBlock) {
		t.Fatalf("deferred archive shard events = %+v, want %s", deferred, storage.FormatBlockRef(nextBlock))
	}
}

func TestArchiveMasterApplyCollectsBeforePostApplyPublication(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)
	prepared, err := (&SyncCoordinator{}).prepareDownloadedBlockForApply(*downloaded)
	if err != nil {
		t.Fatalf("prepare fixture masterchain block: %v", err)
	}
	previousRoot := prepared.StateUpdate.MustPeekRef(0)
	current := &storage.BlockState{
		Block: prepared.Meta.PrevRefs[0],
		Cell:  previousRoot,
	}
	proof := &masterchainConsensusProof{
		block:               prepared.ID,
		prevRef:             current.Block,
		stateUpdateFromHash: previousRoot.Virtualize(0).HashKeyAt(0),
		hardforkChecked:     true,
	}
	// The fixture's key-block status is irrelevant to this observer test and
	// would publish validator policy through a fully assembled p2p node.
	prepared.Meta.Flags &^= storage.BlockMetaIsKeyBlock

	processorCalls := 0
	svc := &SyncCoordinator{
		log:    zerolog.Nop(),
		status: newTestStatusTracker(nil, nil),
		blockAppliedProcessor: &blockAppliedProcessorRunner{
			log: zerolog.Nop(),
			processor: blockAppliedProcessorFunc(func(context.Context, BlockAppliedEvent) error {
				processorCalls++
				return nil
			}),
			retryDelay: time.Millisecond,
		},
	}
	svc.broadcastValidatorCache.shardTopView = &shardTopValidationView{
		masterchain:   &storage.BlockState{Block: prepared.ID},
		stateRootHash: cell.Hash{0xff},
	}
	window := &shardClientArchiveWindow{}
	enableArchiveBlockAppliedEvents(t, window)
	observerMeta := &blockAppliedObserverMeta{deferEvent: window.blockApplied.deferMaster}

	_, _, _, err = svc.applyArchiveMasterBlock(t.Context(), current, &prepared, proof, nil, observerMeta)
	if !errors.Is(err, errShardTopValidationViewConflict) {
		t.Fatalf("archive master post-apply publication error = %v, want %v", err, errShardTopValidationViewConflict)
	}
	if processorCalls != 0 {
		t.Fatalf("processor calls during archive master apply = %d, want 0", processorCalls)
	}

	window.blockApplied.mu.Lock()
	deferred := window.blockApplied.masters
	window.blockApplied.mu.Unlock()
	if len(deferred) != 1 || deferred[0].Meta == nil || !deferred[0].Meta.ID.Equals(&prepared.ID) {
		t.Fatalf("deferred archive master events = %+v, want %s", deferred, storage.FormatBlockRef(prepared.ID))
	}
	if deferred[0].CurrentState == nil {
		t.Fatal("deferred archive master event has no applied state")
	}
}

func TestNextSyncProcessesCommittedBlockEventsMasterFirst(t *testing.T) {
	master := testBlockID(-1, topShard, 30)
	shard := testBlockID(0, topShard, 31)
	var processed []ton.BlockIDExt
	svc := &SyncCoordinator{
		blockAppliedProcessor: &blockAppliedProcessorRunner{
			log: zerolog.Nop(),
			processor: blockAppliedProcessorFunc(func(_ context.Context, event BlockAppliedEvent) error {
				processed = append(processed, event.Meta.ID)
				return nil
			}),
			retryDelay: time.Millisecond,
		},
	}
	runner := &nextSyncRunner{
		service:           svc,
		ctx:               context.Background(),
		shardBlockApplied: map[uint32][]BlockAppliedEvent{},
	}
	runner.deferShardBlockApplied(master.SeqNo, BlockAppliedEvent{Meta: &storage.BlockMeta{ID: shard}})

	masterEvent := BlockAppliedEvent{Meta: &storage.BlockMeta{ID: master}}
	err := runner.processBlockAppliedEvents(nextAppliedMaster{
		master:            &storage.BlockState{Block: master},
		blockAppliedEvent: &masterEvent,
	})
	if err != nil {
		t.Fatalf("process committed events: %v", err)
	}
	if len(processed) != 2 || !processed[0].Equals(&master) || !processed[1].Equals(&shard) {
		t.Fatalf("processed order = %+v, want master then shard", processed)
	}
	if len(runner.shardBlockApplied) != 0 {
		t.Fatalf("deferred shard events retained after commit: %+v", runner.shardBlockApplied)
	}
}
