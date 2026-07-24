package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errTestApplyHookRetry = errors.New("retry apply hook")

type blockApplyHookFunc func(context.Context, hooks.BlockAppliedEvent) error

func (blockApplyHookFunc) Start(context.Context) error {
	return nil
}

func (blockApplyHookFunc) Close(context.Context) error {
	return nil
}

func (f blockApplyHookFunc) OnBlockApplied(ctx context.Context, event hooks.BlockAppliedEvent) error {
	return f(ctx, event)
}

func (blockApplyHookFunc) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (blockApplyHookFunc) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

func TestExtensionApplyHookRetriesUntilSuccess(t *testing.T) {
	calls := 0
	runner := &blockApplyHookRunner{
		log: zerolog.Nop(),
		extension: blockApplyHookFunc(func(context.Context, hooks.BlockAppliedEvent) error {
			calls++
			if calls < 3 {
				return errTestApplyHookRetry
			}
			return nil
		}),
		retryDelay: time.Millisecond,
	}

	if err := runner.run(context.Background(), hooks.BlockAppliedEvent{
		Meta: &storage.BlockMeta{},
	}); err != nil {
		t.Fatalf("run hook: %v", err)
	}
	if calls != 3 {
		t.Fatalf("hook calls = %d, want 3", calls)
	}
}

func TestExtensionApplyGateAllowsParallelApplyAndHooks(t *testing.T) {
	allowHook := make(chan struct{})
	firstHookAttempted := make(chan struct{})
	secondHookAttempted := make(chan struct{})
	var firstHookAttemptOnce sync.Once
	runner := &blockApplyHookRunner{
		log: zerolog.Nop(),
		extension: blockApplyHookFunc(func(_ context.Context, event hooks.BlockAppliedEvent) error {
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
				return errTestApplyHookRetry
			}
		}),
		retryDelay: time.Millisecond,
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runner.run(context.Background(), hooks.BlockAppliedEvent{
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
		secondDone <- runner.run(context.Background(), hooks.BlockAppliedEvent{
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

	var event hooks.BlockAppliedEvent
	svc := &Service{
		recentTPS: newRecentTPSTracker(recentTPSWindow),
		applyHooks: &blockApplyHookRunner{
			log: zerolog.Nop(),
			extension: blockApplyHookFunc(func(_ context.Context, e hooks.BlockAppliedEvent) error {
				event = e
				return nil
			}),
			retryDelay: time.Millisecond,
		},
	}

	next, err := svc.applyBlockWithHooks(
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
		&blockApplyHookMeta{
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
