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
)

var errTestApplyHookRetry = errors.New("retry apply hook")

type blockApplyHookFunc func(context.Context, hooks.BlockAppliedEvent) error

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
