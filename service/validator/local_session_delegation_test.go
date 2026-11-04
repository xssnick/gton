package validator

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
)

func TestFirstDelegationRetriesPreparationUntilAvailable(t *testing.T) {
	fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, newLocalBackendTestDelegations())
	var probes atomic.Uint32
	fixture.producer.probe = func(ctx context.Context, _ collator.WindowPreparation) error {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			return errors.New("preparation has no timeout")
		}
		if probes.Add(1) == 1 {
			return collator.ErrUnavailable
		}
		return nil
	}
	committed := make(chan struct{}, 1)
	fixture.producer.commit = func(context.Context, collator.WindowRequest) error {
		committed <- struct{}{}
		return nil
	}
	fixture.backend.scheduleFirstDelegation(context.Background())
	select {
	case <-committed:
	case <-time.After(2 * time.Second):
		t.Fatal("temporary preparation failure abandoned the first delegation")
	}
	if err := fixture.backend.Close(); err != nil {
		t.Fatal(err)
	}
	if probes.Load() != 2 || fixture.signer.calls != 1 {
		t.Fatalf("got %d preparation attempts and %d signatures", probes.Load(), fixture.signer.calls)
	}
}

func TestUpcomingDelegationRetriesPreparationAfterCallerReturns(t *testing.T) {
	fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, newLocalBackendTestDelegations())
	var probes atomic.Uint32
	fixture.producer.probe = func(context.Context, collator.WindowPreparation) error {
		if probes.Add(1) == 1 {
			return collator.ErrUnavailable
		}
		return nil
	}
	committed := make(chan collator.WindowRequest, 1)
	fixture.producer.commit = func(_ context.Context, request collator.WindowRequest) error {
		committed <- request
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := fixture.backend.HandleLeaderWindow(ctx, fixture.window(0, 0)); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case request := <-committed:
		if request.ID().StartSlot != 2 {
			t.Fatalf("delegated slot %d, want next window at 2", request.ID().StartSlot)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upcoming preparation was not retried after caller cancellation")
	}
	if err := fixture.backend.Close(); err != nil {
		t.Fatal(err)
	}
	if probes.Load() != 2 || fixture.signer.calls != 1 {
		t.Fatalf("got %d preparation attempts and %d signatures", probes.Load(), fixture.signer.calls)
	}
}

func TestDelegationDeliveryRetriesDurableSignature(t *testing.T) {
	store := newLocalBackendTestDelegations()
	fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, store)
	attempts := make(chan collator.WindowRequest, 4)
	fixture.producer.commit = func(_ context.Context, request collator.WindowRequest) error {
		attempts <- request
		return collator.ErrUnavailable
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := fixture.backend.HandleLeaderWindow(ctx, fixture.window(0, 0)); err != nil {
		t.Fatal(err)
	}
	first := <-attempts
	cancel()
	var second collator.WindowRequest
	select {
	case second = <-attempts:
	case <-time.After(2 * time.Second):
		t.Fatal("durable delegation was not retried after caller cancellation")
	}
	if first.ID() != second.ID() || !bytes.Equal(first.PleaseCollate.Signature, second.PleaseCollate.Signature) {
		t.Fatal("retry changed signed authority")
	}
	fixture.backend.controlMu.Lock()
	fixture.backend.cancelStartedDelegationsLocked(2)
	fixture.backend.update.HasCurrentWindow = true
	fixture.backend.update.CurrentWindowStart = 2
	fixture.backend.controlMu.Unlock()
	if err := fixture.backend.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-attempts:
		t.Fatal("delivery continued after window started")
	default:
	}
	if fixture.signer.calls != 1 {
		t.Fatalf("signed %d times, want durable signature reuse", fixture.signer.calls)
	}
}

func TestFirstDelegationExpectedStartAndClose(t *testing.T) {
	t.Run("before session start", func(t *testing.T) {
		fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, newLocalBackendTestDelegations())
		fixture.backend.config.ExpectedStart = time.Now().Add(5 * time.Second)
		attempts := make(chan collator.WindowRequest, 1)
		fixture.producer.commit = func(_ context.Context, request collator.WindowRequest) error { attempts <- request; return nil }
		fixture.backend.scheduleFirstDelegation(context.Background())
		select {
		case request := <-attempts:
			if request.ID().StartSlot != 0 {
				t.Fatal("initial delegation was not for window zero")
			}
		case <-time.After(time.Second):
			t.Fatal("first window was not delegated five seconds before expected start")
		}
	})
	t.Run("cancel scheduled preparation", func(t *testing.T) {
		fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, newLocalBackendTestDelegations())
		fixture.backend.config.ExpectedStart = time.Now().Add(time.Hour)
		fixture.backend.scheduleFirstDelegation(context.Background())
		if err := fixture.backend.Close(); err != nil {
			t.Fatal(err)
		}
		probes, commits, _ := fixture.producer.delegationCallSnapshot()
		if len(probes) != 0 || len(commits) != 0 {
			t.Fatal("scheduled delegation ran before its deadline")
		}
	})
	t.Run("close interrupts delivery", func(t *testing.T) {
		fixture := newLocalBackendProductionTestFixture(t, collator.ProductionModeDelegated, newLocalBackendTestDelegations())
		entered := make(chan struct{})
		fixture.producer.commit = func(ctx context.Context, _ collator.WindowRequest) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		fixture.backend.scheduleFirstDelegation(context.Background())
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("delivery did not start")
		}
		if err := fixture.backend.Close(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
