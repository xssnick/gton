package collator

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestControllerProgressReadiness(t *testing.T) {
	for _, name := range []string{"unlock", "unlock before wait", "cancel", "retire"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				managed := newControlledSession()
				controller := &Controller{managed: map[[32]byte]*controlledSession{{}: managed}}
				managed.mu.Lock()
				err := controller.handleConsensusProgress(t.Context(), ConsensusProgress{})
				var deferred *controllerProgressDeferred
				if !errors.Is(err, ErrSessionUpdateDeferred) || !errors.As(err, &deferred) {
					t.Fatalf("busy controller error = %v, want readiness refusal", err)
				}

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if name == "unlock before wait" {
					managed.mu.Unlock()
				}
				started := time.Now()
				result := make(chan error, 1)
				go func() { result <- deferred.WaitReady(ctx) }()
				synctest.Wait()
				if name != "unlock before wait" {
					select {
					case err := <-result:
						t.Fatalf("readiness returned while controller is locked: %v", err)
					default:
					}
				}

				var want error
				switch name {
				case "unlock":
					managed.mu.Unlock()
				case "cancel":
					cancel()
					want = context.Canceled
				case "retire":
					controller.mu.Lock()
					delete(controller.managed, [32]byte{})
					controller.mu.Unlock()
					managed.mu.Unlock()
				}

				if err := <-result; !errors.Is(err, want) {
					t.Fatalf("readiness error = %v, want %v", err, want)
				}
				if !time.Now().Equal(started) {
					t.Fatal("readiness added a timer delay")
				}
				if name == "cancel" {
					managed.mu.Unlock()
				}
				if !managed.mu.TryLock() {
					t.Fatal("readiness waiter retained the controller lock")
				}
				managed.mu.Unlock()
				if name == "retire" {
					if err := controller.handleConsensusProgress(t.Context(), ConsensusProgress{}); !errors.Is(err, ErrNotFound) {
						t.Fatalf("progress after retirement = %v, want ErrNotFound", err)
					}
				}
			})
		})
	}
}

func BenchmarkControllerProgressReadiness(b *testing.B) {
	managed := newControlledSession()
	controller := &Controller{managed: map[[32]byte]*controlledSession{{}: managed}}
	ctx := b.Context()
	b.ReportAllocs()

	for b.Loop() {
		managed.mu.Lock()
		err := controller.handleConsensusProgress(ctx, ConsensusProgress{})
		managed.mu.Unlock()
		if err := err.(*controllerProgressDeferred).WaitReady(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
