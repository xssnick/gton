package validator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
)

type progressReadinessTestError struct {
	ready   <-chan struct{}
	waiting chan struct{}
}

func (e *progressReadinessTestError) Error() string {
	return collator.ErrSessionUpdateDeferred.Error()
}

func (e *progressReadinessTestError) Unwrap() error {
	return collator.ErrSessionUpdateDeferred
}

func (e *progressReadinessTestError) WaitReady(ctx context.Context) error {
	close(e.waiting)
	select {
	case <-e.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSessionRuntimeProgressReadiness(t *testing.T) {
	for _, name := range []string{"ready", "finalized changed", "new window", "deadline"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ready := make(chan struct{})
				refusal := &progressReadinessTestError{ready: ready, waiting: make(chan struct{})}
				attempts := 0
				runtime := &sessionRuntime{
					observe: func(context.Context, sessionConsensusProgress) error {
						attempts++
						if attempts == 1 {
							return fmt.Errorf("controller: %w", refusal)
						}

						return nil
					},
				}
				started := time.Now()
				window := simplex.Window{
					StartSlot: 4, ObservedSlot: 4, EndSlot: 8, ObservedAt: started,
				}
				ctx, finish := runtime.beginWindowProgress(t.Context(), window, time.Millisecond)
				defer finish()
				result := make(chan error, 1)
				go func() {
					result <- runtime.observeWindowProgress(ctx, sessionConsensusProgress{Window: window}, nil)
				}()
				<-refusal.waiting
				synctest.Wait()
				if !runtime.lifecycleMu.TryLock() {
					t.Fatal("runtime lifecycle lock held while waiting for controller readiness")
				}
				runtime.lifecycleMu.Unlock()
				if attempts != 1 {
					t.Fatalf("attempts while controller is busy = %d, want 1", attempts)
				}

				var want error
				wantAttempts := 1
				switch name {
				case "ready":
					close(ready)
					wantAttempts = 2
				case "finalized changed":
					runtime.stateMu.Lock()
					runtime.state.FinalizedBlock = &ton.BlockIDExt{SeqNo: 1}
					runtime.stateMu.Unlock()
					close(ready)
					want = errWindowFinalizedChanged
				case "new window":
					runtime.noteObservedWindow(window.EndSlot)
					want = context.Canceled
				case "deadline":
					time.Sleep(4 * time.Millisecond)
					want = context.DeadlineExceeded
				}

				if err := <-result; !errors.Is(err, want) {
					t.Fatalf("progress error = %v, want %v", err, want)
				}
				if attempts != wantAttempts {
					t.Fatalf("attempts = %d, want %d", attempts, wantAttempts)
				}
				if name != "deadline" && !time.Now().Equal(started) {
					t.Fatal("progress readiness added a timer delay")
				}
			})
		})
	}
}

func TestObserverStartupFlushWaitsForReadiness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ready := make(chan struct{})
		refusal := &progressReadinessTestError{ready: ready, waiting: make(chan struct{})}
		observer := &ConsensusObserver{}
		attempts := 0
		started := time.Now()
		result := make(chan error, 1)
		go func() {
			result <- observer.deliverRetrying(t.Context(), func() error {
				attempts++
				if attempts == 1 {
					return fmt.Errorf("startup: %w", refusal)
				}

				return nil
			})
		}()
		<-refusal.waiting
		synctest.Wait()
		if attempts != 1 {
			t.Fatalf("startup attempts before readiness = %d, want 1", attempts)
		}

		close(ready)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if attempts != 2 || !time.Now().Equal(started) {
			t.Fatalf("startup retry: attempts = %d, delay = %s", attempts, time.Since(started))
		}
	})
}
