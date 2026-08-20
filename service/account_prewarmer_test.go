package service

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type accountPrewarmerTestWarmer func(context.Context, cell.Hash) error

func (f accountPrewarmerTestWarmer) WarmCellRecords(ctx context.Context, root cell.Hash) error {
	return f(ctx, root)
}

type accountPrewarmerTestResolver func(context.Context, int32, [32]byte) (cell.Hash, error)

func (f accountPrewarmerTestResolver) AccountRoot(ctx context.Context, workchain int32, account [32]byte) (cell.Hash, error) {
	return f(ctx, workchain, account)
}

func TestAccountPrewarmerDefaultsAndLifecycle(t *testing.T) {
	warmer := accountPrewarmerTestWarmer(func(context.Context, cell.Hash) error {
		return nil
	})
	resolver := accountPrewarmerTestResolver(func(context.Context, int32, [32]byte) (cell.Hash, error) {
		return cell.Hash{1}, nil
	})
	prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{})
	if err != nil {
		t.Fatalf("new account prewarmer: %v", err)
	}
	if prewarmer.workers != DefaultAccountPrewarmerWorkers {
		t.Fatalf("workers = %d, want %d", prewarmer.workers, DefaultAccountPrewarmerWorkers)
	}
	if capacity := cap(prewarmer.tasks); capacity != DefaultAccountPrewarmerQueueSize {
		t.Fatalf("queue capacity = %d, want %d", capacity, DefaultAccountPrewarmerQueueSize)
	}
	if capacity := prewarmer.PrewarmCapacity(); capacity != DefaultAccountPrewarmerWorkers+DefaultAccountPrewarmerQueueSize {
		t.Fatalf("prewarm capacity = %d, want workers plus bounded queue", capacity)
	}
	if prewarmer.EnqueueRoot(cell.Hash{1}) {
		t.Fatal("enqueue before start succeeded")
	}

	if err = prewarmer.Start(t.Context()); err != nil {
		t.Fatalf("start account prewarmer: %v", err)
	}
	if err = prewarmer.Start(t.Context()); err != nil {
		t.Fatalf("repeat start account prewarmer: %v", err)
	}

	prewarmer.Close()
	prewarmer.Close()
	if prewarmer.EnqueueRoot(cell.Hash{2}) {
		t.Fatal("enqueue after close succeeded")
	}
	if err = prewarmer.Start(t.Context()); !errors.Is(err, ErrAccountPrewarmerClosed) {
		t.Fatalf("start after close error = %v, want ErrAccountPrewarmerClosed", err)
	}
}

func TestAccountPrewarmerDefaultQueueScalesWithWorkers(t *testing.T) {
	warmer := accountPrewarmerTestWarmer(func(context.Context, cell.Hash) error {
		return nil
	})
	resolver := accountPrewarmerTestResolver(func(context.Context, int32, [32]byte) (cell.Hash, error) {
		return cell.Hash{1}, nil
	})
	prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{Workers: 8})
	if err != nil {
		t.Fatalf("new account prewarmer: %v", err)
	}

	wantQueue := 8 * accountPrewarmerQueuedPerWorker
	if capacity := cap(prewarmer.tasks); capacity != wantQueue {
		t.Fatalf("queue capacity = %d, want %d", capacity, wantQueue)
	}
	if capacity := prewarmer.PrewarmCapacity(); capacity != 8+wantQueue {
		t.Fatalf("prewarm capacity = %d, want %d", capacity, 8+wantQueue)
	}
}

func TestAccountPrewarmerRejectsInvalidConstruction(t *testing.T) {
	warmer := accountPrewarmerTestWarmer(func(context.Context, cell.Hash) error {
		return nil
	})
	resolver := accountPrewarmerTestResolver(func(context.Context, int32, [32]byte) (cell.Hash, error) {
		return cell.Hash{}, nil
	})

	tests := []struct {
		name     string
		warmer   CellRecordWarmer
		resolver AccountRootResolver
		opts     AccountPrewarmerOptions
	}{
		{name: "missing warmer", resolver: resolver},
		{name: "missing resolver", warmer: warmer},
		{name: "negative workers", warmer: warmer, resolver: resolver, opts: AccountPrewarmerOptions{Workers: -1}},
		{name: "negative queue", warmer: warmer, resolver: resolver, opts: AccountPrewarmerOptions{QueueSize: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewAccountPrewarmer(zerolog.Nop(), test.warmer, test.resolver, test.opts); err == nil {
				t.Fatal("construction succeeded")
			}
		})
	}
}

func TestAccountPrewarmerEnqueueIsBoundedAndDeduplicated(t *testing.T) {
	started := make(chan cell.Hash, 4)
	release := make(chan struct{})
	warmer := accountPrewarmerTestWarmer(func(ctx context.Context, root cell.Hash) error {
		select {
		case started <- root:
		case <-ctx.Done():
			return ctx.Err()
		}

		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	resolver := accountPrewarmerTestResolver(func(context.Context, int32, [32]byte) (cell.Hash, error) {
		return cell.Hash{}, storage.ErrNotFound
	})
	prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{
		Workers:   1,
		QueueSize: 1,
	})
	if err != nil {
		t.Fatalf("new account prewarmer: %v", err)
	}
	if err = prewarmer.Start(t.Context()); err != nil {
		t.Fatalf("start account prewarmer: %v", err)
	}
	t.Cleanup(prewarmer.Close)

	first := cell.Hash{1}
	second := cell.Hash{2}
	third := cell.Hash{3}
	if !prewarmer.EnqueueRoot(first) {
		t.Fatal("first root was not enqueued")
	}
	if got := receivePrewarmHash(t, started); got != first {
		t.Fatalf("first running root = %x, want %x", got, first)
	}
	if prewarmer.EnqueueRoot(first) {
		t.Fatal("running root was enqueued twice")
	}
	if !prewarmer.EnqueueRoot(second) {
		t.Fatal("second root was not queued")
	}
	if prewarmer.EnqueueRoot(third) {
		t.Fatal("root was enqueued into a full queue")
	}

	close(release)
	if got := receivePrewarmHash(t, started); got != second {
		t.Fatalf("second running root = %x, want %x", got, second)
	}
	if !prewarmer.EnqueueRoot(first) {
		t.Fatal("completed root could not be enqueued again")
	}
}

func TestAccountPrewarmerImmediateAccountBypassesFullQueue(t *testing.T) {
	started := make(chan cell.Hash, 3)
	release := make(chan struct{})
	warmer := accountPrewarmerTestWarmer(func(ctx context.Context, root cell.Hash) error {
		select {
		case started <- root:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	resolver := accountPrewarmerTestResolver(func(_ context.Context, _ int32, account [32]byte) (cell.Hash, error) {
		return cell.Hash{account[0]}, nil
	})
	prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{
		Workers:   1,
		QueueSize: 1,
	})
	if err != nil {
		t.Fatalf("new account prewarmer: %v", err)
	}
	if err = prewarmer.Start(t.Context()); err != nil {
		t.Fatalf("start account prewarmer: %v", err)
	}
	t.Cleanup(prewarmer.Close)

	if !prewarmer.EnqueueRoot(cell.Hash{1}) {
		t.Fatal("running root was not enqueued")
	}
	if got := receivePrewarmHash(t, started); got != (cell.Hash{1}) {
		t.Fatalf("running root = %x, want 01", got)
	}
	if !prewarmer.EnqueueRoot(cell.Hash{2}) {
		t.Fatal("queued root was not enqueued")
	}
	if !prewarmer.PrewarmAccountNow(0, [32]byte{3}) {
		t.Fatal("immediate account was not started")
	}
	if got := receivePrewarmHash(t, started); got != (cell.Hash{3}) {
		t.Fatalf("next started root = %x, want immediate 03", got)
	}
	if prewarmer.PrewarmAccountNow(0, [32]byte{3}) {
		t.Fatal("running immediate account was started twice")
	}
	if prewarmer.PrewarmAccountNow(0, [32]byte{4}) {
		t.Fatal("second immediate account exceeded the direct concurrency bound")
	}

	close(release)
}

func TestAccountPrewarmerImmediateSupersedesQueuedCopy(t *testing.T) {
	for _, queuedKind := range []accountPrewarmTaskKind{accountPrewarmAddress, accountPrewarmRoot} {
		name := "address"
		if queuedKind == accountPrewarmRoot {
			name = "exact_root"
		}
		t.Run(name, func(t *testing.T) {
			started := make(chan cell.Hash, 4)
			releaseWorker := make(chan struct{})
			releaseImmediate := make(chan struct{})
			var rootWarmCalls atomic.Int32
			warmer := accountPrewarmerTestWarmer(func(ctx context.Context, root cell.Hash) error {
				select {
				case started <- root:
				case <-ctx.Done():
					return ctx.Err()
				}

				var release <-chan struct{}
				switch root {
				case cell.Hash{1}:
					release = releaseWorker
				case cell.Hash{2}:
					rootWarmCalls.Add(1)
					release = releaseImmediate
				default:
					return nil
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			var resolveCalls atomic.Int32
			resolver := accountPrewarmerTestResolver(func(_ context.Context, _ int32, account [32]byte) (cell.Hash, error) {
				resolveCalls.Add(1)
				return cell.Hash{account[0]}, nil
			})
			prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{
				Workers:   1,
				QueueSize: 1,
			})
			if err != nil {
				t.Fatalf("new account prewarmer: %v", err)
			}
			if err = prewarmer.Start(t.Context()); err != nil {
				t.Fatalf("start account prewarmer: %v", err)
			}
			t.Cleanup(prewarmer.Close)

			if !prewarmer.EnqueueRoot(cell.Hash{1}) {
				t.Fatal("worker blocker was not enqueued")
			}
			if got := receivePrewarmHash(t, started); got != (cell.Hash{1}) {
				t.Fatalf("first running root = %x, want 01", got)
			}
			account := [32]byte{2}
			if queuedKind == accountPrewarmAddress {
				if !prewarmer.EnqueueAccount(0, account) {
					t.Fatal("account was not queued")
				}
			} else if !prewarmer.EnqueueRoot(cell.Hash{2}) {
				t.Fatal("exact root was not queued")
			}
			if !prewarmer.PrewarmAccountNow(0, account) {
				t.Fatal("immediate account was not started")
			}
			if got := receivePrewarmHash(t, started); got != (cell.Hash{2}) {
				t.Fatalf("immediate root = %x, want 02", got)
			}

			close(releaseImmediate)
			waitPrewarmTaskAbsent(t, prewarmer, accountPrewarmTask{
				kind:      accountPrewarmAddress,
				workchain: 0,
				account:   account,
			})
			close(releaseWorker)
			enqueuePrewarmRootEventually(t, prewarmer, cell.Hash{3})
			if got := receivePrewarmHash(t, started); got != (cell.Hash{3}) {
				t.Fatalf("root after stale queue copy = %x, want 03", got)
			}
			if calls := resolveCalls.Load(); calls != 1 {
				t.Fatalf("account resolutions = %d, want 1", calls)
			}
			if calls := rootWarmCalls.Load(); calls != 1 {
				t.Fatalf("root 02 warm calls = %d, want 1", calls)
			}
		})
	}
}

func TestAccountPrewarmerResolvesAccountOffCaller(t *testing.T) {
	resolveStarted := make(chan struct{})
	releaseResolve := make(chan struct{})
	resolvedRoot := cell.Hash{7}
	workchain := int32(0)
	account := [32]byte{9}

	resolver := accountPrewarmerTestResolver(func(ctx context.Context, gotWorkchain int32, gotAccount [32]byte) (cell.Hash, error) {
		if gotWorkchain != workchain || gotAccount != account {
			t.Errorf("resolved account = %d:%x, want %d:%x", gotWorkchain, gotAccount, workchain, account)
		}
		close(resolveStarted)
		select {
		case <-releaseResolve:
			return resolvedRoot, nil
		case <-ctx.Done():
			return cell.Hash{}, ctx.Err()
		}
	})
	warmed := make(chan cell.Hash, 1)
	warmer := accountPrewarmerTestWarmer(func(_ context.Context, root cell.Hash) error {
		warmed <- root
		return nil
	})
	prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{
		Workers:   1,
		QueueSize: 2,
	})
	if err != nil {
		t.Fatalf("new account prewarmer: %v", err)
	}
	if err = prewarmer.Start(t.Context()); err != nil {
		t.Fatalf("start account prewarmer: %v", err)
	}
	t.Cleanup(prewarmer.Close)

	if !prewarmer.EnqueueAccount(workchain, account) {
		t.Fatal("account was not enqueued")
	}
	receivePrewarmSignal(t, resolveStarted, "root resolution")
	if prewarmer.EnqueueAccount(workchain, account) {
		t.Fatal("resolving account was enqueued twice")
	}
	if prewarmer.PrewarmAccountNow(workchain, account) {
		t.Fatal("running account was promoted into duplicate work")
	}
	close(releaseResolve)
	if got := receivePrewarmHash(t, warmed); got != resolvedRoot {
		t.Fatalf("warmed root = %x, want %x", got, resolvedRoot)
	}
}

func TestAccountPrewarmerIgnoresNotFound(t *testing.T) {
	var output bytes.Buffer
	logger := zerolog.New(&output)
	resolved := make(chan struct{})
	resolver := accountPrewarmerTestResolver(func(context.Context, int32, [32]byte) (cell.Hash, error) {
		close(resolved)
		return cell.Hash{}, storage.ErrNotFound
	})
	var warmCalls atomic.Int32
	warmer := accountPrewarmerTestWarmer(func(context.Context, cell.Hash) error {
		warmCalls.Add(1)
		return nil
	})
	prewarmer, err := NewAccountPrewarmer(logger, warmer, resolver, AccountPrewarmerOptions{
		Workers:   1,
		QueueSize: 1,
	})
	if err != nil {
		t.Fatalf("new account prewarmer: %v", err)
	}
	if err = prewarmer.Start(t.Context()); err != nil {
		t.Fatalf("start account prewarmer: %v", err)
	}

	if !prewarmer.EnqueueAccount(0, [32]byte{1}) {
		t.Fatal("account was not enqueued")
	}
	receivePrewarmSignal(t, resolved, "root resolution")
	prewarmer.Close()
	if calls := warmCalls.Load(); calls != 0 {
		t.Fatalf("warm calls = %d, want 0", calls)
	}
	if output.Len() != 0 {
		t.Fatalf("not-found error was logged: %s", output.String())
	}
}

func TestAccountPrewarmerCloseCancelsActiveWarm(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	warmer := accountPrewarmerTestWarmer(func(ctx context.Context, root cell.Hash) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	})
	resolver := accountPrewarmerTestResolver(func(context.Context, int32, [32]byte) (cell.Hash, error) {
		return cell.Hash{}, storage.ErrNotFound
	})
	prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{
		Workers:   1,
		QueueSize: 1,
	})
	if err != nil {
		t.Fatalf("new account prewarmer: %v", err)
	}
	if err = prewarmer.Start(t.Context()); err != nil {
		t.Fatalf("start account prewarmer: %v", err)
	}
	if !prewarmer.EnqueueRoot(cell.Hash{1}) {
		t.Fatal("root was not enqueued")
	}
	receivePrewarmSignal(t, started, "warm start")

	var closes sync.WaitGroup
	closes.Add(2)
	go func() {
		defer closes.Done()
		prewarmer.Close()
	}()
	go func() {
		defer closes.Done()
		prewarmer.Close()
	}()
	closes.Wait()
	receivePrewarmSignal(t, canceled, "warm cancellation")
}

func TestAccountPrewarmerPromotionRacesClose(t *testing.T) {
	const iterations = 64

	for iteration := 0; iteration < iterations; iteration++ {
		started := make(chan struct{})
		warmer := accountPrewarmerTestWarmer(func(ctx context.Context, root cell.Hash) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
		resolver := accountPrewarmerTestResolver(func(ctx context.Context, _ int32, _ [32]byte) (cell.Hash, error) {
			<-ctx.Done()
			return cell.Hash{}, ctx.Err()
		})
		prewarmer, err := NewAccountPrewarmer(zerolog.Nop(), warmer, resolver, AccountPrewarmerOptions{
			Workers:   1,
			QueueSize: 1,
		})
		if err != nil {
			t.Fatalf("iteration %d: new account prewarmer: %v", iteration, err)
		}
		if err = prewarmer.Start(t.Context()); err != nil {
			t.Fatalf("iteration %d: start account prewarmer: %v", iteration, err)
		}
		if !prewarmer.EnqueueRoot(cell.Hash{1}) {
			t.Fatalf("iteration %d: worker blocker was not enqueued", iteration)
		}
		receivePrewarmSignal(t, started, "warm start")
		account := [32]byte{2}
		if !prewarmer.EnqueueAccount(0, account) {
			t.Fatalf("iteration %d: account was not queued", iteration)
		}

		begin := make(chan struct{})
		var calls sync.WaitGroup
		calls.Add(2)
		go func() {
			defer calls.Done()
			<-begin
			prewarmer.PrewarmAccountNow(0, account)
		}()
		go func() {
			defer calls.Done()
			<-begin
			prewarmer.Close()
		}()
		close(begin)
		calls.Wait()

		prewarmer.mu.Lock()
		states := len(prewarmer.taskStates)
		prewarmer.mu.Unlock()
		if states != 0 {
			t.Fatalf("iteration %d: task states after close = %d, want 0", iteration, states)
		}
		if slots := len(prewarmer.immediateSlots); slots != 0 {
			t.Fatalf("iteration %d: immediate slots after close = %d, want 0", iteration, slots)
		}
	}
}

func waitPrewarmTaskAbsent(t *testing.T, prewarmer *AccountPrewarmer, task accountPrewarmTask) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		prewarmer.mu.Lock()
		_, exists := prewarmer.taskStates[task]
		prewarmer.mu.Unlock()
		if !exists {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for prewarm task completion")
		}
		runtime.Gosched()
	}
}

func enqueuePrewarmRootEventually(t *testing.T, prewarmer *AccountPrewarmer, root cell.Hash) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !prewarmer.EnqueueRoot(root) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for prewarm queue capacity")
		}
		runtime.Gosched()
	}
}

func receivePrewarmHash(t *testing.T, values <-chan cell.Hash) cell.Hash {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prewarm root")
		return cell.Hash{}
	}
}

func receivePrewarmSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}
