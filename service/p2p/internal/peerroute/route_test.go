package peerroute

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryDeadlineOnlyExtends(t *testing.T) {
	policy := testRetryPolicy()
	route := NewRoute("", policy)
	now := time.Unix(1_000_000, 0)

	route.DeferQUICDial(now)
	want := now.Add(policy.delay(1)).UnixNano()
	route.DeferQUICDial(now.Add(-time.Second))

	if got := route.retryState.Load(); got != want {
		t.Fatalf("retry deadline = %d, want %d", got, want)
	}
}

func TestRetryBacksOffAndResets(t *testing.T) {
	policy := testRetryPolicy()
	route := NewRoute("", policy)
	now := time.Unix(1_000_000, 0)

	for failure, wantDelay := range []time.Duration{
		policy.MinDelay,
		2 * policy.MinDelay,
		policy.MaxDelay,
		policy.MaxDelay,
		policy.MaxDelay,
	} {
		if !route.BeginQUICDial(now) {
			t.Fatalf("failure %d: route rejected dial", failure+1)
		}
		route.FailQUICDial(now)
		want := now.Add(wantDelay).UnixNano()
		if got := route.retryState.Load(); got != want {
			t.Fatalf("failure %d: retry deadline = %d, want %d", failure+1, got, want)
		}
		now = time.Unix(0, want)
	}

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected successful retry")
	}
	route.FinishQUICDial()
	if route.dialFailures != 0 {
		t.Fatalf("failures after successful dial = %d, want 0", route.dialFailures)
	}

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected dial after success")
	}
	route.FailQUICDial(now)
	want := now.Add(policy.MinDelay).UnixNano()
	if got := route.retryState.Load(); got != want {
		t.Fatalf("retry deadline after reset = %d, want %d", got, want)
	}
}

func TestExistingQUICPathClearsCooldown(t *testing.T) {
	route := NewRoute("127.0.0.1:30303", testRetryPolicy())
	route.retryState.Store(time.Now().Add(time.Minute).UnixNano())
	route.retryMu.Lock()
	route.dialFailures = 3
	route.retryMu.Unlock()
	route.MarkQUICAddressStale()

	route.SucceedQUICDial(false)

	if got := route.retryState.Load(); got != 0 {
		t.Fatalf("retry state after existing path success = %d, want 0", got)
	}
	route.retryMu.Lock()
	failures := route.dialFailures
	route.retryMu.Unlock()
	if failures != 0 {
		t.Fatalf("failures after existing path success = %d, want 0", failures)
	}
	if route.QUICAddressStale() {
		t.Fatal("existing successful path left QUIC route stale")
	}
}

func TestSerializesQUICDialAttempts(t *testing.T) {
	route := NewRoute("", testRetryPolicy())
	now := time.Unix(1_000_000, 0)

	if !route.BeginQUICDial(now) {
		t.Fatal("idle route rejected the first quic dial")
	}
	if route.BeginQUICDial(now) {
		t.Fatal("route admitted a second concurrent quic dial")
	}
	if !route.QUICReady(now) {
		t.Fatal("in-flight dial hid a peer from fanout")
	}

	route.FinishQUICDial()
	if !route.BeginQUICDial(now) {
		t.Fatal("successful quic dial did not release the route")
	}
	route.FailQUICDial(now)
	route.FinishQUICDial()
	if route.QUICReady(now) {
		t.Fatal("successful cleanup erased a concurrent failure deadline")
	}
}

func TestWaitQUICDialHonorsContext(t *testing.T) {
	route := NewRoute("", testRetryPolicy())
	if !route.BeginQUICDial(time.Now()) {
		t.Fatal("idle route rejected dial ownership")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := route.WaitQUICDial(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v, want context deadline", err)
	}
	if !route.QUICDialInFlight() {
		t.Fatal("waiter deadline released the owner's dial claim")
	}

	route.FinishQUICDial()
}

func TestWaitQUICDialWakesOnCompletion(t *testing.T) {
	route := NewRoute("", testRetryPolicy())
	if !route.BeginQUICDial(time.Now()) {
		t.Fatal("idle route rejected dial ownership")
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- route.WaitQUICDial(context.Background())
	}()
	route.FinishQUICDial()

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("wait for completed dial: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dial completion did not wake waiter")
	}
	if route.QUICDialInFlight() {
		t.Fatal("completed dial remains in flight")
	}
}

func TestNonOwnerFailureDoesNotOverrideDial(t *testing.T) {
	route := NewRoute("", testRetryPolicy())
	now := time.Unix(1_000_000, 0)

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected replacement quic dial")
	}
	route.DeferQUICDial(now)
	route.FinishQUICDial()

	if !route.QUICReady(now) {
		t.Fatal("non-owner failure suppressed a successful quic dial")
	}
}
