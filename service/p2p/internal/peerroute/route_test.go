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
	route.SucceedQUICDial(true)
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

func TestRouteQUICDialFailing(t *testing.T) {
	policy := testRetryPolicy()
	route := NewRoute("", policy)
	now := time.Unix(1_000_000, 0)

	if route.QUICDialFailing() {
		t.Fatal("new route reports a failing dial")
	}

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected the first dial")
	}
	if route.QUICDialFailing() {
		t.Fatal("dial in flight on a fresh route reports failing")
	}
	route.FailQUICDial(now)
	if !route.QUICDialFailing() {
		t.Fatal("failed dial is not reported as failing")
	}

	// The retry gate expired and a new dial is in flight: the route keeps
	// reporting the last finished outcome until that dial finishes.
	now = now.Add(policy.MaxDelay)
	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial after the retry gate expired")
	}
	if !route.QUICDialFailing() {
		t.Fatal("dial in flight cleared the failing state before it finished")
	}
	route.SucceedQUICDial(true)
	if route.QUICDialFailing() {
		t.Fatal("successful claimed dial left the route failing")
	}

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial after success")
	}
	route.FailQUICDial(now)
	if !route.QUICDialFailing() {
		t.Fatal("second failure is not reported as failing")
	}

	// An abandoned dial (its owner gave up waiting for a dial slot or was
	// cancelled) reopens the gate but says nothing about the peer: the route
	// keeps failing, and the next failure keeps backing off from the history.
	now = now.Add(policy.MaxDelay)
	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial after the second retry gate expired")
	}
	route.FinishQUICDial()
	if !route.QUICDialFailing() {
		t.Fatal("abandoned dial cleared the failing state")
	}
	if !route.QUICDialPermitted(now) {
		t.Fatal("abandoned dial did not reopen the retry gate")
	}
	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial after an abandoned one")
	}
	route.FailQUICDial(now)
	if got, want := route.retryState.Load(), now.Add(policy.delay(2)).UnixNano(); got != want {
		t.Fatalf("retry deadline after abandoned dial = %d, want the second backoff step %d", got, want)
	}

	// An abandoned dial on a route that never failed leaves it healthy.
	fresh := NewRoute("", policy)
	if !fresh.BeginQUICDial(now) {
		t.Fatal("fresh route rejected a dial")
	}
	fresh.FinishQUICDial()
	if fresh.QUICDialFailing() {
		t.Fatal("abandoned dial marked a fresh route failing")
	}
}

func TestReopenFailingQUICDialReopensFailingRoute(t *testing.T) {
	policy := testRetryPolicy()
	route := NewRoute("127.0.0.1:30303", policy)
	now := time.Unix(1_000_000, 0)

	// Two failures: the gate is armed for the second backoff step and the
	// failed endpoint is stale.
	for range 2 {
		if !route.BeginQUICDial(now) {
			t.Fatal("route rejected a dial")
		}
		route.MarkQUICAddressStale()
		route.FailQUICDial(now)
		now = time.Unix(0, route.retryState.Load())
	}
	now = now.Add(-time.Millisecond)
	if !route.QUICDialFailing() || route.QUICDialPermitted(now) {
		t.Fatal("precondition: the route is failing inside its retry window")
	}

	if !route.ReopenFailingQUICDial() {
		t.Fatal("failing route was not reopened")
	}
	if route.QUICDialFailing() {
		t.Fatal("reopened route still reports a failing dial")
	}
	if !route.QUICDialPermitted(now) {
		t.Fatal("reopened route kept its retry gate armed")
	}
	if !route.QUICAddressStale() {
		t.Fatal("reopen forgot that the learned address is stale")
	}
	if route.ReopenFailingQUICDial() {
		t.Fatal("second reopen before any verdict reported a change")
	}

	// An abandoned dial is no verdict: the route stays reopened.
	if !route.BeginQUICDial(now) {
		t.Fatal("reopened route rejected a dial")
	}
	route.FinishQUICDial()
	if route.QUICDialFailing() || !route.QUICDialPermitted(now) {
		t.Fatal("abandoned dial undid the reopen")
	}

	// A failure backs off from where the streak was: the third step.
	if !route.BeginQUICDial(now) {
		t.Fatal("reopened route rejected a dial after the abandoned one")
	}
	route.FailQUICDial(now)
	if got, want := route.retryState.Load(), now.Add(policy.delay(3)).UnixNano(); got != want {
		t.Fatalf("retry deadline after the reopened dial failed = %d, want the third backoff step %d", got, want)
	}
	if !route.QUICDialFailing() {
		t.Fatal("failed dial after the reopen left the route healthy")
	}
}

// One reopen per failing streak: a peer that is alive but not dialable from
// here cannot keep reopening the gate, and only a successful dial starts a new
// streak.
func TestReopenFailingQUICDialOncePerFailingStreak(t *testing.T) {
	policy := testRetryPolicy()
	route := NewRoute("127.0.0.1:30303", policy)
	now := time.Unix(1_000_000, 0)

	failRouteDial(t, route, now)
	if !route.ReopenFailingQUICDial() {
		t.Fatal("failing route was not reopened")
	}
	failRouteDial(t, route, now)

	if route.ReopenFailingQUICDial() {
		t.Fatal("second reopen in the same failing streak was accepted")
	}
	if !route.QUICDialFailing() || route.QUICDialPermitted(now) {
		t.Fatal("refused reopen changed the failing route")
	}

	// Further failures inside the streak keep it spent.
	now = time.Unix(0, route.retryState.Load())
	failRouteDial(t, route, now)
	if route.ReopenFailingQUICDial() {
		t.Fatal("reopen accepted after a later failure of the same streak")
	}

	// A successful dial ends the streak; the next one may be reopened again.
	now = time.Unix(0, route.retryState.Load())
	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial after the retry gate expired")
	}
	route.SucceedQUICDial(true)
	failRouteDial(t, route, now)
	if !route.ReopenFailingQUICDial() {
		t.Fatal("failing route of a new streak was not reopened")
	}
	if route.QUICDialFailing() {
		t.Fatal("reopened route of a new streak still reports a failing dial")
	}
}

func TestReopenFailingQUICDialLeavesDialInFlight(t *testing.T) {
	policy := testRetryPolicy()
	route := NewRoute("", policy)
	now := time.Unix(1_000_000, 0)

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected the first dial")
	}
	route.FailQUICDial(now)
	now = now.Add(policy.MaxDelay)
	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial after the retry gate expired")
	}

	if route.ReopenFailingQUICDial() {
		t.Fatal("reopen took over a route with a dial in flight")
	}
	if !route.QUICDialInFlight() {
		t.Fatal("reopen released the in-flight dial claim")
	}
	if !route.QUICDialFailing() {
		t.Fatal("reopen cleared the failure verdict under a dial in flight")
	}

	// The in-flight dial still owns its verdict, and the refused reopen did
	// not spend the streak's one reopen.
	route.FailQUICDial(now)
	if got, want := route.retryState.Load(), now.Add(policy.delay(2)).UnixNano(); got != want {
		t.Fatalf("retry deadline = %d, want the second backoff step %d", got, want)
	}
	if !route.ReopenFailingQUICDial() {
		t.Fatal("route was not reopened once the in-flight dial failed")
	}
}

func TestReopenFailingQUICDialIgnoresHealthyRoute(t *testing.T) {
	route := NewRoute("127.0.0.1:30303", testRetryPolicy())
	now := time.Unix(1_000_000, 0)

	if route.ReopenFailingQUICDial() {
		t.Fatal("reopen of a fresh route reported a change")
	}

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial")
	}
	route.SucceedQUICDial(true)
	if route.ReopenFailingQUICDial() {
		t.Fatal("reopen of a route whose dial succeeded reported a change")
	}

	// A fresh route with a dial in flight is not failing either.
	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected a dial after success")
	}
	if route.ReopenFailingQUICDial() {
		t.Fatal("reopen of a healthy route with a dial in flight reported a change")
	}
	if !route.QUICDialInFlight() {
		t.Fatal("reopen released a healthy route's dial claim")
	}
	route.FinishQUICDial()
}

// failRouteDial claims a dial at now and records its failure.
func failRouteDial(t *testing.T, route *Route, now time.Time) {
	t.Helper()

	if !route.BeginQUICDial(now) {
		t.Fatal("route rejected the dial that is about to fail")
	}
	route.FailQUICDial(now)
}
