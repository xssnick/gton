package p2p

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// runAsync (node.go) gates wg.Add behind asyncStopped under asyncMx and returns
// a bool; stop() sets asyncStopped=true under asyncMx before wg.Wait. The two
// tests below guard that ordering: refused work is never started, and racing
// runAsync against the shutdown flip never re-adds to a WaitGroup a concurrent
// Wait already observed at zero.

// setAsyncStoppedLikeStop mirrors the exact seam stop() uses to enter the
// async-refusal state, without adding a production setter.
func setAsyncStoppedLikeStop(n *Node) {
	n.asyncMx.Lock()
	n.asyncStopped = true
	n.asyncMx.Unlock()
}

func TestNodeRunAsyncRefusesWorkAfterShutdown(t *testing.T) {
	node := newTestNode(t)
	setAsyncStoppedLikeStop(node)

	var invoked atomic.Int64
	if node.runAsync(func() { invoked.Add(1) }) {
		t.Fatal("runAsync accepted work after shutdown")
	}

	// A refused runAsync must not spawn the goroutine at all; give any
	// erroneously spawned goroutine several scheduling opportunities to run.
	for range 100 {
		runtime.Gosched()
	}
	if got := invoked.Load(); got != 0 {
		t.Fatalf("refused runAsync invoked fn %d times, want 0", got)
	}
}

func TestNodeRunAsyncShutdownRaceDoesNotReuseWaitGroup(t *testing.T) {
	node := newTestNode(t)

	const (
		workers   = 64
		perWorker = 64
	)
	var accepted atomic.Int64
	var completed atomic.Int64

	var start sync.WaitGroup
	start.Add(1)

	var callers sync.WaitGroup
	callers.Add(workers)
	for range workers {
		go func() {
			defer callers.Done()
			start.Wait()
			for range perWorker {
				if node.runAsync(func() { completed.Add(1) }) {
					accepted.Add(1)
				}
			}
		}()
	}

	// The stopper flips asyncStopped under asyncMx and drains wg, exactly like
	// stop(). Without runAsync's asyncStopped gate this Wait would race a
	// concurrent wg.Add and panic ("WaitGroup is reused before previous Wait
	// has returned").
	stopperDone := make(chan struct{})
	go func() {
		defer close(stopperDone)
		start.Wait()
		runtime.Gosched()
		setAsyncStoppedLikeStop(node)
		node.wg.Wait()
	}()

	start.Done()
	callers.Wait()
	<-stopperDone

	// Every runAsync that returned true added to wg and spawned its fn; the
	// stopper's wg.Wait guarantees all of them ran to completion.
	if got, want := completed.Load(), accepted.Load(); got != want {
		t.Fatalf("completed fn = %d, accepted runAsync = %d, want equal", got, want)
	}
}
