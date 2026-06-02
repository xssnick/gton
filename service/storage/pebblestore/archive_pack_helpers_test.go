package pebblestore

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyncPathsParallelHonorsWorkerLimit(t *testing.T) {
	paths := make([]string, 10)
	for i := range paths {
		paths[i] = fmt.Sprintf("path-%02d", i)
	}

	started := make(chan struct{}, len(paths))
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32

	done := make(chan error, 1)
	go func() {
		done <- syncPathsParallel(paths, 3, func(string) error {
			current := active.Add(1)
			for {
				maxSeen := maxActive.Load()
				if current <= maxSeen || maxActive.CompareAndSwap(maxSeen, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			return nil
		})
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("worker %d did not start", i)
		}
	}

	select {
	case <-started:
		t.Fatal("sync started more paths than the worker limit")
	default:
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sync paths: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sync paths did not finish")
	}

	if got := maxActive.Load(); got != 3 {
		t.Fatalf("max active workers = %d, want 3", got)
	}
}

func TestSyncPathsParallelReturnsFirstPathError(t *testing.T) {
	firstErr := errors.New("first")
	secondErr := errors.New("second")
	paths := []string{"a", "b", "c"}

	err := syncPathsParallel(paths, 2, func(path string) error {
		switch path {
		case "a":
			return firstErr
		case "b":
			return secondErr
		default:
			return nil
		}
	})
	if !errors.Is(err, firstErr) {
		t.Fatalf("error = %v, want first path error", err)
	}
}
