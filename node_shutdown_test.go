package gton

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestWaitForLiteserverShutdownReturnsWhenWaitCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := waitForLiteserverShutdown(ctx, func() {}); err != nil {
		t.Fatalf("wait for completed shutdown: %v", err)
	}
}

func TestWaitForLiteserverShutdownReturnsAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		waitReturned := make(chan struct{})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		err := waitForLiteserverShutdown(ctx, func() {
			defer close(waitReturned)
			<-release
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("wait error = %v, want %v", err, context.DeadlineExceeded)
		}

		close(release)
		<-waitReturned
		synctest.Wait()
	})
}
