package service

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

func newTestStatusTracker(store StatusStorage, liveBlockCache *storage.LiveBlockCache) *StatusTracker {
	return NewStatusTracker(zerolog.Nop(), store, liveBlockCache)
}

func newTestStatusTrackerWithCurrent(store StatusStorage, current *storage.CurrentState) *StatusTracker {
	tracker := newTestStatusTracker(store, nil)
	tracker.current = current

	return tracker
}

func TestStatusTrackerStartWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker := newTestStatusTracker(nil, nil)
		ctx, cancel := context.WithCancel(t.Context())

		tracker.Start(ctx)
		tracker.Start(ctx)
		cancel()
		synctest.Wait()
		tracker.Wait()
	})
}
