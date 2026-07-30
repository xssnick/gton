package service

import (
	"context"
	"sort"
	"sync"
	"time"
)

type shardObtainRecorderContextKey struct{}

type shardObtainRecorder struct {
	mu        sync.Mutex
	intervals []shardObtainInterval
}

type shardObtainInterval struct {
	start time.Time
	end   time.Time
}

func newShardObtainRecorder() *shardObtainRecorder {
	return &shardObtainRecorder{}
}

func contextWithShardObtainRecorder(ctx context.Context, recorder *shardObtainRecorder) context.Context {
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, shardObtainRecorderContextKey{}, recorder)
}

// shardObtainRecorderFromContext lets a caller hand its own recorder to the
// shard resolution it is about to start, so downloads performed by the
// apply-ahead stage are still attributed to the master block that needed them
// once the commit stage reports its obtain stats.
func shardObtainRecorderFromContext(ctx context.Context) *shardObtainRecorder {
	recorder, _ := ctx.Value(shardObtainRecorderContextKey{}).(*shardObtainRecorder)
	return recorder
}

func observeShardBlockObtain(ctx context.Context, started time.Time) {
	recorder, _ := ctx.Value(shardObtainRecorderContextKey{}).(*shardObtainRecorder)
	if recorder == nil {
		return
	}
	recorder.observe(started, time.Now())
}

func (r *shardObtainRecorder) observe(started time.Time, finished time.Time) {
	if started.IsZero() || finished.Before(started) {
		return
	}

	r.mu.Lock()
	r.intervals = append(r.intervals, shardObtainInterval{
		start: started,
		end:   finished,
	})
	r.mu.Unlock()
}

func (r *shardObtainRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.intervals)
}

func (r *shardObtainRecorder) duration() time.Duration {
	r.mu.Lock()
	intervals := append([]shardObtainInterval(nil), r.intervals...)
	r.mu.Unlock()
	if len(intervals) == 0 {
		return 0
	}

	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].start.Before(intervals[j].start)
	})

	currentStart := intervals[0].start
	currentEnd := intervals[0].end
	var total time.Duration
	for _, interval := range intervals[1:] {
		if !interval.start.After(currentEnd) {
			if interval.end.After(currentEnd) {
				currentEnd = interval.end
			}
			continue
		}

		total += currentEnd.Sub(currentStart)
		currentStart = interval.start
		currentEnd = interval.end
	}
	total += currentEnd.Sub(currentStart)
	return total
}
