// Package ratelimit is a lock-free per-key rate limiter.
//
// It is a virtual-scheduling (GCRA) bucket: a key's whole state is one atomic
// int64 holding the instant its budget is exhausted until. Admitting a request
// pushes that instant forward by the request's cost and rejects it if the push
// would land further than the burst allowance ahead of now. There is no mutex
// and no per-request allocation on the hot path - one sync.Map load plus one
// compare-and-swap - which is what makes it usable on a path taken by every
// inbound request.
//
// Idle keys are reclaimed by Prune, which the owner calls from a ticker. Without
// it the key set grows with every distinct source ever seen.
package ratelimit

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// maxNanos is the horizon a bucket can be pushed to. Costs are clamped against
// it so a huge weight saturates instead of wrapping into the past.
const maxNanos = int64(1<<63 - 1)

// Limiter admits weighted requests per key at unitsPerSecond, tolerating bursts
// of burstUnits. The zero Limiter is not usable; build one with New.
type Limiter struct {
	unitNanos  int64
	burstNanos int64
	buckets    sync.Map
}

type bucket struct {
	until   atomic.Int64
	deleted atomic.Bool
}

// New returns a limiter admitting unitsPerSecond units per key per second with a
// burst of burstUnits. It returns nil when either bound is non-positive, so a
// disabled limiter is representable as a nil *Limiter and every method is safe
// on it.
func New(unitsPerSecond float64, burstUnits int) *Limiter {
	if burstUnits <= 0 || unitsPerSecond <= 0 ||
		math.IsNaN(unitsPerSecond) {
		return nil
	}

	unitNanos := costNanos(unitsPerSecond)
	return &Limiter{
		unitNanos:  unitNanos,
		burstNanos: burstNanos(unitNanos, burstUnits),
	}
}

// Allow admits one unit for key.
func (l *Limiter) Allow(key string, now time.Time) bool {
	return l.AllowN(key, 1, now)
}

// AllowN admits weight units for key, all or nothing. A nil limiter admits
// everything.
func (l *Limiter) AllowN(key string, weight uint64, now time.Time) bool {
	if l == nil {
		return true
	}
	if weight == 0 {
		return true
	}

	cost := l.costFor(weight)
	nowNanos := now.UnixNano()
	for {
		b := l.bucket(key)
		if b.deleted.Load() {
			l.buckets.CompareAndDelete(key, b)
			continue
		}

		admitted, until := l.reserve(b, nowNanos, cost)
		if !admitted {
			return false
		}
		if !b.deleted.Load() {
			return true
		}

		// The bucket was pruned between the reservation and now, so the
		// reservation would be lost. Re-file it, and if someone else already
		// filed a live bucket for this key, fold the reservation into theirs.
		l.buckets.CompareAndDelete(key, b)
		replacement := &bucket{}
		replacement.until.Store(until)
		loaded, stored := l.buckets.LoadOrStore(key, replacement)
		if !stored {
			return true
		}
		active := loaded.(*bucket)
		if !active.deleted.Load() {
			raiseUntil(active, until)
			return true
		}
	}
}

// Prune drops keys whose budget is fully replenished. Call it from a ticker;
// nothing else reclaims keys. Returns how many were dropped.
func (l *Limiter) Prune(now time.Time) int {
	if l == nil {
		return 0
	}

	nowNanos := now.UnixNano()
	deleted := 0
	l.buckets.Range(func(key, value any) bool {
		b := value.(*bucket)
		if b.until.Load() > nowNanos {
			return true
		}
		if !b.deleted.CompareAndSwap(false, true) {
			return true
		}
		if l.buckets.CompareAndDelete(key, b) {
			deleted++
		}
		return true
	})
	return deleted
}

// Len is the number of tracked keys. O(keys); for metrics and tests, not the hot
// path.
func (l *Limiter) Len() int {
	if l == nil {
		return 0
	}

	count := 0
	l.buckets.Range(func(any, any) bool {
		count++
		return true
	})
	return count
}

func (l *Limiter) costFor(weight uint64) int64 {
	if weight > uint64(maxNanos/l.unitNanos) {
		return maxNanos
	}
	return l.unitNanos * int64(weight)
}

func (l *Limiter) bucket(key string) *bucket {
	if loaded, ok := l.buckets.Load(key); ok {
		return loaded.(*bucket)
	}

	fresh := &bucket{}
	if loaded, ok := l.buckets.LoadOrStore(key, fresh); ok {
		return loaded.(*bucket)
	}
	return fresh
}

func (l *Limiter) reserve(b *bucket, nowNanos int64, cost int64) (bool, int64) {
	for {
		until := b.until.Load()
		base := until
		if base < nowNanos {
			base = nowNanos
		}
		if base > maxNanos-cost {
			return false, until
		}

		next := base + cost
		if next-nowNanos > l.burstNanos {
			return false, until
		}
		if b.until.CompareAndSwap(until, next) {
			return true, next
		}
	}
}

func costNanos(unitsPerSecond float64) int64 {
	if unitsPerSecond <= 0 || math.IsNaN(unitsPerSecond) {
		return maxNanos
	}
	if math.IsInf(unitsPerSecond, 1) {
		return 1
	}

	nanos := math.Ceil(float64(time.Second) / unitsPerSecond)
	if nanos < 1 {
		return 1
	}
	if nanos >= float64(maxNanos) {
		return maxNanos
	}
	return int64(nanos)
}

func burstNanos(unitNanos int64, burstUnits int) int64 {
	if unitNanos <= 0 || burstUnits <= 0 {
		return 0
	}
	units := int64(burstUnits)
	if unitNanos > maxNanos/units {
		return maxNanos
	}
	return unitNanos * units
}

func raiseUntil(b *bucket, until int64) {
	for {
		current := b.until.Load()
		if current >= until || b.deleted.Load() {
			return
		}
		if b.until.CompareAndSwap(current, until) {
			return
		}
	}
}
