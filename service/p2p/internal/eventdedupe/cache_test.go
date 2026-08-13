package eventdedupe

import (
	"fmt"
	"testing"
	"time"
)

func TestCacheEvictsOldestAtCapacity(t *testing.T) {
	cache := New(time.Hour, 2)
	now := time.Unix(1_000_000, 0)

	if !cache.Mark("a", now) {
		t.Fatal("first key was treated as duplicate")
	}
	if !cache.Mark("b", now.Add(time.Second)) {
		t.Fatal("second key was treated as duplicate")
	}
	if !cache.Mark("c", now.Add(2*time.Second)) {
		t.Fatal("third key was treated as duplicate")
	}

	if !cache.Mark("a", now.Add(3*time.Second)) {
		t.Fatal("oldest key was not evicted")
	}
	if cache.Mark("c", now.Add(4*time.Second)) {
		t.Fatal("newest key was evicted")
	}
}

func TestCacheExpiresAndForgetsKeys(t *testing.T) {
	cache := New(time.Minute, 16)
	now := time.Unix(1_000_000, 0)

	cache.Mark("expired", now)
	if cache.Seen("expired", now.Add(time.Minute)) {
		t.Fatal("expired key remained visible")
	}

	cache.Mark("forgotten", now)
	cache.Forget("forgotten")
	if cache.Seen("forgotten", now) {
		t.Fatal("forgotten key remained visible")
	}
}

func TestCacheHasHardCap(t *testing.T) {
	cache := New(time.Hour, 16)
	now := time.Now()

	for i := 0; i < 10_000; i++ {
		if !cache.Mark(fmt.Sprintf("fp-%d", i), now) {
			t.Fatalf("unique key %d was treated as duplicate", i)
		}
	}
	if len(cache.seen) > 16 {
		t.Fatalf("cache exceeded hard cap: got %d want <= 16", len(cache.seen))
	}
}

func BenchmarkCacheMarkDuplicate(b *testing.B) {
	cache := New(time.Hour, 16)
	now := time.Unix(1_000_000, 0)
	cache.Mark("duplicate", now)

	b.ReportAllocs()
	for b.Loop() {
		if cache.Mark("duplicate", now) {
			b.Fatal("duplicate key was accepted")
		}
	}
}

func BenchmarkCacheSeen(b *testing.B) {
	cache := New(time.Hour, 16)
	now := time.Unix(1_000_000, 0)
	cache.Mark("seen", now)

	b.ReportAllocs()
	for b.Loop() {
		if !cache.Seen("seen", now) {
			b.Fatal("cached key was not found")
		}
	}
}
