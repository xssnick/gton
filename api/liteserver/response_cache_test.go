package liteserver

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func testCacheKey(i int) liteResponseKey {
	var a storage.BlockRootHash
	binary.BigEndian.PutUint64(a[:8], uint64(i))
	return liteResponseKey{kind: liteResponseBlockHeader, a: a}
}

func TestLiteResponseCacheReusesCachedValueWithoutRebuild(t *testing.T) {
	c := newLiteResponseCache()

	builds := 0
	for i := 0; i < 3; i++ {
		value, err := c.do(context.Background(), testCacheKey(7), func(context.Context) (any, error) {
			builds++
			return "value", nil
		})
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		if value != "value" {
			t.Fatalf("value = %v, want %q", value, "value")
		}
	}

	if builds != 1 {
		t.Fatalf("builds = %d, want 1", builds)
	}
	c.mu.RLock()
	items, orderLen := len(c.items), len(c.order)
	c.mu.RUnlock()
	if items != 1 || orderLen != 1 {
		t.Fatalf("items = %d, order = %d, want 1, 1", items, orderLen)
	}
}

func TestLiteResponseCacheEvictsInFIFOOrderAcrossWraparound(t *testing.T) {
	c := newLiteResponseCache()

	builds := 0
	insert := func(i int) {
		t.Helper()
		value, err := c.do(context.Background(), testCacheKey(i), func(context.Context) (any, error) {
			builds++
			return i, nil
		})
		if err != nil {
			t.Fatalf("do(%d): %v", i, err)
		}
		if got, ok := value.(int); !ok || got != i {
			t.Fatalf("value = %v, want %d", value, i)
		}
	}
	cached := func(i int) bool {
		c.mu.RLock()
		_, ok := c.items[testCacheKey(i)]
		c.mu.RUnlock()
		return ok
	}

	// Wrap the circular order buffer more than twice.
	total := liteResponseCacheLimit*2 + liteResponseCacheLimit/2
	for i := 0; i < total; i++ {
		insert(i)
	}
	if builds != total {
		t.Fatalf("builds = %d, want %d", builds, total)
	}

	c.mu.RLock()
	items, orderLen, head := len(c.items), len(c.order), c.head
	c.mu.RUnlock()
	if items != liteResponseCacheLimit {
		t.Fatalf("items = %d, want %d", items, liteResponseCacheLimit)
	}
	if orderLen != liteResponseCacheLimit {
		t.Fatalf("order length = %d, want %d", orderLen, liteResponseCacheLimit)
	}
	if wantHead := (total - liteResponseCacheLimit) % liteResponseCacheLimit; head != wantHead {
		t.Fatalf("head = %d, want %d", head, wantHead)
	}

	// Exactly the newest liteResponseCacheLimit keys survive, in FIFO terms.
	for i := 0; i < total-liteResponseCacheLimit; i++ {
		if cached(i) {
			t.Fatalf("key %d should have been evicted", i)
		}
	}
	for i := total - liteResponseCacheLimit; i < total; i++ {
		if !cached(i) {
			t.Fatalf("key %d should still be cached", i)
		}
	}

	// A re-inserted evicted key rebuilds and displaces the current oldest key.
	oldest := total - liteResponseCacheLimit
	insert(0)
	if builds != total+1 {
		t.Fatalf("builds after reinsert = %d, want %d", builds, total+1)
	}
	if !cached(0) {
		t.Fatal("reinserted key 0 should be cached")
	}
	if cached(oldest) {
		t.Fatalf("key %d should have been evicted by the reinsert", oldest)
	}
	c.mu.RLock()
	items = len(c.items)
	c.mu.RUnlock()
	if items != liteResponseCacheLimit {
		t.Fatalf("items after reinsert = %d, want %d", items, liteResponseCacheLimit)
	}
}
