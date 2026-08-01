package externalmsg

import (
	"testing"
	"time"
)

func TestMessageCacheMarkDropAndExpiry(t *testing.T) {
	cache := NewMessageCache()
	now := time.Unix(1700000000, 0)

	if !cache.Mark(1, now) {
		t.Fatal("first mark should accept message")
	}
	if cache.Mark(1, now.Add(time.Second)) {
		t.Fatal("duplicate mark inside TTL should reject message")
	}

	cache.Drop(1)
	if !cache.Mark(1, now.Add(2*time.Second)) {
		t.Fatal("dropped message should be accepted again")
	}

	if !cache.Mark(2, now) {
		t.Fatal("first mark for second key should accept message")
	}
	if !cache.Mark(2, now.Add(messageCacheTTL)) {
		t.Fatal("expired message should be accepted again")
	}
}

func TestMessageCachePrunesOverflowPerShard(t *testing.T) {
	cache := NewMessageCache()
	now := time.Unix(1700000000, 0)

	first := uint64(7)
	for i := 0; i <= messageCacheEntriesPerShard; i++ {
		key := first + uint64(i*messageCacheShards)
		if !cache.Mark(key, now.Add(time.Duration(i)*time.Millisecond)) {
			t.Fatalf("mark %d should accept message", i)
		}
	}
	if !cache.Mark(first, now.Add(3*time.Second)) {
		t.Fatal("oldest same-shard message should be pruned on overflow")
	}
}
