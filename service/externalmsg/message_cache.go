package externalmsg

import (
	"container/list"
	"hash/crc64"
	"sync"
	"time"
)

const (
	messageCacheTTL             = time.Minute
	messageCacheMaxEntries      = 1 << 17
	messageCacheShards          = 64
	messageCacheEntriesPerShard = (messageCacheMaxEntries + messageCacheShards - 1) / messageCacheShards
)

var messageCRC64Table = crc64.MakeTable(crc64.ECMA)

type MessageCache struct {
	shards [messageCacheShards]messageCacheShard
}

type messageCacheShard struct {
	mx    sync.Mutex
	seen  map[uint64]*messageCacheEntry
	order *list.List
}

type messageCacheEntry struct {
	key     uint64
	seenAt  time.Time
	element *list.Element
}

func NewMessageCache() *MessageCache {
	cache := &MessageCache{}
	for i := range cache.shards {
		cache.shards[i] = messageCacheShard{
			seen:  map[uint64]*messageCacheEntry{},
			order: list.New(),
		}
	}
	return cache
}

func (c *MessageCache) Mark(key uint64, now time.Time) bool {
	shard := c.shard(key)
	shard.mx.Lock()
	defer shard.mx.Unlock()

	if entry := shard.seen[key]; entry != nil {
		if now.Sub(entry.seenAt) < messageCacheTTL {
			return false
		}
		entry.seenAt = now
		shard.order.MoveToBack(entry.element)
		shard.pruneExpiredLocked(now, messageCacheTTL)
		shard.pruneOverflowLocked(messageCacheEntriesPerShard)
		return true
	}

	entry := &messageCacheEntry{
		key:    key,
		seenAt: now,
	}
	entry.element = shard.order.PushBack(entry)
	shard.seen[key] = entry

	shard.pruneExpiredLocked(now, messageCacheTTL)
	shard.pruneOverflowLocked(messageCacheEntriesPerShard)
	return true
}

func (c *MessageCache) Drop(key uint64) {
	shard := c.shard(key)
	shard.mx.Lock()
	defer shard.mx.Unlock()

	shard.deleteEntryLocked(shard.seen[key])
}

func (c *MessageCache) shard(key uint64) *messageCacheShard {
	return &c.shards[key&(messageCacheShards-1)]
}

func MessageCacheKey(body []byte) uint64 {
	return crc64.Checksum(body, messageCRC64Table)
}

func (s *messageCacheShard) pruneExpiredLocked(now time.Time, ttl time.Duration) {
	for elem := s.order.Front(); elem != nil; {
		entry := elem.Value.(*messageCacheEntry)
		if now.Sub(entry.seenAt) < ttl {
			return
		}
		next := elem.Next()
		s.deleteEntryLocked(entry)
		elem = next
	}
}

func (s *messageCacheShard) pruneOverflowLocked(maxEntries int) {
	if maxEntries <= 0 {
		return
	}
	for len(s.seen) > maxEntries {
		elem := s.order.Front()
		s.deleteEntryLocked(elem.Value.(*messageCacheEntry))
	}
}

func (s *messageCacheShard) deleteEntryLocked(entry *messageCacheEntry) {
	if entry == nil {
		return
	}
	delete(s.seen, entry.key)
	s.order.Remove(entry.element)
	entry.element = nil
}
