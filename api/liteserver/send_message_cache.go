package liteserver

import (
	"container/list"
	"hash/crc64"
	"sync"
	"time"
)

const (
	sendMessageCacheTTL        = time.Minute
	sendMessageCacheMaxEntries = 1 << 17
	sendMessageCacheShards     = 64
)

var sendMessageCRC64Table = crc64.MakeTable(crc64.ECMA)

type sendMessageCache struct {
	ttl        time.Duration
	maxEntries int
	shards     [sendMessageCacheShards]sendMessageCacheShard
}

type sendMessageCacheShard struct {
	mx    sync.Mutex
	seen  map[uint64]*sendMessageCacheEntry
	order *list.List
}

type sendMessageCacheEntry struct {
	key     uint64
	seenAt  time.Time
	element *list.Element
}

func newSendMessageCache() *sendMessageCache {
	cache := &sendMessageCache{
		ttl:        sendMessageCacheTTL,
		maxEntries: sendMessageCacheMaxEntries,
	}
	for i := range cache.shards {
		cache.shards[i] = sendMessageCacheShard{
			seen:  map[uint64]*sendMessageCacheEntry{},
			order: list.New(),
		}
	}
	return cache
}

func (c *sendMessageCache) Mark(key uint64, now time.Time) bool {
	shard := c.shard(key)
	shard.mx.Lock()
	defer shard.mx.Unlock()

	if entry := shard.seen[key]; entry != nil {
		if now.Sub(entry.seenAt) < c.ttl {
			return false
		}
		entry.seenAt = now
		shard.order.MoveToBack(entry.element)
		shard.pruneExpiredLocked(now, c.ttl)
		shard.pruneOverflowLocked(c.maxEntriesPerShard())
		return true
	}

	entry := &sendMessageCacheEntry{
		key:    key,
		seenAt: now,
	}
	entry.element = shard.order.PushBack(entry)
	shard.seen[key] = entry

	shard.pruneExpiredLocked(now, c.ttl)
	shard.pruneOverflowLocked(c.maxEntriesPerShard())
	return true
}

func (c *sendMessageCache) Drop(key uint64) {
	shard := c.shard(key)
	shard.mx.Lock()
	defer shard.mx.Unlock()

	shard.deleteEntryLocked(shard.seen[key])
}

func (c *sendMessageCache) shard(key uint64) *sendMessageCacheShard {
	return &c.shards[key&(sendMessageCacheShards-1)]
}

func (c *sendMessageCache) maxEntriesPerShard() int {
	if c.maxEntries <= 0 {
		return 0
	}
	return (c.maxEntries + sendMessageCacheShards - 1) / sendMessageCacheShards
}

func (s *sendMessageCacheShard) pruneExpiredLocked(now time.Time, ttl time.Duration) {
	for elem := s.order.Front(); elem != nil; {
		entry := elem.Value.(*sendMessageCacheEntry)
		if now.Sub(entry.seenAt) < ttl {
			return
		}
		next := elem.Next()
		s.deleteEntryLocked(entry)
		elem = next
	}
}

func (s *sendMessageCacheShard) pruneOverflowLocked(maxEntries int) {
	if maxEntries <= 0 {
		return
	}
	for len(s.seen) > maxEntries {
		elem := s.order.Front()
		if elem == nil {
			return
		}
		s.deleteEntryLocked(elem.Value.(*sendMessageCacheEntry))
	}
}

func (s *sendMessageCacheShard) deleteEntryLocked(entry *sendMessageCacheEntry) {
	if entry == nil {
		return
	}
	delete(s.seen, entry.key)
	if elem := entry.element; elem != nil {
		s.order.Remove(elem)
		entry.element = nil
	}
}

func (s *Server) cacheSendMessage(body []byte) (uint64, bool) {
	key := crc64.Checksum(body, sendMessageCRC64Table)
	return key, s.sendMessageCache.Mark(key, s.now())
}

func (s *Server) dropCachedSendMessage(key uint64) {
	s.sendMessageCache.Drop(key)
}
