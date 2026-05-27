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
)

var sendMessageCRC64Table = crc64.MakeTable(crc64.ECMA)

type sendMessageCache struct {
	ttl        time.Duration
	maxEntries int
	mx         sync.Mutex
	seen       map[uint64]*sendMessageCacheEntry
	order      *list.List
}

type sendMessageCacheEntry struct {
	key     uint64
	seenAt  time.Time
	element *list.Element
}

func newSendMessageCache() *sendMessageCache {
	return &sendMessageCache{
		ttl:        sendMessageCacheTTL,
		maxEntries: sendMessageCacheMaxEntries,
		seen:       map[uint64]*sendMessageCacheEntry{},
		order:      list.New(),
	}
}

func (c *sendMessageCache) Mark(key uint64, now time.Time) bool {
	c.mx.Lock()
	defer c.mx.Unlock()

	if entry := c.seen[key]; entry != nil {
		if now.Sub(entry.seenAt) < c.ttl {
			return false
		}
		entry.seenAt = now
		c.order.MoveToBack(entry.element)
		c.pruneExpiredLocked(now)
		c.pruneOverflowLocked()
		return true
	}

	entry := &sendMessageCacheEntry{
		key:    key,
		seenAt: now,
	}
	entry.element = c.order.PushBack(entry)
	c.seen[key] = entry

	c.pruneExpiredLocked(now)
	c.pruneOverflowLocked()
	return true
}

func (c *sendMessageCache) Drop(key uint64) {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.deleteEntryLocked(c.seen[key])
}

func (c *sendMessageCache) pruneExpiredLocked(now time.Time) {
	for elem := c.order.Front(); elem != nil; {
		entry := elem.Value.(*sendMessageCacheEntry)
		if now.Sub(entry.seenAt) < c.ttl {
			return
		}
		next := elem.Next()
		c.deleteEntryLocked(entry)
		elem = next
	}
}

func (c *sendMessageCache) pruneOverflowLocked() {
	if c.maxEntries <= 0 {
		return
	}
	for len(c.seen) > c.maxEntries {
		elem := c.order.Front()
		if elem == nil {
			return
		}
		c.deleteEntryLocked(elem.Value.(*sendMessageCacheEntry))
	}
}

func (c *sendMessageCache) deleteEntryLocked(entry *sendMessageCacheEntry) {
	if entry == nil {
		return
	}
	delete(c.seen, entry.key)
	if elem := entry.element; elem != nil {
		c.order.Remove(elem)
		entry.element = nil
	}
}

func (s *Server) cacheSendMessage(body []byte) (uint64, bool) {
	key := crc64.Checksum(body, sendMessageCRC64Table)
	cache := s.ensureSendMessageCache()
	return key, cache.Mark(key, s.now())
}

func (s *Server) dropCachedSendMessage(key uint64) {
	s.ensureSendMessageCache().Drop(key)
}

func (s *Server) ensureSendMessageCache() *sendMessageCache {
	s.sendMessageCacheInitMu.Lock()
	defer s.sendMessageCacheInitMu.Unlock()

	if s.sendMessageCache == nil {
		s.sendMessageCache = newSendMessageCache()
	}
	return s.sendMessageCache
}
