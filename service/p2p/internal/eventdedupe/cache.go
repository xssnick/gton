package eventdedupe

import (
	"container/list"
	"sync"
	"time"
)

// Cache suppresses recently seen string keys while bounding retained state.
type Cache struct {
	ttl        time.Duration
	maxEntries int
	mu         sync.Mutex
	seen       map[string]*entry
	order      *list.List
}

type entry struct {
	key     string
	seenAt  time.Time
	element *list.Element
}

func New(ttl time.Duration, maxEntries int) *Cache {
	return &Cache{
		ttl:        ttl,
		maxEntries: maxEntries,
		seen:       make(map[string]*entry),
		order:      list.New(),
	}
}

// Mark records key and reports whether it was absent or expired.
func (c *Cache) Mark(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if current := c.seen[key]; current != nil {
		if now.Sub(current.seenAt) < c.ttl {
			return false
		}
		current.seenAt = now
		c.order.MoveToBack(current.element)
		c.pruneExpiredLocked(now)
		c.pruneOverflowLocked()
		return true
	}

	current := &entry{
		key:    key,
		seenAt: now,
	}
	current.element = c.order.PushBack(current)
	c.seen[key] = current

	c.pruneExpiredLocked(now)
	c.pruneOverflowLocked()
	return true
}

func (c *Cache) Seen(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.seen[key]
	if current == nil {
		return false
	}
	if now.Sub(current.seenAt) >= c.ttl {
		c.deleteLocked(current)
		return false
	}
	return true
}

func (c *Cache) Forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.deleteLocked(c.seen[key])
}

func (c *Cache) pruneExpiredLocked(now time.Time) {
	for element := c.order.Front(); element != nil; {
		current := element.Value.(*entry)
		if now.Sub(current.seenAt) < c.ttl {
			return
		}
		next := element.Next()
		c.deleteLocked(current)
		element = next
	}
}

func (c *Cache) pruneOverflowLocked() {
	if c.maxEntries <= 0 {
		return
	}
	for len(c.seen) > c.maxEntries {
		c.deleteLocked(c.order.Front().Value.(*entry))
	}
}

func (c *Cache) deleteLocked(current *entry) {
	if current == nil {
		return
	}
	delete(c.seen, current.key)
	c.order.Remove(current.element)
	current.element = nil
}
