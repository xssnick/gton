package p2p

import (
	"container/list"
	"sync"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type broadcastBlockCache struct {
	mu sync.Mutex

	ttl      time.Duration
	maxBytes int64
	maxItems int
	kind     string

	entries map[tnstore.BlockRootHash]*broadcastBlockCacheEntry
	order   *list.List
	bytes   int64
}

type broadcastBlockCacheEntry struct {
	key          tnstore.BlockRootHash
	element      *list.Element
	block        ton.BlockIDExt
	kind         string
	blockRoot    *cell.Cell
	proofRoot    *cell.Cell
	stateUpdate  *cell.Cell
	blockBOC     []byte
	proofBOC     []byte
	isLink       bool
	meta         *tnstore.BlockMeta
	sourcePeerID PeerID
	expiresAt    time.Time
	bytes        int64
}

func newBroadcastBlockCache(ttl time.Duration, maxBytes int64, maxItems int, kind string) broadcastBlockCache {
	return broadcastBlockCache{
		ttl:      ttl,
		maxBytes: maxBytes,
		maxItems: maxItems,
		kind:     kind,
		entries:  map[tnstore.BlockRootHash]*broadcastBlockCacheEntry{},
		order:    list.New(),
	}
}

func (c *broadcastBlockCache) storeEntry(entry *broadcastBlockCacheEntry, now time.Time) {
	entry.expiresAt = now.Add(c.ttl)

	c.mu.Lock()
	defer c.mu.Unlock()

	if old := c.entries[entry.key]; old != nil {
		c.deleteEntryLocked(old)
	}

	entry.element = c.order.PushBack(entry)
	c.entries[entry.key] = entry
	c.bytes += entry.bytes

	c.pruneExpiredLocked(now)
	c.pruneOverflowLocked()
}

func (c *broadcastBlockCache) blockAt(key tnstore.BlockRootHash, now time.Time) (*DownloadedBlock, error) {
	if c == nil {
		return nil, tnstore.ErrNotFound
	}

	c.mu.Lock()
	entry := c.entries[key]
	if entry == nil {
		c.mu.Unlock()
		return nil, tnstore.ErrNotFound
	}
	if !entry.expiresAt.After(now) {
		c.deleteEntryLocked(entry)
		c.mu.Unlock()
		return nil, tnstore.ErrNotFound
	}

	downloaded := entry.downloaded(c.kind)
	c.mu.Unlock()
	return downloaded, nil
}

func (c *broadcastBlockCache) has(key tnstore.BlockRootHash, now time.Time) bool {
	if c == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.entries[key]
	if entry == nil {
		return false
	}
	if !entry.expiresAt.After(now) {
		c.deleteEntryLocked(entry)
		return false
	}
	return true
}

func (c *broadcastBlockCache) prune(now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pruneExpiredLocked(now)
	c.pruneOverflowLocked()
}

func (c *broadcastBlockCache) pruneExpiredLocked(now time.Time) {
	for elem := c.order.Front(); elem != nil; {
		entry := elem.Value.(*broadcastBlockCacheEntry)
		if !entry.expiresAt.After(now) {
			next := elem.Next()
			c.deleteEntryLocked(entry)
			elem = next
			continue
		}
		return
	}
}

func (c *broadcastBlockCache) pruneOverflowLocked() {
	for len(c.entries) > c.maxItems || c.bytes > c.maxBytes {
		elem := c.order.Front()
		if elem == nil {
			return
		}
		c.deleteEntryLocked(elem.Value.(*broadcastBlockCacheEntry))
	}
}

func (c *broadcastBlockCache) deleteEntryLocked(entry *broadcastBlockCacheEntry) {
	if entry == nil {
		return
	}
	delete(c.entries, entry.key)
	if entry.element != nil {
		c.order.Remove(entry.element)
		entry.element = nil
	}
	c.bytes -= entry.bytes
	if c.bytes < 0 {
		c.bytes = 0
	}
}

func (e *broadcastBlockCacheEntry) downloaded(defaultKind string) *DownloadedBlock {
	kind := e.kind
	if kind == "" {
		kind = defaultKind
	}
	return &DownloadedBlock{
		ID:               cloneBlockID(e.block),
		Kind:             kind,
		Block:            e.blockRoot,
		Proof:            e.proofRoot,
		BlockBOC:         e.blockBOC,
		ProofBOC:         e.proofBOC,
		Meta:             e.meta.Clone(),
		StateUpdate:      e.stateUpdate,
		SourcePeerID:     e.sourcePeerID,
		IsLink:           e.isLink,
		VerifiedRootHash: true,
	}
}
