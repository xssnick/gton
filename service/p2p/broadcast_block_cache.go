package p2p

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

// broadcastBlockCacheHotTTL bridges broadcast arrival to the immediate sync
// consumer without another DAG decode. The recovery entry itself lives longer,
// but only as BOCs; the janitor releases parsed roots after this short window.
const broadcastBlockCacheHotTTL = 5 * time.Second

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
	blockBOC     []byte
	proofBOC     []byte
	isLink       bool
	sourcePeerID PeerID
	expiresAt    time.Time
	bytes        int64
	hot          *DownloadedBlock
	hotExpiresAt time.Time
	// signaturesVerifiedKey preserves DownloadedBlock.SignaturesVerifiedKey
	// across the cache round-trip.
	signaturesVerifiedKey []byte
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
	if entry.hot != nil {
		entry.hotExpiresAt = now.Add(broadcastBlockCacheHotTTL)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseExpiredHotLocked(now)

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

	if entry.hot != nil && entry.hotExpiresAt.After(now) {
		downloaded := cloneCachedDownloadedBlock(*entry.hot)
		c.mu.Unlock()
		return &downloaded, nil
	}
	entry.hot = nil
	entry.hotExpiresAt = time.Time{}
	snapshot := *entry
	c.mu.Unlock()

	return snapshot.downloaded(c.kind)
}

func (c *broadcastBlockCache) has(key tnstore.BlockRootHash, now time.Time) bool {
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
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pruneExpiredLocked(now)
	c.releaseExpiredHotLocked(now)
	c.pruneOverflowLocked()
}

func (c *broadcastBlockCache) releaseExpiredHotLocked(now time.Time) {
	for _, entry := range c.entries {
		if entry.hot == nil || entry.hotExpiresAt.After(now) {
			continue
		}
		entry.hot = nil
		entry.hotExpiresAt = time.Time{}
	}
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
		c.deleteEntryLocked(elem.Value.(*broadcastBlockCacheEntry))
	}
}

func (c *broadcastBlockCache) deleteEntryLocked(entry *broadcastBlockCacheEntry) {
	delete(c.entries, entry.key)
	c.order.Remove(entry.element)
	entry.element = nil
	c.bytes -= entry.bytes
}

func (e *broadcastBlockCacheEntry) downloaded(defaultKind string) (*DownloadedBlock, error) {
	kind := e.kind
	if kind == "" {
		kind = defaultKind
	}

	proofRoot, err := parseDownloadedBlockProof(e.proofBOC)
	if err != nil {
		return nil, fmt.Errorf("decode cached %s proof: %w", kind, err)
	}
	downloaded, err := decodeRawDownloadedBlockWithShape(
		kind,
		cloneBlockID(e.block),
		e.proofBOC,
		e.blockBOC,
		e.isLink,
		proofRoot,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("decode cached %s block: %w", kind, err)
	}
	downloaded.SourcePeerID = e.sourcePeerID
	downloaded.SignaturesVerifiedKey = append([]byte(nil), e.signaturesVerifiedKey...)

	return downloaded, nil
}

func cloneCachedDownloadedBlock(downloaded DownloadedBlock) DownloadedBlock {
	downloaded.ID = cloneBlockID(downloaded.ID)
	downloaded.Meta = downloaded.Meta.Clone()
	downloaded.SignaturesVerifiedKey = append([]byte(nil), downloaded.SignaturesVerifiedKey...)

	return downloaded
}

func downloadedHotCacheEntry(downloaded DownloadedBlock) *DownloadedBlock {
	cloned := cloneCachedDownloadedBlock(downloaded)

	return &cloned
}
