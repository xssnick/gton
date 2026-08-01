package p2p

import (
	"errors"
	"sync"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	decodedBroadcastCacheLimit = 32
	decodedBroadcastCacheTTL   = 30 * time.Second
)

var errDecodedBroadcastProcessed = errors.New("broadcast decode already processed")

// decodedBroadcastCache remembers recently decoded block broadcasts keyed by
// (kind, block id). The same block routinely arrives once per delivery path
// (public-overlay FEC and custom-overlay two-step) with different broadcast
// fingerprints, so the payload deduper cannot collapse them; the decode
// result, however, is pinned by the block's root/file hash and is identical
// for every copy. Once the first delivery finishes local processing, the full
// result is replaced by a payload-free marker so later copies stay relay-only.
// Entries are held by pointer: the TTL sweep and the eviction scan only read
// storedAt, and ranging a map of 272-byte values copies the whole entry (the
// DownloadedBlock included) on every iteration to get at it.
type decodedBroadcastCache struct {
	mu      sync.Mutex
	entries map[string]*decodedBroadcastEntry
}

type decodedBroadcastEntry struct {
	downloaded DownloadedBlock
	signatures *blockproof.ValidatorSignatureSet
	storedAt   time.Time
	processed  bool
}

func decodedBroadcastKey(kind string, block ton.BlockIDExt) string {
	return kind + "\x00" + string(block.RootHash) + string(block.FileHash)
}

func (c *decodedBroadcastCache) get(kind string, block ton.BlockIDExt) (*DownloadedBlock, *blockproof.ValidatorSignatureSet, error) {
	key := decodedBroadcastKey(kind, block)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.entries[key]
	if entry == nil {
		return nil, nil, storage.ErrNotFound
	}
	if time.Since(entry.storedAt) > decodedBroadcastCacheTTL {
		delete(c.entries, key)
		return nil, nil, storage.ErrNotFound
	}
	if entry.processed {
		return nil, nil, errDecodedBroadcastProcessed
	}

	// callers mutate per-delivery fields (SourcePeerID, SignaturesVerifiedKey),
	// so hand out a copy; pointer fields are immutable after decode
	downloaded := entry.downloaded
	return &downloaded, entry.signatures, nil
}

func (c *decodedBroadcastCache) put(kind string, block ton.BlockIDExt, downloaded *DownloadedBlock, signatures *blockproof.ValidatorSignatureSet) {
	key := decodedBroadcastKey(kind, block)
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries == nil {
		c.entries = make(map[string]*decodedBroadcastEntry, decodedBroadcastCacheLimit)
	}

	c.pruneExpiredLocked(now)
	existing := c.entries[key]
	if existing != nil && existing.processed {
		return
	}
	if existing == nil && len(c.entries) >= decodedBroadcastCacheLimit {
		c.evictOldestLocked()
	}

	c.entries[key] = &decodedBroadcastEntry{
		downloaded: *downloaded,
		signatures: signatures,
		storedAt:   now,
	}
}

func (c *decodedBroadcastCache) markProcessed(kind string, block ton.BlockIDExt) {
	key := decodedBroadcastKey(kind, block)
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries == nil {
		c.entries = make(map[string]*decodedBroadcastEntry, decodedBroadcastCacheLimit)
	}
	// Overwrite the whole value so no graph, BOC or signature slice remains
	// reachable. put preserves this marker if a concurrent decode finishes late.
	// The common case is replacing the entry put() created moments ago on this
	// goroutine, which neither expires anything nor needs a slot — take that
	// path without the full TTL sweep.
	if entry := c.entries[key]; entry != nil {
		*entry = decodedBroadcastEntry{
			storedAt:  now,
			processed: true,
		}
		return
	}

	// Reached when the decode was reused from an earlier delivery's entry (no
	// put on this goroutine) or when concurrent puts evicted the key: this is
	// the sweep that releases those older payloads.
	c.pruneExpiredLocked(now)
	if len(c.entries) >= decodedBroadcastCacheLimit {
		c.evictOldestLocked()
	}
	c.entries[key] = &decodedBroadcastEntry{
		storedAt:  now,
		processed: true,
	}
}

func (c *decodedBroadcastCache) pruneExpiredLocked(now time.Time) {
	for key, entry := range c.entries {
		if now.Sub(entry.storedAt) > decodedBroadcastCacheTTL {
			delete(c.entries, key)
		}
	}
}

func (c *decodedBroadcastCache) evictOldestLocked() {
	oldestKey := ""
	var oldestAt time.Time
	for key, entry := range c.entries {
		if oldestKey == "" || entry.storedAt.Before(oldestAt) {
			oldestKey = key
			oldestAt = entry.storedAt
		}
	}
	delete(c.entries, oldestKey)
}
