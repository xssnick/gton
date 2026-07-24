package p2p

import (
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

// decodedBroadcastCache remembers recently decoded block broadcasts keyed by
// (kind, block id). The same block routinely arrives once per delivery path
// (public-overlay FEC and custom-overlay two-step) with different broadcast
// fingerprints, so the payload deduper cannot collapse them; the decode
// result, however, is pinned by the block's root/file hash and is identical
// for every copy.
type decodedBroadcastCache struct {
	mu      sync.Mutex
	entries map[string]decodedBroadcastEntry
}

type decodedBroadcastEntry struct {
	downloaded DownloadedBlock
	signatures *blockproof.ValidatorSignatureSet
	storedAt   time.Time
}

func decodedBroadcastKey(kind string, block ton.BlockIDExt) string {
	return kind + "\x00" + string(block.RootHash) + string(block.FileHash)
}

func (c *decodedBroadcastCache) get(kind string, block ton.BlockIDExt) (*DownloadedBlock, *blockproof.ValidatorSignatureSet, error) {
	key := decodedBroadcastKey(kind, block)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, nil, storage.ErrNotFound
	}
	if time.Since(entry.storedAt) > decodedBroadcastCacheTTL {
		delete(c.entries, key)
		return nil, nil, storage.ErrNotFound
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
		c.entries = make(map[string]decodedBroadcastEntry, decodedBroadcastCacheLimit)
	}

	for existing, entry := range c.entries {
		if now.Sub(entry.storedAt) > decodedBroadcastCacheTTL {
			delete(c.entries, existing)
		}
	}
	if len(c.entries) >= decodedBroadcastCacheLimit {
		oldestKey := ""
		var oldestAt time.Time
		for existing, entry := range c.entries {
			if oldestKey == "" || entry.storedAt.Before(oldestAt) {
				oldestKey = existing
				oldestAt = entry.storedAt
			}
		}
		delete(c.entries, oldestKey)
	}

	c.entries[key] = decodedBroadcastEntry{
		downloaded: *downloaded,
		signatures: signatures,
		storedAt:   now,
	}
}
