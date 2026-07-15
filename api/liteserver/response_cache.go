package liteserver

import (
	"context"
	"sync"

	"github.com/xssnick/tonutils-go/ton"
)

const liteResponseCacheLimit = 512

type liteResponseKind uint8

const (
	liteResponseBlockHeader liteResponseKind = iota
	liteResponseAllShardsInfo
	liteResponseShardBlockLinks
	liteResponseBlockProofLinkForward
	liteResponseBlockProofLinkBackward
	liteResponseConfigAll
	liteResponseLookupMasterProofs
)

type liteResponseKey struct {
	kind liteResponseKind
	mode uint32
	a    liteBlockKey
	b    liteBlockKey
}

type liteBlockKey struct {
	workchain int32
	shard     int64
	seqno     uint32
	rootHash  [32]byte
	fileHash  [32]byte
}

func liteBlockKeyFromBlock(block ton.BlockIDExt) liteBlockKey {
	key := liteBlockKey{
		workchain: block.Workchain,
		shard:     block.Shard,
		seqno:     block.SeqNo,
	}
	copy(key.rootHash[:], block.RootHash)
	copy(key.fileHash[:], block.FileHash)
	return key
}

// liteResponseCache memoizes deterministic per-block response parts keyed by
// full block identities. Cached values are shared between responses and must be
// treated as read-only.
type liteResponseCache struct {
	mu    sync.RWMutex
	items map[liteResponseKey]any
	// order is a FIFO of cached keys. It grows up to liteResponseCacheLimit and
	// then becomes a circular buffer: head indexes the oldest key, which is
	// evicted and overwritten in place on every insert at capacity.
	order []liteResponseKey
	head  int
	load  liveLoadGroup[liteResponseKey]
}

func newLiteResponseCache() *liteResponseCache {
	return &liteResponseCache{items: map[liteResponseKey]any{}}
}

func (c *liteResponseCache) do(ctx context.Context, key liteResponseKey, build func(ctx context.Context) (any, error)) (any, error) {
	c.mu.RLock()
	cached, ok := c.items[key]
	c.mu.RUnlock()
	if ok {
		return cached, nil
	}

	return c.load.do(ctx, key, func() (any, error) {
		c.mu.RLock()
		cached, ok := c.items[key]
		c.mu.RUnlock()
		if ok {
			return cached, nil
		}

		// Build detached from the initiating request so one disconnecting client
		// cannot fail the shared result for concurrent waiters.
		value, err := build(context.WithoutCancel(ctx))
		if err != nil {
			return nil, err
		}

		c.mu.Lock()
		if existing, ok := c.items[key]; ok {
			value = existing
		} else {
			c.items[key] = value
			if len(c.order) < liteResponseCacheLimit {
				c.order = append(c.order, key)
			} else {
				delete(c.items, c.order[c.head])
				c.order[c.head] = key
				c.head = (c.head + 1) % len(c.order)
			}
		}
		c.mu.Unlock()
		return value, nil
	})
}

func queryErrorFromResponse(resp ton.LSError) error {
	return newQueryError(resp.Code, resp.Text)
}
