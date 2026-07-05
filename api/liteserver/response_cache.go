package liteserver

import (
	"context"
	"sync"

	"github.com/xssnick/gton/service/storage"

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
)

type liteResponseKey struct {
	kind liteResponseKind
	mode uint32
	a    storage.BlockRootHash
	b    storage.BlockRootHash
}

// liteResponseCache memoizes deterministic per-block response parts keyed by the
// immutable block content hashes. Cached values are shared between responses and
// must be treated as read-only.
type liteResponseCache struct {
	mu    sync.RWMutex
	items map[liteResponseKey]any
	order []liteResponseKey
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
			c.order = append(c.order, key)
			if len(c.order) > liteResponseCacheLimit {
				delete(c.items, c.order[0])
				copy(c.order, c.order[1:])
				c.order = c.order[:len(c.order)-1]
			}
		}
		c.mu.Unlock()
		return value, nil
	})
}

func queryErrorFromResponse(resp ton.LSError) error {
	return newQueryError(resp.Code, resp.Text)
}
