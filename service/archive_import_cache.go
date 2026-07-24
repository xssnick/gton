package service

import (
	"context"
	"errors"
	"sync"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"
)

type archiveImportResult struct {
	stats      *archive.ImportStats
	blocks     map[storage.BlockRootHash]PreparedBlock
	splitDepth uint32
	cacheKey   archiveImportCacheKey
}

type archiveImportCacheKey struct {
	masterchainSeqno uint32
	shard            archive.ShardID
	splitDepth       uint32
}

type archiveImportWaiter struct {
	done   chan struct{}
	result *archiveImportResult
	err    error
}

type archiveImportDownload struct {
	imported *archiveImportResult
	cached   bool
}

type archiveImportCacheLoad struct {
	archiveImportDownload
	retry bool
}

type archiveImportCache struct {
	mu       sync.Mutex
	entries  map[archiveImportCacheKey]*archiveImportResult
	waiters  map[archiveImportCacheKey]*archiveImportWaiter
	hitCount uint64
}

func newArchiveImportCache() *archiveImportCache {
	return &archiveImportCache{
		entries: map[archiveImportCacheKey]*archiveImportResult{},
		waiters: map[archiveImportCacheKey]*archiveImportWaiter{},
	}
}

func (c *archiveImportCache) load(ctx context.Context, key archiveImportCacheKey, load func(context.Context) (*archiveImportResult, error)) (archiveImportDownload, error) {
	for {
		loaded, err := c.loadOnce(ctx, key, load)
		if !loaded.retry {
			return loaded.archiveImportDownload, err
		}
	}
}

func (c *archiveImportCache) loadOnce(ctx context.Context, key archiveImportCacheKey, load func(context.Context) (*archiveImportResult, error)) (archiveImportCacheLoad, error) {
	c.mu.Lock()
	if result := c.entries[key]; result != nil {
		c.hitCount++
		c.mu.Unlock()
		return archiveImportCacheLoad{
			archiveImportDownload: archiveImportDownload{
				imported: cloneArchiveImportResult(result),
				cached:   true,
			},
		}, nil
	}
	if waiter := c.waiters[key]; waiter != nil {
		c.mu.Unlock()
		select {
		case <-waiter.done:
			if waiter.err != nil {
				if errors.Is(waiter.err, context.Canceled) && ctx.Err() == nil {
					return archiveImportCacheLoad{retry: true}, nil
				}
				return archiveImportCacheLoad{}, waiter.err
			}
			return archiveImportCacheLoad{
				archiveImportDownload: archiveImportDownload{
					imported: cloneArchiveImportResult(waiter.result),
					cached:   true,
				},
			}, nil
		case <-ctx.Done():
			return archiveImportCacheLoad{}, ctx.Err()
		}
	}

	waiter := &archiveImportWaiter{done: make(chan struct{})}
	c.waiters[key] = waiter
	c.mu.Unlock()

	result, err := load(ctx)

	c.mu.Lock()
	if err == nil {
		c.entries[key] = cloneArchiveImportResult(result)
	}
	waiter.result = cloneArchiveImportResult(result)
	waiter.err = err
	delete(c.waiters, key)
	close(waiter.done)
	c.mu.Unlock()

	return archiveImportCacheLoad{archiveImportDownload: archiveImportDownload{imported: result}}, err
}

func (c *archiveImportCache) drop(key archiveImportCacheKey) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

func (c *archiveImportCache) dropBefore(masterchainSeqno uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		if key.masterchainSeqno < masterchainSeqno {
			delete(c.entries, key)
		}
	}
}

func (c *archiveImportCache) stats() (int, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.hitCount
}

func cloneImportStats(stats *archive.ImportStats) *archive.ImportStats {
	cloned := *stats
	return &cloned
}

func cloneArchiveImportResult(result *archiveImportResult) *archiveImportResult {
	if result == nil {
		return nil
	}

	cloned := &archiveImportResult{
		stats:      cloneImportStats(result.stats),
		blocks:     make(map[storage.BlockRootHash]PreparedBlock, len(result.blocks)),
		splitDepth: result.splitDepth,
		cacheKey:   result.cacheKey,
	}
	for key, block := range result.blocks {
		block.Meta = block.Meta.Clone()
		cloned.blocks[key] = block
	}
	return cloned
}
