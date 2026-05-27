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
	blocks     map[string]PreparedBlock
	stored     storage.ServedArchiveImport
	splitDepth uint32
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
	if c == nil {
		imported, err := load(ctx)
		return archiveImportDownload{imported: imported}, err
	}

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
	if err == nil && result != nil {
		c.entries[key] = cloneArchiveImportResult(result)
	}
	waiter.result = cloneArchiveImportResult(result)
	waiter.err = err
	delete(c.waiters, key)
	close(waiter.done)
	c.mu.Unlock()

	return archiveImportCacheLoad{archiveImportDownload: archiveImportDownload{imported: result}}, err
}

func (c *archiveImportCache) dropBefore(masterchainSeqno uint32) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		if key.masterchainSeqno < masterchainSeqno {
			delete(c.entries, key)
		}
	}
}

func (c *archiveImportCache) drop(key archiveImportCacheKey) {
	if c == nil {
		return
	}

	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

func (c *archiveImportCache) stats() (int, uint64) {
	if c == nil {
		return 0, 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.hitCount
}

func cloneImportStats(stats *archive.ImportStats) *archive.ImportStats {
	if stats == nil {
		return &archive.ImportStats{}
	}
	cloned := *stats
	return &cloned
}

func cloneArchiveImportResult(result *archiveImportResult) *archiveImportResult {
	if result == nil {
		return nil
	}

	cloned := &archiveImportResult{
		stats:      cloneImportStats(result.stats),
		blocks:     make(map[string]PreparedBlock, len(result.blocks)),
		stored:     cloneServedArchiveImport(result.stored),
		splitDepth: result.splitDepth,
	}
	for key, block := range result.blocks {
		cloned.blocks[key] = block
	}
	return cloned
}

func cloneServedArchiveImport(imported storage.ServedArchiveImport) storage.ServedArchiveImport {
	cloned := storage.ServedArchiveImport{
		FullBlocks: make([]*storage.ServedBlockFull, 0, len(imported.FullBlocks)),
		BlockData:  make([]storage.ServedBlockData, 0, len(imported.BlockData)),
		Proofs:     make([]storage.ServedBlockProof, 0, len(imported.Proofs)),
		Links:      append([]storage.ServedBlockLink(nil), imported.Links...),
	}
	for _, full := range imported.FullBlocks {
		if full == nil {
			cloned.FullBlocks = append(cloned.FullBlocks, nil)
			continue
		}
		next := *full
		next.ProofRef = full.ProofRef.Clone()
		next.BlockRef = full.BlockRef.Clone()
		next.Meta = full.Meta.Clone()
		cloned.FullBlocks = append(cloned.FullBlocks, &next)
	}
	for _, block := range imported.BlockData {
		cloned.BlockData = append(cloned.BlockData, cloneServedBlockData(block))
	}
	for _, proof := range imported.Proofs {
		cloned.Proofs = append(cloned.Proofs, cloneServedBlockProof(proof))
	}
	return cloned
}

func cloneServedBlockData(block storage.ServedBlockData) storage.ServedBlockData {
	return storage.ServedBlockData{
		ID:   block.ID,
		Data: block.Data,
		Ref:  block.Ref.Clone(),
	}
}

func cloneServedBlockProof(proof storage.ServedBlockProof) storage.ServedBlockProof {
	return storage.ServedBlockProof{
		Kind: proof.Kind,
		ID:   proof.ID,
		Data: proof.Data,
		Ref:  proof.Ref.Clone(),
	}
}
