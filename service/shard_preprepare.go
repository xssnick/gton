package service

import (
	"context"
	"sync"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	preparedShardBlockTTL          = 3 * time.Minute
	preparedShardBlockMaxItems     = 512
	preparedShardBlockMaxBytes     = int64(256 << 20)
	preparedShardBlockWorkers      = 8
	preparedShardBlockFetchTimeout = 15 * time.Second

	// preparedShardBlockQueueSize bounds the requests parked between the feeds
	// (decoded broadcasts, description-hint prefetches) and the workers. Queued
	// payloads reference data already pinned by the p2p broadcast caches, so
	// the queue itself adds little memory.
	preparedShardBlockQueueSize = 32
)

type preparedShardBlockEntry struct {
	block    PreparedBlock
	bytes    int64
	storedAt time.Time
}

// preparedShardBlockCache holds shard blocks prepared ahead of the commit
// stage (parsed, state-update cells extracted), keyed by block root hash. The
// shard resolver takes them on the master commit critical path so only the
// merkle apply and bookkeeping remain there.
type preparedShardBlockCache struct {
	mu       sync.Mutex
	entries  map[storage.BlockRootHash]preparedShardBlockEntry
	order    []storage.BlockRootHash
	bytes    int64
	inflight map[storage.BlockRootHash]struct{}
}

func (c *preparedShardBlockCache) take(block ton.BlockIDExt) (PreparedBlock, error) {
	key := storage.BlockKey(block)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return PreparedBlock{}, storage.ErrNotFound
	}
	delete(c.entries, key)
	c.bytes -= entry.bytes
	if time.Since(entry.storedAt) > preparedShardBlockTTL {
		return PreparedBlock{}, storage.ErrNotFound
	}
	return entry.block, nil
}

// beginPrepare reserves the block for a prepare worker. It returns false when
// the block is already prepared or another worker is preparing it.
func (c *preparedShardBlockCache) beginPrepare(block ton.BlockIDExt) bool {
	key := storage.BlockKey(block)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.pruneLocked(time.Now())
	if _, ok := c.entries[key]; ok {
		return false
	}
	if _, ok := c.inflight[key]; ok {
		return false
	}
	if c.inflight == nil {
		c.inflight = map[storage.BlockRootHash]struct{}{}
	}
	c.inflight[key] = struct{}{}
	return true
}

func (c *preparedShardBlockCache) abortPrepare(block ton.BlockIDExt) {
	key := storage.BlockKey(block)

	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()
}

func (c *preparedShardBlockCache) storePrepared(prepared PreparedBlock) {
	key := storage.BlockKey(prepared.ID)
	// Same budget shape as the queued masterchain blocks: payload plus an
	// approximate decoded-cells overhead.
	bytes := queuedMasterchainBlockBytes(prepared)
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.inflight, key)
	if bytes > preparedShardBlockMaxBytes {
		return
	}
	if _, ok := c.entries[key]; ok {
		return
	}
	if c.entries == nil {
		c.entries = map[storage.BlockRootHash]preparedShardBlockEntry{}
	}
	c.entries[key] = preparedShardBlockEntry{
		block:    prepared,
		bytes:    bytes,
		storedAt: now,
	}
	c.order = append(c.order, key)
	c.bytes += bytes
	c.pruneLocked(now)
}

// pruneLocked drops expired entries and evicts oldest ones while the cache is
// over its limits. Taken entries leave tombstone keys in order; those are
// skipped for free here.
func (c *preparedShardBlockCache) pruneLocked(now time.Time) {
	cutoff := now.Add(-preparedShardBlockTTL)
	for len(c.order) > 0 {
		key := c.order[0]
		entry, ok := c.entries[key]
		if ok && entry.storedAt.After(cutoff) {
			break
		}
		c.order = c.order[1:]
		if ok {
			c.bytes -= entry.bytes
			delete(c.entries, key)
		}
	}
	for len(c.order) > 0 && (len(c.entries) > preparedShardBlockMaxItems || c.bytes > preparedShardBlockMaxBytes) {
		key := c.order[0]
		c.order = c.order[1:]
		if entry, ok := c.entries[key]; ok {
			c.bytes -= entry.bytes
			delete(c.entries, key)
		}
	}
}

// known reports the block is already prepared or being prepared. It is a
// read-only peek used to keep duplicate feed requests out of the bounded
// queue; beginPrepare stays the one authority a worker reserves through.
func (c *preparedShardBlockCache) known(block ton.BlockIDExt) bool {
	key := storage.BlockKey(block)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.entries[key]; ok {
		return true
	}
	_, ok := c.inflight[key]
	return ok
}

// shardPrepareRequest is one shard block to verify and pre-prepare. At most
// one payload field is set: verified comes from an already-verified synced
// block, downloaded from a decoded broadcast, and neither means the worker
// fetches the block by id (a hot-cache hit after a description prefetch).
type shardPrepareRequest struct {
	block      ton.BlockIDExt
	verified   *VerifiedBlock
	downloaded *p2p.DownloadedBlock
}

// enqueueShardBlockPrepare feeds the pre-prepare workers without blocking the
// caller (broadcast accept paths and the serial block processor both land
// here). Refuse-newest on a full queue: the commit stage consumes blocks
// oldest-first, so under a burst the queued older entries are the ones it
// needs next, while a refused block stays in the p2p hot cache and is
// prepared inline by the resolver later — losing an entry only costs latency,
// never correctness.
func (s *SyncCoordinator) enqueueShardBlockPrepare(req shardPrepareRequest) {
	if req.block.Workchain == -1 || s.shardPrepareQueue == nil {
		return
	}
	if s.preparedShardBlocks.known(req.block) {
		return
	}
	select {
	case s.shardPrepareQueue <- req:
	default:
	}
}

func (s *SyncCoordinator) runShardPrepareWorkers(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < preparedShardBlockWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case req := <-s.shardPrepareQueue:
					s.prepareShardBlockRequest(ctx, req)
				}
			}
		}()
	}
	wg.Wait()
}

func (s *SyncCoordinator) prepareShardBlockRequest(ctx context.Context, req shardPrepareRequest) {
	if !s.preparedShardBlocks.beginPrepare(req.block) {
		return
	}

	verified, err := s.verifiedShardBlockForPrepare(ctx, req)
	var prepared PreparedBlock
	if err == nil {
		prepared, err = prepareVerifiedBlockForApply(verified)
	}
	if err != nil {
		s.preparedShardBlocks.abortPrepare(req.block)
		if ctx.Err() == nil {
			s.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(req.block)).
				Msg("failed to prepare shard block ahead of apply")
		}
		return
	}
	s.preparedShardBlocks.storePrepared(prepared)
}

func (s *SyncCoordinator) verifiedShardBlockForPrepare(ctx context.Context, req shardPrepareRequest) (VerifiedBlock, error) {
	if req.verified != nil {
		return *req.verified, nil
	}
	if req.downloaded != nil {
		return s.verifyDownloadedBlock(*req.downloaded)
	}

	fetchCtx, cancel := context.WithTimeout(ctx, preparedShardBlockFetchTimeout)
	defer cancel()

	downloaded, err := s.node.DownloadBlockFull(fetchCtx, req.block)
	if err != nil {
		return VerifiedBlock{}, err
	}
	return s.verifyDownloadedBlock(*downloaded)
}

// prepareShardBlockAheadFromVerified queues an already-verified shard block so
// a prepare worker runs PrepareStateUpdateCells off the caller's goroutine.
func (s *SyncCoordinator) prepareShardBlockAheadFromVerified(verified VerifiedBlock) {
	s.enqueueShardBlockPrepare(shardPrepareRequest{block: verified.ID, verified: &verified})
}

// prepareShardBlockAheadByID queues a shard block (usually a broadcast-cache
// hit after a description prefetch) for fetch and pre-preparation.
func (s *SyncCoordinator) prepareShardBlockAheadByID(_ context.Context, block ton.BlockIDExt) {
	s.enqueueShardBlockPrepare(shardPrepareRequest{block: block})
}
