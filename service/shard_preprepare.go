package service

import (
	"context"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	preparedShardBlockTTL          = 3 * time.Minute
	preparedShardBlockMaxItems     = 512
	preparedShardBlockMaxBytes     = int64(256 << 20)
	preparedShardBlockWorkers      = 3
	preparedShardBlockFetchTimeout = 15 * time.Second
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

func (c *preparedShardBlockCache) take(block ton.BlockIDExt) (PreparedBlock, bool) {
	key := storage.BlockKey(block)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return PreparedBlock{}, false
	}
	delete(c.entries, key)
	c.bytes -= entry.bytes
	if c.bytes < 0 {
		c.bytes = 0
	}
	if time.Since(entry.storedAt) > preparedShardBlockTTL {
		return PreparedBlock{}, false
	}
	return entry.block, true
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
	if c.bytes < 0 {
		c.bytes = 0
	}
}

// prepareShardBlockAheadFromVerified prepares an already-verified shard block
// in the caller's goroutine, bounded by the prepare worker slots; when all
// slots are busy the block is skipped and the resolver prepares it on demand.
func (s *Service) prepareShardBlockAheadFromVerified(verified VerifiedBlock) {
	if verified.ID.Workchain == -1 {
		return
	}
	if !s.preparedShardBlocks.beginPrepare(verified.ID) {
		return
	}
	select {
	case s.preparedShardBlockSlots <- struct{}{}:
	default:
		s.preparedShardBlocks.abortPrepare(verified.ID)
		return
	}
	defer func() { <-s.preparedShardBlockSlots }()

	prepared, err := prepareVerifiedBlockForApply(verified)
	if err != nil {
		s.preparedShardBlocks.abortPrepare(verified.ID)
		s.log.Debug().
			Err(err).
			Str("block", verified.BlockRef()).
			Msg("failed to prepare shard block ahead of apply")
		return
	}
	s.preparedShardBlocks.storePrepared(prepared)
}

// prepareShardBlockAheadByID fetches a shard block (usually a broadcast-cache
// hit after a description prefetch) and prepares it ahead of apply.
func (s *Service) prepareShardBlockAheadByID(ctx context.Context, block ton.BlockIDExt) {
	if block.Workchain == -1 || s.node == nil {
		return
	}
	if !s.preparedShardBlocks.beginPrepare(block) {
		return
	}
	select {
	case s.preparedShardBlockSlots <- struct{}{}:
	default:
		s.preparedShardBlocks.abortPrepare(block)
		return
	}
	defer func() { <-s.preparedShardBlockSlots }()

	fetchCtx, cancel := context.WithTimeout(ctx, preparedShardBlockFetchTimeout)
	defer cancel()

	downloaded, err := s.node.DownloadBlockFull(fetchCtx, block)
	if err == nil && downloaded == nil {
		err = storage.ErrNotFound
	}
	var verified VerifiedBlock
	if err == nil {
		verified, err = verifyDownloadedBlock(*downloaded)
	}
	var prepared PreparedBlock
	if err == nil {
		prepared, err = prepareVerifiedBlockForApply(verified)
	}
	if err != nil {
		s.preparedShardBlocks.abortPrepare(block)
		if ctx.Err() == nil {
			s.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(block)).
				Msg("failed to prepare shard block ahead of apply")
		}
		return
	}
	s.preparedShardBlocks.storePrepared(prepared)
}
