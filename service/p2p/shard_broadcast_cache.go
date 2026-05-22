package p2p

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	shardBroadcastBlockCacheTTL      = 3 * time.Minute
	shardBroadcastBlockCacheMaxBytes = 128 << 20
	shardBroadcastBlockCacheMaxItems = 4096
	shardBroadcastBlockCacheOverhead = 256
)

type shardBroadcastBlockCache struct {
	mu sync.Mutex

	ttl      time.Duration
	maxBytes int64
	maxItems int

	entries map[string]*shardBroadcastBlockCacheEntry
	order   *list.List
	bytes   int64
}

type shardBroadcastBlockCacheEntry struct {
	key       string
	element   *list.Element
	block     ton.BlockIDExt
	kind      string
	blockRoot *cell.Cell
	proofRoot *cell.Cell
	blockBOC  []byte
	proofBOC  []byte
	isLink    bool
	meta      *tnstore.BlockMeta
	expiresAt time.Time
	bytes     int64
}

func newShardBroadcastBlockCache(ttl time.Duration, maxBytes int64, maxItems int) *shardBroadcastBlockCache {
	return &shardBroadcastBlockCache{
		ttl:      ttl,
		maxBytes: maxBytes,
		maxItems: maxItems,
		entries:  map[string]*shardBroadcastBlockCacheEntry{},
		order:    list.New(),
	}
}

func (c *shardBroadcastBlockCache) Store(downloaded DownloadedBlock, meta *tnstore.BlockMeta) error {
	return c.storeAt(downloaded, meta, downloaded.Block, downloaded.Proof, time.Now())
}

func (c *shardBroadcastBlockCache) storeAt(downloaded DownloadedBlock, meta *tnstore.BlockMeta, blockRoot *cell.Cell, proofRoot *cell.Cell, now time.Time) error {
	if c == nil {
		return tnstore.ErrNotFound
	}
	if isMasterchainBlock(downloaded.ID) {
		return fmt.Errorf("masterchain block %s is not a shard broadcast cache candidate", formatBlockRef(downloaded.ID))
	}
	if !downloaded.VerifiedRootHash || !downloaded.VerifiedFileHash {
		return fmt.Errorf("block %s is not hash verified", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.BlockBOC) == 0 {
		return fmt.Errorf("block %s has empty block data", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.ProofBOC) == 0 {
		return fmt.Errorf("block %s has empty proof", formatBlockRef(downloaded.ID))
	}
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return fmt.Errorf("shard broadcast cache is disabled")
	}
	if blockRoot == nil {
		var err error
		blockRoot, err = cell.FromBOC(downloaded.BlockBOC)
		if err != nil {
			return fmt.Errorf("parse block data for %s: %w", formatBlockRef(downloaded.ID), err)
		}
	}
	if proofRoot == nil {
		var err error
		proofRoot, err = cell.FromBOC(downloaded.ProofBOC)
		if err != nil {
			return fmt.Errorf("parse proof for %s: %w", formatBlockRef(downloaded.ID), err)
		}
	}

	size := int64(len(downloaded.BlockBOC) + len(downloaded.ProofBOC) + shardBroadcastBlockCacheOverhead)
	if size > c.maxBytes {
		return fmt.Errorf("block %s is too large for shard broadcast cache: %d > %d", formatBlockRef(downloaded.ID), size, c.maxBytes)
	}

	key := tnstore.BlockKey(downloaded.ID)
	entry := &shardBroadcastBlockCacheEntry{
		key:       key,
		block:     cloneBlockID(downloaded.ID),
		kind:      downloaded.Kind,
		blockRoot: blockRoot,
		proofRoot: proofRoot,
		blockBOC:  append([]byte(nil), downloaded.BlockBOC...),
		proofBOC:  append([]byte(nil), downloaded.ProofBOC...),
		isLink:    downloaded.IsLink,
		meta:      meta.Clone(),
		expiresAt: now.Add(c.ttl),
		bytes:     size,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if old := c.entries[key]; old != nil {
		c.deleteEntryLocked(old)
	}

	entry.element = c.order.PushBack(entry)
	c.entries[key] = entry
	c.bytes += size

	c.pruneExpiredLocked(now)
	c.pruneOverflowLocked()
	return nil
}

func (c *shardBroadcastBlockCache) PopBlock(block ton.BlockIDExt) (*DownloadedBlock, error) {
	return c.popBlockAt(block, time.Now())
}

func (c *shardBroadcastBlockCache) HasBlock(block ton.BlockIDExt) bool {
	return c.hasBlockAt(block, time.Now())
}

func (c *shardBroadcastBlockCache) hasBlockAt(block ton.BlockIDExt, now time.Time) bool {
	if c == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.entries[tnstore.BlockKey(block)]
	if entry == nil {
		return false
	}
	if !entry.expiresAt.After(now) {
		c.deleteEntryLocked(entry)
		return false
	}
	return true
}

func (c *shardBroadcastBlockCache) popBlockAt(block ton.BlockIDExt, now time.Time) (*DownloadedBlock, error) {
	if c == nil {
		return nil, tnstore.ErrNotFound
	}

	c.mu.Lock()
	key := tnstore.BlockKey(block)
	entry := c.entries[key]
	if entry == nil {
		c.mu.Unlock()
		return nil, tnstore.ErrNotFound
	}
	c.deleteEntryLocked(entry)
	c.mu.Unlock()

	if !entry.expiresAt.After(now) {
		return nil, tnstore.ErrNotFound
	}

	kind := entry.kind
	if kind == "" {
		kind = "shard broadcast block cache"
	}
	return &DownloadedBlock{
		ID:               cloneBlockID(entry.block),
		Kind:             kind,
		Block:            entry.blockRoot,
		Proof:            entry.proofRoot,
		BlockBOC:         append([]byte(nil), entry.blockBOC...),
		ProofBOC:         append([]byte(nil), entry.proofBOC...),
		Meta:             entry.meta.Clone(),
		IsLink:           entry.isLink,
		VerifiedRootHash: true,
		VerifiedFileHash: true,
	}, nil
}

func (c *shardBroadcastBlockCache) Prune(now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pruneExpiredLocked(now)
	c.pruneOverflowLocked()
}

func (c *shardBroadcastBlockCache) pruneExpiredLocked(now time.Time) {
	for elem := c.order.Front(); elem != nil; {
		entry := elem.Value.(*shardBroadcastBlockCacheEntry)
		if !entry.expiresAt.After(now) {
			next := elem.Next()
			c.deleteEntryLocked(entry)
			elem = next
			continue
		}
		return
	}
}

func (c *shardBroadcastBlockCache) pruneOverflowLocked() {
	for len(c.entries) > c.maxItems || c.bytes > c.maxBytes {
		elem := c.order.Front()
		if elem == nil {
			return
		}
		c.deleteEntryLocked(elem.Value.(*shardBroadcastBlockCacheEntry))
	}
}

func (c *shardBroadcastBlockCache) deleteEntryLocked(entry *shardBroadcastBlockCacheEntry) {
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

func (n *Node) rememberShardBroadcastBlock(downloaded *DownloadedBlock) {
	if downloaded == nil || n.shardBroadcastCache == nil {
		return
	}
	if isMasterchainBlock(downloaded.ID) {
		return
	}

	validated, err := validateShardBroadcastBlock(downloaded)
	if err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(downloaded.ID)).
			Msg("dropping shard block broadcast from hot cache")
		return
	}

	if err = n.shardBroadcastCache.storeAt(*downloaded, validated.meta, validated.blockRoot, validated.proofRoot, time.Now()); err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(downloaded.ID)).
			Msg("failed to cache shard block broadcast")
		return
	}

	n.log.Debug().
		Str("block", formatBlockRef(downloaded.ID)).
		Msg("cached shard block broadcast")
}

func (n *Node) popShardBroadcastBlock(block ton.BlockIDExt) (*DownloadedBlock, error) {
	if n.shardBroadcastCache == nil || isMasterchainBlock(block) {
		return nil, tnstore.ErrNotFound
	}

	downloaded, err := n.shardBroadcastCache.PopBlock(block)
	if err != nil {
		return nil, err
	}

	n.log.Debug().
		Str("block", formatBlockRef(block)).
		Msg("using cached shard block broadcast")
	return downloaded, nil
}

func (n *Node) runShardBroadcastCacheJanitor(ctx context.Context) {
	if n.shardBroadcastCache == nil {
		return
	}

	interval := shardBroadcastBlockCacheTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n.shardBroadcastCache.Prune(now)
		}
	}
}

type validatedShardBroadcastBlock struct {
	meta      *tnstore.BlockMeta
	blockRoot *cell.Cell
	proofRoot *cell.Cell
}

func validateShardBroadcastBlock(downloaded *DownloadedBlock) (validatedShardBroadcastBlock, error) {
	if downloaded == nil {
		return validatedShardBroadcastBlock{}, fmt.Errorf("downloaded block is nil")
	}
	if isMasterchainBlock(downloaded.ID) {
		return validatedShardBroadcastBlock{}, fmt.Errorf("masterchain block %s is not a shard block", formatBlockRef(downloaded.ID))
	}
	if !downloaded.VerifiedRootHash || !downloaded.VerifiedFileHash {
		return validatedShardBroadcastBlock{}, fmt.Errorf("block %s is not hash verified", formatBlockRef(downloaded.ID))
	}

	proof := downloaded.Proof
	if proof == nil {
		var err error
		proof, err = cell.FromBOC(downloaded.ProofBOC)
		if err != nil {
			return validatedShardBroadcastBlock{}, fmt.Errorf("parse proof for %s: %w", formatBlockRef(downloaded.ID), err)
		}
	}
	if err := blockproof.CheckProofShape(downloaded.ID, proof, downloaded.IsLink); err != nil {
		return validatedShardBroadcastBlock{}, err
	}

	root := downloaded.Block
	if root == nil {
		var err error
		root, err = cell.FromBOC(downloaded.BlockBOC)
		if err != nil {
			return validatedShardBroadcastBlock{}, fmt.Errorf("parse block data for %s: %w", formatBlockRef(downloaded.ID), err)
		}
	}
	block, err := tnstore.ParseVerifiedBlockCell(downloaded.ID, root)
	if err != nil {
		return validatedShardBroadcastBlock{}, err
	}

	meta, err := tnstore.BuildBlockMetaFromParsedBlock(downloaded.ID, block)
	if err != nil {
		return validatedShardBroadcastBlock{}, err
	}
	return validatedShardBroadcastBlock{
		meta:      meta,
		blockRoot: root,
		proofRoot: proof,
	}, nil
}

func cloneBlockID(block ton.BlockIDExt) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo,
		RootHash:  append([]byte(nil), block.RootHash...),
		FileHash:  append([]byte(nil), block.FileHash...),
	}
}
