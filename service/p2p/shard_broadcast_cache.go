package p2p

import (
	"bytes"
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
	shardBroadcastBlockCacheMaxBytes = 256 << 20
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
	key         string
	element     *list.Element
	block       ton.BlockIDExt
	kind        string
	blockRoot   *cell.Cell
	proofRoot   *cell.Cell
	stateUpdate *cell.Cell
	blockBOC    []byte
	proofBOC    []byte
	isLink      bool
	meta        *tnstore.BlockMeta
	sourceKey   string
	expiresAt   time.Time
	bytes       int64
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
	validated, err := validateShardBroadcastBlock(&downloaded)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = validated.meta
	}
	return c.storeAt(downloaded, meta, validated.blockRoot, validated.proofRoot, validated.stateUpdate, time.Now())
}

func (c *shardBroadcastBlockCache) storeAt(downloaded DownloadedBlock, meta *tnstore.BlockMeta, blockRoot *cell.Cell, proofRoot *cell.Cell, stateUpdate *cell.Cell, now time.Time) error {
	if c == nil {
		return tnstore.ErrNotFound
	}
	if isMasterchainBlock(downloaded.ID) {
		return fmt.Errorf("masterchain block %s is not a shard broadcast cache candidate", formatBlockRef(downloaded.ID))
	}
	if !downloaded.VerifiedRootHash {
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
	if meta == nil {
		return fmt.Errorf("block %s has no metadata", formatBlockRef(downloaded.ID))
	}
	if blockRoot == nil {
		return fmt.Errorf("block %s has no parsed block root", formatBlockRef(downloaded.ID))
	}
	if proofRoot == nil {
		return fmt.Errorf("block %s has no parsed proof root", formatBlockRef(downloaded.ID))
	}
	if stateUpdate == nil {
		return fmt.Errorf("block %s has no state update", formatBlockRef(downloaded.ID))
	}

	size := shardBroadcastBlockCacheSize(downloaded.BlockBOC, downloaded.ProofBOC)
	if size > c.maxBytes {
		return fmt.Errorf("block %s is too large for shard broadcast cache: %d > %d", formatBlockRef(downloaded.ID), size, c.maxBytes)
	}

	key := tnstore.BlockKey(downloaded.ID)
	entry := &shardBroadcastBlockCacheEntry{
		key:         key,
		block:       cloneBlockID(downloaded.ID),
		kind:        downloaded.Kind,
		blockRoot:   blockRoot,
		proofRoot:   proofRoot,
		stateUpdate: stateUpdate,
		blockBOC:    append([]byte(nil), downloaded.BlockBOC...),
		proofBOC:    append([]byte(nil), downloaded.ProofBOC...),
		isLink:      downloaded.IsLink,
		meta:        meta.Clone(),
		sourceKey:   downloaded.SourceKey,
		expiresAt:   now.Add(c.ttl),
		bytes:       size,
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
		BlockBOC:         entry.blockBOC,
		ProofBOC:         entry.proofBOC,
		Meta:             entry.meta.Clone(),
		StateUpdate:      entry.stateUpdate,
		SourceKey:        entry.sourceKey,
		IsLink:           entry.isLink,
		VerifiedRootHash: true,
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

func shardBroadcastBlockCacheSize(blockBOC []byte, proofBOC []byte) int64 {
	return int64(len(blockBOC)*2 + len(proofBOC)*2 + shardBroadcastBlockCacheOverhead)
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

	if err = n.shardBroadcastCache.storeAt(*downloaded, validated.meta, validated.blockRoot, validated.proofRoot, validated.stateUpdate, time.Now()); err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(downloaded.ID)).
			Msg("failed to cache shard block broadcast")
		return
	}

	n.log.Debug().
		Str("block", formatBlockRef(downloaded.ID)).
		Msg("cached shard block broadcast")
	n.notifyShardBroadcastBlock(downloaded.ID)
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

func (n *Node) watchShardBroadcastBlock(block ton.BlockIDExt) (<-chan struct{}, func()) {
	if n == nil || n.shardBroadcastCache == nil || isMasterchainBlock(block) {
		return nil, func() {}
	}

	key := tnstore.BlockKey(block)
	ch := make(chan struct{})

	n.shardBroadcastWaitMx.Lock()
	if n.shardBroadcastWaiters == nil {
		n.shardBroadcastWaiters = map[string][]chan struct{}{}
	}
	n.shardBroadcastWaiters[key] = append(n.shardBroadcastWaiters[key], ch)
	n.shardBroadcastWaitMx.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			n.shardBroadcastWaitMx.Lock()
			waiters := n.shardBroadcastWaiters[key]
			for i, waiter := range waiters {
				if waiter == ch {
					waiters = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
			if len(waiters) == 0 {
				delete(n.shardBroadcastWaiters, key)
			} else {
				n.shardBroadcastWaiters[key] = waiters
			}
			n.shardBroadcastWaitMx.Unlock()
		})
	}
	return ch, cancel
}

func (n *Node) notifyShardBroadcastBlock(block ton.BlockIDExt) {
	if n == nil {
		return
	}

	key := tnstore.BlockKey(block)
	n.shardBroadcastWaitMx.Lock()
	waiters := n.shardBroadcastWaiters[key]
	delete(n.shardBroadcastWaiters, key)
	n.shardBroadcastWaitMx.Unlock()

	for _, waiter := range waiters {
		close(waiter)
	}
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

type validatedShardBroadcastRoots struct {
	blockRoot *cell.Cell
	proofRoot *cell.Cell
}

type validatedShardBroadcastBlock struct {
	meta        *tnstore.BlockMeta
	blockRoot   *cell.Cell
	proofRoot   *cell.Cell
	stateUpdate *cell.Cell
}

func validateShardBroadcastRoots(downloaded *DownloadedBlock) (validatedShardBroadcastRoots, error) {
	if isMasterchainBlock(downloaded.ID) {
		return validatedShardBroadcastRoots{}, fmt.Errorf("masterchain block %s is not a shard block", formatBlockRef(downloaded.ID))
	}
	if !downloaded.VerifiedRootHash {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s is not hash verified", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.BlockBOC) == 0 {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has empty block data", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.ProofBOC) == 0 {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has empty proof", formatBlockRef(downloaded.ID))
	}

	proof := downloaded.Proof
	if proof == nil {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has no parsed proof root", formatBlockRef(downloaded.ID))
	}
	if err := blockproof.CheckProofShape(downloaded.ID, proof, downloaded.IsLink); err != nil {
		return validatedShardBroadcastRoots{}, err
	}

	root := downloaded.Block
	if root == nil {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has no parsed block root", formatBlockRef(downloaded.ID))
	}
	root, err := effectiveDownloadedBlockRoot(downloaded.ID, downloaded.IsLink, root)
	if err != nil {
		return validatedShardBroadcastRoots{}, err
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], downloaded.ID.RootHash) {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s root hash mismatch", formatBlockRef(downloaded.ID))
	}

	return validatedShardBroadcastRoots{
		blockRoot: root,
		proofRoot: proof,
	}, nil
}

func validateShardBroadcastBlock(downloaded *DownloadedBlock) (validatedShardBroadcastBlock, error) {
	roots, err := validateShardBroadcastRoots(downloaded)
	if err != nil {
		return validatedShardBroadcastBlock{}, err
	}

	if downloaded.Meta == nil {
		return validatedShardBroadcastBlock{}, fmt.Errorf("block %s has no metadata", formatBlockRef(downloaded.ID))
	}
	if downloaded.StateUpdate == nil {
		return validatedShardBroadcastBlock{}, fmt.Errorf("block %s has no state update", formatBlockRef(downloaded.ID))
	}
	return validatedShardBroadcastBlock{
		meta:        downloaded.Meta.Clone(),
		blockRoot:   roots.blockRoot,
		proofRoot:   roots.proofRoot,
		stateUpdate: downloaded.StateUpdate,
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
