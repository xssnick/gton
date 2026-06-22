package storage

import (
	"container/list"
	"context"
	"sync"

	"github.com/xssnick/tonutils-go/ton"
)

const DefaultLiveBlockCacheMaxBlocks = 8192

type LiveBlockCache struct {
	mu         sync.Mutex
	max        int
	blocks     map[BlockRootHash]*LiveBlockCacheBlock
	nextBlocks map[BlockRootHash]ton.BlockIDExt
	order      *list.List
	orderIndex map[BlockRootHash]*list.Element
}

type LiveBlockCacheBlock struct {
	id              ton.BlockIDExt
	data            []byte
	meta            *BlockMeta
	proofs          map[ServedProofKind][]byte
	artifactFlushed bool
}

type CachedBlockData struct {
	Data            []byte
	ArtifactFlushed bool
}

func NewLiveBlockCache(max int) *LiveBlockCache {
	if max <= 0 {
		max = DefaultLiveBlockCacheMaxBlocks
	}
	return &LiveBlockCache{
		max:        max,
		blocks:     map[BlockRootHash]*LiveBlockCacheBlock{},
		nextBlocks: map[BlockRootHash]ton.BlockIDExt{},
		order:      list.New(),
		orderIndex: map[BlockRootHash]*list.Element{},
	}
}

func (c *LiveBlockCache) PublishLiveBlockArtifacts(artifacts LiveBlockArtifacts) error {
	if c == nil || !validLiveBlockID(artifacts.Block) {
		return nil
	}

	key := BlockKey(artifacts.Block)
	incoming := LiveBlockCacheBlock{
		id:     artifacts.Block,
		data:   artifacts.BlockData,
		meta:   artifacts.Meta.Clone(),
		proofs: liveBlockCacheProofs(artifacts.Proofs),
	}
	if incoming.meta != nil {
		incoming.meta.ID = artifacts.Block
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	block := c.blocks[key]
	if block == nil {
		block = &LiveBlockCacheBlock{id: artifacts.Block}
		c.blocks[key] = block
	}
	block.artifactFlushed = block.artifactFlushed || artifacts.ArtifactFlushed
	if len(incoming.data) > 0 {
		block.data = incoming.data
	}
	if incoming.meta != nil {
		block.meta = MergeBlockMeta(block.meta, incoming.meta)
		block.meta.ID = artifacts.Block
	}
	if len(incoming.proofs) > 0 {
		if block.proofs == nil {
			block.proofs = map[ServedProofKind][]byte{}
		}
		for kind, data := range incoming.proofs {
			block.proofs[kind] = data
		}
	}
	if block.meta != nil {
		for _, prev := range block.meta.PrevRefs {
			c.setNextBlockLocked(prev, artifacts.Block)
		}
	}

	c.touchLocked(key)
	c.evictLocked()
	return nil
}

func validLiveBlockID(block ton.BlockIDExt) bool {
	return len(block.RootHash) == 32 && len(block.FileHash) == 32
}

func liveBlockCacheProofs(proofs []LiveBlockProofArtifact) map[ServedProofKind][]byte {
	if len(proofs) == 0 {
		return nil
	}

	copied := make(map[ServedProofKind][]byte, len(proofs))
	for _, proof := range proofs {
		if len(proof.Data) == 0 {
			continue
		}
		copied[proof.Kind] = proof.Data
	}
	if len(copied) == 0 {
		return nil
	}
	return copied
}

func (c *LiveBlockCache) BlockFull(_ context.Context, block ton.BlockIDExt) (*ServedBlockFull, error) {
	cached, kind, ok := c.cachedBlockFull(block)
	if !ok {
		return nil, ErrNotFound
	}
	return &ServedBlockFull{
		ID:     cached.id,
		Block:  cached.data,
		Proof:  cached.proofs[kind],
		Meta:   cached.meta,
		IsLink: liveBlockCacheProofIsLink(kind),
	}, nil
}

func (c *LiveBlockCache) NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*ServedBlockFull, error) {
	next, ok := c.nextBlock(prev)
	if !ok {
		return nil, ErrNotFound
	}
	return c.BlockFull(ctx, next)
}

func (c *LiveBlockCache) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	cached, err := c.CachedBlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	return cached.Data, nil
}

func (c *LiveBlockCache) CachedBlockData(_ context.Context, block ton.BlockIDExt) (CachedBlockData, error) {
	if c == nil {
		return CachedBlockData{}, ErrNotFound
	}

	key := BlockKey(block)
	c.mu.Lock()
	cached := c.blocks[key]
	if cached == nil || len(cached.data) == 0 {
		c.mu.Unlock()
		return CachedBlockData{}, ErrNotFound
	}
	data := CachedBlockData{
		Data:            cached.data,
		ArtifactFlushed: cached.artifactFlushed,
	}
	c.touchLocked(key)
	c.mu.Unlock()
	return data, nil
}

func (c *LiveBlockCache) BlockProof(_ context.Context, kind ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	if c == nil {
		return nil, ErrNotFound
	}

	key := BlockKey(block)
	c.mu.Lock()
	cached := c.blocks[key]
	if cached == nil {
		c.mu.Unlock()
		return nil, ErrNotFound
	}
	proof := cached.proofs[kind]
	if len(proof) == 0 {
		c.mu.Unlock()
		return nil, ErrNotFound
	}
	c.touchLocked(key)
	c.mu.Unlock()
	return proof, nil
}

func (c *LiveBlockCache) MarkBlockFlushed(block ton.BlockIDExt) {
	if c == nil || !validLiveBlockID(block) {
		return
	}

	key := BlockKey(block)
	c.mu.Lock()
	c.deleteBlockLocked(key)
	c.mu.Unlock()
}

func (c *LiveBlockCache) cachedBlockFull(block ton.BlockIDExt) (*LiveBlockCacheBlock, ServedProofKind, bool) {
	if c == nil {
		return nil, "", false
	}

	key := BlockKey(block)
	c.mu.Lock()
	cached := c.blocks[key]
	if cached == nil || len(cached.data) == 0 {
		c.mu.Unlock()
		return nil, "", false
	}

	kind, ok := liveBlockCacheProofKind(cached)
	if !ok {
		c.mu.Unlock()
		return nil, "", false
	}

	cloned := &LiveBlockCacheBlock{
		id:              cached.id,
		data:            cached.data,
		meta:            cached.meta.Clone(),
		proofs:          map[ServedProofKind][]byte{kind: cached.proofs[kind]},
		artifactFlushed: cached.artifactFlushed,
	}
	c.touchLocked(key)
	c.mu.Unlock()
	return cloned, kind, true
}

func liveBlockCacheProofKind(block *LiveBlockCacheBlock) (ServedProofKind, bool) {
	for _, kind := range ProofCandidates(block.meta) {
		if len(block.proofs[kind]) > 0 {
			return kind, true
		}
	}

	for _, kind := range []ServedProofKind{
		ServedProofBlock,
		ServedProofKeyBlock,
		ServedProofBlockLink,
		ServedProofKeyBlockLink,
	} {
		if len(block.proofs[kind]) > 0 {
			return kind, true
		}
	}
	return "", false
}

func liveBlockCacheProofIsLink(kind ServedProofKind) bool {
	return kind == ServedProofBlockLink || kind == ServedProofKeyBlockLink
}

func (c *LiveBlockCache) nextBlock(prev ton.BlockIDExt) (ton.BlockIDExt, bool) {
	if c == nil {
		return ton.BlockIDExt{}, false
	}

	c.mu.Lock()
	next, ok := c.nextBlocks[BlockKey(prev)]
	if ok {
		c.touchLocked(BlockKey(next))
	}
	c.mu.Unlock()
	return next, ok
}

func (c *LiveBlockCache) setNextBlockLocked(prev ton.BlockIDExt, next ton.BlockIDExt) {
	key := BlockKey(prev)
	current, ok := c.nextBlocks[key]
	if !ok || current.Equals(&next) {
		c.nextBlocks[key] = next
		return
	}

	if selected, ok := selectLiveBlockCacheSplitNext(prev, current, next); ok {
		c.nextBlocks[key] = selected
	}
}

func selectLiveBlockCacheSplitNext(prev ton.BlockIDExt, current ton.BlockIDExt, next ton.BlockIDExt) (ton.BlockIDExt, bool) {
	if prev.Workchain == -1 || current.Workchain != prev.Workchain || next.Workchain != prev.Workchain {
		return ton.BlockIDExt{}, false
	}
	if prev.SeqNo == ^uint32(0) || current.SeqNo != prev.SeqNo+1 || next.SeqNo != prev.SeqNo+1 {
		return ton.BlockIDExt{}, false
	}
	if current.Shard == next.Shard {
		return ton.BlockIDExt{}, false
	}

	left := liveBlockCacheShardChild(prev.Shard, true)
	if current.Shard == left {
		return current, true
	}
	if next.Shard == left {
		return next, true
	}
	return ton.BlockIDExt{}, false
}

func liveBlockCacheShardChild(shard int64, left bool) int64 {
	value := uint64(shard)
	bit := value & -value
	if bit <= 1 {
		return shard
	}
	childBit := bit >> 1
	prefix := value &^ bit
	if !left {
		prefix |= bit
	}
	return int64(prefix | childBit)
}

func (c *LiveBlockCache) touchLocked(key BlockRootHash) {
	if elem := c.orderIndex[key]; elem != nil {
		c.order.MoveToBack(elem)
		return
	}
	c.orderIndex[key] = c.order.PushBack(key)
}

func (c *LiveBlockCache) evictLocked() {
	for len(c.blocks) > c.max {
		evicted := false
		for elem := c.order.Front(); elem != nil; {
			next := elem.Next()
			key, ok := elem.Value.(BlockRootHash)
			if !ok {
				c.order.Remove(elem)
				evicted = true
				elem = next
				continue
			}

			block := c.blocks[key]
			if liveBlockCacheBlockEvictable(block) {
				c.deleteBlockLocked(key)
				evicted = true
				break
			}

			elem = next
		}
		if !evicted {
			return
		}
	}
}

func liveBlockCacheBlockEvictable(block *LiveBlockCacheBlock) bool {
	if block == nil || block.artifactFlushed {
		return true
	}
	return len(block.data) == 0 && len(block.proofs) == 0
}

func (c *LiveBlockCache) deleteBlockLocked(key BlockRootHash) {
	delete(c.blocks, key)
	if elem := c.orderIndex[key]; elem != nil {
		c.order.Remove(elem)
		delete(c.orderIndex, key)
	}
	for prev, next := range c.nextBlocks {
		if BlockKey(next) == key {
			delete(c.nextBlocks, prev)
		}
	}
}
