package storage

import (
	"bytes"
	"container/list"
	"context"
	"sync"

	"github.com/xssnick/tonutils-go/ton"
)

const DefaultLiveBlockCacheMaxBlocks = 8192

// LiveBlockCache keeps recently published block artifacts in memory. Reads are
// lock-free: entries are immutable once stored, publishers replace them
// wholesale under publishMu, and eviction runs in publish order (blocks age out
// naturally, so FIFO matches the access pattern without touching on reads).
type LiveBlockCache struct {
	max int

	blocks     sync.Map // BlockRootHash -> *LiveBlockCacheBlock
	nextBlocks sync.Map // BlockRootHash of prev -> liveBlockCacheNext

	publishMu  sync.Mutex
	count      int
	order      *list.List // publish-order queue of BlockRootHash for eviction
	orderIndex map[BlockRootHash]*list.Element
	prevKeys   map[BlockRootHash][]BlockRootHash // BlockRootHash of next -> prev keys pointing at it
}

type LiveBlockCacheBlock struct {
	id              ton.BlockIDExt
	data            []byte
	meta            *BlockMeta
	proofs          map[ServedProofKind][]byte
	artifactFlushed bool
}

type liveBlockCacheNext struct {
	prev ton.BlockIDExt
	next ton.BlockIDExt
}

type LiveBlockCacheArtifacts struct {
	Block           ton.BlockIDExt
	BlockData       []byte
	Meta            *BlockMeta
	Proofs          []LiveBlockProofArtifact
	ArtifactFlushed bool
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
		order:      list.New(),
		orderIndex: map[BlockRootHash]*list.Element{},
		prevKeys:   map[BlockRootHash][]BlockRootHash{},
	}
}

func (c *LiveBlockCache) PublishLiveBlockArtifacts(artifacts LiveBlockCacheArtifacts) error {
	if err := ValidateBlockIDHashes(artifacts.Block); err != nil {
		return err
	}

	key := BlockKey(artifacts.Block)
	incomingMeta := artifacts.Meta.Clone()
	if incomingMeta != nil {
		incomingMeta.ID = artifacts.Block
	}
	incomingProofs := liveBlockCacheProofs(artifacts.Proofs)

	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	block := &LiveBlockCacheBlock{id: artifacts.Block}
	isNew := true
	if loaded, ok := c.blocks.Load(key); ok {
		*block = *loaded.(*LiveBlockCacheBlock)
		isNew = false
	}
	block.artifactFlushed = block.artifactFlushed || artifacts.ArtifactFlushed
	if len(artifacts.BlockData) > 0 {
		block.data = artifacts.BlockData
	}
	if incomingMeta != nil {
		block.meta = MergeBlockMeta(block.meta, incomingMeta)
		block.meta.ID = artifacts.Block
	}
	if len(incomingProofs) > 0 {
		proofs := make(map[ServedProofKind][]byte, len(block.proofs)+len(incomingProofs))
		for kind, data := range block.proofs {
			proofs[kind] = data
		}
		for kind, data := range incomingProofs {
			proofs[kind] = data
		}
		block.proofs = proofs
	}

	c.blocks.Store(key, block)
	if isNew {
		c.count++
		c.orderIndex[key] = c.order.PushBack(key)
	}
	if block.meta != nil {
		for _, prev := range block.meta.PrevRefs {
			c.setNextBlockLocked(prev, artifacts.Block)
		}
	}

	c.evictLocked()
	return nil
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
	cached, kind, err := c.cachedBlockFull(block)
	if err != nil {
		return nil, err
	}
	return &ServedBlockFull{
		ID:     cached.id,
		Block:  cached.data,
		Proof:  cached.proofs[kind],
		Meta:   cached.meta.Clone(),
		IsLink: kind == ServedProofBlockLink || kind == ServedProofKeyBlockLink,
	}, nil
}

func (c *LiveBlockCache) NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*ServedBlockFull, error) {
	next, err := c.nextBlock(prev)
	if err != nil {
		return nil, err
	}
	return c.BlockFull(ctx, next)
}

func (c *LiveBlockCache) BlockMeta(_ context.Context, block ton.BlockIDExt) (*BlockMeta, error) {
	cached, blockErr := c.cachedBlock(block)
	next, nextErr := c.nextBlock(block)
	if blockErr != nil && nextErr != nil {
		return nil, ErrNotFound
	}

	meta := &BlockMeta{ID: block}
	if cached != nil {
		meta.ID = cached.id
		meta = MergeBlockMeta(meta, cached.meta)

		if len(cached.data) > 0 {
			meta.Mark(BlockMetaHasBlockData)
		}
		for kind, proof := range cached.proofs {
			if len(proof) > 0 {
				meta.Mark(BlockMetaFlagForProof(kind))
			}
		}

		if len(cached.data) > 0 {
			if kind, err := liveBlockCacheProofKind(cached); err == nil {
				meta.Mark(BlockMetaHasServedFull)
				if kind == ServedProofBlockLink || kind == ServedProofKeyBlockLink {
					meta.Mark(BlockMetaServedFullIsLink)
				}
			}
		}
	}
	if nextErr == nil {
		meta.NextRefs = []ton.BlockIDExt{next}
	}

	return meta, nil
}

func (c *LiveBlockCache) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	cached, err := c.CachedBlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	return cached.Data, nil
}

func (c *LiveBlockCache) CachedBlockData(_ context.Context, block ton.BlockIDExt) (CachedBlockData, error) {
	cached, err := c.cachedBlock(block)
	if err != nil || len(cached.data) == 0 {
		return CachedBlockData{}, ErrNotFound
	}
	return CachedBlockData{
		Data:            cached.data,
		ArtifactFlushed: cached.artifactFlushed,
	}, nil
}

func (c *LiveBlockCache) HasBlockData(block ton.BlockIDExt) bool {
	cached, err := c.cachedBlock(block)
	return err == nil && len(cached.data) > 0
}

func (c *LiveBlockCache) BlockProof(_ context.Context, kind ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	cached, err := c.cachedBlock(block)
	if err != nil {
		return nil, err
	}
	proof := cached.proofs[kind]
	if len(proof) == 0 {
		return nil, ErrNotFound
	}
	return proof, nil
}

func (c *LiveBlockCache) MarkBlockFlushed(block ton.BlockIDExt) {
	if !BlockIDHashesKnown(block) {
		return
	}

	key := BlockKey(block)
	c.publishMu.Lock()
	c.deleteBlockLocked(key)
	c.publishMu.Unlock()
}

func (c *LiveBlockCache) cachedBlock(block ton.BlockIDExt) (*LiveBlockCacheBlock, error) {
	loaded, ok := c.blocks.Load(BlockKey(block))
	if !ok {
		return nil, ErrNotFound
	}
	cached := loaded.(*LiveBlockCacheBlock)
	if !blockIDMatchesRootKey(cached.id, block) {
		return nil, ErrNotFound
	}
	return cached, nil
}

func (c *LiveBlockCache) cachedBlockFull(block ton.BlockIDExt) (*LiveBlockCacheBlock, ServedProofKind, error) {
	cached, err := c.cachedBlock(block)
	if err != nil || len(cached.data) == 0 {
		return nil, "", ErrNotFound
	}

	kind, err := liveBlockCacheProofKind(cached)
	if err != nil {
		return nil, "", err
	}
	return cached, kind, nil
}

func liveBlockCacheProofKind(block *LiveBlockCacheBlock) (ServedProofKind, error) {
	for _, kind := range ProofCandidates(block.meta) {
		if len(block.proofs[kind]) > 0 {
			return kind, nil
		}
	}

	for _, kind := range []ServedProofKind{
		ServedProofBlock,
		ServedProofKeyBlock,
		ServedProofBlockLink,
		ServedProofKeyBlockLink,
	} {
		if len(block.proofs[kind]) > 0 {
			return kind, nil
		}
	}
	return "", ErrNotFound
}

func (c *LiveBlockCache) nextBlock(prev ton.BlockIDExt) (ton.BlockIDExt, error) {
	loaded, ok := c.nextBlocks.Load(BlockKey(prev))
	if !ok {
		return ton.BlockIDExt{}, ErrNotFound
	}
	cached := loaded.(liveBlockCacheNext)
	if !blockIDMatchesRootKey(cached.prev, prev) {
		return ton.BlockIDExt{}, ErrNotFound
	}
	return cached.next, nil
}

func (c *LiveBlockCache) setNextBlockLocked(prev ton.BlockIDExt, next ton.BlockIDExt) {
	key := BlockKey(prev)
	selected := next
	if loaded, ok := c.nextBlocks.Load(key); ok {
		cached := loaded.(liveBlockCacheNext)
		if !blockIDMatchesRootKey(cached.prev, prev) {
			return
		}
		current := cached.next
		if current.Equals(&next) {
			return
		}

		chosen, ok := selectLiveBlockCacheSplitNext(prev, current, next)
		if !ok || chosen.Equals(&current) {
			return
		}
		selected = chosen
	}

	c.nextBlocks.Store(key, liveBlockCacheNext{prev: prev, next: selected})
	nextKey := BlockKey(selected)
	c.prevKeys[nextKey] = append(c.prevKeys[nextKey], key)
}

func blockIDMatchesRootKey(cached, requested ton.BlockIDExt) bool {
	return cached.Workchain == requested.Workchain &&
		cached.Shard == requested.Shard &&
		cached.SeqNo == requested.SeqNo &&
		len(cached.RootHash) == 32 && len(requested.RootHash) == 32 &&
		bytes.Equal(cached.FileHash, requested.FileHash)
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

func (c *LiveBlockCache) evictLocked() {
	for c.count > c.max {
		evicted := false
		for elem := c.order.Front(); elem != nil; {
			next := elem.Next()
			key := elem.Value.(BlockRootHash)

			var block *LiveBlockCacheBlock
			if loaded, ok := c.blocks.Load(key); ok {
				block = loaded.(*LiveBlockCacheBlock)
			}
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
	if _, ok := c.blocks.LoadAndDelete(key); ok {
		c.count--
	}
	if elem := c.orderIndex[key]; elem != nil {
		c.order.Remove(elem)
		delete(c.orderIndex, key)
	}

	for _, prev := range c.prevKeys[key] {
		if loaded, ok := c.nextBlocks.Load(prev); ok && BlockKey(loaded.(liveBlockCacheNext).next) == key {
			c.nextBlocks.Delete(prev)
		}
	}
	delete(c.prevKeys, key)
}
