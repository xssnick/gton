package liveview

import (
	"bytes"
	"errors"
	"sort"

	"github.com/xssnick/gton/service/blockproof"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Store) cachedBlockState(block ton.BlockIDExt) (*storage.BlockState, error) {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return nil, storage.ErrNotFound
	}

	s.mu.RLock()
	state, ok := s.states[key]
	s.mu.RUnlock()
	if !ok {
		return nil, storage.ErrNotFound
	}
	return &state, nil
}

// cachedBlockMeta returns the shared cached meta; indexed metas are immutable
// once published, callers treat them as read-only.
func (s *Store) cachedBlockMeta(block ton.BlockIDExt) (*storage.BlockMeta, error) {
	key := storage.BlockKey(block)

	s.mu.RLock()
	meta := s.metas[key]
	s.mu.RUnlock()
	if meta != nil && !blockIDEqual(meta.ID, block) {
		return nil, storage.ErrNotFound
	}
	if meta == nil {
		return nil, storage.ErrNotFound
	}
	return meta, nil
}

func (s *Store) cachedBlockBySeqNo(ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	s.mu.RLock()
	block, ok := s.seqIndex[liveSeqKey{workchain: ref.Workchain, shard: ref.Shard, seqno: ref.SeqNo}]
	s.mu.RUnlock()
	if !ok {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return cloneBlockID(block), nil
}

func (s *Store) cachedBlockBySeqNoForPrefix(ref storage.BlockSeqRef) (blockPrefixCandidate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	maxDepth := uint32(60)
	if ref.Workchain == masterchainID {
		maxDepth = 0
	}
	for depth := uint32(0); depth <= maxDepth; depth++ {
		shard, err := sharddomain.FromAccountPrefix(uint64(ref.Shard), depth)
		if err != nil {
			return blockPrefixCandidate{}, err
		}
		block, ok := s.seqIndex[liveSeqKey{workchain: ref.Workchain, shard: shard, seqno: ref.SeqNo}]
		if ok {
			return s.cachedPrefixCandidateLocked(block, true), nil
		}
	}
	return blockPrefixCandidate{}, storage.ErrNotFound
}

func liveLTIndexCovers(entries []liveLTIndexEntry, idx int, lt uint64) bool {
	entry := entries[idx]
	if entry.startLT != 0 && lt >= entry.startLT {
		return true
	}
	return idx > 0 && entries[idx-1].endLT <= lt
}

func (s *Store) cachedPrefixCandidateLocked(block ton.BlockIDExt, exact bool) blockPrefixCandidate {
	candidate := blockPrefixCandidate{block: cloneBlockID(block), exact: exact}
	if cached := s.blocks[storage.BlockKey(block)]; cached != nil && blockIDEqual(cached.id, block) {
		candidate.artifactFlushed = cached.artifactFlushed
	}
	return candidate
}

func (s *Store) cachedBlockRoot(block ton.BlockIDExt) (*cell.Cell, error) {
	key := storage.BlockKey(block)

	s.mu.RLock()
	cached := s.blocks[key]
	s.mu.RUnlock()
	if cached == nil || !blockIDEqual(cached.id, block) || cached.root == nil {
		return nil, storage.ErrNotFound
	}
	return cached.root, nil
}

func (s *Store) cachedBlockFragments(block ton.BlockIDExt) (*BlockView, error) {
	key := storage.BlockKey(block)

	// Read the field itself under the lock: fragments is the one field of a
	// published liveBlock that is written in place (by rememberBlockFragments),
	// and with background builds that write races every live query.
	s.mu.RLock()
	cached := s.blocks[key]
	var fragments *BlockView
	if cached != nil && blockIDEqual(cached.id, block) {
		fragments = cached.fragments
	}
	s.mu.RUnlock()
	if fragments == nil {
		return nil, storage.ErrNotFound
	}
	return fragments, nil
}

func (s *Store) rememberBlockFragments(block ton.BlockIDExt, fragments *BlockView) *BlockView {
	key := storage.BlockKey(block)

	s.mu.Lock()
	cached := s.blocks[key]
	if cached == nil || !blockIDEqual(cached.id, block) {
		s.mu.Unlock()
		return fragments
	}
	released := false
	if cached.fragments != nil {
		fragments = cached.fragments
	} else {
		cached.fragments = fragments
		released = cached.currentCachesReleased
	}
	s.mu.Unlock()

	// The block left the current state while this view was being built, so the
	// per-current caches it would fill are never going to be read.
	if released {
		fragments.releaseCurrentCaches()
	}
	return fragments
}

func (s *Store) putBlockLocked(key storage.BlockRootHash, block *liveBlock, state *storage.BlockState) {
	var stateKey liveBlockLookupKey
	if state != nil {
		var ok bool
		stateKey, ok = liveBlockLookupKeyFromBlock(state.Block)
		if !ok || !blockIDEqual(state.Block, block.id) {
			panic("liveview: block state does not match block")
		}
	}

	if existing := s.blocks[key]; existing != nil {
		existingEvictable := s.liveBlockEvictableLocked(existing)
		if block.root == nil {
			block.root = existing.root
		}
		if existing.meta != nil || block.meta != nil {
			block.meta = storage.MergeBlockMeta(existing.meta, block.meta)
		}
		block.artifactFlushed = block.artifactFlushed || existing.artifactFlushed
		block.stateFlushed = block.stateFlushed || existing.stateFlushed
		block.currentCachesReleased = block.currentCachesReleased || existing.currentCachesReleased
		if block.fragments == nil {
			block.fragments = existing.fragments
		}
		existingKind := liveBlockKind(existing.id)
		s.removeBlockOrderLocked(key, existingKind)
		if existingEvictable {
			s.adjustLiveEvictableLocked(existingKind, -1)
		}
	}
	// Publish block and state as one mutation. Refreshing after the block and
	// again after its state makes the second refresh linearly remove the index
	// entries that the first refresh just added.
	if state != nil {
		s.states[stateKey] = *storage.CloneBlockState(state)
	}
	s.blocks[key] = block
	s.refreshBlockIndexLocked(block.id)
	kind := liveBlockKind(block.id)
	s.blockOrderLocked(kind).pushBack(key)
	if s.liveBlockEvictableLocked(block) {
		s.adjustLiveEvictableLocked(kind, 1)
	}
}

func (s *Store) trimBlocksLocked(kind liveBlockCacheKind) {
	limit := s.shardCacheSize
	if kind == liveBlockMaster {
		limit = s.masterCacheSize
	}
	if limit < 0 {
		return
	}

	evictable := s.liveEvictableCountLocked(kind)
	if evictable <= limit {
		return
	}

	protected := s.protectedLiveBlocksLocked()
	order := s.blockOrderLocked(kind)
	for elem := order.items.Front(); elem != nil && evictable > limit; {
		next := elem.Next()
		key := elem.Value.(storage.BlockRootHash)
		cached := s.blocks[key]
		if cached == nil {
			order.remove(key)
			elem = next
			continue
		}

		if s.liveBlockEvictableLocked(cached) {
			if _, ok := protected[key]; !ok {
				s.deleteLiveBlockLocked(key, cached, kind)
				evictable--
			}
		}

		elem = next
	}
}

func (s *Store) markLiveBlockStateFlushedLocked(block ton.BlockIDExt, rememberMissing bool) {
	key := storage.BlockKey(block)
	if cached := s.blocks[key]; cached != nil {
		evictableBefore := s.liveBlockEvictableLocked(cached)
		cached.stateFlushed = true
		s.adjustLiveEvictableStateLocked(cached, evictableBefore)
		return
	}
	if !rememberMissing {
		return
	}

	flushed := s.flushed[key]
	flushed.state = true
	s.flushed[key] = flushed
}

func (s *Store) adjustLiveEvictableStateLocked(cached *liveBlock, evictableBefore bool) {
	evictableAfter := s.liveBlockEvictableLocked(cached)
	if evictableBefore == evictableAfter {
		return
	}
	if evictableAfter {
		s.adjustLiveEvictableLocked(liveBlockKind(cached.id), 1)
		return
	}
	s.adjustLiveEvictableLocked(liveBlockKind(cached.id), -1)
}

func (s *Store) liveBlockEvictableLocked(cached *liveBlock) bool {
	if cached == nil {
		return false
	}
	if (cached.root != nil || cached.meta != nil) && !cached.artifactFlushed {
		return false
	}
	if s.liveBlockHasStateLocked(cached.id) && !cached.stateFlushed {
		return false
	}
	return true
}

func (s *Store) liveBlockHasStateLocked(block ton.BlockIDExt) bool {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return false
	}
	_, ok = s.states[key]
	return ok
}

func (s *Store) rememberBlockStateLocked(state storage.BlockState) {
	if !blockproof.IsFullBlockID(state.Block) {
		return
	}
	if key, ok := liveBlockLookupKeyFromBlock(state.Block); ok {
		cached := s.blocks[storage.BlockKey(state.Block)]
		evictableBefore := s.liveBlockEvictableLocked(cached)
		s.states[key] = *storage.CloneBlockState(&state)
		if cached != nil {
			s.adjustLiveEvictableStateLocked(cached, evictableBefore)
		}
		s.refreshBlockIndexLocked(state.Block)
	}
}

func (s *Store) removeBlockStateLocked(block ton.BlockIDExt) {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if ok {
		delete(s.states, key)
	}
	s.refreshBlockIndexLocked(block)
}

func (s *Store) rememberCurrentBlockStatesLocked(current *storage.CurrentState) {
	if current == nil {
		return
	}

	s.rememberBlockStateLocked(current.Masterchain)
	for _, shard := range current.Shards {
		s.rememberBlockStateLocked(shard)
	}
}

func (s *Store) releaseRetiredCurrentCachesLocked(previous, next *storage.CurrentState) {
	if previous == nil {
		return
	}

	release := func(block ton.BlockIDExt) {
		if currentHasBlockState(next, block) {
			return
		}
		cached := s.blocks[storage.BlockKey(block)]
		if cached == nil {
			return
		}
		// Marked even when the view is missing: a build still in flight installs
		// its view after this point and must not re-enable the caches.
		cached.currentCachesReleased = true
		if cached.fragments != nil {
			cached.fragments.releaseCurrentCaches()
		}
	}

	release(previous.Masterchain.Block)
	for _, shard := range previous.Shards {
		release(shard.Block)
	}
}

func (s *Store) refreshBlockIndexLocked(block ton.BlockIDExt) {
	key := storage.BlockKey(block)
	if old := s.metas[key]; old != nil {
		s.removeMetaHistoryIndexLocked(old)
	}
	delete(s.metas, key)
	delete(s.seqIndex, liveSeqKey{workchain: block.Workchain, shard: block.Shard, seqno: block.SeqNo})

	var meta *storage.BlockMeta
	indexed := false
	if state, ok := liveBlockLookupKeyFromBlock(block); ok {
		if cached, ok := s.states[state]; ok {
			stateMeta, err := storage.BuildBlockMetaFromState(cached)
			if err == nil {
				meta = storage.MergeBlockMeta(meta, stateMeta)
				indexed = true
			}
		}
	}
	if cached := s.blocks[key]; cached != nil {
		if cached.meta != nil {
			meta = storage.MergeBlockMeta(meta, cached.meta)
			indexed = true
		} else if cached.root != nil {
			indexed = true
		}
	}
	if indexed {
		s.seqIndex[liveSeqKey{workchain: block.Workchain, shard: block.Shard, seqno: block.SeqNo}] = block
	}
	if meta == nil {
		return
	}

	s.metas[key] = meta
	s.addMetaHistoryIndexLocked(meta)
}

func (s *Store) addMetaHistoryIndexLocked(meta *storage.BlockMeta) {
	key := liveHistoryKey{workchain: meta.ID.Workchain, shard: meta.ID.Shard}
	if meta.EndLT != 0 {
		entry := liveLTIndexEntry{
			startLT: meta.StartLT,
			endLT:   meta.EndLT,
			seqno:   meta.ID.SeqNo,
			block:   cloneBlockID(meta.ID),
		}
		entries := s.ltIndex[key]
		if len(entries) == 0 || entries[len(entries)-1].endLT < entry.endLT || entries[len(entries)-1].endLT == entry.endLT && entries[len(entries)-1].seqno < entry.seqno {
			s.ltIndex[key] = append(entries, entry)
		} else {
			idx := sort.Search(len(entries), func(i int) bool {
				return entries[i].endLT > entry.endLT || entries[i].endLT == entry.endLT && entries[i].seqno >= entry.seqno
			})
			entries = append(entries, liveLTIndexEntry{})
			copy(entries[idx+1:], entries[idx:])
			entries[idx] = entry
			s.ltIndex[key] = entries
		}
	}

	if meta.GenUTime != 0 {
		entry := liveUnixIndexEntry{
			genUTime: meta.GenUTime,
			seqno:    meta.ID.SeqNo,
			block:    cloneBlockID(meta.ID),
		}
		entries := s.unixIndex[key]
		if len(entries) == 0 || entries[len(entries)-1].genUTime < entry.genUTime || entries[len(entries)-1].genUTime == entry.genUTime && entries[len(entries)-1].seqno < entry.seqno {
			s.unixIndex[key] = append(entries, entry)
		} else {
			idx := sort.Search(len(entries), func(i int) bool {
				return entries[i].genUTime > entry.genUTime || entries[i].genUTime == entry.genUTime && entries[i].seqno >= entry.seqno
			})
			entries = append(entries, liveUnixIndexEntry{})
			copy(entries[idx+1:], entries[idx:])
			entries[idx] = entry
			s.unixIndex[key] = entries
		}
	}
}

func (s *Store) removeMetaHistoryIndexLocked(meta *storage.BlockMeta) {
	key := liveHistoryKey{workchain: meta.ID.Workchain, shard: meta.ID.Shard}
	if meta.EndLT != 0 {
		entries := s.ltIndex[key]
		for i, entry := range entries {
			if blockIDEqual(entry.block, meta.ID) {
				entries = append(entries[:i], entries[i+1:]...)
				if len(entries) == 0 {
					delete(s.ltIndex, key)
				} else {
					s.ltIndex[key] = entries
				}
				break
			}
		}
	}

	if meta.GenUTime != 0 {
		entries := s.unixIndex[key]
		for i, entry := range entries {
			if blockIDEqual(entry.block, meta.ID) {
				entries = append(entries[:i], entries[i+1:]...)
				if len(entries) == 0 {
					delete(s.unixIndex, key)
				} else {
					s.unixIndex[key] = entries
				}
				break
			}
		}
	}
}

func (s *Store) removeBlockOrderLocked(key storage.BlockRootHash, kind liveBlockCacheKind) {
	s.blockOrderLocked(kind).remove(key)
}

func (s *Store) blockOrderLocked(kind liveBlockCacheKind) *liveBlockOrder {
	order := &s.shardOrder
	if kind == liveBlockMaster {
		order = &s.masterOrder
	}
	return order
}

func (s *Store) liveEvictableCountLocked(kind liveBlockCacheKind) int {
	if kind == liveBlockMaster {
		return s.masterEvictable
	}
	return s.shardEvictable
}

func (s *Store) adjustLiveEvictableLocked(kind liveBlockCacheKind, delta int) {
	if kind == liveBlockMaster {
		s.masterEvictable += delta
		if s.masterEvictable < 0 {
			s.masterEvictable = 0
		}
		return
	}

	s.shardEvictable += delta
	if s.shardEvictable < 0 {
		s.shardEvictable = 0
	}
}

func (s *Store) deleteLiveBlockLocked(key storage.BlockRootHash, cached *liveBlock, kind liveBlockCacheKind) {
	delete(s.blocks, key)
	s.removeBlockOrderLocked(key, kind)
	if s.liveBlockEvictableLocked(cached) {
		s.adjustLiveEvictableLocked(kind, -1)
	}
	s.removeBlockStateLocked(cached.id)
}

func (s *Store) protectedLiveBlocksLocked() map[storage.BlockRootHash]struct{} {
	protected := make(map[storage.BlockRootHash]struct{})
	addCurrentLiveBlocks(protected, s.current)
	addCurrentLiveBlocks(protected, s.pendingCurrent)
	if s.nonFinalEnabled {
		for _, pending := range s.nonFinalPending {
			protected[storage.BlockKey(pending.block)] = struct{}{}
		}
	}
	return protected
}

func addCurrentLiveBlocks(protected map[storage.BlockRootHash]struct{}, current *storage.CurrentState) {
	if current == nil {
		return
	}
	protected[storage.BlockKey(current.Masterchain.Block)] = struct{}{}
	for _, shard := range current.Shards {
		protected[storage.BlockKey(shard.Block)] = struct{}{}
	}
}

type liveBlockCacheKind uint8

const (
	liveBlockMaster liveBlockCacheKind = iota
	liveBlockShard
)

func liveBlockKind(block ton.BlockIDExt) liveBlockCacheKind {
	if block.Workchain == masterchainID && block.Shard == masterchainShard {
		return liveBlockMaster
	}
	return liveBlockShard
}

func (s *Store) backingBlockAllowed(block ton.BlockIDExt) bool {
	if !blockproof.IsFullBlockID(block) {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backingBlockAllowedLocked(block)
}

func (s *Store) backingBlockAllowedLocked(block ton.BlockIDExt) bool {
	return s.backingSeqnoLookupAllowedLocked(storage.BlockSeqRefFromBlock(block))
}

func (s *Store) backingSeqnoLookupAllowed(ref storage.BlockSeqRef) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backingSeqnoLookupAllowedLocked(ref)
}

func (s *Store) backingSeqnoLookupAllowedLocked(ref storage.BlockSeqRef) bool {
	if s.current == nil && s.pendingCurrent == nil {
		return true
	}

	if ref.Workchain == masterchainID && ref.Shard == masterchainShard {
		return ref.SeqNo <= maxKnownMasterSeqno(s.current, s.pendingCurrent)
	}

	maxSeqno, err := maxKnownShardSeqno(ref.HistoryKey(), s.current, s.pendingCurrent)
	if errors.Is(err, storage.ErrNotFound) {
		return true
	}
	return ref.SeqNo <= maxSeqno
}

func (s *Store) currentRefersToBlockLocked(block ton.BlockIDExt) bool {
	return currentRefersToBlock(s.current, block) || currentRefersToBlock(s.pendingCurrent, block)
}

func currentRefersToBlock(current *storage.CurrentState, block ton.BlockIDExt) bool {
	if current == nil {
		return false
	}
	if blockIDEqual(current.Masterchain.Block, block) {
		return true
	}
	for _, shard := range current.Shards {
		if blockIDEqual(shard.Block, block) {
			return true
		}
	}
	return false
}

func maxKnownMasterSeqno(states ...*storage.CurrentState) uint32 {
	var max uint32
	for _, current := range states {
		if current == nil {
			continue
		}
		seqno := current.Masterchain.Block.SeqNo
		if seqno > max {
			max = seqno
		}
	}
	return max
}

func maxKnownShardSeqno(key storage.BlockHistoryKey, states ...*storage.CurrentState) (uint32, error) {
	var max uint32
	found := false
	for _, current := range states {
		if current == nil {
			continue
		}
		for _, shard := range current.Shards {
			shardKey := storage.ShardKeyFromBlock(shard.Block)
			if shardKey.Workchain != key.Workchain || !sharddomain.Intersects(shardKey.Shard, key.Shard) {
				continue
			}
			if !found || shard.Block.SeqNo > max {
				max = shard.Block.SeqNo
				found = true
			}
		}
	}
	if !found {
		return 0, storage.ErrNotFound
	}
	return max, nil
}

type liveSeqKey struct {
	workchain int32
	shard     int64
	seqno     uint32
}

type liveHistoryKey struct {
	workchain int32
	shard     int64
}

type liveLTIndexEntry struct {
	startLT uint64
	endLT   uint64
	seqno   uint32
	block   ton.BlockIDExt
}

type liveUnixIndexEntry struct {
	genUTime uint32
	seqno    uint32
	block    ton.BlockIDExt
}

type liveBlockLookupKey struct {
	workchain int32
	shard     int64
	seqno     uint32
	rootHash  [32]byte
	fileHash  [32]byte
}

func liveBlockLookupKeyFromBlock(block ton.BlockIDExt) (liveBlockLookupKey, bool) {
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return liveBlockLookupKey{}, false
	}

	key := liveBlockLookupKey{
		workchain: block.Workchain,
		shard:     block.Shard,
		seqno:     block.SeqNo,
	}
	copy(key.rootHash[:], block.RootHash)
	copy(key.fileHash[:], block.FileHash)
	return key, true
}

func blockStateFromCurrent(current *storage.CurrentState, block ton.BlockIDExt) *storage.BlockState {
	if current == nil {
		return nil
	}
	if blockIDEqual(current.Masterchain.Block, block) {
		return storage.CloneBlockState(&current.Masterchain)
	}
	shard, ok := current.Shards[storage.ShardKeyFromBlock(block)]
	if ok && blockIDEqual(shard.Block, block) {
		return storage.CloneBlockState(&shard)
	}
	return nil
}

func currentHasBlockState(current *storage.CurrentState, block ton.BlockIDExt) bool {
	if current == nil {
		return false
	}
	if blockIDEqual(current.Masterchain.Block, block) {
		return true
	}
	shard, ok := current.Shards[storage.ShardKeyFromBlock(block)]
	return ok && blockIDEqual(shard.Block, block)
}

func normalizeLiveBlockRoot(block ton.BlockIDExt, root *cell.Cell) (*cell.Cell, error) {
	if root.GetType() == cell.MerkleProofCellType {
		unwrapped, err := cell.UnwrapProof(root, block.RootHash)
		if err != nil {
			return nil, err
		}
		root = unwrapped
	}

	hash := root.HashKey()
	if !bytes.Equal(hash[:], block.RootHash) {
		return nil, errors.New("live block root hash mismatch")
	}
	return root, nil
}
