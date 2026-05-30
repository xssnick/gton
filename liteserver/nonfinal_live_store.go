package liteserver

import (
	"bytes"
	"errors"
	"sort"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const nonfinalMaxCurrentMasterUTimeLead = 30

var errInvalidNonfinalKind = errors.New("invalid non-final block kind")

type liveNonfinalPending struct {
	block ton.BlockIDExt
	kind  storage.LiveBlockNonfinalKind
	cells map[cell.Hash]*cell.Cell
}

type liveNonfinalWaiting struct {
	artifacts storage.LiveBlockArtifacts
	kind      storage.LiveBlockNonfinalKind
}

type liveNonfinalWaitingSnapshot struct {
	artifacts storage.LiveBlockArtifacts
	kind      storage.LiveBlockNonfinalKind
}

func (s *LiveStore) PublishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind) error {
	published, err := s.publishNonfinalBlockArtifacts(artifacts, kind, true)
	if err != nil {
		return err
	}
	if published {
		s.promoteNonfinalWaiting()
	}
	return nil
}

func (s *LiveStore) NonfinalBlockCacheEnabled() bool {
	return s.nonFinalEnabled
}

func (s *LiveStore) publishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind, keepWaiting bool) (bool, error) {
	if !s.nonFinalEnabled {
		return false, nil
	}
	if !validNonfinalKind(kind) {
		return false, errInvalidNonfinalKind
	}

	block := artifacts.Block
	if block.Workchain == masterchainID && block.Shard == masterchainShard {
		return false, nil
	}
	if !isFullBlockID(&block) {
		return false, storage.ErrNotFound
	}

	original := artifacts
	s.mu.RLock()
	stale := s.nonfinalCoveredByCurrentLocked(block)
	s.mu.RUnlock()
	if stale {
		s.deleteNonfinalWaiting(block)
		return false, nil
	}
	if s.nonfinalTooFarAheadOfCurrentMaster(artifacts.Meta) {
		s.deleteNonfinalWaiting(block)
		return false, nil
	}

	state, meta, cells, err := nonfinalStateFromUpdate(artifacts, s.nonfinalBlockCellLoader(block))
	if err != nil {
		if keepWaiting {
			s.deleteNonfinalWaiting(block)
		}
		return false, err
	}
	if state == nil {
		if keepWaiting {
			s.deleteNonfinalWaiting(block)
		}
		return false, storage.ErrNotFound
	}
	artifacts.State = state
	artifacts.Meta = storage.MergeBlockMeta(meta, artifacts.Meta)
	if s.nonfinalTooFarAheadOfCurrentMaster(artifacts.Meta) {
		s.deleteNonfinalWaiting(block)
		return false, nil
	}

	prepared, err := prepareLiveBlockArtifacts(artifacts)
	if err != nil {
		if keepWaiting {
			s.deleteNonfinalWaiting(block)
		}
		return false, err
	}

	key := storage.BlockKey(block)
	s.mu.Lock()
	if s.nonfinalCoveredByCurrentLocked(block) {
		s.deleteNonfinalWaitingLocked(key)
		s.mu.Unlock()
		return false, nil
	}
	if !s.nonfinalPrevRefsReadyLocked(prepared.meta) {
		if keepWaiting {
			s.rememberNonfinalWaitingLocked(original, kind)
			s.trimNonfinalWaitingLocked()
		}
		s.mu.Unlock()
		return false, nil
	}

	s.deleteNonfinalWaitingLocked(key)
	pending := s.nonFinalPending[key]
	pending.block = block
	pending.kind |= kind
	pending.cells = cells
	s.putNonfinalPendingLocked(key, pending)

	published, ready := s.publishLiveBlockArtifactsPreparedLocked(prepared)
	s.trimNonfinalPendingLocked()
	if published || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
	return true, nil
}

func (s *LiveStore) NonfinalPendingShardBlocks(filter *storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	signed := make([]ton.BlockIDExt, 0)
	candidates := make([]ton.BlockIDExt, 0)
	for _, pending := range s.nonFinalPending {
		if filter != nil && !shardIntersects(*filter, storage.ShardKeyFromBlock(pending.block)) {
			continue
		}
		if pending.kind&storage.LiveBlockNonfinalSigned != 0 {
			signed = append(signed, *cloneBlockID(pending.block))
		}
		if pending.kind&storage.LiveBlockNonfinalCandidate != 0 {
			candidates = append(candidates, *cloneBlockID(pending.block))
		}
	}
	sortBlockIDs(signed)
	sortBlockIDs(candidates)
	return signed, candidates
}

func validNonfinalKind(kind storage.LiveBlockNonfinalKind) bool {
	const allowed = storage.LiveBlockNonfinalSigned | storage.LiveBlockNonfinalCandidate
	return kind != 0 && kind&^allowed == 0
}

func (s *LiveStore) nonfinalTooFarAheadOfCurrentMaster(meta *storage.BlockMeta) bool {
	if meta == nil || meta.GenUTime == 0 {
		return false
	}

	s.mu.RLock()
	currentUTime := s.currentMasterUTimeLocked()
	s.mu.RUnlock()
	if currentUTime == 0 || meta.GenUTime < currentUTime {
		return false
	}
	return meta.GenUTime-currentUTime >= nonfinalMaxCurrentMasterUTimeLead
}

func (s *LiveStore) currentMasterUTimeLocked() uint32 {
	if s.current == nil {
		return 0
	}
	if s.current.Masterchain.Parsed != nil {
		return s.current.Masterchain.Parsed.GenUTime
	}
	meta := s.metas[storage.BlockKey(s.current.Masterchain.Block)]
	if meta == nil {
		return 0
	}
	return meta.GenUTime
}

func (s *LiveStore) nonfinalBlockCellLoader(block ton.BlockIDExt) cell.LazyCellLoader {
	return func(hash cell.Hash) (*cell.Cell, error) {
		return s.loadNonfinalCell(block, hash)
	}
}

func (s *LiveStore) loadNonfinalCell(block ton.BlockIDExt, hash cell.Hash) (*cell.Cell, error) {
	s.mu.RLock()
	cached := s.nonfinalCellLocked(block, hash)
	s.mu.RUnlock()
	if cached != nil {
		if cached.RefsNum() == 0 {
			return cached, nil
		}
		return nonfinalLazyCell(cached, s.nonfinalBlockCellLoader(block))
	}
	if s.nonFinalCellLoader == nil {
		return nil, cell.ErrLazyRefNotFound
	}
	return s.nonFinalCellLoader(hash)
}

func (s *LiveStore) nonfinalCellLocked(block ton.BlockIDExt, hash cell.Hash) *cell.Cell {
	idx := s.nonfinalOrderIndexLocked(storage.BlockKey(block))
	if idx < 0 {
		return nil
	}

	for i := idx; i >= 0; i-- {
		pending := s.nonFinalPending[s.nonFinalOrder[i]]
		if root := pending.cells[hash]; root != nil {
			return root
		}
	}
	return nil
}

func (s *LiveStore) putNonfinalPendingLocked(key storage.BlockRootHash, pending liveNonfinalPending) {
	if _, ok := s.nonFinalPending[key]; !ok {
		s.nonFinalOrder = append(s.nonFinalOrder, key)
	}
	s.nonFinalPending[key] = pending
}

func (s *LiveStore) nonfinalOrderIndexLocked(key storage.BlockRootHash) int {
	for i := len(s.nonFinalOrder) - 1; i >= 0; i-- {
		if s.nonFinalOrder[i] == key {
			return i
		}
	}
	return -1
}

func (s *LiveStore) nonfinalPrevRefsReadyLocked(meta *storage.BlockMeta) bool {
	if meta == nil || len(meta.PrevRefs) == 0 {
		return false
	}

	for _, prev := range meta.PrevRefs {
		if !s.nonfinalBlockStateReadyLocked(prev) {
			return false
		}
	}
	return true
}

func (s *LiveStore) nonfinalBlockStateReadyLocked(block ton.BlockIDExt) bool {
	if currentRefersToBlock(s.current, block) || currentRefersToBlock(s.pendingCurrent, block) {
		return true
	}
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return false
	}
	state, ok := s.states[key]
	return ok && blockIDEqual(state.Block, block)
}

func (s *LiveStore) rememberNonfinalWaitingLocked(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind) {
	key := storage.BlockKey(artifacts.Block)
	waiting := s.nonFinalWaiting[key]
	waiting.artifacts = cloneLiveBlockArtifacts(artifacts)
	waiting.kind |= kind
	s.nonFinalWaiting[key] = waiting
}

func (s *LiveStore) deleteNonfinalWaiting(block ton.BlockIDExt) {
	key := storage.BlockKey(block)

	s.mu.Lock()
	s.deleteNonfinalWaitingLocked(key)
	s.mu.Unlock()
}

func (s *LiveStore) deleteNonfinalWaitingLocked(key storage.BlockRootHash) {
	delete(s.nonFinalWaiting, key)
}

func (s *LiveStore) trimNonfinalWaitingLocked() {
	if s.nonFinalCache < 0 || len(s.nonFinalWaiting) <= s.nonFinalCache {
		return
	}

	for len(s.nonFinalWaiting) > s.nonFinalCache {
		var evictKey storage.BlockRootHash
		var evictBlock ton.BlockIDExt
		first := true
		for key, waiting := range s.nonFinalWaiting {
			block := waiting.artifacts.Block
			if first || nonfinalEvictLess(block, evictBlock) {
				evictKey = key
				evictBlock = block
				first = false
			}
		}
		if first {
			return
		}
		delete(s.nonFinalWaiting, evictKey)
	}
}

func (s *LiveStore) nonfinalWaitingSnapshot() []liveNonfinalWaitingSnapshot {
	s.mu.RLock()
	out := make([]liveNonfinalWaitingSnapshot, 0, len(s.nonFinalWaiting))
	for _, waiting := range s.nonFinalWaiting {
		out = append(out, liveNonfinalWaitingSnapshot{
			artifacts: waiting.artifacts,
			kind:      waiting.kind,
		})
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return nonfinalEvictLess(out[i].artifacts.Block, out[j].artifacts.Block)
	})
	return out
}

func (s *LiveStore) nonfinalWaitingLen() int {
	s.mu.RLock()
	n := len(s.nonFinalWaiting)
	s.mu.RUnlock()
	return n
}

func (s *LiveStore) promoteNonfinalWaiting() {
	for {
		waiting := s.nonfinalWaitingSnapshot()
		if len(waiting) == 0 {
			return
		}

		progress := false
		for _, item := range waiting {
			published, err := s.publishNonfinalBlockArtifacts(item.artifacts, item.kind, false)
			if err != nil {
				s.deleteNonfinalWaiting(item.artifacts.Block)
				continue
			}
			if published {
				progress = true
			}
		}
		if !progress && s.nonfinalWaitingLen() >= len(waiting) {
			return
		}
	}
}

func (s *LiveStore) cleanupNonfinalPendingLocked() {
	if s.current == nil {
		return
	}

	for key, pending := range s.nonFinalPending {
		if !s.nonfinalCoveredByCurrentLocked(pending.block) {
			continue
		}
		s.deleteNonfinalBlockLocked(key, pending.block)
	}
	for key, waiting := range s.nonFinalWaiting {
		if !s.nonfinalCoveredByCurrentLocked(waiting.artifacts.Block) {
			continue
		}
		delete(s.nonFinalWaiting, key)
	}
	s.dropNonfinalGapsLocked()
}

func (s *LiveStore) trimNonfinalPendingLocked() {
	if s.nonFinalCache < 0 || len(s.nonFinalPending) <= s.nonFinalCache {
		return
	}

	for len(s.nonFinalPending) > s.nonFinalCache && len(s.nonFinalOrder) > 0 {
		key := s.nonFinalOrder[0]
		pending := s.nonFinalPending[key]
		s.deleteNonfinalBlockLocked(key, pending.block)
	}
	s.dropNonfinalGapsLocked()
}

func (s *LiveStore) deleteNonfinalBlockLocked(key storage.BlockRootHash, block ton.BlockIDExt) {
	s.deleteNonfinalPendingLocked(key)
	if s.currentRefersToBlockLocked(block) {
		return
	}
	liveKey := storage.BlockKey(block)
	if cached := s.blocks[liveKey]; cached != nil {
		s.deleteLiveBlockLocked(liveKey, cached, liveBlockKind(cached.id))
	}
}

func (s *LiveStore) deleteNonfinalPendingLocked(key storage.BlockRootHash) {
	if _, ok := s.nonFinalPending[key]; !ok {
		return
	}
	delete(s.nonFinalPending, key)

	for i, item := range s.nonFinalOrder {
		if item != key {
			continue
		}
		copy(s.nonFinalOrder[i:], s.nonFinalOrder[i+1:])
		s.nonFinalOrder[len(s.nonFinalOrder)-1] = storage.BlockRootHash{}
		s.nonFinalOrder = s.nonFinalOrder[:len(s.nonFinalOrder)-1]
		return
	}
}

func (s *LiveStore) dropNonfinalGapsLocked() {
	for {
		dropped := false
		for key, pending := range s.nonFinalPending {
			meta := s.metas[storage.BlockKey(pending.block)]
			if s.nonfinalPrevRefsReadyLocked(meta) {
				continue
			}
			s.deleteNonfinalBlockLocked(key, pending.block)
			dropped = true
		}
		if !dropped {
			return
		}
	}
}

func (s *LiveStore) nonfinalCoveredByCurrentLocked(block ton.BlockIDExt) bool {
	if s.current == nil {
		return false
	}
	if block.Workchain == masterchainID && block.Shard == masterchainShard {
		return s.current.Masterchain.Block.SeqNo >= block.SeqNo
	}

	key := storage.ShardKeyFromBlock(block)
	for _, shard := range s.current.Shards {
		shardKey := storage.ShardKeyFromBlock(shard.Block)
		if !shardIntersects(key, shardKey) {
			continue
		}
		if shard.Block.SeqNo >= block.SeqNo {
			return true
		}
	}
	return false
}

func sortBlockIDs(blocks []ton.BlockIDExt) {
	sort.Slice(blocks, func(i, j int) bool {
		return blockIDLess(blocks[i], blocks[j])
	})
}

func nonfinalEvictLess(left, right ton.BlockIDExt) bool {
	if left.SeqNo != right.SeqNo {
		return left.SeqNo < right.SeqNo
	}
	return blockIDLess(left, right)
}

func blockIDLess(left, right ton.BlockIDExt) bool {
	if left.Workchain != right.Workchain {
		return left.Workchain < right.Workchain
	}
	if left.Shard != right.Shard {
		return left.Shard < right.Shard
	}
	if left.SeqNo != right.SeqNo {
		return left.SeqNo < right.SeqNo
	}
	if cmp := bytes.Compare(left.RootHash, right.RootHash); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(left.FileHash, right.FileHash) < 0
}
