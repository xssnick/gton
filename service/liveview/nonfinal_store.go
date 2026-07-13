package liveview

import (
	"bytes"
	"errors"
	"sort"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const nonfinalMaxCurrentMasterUTimeLead = 30

var errInvalidNonfinalKind = errors.New("invalid non-final block kind")

type liveNonfinalPending struct {
	block ton.BlockIDExt
	kind  storage.LiveBlockNonfinalKind
	cells storage.StateCellRecords
}

type liveNonfinalCellIndexEntry struct {
	block storage.BlockRootHash
	data  []byte
}

type liveNonfinalWaiting struct {
	artifacts storage.LiveBlockArtifacts
	kind      storage.LiveBlockNonfinalKind
}

type liveNonfinalWaitingSnapshot struct {
	artifacts storage.LiveBlockArtifacts
	kind      storage.LiveBlockNonfinalKind
}

func (s *Store) PublishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind) error {
	published, err := s.publishNonfinalBlockArtifacts(artifacts, kind, true)
	if err != nil {
		return err
	}
	if published {
		s.promoteNonfinalWaiting()
	}
	return nil
}

func (s *Store) NonfinalBlockCacheEnabled() bool {
	return s.nonFinalEnabled
}

func (s *Store) publishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind, keepWaiting bool) (bool, error) {
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

	key := storage.BlockKey(block)
	s.mu.Lock()
	if pending, ok := s.nonFinalPending[key]; ok {
		// The pending map is keyed by the block root hash, so an existing entry
		// already holds identical content (typically the candidate broadcast that
		// preceded this signed one). Merge the kind bit directly instead of
		// re-running the whole parse/apply/index pipeline; a bare map write keeps
		// the order and cell indexes untouched.
		if pending.kind&kind != kind {
			pending.kind |= kind
			s.nonFinalPending[key] = pending
		}
		s.deleteNonfinalWaitingLocked(key)
		s.mu.Unlock()
		return false, nil
	}
	s.mu.Unlock()

	loader := s.nonfinalBlockCellLoader(block)
	var state *storage.BlockState
	var meta *storage.BlockMeta
	var cells storage.StateCellRecords
	var update *cell.Cell
	var err error
	if artifacts.State != nil {
		s.mu.Lock()
		ready := s.nonfinalReadyToPrepareLocked(key, block, artifacts.Meta, original, kind, keepWaiting)
		s.mu.Unlock()
		if !ready {
			return false, nil
		}

		state, meta, cells, err = nonfinalStateFromSnapshot(artifacts, loader)
	} else {
		parsed, parseErr := nonfinalParseStateUpdate(artifacts, kind&storage.LiveBlockNonfinalCandidate == 0)
		if parseErr != nil {
			if keepWaiting {
				s.deleteNonfinalWaiting(block)
			}
			return false, parseErr
		}
		artifacts.Root = parsed.root
		meta = parsed.meta
		update = parsed.update
		artifacts.Meta = storage.MergeBlockMeta(meta, artifacts.Meta)
		// Waiting entries are remembered from `original`; carry the parse results
		// over so promotion retries skip re-deriving them from the raw block.
		original.Root = parsed.root
		original.StateUpdate = parsed.update
		original.Meta = artifacts.Meta
		if s.nonfinalTooFarAheadOfCurrentMaster(artifacts.Meta) {
			s.deleteNonfinalWaiting(block)
			return false, nil
		}

		s.mu.Lock()
		ready := s.nonfinalReadyToPrepareLocked(key, block, artifacts.Meta, original, kind, keepWaiting)
		s.mu.Unlock()
		if !ready {
			return false, nil
		}

		state, cells, err = nonfinalStateFromParsedUpdate(artifacts, parsed, loader)
	}
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
	artifacts.BlockData = nil
	artifacts.Proofs = nil
	artifacts.Meta = nonfinalStateOnlyMeta(artifacts.Meta)

	s.mu.Lock()
	ready := s.nonfinalReadyToPrepareLocked(key, block, artifacts.Meta, original, kind, keepWaiting)
	s.mu.Unlock()
	if !ready {
		return false, nil
	}

	prepared, err := prepareLiveBlockArtifacts(artifacts)
	if err != nil {
		if keepWaiting {
			s.deleteNonfinalWaiting(block)
		}
		return false, err
	}

	s.mu.Lock()
	ready, err = s.nonfinalReadyToPublishLocked(key, block, prepared.meta, update, original, kind, keepWaiting)
	if !ready || err != nil {
		s.mu.Unlock()
		return false, err
	}

	s.deleteNonfinalWaitingLocked(key)
	pending := s.nonFinalPending[key]
	pending.block = block
	pending.kind |= kind
	pending.cells = cells
	s.putNonfinalPendingLocked(key, pending)

	published, ready := s.publishLiveBlockArtifactsPreparedLocked(prepared)
	s.removeNonfinalLookupIndexesLocked(block)
	s.trimNonfinalPendingLocked()
	if published || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
	return true, nil
}

func (s *Store) nonfinalReadyToPrepareLocked(key storage.BlockRootHash, block ton.BlockIDExt, meta *storage.BlockMeta, original storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind, keepWaiting bool) bool {
	if s.nonfinalCoveredByCurrentLocked(block) {
		s.deleteNonfinalWaitingLocked(key)
		return false
	}
	if !s.nonfinalPrevRefsReadyLocked(meta) {
		if keepWaiting {
			s.rememberNonfinalWaitingLocked(original, kind)
			s.trimNonfinalWaitingLocked()
		}
		return false
	}
	return true
}

func (s *Store) nonfinalReadyToPublishLocked(key storage.BlockRootHash, block ton.BlockIDExt, meta *storage.BlockMeta, update *cell.Cell, original storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind, keepWaiting bool) (bool, error) {
	if !s.nonfinalReadyToPrepareLocked(key, block, meta, original, kind, keepWaiting) {
		return false, nil
	}
	if update == nil {
		return true, nil
	}

	previous, ok := s.nonfinalPreviousStatesLocked(meta)
	if !ok {
		if keepWaiting {
			s.rememberNonfinalWaitingLocked(original, kind)
			s.trimNonfinalWaitingLocked()
		}
		return false, nil
	}
	if err := nonfinalStateUpdateApplies(previous, update); err != nil {
		if keepWaiting {
			s.deleteNonfinalWaitingLocked(key)
		}
		return false, err
	}
	return true, nil
}

func (s *Store) NonfinalPendingShardBlocks(filter *storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	signed := make([]ton.BlockIDExt, 0)
	candidates := make([]ton.BlockIDExt, 0)
	for _, pending := range s.nonFinalPending {
		if filter != nil && !blockproof.ShardIntersects(*filter, storage.ShardKeyFromBlock(pending.block)) {
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

func nonfinalStateOnlyMeta(meta *storage.BlockMeta) *storage.BlockMeta {
	cloned := meta.Clone()
	if cloned == nil {
		return nil
	}

	const servedArtifactFlags = storage.BlockMetaHasServedFull |
		storage.BlockMetaServedFullIsLink |
		storage.BlockMetaHasBlockData |
		storage.BlockMetaHasProofBlock |
		storage.BlockMetaHasProofBlockLink |
		storage.BlockMetaHasProofKeyBlock |
		storage.BlockMetaHasProofKeyBlockLink

	cloned.Flags &^= servedArtifactFlags
	return cloned
}

func (s *Store) nonfinalTooFarAheadOfCurrentMaster(meta *storage.BlockMeta) bool {
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

func (s *Store) currentMasterUTimeLocked() uint32 {
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

func (s *Store) nonfinalBlockCellLoader(block ton.BlockIDExt) cell.LazyCellLoader {
	var loader cell.LazyCellLoader
	loader = func(hash cell.Hash) (*cell.Cell, error) {
		return s.loadNonfinalCell(block, hash, loader)
	}
	return loader
}

func (s *Store) loadNonfinalCell(block ton.BlockIDExt, hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	s.mu.RLock()
	cached := s.nonfinalCellDataLocked(block, hash)
	base := s.nonFinalCellLoader
	s.mu.RUnlock()
	if len(cached) > 0 {
		return nonfinalLazyCellRecord(hash, cached, loader)
	}
	if base == nil {
		return nil, cell.ErrLazyRefNotFound
	}
	return base(hash)
}

func (s *Store) nonfinalCellDataLocked(block ton.BlockIDExt, hash cell.Hash) []byte {
	idx := s.nonfinalOrderIndexLocked(storage.BlockKey(block))
	if idx < 0 {
		return nil
	}

	bestIdx := -1
	var data []byte
	for _, entry := range s.nonFinalCellIndex[hash] {
		entryIdx := s.nonfinalOrderIndexLocked(entry.block)
		if entryIdx < 0 || entryIdx > idx || entryIdx <= bestIdx {
			continue
		}
		bestIdx = entryIdx
		data = entry.data
	}
	return data
}

func (s *Store) putNonfinalPendingLocked(key storage.BlockRootHash, pending liveNonfinalPending) {
	current, ok := s.nonFinalPending[key]
	if ok {
		s.removeNonfinalCellIndexLocked(key, current.cells)
	} else {
		s.nonFinalOrderIndex[key] = len(s.nonFinalOrder)
		s.nonFinalOrder = append(s.nonFinalOrder, key)
	}
	s.nonFinalPending[key] = pending
	s.addNonfinalCellIndexLocked(key, pending.cells)
}

func (s *Store) addNonfinalCellIndexLocked(key storage.BlockRootHash, records storage.StateCellRecords) {
	if records.Empty() {
		return
	}
	if s.nonFinalCellIndex == nil {
		s.nonFinalCellIndex = map[cell.Hash][]liveNonfinalCellIndexEntry{}
	}

	seen := map[cell.Hash]struct{}{}
	_ = records.ForEach(func(record storage.EncodedCellRecord) error {
		if len(record.Data) == 0 {
			return nil
		}
		if _, ok := seen[record.Hash]; ok {
			return nil
		}
		seen[record.Hash] = struct{}{}
		s.nonFinalCellIndex[record.Hash] = append(s.nonFinalCellIndex[record.Hash], liveNonfinalCellIndexEntry{
			block: key,
			data:  record.Data,
		})
		return nil
	})
}

func (s *Store) removeNonfinalCellIndexLocked(key storage.BlockRootHash, records storage.StateCellRecords) {
	if records.Empty() || len(s.nonFinalCellIndex) == 0 {
		return
	}

	seen := map[cell.Hash]struct{}{}
	_ = records.ForEach(func(record storage.EncodedCellRecord) error {
		if len(record.Data) == 0 {
			return nil
		}
		if _, ok := seen[record.Hash]; ok {
			return nil
		}
		seen[record.Hash] = struct{}{}

		entries := s.nonFinalCellIndex[record.Hash]
		for i := 0; i < len(entries); i++ {
			if entries[i].block != key {
				continue
			}
			copy(entries[i:], entries[i+1:])
			entries[len(entries)-1] = liveNonfinalCellIndexEntry{}
			entries = entries[:len(entries)-1]
			i--
		}
		if len(entries) == 0 {
			delete(s.nonFinalCellIndex, record.Hash)
		} else {
			s.nonFinalCellIndex[record.Hash] = entries
		}
		return nil
	})
}

func (s *Store) nonfinalOrderIndexLocked(key storage.BlockRootHash) int {
	idx, ok := s.nonFinalOrderIndex[key]
	if !ok {
		return -1
	}
	return idx
}

func (s *Store) nonfinalPrevRefsReadyLocked(meta *storage.BlockMeta) bool {
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

func (s *Store) nonfinalBlockStateReadyLocked(block ton.BlockIDExt) bool {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if ok {
		if state, exists := s.states[key]; exists && blockIDEqual(state.Block, block) && state.Cell != nil {
			return true
		}
	}
	return currentBlockStateReady(s.pendingCurrent, block) || currentBlockStateReady(s.current, block)
}

func currentBlockStateReady(current *storage.CurrentState, block ton.BlockIDExt) bool {
	if current == nil {
		return false
	}
	if blockIDEqual(current.Masterchain.Block, block) {
		return current.Masterchain.Cell != nil
	}
	shard, ok := current.Shards[storage.ShardKeyFromBlock(block)]
	return ok && blockIDEqual(shard.Block, block) && shard.Cell != nil
}

func (s *Store) nonfinalPreviousStatesLocked(meta *storage.BlockMeta) ([]*storage.BlockState, bool) {
	if meta == nil || len(meta.PrevRefs) == 0 {
		return nil, false
	}

	previous := make([]*storage.BlockState, 0, len(meta.PrevRefs))
	for _, prev := range meta.PrevRefs {
		state := s.nonfinalBlockStateLocked(prev)
		if state == nil || state.Cell == nil {
			return nil, false
		}
		previous = append(previous, state)
	}
	return previous, true
}

func (s *Store) nonfinalBlockStateLocked(block ton.BlockIDExt) *storage.BlockState {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if ok {
		if state, exists := s.states[key]; exists && blockIDEqual(state.Block, block) {
			return storage.CloneBlockState(&state)
		}
	}
	if state := blockStateFromCurrent(s.pendingCurrent, block); state != nil {
		return state
	}
	return blockStateFromCurrent(s.current, block)
}

func (s *Store) removeNonfinalLookupIndexesLocked(block ton.BlockIDExt) {
	seqKey := liveSeqKey{workchain: block.Workchain, shard: block.Shard, seqno: block.SeqNo}
	if indexed, ok := s.seqIndex[seqKey]; ok && blockIDEqual(indexed, block) {
		delete(s.seqIndex, seqKey)
	}
	if meta := s.metas[storage.BlockKey(block)]; meta != nil {
		s.removeMetaHistoryIndexLocked(meta)
	}
}

func (s *Store) rememberNonfinalWaitingLocked(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind) {
	key := storage.BlockKey(artifacts.Block)
	waiting := s.nonFinalWaiting[key]
	waiting.artifacts = cloneLiveBlockArtifacts(artifacts)
	waiting.kind |= kind
	s.nonFinalWaiting[key] = waiting
}

func (s *Store) deleteNonfinalWaiting(block ton.BlockIDExt) {
	key := storage.BlockKey(block)

	s.mu.Lock()
	s.deleteNonfinalWaitingLocked(key)
	s.mu.Unlock()
}

func (s *Store) deleteNonfinalWaitingLocked(key storage.BlockRootHash) {
	delete(s.nonFinalWaiting, key)
}

func (s *Store) trimNonfinalWaitingLocked() {
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

func (s *Store) nonfinalWaitingSnapshot() []liveNonfinalWaitingSnapshot {
	s.mu.RLock()
	out := make([]liveNonfinalWaitingSnapshot, 0, len(s.nonFinalWaiting))
	for _, waiting := range s.nonFinalWaiting {
		out = append(out, liveNonfinalWaitingSnapshot(waiting))
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return nonfinalEvictLess(out[i].artifacts.Block, out[j].artifacts.Block)
	})
	return out
}

func (s *Store) nonfinalWaitingLen() int {
	s.mu.RLock()
	n := len(s.nonFinalWaiting)
	s.mu.RUnlock()
	return n
}

func (s *Store) promoteNonfinalWaiting() {
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

func (s *Store) cleanupNonfinalPendingLocked() {
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

func (s *Store) trimNonfinalPendingLocked() {
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

func (s *Store) deleteNonfinalBlockLocked(key storage.BlockRootHash, block ton.BlockIDExt) {
	s.deleteNonfinalPendingLocked(key)
	if s.currentRefersToBlockLocked(block) {
		return
	}
	liveKey := storage.BlockKey(block)
	if cached := s.blocks[liveKey]; cached != nil {
		s.deleteLiveBlockLocked(liveKey, cached, liveBlockKind(cached.id))
	}
}

func (s *Store) deleteNonfinalPendingLocked(key storage.BlockRootHash) {
	pending, ok := s.nonFinalPending[key]
	if !ok {
		return
	}
	s.removeNonfinalCellIndexLocked(key, pending.cells)
	delete(s.nonFinalPending, key)

	idx := s.nonFinalOrderIndex[key]
	delete(s.nonFinalOrderIndex, key)

	copy(s.nonFinalOrder[idx:], s.nonFinalOrder[idx+1:])
	s.nonFinalOrder[len(s.nonFinalOrder)-1] = storage.BlockRootHash{}
	s.nonFinalOrder = s.nonFinalOrder[:len(s.nonFinalOrder)-1]
	for i := idx; i < len(s.nonFinalOrder); i++ {
		s.nonFinalOrderIndex[s.nonFinalOrder[i]] = i
	}
}

func (s *Store) dropNonfinalGapsLocked() {
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

func (s *Store) nonfinalCoveredByCurrentLocked(block ton.BlockIDExt) bool {
	if s.current == nil {
		return false
	}
	if block.Workchain == masterchainID && block.Shard == masterchainShard {
		return s.current.Masterchain.Block.SeqNo >= block.SeqNo
	}

	key := storage.ShardKeyFromBlock(block)
	for _, shard := range s.current.Shards {
		shardKey := storage.ShardKeyFromBlock(shard.Block)
		if !blockproof.ShardIntersects(key, shardKey) {
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
