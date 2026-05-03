package liteserver

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultMasterBlockCache = 128
	DefaultShardBlockCache  = 4096
)

var (
	errWaitMasterchainTimeout = errors.New("timeout")
	errWaitMasterchainTooFar  = errors.New("too big masterchain block seqno")
)

type LiveStoreOptions struct {
	MasterBlockCache int
	ShardBlockCache  int
}

type LiveStore struct {
	Store

	mu               sync.RWMutex
	current          *storage.CurrentState
	pendingCurrent   *storage.CurrentState
	readyMasterSeqno uint32
	notify           chan struct{}
	blocks           map[string]*liveBlock
	metas            map[string]*storage.BlockMeta
	seqIndex         map[liveSeqKey]ton.BlockIDExt
	flushed          map[string]struct{}
	masterOrder      []string
	shardOrder       []string
	masterCacheSize  int
	shardCacheSize   int
}

type liveBlock struct {
	id      ton.BlockIDExt
	root    *cell.Cell
	data    []byte
	meta    *storage.BlockMeta
	flushed bool
}

func NewLiveStore(store Store, opts ...LiveStoreOptions) *LiveStore {
	cfg := LiveStoreOptions{
		MasterBlockCache: DefaultMasterBlockCache,
		ShardBlockCache:  DefaultShardBlockCache,
	}
	if len(opts) > 0 {
		cfg = opts[0]
	}

	return &LiveStore{
		Store:           store,
		notify:          make(chan struct{}),
		blocks:          map[string]*liveBlock{},
		metas:           map[string]*storage.BlockMeta{},
		seqIndex:        map[liveSeqKey]ton.BlockIDExt{},
		flushed:         map[string]struct{}{},
		masterCacheSize: cfg.MasterBlockCache,
		shardCacheSize:  cfg.ShardBlockCache,
	}
}

func (s *LiveStore) SetLiveCurrentState(current *storage.CurrentState) {
	next := storage.CloneCurrentState(current)

	s.mu.Lock()
	prevSeqno := currentMasterchainSeqno(s.current)
	s.pendingCurrent = next
	published := s.publishPendingCurrentLocked()
	nextSeqno := currentMasterchainSeqno(s.current)
	s.rebuildIndexesLocked()
	ready := s.updateReadyMasterSeqnoLocked()
	if published || ready || nextSeqno > prevSeqno || currentMasterchainSeqno(next) > prevSeqno {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
}

func (s *LiveStore) CurrentState(ctx context.Context) (*storage.CurrentState, error) {
	s.mu.RLock()
	current := storage.CloneCurrentState(s.current)
	s.mu.RUnlock()
	if current != nil {
		return current, nil
	}
	if s.Store == nil {
		return nil, storage.ErrNotFound
	}

	current, err := s.Store.CurrentState(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.storedCurrentBlocksReady(ctx, current); err != nil {
		return nil, err
	}
	return current, nil
}

func (s *LiveStore) SetLiveBlock(block ton.BlockIDExt, root *cell.Cell, data []byte, flushed bool) error {
	if root == nil {
		if len(data) == 0 {
			return errors.New("live block has no cell tree or BOC")
		}

		parsed, err := parseTrustedBlockBOC(block, data)
		if err != nil {
			return err
		}
		root = parsed
	}

	root, err := normalizeLiveBlockRoot(block, root)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		data = append([]byte(nil), data...)
	}
	meta, _ := storage.BuildBlockMetaFromBlockCell(block, root)

	key := storage.BlockKey(block)

	s.mu.Lock()
	if _, ok := s.flushed[key]; ok {
		flushed = true
		delete(s.flushed, key)
	}
	s.putBlockLocked(key, &liveBlock{
		id:      block,
		root:    root,
		data:    data,
		meta:    meta,
		flushed: flushed,
	})
	published := s.publishPendingCurrentLocked()
	s.trimBlocksLocked(liveBlockKind(block))
	s.rebuildIndexesLocked()
	ready := s.updateReadyMasterSeqnoLocked()
	if published || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
	return nil
}

func (s *LiveStore) MarkLiveBlockFlushed(block ton.BlockIDExt) {
	key := storage.BlockKey(block)

	s.mu.Lock()
	if cached := s.blocks[key]; cached != nil {
		cached.flushed = true
		published := s.publishPendingCurrentLocked()
		s.trimBlocksLocked(liveBlockKind(block))
		s.rebuildIndexesLocked()
		ready := s.updateReadyMasterSeqnoLocked()
		if published || ready {
			close(s.notify)
			s.notify = make(chan struct{})
		}
	} else {
		s.flushed[key] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *LiveStore) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if state := s.cachedBlockState(block); state != nil {
		return state, nil
	}
	if s.Store == nil {
		return nil, storage.ErrNotFound
	}
	return s.Store.BlockState(ctx, block)
}

func (s *LiveStore) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, uint64, error) {
	if s.Store == nil {
		return nil, 0, storage.ErrNotFound
	}
	return s.Store.LoadStateCellTree(ctx, block, rootHash)
}

func (s *LiveStore) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	cached := s.cachedBlockMeta(block)
	if cached == nil {
		if s.Store == nil {
			return nil, storage.ErrNotFound
		}
		return s.Store.BlockMeta(ctx, block)
	}
	if s.Store == nil {
		return cached, nil
	}

	stored, err := s.Store.BlockMeta(ctx, block)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return cached, nil
		}
		return nil, err
	}
	return storage.MergeBlockMeta(stored, cached), nil
}

func (s *LiveStore) LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	if block, ok := s.cachedBlockBySeqNo(key, seqno); ok {
		return block, nil
	}
	if s.Store == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return s.Store.LookupBlockBySeqNo(ctx, key, seqno)
}

func (s *LiveStore) LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	if block, ok := s.cachedBlockByLT(key, lt); ok {
		return block, nil
	}
	if s.Store == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return s.Store.LookupBlockByLT(ctx, key, lt)
}

func (s *LiveStore) LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	if block, ok := s.cachedBlockByUnixTime(key, utime); ok {
		return block, nil
	}
	if s.Store == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return s.Store.LookupBlockByUnixTime(ctx, key, utime)
}

func (s *LiveStore) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if root := s.cachedBlockRoot(block); root != nil {
		return root, nil
	}
	if s.Store == nil {
		return nil, storage.ErrNotFound
	}

	data, err := s.Store.BlockData(ctx, block)
	if err != nil {
		return nil, err
	}

	root, err := parseTrustedBlockBOC(block, data)
	if err != nil {
		return nil, err
	}
	if err = s.SetLiveBlock(block, root, data, true); err != nil {
		return nil, err
	}
	return root, nil
}

func (s *LiveStore) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	if data, ok := s.cachedBlockData(block); ok {
		return data, nil
	}
	if s.Store == nil {
		return nil, storage.ErrNotFound
	}

	data, err := s.Store.BlockData(ctx, block)
	if err != nil {
		return nil, err
	}

	root, err := parseTrustedBlockBOC(block, data)
	if err == nil {
		err = s.SetLiveBlock(block, root, data, true)
	}
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), data...), nil
}

func (s *LiveStore) ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	if s.Store == nil {
		return nil, storage.ErrNotFound
	}
	return s.Store.ZeroState(ctx, block)
}

func (s *LiveStore) WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error {
	if timeout > 10*time.Second {
		timeout = 10 * time.Second
	}
	if timeout < 0 {
		timeout = 0
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		current, readySeqno, hasLiveCurrent, notify := s.waitSnapshot()
		if current == nil && s.Store != nil {
			stored, err := s.Store.CurrentState(waitCtx)
			if err == nil {
				err = s.storedCurrentBlocksReady(waitCtx, stored)
			}
			if err == nil {
				current = stored
			} else if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		}

		currentSeqno := currentMasterchainSeqno(current)
		if readySeqno >= seqno || !hasLiveCurrent && currentSeqno >= seqno {
			return nil
		}
		if currentSeqno > 0 && seqno > currentSeqno+100 {
			return errWaitMasterchainTooFar
		}

		select {
		case <-notify:
		case <-waitCtx.Done():
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return errWaitMasterchainTimeout
			}
			return waitCtx.Err()
		}
	}
}

func (s *LiveStore) publishPendingCurrentLocked() bool {
	if s.pendingCurrent == nil || !s.currentStateShardsReadyLocked(s.pendingCurrent) {
		return false
	}

	prevSeqno := currentMasterchainSeqno(s.current)
	nextSeqno := currentMasterchainSeqno(s.pendingCurrent)
	if nextSeqno < prevSeqno {
		s.pendingCurrent = nil
		return false
	}

	s.current = storage.CloneCurrentState(s.pendingCurrent)
	s.pendingCurrent = nil
	return nextSeqno > prevSeqno
}

func (s *LiveStore) currentStateShardsReadyLocked(current *storage.CurrentState) bool {
	if current == nil {
		return false
	}
	if !s.blockDataReadyLocked(current.Masterchain.Block) {
		return false
	}
	for _, shard := range current.Shards {
		if !s.blockDataReadyLocked(shard.Block) {
			return false
		}
	}
	return true
}

func (s *LiveStore) blockDataReadyLocked(block ton.BlockIDExt) bool {
	if blockStateFromCurrent(s.current, block) != nil {
		return true
	}

	cached := s.blocks[storage.BlockKey(block)]
	return cached != nil && cached.flushed && liveBlockHasData(cached)
}

func (s *LiveStore) updateReadyMasterSeqnoLocked() bool {
	next := s.readyMasterSeqno
	if s.current != nil && s.current.Masterchain.Block.SeqNo > next {
		next = s.current.Masterchain.Block.SeqNo
	}

	if next <= s.readyMasterSeqno {
		return false
	}
	s.readyMasterSeqno = next
	return true
}

func liveBlockHasData(block *liveBlock) bool {
	if block == nil {
		return false
	}
	if len(block.data) > 0 {
		return true
	}
	return block.root != nil
}

func (s *LiveStore) waitSnapshot() (*storage.CurrentState, uint32, bool, <-chan struct{}) {
	s.mu.RLock()
	current := storage.CloneCurrentState(s.current)
	readySeqno := s.readyMasterSeqno
	hasLiveCurrent := s.current != nil
	notify := s.notify
	s.mu.RUnlock()
	return current, readySeqno, hasLiveCurrent, notify
}

func currentMasterchainSeqno(current *storage.CurrentState) uint32 {
	if current == nil {
		return 0
	}
	return current.Masterchain.Block.SeqNo
}

func (s *LiveStore) cachedBlockState(block ton.BlockIDExt) *storage.BlockState {
	s.mu.RLock()
	state := blockStateFromCurrent(s.current, block)
	defer s.mu.RUnlock()

	return state
}

func (s *LiveStore) cachedBlockMeta(block ton.BlockIDExt) *storage.BlockMeta {
	key := storage.BlockKey(block)

	s.mu.RLock()
	meta := s.metas[key]
	s.mu.RUnlock()
	if meta == nil {
		return nil
	}
	return meta.Clone()
}

func (s *LiveStore) cachedBlockBySeqNo(key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, bool) {
	s.mu.RLock()
	block, ok := s.seqIndex[liveSeqKey{workchain: key.Workchain, shard: key.Shard, seqno: seqno}]
	s.mu.RUnlock()
	if !ok {
		return ton.BlockIDExt{}, false
	}
	return *cloneBlockID(block), true
}

func (s *LiveStore) cachedBlockByLT(key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ge *storage.BlockMeta
	var floor *storage.BlockMeta
	for _, meta := range s.metas {
		if meta.ID.Workchain != key.Workchain || meta.ID.Shard != key.Shard || meta.EndLT == 0 {
			continue
		}
		if meta.EndLT >= lt && (ge == nil || meta.EndLT < ge.EndLT || meta.EndLT == ge.EndLT && meta.ID.SeqNo < ge.ID.SeqNo) {
			ge = meta
		}
		if meta.EndLT <= lt && (floor == nil || meta.EndLT > floor.EndLT || meta.EndLT == floor.EndLT && meta.ID.SeqNo > floor.ID.SeqNo) {
			floor = meta
		}
	}
	if ge != nil && ge.StartLT <= lt && lt <= ge.EndLT {
		return *cloneBlockID(ge.ID), true
	}
	if floor != nil {
		return *cloneBlockID(floor.ID), true
	}
	return ton.BlockIDExt{}, false
}

func (s *LiveStore) cachedBlockByUnixTime(key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var best *storage.BlockMeta
	for _, meta := range s.metas {
		if meta.ID.Workchain != key.Workchain || meta.ID.Shard != key.Shard || meta.GenUTime == 0 || meta.GenUTime > utime {
			continue
		}
		if best == nil || meta.GenUTime > best.GenUTime || meta.GenUTime == best.GenUTime && meta.ID.SeqNo > best.ID.SeqNo {
			best = meta
		}
	}
	if best == nil {
		return ton.BlockIDExt{}, false
	}
	return *cloneBlockID(best.ID), true
}

func (s *LiveStore) cachedBlockRoot(block ton.BlockIDExt) *cell.Cell {
	key := storage.BlockKey(block)

	s.mu.RLock()
	cached := s.blocks[key]
	s.mu.RUnlock()
	if cached == nil {
		return nil
	}
	return cached.root
}

func (s *LiveStore) cachedBlockData(block ton.BlockIDExt) ([]byte, bool) {
	key := storage.BlockKey(block)

	s.mu.RLock()
	cached := s.blocks[key]
	s.mu.RUnlock()
	if cached == nil {
		return nil, false
	}
	if len(cached.data) > 0 {
		return append([]byte(nil), cached.data...), true
	}
	return nil, false
}

func (s *LiveStore) putBlockLocked(key string, block *liveBlock) {
	if existing := s.blocks[key]; existing != nil {
		if existing.flushed && (len(existing.data) > 0 || len(block.data) == 0) {
			block.flushed = true
		}
		s.removeBlockOrderLocked(key, liveBlockKind(existing.id))
	}

	s.blocks[key] = block
	if liveBlockKind(block.id) == liveBlockMaster {
		s.masterOrder = append(s.masterOrder, key)
		return
	}
	s.shardOrder = append(s.shardOrder, key)
}

func (s *LiveStore) trimBlocksLocked(kind liveBlockCacheKind) {
	limit := s.shardCacheSize
	order := &s.shardOrder
	if kind == liveBlockMaster {
		limit = s.masterCacheSize
		order = &s.masterOrder
	}
	if limit < 0 {
		return
	}

	for flushedBlocksInOrder(s.blocks, *order) > limit {
		removed := false
		for i, key := range *order {
			cached := s.blocks[key]
			if cached == nil {
				*order = append((*order)[:i], (*order)[i+1:]...)
				removed = true
				break
			}
			if !cached.flushed {
				continue
			}
			if s.currentRefersToBlockLocked(cached.id) {
				continue
			}

			delete(s.blocks, key)
			*order = append((*order)[:i], (*order)[i+1:]...)
			removed = true
			break
		}
		if !removed {
			return
		}
	}
}

func (s *LiveStore) rebuildIndexesLocked() {
	s.metas = map[string]*storage.BlockMeta{}
	s.seqIndex = map[liveSeqKey]ton.BlockIDExt{}

	if s.current != nil {
		s.indexBlockStateLocked(s.current.Masterchain)
		for _, shard := range s.current.Shards {
			s.indexBlockStateLocked(shard)
		}
	}

	for _, block := range s.blocks {
		s.indexBlockLocked(block.id, block.meta)
	}
}

func (s *LiveStore) indexBlockStateLocked(state storage.BlockState) {
	if !isFullBlockID(&state.Block) {
		return
	}
	s.indexBlockLocked(state.Block, storage.BuildBlockMetaFromState(state))
}

func (s *LiveStore) indexBlockLocked(block ton.BlockIDExt, meta *storage.BlockMeta) {
	s.seqIndex[liveSeqKey{workchain: block.Workchain, shard: block.Shard, seqno: block.SeqNo}] = block
	if meta == nil {
		return
	}

	key := storage.BlockKey(block)
	s.metas[key] = storage.MergeBlockMeta(s.metas[key], meta)
}

func (s *LiveStore) removeBlockOrderLocked(key string, kind liveBlockCacheKind) {
	order := &s.shardOrder
	if kind == liveBlockMaster {
		order = &s.masterOrder
	}
	for i, existing := range *order {
		if existing == key {
			*order = append((*order)[:i], (*order)[i+1:]...)
			return
		}
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

func flushedBlocksInOrder(blocks map[string]*liveBlock, order []string) int {
	count := 0
	for _, key := range order {
		if cached := blocks[key]; cached != nil && cached.flushed {
			count++
		}
	}
	return count
}

func (s *LiveStore) storedCurrentBlocksReady(ctx context.Context, current *storage.CurrentState) error {
	if current == nil {
		return storage.ErrNotFound
	}
	if err := s.storedBlockReady(ctx, current.Masterchain); err != nil {
		return err
	}
	for _, shard := range current.Shards {
		if err := s.storedBlockReady(ctx, shard); err != nil {
			return err
		}
	}
	return nil
}

func (s *LiveStore) storedBlockReady(ctx context.Context, state storage.BlockState) error {
	block := state.Block
	if !isFullBlockID(&block) {
		return storage.ErrNotFound
	}
	if _, err := s.Store.BlockData(ctx, block); err != nil {
		return err
	}
	_, _, err := s.Store.LoadStateCellTree(ctx, block, state.StateRootHash)
	return err
}

func (s *LiveStore) currentRefersToBlockLocked(block ton.BlockIDExt) bool {
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

type liveSeqKey struct {
	workchain int32
	shard     int64
	seqno     uint32
}

func blockStateFromCurrent(current *storage.CurrentState, block ton.BlockIDExt) *storage.BlockState {
	if current == nil {
		return nil
	}
	if blockIDEqual(current.Masterchain.Block, block) {
		return storage.CloneBlockState(&current.Masterchain)
	}
	for _, shard := range current.Shards {
		if blockIDEqual(shard.Block, block) {
			return storage.CloneBlockState(&shard)
		}
	}
	return nil
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
