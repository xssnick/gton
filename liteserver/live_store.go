package liteserver

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"

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
	masterchainInfo  liveMasterchainInfo
	readyMasterSeqno uint32
	notify           chan struct{}
	blocks           map[string]*liveBlock
	metas            map[string]*storage.BlockMeta
	states           map[liveBlockLookupKey]storage.BlockState
	seqIndex         map[liveSeqKey]ton.BlockIDExt
	ltIndex          map[liveHistoryKey][]liveLTIndexEntry
	unixIndex        map[liveHistoryKey][]liveUnixIndexEntry
	flushed          map[string]liveBlockFlush
	masterOrder      liveBlockOrder
	shardOrder       liveBlockOrder
	masterEvictable  int
	shardEvictable   int
	masterCacheSize  int
	shardCacheSize   int
	blockLoad        liveBlockLoadGroup
	fragmentLoad     liveBlockLoadGroup
}

type liveBlock struct {
	id     ton.BlockIDExt
	root   *cell.Cell
	data   []byte
	meta   *storage.BlockMeta
	proofs map[storage.ServedProofKind][]byte

	blockDataFlushed bool
	stateFlushed     bool
	proofsFlushed    bool

	fragments *liveBlockFragments
}

type liveBlockFlush struct {
	blockData bool
	state     bool
	proofs    bool
}

type liveMasterchainInfo struct {
	block         ton.BlockIDExt
	stateRootHash []byte
	lastUTime     uint32
	valid         bool
}

type liveBlockOrder struct {
	items *list.List
	index map[string]*list.Element
}

func newLiveBlockOrder() liveBlockOrder {
	return liveBlockOrder{
		items: list.New(),
		index: map[string]*list.Element{},
	}
}

func (o *liveBlockOrder) ensure() {
	if o.items == nil {
		o.items = list.New()
	}
	if o.index == nil {
		o.index = map[string]*list.Element{}
	}
}

func (o *liveBlockOrder) pushBack(key string) {
	o.ensure()
	if elem := o.index[key]; elem != nil {
		o.items.MoveToBack(elem)
		return
	}
	o.index[key] = o.items.PushBack(key)
}

func (o *liveBlockOrder) remove(key string) bool {
	o.ensure()
	elem := o.index[key]
	if elem == nil {
		return false
	}
	o.items.Remove(elem)
	delete(o.index, key)
	return true
}

func (o *liveBlockOrder) front() *list.Element {
	o.ensure()
	return o.items.Front()
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
		states:          map[liveBlockLookupKey]storage.BlockState{},
		seqIndex:        map[liveSeqKey]ton.BlockIDExt{},
		ltIndex:         map[liveHistoryKey][]liveLTIndexEntry{},
		unixIndex:       map[liveHistoryKey][]liveUnixIndexEntry{},
		flushed:         map[string]liveBlockFlush{},
		masterOrder:     newLiveBlockOrder(),
		shardOrder:      newLiveBlockOrder(),
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

func (s *LiveStore) CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error) {
	s.mu.RLock()
	info := s.masterchainInfo
	s.mu.RUnlock()
	if info.valid {
		return info.block, info.stateRootHash, info.lastUTime, nil
	}
	if s.Store == nil {
		return ton.BlockIDExt{}, nil, 0, storage.ErrNotFound
	}

	current, err := s.CurrentState(ctx)
	if err != nil {
		return ton.BlockIDExt{}, nil, 0, err
	}

	block, stateRootHash, lastUTime, err := currentMasterchainInfo(ctx, s, current)
	if err != nil {
		return ton.BlockIDExt{}, nil, 0, err
	}
	return block, stateRootHash, lastUTime, nil
}

func (s *LiveStore) publishLiveBlockData(block ton.BlockIDExt, root *cell.Cell, data []byte, flushed bool) error {
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

	return s.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:            block,
		Root:             root,
		BlockData:        data,
		Meta:             meta,
		BlockDataFlushed: flushed,
	})
}

func (s *LiveStore) PublishLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) error {
	block := artifacts.Block
	if !isFullBlockID(&block) {
		return storage.ErrNotFound
	}

	root := artifacts.Root
	if root == nil && len(artifacts.BlockData) > 0 {
		parsed, err := parseTrustedBlockBOC(block, artifacts.BlockData)
		if err != nil {
			return err
		}
		root = parsed
	}
	if root != nil {
		normalized, err := normalizeLiveBlockRoot(block, root)
		if err != nil {
			return err
		}
		root = normalized
	}

	var data []byte
	if len(artifacts.BlockData) > 0 {
		data = append([]byte(nil), artifacts.BlockData...)
	}

	meta := artifacts.Meta.Clone()
	if artifacts.State != nil {
		meta = storage.MergeBlockMeta(meta, storage.BuildBlockMetaFromState(*artifacts.State))
	}
	if meta != nil {
		meta.ID = block
		if len(data) > 0 {
			meta.Mark(storage.BlockMetaHasBlockData)
		}
		for _, proof := range artifacts.Proofs {
			if len(proof.Data) > 0 {
				meta.Mark(storage.BlockMetaFlagForProof(proof.Kind))
			}
		}
	}

	proofs := make(map[storage.ServedProofKind][]byte, len(artifacts.Proofs))
	for _, proof := range artifacts.Proofs {
		if len(proof.Data) == 0 {
			continue
		}
		proofs[proof.Kind] = append([]byte(nil), proof.Data...)
	}
	if len(proofs) == 0 {
		proofs = nil
	}

	key := storage.BlockKey(block)

	s.mu.Lock()
	flushed := s.flushed[key]
	if flushed.blockData || flushed.state || flushed.proofs {
		delete(s.flushed, key)
	}
	s.putBlockLocked(key, &liveBlock{
		id:               block,
		root:             root,
		data:             data,
		meta:             meta,
		proofs:           proofs,
		blockDataFlushed: artifacts.BlockDataFlushed || flushed.blockData,
		stateFlushed:     artifacts.StateFlushed || flushed.state,
		proofsFlushed:    artifacts.ProofsFlushed || flushed.proofs,
	})
	if artifacts.State != nil {
		s.rememberBlockStateLocked(*artifacts.State)
	}
	published := s.publishPendingCurrentLocked()
	s.trimBlocksLocked(liveBlockKind(block))
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
		s.markLiveBlockDataFlushedLocked(cached)
		published := s.publishPendingCurrentLocked()
		s.trimBlocksLocked(liveBlockKind(block))
		ready := s.updateReadyMasterSeqnoLocked()
		if published || ready {
			close(s.notify)
			s.notify = make(chan struct{})
		}
	} else {
		flushed := s.flushed[key]
		flushed.blockData = true
		flushed.proofs = true
		s.flushed[key] = flushed
	}
	s.mu.Unlock()
}

func (s *LiveStore) MarkLiveBlockStatesFlushed(blocks []ton.BlockIDExt) {
	if len(blocks) == 0 {
		return
	}

	s.mu.Lock()
	for _, block := range blocks {
		s.markLiveBlockStateFlushedLocked(block, false)
	}
	s.trimBlocksLocked(liveBlockMaster)
	s.trimBlocksLocked(liveBlockShard)
	s.mu.Unlock()
}

func (s *LiveStore) MarkLiveCurrentStateFlushed(current *storage.CurrentState) {
	if current == nil {
		return
	}

	s.mu.Lock()
	s.markLiveBlockStateFlushedLocked(current.Masterchain.Block, true)
	for _, shard := range current.Shards {
		s.markLiveBlockStateFlushedLocked(shard.Block, true)
	}
	s.trimBlocksLocked(liveBlockMaster)
	s.trimBlocksLocked(liveBlockShard)
	s.mu.Unlock()
}

func (s *LiveStore) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if state := s.cachedBlockState(block); state != nil {
		return state, nil
	}
	if s.Store == nil || !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.Store.BlockState(ctx, block)
}

func (s *LiveStore) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error) {
	if state := s.cachedBlockState(block); state != nil && state.Cell != nil {
		if len(rootHash) > 0 && !bytes.Equal(state.StateRootHash, rootHash) {
			return nil, storage.ErrNotFound
		}

		hash := state.Cell.HashKey(0)
		if !bytes.Equal(hash[:], state.StateRootHash) {
			return nil, storage.ErrNotFound
		}
		return state.Cell, nil
	}

	if s.Store == nil || !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.Store.LoadStateCellTree(ctx, block, rootHash)
}

func (s *LiveStore) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	if cached := s.cachedBlockMeta(block); cached != nil {
		return cached, nil
	}
	if s.Store == nil || !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.Store.BlockMeta(ctx, block)
}

func (s *LiveStore) LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	if block, ok := s.cachedBlockBySeqNo(key, seqno); ok {
		return block, nil
	}
	if s.Store == nil || !s.backingSeqnoLookupAllowed(key, seqno) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := s.Store.LookupBlockBySeqNo(ctx, key, seqno)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *LiveStore) LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	if block, ok := s.cachedBlockByLT(key, lt); ok {
		return block, nil
	}
	if s.Store == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := s.Store.LookupBlockByLT(ctx, key, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *LiveStore) LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	for _, shard := range storage.AccountShardCandidates(workchain, account) {
		if block, ok := s.cachedBlockCoveringLT(storage.BlockHistoryKey{Workchain: workchain, Shard: shard}, lt); ok {
			return block, nil
		}
	}
	if s.Store == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := s.Store.LookupBlockByAccountLT(ctx, workchain, account, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *LiveStore) LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	if block, ok := s.cachedBlockByUnixTime(key, utime); ok {
		return block, nil
	}
	if s.Store == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := s.Store.LookupBlockByUnixTime(ctx, key, utime)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *LiveStore) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if root := s.cachedBlockRoot(block); root != nil {
		return root, nil
	}
	if s.Store == nil || !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}

	loaded, err := s.loadStoredBlock(ctx, block)
	if err != nil {
		return nil, err
	}
	return loaded.root, nil
}

func (s *LiveStore) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	if data, ok := s.cachedBlockData(block); ok {
		return data, nil
	}
	if s.Store == nil || !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}

	loaded, err := s.loadStoredBlock(ctx, block)
	if err != nil {
		return nil, err
	}
	return loaded.data, nil
}

func (s *LiveStore) BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	if proof, ok := s.cachedBlockProof(kind, block); ok {
		return proof, nil
	}
	if s.Store == nil || !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.Store.BlockProof(ctx, kind, block)
}

func (s *LiveStore) BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveBlockFragments, error) {
	if fragments := s.cachedBlockFragments(block); fragments != nil {
		return fragments, nil
	}
	if s.Store == nil {
		return nil, storage.ErrNotFound
	}

	value, err := s.fragmentLoad.do(ctx, storage.BlockKey(block), func() (any, error) {
		if fragments := s.cachedBlockFragments(block); fragments != nil {
			return fragments, nil
		}

		blockRoot, err := s.BlockRoot(ctx, block)
		if err != nil {
			return nil, err
		}

		stateRootHash, err := stateRootHashFromBlock(block, blockRoot)
		if err != nil {
			return nil, err
		}

		stateRoot, err := s.LoadStateCellTree(ctx, block, stateRootHash)
		if err != nil {
			return nil, err
		}

		fragments, err := buildLiveBlockFragments(block, blockRoot, stateRoot)
		if err != nil {
			return nil, err
		}
		return s.rememberBlockFragments(block, fragments), nil
	})
	if err != nil {
		return nil, err
	}
	fragments, ok := value.(*liveBlockFragments)
	if !ok {
		return nil, errors.New("invalid live block fragments")
	}
	return fragments, nil
}

func (s *LiveStore) loadStoredBlock(ctx context.Context, block ton.BlockIDExt) (*liveBlockLoadResult, error) {
	value, err := s.blockLoad.do(ctx, storage.BlockKey(block), func() (any, error) {
		if data, ok := s.cachedBlockData(block); ok {
			root := s.cachedBlockRoot(block)
			if root == nil {
				parsed, err := parseTrustedBlockBOC(block, data)
				if err != nil {
					return nil, err
				}
				root = parsed
			}
			return &liveBlockLoadResult{root: root, data: data}, nil
		}

		data, err := s.Store.BlockData(ctx, block)
		if err != nil {
			return nil, err
		}

		root, err := parseTrustedBlockBOC(block, data)
		if err != nil {
			return nil, err
		}
		if err = s.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:            block,
			Root:             root,
			BlockData:        data,
			BlockDataFlushed: true,
		}); err != nil {
			return nil, err
		}
		return &liveBlockLoadResult{root: root, data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	loaded, ok := value.(*liveBlockLoadResult)
	if !ok {
		return nil, errors.New("invalid live block load result")
	}
	return loaded, nil
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
	s.rememberCurrentBlockStatesLocked(s.current)
	s.updateMasterchainInfoLocked(s.current)
	return nextSeqno > prevSeqno
}

func (s *LiveStore) updateMasterchainInfoLocked(current *storage.CurrentState) {
	s.masterchainInfo = liveMasterchainInfo{}
	if current == nil {
		return
	}

	block := current.Masterchain.Block
	stateRootHash := bytes.Clone(current.Masterchain.StateRootHash)
	lastUTime := uint32(0)
	if current.Masterchain.Parsed != nil {
		lastUTime = current.Masterchain.Parsed.GenUTime
	}

	if meta := s.metas[storage.BlockKey(block)]; meta != nil {
		if len(stateRootHash) == 0 {
			stateRootHash = bytes.Clone(meta.StateRootHash)
		}
		if lastUTime == 0 {
			lastUTime = meta.GenUTime
		}
	}
	if len(stateRootHash) != 32 {
		return
	}

	s.masterchainInfo = liveMasterchainInfo{
		block:         *cloneBlockID(block),
		stateRootHash: stateRootHash,
		lastUTime:     lastUTime,
		valid:         true,
	}
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
	return cached != nil && liveBlockHasData(cached)
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
	return len(block.data) > 0
}

func mergeLiveBlockProofs(base map[storage.ServedProofKind][]byte, next map[storage.ServedProofKind][]byte) map[storage.ServedProofKind][]byte {
	if len(base) == 0 && len(next) == 0 {
		return nil
	}

	merged := make(map[storage.ServedProofKind][]byte, len(base)+len(next))
	for kind, data := range base {
		merged[kind] = data
	}
	for kind, data := range next {
		merged[kind] = data
	}
	return merged
}

func liveProofsCovered(base map[storage.ServedProofKind][]byte, next map[storage.ServedProofKind][]byte) bool {
	for kind, data := range next {
		if !bytes.Equal(base[kind], data) {
			return false
		}
	}
	return true
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
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return nil
	}

	s.mu.RLock()
	state, ok := s.states[key]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return storage.CloneBlockState(&state)
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

	entries := s.ltIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	geIdx := sort.Search(len(entries), func(i int) bool {
		return entries[i].endLT >= lt
	})
	if geIdx < len(entries) && entries[geIdx].startLT <= lt && lt <= entries[geIdx].endLT {
		return *cloneBlockID(entries[geIdx].block), true
	}

	floorIdx := sort.Search(len(entries), func(i int) bool {
		return entries[i].endLT > lt
	}) - 1
	if floorIdx >= 0 {
		return *cloneBlockID(entries[floorIdx].block), true
	}
	return ton.BlockIDExt{}, false
}

func (s *LiveStore) cachedBlockCoveringLT(key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.ltIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	geIdx := sort.Search(len(entries), func(i int) bool {
		return entries[i].endLT >= lt
	})
	if geIdx < len(entries) && entries[geIdx].startLT < lt && lt < entries[geIdx].endLT {
		return *cloneBlockID(entries[geIdx].block), true
	}
	return ton.BlockIDExt{}, false
}

func (s *LiveStore) cachedBlockByUnixTime(key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.unixIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].genUTime > utime
	}) - 1
	if idx < 0 {
		return ton.BlockIDExt{}, false
	}
	return *cloneBlockID(entries[idx].block), true
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
		return cached.data, true
	}
	return nil, false
}

func (s *LiveStore) cachedBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, bool) {
	key := storage.BlockKey(block)

	s.mu.RLock()
	cached := s.blocks[key]
	s.mu.RUnlock()
	if cached == nil {
		return nil, false
	}
	proof := cached.proofs[kind]
	if len(proof) == 0 {
		return nil, false
	}
	return proof, true
}

func (s *LiveStore) cachedBlockFragments(block ton.BlockIDExt) *liveBlockFragments {
	key := storage.BlockKey(block)

	s.mu.RLock()
	cached := s.blocks[key]
	s.mu.RUnlock()
	if cached == nil {
		return nil
	}
	return cached.fragments
}

func (s *LiveStore) rememberBlockFragments(block ton.BlockIDExt, fragments *liveBlockFragments) *liveBlockFragments {
	key := storage.BlockKey(block)

	s.mu.Lock()
	cached := s.blocks[key]
	if cached == nil {
		s.mu.Unlock()
		return fragments
	}
	if cached.fragments != nil {
		fragments = cached.fragments
	} else {
		cached.fragments = fragments
	}
	s.mu.Unlock()
	return fragments
}

func (s *LiveStore) putBlockLocked(key string, block *liveBlock) {
	if existing := s.blocks[key]; existing != nil {
		existingEvictable := s.liveBlockEvictableLocked(existing)
		incomingProofs := block.proofs
		if block.root == nil {
			block.root = existing.root
		}
		if len(block.data) == 0 {
			block.data = existing.data
		}
		if existing.meta != nil || block.meta != nil {
			block.meta = storage.MergeBlockMeta(existing.meta, block.meta)
		}
		block.proofs = mergeLiveBlockProofs(existing.proofs, block.proofs)
		block.blockDataFlushed = block.blockDataFlushed || existing.blockDataFlushed
		block.stateFlushed = block.stateFlushed || existing.stateFlushed
		block.proofsFlushed = block.proofsFlushed || existing.proofsFlushed && liveProofsCovered(existing.proofs, incomingProofs)
		if block.fragments == nil {
			block.fragments = existing.fragments
		}
		existingKind := liveBlockKind(existing.id)
		s.removeBlockOrderLocked(key, existingKind)
		if existingEvictable {
			s.adjustLiveEvictableLocked(existingKind, -1)
		}
	}

	s.blocks[key] = block
	s.refreshBlockIndexLocked(block.id)
	kind := liveBlockKind(block.id)
	s.blockOrderLocked(kind).pushBack(key)
	if s.liveBlockEvictableLocked(block) {
		s.adjustLiveEvictableLocked(kind, 1)
	}
}

func (s *LiveStore) trimBlocksLocked(kind liveBlockCacheKind) {
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
	for elem := order.front(); elem != nil && evictable > limit; {
		next := elem.Next()
		key := elem.Value.(string)
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

func (s *LiveStore) markLiveBlockDataFlushedLocked(cached *liveBlock) {
	evictableBefore := s.liveBlockEvictableLocked(cached)
	cached.blockDataFlushed = true
	cached.proofsFlushed = true
	s.adjustLiveEvictableStateLocked(cached, evictableBefore)
}

func (s *LiveStore) markLiveBlockStateFlushedLocked(block ton.BlockIDExt, rememberMissing bool) {
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

func (s *LiveStore) adjustLiveEvictableStateLocked(cached *liveBlock, evictableBefore bool) {
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

func (s *LiveStore) liveBlockEvictableLocked(cached *liveBlock) bool {
	if cached == nil {
		return false
	}
	if (cached.root != nil || len(cached.data) > 0) && !cached.blockDataFlushed {
		return false
	}
	if len(cached.proofs) > 0 && !cached.proofsFlushed {
		return false
	}
	if s.liveBlockHasStateLocked(cached.id) && !cached.stateFlushed {
		return false
	}
	return true
}

func (s *LiveStore) liveBlockHasStateLocked(block ton.BlockIDExt) bool {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return false
	}
	_, ok = s.states[key]
	return ok
}

func (s *LiveStore) rememberBlockStateLocked(state storage.BlockState) {
	if !isFullBlockID(&state.Block) {
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

func (s *LiveStore) removeBlockStateLocked(block ton.BlockIDExt) {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if ok {
		delete(s.states, key)
	}
	s.refreshBlockIndexLocked(block)
}

func (s *LiveStore) rememberCurrentBlockStatesLocked(current *storage.CurrentState) {
	if current == nil {
		return
	}

	s.rememberBlockStateLocked(current.Masterchain)
	for _, shard := range current.Shards {
		s.rememberBlockStateLocked(shard)
	}
}

func (s *LiveStore) refreshBlockIndexLocked(block ton.BlockIDExt) {
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
			meta = storage.MergeBlockMeta(meta, storage.BuildBlockMetaFromState(cached))
			indexed = true
		}
	}
	if cached := s.blocks[key]; cached != nil {
		if cached.meta != nil {
			meta = storage.MergeBlockMeta(meta, cached.meta)
			indexed = true
		} else if cached.root != nil || len(cached.data) > 0 {
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

func (s *LiveStore) addMetaHistoryIndexLocked(meta *storage.BlockMeta) {
	key := liveHistoryKey{workchain: meta.ID.Workchain, shard: meta.ID.Shard}
	if meta.EndLT != 0 {
		entry := liveLTIndexEntry{
			startLT: meta.StartLT,
			endLT:   meta.EndLT,
			seqno:   meta.ID.SeqNo,
			block:   *cloneBlockID(meta.ID),
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
			block:    *cloneBlockID(meta.ID),
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

func (s *LiveStore) removeMetaHistoryIndexLocked(meta *storage.BlockMeta) {
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

func (s *LiveStore) removeBlockOrderLocked(key string, kind liveBlockCacheKind) {
	s.blockOrderLocked(kind).remove(key)
}

func (s *LiveStore) blockOrderLocked(kind liveBlockCacheKind) *liveBlockOrder {
	order := &s.shardOrder
	if kind == liveBlockMaster {
		order = &s.masterOrder
	}
	return order
}

func (s *LiveStore) liveEvictableCountLocked(kind liveBlockCacheKind) int {
	if kind == liveBlockMaster {
		return s.masterEvictable
	}
	return s.shardEvictable
}

func (s *LiveStore) adjustLiveEvictableLocked(kind liveBlockCacheKind, delta int) {
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

func (s *LiveStore) deleteLiveBlockLocked(key string, cached *liveBlock, kind liveBlockCacheKind) {
	delete(s.blocks, key)
	s.removeBlockOrderLocked(key, kind)
	if s.liveBlockEvictableLocked(cached) {
		s.adjustLiveEvictableLocked(kind, -1)
	}
	s.removeBlockStateLocked(cached.id)
}

func (s *LiveStore) protectedLiveBlocksLocked() map[string]struct{} {
	protected := make(map[string]struct{})
	addCurrentLiveBlocks(protected, s.current)
	addCurrentLiveBlocks(protected, s.pendingCurrent)
	return protected
}

func addCurrentLiveBlocks(protected map[string]struct{}, current *storage.CurrentState) {
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
	_, err := s.Store.LoadStateCellTree(ctx, block, state.StateRootHash)
	return err
}

func (s *LiveStore) backingBlockAllowed(block ton.BlockIDExt) bool {
	if !isFullBlockID(&block) {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backingBlockAllowedLocked(block)
}

func (s *LiveStore) backingBlockAllowedLocked(block ton.BlockIDExt) bool {
	key := storage.BlockHistoryKey{Workchain: block.Workchain, Shard: block.Shard}
	return s.backingSeqnoLookupAllowedLocked(key, block.SeqNo)
}

func (s *LiveStore) backingSeqnoLookupAllowed(key storage.BlockHistoryKey, seqno uint32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backingSeqnoLookupAllowedLocked(key, seqno)
}

func (s *LiveStore) backingSeqnoLookupAllowedLocked(key storage.BlockHistoryKey, seqno uint32) bool {
	if s.current == nil && s.pendingCurrent == nil {
		return true
	}

	if key.Workchain == masterchainID && key.Shard == masterchainShard {
		maxSeqno, ok := maxKnownMasterSeqno(s.current, s.pendingCurrent)
		return !ok || seqno <= maxSeqno
	}

	maxSeqno, ok := maxKnownShardSeqno(key, s.current, s.pendingCurrent)
	return !ok || seqno <= maxSeqno
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

func maxKnownMasterSeqno(states ...*storage.CurrentState) (uint32, bool) {
	var max uint32
	var ok bool
	for _, current := range states {
		if current == nil {
			continue
		}
		seqno := current.Masterchain.Block.SeqNo
		if !ok || seqno > max {
			max = seqno
			ok = true
		}
	}
	return max, ok
}

func maxKnownShardSeqno(key storage.BlockHistoryKey, states ...*storage.CurrentState) (uint32, bool) {
	var max uint32
	var ok bool
	for _, current := range states {
		if current == nil {
			continue
		}
		for _, shard := range current.Shards {
			shardKey := storage.ShardKeyFromBlock(shard.Block)
			if shardKey.Workchain != key.Workchain || !shardIntersects(shardKey, storage.ShardKey{Workchain: key.Workchain, Shard: key.Shard}) {
				continue
			}
			if !ok || shard.Block.SeqNo > max {
				max = shard.Block.SeqNo
				ok = true
			}
		}
	}
	return max, ok
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
