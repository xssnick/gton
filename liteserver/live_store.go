package liteserver

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	MasterBlockCache   int
	ShardBlockCache    int
	NonFinalEnabled    bool
	NonFinalCache      int
	NonFinalCellLoader cell.LazyCellLoader
	LiveBlockCache     *storage.LiveBlockCache
}

type LiveStore struct {
	backing LiveStoreBacking

	mu                 sync.RWMutex
	current            *storage.CurrentState
	pendingCurrent     *storage.CurrentState
	masterchainInfo    liveMasterchainInfo
	readyMasterSeqno   uint32
	notify             chan struct{}
	blocks             map[storage.BlockRootHash]*liveBlock
	metas              map[storage.BlockRootHash]*storage.BlockMeta
	states             map[liveBlockLookupKey]storage.BlockState
	seqIndex           map[liveSeqKey]ton.BlockIDExt
	ltIndex            map[liveHistoryKey][]liveLTIndexEntry
	unixIndex          map[liveHistoryKey][]liveUnixIndexEntry
	flushed            map[storage.BlockRootHash]liveBlockFlush
	liveBlockCache     *storage.LiveBlockCache
	masterOrder        liveBlockOrder
	shardOrder         liveBlockOrder
	masterEvictable    int
	shardEvictable     int
	masterCacheSize    int
	shardCacheSize     int
	nonFinalEnabled    bool
	nonFinalCache      int
	nonFinalPending    map[storage.BlockRootHash]liveNonfinalPending
	nonFinalOrder      []storage.BlockRootHash
	nonFinalOrderIndex map[storage.BlockRootHash]int
	nonFinalWaiting    map[storage.BlockRootHash]liveNonfinalWaiting
	nonFinalCellIndex  map[cell.Hash][]liveNonfinalCellIndexEntry
	nonFinalCellLoader cell.LazyCellLoader
	blockDataLoad      liveLoadGroup[storage.BlockRootHash]
	blockLoad          liveLoadGroup[storage.BlockRootHash]
	fragmentLoad       liveLoadGroup[storage.BlockRootHash]
}

type liveBlock struct {
	id              ton.BlockIDExt
	root            *cell.Cell
	meta            *storage.BlockMeta
	artifactFlushed bool
	stateFlushed    bool

	fragments *liveBlockFragments
}

type liveBlockFlush struct {
	artifact bool
	state    bool
}

type storedCurrentBlock struct {
	state storage.BlockState
	meta  *storage.BlockMeta
}

type livePreparedBlockArtifacts struct {
	block           ton.BlockIDExt
	root            *cell.Cell
	data            []byte
	meta            *storage.BlockMeta
	state           *storage.BlockState
	fragments       *liveBlockFragments
	proofs          []storage.LiveBlockProofArtifact
	artifactFlushed bool
	stateFlushed    bool
}

type liveMasterchainInfo struct {
	block         ton.BlockIDExt
	stateRootHash []byte
	lastUTime     uint32
	valid         bool
}

type liveBlockOrder struct {
	items *list.List
	index map[storage.BlockRootHash]*list.Element
}

func newLiveBlockOrder() liveBlockOrder {
	return liveBlockOrder{
		items: list.New(),
		index: map[storage.BlockRootHash]*list.Element{},
	}
}

func (o *liveBlockOrder) ensure() {
	if o.items == nil {
		o.items = list.New()
	}
	if o.index == nil {
		o.index = map[storage.BlockRootHash]*list.Element{}
	}
}

func (o *liveBlockOrder) pushBack(key storage.BlockRootHash) {
	o.ensure()
	if elem := o.index[key]; elem != nil {
		o.items.MoveToBack(elem)
		return
	}
	o.index[key] = o.items.PushBack(key)
}

func (o *liveBlockOrder) remove(key storage.BlockRootHash) bool {
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

func NewLiveStore(store LiveStoreBacking, opts ...LiveStoreOptions) *LiveStore {
	if store == nil {
		panic("liteserver live store backing is required")
	}

	cfg := LiveStoreOptions{
		MasterBlockCache: DefaultMasterBlockCache,
		ShardBlockCache:  DefaultShardBlockCache,
	}
	if len(opts) > 0 {
		cfg = opts[0]
	}
	if cfg.NonFinalCache == 0 {
		cfg.NonFinalCache = cfg.ShardBlockCache
	}
	if cfg.NonFinalCache == 0 {
		cfg.NonFinalCache = DefaultShardBlockCache
	}

	nonFinalCellLoader := store.LazyCellLoader()
	if cfg.NonFinalCellLoader != nil {
		nonFinalCellLoader = cfg.NonFinalCellLoader
	}
	liveBlockCache := cfg.LiveBlockCache
	if liveBlockCache == nil {
		liveBlockCache = storage.NewLiveBlockCache(storage.DefaultLiveBlockCacheMaxBlocks)
	}

	live := &LiveStore{
		backing:            store,
		notify:             make(chan struct{}),
		blocks:             map[storage.BlockRootHash]*liveBlock{},
		metas:              map[storage.BlockRootHash]*storage.BlockMeta{},
		states:             map[liveBlockLookupKey]storage.BlockState{},
		seqIndex:           map[liveSeqKey]ton.BlockIDExt{},
		ltIndex:            map[liveHistoryKey][]liveLTIndexEntry{},
		unixIndex:          map[liveHistoryKey][]liveUnixIndexEntry{},
		flushed:            map[storage.BlockRootHash]liveBlockFlush{},
		liveBlockCache:     liveBlockCache,
		masterOrder:        newLiveBlockOrder(),
		shardOrder:         newLiveBlockOrder(),
		masterCacheSize:    cfg.MasterBlockCache,
		shardCacheSize:     cfg.ShardBlockCache,
		nonFinalEnabled:    cfg.NonFinalEnabled,
		nonFinalCache:      cfg.NonFinalCache,
		nonFinalPending:    map[storage.BlockRootHash]liveNonfinalPending{},
		nonFinalOrderIndex: map[storage.BlockRootHash]int{},
		nonFinalWaiting:    map[storage.BlockRootHash]liveNonfinalWaiting{},
		nonFinalCellIndex:  map[cell.Hash][]liveNonfinalCellIndexEntry{},
		nonFinalCellLoader: nonFinalCellLoader,
	}
	live.loadInitialStoredCurrentState(context.Background())
	return live
}

func (s *LiveStore) SetNonfinalCellLoader(loader cell.LazyCellLoader) {
	s.mu.Lock()
	s.nonFinalCellLoader = loader
	s.mu.Unlock()
}

func (s *LiveStore) LazyCellLoader() cell.LazyCellLoader {
	return s.backing.LazyCellLoader()
}

func (s *LiveStore) loadInitialStoredCurrentState(ctx context.Context) {
	// Fresh and from-zero nodes do not have a stored current state yet. Missing
	// current is allowed; real storage errors should fail during initialization.
	if _, err := s.loadStoredCurrentState(ctx); err != nil && !errors.Is(err, storage.ErrNotFound) {
		panic(fmt.Sprintf("load stored current state: %v", err))
	}
}

func (s *LiveStore) loadStoredCurrentState(ctx context.Context) (*storage.CurrentState, error) {
	current, err := s.backing.CurrentState(ctx)
	if err != nil {
		return nil, err
	}

	loaded, blocks, err := s.prepareStoredCurrentState(ctx, current)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	prevSeqno := currentMasterchainSeqno(s.current)
	nextSeqno := currentMasterchainSeqno(loaded)
	if nextSeqno < prevSeqno {
		current = storage.CloneCurrentState(s.current)
		s.mu.Unlock()
		return current, nil
	}

	for _, block := range blocks {
		s.putBlockLocked(storage.BlockKey(block.state.Block), &liveBlock{
			id:              block.state.Block,
			meta:            block.meta,
			artifactFlushed: true,
			stateFlushed:    true,
		})
	}

	s.current = storage.CloneCurrentState(loaded)
	s.pendingCurrent = nil
	s.rememberCurrentBlockStatesLocked(s.current)
	s.updateMasterchainInfoLocked(s.current)
	if s.nonFinalEnabled {
		s.cleanupNonfinalPendingLocked()
	}
	ready := s.updateReadyMasterSeqnoLocked()
	if nextSeqno > prevSeqno || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	current = storage.CloneCurrentState(s.current)
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
	return current, nil
}

func (s *LiveStore) prepareStoredCurrentState(ctx context.Context, current *storage.CurrentState) (*storage.CurrentState, []storedCurrentBlock, error) {
	if current == nil {
		return nil, nil, storage.ErrNotFound
	}

	loaded := &storage.CurrentState{
		SyncedAt:         current.SyncedAt,
		ShardClientSeqno: current.ShardClientSeqno,
		Shards:           make(map[storage.ShardKey]storage.BlockState, len(current.Shards)),
	}
	blocks := make([]storedCurrentBlock, 0, 1+len(current.Shards))

	master, err := s.loadStoredCurrentBlock(ctx, current.Masterchain)
	if err != nil {
		return nil, nil, err
	}
	loaded.Masterchain = master.state
	blocks = append(blocks, master)

	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard, err := s.loadStoredCurrentBlock(ctx, current.Shards[key])
		if err != nil {
			return nil, nil, err
		}
		loaded.Shards[key] = shard.state
		blocks = append(blocks, shard)
	}
	return loaded, blocks, nil
}

func (s *LiveStore) loadStoredCurrentBlock(ctx context.Context, state storage.BlockState) (storedCurrentBlock, error) {
	block := state.Block
	if !isFullBlockID(&block) {
		return storedCurrentBlock{}, storage.ErrNotFound
	}
	if _, err := s.backing.BlockData(ctx, block); err != nil {
		return storedCurrentBlock{}, err
	}

	loaded, err := s.backing.BlockState(ctx, block)
	if err != nil {
		return storedCurrentBlock{}, err
	}
	if !blockIDEqual(loaded.Block, block) {
		return storedCurrentBlock{}, fmt.Errorf("stored current block state mismatch: got %s want %s", storage.FormatBlockRef(loaded.Block), storage.FormatBlockRef(block))
	}
	if len(state.StateRootHash) > 0 && !bytes.Equal(loaded.StateRootHash, state.StateRootHash) {
		return storedCurrentBlock{}, fmt.Errorf("stored current state root mismatch for %s", storage.FormatBlockRef(block))
	}

	meta, err := s.backing.BlockMeta(ctx, block)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return storedCurrentBlock{}, err
	}
	meta = storage.MergeBlockMeta(meta, storage.BuildBlockMetaFromState(*loaded))
	if meta != nil {
		meta.ID = block
	}

	return storedCurrentBlock{
		state: *storage.CloneBlockState(loaded),
		meta:  meta,
	}, nil
}

func (s *LiveStore) SetLiveCurrentState(current *storage.CurrentState) {
	next := storage.CloneCurrentState(current)

	s.mu.Lock()
	prevSeqno := currentMasterchainSeqno(s.current)
	s.pendingCurrent = next
	published := s.publishPendingCurrentLocked()
	if s.nonFinalEnabled {
		s.cleanupNonfinalPendingLocked()
	}
	nextSeqno := currentMasterchainSeqno(s.current)
	ready := s.updateReadyMasterSeqnoLocked()
	if published || ready || nextSeqno > prevSeqno || currentMasterchainSeqno(next) > prevSeqno {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
}

func (s *LiveStore) CurrentState(_ context.Context) (*storage.CurrentState, error) {
	s.mu.RLock()
	current := storage.CloneCurrentState(s.current)
	s.mu.RUnlock()
	if current != nil {
		return current, nil
	}
	return nil, storage.ErrNotFound
}

func (s *LiveStore) CurrentAccountBlocks(_ context.Context, workchain int32, account []byte) (CurrentAccountBlockIDs, error) {
	s.mu.RLock()
	blocks, err := currentAccountBlocksFromState(s.current, workchain, account)
	s.mu.RUnlock()
	return blocks, err
}

func (s *LiveStore) CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error) {
	s.mu.RLock()
	info := s.masterchainInfo
	s.mu.RUnlock()
	if info.valid {
		return info.block, info.stateRootHash, info.lastUTime, nil
	}

	current, err := s.CurrentState(ctx)
	if err != nil {
		return ton.BlockIDExt{}, nil, 0, err
	}

	block, stateRootHash, lastUTime, err := currentMasterchainInfo(current)
	if err != nil {
		return ton.BlockIDExt{}, nil, 0, err
	}
	if lastUTime == 0 {
		meta, err := s.BlockMeta(ctx, block)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return ton.BlockIDExt{}, nil, 0, err
		}
		if meta != nil {
			lastUTime = meta.GenUTime
		}
	}
	return block, stateRootHash, lastUTime, nil
}

func (s *LiveStore) PublishLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) error {
	prepared, err := prepareLiveBlockArtifacts(artifacts)
	if err != nil {
		return err
	}
	if s.liveBlockCache != nil {
		cacheArtifacts := storage.LiveBlockArtifacts{
			Block:           prepared.block,
			Root:            prepared.root,
			BlockData:       prepared.data,
			Meta:            prepared.meta,
			State:           prepared.state,
			ArtifactFlushed: prepared.artifactFlushed,
			StateFlushed:    prepared.stateFlushed,
		}
		cacheArtifacts.Proofs = prepared.proofs
		if err = s.liveBlockCache.PublishLiveBlockArtifacts(cacheArtifacts); err != nil {
			return err
		}
	}

	s.mu.Lock()
	published, ready := s.publishLiveBlockArtifactsPreparedLocked(prepared)
	if published || ready {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
	return nil
}

func prepareLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) (livePreparedBlockArtifacts, error) {
	block := artifacts.Block
	if !isFullBlockID(&block) {
		return livePreparedBlockArtifacts{}, storage.ErrNotFound
	}

	root := artifacts.Root
	if root == nil && len(artifacts.BlockData) > 0 {
		parsed, err := parseTrustedBlockBOC(block, artifacts.BlockData)
		if err != nil {
			return livePreparedBlockArtifacts{}, err
		}
		root = parsed
	}
	if root != nil {
		normalized, err := normalizeLiveBlockRoot(block, root)
		if err != nil {
			return livePreparedBlockArtifacts{}, err
		}
		root = normalized
	}

	var data []byte
	if len(artifacts.BlockData) > 0 {
		data = artifacts.BlockData
	}

	meta := artifacts.Meta.Clone()
	state := storage.CloneBlockState(artifacts.State)
	if state != nil {
		meta = storage.MergeBlockMeta(meta, storage.BuildBlockMetaFromState(*state))
	}
	if meta != nil {
		meta.ID = block
		if len(data) > 0 {
			meta.Mark(storage.BlockMetaHasBlockData)
		}
	}

	var proofs []storage.LiveBlockProofArtifact
	for _, proof := range artifacts.Proofs {
		if len(proof.Data) == 0 {
			continue
		}
		if meta != nil {
			meta.Mark(storage.BlockMetaFlagForProof(proof.Kind))
		}
		proofs = append(proofs, storage.LiveBlockProofArtifact{
			Kind: proof.Kind,
			Data: proof.Data,
		})
	}

	var fragments *liveBlockFragments
	if root != nil && state != nil && state.Cell != nil {
		// Fragment preparation is a liteserver hot-path optimization. Some publishers and
		// tests use hash-valid cache artifacts which are not full parseable block/state cells.
		built, err := buildLiveBlockFragments(block, root, state.Cell)
		if err == nil {
			fragments = built
			fragments.prewarmForLiteServer()
		}
	}

	return livePreparedBlockArtifacts{
		block:           block,
		root:            root,
		data:            data,
		meta:            meta,
		state:           state,
		fragments:       fragments,
		proofs:          proofs,
		artifactFlushed: artifacts.ArtifactFlushed,
		stateFlushed:    artifacts.StateFlushed,
	}, nil
}

func cloneLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
	cloned := storage.LiveBlockArtifacts{
		Block:           artifacts.Block,
		Root:            artifacts.Root,
		BlockData:       artifacts.BlockData,
		Meta:            artifacts.Meta.Clone(),
		State:           storage.CloneBlockState(artifacts.State),
		ArtifactFlushed: artifacts.ArtifactFlushed,
		StateFlushed:    artifacts.StateFlushed,
	}
	if len(artifacts.Proofs) > 0 {
		cloned.Proofs = make([]storage.LiveBlockProofArtifact, 0, len(artifacts.Proofs))
		for _, proof := range artifacts.Proofs {
			cloned.Proofs = append(cloned.Proofs, storage.LiveBlockProofArtifact{
				Kind: proof.Kind,
				Data: proof.Data,
			})
		}
	}
	return cloned
}

func (s *LiveStore) publishLiveBlockArtifactsPreparedLocked(prepared livePreparedBlockArtifacts) (bool, bool) {
	key := storage.BlockKey(prepared.block)
	flushed := s.flushed[key]
	if flushed.artifact || flushed.state {
		delete(s.flushed, key)
	}
	s.putBlockLocked(key, &liveBlock{
		id:              prepared.block,
		root:            prepared.root,
		meta:            prepared.meta,
		artifactFlushed: prepared.artifactFlushed || flushed.artifact,
		stateFlushed:    prepared.stateFlushed || flushed.state,
		fragments:       prepared.fragments,
	})
	if prepared.state != nil {
		s.rememberBlockStateLocked(*prepared.state)
	}
	published := s.publishPendingCurrentLocked()
	if s.current != nil && blockIDEqual(s.current.Masterchain.Block, prepared.block) {
		s.updateMasterchainInfoLocked(s.current)
	}
	if s.nonFinalEnabled {
		s.cleanupNonfinalPendingLocked()
	}
	s.trimBlocksLocked(liveBlockKind(prepared.block))
	ready := s.updateReadyMasterSeqnoLocked()
	return published, ready
}

func (s *LiveStore) MarkLiveBlockFlushed(block ton.BlockIDExt) {
	if s.liveBlockCache != nil {
		s.liveBlockCache.MarkBlockFlushed(block)
	}

	key := storage.BlockKey(block)

	s.mu.Lock()
	if cached := s.blocks[key]; cached != nil {
		evictableBefore := s.liveBlockEvictableLocked(cached)
		cached.artifactFlushed = true
		s.adjustLiveEvictableStateLocked(cached, evictableBefore)
		published := s.publishPendingCurrentLocked()
		if s.nonFinalEnabled {
			s.cleanupNonfinalPendingLocked()
		}
		s.trimBlocksLocked(liveBlockKind(block))
		ready := s.updateReadyMasterSeqnoLocked()
		if published || ready {
			close(s.notify)
			s.notify = make(chan struct{})
		}
	} else {
		flushed := s.flushed[key]
		flushed.artifact = true
		s.flushed[key] = flushed
	}
	s.mu.Unlock()
	if s.nonFinalEnabled {
		s.promoteNonfinalWaiting()
	}
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
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.backing.BlockState(ctx, block)
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

	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.backing.LoadStateCellTree(ctx, block, rootHash)
}

func (s *LiveStore) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	if cached := s.cachedBlockMeta(block); cached != nil {
		return cached, nil
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.backing.BlockMeta(ctx, block)
}

func (s *LiveStore) LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	if block, ok := s.cachedBlockBySeqNo(key, seqno); ok {
		return block, nil
	}
	if !s.backingSeqnoLookupAllowed(key, seqno) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := s.backing.LookupBlockBySeqNo(ctx, key, seqno)
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
	block, err := s.backing.LookupBlockByLT(ctx, key, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *LiveStore) LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	var best ton.BlockIDExt
	found := false
	for _, shard := range storage.AccountShardCandidates(workchain, account) {
		if block, ok := s.cachedBlockByLT(storage.BlockHistoryKey{Workchain: workchain, Shard: shard}, lt); ok {
			if !found || best.SeqNo > block.SeqNo {
				best = block
				found = true
			}
		}
	}
	if found {
		return best, nil
	}
	block, err := s.backing.LookupBlockByAccountLT(ctx, workchain, account, lt)
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
	block, err := s.backing.LookupBlockByUnixTime(ctx, key, utime)
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
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}

	loaded, err := s.loadStoredBlock(ctx, block)
	if err != nil {
		return nil, err
	}
	return loaded.root, nil
}

func (s *LiveStore) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	if s.liveBlockCache != nil {
		data, err := s.liveBlockCache.BlockData(ctx, block)
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}

	data, err := s.loadStoredBlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *LiveStore) BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	if s.liveBlockCache != nil {
		proof, err := s.liveBlockCache.BlockProof(ctx, kind, block)
		if err == nil {
			return proof, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.backing.BlockProof(ctx, kind, block)
}

func (s *LiveStore) BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveBlockFragments, error) {
	if fragments := s.cachedBlockFragments(block); fragments != nil {
		return fragments, nil
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

func (s *LiveStore) loadStoredBlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	value, err := s.blockDataLoad.do(ctx, storage.BlockKey(block), func() (any, error) {
		cached, err := s.cachedBlockData(ctx, block)
		if err == nil {
			return cached.Data, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		data, err := s.backing.BlockData(ctx, block)
		if err != nil {
			return nil, err
		}

		if s.liveBlockCache != nil {
			err = s.liveBlockCache.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
				Block:           block,
				BlockData:       data,
				ArtifactFlushed: true,
			})
			if err != nil {
				return nil, err
			}
		}
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	data, ok := value.([]byte)
	if !ok {
		return nil, errors.New("invalid live block data load result")
	}
	return data, nil
}

func (s *LiveStore) loadStoredBlock(ctx context.Context, block ton.BlockIDExt) (*liveBlockLoadResult, error) {
	value, err := s.blockLoad.do(ctx, storage.BlockKey(block), func() (any, error) {
		cached, err := s.cachedBlockData(ctx, block)
		if err == nil {
			data := cached.Data
			root := s.cachedBlockRoot(block)
			if root == nil {
				parsed, err := parseTrustedBlockBOC(block, data)
				if err != nil {
					return nil, err
				}
				root = parsed
				if err = s.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
					Block:           block,
					Root:            root,
					BlockData:       data,
					ArtifactFlushed: cached.ArtifactFlushed,
				}); err != nil {
					return nil, err
				}
			}
			return &liveBlockLoadResult{root: root, data: data}, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		data, err := s.backing.BlockData(ctx, block)
		if err != nil {
			return nil, err
		}

		root, err := parseTrustedBlockBOC(block, data)
		if err != nil {
			return nil, err
		}
		if err = s.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:           block,
			Root:            root,
			BlockData:       data,
			ArtifactFlushed: true,
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
	return s.backing.ZeroState(ctx, block)
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
		currentSeqno, readySeqno, notify := s.waitSnapshot()
		if readySeqno >= seqno {
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

	s.current = s.pendingCurrent
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
	stateRootHash := current.Masterchain.StateRootHash
	lastUTime := uint32(0)
	if current.Masterchain.Parsed != nil {
		lastUTime = current.Masterchain.Parsed.GenUTime
	}
	if meta := s.metas[storage.BlockKey(block)]; meta != nil {
		if len(stateRootHash) == 0 {
			stateRootHash = meta.StateRootHash
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
	if currentHasBlockState(s.current, block) {
		return true
	}
	if s.liveBlockCache != nil {
		if _, err := s.liveBlockCache.BlockData(context.Background(), block); err == nil {
			return true
		}
	}

	cached := s.blocks[storage.BlockKey(block)]
	return cached != nil && cached.artifactFlushed
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

func (s *LiveStore) waitSnapshot() (uint32, uint32, <-chan struct{}) {
	s.mu.RLock()
	currentSeqno := currentMasterchainSeqno(s.current)
	readySeqno := s.readyMasterSeqno
	notify := s.notify
	s.mu.RUnlock()
	return currentSeqno, readySeqno, notify
}

func currentMasterchainSeqno(current *storage.CurrentState) uint32 {
	if current == nil {
		return 0
	}
	return current.Masterchain.Block.SeqNo
}

func currentAccountBlocksFromState(current *storage.CurrentState, workchain int32, account []byte) (CurrentAccountBlockIDs, error) {
	if current == nil {
		return CurrentAccountBlockIDs{}, storage.ErrNotFound
	}

	master := current.Masterchain.Block
	if workchain == masterchainID {
		return CurrentAccountBlockIDs{Master: master, Account: master}, nil
	}
	if len(account) < 8 {
		return CurrentAccountBlockIDs{}, storage.ErrNotFound
	}

	prefix := binary.BigEndian.Uint64(account[:8])
	for length := 60; length >= 1; length-- {
		shardID := accountShardPrefix(prefix, length)
		shard, ok := current.Shards[storage.ShardKey{Workchain: workchain, Shard: shardID}]
		if ok {
			return CurrentAccountBlockIDs{Master: master, Account: shard.Block}, nil
		}
	}

	shard, ok := current.Shards[storage.ShardKey{Workchain: workchain, Shard: masterchainShard}]
	if ok {
		return CurrentAccountBlockIDs{Master: master, Account: shard.Block}, nil
	}
	return CurrentAccountBlockIDs{}, storage.ErrNotFound
}

func accountShardPrefix(prefix uint64, length int) int64 {
	x := uint64(1) << (63 - uint(length))
	return int64((prefix & ^(x - 1)) | x)
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
	if geIdx < len(entries) {
		return *cloneBlockID(entries[geIdx].block), true
	}
	return ton.BlockIDExt{}, false
}

func (s *LiveStore) cachedBlockByUnixTime(key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.unixIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].genUTime >= utime
	})
	if idx >= len(entries) {
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

func (s *LiveStore) cachedBlockData(ctx context.Context, block ton.BlockIDExt) (storage.CachedBlockData, error) {
	if s.liveBlockCache != nil {
		return s.liveBlockCache.CachedBlockData(ctx, block)
	}

	return storage.CachedBlockData{}, storage.ErrNotFound
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

func (s *LiveStore) putBlockLocked(key storage.BlockRootHash, block *liveBlock) {
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
	if (cached.root != nil || cached.meta != nil) && !cached.artifactFlushed {
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

func (s *LiveStore) removeBlockOrderLocked(key storage.BlockRootHash, kind liveBlockCacheKind) {
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

func (s *LiveStore) deleteLiveBlockLocked(key storage.BlockRootHash, cached *liveBlock, kind liveBlockCacheKind) {
	delete(s.blocks, key)
	s.removeBlockOrderLocked(key, kind)
	if s.liveBlockEvictableLocked(cached) {
		s.adjustLiveEvictableLocked(kind, -1)
	}
	s.removeBlockStateLocked(cached.id)
}

func (s *LiveStore) protectedLiveBlocksLocked() map[storage.BlockRootHash]struct{} {
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
			if shardKey.Workchain != key.Workchain || !shardIntersects(shardKey, storage.ShardKey(key)) {
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
