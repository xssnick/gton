package liveview

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultMasterBlockCache = 128
	DefaultShardBlockCache  = 1024
	// DefaultFragmentBuildWorkers bounds the shard-view prewarms that run off
	// the publishing goroutine. Masterchain blocks get their own slot on top of
	// this, and a publish that finds no free slot simply publishes the cold
	// view, so this is a latency knob and never a correctness one.
	DefaultFragmentBuildWorkers = 2
)

var (
	ErrWaitMasterchainTimeout = errors.New("timeout")
	ErrWaitMasterchainTooFar  = errors.New("too big masterchain block seqno")
)

type Options struct {
	MasterBlockCache   int
	ShardBlockCache    int
	NonFinalEnabled    bool
	NonFinalCache      int
	NonFinalCellLoader cell.LazyCellLoader
	LiveBlockCache     *storage.LiveBlockCache
	// FragmentBuildWorkers moves the per-block BlockView build (state root
	// proof, accounts-dict prewarm, and for masterchain blocks the config epoch
	// and global libraries) off the publishing goroutine. Publishing happens on
	// the block apply path, so on a live node this is what keeps the prewarm out
	// of the head latency. Zero keeps the build inline, which is what tests and
	// non-final publishing want.
	FragmentBuildWorkers int
	// OnFragmentBuildError reports a failed background build. The block stays
	// published and its view is rebuilt on the first query, so this is a
	// diagnostic, not an error path.
	OnFragmentBuildError func(ton.BlockIDExt, error)
}

type Store struct {
	backing Backing

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
	blockDataLoad      liveLoadGroup[liveBlockLookupKey, []byte]
	blockLoad          liveLoadGroup[liveBlockLookupKey, *liveBlockLoadResult]
	fragmentLoad       liveLoadGroup[liveBlockLookupKey, *BlockView]

	fragmentBuildSlots   chan struct{}
	fragmentMasterSlots  chan struct{}
	onFragmentBuildError func(ton.BlockIDExt, error)
}

type liveBlock struct {
	id              ton.BlockIDExt
	root            *cell.Cell
	meta            *storage.BlockMeta
	artifactFlushed bool
	stateFlushed    bool

	fragments *BlockView
	// currentCachesReleased records that the block left the current state while
	// its view was still being built, so a view installed afterwards must not
	// start filling the per-current caches again.
	currentCachesReleased bool
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
	fragments       *BlockView
	wantFragments   bool
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

func (o *liveBlockOrder) pushBack(key storage.BlockRootHash) {
	if elem := o.index[key]; elem != nil {
		o.items.MoveToBack(elem)
		return
	}
	o.index[key] = o.items.PushBack(key)
}

func (o *liveBlockOrder) remove(key storage.BlockRootHash) {
	elem := o.index[key]
	if elem == nil {
		return
	}
	o.items.Remove(elem)
	delete(o.index, key)
}

func New(store Backing, opts ...Options) *Store {
	cfg := Options{
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

	var fragmentBuildSlots, fragmentMasterSlots chan struct{}
	if cfg.FragmentBuildWorkers > 0 {
		fragmentBuildSlots = make(chan struct{}, cfg.FragmentBuildWorkers)
		fragmentMasterSlots = make(chan struct{}, 1)
	}

	live := &Store{
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

		fragmentBuildSlots:   fragmentBuildSlots,
		fragmentMasterSlots:  fragmentMasterSlots,
		onFragmentBuildError: cfg.OnFragmentBuildError,
	}
	live.loadInitialStoredCurrentState(context.Background())
	return live
}

func (s *Store) SetNonfinalCellLoader(loader cell.LazyCellLoader) {
	s.mu.Lock()
	s.nonFinalCellLoader = loader
	s.mu.Unlock()
}

func (s *Store) LazyCellLoader() cell.LazyCellLoader {
	return s.backing.LazyCellLoader()
}
