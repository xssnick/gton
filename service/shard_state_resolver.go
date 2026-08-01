package service

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	shardStateResolverCacheLimit = 2048
	// Leave one-eighth insertion headroom so the ordered prune is amortized
	// while every completed insertion stays bounded.
	shardStateResolverCacheRetain = shardStateResolverCacheLimit * 7 / 8
)

type shardStateApplyFunc func(context.Context, ton.BlockIDExt, []*storage.BlockState, PreparedBlock) (*storage.BlockState, error)
type shardStateAfterApplyFunc func(context.Context, *storage.BlockState, PreparedBlock, time.Duration) error

type shardApplyMasterContextKey struct{}

// contextWithShardApplyMaster pins the masterchain state whose shard
// description pulled these blocks in. The apply callbacks stamp
// BlockState.MasterchainRef / BlockMeta.MasterchainRefSeqno from it (cppnode's
// BlockHandle::masterchain_ref_block), so it must travel with the resolution
// instead of being read from the commit stage's current master: the resolution
// can run ahead of the commit, on another goroutine.
func contextWithShardApplyMaster(ctx context.Context, master *storage.BlockState) context.Context {
	if master == nil {
		return ctx
	}
	return context.WithValue(ctx, shardApplyMasterContextKey{}, master)
}

func shardApplyMasterFromContext(ctx context.Context) (*storage.BlockState, error) {
	master, _ := ctx.Value(shardApplyMasterContextKey{}).(*storage.BlockState)
	if master == nil {
		return nil, errShardApplyMasterMissing
	}
	return master, nil
}

type shardStateResolverConfig struct {
	current         map[storage.ShardKey]storage.BlockState
	cache           map[storage.BlockRootHash]*storage.BlockState
	loadState       func(context.Context, storage.BlockState) (*storage.BlockState, error)
	loadBlock       shardBlockLoader
	apply           shardStateApplyFunc
	afterApplyState shardStateAfterApplyFunc
	save            func(context.Context, *storage.BlockState) error
}

type shardStateResolverTask struct {
	done  chan struct{}
	state *storage.BlockState
	err   error
}

type shardStateResolverStats struct {
	applyElapsed  time.Duration
	blocksApplied int
	blocksReused  int
}

type shardStateCacheEviction struct {
	key   storage.BlockRootHash
	seqno uint32
	empty bool
}

type shardStateResolver struct {
	ctx context.Context

	current         map[storage.ShardKey]storage.BlockState
	cache           map[storage.BlockRootHash]*storage.BlockState
	loadState       func(context.Context, storage.BlockState) (*storage.BlockState, error)
	loadBlock       shardBlockLoader
	apply           shardStateApplyFunc
	afterApplyState shardStateAfterApplyFunc
	save            func(context.Context, *storage.BlockState) error

	mu    sync.Mutex
	tasks map[storage.BlockRootHash]*shardStateResolverTask
	stats shardStateResolverStats
}

func newShardStateResolver(ctx context.Context, cfg shardStateResolverConfig) *shardStateResolver {
	cache := cfg.cache
	if cache == nil {
		cache = map[storage.BlockRootHash]*storage.BlockState{}
	}

	return &shardStateResolver{
		ctx:             ctx,
		current:         cfg.current,
		cache:           cache,
		loadState:       cfg.loadState,
		loadBlock:       cfg.loadBlock,
		apply:           cfg.apply,
		afterApplyState: cfg.afterApplyState,
		save:            cfg.save,
		tasks:           map[storage.BlockRootHash]*shardStateResolverTask{},
	}
}

func (r *shardStateResolver) resolveWithContext(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	key := storage.BlockKey(block)

	r.mu.Lock()
	if state := r.cache[key]; state != nil {
		r.stats.blocksReused++
		r.mu.Unlock()
		return state, nil
	}
	if task := r.tasks[key]; task != nil {
		r.mu.Unlock()
		select {
		case <-task.done:
			if task.err != nil {
				return nil, task.err
			}
			r.mu.Lock()
			r.stats.blocksReused++
			r.mu.Unlock()
			return task.state, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	task := &shardStateResolverTask{done: make(chan struct{})}
	r.tasks[key] = task
	r.mu.Unlock()

	state, err := r.resolveOwned(ctx, block)

	r.mu.Lock()
	if err == nil {
		r.cache[key] = state
		r.evictCacheLocked()
	}
	delete(r.tasks, key)
	task.state = state
	task.err = err
	close(task.done)
	r.mu.Unlock()
	return state, err
}

func (r *shardStateResolver) resolveOwned(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if state, err := r.currentExactState(ctx, block); err == nil {
		r.markReused()
		return state, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	state, err := r.loadState(ctx, storage.BlockState{Block: block})
	if err == nil {
		r.markReused()
		return state, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("load stored shard state %s: %w", storage.FormatBlockRef(block), err)
	}

	downloaded, err := r.loadBlock(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load shard block %s: %w", storage.FormatBlockRef(block), err)
	}
	if !downloaded.ID.Equals(&block) {
		return nil, fmt.Errorf("loaded shard block %s instead of %s", downloaded.BlockRef(), storage.FormatBlockRef(block))
	}
	prevRefs := downloaded.Meta.PrevRefs
	if len(prevRefs) == 0 || len(prevRefs) > 2 {
		return nil, fmt.Errorf("%w: shard block %s has %d previous refs", errShardCatchUpNeedsSnapshot, downloaded.BlockRef(), len(prevRefs))
	}

	previous := make([]*storage.BlockState, len(prevRefs))
	for i, prevRef := range prevRefs {
		if err := validateShardStatePrevRef(block, prevRef); err != nil {
			return nil, err
		}
		prev, err := r.resolveWithContext(ctx, prevRef)
		if err != nil {
			return nil, err
		}
		previous[i] = prev
	}

	applyStarted := time.Now()
	next, err := r.apply(ctx, block, previous, downloaded)
	applyElapsed := time.Since(applyStarted)
	if err != nil {
		return nil, err
	}
	r.markApplied(applyElapsed)

	if r.afterApplyState != nil {
		if err = r.afterApplyState(ctx, next, downloaded, applyElapsed); err != nil {
			return nil, err
		}
	}

	if r.save != nil {
		if err = r.save(ctx, next); err != nil {
			return nil, fmt.Errorf("persist resolved shard state %s: %w", storage.FormatBlockRef(next.Block), err)
		}
	}
	return next, nil
}

func validateShardStatePrevRef(block ton.BlockIDExt, prev ton.BlockIDExt) error {
	if prev.Workchain != block.Workchain {
		return fmt.Errorf("shard block %s has previous ref from another workchain %s", storage.FormatBlockRef(block), storage.FormatBlockRef(prev))
	}
	if prev.SeqNo >= block.SeqNo {
		return fmt.Errorf("shard block %s has non-previous ref %s", storage.FormatBlockRef(block), storage.FormatBlockRef(prev))
	}
	return nil
}

func (r *shardStateResolver) currentExactState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	r.mu.Lock()
	current, ok := r.current[storage.ShardKeyFromBlock(block)]
	r.mu.Unlock()

	if !ok || !current.Block.Equals(&block) {
		return nil, storage.ErrNotFound
	}
	if current.Cell != nil {
		return storage.CloneBlockState(&current), nil
	}

	state, err := r.loadState(ctx, current)
	if err != nil {
		return nil, fmt.Errorf("load current shard state %s: %w", storage.FormatBlockRef(block), err)
	}
	return state, nil
}

func (r *shardStateResolver) updateCurrent(current map[storage.ShardKey]storage.BlockState) {
	r.mu.Lock()
	r.current = current
	r.mu.Unlock()
}

// hasCurrentBlock reports that the block already is the shard-client tip for
// its shard. The commit stage carries those over without touching the resolver
// (keeping their original inclusion master), so the apply-ahead stage must not
// resolve them either.
func (r *shardStateResolver) hasCurrentBlock(block ton.BlockIDExt) bool {
	r.mu.Lock()
	current, ok := r.current[storage.ShardKeyFromBlock(block)]
	r.mu.Unlock()

	return ok && current.Block.Equals(&block)
}

func (r *shardStateResolver) currentBlocksForBlock(block ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var blocks []ton.BlockIDExt
	for _, state := range r.current {
		if !shardBlocksRelated(state.Block, block) {
			continue
		}
		blocks = append(blocks, cloneServiceBlockID(state.Block))
	}
	if len(blocks) == 0 {
		return nil, storage.ErrNotFound
	}
	return blocks, nil
}

func (r *shardStateResolver) evictCacheLocked() {
	if len(r.cache) <= shardStateResolverCacheLimit {
		return
	}

	evictions := make([]shardStateCacheEviction, 0, len(r.cache))
	for key, state := range r.cache {
		eviction := shardStateCacheEviction{key: key, empty: state == nil}
		if state != nil {
			eviction.seqno = state.Block.SeqNo
		}
		evictions = append(evictions, eviction)
	}
	slices.SortFunc(evictions, func(a, b shardStateCacheEviction) int {
		if a.empty != b.empty {
			if a.empty {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.seqno, b.seqno)
	})

	remove := len(r.cache) - shardStateResolverCacheRetain
	for _, eviction := range evictions[:remove] {
		delete(r.cache, eviction.key)
	}
}

func (r *shardStateResolver) markReused() {
	r.mu.Lock()
	r.stats.blocksReused++
	r.mu.Unlock()
}

func (r *shardStateResolver) markApplied(elapsed time.Duration) {
	r.mu.Lock()
	r.stats.blocksApplied++
	r.stats.applyElapsed += elapsed
	r.mu.Unlock()
}

func (r *shardStateResolver) statsSnapshot() shardStateResolverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

func (r *nextSyncRunner) applyResolvedShardBlock(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded PreparedBlock) (*storage.BlockState, error) {
	master, err := shardApplyMasterFromContext(ctx)
	if err != nil {
		return nil, err
	}

	inclusion := master.Block
	return r.service.applyResolvedShardBlock(ctx, target, previous, downloaded, r.stateCells, &blockApplyHookMeta{
		InclusionMasterRef:   &inclusion,
		InclusionMasterState: master.Cell,
	})
}

func (s *Service) applyResolvedShardBlock(ctx context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded PreparedBlock, applier stateUpdateApplier, hook *blockApplyHookMeta) (*storage.BlockState, error) {
	if !downloaded.ID.Equals(&target) {
		return nil, fmt.Errorf("shard resolver downloaded %s instead of target %s", downloaded.BlockRef(), storage.FormatBlockRef(target))
	}
	if len(downloaded.Meta.PrevRefs) != len(previous) {
		return nil, fmt.Errorf("%w: shard %s route has %d previous refs, expected %d", errShardCatchUpNeedsSnapshot, downloaded.BlockRef(), len(downloaded.Meta.PrevRefs), len(previous))
	}

	for i := range previous {
		if !downloaded.Meta.PrevRefs[i].Equals(&previous[i].Block) {
			return nil, fmt.Errorf("%w: shard %s previous[%d] is %s, resolved state is %s", errShardCatchUpNeedsSnapshot, downloaded.BlockRef(), i, storage.FormatBlockRef(downloaded.Meta.PrevRefs[i]), storage.FormatBlockRef(previous[i].Block))
		}
	}

	// Guarded: this runs once per applied shard block on the commit path, and
	// the field arguments (block-ref formatting, route classification) are
	// evaluated by Go before zerolog can discard them on a disabled event.
	if event := s.log.Debug(); event.Enabled() {
		event.
			Stringer("target", storage.BlockRef(downloaded.ID)).
			Str("route", shardTransitionKind(target, downloaded.Meta.PrevRefs))
		if len(previous) > 0 {
			event.Stringer("prev", storage.BlockRef(previous[0].Block))
		}
		if len(previous) > 1 {
			event.Stringer("prev_right", storage.BlockRef(previous[1].Block))
		}
		event.Msg("applying shard state dependency")
	}

	next, err := s.applyBlockWithHooks(ctx, previous, downloaded, applier, hook)
	if err != nil {
		return nil, fmt.Errorf("apply shard block %s: %w", downloaded.BlockRef(), err)
	}
	return next, nil
}

func (s *Service) stateCellLoader() cell.LazyCellLoader {
	base := s.storage.LazyCellLoader()

	return func(hash cell.Hash) (*cell.Cell, error) {
		for _, loader := range s.stateCellLoadersSnapshot() {
			loaded, err := loader(hash)
			if err == nil {
				return loaded, nil
			}
			if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, cell.ErrLazyRefNotFound) {
				return nil, err
			}
		}
		return base(hash)
	}
}

func (s *Service) retainStateCellLoader(loader cell.LazyCellLoader) func() {
	s.stateCellLoaderMu.Lock()
	s.nextStateCellLoaderID++
	id := s.nextStateCellLoaderID
	s.stateCellLoaders[id] = loader
	s.storeStateCellLoadersSnapshotLocked()
	s.stateCellLoaderMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.stateCellLoaderMu.Lock()
			delete(s.stateCellLoaders, id)
			s.storeStateCellLoadersSnapshotLocked()
			s.stateCellLoaderMu.Unlock()
		})
	}
}

func (s *Service) stateCellLoadersSnapshot() []cell.LazyCellLoader {
	loaders, _ := s.stateCellLoaderSnapshot.Load().([]cell.LazyCellLoader)
	return loaders
}

func (s *Service) storeStateCellLoadersSnapshotLocked() {
	loaders := make([]cell.LazyCellLoader, 0, len(s.stateCellLoaders))
	for _, loader := range s.stateCellLoaders {
		loaders = append(loaders, loader)
	}
	s.stateCellLoaderSnapshot.Store(loaders)
}

func shardTransitionKind(target ton.BlockIDExt, prevRefs []ton.BlockIDExt) string {
	if len(prevRefs) == 2 {
		return "merge"
	}
	if len(prevRefs) == 1 && (prevRefs[0].Workchain != target.Workchain || prevRefs[0].Shard != target.Shard) {
		return "split"
	}
	return "linear"
}

func shardBlocksRelated(a ton.BlockIDExt, b ton.BlockIDExt) bool {
	if a.Workchain != b.Workchain {
		return false
	}
	aShard := tlb.ShardID(uint64(a.Shard))
	bShard := tlb.ShardID(uint64(b.Shard))
	return aShard == bShard || aShard.IsAncestor(bShard) || bShard.IsAncestor(aShard)
}

func cloneServiceBlockID(block ton.BlockIDExt) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo,
		RootHash:  append([]byte(nil), block.RootHash...),
		FileHash:  append([]byte(nil), block.FileHash...),
	}
}
