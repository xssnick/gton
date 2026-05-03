package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"flexserver/service/p2p"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const shardStateResolverCacheLimit = 2048

type shardStateApplyFunc func(context.Context, ton.BlockIDExt, []*storage.BlockState, p2p.DownloadedBlock) (*storage.BlockState, error)

type shardStateResolverConfig struct {
	current   map[storage.ShardKey]storage.BlockState
	cache     map[string]*storage.BlockState
	loadState func(context.Context, storage.BlockState) (*storage.BlockState, error)
	loadBlock shardBlockLoader
	apply     shardStateApplyFunc
	save      func(context.Context, *storage.BlockState) error
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
	appliedBlocks []ton.BlockIDExt
}

type shardStateResolver struct {
	ctx context.Context

	current   map[storage.ShardKey]storage.BlockState
	cache     map[string]*storage.BlockState
	loadState func(context.Context, storage.BlockState) (*storage.BlockState, error)
	loadBlock shardBlockLoader
	apply     shardStateApplyFunc
	save      func(context.Context, *storage.BlockState) error

	mu    sync.Mutex
	tasks map[string]*shardStateResolverTask
	stats shardStateResolverStats
}

func newShardStateResolver(ctx context.Context, cfg shardStateResolverConfig) *shardStateResolver {
	cache := cfg.cache
	if cache == nil {
		cache = map[string]*storage.BlockState{}
	}

	return &shardStateResolver{
		ctx:       ctx,
		current:   cfg.current,
		cache:     cache,
		loadState: cfg.loadState,
		loadBlock: cfg.loadBlock,
		apply:     cfg.apply,
		save:      cfg.save,
		tasks:     map[string]*shardStateResolverTask{},
	}
}

func (r *shardStateResolver) resolve(block ton.BlockIDExt) (*storage.BlockState, error) {
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
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	}

	task := &shardStateResolverTask{done: make(chan struct{})}
	r.tasks[key] = task
	r.mu.Unlock()

	state, err := r.resolveOwned(block)

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

func (r *shardStateResolver) resolveOwned(block ton.BlockIDExt) (*storage.BlockState, error) {
	if state, err := r.currentExactState(block); err == nil {
		r.markReused()
		return state, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	state, err := r.loadState(r.ctx, storage.BlockState{Block: block})
	if err == nil {
		r.markReused()
		return state, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("load stored shard state %s: %w", storage.FormatBlockRef(block), err)
	}

	downloaded, err := r.loadBlock(r.ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load shard block %s: %w", storage.FormatBlockRef(block), err)
	}
	if !downloaded.ID.Equals(&block) {
		return nil, fmt.Errorf("loaded shard block %s instead of %s", downloaded.BlockRef(), storage.FormatBlockRef(block))
	}
	if downloaded.Meta == nil {
		downloaded, err = prepareDownloadedBlock(downloaded)
		if err != nil {
			return nil, err
		}
	}

	prevRefs := downloaded.Meta.PrevRefs
	if len(prevRefs) == 0 || len(prevRefs) > 2 {
		return nil, fmt.Errorf("%w: shard block %s has %d previous refs", errShardCatchUpNeedsSnapshot, downloaded.BlockRef(), len(prevRefs))
	}

	previous := make([]*storage.BlockState, len(prevRefs))
	for i, prevRef := range prevRefs {
		prev, err := r.resolve(prevRef)
		if err != nil {
			return nil, err
		}
		previous[i] = prev
	}

	applyStarted := time.Now()
	next, err := r.apply(r.ctx, block, previous, downloaded)
	applyElapsed := time.Since(applyStarted)
	if err != nil {
		return nil, err
	}
	r.markApplied(block, applyElapsed)

	if r.save != nil {
		if err = r.save(r.ctx, next); err != nil {
			return nil, fmt.Errorf("persist resolved shard state %s: %w", storage.FormatBlockRef(next.Block), err)
		}
	}
	return next, nil
}

func (r *shardStateResolver) currentExactState(block ton.BlockIDExt) (*storage.BlockState, error) {
	r.mu.Lock()
	current, ok := r.current[storage.ShardKeyFromBlock(block)]
	r.mu.Unlock()

	if !ok || !current.Block.Equals(&block) {
		return nil, storage.ErrNotFound
	}
	if current.Cell != nil {
		return storage.CloneBlockState(&current), nil
	}

	state, err := r.loadState(r.ctx, current)
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

func (r *shardStateResolver) evictCacheLocked() {
	if len(r.cache) <= shardStateResolverCacheLimit {
		return
	}

	var evictKey string
	var evictSeqno uint32
	first := true
	for key, state := range r.cache {
		if state == nil {
			evictKey = key
			break
		}
		if first || state.Block.SeqNo < evictSeqno {
			evictKey = key
			evictSeqno = state.Block.SeqNo
			first = false
		}
	}
	delete(r.cache, evictKey)
}

func (r *shardStateResolver) markReused() {
	r.mu.Lock()
	r.stats.blocksReused++
	r.mu.Unlock()
}

func (r *shardStateResolver) markApplied(block ton.BlockIDExt, elapsed time.Duration) {
	r.mu.Lock()
	r.stats.blocksApplied++
	r.stats.applyElapsed += elapsed
	r.stats.appliedBlocks = append(r.stats.appliedBlocks, block)
	r.mu.Unlock()
}

func (r *shardStateResolver) statsSnapshot() shardStateResolverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.stats
	stats.appliedBlocks = append([]ton.BlockIDExt(nil), r.stats.appliedBlocks...)
	return stats
}

func (r *shardStateResolver) cachedState(block ton.BlockIDExt) (*storage.BlockState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.cache[storage.BlockKey(block)]
	if state == nil {
		return nil, false
	}
	return state, true
}

func (r *shardStateResolver) rememberState(state *storage.BlockState) {
	if state == nil {
		return
	}

	r.mu.Lock()
	r.cache[storage.BlockKey(state.Block)] = state
	r.mu.Unlock()
}

func (s *Service) applyResolvedShardBlock(_ context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*storage.BlockState, error) {
	downloaded, err := prepareDownloadedBlock(downloaded)
	if err != nil {
		return nil, err
	}
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

	event := s.log.Debug().
		Str("target", downloaded.BlockRef()).
		Str("route", shardTransitionKind(target, downloaded.Meta.PrevRefs))
	if len(previous) > 0 {
		event.Str("prev", storage.FormatBlockRef(previous[0].Block))
	}
	if len(previous) > 1 {
		event.Str("prev_right", storage.FormatBlockRef(previous[1].Block))
	}
	event.Msg("applying shard state dependency")

	next, err := ApplyBlockWithPreviousStates(previous, downloaded)
	if err != nil {
		return nil, fmt.Errorf("apply shard block %s: %w", downloaded.BlockRef(), err)
	}
	return next, nil
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
