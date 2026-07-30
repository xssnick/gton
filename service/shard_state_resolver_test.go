package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

func (r *nextSyncRunner) validateShardDescriptionPrefetch(desc *p2p.ShardBlockDescription) error {
	currentBlocks, err := r.shardResolver.currentBlocksForBlock(desc.Block)
	if err != nil {
		return err
	}
	return validateShardDescriptionPrefetchAgainst(desc, currentBlocks)
}

func TestShardStateResolverResolvesSplitDescendantFromParent(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 10)
	left := int64(tlb.ShardID(uint64(parent.Shard)).GetChild(true))
	firstChild := testBlockID(0, left, 11)
	target := testBlockID(0, left, 12)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addBlock(firstChild, parent)
	env.addBlock(target, firstChild)

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
		save:      env.save,
	})

	state, err := resolver.resolveWithContext(ctx, target)
	if err != nil {
		t.Fatalf("resolve split descendant: %v", err)
	}
	if !state.Block.Equals(&target) {
		t.Fatalf("unexpected resolved state %s", storage.FormatBlockRef(state.Block))
	}
	assertBlockSeq(t, "applied", env.applied, []ton.BlockIDExt{firstChild, target})
	assertBlockSeq(t, "saved", env.saved, []ton.BlockIDExt{firstChild, target})

	stats := resolver.statsSnapshot()
	if stats.blocksApplied != 2 || stats.blocksReused != 1 {
		t.Fatalf("unexpected stats applied=%d reused=%d", stats.blocksApplied, stats.blocksReused)
	}
}

func TestShardStateResolverReplaysStoredStateRejectedAsMissingFullBlock(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 13)
	target := testBlockID(0, topShard, 14)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addState(target)
	env.addBlock(target, parent)

	loadState := func(ctx context.Context, state storage.BlockState) (*storage.BlockState, error) {
		if state.Block.Equals(&target) {
			if _, err := env.loadState(ctx, state); err != nil {
				return nil, err
			}
			return nil, storage.ErrNotFound
		}
		return env.loadState(ctx, state)
	}

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
		save:      env.save,
	})

	state, err := resolver.resolveWithContext(ctx, target)
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if !state.Block.Equals(&target) {
		t.Fatalf("resolved state = %s, want %s", storage.FormatBlockRef(state.Block), storage.FormatBlockRef(target))
	}
	if got := env.stateLoads[storage.BlockKey(target)]; got != 1 {
		t.Fatalf("target state loads = %d, want 1", got)
	}
	if got := env.blockLoads[storage.BlockKey(target)]; got != 1 {
		t.Fatalf("target block loads = %d, want 1", got)
	}
	assertBlockSeq(t, "applied", env.applied, []ton.BlockIDExt{target})
	assertBlockSeq(t, "saved", env.saved, []ton.BlockIDExt{target})
}

func TestShardStateResolverResolvesMergeFromTwoChildren(t *testing.T) {
	ctx := context.Background()
	parentShard := int64(topShard)
	left := int64(tlb.ShardID(uint64(parentShard)).GetChild(true))
	right := int64(tlb.ShardID(uint64(parentShard)).GetChild(false))
	leftState := testBlockID(0, left, 20)
	rightState := testBlockID(0, right, 21)
	target := testBlockID(0, parentShard, 22)

	env := newFakeShardStateResolverEnv()
	env.addState(leftState)
	env.addState(rightState)
	env.addBlock(target, leftState, rightState)

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(leftState):  {Block: leftState},
			storage.ShardKeyFromBlock(rightState): {Block: rightState},
		},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
		save:      env.save,
	})

	state, err := resolver.resolveWithContext(ctx, target)
	if err != nil {
		t.Fatalf("resolve merge target: %v", err)
	}
	if !state.Block.Equals(&target) {
		t.Fatalf("unexpected resolved state %s", storage.FormatBlockRef(state.Block))
	}
	assertBlockSeq(t, "applied", env.applied, []ton.BlockIDExt{target})
	assertBlockSeq(t, "saved", env.saved, []ton.BlockIDExt{target})

	stats := resolver.statsSnapshot()
	if stats.blocksApplied != 1 || stats.blocksReused != 2 {
		t.Fatalf("unexpected stats applied=%d reused=%d", stats.blocksApplied, stats.blocksReused)
	}
}

func TestShardStateResolverCacheEvictionRetainsNewestBatch(t *testing.T) {
	resolver := &shardStateResolver{
		cache: make(map[storage.BlockRootHash]*storage.BlockState, shardStateResolverCacheLimit+1),
	}
	for seqno := uint32(0); seqno <= shardStateResolverCacheLimit; seqno++ {
		block := testBlockID(0, topShard, seqno)
		resolver.cache[storage.BlockKey(block)] = &storage.BlockState{Block: block}
	}

	resolver.evictCacheLocked()

	if got := len(resolver.cache); got != shardStateResolverCacheRetain {
		t.Fatalf("resolver cache size = %d, want %d", got, shardStateResolverCacheRetain)
	}
	removed := shardStateResolverCacheLimit + 1 - shardStateResolverCacheRetain
	for seqno := uint32(0); seqno < uint32(removed); seqno++ {
		if _, ok := resolver.cache[storage.BlockKey(testBlockID(0, topShard, seqno))]; ok {
			t.Fatalf("old shard state seqno %d remained cached", seqno)
		}
	}
	for seqno := uint32(removed); seqno <= shardStateResolverCacheLimit; seqno++ {
		if _, ok := resolver.cache[storage.BlockKey(testBlockID(0, topShard, seqno))]; !ok {
			t.Fatalf("new shard state seqno %d was evicted", seqno)
		}
	}
}

func TestValidateShardDescriptionPrefetchUsesRelatedCurrentShard(t *testing.T) {
	parent := testBlockID(0, topShard, 10)
	leftShard := int64(tlb.ShardID(uint64(parent.Shard)).GetChild(true))
	target := testBlockID(0, leftShard, 12)

	resolver := newShardStateResolver(context.Background(), shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
	})
	runner := &nextSyncRunner{shardResolver: resolver}

	desc := &p2p.ShardBlockDescription{
		Block: target,
		Chain: []p2p.ShardDescriptionLink{
			{Block: target, PrevRefs: []ton.BlockIDExt{parent}},
		},
	}
	if err := runner.validateShardDescriptionPrefetch(desc); err != nil {
		t.Fatalf("validate related shard target: %v", err)
	}

	unanchored := &p2p.ShardBlockDescription{
		Block: target,
		Chain: []p2p.ShardDescriptionLink{
			{Block: target, PrevRefs: []ton.BlockIDExt{testBlockID(0, leftShard, parent.SeqNo)}},
		},
	}
	if err := runner.validateShardDescriptionPrefetch(unanchored); err == nil {
		t.Fatal("expected unanchored shard description to be rejected")
	}

	old := testBlockID(0, leftShard, 10)
	if err := runner.validateShardDescriptionPrefetch(&p2p.ShardBlockDescription{Block: old}); err != errShardDescriptionTooOld {
		t.Fatalf("old shard target error = %v, want errShardDescriptionTooOld", err)
	}

	far := testBlockID(0, leftShard, parent.SeqNo+shardDescriptionPrefetchMaxAhead+1)
	if err := runner.validateShardDescriptionPrefetch(&p2p.ShardBlockDescription{Block: far}); err != errShardDescriptionTooNew {
		t.Fatalf("far shard target error = %v, want errShardDescriptionTooNew", err)
	}
}

func TestShardStateResolverCallsAfterApplyState(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 25)
	target := testBlockID(0, topShard, 26)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addBlock(target, parent)

	var callbackState ton.BlockIDExt
	var callbackBlock ton.BlockIDExt
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
		afterApplyState: func(_ context.Context, state *storage.BlockState, downloaded PreparedBlock, _ time.Duration) error {
			callbackState = state.Block
			callbackBlock = downloaded.ID
			return nil
		},
	})

	if _, err := resolver.resolveWithContext(ctx, target); err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if !callbackState.Equals(&target) {
		t.Fatalf("callback state = %s, want %s", storage.FormatBlockRef(callbackState), storage.FormatBlockRef(target))
	}
	if !callbackBlock.Equals(&target) {
		t.Fatalf("callback block = %s, want %s", storage.FormatBlockRef(callbackBlock), storage.FormatBlockRef(target))
	}
}

func TestShardStateResolverWaitUsesCallContext(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 27)
	target := testBlockID(0, topShard, 28)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	started := make(chan struct{})
	unblock := make(chan struct{})
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: env.loadState,
		loadBlock: func(ctx context.Context, block ton.BlockIDExt) (PreparedBlock, error) {
			close(started)
			select {
			case <-unblock:
				return PreparedBlock{}, storage.ErrNotFound
			case <-ctx.Done():
				return PreparedBlock{}, ctx.Err()
			}
		},
		apply: env.apply,
	})

	done := make(chan error, 1)
	go func() {
		_, err := resolver.resolveWithContext(ctx, target)
		done <- err
	}()
	<-started

	waitCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := resolver.resolveWithContext(waitCtx, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("shared task wait error = %v, want context.Canceled", err)
	}

	close(unblock)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("owner resolve did not finish")
	}
}

func TestShardStateResolverRejectsNonPreviousRef(t *testing.T) {
	ctx := context.Background()
	target := testBlockID(0, topShard, 29)

	env := newFakeShardStateResolverEnv()
	env.addBlock(target, target)
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current:   map[storage.ShardKey]storage.BlockState{},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})

	if _, err := resolver.resolveWithContext(ctx, target); err == nil {
		t.Fatal("expected non-previous ref to be rejected")
	}
}

func TestShardStateResolverReusesIntermediateStateForSiblingTargets(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 30)
	left := int64(tlb.ShardID(uint64(parent.Shard)).GetChild(true))
	firstChild := testBlockID(0, left, 31)
	targetA := testBlockID(0, left, 32)
	targetB := testBlockID(0, left, 33)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addBlock(firstChild, parent)
	env.addBlock(targetA, firstChild)
	env.addBlock(targetB, firstChild)

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})

	if _, err := resolver.resolveWithContext(ctx, targetA); err != nil {
		t.Fatalf("resolve target A: %v", err)
	}
	if _, err := resolver.resolveWithContext(ctx, targetB); err != nil {
		t.Fatalf("resolve target B: %v", err)
	}
	assertBlockSeq(t, "applied", env.applied, []ton.BlockIDExt{firstChild, targetA, targetB})

	stats := resolver.statsSnapshot()
	if stats.blocksApplied != 3 || stats.blocksReused != 2 {
		t.Fatalf("unexpected stats applied=%d reused=%d", stats.blocksApplied, stats.blocksReused)
	}
}

func TestShardStateResolverResolvesSplitSiblingsFromSameParent(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 40)
	leftShard := int64(tlb.ShardID(uint64(parent.Shard)).GetChild(true))
	rightShard := int64(tlb.ShardID(uint64(parent.Shard)).GetChild(false))
	left := testBlockID(0, leftShard, 41)
	right := testBlockID(0, rightShard, 41)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addBlock(left, parent)
	env.addBlock(right, parent)

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})

	if _, err := resolver.resolveWithContext(ctx, left); err != nil {
		t.Fatalf("resolve left split child: %v", err)
	}
	if _, err := resolver.resolveWithContext(ctx, right); err != nil {
		t.Fatalf("resolve right split child: %v", err)
	}
	assertBlockSeq(t, "applied", env.applied, []ton.BlockIDExt{left, right})

	if got := env.blockLoads[storage.BlockKey(parent)]; got != 0 {
		t.Fatalf("parent shard was downloaded %d times", got)
	}
	if got := env.stateLoads[storage.BlockKey(parent)]; got != 1 {
		t.Fatalf("parent shard state loaded %d times, want 1", got)
	}

	stats := resolver.statsSnapshot()
	if stats.blocksApplied != 2 || stats.blocksReused != 2 {
		t.Fatalf("unexpected stats applied=%d reused=%d", stats.blocksApplied, stats.blocksReused)
	}
}

func TestShardStateResolverReusesSplitChildrenWhenTheyMerge(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 50)
	leftShard := int64(tlb.ShardID(uint64(parent.Shard)).GetChild(true))
	rightShard := int64(tlb.ShardID(uint64(parent.Shard)).GetChild(false))
	left := testBlockID(0, leftShard, 51)
	right := testBlockID(0, rightShard, 51)
	merged := testBlockID(0, topShard, 52)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addBlock(left, parent)
	env.addBlock(right, parent)
	env.addBlock(merged, left, right)

	cache := map[storage.BlockRootHash]*storage.BlockState{}
	splitResolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		cache:     cache,
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})
	if _, err := splitResolver.resolveWithContext(ctx, left); err != nil {
		t.Fatalf("resolve left split child: %v", err)
	}
	if _, err := splitResolver.resolveWithContext(ctx, right); err != nil {
		t.Fatalf("resolve right split child: %v", err)
	}

	leftStateLoadsBeforeMerge := env.stateLoads[storage.BlockKey(left)]
	rightStateLoadsBeforeMerge := env.stateLoads[storage.BlockKey(right)]
	leftBlockLoadsBeforeMerge := env.blockLoads[storage.BlockKey(left)]
	rightBlockLoadsBeforeMerge := env.blockLoads[storage.BlockKey(right)]
	mergeResolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(left):  {Block: left},
			storage.ShardKeyFromBlock(right): {Block: right},
		},
		cache:     cache,
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})
	if _, err := mergeResolver.resolveWithContext(ctx, merged); err != nil {
		t.Fatalf("resolve merged shard: %v", err)
	}
	assertBlockSeq(t, "applied", env.applied, []ton.BlockIDExt{left, right, merged})

	if got := env.stateLoads[storage.BlockKey(left)] - leftStateLoadsBeforeMerge; got != 0 {
		t.Fatalf("merge loaded left previous shard state %d times, want 0", got)
	}
	if got := env.stateLoads[storage.BlockKey(right)] - rightStateLoadsBeforeMerge; got != 0 {
		t.Fatalf("merge loaded right previous shard state %d times, want 0", got)
	}
	if got := env.blockLoads[storage.BlockKey(left)] - leftBlockLoadsBeforeMerge; got != 0 {
		t.Fatalf("merge downloaded left previous shard block %d times, want 0", got)
	}
	if got := env.blockLoads[storage.BlockKey(right)] - rightBlockLoadsBeforeMerge; got != 0 {
		t.Fatalf("merge downloaded right previous shard block %d times, want 0", got)
	}
}

func TestShardStateResolverDropsCompletedTasks(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 60)
	target := testBlockID(0, topShard, 61)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addBlock(target, parent)

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})

	if _, err := resolver.resolveWithContext(ctx, target); err != nil {
		t.Fatalf("resolve target: %v", err)
	}

	resolver.mu.Lock()
	tasks := len(resolver.tasks)
	resolver.mu.Unlock()
	if tasks != 0 {
		t.Fatalf("completed resolver tasks = %d, want 0", tasks)
	}
}

func TestShardStateResolverUsesUpdatedCurrentState(t *testing.T) {
	ctx := context.Background()
	parent := testBlockID(0, topShard, 70)
	next := testBlockID(0, topShard, 71)
	target := testBlockID(0, topShard, 72)

	env := newFakeShardStateResolverEnv()
	env.addState(parent)
	env.addBlock(next, parent)
	env.addBlock(target, next)

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})

	nextState, err := resolver.resolveWithContext(ctx, next)
	if err != nil {
		t.Fatalf("resolve next: %v", err)
	}
	nextState.Cell = testShardStateCell(t, nextState.Block)

	nextBlockLoads := env.blockLoads[storage.BlockKey(next)]
	resolver.mu.Lock()
	resolver.cache = map[storage.BlockRootHash]*storage.BlockState{}
	resolver.mu.Unlock()
	resolver.updateCurrent(map[storage.ShardKey]storage.BlockState{
		storage.ShardKeyFromBlock(next): *nextState,
	})

	if _, err = resolver.resolveWithContext(ctx, target); err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if got := env.blockLoads[storage.BlockKey(next)]; got != nextBlockLoads {
		t.Fatalf("updated current state was not reused: next block loads got %d want %d", got, nextBlockLoads)
	}
}

type fakeShardStateResolverEnv struct {
	// mu guards every field: the resolver calls these callbacks from its own
	// worker pool, and the apply-ahead stage adds a second concurrent caller.
	mu           sync.Mutex
	states       map[storage.BlockRootHash]*storage.BlockState
	blocks       map[storage.BlockRootHash]PreparedBlock
	stateLoads   map[storage.BlockRootHash]int
	blockLoads   map[storage.BlockRootHash]int
	stateLoadSeq []ton.BlockIDExt
	blockLoadSeq []ton.BlockIDExt
	applied      []ton.BlockIDExt
	saved        []ton.BlockIDExt
}

func newFakeShardStateResolverEnv() *fakeShardStateResolverEnv {
	return &fakeShardStateResolverEnv{
		states:     map[storage.BlockRootHash]*storage.BlockState{},
		blocks:     map[storage.BlockRootHash]PreparedBlock{},
		stateLoads: map[storage.BlockRootHash]int{},
		blockLoads: map[storage.BlockRootHash]int{},
	}
}

func (e *fakeShardStateResolverEnv) addState(block ton.BlockIDExt) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.states[storage.BlockKey(block)] = &storage.BlockState{Block: block}
}

func (e *fakeShardStateResolverEnv) addBlock(block ton.BlockIDExt, prevRefs ...ton.BlockIDExt) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.blocks[storage.BlockKey(block)] = PreparedBlock{
		ID: block,
		Meta: &storage.BlockMeta{
			ID:       block,
			PrevRefs: append([]ton.BlockIDExt(nil), prevRefs...),
		},
	}
}

func (e *fakeShardStateResolverEnv) loadState(_ context.Context, state storage.BlockState) (*storage.BlockState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := storage.BlockKey(state.Block)
	e.stateLoads[key]++
	e.stateLoadSeq = append(e.stateLoadSeq, state.Block)

	stored := e.states[key]
	if stored == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneBlockState(stored), nil
}

func (e *fakeShardStateResolverEnv) loadBlock(_ context.Context, block ton.BlockIDExt) (PreparedBlock, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := storage.BlockKey(block)
	e.blockLoads[key]++
	e.blockLoadSeq = append(e.blockLoadSeq, block)

	downloaded, ok := e.blocks[key]
	if !ok {
		return PreparedBlock{}, storage.ErrNotFound
	}
	return downloaded, nil
}

func (e *fakeShardStateResolverEnv) apply(_ context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded PreparedBlock) (*storage.BlockState, error) {
	if !downloaded.ID.Equals(&target) {
		return nil, fmt.Errorf("downloaded %s instead of target %s", storage.FormatBlockRef(downloaded.ID), storage.FormatBlockRef(target))
	}
	if len(previous) != len(downloaded.Meta.PrevRefs) {
		return nil, fmt.Errorf("got %d previous states, want %d", len(previous), len(downloaded.Meta.PrevRefs))
	}
	for i, prev := range previous {
		if !prev.Block.Equals(&downloaded.Meta.PrevRefs[i]) {
			return nil, fmt.Errorf("previous[%d] = %s, want %s", i, storage.FormatBlockRef(prev.Block), storage.FormatBlockRef(downloaded.Meta.PrevRefs[i]))
		}
	}
	e.mu.Lock()
	e.applied = append(e.applied, target)
	e.mu.Unlock()
	return &storage.BlockState{Block: target}, nil
}

func (e *fakeShardStateResolverEnv) save(_ context.Context, state *storage.BlockState) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.saved = append(e.saved, state.Block)
	e.states[storage.BlockKey(state.Block)] = storage.CloneBlockState(state)
	return nil
}

func assertBlockSeq(t *testing.T, name string, got []ton.BlockIDExt, want []ton.BlockIDExt) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s length=%d want=%d got=%v", name, len(got), len(want), formatBlockSeq(got))
	}
	for i := range want {
		if !got[i].Equals(&want[i]) {
			t.Fatalf("%s[%d]=%s want=%s", name, i, storage.FormatBlockRef(got[i]), storage.FormatBlockRef(want[i]))
		}
	}
}

func formatBlockSeq(blocks []ton.BlockIDExt) []string {
	out := make([]string, 0, len(blocks))
	for _, block := range blocks {
		out = append(out, storage.FormatBlockRef(block))
	}
	return out
}
