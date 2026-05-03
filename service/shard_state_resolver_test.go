package service

import (
	"context"
	"fmt"
	"testing"

	"flexserver/service/p2p"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

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

	state, err := resolver.resolve(target)
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

	state, err := resolver.resolve(target)
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

	if _, err := resolver.resolve(targetA); err != nil {
		t.Fatalf("resolve target A: %v", err)
	}
	if _, err := resolver.resolve(targetB); err != nil {
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

	if _, err := resolver.resolve(left); err != nil {
		t.Fatalf("resolve left split child: %v", err)
	}
	if _, err := resolver.resolve(right); err != nil {
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

	cache := map[string]*storage.BlockState{}
	splitResolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(parent): {Block: parent},
		},
		cache:     cache,
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})
	if _, err := splitResolver.resolve(left); err != nil {
		t.Fatalf("resolve left split child: %v", err)
	}
	if _, err := splitResolver.resolve(right); err != nil {
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
	if _, err := mergeResolver.resolve(merged); err != nil {
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

	if _, err := resolver.resolve(target); err != nil {
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

	nextState, err := resolver.resolve(next)
	if err != nil {
		t.Fatalf("resolve next: %v", err)
	}
	nextState.Cell = testShardStateCell(t, nextState.Block)

	nextBlockLoads := env.blockLoads[storage.BlockKey(next)]
	resolver.mu.Lock()
	resolver.cache = map[string]*storage.BlockState{}
	resolver.mu.Unlock()
	resolver.updateCurrent(map[storage.ShardKey]storage.BlockState{
		storage.ShardKeyFromBlock(next): *nextState,
	})

	if _, err = resolver.resolve(target); err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if got := env.blockLoads[storage.BlockKey(next)]; got != nextBlockLoads {
		t.Fatalf("updated current state was not reused: next block loads got %d want %d", got, nextBlockLoads)
	}
}

type fakeShardStateResolverEnv struct {
	states       map[string]*storage.BlockState
	blocks       map[string]p2p.DownloadedBlock
	stateLoads   map[string]int
	blockLoads   map[string]int
	stateLoadSeq []ton.BlockIDExt
	blockLoadSeq []ton.BlockIDExt
	applied      []ton.BlockIDExt
	saved        []ton.BlockIDExt
}

func newFakeShardStateResolverEnv() *fakeShardStateResolverEnv {
	return &fakeShardStateResolverEnv{
		states:     map[string]*storage.BlockState{},
		blocks:     map[string]p2p.DownloadedBlock{},
		stateLoads: map[string]int{},
		blockLoads: map[string]int{},
	}
}

func (e *fakeShardStateResolverEnv) addState(block ton.BlockIDExt) {
	e.states[storage.BlockKey(block)] = &storage.BlockState{Block: block}
}

func (e *fakeShardStateResolverEnv) addBlock(block ton.BlockIDExt, prevRefs ...ton.BlockIDExt) {
	e.blocks[storage.BlockKey(block)] = p2p.DownloadedBlock{
		ID: block,
		Meta: &storage.BlockMeta{
			ID:       block,
			PrevRefs: append([]ton.BlockIDExt(nil), prevRefs...),
		},
	}
}

func (e *fakeShardStateResolverEnv) loadState(_ context.Context, state storage.BlockState) (*storage.BlockState, error) {
	key := storage.BlockKey(state.Block)
	e.stateLoads[key]++
	e.stateLoadSeq = append(e.stateLoadSeq, state.Block)

	stored := e.states[key]
	if stored == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneBlockState(stored), nil
}

func (e *fakeShardStateResolverEnv) loadBlock(_ context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	key := storage.BlockKey(block)
	e.blockLoads[key]++
	e.blockLoadSeq = append(e.blockLoadSeq, block)

	downloaded, ok := e.blocks[key]
	if !ok {
		return p2p.DownloadedBlock{}, storage.ErrNotFound
	}
	return downloaded, nil
}

func (e *fakeShardStateResolverEnv) apply(_ context.Context, target ton.BlockIDExt, previous []*storage.BlockState, downloaded p2p.DownloadedBlock) (*storage.BlockState, error) {
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
	e.applied = append(e.applied, target)
	return &storage.BlockState{Block: target}, nil
}

func (e *fakeShardStateResolverEnv) save(_ context.Context, state *storage.BlockState) error {
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
