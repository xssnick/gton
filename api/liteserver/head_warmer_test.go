package liteserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const testWarmHeadUTime uint32 = 1000

// warmStoreCounts is the per-block read count of every store call a warmed
// artifact's builder makes. A cache hit performs none of them, so the counters
// are an exact seam for "the real query was served from the warmed entry".
type warmStoreCounts struct {
	blockRoot int
	fragments int
	meta      int
	stateTree int
}

type warmCountingStore struct {
	Store

	mu     sync.Mutex
	counts map[storage.BlockRootHash]*warmStoreCounts

	// gate, when non-nil, blocks the first BlockRoot call until it is closed and
	// signals entry on started. It lets a test hold a warm inside its first step
	// while a newer head is published.
	gateOnce sync.Once
	started  chan struct{}
	gate     chan struct{}
}

func newWarmCountingStore(store Store) *warmCountingStore {
	return &warmCountingStore{Store: store, counts: map[storage.BlockRootHash]*warmStoreCounts{}}
}

func (s *warmCountingStore) at(block ton.BlockIDExt) *warmStoreCounts {
	key := storage.BlockKey(block)
	entry := s.counts[key]
	if entry == nil {
		entry = &warmStoreCounts{}
		s.counts[key] = entry
	}
	return entry
}

func (s *warmCountingStore) snapshot(block ton.BlockIDExt) warmStoreCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.at(block)
}

func (s *warmCountingStore) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	s.mu.Lock()
	s.at(block).blockRoot++
	gate, started := s.gate, s.started
	s.mu.Unlock()

	if gate != nil {
		s.gateOnce.Do(func() { close(started) })
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.Store.BlockRoot(ctx, block)
}

func (s *warmCountingStore) BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveview.BlockView, error) {
	s.mu.Lock()
	s.at(block).fragments++
	s.mu.Unlock()
	return s.Store.BlockFragments(ctx, block)
}

func (s *warmCountingStore) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	s.mu.Lock()
	s.at(block).meta++
	s.mu.Unlock()
	return s.Store.BlockMeta(ctx, block)
}

func (s *warmCountingStore) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error) {
	s.mu.Lock()
	s.at(block).stateTree++
	s.mu.Unlock()
	return s.Store.LoadStateCellTree(ctx, block, rootHash)
}

type testWarmHead struct {
	head    ton.BlockIDExt
	known   ton.BlockIDExt
	root    *cell.Cell
	state   *cell.Cell
	backing *fakeStore
}

// testWarmHeadFixture builds a masterchain head that satisfies every warmed
// artifact at once: a block whose extra carries shard hashes (allShardsInfo),
// whose state carries the config (getConfigAll) and the previous masterchain
// blocks (the getBlockProof base), plus the preceding key block so a real
// getBlockProof query resolves against that base.
func testWarmHeadFixture(t *testing.T) testWarmHead {
	t.Helper()

	return testWarmHeadFixtureAt(t, 6)
}

func testWarmHeadFixtureAt(t *testing.T, seqno uint32) testWarmHead {
	t.Helper()

	catchainConfig := cell.BeginCell().MustStoreUInt(0x28, 8).EndCell()
	validatorConfig := cell.BeginCell().MustStoreUInt(0x34, 8).EndCell()
	keyCustom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    catchainConfig,
		int32(tlb.ConfigParamCurrentValidators): validatorConfig,
	})
	keyStateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 5}, nil)
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 5, 5, true, keyStateRoot, keyCustom)

	headRef := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: seqno}
	stateRoot := testMasterStateWithOldBlocks(t, headRef, []testOldMasterBlock{{id: keyID, isKey: true, endLT: 600}})

	shardState := cell.BeginCell().MustStoreUInt(0x55, 8).EndCell()
	shard, _ := testBlockForState(t, 0, masterchainShard, 3, shardState)
	headID, headRoot := testMasterBlockForStateWithPrevKey(t, seqno, 5, false, stateRoot, testMcBlockExtraWithShardHashes(t, testShardHashes(t, shard)))

	store := testBlockProofStore(t, keyID, keyRoot, headID, headRoot, stateRoot, testBlockProofSignatures(t))
	store.current = &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         headID,
			StateRootHash: stateRoot.Hash(0),
			Cell:          stateRoot,
			Parsed:        &tlb.ShardStateUnsplit{GenUTime: testWarmHeadUTime},
		},
	}

	return testWarmHead{head: headID, known: keyID, root: headRoot, state: stateRoot, backing: store}
}

// testWarmServer returns a server whose clock sits on the head's generation
// time, so the head counts as live for the warmer.
func testWarmServer(store Store) *Server {
	srv := testServer(store)
	srv.now = func() time.Time { return time.Unix(int64(testWarmHeadUTime), 0) }
	return srv
}

// warmHeadQueries are the canonical live-head client queries whose responses the
// warmer pre-builds, each paired with the store read its builder performs.
func warmHeadQueries(fixture testWarmHead) []struct {
	name  string
	query any
	reads func(warmStoreCounts) int
} {
	head := blockproof.CloneBlockID(fixture.head)
	known := blockproof.CloneBlockID(fixture.known)
	return []struct {
		name  string
		query any
		reads func(warmStoreCounts) int
	}{
		{
			name:  "block_header",
			query: ton.GetBlockHeader{ID: head, Mode: 0},
			reads: func(c warmStoreCounts) int { return c.blockRoot },
		},
		{
			name:  "all_shards_info",
			query: ton.GetAllShardsInfo{ID: head},
			reads: func(c warmStoreCounts) int { return c.blockRoot },
		},
		{
			name:  "config_all",
			query: ton.GetConfigAll{Mode: 0, BlockID: head},
			reads: func(c warmStoreCounts) int { return c.fragments },
		},
		{
			// Mode 0x1001 pins the reference base to the target block, which is
			// the head; the mode-default variants take the same base from the
			// current state, so both land on the warmed entry.
			name:  "block_proof_base",
			query: ton.GetBlockProof{Mode: 0x1001, KnownBlock: known, TargetBlock: head},
			reads: func(c warmStoreCounts) int { return c.stateTree },
		},
	}
}

// TestHeadWarmerServesFirstHeadQueryFromCache is the point of the whole change:
// after the warm, the first real client query for each artifact must not reach
// the builder. Every case is measured against an identical cold server so the
// seam is proven to move at all.
func TestHeadWarmerServesFirstHeadQueryFromCache(t *testing.T) {
	for _, tc := range warmHeadQueries(testWarmHeadFixture(t)) {
		t.Run(tc.name, func(t *testing.T) {
			fixture := testWarmHeadFixture(t)

			coldStore := newWarmCountingStore(fixture.backing)
			cold := testWarmServer(coldStore)
			coldBefore := tc.reads(coldStore.snapshot(fixture.head))
			coldResp := cold.handleQuery(t.Context(), tc.query)
			coldReads := tc.reads(coldStore.snapshot(fixture.head)) - coldBefore
			if coldReads == 0 {
				t.Fatalf("cold query performed no builder reads, the seam does not measure the build")
			}
			if lsErr, ok := coldResp.(ton.LSError); ok {
				t.Fatalf("cold query failed: %+v", lsErr)
			}

			warmFixture := testWarmHeadFixture(t)
			warmStore := newWarmCountingStore(warmFixture.backing)
			warm := testWarmServer(warmStore)
			warm.warmHead(t.Context(), warmFixture.head)

			warmBefore := tc.reads(warmStore.snapshot(warmFixture.head))
			if warmBefore == 0 {
				t.Fatalf("warm performed no builder reads for %s", tc.name)
			}
			warmResp := warm.handleQuery(t.Context(), tc.query)
			if warmReads := tc.reads(warmStore.snapshot(warmFixture.head)) - warmBefore; warmReads != 0 {
				t.Fatalf("warmed query performed %d builder reads, want 0 (cold: %d)", warmReads, coldReads)
			}
			if lsErr, ok := warmResp.(ton.LSError); ok {
				t.Fatalf("warmed query failed: %+v", lsErr)
			}
			if got, want := liteserverTypeName(warmResp), liteserverTypeName(coldResp); got != want {
				t.Fatalf("warmed response type = %s, want %s", got, want)
			}
		})
	}
}

// TestHeadWarmerRepeatedWarmRebuildsNothing pins that the warmer itself goes
// through the shared cache: warming the same head twice builds once.
func TestHeadWarmerRepeatedWarmRebuildsNothing(t *testing.T) {
	fixture := testWarmHeadFixture(t)
	store := newWarmCountingStore(fixture.backing)
	srv := testWarmServer(store)

	srv.warmHead(t.Context(), fixture.head)
	first := store.snapshot(fixture.head)
	srv.warmHead(t.Context(), fixture.head)

	if second := store.snapshot(fixture.head); second != first {
		t.Fatalf("second warm read the store again: %+v, want %+v", second, first)
	}
}

// TestHeadWarmerAbandonsSupersededHead pins the latest-wins rule at its
// coarsest: a head that is already superseded when the warm starts is not
// warmed at all.
func TestHeadWarmerAbandonsSupersededHead(t *testing.T) {
	fixture := testWarmHeadFixture(t)
	// Publishing a newer head makes MasterchainSeqnoReady(head+1) true.
	newer := fixture.head
	newer.SeqNo++
	fixture.backing.current = &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         newer,
			StateRootHash: fixture.state.Hash(0),
			Cell:          fixture.state,
			Parsed:        &tlb.ShardStateUnsplit{GenUTime: testWarmHeadUTime},
		},
	}

	store := newWarmCountingStore(fixture.backing)
	srv := testWarmServer(store)
	srv.warmHead(t.Context(), fixture.head)

	if counts := store.snapshot(fixture.head); counts != (warmStoreCounts{}) {
		t.Fatalf("superseded head was warmed: %+v", counts)
	}
	srv.respCache.mu.RLock()
	cached := len(srv.respCache.items)
	srv.respCache.mu.RUnlock()
	if cached != 0 {
		t.Fatalf("response cache holds %d entries after an abandoned warm, want 0", cached)
	}
}

// TestHeadWarmerAbandonsWarmWhenNewerHeadArrives pins the abandon check between
// steps: a head that is superseded mid-warm keeps the artifact it already
// built and drops the rest.
func TestHeadWarmerAbandonsWarmWhenNewerHeadArrives(t *testing.T) {
	fixture := testWarmHeadFixture(t)
	store := newWarmCountingStore(fixture.backing)
	store.started = make(chan struct{})
	store.gate = make(chan struct{})
	srv := testWarmServer(store)

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.warmHead(context.Background(), fixture.head)
	}()

	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("warm did not reach its first step")
	}

	newer := fixture.head
	newer.SeqNo++
	store.mu.Lock()
	fixture.backing.current = &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         newer,
			StateRootHash: fixture.state.Hash(0),
			Cell:          fixture.state,
			Parsed:        &tlb.ShardStateUnsplit{GenUTime: testWarmHeadUTime},
		},
	}
	store.mu.Unlock()
	close(store.gate)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("warm did not finish")
	}

	// The header proof was in flight and completes; nothing after it runs.
	counts := store.snapshot(fixture.head)
	if counts.blockRoot != 1 {
		t.Fatalf("block root reads = %d, want 1", counts.blockRoot)
	}
	if counts.fragments != 0 || counts.meta != 0 || counts.stateTree != 0 {
		t.Fatalf("abandoned warm kept building: %+v", counts)
	}
	srv.respCache.mu.RLock()
	cached := len(srv.respCache.items)
	srv.respCache.mu.RUnlock()
	if cached != 1 {
		t.Fatalf("response cache holds %d entries, want only the in-flight artifact", cached)
	}
}

// TestHeadWarmerSkipsHeadsThatAreNotAtTip pins the no-op paths: a head the store
// cannot date, a head far behind the clock, and no head at all are all skipped.
func TestHeadWarmerSkipsHeadsThatAreNotAtTip(t *testing.T) {
	tests := []struct {
		name    string
		current func(fixture testWarmHead) *storage.CurrentState
		now     time.Time
	}{
		{
			name:    "no current state",
			current: func(testWarmHead) *storage.CurrentState { return nil },
			now:     time.Unix(int64(testWarmHeadUTime), 0),
		},
		{
			name: "undated head",
			current: func(fixture testWarmHead) *storage.CurrentState {
				return &storage.CurrentState{Masterchain: storage.BlockState{
					Block:         fixture.head,
					StateRootHash: fixture.state.Hash(0),
					Cell:          fixture.state,
				}}
			},
			now: time.Unix(int64(testWarmHeadUTime), 0),
		},
		{
			name:    "catching up",
			current: func(fixture testWarmHead) *storage.CurrentState { return fixture.backing.current },
			now:     time.Unix(int64(testWarmHeadUTime)+int64(headWarmMaxLag/time.Second)+1, 0),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := testWarmHeadFixture(t)
			fixture.backing.current = tc.current(fixture)
			store := newWarmCountingStore(fixture.backing)
			srv := testWarmServer(store)
			srv.now = func() time.Time { return tc.now }

			if srv.headWarmTarget(t.Context()).live {
				t.Fatal("head reported as live")
			}
			if counts := store.snapshot(fixture.head); counts != (warmStoreCounts{}) {
				t.Fatalf("skipped head was read: %+v", counts)
			}
		})
	}
}

// TestHeadWarmerWarmsEveryPublishedHead runs the real loop against the live
// store and pins that it wakes on each publish and warms the newest head.
func TestHeadWarmerWarmsEveryPublishedHead(t *testing.T) {
	first := testWarmHeadFixture(t)
	live := NewLiveStore(&fakeStore{})
	store := newWarmCountingStore(live)
	srv := testWarmServer(store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.runHeadWarmer(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("head warmer did not stop")
		}
	}()

	publish := func(block ton.BlockIDExt, root *cell.Cell, state *cell.Cell) {
		t.Helper()
		if err := live.publishLiveBlockData(block, root, testBlockBOC(root), true); err != nil {
			t.Fatalf("publish live master block %d: %v", block.SeqNo, err)
		}
		live.SetLiveCurrentState(&storage.CurrentState{Masterchain: storage.BlockState{
			Block:         block,
			StateRootHash: state.Hash(0),
			Cell:          state,
			Parsed:        &tlb.ShardStateUnsplit{GenUTime: testWarmHeadUTime},
		}})
	}

	waitWarmed := func(block ton.BlockIDExt) {
		t.Helper()
		key := liteResponseKey{kind: liteResponseBlockHeader, a: liteBlockKeyFromBlock(block)}
		deadline := time.Now().Add(5 * time.Second)
		for {
			srv.respCache.mu.RLock()
			_, ok := srv.respCache.items[key]
			srv.respCache.mu.RUnlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("head %d was not warmed", block.SeqNo)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// The warm must follow the publish directly. Anything near the idle backoff
	// means the loop slept through a notify instead of waking on it.
	started := time.Now()
	publish(first.head, first.root, first.state)
	waitWarmed(first.head)

	second := testWarmHeadFixtureAt(t, 7)
	publish(second.head, second.root, second.state)
	waitWarmed(second.head)
	if elapsed := time.Since(started); elapsed >= headWarmIdleBackoff {
		t.Fatalf("warming two published heads took %s, want well under %s", elapsed, headWarmIdleBackoff)
	}

	// Both heads keep their own entries and neither was warmed twice: the block
	// header proof and allShardsInfo each read the block root exactly once.
	const wantBlockRootReads = 2
	if counts := store.snapshot(first.head); counts.blockRoot != wantBlockRootReads {
		t.Fatalf("first head block root reads = %d, want %d", counts.blockRoot, wantBlockRootReads)
	}
	if counts := store.snapshot(second.head); counts.blockRoot != wantBlockRootReads {
		t.Fatalf("second head block root reads = %d, want %d", counts.blockRoot, wantBlockRootReads)
	}
}

// TestHeadWarmerStopsWithoutHead pins that the loop parks instead of spinning
// when no head is published, and unparks on shutdown.
func TestHeadWarmerStopsWithoutHead(t *testing.T) {
	live := NewLiveStore(&fakeStore{})
	store := newWarmCountingStore(live)
	srv := testWarmServer(store)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.runHeadWarmer(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("head warmer did not stop")
	}

	srv.respCache.mu.RLock()
	cached := len(srv.respCache.items)
	srv.respCache.mu.RUnlock()
	if cached != 0 {
		t.Fatalf("response cache holds %d entries without a head, want 0", cached)
	}
}
