package liveview

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestStoreCheckpointFlushDoesNotRememberMissingBlocks(t *testing.T) {
	missing := ton.BlockIDExt{
		Workchain: 0,
		Shard:     1 << 3,
		SeqNo:     35,
		RootHash:  bytes.Repeat([]byte{0x35}, 32),
		FileHash:  bytes.Repeat([]byte{0x36}, 32),
	}
	live := New(noopBacking{})

	live.MarkLiveBlockStatesFlushed([]ton.BlockIDExt{missing})
	if len(live.flushed) != 0 {
		t.Fatalf("missing checkpoint-flushed blocks remembered = %d, want 0", len(live.flushed))
	}
}

func TestStorePublishBlockWithStateMaintainsFinalIndexesAndEvictability(t *testing.T) {
	block := testLiveBlockID(0, int64(1)<<62, 36, 0x36)
	state := storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Repeat([]byte{0x37}, 32),
		StateFileHash: bytes.Repeat([]byte{0x38}, 32),
	}
	meta := &storage.BlockMeta{
		ID:       block,
		GenUTime: 1_720_000_000,
		StartLT:  100,
		EndLT:    200,
	}
	live := New(noopBacking{})

	err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:            block,
		Meta:             meta,
		State:            &state,
		ArtifactFlushed:  true,
		AvailabilityOnly: true,
	})
	if err != nil {
		t.Fatalf("publish block with state: %v", err)
	}

	blockKey := storage.BlockKey(block)
	stateKey, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		t.Fatal("test block has an incomplete id")
	}
	if cached := live.blocks[blockKey]; cached == nil || !blockIDEqual(cached.id, block) {
		t.Fatal("published block is missing")
	}
	if cached, ok := live.states[stateKey]; !ok || !blockIDEqual(cached.Block, block) {
		t.Fatal("published block state is missing")
	}
	cachedMeta := live.metas[blockKey]
	if cachedMeta == nil || cachedMeta.StartLT != meta.StartLT || cachedMeta.EndLT != meta.EndLT || cachedMeta.GenUTime != meta.GenUTime {
		t.Fatalf("indexed meta = %+v, want merged block meta", cachedMeta)
	}
	seqKey := liveSeqKey{workchain: block.Workchain, shard: block.Shard, seqno: block.SeqNo}
	if indexed, ok := live.seqIndex[seqKey]; !ok || !blockIDEqual(indexed, block) {
		t.Fatal("block is missing from seqno index")
	}
	historyKey := liveHistoryKey{workchain: block.Workchain, shard: block.Shard}
	if entries := live.ltIndex[historyKey]; len(entries) != 1 || !blockIDEqual(entries[0].block, block) {
		t.Fatalf("LT index entries = %d, want the published block", len(entries))
	}
	if entries := live.unixIndex[historyKey]; len(entries) != 1 || !blockIDEqual(entries[0].block, block) {
		t.Fatalf("unix index entries = %d, want the published block", len(entries))
	}
	if live.shardEvictable != 0 {
		t.Fatalf("evictable shard blocks = %d, want 0 for unflushed state", live.shardEvictable)
	}

	live.MarkLiveBlockStatesFlushed([]ton.BlockIDExt{block})
	if live.shardEvictable != 1 {
		t.Fatalf("evictable shard blocks after state flush = %d, want 1", live.shardEvictable)
	}
}

func TestStoreRememberBlockStateWithoutLiveBlock(t *testing.T) {
	block := testLiveBlockID(0, int64(1)<<62, 37, 0x37)
	state := storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Repeat([]byte{0x38}, 32),
		StateFileHash: bytes.Repeat([]byte{0x39}, 32),
	}
	live := New(noopBacking{})

	live.rememberBlockStateLocked(state)

	blockKey := storage.BlockKey(block)
	if live.blocks[blockKey] != nil {
		t.Fatal("state-only publish created a live block")
	}
	stateKey, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		t.Fatal("test block has an incomplete id")
	}
	if cached, ok := live.states[stateKey]; !ok || !blockIDEqual(cached.Block, block) {
		t.Fatal("state-only publish did not remember the state")
	}
	if meta := live.metas[blockKey]; meta == nil || !blockIDEqual(meta.ID, block) {
		t.Fatal("state-only publish did not index state metadata")
	}
	seqKey := liveSeqKey{workchain: block.Workchain, shard: block.Shard, seqno: block.SeqNo}
	if indexed, ok := live.seqIndex[seqKey]; !ok || !blockIDEqual(indexed, block) {
		t.Fatal("state-only publish did not update the seqno index")
	}
}

func TestStorePublishRejectsMismatchedBlockState(t *testing.T) {
	block := testLiveBlockID(0, int64(1)<<62, 38, 0x38)
	state := storage.BlockState{Block: testLiveBlockID(0, int64(1)<<62, 39, 0x39)}
	live := New(noopBacking{})

	err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:            block,
		State:            &state,
		AvailabilityOnly: true,
	})
	if err == nil {
		t.Fatal("mismatched block state was accepted")
	}
	if len(live.blocks) != 0 || len(live.states) != 0 || len(live.metas) != 0 {
		t.Fatal("mismatched block state changed the live view")
	}
}

func TestStorePublishLiveBlockArtifactsRejectsInvalidReadFragments(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
	stateRoot := cell.BeginCell().MustStoreUInt(0x32, 8).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     1 << 62,
		SeqNo:     35,
		RootHash:  root.Hash(),
		FileHash:  bytes.Repeat([]byte{0x36}, 32),
	}
	state := storage.BlockState{
		Block:         block,
		StateRootHash: stateRoot.Hash(),
		Cell:          stateRoot,
	}
	artifacts := storage.LiveBlockArtifacts{
		Block: block,
		Root:  root,
		State: &state,
	}

	t.Run("read artifacts", func(t *testing.T) {
		live := New(noopBacking{})

		if err := live.PublishLiveBlockArtifacts(artifacts); err == nil {
			t.Fatal("unparseable read fragments were accepted")
		}
		if _, err := live.BlockRoot(t.Context(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("block root after rejected publish error = %v, want ErrNotFound", err)
		}
		if _, err := live.BlockState(t.Context(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("block state after rejected publish error = %v, want ErrNotFound", err)
		}
	})

	t.Run("availability only", func(t *testing.T) {
		live := New(noopBacking{})
		availabilityArtifacts := artifacts
		availabilityArtifacts.AvailabilityOnly = true

		if err := live.PublishLiveBlockArtifacts(availabilityArtifacts); err != nil {
			t.Fatalf("publish availability-only artifacts: %v", err)
		}
		if _, err := live.BlockRoot(t.Context(), block); err != nil {
			t.Fatalf("load availability-only block root: %v", err)
		}
	})
}

func TestStoreReleasesCurrentCachesWhenBlockLeavesCurrent(t *testing.T) {
	retiredBlock := testLiveBlockID(-1, masterchainShard, 40, 0x40)
	currentBlock := testLiveBlockID(-1, masterchainShard, 41, 0x41)
	keptShard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)
	proof := cell.BeginCell().MustStoreUInt(1, 1).EndCell()

	retired := &BlockView{
		retainCurrentCaches: true,
		accountProofs: map[accountProofKey]accountProofValue{
			{}: {proof: []*cell.Cell{proof}},
		},
		shardHashesProofs: map[shardHashesProofKey]*cell.Cell{{}: proof},
		externalMsgAccounts: map[externalMessageAccountKey]externalMessageAccountValue{
			{}: {},
		},
	}
	kept := &BlockView{
		retainCurrentCaches: true,
		accountProofs: map[accountProofKey]accountProofValue{
			{}: {proof: []*cell.Cell{proof}},
		},
		externalMsgAccounts: map[externalMessageAccountKey]externalMessageAccountValue{
			{}: {},
		},
	}

	live := New(noopBacking{})
	live.blocks[storage.BlockKey(retiredBlock)] = &liveBlock{id: retiredBlock, fragments: retired}
	live.blocks[storage.BlockKey(keptShard)] = &liveBlock{id: keptShard, fragments: kept}
	previous := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: retiredBlock},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(keptShard): {Block: keptShard},
		},
	}
	next := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: currentBlock},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(keptShard): {Block: keptShard},
		},
	}

	live.releaseRetiredCurrentCachesLocked(previous, next)

	if retired.retainCurrentCaches || retired.accountProofs != nil || retired.shardHashesProofs != nil || retired.externalMsgAccounts != nil {
		t.Fatal("retired block retained current-view caches")
	}
	if !kept.retainCurrentCaches || len(kept.accountProofs) != 1 || len(kept.externalMsgAccounts) != 1 {
		t.Fatal("current shard cache was released")
	}

	retired.mu.Lock()
	retired.rememberAccountProofStateLocked(accountProofKey{}, []*cell.Cell{proof}, proof, false)
	retired.mu.Unlock()
	if retired.accountProofs != nil {
		t.Fatal("retired block repopulated its account proof cache")
	}
}

func TestStoreRequiresFullBlockIDForRootKeyedCaches(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()
	block := testLiveBlockID(0, 1<<62, 51, 0x51)
	block.RootHash = root.Hash()
	forged := block
	forged.FileHash = bytes.Repeat([]byte{0xff}, 32)
	fragments := &BlockView{}
	meta := &storage.BlockMeta{ID: block}

	live := New(noopBacking{})
	key := storage.BlockKey(block)
	live.blocks[key] = &liveBlock{id: block, root: root, meta: meta, fragments: fragments}
	live.metas[key] = meta

	if _, err := live.BlockRoot(t.Context(), forged); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged block root error = %v, want ErrNotFound", err)
	}
	if _, err := live.BlockMeta(t.Context(), forged); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged block meta error = %v, want ErrNotFound", err)
	}
	if _, err := live.BlockFragments(t.Context(), forged); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged block fragments error = %v, want ErrNotFound", err)
	}

	if got, err := live.BlockRoot(t.Context(), block); err != nil || got != root {
		t.Fatalf("canonical block root = %p, %v, want %p, nil", got, err, root)
	}
	if got, err := live.BlockMeta(t.Context(), block); err != nil || got != meta {
		t.Fatalf("canonical block meta = %p, %v, want %p, nil", got, err, meta)
	}
	if got, err := live.BlockFragments(t.Context(), block); err != nil || got != fragments {
		t.Fatalf("canonical block fragments = %p, %v, want %p, nil", got, err, fragments)
	}
}

func TestStoreDoesNotSingleflightDifferentFullBlockIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	block := testLiveBlockID(0, 1<<62, 61, 0x61)
	forged := block
	forged.FileHash = bytes.Repeat([]byte{0xff}, 32)
	backing := &blockingBlockDataBacking{
		canonical: block,
		data:      []byte{0x61},
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	live := New(backing)
	defer func() {
		select {
		case <-backing.release:
		default:
			close(backing.release)
		}
	}()

	type result struct {
		data []byte
		err  error
	}
	canonicalResult := make(chan result, 1)
	go func() {
		data, err := live.BlockData(ctx, block)
		canonicalResult <- result{data: data, err: err}
	}()

	select {
	case <-backing.started:
	case <-ctx.Done():
		t.Fatalf("canonical block load did not start: %v", ctx.Err())
	}
	forgedResult := make(chan result, 1)
	go func() {
		data, err := live.BlockData(ctx, forged)
		forgedResult <- result{data: data, err: err}
	}()

	select {
	case got := <-forgedResult:
		if !errors.Is(got.err, storage.ErrNotFound) {
			t.Fatalf("forged concurrent block data = %x, %v, want ErrNotFound", got.data, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("forged block load joined the canonical full block id: %v", ctx.Err())
	}

	close(backing.release)
	got := <-canonicalResult
	if got.err != nil || !bytes.Equal(got.data, backing.data) {
		t.Fatalf("canonical block data = %x, %v, want %x, nil", got.data, got.err, backing.data)
	}
}

func testLiveBlockID(workchain int32, shard int64, seqno uint32, fill byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{fill}, 32),
		FileHash:  bytes.Repeat([]byte{fill + 1}, 32),
	}
}

type noopBacking struct{}

func (noopBacking) BlockData(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) BlockProof(context.Context, storage.ServedProofKind, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) ZeroState(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) CurrentState(context.Context) (*storage.CurrentState, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) LookupBlockBySeqNo(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockBySeqNoForPrefix(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByLT(context.Context, storage.BlockHistoryKey, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByLTForPrefix(context.Context, storage.BlockHistoryKey, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByAccountLT(context.Context, int32, []byte, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByUnixTime(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByUnixTimeForPrefix(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LazyCellLoader() cell.LazyCellLoader {
	return nil
}

type blockingBlockDataBacking struct {
	noopBacking
	canonical ton.BlockIDExt
	data      []byte
	started   chan struct{}
	release   chan struct{}
}

func (b *blockingBlockDataBacking) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	if !blockIDEqual(block, b.canonical) {
		return nil, storage.ErrNotFound
	}

	close(b.started)
	select {
	case <-b.release:
		return b.data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func testFragmentBlockView(block ton.BlockIDExt) *BlockView {
	return &BlockView{block: cloneBlockID(block), retainCurrentCaches: true}
}

// The view build doubles as the structural check on the block root and state,
// so it stays synchronous: turning on background prewarm workers must not make
// an unpublishable block publishable.
func TestStorePublishStillRejectsInvalidFragmentsWithPrewarmWorkers(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
	stateRoot := cell.BeginCell().MustStoreUInt(0x32, 8).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     1 << 62,
		SeqNo:     35,
		RootHash:  root.Hash(),
		FileHash:  bytes.Repeat([]byte{0x36}, 32),
	}
	state := storage.BlockState{Block: block, StateRootHash: stateRoot.Hash(), Cell: stateRoot}

	live := New(noopBacking{}, Options{FragmentBuildWorkers: 2})
	err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block: block,
		Root:  root,
		State: &state,
	})
	if err == nil {
		t.Fatal("unparseable read fragments were accepted with prewarm workers enabled")
	}
	if _, err := live.BlockRoot(t.Context(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block root after rejected publish = %v, want ErrNotFound", err)
	}
}

func TestStoreFragmentPrewarmSlotsSeparateMasterFromShards(t *testing.T) {
	master := testLiveBlockID(-1, masterchainShard, 41, 0x41)
	shard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)

	inline := New(noopBacking{})
	if inline.fragmentPrewarmSlots(master) != nil || inline.fragmentPrewarmSlots(shard) != nil {
		t.Fatal("prewarm slots exist without configured workers")
	}

	live := New(noopBacking{}, Options{FragmentBuildWorkers: 1})
	masterSlots := live.fragmentPrewarmSlots(master)
	shardSlots := live.fragmentPrewarmSlots(shard)
	if masterSlots == nil || shardSlots == nil {
		t.Fatal("prewarm slots missing with configured workers")
	}
	if masterSlots == shardSlots {
		// A shard burst arrives from the shard apply workers and would otherwise
		// crowd out the masterchain view, which is the one whose prewarm covers
		// the config epoch and global libraries.
		t.Fatal("masterchain prewarm shares the shard slots")
	}
}

// A publish must never block on a busy prewarm slot: that would put the cost
// back on the block apply path. The cold view is installed instead.
func TestStoreFragmentPrewarmPublishesColdViewWhenSlotsAreBusy(t *testing.T) {
	shard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)
	live := New(noopBacking{}, Options{FragmentBuildWorkers: 1})
	live.blocks[storage.BlockKey(shard)] = &liveBlock{id: shard}

	live.fragmentBuildSlots <- struct{}{}
	defer func() { <-live.fragmentBuildSlots }()

	view := testFragmentBlockView(shard)
	done := make(chan struct{})
	go func() {
		defer close(done)
		live.scheduleFragmentPrewarm(shard, view)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduleFragmentPrewarm blocked on a busy slot")
	}

	cached, err := live.cachedBlockFragments(shard)
	if err != nil {
		t.Fatalf("cold view was not published: %v", err)
	}
	if cached != view {
		t.Fatal("published view is not the one handed to the prewarm")
	}
}

func TestStoreFragmentPrewarmInstallsView(t *testing.T) {
	shard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)
	live := New(noopBacking{}, Options{FragmentBuildWorkers: 1})
	live.blocks[storage.BlockKey(shard)] = &liveBlock{id: shard}

	view := testFragmentBlockView(shard)
	live.scheduleFragmentPrewarm(shard, view)

	deadline := time.Now().Add(2 * time.Second)
	for {
		cached, err := live.cachedBlockFragments(shard)
		if err == nil {
			if cached != view {
				t.Fatal("installed view is not the one handed to the prewarm")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("background prewarm never installed the view")
		}
		time.Sleep(time.Millisecond)
	}
}

// A block can leave the current state while its view is still being prewarmed;
// the view installed afterwards must not start filling the per-current caches.
func TestStoreFragmentInstallHonoursReleasedCurrentCaches(t *testing.T) {
	shard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)
	live := New(noopBacking{})
	live.blocks[storage.BlockKey(shard)] = &liveBlock{id: shard}

	previous := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testLiveBlockID(-1, masterchainShard, 40, 0x40)},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard): {Block: shard},
		},
	}
	next := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testLiveBlockID(-1, masterchainShard, 41, 0x41)},
	}
	live.releaseRetiredCurrentCachesLocked(previous, next)

	view := testFragmentBlockView(shard)
	if got := live.rememberBlockFragments(shard, view); got != view {
		t.Fatal("view was not installed")
	}
	view.mu.Lock()
	retained := view.retainCurrentCaches
	view.mu.Unlock()
	if retained {
		t.Fatal("view installed after the block was retired still retains current caches")
	}
}

func TestStoreFragmentInstallSkipsEvictedBlock(t *testing.T) {
	shard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)
	live := New(noopBacking{})

	view := testFragmentBlockView(shard)
	if got := live.rememberBlockFragments(shard, view); got != view {
		t.Fatal("rememberBlockFragments did not return the caller's view for an absent block")
	}
	if len(live.blocks) != 0 {
		t.Fatalf("live blocks after installing onto an absent block = %d, want 0", len(live.blocks))
	}
}

// cachedBlockFragments used to read the field after releasing the read lock,
// which raced rememberBlockFragments. Background prewarm makes that write
// happen on every publish, so the read must stay inside the lock.
func TestCachedBlockFragmentsIsRaceFreeAgainstInstall(t *testing.T) {
	shard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)
	live := New(noopBacking{})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			live.mu.Lock()
			live.blocks[storage.BlockKey(shard)] = &liveBlock{id: shard}
			live.mu.Unlock()
			live.rememberBlockFragments(shard, testFragmentBlockView(shard))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = live.cachedBlockFragments(shard)
		}
	}()
	wg.Wait()
}

// masterchainHistoryKey is the history key every masterchain lookup lands on.
var masterchainHistoryKey = storage.BlockHistoryKey{Workchain: masterchainID, Shard: -0x8000000000000000}

func seedLiveHistory(live *Store, metas ...*storage.BlockMeta) {
	live.mu.Lock()
	defer live.mu.Unlock()
	for _, meta := range metas {
		live.addMetaHistoryIndexLocked(meta)
	}
}

func testHistoryMeta(key storage.BlockHistoryKey, seqno uint32, startLT, endLT uint64, utime uint32) *storage.BlockMeta {
	return &storage.BlockMeta{
		ID: ton.BlockIDExt{
			Workchain: key.Workchain,
			Shard:     key.Shard,
			SeqNo:     seqno,
			RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
			FileHash:  bytes.Repeat([]byte{byte(seqno + 1)}, 32),
		},
		StartLT:  startLT,
		EndLT:    endLT,
		GenUTime: utime,
	}
}

// The live index is a sliding window, so a hit whose predecessor is missing
// from it says nothing about blocks older than the window: the answer has to
// come from the backing store instead. Skipping that check for the masterchain
// made lookupBlock hand clients the oldest cached master for any lt or utime
// below the window.
func TestCachedDirectLookupRequiresLiveWindowCoverage(t *testing.T) {
	shardKey := storage.BlockHistoryKey{Workchain: 0, Shard: 1 << 62}

	for _, tc := range []struct {
		name string
		key  storage.BlockHistoryKey
	}{
		{name: "masterchain", key: masterchainHistoryKey},
		{name: "shard", key: shardKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := New(noopBacking{})
			seedLiveHistory(live,
				testHistoryMeta(tc.key, 100, 2000, 2009, 2000),
				testHistoryMeta(tc.key, 101, 2010, 2019, 2010),
			)

			if _, err := live.cachedDirectBlockByLTForPrefix(tc.key, 1000); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("lt below the window: err = %v, want ErrNotFound", err)
			}
			if _, err := live.cachedDirectBlockByUnixTimeForPrefix(tc.key, 1000); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("utime below the window: err = %v, want ErrNotFound", err)
			}

			// Covered lookups must still be answered from the window, or the
			// live fast path would be gone.
			block, err := live.cachedDirectBlockByLTForPrefix(tc.key, 2015)
			if err != nil {
				t.Fatalf("lt inside the window: unexpected error %v", err)
			}
			if block.SeqNo != 101 {
				t.Fatalf("lt inside the window: seqno = %d, want 101", block.SeqNo)
			}
			block, err = live.cachedDirectBlockByUnixTimeForPrefix(tc.key, 2005)
			if err != nil {
				t.Fatalf("utime inside the window: unexpected error %v", err)
			}
			if block.SeqNo != 101 {
				t.Fatalf("utime inside the window: seqno = %d, want 101", block.SeqNo)
			}
		})
	}
}
