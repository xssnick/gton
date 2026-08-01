package service

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func testPreparedShardBlockID(seqno uint32) ton.BlockIDExt {
	rootHash := make([]byte, 32)
	binary.BigEndian.PutUint32(rootHash, seqno)
	fileHash := make([]byte, 32)
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}
}

func TestPreparedShardBlockCacheTakeAndDedup(t *testing.T) {
	var cache preparedShardBlockCache
	block := testPreparedShardBlockID(1)

	if !cache.beginPrepare(block) {
		t.Fatal("first beginPrepare rejected")
	}
	if cache.beginPrepare(block) {
		t.Fatal("beginPrepare accepted while inflight")
	}

	cache.storePrepared(PreparedBlock{ID: block, BlockBOC: []byte{1}})
	if cache.beginPrepare(block) {
		t.Fatal("beginPrepare accepted while cached")
	}

	got, err := cache.take(block)
	if err != nil {
		t.Fatal("prepared block not taken")
	}
	if !got.ID.Equals(&block) {
		t.Fatalf("taken block %s, want seqno %d", got.BlockRef(), block.SeqNo)
	}
	if _, err = cache.take(block); err == nil {
		t.Fatal("prepared block taken twice")
	}

	if !cache.beginPrepare(block) {
		t.Fatal("beginPrepare rejected after take")
	}
	cache.abortPrepare(block)
	if !cache.beginPrepare(block) {
		t.Fatal("beginPrepare rejected after abort")
	}
}

// countingBlockFullStore records every fallback load the resolver would pay
// for when the pre-prepared entry is missed.
type countingBlockFullStore struct {
	tnstore.Storage
	blockFullCalls int
}

func (s *countingBlockFullStore) BlockFull(context.Context, ton.BlockIDExt) (*tnstore.ServedBlockFull, error) {
	s.blockFullCalls++
	return nil, tnstore.ErrNotFound
}

// testBroadcastShardBlock builds the decoded shard broadcast shape the p2p
// accept path hands to the observer: verified root hash, parsed root, meta and
// a real merkle update the prepare walks.
func testBroadcastShardBlock(t *testing.T, seqno uint32) *p2p.DownloadedBlock {
	t.Helper()

	root := cell.BeginCell().
		MustStoreUInt(uint64(seqno), 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()).
		EndCell()
	update := mustMerkleUpdateCell(t, root, root)
	id := testBlockID(0, topShard, seqno)

	return &p2p.DownloadedBlock{
		ID:               id,
		Kind:             "tonNode.blockBroadcastCompressedV2",
		Block:            root,
		BlockBOC:         root.ToBOC(),
		Meta:             &tnstore.BlockMeta{ID: id},
		StateUpdate:      update,
		VerifiedRootHash: true,
	}
}

func waitPreparedShardBlock(t *testing.T, svc *Service, block ton.BlockIDExt) {
	t.Helper()

	key := tnstore.BlockKey(block)
	deadline := time.Now().Add(5 * time.Second)
	for {
		svc.preparedShardBlocks.mu.Lock()
		_, ok := svc.preparedShardBlocks.entries[key]
		svc.preparedShardBlocks.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("shard block broadcast was never pre-prepared")
		}
		time.Sleep(time.Millisecond)
	}
}

// A decoded shard broadcast must reach the pre-prepare workers through the
// block-received observer, so the resolver's load path takes ready
// state-update cells instead of paying PrepareStateUpdateCells inline.
func TestBroadcastShardBlockIsPreparedAheadAndTakenByResolverLoad(t *testing.T) {
	store := &countingBlockFullStore{}
	svc := &Service{
		log:               zerolog.Nop(),
		storage:           store,
		shardPrepareQueue: make(chan shardPrepareRequest, preparedShardBlockQueueSize),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workers := make(chan struct{})
	go func() {
		defer close(workers)
		svc.runShardPrepareWorkers(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-workers
	})

	downloaded := testBroadcastShardBlock(t, 301)
	svc.ObserveBlockReceived(ctx, p2p.BlockReceivedEvent{
		IsSigned:      true,
		FromBroadcast: true,
		Downloaded:    downloaded,
	})
	waitPreparedShardBlock(t, svc, downloaded.ID)

	prepared, err := svc.loadOrDownloadBlockForApply(ctx, downloaded.ID)
	if err != nil {
		t.Fatalf("resolver load: %v", err)
	}
	if prepared.BlockRoot != downloaded.Block {
		t.Fatal("resolver load re-parsed the block instead of taking the pre-prepared entry")
	}
	if prepared.StateUpdateToCells.Len() == 0 {
		t.Fatal("pre-prepared entry carries no state update cells")
	}
	if store.blockFullCalls != 0 {
		t.Fatalf("resolver load paid %d stored-block loads, want 0", store.blockFullCalls)
	}

	// the entry is consumed by the take, so it cannot be applied twice
	if _, err = svc.preparedShardBlocks.take(downloaded.ID); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("prepared entry survived the resolver take: %v", err)
	}
}

// One preparation per block: the description-hint prefetch and the broadcast
// feed race for the same blocks, and the prepared store is the single dedupe.
func TestShardPrepareFeedsDedupeAcrossSources(t *testing.T) {
	svc := &Service{
		log:               zerolog.Nop(),
		shardPrepareQueue: make(chan shardPrepareRequest, preparedShardBlockQueueSize),
	}
	block := testBlockID(0, topShard, 302)

	svc.prepareShardBlockAheadByID(context.Background(), block)
	if got := len(svc.shardPrepareQueue); got != 1 {
		t.Fatalf("queued prepare requests = %d, want 1", got)
	}

	// a worker reserved the block: later feeds for it must not queue again
	if !svc.preparedShardBlocks.beginPrepare(block) {
		t.Fatal("beginPrepare rejected the first reservation")
	}
	svc.enqueueShardBlockPrepare(shardPrepareRequest{block: block})
	if got := len(svc.shardPrepareQueue); got != 1 {
		t.Fatalf("in-flight block was queued again: %d requests", got)
	}

	svc.preparedShardBlocks.storePrepared(PreparedBlock{ID: block, BlockBOC: []byte{1}})
	svc.enqueueShardBlockPrepare(shardPrepareRequest{block: block})
	if got := len(svc.shardPrepareQueue); got != 1 {
		t.Fatalf("prepared block was queued again: %d requests", got)
	}
}

// The feed never blocks its caller: broadcast accept paths and the serial
// block processor both enqueue, and a full queue refuses the newest request.
func TestShardPrepareQueueRefusesNewestWhenFull(t *testing.T) {
	svc := &Service{
		log:               zerolog.Nop(),
		shardPrepareQueue: make(chan shardPrepareRequest, preparedShardBlockQueueSize),
	}

	for i := 0; i < preparedShardBlockQueueSize+4; i++ {
		svc.enqueueShardBlockPrepare(shardPrepareRequest{block: testBlockID(0, topShard, uint32(400+i))})
	}
	if got := len(svc.shardPrepareQueue); got != preparedShardBlockQueueSize {
		t.Fatalf("queued requests = %d, want %d", got, preparedShardBlockQueueSize)
	}

	first := <-svc.shardPrepareQueue
	if first.block.SeqNo != 400 {
		t.Fatalf("queue head seqno = %d, want the oldest 400", first.block.SeqNo)
	}
}

// Masterchain blocks are advanced by the sync pipeline itself and must never
// occupy a shard prepare slot.
func TestShardPrepareIgnoresMasterchainBroadcast(t *testing.T) {
	svc := &Service{
		log:               zerolog.Nop(),
		shardPrepareQueue: make(chan shardPrepareRequest, preparedShardBlockQueueSize),
	}
	master := testBlockID(-1, topShard, 303)

	svc.ObserveBlockReceived(context.Background(), p2p.BlockReceivedEvent{
		IsSigned:      true,
		FromBroadcast: true,
		Downloaded:    &p2p.DownloadedBlock{ID: master},
	})
	if got := len(svc.shardPrepareQueue); got != 0 {
		t.Fatalf("masterchain broadcast queued %d prepare requests, want 0", got)
	}
}

// Broadcast-fed entries whose master never arrives must not pin memory: they
// expire by TTL on both the take and the next prepare's prune.
func TestPreparedShardBlockCacheExpiresStaleEntries(t *testing.T) {
	var cache preparedShardBlockCache
	stale := testPreparedShardBlockID(700)
	fresh := testPreparedShardBlockID(701)

	cache.storePrepared(PreparedBlock{ID: stale, BlockBOC: []byte{1}})
	cache.mu.Lock()
	entry := cache.entries[tnstore.BlockKey(stale)]
	entry.storedAt = time.Now().Add(-2 * preparedShardBlockTTL)
	cache.entries[tnstore.BlockKey(stale)] = entry
	cache.mu.Unlock()

	if _, err := cache.take(stale); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("expired entry was served: %v", err)
	}

	// re-store it and let the next prepare's prune reclaim it instead
	cache.storePrepared(PreparedBlock{ID: stale, BlockBOC: []byte{1}})
	cache.mu.Lock()
	entry = cache.entries[tnstore.BlockKey(stale)]
	entry.storedAt = time.Now().Add(-2 * preparedShardBlockTTL)
	cache.entries[tnstore.BlockKey(stale)] = entry
	cache.mu.Unlock()

	cache.storePrepared(PreparedBlock{ID: fresh, BlockBOC: []byte{2}})
	cache.mu.Lock()
	_, stalePresent := cache.entries[tnstore.BlockKey(stale)]
	bytes := cache.bytes
	cache.mu.Unlock()
	if stalePresent {
		t.Fatal("expired entry survived the prune")
	}
	if bytes <= 0 {
		t.Fatalf("cache byte accounting = %d after pruning, want the fresh entry only", bytes)
	}
}

func TestPreparedShardBlockCacheEvictsOldest(t *testing.T) {
	var cache preparedShardBlockCache

	for i := uint32(0); i < preparedShardBlockMaxItems+1; i++ {
		cache.storePrepared(PreparedBlock{ID: testPreparedShardBlockID(i), BlockBOC: []byte{byte(i)}})
	}

	if _, err := cache.take(testPreparedShardBlockID(0)); err == nil {
		t.Fatal("oldest entry survived eviction")
	}
	if _, err := cache.take(testPreparedShardBlockID(preparedShardBlockMaxItems)); err != nil {
		t.Fatal("newest entry evicted")
	}
}
