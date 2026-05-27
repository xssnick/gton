package p2p

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestShardBroadcastCachePopConsumesBlock(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 16)
	downloaded := testShardBroadcastDownloadedBlock(t, 10, 0x10)
	meta := &tnstore.BlockMeta{ID: downloaded.ID, GenUTime: 123}

	if err := cache.Store(downloaded, meta); err != nil {
		t.Fatalf("store block: %v", err)
	}

	got, err := cache.PopBlock(downloaded.ID)
	if err != nil {
		t.Fatalf("pop block: %v", err)
	}
	if !got.ID.Equals(&downloaded.ID) {
		t.Fatalf("block = %s, want %s", formatBlockRef(got.ID), formatBlockRef(downloaded.ID))
	}
	if got.Kind != downloaded.Kind {
		t.Fatalf("kind = %q, want %q", got.Kind, downloaded.Kind)
	}
	if got.Block == nil {
		t.Fatal("cached block root was not decoded")
	}
	if got.Proof == nil {
		t.Fatal("cached proof root was not decoded")
	}
	if got.Block != downloaded.Block {
		t.Fatal("cached block root was reparsed instead of reused")
	}
	if got.Proof != downloaded.Proof {
		t.Fatal("cached proof root was reparsed instead of reused")
	}
	if got.Meta == nil || got.Meta.GenUTime != 123 {
		t.Fatalf("meta = %+v, want cached meta", got.Meta)
	}

	if _, err = cache.PopBlock(downloaded.ID); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("second pop err = %v, want ErrNotFound", err)
	}
}

func TestShardBroadcastCachePrunesExpiredBlocks(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Second, 1<<20, 16)
	now := time.Unix(100, 0)
	downloaded := testShardBroadcastDownloadedBlock(t, 11, 0x11)

	if err := cache.storeAt(downloaded, testShardBroadcastMeta(downloaded), downloaded.Block, downloaded.Proof, downloaded.StateUpdate, now); err != nil {
		t.Fatalf("store block: %v", err)
	}
	cache.Prune(now.Add(2 * time.Second))

	if entries := shardBroadcastCacheLen(cache); entries != 0 {
		t.Fatalf("cache entries = %d, want 0", entries)
	}
	if _, err := cache.popBlockAt(downloaded.ID, now.Add(2*time.Second)); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("pop expired err = %v, want ErrNotFound", err)
	}
}

func TestShardBroadcastCachePrunesOldestOverflow(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 2)
	now := time.Unix(200, 0)
	first := testShardBroadcastDownloadedBlock(t, 12, 0x12)
	second := testShardBroadcastDownloadedBlock(t, 13, 0x13)
	third := testShardBroadcastDownloadedBlock(t, 14, 0x14)

	for i, downloaded := range []DownloadedBlock{first, second, third} {
		if err := cache.storeAt(downloaded, testShardBroadcastMeta(downloaded), downloaded.Block, downloaded.Proof, downloaded.StateUpdate, now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("store block %d: %v", i, err)
		}
	}

	if entries := shardBroadcastCacheLen(cache); entries != 2 {
		t.Fatalf("cache entries = %d, want 2", entries)
	}
	popAt := now.Add(10 * time.Second)
	if _, err := cache.popBlockAt(first.ID, popAt); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("oldest pop err = %v, want ErrNotFound", err)
	}
	if _, err := cache.popBlockAt(second.ID, popAt); err != nil {
		t.Fatalf("second block was evicted: %v", err)
	}
	if _, err := cache.popBlockAt(third.ID, popAt); err != nil {
		t.Fatalf("third block was evicted: %v", err)
	}
}

func TestShardBroadcastCacheReplacementMovesEntryToBack(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 2)
	now := time.Unix(250, 0)
	first := testShardBroadcastDownloadedBlock(t, 16, 0x16)
	second := testShardBroadcastDownloadedBlock(t, 17, 0x17)
	third := testShardBroadcastDownloadedBlock(t, 18, 0x18)

	if err := cache.storeAt(first, testShardBroadcastMeta(first), first.Block, first.Proof, first.StateUpdate, now); err != nil {
		t.Fatalf("store first block: %v", err)
	}
	if err := cache.storeAt(second, testShardBroadcastMeta(second), second.Block, second.Proof, second.StateUpdate, now.Add(time.Second)); err != nil {
		t.Fatalf("store second block: %v", err)
	}
	first.Kind = "updated"
	if err := cache.storeAt(first, testShardBroadcastMeta(first), first.Block, first.Proof, first.StateUpdate, now.Add(2*time.Second)); err != nil {
		t.Fatalf("replace first block: %v", err)
	}
	if err := cache.storeAt(third, testShardBroadcastMeta(third), third.Block, third.Proof, third.StateUpdate, now.Add(3*time.Second)); err != nil {
		t.Fatalf("store third block: %v", err)
	}

	popAt := now.Add(10 * time.Second)
	got, err := cache.popBlockAt(first.ID, popAt)
	if err != nil {
		t.Fatalf("replaced first block was evicted: %v", err)
	}
	if got.Kind != "updated" {
		t.Fatalf("replaced kind = %q, want updated", got.Kind)
	}
	if _, err = cache.popBlockAt(second.ID, popAt); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("second block error = %v, want ErrNotFound", err)
	}
	if _, err = cache.popBlockAt(third.ID, popAt); err != nil {
		t.Fatalf("third block was evicted: %v", err)
	}
}

func TestDownloadBlockFullUsesShardBroadcastCacheBeforeOverlay(t *testing.T) {
	node := newTestNode(t)
	downloaded := testShardBroadcastDownloadedBlock(t, 15, 0x15)

	if err := node.shardBroadcastCache.Store(downloaded, testShardBroadcastMeta(downloaded)); err != nil {
		t.Fatalf("store block: %v", err)
	}

	got, err := node.DownloadBlockFull(context.Background(), downloaded.ID)
	if err != nil {
		t.Fatalf("download block: %v", err)
	}
	if !got.ID.Equals(&downloaded.ID) || got.Kind != downloaded.Kind {
		t.Fatalf("unexpected downloaded block: %#v", got)
	}
	if _, err = node.shardBroadcastCache.PopBlock(downloaded.ID); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("cache was not consumed, err=%v", err)
	}
	if _, err = node.peerStorage.BlockFull(context.Background(), downloaded.ID); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("block was stored in peer cache, err=%v", err)
	}
}

func TestShardBroadcastCacheNotifiesWaiters(t *testing.T) {
	node := newTestNode(t)
	downloaded := testShardBroadcastDownloadedBlock(t, 19, 0x19)

	wake, cancel := node.watchShardBroadcastBlock(downloaded.ID)
	defer cancel()
	if wake == nil {
		t.Fatal("watch returned nil")
	}

	if err := node.shardBroadcastCache.Store(downloaded, testShardBroadcastMeta(downloaded)); err != nil {
		t.Fatalf("store block: %v", err)
	}
	node.notifyShardBroadcastBlock(downloaded.ID)

	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("waiter was not notified")
	}
}

func shardBroadcastCacheLen(cache *shardBroadcastBlockCache) int {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return len(cache.entries)
}

func testShardBroadcastMeta(downloaded DownloadedBlock) *tnstore.BlockMeta {
	return &tnstore.BlockMeta{ID: downloaded.ID}
}

func testShardBroadcastDownloadedBlock(t *testing.T, seqno uint32, payload uint64) DownloadedBlock {
	t.Helper()

	root := cell.BeginCell().MustStoreUInt(payload, 16).EndCell()
	blockBOC := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockBOC)

	block := testBlockID(0, topShard, seqno)
	block.RootHash = append([]byte(nil), rootHash[:]...)
	block.FileHash = append([]byte(nil), fileHash[:]...)

	proof := testBlockProofCell(t, block, nil)
	proofBOC := proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})

	return DownloadedBlock{
		ID:               block,
		Kind:             "tonNode.blockBroadcast",
		Block:            root,
		Proof:            proof,
		BlockBOC:         blockBOC,
		ProofBOC:         proofBOC,
		Meta:             testShardBroadcastMeta(DownloadedBlock{ID: block}),
		StateUpdate:      cell.BeginCell().EndCell(),
		IsLink:           true,
		VerifiedRootHash: true,
	}
}
