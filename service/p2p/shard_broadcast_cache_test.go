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

func TestShardBroadcastCacheBlockCanBeReadRepeatedly(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 16)
	downloaded := testShardBroadcastDownloadedBlock(t, 10, 0x10)
	meta := &tnstore.BlockMeta{ID: downloaded.ID, GenUTime: 123}

	if err := cache.Store(downloaded, meta); err != nil {
		t.Fatalf("store block: %v", err)
	}

	got, err := cache.Block(downloaded.ID)
	if err != nil {
		t.Fatalf("read block: %v", err)
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

	got, err = cache.Block(downloaded.ID)
	if err != nil {
		t.Fatalf("second read block: %v", err)
	}
	if !got.ID.Equals(&downloaded.ID) {
		t.Fatalf("second block = %s, want %s", formatBlockRef(got.ID), formatBlockRef(downloaded.ID))
	}
}

func TestShardBroadcastCacheOwnsStoredBlockID(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 16)
	downloaded := testShardBroadcastDownloadedBlock(t, 101, 0x101)
	target := downloaded.ID
	target.RootHash = append([]byte(nil), target.RootHash...)
	target.FileHash = append([]byte(nil), target.FileHash...)

	if err := cache.Store(downloaded, testShardBroadcastMeta(downloaded)); err != nil {
		t.Fatalf("store block: %v", err)
	}

	for i := range downloaded.ID.RootHash {
		downloaded.ID.RootHash[i] = 0xaa
	}
	for i := range downloaded.ID.FileHash {
		downloaded.ID.FileHash[i] = 0xbb
	}

	got, err := cache.Block(target)
	if err != nil {
		t.Fatalf("read block: %v", err)
	}
	if !got.ID.Equals(&target) {
		t.Fatalf("block = %s, want %s", formatBlockRef(got.ID), formatBlockRef(target))
	}

	got.ID.RootHash[0] ^= 0xff
	got.ID.FileHash[0] ^= 0xff

	again, err := cache.Block(target)
	if err != nil {
		t.Fatalf("read block again: %v", err)
	}
	if !again.ID.Equals(&target) {
		t.Fatalf("second block = %s, want %s", formatBlockRef(again.ID), formatBlockRef(target))
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
	if _, err := cache.blockAt(downloaded.ID, now.Add(2*time.Second)); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("read expired err = %v, want ErrNotFound", err)
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
	if _, err := cache.blockAt(first.ID, popAt); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("oldest read err = %v, want ErrNotFound", err)
	}
	if _, err := cache.blockAt(second.ID, popAt); err != nil {
		t.Fatalf("second block was evicted: %v", err)
	}
	if _, err := cache.blockAt(third.ID, popAt); err != nil {
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
	got, err := cache.blockAt(first.ID, popAt)
	if err != nil {
		t.Fatalf("replaced first block was evicted: %v", err)
	}
	if got.Kind != "updated" {
		t.Fatalf("replaced kind = %q, want updated", got.Kind)
	}
	if _, err = cache.blockAt(second.ID, popAt); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("second block error = %v, want ErrNotFound", err)
	}
	if _, err = cache.blockAt(third.ID, popAt); err != nil {
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
	if _, err = node.shardBroadcastCache.Block(downloaded.ID); err != nil {
		t.Fatalf("cache was consumed: %v", err)
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

func TestShardCandidateAndDescriptionProofAssemblesHotBlock(t *testing.T) {
	node := newTestNode(t)
	downloaded := testShardBroadcastDownloadedBlock(t, 20, 0x20)
	candidate := downloaded
	candidate.Kind = "tonNode.newBlockCandidateBroadcast"
	candidate.Proof = nil
	candidate.ProofBOC = nil
	candidate.IsLink = false

	node.rememberShardBlockCandidate(&candidate)
	node.RememberShardDescriptionProofs([]ShardDescriptionProof{{
		Block:    downloaded.ID,
		Proof:    downloaded.Proof,
		ProofBOC: downloaded.ProofBOC,
	}})

	got, err := node.DownloadBlockFull(context.Background(), downloaded.ID)
	if err != nil {
		t.Fatalf("download block: %v", err)
	}
	if !got.ID.Equals(&downloaded.ID) {
		t.Fatalf("block = %s, want %s", formatBlockRef(got.ID), formatBlockRef(downloaded.ID))
	}
	if got.Kind != shardDescriptionBroadcastKind {
		t.Fatalf("kind = %q, want %q", got.Kind, shardDescriptionBroadcastKind)
	}
	if len(got.BlockBOC) == 0 || len(got.ProofBOC) == 0 || got.StateUpdate == nil {
		t.Fatalf("assembled block is incomplete: block=%d proof=%d state_update=%v", len(got.BlockBOC), len(got.ProofBOC), got.StateUpdate != nil)
	}
}

func TestShardDescriptionProofAndCandidateAssemblesHotBlock(t *testing.T) {
	node := newTestNode(t)
	downloaded := testShardBroadcastDownloadedBlock(t, 21, 0x21)
	candidate := downloaded
	candidate.Kind = "tonNode.newBlockCandidateBroadcast"
	candidate.Proof = nil
	candidate.ProofBOC = nil
	candidate.IsLink = false

	node.RememberShardDescriptionProofs([]ShardDescriptionProof{{
		Block:    downloaded.ID,
		Proof:    downloaded.Proof,
		ProofBOC: downloaded.ProofBOC,
	}})
	node.rememberShardBlockCandidate(&candidate)

	got, err := node.DownloadBlockFull(context.Background(), downloaded.ID)
	if err != nil {
		t.Fatalf("download block: %v", err)
	}
	if !got.ID.Equals(&downloaded.ID) || got.Kind != shardDescriptionBroadcastKind {
		t.Fatalf("unexpected assembled block: %#v", got)
	}
}

func TestShardCandidateCacheAssemblesProofAfterCandidateBeforeOverflowPrune(t *testing.T) {
	cache := newShardBlockCandidateCache(time.Minute, 1<<20, 1)
	now := time.Unix(300, 0)
	downloaded := testShardBroadcastDownloadedBlock(t, 22, 0x22)
	downloaded.SourcePeerID[0] = 0xA1
	downloaded.SignaturesVerifiedKey = []byte{0xB1, 0xB2}

	assembled, err := cache.StoreCandidate(testShardBlockCandidate(downloaded), now)
	if err != nil {
		t.Fatalf("store candidate: %v", err)
	}
	if len(assembled) != 0 {
		t.Fatalf("assembled from candidate without proof: %d", len(assembled))
	}

	assembled, err = cache.StoreProofs([]ShardDescriptionProof{{
		Block:    downloaded.ID,
		Proof:    downloaded.Proof,
		ProofBOC: downloaded.ProofBOC,
	}}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("store proof: %v", err)
	}
	requireAssembledShardCandidate(t, assembled, downloaded)
}

func TestShardCandidateCacheAssemblesCandidateAfterProofBeforeOverflowPrune(t *testing.T) {
	cache := newShardBlockCandidateCache(time.Minute, 1<<20, 1)
	now := time.Unix(400, 0)
	downloaded := testShardBroadcastDownloadedBlock(t, 23, 0x23)

	assembled, err := cache.StoreProofs([]ShardDescriptionProof{{
		Block:    downloaded.ID,
		Proof:    downloaded.Proof,
		ProofBOC: downloaded.ProofBOC,
	}}, now)
	if err != nil {
		t.Fatalf("store proof: %v", err)
	}
	if len(assembled) != 0 {
		t.Fatalf("assembled from proof without candidate: %d", len(assembled))
	}

	assembled, err = cache.StoreCandidate(testShardBlockCandidate(downloaded), now.Add(time.Second))
	if err != nil {
		t.Fatalf("store candidate: %v", err)
	}
	requireAssembledShardCandidate(t, assembled, downloaded)
}

func shardBroadcastCacheLen(cache *shardBroadcastBlockCache) int {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return len(cache.entries)
}

func requireAssembledShardCandidate(t *testing.T, assembled []DownloadedBlock, want DownloadedBlock) {
	t.Helper()

	if len(assembled) != 1 {
		t.Fatalf("assembled blocks = %d, want 1", len(assembled))
	}
	got := assembled[0]
	if !got.ID.Equals(&want.ID) {
		t.Fatalf("assembled block = %s, want %s", formatBlockRef(got.ID), formatBlockRef(want.ID))
	}
	if got.Kind != shardDescriptionBroadcastKind {
		t.Fatalf("assembled kind = %q, want %q", got.Kind, shardDescriptionBroadcastKind)
	}
	if got.Proof == nil || len(got.ProofBOC) == 0 || !got.IsLink || !got.VerifiedRootHash {
		t.Fatalf("assembled block is incomplete: proof=%v proof_boc=%d is_link=%v verified=%v", got.Proof != nil, len(got.ProofBOC), got.IsLink, got.VerifiedRootHash)
	}
	if got.Block == nil || len(got.BlockBOC) == 0 || got.Meta == nil || got.StateUpdate == nil {
		t.Fatalf("assembled candidate payload is incomplete: block=%v block_boc=%d meta=%v state_update=%v", got.Block != nil, len(got.BlockBOC), got.Meta != nil, got.StateUpdate != nil)
	}
	if got.SourcePeerID != want.SourcePeerID {
		t.Fatalf("assembled source peer = %x, want %x", got.SourcePeerID, want.SourcePeerID)
	}
	if string(got.SignaturesVerifiedKey) != string(want.SignaturesVerifiedKey) {
		t.Fatalf("assembled signature key = %x, want %x", got.SignaturesVerifiedKey, want.SignaturesVerifiedKey)
	}
}

func testShardBlockCandidate(downloaded DownloadedBlock) DownloadedBlock {
	candidate := downloaded
	candidate.Kind = "tonNode.newBlockCandidateBroadcast"
	candidate.Proof = nil
	candidate.ProofBOC = nil
	candidate.IsLink = false
	return candidate
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
