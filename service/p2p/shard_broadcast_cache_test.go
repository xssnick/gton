package p2p

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func storeTestShardBroadcast(cache *shardBroadcastBlockCache, downloaded DownloadedBlock) error {
	_, err := validateShardBroadcastBlock(&downloaded)
	if err != nil {
		return err
	}
	return cache.storeAt(downloaded, time.Now())
}

func TestShardBroadcastCacheKeepsImmediateSyncHotThenFallsBackToBOC(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 16)
	downloaded := testShardBroadcastDownloadedBlock(t, 10, 0x10)
	now := time.Now()
	if _, err := validateShardBroadcastBlock(&downloaded); err != nil {
		t.Fatalf("validate block: %v", err)
	}
	if err := cache.storeAt(downloaded, now); err != nil {
		t.Fatalf("store block: %v", err)
	}

	key := tnstore.BlockKey(downloaded.ID)
	got, err := cache.broadcastBlockCache.blockAt(key, now.Add(time.Second))
	if err != nil {
		t.Fatalf("read block: %v", err)
	}
	if !got.ID.Equals(&downloaded.ID) {
		t.Fatalf("block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(downloaded.ID))
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
		t.Fatal("immediate sync read reparsed the hot block root")
	}
	if got.Proof != downloaded.Proof {
		t.Fatal("immediate sync read reparsed the hot proof root")
	}
	if got.Meta == nil || got.Meta.GenUTime != downloaded.Meta.GenUTime {
		t.Fatalf("meta = %+v, want decoded metadata", got.Meta)
	}

	coldAt := now.Add(broadcastBlockCacheHotTTL + time.Millisecond)
	cache.prune(coldAt)
	cache.mu.Lock()
	hot := cache.entries[key].hot
	cache.mu.Unlock()
	if hot != nil {
		t.Fatal("parsed block remained in the long-lived cache after the hot window")
	}

	got, err = cache.broadcastBlockCache.blockAt(key, coldAt)
	if err != nil {
		t.Fatalf("cold read block: %v", err)
	}
	if !got.ID.Equals(&downloaded.ID) {
		t.Fatalf("cold block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(downloaded.ID))
	}
	if got.Block == downloaded.Block || got.Proof == downloaded.Proof {
		t.Fatal("cold read reused a parsed root released with the hot window")
	}
}

func TestShardBroadcastCacheOwnsStoredBlockID(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 16)
	downloaded := testShardBroadcastDownloadedBlock(t, 101, 0x101)
	target := downloaded.ID
	target.RootHash = append([]byte(nil), target.RootHash...)
	target.FileHash = append([]byte(nil), target.FileHash...)

	if err := storeTestShardBroadcast(cache, downloaded); err != nil {
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
		t.Fatalf("block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(target))
	}

	got.ID.RootHash[0] ^= 0xff
	got.ID.FileHash[0] ^= 0xff

	again, err := cache.Block(target)
	if err != nil {
		t.Fatalf("read block again: %v", err)
	}
	if !again.ID.Equals(&target) {
		t.Fatalf("second block = %s, want %s", tnstore.FormatBlockRef(again.ID), tnstore.FormatBlockRef(target))
	}
}

func TestShardBroadcastCachePrunesExpiredBlocks(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Second, 1<<20, 16)
	now := time.Unix(100, 0)
	downloaded := testShardBroadcastDownloadedBlock(t, 11, 0x11)

	if err := cache.storeAt(downloaded, now); err != nil {
		t.Fatalf("store block: %v", err)
	}
	cache.prune(now.Add(2 * time.Second))

	if entries := shardBroadcastCacheLen(cache); entries != 0 {
		t.Fatalf("cache entries = %d, want 0", entries)
	}
	if _, err := cache.broadcastBlockCache.blockAt(tnstore.BlockKey(downloaded.ID), now.Add(2*time.Second)); !errors.Is(err, tnstore.ErrNotFound) {
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
		if err := cache.storeAt(downloaded, now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("store block %d: %v", i, err)
		}
	}

	if entries := shardBroadcastCacheLen(cache); entries != 2 {
		t.Fatalf("cache entries = %d, want 2", entries)
	}
	popAt := now.Add(10 * time.Second)
	if _, err := cache.broadcastBlockCache.blockAt(tnstore.BlockKey(first.ID), popAt); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("oldest read err = %v, want ErrNotFound", err)
	}
	if _, err := cache.broadcastBlockCache.blockAt(tnstore.BlockKey(second.ID), popAt); err != nil {
		t.Fatalf("second block was evicted: %v", err)
	}
	if _, err := cache.broadcastBlockCache.blockAt(tnstore.BlockKey(third.ID), popAt); err != nil {
		t.Fatalf("third block was evicted: %v", err)
	}
}

func TestShardBroadcastCacheReplacementMovesEntryToBack(t *testing.T) {
	cache := newShardBroadcastBlockCache(time.Minute, 1<<20, 2)
	now := time.Unix(250, 0)
	first := testShardBroadcastDownloadedBlock(t, 16, 0x16)
	second := testShardBroadcastDownloadedBlock(t, 17, 0x17)
	third := testShardBroadcastDownloadedBlock(t, 18, 0x18)

	if err := cache.storeAt(first, now); err != nil {
		t.Fatalf("store first block: %v", err)
	}
	if err := cache.storeAt(second, now.Add(time.Second)); err != nil {
		t.Fatalf("store second block: %v", err)
	}
	first.Kind = "updated"
	if err := cache.storeAt(first, now.Add(2*time.Second)); err != nil {
		t.Fatalf("replace first block: %v", err)
	}
	if err := cache.storeAt(third, now.Add(3*time.Second)); err != nil {
		t.Fatalf("store third block: %v", err)
	}

	popAt := now.Add(10 * time.Second)
	got, err := cache.broadcastBlockCache.blockAt(tnstore.BlockKey(first.ID), popAt)
	if err != nil {
		t.Fatalf("replaced first block was evicted: %v", err)
	}
	if got.Kind != "updated" {
		t.Fatalf("replaced kind = %q, want updated", got.Kind)
	}
	if _, err = cache.broadcastBlockCache.blockAt(tnstore.BlockKey(second.ID), popAt); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("second block error = %v, want ErrNotFound", err)
	}
	if _, err = cache.broadcastBlockCache.blockAt(tnstore.BlockKey(third.ID), popAt); err != nil {
		t.Fatalf("third block was evicted: %v", err)
	}
}

func TestDownloadBlockFullUsesShardBroadcastCacheBeforeOverlay(t *testing.T) {
	node := newTestNode(t)
	downloaded := testShardBroadcastDownloadedBlock(t, 15, 0x15)

	if err := storeTestShardBroadcast(node.shardBroadcastCache, downloaded); err != nil {
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

	if err := storeTestShardBroadcast(node.shardBroadcastCache, downloaded); err != nil {
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
		t.Fatalf("block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(downloaded.ID))
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

func TestShardCandidateCacheReleasesAssembledPayloadAndSuppressesDuplicates(t *testing.T) {
	cache := newShardBlockCandidateCache(time.Minute, 1<<20, 16)
	now := time.Unix(500, 0)
	downloaded := testShardBroadcastDownloadedBlock(t, 24, 0x24)
	candidate := testShardBlockCandidate(downloaded)
	proof := ShardDescriptionProof{
		Block:    downloaded.ID,
		Proof:    downloaded.Proof,
		ProofBOC: downloaded.ProofBOC,
	}

	if blocks, err := cache.StoreCandidate(candidate, now); err != nil || len(blocks) != 0 {
		t.Fatalf("store candidate: blocks=%d err=%v", len(blocks), err)
	}
	blocks, err := cache.StoreProofs([]ShardDescriptionProof{proof}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("store proof: %v", err)
	}
	requireAssembledShardCandidate(t, blocks, downloaded)

	key := tnstore.BlockKey(downloaded.ID)
	if len(cache.candidates) != 0 || len(cache.proofs) != 0 {
		t.Fatalf("assembled payload retained: candidates=%d proofs=%d", len(cache.candidates), len(cache.proofs))
	}
	if _, ok := cache.assembled[key]; !ok {
		t.Fatal("assembled marker was not stored")
	}
	if cache.bytes != shardBlockCandidateCacheOverhead {
		t.Fatalf("assembled cache bytes = %d, want %d", cache.bytes, shardBlockCandidateCacheOverhead)
	}

	if blocks, err = cache.StoreCandidate(candidate, now.Add(2*time.Second)); err != nil || len(blocks) != 0 {
		t.Fatalf("store repeated candidate: blocks=%d err=%v", len(blocks), err)
	}
	if blocks, err = cache.StoreProofs([]ShardDescriptionProof{proof}, now.Add(3*time.Second)); err != nil || len(blocks) != 0 {
		t.Fatalf("store repeated proof: blocks=%d err=%v", len(blocks), err)
	}
	if len(cache.candidates) != 0 || len(cache.proofs) != 0 {
		t.Fatalf("duplicate payload retained: candidates=%d proofs=%d", len(cache.candidates), len(cache.proofs))
	}
}

func TestShardCandidateCacheAssemblesRemainingLinksAfterBadProof(t *testing.T) {
	cache := newShardBlockCandidateCache(time.Minute, 1<<20, 16)
	now := time.Unix(700, 0)

	// A candidate is keyed by root hash alone, so a peer can park one whose
	// declared FileHash does not match the signed chain link. Assembling it
	// fails, and it sits ahead of the healthy link in the same batch.
	poisoned := testShardBroadcastDownloadedBlock(t, 26, 0x26)
	badCandidate := testShardBlockCandidate(poisoned)
	badCandidate.ID.FileHash = bytes.Repeat([]byte{0xEE}, 32)

	healthy := testShardBroadcastDownloadedBlock(t, 27, 0x27)
	goodCandidate := testShardBlockCandidate(healthy)

	for _, candidate := range []DownloadedBlock{badCandidate, goodCandidate} {
		if blocks, err := cache.StoreCandidate(candidate, now); err != nil || len(blocks) != 0 {
			t.Fatalf("store candidate %s: blocks=%d err=%v", tnstore.FormatBlockRef(candidate.ID), len(blocks), err)
		}
	}

	proofs := []ShardDescriptionProof{
		{Block: poisoned.ID, Proof: poisoned.Proof, ProofBOC: poisoned.ProofBOC},
		{Block: healthy.ID, Proof: healthy.Proof, ProofBOC: healthy.ProofBOC},
	}
	blocks, err := cache.StoreProofs(proofs, now.Add(time.Second))
	if err == nil {
		t.Fatal("bad chain link did not report an error")
	}
	requireAssembledShardCandidate(t, blocks, healthy)

	// the healthy link is released and suppressed; the bad one keeps its payload
	// so a corrected candidate can still assemble it
	if _, ok := cache.assembled[tnstore.BlockKey(healthy.ID)]; !ok {
		t.Fatal("healthy link was not marked assembled")
	}
	if _, ok := cache.assembled[tnstore.BlockKey(poisoned.ID)]; ok {
		t.Fatal("failed link was marked assembled")
	}
	if _, ok := cache.candidates[tnstore.BlockKey(poisoned.ID)]; !ok {
		t.Fatal("failed link lost its candidate payload")
	}
}

func TestShardCandidateCacheAssembledMarkerExpires(t *testing.T) {
	ttl := 10 * time.Millisecond
	cache := newShardBlockCandidateCache(ttl, 1<<20, 16)
	now := time.Unix(600, 0)
	downloaded := testShardBroadcastDownloadedBlock(t, 25, 0x25)
	candidate := testShardBlockCandidate(downloaded)
	proof := ShardDescriptionProof{
		Block:    downloaded.ID,
		Proof:    downloaded.Proof,
		ProofBOC: downloaded.ProofBOC,
	}

	if blocks, err := cache.StoreCandidate(candidate, now); err != nil || len(blocks) != 0 {
		t.Fatalf("store candidate: blocks=%d err=%v", len(blocks), err)
	}
	blocks, err := cache.StoreProofs([]ShardDescriptionProof{proof}, now.Add(time.Millisecond))
	if err != nil {
		t.Fatalf("store proof: %v", err)
	}
	requireAssembledShardCandidate(t, blocks, downloaded)

	if blocks, err = cache.StoreCandidate(candidate, now.Add(ttl+2*time.Millisecond)); err != nil || len(blocks) != 0 {
		t.Fatalf("store candidate after marker expiry: blocks=%d err=%v", len(blocks), err)
	}
	blocks, err = cache.StoreProofs([]ShardDescriptionProof{proof}, now.Add(ttl+3*time.Millisecond))
	if err != nil {
		t.Fatalf("store proof after marker expiry: %v", err)
	}
	requireAssembledShardCandidate(t, blocks, downloaded)
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
		t.Fatalf("assembled block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(want.ID))
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

func testShardBroadcastDownloadedBlock(t *testing.T, seqno uint32, _ uint64) DownloadedBlock {
	t.Helper()

	root := testPeerBlockRoot(t, 0, seqno)
	blockBOC := serializeCompressedBlockRoot(root)
	rootHash := root.HashKey()

	block := testBlockID(0, topShard, seqno)
	block.RootHash = bytes.Clone(rootHash[:])
	block.FileHash = hashSimpleBroadcastPayload(blockBOC)

	downloaded, err := newVerifiedBlockCandidateBroadcast(
		"tonNode.newBlockCandidateBroadcastCompressedV2",
		block,
		blockBOC,
		root,
	)
	if err != nil {
		t.Fatalf("build verified shard candidate: %v", err)
	}

	proof := testBlockProofCell(t, block, nil)
	downloaded.Kind = "tonNode.blockBroadcast"
	downloaded.Proof = proof
	downloaded.ProofBOC = proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	downloaded.IsLink = true

	return *downloaded
}
