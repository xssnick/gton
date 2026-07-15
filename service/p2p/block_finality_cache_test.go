package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestBlockFinalityCacheAssemblesShardBlockInBothOrders(t *testing.T) {
	now := time.Unix(1700000000, 0)

	tests := []struct {
		name          string
		finalityFirst bool
		seqno         uint32
	}{
		{
			name:  "candidate first",
			seqno: 310,
		},
		{
			name:          "finality first",
			finalityFirst: true,
			seqno:         311,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newBlockFinalityCache(time.Minute, 1<<20, 16)
			candidate := testBlockFinalityCandidate(t, tt.seqno)
			finality := testShardBlockFinality(candidate.ID)

			var blocks []DownloadedBlock
			var err error
			if tt.finalityFirst {
				blocks, err = cache.StoreFinality(finality, now)
				if err != nil {
					t.Fatalf("store finality first: %v", err)
				}
				if len(blocks) != 0 {
					t.Fatalf("store finality first assembled %d blocks, want 0", len(blocks))
				}

				blocks, err = cache.StoreCandidate(candidate, now.Add(time.Millisecond))
			} else {
				blocks, err = cache.StoreCandidate(candidate, now)
				if err != nil {
					t.Fatalf("store candidate first: %v", err)
				}
				if len(blocks) != 0 {
					t.Fatalf("store candidate first assembled %d blocks, want 0", len(blocks))
				}

				blocks, err = cache.StoreFinality(finality, now.Add(time.Millisecond))
			}
			if err != nil {
				t.Fatalf("store matching part: %v", err)
			}

			requireAssembledShardFinalityBlock(t, candidate, finality, blocks)
			requireNoRepeatedFinalityAssembly(t, cache, candidate, finality, now.Add(2*time.Millisecond))
		})
	}
}

func TestBlockFinalityCacheAssemblesMasterchainBlock(t *testing.T) {
	cache := newBlockFinalityCache(time.Minute, 1<<20, 16)
	candidate := testMasterchainBlockFinalityCandidate(t)
	finality := testMasterchainBlockFinality(t, candidate.ID)
	now := time.Unix(1700000001, 0)

	blocks, err := cache.StoreCandidate(candidate, now)
	if err != nil {
		t.Fatalf("store masterchain candidate: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("store masterchain candidate assembled %d blocks, want 0", len(blocks))
	}

	blocks, err = cache.StoreFinality(finality, now.Add(time.Millisecond))
	if err != nil {
		t.Fatalf("store masterchain finality: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("assembled masterchain blocks = %d, want 1", len(blocks))
	}

	got := blocks[0]
	if got.IsLink {
		t.Fatal("assembled masterchain finality block is a proof link")
	}
	if got.Proof == nil || len(got.ProofBOC) == 0 {
		t.Fatal("assembled masterchain finality block has no proof")
	}
	if err := blockproof.CheckProofShape(candidate.ID, got.Proof, false); err != nil {
		t.Fatalf("assembled masterchain proof shape: %v", err)
	}
	parsed, err := blockproof.ParseCell(candidate.ID, got.Proof)
	if err != nil {
		t.Fatalf("parse assembled masterchain proof: %v", err)
	}
	if parsed.Proof.Signatures.HashKey() != finality.signaturesCell.HashKey() {
		t.Fatal("assembled masterchain proof signatures mismatch")
	}
	if !bytes.Equal(got.SignaturesVerifiedKey, finality.signaturesVerifiedKey) {
		t.Fatalf("assembled masterchain signature key = %x, want %x", got.SignaturesVerifiedKey, finality.signaturesVerifiedKey)
	}

	requireNoRepeatedFinalityAssembly(t, cache, candidate, finality, now.Add(2*time.Millisecond))
}

func TestBlockFinalityCacheEvictsOldestEntry(t *testing.T) {
	cache := newBlockFinalityCache(time.Minute, 1<<20, 2)
	first := testBlockFinalityCandidate(t, 320)
	second := testBlockFinalityCandidate(t, 321)
	third := testBlockFinalityCandidate(t, 322)
	secondFinality := testShardBlockFinality(second.ID)
	now := time.Unix(1700000002, 0)

	if blocks, err := cache.StoreCandidate(first, now); err != nil || len(blocks) != 0 {
		t.Fatalf("store first candidate: blocks=%d err=%v", len(blocks), err)
	}
	if blocks, err := cache.StoreFinality(secondFinality, now.Add(time.Millisecond)); err != nil || len(blocks) != 0 {
		t.Fatalf("store second finality: blocks=%d err=%v", len(blocks), err)
	}
	if blocks, err := cache.StoreCandidate(third, now.Add(2*time.Millisecond)); err != nil || len(blocks) != 0 {
		t.Fatalf("store third candidate: blocks=%d err=%v", len(blocks), err)
	}

	if cache.order.Len() != 2 {
		t.Fatalf("cache order len = %d, want 2", cache.order.Len())
	}
	if cache.candidates[tnstore.BlockKey(first.ID)] != nil {
		t.Fatal("oldest candidate was not evicted")
	}
	if cache.finalities[tnstore.BlockKey(second.ID)] == nil {
		t.Fatal("newer finality was evicted")
	}
	if cache.candidates[tnstore.BlockKey(third.ID)] == nil {
		t.Fatal("newest candidate was evicted")
	}
}

func TestBlockFinalityCacheAssembledMarkerExpires(t *testing.T) {
	ttl := 10 * time.Millisecond
	cache := newBlockFinalityCache(ttl, 1<<20, 16)
	candidate := testBlockFinalityCandidate(t, 323)
	finality := testShardBlockFinality(candidate.ID)
	now := time.Unix(1700000003, 0)

	if blocks, err := cache.StoreCandidate(candidate, now); err != nil || len(blocks) != 0 {
		t.Fatalf("store candidate: blocks=%d err=%v", len(blocks), err)
	}
	blocks, err := cache.StoreFinality(finality, now.Add(time.Millisecond))
	if err != nil {
		t.Fatalf("store finality: %v", err)
	}
	requireAssembledShardFinalityBlock(t, candidate, finality, blocks)

	if blocks, err = cache.StoreFinality(finality, now.Add(2*time.Millisecond)); err != nil || len(blocks) != 0 {
		t.Fatalf("store finality before marker expiry: blocks=%d err=%v", len(blocks), err)
	}
	if blocks, err = cache.StoreFinality(finality, now.Add(ttl+2*time.Millisecond)); err != nil || len(blocks) != 0 {
		t.Fatalf("store finality after marker expiry: blocks=%d err=%v", len(blocks), err)
	}
	blocks, err = cache.StoreCandidate(candidate, now.Add(ttl+3*time.Millisecond))
	if err != nil {
		t.Fatalf("store candidate after marker expiry: %v", err)
	}
	requireAssembledShardFinalityBlock(t, candidate, finality, blocks)
}

func requireNoRepeatedFinalityAssembly(t *testing.T, cache *blockFinalityCache, candidate DownloadedBlock, finality checkedBlockFinality, now time.Time) {
	t.Helper()

	blocks, err := cache.StoreFinality(finality, now)
	if err != nil {
		t.Fatalf("store repeated finality: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("repeated finality assembled %d blocks, want 0", len(blocks))
	}

	blocks, err = cache.StoreCandidate(candidate, now.Add(time.Millisecond))
	if err != nil {
		t.Fatalf("store repeated candidate: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("repeated candidate assembled %d blocks, want 0", len(blocks))
	}

	blocks, err = cache.StoreFinality(finality, now.Add(2*time.Millisecond))
	if err != nil {
		t.Fatalf("store repeated pair finality: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("repeated pair assembled %d blocks, want 0", len(blocks))
	}
}

func requireAssembledShardFinalityBlock(t *testing.T, candidate DownloadedBlock, finality checkedBlockFinality, blocks []DownloadedBlock) {
	t.Helper()

	if len(blocks) != 1 {
		t.Fatalf("assembled blocks = %d, want 1", len(blocks))
	}

	got := blocks[0]
	if !got.ID.Equals(&candidate.ID) {
		t.Fatalf("assembled block id = %s, want %s", formatBlockRef(got.ID), formatBlockRef(candidate.ID))
	}
	if got.Kind != blockFinalityBroadcastKind {
		t.Fatalf("assembled kind = %q, want %q", got.Kind, blockFinalityBroadcastKind)
	}
	if !got.IsLink {
		t.Fatal("assembled shard finality block is not a proof link")
	}
	if got.Proof == nil || len(got.ProofBOC) == 0 {
		t.Fatal("assembled shard finality block has no proof link")
	}
	if err := blockproof.CheckProofShape(candidate.ID, got.Proof, true); err != nil {
		t.Fatalf("assembled proof shape: %v", err)
	}
	if got.Block.HashKey() != candidate.Block.HashKey() {
		t.Fatal("assembled block root hash mismatch")
	}
	if !bytes.Equal(got.BlockBOC, candidate.BlockBOC) {
		t.Fatal("assembled block boc mismatch")
	}
	if !bytes.Equal(got.SignaturesVerifiedKey, finality.signaturesVerifiedKey) {
		t.Fatalf("assembled signature key = %x, want %x", got.SignaturesVerifiedKey, finality.signaturesVerifiedKey)
	}
	if got.SourcePeerID != finality.sourcePeerID {
		t.Fatalf("assembled source peer mismatch")
	}
}

func testBlockFinalityCandidate(t *testing.T, seqno uint32) DownloadedBlock {
	t.Helper()

	root := testPeerBlockRoot(t, 0, topShard, seqno)
	blockBOC := serializeCompressedBlockRoot(root)
	rootHash := root.HashKey()
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     seqno,
		RootHash:  bytes.Clone(rootHash[:]),
		FileHash:  hashSimpleBroadcastPayload(blockBOC),
	}

	downloaded, err := newVerifiedBlockCandidateBroadcast("tonNode.newBlockCandidateBroadcastCompressedV2", id, blockBOC, root)
	if err != nil {
		t.Fatalf("build verified block candidate: %v", err)
	}
	downloaded.SourcePeerID = testPeerID("candidate")
	return *downloaded
}

type masterchainBlockFinalityFixture struct {
	Block struct {
		Workchain   int32  `json:"workchain"`
		Shard       string `json:"shard"`
		SeqNo       uint32 `json:"seqno"`
		RootHashHex string `json:"root_hash_hex"`
		FileHashHex string `json:"file_hash_hex"`
	} `json:"block"`
	RawBOCBase64 string `json:"raw_boc_base64"`
}

func testMasterchainBlockFinalityCandidate(t *testing.T) DownloadedBlock {
	t.Helper()

	rawFixture, err := os.ReadFile("../testdata/masterchain_block_fixture.json")
	if err != nil {
		t.Fatalf("read masterchain fixture: %v", err)
	}

	var fixture masterchainBlockFinalityFixture
	if err = json.Unmarshal(rawFixture, &fixture); err != nil {
		t.Fatalf("decode masterchain fixture: %v", err)
	}

	blockBOC, err := base64.StdEncoding.DecodeString(fixture.RawBOCBase64)
	if err != nil {
		t.Fatalf("decode masterchain block boc: %v", err)
	}
	blockRoot, err := cell.FromBOC(blockBOC)
	if err != nil {
		t.Fatalf("parse masterchain block boc: %v", err)
	}
	rootHash, err := hex.DecodeString(fixture.Block.RootHashHex)
	if err != nil {
		t.Fatalf("decode masterchain root hash: %v", err)
	}
	fileHash, err := hex.DecodeString(fixture.Block.FileHashHex)
	if err != nil {
		t.Fatalf("decode masterchain file hash: %v", err)
	}
	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		t.Fatalf("parse masterchain shard: %v", err)
	}

	id := ton.BlockIDExt{
		Workchain: fixture.Block.Workchain,
		Shard:     int64(shard),
		SeqNo:     fixture.Block.SeqNo,
		RootHash:  rootHash,
		FileHash:  fileHash,
	}
	parsed, err := tnstore.ParseVerifiedBlockCell(id, blockRoot)
	if err != nil {
		t.Fatalf("parse verified masterchain block: %v", err)
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		t.Fatalf("build masterchain block meta: %v", err)
	}

	return DownloadedBlock{
		ID:               id,
		Kind:             "tonNode.newBlockCandidateBroadcast",
		Block:            blockRoot,
		BlockBOC:         blockBOC,
		Meta:             meta,
		StateUpdate:      parsed.StateUpdate,
		SourcePeerID:     testPeerID("candidate"),
		VerifiedRootHash: true,
	}
}

func testShardBlockFinality(block ton.BlockIDExt) checkedBlockFinality {
	signatures := blockproof.NewSimplexValidatorSignatureSet(
		7,
		0x11223344,
		nil,
		false,
		bytes.Repeat([]byte{0x55}, 32),
		3,
		[]byte{0x01, 0x02, 0x03},
	)
	return checkedBlockFinality{
		block:                 block,
		signatures:            signatures,
		signaturesVerifiedKey: signatures.ContentKey(block),
		sourcePeerID:          testPeerID("finality"),
		payloadBytes:          128,
	}
}

func testMasterchainBlockFinality(t *testing.T, block ton.BlockIDExt) checkedBlockFinality {
	t.Helper()

	sessionID := bytes.Repeat([]byte{0x77}, 32)
	candidate, err := tl.Serialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            block,
		CollatedFileHash: bytes.Repeat([]byte{0x88}, 32),
		Parent:           ton.ConsensusCandidateWithoutParents{},
	}, true)
	if err != nil {
		t.Fatalf("serialize simplex candidate: %v", err)
	}

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate validator key: %v", err)
	}
	validators := []*tlb.ValidatorAddr{{
		PublicKey: tlb.SigPubKeyED25519{Key: pub},
		Weight:    1,
		ADNLAddr:  bytes.Repeat([]byte{0x99}, 32),
	}}
	catchainSeqno := uint32(7)
	setHash, err := blockproof.ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	prepared, err := blockproof.PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	signatures := blockproof.NewSimplexValidatorSignatureSet(catchainSeqno, setHash, nil, true, sessionID, 3, candidate)
	signaturesCell, err := signatures.FinalitySignaturesCell(prepared)
	if err != nil {
		t.Fatalf("serialize finality signatures: %v", err)
	}

	return checkedBlockFinality{
		block:                 block,
		signatures:            signatures,
		signaturesCell:        signaturesCell,
		signaturesVerifiedKey: signatures.ContentKey(block),
		sourcePeerID:          testPeerID("finality"),
		payloadBytes:          256,
	}
}
