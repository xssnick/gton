package p2p

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// BenchmarkShardDescriptionProofForFinalityHeldBlock measures a shard top
// description link arriving for a block whose finality broadcast already put it
// into the hot shard cache while its candidate is still parked in the shard
// candidate cache. The fixture is a real mainnet basechain block with 269
// transactions.
func BenchmarkShardDescriptionProofForFinalityHeldBlock(b *testing.B) {
	raw, err := os.ReadFile(protocol1CandidateBenchmarkFixturePath)
	if err != nil {
		b.Fatal(err)
	}

	var fixture protocol1CandidateBenchmarkFixture
	if err = json.Unmarshal(raw, &fixture); err != nil {
		b.Fatal(err)
	}
	blockBOC, err := base64.StdEncoding.DecodeString(fixture.BlockBOCBase64)
	if err != nil {
		b.Fatal(err)
	}
	root, err := cell.FromBOC(blockBOC)
	if err != nil {
		b.Fatal(err)
	}
	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		b.Fatal(err)
	}
	prepared, err := tnstore.PrepareBlockCandidate(fixture.Block.Workchain, int64(shard), fixture.Block.SeqNo, root)
	if err != nil {
		b.Fatal(err)
	}
	id := prepared.ID()

	candidate, err := decodeRawBlockCandidateBroadcast(trustedConsensusCandidateKind, id, prepared.BlockBOC())
	if err != nil {
		b.Fatal(err)
	}
	proofRoot, err := blockproof.BroadcastProofRoot(id, candidate.Block)
	if err != nil {
		b.Fatal(err)
	}
	proof, proofBOC, err := blockproof.LinkFromRoot(id, proofRoot)
	if err != nil {
		b.Fatal(err)
	}

	node := newTestNode(b)
	held := *candidate
	held.Kind = blockFinalityBroadcastKind
	held.Proof = proof
	held.ProofBOC = proofBOC
	held.IsLink = true
	if !node.rememberShardBroadcastBlock(&held) {
		b.Fatal("finality-assembled block was not cached")
	}
	proofs := []ShardDescriptionProof{{Block: id, Proof: proof, ProofBOC: proofBOC}}

	b.ReportAllocs()
	b.SetBytes(int64(len(candidate.BlockBOC)))

	for b.Loop() {
		b.StopTimer()
		node.shardCandidateCache = newShardBlockCandidateCache(shardBlockCandidateCacheTTL, shardBlockCandidateCacheMaxBytes, shardBlockCandidateCacheMaxItems)
		node.rememberShardBlockCandidate(candidate)
		b.StartTimer()

		node.RememberShardDescriptionProofs(proofs)
	}
}
