package collator

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The two tests below pin the property the reference validator has and this one
// used to lack: with full collated data, shard validation reads the candidate's
// own proofs and nothing else. One proves the resident state is no longer
// needed, the other proves it is no longer consulted — a fallback would make
// the first pass and the second fail to reject.

// TestFullCollatedShardVerificationRunsOnProofsNotResidentState narrows the
// resident predecessor to a root-only proof and requires validation to succeed
// anyway. Everything the structural pass and the replay walk therefore came out
// of collated data. The control at the end takes the same narrowed state into a
// candidate without full collated data, where it must fail: otherwise the
// narrowing is not actually removing anything and the test proves nothing.
func TestFullCollatedShardVerificationRunsOnProofsNotResidentState(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate mainnet candidate: %v", err)
	}
	assertBenchMainnetCollated(t, candidate)

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Previous.State = narrowedStateRoot(t, req.Previous.State)
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)

	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify collated candidate without a resident predecessor: %v", err)
	}

	plainReq := benchMainnetCollatedRequest(t, 1)
	plainReq.Masterchain.Config.capabilities &^= capFullCollatedData
	plain, err := testBuilder().BuildShard(context.Background(), plainReq)
	if err != nil {
		t.Fatalf("collate candidate without full collated data: %v", err)
	}
	control := shardVerificationRequest(plainReq, plain)
	control.NeighborShardEndLT = plainReq.NeighborShardEndLT
	control.Semantics = NewSemanticVerifier(tvm.NewTVM())
	control.Previous.State = narrowedStateRoot(t, plainReq.Previous.State)
	if err = VerifyShardCandidate(context.Background(), control); err == nil {
		t.Fatal("narrowed predecessor verified without collated proofs to replace it")
	}
}

// TestFullCollatedShardVerificationRejectsNarrowedPredecessorProof replaces the
// predecessor state proof inside collated data with one that proves only the
// root, and keeps the resident state whole. A validator that still reads the
// resident tree accepts this candidate; the reference node rejects it, and so
// must this one.
func TestFullCollatedShardVerificationRejectsNarrowedPredecessorProof(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate mainnet candidate: %v", err)
	}
	assertBenchMainnetCollated(t, candidate)

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatalf("decode collated data: %v", err)
	}
	stateHash := req.Previous.State.HashKeyAt(0)
	replaced := 0
	for i, root := range roots {
		if !root.IsSpecial() {
			continue
		}
		body, unwrapErr := unwrapCollatedProof(root)
		if unwrapErr != nil || body.HashKeyAt(0) != stateHash {
			continue
		}
		narrow, proofErr := req.Previous.State.CreateHashUsageProof(func(cell.Hash) bool { return false })
		if proofErr != nil {
			t.Fatalf("build narrowed predecessor proof: %v", proofErr)
		}
		roots[i] = narrow
		replaced++
	}
	if replaced != 1 {
		t.Fatalf("replaced %d predecessor state proofs, want exactly 1", replaced)
	}

	tampered := *candidate
	if tampered.CollatedData, err = cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{
		WithCRC32C: true,
	}); err != nil {
		t.Fatalf("serialize tampered collated data: %v", err)
	}
	tampered.CollatedFileHash = sha256.Sum256(tampered.CollatedData)

	verification := shardVerificationRequest(req, &tampered)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())

	err = VerifyShardCandidate(context.Background(), verification)
	if err == nil {
		t.Fatal("candidate with an under-proven predecessor was accepted")
	}
	if strings.Contains(err.Error(), "collated state differs from supplied state") {
		t.Fatalf("rejection came from the resident cross-check, not from executing on the proof: %v", err)
	}
}

// collatedNeighborQueues re-resolves the shard neighbour queues out of collated
// data, the way loadExpectedNeighbors does in production. Without it the
// fixture hands validation the resident predecessor queue it captured when the
// request was built, and the queue half of the proof closure goes untested. The
// masterchain neighbour keeps its resident view on purpose: collated data
// carries no masterchain state proof.
func collatedNeighborQueues(tb testing.TB, req ShardRequest, candidate *Candidate) []Neighbor {
	tb.Helper()

	data, err := verifyCollatedData(candidate, nil, req.Header.GenUtime)
	if err != nil {
		tb.Fatalf("decode candidate collated data: %v", err)
	}
	if !data.full {
		tb.Fatal("candidate carries no full collated data")
	}

	neighbors := make([]Neighbor, len(req.Neighbors))
	copy(neighbors, req.Neighbors)
	proven := 0
	for i := range neighbors {
		if neighbors[i].Block.Workchain == address.MasterchainID {
			continue
		}
		stateRoot, rootErr := collatedStateRoot(&data, neighbors[i].Block)
		if rootErr != nil {
			tb.Fatalf("load collated neighbour state: %v", rootErr)
		}
		var state tlb.ShardStateUnsplit
		if err = parseProofExact(&state, stateRoot); err != nil {
			tb.Fatalf("decode collated neighbour state: %v", err)
		}
		neighbors[i].OutMsgQueueInfo = state.OutMsgQueueInfo
		proven++
	}
	if proven == 0 {
		tb.Fatal("fixture has no shard neighbour to prove")
	}

	return neighbors
}

// narrowedStateRoot returns the state root with every subtree pruned away. It
// keeps the root hash, which is all the collated binding checks compare, and
// loses everything a decode would need.
func narrowedStateRoot(tb testing.TB, state *cell.Cell) *cell.Cell {
	tb.Helper()

	proof, err := state.CreateHashUsageProof(func(cell.Hash) bool { return false })
	if err != nil {
		tb.Fatalf("build narrowed state proof: %v", err)
	}
	narrow, err := unwrapCollatedProof(proof)
	if err != nil {
		tb.Fatalf("unwrap narrowed state proof: %v", err)
	}
	if narrow.HashKeyAt(0) != state.HashKeyAt(0) {
		tb.Fatal("narrowed state root changed the state hash")
	}

	return narrow
}
