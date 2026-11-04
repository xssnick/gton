package collator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type collatedRootCase struct {
	name            string
	roots           []*cell.Cell
	wantError       string
	states, storage int
}

func TestVerifyGV16CollatedRoots(t *testing.T) {
	hidden := cell.BeginCell().MustStoreUInt(123, 8).MustStoreRef(cell.BeginCell().MustStoreUInt(42, 8).EndCell()).EndCell()
	pruned, err := cell.CreatePrunedBranch(hidden, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw := cell.BeginCell().MustStoreUInt(456, 32).MustStoreRef(pruned).EndCell()
	declaration := func(tag uint64) *cell.Cell {
		return cell.BeginCell().MustStoreUInt(tag, 32).MustStoreSlice(raw.Hash(0), 256).EndCell()
	}
	state := declaration(collatedRootStateTag)
	storage := declaration(collatedRootStorageTag)
	separator := cell.BeginCell().MustStoreUInt(collatedSeparatorTag, 32).EndCell()
	consensus := collatedTestConsensus(100)
	legacy, err := cell.CreateMerkleProof(raw.Virtualize(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []collatedRootCase{
		{name: "states and storage share a raw root", roots: []*cell.Cell{consensus, state, storage, separator, raw}, states: 1, storage: 1},
		{name: "duplicate raw roots match reference", roots: []*cell.Cell{consensus, state, separator, raw, raw}, states: 1},
		{name: "unused raw root matches reference", roots: []*cell.Cell{consensus, separator, raw}},
		{name: "declaration without separator", roots: []*cell.Cell{consensus, state}, wantError: "is absent"},
		{name: "missing state", roots: []*cell.Cell{consensus, state, separator}, wantError: "is absent"},
		{name: "missing storage", roots: []*cell.Cell{consensus, storage, separator}, wantError: "is absent"},
		{name: "duplicate state declaration", roots: []*cell.Cell{consensus, state, state, separator, raw}, wantError: "duplicate collated proof"},
		{name: "duplicate storage declaration", roots: []*cell.Cell{consensus, storage, storage, separator, raw}, wantError: "duplicate account storage"},
		{name: "old and new duplicate", roots: []*cell.Cell{consensus, legacy, state, separator, raw}, wantError: "duplicate collated proof"},
		{name: "nonzero level before separator", roots: []*cell.Cell{consensus, raw, separator}, wantError: "non-zero level"},
		{name: "malformed declaration", roots: []*cell.Cell{consensus, cell.BeginCell().MustStoreUInt(collatedRootStateTag, 32).EndCell()}, wantError: "malformed collated root declaration"},
		{name: "malformed separator", roots: []*cell.Cell{consensus, cell.BeginCell().MustStoreUInt(collatedSeparatorTag, 32).MustStoreRef(state).EndCell()}, wantError: "malformed collated separator"},
		{name: "consensus after separator", roots: []*cell.Cell{separator, consensus}, wantError: "no consensus root"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := verifyCollatedRoots(test.roots, 100)
			if test.wantError != "" {
				if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error=%v want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got.virtualRoots) != test.states || len(got.accountStorage) != test.storage {
				t.Fatalf("states=%d storage=%d", len(got.virtualRoots), len(got.accountStorage))
			}
			boc, err := cell.ToBOCWithOptionsErr(test.roots, cell.BOCSerializeOptions{WithCRC32C: true})
			if err != nil {
				t.Fatal(err)
			}
			roots, err := cell.FromBOCMultiRootWithOptions(boc, cell.BOCParseOptions{AllowNonZeroLevelRoot: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = verifyCollatedRoots(roots, 100); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGV16CollatedProofTreesUnionSelectedBranches(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(1, 8).MustStoreRef(cell.BeginCell().MustStoreUInt(11, 8).EndCell()).EndCell()
	right := cell.BeginCell().MustStoreUInt(2, 8).MustStoreRef(cell.BeginCell().MustStoreUInt(22, 8).EndCell()).EndCell()
	shared := cell.BeginCell().MustStoreUInt(3, 8).MustStoreRef(left).MustStoreRef(right).EndCell()
	state := cell.BeginCell().MustStoreUInt(4, 32).MustStoreRef(shared).EndCell()
	stateProof, err := state.CreateHashUsageProof(func(hash cell.Hash) bool {
		return hash == state.HashKey() || hash == shared.HashKey() || hash == left.HashKey()
	})
	if err != nil {
		t.Fatal(err)
	}
	storageProof, err := shared.CreateHashUsageProof(func(hash cell.Hash) bool { return hash == shared.HashKey() || hash == right.HashKey() })
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCollatedProofTrees([]*cell.Cell{collatedTestConsensus(100), stateProof, wrapAccountStorageProof(storageProof)})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCollatedRoots(encoded, 100)
	if err != nil {
		t.Fatal(err)
	}
	provenState := verified.virtualRoots[state.HashKey()]
	provenStorage := verified.accountStorage[shared.HashKey()]
	for _, root := range []*cell.Cell{provenState.MustPeekRef(0), provenStorage} {
		for i := 0; i < 2; i++ {
			if _, err := root.MustPeekRef(i).BeginParse(); err != nil {
				t.Fatalf("shared branch %d omitted: %v", i, err)
			}
		}
	}
	rawState, rawStorage := encoded[len(encoded)-2], encoded[len(encoded)-1]
	if rawState.MustPeekRef(0) != rawStorage {
		t.Fatal("shared tree was rebuilt independently")
	}
}

func TestBuildAndVerifyGV16CollatedCandidates(t *testing.T) {
	t.Run("mainnet transactions", func(t *testing.T) {
		req := fullCollatedMainnetRequest(t)
		req.Masterchain.Config.globalVersion = 16
		candidate, err := testBuilder().BuildShard(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		verification := shardVerificationRequest(req, candidate)
		verification.NeighborShardEndLT = req.NeighborShardEndLT
		verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
		verification.Previous.State = narrowedStateRoot(t, req.Previous.State)
		verification.Neighbors = collatedNeighborQueues(t, req, candidate)
		if err = verifyShardCandidateForTest(context.Background(), verification); err != nil {
			t.Fatal(err)
		}
		assertGV16CollatedCandidate(t, candidate, req.Header.GenUtime)
	})
	t.Run("shard", func(t *testing.T) {
		req := advanceCandidateRequest(t, emptyCandidateRequest(t))
		req.Masterchain.Config.capabilities |= capFullCollatedData
		req.Masterchain.Config.globalVersion = 16
		attachFullCollatedTestNeighbors(t, &req)
		candidate, err := testBuilder().BuildShard(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		verification := shardVerificationRequest(req, candidate)
		if err = verifyShardCandidateForTest(context.Background(), verification); err != nil {
			t.Fatal(err)
		}
		assertGV16CollatedCandidate(t, candidate, req.Header.GenUtime)
	})
	t.Run("master", func(t *testing.T) {
		fixture := newFullMasterFixture(t)
		fixture.request.Config.globalVersion = 16
		candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
		if err != nil {
			t.Fatal(err)
		}
		verification := MasterVerificationRequest{Previous: fixture.request.Previous, Config: fixture.request.Config, Groups: fixture.request.Groups, ShardTops: fixture.request.ShardTops, Neighbors: fixture.request.Neighbors, Semantics: testCandidateTransitionVerifier, Candidate: candidate}
		if err = verifyMasterCandidateForTest(context.Background(), verification); err != nil {
			t.Fatal(err)
		}
		assertGV16CollatedCandidate(t, candidate, fixture.request.Header.GenUtime)
	})
}

func assertGV16CollatedCandidate(t *testing.T, candidate *Candidate, genUtime uint32) {
	t.Helper()
	roots, err := cell.FromBOCMultiRootWithOptions(candidate.CollatedData, cell.BOCParseOptions{AllowNonZeroLevelRoot: true})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCollatedRoots(roots, genUtime)
	if err != nil {
		t.Fatal(err)
	}
	if !verified.full {
		t.Fatal("candidate does not carry full proofs")
	}
	separator := -1
	for i, root := range roots {
		if root.Level() == 0 && !root.IsSpecial() {
			s := root.MustBeginParse()
			if s.BitsLeft() == 32 && s.MustLoadUInt(32) == collatedSeparatorTag {
				separator = i
				break
			}
		}
	}
	if separator < 0 || separator == len(roots)-1 {
		t.Fatal("missing separator or raw proofs")
	}
}

func TestGV16CollatedProofTreesPreservesNestedMerkleLevels(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	hidden := cell.BeginCell().MustStoreUInt(2, 8).MustStoreRef(leaf).EndCell()
	inner := cell.BeginCell().MustStoreUInt(3, 8).MustStoreRef(hidden).EndCell()
	innerProof, err := cell.CreateMerkleProof(inner)
	if err != nil {
		t.Fatal(err)
	}
	state := cell.BeginCell().MustStoreUInt(4, 32).MustStoreRef(innerProof).EndCell()
	selected := map[cell.Hash]bool{state.HashKey(): true, innerProof.HashKey(): true, inner.HashKey(): true}
	oldProof, err := state.CreateHashUsageProof(func(hash cell.Hash) bool { return selected[hash] })
	if err != nil {
		t.Fatal(err)
	}
	roots, err := encodeCollatedProofTrees([]*cell.Cell{collatedTestConsensus(100), oldProof})
	if err != nil {
		t.Fatal(err)
	}
	raw := roots[len(roots)-1]
	if raw.HashKeyAt(0) != state.HashKey() {
		t.Fatal("nested Merkle proof changed the state hash")
	}
	boundary := raw.MustPeekRef(0).MustPeekRef(0).MustPeekRef(0)
	if boundary.Level() != 3 || boundary.GetType() != cell.PrunedCellType {
		t.Fatalf("boundary level/type = %d/%v", boundary.Level(), boundary.GetType())
	}
	boc, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := cell.FromBOCMultiRootWithOptions(boc, cell.BOCParseOptions{AllowNonZeroLevelRoot: true})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCollatedRoots(decoded, 100)
	if err != nil {
		t.Fatal(err)
	}
	if verified.virtualRoots[state.HashKey()] == nil {
		t.Fatal("nested proof state is absent")
	}
	size, err := collatedBOCSize(roots)
	if err != nil {
		t.Fatal(err)
	}
	if size != uint64(len(boc)) {
		t.Fatalf("estimated BOC size=%d actual=%d", size, len(boc))
	}
}

func BenchmarkGV16CollatedProofTrees(b *testing.B) {
	req := fullCollatedMainnetRequest(b)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := encodeCollatedProofTrees(roots); err != nil {
			b.Fatal(err)
		}
	}
}
