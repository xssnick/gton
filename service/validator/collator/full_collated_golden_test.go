package collator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The golden fixture that guards byte-identity elsewhere runs with full collated
// data switched off, so its candidate carries an empty collated marker and the
// proof machinery — predecessor state proof, account storage proofs, the size
// estimator that admits against them — is exercised by nothing that pins bytes.
// Those proofs are what remote validators check the block against, so a silent
// change there is worse than a changed block: the block still verifies locally.
//
// What is pinned here is the property, not a constant: the same inputs must
// produce the same proofs. A hash constant would be the stronger guard, but it
// can only be taken from a tree that is not being changed underneath it, and
// pinning one during in-flight work encodes somebody's intermediate state as
// the reference.

func fullCollatedMainnetCandidate(tb testing.TB) (ShardRequest, *Candidate) {
	tb.Helper()

	now, floorLT := benchMainnetTimeBase(tb)
	base := benchRebaseTime(tb, emptyCandidateRequest(tb), now, floorLT)
	req, _ := benchMainnetRequestFrom(tb, 0, 1, true, base)
	req.Masterchain.Config.capabilities |= capFullCollatedData

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		tb.Fatalf("collate mainnet workload with full collated data: %v", err)
	}
	return req, candidate
}

func TestFullCollatedMainnetCandidateCarriesAnOpenableProof(t *testing.T) {
	req, candidate := fullCollatedMainnetCandidate(t)

	if len(candidate.CollatedData) < 1024 {
		t.Fatalf("collated data is %d bytes, so the proof paths did not run",
			len(candidate.CollatedData))
	}
	blockSum := sha256.Sum256(candidate.BlockBOC)
	collatedSum := sha256.Sum256(candidate.CollatedData)
	t.Logf("block %d bytes %s, collated %d bytes %s",
		len(candidate.BlockBOC), hex.EncodeToString(blockSum[:]),
		len(candidate.CollatedData), hex.EncodeToString(collatedSum[:]))

	// A proof that serialises but does not open is the failure a hash alone
	// would let through whenever the hash itself is regenerated.
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatalf("decode collated data: %v", err)
	}
	var proven *cell.Cell
	for _, root := range roots {
		if unwrapped, unwrapErr := cell.UnwrapProof(root, req.Previous.State.Hash()); unwrapErr == nil {
			proven = unwrapped
			break
		}
	}
	if proven == nil {
		t.Fatal("collated data carries no predecessor state proof")
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, proven); err != nil {
		t.Fatalf("parse proven predecessor state: %v", err)
	}
	if _, err = state.Accounts.ShardAccounts.LoadRootExtra(); err != nil {
		t.Fatalf("proven predecessor accounts root extra: %v", err)
	}
}

func TestFullCollatedLazyPredecessorDoesNotMaterializeIdleAccounts(t *testing.T) {
	req := emptyCandidateRequest(t)
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	code := externalAcceptCode(t)
	for i := range 2_048 {
		benchSetAccount(t, accounts, activeContract{
			address: benchAddress("lazy-proof-idle", i),
			code:    code,
			balance: 1_000_000_000,
		}, req.Header.GenUtime-1)
	}
	req.Previous.State = benchPreviousState(t, req.Previous.State, accounts, nil, nil)
	req = benchMintPredecessorBlock(t, req)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	predecessorBOC, err := req.Previous.State.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithIndex:     true,
		WithCacheBits: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lazyPredecessor, err := cell.FromBOCWithOptions(predecessorBOC, cell.BOCParseOptions{Lazy: true})
	if err != nil {
		t.Fatal(err)
	}
	req.Previous.State = lazyPredecessor

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.CollatedData)*4 >= len(predecessorBOC) {
		t.Fatalf("idle proof materialized the predecessor: collated=%d predecessor=%d",
			len(candidate.CollatedData), len(predecessorBOC))
	}
	t.Logf("idle lazy predecessor %d bytes produced %d bytes of collated data",
		len(predecessorBOC), len(candidate.CollatedData))
}

// The proof path must be stable run to run. This is the guard that matters most
// here: a proof whose contents depend on map iteration or goroutine scheduling
// passes any single golden check and still makes two validators disagree, and
// unlike the block itself nothing downstream would notice locally.
func TestFullCollatedMainnetCandidateIsDeterministic(t *testing.T) {
	_, first := fullCollatedMainnetCandidate(t)
	firstBlock := sha256.Sum256(first.BlockBOC)
	firstCollated := sha256.Sum256(first.CollatedData)

	// Five collations catch the observed divergence rate (roughly one run in
	// three to one in twelve, depending on the tree) most of the time; a loop
	// that has to be lucky is a loop that will be believed when it is quiet.
	for i := range 8 {
		_, next := fullCollatedMainnetCandidate(t)
		if sha256.Sum256(next.BlockBOC) != firstBlock {
			t.Fatalf("run %d produced a different block", i+1)
		}
		if sha256.Sum256(next.CollatedData) != firstCollated {
			firstRoots, firstErr := cell.FromBOCMultiRoot(first.CollatedData)
			nextRoots, nextErr := cell.FromBOCMultiRoot(next.CollatedData)
			if firstErr != nil || nextErr != nil {
				t.Fatalf("run %d produced different collated data (decode first=%v next=%v)", i+1, firstErr, nextErr)
			}
			for root := range min(len(firstRoots), len(nextRoots)) {
				if firstRoots[root].HashKey() == nextRoots[root].HashKey() {
					continue
				}
				t.Logf("collated root %d changed: %x -> %x", root, firstRoots[root].Hash(), nextRoots[root].Hash())
			}
			t.Fatalf("run %d produced different collated data (%d roots -> %d roots)", i+1, len(firstRoots), len(nextRoots))
		}
	}
}
