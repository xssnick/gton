package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// lazyParentChildlessBoundaries is the number of cells that are childless in the
// predecessor and therefore left ordinary when it is resident, but replaced by a
// pruned boundary when the same collation runs over a store-shaped one.
//
// This is a COUNT, and pinning a count rather than a byte figure is the whole
// point of this file. The bytes are not invariant: across the filler axis the
// same 213 decisions cost +30,816 B on a block with no filler accounts
// (+11.40% of it), +22,209 at 10,000 accounts (+6.06%) and +21,642 at 100,000
// (+5.09%) — the delta moves by 42% while the count does not move at all. A test
// pinning +21,642 would pin one coordinate of one fixture and fail the next time
// the account population changed, while saying nothing about the decision it is
// meant to guard.
//
// 213, not the 208 an earlier round reported. 208 is a different quantity that
// happens to sit nearby: it is the growth in DISTINCT cells of the block BoC
// (11,732 -> 11,940 at repeat=1, 17,750 -> 17,958 at repeat=3), which nets 213
// new boundaries against the 203 of them that the block already carried
// elsewhere and the 198 ancestors that changed hash. Counting the decisions
// directly, by walking the two blocks in lockstep, gives 213 at every point of
// the filler axis and at both repeats.
//
// The decision is not a defect and must not be "fixed". A placeholder standing
// in for an unresolved subtree reports no references because it is a
// placeholder, not because the subtree is childless; believing that report would
// spend a celldb read per cell on the collation hot path to save the boundary.
// The reference decides the same way, gating the childless check behind
// cell->is_loaded() (MerkleUpdate.cpp:228-240, ExtCell::is_loaded).
const lazyParentChildlessBoundaries = 213

// lazyParentLevelLiftedCells is the number of cells that are byte-for-byte
// identical between the two blocks, over children with identical hashes, and
// still diverge — because the lazy block's copy carries level mask 1 where the
// resident block's carries 0. A pruned boundary raises the level of every
// ancestor up to the Merkle root, so this is the same 213 decisions seen from
// above rather than a second effect, and it is why the classifier keeps it apart
// from a real body difference (see lazyCellSelfDiffers).
//
// 40 at every point of the matrix, on both axes, level 0 against level 1 in all
// of them. Pinned for the same reason the count above is: it does not move.
const lazyParentLevelLiftedCells = 40

// lazyParentPredecessor is the store-shaped form of a request's predecessor:
// serialized once and parsed back with every reference below the root left as an
// unresolved stub, which is the shape a predecessor loaded out of celldb has.
func lazyParentPredecessor(tb testing.TB, req ShardRequest) ShardRequest {
	tb.Helper()

	req.Previous.State = benchMainnetLazyPredecessor(tb, benchMainnetPredecessorBOC(tb, req))

	return req
}

// lazyParentRequest is one point of the fixture family with full collated data
// switched on. filler is honoured only off the fixture assembly, which pins its
// own account population, so the two axes are separate call shapes.
func lazyParentRequest(tb testing.TB, filler, repeat int, fixture bool) ShardRequest {
	tb.Helper()

	req := benchMainnetHandout(benchMainnetWorkloadFor(tb, benchMainnetVariant{
		filler:  filler,
		repeat:  repeat,
		fixture: fixture,
		mint:    true,
	}))
	req.Masterchain.Config.capabilities |= capFullCollatedData

	return req
}

func lazyParentCandidate(tb testing.TB, req ShardRequest) *Candidate {
	tb.Helper()

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		tb.Fatalf("collate: %v", err)
	}
	// capFullCollatedData off degenerates the collated data to a 33-byte marker,
	// and a block whose proofs were never built is not the block under test.
	if len(candidate.CollatedData) <= 33 {
		tb.Fatalf("collated data is %d bytes: the capability is not in effect",
			len(candidate.CollatedData))
	}

	return candidate
}

// prunedRepresentedHash is the hash a pruned branch stands for: type byte, level
// mask byte, then the represented hash.
func prunedRepresentedHash(tb testing.TB, c *cell.Cell) []byte {
	tb.Helper()

	slice, err := c.BeginParse()
	if err != nil {
		tb.Fatalf("open pruned branch: %v", err)
	}
	if err = slice.SkipBits(16); err != nil {
		tb.Fatalf("skip pruned branch header: %v", err)
	}
	hash, err := slice.LoadSlice(256)
	if err != nil {
		tb.Fatalf("read pruned branch hash: %v", err)
	}

	return hash
}

type lazyBoundaryCounts struct {
	// childless: the lazy block put a boundary where the resident block kept a
	// childless cell. This is the decision under test.
	childless int
	// withChildren: the lazy block put a boundary over a cell that really has
	// children. Nothing may land here — that would be a boundary the source
	// proof has to be able to expose, and a different decision entirely.
	withChildren int
	// expanded: the mirror image — the resident block pruned a subtree the lazy
	// block carries expanded, because a source graph built over placeholders
	// holds fewer hashes to prune onto. Benign and not invariant: 60 positions
	// with no filler accounts, 201 at 10,000, 204 at 100,000. Reported, never
	// pinned.
	expanded int
	// bothPruned: both sides put a boundary here and the two boundaries differ,
	// so they stand for different cells. Not a decision about childlessness at
	// all, and it used to fall through the classifier unnoticed: two pruned cells
	// both report zero references, so the ref-count arm agreed and the descent
	// arm had nothing to descend into.
	bothPruned int
	// body: the two cells agree on reference count and differ IN THEMSELVES — cell
	// type, bit length, or the bits. Nothing may land here: the parent shape
	// decides how the state update is pruned, never what the block says.
	//
	// This was the second silent drop. A pair that differs only in its own body
	// reached the descent arm, which walked the children, found them equal, and
	// returned having counted nothing — for a pair that provably diverges, since
	// the walk only reaches pairs whose hashes differ.
	body int
	// levelLifted: the two cells are byte-for-byte the same cell with the same
	// children, and differ only in level mask — the lazy one is level 1 because a
	// pruned boundary sits somewhere below it. This is not an independent
	// divergence but the upward propagation of one already counted in childless,
	// which is exactly why it needs its own counter rather than being folded into
	// body: it is expected, and a classifier that called it a body difference
	// could never assert body == 0.
	//
	// Measured at 40 on every arm of the matrix, level 0 against level 1 in all of
	// them, with identical bits and identical bit lengths. Pinned for the same
	// reason childless is: it is a count that does not move along either axis.
	levelLifted int
	// other: any divergence that is none of the above. With bothPruned and body
	// classified, the only shape left here is a reference-count mismatch.
	other int
}

// lazyCellSelfDiffers classifies how two cells differ in themselves, ignoring
// their references. It is called only on a pair whose hashes already differ, so
// "neither" means the whole divergence is below this cell.
//
// body covers what the block SAYS: cell type, bit length, the bits. levelLifted
// covers what the block's shape implies: the same bytes at a higher level mask,
// which is what a pruned boundary somewhere below does to every ancestor. Keeping
// them apart is the difference between an assertion and a report.
func lazyCellSelfDiffers(tb testing.TB, resident, lazy *cell.Cell) (body, levelLifted bool) {
	tb.Helper()

	if resident.GetType() != lazy.GetType() || resident.BitsSize() != lazy.BitsSize() {
		return true, false
	}
	if bits := resident.BitsSize(); bits > 0 {
		residentSlice, err := resident.BeginParse()
		if err != nil {
			tb.Fatalf("open resident cell: %v", err)
		}
		lazySlice, err := lazy.BeginParse()
		if err != nil {
			tb.Fatalf("open lazy cell: %v", err)
		}
		residentBits, err := residentSlice.LoadSlice(bits)
		if err != nil {
			tb.Fatalf("read resident cell bits: %v", err)
		}
		lazyBits, err := lazySlice.LoadSlice(bits)
		if err != nil {
			tb.Fatalf("read lazy cell bits: %v", err)
		}
		if !bytes.Equal(residentBits, lazyBits) {
			return true, false
		}
	}

	return false, resident.LevelMask() != lazy.LevelMask()
}

// countLazyBoundaries walks the two blocks in lockstep and classifies every
// position where they diverge. Equal hashes mean the whole subtree below is
// equal, so the walk stops there; what is left is exactly the set of cells the
// two parent shapes decided differently about.
//
// EVERY divergence must land in a counter. That is the property the caller's
// counts.other == 0 assertion is worth something under, and it did not hold
// before: a pair of differing pruned boundaries and a pair differing only in
// their own bits both reached the descent arm and were counted nowhere, so a
// class of divergence nobody had thought about would have been reported as
// "no unknown divergence". The classifier is total now — pruned/pruned,
// pruned/ordinary either way, ref-count mismatch, body difference — and the
// counters the caller does not pin are logged.
func countLazyBoundaries(tb testing.TB, resident, lazy *cell.Cell) lazyBoundaryCounts {
	tb.Helper()

	type pair struct{ resident, lazy *cell.Cell }
	var counts lazyBoundaryCounts
	seen := make(map[[2]cell.Hash]struct{})
	stack := []pair{{resident, lazy}}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if bytes.Equal(p.resident.Hash(), p.lazy.Hash()) {
			continue
		}
		key := [2]cell.Hash{p.resident.HashKey(), p.lazy.HashKey()}
		if _, known := seen[key]; known {
			continue
		}
		seen[key] = struct{}{}

		residentPruned := p.resident.GetType() == cell.PrunedCellType
		lazyPruned := p.lazy.GetType() == cell.PrunedCellType
		switch {
		case lazyPruned && residentPruned:
			counts.bothPruned++
		case lazyPruned && !residentPruned:
			// The boundary must stand for the very cell the resident block put
			// there, or this is not the divergence under test.
			if got := prunedRepresentedHash(tb, p.lazy); !bytes.Equal(got, p.resident.Hash()) {
				tb.Fatalf("a lazy boundary stands for %x, not for the %x the resident block holds",
					got[:8], p.resident.Hash()[:8])
			}
			if p.resident.RefsNum() == 0 {
				counts.childless++
			} else {
				counts.withChildren++
			}
		case residentPruned && !lazyPruned:
			// Same cell, carried instead of pruned. Level 0 is what identifies
			// it: the expanded copy has level 1 because something below it is
			// pruned, so its top-level hash is not the one the boundary names.
			if got := prunedRepresentedHash(tb, p.resident); !bytes.Equal(got, p.lazy.Hash(0)) {
				tb.Fatalf("a resident boundary stands for %x, which is not the %x the lazy block carries",
					got[:8], p.lazy.Hash(0)[:8])
			}
			counts.expanded++
		case p.resident.RefsNum() != p.lazy.RefsNum():
			counts.other++
		default:
			// Counted AND descended: a cell can diverge in its own bits and below
			// them at the same time, and stopping here would drop the children.
			switch body, levelLifted := lazyCellSelfDiffers(tb, p.resident, p.lazy); {
			case body:
				counts.body++
			case levelLifted:
				counts.levelLifted++
			}
			for i := range int(p.resident.RefsNum()) {
				residentRef, err := p.resident.PeekRef(i)
				if err != nil {
					tb.Fatal(err)
				}
				lazyRef, err := p.lazy.PeekRef(i)
				if err != nil {
					tb.Fatal(err)
				}
				stack = append(stack, pair{residentRef, lazyRef})
			}
		}
	}

	return counts
}

// The matrix has to cover distinct POINTS, not distinct arm names. It used to
// have five arms over four points: benchMainnetFiller is 100_000, and the
// fixture assembly at repeat=1 pins the same account population, so
// {filler=100000, repeat=1} and {fixture, repeat=1} collated the same block
// twice — identical to the byte, 424,977 -> 446,619 with 204 expanded positions
// in both. The duplicate is replaced by {filler=0, repeat=3}, which is the one
// corner the old matrix left uncovered: the repeat axis over a shard with no
// filler accounts. distinctPoints below is what keeps the property mechanical
// rather than remembered.

// countLazyBoundaries is the instrument the matrix above reads, so its blind
// spots are the matrix's blind spots. Both of the ones it used to have are
// driven here on hand-built pairs, because neither is reachable from the mainnet
// fixture — which is exactly what made them survive: a classifier that returns
// all zeros for a divergence it cannot name reports "no unknown divergence", and
// the arms above would have passed while missing the class entirely.
func TestCountLazyBoundariesClassifiesEveryDivergence(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(0x5e, 8).EndCell()
	other := cell.BeginCell().MustStoreUInt(0x5f, 8).EndCell()

	// Body: equal reference count, identical children, different bits. The old
	// classifier descended into the (equal) children and counted nothing.
	residentBody := cell.BeginCell().MustStoreUInt(1, 8).MustStoreRef(leaf).EndCell()
	lazyBody := cell.BeginCell().MustStoreUInt(2, 8).MustStoreRef(leaf).EndCell()
	if bytes.Equal(residentBody.Hash(), lazyBody.Hash()) {
		t.Fatal("the body fixture does not diverge")
	}
	if counts := countLazyBoundaries(t, residentBody, lazyBody); counts.body != 1 ||
		counts.childless != 0 || counts.withChildren != 0 || counts.expanded != 0 ||
		counts.bothPruned != 0 || counts.levelLifted != 0 || counts.other != 0 {
		t.Fatalf("a body divergence was classified as %+v", counts)
	}

	// Both pruned, standing for different cells. Two pruned cells both report
	// zero references, so the ref-count arm agreed and the descent arm had
	// nothing to walk. A boundary can only be cut over a cell that has children —
	// CreatePrunedBranch hands a childless leaf back unchanged — so the two
	// sources carry one reference each.
	residentSubtree := cell.BeginCell().MustStoreUInt(1, 8).MustStoreRef(leaf).EndCell()
	lazySubtree := cell.BeginCell().MustStoreUInt(1, 8).MustStoreRef(other).EndCell()
	residentPruned := mustPrunedFor(t, residentSubtree)
	lazyPruned := mustPrunedFor(t, lazySubtree)
	if residentPruned.GetType() != cell.PrunedCellType || lazyPruned.GetType() != cell.PrunedCellType {
		t.Fatalf("the fixture is not two boundaries: %v and %v",
			residentPruned.GetType(), lazyPruned.GetType())
	}
	if counts := countLazyBoundaries(t, residentPruned, lazyPruned); counts.bothPruned != 1 ||
		counts.childless != 0 || counts.withChildren != 0 || counts.expanded != 0 ||
		counts.body != 0 || counts.levelLifted != 0 || counts.other != 0 {
		t.Fatalf("two differing boundaries were classified as %+v", counts)
	}

	// And the classes that were already there still land where they did: a
	// boundary cut over a cell that really has children is withChildren and not
	// the childless decision the matrix pins.
	if counts := countLazyBoundaries(t, residentSubtree, residentPruned); counts.withChildren != 1 ||
		counts.childless != 0 || counts.body != 0 || counts.bothPruned != 0 || counts.other != 0 {
		t.Fatalf("a boundary over a cell with children was classified as %+v", counts)
	}
	// The mirror direction is expanded.
	if counts := countLazyBoundaries(t, residentPruned, residentSubtree); counts.expanded != 1 ||
		counts.childless != 0 || counts.body != 0 || counts.bothPruned != 0 || counts.other != 0 {
		t.Fatalf("a carried subtree against a boundary was classified as %+v", counts)
	}
	// And a reference-count mismatch is the one shape left for other.
	if counts := countLazyBoundaries(t, residentSubtree, leaf); counts.other != 1 ||
		counts.body != 0 || counts.bothPruned != 0 || counts.levelLifted != 0 {
		t.Fatalf("a reference-count mismatch was classified as %+v", counts)
	}
}

func mustPrunedFor(tb testing.TB, c *cell.Cell) *cell.Cell {
	tb.Helper()

	pruned, err := cell.CreatePrunedBranch(c, 1, 0)
	if err != nil {
		tb.Fatal(err)
	}

	return pruned
}

func TestLazyParentPrunesExactlyTheChildlessBoundaries(t *testing.T) {
	type point struct{ resident, lazy int }
	distinctPoints := map[point]string{}
	for _, arm := range []struct {
		name    string
		filler  int
		repeat  int
		fixture bool
	}{
		{"filler=0", 0, 1, false},
		{"filler=10000", 10_000, 1, false},
		{"filler=0 repeat=3", 0, 3, false},
		{"fixture repeat=1", benchMainnetFiller, 1, true},
		{"fixture repeat=3", benchMainnetFiller, 3, true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			resident := lazyParentCandidate(t, lazyParentRequest(t, arm.filler, arm.repeat, arm.fixture))
			lazy := lazyParentCandidate(t,
				lazyParentPredecessor(t, lazyParentRequest(t, arm.filler, arm.repeat, arm.fixture)))

			residentRoot, err := cell.FromBOC(resident.BlockBOC)
			if err != nil {
				t.Fatal(err)
			}
			lazyRoot, err := cell.FromBOC(lazy.BlockBOC)
			if err != nil {
				t.Fatal(err)
			}

			counts := countLazyBoundaries(t, residentRoot, lazyRoot)
			delta := len(lazy.BlockBOC) - len(resident.BlockBOC)
			t.Logf("%s: %d childless boundaries, %d expanded, %d level-lifted, %d body, "+
				"%d both-pruned; block %d B -> %d B (+%d, +%.2f%%)",
				arm.name, counts.childless, counts.expanded, counts.levelLifted,
				counts.body, counts.bothPruned,
				len(resident.BlockBOC), len(lazy.BlockBOC), delta,
				100*float64(delta)/float64(len(resident.BlockBOC)))

			// One arm, one point. A duplicate arm inflates the apparent coverage of
			// this matrix without adding an input, which is how five arms came to
			// cover four points.
			here := point{resident: len(resident.BlockBOC), lazy: len(lazy.BlockBOC)}
			if other, clash := distinctPoints[here]; clash {
				t.Fatalf("%s collates the same %d -> %d block as %s: the matrix has two arms "+
					"over one point", arm.name, here.resident, here.lazy, other)
			}
			distinctPoints[here] = arm.name

			if counts.childless != lazyParentChildlessBoundaries {
				t.Fatalf("%s: %d childless cells became boundaries, want %d",
					arm.name, counts.childless, lazyParentChildlessBoundaries)
			}
			if counts.withChildren != 0 {
				t.Fatalf("%s: %d cells with children were pruned under the lazy parent",
					arm.name, counts.withChildren)
			}
			if counts.other != 0 {
				t.Fatalf("%s: the two blocks diverge at %d positions of no known class",
					arm.name, counts.other)
			}
			// Two boundaries standing for different cells would be a proof-shape
			// divergence rather than a childlessness decision, and nothing in this
			// fixture family produces one.
			if counts.bothPruned != 0 {
				t.Fatalf("%s: %d positions hold two different boundaries", arm.name, counts.bothPruned)
			}
			// The parent shape may change how the state update is pruned. It may not
			// change a single bit the block states.
			if counts.body != 0 {
				t.Fatalf("%s: %d cells differ in their own contents, not in their pruning",
					arm.name, counts.body)
			}
			if counts.levelLifted != lazyParentLevelLiftedCells {
				t.Fatalf("%s: %d ancestors were lifted to a higher level mask, want %d",
					arm.name, counts.levelLifted, lazyParentLevelLiftedCells)
			}
			// The delta must move along this axis, or the fixture is not
			// exercising the axis and the count above proves less than it looks.
			if delta <= 0 {
				t.Fatalf("%s: the lazy parent produced a %d-byte delta", arm.name, delta)
			}
		})
	}
}

// Anyone about to build a gate on "recollate the block and compare" needs this
// before they build it: the two parent shapes produce different RootHash and
// FileHash. MerkleUpdate hashes its children at level 1 — by their pruned
// representation — so replacing a childless cell with a boundary changes the
// update's hash, and the block's with it. Both blocks are valid, each under
// either parent shape; they are simply not the same bytes, and which one a
// collator produces is decided by the shape of the predecessor it was handed.
func TestLazyParentChangesRootAndFileHash(t *testing.T) {
	residentReq := lazyParentRequest(t, benchMainnetFiller, 1, true)
	resident := lazyParentCandidate(t, residentReq)
	lazyReq := lazyParentPredecessor(t, lazyParentRequest(t, benchMainnetFiller, 1, true))
	lazy := lazyParentCandidate(t, lazyReq)

	residentFile := sha256.Sum256(resident.BlockBOC)
	lazyFile := sha256.Sum256(lazy.BlockBOC)
	t.Logf("resident root %s file %s (%d B block, %d B collated)",
		hex.EncodeToString(resident.ID.RootHash[:8]), hex.EncodeToString(residentFile[:8]),
		len(resident.BlockBOC), len(resident.CollatedData))
	t.Logf("lazy     root %s file %s (%d B block, %d B collated)",
		hex.EncodeToString(lazy.ID.RootHash[:8]), hex.EncodeToString(lazyFile[:8]),
		len(lazy.BlockBOC), len(lazy.CollatedData))

	if bytes.Equal(resident.ID.RootHash, lazy.ID.RootHash) {
		t.Fatal("the two parent shapes produced the same root hash — either the pruning " +
			"predicate changed or the fixture is not store-shaped")
	}
	if residentFile == lazyFile {
		t.Fatal("the two parent shapes produced the same file hash")
	}
	// Everything else about the collation is the same block. If these diverge,
	// the parent shape has started changing what was executed rather than how
	// the result was serialized, and that would be a defect.
	if resident.Stats.Transactions != lazy.Stats.Transactions ||
		resident.Stats.GasUsed != lazy.Stats.GasUsed {
		t.Fatalf("the parent shape changed execution: %d tx/%d gas resident against %d tx/%d gas lazy",
			resident.Stats.Transactions, resident.Stats.GasUsed,
			lazy.Stats.Transactions, lazy.Stats.GasUsed)
	}
	// Each block must be individually stable, or the counts elsewhere are noise.
	again := lazyParentCandidate(t, lazyParentPredecessor(t, lazyParentRequest(t, benchMainnetFiller, 1, true)))
	if !bytes.Equal(again.BlockBOC, lazy.BlockBOC) {
		t.Fatal("two collations over a fresh store-shaped parent produced different blocks")
	}

	// And both are valid under either shape. testCandidateTransitionVerifier is
	// the accepting stub, which would make this assertion unreachable, so the
	// real semantic verifier is wired in on purpose.
	for _, arm := range []struct {
		name      string
		req       ShardRequest
		candidate *Candidate
	}{
		{"lazy block against a resident parent", residentReq, lazy},
		{"resident block against a lazy parent", lazyReq, resident},
		{"lazy block against a lazy parent", lazyReq, lazy},
		{"resident block against a resident parent", residentReq, resident},
	} {
		verification := shardVerificationRequest(arm.req, arm.candidate)
		verification.NeighborShardEndLT = arm.req.NeighborShardEndLT
		verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
		if err := VerifyShardCandidate(context.Background(), verification); err != nil {
			t.Fatalf("%s: %v", arm.name, err)
		}
	}
}

// The block must not be a function of the memo hint the previous build left
// behind. The hint is a capacity estimate carried on the Builder, and one
// Builder serves both chains, so a masterchain build's figure sizes the next
// shard build; a hint that reached the output would make the block depend on
// what was collated before it, and on which chain.
//
// It did. The hint gates the state update's worker count at hint+hint/8 >= 256,
// and over a store-shaped parent the forked source-proof build gave the branch
// its own build memo: a subtree reached from both sides was then built twice,
// once from the resolved instance and once from the placeholder, and those are
// not the same cell. The update's OLD side took different values either side of
// the gate — 0/228/255 gave one and 200/227 another — while the destination
// side, the collated data, the boundary set and the block's length were
// identical, which is why nothing else noticed. The fork is now withheld over a
// lazy source (readset_update.go), and the value below is the one the
// sequential build has always produced.
//
// The sweep straddles the gate on purpose: 200 and 227 derive to 225 and 255,
// below it, and 228 and 255 to 256 and 286, above. A hint of 0 means "no
// estimate" and sizes from the read set instead, which is a third path through
// the same code. A fresh parent per run holds the parent SHAPE fixed, which is
// a different variable and one that legitimately moves the hash — see
// TestLazyParentChangesRootAndFileHash.
func TestLazyParentBlockDoesNotDependOnTheCarriedMemoHint(t *testing.T) {
	for _, parent := range []struct {
		name  string
		build func(testing.TB) ShardRequest
	}{
		{"lazy", func(tb testing.TB) ShardRequest {
			return lazyParentPredecessor(tb, lazyParentRequest(tb, benchMainnetFiller, 1, true))
		}},
		{"resident", func(tb testing.TB) ShardRequest {
			return lazyParentRequest(tb, benchMainnetFiller, 1, true)
		}},
	} {
		t.Run(parent.name, func(t *testing.T) {
			var want [2][32]byte
			for i, hint := range []int{0, 200, 227, 228, 255, 4096, 1 << 20} {
				builder := testBuilder()
				builder.observeBuildSizes(0, 0, 0, hint)
				candidate, err := builder.BuildShard(context.Background(), parent.build(t))
				if err != nil {
					t.Fatalf("hint %d: collate: %v", hint, err)
				}
				if len(candidate.CollatedData) <= 33 {
					t.Fatalf("hint %d: collated data is %d bytes: the capability is not in effect",
						hint, len(candidate.CollatedData))
				}
				got := [2][32]byte{
					sha256.Sum256(candidate.BlockBOC),
					sha256.Sum256(candidate.CollatedData),
				}
				if i == 0 {
					want = got
					t.Logf("%s parent: block %s collated %s", parent.name,
						hex.EncodeToString(got[0][:8]), hex.EncodeToString(got[1][:8]))
					continue
				}
				if got[0] != want[0] {
					t.Errorf("hint %d moved the block: %s against %s", hint,
						hex.EncodeToString(got[0][:8]), hex.EncodeToString(want[0][:8]))
				}
				if got[1] != want[1] {
					t.Errorf("hint %d moved the collated data: %s against %s", hint,
						hex.EncodeToString(got[1][:8]), hex.EncodeToString(want[1][:8]))
				}
			}
		})
	}
}
