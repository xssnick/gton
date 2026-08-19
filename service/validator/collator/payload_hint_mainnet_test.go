package collator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The recorded output of the mainnet collated fixture on the four arms the
// broadcast payload hint has to leave untouched: two predecessor shapes at
// repeat=1 and repeat=3. The hint sizes scratch structures inside a third
// serialization and cannot reach these bytes, which is exactly why they are
// pinned — a change here means something other than the hint moved.
//
// Regenerate only from a run whose output was reviewed on purpose.
//
// PROVENANCE OF THE repeat=3 VALUES. They were re-pinned on 2026-08-17 after a
// known divergence was closed, and the values they replaced were produced BY
// that divergence:
//
//	resident     block 666492 / 2c75b97c8b66dba9…  ->  667523 / 4f7aa861096081ef…
//	store-shaped block 688134 / c59d879175c34ea7…  ->  689165 / d6b960ce67e04963…
//
// (collated data did not move on either arm: 421534 / fd27f9fa… and
// 420107 / 7470db59… still stand.)
//
// The divergence was in the block-limit size estimator's proof walk, which
// charged a 40-byte boundary for cells the produced update never prunes onto —
// see the dedup-before-read-set ordering in tonutils-go
// tvm/cell/cell_storage_stat.go walk(). It over-estimated the block by 4,732 B
// against a 1,048,576 B budget on the repeat=3 arm and stopped the import loop
// six inbound messages early. The old bytes above are therefore the bytes of a
// block that was six transactions short.
//
// VALIDATED AGAINST THE REFERENCE, NOT AGAINST OURSELVES. Both repeat=3 blocks
// were dumped and compared with the reference collator's own block for the same
// fixture (bench-collate --fixture /root/bench/fx-cpp-parity/mainnet-heavy
// --iterations 1 --dump-block, then a (account, lt) set diff):
//
//	before: ours 747 transactions, reference 753, only-in-reference 6 —
//	        d4331c4b…, d553394b…, d5cb623e…, d67b6882…, d6acf203…, d887d0e2…,
//	        all at lt 76734071000003, all of them absent from our block;
//	after:  ours 754, reference 753, only-in-reference 0, only-in-ours 1 —
//	        d9f83b85… at lt 76734071000003.
//
// So the six are recovered and every transaction the reference produces is now
// produced here too; gas lands on the reference's 557695 exactly. One residual
// remains and is NOT closed: we admit one message the reference does not,
// because at the stopping message our estimate is 337 B below the reference's
// (1,048,376 vs 1,048,713 on a 1,048,576 budget, 0.03%). That last 337 B is the
// difference between a hash-keyed and a per-object boundary predicate and cannot
// be removed without replacing the update builder's hash-keyed pruning — see
// TestMainnetHeavyStopsWhereTheReferenceStops for the term-by-term account.
//
// The repeat=1 arms did not move: they never reach a limit, and the oracle
// confirms 345/345 transactions with an empty set difference in both
// directions, on both predecessor shapes.
//
// The BYTES here remain self-validated. The reference's own block differs from
// ours in gen_software, rand_seed and Merkle-update pruning shape, so no
// reference run can certify a byte count; what the oracle certifies is the
// (account, lt) transaction set around them.
type payloadHintArm struct {
	name        string
	repeat      int
	lazy        bool
	blockBytes  int
	blockSum    string
	collatedLen int
	collatedSum string
}

func payloadHintArms() []payloadHintArm {
	return []payloadHintArm{
		{
			"resident repeat=1", 1, false,
			424977, "726bd7970581d9916c1405a1a337b5c877e1fc0ba4adbe1dfe874f6e2ae6ea41",
			334013, "4159ffc1d457459ad33d905eb5f6819f12c8da558cad80b101c3a95567ae5a54",
		},
		{
			"store-shaped repeat=1", 1, true,
			446619, "cc094895cee364e387a9c484527ce8383d58cc8e5349622856e871ba36a17d5c",
			332586, "24ff26a89be45b2cf325ae9a055a90f92e257e9568c341cac80d9dca973ded80",
		},
		{
			"resident repeat=3", 3, false,
			667523, "4f7aa861096081ef65665646f87b6fb20dad21b4ccf22454d40a85e9ff4e91ed",
			421534, "fd27f9facd71b180db7c0691b856f9d755955a484be8bf8455226f56b855cd30",
		},
		{
			"store-shaped repeat=3", 3, true,
			689165, "d6b960ce67e0496e308010d8a865ce64ac304a946a4c5a8a8ed39c574a52aebe",
			420107, "7470db5948a4d75d6edd124bb541803359a12504277df7b569492389ec58a5cc",
		},
	}
}

func payloadHintCandidate(tb testing.TB, arm payloadHintArm) *Candidate {
	tb.Helper()

	// A fresh request per arm. benchMainnetCollatedRequest hands out a copy of
	// the shared workload and sets capFullCollatedData on that copy, so one arm
	// cannot leave the capability — or a materialized predecessor — behind for
	// the next.
	req := benchMainnetCollatedRequest(tb, arm.repeat)
	if arm.lazy {
		req = lazyParentPredecessor(tb, req)
	}

	return lazyParentCandidate(tb, req)
}

// The union argument on the input that matters, and the byte identity that lets
// the hint be changed later without re-proving the wire.
func TestL3HintOnMainnetFixture(t *testing.T) {
	for _, arm := range payloadHintArms() {
		t.Run(arm.name, func(t *testing.T) {
			candidate := payloadHintCandidate(t, arm)

			blockSum := sha256.Sum256(candidate.BlockBOC)
			collatedSum := sha256.Sum256(candidate.CollatedData)
			if len(candidate.BlockBOC) != arm.blockBytes ||
				hex.EncodeToString(blockSum[:]) != arm.blockSum {
				t.Fatalf("%s: block is %d bytes %s, want the recorded %d bytes %s",
					arm.name, len(candidate.BlockBOC), hex.EncodeToString(blockSum[:]),
					arm.blockBytes, arm.blockSum)
			}
			if len(candidate.CollatedData) != arm.collatedLen ||
				hex.EncodeToString(collatedSum[:]) != arm.collatedSum {
				t.Fatalf("%s: collated data is %d bytes %s, want the recorded %d bytes %s",
					arm.name, len(candidate.CollatedData), hex.EncodeToString(collatedSum[:]),
					arm.collatedLen, arm.collatedSum)
			}

			blockRoot, err := cell.FromBOC(candidate.BlockBOC)
			if err != nil {
				t.Fatal(err)
			}
			collatedRoots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
			if err != nil {
				t.Fatal(err)
			}
			roots := make([]*cell.Cell, 1, len(collatedRoots)+1)
			roots[0] = blockRoot
			roots = append(roots, collatedRoots...)

			hint := simplex.PayloadCellHint(candidate.BlockBOC, candidate.CollatedData)
			want, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
			if err != nil {
				t.Fatal(err)
			}
			actual := payloadHintDeclaredCells(t, want)
			t.Logf("%s: combined %d B over %d cells, hint %d (+%.2f%%)",
				arm.name, len(want), actual, hint,
				100*float64(hint-actual)/float64(actual))

			if hint < actual {
				t.Fatalf("%s: hint %d is below the %d cells the broadcast payload holds",
					arm.name, hint, actual)
			}
			// A hint that is not honoured is not a hint. The fixture must land
			// between the floor and the ceiling, or this arm proves nothing
			// about the sizing it is here to check.
			if hint == 0 {
				t.Fatalf("%s: the hint fell back to the default sizing", arm.name)
			}
			for _, candidateHint := range []int{0, hint, actual, 10 * hint} {
				got, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{
					WithCRC32C:     true,
					CellsCountHint: candidateHint,
				})
				if err != nil {
					t.Fatalf("%s: hint %d: %v", arm.name, candidateHint, err)
				}
				if !bytes.Equal(want, got) {
					t.Fatalf("%s: hint %d changed the broadcast payload BOC", arm.name, candidateHint)
				}
			}
		})
	}
}

// payloadHintDeclaredCells reads the cells counter out of a BoC header the same
// way the hint does, so the assertion above compares like with like.
func payloadHintDeclaredCells(tb testing.TB, boc []byte) int {
	tb.Helper()

	if len(boc) < 6 {
		tb.Fatalf("boc is %d bytes", len(boc))
	}
	size := int(boc[4] & 0b111)
	if size < 1 || size > 4 || len(boc) < 6+size {
		tb.Fatalf("boc declares a %d-byte cells counter", size)
	}
	cells := 0
	for _, b := range boc[6 : 6+size] {
		cells = cells<<8 | int(b)
	}

	return cells
}
