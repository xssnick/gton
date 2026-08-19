package collator

import (
	"context"
	"testing"
)

// Where the block-limit estimator stops admitting inbound internals on the
// mainnet-heavy fixture, pinned against the reference implementation's own
// stopping point.
//
// THE DIVERGENCE THIS EXISTS FOR. The reference collator and this one were run
// on byte-identical inputs (/root/bench/fx-cpp-parity/mainnet-heavy, which is
// benchMainnetCollatedRequest at benchMainnetHeavyRepeat) with both admission
// loops instrumented per queue entry. Both gates sit at the top of the loop and
// are evaluated before the message is imported — here processInternals'
// !fits(LoadNormal), there block_full_ = !fits(cl_normal) — and all 736 shared
// entries carried byte-identical (lt, key), so the merge order was never in
// question. What differed was the estimate:
//
//	                ours        reference   delta
//	bits/8          415,112     415,390      -278
//	cells*12        199,692     199,836      -144
//	internal*3       82,947      83,433      -486
//	external*40     171,440     165,800    +5,640   <- the whole divergence
//	transactions*200 147,000    147,000         0
//	extra_out*300     30,900     30,900         0
//	total         1,049,091   1,044,359    +4,732   (budget 1,048,576)
//
// Every other term was identical at every one of the 736 indices, including gas
// and the lt delta, and add_proof(state_root) — the suspect from the earlier
// estimator-parity audit — was charged at the same point on both sides and to
// within 6 reclassified edges of the same size. The external-reference term was
// the only one that moved, and it moved because the boundary predicate here was
// the read set, which GROWS during a collation and contains cells the transition
// rebuilt and read back, while the reference's is per-object usage-tree
// provenance, fixed when the cell is created. 113 hashes over 138 edges, none of
// them held by the source tree, were charged 40 bytes each after this same walk
// had already serialized them in full. See the ordering rule and its unit test
// in tonutils-go tvm/cell/cell_storage_stat.go.
//
// The consequence was six inbound internal messages the reference admitted and
// this collator did not, on accounts d4331c4b…, d553394b…, d5cb623e…,
// d67b6882…, d6acf203… and d887d0e2…, all at lt 76734071000003.
//
// WHAT IS CLOSED AND WHAT IS NOT. The reference's block was dumped and diffed
// against ours by (account, lt): all 753 of its transactions are now produced
// here, gas lands on its 557,695 exactly, and the per-site account-dictionary
// proof charge is identical to the byte (46 calls, 2,404 cells, 256,450 bits,
// 2,725 internal and 1,899 external refs on both sides). One residual is NOT
// closed and is pinned below rather than hidden: at the message the reference
// stops on, our estimate is 337 B BELOW its 1,048,713 — 0.03% of the budget —
// so we admit one message more than it does (d9f83b85… at lt 76734071000003).
// The residual is split between the out-queue root proof (-405 B) and the
// per-transaction account proof (+29 B), and it is the irreducible part of a
// hash-keyed predicate standing in for a per-object one: the reference can
// serialize a body and charge a boundary for the same hash in one walk, because
// two different cell objects carry it, and no hash-keyed set can reproduce that.
// Closing it would mean replacing the update builder's hash-keyed pruning, which
// the ReadSet design deliberately rejected.
//
// So these numbers are pinned to catch movement, not to claim identity. If they
// change, re-run the oracle before re-pinning them:
//
//	bench-collate --fixture /root/bench/fx-cpp-parity/mainnet-heavy \
//	    --iterations 1 --dump-block ref.boc
//	# reference: 753 transactions, 557695 gas
func TestMainnetHeavyStopsWhereTheReferenceStops(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("heavy mainnet collation is slow")
	}

	// The reference's own figures on the same fixture, measured, not assumed.
	const (
		referenceImported     = 741
		referenceTransactions = 753
		referenceGas          = 557695
	)

	req := benchMainnetCollatedRequest(t, benchMainnetHeavyRepeat)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate mainnet-heavy: %v", err)
	}
	stats := candidate.Stats

	// The arm has to actually reach the limit, or it proves nothing about where
	// admission stops.
	if stats.OverloadReason != OverloadBlockLimit {
		t.Fatalf("overload reason is %v, want %v — this arm no longer stops on a block limit",
			stats.OverloadReason, OverloadBlockLimit)
	}
	if stats.EnqueuedMessages == 0 {
		t.Fatal("nothing was enqueued, so the block did not fill")
	}

	if stats.GasUsed != referenceGas {
		t.Fatalf("gas is %d, want the reference's %d", stats.GasUsed, referenceGas)
	}
	if stats.InternalsImported != referenceImported+1 {
		t.Fatalf("imported %d internals, want %d — the reference imports %d and the "+
			"known residual is exactly one message",
			stats.InternalsImported, referenceImported+1, referenceImported)
	}
	if stats.Transactions != referenceTransactions+1 {
		t.Fatalf("produced %d transactions, want %d — the reference produces %d and the "+
			"known residual is exactly one transaction",
			stats.Transactions, referenceTransactions+1, referenceTransactions)
	}
}
