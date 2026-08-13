package collator

import (
	"context"
	"errors"
	"testing"
)

// A block that overflows the consensus size limit used to end the slot with no
// block at all: the check runs on the serialized candidate, and the production
// loop would have retried the identical collation forever. Collation now rebuilds
// itself a bounded number of times with a narrower byte budget, so the slot gets
// a smaller block instead of nothing.
func TestBuildShardRebuildsUnderSizeLimit(t *testing.T) {
	req, _ := benchMainnetRequest(t, 0)
	natural, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("baseline collation: %v", err)
	}

	// Just under what this workload naturally produces, so the first attempt
	// overflows and a narrower one has room to fit.
	req.Masterchain.Config.maxBlockBytes = uint32(len(natural.BlockBOC)) - 1

	smaller, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collation did not recover from the size limit: %v", err)
	}
	if len(smaller.BlockBOC) > int(req.Masterchain.Config.maxBlockBytes) {
		t.Fatalf("rebuilt block is %d bytes, over the %d limit",
			len(smaller.BlockBOC), req.Masterchain.Config.maxBlockBytes)
	}
	if smaller.Stats.Transactions == 0 {
		t.Fatal("rebuilt block carries no transactions")
	}
	if smaller.Stats.Transactions > natural.Stats.Transactions {
		t.Fatalf("rebuilt block grew to %d transactions from %d",
			smaller.Stats.Transactions, natural.Stats.Transactions)
	}
	t.Logf("natural %d bytes / %d tx, rebuilt %d bytes / %d tx",
		len(natural.BlockBOC), natural.Stats.Transactions,
		len(smaller.BlockBOC), smaller.Stats.Transactions)
}

// A limit no block can meet — not even an empty one — must end in a reported
// failure rather than in a loop. The rebuild is bounded by construction, and
// this is what proves the bound is reached rather than merely intended.
func TestBuildShardGivesUpOnUnreachableSizeLimit(t *testing.T) {
	req, _ := benchMainnetRequest(t, 0)
	req.Masterchain.Config.maxBlockBytes = 64

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err == nil {
		t.Fatalf("collation produced a %d byte block under a 64 byte limit", len(candidate.BlockBOC))
	}
	if !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("err = %v, want ErrSizeLimit", err)
	}
}

// The test above shaves a single byte off what the workload produces, which the
// first narrowing clears easily. That leaves the arithmetic itself uncovered: a
// ceiling is only useful if it is derived on the scale admission enforces, and a
// ratio measured against some wider estimate loosens every rebuild by the
// difference. Such a bug survives the one-byte case and fails everywhere else,
// so this drives a heavy block into limits that genuinely bind.
//
// It has already earned its keep: adding the finished-state proof to the
// estimate (build.go) silently moved the ratio onto the post-admission scale,
// and every case below stopped recovering while the one-byte case still passed.
func TestBuildShardRebuildsUnderBindingSizeLimits(t *testing.T) {
	req, _ := benchMainnetRequestRepeated(t, benchMainnetFiller, benchMainnetHeavyRepeat)
	natural, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("baseline collation: %v", err)
	}

	for _, shave := range []int{10, 25, 40} {
		limit := len(natural.BlockBOC) * (100 - shave) / 100
		attempt := req
		attempt.Masterchain.Config.maxBlockBytes = uint32(limit)

		smaller, err := testBuilder().BuildShard(context.Background(), attempt)
		if err != nil {
			t.Errorf("limit %d%% below the natural %d bytes: no recovery: %v",
				shave, len(natural.BlockBOC), err)
			continue
		}
		if len(smaller.BlockBOC) > limit {
			t.Errorf("rebuilt block is %d bytes, over the %d limit", len(smaller.BlockBOC), limit)
			continue
		}
		if smaller.Stats.Transactions == 0 {
			t.Errorf("rebuilt block under a %d-byte limit carries no transactions", limit)
			continue
		}
		t.Logf("limit %d bytes (%d%% under natural): rebuilt %d bytes / %d tx",
			limit, shave, len(smaller.BlockBOC), smaller.Stats.Transactions)
	}
}
