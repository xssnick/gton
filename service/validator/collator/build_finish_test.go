package collator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// loadedCandidateRequest carries enough accounts, transactions and collated
// proof material that the two serialization tails of finish() genuinely overlap.
// A near-empty candidate finishes both before either goroutine is scheduled and
// would not exercise the concurrency at all.
func loadedCandidateRequest(tb testing.TB, accounts int) ShardRequest {
	tb.Helper()

	req := emptyCandidateRequest(tb)
	req.Internals = &msgpool.Cut{}
	contracts := make([]activeContract, accounts)
	code := externalAcceptCode(tb)
	for i := range contracts {
		id := bytes.Repeat([]byte{byte(i + 1)}, 32)
		contracts[i] = activeContract{
			address: address.NewAddress(0, 0, id),
			code:    code,
			balance: 100_000_000_000,
		}
	}
	req.Previous.State = stateWithAccounts(
		tb,
		req.Previous.State,
		activeContracts(tb, req.Header.GenUtime, contracts...),
	)
	for _, contract := range contracts {
		message, err := tlb.ToCell(&tlb.ExternalMessage{
			DstAddr: contract.address,
			Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
		})
		if err != nil {
			tb.Fatal(err)
		}
		req.Externals = append(req.Externals, externalInput(tb, message))
	}

	req = advanceCandidateRequest(tb, req)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(tb, &req)

	return req
}

// finish() serializes the block and the collated data concurrently. Both tails
// read the finished cell tree, so the outputs must be byte-identical run after
// run; under -race the repetitions are also what surfaces an unsynchronized
// read. A drifting BlockBOC would change the candidate hash validators sign.
func TestBuildShardFinishIsByteIdenticalAcrossRuns(t *testing.T) {
	req := loadedCandidateRequest(t, 48)

	first, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Both tails must carry real work; a fixture that shrinks back to a couple
	// of cells would pass the comparison without ever overlapping.
	if first.Stats.Transactions != 48 {
		t.Fatalf("fixture produced %d transactions, want 48", first.Stats.Transactions)
	}
	if len(first.BlockBOC) < 8*1024 || len(first.CollatedData) < 4*1024 {
		t.Fatalf("fixture is too small to overlap: block %d bytes, collated %d bytes",
			len(first.BlockBOC), len(first.CollatedData))
	}

	for run := range 8 {
		next, buildErr := testBuilder().BuildShard(context.Background(), req)
		if buildErr != nil {
			t.Fatalf("run %d: %v", run, buildErr)
		}
		if !bytes.Equal(first.BlockBOC, next.BlockBOC) {
			t.Fatalf("run %d produced a different block BOC", run)
		}
		if !bytes.Equal(first.CollatedData, next.CollatedData) {
			t.Fatalf("run %d produced different collated data", run)
		}
		if !bytes.Equal(first.ID.RootHash, next.ID.RootHash) {
			t.Fatalf("run %d produced a different candidate root hash", run)
		}
	}
}

// The collated serialization runs on its own goroutine, so a failure there must
// still reach the caller instead of being swallowed by the block tail
// succeeding. A collated size cap is the reachable way to make exactly that
// tail fail on an otherwise valid candidate.
func TestBuildShardFinishReportsCollatedFailure(t *testing.T) {
	req := loadedCandidateRequest(t, 48)
	req.Masterchain.Config.maxCollatedBytes = 1

	_, err := testBuilder().BuildShard(context.Background(), req)
	if !errors.Is(err, ErrSizeLimit) || !strings.Contains(err.Error(), "collated data is") {
		t.Fatalf("oversized collated data error = %v, want the collated serialization limit", err)
	}
}
