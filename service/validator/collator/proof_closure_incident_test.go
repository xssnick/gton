package collator

import (
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestIncidentCachedSharedStorageProofClosure(t *testing.T) {
	ballast := sharedStorageDictBallast(t, 80)
	code := sharedStorageDictContract(t, ballast[len(ballast)-1])
	first := address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32))
	second := address.NewAddress(0, 0, bytes.Repeat([]byte{0x72}, 32))
	req := emptyCandidateRequest(t)
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: first, code: code, balance: 100_000_000_000},
		activeContract{address: second, code: code, balance: 100_000_000_000},
	))
	req.Externals = []ExternalInput{
		sharedStorageDictExternal(t, first, nil),
		sharedStorageDictExternal(t, second, nil),
	}

	for round := 0; round < 12; round++ {
		candidate, err := testBuilder().BuildShard(t.Context(), req)
		if err != nil {
			t.Fatalf("build round %d: %v", round, err)
		}
		if candidate.Stats.Transactions != 2 {
			t.Fatalf("round %d transactions = %d", round, candidate.Stats.Transactions)
		}
		if round > 0 {
			verification := shardVerificationRequest(req, candidate)
			verification.Neighbors = collatedNeighborQueues(t, req, candidate)
			verification.NeighborShardEndLT = req.NeighborShardEndLT
			semantics := NewSemanticVerifier(tvm.NewTVM())
			var recomputed atomic.Int64
			semantics.SetStorageStatRecomputeObserver(func(MetricChain, [32]byte) {
				recomputed.Add(1)
			})
			verification.Semantics = semantics
			if err = verifyShardCandidateForTest(t.Context(), verification); err != nil {
				t.Fatalf("verify round %d: %v", round, err)
			}
			if got := recomputed.Load(); got != 0 {
				t.Fatalf("round %d recomputed %d storage stats", round, got)
			}
		}
		blockRoot, err := cell.FromBOC(candidate.BlockBOC)
		if err != nil {
			t.Fatal(err)
		}
		queueSize := candidate.Stats.OutQueueSize
		req.Previous = PreviousBlock{
			ID: candidate.ID, Block: blockRoot, State: candidate.State, OutQueueSize: &queueSize,
		}
		req.StorageStats = candidate.StorageStats
		if len(req.StorageStats) == 0 {
			t.Fatalf("round %d produced no storage stats", round)
		}
		req.Header.GenUtime++
		req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1_000
		req.Masterchain.Config.capabilities |= capFullCollatedData
		attachFullCollatedTestNeighbors(t, &req)
		req.Externals = []ExternalInput{
			sharedStorageDictExternal(t, first, ballast[round*3]),
			sharedStorageDictExternal(t, second, ballast[79-round*3]),
		}
	}
}
