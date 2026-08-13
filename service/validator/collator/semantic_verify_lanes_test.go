package collator

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// mergeableLane is the shape replayAccount hands to the merge: an account that
// never existed, so applying it to the replayed dictionary is a no-op and the
// test can drive the merge with nothing but the cross-account accumulators.
func mergeableLane(t *testing.T, key byte) semanticAccountLane {
	t.Helper()

	shardAccount := &tlb.ShardAccount{
		Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
		LastTransHash: make([]byte, 32),
	}
	var id [32]byte
	id[0] = key
	prepared, err := tvm.PrepareAccount(shardAccount, address.NewAddress(0, 0, id[:]))
	if err != nil {
		t.Fatal(err)
	}

	return semanticAccountLane{
		key: id,
		account: accountLane{
			key:      id,
			original: shardAccount,
			current:  prepared,
		},
	}
}

// The lanes replay concurrently, so the merge is what makes the rejection
// reason independent of scheduling: it walks lanes in ascending account-key
// order, which is the only order the sequential replay ever had.
func TestSemanticAccountLaneMergeRanksFailuresByAccount(t *testing.T) {
	replay, _ := semanticOrdinaryReplay(t)
	first := errors.New("first account")
	second := errors.New("second account")

	err := replay.mergeAccountLanes([]semanticAccountLane{
		{key: [32]byte{0x01}, err: first},
		{key: [32]byte{0x02}, err: second},
	})
	if !errors.Is(err, first) {
		t.Fatalf("merge error = %v, want the lowest-keyed lane failure", err)
	}
}

// A message claimed by two accounts cannot be detected inside a lane, so the
// merge has to catch it — and has to report it identically to the sequential
// replay, which saw the lower-keyed account first.
func TestSemanticAccountLaneMergeCatchesCrossAccountDuplicates(t *testing.T) {
	hash := cell.Hash{0x77}

	t.Run("inbound", func(t *testing.T) {
		replay, _ := semanticOrdinaryReplay(t)
		first, second := mergeableLane(t, 0x01), mergeableLane(t, 0x02)
		first.consumedIn = []cell.Hash{hash}
		second.consumedIn = []cell.Hash{hash}
		err := replay.mergeAccountLanes([]semanticAccountLane{first, second})
		if !errors.Is(err, ErrInvalidInput) ||
			!strings.Contains(err.Error(), "processed by more than one transaction") {
			t.Fatalf("duplicate inbound error = %v", err)
		}
	})

	t.Run("outbound", func(t *testing.T) {
		replay, _ := semanticOrdinaryReplay(t)
		first, second := mergeableLane(t, 0x01), mergeableLane(t, 0x02)
		first.consumedOut = []cell.Hash{hash}
		second.consumedOut = []cell.Hash{hash}
		err := replay.mergeAccountLanes([]semanticAccountLane{first, second})
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "emitted more than once") {
			t.Fatalf("duplicate outbound error = %v", err)
		}
	})
}

// Gas is summed per lane, so the block-wide allowance is only visible once the
// lanes are combined.
func TestSemanticAccountLaneMergeAggregatesGasAcrossLanes(t *testing.T) {
	replay, _ := semanticOrdinaryReplay(t)
	replay.normalGasLimit = 10

	first, second := mergeableLane(t, 0x01), mergeableLane(t, 0x02)
	first.normalGas, second.normalGas = 6, 6
	err := replay.mergeAccountLanes([]semanticAccountLane{first, second})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "aggregate gas") {
		t.Fatalf("aggregate gas error = %v", err)
	}
	if replay.normalGasUsed != 12 {
		t.Fatalf("aggregate gas = %d, want 12", replay.normalGasUsed)
	}
}

// A cancelled replay must never be reported as an invalid candidate: the
// runtime retries a deadline but treats ErrInvalidInput as a consensus reject.
func TestSemanticAccountLaneMergeReportsCancellationOverLaneFailure(t *testing.T) {
	replay, _ := semanticOrdinaryReplay(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	replay.ctx = ctx

	err := replay.mergeAccountLanes([]semanticAccountLane{
		{key: [32]byte{0x01}, err: errors.New("lane rejected")},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("merge error = %v, want context.Canceled", err)
	}
}

// semanticMultiAccountReplay builds a candidate whose account-block dictionary
// has one block per account, which is what puts more than one lane in flight.
func semanticMultiAccountReplay(tb testing.TB, accounts int) *semanticReplay {
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

	replay := semanticPreparedShardReplay(tb, req)
	lanes, err := replay.decodeAccountLanes()
	if err != nil {
		tb.Fatal(err)
	}
	if len(lanes) != accounts {
		tb.Fatalf("candidate has %d account blocks, want %d", len(lanes), accounts)
	}

	return replay
}

// The concurrent path only runs once there is more than one lane, so the
// end-to-end accept has to be exercised on a genuinely multi-account candidate
// rather than on the single-lane fixture the other tests share.
func TestSemanticVerifierAcceptsConcurrentlyReplayedAccounts(t *testing.T) {
	replay := semanticMultiAccountReplay(t, 8)
	if err := replay.verifyAccounts(); err != nil {
		t.Fatalf("verify concurrently replayed accounts: %v", err)
	}
}

// Lanes fail while other lanes are still running, so the verdict must come from
// the lowest-keyed failure and not from whichever worker finished first.
// Repetition is what makes the assertion meaningful: a scheduling-dependent
// ranking would only show up on some runs.
func TestSemanticVerifierRanksConcurrentLaneFailuresDeterministically(t *testing.T) {
	first := errors.New("lower account")
	second := errors.New("higher account")

	for run := range 32 {
		replay := semanticMultiAccountReplay(t, 8)
		lanes, err := replay.decodeAccountLanes()
		if err != nil {
			t.Fatal(err)
		}
		lanes[2].err = first
		lanes[5].err = second

		replay.replayAccountLanes(lanes)
		if err = replay.mergeAccountLanes(lanes); !errors.Is(err, first) {
			t.Fatalf("run %d: merge error = %v, want the lowest-keyed lane failure", run, err)
		}
	}
}

// A candidate is either valid or it is not, whatever the machine it lands on:
// the number of workers must not change the verdict, the account root, or the
// message a rejection reports.
func TestSemanticVerifierVerdictIsIndependentOfWorkerCount(t *testing.T) {
	original := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(original) })

	type outcome struct {
		root cell.Hash
		fail string
	}
	results := make([]outcome, 0, 2)
	for _, procs := range []int{1, 8} {
		runtime.GOMAXPROCS(procs)

		replay := semanticMultiAccountReplay(t, 8)
		if err := replay.verifyAccounts(); err != nil {
			t.Fatalf("GOMAXPROCS=%d: verify valid candidate: %v", procs, err)
		}

		rejected := semanticMultiAccountReplay(t, 8)
		lanes, err := rejected.decodeAccountLanes()
		if err != nil {
			t.Fatal(err)
		}
		lanes[3].err = errors.New("rejected account")
		lanes[6].err = errors.New("later rejected account")
		rejected.replayAccountLanes(lanes)
		err = rejected.mergeAccountLanes(lanes)
		if err == nil {
			t.Fatalf("GOMAXPROCS=%d: a rejected lane was accepted", procs)
		}

		results = append(results, outcome{
			root: replay.replayedAccounts.RootCell().HashKey(),
			fail: err.Error(),
		})
	}
	if results[0] != results[1] {
		t.Fatalf("worker count changed the outcome: %+v vs %+v", results[0], results[1])
	}
}

// The replay no longer walks the candidate's account dictionary; it rebuilds
// the dictionary from the predecessor and compares roots. That comparison is
// the only thing standing between a tampered dictionary and acceptance, so it
// has to reject every shape of tampering on its own.
func TestSemanticVerifierRejectsTamperedAccountDictionary(t *testing.T) {
	tamper := map[string]func(*testing.T, *tlb.ShardAccountsAugDict){
		"entry added": func(t *testing.T, accounts *tlb.ShardAccountsAugDict) {
			extra := address.NewAddress(0, 0, bytes.Repeat([]byte{0xfe}, 32))
			key := cell.BeginCell().MustStoreSlice(extra.Data(), 256).EndCell()
			shardAccount := activeShardAccount(t, activeContract{
				address: extra,
				code:    externalAcceptCode(t),
				balance: 1_000_000_000,
			}, 0)
			if err := accounts.Set(key, shardAccount); err != nil {
				t.Fatal(err)
			}
		},
		"entry removed": func(t *testing.T, accounts *tlb.ShardAccountsAugDict) {
			key := cell.BeginCell().
				MustStoreSlice(bytes.Repeat([]byte{0x01}, 32), 256).
				EndCell()
			if err := accounts.Delete(key); err != nil {
				t.Fatal(err)
			}
		},
	}

	for name, apply := range tamper {
		t.Run(name, func(t *testing.T) {
			replay := semanticMultiAccountReplay(t, 4)
			apply(t, replay.candidate.state.Accounts.ShardAccounts)

			err := replay.verifyAccounts()
			if !errors.Is(err, ErrInvalidInput) ||
				!strings.Contains(err.Error(), "replayed account dictionary differs") {
				t.Fatalf("tampered dictionary error = %v", err)
			}
		})
	}
}

// The concurrent and the single-worker paths are the same code, so a candidate
// must validate identically however many times it is replayed.
func TestSemanticVerifierReplayIsStableAcrossRuns(t *testing.T) {
	replay, _ := semanticOrdinaryReplay(t)
	if err := replay.verifyAccounts(); err != nil {
		t.Fatalf("first replay: %v", err)
	}
	want := replay.replayedAccounts.RootCell().HashKey()

	for i := 0; i < 8; i++ {
		fresh, _ := semanticOrdinaryReplay(t)
		if err := fresh.verifyAccounts(); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		if got := fresh.replayedAccounts.RootCell().HashKey(); got != want {
			t.Fatalf("replay %d produced account root %x, want %x", i, got, want)
		}
	}
}
