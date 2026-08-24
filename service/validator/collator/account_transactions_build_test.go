package collator

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// multiTransactionAccountBlock builds a block in which one account executes
// several times — four externals to the same destination — and returns that
// account's AccountBlock. One transaction per account is the shape where every
// way of building the dictionary agrees trivially, so it proves nothing about
// how the dictionary is built.
func multiTransactionAccountBlock(t *testing.T, transactions int) tlb.AccountBlock {
	t.Helper()

	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(
		t,
		req.Header.GenUtime,
		activeContract{address: addr, code: externalAcceptCode(t), balance: 100_000_000_000},
	))
	for i := range transactions {
		// Distinct bodies: the pool keys externals by message hash, so identical
		// ones would collapse into a single admission.
		message, err := tlb.ToCell(&tlb.ExternalMessage{
			DstAddr: addr,
			Body:    cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell(),
		})
		if err != nil {
			t.Fatal(err)
		}
		req.Externals = append(req.Externals, externalInput(t, message))
	}

	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	var blocks tlb.ShardAccountBlocks
	if err = parseExact(&blocks, block.Extra.ShardAccountBlocks); err != nil {
		t.Fatal(err)
	}
	var key [32]byte
	copy(key[:], addr.Data())
	value, err := blocks.Accounts.LoadValue(accountBlockKeyCell(key))
	if err != nil {
		t.Fatalf("candidate has no account block for %s: %v", addr, err)
	}
	var accountBlock tlb.AccountBlock
	if err = loadExactSlice(&accountBlock, value); err != nil {
		t.Fatal(err)
	}
	return accountBlock
}

// accountBlockTransactionPairs reads back the (lt, transaction) pairs of an
// account block in dictionary order.
func accountBlockTransactionPairs(t *testing.T, block tlb.AccountBlock) []accountTransaction {
	t.Helper()

	iterator, err := block.Transactions.IteratorExtra(false, false)
	if err != nil {
		t.Fatal(err)
	}
	var pairs []accountTransaction
	for iterator.Next() {
		item := iterator.View()
		lt, keyErr := item.Key.LoadUInt(64)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		root, valueErr := item.Value.LoadRefCell()
		if valueErr != nil {
			t.Fatal(valueErr)
		}
		pairs = append(pairs, accountTransaction{startLT: lt, root: root})
	}
	if err = iterator.Err(); err != nil {
		t.Fatal(err)
	}
	return pairs
}

// The whole safety argument of building the dictionary once is that SetMany
// produces the dictionary repeated Set would have produced — same cells, same
// augmentations. That claim is documented for the bulk writer in general
// (tonutils tvm/cell/aug_dict_bulk.go), and this pins it for the augmentation
// the AccountBlock actually carries: aug_AccountTransactions, whose fork rule
// adds two CurrencyCollections and whose leaf rule digs total_fees out of the
// transaction. The reference builds this dictionary with exactly the per-key
// Add sequence rebuilt here — Account::create_account_block,
// crypto/block/transaction.cpp:4176.
//
// The candidate's own dictionary is the left-hand side, so a divergence shows
// up as a different account block in a real block rather than as a difference
// between two test-built tries.
//
// The order the lane hands its transactions over in is not a variable this test
// can move, and rebuilding here from a reversed slice would assert nothing:
// SetMany sorts the batch itself, and that is pinned where it belongs, on a
// shuffled batch against repeated Set (tonutils tvm/cell/aug_dict_bulk_test.go).
// What the build's own sort buys is pinned by
// TestBuildAccountTransactionsRejectsDuplicateLogicalTime.
func TestMultiTransactionAccountBlockMatchesPerTransactionInserts(t *testing.T) {
	const want = 4
	block := multiTransactionAccountBlock(t, want)
	pairs := accountBlockTransactionPairs(t, block)
	if len(pairs) != want {
		t.Fatalf("account executed %d times in the block, want %d", len(pairs), want)
	}
	for i := 1; i < len(pairs); i++ {
		if pairs[i].startLT <= pairs[i-1].startLT {
			t.Fatalf("transaction %d lt %d does not exceed %d", i, pairs[i].startLT, pairs[i-1].startLT)
		}
	}

	sequential, err := tlb.NewAccountTransactionsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range pairs {
		inserted, setErr := sequential.SetWithMode(
			cell.BeginCell().MustStoreUInt(pair.startLT, 64).EndCell(),
			cell.BeginCell().MustStoreRef(pair.root).EndCell(),
			cell.DictSetModeAdd,
		)
		if setErr != nil || !inserted {
			t.Fatalf("insert transaction %d: %v", pair.startLT, setErr)
		}
	}
	if sequential.RootCell().HashKey() != block.Transactions.RootCell().HashKey() {
		t.Fatalf("account block dictionary %x differs from the one repeated inserts produce %x",
			block.Transactions.RootCell().Hash(), sequential.RootCell().Hash())
	}
}

// Two transactions of one account may never share a logical time: the key is
// the lt, so the second would silently replace the first and the block would
// carry fewer transactions than it executed. The insert used to refuse it one
// transaction at a time; the check moved to the build with the dictionary, and
// the reference refuses it in the same place (create_account_block logs
// "cannot add transaction with lt=" and fails the collation).
//
// This is also the one thing the sort in buildAccountTransactions buys, so both
// arrival orders are exercised: the check is adjacency, and only sorting makes
// it see a pair the account did not execute back to back. The refusal itself
// does not depend on the sort — the writer refuses a repeated key on its own —
// but it then arrives as "duplicate key in bulk update", which names no lt and
// no account, and a collation that stops has to say which transaction stopped
// it.
func TestBuildAccountTransactionsRejectsDuplicateLogicalTime(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	for _, arrival := range []struct {
		name string
		lts  []uint64
	}{
		{name: "adjacent", lts: []uint64{41, 41, 43}},
		{name: "separated", lts: []uint64{41, 43, 41}},
	} {
		t.Run(arrival.name, func(t *testing.T) {
			lane := &accountLane{}
			for _, lt := range arrival.lts {
				lane.transactions = append(lane.transactions, accountTransaction{startLT: lt, root: root})
			}

			_, err := buildAccountTransactions(lane)
			if err == nil {
				t.Fatal("a repeated transaction lt was accepted into an account block")
			}
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "duplicate transaction lt 41") {
				t.Fatalf("duplicate lt reported as %v", err)
			}
		})
	}
}

// The dictionary is built once per account, at finalize, and not touched while
// the account executes. An aug-dict Set walks a full root-to-leaf path and
// recombines the augmentation of every node on the way back up, so an insert
// per transaction rebuilds the nodes a long account chain shares once per
// transaction — 1.36 ms per block on the mainnet fixture, and production
// accounts run far longer chains than that one.
//
// Nothing about the produced block would change if an insert crept back in, so
// the shape is pinned here rather than left to a benchmark nobody reruns.
func TestCommitExecutionDefersTheAccountTransactionsDictionary(t *testing.T) {
	if constructed := methodCallOrder(t, "execute.go", "commitExecution", "tlb"); len(constructed) != 0 {
		t.Fatalf("commitExecution builds dictionary state per transaction: %v", constructed)
	}
	// finishAccounts reaches the per-lane build through buildAccountLanes, which
	// is what spreads it over workers; both links are pinned so neither the
	// dictionary nor the fan-out can quietly move back into execution.
	if reached := functionCallOrder(t, "execute.go", "finishAccounts"); !slices.Contains(reached, "buildAccountLanes") {
		t.Fatalf("finishAccounts no longer builds the account lanes: %v", reached)
	}
	if built := functionCallOrder(t, "execute.go", "buildAccountLane"); !slices.Contains(built, "buildAccountTransactions") {
		t.Fatalf("the account lane build no longer builds the transaction dictionary: %v", built)
	}
}
