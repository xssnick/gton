package collator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestTraceAccountValidationClosureRetainsStructuralDiff(t *testing.T) {
	oldAccounts := testAccountDiffDictionary(t, map[byte]uint64{
		0x00: 10,
		0x80: 20,
		0xc0: 30,
	})
	oldRoot, err := oldAccounts.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	usage := cell.NewReadSet(oldRoot)
	oldLoader := usage.Root().MustBeginParse()
	var tracedOld tlb.ShardAccountsAugDict
	if err = tracedOld.LoadFromCell(oldLoader); err != nil {
		t.Fatal(err)
	}
	newAccounts := &tlb.ShardAccountsAugDict{AugmentedDictionary: tracedOld.Copy()}
	key := testAccountDiffKey(0x00)
	if err = newAccounts.Set(key, testAccountDiffValue(t, 11)); err != nil {
		t.Fatal(err)
	}
	newRoot, err := newAccounts.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	oldState := tlb.ShardStateUnsplit{}
	oldState.Accounts.ShardAccounts = &tracedOld
	c := collation{usage: usage, oldState: oldState, accounts: newAccounts}
	if err = c.traceAccountValidationClosure(); err != nil {
		t.Fatal(err)
	}

	update, _, err := usage.CreateMerkleUpdateApplied(newRoot)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := usage.Proof()
	if err != nil {
		t.Fatal(err)
	}
	provenOldRoot, err := cell.UnwrapProofVirtualized(proof, oldRoot.Hash())
	if err != nil {
		t.Fatal(err)
	}
	provenNewRoot, err := cell.ApplyMerkleUpdate(provenOldRoot, update)
	if err != nil {
		t.Fatal(err)
	}

	provenOld := loadAccountDiffDictionary(t, provenOldRoot)
	provenNew := loadAccountDiffDictionary(t, provenNewRoot)
	callbacks := 0
	if err = provenOld.ScanDiff(provenNew.AugmentedDictionary, true, func(
		*cell.Cell,
		*cell.Slice,
		*cell.Slice,
	) error {
		callbacks++
		return nil
	}); err != nil {
		t.Fatalf("scan structural account diff through collated proof: %v", err)
	}
	if callbacks != 1 {
		t.Fatalf("changed accounts = %d, want 1", callbacks)
	}
}

func TestSemanticPrecheckRejectsIncompleteAccountDiff(t *testing.T) {
	oldAccounts := testAccountDiffDictionary(t, map[byte]uint64{
		0x00: 10,
		0x80: 20,
		0xc0: 30,
	})
	newAccounts := testAccountDiffDictionary(t, map[byte]uint64{
		0x00: 11,
		0x80: 20,
		0xc0: 30,
	})

	usage := cell.NewReadSet(newAccounts.RootCell())
	tracedNew := newAccounts.Copy().SetTrace(usage.Trace())
	if _, err := tracedNew.LoadValue(testAccountDiffKey(0x00)); err != nil {
		t.Fatal(err)
	}
	proof, err := usage.Proof()
	if err != nil {
		t.Fatal(err)
	}
	partialRoot, err := cell.UnwrapProofVirtualized(proof, newAccounts.RootCell().Hash())
	if err != nil {
		t.Fatal(err)
	}
	partialNew := &tlb.ShardAccountsAugDict{
		AugmentedDictionary: partialRoot.AsAugDict(256, tlb.AugShardAccounts{}),
	}
	accountBlocks, err := tlb.NewShardAccountBlocksAugDict()
	if err != nil {
		t.Fatal(err)
	}
	previous := &tlb.ShardStateUnsplit{}
	previous.Accounts.ShardAccounts = oldAccounts
	candidateState := tlb.ShardStateUnsplit{}
	candidateState.Accounts.ShardAccounts = partialNew
	replay := semanticReplay{
		ctx:      context.Background(),
		previous: previous,
		candidate: &verifiedCandidate{
			state:         candidateState,
			accountBlocks: accountBlocks,
		},
		accountBlocks: accountBlocks,
	}

	err = replay.precheckAccountUpdates()
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "ShardAccounts dictionary difference") {
		t.Fatalf("incomplete account diff error = %v", err)
	}
}

func testAccountDiffDictionary(t testing.TB, entries map[byte]uint64) *tlb.ShardAccountsAugDict {
	t.Helper()
	dict, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for key, lt := range entries {
		if err = dict.Set(testAccountDiffKey(key), testAccountDiffValue(t, lt)); err != nil {
			t.Fatal(err)
		}
	}
	return dict
}

func testAccountDiffKey(prefix byte) *cell.Cell {
	var key [32]byte
	key[0] = prefix
	return cell.BeginCell().MustStoreSlice(key[:], 256).EndCell()
}

func testAccountDiffValue(t testing.TB, lt uint64) *cell.Cell {
	t.Helper()
	value, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
		LastTransHash: make([]byte, 32),
		LastTransLT:   lt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func loadAccountDiffDictionary(t testing.TB, root *cell.Cell) *tlb.ShardAccountsAugDict {
	t.Helper()
	loader, err := root.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	var dict tlb.ShardAccountsAugDict
	if err = dict.LoadFromCellAsProof(loader); err != nil {
		t.Fatal(err)
	}
	return &dict
}
