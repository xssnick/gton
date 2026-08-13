package shardstate

import (
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMergeCombinesSplitAccountRoots(t *testing.T) {
	firstKey, firstPart := testAccountsPart(t, 1)
	secondKey, secondPart := testAccountsPart(t, 2)
	header := testStateHeader(t)

	root, err := Merge(header, []*cell.Cell{firstPart, secondPart})
	if err != nil {
		t.Fatalf("merge split state: %v", err)
	}

	var merged tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&merged, root.MustBeginParse()); err != nil {
		t.Fatalf("parse merged state: %v", err)
	}
	accounts := merged.Accounts.ShardAccounts
	if accounts == nil || accounts.Get(firstKey) == nil || accounts.Get(secondKey) == nil {
		t.Fatal("merged state does not contain both accounts")
	}
}

func TestMergeRejectsDuplicateAccounts(t *testing.T) {
	_, part := testAccountsPart(t, 1)
	_, err := Merge(testStateHeader(t), []*cell.Cell{part, part})
	if err == nil || !strings.Contains(err.Error(), "duplicate account") {
		t.Fatalf("merge duplicate error = %v", err)
	}
}

func testAccountsPart(t *testing.T, keyByte byte) (*cell.Cell, *cell.Cell) {
	t.Helper()

	accounts, err := NewAccounts()
	if err != nil {
		t.Fatalf("create accounts: %v", err)
	}
	keyData := make([]byte, 32)
	keyData[31] = keyByte
	key := cell.BeginCell().MustStoreSlice(keyData, 256).EndCell()
	account, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
		LastTransHash: make([]byte, 32),
		LastTransLT:   uint64(keyByte),
	})
	if err != nil {
		t.Fatalf("build account: %v", err)
	}
	if err = accounts.Set(key, account); err != nil {
		t.Fatalf("set account: %v", err)
	}

	wrapped, err := WrapAccountsRoot(accounts.RootCell())
	if err != nil {
		t.Fatalf("wrap accounts root: %v", err)
	}
	return key, wrapped
}

func testStateHeader(t *testing.T) *tlb.ShardStateUnsplit {
	t.Helper()

	accounts, err := NewAccounts()
	if err != nil {
		t.Fatalf("create header accounts: %v", err)
	}
	header := &tlb.ShardStateUnsplit{
		GlobalID:        -239,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           cell.BeginCell().EndCell(),
	}
	header.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}
	return header
}
