package storage

import (
	"encoding/binary"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// testCurrencyAug builds HashmapAug extras shaped like zero CurrencyCollection
// so tlb.AugShardAccountBlocks / AugAccountTransactions skippers consume them.
type testCurrencyAug struct{}

func (testCurrencyAug) SkipExtra(loader *cell.Slice) error {
	return tlb.LoadFromCell(new(tlb.CurrencyCollection), loader)
}

func (testCurrencyAug) EmptyExtra(dst *cell.Builder) error {
	return dst.StoreUInt(0, 5)
}

func (testCurrencyAug) LeafExtra(_ *cell.Slice, dst *cell.Builder) error {
	return dst.StoreUInt(0, 5)
}

func (testCurrencyAug) CombineExtra(_, _ *cell.Slice, dst *cell.Builder) error {
	return dst.StoreUInt(0, 5)
}

// testInlineAugDictRoot unwraps a HashmapAugE cell into the inline root node
// form used inside an AccountBlock.
func testInlineAugDictRoot(t testing.TB, dict *cell.AugmentedDictionary) *cell.Builder {
	t.Helper()

	wrapped, err := dict.ToCell()
	if err != nil {
		t.Fatalf("serialize augmented dict: %v", err)
	}
	loader := wrapped.MustBeginParse()
	hasRoot, err := loader.LoadBoolBit()
	if err != nil || !hasRoot {
		t.Fatalf("augmented dict wrapper has no root: has=%v err=%v", hasRoot, err)
	}
	root, err := loader.LoadRefCell()
	if err != nil {
		t.Fatalf("load augmented dict root: %v", err)
	}
	return root.ToBuilder()
}

// testShardAccountBlocksCell builds the ShardAccountBlocks cell of a block with
// the given per-account transaction counts.
func testShardAccountBlocksCell(t testing.TB, accounts int, txsPerAccount func(i int) int) (*cell.Cell, uint32) {
	t.Helper()

	accountBlocks, err := cell.NewAugDict(256, testCurrencyAug{})
	if err != nil {
		t.Fatalf("create account blocks dict: %v", err)
	}

	total := uint32(0)
	for a := 0; a < accounts; a++ {
		account := make([]byte, 32)
		binary.BigEndian.PutUint32(account[:4], uint32(a)*2654435761)
		binary.BigEndian.PutUint32(account[28:], uint32(a))

		txDict, err := cell.NewAugDict(64, testCurrencyAug{})
		if err != nil {
			t.Fatalf("create tx dict: %v", err)
		}
		for i := 0; i < txsPerAccount(a); i++ {
			lt := uint64(1000 + a*10 + i)
			tx := cell.BeginCell().MustStoreUInt(uint64(a)<<32|uint64(i), 64).EndCell()
			if err = txDict.Set(cell.BeginCell().MustStoreUInt(lt, 64).EndCell(), cell.BeginCell().MustStoreRef(tx).EndCell()); err != nil {
				t.Fatalf("set tx: %v", err)
			}
			total++
		}

		accountBlock := cell.BeginCell().
			MustStoreUInt(0x5, 4).
			MustStoreSlice(account, 256).
			MustStoreBuilder(testInlineAugDictRoot(t, txDict)).
			MustStoreRef(cell.BeginCell().EndCell()).
			EndCell()
		key := cell.BeginCell().MustStoreSlice(account, 256).EndCell()
		if err = accountBlocks.Set(key, accountBlock); err != nil {
			t.Fatalf("set account block: %v", err)
		}
	}

	shardBlocks, err := tlb.ToCell(&tlb.ShardAccountBlocks{
		Accounts: &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: accountBlocks},
	})
	if err != nil {
		t.Fatalf("build shard account blocks: %v", err)
	}
	return shardBlocks, total
}

func TestCountBlockTransactionsMultiAccount(t *testing.T) {
	for _, tc := range []struct {
		name     string
		accounts int
		txs      func(i int) int
	}{
		{name: "single_account_single_tx", accounts: 1, txs: func(int) int { return 1 }},
		{name: "single_account_many_txs", accounts: 1, txs: func(int) int { return 17 }},
		{name: "many_accounts_mixed", accounts: 25, txs: func(i int) int { return 1 + i%3 }},
		{name: "many_accounts_wide", accounts: 128, txs: func(i int) int { return 1 + i%5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, expected := testShardAccountBlocksCell(t, tc.accounts, tc.txs)
			got, err := countBlockTransactions(root)
			if err != nil {
				t.Fatalf("count block transactions: %v", err)
			}
			if got != expected {
				t.Fatalf("count = %d, want %d", got, expected)
			}
		})
	}
}

func TestCountBlockTransactionsEmptyDict(t *testing.T) {
	empty, err := cell.NewAugDict(256, testCurrencyAug{})
	if err != nil {
		t.Fatalf("create empty dict: %v", err)
	}
	shardBlocks, err := tlb.ToCell(&tlb.ShardAccountBlocks{
		Accounts: &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: empty},
	})
	if err != nil {
		t.Fatalf("build shard account blocks: %v", err)
	}
	got, err := countBlockTransactions(shardBlocks)
	if err != nil {
		t.Fatalf("count block transactions: %v", err)
	}
	if got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
}

func BenchmarkCountBlockTransactions(b *testing.B) {
	root, expected := testShardAccountBlocksCell(b, 200, func(int) int { return 2 })
	b.ReportAllocs()
	for b.Loop() {
		got, err := countBlockTransactions(root)
		if err != nil {
			b.Fatal(err)
		}
		if got != expected {
			b.Fatalf("count = %d, want %d", got, expected)
		}
	}
}
