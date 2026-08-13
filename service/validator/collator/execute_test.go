package collator

import (
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// accountDictionaryEntry renders the account with PreparedAccount.ShardAccountCell
// instead of tlb.ToCell over the same struct. The cell it writes is a consensus
// value — it is what the accounts dictionary hashes into the state — so the two
// have to agree bit for bit, not merely in shape. Real mainnet accounts rather
// than hand-built ones, because the fields that could diverge (a frozen or
// uninitialised account cell, a last-transaction hash that is not exactly 32
// bytes) only occur in traffic.
func TestShardAccountCellMatchesReflection(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	previous := loadPreviousShardState(t, req)

	const sample = 64
	checked := 0
	if _, err := previous.Accounts.ShardAccounts.CheckForEachExtra(
		func(value, _ *cell.Slice, key *cell.Cell) (bool, error) {
			if checked >= sample {
				return false, nil
			}
			id, err := key.BeginParse()
			if err != nil {
				return false, err
			}
			raw, err := id.LoadSlice(256)
			if err != nil {
				return false, err
			}

			var account tlb.ShardAccount
			if err = loadExactSlice(&account, value.Copy()); err != nil {
				return false, err
			}
			prepared, err := tvm.PrepareAccount(&account, address.NewAddress(0, 0, raw))
			if err != nil {
				return false, err
			}
			want, err := tlb.ToCell(prepared.ShardAccount())
			if err != nil {
				return false, err
			}
			if got := prepared.ShardAccountCell(); got == nil || got.HashKey() != want.HashKey() {
				t.Fatalf("account %x: ShardAccountCell differs from tlb.ToCell", raw)
			}
			checked++
			return true, nil
		}, false); err != nil {
		t.Fatalf("walk predecessor accounts: %v", err)
	}
	if checked == 0 {
		t.Fatal("fixture carried no predecessor accounts, the comparison proved nothing")
	}
}
