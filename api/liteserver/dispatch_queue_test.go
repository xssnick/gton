package liteserver

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDispatchQueueQueriesAugmentationFormats(t *testing.T) {
	extra := cell.NewDict(32)
	if err := extra.SetIntKey(big.NewInt(7), cell.BeginCell().MustStoreBigVarUInt(big.NewInt(1234), 32).EndCell()); err != nil {
		t.Fatal(err)
	}
	balance := &tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1_000_000_000), ExtraCurrencies: extra}
	zero := &tlb.CurrencyCollection{}
	type formatCase struct {
		name   string
		first  *tlb.CurrencyCollection
		second *tlb.CurrencyCollection
	}
	cases := []formatCase{
		{name: "legacy"},
		{name: "stored_zero", first: zero, second: zero},
		{name: "stored_balance_with_extra_currency", first: balance, second: balance},
		{name: "mixed", second: balance},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := bytes.Repeat([]byte{0x31}, 32)
			second := bytes.Repeat([]byte{0x32}, 32)
			queue := testDispatchQueue(t,
				testDispatchQueueAccount{addr: first, lts: []uint64{10, 20}, balance: tc.first},
				testDispatchQueueAccount{addr: second, lts: []uint64{30}, balance: tc.second},
			)

			info, complete, err := collectDispatchQueueInfo(queue, first, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			if complete || len(info) != 1 || !bytes.Equal(info[0].Addr, first) || info[0].Size != 2 || info[0].MinLT != 10 || info[0].MaxLT != 20 {
				t.Fatalf("first info page = %+v, complete=%v", info, complete)
			}
			info, complete, err = collectDispatchQueueInfo(queue, first, false, 1)
			if err != nil {
				t.Fatal(err)
			}
			if !complete || len(info) != 1 || !bytes.Equal(info[0].Addr, second) || info[0].Size != 1 || info[0].MinLT != 30 || info[0].MaxLT != 30 {
				t.Fatalf("last info page = %+v, complete=%v", info, complete)
			}

			messages, roots, complete, err := collectDispatchQueueMessages(queue, first, 0, 2, false, true)
			if err != nil {
				t.Fatal(err)
			}
			if complete || len(messages) != 2 || len(roots) != 2 || messages[0].LT != 10 || messages[1].LT != 20 || !bytes.Equal(messages[1].Addr, first) {
				t.Fatalf("first message page = %+v, roots=%d, complete=%v", messages, len(roots), complete)
			}
			messages, roots, complete, err = collectDispatchQueueMessages(queue, first, 20, 2, false, true)
			if err != nil {
				t.Fatal(err)
			}
			if !complete || len(messages) != 1 || len(roots) != 1 || messages[0].LT != 30 || !bytes.Equal(messages[0].Addr, second) {
				t.Fatalf("last message page = %+v, roots=%d, complete=%v", messages, len(roots), complete)
			}
		})
	}
}
