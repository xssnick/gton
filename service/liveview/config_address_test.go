package liveview

import (
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
)

func TestBlockViewConfigAddressControlsExternalImportFees(t *testing.T) {
	data, err := os.ReadFile("../../../tonutils-go/tlb/testdata/blockchain_config_mainnet.boc")
	if err != nil {
		t.Fatal(err)
	}
	root, err := cell.FromBOC(data)
	if err != nil {
		t.Fatal(err)
	}

	const now uint32 = 1_750_000_000
	const blockLT uint64 = 1_000_000
	actual := [32]byte{0x91}
	other := [32]byte{0x92}
	addr := address.NewAddress(0, 0xff, actual[:])
	messageRoot, err := tlb.ToCell(&tlb.ExternalMessage{DstAddr: addr, Body: cell.BeginCell().EndCell()})
	if err != nil {
		t.Fatal(err)
	}
	message, err := tvm.PrepareMessage(messageRoot)
	if err != nil {
		t.Fatal(err)
	}

	for _, absentParam0 := range []bool{false, true} {
		name := "param0 points elsewhere"
		if absentParam0 {
			name = "param0 absent"
		}
		t.Run(name, func(t *testing.T) {
			config := root.AsDict(32).Copy()
			if absentParam0 {
				if err := config.DeleteIntKey(big.NewInt(0)); err != nil {
					t.Fatal(err)
				}
			}
			configRoot := config.AsCell()
			first := configAddressBlockView(t, configRoot, actual, other, now, blockLT)
			second := configAddressBlockView(t, configRoot, other, actual, now, blockLT)
			firstContext, err := first.BlockContext(now, blockLT)
			if err != nil {
				t.Fatal(err)
			}
			secondContext, err := second.BlockContext(now, blockLT)
			if err != nil {
				t.Fatal(err)
			}
			if firstContext.Config() != secondContext.Config() {
				t.Fatal("identical config roots did not share the prepared epoch")
			}
			if firstContext.Config().Root().HashKey() != configRoot.HashKey() {
				t.Fatal("actual config address rewrote the raw config dictionary")
			}
			shard, state, err := first.ExternalMessageAccount(addr)
			if err != nil {
				t.Fatal(err)
			}
			account, err := tvm.PrepareParsedAccount(shard, state, addr)
			if err != nil {
				t.Fatal(err)
			}
			machine := tvm.NewTVM()
			opts := tvm.TransactionOptions{LogicalTimeUint64: blockLT + 1}

			// One nanoTON cannot cover the ordinary external import fee. The actual
			// config contract is exempt and can reach ACCEPT; sharing its epoch
			// with another view must neither lose nor transfer that exemption.
			accepted, err := machine.CheckExternalMessageAccepted(firstContext, account, message, opts)
			if err != nil || !accepted {
				t.Fatalf("actual config account did not accept: accepted=%t err=%v", accepted, err)
			}
			accepted, err = machine.CheckExternalMessageAccepted(secondContext, account, message, opts)
			if accepted || err == nil || !strings.Contains(err.Error(), "external import fees exceed account balance") {
				t.Fatalf("ordinary account inherited config exemption: accepted=%t err=%v", accepted, err)
			}
			accepted, err = machine.CheckExternalMessageAccepted(firstContext, account, message, opts)
			if err != nil || !accepted {
				t.Fatalf("second view changed first context: accepted=%t err=%v", accepted, err)
			}
		})
	}
}

func configAddressBlockView(t *testing.T, config *cell.Cell, active, other [32]byte, now uint32, blockLT uint64) *BlockView {
	t.Helper()

	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range [][32]byte{active, other} {
		account, err := (&tlb.AccountState{
			IsValid: true, Address: address.NewAddress(0, 0xff, id[:]),
			StorageInfo: tlb.StorageInfo{
				StorageUsed:  tlb.StorageUsed{CellsUsed: big.NewInt(0), BitsUsed: big.NewInt(0)},
				StorageExtra: tlb.StorageExtraNone{}, LastPaid: now,
			},
			AccountStorage: tlb.AccountStorage{
				Status: tlb.AccountStatusActive, Balance: tlb.FromNanoTONU(1),
				StateInit: &tlb.StateInit{
					Code: cell.BeginCell().MustStoreBuilder(funcsop.ACCEPT().Serialize()).EndCell(),
					Data: cell.BeginCell().MustStoreRef(config).EndCell(),
				},
			},
		}).ToCell()
		if err != nil {
			t.Fatal(err)
		}
		shard, err := tlb.ToCell(&tlb.ShardAccount{Account: account, LastTransHash: make([]byte, 32)})
		if err != nil {
			t.Fatal(err)
		}
		if err = accounts.Set(cell.BeginCell().MustStoreSlice(id[:], 256).EndCell(), shard); err != nil {
			t.Fatal(err)
		}
	}
	var history tlb.OldMcBlocksInfoAugDict
	if err = history.LoadFromCell(cell.BeginCell().MustStoreBoolBit(false).MustStoreBoolBit(false).MustStoreUInt(0, 64).EndCell().MustBeginParse()); err != nil {
		t.Fatal(err)
	}
	info, err := (&tlb.McStateExtraBlockInfo{PrevBlocks: &history, AfterKeyBlock: true}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	extra := tlb.McStateExtra{ShardHashes: cell.NewDict(32), Info: info}
	extra.ConfigParams.ConfigAddr = active[:]
	extra.ConfigParams.Config.Params = config.AsDict(32)
	extraRoot, err := tlb.ToCell(&extra)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := (&tlb.ShardStateStats{TotalBalance: tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(2)}, Libraries: cell.NewDict(256)}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	queueInfo, err := (&tlb.OutMsgQueueInfo{OutQueue: queue, ProcInfo: cell.NewDict(96)}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	state := tlb.ShardStateUnsplit{
		ShardIdent: tlb.ShardIdent{WorkchainID: -1}, GenUTime: now, GenLT: blockLT,
		OutMsgQueueInfo: queueInfo, Stats: stats, McStateExtra: extraRoot,
	}
	state.Accounts.ShardAccounts = accounts
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	accountsRoot, err := accountsDictRoot(stateRoot)
	if err != nil {
		t.Fatal(err)
	}

	return &BlockView{
		block:        ton.BlockIDExt{Workchain: -1, Shard: -1 << 63, RootHash: stateRoot.Hash(), FileHash: make([]byte, 32)},
		stateRoot:    stateRoot,
		accountsRoot: accountsRoot,
	}
}
