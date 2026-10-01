package liveview

import (
	"math/big"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestExternalMessageLimitsFromConfigRoot(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name   string
		config any
		want   ExternalMessageSizeLimits
	}
	tests := []testCase{
		{
			name: "absent",
			want: ExternalMessageSizeLimits{MaxSize: 65535, MaxDepth: 512},
		},
		{
			name:   "v1",
			config: tlb.SizeLimitsConfigV1{MaxExtMsgSize: 256, MaxExtMsgDepth: 3},
			want:   ExternalMessageSizeLimits{MaxSize: 256, MaxDepth: 3},
		},
		{
			name:   "v2",
			config: tlb.SizeLimitsConfigV2{MaxExtMsgSize: 512, MaxExtMsgDepth: 4},
			want:   ExternalMessageSizeLimits{MaxSize: 512, MaxDepth: 4},
		},
		{
			name: "v3",
			config: tlb.SizeLimitsConfigV3{
				MaxExtMsgSize:    1024,
				MaxExtMsgDepth:   5,
				MaxTotalMsgBits:  2097152,
				MaxTotalMsgCells: 8192,
			},
			want: ExternalMessageSizeLimits{MaxSize: 1024, MaxDepth: 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := cell.NewDict(32)
			configAddress := cell.BeginCell().MustStoreSlice(make([]byte, 32), 256).EndCell()
			if err := config.SetIntKey(big.NewInt(0), cell.BeginCell().MustStoreRef(configAddress).EndCell()); err != nil {
				t.Fatalf("store config address: %v", err)
			}
			if tt.config != nil {
				param, err := tlb.ToCell(&tlb.SizeLimitsConfig{Config: tt.config})
				if err != nil {
					t.Fatalf("serialize size limits: %v", err)
				}
				if err := config.SetIntKey(big.NewInt(int64(tlb.ConfigParamSizeLimits)), cell.BeginCell().MustStoreRef(param).EndCell()); err != nil {
					t.Fatalf("store size limits: %v", err)
				}
			}

			limits, err := externalMessageLimitsFromConfigRoot(config.AsCell())
			if err != nil {
				t.Fatalf("load external message limits: %v", err)
			}
			if limits != tt.want {
				t.Fatalf("external message limits = %+v, want %+v", limits, tt.want)
			}

			root := cell.BeginCell().EndCell()
			for depth := uint16(1); depth < tt.want.MaxDepth; depth++ {
				root = cell.BeginCell().MustStoreRef(root).EndCell()
			}
			data := make([]byte, tt.want.MaxSize)
			if err := CheckExternalMessageLimits(limits, data, root); err != nil {
				t.Fatalf("message within size and depth limits was rejected: %v", err)
			}
			if err := CheckExternalMessageLimits(limits, append(data, 0), root); err == nil {
				t.Fatal("message exceeding size limit was accepted")
			}
			if err := CheckExternalMessageLimits(limits, data, cell.BeginCell().MustStoreRef(root).EndCell()); err == nil {
				t.Fatal("message at depth limit was accepted")
			}
		})
	}
}

func TestBlockViewExternalMessageAccountCacheLifecycle(t *testing.T) {
	view := &BlockView{retainCurrentCaches: true}
	addr := address.NewAddress(0, 0, make([]byte, 32))

	firstShard, firstAccount, err := view.ExternalMessageAccount(addr)
	if err != nil {
		t.Fatalf("load first account: %v", err)
	}
	secondShard, secondAccount, err := view.ExternalMessageAccount(addr)
	if err != nil {
		t.Fatalf("load cached account: %v", err)
	}
	if firstShard != secondShard || firstAccount != secondAccount {
		t.Fatal("repeated account lookup did not reuse parsed values")
	}
	if len(view.externalMsgAccounts) != 1 {
		t.Fatalf("cached accounts = %d, want 1", len(view.externalMsgAccounts))
	}

	view.releaseCurrentCaches()
	if view.externalMsgAccounts != nil {
		t.Fatal("retired view retained parsed accounts")
	}
	if _, _, err = view.ExternalMessageAccount(addr); err != nil {
		t.Fatalf("load account after retirement: %v", err)
	}
	if view.externalMsgAccounts != nil {
		t.Fatal("retired view republished a parsed account")
	}
}

func TestBlockViewExternalMessageAccountCacheIsBoundedAndConcurrent(t *testing.T) {
	view := &BlockView{retainCurrentCaches: true}
	for i := 0; i <= liveExternalMessageAccountCacheLimit; i++ {
		account := make([]byte, 32)
		account[0] = byte(i)
		if _, _, err := view.ExternalMessageAccount(address.NewAddress(0, 0, account)); err != nil {
			t.Fatalf("load account %d: %v", i, err)
		}
	}
	if len(view.externalMsgAccounts) > liveExternalMessageAccountCacheLimit {
		t.Fatalf("cached accounts = %d, limit %d", len(view.externalMsgAccounts), liveExternalMessageAccountCacheLimit)
	}

	addr := address.NewAddress(0, 0, make([]byte, 32))
	const workers = 16
	shards := make([]*tlb.ShardAccount, workers)
	accounts := make([]*tlb.AccountState, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shards[i], accounts[i], errs[i] = view.ExternalMessageAccount(addr)
		}()
	}
	wg.Wait()
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("concurrent account %d: %v", i, errs[i])
		}
		if shards[i] != shards[0] || accounts[i] != accounts[0] {
			t.Fatal("concurrent account lookups did not adopt one cached value")
		}
	}
}

func BenchmarkBlockViewExternalMessageAccount(b *testing.B) {
	addr := address.NewAddress(0, 0, make([]byte, 32))

	b.Run("view_cache", func(b *testing.B) {
		view := &BlockView{retainCurrentCaches: true}
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := view.ExternalMessageAccount(addr); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parse_each_time", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := externalMessageAccountFromAccountsRoot(nil, addr); err != nil {
				b.Fatal(err)
			}
		}
	})
}
