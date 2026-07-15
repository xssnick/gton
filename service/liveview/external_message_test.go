package liveview

import (
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
)

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
