package liveview

import (
	"errors"
	"math"
	"testing"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
)

func TestStoreAccountRootUsesCurrentLiveView(t *testing.T) {
	accountID := [32]byte{0x31}
	account := emptyShardAccount()
	value, err := tlb.ToCell(account)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	if err = accounts.Set(blockproof.AccountKey(accountID[:]), value); err != nil {
		t.Fatal(err)
	}

	master := testLiveBlockID(-1, math.MinInt64, 10, 0x11)
	shard := testLiveBlockID(0, math.MinInt64, 20, 0x21)
	view := &BlockView{
		block:               shard,
		accountsRoot:        accounts.RootCell(),
		retainCurrentCaches: true,
	}
	live := New(noopBacking{})
	live.mu.Lock()
	live.current = &storage.CurrentState{
		Masterchain: storage.BlockState{Block: master},
		Shards: map[storage.ShardKey]storage.BlockState{
			{Workchain: 0, Shard: math.MinInt64}: {Block: shard},
		},
	}
	live.mu.Unlock()

	if _, err = live.AccountRoot(t.Context(), 0, accountID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cold live view error = %v, want ErrNotFound", err)
	}

	live.mu.Lock()
	live.blocks[storage.BlockKey(shard)] = &liveBlock{id: shard, fragments: view}
	live.mu.Unlock()

	root, err := live.AccountRoot(t.Context(), 0, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if root != account.Account.HashKey() {
		t.Fatalf("account root = %x, want %x", root, account.Account.HashKey())
	}

	missing := [32]byte{0xff}
	if _, err = live.AccountRoot(t.Context(), 0, missing); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing account error = %v, want ErrNotFound", err)
	}
}
