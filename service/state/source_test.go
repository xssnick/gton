package state

import (
	"bytes"
	"context"
	"errors"
	"flexserver/service/p2p"
	"flexserver/service/storage/pebblestore"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

func TestP2PSourceUsesRuntimeSeenMasterchainHint(t *testing.T) {
	ctx := context.Background()
	store := newSourceTestPebbleStore(t)
	node, err := p2p.New(p2p.Options{Storage: store})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	stored := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     123,
		RootHash:  bytes.Repeat([]byte{1}, 32),
		FileHash:  bytes.Repeat([]byte{2}, 32),
	}
	node.RememberSeenMasterchainBlock(stored)

	source := NewP2PSource(node)
	got, err := source.LatestMasterchainBlock(ctx)
	if err != nil {
		t.Fatalf("latest masterchain block: %v", err)
	}
	if !got.Equals(&stored) {
		t.Fatalf("latest hint = %+v, want %+v", got, stored)
	}

	runtime, err := node.ObservedMasterchainBlock()
	if err != nil {
		t.Fatalf("expected node runtime latest to be seeded: %v", err)
	}
	if !runtime.Equals(&stored) {
		t.Fatalf("runtime latest = %+v, want %+v", runtime, stored)
	}
}

func TestP2PSourceDoesNotUseStoredSeenMasterchainHint(t *testing.T) {
	store := newSourceTestPebbleStore(t)
	node, err := p2p.New(p2p.Options{Storage: store})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	stored := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     123,
		RootHash:  bytes.Repeat([]byte{1}, 32),
		FileHash:  bytes.Repeat([]byte{2}, 32),
	}
	if err = store.SaveSeenMasterchainBlock(context.Background(), stored); err != nil {
		t.Fatalf("save seen hint: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	source := NewP2PSource(node)
	if _, err = source.LatestMasterchainBlock(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("latest masterchain from stored hint = %v, want context deadline exceeded", err)
	}
}

func newSourceTestPebbleStore(tb testing.TB) *pebblestore.Store {
	tb.Helper()

	store, err := pebblestore.Open(pebblestore.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open pebble store: %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	return store
}

func TestChoosePersistentKeyBlockSkipsFreshAndNonPersistent(t *testing.T) {
	now := time.Unix(13_500_000, 0)
	bucket := uint32(1 << 17)
	prev := uint32(99 * bucket)
	persistent := uint32(100*bucket + 1000)
	nonPersistent := persistent + 1000
	freshPersistent := uint32(now.Unix()) - uint32((initialStateMinAge/time.Second)/2)

	got, ok := choosePersistentKeyBlock([]keyBlockCandidate{
		{block: ton.BlockIDExt{SeqNo: 10}, utime: prev},
		{block: ton.BlockIDExt{SeqNo: 20}, utime: persistent},
		{block: ton.BlockIDExt{SeqNo: 30}, utime: nonPersistent},
		{block: ton.BlockIDExt{SeqNo: 40}, utime: freshPersistent},
	}, now)
	if !ok {
		t.Fatal("expected persistent key block")
	}
	if got.SeqNo != 20 {
		t.Fatalf("unexpected key block seqno %d", got.SeqNo)
	}
}
