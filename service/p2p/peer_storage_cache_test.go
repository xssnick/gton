package p2p

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestRememberBlockFullSkipsPayloadWhenFileHashIsNotVerified(t *testing.T) {
	store := newTestPeerStore()
	observer := &testBlockCacheObserver{}
	node := &Node{
		log:                discardLogger(),
		peerStorage:        store,
		blockCacheObserver: observer,
	}

	block := testBlockID(-1, topShard, 42)
	node.rememberBlockFull(nil, &DownloadedBlock{
		ID:               block,
		ProofBOC:         []byte{0xaa, 0xbb},
		BlockBOC:         []byte{0xcc, 0xdd},
		Meta:             &storage.BlockMeta{ID: block, GenUTime: 123},
		VerifiedFileHash: false,
	}, true)

	if _, err := store.BlockData(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block data error = %v, want ErrNotFound", err)
	}
	if len(observer.flushed) != 0 {
		t.Fatalf("flushed notifications = %d, want 0", len(observer.flushed))
	}

	stored := store.blocks[storage.BlockKey(block)]
	if stored == nil {
		t.Fatal("expected proof/meta record to be stored")
	}
	if len(stored.Block) != 0 {
		t.Fatalf("stored block payload len = %d, want 0", len(stored.Block))
	}
	if stored.Meta == nil || stored.Meta.GenUTime != 123 {
		t.Fatalf("stored meta = %+v, want copied meta", stored.Meta)
	}
}

type testBlockCacheObserver struct {
	flushed []ton.BlockIDExt
}

func (o *testBlockCacheObserver) MarkLiveBlockFlushed(block ton.BlockIDExt) {
	o.flushed = append(o.flushed, block)
}
