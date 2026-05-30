package p2p

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestRememberBlockFullSkipsUnverifiedBlock(t *testing.T) {
	store := newTestPeerStore()
	observer := &testBlockCacheObserver{}
	node := &Node{
		log:                discardLogger(),
		peerStorage:        store,
		blockCacheObserver: observer,
	}

	block := testBlockID(-1, topShard, 42)
	node.rememberBlockFull(nil, &DownloadedBlock{
		ID:       block,
		ProofBOC: []byte{0xaa, 0xbb},
		BlockBOC: []byte{0xcc, 0xdd},
		Meta:     &storage.BlockMeta{ID: block, GenUTime: 123},
	}, false)

	if _, err := store.BlockData(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("block data error = %v, want ErrNotFound", err)
	}
	if len(observer.flushed) != 0 {
		t.Fatalf("flushed notifications = %d, want 0", len(observer.flushed))
	}

	if stored := store.blocks[storage.BlockKey(block)]; stored != nil {
		t.Fatalf("stored block = %+v, want nil", stored)
	}
}

type testBlockCacheObserver struct {
	flushed         []ton.BlockIDExt
	nonfinalEnabled bool
	nonfinal        []storage.LiveBlockArtifacts
}

func (o *testBlockCacheObserver) MarkLiveBlockFlushed(block ton.BlockIDExt) {
	o.flushed = append(o.flushed, block)
}

func (o *testBlockCacheObserver) NonfinalBlockCacheEnabled() bool {
	return o.nonfinalEnabled
}

func (o *testBlockCacheObserver) PublishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, _ storage.LiveBlockNonfinalKind) error {
	o.nonfinal = append(o.nonfinal, artifacts)
	return nil
}

func TestPublishNonfinalDownloadedBlockSkipsWhenDisabled(t *testing.T) {
	observer := &testBlockCacheObserver{}
	node := &Node{
		log:                discardLogger(),
		blockCacheObserver: observer,
	}
	block := testBlockID(0, topShard, 43)

	node.publishNonfinalDownloadedBlock(&DownloadedBlock{
		ID:       block,
		Block:    cell.BeginCell().MustStoreUInt(1, 1).EndCell(),
		BlockBOC: []byte{0x01},
	}, storage.LiveBlockNonfinalSigned)

	if len(observer.nonfinal) != 0 {
		t.Fatalf("non-final publishes = %d, want 0", len(observer.nonfinal))
	}
}

func TestPublishNonfinalDownloadedBlockPublishesWhenEnabled(t *testing.T) {
	observer := &testBlockCacheObserver{nonfinalEnabled: true}
	node := &Node{
		log:                discardLogger(),
		blockCacheObserver: observer,
	}
	block := testBlockID(0, topShard, 44)

	node.publishNonfinalDownloadedBlock(&DownloadedBlock{
		ID:       block,
		Block:    cell.BeginCell().MustStoreUInt(1, 1).EndCell(),
		BlockBOC: []byte{0x01},
	}, storage.LiveBlockNonfinalSigned)

	if len(observer.nonfinal) != 1 || !observer.nonfinal[0].Block.Equals(&block) {
		t.Fatalf("non-final publishes = %+v, want block", observer.nonfinal)
	}
}
