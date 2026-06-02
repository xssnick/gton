package p2p

import (
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testBlockCacheObserver struct {
	nonfinalEnabled bool
	nonfinal        []storage.LiveBlockArtifacts
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
