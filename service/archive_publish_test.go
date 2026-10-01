package service

import (
	"context"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

func TestImportArchiveBlocksRejectsInvalidNewArchive(t *testing.T) {
	store := openTestPebbleStorage(t)
	runner := &ArchiveRunner{storage: store}
	ctx := context.Background()
	importer := archive.NewImporter()
	t.Cleanup(importer.Close)

	badData := []byte{0x00, 0x01, 0x02, 0x03}
	_, err := runner.importArchiveBlocks(ctx, importer, &archive.Downloaded{
		MasterchainSeqno: 21,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Data:             badData,
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "archive package magic mismatch") {
		t.Fatalf("import corrupt archive error = %v, want package magic mismatch", err)
	}
}

func TestObserveImportedArchiveBlocksReceived(t *testing.T) {
	observer := &testBlockReceivedObserver{}
	runner := &ArchiveRunner{
		blockReceivedObserver: &blockReceivedObserverRunner{
			log:      zerolog.Nop(),
			observer: observer,
		},
	}
	first := testBlockID(0, topShard, 10)
	second := testBlockID(0, topShard, 11)
	imported := &archive.Imported{
		FullBlocks: []*storage.ServedBlockFull{
			{ID: first},
			{ID: first},
			{ID: second},
		},
	}
	result := &archiveImportResult{
		blocks: map[storage.BlockRootHash]PreparedBlock{
			storage.BlockKey(first): {
				ID:       first,
				BlockBOC: []byte{0x01},
				ProofBOC: []byte{0x02},
				Meta:     &storage.BlockMeta{ID: first},
			},
			storage.BlockKey(second): {
				ID:       second,
				BlockBOC: []byte{0x03},
				ProofBOC: []byte{0x04},
				Meta:     &storage.BlockMeta{ID: second},
			},
		},
	}

	runner.observeImportedArchiveBlocksReceived(context.Background(), imported, result)

	if len(observer.events) != 2 {
		t.Fatalf("archive block received events = %d, want 2", len(observer.events))
	}
	if !observer.events[0].Meta.ID.Equals(&first) || !observer.events[1].Meta.ID.Equals(&second) {
		t.Fatalf("archive block received event order mismatch: %+v", observer.events)
	}
	if !observer.events[0].IsSigned || !observer.events[1].IsSigned {
		t.Fatalf("archive block received events should be signed: %+v", observer.events)
	}
}
