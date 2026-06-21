package service

import (
	"context"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/archive"
)

func TestImportArchiveBlocksRejectsInvalidNewArchive(t *testing.T) {
	store := openTestPebbleStorage(t)
	svc := &Service{storage: store}
	ctx := context.Background()

	badData := []byte{0x00, 0x01, 0x02, 0x03}
	_, err := svc.importArchiveBlocks(ctx, &archive.Downloaded{
		MasterchainSeqno: 21,
		Shard:            archive.ShardID{Workchain: -1, Shard: topShard},
		ArchiveID:        0,
		Data:             badData,
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "archive package magic mismatch") {
		t.Fatalf("import corrupt archive error = %v, want package magic mismatch", err)
	}
}
