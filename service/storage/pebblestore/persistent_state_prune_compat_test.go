package pebblestore

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
)

func TestPersistentStateFileExpiredUsesExactDescriptionWhenBlockMetaWasPruned(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	nowUnix := uint64(time.Now().Unix())
	master := testArchivePruneBlock(100, 0x10)
	endTime := nowUnix + 3600
	if err = store.SavePersistentStateDescription(context.Background(), &storage.PersistentStateDescription{
		MasterchainBlock: master,
		StartTime:        uint32(nowUnix - 60),
		EndTime:          endTime,
	}); err != nil {
		t.Fatalf("save persistent state description: %v", err)
	}

	expired, err := store.persistentStateFileExpired(context.Background(), master, endTime-1)
	if err != nil {
		t.Fatalf("check unexpired persistent state: %v", err)
	}
	if expired {
		t.Fatal("persistent state with an exact unexpired description was expired")
	}

	expired, err = store.persistentStateFileExpired(context.Background(), master, endTime+1)
	if err != nil {
		t.Fatalf("check expired persistent state: %v", err)
	}
	if !expired {
		t.Fatal("persistent state past the described end time was retained")
	}

	different := testArchivePruneBlock(master.SeqNo, 0x20)
	expired, err = store.persistentStateFileExpired(context.Background(), different, endTime-1)
	if err != nil {
		t.Fatalf("check mismatched persistent state description: %v", err)
	}
	if !expired {
		t.Fatal("description for a different block with the same seqno was accepted")
	}
}
