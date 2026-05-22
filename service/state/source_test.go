package state

import (
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

func TestChoosePersistentKeyBlockSkipsFreshAndNonPersistent(t *testing.T) {
	now := time.Unix(13_500_000, 0)
	bucket := uint32(1 << 17)
	prev := uint32(99 * bucket)
	persistent := uint32(100*bucket + 1000)
	nonPersistent := persistent + 1000
	freshPersistent := uint32(now.Unix()) - uint32((DefaultSyncBefore/time.Second)/2)

	got, ok := choosePersistentKeyBlock([]keyBlockCandidate{
		{block: ton.BlockIDExt{SeqNo: 10}, utime: prev},
		{block: ton.BlockIDExt{SeqNo: 20}, utime: persistent},
		{block: ton.BlockIDExt{SeqNo: 30}, utime: nonPersistent},
		{block: ton.BlockIDExt{SeqNo: 40}, utime: freshPersistent},
	}, now, DefaultSyncBefore)
	if !ok {
		t.Fatal("expected persistent key block")
	}
	if got.SeqNo != 20 {
		t.Fatalf("unexpected key block seqno %d", got.SeqNo)
	}
}

func TestChoosePersistentKeyBlockUsesSyncBefore(t *testing.T) {
	now := time.Unix(13_500_000, 0)
	bucket := uint32(1 << 17)
	prev := uint32(now.Unix()/int64(bucket))*bucket - 1000
	recentPersistent := uint32(now.Unix()) - uint32(30*time.Minute/time.Second)

	candidates := []keyBlockCandidate{
		{block: ton.BlockIDExt{SeqNo: 10}, utime: prev, allowBoundaryWithoutPrev: true},
		{block: ton.BlockIDExt{SeqNo: 20}, utime: recentPersistent},
	}

	got, ok := choosePersistentKeyBlock(candidates, now, time.Hour)
	if !ok {
		t.Fatal("expected fallback persistent key block")
	}
	if got.SeqNo != 10 {
		t.Fatalf("unexpected default sync_before key block seqno %d", got.SeqNo)
	}

	got, ok = choosePersistentKeyBlock(candidates, now, 10*time.Minute)
	if !ok {
		t.Fatal("expected recent persistent key block")
	}
	if got.SeqNo != 20 {
		t.Fatalf("unexpected custom sync_before key block seqno %d", got.SeqNo)
	}
}

func TestChoosePersistentKeyBlockDoesNotUseResumedAnchorAsBoundary(t *testing.T) {
	now := time.Unix(13_500_000, 0)
	bucket := uint32(1 << 17)
	resumedAnchor := uint32(100*bucket + 1000)
	nonPersistent := resumedAnchor + 1000

	_, ok := choosePersistentKeyBlock([]keyBlockCandidate{
		{block: ton.BlockIDExt{SeqNo: 10}, utime: resumedAnchor},
		{block: ton.BlockIDExt{SeqNo: 20}, utime: nonPersistent},
	}, now, time.Hour)
	if ok {
		t.Fatal("resumed anchor without previous key block must not be selected as persistent boundary")
	}
}
