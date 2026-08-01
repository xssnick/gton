package state

import (
	"errors"
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

	got, err := choosePersistentKeyBlock([]keyBlockCandidate{
		{block: ton.BlockIDExt{SeqNo: 10}, utime: prev},
		{block: ton.BlockIDExt{SeqNo: 20}, utime: persistent},
		{block: ton.BlockIDExt{SeqNo: 30}, utime: nonPersistent},
		{block: ton.BlockIDExt{SeqNo: 40}, utime: freshPersistent},
	}, now, DefaultSyncBefore, 0)
	if err != nil {
		t.Fatalf("choose persistent key block: %v", err)
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

	got, err := choosePersistentKeyBlock(candidates, now, time.Hour, 0)
	if err != nil {
		t.Fatalf("choose fallback persistent key block: %v", err)
	}
	if got.SeqNo != 10 {
		t.Fatalf("unexpected default sync_before key block seqno %d", got.SeqNo)
	}

	got, err = choosePersistentKeyBlock(candidates, now, 10*time.Minute, 0)
	if err != nil {
		t.Fatalf("choose recent persistent key block: %v", err)
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

	_, err := choosePersistentKeyBlock([]keyBlockCandidate{
		{block: ton.BlockIDExt{SeqNo: 10}, utime: resumedAnchor},
		{block: ton.BlockIDExt{SeqNo: 20}, utime: nonPersistent},
	}, now, time.Hour, 0)
	if !errors.Is(err, errNoPersistentKeyBlockCandidate) {
		t.Fatalf("choose resumed anchor: got %v, want %v", err, errNoPersistentKeyBlockCandidate)
	}
}

func TestChoosePersistentKeyBlockUsesSyncUntil(t *testing.T) {
	now := time.Unix(13_500_000, 0)
	bucket := uint32(1 << 17)
	prev := uint32(99 * bucket)
	first := uint32(100*bucket + 1000)
	second := uint32(101*bucket + 1000)

	got, err := choosePersistentKeyBlock([]keyBlockCandidate{
		{block: ton.BlockIDExt{SeqNo: 10}, utime: prev},
		{block: ton.BlockIDExt{SeqNo: 20}, utime: first},
		{block: ton.BlockIDExt{SeqNo: 30}, utime: second},
	}, now, DefaultSyncBefore, first)
	if err != nil {
		t.Fatalf("choose persistent key block before sync_until: %v", err)
	}
	if got.SeqNo != 20 {
		t.Fatalf("unexpected key block seqno %d", got.SeqNo)
	}
}
