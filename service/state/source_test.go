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
