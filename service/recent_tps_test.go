package service

import (
	"context"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestRecentTPSWindowUsesBlockGenUTime(t *testing.T) {
	state := newRecentTPSWindowState(10 * time.Second)
	start := time.Unix(90, 0)

	state.add(recentTPSTestBlock(1, 90), 100)
	if snapshot := state.snapshot(start); snapshot.Complete {
		t.Fatalf("initial snapshot is complete: %+v", snapshot)
	}
	state.add(recentTPSTestBlock(2, 91), 20)
	state.add(recentTPSTestBlock(3, 95), 30)
	state.add(recentTPSTestBlock(4, 100), 50)
	state.add(recentTPSTestBlock(5, 101), 70)

	snapshot := state.snapshot(time.Unix(100, 0))
	if !snapshot.Complete {
		t.Fatalf("snapshot is incomplete: %+v", snapshot)
	}
	if snapshot.DurationSeconds != 10 {
		t.Fatalf("window = %d seconds, want 10", snapshot.DurationSeconds)
	}
	if snapshot.Transactions != 100 {
		t.Fatalf("transactions = %d, want 100", snapshot.Transactions)
	}
	if math.Abs(snapshot.TPS-10) > 1e-9 {
		t.Fatalf("tps = %f, want 10", snapshot.TPS)
	}

	snapshot = state.snapshot(time.Unix(101, 0))
	if snapshot.Transactions != 150 {
		t.Fatalf("transactions after window shift = %d, want 150", snapshot.Transactions)
	}
	if math.Abs(snapshot.TPS-15) > 1e-9 {
		t.Fatalf("tps after window shift = %f, want 15", snapshot.TPS)
	}
}

func TestRecentTPSWindowDeduplicatesBlocks(t *testing.T) {
	state := newRecentTPSWindowState(10 * time.Second)
	block := recentTPSTestBlock(1, 100)

	state.add(block, 25)
	state.add(block, 25)
	snapshot := state.snapshot(time.Unix(100, 0))
	if snapshot.Transactions != 25 {
		t.Fatalf("transactions = %d, want 25", snapshot.Transactions)
	}
}

func TestRecentTPSTrackerSkipsExpiredBlockBeforeQueue(t *testing.T) {
	tracker := newRecentTPSTracker(10 * time.Second)
	block := testBlockID(0, topShard, 1)

	tracker.observe(block, nil, 90, time.Unix(100, 0))
	if queued := len(tracker.blocks); queued != 0 {
		t.Fatalf("queued blocks = %d, want 0", queued)
	}
	if invalidations := tracker.invalidations.Load(); invalidations != 0 {
		t.Fatalf("invalidations = %d, want 0", invalidations)
	}
}

func TestRecentTPSTrackerInvalidatesOnQueueOverflow(t *testing.T) {
	tracker := newRecentTPSTracker(10 * time.Second)
	root := cell.BeginCell().EndCell()
	now := time.Unix(100, 0)

	for seqno := uint32(1); seqno <= recentTPSQueueCapacity+1; seqno++ {
		tracker.observe(testBlockID(0, topShard, seqno), root, 100, now)
	}
	if invalidations := tracker.invalidations.Load(); invalidations != 1 {
		t.Fatalf("invalidations = %d, want 1", invalidations)
	}
	if snapshot := tracker.statusSnapshot(); snapshot.Complete {
		t.Fatalf("overflow snapshot is complete: %+v", snapshot)
	}
}

func TestRecentTPSTrackerCountsInBackground(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block, root, _, _ := mustStatusFixtureBlock(t)
		tracker := newRecentTPSTracker(10 * time.Second)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			tracker.run(ctx)
		}()

		now := time.Now()
		tracker.observe(block, root, uint32(now.Unix()), now)
		synctest.Wait()

		snapshot := tracker.statusSnapshot()
		if snapshot.Transactions == 0 {
			t.Fatalf("background snapshot has no transactions: %+v", snapshot)
		}
		if snapshot.Complete {
			t.Fatalf("background snapshot completed before warm-up: %+v", snapshot)
		}

		cancel()
		synctest.Wait()
		<-done
	})
}

func BenchmarkRecentTPSTrackerSkipExpiredBlock(b *testing.B) {
	tracker := newRecentTPSTracker(10 * time.Second)
	block := testBlockID(0, topShard, 1)
	genUTime := uint32(time.Now().Add(-tracker.window).Unix())

	b.ReportAllocs()
	for b.Loop() {
		tracker.observe(block, nil, genUTime, time.Now())
	}
}

func recentTPSTestBlock(seqno uint32, genUTime uint32) recentTPSBlock {
	id := testBlockID(0, topShard, seqno)
	return recentTPSBlock{
		id:       id,
		key:      storage.BlockKey(id),
		root:     cell.BeginCell().EndCell(),
		genUTime: genUTime,
	}
}
