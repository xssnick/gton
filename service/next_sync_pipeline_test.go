package service

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestDownloadElapsedExcludingInlinePrepare(t *testing.T) {
	tests := []struct {
		name    string
		elapsed time.Duration
		prepare time.Duration
		want    time.Duration
	}{
		{
			name:    "no inline prepare",
			elapsed: 20 * time.Millisecond,
			want:    20 * time.Millisecond,
		},
		{
			name:    "subtract inline prepare",
			elapsed: 20 * time.Millisecond,
			prepare: 7 * time.Millisecond,
			want:    13 * time.Millisecond,
		},
		{
			name:    "clamp negative duration",
			elapsed: 5 * time.Millisecond,
			prepare: 7 * time.Millisecond,
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := downloadElapsedExcludingInlinePrepare(tt.elapsed, tt.prepare)
			if got != tt.want {
				t.Fatalf("downloadElapsedExcludingInlinePrepare(%s, %s) = %s, want %s", tt.elapsed, tt.prepare, got, tt.want)
			}
		})
	}
}

// testShardDescription describes block `to` of the top shard of workchain,
// with one chain link per block from `from` to `to`, each pointing at its
// predecessor.
func testShardDescription(workchain int32, from, to uint32) p2p.ShardBlockDescription {
	desc := p2p.ShardBlockDescription{Block: testBlockID(workchain, topShard, to)}
	for seqno := from; seqno <= to; seqno++ {
		desc.Chain = append(desc.Chain, p2p.ShardDescriptionLink{
			Block:    testBlockID(workchain, topShard, seqno),
			PrevRefs: []ton.BlockIDExt{testBlockID(workchain, topShard, seqno-1)},
		})
	}
	return desc
}

func storeTestShardDescriptionHint(svc *SyncCoordinator, desc p2p.ShardBlockDescription) {
	svc.storeShardDescriptionHint(shardDescriptionHint{
		Description: desc,
		Overlay:     "test",
		ReceivedAt:  time.Now(),
	})
}

// newShardDescriptionPrefetchTestRunner builds a runner whose resolver knows
// tip as the only current shard block and whose node is offline, so every
// scheduled prefetch returns immediately and frees its worker slot.
func newShardDescriptionPrefetchTestRunner(t *testing.T, tip ton.BlockIDExt) *nextSyncRunner {
	t.Helper()

	ctx := context.Background()
	return &nextSyncRunner{
		service: &SyncCoordinator{
			node:                  newFrozenTestNode(t),
			shardDescriptionHints: map[storage.BlockRootHash]shardDescriptionHint{},
		},
		ctx:                               ctx,
		shardPrefetchSlots:                make(chan struct{}, nextShardPrefetchWorkers),
		shardDescriptionPrefetchScheduled: map[storage.BlockRootHash]struct{}{},
		shardResolver: newShardStateResolver(ctx, shardStateResolverConfig{
			current: map[storage.ShardKey]storage.BlockState{
				storage.ShardKeyFromBlock(tip): {Block: tip},
			},
		}),
	}
}

// drainShardPrefetchSlots waits for every scheduled prefetch goroutine to
// release its worker slot.
func drainShardPrefetchSlots(r *nextSyncRunner) {
	for i := 0; i < cap(r.shardPrefetchSlots); i++ {
		r.shardPrefetchSlots <- struct{}{}
	}
	for i := 0; i < cap(r.shardPrefetchSlots); i++ {
		<-r.shardPrefetchSlots
	}
}

func scheduledShardDescriptionPrefetches(r *nextSyncRunner) []ton.BlockIDExt {
	r.shardDescriptionPrefetchMu.Lock()
	defer r.shardDescriptionPrefetchMu.Unlock()

	if len(r.shardDescriptionPrefetchOrder) != len(r.shardDescriptionPrefetchScheduled) {
		panic("scheduled prefetch order and set disagree")
	}
	r.service.shardDescriptionMu.Lock()
	defer r.service.shardDescriptionMu.Unlock()

	blocks := make([]ton.BlockIDExt, 0, len(r.shardDescriptionPrefetchOrder))
	for _, key := range r.shardDescriptionPrefetchOrder {
		if _, ok := r.shardDescriptionPrefetchScheduled[key]; !ok {
			panic("scheduled prefetch order lists an unscheduled key")
		}
		blocks = append(blocks, r.service.shardDescriptionHints[key].Description.Block)
	}
	return blocks
}

func retainedShardDescriptionHints(svc *SyncCoordinator) []ton.BlockIDExt {
	svc.shardDescriptionMu.Lock()
	defer svc.shardDescriptionMu.Unlock()

	blocks := make([]ton.BlockIDExt, 0, len(svc.shardDescriptionOrder))
	for _, key := range svc.shardDescriptionOrder {
		hint, ok := svc.shardDescriptionHints[key]
		if !ok {
			continue
		}
		blocks = append(blocks, hint.Description.Block)
	}
	return blocks
}

func requireBlocks(t *testing.T, what string, got []ton.BlockIDExt, want ...ton.BlockIDExt) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s = %d blocks, want %d", what, len(got), len(want))
	}
	for i := range want {
		if !got[i].Equals(&want[i]) {
			t.Fatalf("%s[%d] = %s, want %s", what, i, storage.FormatBlockRef(got[i]), storage.FormatBlockRef(want[i]))
		}
	}
}

func TestPrefetchShardDescriptionHintsSchedulesAdmissibleHints(t *testing.T) {
	tip := testBlockID(0, topShard, 10)
	runner := newShardDescriptionPrefetchTestRunner(t, tip)
	svc := runner.service

	next := testShardDescription(0, 11, 11)
	afterNext := testShardDescription(0, 11, 12)
	tooNew := testShardDescription(0, 40, 40)
	old := testShardDescription(0, 10, 10)
	unanchored := testShardDescription(0, 13, 13)
	otherWorkchain := testShardDescription(1, 11, 11)
	for _, desc := range []p2p.ShardBlockDescription{next, tooNew, old, afterNext, unanchored, otherWorkchain} {
		storeTestShardDescriptionHint(svc, desc)
	}

	runner.prefetchShardDescriptionHints()
	drainShardPrefetchSlots(runner)

	requireBlocks(t, "scheduled", scheduledShardDescriptionPrefetches(runner), next.Block, afterNext.Block)
	// Too-new and unrelated hints wait for the current state to move; too-old
	// and unanchored ones are dropped for good.
	requireBlocks(t, "retained", retainedShardDescriptionHints(svc), next.Block, tooNew.Block, afterNext.Block, otherWorkchain.Block)

	// A pass over the same hints and the same current state changes nothing.
	runner.prefetchShardDescriptionHints()
	drainShardPrefetchSlots(runner)

	requireBlocks(t, "scheduled after repeat", scheduledShardDescriptionPrefetches(runner), next.Block, afterNext.Block)
	requireBlocks(t, "retained after repeat", retainedShardDescriptionHints(svc), next.Block, tooNew.Block, afterNext.Block, otherWorkchain.Block)

	// A hint added to an otherwise unchanged table is judged on the next pass.
	third := testShardDescription(0, 11, 13)
	storeTestShardDescriptionHint(svc, third)
	runner.prefetchShardDescriptionHints()
	drainShardPrefetchSlots(runner)

	requireBlocks(t, "scheduled after new hint", scheduledShardDescriptionPrefetches(runner), next.Block, afterNext.Block, third.Block)

	// Once the current state moves, hints anchored to the new tip become
	// admissible even though the table itself did not change. Scheduled hints
	// are never re-judged, so the overtaken ones stay until they expire.
	newTip := afterNext.Block
	fourth := testShardDescription(0, 13, 14)
	storeTestShardDescriptionHint(svc, fourth)
	runner.updateShardResolverCurrent(map[storage.ShardKey]storage.BlockState{
		storage.ShardKeyFromBlock(newTip): {Block: newTip},
	})
	runner.prefetchShardDescriptionHints()
	drainShardPrefetchSlots(runner)

	requireBlocks(t, "scheduled after current moved", scheduledShardDescriptionPrefetches(runner), next.Block, afterNext.Block, third.Block, fourth.Block)
	requireBlocks(t, "retained after current moved", retainedShardDescriptionHints(svc), next.Block, tooNew.Block, afterNext.Block, otherWorkchain.Block, third.Block, fourth.Block)
}

func TestPrefetchShardDescriptionHintsRetriesHintsLeftWithoutWorker(t *testing.T) {
	tip := testBlockID(0, topShard, 10)
	runner := newShardDescriptionPrefetchTestRunner(t, tip)
	runner.shardPrefetchSlots = make(chan struct{}, 1)

	first := testShardDescription(0, 11, 11)
	second := testShardDescription(0, 11, 12)
	third := testShardDescription(0, 11, 13)
	for _, desc := range []p2p.ShardBlockDescription{first, second, third} {
		storeTestShardDescriptionHint(runner.service, desc)
	}

	// One worker slot. A pass schedules what the slot admits — one hint, or
	// more when the worker of the first is already done by the time the pass
	// reaches the next — and the hints it had to leave behind are retried on
	// the following passes over the unchanged table until none is left.
	want := map[storage.BlockRootHash]struct{}{
		storage.BlockKey(first.Block): {}, storage.BlockKey(second.Block): {}, storage.BlockKey(third.Block): {},
	}
	scheduled := 0
	for pass := 1; scheduled < len(want); pass++ {
		if pass > len(want) {
			t.Fatalf("hints left without a worker were not retried: %d of %d scheduled after %d passes",
				scheduled, len(want), pass-1)
		}
		runner.prefetchShardDescriptionHints()
		drainShardPrefetchSlots(runner)
		blocks := scheduledShardDescriptionPrefetches(runner)
		for _, block := range blocks {
			if _, ok := want[storage.BlockKey(block)]; !ok {
				t.Fatalf("pass %d scheduled %s, which was never a hint", pass, storage.FormatBlockRef(block))
			}
		}
		if len(blocks) <= scheduled {
			t.Fatalf("pass %d scheduled nothing although %d hints were left without a worker",
				pass, len(want)-scheduled)
		}
		if pass == 1 {
			requireBlocks(t, "first scheduled hint", blocks[:1], first.Block)
		}
		scheduled = len(blocks)
	}
	requireBlocks(t, "scheduled once every hint had its worker",
		scheduledShardDescriptionPrefetches(runner), first.Block, second.Block, third.Block)

	// Nothing left behind: the next pass over the unchanged table is a no-op.
	runner.prefetchShardDescriptionHints()
	drainShardPrefetchSlots(runner)
	requireBlocks(t, "scheduled after a pass with nothing left",
		scheduledShardDescriptionPrefetches(runner), first.Block, second.Block, third.Block)
}

func TestPrefetchShardDescriptionHintsUnchangedHintsAllocateNothing(t *testing.T) {
	tip := testBlockID(0, topShard, 10)
	runner := newShardDescriptionPrefetchTestRunner(t, tip)

	// Fewer schedulable hints than worker slots, so the first pass schedules
	// all of them, plus a realistic tail of hints the pass must keep skipping.
	for seqno := uint32(11); seqno <= 14; seqno++ {
		storeTestShardDescriptionHint(runner.service, testShardDescription(0, 11, seqno))
	}
	for seqno := uint32(40); seqno < 70; seqno++ {
		storeTestShardDescriptionHint(runner.service, testShardDescription(0, seqno, seqno))
	}
	for seqno := uint32(1); seqno <= 30; seqno++ {
		storeTestShardDescriptionHint(runner.service, testShardDescription(1, seqno, seqno))
	}

	runner.prefetchShardDescriptionHints()
	drainShardPrefetchSlots(runner)
	if got := len(scheduledShardDescriptionPrefetches(runner)); got != 4 {
		t.Fatalf("scheduled prefetches = %d, want 4", got)
	}

	allocs := testing.AllocsPerRun(200, runner.prefetchShardDescriptionHints)
	t.Logf("prefetch pass over %d unchanged hints: %.0f allocs/op", len(retainedShardDescriptionHints(runner.service)), allocs)
	if allocs != 0 {
		t.Fatalf("prefetch pass over unchanged hints allocated %.0f objects, want 0", allocs)
	}
}
