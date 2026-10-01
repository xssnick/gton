package msgpool

import (
	"testing"
)

// A transit or requeue addition may carry a canonical lt below entries the
// run already dequeued. Those tombstones stay in the committed run for
// historical cuts, so the run must remain sorted with them included: a later
// merge walks the whole slice and would otherwise place a new message ahead of
// live messages with smaller keys.
func TestInternalsAdditionBelowTombstonedTailKeepsRunOrder(t *testing.T) {
	pool := New(Config{})
	defer pool.Close()
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]ShardIdent{testOwner}); err != nil {
		t.Fatal(err)
	}

	seeded := []*InternalMessage{imsg(90, 1), imsg(100, 2)}
	bindTestMessages(baseSource, 10, seeded)
	if err := internals.Seed(testOwner, baseSource, sref(10, 0xaa), seeded, uint64(len(seeded))); err != nil {
		t.Fatal(err)
	}

	apply := func(ref SourceRef, delta *InternalsDelta) {
		t.Helper()
		bindTestMessages(baseSource, ref.Seqno, delta.Added)
		if err := internals.ApplyBlock(testOwner, baseSource, ref, delta); err != nil {
			t.Fatal(err)
		}
	}

	// Block 11 dequeues the whole seeded tail and enqueues a transit message
	// ordered before it; blocks 12 and 13 keep adding below the tombstones.
	apply(sref(11, 0xab), &InternalsDelta{
		Added:        []*InternalMessage{imsg(50, 3)},
		RemovedKeys:  []QueueKey{seeded[0].Key, seeded[1].Key},
		AddedTotal:   1,
		RemovedTotal: 2,
	})
	apply(sref(12, 0xac), &InternalsDelta{Added: []*InternalMessage{imsg(60, 4)}, AddedTotal: 1})
	apply(sref(13, 0xad), &InternalsDelta{Added: []*InternalMessage{imsg(55, 5)}, AddedTotal: 1})

	state, err := internals.destination(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	entries := state.runs[baseSource].entries
	lts := make([]uint64, len(entries))
	sorted := true
	for index := range entries {
		lts[index] = entries[index].msg.EnqueuedLT
		if index > 0 && CompareLtHash(entries[index-1].msg, entries[index].msg) >= 0 {
			sorted = false
		}
	}
	state.mu.Unlock()
	if !sorted {
		t.Fatalf("run entries lts = %v, want sorted with tombstones included", lts)
	}

	committed, err := internals.Cut(testOwner, CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(13, 0xad)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, committed, 50, 55, 60)

	// The collator reads committed runs through branch pins, which copy the
	// run order verbatim; every retained position must stay canonical.
	branch, err := internals.OpenBranch(testOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer branch.Close()

	cutAt := func(ref SourceRef, want ...uint64) {
		t.Helper()
		cut, err := branch.Cut(CutRequest{Sources: map[ShardIdent]CutSource{baseSource: {Visible: ref}}})
		if err != nil {
			t.Fatal(err)
		}
		requireLts(t, cut, want...)
	}
	cutAt(sref(13, 0xad), 50, 55, 60)
	cutAt(sref(12, 0xac), 50, 60)
	cutAt(sref(11, 0xab), 50)
	cutAt(sref(10, 0xaa), 90, 100)
}
