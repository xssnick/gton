package msgpool

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestInternalsExpiredTombstoneReleasesMessage(t *testing.T) {
	t.Parallel()
	n := testInternals(t)
	removed := imsg(100, 1)
	live := imsg(200, 2)
	seedN(t, n, baseSource, sref(10, 10), []*InternalMessage{removed, live})
	if err := n.ApplyBlock(baseSource, sref(11, 11), &InternalsDelta{
		RemovedKeys: []QueueKey{removed.Key}, RemovedTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// A single removed message never reaches the ratio-compaction threshold.
	// Once its historical position expires it must no longer pin its BOC.
	for seqno := uint32(12); seqno <= 11+2*maxSourceRefHistory; seqno++ {
		if err := n.ApplyBlock(baseSource, sref(seqno, byte(seqno)), &InternalsDelta{}); err != nil {
			t.Fatal(err)
		}
	}
	if stats := n.Stats(); stats.Removed != 0 || stats.Entries != 1 {
		t.Fatalf("expired queue retains removed=%d live=%d, want 0 and 1", stats.Removed, stats.Entries)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	for _, entry := range n.runs[baseSource].entries[:cap(n.runs[baseSource].entries)] {
		if entry.msg == removed {
			t.Fatal("expired tombstone still retains its message in the backing array")
		}
	}
}

func TestInternalsTombstoneExpiryPreservesHistoricalCuts(t *testing.T) {
	t.Parallel()
	n := testInternals(t)
	old := imsg(100, 1)
	recent := imsg(200, 2)
	live := imsg(300, 3)
	seedN(t, n, baseSource, sref(10, 10), []*InternalMessage{old, recent, live})
	for seqno := uint32(11); seqno <= 10+2*maxSourceRefHistory; seqno++ {
		delta := &InternalsDelta{}
		if seqno == 11 {
			delta.RemovedKeys, delta.RemovedTotal = []QueueKey{old.Key}, 1
		}
		if seqno == 10+2*maxSourceRefHistory {
			delta.RemovedEnvHashes, delta.RemovedTotal = [][32]byte{recent.EnvHash}, 1
		}
		if err := n.ApplyBlock(baseSource, sref(seqno, byte(seqno)), delta); err != nil {
			t.Fatal(err)
		}
	}
	if stats := n.Stats(); stats.Removed != 1 {
		t.Fatalf("retained tombstones=%d, want only the recent removal", stats.Removed)
	}
	latest := uint32(10 + 2*maxSourceRefHistory)
	floor := latest - maxSourceRefHistory + 1
	for _, seqno := range []uint32{floor, latest - 1, latest} {
		cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
			baseSource: {Visible: sref(seqno, byte(seqno))},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if seqno < latest {
			requireLts(t, cut, 200, 300)
		} else {
			requireLts(t, cut, 300)
		}
	}
	if _, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(floor-1, byte(floor-1))},
	}}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("expired historical cut=%v, want ErrCutStale", err)
	}

	// Expiry shifts entry positions: both removal indexes must still work.
	if err := n.ApplyBlock(baseSource, sref(latest+1, byte(latest+1)), &InternalsDelta{
		RemovedKeys: []QueueKey{live.Key}, RemovedTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInternalsTombstoneExpiryPreservesPinnedBranchAndSnapshot(t *testing.T) {
	t.Parallel()
	pool, branch, base := branchFixture(t, 4)
	defer pool.Close()
	defer branch.Close()
	tip := sref(base.Seqno+1, 0xc1).RootHash
	if err := branch.AddCandidate(CandidateRequest{
		ID: tip, Seqno: base.Seqno + 1,
		Base: []CandidateSource{{Source: baseSource, Visible: base}}, Delta: &InternalsDelta{},
	}); err != nil {
		t.Fatal(err)
	}
	request := CutRequest{
		Sources: map[ShardIdent]CutSource{baseSource: {Visible: base}}, CandidateTip: &tip,
	}
	snapshot, err := branch.Cut(request)
	if err != nil {
		t.Fatal(err)
	}
	pagedRequest := request
	pagedRequest.Limit = 1
	paged, err := branch.Cut(pagedRequest)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < 2*maxSourceRefHistory; i++ {
			cut, err := branch.Cut(request)
			if err != nil {
				t.Error(err)
				return
			}
			if len(cut.Messages) != len(snapshot.Messages) {
				t.Error("feed expiry changed the pinned branch message count")
				return
			}
			for j, msg := range cut.Messages {
				if msg != snapshot.Messages[j] || msg.EnqueuedLT != uint64(1_000+j) {
					t.Error("feed expiry changed the immutable branch snapshot")
					return
				}
			}
		}
	})
	defer wg.Wait()
	for seqno := base.Seqno + 1; seqno <= base.Seqno+2*maxSourceRefHistory; seqno++ {
		delta := &InternalsDelta{}
		if seqno == base.Seqno+1 {
			delta.RemovedKeys, delta.RemovedTotal = []QueueKey{snapshot.Messages[0].Key}, 1
		}
		if err := pool.Internals().ApplyBlock(testOwner, baseSource, sref(seqno, byte(seqno)), delta); err != nil {
			t.Fatal(err)
		}
	}
	if stats := pool.Internals().Stats(); stats.Removed != 0 {
		t.Fatalf("feed still owns %d expired tombstones", stats.Removed)
	}
	requireLts(t, snapshot, 1_000, 1_001, 1_002, 1_003)
	for paged.More {
		if paged.LoadMore(2) == 0 {
			t.Fatal("feed expiry invalidated the previously acquired continuation")
		}
	}
	requireLts(t, paged, 1_000, 1_001, 1_002, 1_003)
}

func TestInternalsTombstoneExpiryBoundaryAndReenqueue(t *testing.T) {
	t.Parallel()
	n := testInternals(t)
	old := imsg(100, 1)
	atFloor := imsg(200, 2)
	afterFloor := imsg(300, 3)
	seedN(t, n, baseSource, sref(10, 10), []*InternalMessage{old, atFloor, afterFloor})
	floor := uint32(10 + maxSourceRefHistory)
	top := floor + maxSourceRefHistory - 1
	for seqno := uint32(11); seqno <= top; seqno++ {
		delta := &InternalsDelta{}
		switch seqno {
		case 11:
			delta.RemovedKeys, delta.RemovedTotal = []QueueKey{old.Key}, 1
		case 12:
			// Identical canonical identity, but a new source-block generation.
			delta.Added, delta.AddedTotal = []*InternalMessage{imsg(100, 1)}, 1
		case floor:
			delta.RemovedKeys, delta.RemovedTotal = []QueueKey{atFloor.Key}, 1
		case floor + 1:
			delta.RemovedKeys, delta.RemovedTotal = []QueueKey{afterFloor.Key}, 1
		}
		if err := n.ApplyBlock(baseSource, sref(seqno, byte(seqno)), delta); err != nil {
			t.Fatal(err)
		}
	}
	if stats := n.Stats(); stats.Removed != 1 || stats.Entries != 1 {
		t.Fatalf("after expiry removed=%d live=%d, want 1 and 1", stats.Removed, stats.Entries)
	}
	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(floor, byte(floor))},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 100, 300)
	if cut.Messages[0] == old || cut.Messages[0].SourceSeqno != 12 {
		t.Fatal("expiry confused re-enqueued message generations")
	}
	if err := n.ApplyBlock(baseSource, sref(top+1, byte(top+1)), &InternalsDelta{
		RemovedEnvHashes: [][32]byte{old.EnvHash}, RemovedTotal: 1,
	}); err != nil {
		t.Fatalf("remove re-enqueued message through relocated envelope index: %v", err)
	}
}

func BenchmarkInternalsTombstoneHistoryAdvance(b *testing.B) {
	for _, count := range []int{8_192, 65_536} {
		b.Run(fmt.Sprintf("live=%d", count), func(b *testing.B) {
			n := newDestinationState(testOwner)
			messages := make([]*InternalMessage, count)
			for i := range messages {
				messages[i] = imsg(uint64(i), uint16(i%60_000))
			}
			if err := n.Seed(baseSource, sref(10, 10), messages, uint64(count)); err != nil {
				b.Fatal(err)
			}
			seqno := uint32(10)
			b.ReportAllocs()
			for b.Loop() {
				seqno++
				if err := n.ApplyBlock(baseSource, sref(seqno, byte(seqno)), &InternalsDelta{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkInternalsTombstoneExpiry(b *testing.B) {
	for _, count := range []int{8_192, 65_536} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			n := newDestinationState(testOwner)
			messages := make([]*InternalMessage, count)
			for i := range messages {
				messages[i] = imsg(uint64(i), uint16(i%60_000))
			}
			if err := n.Seed(baseSource, sref(10, 10), messages, uint64(count)); err != nil {
				b.Fatal(err)
			}
			run := n.runs[baseSource]
			entries := append([]runEntry(nil), run.entries...)
			refs := make([]SourceRef, maxSourceRefHistory+1)
			for i := range refs {
				refs[i] = sref(uint32(10+maxSourceRefHistory-1+i), byte(i))
			}
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				run.entries = run.entries[:len(entries)]
				copy(run.entries, entries)
				run.refs = append(run.refs[:0], refs...)
				run.seedSeqno = 10
				run.lastExpiryFloor = 10
				run.removedCount = 1
				run.entries[0].removedAt = 11
				delete(run.byKey, messages[0].Key)
				delete(run.byEnv, messages[0].EnvHash)
				b.StartTimer()
				n.trimSourceHistoryLocked(run)
			}
			if run.removedCount != 0 || len(run.entries) != count-1 {
				b.Fatal("history sweep did not expire the benchmark tombstone")
			}
		})
	}
}
