package service

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

// Once the run is canceled the master stages must not hand anything more
// downstream: the commit stage is gone and the run's cell window is about to
// be released.
func TestNextSyncMasterStagesSendNothingAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &nextSyncRunner{
		service: &SyncCoordinator{log: zerolog.Nop(), status: newTestStatusTracker(nil, nil)},
		ctx:     ctx,
		cancel:  cancel,
		master:  &tnstore.BlockState{Block: testMasterBlockID(10)},
	}

	// A select with both a ready send and a closed Done picks at random, so a
	// single attempt would pass by chance half of the time.
	for i := 0; i < 64; i++ {
		downloads := make(chan nextMasterDownload, 1)
		downloads <- nextMasterDownload{err: errors.New("download failed")}
		close(downloads)
		if item, ok := <-r.startMasterApply(downloads); ok {
			t.Fatalf("canceled apply stage sent %v", item.err)
		}

		applied := make(chan nextAppliedMaster, 1)
		if r.sendAppliedMaster(applied, nextAppliedMaster{}) || len(applied) != 0 {
			t.Fatal("canceled apply stage sent an applied master")
		}

		out := make(chan nextMasterDownload, 1)
		if r.sendMasterDownload(out, nextMasterDownload{}) || len(out) != 0 {
			t.Fatal("canceled source stage sent a download")
		}
	}
}

// logMasterApplied runs on the apply goroutine while the commit goroutine
// replaces r.current without synchronization, so it must not read it. A nil
// commit head turns such a read into a panic instead of a race.
func TestLogMasterAppliedDoesNotReadCommitHead(t *testing.T) {
	var logged bytes.Buffer
	prev := testMasterBlockID(10)
	master := testMasterBlockID(11)
	r := &nextSyncRunner{
		service: &SyncCoordinator{
			log:  zerolog.New(&logged).Level(zerolog.DebugLevel),
			node: newServiceTestNode(t),
		},
		mode: nextSyncBootstrap,
	}

	r.logMasterApplied(nextAppliedMaster{
		prev:   prev,
		block:  testPreparedMasterchainBlock(prev, master),
		master: &tnstore.BlockState{Block: master},
	})

	want := `"latest_masterchain":"` + tnstore.FormatBlockRef(master) + `"`
	if !strings.Contains(logged.String(), want) {
		t.Fatalf("logged %s, want %s", logged.String(), want)
	}
}

// A taken entry leaves its key in order, and an older live entry keeps the
// prune from reaching it. Storing the block again must not let that stale
// position stand in for the new entry, or the entries behind it outlive their
// TTL.
func TestPreparedShardBlockCacheRestoredEntryKeepsTTLOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cache preparedShardBlockCache
		older := testPreparedShardBlockID(800)
		restored := testPreparedShardBlockID(801)
		expired := testPreparedShardBlockID(802)

		cache.storePrepared(PreparedBlock{ID: older, BlockBOC: []byte{1}})
		cache.storePrepared(PreparedBlock{ID: restored, BlockBOC: []byte{2}})
		if _, err := cache.take(restored); err != nil {
			t.Fatalf("take prepared block: %v", err)
		}
		cache.storePrepared(PreparedBlock{ID: expired, BlockBOC: []byte{3}})
		time.Sleep(2 * time.Minute)
		cache.storePrepared(PreparedBlock{ID: restored, BlockBOC: []byte{2}})
		time.Sleep(90 * time.Second)

		// any prepare reservation prunes
		cache.beginPrepare(testPreparedShardBlockID(803))

		cache.mu.Lock()
		_, olderPresent := cache.entries[tnstore.BlockKey(older)]
		_, expiredPresent := cache.entries[tnstore.BlockKey(expired)]
		_, restoredPresent := cache.entries[tnstore.BlockKey(restored)]
		cache.mu.Unlock()
		if olderPresent || expiredPresent {
			t.Fatal("expired entry outlived its TTL behind a stale order position")
		}
		if !restoredPresent {
			t.Fatal("restored entry was pruned before its TTL")
		}
	})
}

// Over the item limit the oldest entry goes, not a block stored again after
// its earlier entry was taken.
func TestPreparedShardBlockCacheRestoredEntryIsNotEvictedFirst(t *testing.T) {
	var cache preparedShardBlockCache
	older := testPreparedShardBlockID(preparedShardBlockMaxItems + 1000)
	restored := testPreparedShardBlockID(preparedShardBlockMaxItems + 1001)
	newest := testPreparedShardBlockID(preparedShardBlockMaxItems + 1002)

	cache.storePrepared(PreparedBlock{ID: older, BlockBOC: []byte{1}})
	cache.storePrepared(PreparedBlock{ID: restored, BlockBOC: []byte{2}})
	if _, err := cache.take(restored); err != nil {
		t.Fatalf("take prepared block: %v", err)
	}
	for i := uint32(0); i < preparedShardBlockMaxItems-1; i++ {
		cache.storePrepared(PreparedBlock{ID: testPreparedShardBlockID(i), BlockBOC: []byte{byte(i)}})
	}
	// the cache is full: storing again evicts older, storing newest evicts the
	// next oldest live entry
	cache.storePrepared(PreparedBlock{ID: restored, BlockBOC: []byte{2}})
	cache.storePrepared(PreparedBlock{ID: newest, BlockBOC: []byte{3}})

	if _, err := cache.take(restored); err != nil {
		t.Fatal("restored entry evicted instead of the oldest one")
	}
	if _, err := cache.take(testPreparedShardBlockID(0)); err == nil {
		t.Fatal("oldest entry survived eviction")
	}
}

func testMasterchainQueueCandidate(prev, block ton.BlockIDExt) VerifiedBlock {
	candidate := testVerifiedMasterchainBlock(prev, block)
	candidate.Kind = "tonNode.blockBroadcast"
	candidate.BlockBOC = []byte{1}
	candidate.ProofBOC = []byte{2}
	candidate.consensus = &masterchainConsensusProof{
		block:   block,
		prevRef: prev,
	}
	return candidate
}

// A queued master the pipeline applied through another source is never taken.
// Once the committed head passes it, it must stop anchoring the queue window,
// or every block nextMasterchainQueueLimit masters later is refused until the
// stale entry's TTL.
func TestMasterchainQueuePrunesEntriesBehindCommittedHead(t *testing.T) {
	svc := newMasterchainQueueTestService()
	publishHead := func(seqno uint32) {
		svc.status.current = &tnstore.CurrentState{
			Masterchain: tnstore.BlockState{Block: testMasterBlockID(seqno)},
		}
	}

	publishHead(90)
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(testMasterBlockID(90), testMasterBlockID(91)), p2p.PeerID{})
	svc.queueMasterchainBroadcastCandidateFromSource(testMasterchainQueueCandidate(testMasterBlockID(91), testMasterBlockID(92)), p2p.PeerID{})

	head := uint32(90 + nextMasterchainQueueLimit)
	publishHead(head)

	// behind the head: it can never be taken, so it must not be queued
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(testMasterBlockID(head-1), testMasterBlockID(head)), p2p.PeerID{})

	next := testMasterBlockID(head + 1)
	candidateNext := testMasterBlockID(head + 2)
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(testMasterBlockID(head), next), p2p.PeerID{})
	svc.queueMasterchainBroadcastCandidateFromSource(testMasterchainQueueCandidate(next, candidateNext), p2p.PeerID{})

	svc.nextMasterchainMx.Lock()
	items := svc.queuedMasterchainItemsLocked()
	svc.nextMasterchainMx.Unlock()
	if items != 2 {
		t.Fatalf("queued items = %d, want only the 2 entries at the head", items)
	}

	if got, err := svc.takeQueuedMasterchainBlock(testMasterBlockID(head), next); err != nil || !got.ID.Equals(&next) {
		t.Fatalf("block at the head was not queued: err=%v", err)
	}
	if _, err := svc.peekQueuedMasterchainCandidate(next, candidateNext); err != nil {
		t.Fatalf("candidate at the head was not queued: %v", err)
	}
}
