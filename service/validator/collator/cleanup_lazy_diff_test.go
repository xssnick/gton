package collator

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// This file replaces cleanup_partition_test.go. It holds the pre-change eager
// cleanup verbatim, as a test-only
// reference, plus the differential that pins the lazy streams to it. The
// reference must never be deleted: it is the only thing that can still answer
// "does the stream dequeue in exactly the order the sort produced".

// referenceCleanupPart is the pre-change cleanupPart: a materialised candidate
// slice and a cursor into it.
type referenceCleanupPart struct {
	neighbor   Neighbor
	candidates []dequeueCandidate
	next       int
}

// partitionQueueCandidates buckets one canonical scan by next hop. The
// canonical order is a total order on queue keys, so a neighbor's slice of the
// global order is exactly the order a scan restricted to that neighbor would
// have produced. An entry no neighbor covers is dropped: it is not deliverable
// to anyone this block can clean up after.
//
// The correctness of the lazy replacement rests on validateNeighbors rejecting
// intersecting neighbor shards, which turns this "first match wins" into "the
// unique match": a prefix-restricted stream yields exactly the keys one bucket
// received. Deleting that check would silently change block bytes.
func partitionQueueCandidates(parts []referenceCleanupPart, candidates []dequeueCandidate) {
	for i := range candidates {
		for j := range parts {
			if parts[j].neighbor.Shard.ContainsPrefix(candidates[i].key.NextHop()) {
				parts[j].candidates = append(parts[j].candidates, candidates[i])
				break
			}
		}
	}
}

// cleanupOutQueueEager is cleanupOutQueue as it stood before the lazy streams.
// It shares dequeueDelivered, flushQueueDeletes and the limit machinery with
// the production path and differs only in enumeration, which is what makes the
// differential below a test of enumeration alone.
func (c *collation) cleanupOutQueueEager() error {
	if len(c.req.neighbors) == 0 {
		return nil
	}
	if c.config.capabilities&capShortDequeue == 0 {
		return fmt.Errorf("%w: queue cleanup requires the short dequeue capability", ErrUnsupported)
	}
	neighbors, err := c.effectiveNeighbors()
	if err != nil {
		return err
	}
	if c.topology.kind == topologyAfterMerge {
		return c.cleanupMergedOutQueue(neighbors)
	}

	parts := make([]referenceCleanupPart, len(neighbors))
	for i := range neighbors {
		parts[i].neighbor = neighbors[i]
	}
	candidates, err := c.queueCandidates()
	if err != nil {
		return err
	}
	partitionQueueCandidates(parts, candidates)

	for i := 0; len(parts) > 0; {
		if c.blockFull {
			break
		}
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if i == len(parts) {
			i = 0
		}

		part := &parts[i]
		if part.next == len(part.candidates) {
			parts[i] = parts[len(parts)-1]
			parts = parts[:len(parts)-1]
			continue
		}

		entry, err := c.loadQueueEntry(part.candidates[part.next].key)
		if err != nil {
			return err
		}
		processed, err := c.shardEndLT.alreadyProcessed(
			part.neighbor.Processed,
			part.neighbor.Shard.Workchain,
			part.neighbor.Shard.Shard,
			&entry.descr,
		)
		if err != nil {
			return fmt.Errorf("%w: check neighbor processed info: %v", ErrInvalidInput, err)
		}
		if !processed {
			parts[i] = parts[len(parts)-1]
			parts = parts[:len(parts)-1]
			continue
		}
		if err = c.dequeueDelivered(entry, part.neighbor.EndLT); err != nil {
			return err
		}
		c.stats.QueueCleaned++
		part.next++
		i++
	}
	if err = c.flushQueueDeletes(); err != nil {
		return err
	}
	return c.limits.addProof(c.outQueue.RootCell())
}

// buildShardEagerCleanup mirrors buildShardAttemptPaced with the eager cleanup
// substituted for the lazy one. Every other phase, and finish, is the
// production code.
func buildShardEagerCleanup(b *Builder, ctx context.Context, req ShardRequest) (*Candidate, error) {
	common := shardCollationRequest(req)
	if err := validateCollationRequest(&common); err != nil {
		return nil, err
	}
	return retryUnderSizeLimit(func(narrowing sizeBudgetCap) (*Candidate, error) {
		c, err := b.prepare(ctx, req)
		if err != nil {
			return nil, err
		}
		c.limits.narrowBytes(narrowing)
		if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
			return nil, err
		}
		if err = c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
			return nil, err
		}
		if err = c.cleanupOutQueueEager(); err != nil {
			return nil, err
		}
		if err = c.processDispatchQueue(); err != nil {
			return nil, err
		}
		if err = c.processInternals(); err != nil {
			return nil, err
		}
		if err = c.processExternals(); err != nil {
			return nil, err
		}
		if err = c.processNewMessages(
			c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete(),
		); err != nil {
			return nil, err
		}
		return c.finishShard()
	})
}

// multiNeighborQueueRequest builds a predecessor whose out queue is split
// between two effective cleanup parts — the masterchain neighbor and the
// predecessor's own shard, which normalizeTrivialNeighbors always appends — and
// packs the masterchain part with lt ties so the 256-bit suffix tie-break, not
// the lt, decides the dequeue order.
//
// The own-shard part is deliberately left undeliverable: it exercises the
// stop-at-first-undelivered rule, and its stream must be dropped after one pull
// rather than walked to the end.
func multiNeighborQueueRequest(t *testing.T, masterBound, shardBound, foreignBound int, full bool) ShardRequest {
	t.Helper()

	req := emptyCandidateRequest(t)
	startLT := requestStartLT(t, req)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc1}, 32))
	fee := tlb.FromNanoTONU(100_000)

	enqueue := func(destination *address.Address, queueLT uint64) {
		msg, _ := queuedInternal(t, source, destination,
			queueLT, req.Header.GenUtime-1, fee, fee, 0,
			msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
		enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: queueLT, Msg: msg.EnvelopeCell}).ToCell()
		if err != nil {
			t.Fatal(err)
		}
		req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msg.Key, enqueued)
	}

	for i := range masterBound {
		var id [32]byte
		id[0], id[1], id[31] = 0xd0, byte(i>>8), byte(i)
		// Every pair of consecutive entries shares one lt: at equal rank the
		// stream must expand forks before emitting, and then order by suffix.
		enqueue(address.NewAddress(0, 0xff, id[:]), startLT-2000+uint64(i/2)*2)
	}
	for i := range shardBound {
		var id [32]byte
		id[0], id[1], id[31] = 0x11, byte(i>>8), byte(i)
		enqueue(address.NewAddress(0, 0, id[:]), startLT-1000+uint64(i))
	}
	for i := range foreignBound {
		var id [32]byte
		id[0], id[1], id[31] = 0x22, byte(i>>8), byte(i)
		// Workchain 1: no neighbor covers it, and neither does our own shard,
		// so no stream may ever yield these keys.
		enqueue(address.NewAddress(0, 0x01, id[:]), startLT-500+uint64(i))
	}
	size := uint64(masterBound + shardBound + foreignBound)
	req.Previous.OutQueueSize = &size

	covering := tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   startLT,
		LastMsgHash: processedInfinityHash,
	}
	if !full {
		req.Neighbors = masterchainNeighbor(req, covering)
		req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return startLT }
		return req
	}

	// Full collated data needs a real predecessor block root, so run one plain
	// block over the fixture first — with no neighbors, so nothing is cleaned
	// up — and collate the second one with the capability on.
	//
	// The neighbor frontiers here are the authenticated ones — full collated
	// data cross-checks them against the verified masterchain context, so they
	// cannot be widened to make entries deliverable. That is exactly the
	// deliverable-nothing column: the eager scan still walks the whole trie and
	// still ships it, the lazy streams open one prefix path each.
	_ = covering
	req = advanceCandidateRequestApplyingUpdate(t, req)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)
	return req
}

// TestNeighborQueueStreamPreservesPerNeighborOrder is the order-isolation gate:
// with no block machinery in the way, each neighbor's lazy stream must yield
// exactly the bucket the eager scan-and-sort produced for it, element for
// element.
func TestNeighborQueueStreamPreservesPerNeighborOrder(t *testing.T) {
	req := multiNeighborQueueRequest(t, 18, 6, 0, false)
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	neighbors, err := c.effectiveNeighbors()
	if err != nil {
		t.Fatal(err)
	}

	reference := make([]referenceCleanupPart, len(neighbors))
	for i := range neighbors {
		reference[i].neighbor = neighbors[i]
	}
	candidates, err := c.queueCandidates()
	if err != nil {
		t.Fatal(err)
	}
	partitionQueueCandidates(reference, candidates)

	covered := 0
	for i := range neighbors {
		stream, streamErr := c.neighborQueueStream(neighbors[i].Shard)
		if streamErr != nil {
			t.Fatal(streamErr)
		}
		var got []dequeueCandidate
		for stream.Next() {
			view := stream.View()
			var key msgpool.QueueKey
			if err = view.Key.LoadSliceInto(key[:], 352); err != nil {
				t.Fatal(err)
			}
			got = append(got, dequeueCandidate{lt: stream.Rank(), key: key})
		}
		if err = stream.Err(); err != nil {
			t.Fatal(err)
		}
		assertSameCleanupOrder(t, i, got, reference[i].candidates)
		covered += len(got)
	}
	if covered != len(candidates) {
		t.Fatalf("streams covered %d of %d queue entries", covered, len(candidates))
	}
	if covered == 0 {
		t.Fatal("fixture produced no queue entries")
	}
}

// assertSameCleanupOrder compares a stream against the eager scan-and-sort
// without assuming the canonical order is total, because it is not.
//
// The comparator is (enqueued_lt, key[12:]) — the lt and the 256-bit message
// hash — while the key also carries the 96-bit next hop the comparator ignores.
// Two entries of one shard with the same lt and the same message hash but
// different next hops therefore TIE, and neither queueCandidates' sort.Slice
// (unstable) nor C++'s std::sort pins which comes first. An element-for-element
// comparison of the full entries would fail such a fixture spuriously, and
// would be asserting an order the reference does not have.
//
// So what is compared is what the order actually determines: the ranks, element
// for element — those are equal for tied entries by definition of the tie — and
// the entries themselves as a multiset. A stream that reordered anything the
// comparator does distinguish moves a rank and fails the first check; one that
// dropped, duplicated or invented an entry fails the second.
func assertSameCleanupOrder(t *testing.T, neighbor int, got, want []dequeueCandidate) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("neighbor %d streamed %d entries, want %d", neighbor, len(got), len(want))
	}
	type rank struct {
		lt   uint64
		hash [32]byte
	}
	rankOf := func(c dequeueCandidate) rank {
		var r rank
		r.lt = c.lt
		copy(r.hash[:], c.key[12:])
		return r
	}
	for j := range want {
		if rankOf(got[j]) != rankOf(want[j]) {
			t.Fatalf("neighbor %d entry %d: got (lt=%d key=%x), want (lt=%d key=%x)",
				neighbor, j, got[j].lt, got[j].key, want[j].lt, want[j].key)
		}
		if j > 0 && rankOf(got[j-1]).lt > rankOf(got[j]).lt {
			t.Fatalf("neighbor %d is not monotone at entry %d: lt %d follows %d",
				neighbor, j, got[j].lt, got[j-1].lt)
		}
	}
	counts := map[dequeueCandidate]int{}
	for _, entry := range want {
		counts[entry]++
	}
	for _, entry := range got {
		counts[entry]--
	}
	for entry, n := range counts {
		if n != 0 {
			t.Fatalf("neighbor %d: entry (lt=%d key=%x) appears %d times more in the eager scan",
				neighbor, entry.lt, entry.key, n)
		}
	}
}

// TestNeighborQueueStreamSkipsUncoveredNextHops keeps the drop rule: an entry
// whose next hop no effective neighbor covers is reached by no stream at all,
// rather than falling into the first one the way a linear probe could.
func TestNeighborQueueStreamSkipsUncoveredNextHops(t *testing.T) {
	const foreign = 5
	req := multiNeighborQueueRequest(t, 8, 4, foreign, false)
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	neighbors, err := c.effectiveNeighbors()
	if err != nil {
		t.Fatal(err)
	}
	streamed := 0
	for i := range neighbors {
		stream, streamErr := c.neighborQueueStream(neighbors[i].Shard)
		if streamErr != nil {
			t.Fatal(streamErr)
		}
		for stream.Next() {
			view := stream.View()
			var key msgpool.QueueKey
			if err = view.Key.LoadSliceInto(key[:], 352); err != nil {
				t.Fatal(err)
			}
			if !neighbors[i].Shard.ContainsPrefix(key.NextHop()) {
				t.Fatalf("neighbor %d stream yielded key %x outside its prefix", i, key)
			}
			if key.NextHop().Workchain == 1 {
				t.Fatalf("a workchain-1 next hop no neighbor covers was streamed: %x", key)
			}
			streamed++
		}
		if err = stream.Err(); err != nil {
			t.Fatal(err)
		}
	}
	total, err := c.queueCandidates()
	if err != nil {
		t.Fatal(err)
	}
	if streamed != len(total)-foreign {
		t.Fatalf("streams covered %d entries, want %d of %d", streamed, len(total)-foreign, len(total))
	}
}

// TestLazyQueueCleanupMatchesEagerCandidate is the end-to-end differential: the
// same request built twice inside one binary, once through the eager reference
// and once through the production streams. The dequeue sequence, the block
// hash and the reported queue size must be identical.
func TestLazyQueueCleanupMatchesEagerCandidate(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "plain"
		if full {
			name = "full-collated"
		}
		t.Run(name, func(t *testing.T) {
			lazyReq := multiNeighborQueueRequest(t, 140, 20, 8, full)
			eagerReq := multiNeighborQueueRequest(t, 140, 20, 8, full)

			lazy, err := testBuilder().BuildShard(context.Background(), lazyReq)
			if err != nil {
				t.Fatalf("lazy build: %v", err)
			}
			eager, err := buildShardEagerCleanup(testBuilder(), context.Background(), eagerReq)
			if err != nil {
				t.Fatalf("eager build: %v", err)
			}

			if !full && lazy.Stats.QueueCleaned == 0 {
				t.Fatal("fixture dequeued nothing; the dequeue differential proves nothing")
			}
			if lazy.Stats.QueueCleaned != eager.Stats.QueueCleaned {
				t.Fatalf("dequeued %d entries, eager dequeued %d",
					lazy.Stats.QueueCleaned, eager.Stats.QueueCleaned)
			}
			if lazy.Stats.OutQueueSize != eager.Stats.OutQueueSize {
				t.Fatalf("out queue size %d, eager %d", lazy.Stats.OutQueueSize, eager.Stats.OutQueueSize)
			}
			if !bytes.Equal(lazy.ID.RootHash, eager.ID.RootHash) {
				t.Fatalf("block root hash %x, eager %x", lazy.ID.RootHash, eager.ID.RootHash)
			}
			if !bytes.Equal(lazy.BlockBOC, eager.BlockBOC) {
				t.Fatal("block BOC differs between the lazy and eager cleanup")
			}
			// The ordered dequeue sequence is observable in OutMsgDescr: the
			// descriptors are keyed by message hash, so a different order would
			// only show as a different block hash. Compare it explicitly so a
			// failure names the divergence instead of just a hash mismatch.
			gv := lazyReq.Masterchain.Config.globalVersion
			if got, want := dequeuedEnvelopeHashes(t, lazy, gv), dequeuedEnvelopeHashes(t, eager, gv); !equalHashLists(got, want) {
				t.Fatalf("dequeue set differs:\n lazy: %x\neager: %x", got, want)
			}

			if full {
				// Going lazy is what shrinks the shipped proof: the eager scan
				// dragged the whole out-queue trie into collated data.
				if len(lazy.CollatedData) >= len(eager.CollatedData) {
					t.Fatalf("lazy collated data %d bytes is not smaller than eager %d",
						len(lazy.CollatedData), len(eager.CollatedData))
				}
				if _, err = cell.FromBOCMultiRoot(lazy.CollatedData); err != nil {
					t.Fatalf("lazy collated data does not open: %v", err)
				}
				t.Logf("collated data: lazy %d B, eager %d B (%.1f%% smaller)",
					len(lazy.CollatedData), len(eager.CollatedData),
					100*(1-float64(len(lazy.CollatedData))/float64(len(eager.CollatedData))))
			}
		})
	}
}

func dequeuedEnvelopeHashes(t *testing.T, candidate *Candidate, globalVersion uint32) [][]byte {
	t.Helper()

	_, outMessages := candidateMessageDescriptors(t, candidate, globalVersion)
	var hashes [][]byte
	err := outMessages.ForEachBorrowed(false, false, func(item cell.AugDictItemView) error {
		tag, tagErr := item.Value.Copy().LoadUInt(4)
		if tagErr != nil {
			return tagErr
		}
		if tag != 0b1101 {
			return nil
		}
		var hash [32]byte
		if keyErr := item.Key.Copy().LoadSliceInto(hash[:], 256); keyErr != nil {
			return keyErr
		}
		hashes = append(hashes, hash[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hashes
}

func equalHashLists(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// replayProfile is a full-collated block that actually dequeues: the shipped
// predecessor proof is selected by the read set, so lazy cleanup ships strictly
// fewer out-queue cells than the eager scan did.
var replayProfile = benchProfile{
	name: "replay-consistency", accounts: 2_000, queued: 800, fullCollated: true,
}

// TestLazyCleanupProofStillReplaysCleanup is the replay-consistency gate. Going
// lazy shrinks CollatedData, so the question that decides whether this change is
// safe at all is whether a validator replaying the block still finds every cell
// it needs in the smaller proof.
//
// It does, and the reason is structural rather than incidental: our validator
// never enumerates the predecessor out queue on an ordinary block. Cleanup is
// replayed descriptor by descriptor — semanticQueueValidation.dequeue does a
// point LoadValueAndDelete on the key the msg_export_deq_short descriptor names
// — so the cells it reads are the root-to-leaf paths of exactly the entries the
// collator dequeued, plus the siblings the same deletes rebuild. The lazy
// collator opened every one of those paths, because it emitted those leaves.
// The predecessor queue size comes from the stored Extra/supplied value
// (existingQueueSize), never from a Count. C++ matches: check_delivered_dequeued
// is the only enumerating check and ValidateQuery calls it under after_merge_
// only, and the after-merge path here keeps the eager scan.
//
// The test proves it the hard way: the resident predecessor is narrowed to a
// root-only proof, so anything the replay reads must have come out of the
// candidate's own collated data.
func TestLazyCleanupProofStillReplaysCleanup(t *testing.T) {
	req := benchRequest(t, replayProfile)
	cleanupStarted := time.Now()
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate: %v", err)
	}
	cleanupWall := time.Since(cleanupStarted)
	if candidate.Stats.QueueCleaned == 0 {
		t.Fatal("fixture dequeued nothing; the replay would not exercise cleanup")
	}

	verification := ShardVerificationRequest{
		Previous:           req.Previous,
		Masterchain:        req.Masterchain,
		Neighbors:          collatedNeighborQueues(t, req, candidate),
		NeighborShardEndLT: req.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          candidate,
	}
	verification.Previous.State = narrowedStateRoot(t, req.Previous.State)
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("lazily collated candidate does not replay on its own proofs: %v", err)
	}

	// The control: the same block built through the eager scan must also
	// replay, and must ship a strictly larger proof. Without it a shrink that
	// happened to break nothing would be indistinguishable from no shrink.
	eagerReq := benchRequest(t, replayProfile)
	eager, err := buildShardEagerCleanup(testBuilder(), context.Background(), eagerReq)
	if err != nil {
		t.Fatalf("eager collate: %v", err)
	}
	if !bytes.Equal(candidate.BlockBOC, eager.BlockBOC) {
		t.Fatal("lazy and eager cleanup produced different blocks")
	}
	// When cleanup drains the queue completely the two paths read the same
	// cells and the proof is byte-identical: laziness only saves what is never
	// consumed. The saving is real exactly where the eager scan was wasteful —
	// when cleanup stops early — which is the second half of this test.
	if len(candidate.CollatedData) > len(eager.CollatedData) {
		t.Fatalf("lazy collated data %d bytes is larger than eager %d",
			len(candidate.CollatedData), len(eager.CollatedData))
	}
	t.Logf("full drain: dequeued %d entries, collated data lazy %d B, eager %d B",
		candidate.Stats.QueueCleaned, len(candidate.CollatedData), len(eager.CollatedData))

	// Now the case the change is for: cleanup stops before consuming the queue.
	// The lazy proof must shrink and the truncated block must still replay on
	// its own — smaller — collated data.
	stoppedReq := benchRequest(t, replayProfile)
	stoppedReq.QueueCleanupUntil = time.Now().Add(-time.Hour)
	stopped, err := testBuilder().BuildShard(context.Background(), stoppedReq)
	if err != nil {
		t.Fatalf("collate with an expired budget: %v", err)
	}
	if stopped.Stats.QueueCleaned != 0 {
		t.Fatalf("expired budget still dequeued %d entries", stopped.Stats.QueueCleaned)
	}
	if len(stopped.CollatedData) >= len(eager.CollatedData) {
		t.Fatalf("a truncated lazy cleanup shipped %d bytes, no less than the eager scan's %d",
			len(stopped.CollatedData), len(eager.CollatedData))
	}
	stoppedVerification := ShardVerificationRequest{
		Previous:           stoppedReq.Previous,
		Masterchain:        stoppedReq.Masterchain,
		Neighbors:          collatedNeighborQueues(t, stoppedReq, stopped),
		NeighborShardEndLT: stoppedReq.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          stopped,
	}
	stoppedVerification.Previous.State = narrowedStateRoot(t, stoppedReq.Previous.State)
	if err = VerifyShardCandidate(context.Background(), stoppedVerification); err != nil {
		t.Fatalf("a truncated candidate does not replay on its own proofs: %v", err)
	}
	t.Logf("nothing deliverable: collated data lazy %d B, eager %d B (%.1f%% smaller)",
		len(stopped.CollatedData), len(eager.CollatedData),
		100*(1-float64(len(stopped.CollatedData))/float64(len(eager.CollatedData))))

	// The mixed case, and the sharpest one: cleanup dequeued some entries and
	// then stopped, so the proof carries the paths of what was consumed and
	// nothing of what was not. Replay must still find every cell it needs.
	for _, share := range []int{4, 16, 64, 256} {
		partialReq := benchRequest(t, replayProfile)
		partial, buildErr := buildShardBudgetedCleanup(
			testBuilder(), context.Background(), partialReq, cleanupWall/time.Duration(share),
		)
		if buildErr != nil {
			t.Fatalf("budgeted full-collated build: %v", buildErr)
		}
		if partial.Stats.QueueCleanupStop != CleanupStopBudget ||
			partial.Stats.QueueCleaned == 0 ||
			partial.Stats.QueueCleaned >= candidate.Stats.QueueCleaned {
			continue
		}
		partialVerification := ShardVerificationRequest{
			Previous:           partialReq.Previous,
			Masterchain:        partialReq.Masterchain,
			Neighbors:          collatedNeighborQueues(t, partialReq, partial),
			NeighborShardEndLT: partialReq.NeighborShardEndLT,
			Semantics:          NewSemanticVerifier(tvm.NewTVM()),
			Candidate:          partial,
		}
		partialVerification.Previous.State = narrowedStateRoot(t, partialReq.Previous.State)
		if err = VerifyShardCandidate(context.Background(), partialVerification); err != nil {
			t.Fatalf("a partially cleaned candidate does not replay on its own proofs: %v", err)
		}
		t.Logf("partial drain: dequeued %d of %d, collated data %d B",
			partial.Stats.QueueCleaned, candidate.Stats.QueueCleaned, len(partial.CollatedData))
		return
	}
	t.Log("cleanup never stopped partway on this machine; the partial-proof case was not exercised")
}

// deepQueueProfile is the scaling fixture: a predecessor whose only interesting
// property is the depth of its outbound queue.
func deepQueueProfile(queued int) benchProfile {
	return benchProfile{name: fmt.Sprintf("deep-queue-%d", queued), accounts: 2_000, queued: queued}
}

// BenchmarkQueueCleanup measures the cleanup phase alone, lazy against the
// eager reference, at two queue depths and both extremes of deliverability.
//
// The deliverable=none column is the cliff: it is the case where the eager scan
// pays O(Q log Q) and reads the whole trie for nothing, and it is the number to
// quote before and after.
func BenchmarkQueueCleanup(b *testing.B) {
	for _, queued := range []int{10_000, 100_000} {
		profile := deepQueueProfile(queued)
		for _, deliverable := range []bool{true, false} {
			label := "deliverable-all"
			if !deliverable {
				label = "deliverable-none"
			}
			req := benchRequest(b, profile)
			if !deliverable {
				// An empty frontier makes every entry undelivered, so each
				// stream is dropped at its first pull.
				for i := range req.Neighbors {
					req.Neighbors[i].Processed = nil
				}
			}
			for _, lazy := range []bool{true, false} {
				mode := "lazy"
				if !lazy {
					mode = "eager"
				}
				b.Run(fmt.Sprintf("%d/%s/%s", queued, label, mode), func(b *testing.B) {
					builder := testBuilder()
					b.ReportAllocs()
					for b.Loop() {
						b.StopTimer()
						c, err := builder.prepare(context.Background(), req)
						if err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
						if lazy {
							err = c.cleanupOutQueue()
						} else {
							err = c.cleanupOutQueueEager()
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
