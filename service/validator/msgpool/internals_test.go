package msgpool

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

var testOwner = ShardIdent{Workchain: 0, Shard: ShardAll}

func testInternals(t *testing.T) *destinationState {
	t.Helper()
	return newDestinationState(testOwner)
}

// imsg fabricates a prepared internal message: the core section never
// parses cells, so tiny stand-ins are enough. The lt and tag make the
// (lt, hash) order and identities unique.
func imsg(lt uint64, tag uint16) *InternalMessage {
	root := cell.BeginCell().MustStoreUInt(uint64(tag), 16).MustStoreUInt(lt, 64).EndCell()
	env := cell.BeginCell().MustStoreUInt(0xe0+uint64(tag), 16).MustStoreRef(root).EndCell()
	hop := AccountPrefix{Workchain: 0, Prefix: 0x8000000000000000 | uint64(tag)<<40}
	return &InternalMessage{
		Key:          MakeQueueKey(hop, root.HashKey()),
		EnqueuedLT:   lt,
		EnvHash:      env.HashKey(),
		EnvelopeCell: env,
		Root:         root,
	}
}

func sref(seqno uint32, tag byte) SourceRef {
	ref := SourceRef{Seqno: seqno}
	for i := range ref.RootHash {
		ref.RootHash[i] = tag
	}
	return ref
}

var (
	baseSource = ShardIdent{Workchain: 0, Shard: ShardAll}
	mcSource   = ShardIdent{Workchain: -1, Shard: ShardAll}
)

// seedN seeds with the message count as the full queue size.
func seedN(t *testing.T, n *destinationState, source ShardIdent, top SourceRef, msgs []*InternalMessage) {
	t.Helper()
	if err := n.Seed(source, top, msgs, uint64(len(msgs))); err != nil {
		t.Fatal(err)
	}
}

func cutLts(t *testing.T, cut *Cut) []uint64 {
	t.Helper()
	lts := make([]uint64, len(cut.Messages))
	for i, msg := range cut.Messages {
		lts[i] = msg.EnqueuedLT
	}
	return lts
}

func requireLts(t *testing.T, cut *Cut, want ...uint64) {
	t.Helper()
	got := cutLts(t, cut)
	if len(got) != len(want) {
		t.Fatalf("cut lts = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cut lts = %v, want %v", got, want)
		}
	}
}

func TestInternalsSeedAndCutMergeOrder(t *testing.T) {
	n := testInternals(t)

	// Two sources with interleaved lts: the cut must produce one globally
	// (lt, hash)-ordered stream.
	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(100, 1), imsg(300, 2), imsg(500, 3)})
	seedN(t, n, mcSource, sref(40, 0xbb), []*InternalMessage{imsg(200, 4), imsg(400, 5)})

	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(10, 0xaa)},
		mcSource:   {Visible: sref(40, 0xbb)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 100, 200, 300, 400, 500)
	if cut.More {
		t.Fatal("cut must be complete")
	}

	// A limit cuts the merged stream short and reports the remainder.
	cut, err = n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(10, 0xaa)},
		mcSource:   {Visible: sref(40, 0xbb)},
	}, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 100, 200, 300)
	if !cut.More {
		t.Fatal("limit must mark the cut as partial")
	}

	// Messages remembered their source.
	full, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(10, 0xaa)},
		mcSource:   {Visible: sref(40, 0xbb)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if full.Messages[1].Source != mcSource || full.Messages[0].Source != baseSource {
		t.Fatal("messages must carry their source")
	}

	// A source missing from the request contributes nothing.
	cut, err = n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		mcSource: {Visible: sref(40, 0xbb)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 200, 400)
}

func TestInternalsCutSharesReadOnlyMessages(t *testing.T) {
	n := testInternals(t)
	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(100, 1)})
	req := CutRequest{Sources: map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}}}

	first, err := n.Cut(req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := n.Cut(req)
	if err != nil {
		t.Fatal(err)
	}
	// Messages are immutable after publication and shared without copies:
	// both cuts hand out the very same objects. Consumers are read-only by
	// contract.
	if first.Messages[0] != second.Messages[0] {
		t.Fatal("cut messages must be shared, not copied")
	}
}

func TestInternalsApplyBlockAndRemovals(t *testing.T) {
	n := testInternals(t)

	seeded := []*InternalMessage{imsg(100, 1), imsg(200, 2)}
	seedN(t, n, baseSource, sref(10, 0xaa), seeded)

	// Block 11 removes one message by key (deq_imm), one by envelope hash
	// (deq_short) and enqueues two more.
	added := []*InternalMessage{imsg(1100, 3), imsg(1101, 4)}
	err := n.ApplyBlock(baseSource, sref(11, 0xab), &InternalsDelta{
		Added:            added,
		RemovedKeys:      []QueueKey{seeded[0].Key},
		RemovedEnvHashes: [][32]byte{seeded[1].EnvHash},
		AddedTotal:       2,
		RemovedTotal:     2,
	})
	if err != nil {
		t.Fatal(err)
	}

	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(11, 0xab)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1100, 1101)

	st := n.Stats()
	if st.AppliedRemoved != 2 || st.AppliedAdded != 2 {
		t.Fatalf("stats = %+v", st)
	}

	// A cut pinned to the pre-block position reconstructs the queue before
	// the block's removals and additions.
	cut, err = n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(10, 0xaa)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 100, 200)

	// A removal of an untracked entry is a disorder: the seed is the exact
	// snapshot, so a miss means the view diverged.
	err = n.ApplyBlock(baseSource, sref(12, 0xac), &InternalsDelta{
		RemovedKeys:  []QueueKey{imsg(999, 99).Key},
		RemovedTotal: 1,
	})
	if !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("removal miss = %v", err)
	}
	if _, err = n.SourceTop(baseSource); !errors.Is(err, ErrNotFound) {
		t.Fatal("removal miss must untrack the source")
	}
}

func TestInternalsApplyBlockMergesTransitByCanonicalLT(t *testing.T) {
	n := testInternals(t)
	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(2000, 2), imsg(4000, 4)})

	if err := n.ApplyBlock(baseSource, sref(11, 0xab), &InternalsDelta{
		Added:      []*InternalMessage{imsg(1000, 1), imsg(3000, 3)},
		AddedTotal: 2,
	}); err != nil {
		t.Fatal(err)
	}
	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(11, 0xab)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1000, 2000, 3000, 4000)
	if size, err := n.queueSize(baseSource); err != nil || size != 4 {
		t.Fatalf("queue size = %d (%v), want 4", size, err)
	}
}

func TestInternalsQueueSizeTracking(t *testing.T) {
	n := testInternals(t)

	// The seed total counts every destination, not only the owner view.
	if err := n.Seed(baseSource, sref(10, 0xaa), []*InternalMessage{imsg(100, 1)}, 5); err != nil {
		t.Fatal(err)
	}
	if size, err := n.queueSize(baseSource); err != nil || size != 5 {
		t.Fatalf("seeded queue size = %d %v", size, err)
	}

	if err := n.ApplyBlock(baseSource, sref(11, 0xab), &InternalsDelta{
		Added:      []*InternalMessage{imsg(1100, 2)},
		AddedTotal: 3, RemovedTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if size, _ := n.queueSize(baseSource); size != 7 {
		t.Fatalf("queue size after delta = %d", size)
	}

	// A delta driving the size negative is a disorder and untracks the
	// source.
	err := n.ApplyBlock(baseSource, sref(12, 0xac), &InternalsDelta{RemovedTotal: 100})
	if !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("negative size = %v", err)
	}
	if _, err := n.queueSize(baseSource); !errors.Is(err, ErrNotFound) {
		t.Fatal("source must be untracked after a size disorder")
	}
}

func TestInternalsApplyGapIdempotencyAndDisorder(t *testing.T) {
	n := testInternals(t)

	if err := n.ApplyBlock(baseSource, sref(11, 1), &InternalsDelta{}); !errors.Is(err, ErrApplyGap) {
		t.Fatalf("unseeded apply = %v", err)
	}
	seeded := imsg(500, 1)
	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{seeded})
	// A replayed or older block is an idempotent no-op.
	if err := n.ApplyBlock(baseSource, sref(10, 0xaa), &InternalsDelta{Added: []*InternalMessage{imsg(600, 9)}}); err != nil {
		t.Fatal(err)
	}
	if top, _ := n.SourceTop(baseSource); top.Seqno != 10 {
		t.Fatalf("top = %d", top.Seqno)
	}
	// A skipped block is a gap.
	if err := n.ApplyBlock(baseSource, sref(12, 2), &InternalsDelta{}); !errors.Is(err, ErrApplyGap) {
		t.Fatalf("gap apply = %v", err)
	}
	// Re-inserting an existing queue key is a disorder: the source is
	// untracked until a reseed. Lower canonical LTs themselves are valid for
	// transit messages and are merged into the run.
	if err := n.ApplyBlock(baseSource, sref(11, 3), &InternalsDelta{
		Added: []*InternalMessage{seeded},
	}); !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("disorder apply = %v", err)
	}
	if _, err := n.SourceTop(baseSource); !errors.Is(err, ErrNotFound) {
		t.Fatal("disorder must untrack the source")
	}
	if _, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(11, 3)},
	}}); !errors.Is(err, ErrCutNotReady) {
		t.Fatalf("cut after disorder = %v", err)
	}

	// A reseed heals the run.
	seedN(t, n, baseSource, sref(11, 3), []*InternalMessage{imsg(400, 4), imsg(500, 1)})
	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(11, 3)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 400, 500)
}

func TestInternalsVisibilityBarrierAndStaleness(t *testing.T) {
	n := testInternals(t)

	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(100, 1)})
	for seq := uint32(11); seq <= 12; seq++ {
		if err := n.ApplyBlock(baseSource, sref(seq, byte(seq)), &InternalsDelta{
			Added:      []*InternalMessage{imsg(uint64(seq)*100, uint16(seq))},
			AddedTotal: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// An untracked source is not ready.
	if _, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		mcSource: {Visible: sref(1, 1)},
	}}); !errors.Is(err, ErrCutNotReady) {
		t.Fatalf("untracked cut = %v", err)
	}
	// The barrier: a position past the run top is not ready yet.
	if _, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(13, 13)},
	}}); !errors.Is(err, ErrCutNotReady) {
		t.Fatalf("future cut = %v", err)
	}
	// A wrong hash at a retained position is stale.
	if _, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(12, 0xff)},
	}}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("stale cut = %v", err)
	}
	// A position before the seed conflicts with the tracked view.
	if _, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(9, 9)},
	}}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("pre-seed cut = %v", err)
	}

	// An older visible position hides messages enqueued past it.
	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(11, 11)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 100, 1100)
}

func TestInternalsHistoricalCutAppliesRemovalAtItsBlock(t *testing.T) {
	n := testInternals(t)

	first := imsg(100, 1)
	second := imsg(200, 2)
	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{first, second})
	if err := n.ApplyBlock(baseSource, sref(11, 0xbb), &InternalsDelta{
		Added:      []*InternalMessage{imsg(1_100, 3)},
		AddedTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyBlock(baseSource, sref(12, 0xcc), &InternalsDelta{
		RemovedKeys:  []QueueKey{first.Key},
		RemovedTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}

	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(11, 0xbb)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 100, 200, 1_100)

	cut, err = n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(12, 0xcc)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 200, 1_100)
}

func TestInternalsCandidateOverlay(t *testing.T) {
	n := testInternals(t)

	committed := []*InternalMessage{imsg(100, 1), imsg(200, 2)}
	seedN(t, n, baseSource, sref(10, 0xaa), committed)

	// Candidate 11 imports the first committed message and enqueues one;
	// candidate 12 consumes the candidate-11 message and enqueues another.
	cand11, cand12 := sref(11, 0xc1), sref(12, 0xc2)
	added11 := imsg(1100, 3)
	if err := n.AddCandidate(baseSource, cand11.RootHash, sref(10, 0xaa).RootHash, 11, &InternalsDelta{
		Added:       []*InternalMessage{added11},
		RemovedKeys: []QueueKey{committed[0].Key},
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.AddCandidate(baseSource, cand12.RootHash, cand11.RootHash, 12, &InternalsDelta{
		Added:       []*InternalMessage{imsg(1200, 4)},
		RemovedKeys: []QueueKey{added11.Key},
	}); err != nil {
		t.Fatal(err)
	}

	cutAt := func(tip [32]byte) (*Cut, error) {
		return n.Cut(CutRequest{
			Sources:      map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}},
			CandidateTip: &tip,
		})
	}

	cut, err := cutAt(cand11.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 200, 1100)

	cut, err = cutAt(cand12.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 200, 1200)

	// A chain that skips a seqno is stale even when the deltas don't collide.
	skip13 := sref(14, 0xc4)
	if err = n.AddCandidate(baseSource, skip13.RootHash, cand12.RootHash, 14, &InternalsDelta{
		Added: []*InternalMessage{imsg(1400, 5)},
	}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("gapped candidate = %v", err)
	}

	// Unknown tip: stale (waiting can never help — own-chain candidates are
	// registered before they are cut on). The recorded candidate owns its exact
	// base position, so a caller may admit that source from a concurrently newer
	// view without changing the selected queue state.
	if _, err = cutAt(sref(13, 0xc3).RootHash); !errors.Is(err, ErrCutStale) {
		t.Fatalf("unknown tip = %v", err)
	}
	tip := cand11.RootHash
	cut, err = n.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: sref(9, 0x99)}},
		CandidateTip: &tip,
	})
	if err != nil {
		t.Fatalf("admitted candidate source: %v", err)
	}
	requireLts(t, cut, 200, 1100)
	if _, err = n.Cut(CutRequest{CandidateTip: &tip}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("unadmitted candidate source = %v", err)
	}

	// Candidate 11 finalizes: the block delta applies to the committed run
	// and both stale candidates are pruned as the chain advances.
	if err = n.ApplyBlock(baseSource, cand11, &InternalsDelta{
		Added:       []*InternalMessage{added11},
		RemovedKeys: []QueueKey{committed[0].Key},
		AddedTotal:  1, RemovedTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if st := n.Stats(); st.Candidates != 1 {
		t.Fatalf("candidates after promote = %d", st.Candidates)
	}
	tip = cand12.RootHash
	cut, err = n.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: cand11}},
		CandidateTip: &tip,
	})
	if err != nil {
		t.Fatalf("candidate child after parent promotion: %v", err)
	}
	requireLts(t, cut, 200, 1200)
	cut, err = n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: cand11},
	}})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 200, 1100)

	n.DropCandidate(cand12.RootHash)
	if st := n.Stats(); st.Candidates != 0 {
		t.Fatalf("candidates after drop = %d", st.Candidates)
	}
}

func TestInternalsContinuedCandidateRacesAppliedParent(t *testing.T) {
	n := testInternals(t)
	base := sref(10, 0xaa)
	parent := sref(11, 0xc1)
	child := sref(12, 0xc2)
	seedN(t, n, baseSource, base, nil)

	if err := n.AddCandidate(baseSource, parent.RootHash, base.RootHash, parent.Seqno, &InternalsDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyBlock(baseSource, parent, &InternalsDelta{}); err != nil {
		t.Fatal(err)
	}
	if err := n.AddCandidate(baseSource, child.RootHash, parent.RootHash, child.Seqno, &InternalsDelta{}); err != nil {
		t.Fatalf("continued candidate after exact parent promotion: %v", err)
	}

	tip := child.RootHash
	if _, err := n.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: parent}},
		CandidateTip: &tip,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInternalsCutSurvivesCandidatePromotion(t *testing.T) {
	n := testInternals(t)
	base := sref(10, 0xaa)
	candidate := sref(11, 0xc1)
	first := imsg(100, 1)
	second := imsg(200, 2)
	added := imsg(1_100, 3)
	seedN(t, n, baseSource, base, []*InternalMessage{first, second})
	delta := &InternalsDelta{
		Added:        []*InternalMessage{added},
		RemovedKeys:  []QueueKey{first.Key},
		AddedTotal:   1,
		RemovedTotal: 1,
	}
	if err := n.AddCandidate(baseSource, candidate.RootHash, base.RootHash, candidate.Seqno, delta); err != nil {
		t.Fatal(err)
	}

	tip := candidate.RootHash
	request := CutRequest{
		Sources:          map[ShardIdent]CutSource{baseSource: {Visible: base}},
		CandidateTip:     &tip,
		CandidateSources: []ShardIdent{baseSource},
	}
	if err := n.ApplyBlock(baseSource, candidate, delta); err != nil {
		t.Fatal(err)
	}

	cut, err := n.Cut(request)
	if err != nil {
		t.Fatalf("cut prepared before candidate promotion: %v", err)
	}
	requireLts(t, cut, 200, 1_100)
}

func TestInternalsCandidateOverlayAppliesSequentiallyInCanonicalOrder(t *testing.T) {
	n := testInternals(t)

	committed := imsg(2_000, 1)
	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{committed})

	cand11, cand12 := sref(11, 0xc1), sref(12, 0xc2)
	if err := n.AddCandidate(baseSource, cand11.RootHash, sref(10, 0xaa).RootHash, 11, &InternalsDelta{
		RemovedKeys: []QueueKey{committed.Key},
	}); err != nil {
		t.Fatal(err)
	}
	readded := imsg(2_000, 1)
	lowerLT := imsg(1_000, 2)
	if err := n.AddCandidate(baseSource, cand12.RootHash, cand11.RootHash, 12, &InternalsDelta{
		Added: []*InternalMessage{lowerLT, readded},
	}); err != nil {
		t.Fatal(err)
	}

	tip := cand12.RootHash
	cut, err := n.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}},
		CandidateTip: &tip,
		Limit:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1_000)
	if !cut.More {
		t.Fatal("re-added committed message must remain after the limited cut")
	}

	cut, err = n.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}},
		CandidateTip: &tip,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 1_000, 2_000)
}

func TestInternalsCandidateOverlayOrdersAdditionsAcrossCandidates(t *testing.T) {
	n := testInternals(t)

	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(10, 1)})

	cand11, cand12 := sref(11, 0xc1), sref(12, 0xc2)
	if err := n.AddCandidate(baseSource, cand11.RootHash, sref(10, 0xaa).RootHash, 11, &InternalsDelta{
		Added: []*InternalMessage{imsg(30, 3)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.AddCandidate(baseSource, cand12.RootHash, cand11.RootHash, 12, &InternalsDelta{
		Added: []*InternalMessage{imsg(20, 2)},
	}); err != nil {
		t.Fatal(err)
	}

	tip := cand12.RootHash
	cut, err := n.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}},
		CandidateTip: &tip,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireLts(t, cut, 10, 20, 30)
}

func TestInternalsCandidateOverlayRejectsDuplicateOrderKeyAcrossCandidates(t *testing.T) {
	n := testInternals(t)

	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(10, 1)})

	first := imsg(30, 2)
	duplicateOrderKey := *first
	duplicateOrderKey.Key = MakeQueueKey(
		AccountPrefix{Workchain: 0, Prefix: 0x9000000000000000},
		first.Root.HashKey(),
	)
	duplicateOrderKey.EnvelopeCell = cell.BeginCell().MustStoreUInt(0xf0, 16).MustStoreRef(first.Root).EndCell()
	duplicateOrderKey.EnvHash = duplicateOrderKey.EnvelopeCell.HashKey()
	if first.Key == duplicateOrderKey.Key || first.EnvHash == duplicateOrderKey.EnvHash ||
		CompareLtHash(first, &duplicateOrderKey) != 0 {
		t.Fatal("duplicate-order fixture does not have distinct queue identities and one canonical order key")
	}

	cand11, cand12 := sref(11, 0xc1), sref(12, 0xc2)
	if err := n.AddCandidate(baseSource, cand11.RootHash, sref(10, 0xaa).RootHash, 11, &InternalsDelta{
		Added: []*InternalMessage{first},
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.AddCandidate(baseSource, cand12.RootHash, cand11.RootHash, 12, &InternalsDelta{
		Added: []*InternalMessage{&duplicateOrderKey},
	}); err != nil {
		t.Fatal(err)
	}

	tip := cand12.RootHash
	if _, err := n.Cut(CutRequest{
		Sources:      map[ShardIdent]CutSource{baseSource: {Visible: sref(10, 0xaa)}},
		CandidateTip: &tip,
	}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("duplicate candidate order key = %v", err)
	}
}

func TestInternalsCompactionKeepsOrderAndIndexes(t *testing.T) {
	n := testInternals(t)

	msgs := make([]*InternalMessage, 512)
	for i := range msgs {
		msgs[i] = imsg(uint64(100+i), uint16(i))
	}
	seedN(t, n, baseSource, sref(10, 0xaa), msgs)
	// Remove every even message across several blocks.
	for block := 0; block < 4; block++ {
		var removed []QueueKey
		for i := block * 128; i < (block+1)*128; i += 2 {
			removed = append(removed, msgs[i].Key)
		}
		if err := n.ApplyBlock(baseSource, sref(uint32(11+block), byte(block)), &InternalsDelta{
			RemovedKeys:  removed,
			RemovedTotal: len(removed),
		}); err != nil {
			t.Fatal(err)
		}
	}

	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(14, 3)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 256 {
		t.Fatalf("live entries = %d", len(cut.Messages))
	}
	for i, msg := range cut.Messages {
		if msg.EnqueuedLT != uint64(100+i*2+1) {
			t.Fatalf("entry %d lt = %d", i, msg.EnqueuedLT)
		}
	}

	// One more removal tips the tombstones over half of the slice and
	// fires the compaction.
	if err = n.ApplyBlock(baseSource, sref(15, 4), &InternalsDelta{
		RemovedKeys:  []QueueKey{msgs[1].Key},
		RemovedTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}
	st := n.Stats()
	if st.Entries != 255 || st.Removed != 0 {
		t.Fatalf("post-compaction stats: entries=%d removed=%d", st.Entries, st.Removed)
	}
	if st.Compactions != 1 || st.CutStaleHistory != 0 {
		t.Fatalf("compaction counters: compactions=%d cut_stale_history=%d",
			st.Compactions, st.CutStaleHistory)
	}
	if _, err = n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(14, 3)},
	}}); !errors.Is(err, ErrCutStale) {
		t.Fatalf("cut before compacted history floor = %v", err)
	}
	// The refused historical cut is the event that sends a collation to the
	// from-state seed walk, so it is counted apart from other cut failures.
	if st = n.Stats(); st.CutStaleHistory != 1 {
		t.Fatalf("cut_stale_history = %d", st.CutStaleHistory)
	}
	// Removals and cuts keep working through the rebuilt indexes.
	if err = n.ApplyBlock(baseSource, sref(16, 5), &InternalsDelta{
		RemovedKeys:      []QueueKey{msgs[3].Key},
		RemovedEnvHashes: [][32]byte{msgs[5].EnvHash},
		RemovedTotal:     2,
	}); err != nil {
		t.Fatal(err)
	}
	cut, err = n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(16, 5)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 253 || cut.Messages[0].EnqueuedLT != 107 {
		t.Fatalf("post-compaction cut: %d messages, first lt %d", len(cut.Messages), cut.Messages[0].EnqueuedLT)
	}
}

func TestInternalsSeedValidation(t *testing.T) {
	n := testInternals(t)

	// Wrong order.
	err := n.Seed(baseSource, sref(10, 1), []*InternalMessage{imsg(200, 1), imsg(100, 2)}, 2)
	if !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("unsorted seed = %v", err)
	}
	// A message routed outside the owner shard.
	foreign := imsg(100, 1)
	foreign.Key = MakeQueueKey(AccountPrefix{Workchain: -1, Prefix: 0x8000000000000000}, foreign.Root.HashKey())
	if err = n.Seed(baseSource, sref(10, 1), []*InternalMessage{foreign}, 1); !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("foreign seed = %v", err)
	}
	// Duplicate queue keys.
	dup := imsg(100, 1)
	dupLater := *dup
	dupLater.EnqueuedLT = 200
	if err = n.Seed(baseSource, sref(10, 1), []*InternalMessage{dup, &dupLater}, 2); !errors.Is(err, ErrApplyDisorder) {
		t.Fatalf("duplicate seed = %v", err)
	}
	if _, err = n.SourceTop(baseSource); !errors.Is(err, ErrNotFound) {
		t.Fatal("failed seed must leave the source untracked")
	}
}

// TestInternalsPruneSources verifies that rotation GC drops sources that left
// the shard configuration without disturbing retained runs.
func TestInternalsPruneSources(t *testing.T) {
	n := testInternals(t)

	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(100, 1)})
	seedN(t, n, mcSource, sref(5, 0xbb), nil)

	// The masterchain source left the configuration: prune keeps only the
	// basechain run.
	n.PruneSources(func(s ShardIdent) bool { return s.Workchain == 0 })
	if _, err := n.SourceTop(mcSource); !errors.Is(err, ErrNotFound) {
		t.Fatal("pruned source must be untracked")
	}
	if _, err := n.SourceTop(baseSource); err != nil {
		t.Fatal("kept source must stay tracked")
	}
}

// TestInternalsConcurrency hammers the section from producers and cutters;
// run with -race.
func TestInternalsConcurrency(t *testing.T) {
	n := testInternals(t)

	seedN(t, n, baseSource, sref(0, 0), nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for seq := uint32(1); seq <= 128; seq++ {
			_ = n.ApplyBlock(baseSource, sref(seq, byte(seq)), &InternalsDelta{
				Added:      []*InternalMessage{imsg(uint64(seq)*10, uint16(seq))},
				AddedTotal: 1,
			})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 256; i++ {
			top, err := n.SourceTop(baseSource)
			if err != nil {
				continue
			}
			cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
				baseSource: {Visible: top},
			}, Limit: 16})
			if err == nil && len(cut.Messages) > 16 {
				panic(fmt.Sprintf("limit ignored: %d", len(cut.Messages)))
			}
			n.Stats()
			n.queueSize(baseSource)
		}
	}()
	wg.Wait()

	cut, err := n.Cut(CutRequest{Sources: map[ShardIdent]CutSource{
		baseSource: {Visible: sref(128, 128)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 128 {
		t.Fatalf("final entries = %d", len(cut.Messages))
	}
	if size, _ := n.queueSize(baseSource); size != 128 {
		t.Fatalf("final queue size = %d", size)
	}
}

// A candidate batch that repeats a queue key must be rejected. The key is the
// (next hop, message hash) pair, so two entries at different enqueued lt can
// collide while still passing the strict CompareLtHash ordering — the ordering
// check alone does not close this, and a view holding one queue key twice would
// feed collation.
func TestInternalsCandidateRejectsDuplicateQueueKeyWithinOneBatch(t *testing.T) {
	n := testInternals(t)
	seedN(t, n, baseSource, sref(10, 0xaa), []*InternalMessage{imsg(100, 1)})

	// The queue key is (next hop, message hash) and carries no lt, so the same
	// message re-enqueued at a higher lt collides with itself.
	first := imsg(1100, 3)
	second := imsg(1100, 3)
	second.EnqueuedLT = 1200
	if first.Key != second.Key {
		t.Fatal("fixture does not produce a queue-key collision")
	}
	if CompareLtHash(first, second) >= 0 {
		t.Fatal("fixture batch is not in canonical order")
	}

	err := n.AddCandidate(baseSource, sref(11, 0xc1).RootHash, sref(10, 0xaa).RootHash, 11, &InternalsDelta{
		Added: []*InternalMessage{first, second},
	})
	if err == nil {
		t.Fatal("a candidate repeating a queue key was accepted")
	}
	if !strings.Contains(err.Error(), "queue key") {
		t.Fatalf("duplicate queue key error = %v", err)
	}
}
