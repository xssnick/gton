package msgpool

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// feedTestChain is one shard chain as both producers of the feed deliver it:
// block k exports message k (msg_export_new in its OutMsgDescr) and the state
// after block k holds messages 1..k in its out-queue with the queue size stored,
// so a block fed through the delta path and a block fed through a reseed arrive
// at the same run. fill distinguishes two chains at the same heights, which is
// what a same-height fork needs.
type feedTestChain struct {
	fill      byte
	messages  []*cell.Cell
	envelopes []*cell.Cell
	keys      []QueueKey
}

func newFeedTestChain(t testing.TB, fill byte, length int) *feedTestChain {
	t.Helper()

	chain := &feedTestChain{fill: fill}
	source := deltaAddr(0, 0x11)
	for index := range length {
		destination := deltaAddr(0, byte(0x20+index))
		message := deltaInternalMsg(t, source, destination, uint64(1000*(index+1)))
		hop, err := AccountPrefixFromAddress(destination)
		if err != nil {
			t.Fatal(err)
		}
		chain.messages = append(chain.messages, message)
		chain.envelopes = append(chain.envelopes, deltaEnvelope(t, message, regularNext(96)))
		chain.keys = append(chain.keys, MakeQueueKey(hop, message.HashKey()))
	}

	return chain
}

// block is block seqno of the chain with its post-state attached, generated
// now so the freshness gate lets it through.
func (c *feedTestChain) block(t testing.TB, seqno uint32) AppliedBlock {
	t.Helper()

	dict := newOutDescrDict(t)
	setDescr(t, dict, c.messages[seqno-1].Hash(), cell.BeginCell().
		MustStoreUInt(0b001, 3).
		MustStoreRef(c.envelopes[seqno-1]).
		MustStoreRef(cell.BeginCell().MustStoreUInt(uint64(c.fill), 8).EndCell()).
		EndCell())
	root := deltaBlockRoot(t, dict.AsCell())

	queued := make(map[QueueKey]tlb.EnqueuedMsg, seqno)
	for index := range int(seqno) {
		queued[c.keys[index]] = tlb.EnqueuedMsg{EnqueuedLT: uint64(1000 * (index + 1)), Msg: c.envelopes[index]}
	}

	return AppliedBlock{
		ID: ton.BlockIDExt{
			Workchain: feedTestSource.Workchain,
			Shard:     int64(feedTestSource.Shard),
			SeqNo:     seqno,
			RootHash:  root.Hash(),
			FileHash:  make([]byte, 32),
		},
		BlockRoot: root,
		StateRoot: stateRootWithQueue(t, queueDictCell(t, queued), uint64(seqno), true),
		GenUTime:  uint32(time.Now().Unix()),
	}
}

func (c *feedTestChain) lts(upTo int) []uint64 {
	lts := make([]uint64, upTo)
	for index := range lts {
		lts[index] = uint64(1000 * (index + 1))
	}

	return lts
}

// newAcceptedTestFeed is a feed whose pool tracks feedTestSource as its own
// destination, the shape of a validator's own shard, with no masterchain
// projection so the source is admitted as it is in every feed test.
func newAcceptedTestFeed(t *testing.T) *Feed {
	t.Helper()

	feed := newTestFeed(t, nil)
	if err := feed.internals.ReconcileDestinations([]ShardIdent{feedTestSource}); err != nil {
		t.Fatal(err)
	}

	return feed
}

func requireSourceTop(t *testing.T, feed *Feed, block AppliedBlock) {
	t.Helper()

	want := SourceRef{Seqno: block.ID.SeqNo}
	copy(want.RootHash[:], block.ID.RootHash)
	top, err := feed.internals.SourceTop(feedTestSource, feedTestSource)
	if err != nil || top != want {
		t.Fatalf("source top = %+v (%v), want %+v", top, err, want)
	}
}

func requireCutLts(t *testing.T, feed *Feed, block AppliedBlock, want ...uint64) {
	t.Helper()

	visible := SourceRef{Seqno: block.ID.SeqNo}
	copy(visible.RootHash[:], block.ID.RootHash)
	cut, err := feed.internals.Cut(feedTestSource, CutRequest{
		Sources: map[ShardIdent]CutSource{feedTestSource: {Visible: visible}},
	})
	if err != nil {
		t.Fatalf("cut at %d: %v", block.ID.SeqNo, err)
	}
	requireLts(t, cut, want...)
}

// TestFeedAcceptedBlockFeedsInternalsAheadOfApply is the reason the accepted
// producer exists: a block this node finalized reaches the pool from its
// acceptance, with nothing applied yet, and the apply of the very same block —
// which arrives about a second later on the stand — neither reseeds the run nor
// applies its delta a second time. Both the seed path (first sight of the
// source) and the delta path (the next block) are exercised, because a
// duplicate would surface differently on each.
func TestFeedAcceptedBlockFeedsInternalsAheadOfApply(t *testing.T) {
	feed := newAcceptedTestFeed(t)
	chain := newFeedTestChain(t, 0xa1, 2)

	first := chain.block(t, 1)
	if !feed.ObserveAccepted(first) {
		t.Fatal("the accepted block did not advance its source")
	}
	requireSourceTop(t, feed, first)
	requireCutLts(t, feed, first, chain.lts(1)...)
	if stats := feed.internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 0 {
		t.Fatalf("first accepted block bookkeeping = %+v, want one seed", stats)
	}

	if feed.Observe(first) {
		t.Fatal("the apply of an accepted block advanced its source again")
	}
	requireSourceTop(t, feed, first)
	requireCutLts(t, feed, first, chain.lts(1)...)
	if stats := feed.internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 0 || stats.Entries != 1 {
		t.Fatalf("apply of an accepted block changed the run: %+v", stats)
	}

	second := chain.block(t, 2)
	if !feed.ObserveAccepted(second) {
		t.Fatal("the second accepted block did not advance its source")
	}
	requireSourceTop(t, feed, second)
	requireCutLts(t, feed, second, chain.lts(2)...)
	if stats := feed.internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 1 {
		t.Fatalf("second accepted block bookkeeping = %+v, want the delta path", stats)
	}

	if feed.Observe(second) {
		t.Fatal("the apply of the second accepted block advanced its source again")
	}
	requireCutLts(t, feed, second, chain.lts(2)...)
	if stats := feed.internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 1 || stats.Entries != 2 {
		t.Fatalf("apply of the second accepted block changed the run: %+v", stats)
	}

	if got := feed.Stats(); got != (FeedStats{AcceptedFed: 2, AppliedSuperseded: 2}) {
		t.Fatalf("feed counters = %+v, want two accepted feeds confirmed by two applies", got)
	}
}

// TestFeedAppliedBlockMakesALaterAcceptanceANoOp is the other order. The apply
// pipeline can be ahead of acceptance — a replayed finalization, a block this
// node validated after the shard client applied it — and the acceptance must
// then be as much of a no-op as the apply is in the ordinary order.
func TestFeedAppliedBlockMakesALaterAcceptanceANoOp(t *testing.T) {
	feed := newAcceptedTestFeed(t)
	chain := newFeedTestChain(t, 0xa2, 1)

	first := chain.block(t, 1)
	if !feed.Observe(first) {
		t.Fatal("the applied block did not advance its source")
	}
	if feed.ObserveAccepted(first) {
		t.Fatal("the acceptance of an applied block advanced its source again")
	}
	requireSourceTop(t, feed, first)
	requireCutLts(t, feed, first, chain.lts(1)...)
	if stats := feed.internals.Stats(); stats.Seeds != 1 || stats.Entries != 1 {
		t.Fatalf("acceptance of an applied block changed the run: %+v", stats)
	}
	if got := feed.Stats(); got != (FeedStats{AppliedFed: 1, AcceptedSuperseded: 1}) {
		t.Fatalf("feed counters = %+v", got)
	}
}

// TestFeedAcceptanceOvertakingTheApplyStreamReseedsOnce pins the cost of the
// switch from one producer to the other. The pool was following the apply
// stream; the first accepted block lands more than one block ahead of the run,
// ApplyBlock refuses the gap and the source is reseeded from the accepted state
// — once. The lagging applies are then no-ops and the next accepted block is
// back on the delta path.
func TestFeedAcceptanceOvertakingTheApplyStreamReseedsOnce(t *testing.T) {
	feed := newAcceptedTestFeed(t)
	chain := newFeedTestChain(t, 0xa3, 4)

	if !feed.Observe(chain.block(t, 1)) {
		t.Fatal("the applied block did not seed its source")
	}
	third := chain.block(t, 3)
	if !feed.ObserveAccepted(third) {
		t.Fatal("the accepted block ahead of the run did not advance its source")
	}
	requireSourceTop(t, feed, third)
	requireCutLts(t, feed, third, chain.lts(3)...)
	if stats := feed.internals.Stats(); stats.Seeds != 2 || stats.AppliedBlocks != 0 {
		t.Fatalf("overtaking acceptance bookkeeping = %+v, want one reseed and no delta", stats)
	}

	for _, seqno := range []uint32{2, 3} {
		if feed.Observe(chain.block(t, seqno)) {
			t.Fatalf("the lagging apply of block %d advanced its source", seqno)
		}
	}
	fourth := chain.block(t, 4)
	if !feed.ObserveAccepted(fourth) {
		t.Fatal("the accepted block after the switch did not advance its source")
	}
	requireCutLts(t, feed, fourth, chain.lts(4)...)
	if stats := feed.internals.Stats(); stats.Seeds != 2 || stats.AppliedBlocks != 1 || stats.Entries != 4 {
		t.Fatalf("post-switch bookkeeping = %+v, want the delta path", stats)
	}
}

// TestFeedSameHeightForkRunsAndReseedsTheDestination keeps the identity mark
// honest: the mark is the block, not its height. A block at an already fed
// height with another root hash is a different block, and feedInternals
// reseeding from its state is the only way the destination moves off the wrong
// chain; a second delivery of that same block is then the ordinary no-op.
func TestFeedSameHeightForkRunsAndReseedsTheDestination(t *testing.T) {
	feed := newAcceptedTestFeed(t)
	left := newFeedTestChain(t, 0xa4, 2)
	right := newFeedTestChain(t, 0xb4, 2)

	if !feed.ObserveAccepted(left.block(t, 1)) {
		t.Fatal("the accepted block did not seed its source")
	}
	fork := right.block(t, 1)
	if !feed.Observe(fork) {
		t.Fatal("a same-height block with another root hash was refused as already fed")
	}
	requireSourceTop(t, feed, fork)
	requireCutLts(t, feed, fork, right.lts(1)...)
	if stats := feed.internals.Stats(); stats.Seeds != 2 || stats.Entries != 1 {
		t.Fatalf("fork bookkeeping = %+v, want a reseed from the fork state", stats)
	}
	if feed.Observe(fork) {
		t.Fatal("the same fork block was fed twice")
	}
	// The mark holds one identity, so the other same-height block is again a
	// different block and runs: the last delivered block at a height owns the
	// run. Two producers cannot disagree on a finalized block, so this is the
	// preloaded-view recovery and never a live ping-pong.
	original := left.block(t, 1)
	if !feed.ObserveAccepted(original) {
		t.Fatal("the other same-height block was refused as already fed")
	}
	requireSourceTop(t, feed, original)
	requireCutLts(t, feed, original, left.lts(1)...)
	if stats := feed.internals.Stats(); stats.Seeds != 3 || stats.Entries != 1 {
		t.Fatalf("fork bookkeeping = %+v, want a reseed per delivered identity", stats)
	}
}

// TestFeedConcurrentProducersConvergeOnTheChainHead runs the two producers
// against each other with no ordering between them, which is what the stand
// does: acceptance runs on the finalization chain and the apply hook on the
// shard client's workers. Whichever order the interleaving takes, the source
// must end at the chain head with exactly the head state's queue — every gap
// the race opens is closed by a reseed from the delivered state — and every
// delivery must be accounted for exactly once.
func TestFeedConcurrentProducersConvergeOnTheChainHead(t *testing.T) {
	const length = 24
	feed := newAcceptedTestFeed(t)
	chain := newFeedTestChain(t, 0xa5, length)
	blocks := make([]AppliedBlock, length)
	for index := range blocks {
		blocks[index] = chain.block(t, uint32(index+1))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for index := range blocks {
			feed.ObserveAccepted(blocks[index])
		}
	}()
	go func() {
		defer wg.Done()
		for index := range blocks {
			feed.Observe(blocks[index])
		}
	}()
	wg.Wait()

	head := blocks[length-1]
	requireSourceTop(t, feed, head)
	requireCutLts(t, feed, head, chain.lts(length)...)
	if stats := feed.internals.Stats(); stats.Entries != length {
		t.Fatalf("run holds %d entries, want %d", stats.Entries, length)
	}
	got := feed.Stats()
	fed := got.AcceptedFed + got.AppliedFed
	if fed == 0 || fed > length || fed+got.AcceptedSuperseded+got.AppliedSuperseded != 2*length {
		t.Fatalf("feed counters = %+v, want every one of %d deliveries fed or superseded", got, 2*length)
	}
}

// TestFeedDeferralReportsNothingFed pins the return value on the deferral
// path: a stale block is remembered for the sweep and advances nothing now.
func TestFeedDeferralReportsNothingFed(t *testing.T) {
	pool := New(Config{})
	t.Cleanup(pool.Close)
	feed := NewFeed(FeedOptions{
		Pool:            pool,
		Logger:          zerolog.Nop(),
		FreshnessWindow: time.Minute,
		HeadSettleDelay: time.Millisecond,
	})

	if feed.ObserveAccepted(feedTestBlock(t, 3, 1)) {
		t.Fatal("a stale accepted block reported itself fed")
	}
	if got := feed.Stats(); got != (FeedStats{Deferred: 1}) {
		t.Fatalf("feed counters = %+v, want one deferral", got)
	}
}
