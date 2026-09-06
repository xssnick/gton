package validator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/msgpool"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The live view is the one store the composition hands a validator, and it is
// the one that publishes accepted states. If it ever stopped satisfying the
// capability, the pool would silently fall back to the apply hook alone.
var _ hooks.AcceptedBlockStatePublisher = (*liveview.Store)(nil)

// acceptedFeedTestStore is the node store as the validator sees it, with the
// accepted-state capability: the test plays the part of BlockAccepter and
// publishes what acceptance publishes.
type acceptedFeedTestStore struct {
	validatorTestStore

	mu        sync.Mutex
	observers []func(storage.LiveBlockArtifacts)
	stopped   int
}

func newAcceptedFeedTestStore() *acceptedFeedTestStore {
	return &acceptedFeedTestStore{validatorTestStore: validatorTestStore{err: storage.ErrNotFound}}
}

func (s *acceptedFeedTestStore) ObserveAcceptedBlockStates(observe func(storage.LiveBlockArtifacts)) func() {
	s.mu.Lock()
	s.observers = append(s.observers, observe)
	s.mu.Unlock()

	return func() {
		s.mu.Lock()
		s.stopped++
		s.observers = nil
		s.mu.Unlock()
	}
}

// publish is liveview.Store.PublishAcceptedBlockState from the observer's side:
// the artifacts are already installed and the observers run synchronously.
func (s *acceptedFeedTestStore) publish(artifacts storage.LiveBlockArtifacts) {
	s.mu.Lock()
	observers := make([]func(storage.LiveBlockArtifacts), len(s.observers))
	copy(observers, s.observers)
	s.mu.Unlock()

	for _, observe := range observers {
		observe(artifacts)
	}
}

func (s *acceptedFeedTestStore) stops() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.stopped
}

// acceptedArtifacts is what BlockAccepter.publishAcceptedState hands the live
// view for one shard block: the parsed root, the metadata built from the block
// and the resident state cell.
func acceptedArtifacts(root, state *cell.Cell, seqno uint32) storage.LiveBlockArtifacts {
	id := ton.BlockIDExt{
		Workchain: 0, Shard: -0x8000000000000000, SeqNo: seqno,
		RootHash: root.Hash(), FileHash: make([]byte, 32),
	}

	return storage.LiveBlockArtifacts{
		Block: id,
		Root:  root,
		Meta:  &storage.BlockMeta{ID: id, GenUTime: uint32(time.Now().Unix())},
		State: &storage.BlockState{Block: id, StateRootHash: state.Hash(), Cell: state},
	}
}

func acceptedFeedEvent(root, state *cell.Cell, seqno uint32) hooks.BlockAppliedEvent {
	event := appliedEvent(root)
	event.Meta.ID.SeqNo = seqno
	event.CurrentState = state

	return event
}

func requireRunLts(t *testing.T, internals *msgpool.Internals, root *cell.Cell, seqno uint32, want ...uint64) {
	t.Helper()

	cut, err := internals.Cut(allShard, msgpool.CutRequest{Sources: map[msgpool.ShardIdent]msgpool.CutSource{
		allShard: {Visible: feedRef(root, seqno)},
	}})
	if err != nil {
		t.Fatalf("cut at %d: %v", seqno, err)
	}
	if len(cut.Messages) != len(want) {
		t.Fatalf("cut at %d holds %d messages, want %d", seqno, len(cut.Messages), len(want))
	}
	for index := range want {
		if cut.Messages[index].EnqueuedLT != want[index] {
			t.Fatalf("cut at %d message %d lt = %d, want %d", seqno, index, cut.Messages[index].EnqueuedLT, want[index])
		}
	}
}

// TestAcceptedShardBlockFeedsInternalsBeforeItsApply is the whole point: a
// block a local session accepted reaches the pool from the accepted-state
// publication with nothing applied, so a collator cutting at that neighbour
// position is served — and the apply of the same block, which lands about a
// second later on the stand, changes nothing in the run: no reseed, no second
// delta, no duplicate message. Both the seed path and the delta path are
// covered, and a block the node did NOT accept still arrives through apply.
func TestAcceptedShardBlockFeedsInternalsBeforeItsApply(t *testing.T) {
	store := newAcceptedFeedTestStore()
	s := newTestServiceWithNode(t, Options{}, hooks.Node{Store: store, Logger: zerolog.Nop()})
	internals := s.pool.Internals()

	// Block 1: the first sight of the source, seeded from the accepted state.
	msgA := feedInternalMsg(t, 0x22, 1000)
	state1 := feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgA, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msgA)},
	})
	block1 := feedBlockRoot(t, nil)
	store.publish(acceptedArtifacts(block1, state1, 1))
	if top, err := internals.SourceTop(allShard, allShard); err != nil || top != feedRef(block1, 1) {
		t.Fatalf("accepted block 1 did not seed the source: top=%+v err=%v", top, err)
	}
	requireRunLts(t, internals, block1, 1, 1000)

	// Its apply is a confirmation and nothing else.
	if err := s.OnBlockApplied(context.Background(), acceptedFeedEvent(block1, state1, 1)); err != nil {
		t.Fatal(err)
	}
	if stats := internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 0 || stats.Entries != 1 {
		t.Fatalf("the apply of accepted block 1 changed the run: %+v", stats)
	}
	requireRunLts(t, internals, block1, 1, 1000)

	// Block 2: accepted with its export, fed through the delta path.
	msgB := feedInternalMsg(t, 0x44, 2000)
	envelopeB := feedEnvelope(t, msgB)
	state2 := feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgA, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msgA)},
		feedQueueKey(t, msgB, 0x44): {EnqueuedLT: 2000, Msg: envelopeB},
	})
	block2 := feedBlockRoot(t, map[*cell.Cell]*cell.Cell{msgB: envelopeB})
	store.publish(acceptedArtifacts(block2, state2, 2))
	if top, err := internals.SourceTop(allShard, allShard); err != nil || top != feedRef(block2, 2) {
		t.Fatalf("accepted block 2 did not advance the source: top=%+v err=%v", top, err)
	}
	if stats := internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 1 {
		t.Fatalf("accepted block 2 did not take the delta path: %+v", stats)
	}
	requireRunLts(t, internals, block2, 2, 1000, 2000)

	if err := s.OnBlockApplied(context.Background(), acceptedFeedEvent(block2, state2, 2)); err != nil {
		t.Fatal(err)
	}
	if stats := internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 1 || stats.Entries != 2 {
		t.Fatalf("the apply of accepted block 2 changed the run: %+v", stats)
	}
	requireRunLts(t, internals, block2, 2, 1000, 2000)

	// Block 3 was never accepted here — a shard this node stopped validating, a
	// downloaded block — and is fed by its apply exactly as before.
	msgC := feedInternalMsg(t, 0x55, 3000)
	envelopeC := feedEnvelope(t, msgC)
	block3 := feedBlockRoot(t, map[*cell.Cell]*cell.Cell{msgC: envelopeC})
	if err := s.OnBlockApplied(context.Background(), acceptedFeedEvent(block3, nil, 3)); err != nil {
		t.Fatal(err)
	}
	if top, err := internals.SourceTop(allShard, allShard); err != nil || top != feedRef(block3, 3) {
		t.Fatalf("applied block 3 did not advance the source: top=%+v err=%v", top, err)
	}
	requireRunLts(t, internals, block3, 3, 1000, 2000, 3000)

	if got := s.feed.Stats(); got != (msgpool.FeedStats{AcceptedFed: 2, AppliedFed: 1, AppliedSuperseded: 2}) {
		t.Fatalf("feed counters = %+v", got)
	}
}

// TestAppliedShardBlockMakesItsLaterAcceptanceANoOp is the other order: the
// apply pipeline got there first — a replayed finalization after a restart, a
// block validated after the shard client applied it — and the acceptance
// publication that follows must leave the run alone.
func TestAppliedShardBlockMakesItsLaterAcceptanceANoOp(t *testing.T) {
	store := newAcceptedFeedTestStore()
	s := newTestServiceWithNode(t, Options{}, hooks.Node{Store: store, Logger: zerolog.Nop()})
	internals := s.pool.Internals()

	msgA := feedInternalMsg(t, 0x22, 1000)
	state1 := feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgA, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msgA)},
	})
	block1 := feedBlockRoot(t, nil)
	if err := s.OnBlockApplied(context.Background(), acceptedFeedEvent(block1, state1, 1)); err != nil {
		t.Fatal(err)
	}
	if top, err := internals.SourceTop(allShard, allShard); err != nil || top != feedRef(block1, 1) {
		t.Fatalf("applied block 1 did not seed the source: top=%+v err=%v", top, err)
	}

	store.publish(acceptedArtifacts(block1, state1, 1))
	if stats := internals.Stats(); stats.Seeds != 1 || stats.AppliedBlocks != 0 || stats.Entries != 1 {
		t.Fatalf("the acceptance of applied block 1 changed the run: %+v", stats)
	}
	requireRunLts(t, internals, block1, 1, 1000)
	if got := s.feed.Stats(); got != (msgpool.FeedStats{AppliedFed: 1, AcceptedSuperseded: 1}) {
		t.Fatalf("feed counters = %+v", got)
	}
}

// TestAcceptedFeedIgnoresIncompletePublications: the live view refuses a
// publication without a root or a state, so this is belt and braces on the
// conversion — a publication the pool cannot read must not be fed as a block
// without a state, which would drop the source.
func TestAcceptedFeedIgnoresIncompletePublications(t *testing.T) {
	store := newAcceptedFeedTestStore()
	s := newTestServiceWithNode(t, Options{}, hooks.Node{Store: store, Logger: zerolog.Nop()})

	state := feedStateRoot(t, nil)
	block := feedBlockRoot(t, nil)
	complete := acceptedArtifacts(block, state, 1)

	withoutState := complete
	withoutState.State = nil
	store.publish(withoutState)
	withoutRoot := complete
	withoutRoot.Root = nil
	store.publish(withoutRoot)
	withoutMeta := complete
	withoutMeta.Meta = nil
	store.publish(withoutMeta)

	if got := s.feed.Stats(); got != (msgpool.FeedStats{}) {
		t.Fatalf("an incomplete publication reached the feed: %+v", got)
	}
}

// TestCloseStopsTheAcceptedFeed: the registration is released on Close, and a
// publication that races the close never reaches the pool the composition root
// is about to close.
func TestCloseStopsTheAcceptedFeed(t *testing.T) {
	store := newAcceptedFeedTestStore()
	s := newTestServiceWithNode(t, Options{}, hooks.Node{Store: store, Logger: zerolog.Nop()})
	if store.stops() != 0 {
		t.Fatal("the registration was stopped before Close")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if store.stops() != 1 {
		t.Fatalf("Close stopped the registration %d times, want once", store.stops())
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if store.stops() != 1 {
		t.Fatalf("a second Close stopped the registration again: %d", store.stops())
	}

	// A late observer call — one the store dispatched before the stop took — is
	// refused by the hook bracket like any hook after Close.
	s.onAcceptedBlockState(acceptedArtifacts(feedBlockRoot(t, nil), feedStateRoot(t, nil), 1))
	if got := s.feed.Stats(); got != (msgpool.FeedStats{}) {
		t.Fatalf("a publication after Close reached the feed: %+v", got)
	}
}

// TestStoreWithoutAcceptedStatesLeavesThePoolOnTheApplyHook: the capability is
// optional, and a store without it keeps the pre-existing behaviour intact.
func TestStoreWithoutAcceptedStatesLeavesThePoolOnTheApplyHook(t *testing.T) {
	s := newTestService(t, Options{})
	if s.stopAcceptedFeed != nil {
		t.Fatal("a store without the capability registered an accepted-state observer")
	}

	block := feedBlockRoot(t, nil)
	if err := s.OnBlockApplied(context.Background(), acceptedFeedEvent(block, feedStateRoot(t, nil), 1)); err != nil {
		t.Fatal(err)
	}
	if top, err := s.pool.Internals().SourceTop(allShard, allShard); err != nil || top != feedRef(block, 1) {
		t.Fatalf("the apply hook did not feed the pool: top=%+v err=%v", top, err)
	}
}
