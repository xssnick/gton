package collator

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type acceptedFeedStore struct {
	testNodeStore
	mu            sync.Mutex
	observers     []func(storage.LiveBlockArtifacts)
	registrations int
	stops         int
}

func (s *acceptedFeedStore) ObserveAcceptedBlockStates(observe func(storage.LiveBlockArtifacts)) func() {
	s.mu.Lock()
	s.observers = append(s.observers, observe)
	s.registrations++
	s.mu.Unlock()

	return func() {
		s.mu.Lock()
		s.observers = nil
		s.stops++
		s.mu.Unlock()
	}
}

// Liveview copies observers before invoking them outside its lock. A copied
// callback can therefore arrive after its registration has been removed.
func (s *acceptedFeedStore) snapshot() []func(storage.LiveBlockArtifacts) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.observers)
}

func (s *acceptedFeedStore) publish(artifacts storage.LiveBlockArtifacts) {
	for _, observe := range s.snapshot() {
		observe(artifacts)
	}
}

type acceptedFeedPrewarmer struct {
	mu       sync.Mutex
	accounts []msgpool.AccountDestination
	roots    []cell.Hash
	entered  chan struct{}
	release  chan struct{}
}

func (w *acceptedFeedPrewarmer) EnqueueAccount(workchain int32, account [32]byte) bool {
	w.mu.Lock()
	w.accounts = append(w.accounts, msgpool.AccountDestination{Workchain: workchain, Account: account})
	w.mu.Unlock()

	if w.entered != nil {
		close(w.entered)
		<-w.release
	}

	return true
}

func (w *acceptedFeedPrewarmer) EnqueueRoot(root cell.Hash) bool {
	w.mu.Lock()
	w.roots = append(w.roots, root)
	w.mu.Unlock()

	return true
}

type acceptedFeedFixture struct {
	extension  hooks.Extension
	store      *acceptedFeedStore
	controller *testController
	messages   *msgpool.Pool
	feed       *msgpool.Feed
}

func newAcceptedFeedFixture(t *testing.T, warmer msgpool.AccountPrewarmer) acceptedFeedFixture {
	t.Helper()

	f := acceptedFeedFixture{
		store:      &acceptedFeedStore{},
		controller: &testController{},
		messages:   msgpool.New(msgpool.Config{}),
	}
	t.Cleanup(f.messages.Close)
	f.feed = msgpool.NewFeed(msgpool.FeedOptions{Pool: f.messages, Prewarmer: warmer})

	var err error
	f.extension, err = New(Options{
		Controller: f.controller,
		ShardTops:  &testShardTopSink{},
		Messages:   f.messages,
		Feed:       f.feed,
	})(hooks.Node{Store: f.store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.extension.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err = f.messages.Internals().ReconcileDestinations([]msgpool.ShardIdent{feedTestShard}); err != nil {
		t.Fatal(err)
	}
	if len(f.store.snapshot()) != 1 {
		t.Fatal("standalone collator did not subscribe to accepted states")
	}

	return f
}

func acceptedFeedArtifacts(t *testing.T) storage.LiveBlockArtifacts {
	t.Helper()

	message := testInternalMessage(t, 0x22, 1000)
	root := testAppliedBlockRoot(t)
	state := testQueueStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		testQueueKey(t, message, 0x22): {EnqueuedLT: 1000, Msg: testMessageEnvelope(t, message)},
	})
	id := ton.BlockIDExt{
		Workchain: 0, Shard: int64(feedTestShard.Shard), SeqNo: 7,
		RootHash: root.Hash(), FileHash: make([]byte, 32),
	}

	return storage.LiveBlockArtifacts{
		Block: id,
		Root:  root,
		Meta:  &storage.BlockMeta{ID: id, GenUTime: uint32(time.Now().Unix())},
		State: &storage.BlockState{Block: id, Cell: state, StateRootHash: state.Hash()},
	}
}

func TestAcceptedShardBlockFeedsAndPrewarmsBeforeApply(t *testing.T) {
	warmer := &acceptedFeedPrewarmer{}
	f := newAcceptedFeedFixture(t, warmer)
	artifacts := acceptedFeedArtifacts(t)
	f.store.publish(artifacts)

	want := msgpool.SourceRef{Seqno: artifacts.Block.SeqNo}
	copy(want.RootHash[:], artifacts.Block.RootHash)
	if top, err := f.messages.Internals().SourceTop(feedTestShard, feedTestShard); err != nil || top != want {
		t.Fatalf("accepted source top = %+v, %v; want %+v", top, err, want)
	}
	cut, err := f.messages.Internals().Cut(feedTestShard, msgpool.CutRequest{
		Sources: map[msgpool.ShardIdent]msgpool.CutSource{feedTestShard: {Visible: want}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0].EnqueuedLT != 1000 {
		t.Fatalf("accepted source did not serve its queued message: %+v", cut.Messages)
	}
	warmer.mu.Lock()
	warmedBeforeApply := len(warmer.accounts) == 1 && len(warmer.roots) == 1
	warmer.mu.Unlock()
	if !warmedBeforeApply {
		t.Fatal("accepted state did not schedule account and envelope warming before apply")
	}

	f.store.publish(artifacts)
	if err = f.extension.OnBlockApplied(t.Context(), hooks.BlockAppliedEvent{
		Meta: artifacts.Meta, BlockRoot: artifacts.Root, CurrentState: artifacts.State.Cell,
	}); err != nil {
		t.Fatal(err)
	}
	if got := f.feed.Stats(); got != (msgpool.FeedStats{
		AcceptedFed: 1, AcceptedSuperseded: 1, AppliedSuperseded: 1,
	}) {
		t.Fatalf("duplicate accepted/apply feed counters = %+v", got)
	}
	if got := f.messages.Internals().Stats(); got.Seeds != 1 || got.Entries != 1 || got.AppliedBlocks != 0 {
		t.Fatalf("duplicate accepted/apply changed the run: %+v", got)
	}
	warmer.mu.Lock()
	defer warmer.mu.Unlock()
	if !slices.Equal(warmer.accounts, []msgpool.AccountDestination{{Workchain: 0, Account: [32]byte{0x22}}}) ||
		!slices.Equal(warmer.roots, []cell.Hash{cell.Hash(cut.Messages[0].EnvHash)}) {
		t.Fatalf("prewarm hints = accounts:%+v roots:%x; want exactly the destination and envelope", warmer.accounts, warmer.roots)
	}
}

func TestAcceptedFeedIgnoresMasterchainAndIncompleteArtifacts(t *testing.T) {
	for _, name := range []string{"masterchain", "metadata", "block root", "state", "state cell"} {
		t.Run(name, func(t *testing.T) {
			f := newAcceptedFeedFixture(t, nil)
			artifacts := acceptedFeedArtifacts(t)
			switch name {
			case "masterchain":
				artifacts.Block.Workchain = masterchainWorkchain
				artifacts.Meta.ID = artifacts.Block
				artifacts.State.Block = artifacts.Block
			case "metadata":
				artifacts.Meta = nil
			case "block root":
				artifacts.Root = nil
			case "state":
				artifacts.State = nil
			case "state cell":
				artifacts.State.Cell = nil
			}

			f.store.publish(artifacts)
			if got := f.feed.Stats(); got != (msgpool.FeedStats{}) {
				t.Fatalf("ignored accepted artifacts reached the feed: %+v", got)
			}
		})
	}
}

func TestAcceptedFeedSubscribesOnlyAfterSuccessfulConstruction(t *testing.T) {
	store := &acceptedFeedStore{}
	var commands console.Registry
	if err := commands.Register("debug collator", func(context.Context, []string) (string, error) {
		return "", nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err := New(testExtensionOptions(t, Options{
		Controller: &testController{}, ShardTops: &testShardTopSink{},
	}))(hooks.Node{Store: store, Commands: &commands})
	if err == nil {
		t.Fatal("constructor accepted a conflicting debug command")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.registrations != 0 {
		t.Fatalf("failed constructor registered %d accepted-state observers", store.registrations)
	}
}

func TestAcceptedFeedCloseDrainsCallbacksAndCanRetry(t *testing.T) {
	for _, name := range []string{"drain", "deadline and retry"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				warmer := &acceptedFeedPrewarmer{entered: make(chan struct{}), release: make(chan struct{})}
				f := newAcceptedFeedFixture(t, warmer)
				release := sync.OnceFunc(func() { close(warmer.release) })
				t.Cleanup(release)
				artifacts := acceptedFeedArtifacts(t)
				copiedObserver := f.store.snapshot()[0]
				published := make(chan struct{})
				go func() {
					f.store.publish(artifacts)
					close(published)
				}()
				<-warmer.entered

				ctx := t.Context()
				if name == "deadline and retry" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer cancel()
				}
				closed := make(chan error, 1)
				go func() { closed <- f.extension.Close(ctx) }()
				synctest.Wait()

				if len(f.store.snapshot()) != 0 {
					t.Fatal("Close retained the accepted-state subscription")
				}
				f.controller.mu.Lock()
				controllerClosed := f.controller.closed != nil
				f.controller.mu.Unlock()
				if controllerClosed {
					t.Fatal("controller closed while an accepted-state callback was active")
				}
				if name == "deadline and retry" {
					if err := <-closed; !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("Close during accepted callback = %v, want DeadlineExceeded", err)
					}
				} else {
					select {
					case err := <-closed:
						t.Fatalf("Close returned before the accepted callback drained: %v", err)
					default:
					}
				}

				release()
				<-published
				late := artifacts
				late.Block.SeqNo++
				if name == "deadline and retry" {
					copiedObserver(late)
					if got := f.feed.Stats().AcceptedFed; got != 1 {
						t.Fatalf("copied callback fed while closing: accepted=%d", got)
					}
					if err := f.extension.Close(t.Context()); err != nil {
						t.Fatalf("Close retry: %v", err)
					}
				} else if err := <-closed; err != nil {
					t.Fatal(err)
				}
				f.controller.mu.Lock()
				controllerClosed = f.controller.closed != nil
				f.controller.mu.Unlock()
				if !controllerClosed {
					t.Fatal("Close did not close the controller after the accepted callback drained")
				}
				copiedObserver(late)
				if got := f.feed.Stats().AcceptedFed; got != 1 {
					t.Fatalf("copied callback fed after close: accepted=%d", got)
				}
				f.store.mu.Lock()
				stops := f.store.stops
				f.store.mu.Unlock()
				if stops != 1 {
					t.Fatalf("accepted-state observer unregistered %d times, want 1", stops)
				}
			})
		})
	}
}
