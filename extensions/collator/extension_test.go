package collator

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	core "github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testNodeStore struct {
	hooks.Store
}

type testAccountPrewarmer struct {
	accounts []msgpool.AccountDestination
}

func (*testAccountPrewarmer) EnqueueRoot(cell.Hash) bool {
	return true
}

func (w *testAccountPrewarmer) EnqueueAccount(workchain int32, account [32]byte) bool {
	w.accounts = append(w.accounts, msgpool.AccountDestination{Workchain: workchain, Account: account})
	return true
}

func (*testAccountPrewarmer) PrewarmAccountNow(int32, [32]byte) bool {
	return true
}

type testController struct {
	mu sync.Mutex

	bootstrapErr error
	startErr     error
	closeErr     error
	statusErr    error
	status       core.ControllerStatus

	history   groups.MasterchainHistory
	buffered  []groups.BufferedMasterchainState
	startedAt time.Time
	started   context.Context
	closed    context.Context
	applied   []core.AppliedMasterchainState

	bootstrapEntered chan struct{}
	bootstrapRelease chan struct{}
}

func (c *testController) BootstrapMasterchain(
	ctx context.Context,
	history groups.MasterchainHistory,
	buffered []groups.BufferedMasterchainState,
	asOf time.Time,
) error {
	c.mu.Lock()
	c.history = history
	c.buffered = append([]groups.BufferedMasterchainState(nil), buffered...)
	c.startedAt = asOf
	entered := c.bootstrapEntered
	release := c.bootstrapRelease
	err := c.bootstrapErr
	c.mu.Unlock()

	if entered != nil {
		close(entered)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return err
}

func (c *testController) ApplyMasterchainState(
	_ context.Context,
	input core.AppliedMasterchainState,
) error {
	c.mu.Lock()
	c.applied = append(c.applied, input)
	c.mu.Unlock()

	return nil
}

func (c *testController) Start(ctx context.Context) error {
	c.mu.Lock()
	c.started = ctx
	err := c.startErr
	c.mu.Unlock()

	return err
}

func (c *testController) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closed = ctx
	err := c.closeErr
	c.mu.Unlock()

	return err
}

func (c *testController) Status(context.Context) (core.ControllerStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.status, c.statusErr
}

type testShardTopSink struct {
	ctx         context.Context
	description *p2p.ShardBlockDescription
	root        *cell.Cell
	err         error
}

func (s *testShardTopSink) StoreShardTopDescription(
	ctx context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	s.ctx = ctx
	s.description = description
	s.root = root

	return s.err
}

func testExtensionOptions(t *testing.T, options Options) Options {
	t.Helper()

	options.Messages = msgpool.New(msgpool.Config{})
	t.Cleanup(options.Messages.Close)
	options.Feed = msgpool.NewFeed(msgpool.FeedOptions{Pool: options.Messages})

	return options
}

func TestExtensionBootstrapsAndAppliesMasterchain(t *testing.T) {
	controller := &testController{}
	shardTops := &testShardTopSink{}
	store := &testNodeStore{}
	warmer := &testAccountPrewarmer{}
	extension, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  shardTops,
	}))(hooks.Node{Store: store, AccountPrewarmer: warmer})
	if err != nil {
		t.Fatal(err)
	}

	parent := testBlockID(10)
	buffered := testMasterchainEvent(11, parent)
	wantBuffered := *buffered.Meta.ID.Copy()
	if err = extension.OnBlockApplied(context.Background(), buffered); err != nil {
		t.Fatal(err)
	}
	buffered.Meta.ID.RootHash[0] ^= 0xff

	if err = extension.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	if controller.history != store || len(controller.buffered) != 1 ||
		!controller.buffered[0].Block.Equals(&wantBuffered) || controller.startedAt.IsZero() ||
		controller.started == nil {
		controller.mu.Unlock()
		t.Fatalf("bootstrap = history:%T buffered:%+v at:%v started:%v",
			controller.history, controller.buffered, controller.startedAt, controller.started)
	}
	controller.mu.Unlock()

	next := testMasterchainEvent(12, wantBuffered)
	if err = extension.OnBlockApplied(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	shard := testMasterchainEvent(13, next.Meta.ID)
	shard.Meta.ID.Workchain = 0
	if err = extension.OnBlockApplied(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	if len(controller.applied) != 1 || !controller.applied[0].Block.Equals(&next.Meta.ID) ||
		controller.applied[0].BlockRoot != next.BlockRoot ||
		controller.applied[0].StateRoot != next.CurrentState || controller.applied[0].AsOf.IsZero() {
		controller.mu.Unlock()
		t.Fatalf("applied masterchain states = %+v", controller.applied)
	}
	controller.mu.Unlock()

	message := testExternalMessage(0x42)
	if err = extension.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{
		SerializedSize: len(message.ToBOC()),
		MessageRoot:    message,
	}); err != nil {
		t.Fatal(err)
	}
	if err = extension.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{
		IsLocal:        true,
		SerializedSize: len(message.ToBOC()),
		MessageRoot:    message,
	}); err != nil {
		t.Fatal(err)
	}
	if pooled := extension.(*Extension).messages.Stats().Pooled; pooled != 1 {
		t.Fatalf("pooled external messages = %d, want 1", pooled)
	}
	destination := msgpool.AccountDestination{Workchain: 0, Account: [32]byte{0x42}}
	if len(warmer.accounts) != 1 || warmer.accounts[0] != destination {
		t.Fatalf("prewarmed external accounts = %+v, want %+v", warmer.accounts, destination)
	}
	appliedRoot := testAppliedBlockRoot(t, message)
	appliedExternal := hooks.BlockAppliedEvent{
		Meta: &storage.BlockMeta{
			ID: ton.BlockIDExt{
				Workchain: 0,
				Shard:     masterchainShard,
				SeqNo:     14,
				RootHash:  appliedRoot.Hash(),
				FileHash:  make([]byte, 32),
			},
			// Fresh: the pool feed defers a block older than its freshness
			// window to the settled-head sweep, exactly as it does for a
			// validator catching up.
			GenUTime: uint32(time.Now().Unix()),
		},
		BlockRoot: appliedRoot,
	}
	if err = extension.OnBlockApplied(context.Background(), appliedExternal); err != nil {
		t.Fatal(err)
	}
	if pooled := extension.(*Extension).messages.Stats().Pooled; pooled != 0 {
		t.Fatalf("pooled external messages after apply = %d, want 0", pooled)
	}
	if err = extension.OnBlockReceived(context.Background(), hooks.BlockReceivedEvent{}); err != nil {
		t.Fatal(err)
	}

	observer, ok := extension.(hooks.ShardTopBlockDescriptionObserver)
	if !ok {
		t.Fatal("collator extension has no shard-top observer")
	}
	description := &p2p.ShardBlockDescription{}
	root := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	observerCtx := context.WithValue(context.Background(), testContextKey{}, "shard-top")
	if err = observer.OnShardTopBlockDescription(observerCtx, description, root); err != nil {
		t.Fatal(err)
	}
	if shardTops.ctx != observerCtx || shardTops.description != description || shardTops.root != root {
		t.Fatal("collator extension changed shard-top sink values")
	}

	if err = extension.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	closed := controller.closed
	controller.mu.Unlock()
	if closed == nil {
		t.Fatal("controller was not closed")
	}
}

func TestExternalCapacityRejectionDoesNotPrewarm(t *testing.T) {
	messages := msgpool.New(msgpool.Config{MempoolLimit: 1})
	t.Cleanup(messages.Close)
	warmer := &testAccountPrewarmer{}
	extension, err := New(Options{
		Controller: &testController{},
		ShardTops:  &testShardTopSink{},
		Messages:   messages,
		Feed:       msgpool.NewFeed(msgpool.FeedOptions{Pool: messages}),
	})(hooks.Node{Store: &testNodeStore{}, AccountPrewarmer: warmer})
	if err != nil {
		t.Fatal(err)
	}

	for _, fill := range []byte{0x51, 0x52} {
		message := testExternalMessage(fill)
		if err = extension.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{
			SerializedSize: len(message.ToBOC()),
			MessageRoot:    message,
		}); err != nil {
			t.Fatal(err)
		}
	}

	stats := messages.Stats()
	if stats.Pooled != 1 || stats.OverflowMempool != 1 {
		t.Fatalf("external pool stats = %+v, want one pooled and one overflow", stats)
	}
	if len(warmer.accounts) != 1 || warmer.accounts[0].Account[0] != 0x51 {
		t.Fatalf("prewarmed external accounts = %+v, want only admitted destination", warmer.accounts)
	}
}

func TestExtensionBlocksPostStartHookUntilBootstrapCompletes(t *testing.T) {
	controller := &testController{
		bootstrapEntered: make(chan struct{}),
		bootstrapRelease: make(chan struct{}),
	}
	extension, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}})
	if err != nil {
		t.Fatal(err)
	}

	startResult := make(chan error, 1)
	go func() { startResult <- extension.Start(context.Background()) }()
	<-controller.bootstrapEntered

	applyResult := make(chan error, 1)
	event := testMasterchainEvent(20, testBlockID(19))
	go func() { applyResult <- extension.OnBlockApplied(context.Background(), event) }()
	select {
	case err = <-applyResult:
		t.Fatalf("hook completed before bootstrap: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(controller.bootstrapRelease)
	if err = <-startResult; err != nil {
		t.Fatal(err)
	}
	if err = <-applyResult; err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	applied := len(controller.applied)
	controller.mu.Unlock()
	if applied != 1 {
		t.Fatalf("applied states = %d, want 1", applied)
	}
	if err = extension.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExtensionDefersControllerUntilMasterchainStateIsAvailable(t *testing.T) {
	controller := &testController{bootstrapErr: groups.ErrNoSnapshot}
	extension, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = extension.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	event := testMasterchainEvent(20, testBlockID(19))
	if err = extension.OnBlockApplied(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	if controller.started != nil {
		controller.mu.Unlock()
		t.Fatal("controller started without a current masterchain state")
	}
	controller.bootstrapErr = nil
	controller.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for {
		controller.mu.Lock()
		started := controller.started != nil
		buffered := append([]groups.BufferedMasterchainState(nil), controller.buffered...)
		controller.mu.Unlock()
		if started {
			if len(buffered) != 1 || !buffered[0].Block.Equals(&event.Meta.ID) {
				t.Fatalf("deferred bootstrap buffered states = %+v", buffered)
			}

			break
		}
		if time.Now().After(deadline) {
			t.Fatal("controller did not start after a masterchain state became available")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err = extension.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExtensionCloseCancelsDeferredBootstrap(t *testing.T) {
	controller := &testController{bootstrapErr: groups.ErrNoSnapshot}
	extension, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}})
	if err != nil {
		t.Fatal(err)
	}
	if err = extension.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = extension.Start(context.Background()); err != nil {
		t.Fatalf("repeated deferred Start: %v", err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = extension.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	started := controller.started
	closed := controller.closed
	controller.mu.Unlock()
	if started != nil {
		t.Fatal("controller started while deferred bootstrap was closing")
	}
	if closed == nil {
		t.Fatal("controller was not closed after deferred bootstrap cancellation")
	}
}

func TestExtensionFailedStartIsCleanedByClose(t *testing.T) {
	startErr := errors.New("start failed")
	controller := &testController{startErr: startErr}
	extension, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}})
	if err != nil {
		t.Fatal(err)
	}

	if err = extension.Start(context.Background()); !errors.Is(err, startErr) {
		t.Fatalf("Start error = %v, want %v", err, startErr)
	}
	controller.mu.Lock()
	closed := controller.closed
	controller.mu.Unlock()
	if closed != nil {
		t.Fatal("Start closed the controller before its lifecycle owner")
	}
	if err = extension.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	closed = controller.closed
	controller.mu.Unlock()
	if closed == nil {
		t.Fatal("Close did not clean up a failed Start")
	}
}

func TestExtensionCloseDuringStartWaitsAndCleansController(t *testing.T) {
	controller := &testController{
		bootstrapEntered: make(chan struct{}),
		bootstrapRelease: make(chan struct{}),
	}
	extension, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}})
	if err != nil {
		t.Fatal(err)
	}

	startResult := make(chan error, 1)
	go func() { startResult <- extension.Start(context.Background()) }()
	<-controller.bootstrapEntered

	closeResult := make(chan error, 1)
	go func() { closeResult <- extension.Close(context.Background()) }()
	if err = <-closeResult; err != nil {
		t.Fatal(err)
	}
	if err = <-startResult; !errors.Is(err, context.Canceled) && !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Start error during Close = %v, want cancellation", err)
	}
	controller.mu.Lock()
	closed := controller.closed
	controller.mu.Unlock()
	if closed == nil {
		t.Fatal("Close racing Start did not clean up the controller")
	}
}

type testContextKey struct{}

func TestFactoryRegistersStatusCommand(t *testing.T) {
	completed := time.Date(2026, 8, 5, 12, 13, 14, 15, time.FixedZone("test", 4*60*60))
	controller := &testController{status: core.ControllerStatus{
		Started:          true,
		ActiveSessions:   2,
		FutureSessions:   3,
		BackendSessions:  2,
		ObserverSessions: 5,
		Backend: core.Status{
			Started:          true,
			ActiveWindows:    3,
			RetryingWindows:  2,
			CompletedWindows: 8,
			FailedWindows:    1,
			LastError:        "build failed",
			LastCompleted:    completed,
			Storage: core.StorageStatus{
				Sessions:      5,
				Candidates:    9,
				PendingWrites: 1,
				DB: core.DBMetrics{
					DiskSize:              2048,
					LiveSize:              1024,
					WALSize:               512,
					MemTableSize:          256,
					ReadAmp:               7,
					L0Files:               3,
					L0Sublevels:           2,
					CompactionDebt:        4096,
					CompactionsInProgress: 1,
				},
			},
		},
	}}
	var registry console.Registry
	_, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}, Commands: &registry})
	if err != nil {
		t.Fatal(err)
	}

	output, err := registry.Execute(context.Background(), "status collator")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"active_sessions=2 future_sessions=3 backend_sessions=2 observer_sessions=5",
		"started=true closing=false closed=false active_windows=3 retrying_windows=2",
		"completed_windows=8 failed_windows=1",
		"last_completed=2026-08-05T08:13:14.000000015Z",
		`last_error="build failed"`,
		"sessions=5 candidates=9 pending_writes=1",
		"disk=2.0KiB live=1.0KiB wal=512B memtable=256B",
		"compaction_debt=4.0KiB compactions_running=1",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("status output does not contain %q:\n%s", expected, output)
		}
	}

	if _, err = registry.Execute(context.Background(), "status collator extra"); !errors.Is(err, console.ErrNotFound) {
		t.Fatalf("status with arguments error = %v, want ErrNotFound", err)
	}
}

func TestFactoryValidationAndStatusErrors(t *testing.T) {
	store := &testNodeStore{}
	if _, err := New(Options{ShardTops: &testShardTopSink{}})(hooks.Node{Store: store}); err == nil {
		t.Fatal("nil controller accepted")
	}
	if _, err := New(Options{Controller: &testController{}})(hooks.Node{Store: store}); err == nil {
		t.Fatal("nil shard top sink accepted")
	}
	if _, err := New(testExtensionOptions(t, Options{
		Controller: &testController{},
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{}); err == nil {
		t.Fatal("nil node store accepted")
	}
	if _, err := New(Options{
		Controller: &testController{},
		ShardTops:  &testShardTopSink{},
	})(hooks.Node{Store: store}); err == nil {
		t.Fatal("nil message pool accepted")
	}

	var registry console.Registry
	if err := registry.Register("status collator", func(context.Context, []string) (string, error) {
		return "", nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := New(testExtensionOptions(t, Options{
		Controller: &testController{},
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: store, Commands: &registry}); err == nil {
		t.Fatal("status command conflict accepted")
	}

	statusErr := errors.New("status failed")
	var statusRegistry console.Registry
	if _, err := New(testExtensionOptions(t, Options{
		Controller: &testController{statusErr: statusErr},
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: store, Commands: &statusRegistry}); err != nil {
		t.Fatal(err)
	}
	if _, err := statusRegistry.Execute(context.Background(), "status collator"); !errors.Is(err, statusErr) {
		t.Fatalf("status error = %v, want %v", err, statusErr)
	}
}

func testMasterchainEvent(seqno uint32, previous ...ton.BlockIDExt) hooks.BlockAppliedEvent {
	id := testBlockID(seqno)
	return hooks.BlockAppliedEvent{
		Meta: &storage.BlockMeta{
			ID:       id,
			PrevRefs: append([]ton.BlockIDExt(nil), previous...),
		},
		BlockRoot:    cell.BeginCell().MustStoreUInt(uint64(seqno), 32).MustStoreBoolBit(true).EndCell(),
		CurrentState: cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell(),
	}
}

func testBlockID(seqno uint32) ton.BlockIDExt {
	root := make([]byte, 32)
	file := make([]byte, 32)
	root[0] = byte(seqno)
	root[31] = 1
	file[0] = byte(seqno)
	file[31] = 2

	return ton.BlockIDExt{
		Workchain: masterchainWorkchain,
		Shard:     masterchainShard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}

func testExternalMessage(fill byte) *cell.Cell {
	destination := make([]byte, 32)
	destination[0] = fill

	return cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreUInt(0b00, 2).
		MustStoreAddr(address.NewAddress(0, 0, destination)).
		MustStoreCoins(0).
		MustStoreUInt(0b01, 2).
		MustStoreRef(cell.BeginCell().MustStoreUInt(1, 64).EndCell()).
		EndCell()
}

func testAppliedBlockRoot(t *testing.T, messages ...*cell.Cell) *cell.Cell {
	t.Helper()

	dictionary, err := cell.NewAugDict(256, tlb.AugInMsgDescr{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range messages {
		value := cell.BeginCell().
			MustStoreUInt(0b000, 3).
			MustStoreRef(messages[i]).
			MustStoreRef(cell.BeginCell().MustStoreUInt(1, 1).EndCell()).
			EndCell()
		if err = dictionary.SetIntKey(new(big.Int).SetBytes(messages[i].Hash()), value); err != nil {
			t.Fatal(err)
		}
	}

	empty := cell.BeginCell().EndCell()
	extra := cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(dictionary.AsCell()).
		MustStoreRef(empty).
		MustStoreRef(empty).
		MustStoreRef(empty).
		EndCell()

	return cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreRef(empty).
		MustStoreRef(empty).
		MustStoreRef(empty).
		MustStoreRef(extra).
		EndCell()
}
