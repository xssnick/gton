package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/keyring"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type validatorTestStore struct {
	hooks.Store
	current *storage.CurrentState
	err     error
	states  map[groupReplayBlockKey]*storage.BlockState
	metas   map[groupReplayBlockKey]*storage.BlockMeta
}

type validatorTestCollator struct {
	mu         sync.Mutex
	started    bool
	closed     bool
	onStart    func()
	closeErrs  []error
	closeCalls int
}

type validatorTestMasterchainHead struct {
	block ton.BlockIDExt
	err   error
}

func (h *validatorTestMasterchainHead) SeenMasterchainBlock() (ton.BlockIDExt, error) {
	return h.block, h.err
}

func (*validatorTestCollator) CollatorID() [32]byte { return [32]byte{1} }

func (c *validatorTestCollator) Start(context.Context) error {
	c.mu.Lock()
	c.started = true
	onStart := c.onStart
	c.mu.Unlock()
	if onStart != nil {
		onStart()
	}

	return nil
}

func (c *validatorTestCollator) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	call := c.closeCalls
	c.closeCalls++
	if call < len(c.closeErrs) && c.closeErrs[call] != nil {
		return c.closeErrs[call]
	}
	c.closed = true

	return nil
}

func (*validatorTestCollator) Session(context.Context, [32]byte) (collator.SessionRecord, error) {
	return collator.SessionRecord{}, collator.ErrNotFound
}

func (*validatorTestCollator) PrepareSession(
	context.Context,
	collator.Session,
	collator.SessionUpdate,
) error {
	return nil
}

func (*validatorTestCollator) ActivateSession(context.Context, collator.SessionActivation) error {
	return nil
}

func (*validatorTestCollator) UpdateSession(context.Context, collator.SessionUpdate) error {
	return nil
}
func (*validatorTestCollator) RetireSession(context.Context, [32]byte) error           { return nil }
func (*validatorTestCollator) Probe(context.Context, collator.WindowPreparation) error { return nil }
func (*validatorTestCollator) CommitDelegation(context.Context, collator.WindowRequest) error {
	return nil
}
func (*validatorTestCollator) Status(context.Context) (collator.Status, error) {
	return collator.Status{}, nil
}

func (s validatorTestStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	return s.current, s.err
}

func (s validatorTestStore) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if state := s.states[groupReplayKey(block)]; state != nil {
		return storage.CloneBlockState(state), nil
	}
	if s.current != nil && s.current.Masterchain.Block.Equals(&block) {
		return storage.CloneBlockState(&s.current.Masterchain), nil
	}
	if s.Store != nil {
		return s.Store.BlockState(ctx, block)
	}

	return nil, storage.ErrNotFound
}

func (s validatorTestStore) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	if meta := s.metas[groupReplayKey(block)]; meta != nil {
		return meta.Clone(), nil
	}
	if s.Store != nil {
		return s.Store.BlockMeta(ctx, block)
	}

	return nil, storage.ErrNotFound
}

type blockingValidatorTestStore struct {
	hooks.Store
	entered chan struct{}
	release chan struct{}
}

type validatorTestStorage struct {
	ValidatorStorage

	mu        sync.Mutex
	status    StorageStatus
	statusErr error
	journals  map[validatorTestNamespace]*validatorTestJournal

	// namespaces is the durable session index the reaper enumerates, and
	// deletions is the ordered record of what it removed from it.
	namespaces  []SessionStorageID
	deletions   []SessionStorageID
	sessionsErr error
	deleteErr   error
	// undeletable is the per-namespace rejection DeleteSession answers with. The
	// store has one that no retry clears: a namespace whose summary row is
	// missing or undecodable is refused for as long as it exists.
	undeletable map[SessionStorageID]error
	attempts    int
	scans       int
}

func newValidatorTestStorage() *validatorTestStorage {
	return &validatorTestStorage{journals: make(map[validatorTestNamespace]*validatorTestJournal)}
}

// validatorTestNamespace mirrors pebblestore.namespaceForSession by hand: the
// consensus.dbId hash, plus the two path components the store wraps around it.
// It is derived here independently of the production key type so a test that
// keys a journal by it is pinning the reaper against the store's own identity
// rather than against the reaper's opinion of it.
type validatorTestNamespace struct {
	dbID          [32]byte
	shard         groups.ShardID
	catchainSeqno uint32
}

func validatorTestNamespaceOf(id SessionStorageID) validatorTestNamespace {
	// A store rejects every operation on an ID whose dbId cannot be derived, so
	// all such IDs collapsing onto one key here is faithful enough for a double.
	dbID, _ := id.Namespace()

	return validatorTestNamespace{dbID: dbID, shard: id.Shard, catchainSeqno: id.CatchainSeqno}
}

func (s *validatorTestStorage) Sessions(context.Context) ([]SessionStorageID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.scans++
	if s.sessionsErr != nil {
		return nil, s.sessionsErr
	}

	return append([]SessionStorageID(nil), s.namespaces...), nil
}

func (s *validatorTestStorage) DeleteSession(_ context.Context, id SessionStorageID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.attempts++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if err := s.undeletable[id]; err != nil {
		return err
	}
	for i, stored := range s.namespaces {
		if stored != id {
			continue
		}
		s.namespaces = append(s.namespaces[:i], s.namespaces[i+1:]...)
		s.deletions = append(s.deletions, id)
		// Deleting a namespace takes its vote journal with it, exactly as
		// pebblestore.DeleteSession does: it range-deletes the vote prefix and
		// marks the bound journal handle deleted. Modelling that here is what
		// makes "the live generation survived a pass" an assertion about the
		// double-vote record rather than about a bookkeeping slice.
		if journal := s.journals[validatorTestNamespaceOf(id)]; journal != nil {
			journal.wipe()
		}
		delete(s.journals, validatorTestNamespaceOf(id))

		return nil
	}

	return storage.ErrNotFound
}

func (s *validatorTestStorage) setNamespaces(ids ...SessionStorageID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.namespaces = append([]SessionStorageID(nil), ids...)
}

func (s *validatorTestStorage) failDelete(id SessionStorageID, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.undeletable == nil {
		s.undeletable = make(map[SessionStorageID]error)
	}
	s.undeletable[id] = err
}

func (s *validatorTestStorage) deleteAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.attempts
}

func (s *validatorTestStorage) storedNamespaces() []SessionStorageID {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]SessionStorageID(nil), s.namespaces...)
}

func (s *validatorTestStorage) deletedNamespaces() []SessionStorageID {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]SessionStorageID(nil), s.deletions...)
}

func (s *validatorTestStorage) scanCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.scans
}

func (s *validatorTestStorage) Journal(id SessionStorageID, _ int) simplex.Journal {
	s.mu.Lock()
	defer s.mu.Unlock()

	namespace := validatorTestNamespaceOf(id)
	journal := s.journals[namespace]
	if journal == nil {
		journal = &validatorTestJournal{}
		s.journals[namespace] = journal
	}

	return journal
}

// journalFor returns the journal bound to a namespace without creating one, so
// a test can tell "still there" from "recreated empty".
func (s *validatorTestStorage) journalFor(id SessionStorageID) *validatorTestJournal {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.journals[validatorTestNamespaceOf(id)]
}

func (s *validatorTestStorage) Status(context.Context) (StorageStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return cloneStorageStatus(s.status), s.statusErr
}

// validatorTestJournal is the durable double-vote record. It keeps the votes it
// was given so a test can assert that a reap pass left them there: an emptied
// journal is what lets a node vote twice in one slot.
type validatorTestJournal struct {
	mu    sync.Mutex
	votes []simplex.Vote
	wiped bool
}

type validatorTestClock struct{}

func (validatorTestClock) Now() time.Time {
	return time.Now()
}

func (*validatorTestJournal) Bootstrap() (*simplex.BootstrapState, error) {
	return &simplex.BootstrapState{}, nil
}

func (j *validatorTestJournal) SaveOurVote(vote simplex.Vote, done func(error)) {
	j.mu.Lock()
	j.votes = append(j.votes, vote)
	j.mu.Unlock()
	done(nil)
}

func (j *validatorTestJournal) wipe() {
	j.mu.Lock()
	j.votes = nil
	j.wiped = true
	j.mu.Unlock()
}

func (j *validatorTestJournal) recordedVotes() ([]simplex.Vote, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()

	return append([]simplex.Vote(nil), j.votes...), j.wiped
}

func (*validatorTestJournal) SaveCertificate(_ *simplex.Certificate, done func(error)) {
	done(nil)
}

func (*validatorTestJournal) SaveFirstNonAnnouncedWindow(_ uint32, done func(error)) {
	done(nil)
}

func (s *blockingValidatorTestStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	close(s.entered)
	<-s.release

	return nil, storage.ErrNotFound
}

func validatorTestNode() hooks.Node {
	return hooks.Node{
		Store:  validatorTestStore{err: storage.ErrNotFound},
		Logger: zerolog.Nop(),
	}
}

func validatorTestNodeWithGroups(t *testing.T) hooks.Node {
	t.Helper()

	config := groupReplayTestConfig(t, groupReplayTestBytes(71))
	head := groupReplayTestState(t, 100, true, true, 0, config, 72)
	store := newGroupReplayTestStore()
	store.add(head, groupReplayTestParent(99, 73))
	store.setCurrent(head)

	return hooks.Node{Store: store, Logger: zerolog.Nop()}
}

// appliedEvent wraps a block root into the full production-shaped event
// (the node always delivers the parsed root and metadata); the generation
// time is fresh so the block passes the freshness gate.
func appliedEvent(root *cell.Cell) hooks.BlockAppliedEvent {
	return hooks.BlockAppliedEvent{
		BlockRoot: root,
		Meta: &storage.BlockMeta{
			ID: ton.BlockIDExt{
				Workchain: 0, Shard: -0x8000000000000000, SeqNo: 1,
				RootHash: root.Hash(), FileHash: make([]byte, 32),
			},
			GenUTime: uint32(time.Now().Unix()),
		},
	}
}

var allShard = msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}

func validatorTestOptions(opts Options) Options {
	if opts.Keys == nil {
		seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
		privateKey := ed25519.NewKeyFromSeed(seed)
		keys, err := keyring.New(privateKey)
		if err != nil {
			panic(err)
		}
		clear(privateKey)
		opts.Keys = keys
	}
	if opts.Storage == nil {
		opts.Storage = newValidatorTestStorage()
	}
	if opts.Runtime == nil {
		opts.Runtime = newValidatorTestRuntime()
	}

	return opts
}

func newValidatorTestRuntime() *Runtime {
	runtime, err := NewRuntime(SharedRuntimeOptions{
		Messages: msgpool.Config{Clock: validatorTestClock{}},
	})
	if err != nil {
		panic(err)
	}

	return runtime
}

func newTestService(t *testing.T, opts Options) *Service {
	t.Helper()
	if opts.StatsInterval == 0 {
		opts.StatsInterval = -1
	}

	factory := New(validatorTestOptions(opts))
	ext, err := factory(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	s := ext.(*Service)
	if !opts.DisableInternals {
		if err = s.pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{allShard}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
		s.opts.Runtime.Close()
	})
	return s
}

func testAddr(fill byte) [32]byte {
	var a [32]byte
	for i := range a {
		a[i] = fill
	}
	return a
}

func extMsgCell(t testing.TB, addr [32]byte, tag uint64) *cell.Cell {
	t.Helper()
	return cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreUInt(0b00, 2).
		MustStoreAddr(address.NewAddress(0, 0, addr[:])).
		MustStoreCoins(0).
		MustStoreUInt(0b01, 2).
		MustStoreRef(cell.BeginCell().MustStoreUInt(tag, 64).EndCell()).
		EndCell()
}

// buildAppliedBlockRoot assembles a minimal block root whose BlockExtra
// carries a real InMsgDescr with msg_import_ext entries for the given
// messages.
func buildAppliedBlockRoot(t *testing.T, msgs []*cell.Cell) *cell.Cell {
	t.Helper()
	dict, err := cell.NewAugDict(256, tlb.AugInMsgDescr{})
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs {
		value := cell.BeginCell().
			MustStoreUInt(0b000, 3). // msg_import_ext
			MustStoreRef(msg).
			MustStoreRef(cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()).
			EndCell()
		if err = dict.SetIntKey(new(big.Int).SetBytes(msg.Hash()), value); err != nil {
			t.Fatal(err)
		}
	}
	stub := cell.BeginCell().EndCell()
	extra := cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(dict.AsCell()). // in_msg_descr
		MustStoreRef(stub).          // out_msg_descr
		MustStoreRef(stub).          // account_blocks
		MustStoreSlice(make([]byte, 64), 512).
		EndCell()
	return cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreUInt(0, 32). // global id
		MustStoreRef(stub).   // info
		MustStoreRef(stub).   // value flow
		MustStoreRef(stub).   // state update
		MustStoreRef(extra).
		EndCell()
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestExternalMessagesArePooledWithPriorities(t *testing.T) {
	s := newTestService(t, Options{})

	local := extMsgCell(t, testAddr(0x01), 1)
	remote := extMsgCell(t, testAddr(0x02), 2)

	if err := s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{
		IsLocal: false, MessageRoot: remote,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{
		IsLocal: true, MessageRoot: local,
	}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return s.pool.Stats().Pooled == 2 }, "messages not pooled")
	sel := s.pool.SelectForBlock(allShard, 0)
	if len(sel) != 2 {
		t.Fatalf("selected %d", len(sel))
	}
	// Local API submissions come first (higher priority).
	var localHash [32]byte
	copy(localHash[:], local.Hash())
	if sel[0].Hash != localHash {
		t.Fatal("local message must have the higher priority")
	}
}

func TestAppliedBlockCleansPool(t *testing.T) {
	s := newTestService(t, Options{})

	msgA := extMsgCell(t, testAddr(0x11), 1)
	msgB := extMsgCell(t, testAddr(0x12), 2)
	for _, m := range []*cell.Cell{msgA, msgB} {
		if err := s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{MessageRoot: m}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return s.pool.Stats().Pooled == 2 }, "messages not pooled")

	// The applied block imports only msgA.
	root := buildAppliedBlockRoot(t, []*cell.Cell{msgA})
	if err := s.OnBlockApplied(context.Background(), appliedEvent(root)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return s.pool.Stats().Pooled == 1 }, "applied external not erased")
	sel := s.pool.SelectForBlock(allShard, 0)
	var bHash [32]byte
	copy(bHash[:], msgB.Hash())
	if len(sel) != 1 || sel[0].Hash != bHash {
		t.Fatal("only the non-imported message must stay pooled")
	}
	if s.pool.Stats().AppliedDeleted != 1 {
		t.Fatal("applied cleanup counter")
	}
}

// TestAppliedBlockNormalizedCleanup: the pooled raw variant differs from the
// imported one (different import fee), cleanup still removes it through the
// normalized hash.
func TestAppliedBlockNormalizedCleanup(t *testing.T) {
	s := newTestService(t, Options{})

	addr := testAddr(0x21)
	body := cell.BeginCell().MustStoreUInt(7, 64).EndCell()
	build := func(fee uint64) *cell.Cell {
		return cell.BeginCell().
			MustStoreUInt(0b10, 2).
			MustStoreUInt(0b00, 2).
			MustStoreAddr(address.NewAddress(0, 0, addr[:])).
			MustStoreCoins(fee).
			MustStoreUInt(0b01, 2).
			MustStoreRef(body).
			EndCell()
	}
	pooled := build(0)
	imported := build(777)

	if err := s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{MessageRoot: pooled}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return s.pool.Stats().Pooled == 1 }, "message not pooled")

	root := buildAppliedBlockRoot(t, []*cell.Cell{imported})
	if err := s.OnBlockApplied(context.Background(), appliedEvent(root)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return s.pool.Stats().Pooled == 0 }, "normalized cleanup failed")
}

func TestMalformedBlockRootIsSwallowed(t *testing.T) {
	s := newTestService(t, Options{})

	// A malformed block root must be swallowed (logged), never errored:
	// erroring here would stall the node apply pipeline.
	bad := cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell()
	if err := s.OnBlockApplied(context.Background(), appliedEvent(bad)); err != nil {
		t.Fatal(err)
	}
	if err := s.OnBlockReceived(context.Background(), hooks.BlockReceivedEvent{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := s.pool.Stats().Pooled; got != 0 {
		t.Fatalf("nothing must be pooled, got %d", got)
	}
}

func TestMalformedMasterchainStateFailsBeforeLossyMailbox(t *testing.T) {
	options := validatorTestOptions(Options{
		DisableInternals: true,
		EnableGroups:     true,
		StatsInterval:    -1,
	})
	extension, err := New(options)(validatorTestNodeWithGroups(t))
	if err != nil {
		t.Fatal(err)
	}
	s := extension.(*Service)
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
		s.opts.Runtime.Close()
	})

	blockRoot := buildAppliedBlockRoot(t, nil)
	event := appliedEvent(blockRoot)
	event.Meta.ID.Workchain = -1
	event.Meta.ID.Shard = -1 << 63
	event.CurrentState = cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell()

	if err := s.OnBlockApplied(context.Background(), event); err == nil {
		t.Fatal("malformed masterchain state was accepted")
	}
}

// TestConcurrentHooks hammers the extension from many goroutines the way
// parallel shard applies do — run with -race.
func TestConcurrentHooks(t *testing.T) {
	s := newTestService(t, Options{})

	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := extMsgCell(t, testAddr(byte(i)), uint64(i))
			_ = s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{
				IsLocal: i%2 == 0, MessageRoot: msg,
			})
			if i%4 == 0 {
				root := buildAppliedBlockRoot(t, []*cell.Cell{extMsgCell(t, testAddr(byte(100+i)), uint64(i))})
				_ = s.OnBlockApplied(context.Background(), appliedEvent(root))
			}
		}(i)
	}
	wg.Wait()

	waitFor(t, func() bool { return s.pool.Stats().Pooled == n }, "all messages pooled")
}

func TestInternalsRequireEffectiveGroupTracking(t *testing.T) {
	event := appliedEvent(buildAppliedBlockRoot(t, nil))
	event.Meta.ID.Workchain = -1
	event.Meta.ID.Shard = -1 << 63
	event.CurrentState = cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell()

	t.Run("internals enable tracker", func(t *testing.T) {
		extension, err := New(validatorTestOptions(Options{StatsInterval: -1}))(validatorTestNodeWithGroups(t))
		if err != nil {
			t.Fatal(err)
		}
		s := extension.(*Service)
		if err = s.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = s.Close(ctx)
			s.opts.Runtime.Close()
		})
		if err := s.OnBlockApplied(context.Background(), event); err == nil {
			t.Fatal("internal-message feed silently ran without parsing its topology")
		}
	})
	t.Run("externals only may park tracker", func(t *testing.T) {
		s := newTestService(t, Options{DisableInternals: true})
		if err := s.OnBlockApplied(context.Background(), event); err != nil {
			t.Fatalf("parked tracker rejected masterchain state: %v", err)
		}
	})
}

func TestCloseIsGracefulAndIdempotent(t *testing.T) {
	factory := New(validatorTestOptions(Options{StatsInterval: -1, EnableGroups: true}))
	ext, err := factory(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	s := ext.(*Service)
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.Start(context.Background()); err == nil {
		t.Fatal("double start must fail")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Hooks after close are ignored quietly.
	if err = s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{
		MessageRoot: extMsgCell(t, testAddr(0x31), 1),
	}); err != nil {
		t.Fatal(err)
	}
	bad := cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell()
	masterchain := appliedEvent(bad)
	masterchain.Meta.ID.Workchain = -1
	masterchain.Meta.ID.Shard = -1 << 63
	masterchain.CurrentState = bad
	if err = s.OnBlockApplied(context.Background(), masterchain); err != nil {
		t.Fatalf("masterchain hook after close: %v", err)
	}
	if _, err = s.tracker.Snapshot(); !errors.Is(err, groups.ErrNoSnapshot) {
		t.Fatalf("tracker changed after close: %v", err)
	}
}

func TestServiceOwnsLocalCollatorLifecycle(t *testing.T) {
	localCollator := &validatorTestCollator{}
	options := validatorTestOptions(Options{
		DisableInternals: true,
		LocalCollator:    localCollator,
		StatsInterval:    -1,
	})
	extension, err := New(options)(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	localCollator.mu.Lock()
	started := localCollator.started
	closed := localCollator.closed
	localCollator.mu.Unlock()
	if !started || closed {
		t.Fatalf("local collator after start: started=%t closed=%t", started, closed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	localCollator.mu.Lock()
	closed = localCollator.closed
	localCollator.mu.Unlock()
	if !closed {
		t.Fatal("validator close did not stop the local collator")
	}
	options.Runtime.Close()
}

func TestStartDefersRecoveredConsensusServicesDuringCatchup(t *testing.T) {
	localCollator := &validatorTestCollator{}
	options := validatorTestOptions(Options{
		LocalCollator:   localCollator,
		PrepareSession:  newSupervisorTestPreparer().prepare,
		HeadSettleDelay: time.Hour,
		StatsInterval:   -1,
	})
	node := validatorTestNodeWithGroups(t)
	node.MasterchainHead = &validatorTestMasterchainHead{block: ton.BlockIDExt{SeqNo: 200}}
	extension, err := New(options)(node)
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if service.opts.ConsensusCatchupThreshold != 80*time.Second {
		t.Fatalf("consensus catch-up threshold = %s, want 80s", service.opts.ConsensusCatchupThreshold)
	}
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	localCollator.mu.Lock()
	started := localCollator.started
	localCollator.mu.Unlock()
	if started || service.consensusStarted {
		t.Fatal("recovered consensus services started from a stale startup snapshot")
	}
	if !service.consensusDeferred.Load() {
		t.Fatal("stale startup snapshot did not enter consensus catch-up gate")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	options.Runtime.Close()
}

func TestCloseRetriesLocalCollatorCleanup(t *testing.T) {
	closeErr := errors.New("persist accepted collator session")
	localCollator := &validatorTestCollator{closeErrs: []error{closeErr}}
	options := validatorTestOptions(Options{
		DisableInternals: true,
		LocalCollator:    localCollator,
		StatsInterval:    -1,
	})
	extension, err := New(options)(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); !errors.Is(err, closeErr) {
		t.Fatalf("first close error = %v, want %v", err, closeErr)
	}
	localCollator.mu.Lock()
	closed := localCollator.closed
	calls := localCollator.closeCalls
	localCollator.mu.Unlock()
	if closed || calls != 1 {
		t.Fatalf("local collator after failed close: closed=%t calls=%d", closed, calls)
	}

	if err = service.Close(ctx); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	localCollator.mu.Lock()
	closed = localCollator.closed
	calls = localCollator.closeCalls
	localCollator.mu.Unlock()
	if !closed || calls != 2 {
		t.Fatalf("local collator after retry: closed=%t calls=%d", closed, calls)
	}
	if err = service.Close(ctx); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	localCollator.mu.Lock()
	calls = localCollator.closeCalls
	localCollator.mu.Unlock()
	if calls != 2 {
		t.Fatalf("idempotent close calls = %d, want 2", calls)
	}
	options.Runtime.Close()
}

func TestCloseWaitsForShardHookAndRejectsLaterHooks(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()

	processing := s.sourceProcessing(allShard)
	processing.mu.Lock()
	locked := true
	defer func() {
		if locked {
			processing.mu.Unlock()
		}
	}()

	first := appliedEvent(feedBlockRoot(t, nil))
	first.CurrentState = feedStateRoot(t, nil)
	hookResult := make(chan error, 1)
	go func() {
		hookResult <- s.OnBlockApplied(context.Background(), first)
	}()

	waitFor(t, func() bool {
		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()

		return s.activeHooks == 1
	}, "shard hook did not enter the lifecycle gate")

	closeResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		closeResult <- s.Close(ctx)
	}()
	waitFor(t, func() bool {
		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()

		return s.closed
	}, "close did not start")

	select {
	case err := <-closeResult:
		t.Fatalf("close returned before the shard hook finished: %v", err)
	default:
	}

	processing.mu.Unlock()
	locked = false
	if err := <-hookResult; err != nil {
		t.Fatal(err)
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}

	top, err := internals.SourceTop(allShard, allShard)
	if err != nil || top.Seqno != 1 {
		t.Fatalf("in-flight shard hook was not drained, top=%+v err=%v", top, err)
	}

	second := appliedEvent(feedBlockRoot(t, nil))
	second.Meta.ID.SeqNo = 2
	second.CurrentState = feedStateRoot(t, nil)
	if err := s.OnBlockApplied(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if top, err = internals.SourceTop(allShard, allShard); err != nil || top.Seqno != 1 {
		t.Fatalf("post-close shard hook mutated the pool, top=%+v err=%v", top, err)
	}
}

func TestCloseDeadlineWhileStartIsBlocked(t *testing.T) {
	store := &blockingValidatorTestStore{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	extension, err := New(validatorTestOptions(Options{StatsInterval: -1, EnableGroups: true}))(hooks.Node{
		Store:  store,
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)

	startResult := make(chan error, 1)
	go func() {
		startResult <- service.Start(context.Background())
	}()
	<-store.entered

	deadlineCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- service.Close(deadlineCtx)
	}()

	select {
	case err = <-closeResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		close(store.release)
		<-startResult
		t.Fatal("close blocked behind Start")
	}

	close(store.release)
	if err = <-startResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("start error = %v, want context canceled", err)
	}

	ctx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClose()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryUsesSharedRuntime(t *testing.T) {
	opts := validatorTestOptions(Options{StatsInterval: -1})
	factory := New(opts)
	firstExtension, err := factory(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	secondExtension, err := factory(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	first := firstExtension.(*Service)
	second := secondExtension.(*Service)
	if first.pool != opts.Runtime.Messages || second.pool != opts.Runtime.Messages ||
		first.tracker != opts.Runtime.Groups || second.tracker != opts.Runtime.Groups {
		t.Fatal("factory replaced the injected shared runtime")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	opts.Runtime.Close()
}

func TestFactoryExposesShardTopObserverOnlyWhenConfigured(t *testing.T) {
	runtime := newValidatorTestRuntime()
	defer runtime.Close()

	base := validatorTestOptions(Options{Runtime: runtime, StatsInterval: -1})
	withoutInbox, err := New(base)(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := withoutInbox.(hooks.ShardTopBlockDescriptionObserver); ok {
		t.Fatal("validator exposed shard-top hook without an inbox")
	}

	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	withInboxOptions := base
	withInboxOptions.ShardTops = inbox
	withInbox, err := New(withInboxOptions)(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	observer, ok := withInbox.(hooks.ShardTopBlockDescriptionObserver)
	if !ok {
		t.Fatal("validator did not expose configured shard-top hook")
	}
	wrapped := observer.(*serviceWithShardTops)
	if wrapped.shardTops != inbox || wrapped.Service.opts.ShardTops != inbox {
		t.Fatal("validator shard-top hook does not use the local collator inbox")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = withoutInbox.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = withInbox.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestServiceCloseLeavesSharedRuntimeToCompositionRoot(t *testing.T) {
	opts := validatorTestOptions(Options{StatsInterval: -1})
	extension, err := New(opts)(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}

	message := extMsgCell(t, testAddr(0x61), 1)
	if err = opts.Runtime.Messages.AddExternal(nil, message, nil, msgpool.ExternalPriorityLocal); err != nil {
		t.Fatalf("shared message pool closed with validator service: %v", err)
	}
	opts.Runtime.Close()
	if err = opts.Runtime.Messages.AddExternal(nil, message, nil, msgpool.ExternalPriorityLocal); !errors.Is(err, msgpool.ErrClosed) {
		t.Fatalf("runtime close error = %v, want message pool closed", err)
	}
}

func TestFactoryUsesInjectedSigningKeys(t *testing.T) {
	opts := validatorTestOptions(Options{StatsInterval: -1})
	factory := New(opts)

	extension, err := factory(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if service.opts.Keys != opts.Keys {
		t.Fatal("factory did not retain the injected signing capability")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryUsesInjectedValidatorStorage(t *testing.T) {
	store := newValidatorTestStorage()
	opts := validatorTestOptions(Options{Storage: store, StatsInterval: -1})
	factory := New(opts)

	extension, err := factory(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if service.opts.Storage != store || service.validatorStore != store {
		t.Fatal("factory did not retain injected validator storage")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryRejectsInvalidOptions(t *testing.T) {
	missingStorage := validatorTestOptions(Options{})
	missingStorage.Storage = nil
	missingRuntime := validatorTestOptions(Options{})
	missingRuntime.Runtime = nil
	tests := []struct {
		name    string
		options Options
	}{
		{name: "missing signing keys", options: Options{}},
		{name: "missing storage", options: missingStorage},
		{name: "missing runtime", options: missingRuntime},
		{name: "negative head settle delay", options: validatorTestOptions(Options{HeadSettleDelay: -time.Second})},
		{name: "sessions without groups", options: validatorTestOptions(Options{
			DisableInternals: true,
			PrepareSession:   newSupervisorTestPreparer().prepare,
		})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.options)(validatorTestNode()); err == nil {
				t.Fatal("factory accepted invalid options")
			}
		})
	}
}

func TestNewRuntimeRejectsInvalidGroupPolicy(t *testing.T) {
	_, err := NewRuntime(SharedRuntimeOptions{Groups: groups.TrackerOptions{
		UnsafeRotations: []groups.UnsafeRotationRule{{CatchainSeqno: 7}},
	}})
	if err == nil {
		t.Fatal("runtime accepted an unsafe rotation with zero id")
	}
}

func TestStartLoadsCurrentStateBeforeLaunching(t *testing.T) {
	bad := cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell()
	current := &storage.CurrentState{Masterchain: storage.BlockState{
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     100,
			RootHash:  bad.Hash(),
			FileHash:  make([]byte, 32),
		},
		Cell: bad,
	}}
	extension, err := New(validatorTestOptions(Options{StatsInterval: -1, EnableGroups: true}))(hooks.Node{
		Store:  validatorTestStore{current: current},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err == nil {
		t.Fatal("start accepted malformed stored masterchain state")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStartContextCancellationStopsRuntime(t *testing.T) {
	extension, err := New(validatorTestOptions(Options{StatsInterval: -1}))(validatorTestNode())
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	runCtx, cancelRun := context.WithCancel(context.Background())
	if err = service.Start(runCtx); err != nil {
		t.Fatal(err)
	}

	cancelRun()
	select {
	case <-service.runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("validator runtime outlived its Start context")
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClose()
	if err = service.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

// feedInternalMsg builds a real int_msg_info$0 cell for internals-feed
// tests.
func feedInternalMsg(t testing.TB, dstFill byte, lt uint64) *cell.Cell {
	t.Helper()
	srcAddr := testAddr(0x0e)
	src := address.NewAddress(0, 0, srcAddr[:])
	dstAddr := testAddr(dstFill)
	return cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBoolBit(true).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreAddr(src).
		MustStoreAddr(address.NewAddress(0, 0, dstAddr[:])).
		MustStoreCoins(1_000_000).
		MustStoreBoolBit(false).
		MustStoreCoins(0).
		MustStoreCoins(1_000).
		MustStoreUInt(lt, 64).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		EndCell()
}

func feedEnvelope(t testing.TB, msg *cell.Cell) *cell.Cell {
	t.Helper()
	env, err := tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: tlb.MustFromTON("0.001"),
		Msg:             msg,
	}.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func feedQueueKey(t testing.TB, msg *cell.Cell, dstFill byte) msgpool.QueueKey {
	t.Helper()
	dstAddr := testAddr(dstFill)
	hop, err := msgpool.AccountPrefixFromAddress(address.NewAddress(0, 0, dstAddr[:]))
	if err != nil {
		t.Fatal(err)
	}
	return msgpool.MakeQueueKey(hop, msg.HashKey())
}

// feedStateRoot wraps queued envelopes into a minimal ShardStateUnsplit
// that stores the queue size, the way mainnet states do.
func feedStateRoot(t testing.TB, queued map[msgpool.QueueKey]tlb.EnqueuedMsg) *cell.Cell {
	t.Helper()
	dict, err := cell.NewAugDict(352, tlb.AugOutMsgQueue{})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range queued {
		valueCell, err := value.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
		if _, err = dict.SetWithMode(keyCell, valueCell, cell.DictSetModeAdd); err != nil {
			t.Fatal(err)
		}
	}
	size := uint64(len(queued))
	dispatchQueue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	extra, err := tlb.OutMsgQueueExtra{
		DispatchQueue: dispatchQueue,
		OutQueueSize:  &size,
	}.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	queueInfo := cell.BeginCell().
		MustStoreBuilder(dict.AsCell().ToBuilder()).
		MustStoreBoolBit(false). // proc_info: empty HashmapE
		MustStoreBoolBit(true).
		MustStoreBuilder(extra.ToBuilder()).
		EndCell()
	return cell.BeginCell().
		MustStoreUInt(0x9023afe2, 32).
		MustStoreRef(queueInfo).
		EndCell()
}

// feedBlockRoot builds a block root with a valid empty InMsgDescr and the
// given export_new internals in OutMsgDescr.
func feedBlockRoot(t testing.TB, exported map[*cell.Cell]*cell.Cell) *cell.Cell {
	t.Helper()
	inDict, err := cell.NewAugDict(256, tlb.AugInMsgDescr{})
	if err != nil {
		t.Fatal(err)
	}
	outDict, err := cell.NewAugDict(256, tlb.AugOutMsgDescr{})
	if err != nil {
		t.Fatal(err)
	}
	for msg, env := range exported {
		value := cell.BeginCell().
			MustStoreUInt(0b001, 3). // msg_export_new
			MustStoreRef(env).
			MustStoreRef(cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()).
			EndCell()
		if err = outDict.SetIntKey(new(big.Int).SetBytes(msg.Hash()), value); err != nil {
			t.Fatal(err)
		}
	}
	stub := cell.BeginCell().EndCell()
	extra := cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(inDict.AsCell()).
		MustStoreRef(outDict.AsCell()).
		MustStoreRef(stub).
		MustStoreSlice(make([]byte, 64), 512).
		EndCell()
	return cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreUInt(0, 32).
		MustStoreRef(stub).
		MustStoreRef(stub).
		MustStoreRef(stub).
		MustStoreRef(extra).
		EndCell()
}

func feedRef(root *cell.Cell, seqno uint32) msgpool.SourceRef {
	ref := msgpool.SourceRef{Seqno: seqno}
	copy(ref.RootHash[:], root.Hash())
	return ref
}

func TestInternalsFeedSeedsAppliesAndHealsGaps(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()

	// Block 1 arrives with its post-state: the source is unseen, the run
	// seeds from the state queue.
	msgA := feedInternalMsg(t, 0x22, 1000)
	state1 := feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgA, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msgA)},
	})
	block1 := feedBlockRoot(t, nil)
	ev1 := appliedEvent(block1)
	ev1.CurrentState = state1
	if err := s.OnBlockApplied(context.Background(), ev1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		top, err := internals.SourceTop(allShard, allShard)
		return err == nil && top == feedRef(block1, 1)
	}, "run not seeded from the first applied block")

	// Block 2 extends the run through the delta fast path: no state is
	// attached, so a reseed fallback would visibly drop the source.
	msgB := feedInternalMsg(t, 0x44, 2000)
	block2 := feedBlockRoot(t, map[*cell.Cell]*cell.Cell{msgB: feedEnvelope(t, msgB)})
	ev2 := appliedEvent(block2)
	ev2.Meta.ID.SeqNo = 2
	ev2.CurrentState = nil
	if err := s.OnBlockApplied(context.Background(), ev2); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		top, err := internals.SourceTop(allShard, allShard)
		return err == nil && top.Seqno == 2
	}, "delta fast path did not advance the run")

	cut, err := internals.Cut(allShard, msgpool.CutRequest{Sources: map[msgpool.ShardIdent]msgpool.CutSource{
		allShard: {Visible: feedRef(block2, 2)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 2 || cut.Messages[0].EnqueuedLT != 1000 || cut.Messages[1].EnqueuedLT != 2000 {
		t.Fatalf("cut after fast path: %+v", cut.Messages)
	}

	// Block 4 skips a seqno — impossible with the synchronous feed, but
	// ApplyBlock's own continuity guard still turns it into a reseed from
	// the attached state.
	msgC := feedInternalMsg(t, 0x55, 3000)
	state4 := feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgC, 0x55): {EnqueuedLT: 3000, Msg: feedEnvelope(t, msgC)},
	})
	block4 := feedBlockRoot(t, nil)
	ev4 := appliedEvent(block4)
	ev4.Meta.ID.SeqNo = 4
	ev4.CurrentState = state4
	if err = s.OnBlockApplied(context.Background(), ev4); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		top, err := internals.SourceTop(allShard, allShard)
		return err == nil && top.Seqno == 4
	}, "gap did not trigger a reseed")

	cut, err = internals.Cut(allShard, msgpool.CutRequest{Sources: map[msgpool.ShardIdent]msgpool.CutSource{
		allShard: {Visible: feedRef(block4, 4)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 || cut.Messages[0].EnqueuedLT != 3000 {
		t.Fatalf("cut after reseed: %+v", cut.Messages)
	}
	if st := internals.Stats(); st.Seeds != 2 || st.AppliedBlocks != 1 {
		t.Fatalf("internals stats: %+v", st)
	}
}

func TestInternalsTopologyPrunesObsoleteSources(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()
	left := msgpool.ShardIdent{Workchain: 0, Shard: 0x4000000000000000}
	right := msgpool.ShardIdent{Workchain: 0, Shard: 0xc000000000000000}
	master := msgpool.ShardIdent{Workchain: -1, Shard: msgpool.ShardAll}

	current := &groups.Snapshot{Active: []groups.Session{{
		Shard: groups.ShardID{Workchain: left.Workchain, Shard: int64(left.Shard)},
		Registered: []groups.ShardDescription{{
			Shard: groups.ShardID{Workchain: allShard.Workchain, Shard: int64(allShard.Shard)},
		}},
	}}}
	if err := s.reconcileInternals(current); err != nil {
		t.Fatal(err)
	}
	for index, source := range []msgpool.ShardIdent{allShard, left, right, master} {
		ref := msgpool.SourceRef{Seqno: 1, RootHash: [32]byte{byte(index + 1)}}
		if err := internals.Seed(left, source, ref, nil, 0); err != nil {
			t.Fatal(err)
		}
	}

	next := &groups.Snapshot{Active: []groups.Session{{
		Shard: groups.ShardID{Workchain: left.Workchain, Shard: int64(left.Shard)},
		Registered: []groups.ShardDescription{{
			Shard: groups.ShardID{Workchain: left.Workchain, Shard: int64(left.Shard)},
		}},
	}}, Future: []groups.Session{{
		Shard: groups.ShardID{Workchain: right.Workchain, Shard: int64(right.Shard)},
	}}}
	if err := s.reconcileInternals(next); err != nil {
		t.Fatal(err)
	}
	s.internalsTopologyMu.RLock()
	futureIsSource := s.internalsTopology.ContainsSource(right)
	s.internalsTopologyMu.RUnlock()
	if futureIsSource {
		t.Fatal("future destination became an internal-message source before activation")
	}
	for _, source := range []msgpool.ShardIdent{allShard, right} {
		if _, err := internals.SourceTop(left, source); !errors.Is(err, msgpool.ErrNotFound) {
			t.Fatalf("obsolete source %+v survived topology change: %v", source, err)
		}
	}
	for _, source := range []msgpool.ShardIdent{left, master} {
		if _, err := internals.SourceTop(left, source); err != nil {
			t.Fatalf("live source %+v was pruned: %v", source, err)
		}
	}

	obsolete := appliedEvent(cell.BeginCell().MustStoreUInt(0xbad, 32).EndCell())
	obsolete.Meta.ID.Shard = int64(allShard.Shard)
	obsolete.Meta.ID.SeqNo = 2
	s.feedInternals(&sourceProcessing{}, obsolete)
	if _, err := internals.SourceTop(left, allShard); !errors.Is(err, msgpool.ErrNotFound) {
		t.Fatalf("obsolete source was restored by a late block: %v", err)
	}
}

// reconcileInternals runs synchronously on every masterchain apply, while the
// projection it computes only moves on a split, a merge or a session rotation.
// The guard that keeps the sweep off the apply path has to be exact in both
// directions: an unchanged shard configuration must skip it, and a change that
// leaves the destination list alone must still run it.
func TestInternalsTopologySkipsOnlyUnchangedProjections(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()
	left := msgpool.ShardIdent{Workchain: 0, Shard: 0x4000000000000000}
	right := msgpool.ShardIdent{Workchain: 0, Shard: 0xc000000000000000}

	snapshot := func(registered msgpool.ShardIdent) *groups.Snapshot {
		return &groups.Snapshot{Active: []groups.Session{{
			Shard: groups.ShardID{Workchain: left.Workchain, Shard: int64(left.Shard)},
			Registered: []groups.ShardDescription{{
				Shard: groups.ShardID{Workchain: registered.Workchain, Shard: int64(registered.Shard)},
			}},
		}}}
	}

	if err := s.reconcileInternals(snapshot(allShard)); err != nil {
		t.Fatal(err)
	}

	// A source seeded between two reconciles survives an identical one. That is
	// the visible consequence of skipping, and it is memory-only: a cut serves
	// just the sources its request names.
	ref := msgpool.SourceRef{Seqno: 1, RootHash: [32]byte{0x01}}
	if err := internals.Seed(left, right, ref, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.reconcileInternals(snapshot(allShard)); err != nil {
		t.Fatal(err)
	}
	if _, err := internals.SourceTop(left, right); err != nil {
		t.Fatalf("an unchanged projection swept the pool: %v", err)
	}

	// Same destination list, different registered set. Comparing destinations
	// alone would call this unchanged and leave a rotated-away neighbor feeding
	// the pool.
	if err := s.reconcileInternals(snapshot(left)); err != nil {
		t.Fatal(err)
	}
	if _, err := internals.SourceTop(left, right); !errors.Is(err, msgpool.ErrNotFound) {
		t.Fatalf("a changed registered set did not sweep: %v", err)
	}
}

// TestInternalsFeedReseedsOnQueueSizeMismatch: the runtime invariant — a
// block whose descriptors disagree with the post-state queue size (the
// simulated parsing-bug case) trips the size check and the run reseeds
// from the state.
func TestInternalsFeedReseedsOnQueueSizeMismatch(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()

	msgA := feedInternalMsg(t, 0x22, 1000)
	state1 := feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgA, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msgA)},
	})
	block1 := feedBlockRoot(t, nil)
	ev1 := appliedEvent(block1)
	ev1.CurrentState = state1
	if err := s.OnBlockApplied(context.Background(), ev1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		cut, err := internals.Cut(allShard, msgpool.CutRequest{Sources: map[msgpool.ShardIdent]msgpool.CutSource{
			allShard: {Visible: feedRef(block1, 1)},
		}})
		return err == nil && len(cut.Messages) == 1
	}, "run not seeded from the first state")

	// Block 2 carries no exports in its descriptors while its post-state
	// holds a second message: tracked 1 vs stored 2 must reseed.
	msgB := feedInternalMsg(t, 0x33, 2000)
	state2 := feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgA, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msgA)},
		feedQueueKey(t, msgB, 0x33): {EnqueuedLT: 2000, Msg: feedEnvelope(t, msgB)},
	})
	block2 := feedBlockRoot(t, nil)
	ev2 := appliedEvent(block2)
	ev2.Meta.ID.SeqNo = 2
	ev2.CurrentState = state2
	if err := s.OnBlockApplied(context.Background(), ev2); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		cut, err := internals.Cut(allShard, msgpool.CutRequest{Sources: map[msgpool.ShardIdent]msgpool.CutSource{
			allShard: {Visible: feedRef(block2, 2)},
		}})
		return err == nil && len(cut.Messages) == 2
	}, "size mismatch did not reseed the run")

	// The size check fires inside ApplyBlock: the drifted delta is never
	// committed (no applied block counted), the source untracks and the
	// reseed reinstalls the exact view.
	if st := internals.Stats(); st.Seeds != 2 || st.AppliedBlocks != 0 {
		t.Fatalf("stats after mismatch reseed: %+v", st)
	}
}

// TestStaleBlocksSkipPoolProcessing: catch-up blocks (generation time
// outside the freshness window) touch neither the externals pool nor the
// internals section; the first fresh block re-arms the feed.
func TestStaleBlocksSkipPoolProcessing(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()

	pooled := extMsgCell(t, testAddr(0x41), 1)
	if err := s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{MessageRoot: pooled}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return s.pool.Stats().Pooled == 1 }, "message not pooled")

	// A stale block importing the pooled message with a state behind it:
	// everything is skipped.
	stale := appliedEvent(buildAppliedBlockRoot(t, []*cell.Cell{pooled}))
	stale.Meta.GenUTime = uint32(time.Now().Add(-time.Hour).Unix())
	stale.CurrentState = feedStateRoot(t, nil)
	if err := s.OnBlockApplied(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if s.pool.Stats().Pooled != 1 {
		t.Fatal("stale block must not clean the pool")
	}
	if _, err := internals.SourceTop(allShard, allShard); !errors.Is(err, msgpool.ErrNotFound) {
		t.Fatal("stale block must not seed internals")
	}

	// The first fresh block processes normally.
	fresh := appliedEvent(buildAppliedBlockRoot(t, []*cell.Cell{pooled}))
	fresh.Meta.ID.SeqNo = 2
	fresh.CurrentState = feedStateRoot(t, nil)
	if err := s.OnBlockApplied(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if s.pool.Stats().Pooled != 0 {
		t.Fatal("fresh block must clean the pool")
	}
	if top, err := internals.SourceTop(allShard, allShard); err != nil || top.Seqno != 2 {
		t.Fatalf("fresh block must seed internals, top=%+v err=%v", top, err)
	}
}

func TestConsensusAdmissionWaitsForCurrentSnapshotAndStaysLatched(t *testing.T) {
	supervisor := newSessionSupervisor(zerolog.Nop(), nil, nil, nil)
	service := &Service{
		log:      zerolog.Nop(),
		opts:     Options{ConsensusCatchupThreshold: 80 * time.Second},
		sessions: supervisor,
	}

	stale := &groups.Snapshot{
		MasterchainBlock: ton.BlockIDExt{SeqNo: 10},
		GenUTime:         uint32(time.Now().Add(-time.Hour).Unix()),
	}
	service.reconcileConsensus(stale)
	if supervisor.hasSnapshot {
		t.Fatal("stale startup snapshot reached consensus supervisor")
	}

	fresh := &groups.Snapshot{
		MasterchainBlock: ton.BlockIDExt{SeqNo: 11},
		GenUTime:         uint32(time.Now().Unix()),
	}
	service.reconcileConsensus(fresh)
	if !supervisor.hasSnapshot || supervisor.highestSeqno != 11 {
		t.Fatalf("fresh snapshot not admitted: has=%t seqno=%d", supervisor.hasSnapshot, supervisor.highestSeqno)
	}

	// The gate is startup-only. C++ does not tear down validator groups merely
	// because an already participating node experiences transient lag.
	staleAfterAdmission := &groups.Snapshot{
		MasterchainBlock: ton.BlockIDExt{SeqNo: 12},
		GenUTime:         stale.GenUTime,
	}
	service.reconcileConsensus(staleAfterAdmission)
	if supervisor.highestSeqno != 12 {
		t.Fatalf("latched consensus did not accept newer snapshot: seqno=%d", supervisor.highestSeqno)
	}
}

func TestConsensusAdmissionWaitsForSignedHeadAndStartsOnHaltedNetwork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	head := &validatorTestMasterchainHead{block: ton.BlockIDExt{SeqNo: 20}}
	localCollator := &validatorTestCollator{}
	supervisor := newSessionSupervisor(zerolog.Nop(), nil, nil, nil)
	defer supervisor.Close()
	service := &Service{
		log:             zerolog.Nop(),
		opts:            Options{ConsensusCatchupThreshold: 80 * time.Second, HeadSettleDelay: time.Second},
		sessions:        supervisor,
		localCollator:   localCollator,
		masterchainHead: head,
		runCtx:          ctx,
	}
	stale := &groups.Snapshot{
		MasterchainBlock: ton.BlockIDExt{SeqNo: 10},
		GenUTime:         uint32(time.Now().Add(-time.Hour).Unix()),
	}
	if service.reconcileConsensus(stale) {
		t.Fatal("stale snapshot was admitted immediately")
	}
	service.consensusMu.Lock()
	service.consensusPending.at = time.Now().Add(-2 * service.opts.HeadSettleDelay)
	service.consensusMu.Unlock()
	if err := service.processSettledConsensus(); err != nil {
		t.Fatal(err)
	}
	if service.consensusAdmitted.Load() || supervisor.hasSnapshot {
		t.Fatal("local catch-up was admitted while a newer signed head was known")
	}
	localCollator.mu.Lock()
	started := localCollator.started
	localCollator.mu.Unlock()
	if started {
		t.Fatal("local collator started during catch-up")
	}

	// Once the local state reaches the newest signed head, its old generation
	// time describes a halted network rather than local lag. The settled head
	// must start production so validators can resume that network.
	head.block.SeqNo = stale.MasterchainBlock.SeqNo
	if err := service.processSettledConsensus(); err != nil {
		t.Fatal(err)
	}
	supervisor.mu.Lock()
	hasSnapshot := supervisor.hasSnapshot
	supervisor.mu.Unlock()
	if !service.consensusAdmitted.Load() || !hasSnapshot {
		t.Fatal("halted network head did not admit consensus")
	}
	localCollator.mu.Lock()
	started = localCollator.started
	localCollator.mu.Unlock()
	if !started {
		t.Fatal("local collator did not start after halted-network admission")
	}
}

func TestConsensusAdmissionCatchupStreamResetsSettlement(t *testing.T) {
	supervisor := newSessionSupervisor(zerolog.Nop(), nil, nil, nil)
	service := &Service{
		log:      zerolog.Nop(),
		opts:     Options{ConsensusCatchupThreshold: 80 * time.Second, HeadSettleDelay: time.Second},
		sessions: supervisor,
	}
	first := &groups.Snapshot{
		MasterchainBlock: ton.BlockIDExt{SeqNo: 10},
		GenUTime:         uint32(time.Now().Add(-time.Hour).Unix()),
	}
	service.reconcileConsensus(first)
	service.consensusMu.Lock()
	service.consensusPending.at = time.Now().Add(-2 * service.opts.HeadSettleDelay)
	service.consensusMu.Unlock()

	second := &groups.Snapshot{
		MasterchainBlock: ton.BlockIDExt{SeqNo: 11},
		GenUTime:         first.GenUTime + 1,
	}
	service.reconcileConsensus(second)
	if err := service.processSettledConsensus(); err != nil {
		t.Fatal(err)
	}
	if service.consensusAdmitted.Load() || supervisor.hasSnapshot {
		t.Fatal("superseding catch-up snapshot did not reset head settlement")
	}
}

// TestFeedInternalsRecoveryBranches pins the four failure branches of the
// feed: a delta that cannot be parsed and a post-state whose out-queue size
// cannot be read both downgrade every apply target to a reseed, and a reseed
// that has no usable state — either none was attached or the attached one
// cannot be walked — drops the source instead of leaving a stale run behind.
func TestFeedInternalsRecoveryBranches(t *testing.T) {
	// A cell that carries neither the block nor the shard-state magic, so
	// both the delta parse and the state walk reject it outright.
	malformed := func(t testing.TB) *cell.Cell {
		t.Helper()

		return cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell()
	}
	msg := func(t testing.TB) *cell.Cell {
		t.Helper()

		return feedInternalMsg(t, 0x22, 1000)
	}

	for _, tt := range []struct {
		name string
		// seqno of the applied block relative to the seeded run at 1.
		seqno     uint32
		blockRoot func(testing.TB) *cell.Cell
		// state is nil when the applied block carries no post-state.
		state       func(testing.TB) *cell.Cell
		wantDropped bool
		wantMessage bool
	}{
		{
			name:      "delta parse failure reseeds from the state",
			seqno:     2,
			blockRoot: malformed,
			state: func(t testing.TB) *cell.Cell {
				exported := msg(t)

				return feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
					feedQueueKey(t, exported, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, exported)},
				})
			},
			wantMessage: true,
		},
		{
			name:        "delta parse failure without a state drops the source",
			seqno:       2,
			blockRoot:   malformed,
			wantDropped: true,
		},
		{
			name:        "size verification failure drops the source it cannot reseed",
			seqno:       2,
			blockRoot:   func(t testing.TB) *cell.Cell { return feedBlockRoot(t, nil) },
			state:       malformed,
			wantDropped: true,
		},
		{
			name:        "same-height fork without a state drops the source",
			seqno:       1,
			blockRoot:   func(t testing.TB) *cell.Cell { return feedBlockRoot(t, nil) },
			wantDropped: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestService(t, Options{})
			internals := s.pool.Internals()

			// A tracked run at seqno 1 whose root differs from every block
			// below: seqno 2 becomes an apply target, seqno 1 a same-height
			// fork.
			seeded := msgpool.SourceRef{Seqno: 1}
			seeded.RootHash[0] = 0xff
			if err := internals.Seed(allShard, allShard, seeded, nil, 0); err != nil {
				t.Fatal(err)
			}

			event := appliedEvent(tt.blockRoot(t))
			event.Meta.ID.SeqNo = tt.seqno
			if tt.state != nil {
				event.CurrentState = tt.state(t)
			}
			s.feedInternals(&sourceProcessing{}, event)

			top, err := internals.SourceTop(allShard, allShard)
			if tt.wantDropped {
				if !errors.Is(err, msgpool.ErrNotFound) {
					t.Fatalf("source must be dropped: top=%+v err=%v", top, err)
				}

				return
			}
			if want := feedRef(event.BlockRoot, tt.seqno); err != nil || top != want {
				t.Fatalf("run not reseeded from the applied state: top=%+v want=%+v err=%v", top, want, err)
			}

			cut, cutErr := internals.Cut(allShard, msgpool.CutRequest{Sources: map[msgpool.ShardIdent]msgpool.CutSource{
				allShard: {Visible: top},
			}})
			if cutErr != nil {
				t.Fatal(cutErr)
			}
			if tt.wantMessage != (len(cut.Messages) == 1) {
				t.Fatalf("reseeded cut = %+v", cut.Messages)
			}
			// The delta never committed: the recovery path seeds, it does
			// not apply.
			if st := internals.Stats(); st.AppliedBlocks != 0 {
				t.Fatalf("recovery must not count an applied block: %+v", st)
			}
		})
	}
}

func TestFeedInternalsReplacesSameHeightForkFromAppliedState(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()
	preloaded := msgpool.SourceRef{Seqno: 1}
	preloaded.RootHash[0] = 0xff
	if err := internals.Seed(allShard, allShard, preloaded, nil, 0); err != nil {
		t.Fatal(err)
	}

	event := appliedEvent(feedBlockRoot(t, nil))
	event.Meta.ID.SeqNo = 1
	event.CurrentState = feedStateRoot(t, nil)
	s.feedInternals(&sourceProcessing{}, event)

	want := feedRef(event.BlockRoot, 1)
	top, err := internals.SourceTop(allShard, allShard)
	if err != nil || top != want {
		t.Fatalf("same-height applied fork did not replace preloaded view: top=%+v want=%+v err=%v", top, want, err)
	}
}

// TestSettledStaleHeadCannotRunAfterFreshBlock fixes the exact ordering where
// the settle loop has already selected N, then the apply hook commits N+1
// before N enters source bookkeeping. N must be rejected as a whole: it may
// neither clean externals nor reseed/drop the internal source behind N+1.
func TestSettledStaleHeadCannotRunAfterFreshBlock(t *testing.T) {
	s := newTestService(t, Options{})
	internals := s.pool.Internals()

	msg := feedInternalMsg(t, 0x22, 1000)
	fresh := appliedEvent(feedBlockRoot(t, nil))
	fresh.Meta.ID.SeqNo = 2
	fresh.CurrentState = feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msg, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msg)},
	})
	if err := s.OnBlockApplied(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}

	pooled := extMsgCell(t, testAddr(0x42), 1)
	if err := s.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{MessageRoot: pooled}); err != nil {
		t.Fatal(err)
	}
	if got := s.pool.Stats().Pooled; got != 1 {
		t.Fatalf("pooled externals = %d, want 1", got)
	}

	stale := appliedEvent(buildAppliedBlockRoot(t, []*cell.Cell{pooled}))
	stale.CurrentState = nil
	if processed := s.processSourceBlock(allShard, stale); processed {
		t.Fatal("settled stale head ran after a newer block")
	}
	if got := s.pool.Stats().Pooled; got != 1 {
		t.Fatalf("stale head cleaned %d pooled externals", 1-got)
	}

	top, err := internals.SourceTop(allShard, allShard)
	if err != nil || top != feedRef(fresh.BlockRoot, 2) {
		t.Fatalf("stale head regressed or dropped source, top=%+v err=%v", top, err)
	}
	if stats := internals.Stats(); stats.Seeds != 1 {
		t.Fatalf("stale head reseeded internals: %+v", stats)
	}
}

// TestHaltedChainHeadSettles: a stale head nobody supersedes (the chain
// halted, the node restarted behind an old head) settles after the delay
// and arms the pool — the collator of the resurrection block needs exactly
// that view.
func TestHaltedChainHeadSettles(t *testing.T) {
	s := newTestService(t, Options{HeadSettleDelay: 30 * time.Millisecond})
	internals := s.pool.Internals()

	msgA := feedInternalMsg(t, 0x22, 1000)
	oldHead := appliedEvent(feedBlockRoot(t, nil))
	oldHead.Meta.GenUTime = uint32(time.Now().Add(-2 * time.Hour).Unix())
	oldHead.CurrentState = feedStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(t, msgA, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(t, msgA)},
	})

	// A superseded stale block never settles: deliver an older one first…
	superseded := appliedEvent(buildAppliedBlockRoot(t, nil))
	superseded.Meta.GenUTime = oldHead.Meta.GenUTime - 10
	superseded.CurrentState = feedStateRoot(t, nil)
	if err := s.OnBlockApplied(context.Background(), superseded); err != nil {
		t.Fatal(err)
	}
	// …then the real head with the same source.
	oldHead.Meta.ID.SeqNo = 2
	if err := s.OnBlockApplied(context.Background(), oldHead); err != nil {
		t.Fatal(err)
	}
	if _, err := internals.SourceTop(allShard, allShard); !errors.Is(err, msgpool.ErrNotFound) {
		t.Fatal("stale head must not be processed before it settles")
	}

	waitFor(t, func() bool {
		top, err := internals.SourceTop(allShard, allShard)
		return err == nil && top.Seqno == 2
	}, "settled head did not arm the pool")
	cut, err := internals.Cut(allShard, msgpool.CutRequest{Sources: map[msgpool.ShardIdent]msgpool.CutSource{
		allShard: {Visible: feedRef(oldHead.BlockRoot, 2)},
	}})
	if err != nil || len(cut.Messages) != 1 || cut.Messages[0].EnqueuedLT != 1000 {
		t.Fatalf("cut from settled head: %v %+v", err, cut)
	}

	// The chain resumes: a fresh block continues the run through the fast
	// path on top of the settled seed.
	msgB := feedInternalMsg(t, 0x44, 2000)
	fresh := appliedEvent(feedBlockRoot(t, map[*cell.Cell]*cell.Cell{msgB: feedEnvelope(t, msgB)}))
	fresh.Meta.ID.SeqNo = 3
	if err = s.OnBlockApplied(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		top, err := internals.SourceTop(allShard, allShard)
		return err == nil && top.Seqno == 3
	}, "resumed chain did not continue from the settled head")
}

func TestInternalsFeedDisabled(t *testing.T) {
	s := newTestService(t, Options{DisableInternals: true})

	ev := appliedEvent(feedBlockRoot(t, nil))
	ev.CurrentState = feedStateRoot(t, nil)
	if err := s.OnBlockApplied(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := s.pool.Internals().SourceTop(allShard, allShard); !errors.Is(err, msgpool.ErrNotFound) {
		t.Fatal("disabled feed must not track sources")
	}
}

func TestStartPropagatesCurrentStateLoadError(t *testing.T) {
	want := errors.New("current state unavailable")
	extension, err := New(validatorTestOptions(Options{StatsInterval: -1, EnableGroups: true}))(hooks.Node{
		Store:  validatorTestStore{err: want},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); !errors.Is(err, want) {
		t.Fatalf("start error = %v, want %v", err, want)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
