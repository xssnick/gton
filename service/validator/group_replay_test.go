package validator

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type groupReplayTestStore struct {
	hooks.Store

	mu sync.Mutex

	current *storage.CurrentState
	states  map[groupReplayBlockKey]*storage.BlockState
	metas   map[groupReplayBlockKey]*storage.BlockMeta
}

type groupReplayBlockingStore struct {
	*groupReplayTestStore

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *groupReplayBlockingStore) CurrentState(ctx context.Context) (*storage.CurrentState, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return s.groupReplayTestStore.CurrentState(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type groupReplayTestKeys struct{}

func (groupReplayTestKeys) KeyIDs() [][32]byte {
	return nil
}

func (groupReplayTestKeys) Sign([32]byte, []byte) ([]byte, error) {
	return nil, errors.New("unexpected test signature")
}

type groupReplayTestValidatorStorage struct {
	ValidatorStorage
}

type groupReplayMasterchainViews struct {
	mu       sync.Mutex
	snapshot *groups.Snapshot
	block    *cell.Cell
	state    *cell.Cell
	calls    int
}

func (v *groupReplayMasterchainViews) PublishMasterchainView(
	ctx context.Context,
	snapshot *groups.Snapshot,
	block *cell.Cell,
	state *cell.Cell,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.mu.Lock()
	v.snapshot = snapshot
	v.block = block
	v.state = state
	v.calls++
	v.mu.Unlock()

	return nil
}

func newGroupReplayTestStore() *groupReplayTestStore {
	return &groupReplayTestStore{
		states: make(map[groupReplayBlockKey]*storage.BlockState),
		metas:  make(map[groupReplayBlockKey]*storage.BlockMeta),
	}
}

func (s *groupReplayTestStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.current == nil {
		return nil, storage.ErrNotFound
	}

	return storage.CloneCurrentState(s.current), nil
}

func (s *groupReplayTestStore) BlockState(_ context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.states[groupReplayKey(block)]
	if state == nil {
		return nil, storage.ErrNotFound
	}

	return storage.CloneBlockState(state), nil
}

func (s *groupReplayTestStore) BlockMeta(_ context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta := s.metas[groupReplayKey(block)]
	if meta == nil {
		return nil, storage.ErrNotFound
	}

	return meta.Clone(), nil
}

func (s *groupReplayTestStore) add(input groups.StateInput, parent *ton.BlockIDExt) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := groupReplayKey(input.Block)
	s.states[key] = &storage.BlockState{
		Block:         copyReplayBlockID(input.Block),
		StateRootHash: input.Root.Hash(),
		Cell:          input.Root,
	}
	meta := &storage.BlockMeta{ID: copyReplayBlockID(input.Block)}
	if parent != nil {
		meta.PrevRefs = []ton.BlockIDExt{copyReplayBlockID(*parent)}
	}
	s.metas[key] = meta
}

func (s *groupReplayTestStore) setCurrent(input groups.StateInput) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.current = &storage.CurrentState{Masterchain: storage.BlockState{
		Block:         copyReplayBlockID(input.Block),
		StateRootHash: input.Root.Hash(),
		Cell:          input.Root,
	}}
}

func TestServiceReplaysPreStartHookAndKeepsExactIdempotency(t *testing.T) {
	config := groupReplayTestConfig(t, groupReplayTestBytes(6))
	rotation := groupReplayTestState(t, 100, true, true, 0, config, 17)
	head := groupReplayTestState(t, 101, false, false, 100, config, 18)

	store := newGroupReplayTestStore()
	store.add(rotation, groupReplayTestParent(99, 19))
	store.setCurrent(rotation)
	service := groupReplayTestService(t, store)
	event := groupReplayTestEvent(head, rotation.Block)
	if err := service.OnBlockApplied(context.Background(), event); err != nil {
		t.Fatalf("buffer pre-start hook: %v", err)
	}
	if _, err := service.tracker.Snapshot(); !errors.Is(err, groups.ErrNoSnapshot) {
		t.Fatalf("pre-start hook published tracker: %v", err)
	}

	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := service.tracker.Snapshot()
	if err != nil || !before.MasterchainBlock.Equals(&head.Block) {
		t.Fatalf("startup snapshot = %+v err=%v", before, err)
	}
	if err = service.OnBlockApplied(context.Background(), event); err != nil {
		t.Fatalf("idempotent post-start hook: %v", err)
	}
	after, err := service.tracker.Snapshot()
	if err != nil || after != before {
		t.Fatal("exact hook replay rebuilt the startup snapshot")
	}

	closeGroupReplayTestService(t, service)
}

func TestServiceInitializesGroupsBeforeStartingLocalCollator(t *testing.T) {
	config := groupReplayTestConfig(t, groupReplayTestBytes(31))
	head := groupReplayTestState(t, 100, true, true, 0, config, 32)
	store := newGroupReplayTestStore()
	store.add(head, groupReplayTestParent(99, 33))
	store.setCurrent(head)

	var service *Service
	var snapshotErr error
	localCollator := &validatorTestCollator{onStart: func() {
		_, snapshotErr = service.tracker.Snapshot()
	}}
	extension, err := New(Options{
		Keys:             groupReplayTestKeys{},
		Storage:          &groupReplayTestValidatorStorage{},
		Runtime:          newValidatorTestRuntime(),
		DisableInternals: true,
		EnableGroups:     true,
		LocalCollator:    localCollator,
		StatsInterval:    -1,
	})(hooks.Node{Store: store, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	service = extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if snapshotErr != nil {
		t.Fatalf("local collator started before group snapshot: %v", snapshotErr)
	}

	closeGroupReplayTestService(t, service)
}

func TestServiceDefersConsensusUntilCurrentStateIsAvailable(t *testing.T) {
	config := groupReplayTestConfig(t, groupReplayTestBytes(41))
	head := groupReplayTestState(t, 100, true, true, 0, config, 42)
	store := newGroupReplayTestStore()
	started := make(chan struct{})
	localCollator := &validatorTestCollator{onStart: func() { close(started) }}

	extension, err := New(Options{
		Keys:             groupReplayTestKeys{},
		Storage:          &groupReplayTestValidatorStorage{},
		Runtime:          newValidatorTestRuntime(),
		DisableInternals: true,
		EnableGroups:     true,
		LocalCollator:    localCollator,
		StatsInterval:    -1,
	})(hooks.Node{Store: store, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
		t.Fatal("local collator started without a current masterchain state")
	case <-time.After(2 * groupBootstrapRetryDelay):
	}
	if _, err = service.tracker.Snapshot(); !errors.Is(err, groups.ErrNoSnapshot) {
		t.Fatalf("empty bootstrap snapshot error = %v, want ErrNoSnapshot", err)
	}

	store.add(head, groupReplayTestParent(99, 43))
	store.setCurrent(head)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("local collator did not start after current state became available")
	}
	snapshot, err := service.tracker.Snapshot()
	if err != nil || !snapshot.MasterchainBlock.Equals(&head.Block) {
		t.Fatalf("deferred startup snapshot = %+v err=%v", snapshot, err)
	}

	closeGroupReplayTestService(t, service)
}

func TestServiceMasterchainHookWaitsForStartupReplay(t *testing.T) {
	config := groupReplayTestConfig(t, groupReplayTestBytes(50))
	head := groupReplayTestState(t, 100, true, true, 0, config, 51)
	parent := groupReplayTestParent(99, 52)

	baseStore := newGroupReplayTestStore()
	baseStore.add(head, parent)
	baseStore.setCurrent(head)
	store := &groupReplayBlockingStore{
		groupReplayTestStore: baseStore,
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}
	service := groupReplayTestService(t, store)

	startResult := make(chan error, 1)
	go func() {
		startResult <- service.Start(context.Background())
	}()
	<-store.entered

	hookResult := make(chan error, 1)
	go func() {
		hookResult <- service.OnBlockApplied(context.Background(), groupReplayTestEvent(head, *parent))
	}()
	select {
	case err := <-hookResult:
		t.Fatalf("hook crossed incomplete startup replay: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	if err := <-startResult; err != nil {
		t.Fatal(err)
	}
	if err := <-hookResult; err != nil {
		t.Fatal(err)
	}

	snapshot, err := service.tracker.Snapshot()
	if err != nil || !snapshot.MasterchainBlock.Equals(&head.Block) {
		t.Fatalf("startup snapshot = %+v err=%v", snapshot, err)
	}
	closeGroupReplayTestService(t, service)
}

func TestServicePublishesResidentMasterchainViewBeforeReturningFromHook(t *testing.T) {
	config := groupReplayTestConfig(t, groupReplayTestBytes(55))
	rotation := groupReplayTestState(t, 100, true, true, 0, config, 56)
	head := groupReplayTestState(t, 101, false, false, 100, config, 57)

	store := newGroupReplayTestStore()
	store.add(rotation, groupReplayTestParent(99, 58))
	store.setCurrent(rotation)
	views := &groupReplayMasterchainViews{}
	extension, err := New(Options{
		Keys:             groupReplayTestKeys{},
		Storage:          &groupReplayTestValidatorStorage{},
		Runtime:          newValidatorTestRuntime(),
		DisableInternals: true,
		EnableGroups:     true,
		MasterchainViews: views,
		StatsInterval:    -1,
	})(hooks.Node{Store: store, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	event := groupReplayTestEvent(head, rotation.Block)
	if err = service.OnBlockApplied(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	views.mu.Lock()
	calls := views.calls
	snapshot := views.snapshot
	block := views.block
	state := views.state
	views.mu.Unlock()
	if calls != 1 {
		t.Fatalf("resident view publications = %d, want 1", calls)
	}
	if snapshot == nil || !snapshot.MasterchainBlock.Equals(&head.Block) {
		t.Fatalf("resident snapshot = %+v, want block %v", snapshot, head.Block)
	}
	if block != event.BlockRoot || state != event.CurrentState {
		t.Fatal("resident publication did not preserve coherent hook artifacts")
	}

	closeGroupReplayTestService(t, service)
}

func TestServiceIgnoresSupersededMasterchainHook(t *testing.T) {
	config := groupReplayTestConfig(t, groupReplayTestBytes(61))
	rotation := groupReplayTestState(t, 100, true, true, 0, config, 62)
	older := groupReplayTestState(t, 101, false, false, 100, config, 63)
	newer := groupReplayTestState(t, 102, false, false, 100, config, 64)

	store := newGroupReplayTestStore()
	store.add(rotation, groupReplayTestParent(99, 65))
	store.setCurrent(rotation)
	views := &groupReplayMasterchainViews{}
	extension, err := New(Options{
		Keys:             groupReplayTestKeys{},
		Storage:          &groupReplayTestValidatorStorage{},
		Runtime:          newValidatorTestRuntime(),
		DisableInternals: true,
		EnableGroups:     true,
		MasterchainViews: views,
		StatsInterval:    -1,
	})(hooks.Node{Store: store, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err = service.OnBlockApplied(context.Background(), groupReplayTestEvent(newer, older.Block)); err != nil {
		t.Fatal(err)
	}
	if err = service.OnBlockApplied(context.Background(), groupReplayTestEvent(older, rotation.Block)); err != nil {
		t.Fatalf("superseded hook: %v", err)
	}

	snapshot, err := service.tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.MasterchainBlock.Equals(&newer.Block) {
		t.Fatalf("tracker regressed to %v", snapshot.MasterchainBlock)
	}
	views.mu.Lock()
	calls := views.calls
	published := views.snapshot
	views.mu.Unlock()
	if calls != 1 || published == nil || !published.MasterchainBlock.Equals(&newer.Block) {
		t.Fatalf("resident view calls=%d snapshot=%+v", calls, published)
	}

	closeGroupReplayTestService(t, service)
}

func TestSharedGroupTrackerBootstrapIsIdempotentAcrossConsumers(t *testing.T) {
	runtime := newValidatorTestRuntime()
	defer runtime.Close()

	store := newGroupReplayTestStore()
	if _, err := runtime.Groups.Bootstrap(t.Context(), store, nil, time.Now()); !errors.Is(err, groups.ErrNoSnapshot) {
		t.Fatalf("empty bootstrap error = %v, want ErrNoSnapshot", err)
	}
	if _, err := runtime.Groups.Snapshot(); !errors.Is(err, groups.ErrNoSnapshot) {
		t.Fatalf("empty history snapshot error = %v, want ErrNoSnapshot", err)
	}

	config := groupReplayTestConfig(t, groupReplayTestBytes(61))
	head := groupReplayTestState(t, 100, true, true, 0, config, 62)
	store.add(head, groupReplayTestParent(99, 63))
	store.setCurrent(head)
	if _, err := runtime.Groups.Bootstrap(t.Context(), store, nil, time.Now()); err != nil {
		t.Fatalf("ready bootstrap: %v", err)
	}
	if _, err := runtime.Groups.Bootstrap(t.Context(), store, nil, time.Now()); err != nil {
		t.Fatalf("second consumer bootstrap: %v", err)
	}
	if _, err := runtime.Groups.Snapshot(); err != nil {
		t.Fatalf("ready snapshot: %v", err)
	}
}

func TestSharedGroupTrackerProjectReusesExactPublishedSnapshot(t *testing.T) {
	runtime := newValidatorTestRuntime()
	defer runtime.Close()

	config := groupReplayTestConfig(t, groupReplayTestBytes(81))
	rotation := groupReplayTestState(t, 100, true, true, 0, config, 82)
	head := groupReplayTestState(t, 101, false, false, 100, config, 83)
	store := newGroupReplayTestStore()
	store.add(rotation, groupReplayTestParent(99, 84))
	store.add(head, &rotation.Block)
	store.setCurrent(head)
	if _, err := runtime.Groups.Bootstrap(t.Context(), store, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	published, err := runtime.Groups.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !published.Ready {
		t.Fatal("bootstrapped snapshot is not ready")
	}

	projected, err := runtime.Groups.Project(nil, groups.ApplyInput{
		Block: head.Block,
		Root:  head.Root,
		AsOf:  time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if projected != published {
		t.Fatal("exact current projection rebuilt the immutable published snapshot")
	}
}

func groupReplayTestService(t *testing.T, store hooks.Store) *Service {
	t.Helper()

	extension, err := New(Options{
		Keys:             groupReplayTestKeys{},
		Storage:          &groupReplayTestValidatorStorage{},
		Runtime:          newValidatorTestRuntime(),
		DisableInternals: true,
		EnableGroups:     true,
		StatsInterval:    -1,
	})(hooks.Node{Store: store, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}

	return extension.(*Service)
}

func closeGroupReplayTestService(t *testing.T, service *Service) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	service.opts.Runtime.Close()
}

func groupReplayTestEvent(input groups.StateInput, parent ton.BlockIDExt) hooks.BlockAppliedEvent {
	return hooks.BlockAppliedEvent{
		BlockRoot:    cell.BeginCell().EndCell(),
		CurrentState: input.Root,
		Meta: &storage.BlockMeta{
			ID:       input.Block,
			PrevRefs: []ton.BlockIDExt{parent},
			GenUTime: uint32(time.Now().Unix()),
		},
	}
}

func groupReplayTestState(
	t *testing.T,
	seqno uint32,
	rotated bool,
	keyState bool,
	lastKeySeqno uint32,
	configRoot *cell.Cell,
	blockSeed byte,
) groups.StateInput {
	t.Helper()

	info := cell.BeginCell().
		MustStoreUInt(0, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(44, 32).
		MustStoreBoolBit(rotated).
		MustStoreBuilder(groupReplayTestEmptyOldMcBlocksInfo().ToBuilder()).
		MustStoreBoolBit(keyState)
	if lastKeySeqno == 0 {
		info.MustStoreBoolBit(false)
	} else {
		lastKey, err := tlb.ToCell(&tlb.ExtBlkRef{
			SeqNo:    lastKeySeqno,
			RootHash: groupReplayTestHash(byte(lastKeySeqno)),
			FileHash: groupReplayTestHash(byte(lastKeySeqno + 1)),
		})
		if err != nil {
			t.Fatal(err)
		}
		info.MustStoreBoolBit(true).MustStoreBuilder(lastKey.ToBuilder())
	}

	shardSeqno := seqno
	if shardSeqno > 0 {
		shardSeqno--
	}
	shardDescription, err := tlb.ToCell(tlb.ShardDesc{
		SeqNo:              shardSeqno,
		RootHash:           groupReplayTestHash(byte(shardSeqno)),
		FileHash:           groupReplayTestHash(byte(shardSeqno + 1)),
		NextCatchainSeqNo:  91,
		NextValidatorShard: groupReplayMasterchainShard,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	})
	if err != nil {
		t.Fatal(err)
	}
	shardTree := cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBuilder(shardDescription.ToBuilder()).
		EndCell()
	shardHashes := cell.NewDict(32)
	workchainKey := cell.BeginCell().MustStoreInt(0, 32).EndCell()
	workchainValue := cell.BeginCell().MustStoreRef(shardTree).EndCell()
	if err = shardHashes.Set(workchainKey, workchainValue); err != nil {
		t.Fatal(err)
	}

	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(shardHashes).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(configRoot).
		MustStoreRef(info.EndCell()).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	accountsRoot, err := tlb.ToCell(accounts)
	if err != nil {
		t.Fatal(err)
	}

	root := cell.BeginCell().
		MustStoreUInt(0x9023afe2, 32).
		MustStoreInt(0, 32).
		MustStoreUInt(0, 2).
		MustStoreUInt(0, 6).
		MustStoreInt(int64(groupReplayMasterchainWorkchain), 32).
		MustStoreUInt(0, 64).
		MustStoreUInt(uint64(seqno), 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(1_700_000_000+seqno), 32).
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 32).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreBoolBit(false).
		MustStoreRef(accountsRoot).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreBoolBit(true).
		MustStoreRef(extra).
		EndCell()

	return groups.StateInput{
		Block: groupReplayTestBlockID(seqno, blockSeed),
		Root:  root,
	}
}

func groupReplayTestConfig(t *testing.T, validatorKey [32]byte) *cell.Cell {
	t.Helper()

	validators := cell.NewDict(16)
	descriptor := cell.BeginCell().
		MustStoreUInt(0x53, 8).
		MustStoreUInt(0x8e81278a, 32).
		MustStoreSlice(validatorKey[:], 256).
		MustStoreUInt(1, 64).
		EndCell()
	groupReplayTestSetDictionaryInt(t, validators, 0, descriptor)
	validatorSet := cell.BeginCell().
		MustStoreUInt(0x12, 8).
		MustStoreUInt(100, 32).
		MustStoreUInt(200, 32).
		MustStoreUInt(1, 16).
		MustStoreUInt(1, 16).
		MustStoreUInt(1, 64).
		MustStoreDict(validators).
		EndCell()
	params := cell.NewDict(32)
	wrapper := cell.BeginCell().MustStoreRef(validatorSet).EndCell()
	groupReplayTestSetDictionaryInt(t, params, 34, wrapper)

	return params.AsCell()
}

func groupReplayTestSetDictionaryInt(
	t *testing.T,
	dict *cell.Dictionary,
	key uint64,
	value *cell.Cell,
) {
	t.Helper()

	if err := dict.SetIntKey(new(big.Int).SetUint64(key), value); err != nil {
		t.Fatal(err)
	}
}

func groupReplayTestEmptyOldMcBlocksInfo() *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 64).
		EndCell()
}

func groupReplayTestBlockID(seqno uint32, seed byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: groupReplayMasterchainWorkchain,
		Shard:     groupReplayMasterchainShard,
		SeqNo:     seqno,
		RootHash:  groupReplayTestHash(seed),
		FileHash:  groupReplayTestHash(seed + 1),
	}
}

func groupReplayTestParent(seqno uint32, seed byte) *ton.BlockIDExt {
	block := groupReplayTestBlockID(seqno, seed)

	return &block
}

func groupReplayTestHash(seed byte) []byte {
	hash := make([]byte, 32)
	for index := range hash {
		hash[index] = seed + byte(index)
	}

	return hash
}

func groupReplayTestBytes(seed byte) [32]byte {
	var value [32]byte
	for index := range value {
		value[index] = seed + byte(index)
	}

	return value
}

// copyReplayBlockID gives the tests an independent block id, the way the
// buffering path copies borrowed metadata before retaining it.
func copyReplayBlockID(block ton.BlockIDExt) ton.BlockIDExt {
	return *block.Copy()
}
