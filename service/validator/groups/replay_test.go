package groups

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type replayTestHistory struct {
	mu sync.Mutex

	current *storage.CurrentState
	states  map[replayBlockKey]*storage.BlockState
	metas   map[replayBlockKey]*storage.BlockMeta
	loaded  []ton.BlockIDExt
}

func newReplayTestHistory() *replayTestHistory {
	return &replayTestHistory{
		states: make(map[replayBlockKey]*storage.BlockState),
		metas:  make(map[replayBlockKey]*storage.BlockMeta),
	}
}

func (h *replayTestHistory) CurrentState(context.Context) (*storage.CurrentState, error) {
	if h.current == nil {
		return nil, storage.ErrNotFound
	}

	return storage.CloneCurrentState(h.current), nil
}

func (h *replayTestHistory) BlockState(_ context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.loaded = append(h.loaded, cloneReplayBlockID(block))
	state := h.states[replayKey(block)]
	if state == nil {
		return nil, storage.ErrNotFound
	}

	return storage.CloneBlockState(state), nil
}

func (h *replayTestHistory) BlockMeta(_ context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	meta := h.metas[replayKey(block)]
	if meta == nil {
		return nil, storage.ErrNotFound
	}

	return meta.Clone(), nil
}

func (h *replayTestHistory) add(input StateInput, parent *ton.BlockIDExt) {
	key := replayKey(input.Block)
	h.states[key] = &storage.BlockState{
		Block:         cloneReplayBlockID(input.Block),
		StateRootHash: input.Root.Hash(),
		Cell:          input.Root,
	}
	meta := &storage.BlockMeta{ID: cloneReplayBlockID(input.Block)}
	if parent != nil {
		meta.PrevRefs = []ton.BlockIDExt{cloneReplayBlockID(*parent)}
	}
	h.metas[key] = meta
}

func (h *replayTestHistory) setCurrent(input StateInput) {
	h.current = &storage.CurrentState{Masterchain: storage.BlockState{
		Block:         cloneReplayBlockID(input.Block),
		StateRootHash: input.Root.Hash(),
		Cell:          input.Root,
	}}
}

func TestTrackerBootstrapKeyRotationUsesKeyRegistry(t *testing.T) {
	validator := testValidatorWire{index: 0, key: groupTestBytes(1), weight: 1}
	contract := groupTestBytes(90)
	oldCollator := groupTestBytes(20)
	keyCollator := groupTestBytes(40)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
		configParamValidatorRegistry: registryTestConfig(contract, 1),
	})
	accounts := func(collator [32]byte) *tlb.ShardAccountsAugDict {
		return registryTestAccounts(t, contract, registryTestStorage(t, map[[32]byte]*cell.Cell{
			validator.key: registryTestValidator(registryTestEntry(t, collator)),
		}))
	}

	previous := replayTestState(t, 100, true, false, 0, configRoot, contract, accounts(oldCollator))
	keyRotation := replayTestState(t, 101, true, true, 0, configRoot, contract, accounts(keyCollator))
	head := replayTestState(t, 102, false, false, 101, configRoot, contract, accounts(keyCollator))

	history := newReplayTestHistory()
	history.add(previous, replayTestParent(99, 9))
	history.add(keyRotation, &previous.Block)
	history.add(head, &keyRotation.Block)
	history.setCurrent(head)
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	transitions, err := tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	wantKeyID := registryTestKeyID(t, validator.key)
	registryTestRequireEqual(t, snapshot.CollatorsByValidator, []CollatorRegistryEntry{{
		ValidatorKeyID:  wantKeyID,
		CollatorADNLIDs: [][32]byte{keyCollator},
	}})
	if len(transitions) != len(snapshot.Active)+len(snapshot.Future) {
		t.Fatalf("head transitions = %d, active/future = %d/%d", len(transitions), len(snapshot.Active), len(snapshot.Future))
	}
	if len(history.loaded) != 2 || !history.loaded[0].Equals(&head.Block) ||
		!history.loaded[1].Equals(&keyRotation.Block) {
		t.Fatalf("loaded startup chain = %+v, want head through key rotation", history.loaded)
	}
}

func TestTrackerBootstrapNonKeyRotationUsesPreviousRegistry(t *testing.T) {
	validator := testValidatorWire{index: 0, key: groupTestBytes(2), weight: 1}
	contract := groupTestBytes(91)
	oldCollator := groupTestBytes(30)
	nextCollator := groupTestBytes(50)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
		configParamValidatorRegistry: registryTestConfig(contract, 1),
	})
	accounts := func(collator [32]byte) *tlb.ShardAccountsAugDict {
		return registryTestAccounts(t, contract, registryTestStorage(t, map[[32]byte]*cell.Cell{
			validator.key: registryTestValidator(registryTestEntry(t, collator)),
		}))
	}

	previous := replayTestState(t, 100, true, true, 0, configRoot, contract, accounts(oldCollator))
	rotation := replayTestState(t, 101, true, false, 100, configRoot, contract, accounts(nextCollator))

	history := newReplayTestHistory()
	history.add(previous, replayTestParent(99, 10))
	history.add(rotation, &previous.Block)
	history.setCurrent(rotation)
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	registryTestRequireEqual(t, snapshot.CollatorsByValidator, []CollatorRegistryEntry{{
		ValidatorKeyID:  registryTestKeyID(t, validator.key),
		CollatorADNLIDs: [][32]byte{oldCollator},
	}})
}

func TestTrackerBootstrapFollowsExactParentID(t *testing.T) {
	configRoot := replayTestConfig(t, 3)
	canonical := replayTestState(t, 100, true, true, 0, configRoot, [32]byte{}, nil)
	head := replayTestState(t, 101, false, false, 100, configRoot, [32]byte{}, nil)
	fork := cloneReplayBlockID(canonical.Block)
	fork.RootHash[0] ^= 0xff

	history := newReplayTestHistory()
	history.add(canonical, replayTestParent(99, 11))
	history.add(head, &fork)
	history.setCurrent(head)
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_000, 0))
	if err == nil || !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forked parent error = %v, want storage.ErrNotFound", err)
	}
	if len(history.loaded) != 2 || !history.loaded[1].Equals(&fork) {
		t.Fatalf("loaded parent = %+v, want exact fork %s", history.loaded, storage.FormatBlockRef(fork))
	}
}

func TestTrackerBootstrapFailureIsTransactionalAndRetryable(t *testing.T) {
	configRoot := replayTestConfig(t, 4)
	rotation := replayTestState(t, 100, true, true, 0, configRoot, [32]byte{}, nil)
	head := replayTestState(t, 101, false, false, 100, configRoot, [32]byte{}, nil)
	missing := replayTestBlockID(100, 12)

	history := newReplayTestHistory()
	history.add(head, &missing)
	history.setCurrent(head)
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_000, 0)); err == nil || !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing history error = %v, want storage.ErrNotFound", err)
	}
	if _, err = tracker.Snapshot(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("failed Bootstrap published snapshot: %v", err)
	}

	history.add(rotation, replayTestParent(99, 13))
	history.add(head, &rotation.Block)
	if _, err = tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_001, 0)); err != nil {
		t.Fatalf("retry Bootstrap: %v", err)
	}
	if _, err = tracker.Snapshot(); err != nil {
		t.Fatalf("retry did not publish snapshot: %v", err)
	}
}

func TestTrackerBootstrapWithoutStateIsTransactionalAndRetryable(t *testing.T) {
	history := newReplayTestHistory()
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = tracker.Bootstrap(
		context.Background(),
		history,
		nil,
		time.Unix(1_700_000_000, 0),
	); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("empty history bootstrap error = %v, want ErrNoSnapshot", err)
	}
	if _, err = tracker.Snapshot(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("empty history bootstrap published snapshot: %v", err)
	}

	configRoot := replayTestConfig(t, 4)
	head := replayTestState(t, 100, true, true, 0, configRoot, [32]byte{}, nil)
	history.add(head, replayTestParent(99, 13))
	history.setCurrent(head)
	if _, err = tracker.Bootstrap(
		context.Background(),
		history,
		nil,
		time.Unix(1_700_000_001, 0),
	); err != nil {
		t.Fatalf("retry Bootstrap: %v", err)
	}
	if _, err = tracker.Snapshot(); err != nil {
		t.Fatalf("retry did not publish snapshot: %v", err)
	}
}

func TestTrackerBootstrapStartsLifecycleOnlyAtHead(t *testing.T) {
	configRoot := replayTestConfig(t, 5)
	rotation := replayTestState(t, 100, true, true, 0, configRoot, [32]byte{}, nil)
	middle := replayTestState(t, 101, false, false, 100, configRoot, [32]byte{}, nil)
	head := replayTestState(t, 102, false, false, 100, configRoot, [32]byte{}, nil)

	history := newReplayTestHistory()
	history.add(rotation, replayTestParent(99, 14))
	history.add(middle, &rotation.Block)
	history.add(head, &middle.Block)
	history.setCurrent(head)
	tracker, err := NewTracker(TrackerOptions{StartGroupsFromSeqno: 1})
	if err != nil {
		t.Fatal(err)
	}

	transitions, err := tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tracker.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.LifecycleEnabled {
		t.Fatal("head did not enable group lifecycle")
	}
	if len(transitions) != len(snapshot.Active)+len(snapshot.Future) {
		t.Fatalf("head transitions = %d, active/future = %d/%d", len(transitions), len(snapshot.Active), len(snapshot.Future))
	}
	for _, transition := range transitions {
		if transition.Kind != TransitionStarted && transition.Kind != TransitionPrepared {
			t.Fatalf("historical lifecycle transition leaked at startup: %s", transition.Kind)
		}
	}
	masterOrigin := uint32(0)
	for _, session := range snapshot.Active {
		if session.Shard.IsMasterchain() {
			masterOrigin = session.MinMasterchain.SeqNo
		}
	}
	if masterOrigin != rotation.Block.SeqNo {
		t.Fatalf("masterchain session origin = %d, want rotation %d", masterOrigin, rotation.Block.SeqNo)
	}
	historical, err := tracker.Project(nil, ApplyInput{
		Block: middle.Block,
		Root:  middle.Root,
		AsOf:  time.Unix(1_700_000_001, 0),
	})
	if err != nil {
		t.Fatalf("load replayed historical snapshot: %v", err)
	}
	if !historical.Ready || !historical.MasterchainBlock.Equals(&middle.Block) {
		t.Fatal("bootstrap did not retain the canonical intermediate group snapshot")
	}
}

func TestTrackerBootstrapRejectsBufferedFork(t *testing.T) {
	configRoot := replayTestConfig(t, 6)
	rotation := replayTestState(t, 100, true, true, 0, configRoot, [32]byte{}, nil)
	first := replayTestState(t, 101, false, false, 100, configRoot, [32]byte{}, nil)
	second := first
	second.Block = cloneReplayBlockID(first.Block)
	second.Block.RootHash[0] ^= 0xff

	history := newReplayTestHistory()
	history.add(rotation, replayTestParent(99, 15))
	history.setCurrent(rotation)
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	buffered := []BufferedMasterchainState{
		{Block: first.Block, Root: first.Root, PrevRefs: []ton.BlockIDExt{rotation.Block}},
		{Block: second.Block, Root: second.Root, PrevRefs: []ton.BlockIDExt{rotation.Block}},
	}

	if _, err = tracker.Bootstrap(context.Background(), history, buffered, time.Unix(1_700_000_000, 0)); err == nil {
		t.Fatal("conflicting buffered startup forks were accepted")
	}
}

func TestTrackerBootstrapRejectsRepeatedBootstrap(t *testing.T) {
	history := newReplayTestHistory()
	configRoot := replayTestConfig(t, 7)
	head := replayTestState(t, 100, true, true, 0, configRoot, [32]byte{}, nil)
	history.add(head, replayTestParent(99, 16))
	history.setCurrent(head)
	tracker, err := NewTracker(TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.Bootstrap(context.Background(), history, nil, time.Unix(1_700_000_001, 0)); !errors.Is(err, ErrTrackerAlreadyBootstrapped) {
		t.Fatalf("second Bootstrap error = %v, want ErrTrackerAlreadyBootstrapped", err)
	}
}

func replayTestConfig(t *testing.T, seed byte) *cell.Cell {
	t.Helper()

	return buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: buildTestValidatorSet(t, []testValidatorWire{{
			index:  0,
			key:    groupTestBytes(seed),
			weight: 1,
		}}, true, 1),
	})
}

func replayTestState(
	t *testing.T,
	seqno uint32,
	rotated bool,
	keyState bool,
	lastKeySeqno uint32,
	configRoot *cell.Cell,
	configAddress [32]byte,
	accounts *tlb.ShardAccountsAugDict,
) StateInput {
	t.Helper()

	shard := ShardID{Workchain: 0, Shard: masterchainShard}
	var lastKey *tlb.ExtBlkRef
	if lastKeySeqno != 0 {
		lastKey = testExtBlockRef(lastKeySeqno)
	}

	return buildStateFixture(t, stateFixtureOptions{
		Seqno:            seqno,
		GenUTime:         1_700_000_000 + seqno,
		CatchainSeqno:    44,
		RotatedAllShards: rotated,
		AfterKeyBlock:    keyState,
		LastKeyBlock:     lastKey,
		ConfigRoot:       configRoot,
		ConfigAddress:    configAddress,
		Accounts:         accounts,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, seqno-1, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
}

func replayTestBlockID(seqno uint32, seed byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: masterchainWorkchain,
		Shard:     masterchainShard,
		SeqNo:     seqno,
		RootHash:  testHash(seed),
		FileHash:  testHash(seed + 1),
	}
}

func replayTestParent(seqno uint32, seed byte) *ton.BlockIDExt {
	block := replayTestBlockID(seqno, seed)

	return &block
}
