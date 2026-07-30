package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func testPeerID(label string) p2p.PeerID {
	return p2p.PeerID(sha256.Sum256([]byte(label)))
}

func newServiceTestNode(t testing.TB) *p2p.Node {
	t.Helper()

	return newServiceTestNodeWithStorage(t, openTestPebbleStorage(t))
}

func newServiceTestNodeWithStorage(t testing.TB, store tnstore.Storage) *p2p.Node {
	t.Helper()

	logger := zerolog.Nop()
	node, err := p2p.New(p2p.Options{
		Logger:        &logger,
		Storage:       store,
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create p2p node: %v", err)
	}
	return node
}

func newFrozenTestNode(t testing.TB) *p2p.Node {
	t.Helper()

	node := newServiceTestNode(t)
	node.EnterOffline("ton.sync_until reached")
	return node
}

// freezeSyncUntil leaves a service exactly where enterSyncUntilOffline does: the
// service knows it finished, and the node is offline. Being frozen is the
// service's own conclusion - the node also goes offline when something breaks,
// which must not read as a completed sync.
func freezeSyncUntil(t testing.TB, svc *Service) {
	t.Helper()

	svc.syncUntilReached.Store(true)
	if svc.node != nil {
		svc.node.EnterOffline("ton.sync_until reached")
	}
}

func newMasterchainQueueTestService() *Service {
	return &Service{
		currentStateWake:                make(chan struct{}),
		nextMasterchainQueue:            make(map[tnstore.BlockRootHash]queuedMasterchainBlock),
		nextMasterchainBySeqno:          make(map[uint32]tnstore.BlockRootHash),
		nextMasterchainCandidates:       make(map[tnstore.BlockRootHash]queuedMasterchainCandidate),
		nextMasterchainCandidateBySeqno: make(map[uint32]tnstore.BlockRootHash),
	}
}

func newMasterchainProbeTestService(t testing.TB) *Service {
	t.Helper()

	svc := newMasterchainQueueTestService()
	svc.node = newServiceTestNode(t)
	return svc
}

func mustNextBlockBootstrapProbeDecision(t testing.TB, svc *Service, prev ton.BlockIDExt, prevUTime int64, state nextBlockBootstrapProbeState) nextBlockBootstrapProbeDecision {
	t.Helper()

	decision, _, err := svc.nextBlockBootstrapProbeDecision(prev, prevUTime, state)
	if err != nil {
		t.Fatalf("make next-block bootstrap probe decision: %v", err)
	}
	return decision
}

func newCurrentStatePersistenceTestService(t testing.TB, store tnstore.Storage, shutdownContext context.Context) *Service {
	t.Helper()

	return &Service{
		log:              zerolog.Nop(),
		node:             newServiceTestNodeWithStorage(t, store),
		storage:          store,
		shutdownContext:  shutdownContext,
		stateCellLoaders: make(map[uint64]cell.LazyCellLoader),
	}
}

type recordingSyncObserver struct {
	blocks []SyncBlockObservation
}

func (o *recordingSyncObserver) ObserveSyncBlock(observation SyncBlockObservation) {
	o.blocks = append(o.blocks, observation)
}

func (o *recordingSyncObserver) ObserveSyncObtain(SyncObtainObservation) {}

func (o *recordingSyncObserver) ObserveSyncPersist(SyncPersistObservation) {}

func testCurrentBlockStates(current *tnstore.CurrentState) []*tnstore.BlockState {
	if current == nil {
		return nil
	}

	states := make([]*tnstore.BlockState, 0, 1+len(current.Shards))
	states = append(states, tnstore.CloneBlockState(&current.Masterchain))
	for _, key := range tnstore.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		if shard.Block.Workchain != -1 && shard.MasterchainRef == nil {
			master := current.Masterchain.Block
			shard.MasterchainRef = &master
		}
		states = append(states, tnstore.CloneBlockState(&shard))
	}
	return states
}

func testStateCheckpointEntries(states []*tnstore.BlockState) []tnstore.StateCheckpointBlock {
	return testStateCheckpointEntriesForCurrent(states, nil)
}

func testStateCheckpointEntriesForCurrent(states []*tnstore.BlockState, current *tnstore.CurrentState) []tnstore.StateCheckpointBlock {
	entries := make([]tnstore.StateCheckpointBlock, 0, len(states))
	for _, state := range states {
		if state == nil {
			continue
		}
		entries = append(entries, testStateCheckpointEntryForCurrent(state, current))
	}
	return entries
}

func testStateCheckpointEntryForCurrent(state *tnstore.BlockState, current *tnstore.CurrentState) tnstore.StateCheckpointBlock {
	entry := tnstore.StateCheckpointBlock{State: state}
	if state != nil && state.Block.SeqNo != 0 {
		entry.Artifact = testStateCheckpointArtifactForCurrent(state, current)
	}
	return entry
}

func testStateCheckpointArtifact(state *tnstore.BlockState) *tnstore.ServedBlockFull {
	return testStateCheckpointArtifactForCurrent(state, nil)
}

func testStateCheckpointArtifactForCurrent(state *tnstore.BlockState, current *tnstore.CurrentState) *tnstore.ServedBlockFull {
	block := state.Block
	meta := &tnstore.BlockMeta{ID: block, GenUTime: block.SeqNo}
	if block.Workchain != -1 {
		if state.MasterchainRef != nil {
			meta.MasterchainRefSeqno = state.MasterchainRef.SeqNo
		} else if current != nil {
			meta.MasterchainRefSeqno = current.Masterchain.Block.SeqNo
		} else {
			meta.MasterchainRefSeqno = block.SeqNo
		}
	}
	return &tnstore.ServedBlockFull{
		ID:    block,
		Block: []byte{0x01},
		Proof: []byte{0x02},
		Meta:  meta,
	}
}

func saveTestStateCheckpoint(ctx context.Context, store *pebblestore.Store, states []*tnstore.BlockState, current *tnstore.CurrentState) error {
	_, err := store.SaveStateCheckpointEntries(ctx, testStateCheckpointEntriesForCurrent(states, current), tnstore.StateCellRecords{}, current)
	return err
}

func saveTestBlockState(ctx context.Context, store *pebblestore.Store, state *tnstore.BlockState) error {
	entries := testStateCheckpointEntries([]*tnstore.BlockState{state})
	if state != nil && state.Block.Workchain != -1 && state.Block.SeqNo != 0 && state.MasterchainRef == nil {
		entries = append(testStateCheckpointEntries([]*tnstore.BlockState{testDummyMasterState(state.Block.SeqNo)}), entries...)
	}
	_, err := store.SaveStateCheckpointEntries(ctx, entries, tnstore.StateCellRecords{}, nil)
	return err
}

func testDummyMasterState(seqno uint32) *tnstore.BlockState {
	return &tnstore.BlockState{
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     seqno,
			RootHash:  testDummyHash(0xf1, seqno),
			FileHash:  testDummyHash(0xf2, seqno),
		},
		StateRootHash: testDummyHash(0xf3, seqno),
	}
}

func testDummyHash(prefix byte, seqno uint32) []byte {
	hash := bytes.Repeat([]byte{prefix}, 32)
	binary.BigEndian.PutUint32(hash[len(hash)-4:], seqno)
	return hash
}

func openManualTestPebbleStorage(t *testing.T) *pebblestore.Store {
	t.Helper()

	store, err := pebblestore.Open(pebblestore.Options{
		Dir: filepath.Join(t.TempDir(), "storage"),
	})
	if err != nil {
		t.Fatalf("open pebble storage: %v", err)
	}
	return store
}

func TestStateMetadataPublishDoesNotMoveCurrentState(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	master := testBlockID(-1, topShard, 50)
	base := testBlockID(0, topShard, 100)
	key := tnstore.ShardKeyFromBlock(base)
	masterState := &tnstore.BlockState{
		Block:         master,
		StateRootHash: master.RootHash,
	}
	baseState := &tnstore.BlockState{
		Block: base,
		Cell:  testShardStateCell(t, base),
	}
	err := saveTestStateCheckpoint(ctx, store, []*tnstore.BlockState{
		masterState,
		baseState,
	}, &tnstore.CurrentState{
		ShardClientSeqno: master.SeqNo,
		Masterchain:      *masterState,
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			key: *baseState,
		},
	})
	if err != nil {
		t.Fatalf("save current state: %v", err)
	}

	next := &tnstore.BlockState{
		Block: testBlockID(0, topShard, 101),
	}
	next.Cell = testShardStateCell(t, next.Block)
	if err = saveTestBlockState(ctx, store, next); err != nil {
		t.Fatalf("persist next shard: %v", err)
	}

	current, err := store.CurrentState(ctx)
	if err != nil {
		t.Fatalf("load current state: %v", err)
	}
	if got := current.Shards[key].Block.SeqNo; got != 100 {
		t.Fatalf("unexpected current shard seqno %d", got)
	}
	if current.ShardClientSeqno != master.SeqNo {
		t.Fatalf("unexpected shard client seqno %d", current.ShardClientSeqno)
	}
	if _, err = store.BlockState(ctx, next.Block); err != nil {
		t.Fatalf("load persisted next shard: %v", err)
	}
}

func TestPublishCommittedCurrentStateDoesNotRegressStatus(t *testing.T) {
	svc := newMasterchainProbeTestService(t)

	newerMaster := testBlockID(-1, topShard, 101)
	olderMaster := testBlockID(-1, topShard, 100)
	newer := &tnstore.CurrentState{
		ShardClientSeqno: newerMaster.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: newerMaster,
		},
	}
	older := &tnstore.CurrentState{
		ShardClientSeqno: olderMaster.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: olderMaster,
		},
	}

	svc.publishCommittedCurrentState(newer)
	svc.publishCommittedCurrentState(older)

	svc.currentStatusMu.RLock()
	defer svc.currentStatusMu.RUnlock()
	if svc.currentStatus == nil {
		t.Fatal("current status is nil")
	}
	if got := svc.currentStatus.Masterchain.Block.SeqNo; got != newerMaster.SeqNo {
		t.Fatalf("current status regressed to seqno %d", got)
	}
}

func TestCatchUpCurrentStateReturnsAfterSyncUntilOffline(t *testing.T) {
	ctx := context.Background()
	cutoff := uint32(200)
	master := testBlockID(-1, topShard, 100)
	current := &tnstore.CurrentState{
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	svc := &Service{
		log:  zerolog.Nop(),
		node: newFrozenTestNode(t),
		storage: &testCellGenerationMigrationStore{
			current: current,
			blockMetas: map[tnstore.BlockRootHash]*tnstore.BlockMeta{
				tnstore.BlockKey(master): &tnstore.BlockMeta{ID: master, GenUTime: cutoff},
			},
		},
		syncUntil:        cutoff,
		currentStateWake: make(chan struct{}),
	}
	freezeSyncUntil(t, svc)
	if err := svc.catchUpCurrentState(ctx); err != nil {
		t.Fatalf("catch up current state: %v", err)
	}
}

func TestInitialStateSyncStopsAfterSyncUntilFrozen(t *testing.T) {
	cutoff := uint32(200)
	master := testBlockID(-1, topShard, 100)
	current := &tnstore.CurrentState{
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}
	svc := &Service{
		log:  zerolog.Nop(),
		node: newFrozenTestNode(t),
		storage: &testCellGenerationMigrationStore{
			current: current,
			blockMetas: map[tnstore.BlockRootHash]*tnstore.BlockMeta{
				tnstore.BlockKey(master): &tnstore.BlockMeta{ID: master, GenUTime: cutoff},
			},
		},
		syncUntil:        cutoff,
		currentStateWake: make(chan struct{}),
	}
	freezeSyncUntil(t, svc)

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.runInitialStateSync(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("initial state sync did not stop after sync_until freeze")
	}
}

func TestMarkLiveCheckpointStatesFlushedPublishesAllEntries(t *testing.T) {
	flusher := &testLiveCheckpointFlusher{}
	svc := &Service{liveState: flusher}
	master := testBlockID(-1, topShard, 102)
	shard := testBlockID(0, topShard, 103)

	svc.markLiveCheckpointStatesFlushed([]tnstore.StateCheckpointBlock{
		{State: &tnstore.BlockState{Block: master}},
		{},
		{State: &tnstore.BlockState{Block: shard}},
	})

	if len(flusher.blocks) != 2 {
		t.Fatalf("flushed blocks = %d, want 2", len(flusher.blocks))
	}
	if !flusher.blocks[0].Equals(&master) {
		t.Fatalf("first flushed block = %s, want %s", tnstore.FormatBlockRef(flusher.blocks[0]), tnstore.FormatBlockRef(master))
	}
	if !flusher.blocks[1].Equals(&shard) {
		t.Fatalf("second flushed block = %s, want %s", tnstore.FormatBlockRef(flusher.blocks[1]), tnstore.FormatBlockRef(shard))
	}
}

func TestPublishLiveCurrentBlockMarkersPublishesOnlyCurrentTips(t *testing.T) {
	flusher := &testLiveCheckpointFlusher{}
	logger := zerolog.Nop()
	svc := &Service{log: logger, liveState: flusher}
	master := testBlockID(-1, topShard, 202)
	shardA := testBlockID(0, topShard, 203)
	shardB := testBlockID(0, topShard/2, 204)
	current := &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: []byte{0x01},
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			tnstore.ShardKeyFromBlock(shardA): {
				Block:         shardA,
				StateRootHash: []byte{0x02},
			},
			tnstore.ShardKeyFromBlock(shardB): {
				Block:         shardB,
				StateRootHash: []byte{0x03},
			},
		},
	}

	svc.publishLiveCurrentBlockMarkers(current)

	if len(flusher.artifacts) != 3 {
		t.Fatalf("published markers = %d, want 3", len(flusher.artifacts))
	}
	wantBlocks := []ton.BlockIDExt{master}
	for _, key := range tnstore.SortedShardKeys(current.Shards) {
		wantBlocks = append(wantBlocks, current.Shards[key].Block)
	}
	for i, artifact := range flusher.artifacts {
		if !artifact.Block.Equals(&wantBlocks[i]) {
			t.Fatalf("marker[%d] block = %s, want %s", i, tnstore.FormatBlockRef(artifact.Block), tnstore.FormatBlockRef(wantBlocks[i]))
		}
		if len(artifact.BlockData) != 0 || len(artifact.Proofs) != 0 {
			t.Fatalf("marker[%d] published payload data=%d proofs=%d", i, len(artifact.BlockData), len(artifact.Proofs))
		}
		if artifact.State == nil || !artifact.State.Block.Equals(&wantBlocks[i]) {
			t.Fatalf("marker[%d] state is missing", i)
		}
		if artifact.Meta == nil || !artifact.Meta.ID.Equals(&wantBlocks[i]) {
			t.Fatalf("marker[%d] meta is missing", i)
		}
		if !artifact.ArtifactFlushed || !artifact.StateFlushed {
			t.Fatalf("marker[%d] flushed flags artifact=%v state=%v", i, artifact.ArtifactFlushed, artifact.StateFlushed)
		}
	}
}

type testLiveCheckpointFlusher struct {
	current   *tnstore.CurrentState
	blocks    []ton.BlockIDExt
	artifacts []tnstore.LiveBlockArtifacts
}

func (f *testLiveCheckpointFlusher) SetLiveCurrentState(current *tnstore.CurrentState) {
	f.current = current
}

func (f *testLiveCheckpointFlusher) SetLiveCurrentStateSnapshot(current *tnstore.CurrentState) {
	f.current = current
}

func (f *testLiveCheckpointFlusher) MarkLiveBlockStatesFlushed(blocks []ton.BlockIDExt) {
	f.blocks = append(f.blocks, blocks...)
}

func (f *testLiveCheckpointFlusher) MarkLiveCurrentStateFlushed(current *tnstore.CurrentState) {
	f.current = current
}

func (f *testLiveCheckpointFlusher) MarkLiveBlockFlushed(ton.BlockIDExt) {}

func (f *testLiveCheckpointFlusher) PublishLiveBlockArtifacts(artifact tnstore.LiveBlockArtifacts) error {
	f.artifacts = append(f.artifacts, artifact)
	return nil
}

func (f *testLiveCheckpointFlusher) NonfinalBlockCacheEnabled() bool {
	return false
}

func (f *testLiveCheckpointFlusher) PublishNonfinalBlockArtifacts(tnstore.LiveBlockArtifacts, tnstore.LiveBlockNonfinalKind) error {
	return nil
}

func (f *testLiveCheckpointFlusher) SetNonfinalCellLoader(cell.LazyCellLoader) {}

func (f *testLiveCheckpointFlusher) BlockState(context.Context, ton.BlockIDExt) (*tnstore.BlockState, error) {
	return nil, tnstore.ErrNotFound
}

func (f *testLiveCheckpointFlusher) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, tnstore.ErrNotFound
}

func TestSyncBlockResultForError(t *testing.T) {
	timeout := &testTimeoutError{}
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", want: "success"},
		{name: "canceled", err: context.Canceled, want: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "timeout"},
		{name: "timeout", err: timeout, want: "timeout"},
		{name: "block miss", err: p2p.ErrBlockNotAvailable, want: "miss"},
		{name: "state miss", err: p2p.ErrStateNotAvailable, want: "miss"},
		{name: "retry", err: errPersistentStateGCActive, want: "retry"},
		{name: "error", err: errors.New("boom"), want: "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := syncBlockResultForError(tc.err); got != tc.want {
				t.Fatalf("result = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSyncBlockSourceForDownloadedBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		want SyncBlockSource
	}{
		{name: "plain overlay download", kind: "tonNode.dataFull", want: SyncBlockSourceNextBlock},
		{name: "broadcast cache", kind: "tonNode.blockBroadcastCompressedV2", want: SyncBlockSourceBroadcastCache},
		{name: "finality assembled broadcast", kind: "tonNode.blockFinalityBroadcast", want: SyncBlockSourceBroadcastCache},
		{name: "shard description broadcast hint", kind: "tonNode.newShardBlockBroadcast", want: SyncBlockSourceBroadcastHint},
		{name: "stored full block", kind: "local full block cache", want: SyncBlockSourceStored},
		{name: "stored next block", kind: "local next block cache", want: SyncBlockSourceStored},
		{name: "stored block data", kind: "stored block", want: SyncBlockSourceStored},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := syncBlockSourceForKind(SyncBlockSourceNextBlock, tc.kind)
			if got != tc.want {
				t.Fatalf("source = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSyncBlockOriginTreatsBroadcastHintAsBroadcast(t *testing.T) {
	if got := syncBlockOriginForSource(SyncBlockSourceBroadcastHint); got != SyncBlockOriginBroadcast {
		t.Fatalf("origin = %q, want broadcast", got)
	}
}

func TestObserveSyncBlockKeepsExplicitOrigin(t *testing.T) {
	observer := &recordingSyncObserver{}
	svc := &Service{sync: observer}

	svc.observeSyncBlock(SyncBlockObservation{
		Pipeline: "next_block_bootstrap",
		Chain:    "shardchain",
		Shard:    "basechain",
		Source:   SyncBlockSourceNextBlock,
		Origin:   SyncBlockOriginBroadcast,
		Result:   "success",
	})

	if len(observer.blocks) != 1 {
		t.Fatalf("observed blocks = %d, want 1", len(observer.blocks))
	}
	got := observer.blocks[0]
	if got.Source != SyncBlockSourceNextBlock {
		t.Fatalf("source = %q, want next_block", got.Source)
	}
	if got.Origin != SyncBlockOriginBroadcast {
		t.Fatalf("origin = %q, want broadcast", got.Origin)
	}
}

func TestPreparedBlockTracksOrigin(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		want SyncBlockOrigin
	}{
		{name: "full block broadcast", kind: "tonNode.blockBroadcastCompressedV2", want: SyncBlockOriginBroadcast},
		{name: "finality assembled broadcast", kind: "tonNode.blockFinalityBroadcast", want: SyncBlockOriginBroadcast},
		{name: "shard hint broadcast", kind: "tonNode.newShardBlockBroadcast", want: SyncBlockOriginBroadcast},
		{name: "overlay download", kind: "tonNode.dataFull", want: SyncBlockOriginDownload},
		{name: "stored block", kind: "stored block", want: SyncBlockOriginStored},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared := preparedBlockWithStateCells(VerifiedBlock{Kind: tc.kind}, tnstore.StateCellRecords{}, 0)
			if prepared.Origin != tc.want {
				t.Fatalf("origin = %q, want %q", prepared.Origin, tc.want)
			}
		})
	}
}

func TestStateRootForCompressedBlockUsesLiveShardState(t *testing.T) {
	block := testBlockID(0, topShard, 123)
	root := testShardStateCell(t, block)
	svc := &Service{
		currentStatus: &tnstore.CurrentState{
			Shards: map[tnstore.ShardKey]tnstore.BlockState{
				tnstore.ShardKeyFromBlock(block): {
					Block: block,
					Cell:  root,
				},
			},
		},
	}

	got, err := svc.StateRootForCompressedBlock(context.Background(), block)
	if err != nil {
		t.Fatalf("state root error = %v", err)
	}
	if got != root {
		t.Fatal("state root was not taken from live shard state")
	}
}

func TestStateRootForCompressedBlockUsesRecentlyAppliedShardState(t *testing.T) {
	block := testBlockID(0, topShard, 124)
	root := testShardStateCell(t, block)
	svc := &Service{
		compressedStateCache: make(map[tnstore.BlockRootHash]compressedBlockStateEntry),
	}

	if !svc.RememberCompressedBlockState(&tnstore.BlockState{
		Block: block,
		Cell:  root,
	}) {
		t.Fatal("state root was not remembered")
	}

	got, err := svc.StateRootForCompressedBlock(context.Background(), block)
	if err != nil {
		t.Fatalf("state root error = %v", err)
	}
	if got != root {
		t.Fatal("state root was not taken from recently applied shard state")
	}
}

func TestCurrentBroadcastValidatorConfigUsesLiveCurrentState(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	persistedMaster := testBlockID(-1, topShard, 120)
	liveMaster := testBlockID(-1, topShard, 121)

	persistedMasterState := tnstore.BlockState{
		Block: persistedMaster,
		Cell:  testShardStateCell(t, persistedMaster),
	}
	if err := saveTestStateCheckpoint(ctx, store, []*tnstore.BlockState{&persistedMasterState}, &tnstore.CurrentState{
		Masterchain: persistedMasterState,
		Shards:      map[tnstore.ShardKey]tnstore.BlockState{},
	}); err != nil {
		t.Fatalf("save persisted state checkpoint: %v", err)
	}

	svc := &Service{
		storage: store,
		currentStatus: &tnstore.CurrentState{
			Masterchain: tnstore.BlockState{Block: liveMaster},
			Shards:      map[tnstore.ShardKey]tnstore.BlockState{},
		},
		masterStateCache: map[tnstore.BlockRootHash]*tnstore.BlockState{
			tnstore.BlockKey(liveMaster): {Block: liveMaster},
		},
	}

	_, err := svc.currentBroadcastValidatorConfig(ctx)
	if err == nil {
		t.Fatal("validator config unexpectedly loaded from incomplete test state")
	}
	if !strings.Contains(err.Error(), tnstore.FormatBlockRef(liveMaster)) {
		t.Fatalf("validator config error = %q, want live master %s", err, tnstore.FormatBlockRef(liveMaster))
	}
	if strings.Contains(err.Error(), tnstore.FormatBlockRef(persistedMaster)) {
		t.Fatalf("validator config used persisted current state: %v", err)
	}
}

func TestShardPrefetchDoesNotMarkScheduledWhenSlotUnavailable(t *testing.T) {
	runner := &nextSyncRunner{
		service:                &Service{node: &p2p.Node{}},
		ctx:                    context.Background(),
		shardPrefetchSlots:     make(chan struct{}, 1),
		shardPrefetchScheduled: map[tnstore.BlockRootHash]struct{}{},
	}
	runner.shardPrefetchSlots <- struct{}{}

	master := testBlockID(-1, topShard, 125)
	target := testBlockID(0, topShard, 126)

	got := runner.scheduleShardPrefetch(master, []ton.BlockIDExt{target})
	if got != 0 {
		t.Fatalf("scheduled prefetch count = %d, want 0", got)
	}
	if _, ok := runner.shardPrefetchScheduled[tnstore.BlockKey(target)]; ok {
		t.Fatal("prefetch target was marked scheduled without an available worker slot")
	}
	if len(runner.shardPrefetchOrder) != 0 {
		t.Fatalf("scheduled order length = %d, want 0", len(runner.shardPrefetchOrder))
	}
}

func TestShardDescriptionPrefetchReportsUnavailableSlot(t *testing.T) {
	runner := &nextSyncRunner{
		service:            &Service{node: &p2p.Node{}},
		ctx:                context.Background(),
		shardPrefetchSlots: make(chan struct{}, 1),
	}
	runner.shardPrefetchSlots <- struct{}{}

	desc := &p2p.ShardBlockDescription{Block: testBlockID(0, topShard, 127)}
	if runner.prefetchShardDescriptionTarget(shardDescriptionHint{Overlay: "test"}, desc) {
		t.Fatal("description prefetch started without an available worker slot")
	}
}

type testTimeoutError struct{}

func (e *testTimeoutError) Error() string {
	return "timeout"
}

func (e *testTimeoutError) Timeout() bool {
	return true
}

func TestPersistArchiveCurrentStateReturnsSavedRoots(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)

	master := testBlockID(-1, topShard, 60)
	shard := testBlockID(0, topShard, 120)
	shardKey := tnstore.ShardKeyFromBlock(shard)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			shardKey: {
				Block:  shard,
				Cell:   testShardStateCell(t, shard),
				Parsed: &tlb.ShardStateUnsplit{},
			},
		},
	}
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store},
		ctx:     ctx,
	}

	persisted, err := runner.persistArchiveCurrentState(current, 1, 0, testStateCheckpointEntriesForCurrent(testCurrentBlockStates(current), current), newTestStateCellWindowCache(nil).beginCheckpoint())
	if err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if persisted.Masterchain.Cell == nil {
		t.Fatal("persisted masterchain state lost saved root")
	}
	shardState := persisted.Shards[shardKey]
	if shardState.Cell == nil {
		t.Fatal("persisted shard state lost saved root")
	}
	if shardState.Parsed == nil {
		t.Fatal("persisted shard state lost parsed state")
	}
	if shardState.Parsed.Seqno != shard.SeqNo {
		t.Fatalf("persisted shard state was not reparsed from saved root, seqno=%d", shardState.Parsed.Seqno)
	}
}

func TestPersistArchiveCurrentStateStoresHistoricalAppliedStates(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)

	master := testBlockID(-1, topShard, 70)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	historical := &tnstore.BlockState{
		Block: testBlockID(0, topShard, 130),
		Cell:  testShardStateCell(t, testBlockID(0, topShard, 130)),
	}
	states := append([]*tnstore.BlockState{historical}, testCurrentBlockStates(current)...)
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store},
		ctx:     ctx,
	}

	if _, err := runner.persistArchiveCurrentState(current, 1, 0, testStateCheckpointEntriesForCurrent(states, current), newTestStateCellWindowCache(nil).beginCheckpoint()); err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if _, err := store.BlockState(ctx, historical.Block); err != nil {
		t.Fatalf("load historical archive state: %v", err)
	}
}

func TestPersistArchiveCurrentStateMarksLiveCheckpointStatesFlushed(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	flusher := &testLiveCheckpointFlusher{}

	master := testBlockID(-1, topShard, 72)
	shard := testBlockID(0, topShard, 132)
	shardKey := tnstore.ShardKeyFromBlock(shard)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			shardKey: {
				Block: shard,
				Cell:  testShardStateCell(t, shard),
			},
		},
	}
	entries := testStateCheckpointEntriesForCurrent(testCurrentBlockStates(current), current)
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store, liveState: flusher},
		ctx:     ctx,
	}

	if _, err := runner.persistArchiveCurrentState(current, 1, 0, entries, newTestStateCellWindowCache(nil).beginCheckpoint()); err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}

	var want []ton.BlockIDExt
	for _, entry := range entries {
		if entry.State != nil {
			want = append(want, entry.State.Block)
		}
	}
	if len(flusher.blocks) != len(want) {
		t.Fatalf("flushed live states = %d, want %d", len(flusher.blocks), len(want))
	}
	for i := range want {
		if !flusher.blocks[i].Equals(&want[i]) {
			t.Fatalf("flushed live state[%d] = %s, want %s", i, tnstore.FormatBlockRef(flusher.blocks[i]), tnstore.FormatBlockRef(want[i]))
		}
	}
}

func TestPersistArchiveCurrentStateStoresImportedMetadataOnlyState(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)

	master := testBlockID(-1, topShard, 71)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	historicalBlock := testBlockID(0, topShard, 131)
	historicalRoot := testShardStateCell(t, historicalBlock)
	preparedCells, err := tnstore.PrepareReachableStateCells(historicalRoot)
	if err != nil {
		t.Fatalf("prepare historical cells: %v", err)
	}
	stateRootHash := historicalRoot.HashKey(0)
	historical := &tnstore.BlockState{
		Block:         historicalBlock,
		StateRootHash: stateRootHash[:],
	}

	overlay := newTestStateCellWindowCache(store.LazyCellLoader())
	currentCells, err := tnstore.PrepareReachableStateCells(current.Masterchain.Cell)
	if err != nil {
		t.Fatalf("prepare current cells: %v", err)
	}
	if err = rememberAppliedForTest(overlay, current.Masterchain.Cell, currentCells); err != nil {
		t.Fatalf("remember current cells: %v", err)
	}
	if err = rememberAppliedForTest(overlay, historicalRoot, preparedCells); err != nil {
		t.Fatalf("remember historical cells: %v", err)
	}
	checkpointCells := overlay.beginCheckpoint()
	entries := append([]tnstore.StateCheckpointBlock{
		testStateCheckpointEntryForCurrent(historical, current),
	}, testStateCheckpointEntriesForCurrent(testCurrentBlockStates(current), current)...)
	runner := &archiveCatchUpRunner{
		service: &Service{log: zerolog.Nop(), storage: store},
		ctx:     ctx,
	}

	if _, err = runner.persistArchiveCurrentState(current, 1, 0, entries, checkpointCells); err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if _, err = store.BlockState(ctx, historical.Block); err != nil {
		t.Fatalf("load historical archive state: %v", err)
	}
	if _, err = store.LoadStateCellTree(ctx, historical.Block, historical.StateRootHash); err != nil {
		t.Fatalf("load historical archive state cells: %v", err)
	}
}

func TestPersistArchiveCurrentStateUsesPrewrittenCells(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := openTestPebbleStorage(t)

	svc := &Service{log: zerolog.Nop(), storage: store}
	svc.stateCellPrewrite = newStateCellPrewriter(zerolog.Nop(), store, 0)
	svc.stateCellPrewrite.start(ctx, ctx, func(fn func()) { go fn() })

	master := testBlockID(-1, topShard, 73)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block: master,
			Cell:  testShardStateCell(t, master),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	historicalBlock := testBlockID(0, topShard, 133)
	historicalRoot := testShardStateCell(t, historicalBlock)
	preparedCells, err := tnstore.PrepareReachableStateCells(historicalRoot)
	if err != nil {
		t.Fatalf("prepare historical cells: %v", err)
	}
	stateRootHash := historicalRoot.HashKey(0)
	historical := &tnstore.BlockState{
		Block:         historicalBlock,
		StateRootHash: stateRootHash[:],
	}

	overlay := newTestStateCellWindowCache(store.LazyCellLoader())
	overlay.setPrewriter(svc.stateCellPrewrite)
	currentCells, err := tnstore.PrepareReachableStateCells(current.Masterchain.Cell)
	if err != nil {
		t.Fatalf("prepare current cells: %v", err)
	}
	if err = rememberAppliedForTest(overlay, current.Masterchain.Cell, currentCells); err != nil {
		t.Fatalf("remember current cells: %v", err)
	}
	if err = rememberAppliedForTest(overlay, historicalRoot, preparedCells); err != nil {
		t.Fatalf("remember historical cells: %v", err)
	}

	checkpointCells := overlay.beginCheckpoint()
	if target, ok := checkpointCells.prewriteTarget(); !ok || target == 0 {
		t.Fatalf("checkpoint cells not marked prewritten: target=%d ok=%v", target, ok)
	}

	entries := append([]tnstore.StateCheckpointBlock{
		testStateCheckpointEntryForCurrent(historical, current),
	}, testStateCheckpointEntriesForCurrent(testCurrentBlockStates(current), current)...)
	runner := &archiveCatchUpRunner{service: svc, ctx: ctx}

	if _, err = runner.persistArchiveCurrentState(current, 1, 0, entries, checkpointCells); err != nil {
		t.Fatalf("persist archive current state: %v", err)
	}
	if _, err = store.BlockState(ctx, historical.Block); err != nil {
		t.Fatalf("load historical archive state: %v", err)
	}
	if _, err = store.LoadStateCellTree(ctx, historical.Block, historical.StateRootHash); err != nil {
		t.Fatalf("load historical archive state cells: %v", err)
	}
}

func TestArchiveCheckpointPersistsEntryStateCellsBeforeMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := newCurrentStatePersistenceTestService(t, store, nil)

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	overlay := newTestStateCellWindowCache(nil)
	if err := rememberAppliedForTest(overlay, root, mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("remember checkpoint cells: %v", err)
	}

	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 72)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	runner := &archiveCatchUpRunner{
		service:             svc,
		ctx:                 ctx,
		current:             current,
		lastCheckpointSeqno: master.SeqNo - 1,
		stateCells:          overlay,
		importCache:         newArchiveImportCache(),
	}
	rememberFullCheckpointStateForTest(t, &runner.checkpointStates, &current.Masterchain)
	if err := runner.startCheckpoint("test"); err != nil {
		t.Fatalf("start checkpoint: %v", err)
	}
	if _, err := runner.finishCheckpoint(true); err != nil {
		t.Fatalf("finish checkpoint: %v", err)
	}
	svc.Wait()

	if _, err := store.LoadStateCellTree(ctx, master, rootHash[:]); err != nil {
		t.Fatalf("load persisted checkpoint state cells: %v", err)
	}
}

func TestNextBlockCheckpointPersistsEntryStateCellsBeforeMetadata(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := newCurrentStatePersistenceTestService(t, store, ctx)

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 73)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	stateCells := newTestStateCellWindowCache(nil)
	if err := stateCells.addPreparedStateRecords(rootHash, mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("remember checkpoint cells: %v", err)
	}
	runner := &nextSyncRunner{
		service:      svc,
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   stateCells,
	}
	rememberFullCheckpointStateForTest(t, &runner.checkpointStates, &current.Masterchain)
	if err := runner.flushStagedCurrentSync("test"); err != nil {
		t.Fatalf("flush staged next-block current: %v", err)
	}

	if _, err := store.LoadStateCellTree(ctx, master, rootHash[:]); err != nil {
		t.Fatalf("load persisted next-block checkpoint state cells: %v", err)
	}
}

func TestNextBlockAsyncCheckpointCompletesSnapshotBeforeUnlock(t *testing.T) {
	ctx := context.Background()
	store := openTestPebbleStorage(t)
	svc := newCurrentStatePersistenceTestService(t, store, ctx)

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 76)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	stateCells := newTestStateCellWindowCache(nil)
	if err := stateCells.addPreparedStateRecords(rootHash, mustPreparedReachableStateCells(t, root)); err != nil {
		t.Fatalf("remember checkpoint cells: %v", err)
	}
	runner := &nextSyncRunner{
		service:      svc,
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   stateCells,
	}
	rememberFullCheckpointStateForTest(t, &runner.checkpointStates, &current.Masterchain)

	svc.currentStatePersistMu.Lock()
	checkpoint, artifactPrewriteTarget := runner.checkpoint()
	cells := runner.stateCells.beginCheckpoint()
	callbackDone := make(chan struct{})
	callbackSawUnlocked := false
	next, err := svc.persistNextBlockCurrentStateLocked(current, &runner.timing, checkpoint.entries, cells, artifactPrewriteTarget, func() {
		if svc.currentStatePersistMu.TryLock() {
			callbackSawUnlocked = true
			svc.currentStatePersistMu.Unlock()
		}
		runner.completeCheckpoint(checkpoint)
		close(callbackDone)
	}, nil, 0, time.Now())
	if err != nil {
		t.Fatalf("schedule async checkpoint: %v", err)
	}
	runner.current = next
	svc.Wait()

	select {
	case <-callbackDone:
	default:
		t.Fatal("async checkpoint did not complete checkpoint metadata callback")
	}
	if err = svc.takeCurrentStatePersistError(); err != nil {
		t.Fatalf("async checkpoint persist error: %v", err)
	}
	if callbackSawUnlocked {
		t.Fatal("checkpoint metadata callback ran after persist mutex was unlocked")
	}
	if stale := runner.stateCells.beginCheckpoint(); len(stale.records()) != 0 {
		t.Fatal("async checkpoint left completed cell snapshot pending")
	}
	if remaining := runner.checkpointStates.cloneEntries(); len(remaining) != 0 {
		t.Fatalf("async checkpoint left %d completed state entries pending", len(remaining))
	}
}

func TestNextMasterApplyCellsArePrivateUntilCheckpointMetadata(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x52, 8).EndCell()
	records := mustPreparedReachableStateCells(t, root)
	shared := newTestStateCellWindowCache(nil)
	applyCells := newNextMasterApplyCellWindow(func(hash cell.Hash) (*cell.Cell, error) {
		return shared.loader()(hash)
	})

	block := testBlockID(-1, topShard, 75)
	applyCells.remember(block, records)
	if checkpoint := shared.beginCheckpoint(); hasCellRecord(checkpoint.records(), root.HashKey()) {
		t.Fatal("apply-ahead cells leaked into checkpoint before metadata")
	}

	if err := shared.addPreparedStateRecords(root.HashKey(0), records); err != nil {
		t.Fatalf("commit master cells: %v", err)
	}
	applyCells.forget(block)
	checkpoint := shared.beginCheckpoint()
	if !hasCellRecord(checkpoint.records(), root.HashKey()) {
		t.Fatal("checkpoint does not contain cells committed with metadata")
	}
}

func TestFlushStagedCurrentAsyncFailureKeepsCheckpointStates(t *testing.T) {
	ctx := context.Background()
	shutdownCtx, cancelShutdown := context.WithCancel(ctx)
	store := openTestPebbleStorage(t)
	svc := newCurrentStatePersistenceTestService(t, store, shutdownCtx)

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	preparedCells = removePreparedCellRecord(preparedCells, root.HashKey())

	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 74)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	runner := &nextSyncRunner{
		service:      svc,
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   newTestStateCellWindowCache(nil),
	}
	if err := runner.stateCells.addPreparedRecords(preparedCells); err != nil {
		t.Fatalf("add prepared records: %v", err)
	}
	rememberFullCheckpointStateForTest(t, &runner.checkpointStates, &current.Masterchain)
	cancelShutdown()
	if err := runner.flushStagedCurrent(); err != nil {
		t.Fatalf("schedule staged current flush: %v", err)
	}
	svc.Wait()

	if err := svc.takeCurrentStatePersistError(); err == nil {
		t.Fatal("expected async checkpoint persist error")
	}
	remaining := runner.checkpointStates.cloneEntries()
	if len(remaining) != 1 {
		t.Fatalf("remaining checkpoint states = %d, want 1", len(remaining))
	}
	if !remaining[0].state.Block.Equals(&master) {
		t.Fatalf("remaining checkpoint block = %s, want %s", tnstore.FormatBlockRef(remaining[0].state.Block), tnstore.FormatBlockRef(master))
	}
}

func TestArchiveCheckpointReleasesRetainedCellLoaderOnPersistFailure(t *testing.T) {
	ctx := context.Background()
	store := openManualTestPebbleStorage(t)
	svc := newCurrentStatePersistenceTestService(t, store, nil)

	child := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	preparedCells := mustPreparedReachableStateCells(t, root)
	preparedCells = removePreparedCellRecord(preparedCells, root.HashKey())

	overlay := newTestStateCellWindowCache(nil)
	overlay.addPreparedRecords(preparedCells)

	rootHash := root.HashKey(0)
	master := testBlockID(-1, topShard, 72)
	current := &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: rootHash[:],
			Cell:          root,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{},
	}

	runner := &archiveCatchUpRunner{
		service:             svc,
		ctx:                 ctx,
		current:             current,
		lastCheckpointSeqno: master.SeqNo - 1,
		stateCells:          overlay,
	}
	rememberFullCheckpointStateForTest(t, &runner.checkpointStates, &current.Masterchain)
	if err := store.Close(); err != nil {
		t.Fatalf("close store before checkpoint: %v", err)
	}
	if err := runner.startCheckpoint("test"); err != nil {
		t.Fatalf("start checkpoint: %v", err)
	}
	if _, err := runner.finishCheckpoint(true); err == nil {
		t.Fatal("checkpoint with closed storage succeeded")
	}
	svc.Wait()

	svc.stateCellLoaderMu.RLock()
	defer svc.stateCellLoaderMu.RUnlock()
	if len(svc.stateCellLoaders) != 0 {
		t.Fatalf("failed archive checkpoint left %d retained state cell loaders", len(svc.stateCellLoaders))
	}
}

func TestCurrentStateForNextMasterStateUsesExactShardTarget(t *testing.T) {
	ctx := context.Background()
	master := &tnstore.BlockState{
		Block:         testBlockID(-1, topShard, 51),
		StateRootHash: bytes32(0x51),
	}
	target := testBlockID(0, topShard, 101)
	ahead := testBlockID(0, topShard, 102)
	key := tnstore.ShardKeyFromBlock(target)
	current := &tnstore.CurrentState{
		ShardClientSeqno: master.Block.SeqNo - 1,
		Masterchain: tnstore.BlockState{
			Block:         testBlockID(-1, topShard, 50),
			StateRootHash: bytes32(0x50),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			key: {
				Block: ahead,
				Cell:  testShardStateCell(t, ahead),
			},
		},
	}

	env := newFakeShardStateResolverEnv()
	env.addState(target)
	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current:   current.Shards,
		loadState: env.loadState,
		loadBlock: env.loadBlock,
		apply:     env.apply,
	})

	next, _, err := (&Service{log: zerolog.Nop()}).currentStateForNextMasterState(ctx, current, master, []ton.BlockIDExt{target}, resolver)
	if err != nil {
		t.Fatalf("build next current state: %v", err)
	}
	got := next.Shards[key].Block
	if !got.Equals(&target) {
		t.Fatalf("next shard = %s, want exact target %s", tnstore.FormatBlockRef(got), tnstore.FormatBlockRef(target))
	}
	if loads := env.stateLoads[tnstore.BlockKey(target)]; loads != 1 {
		t.Fatalf("target state loads = %d, want 1", loads)
	}
	if loads := env.blockLoads[tnstore.BlockKey(target)]; loads != 0 {
		t.Fatalf("target block loads = %d, want 0", loads)
	}
}

func TestCurrentStateForNextMasterStatePreservesUnchangedShardMasterchainRef(t *testing.T) {
	ctx := context.Background()
	inclusionMaster := testBlockID(-1, topShard, 50)
	nextMaster := &tnstore.BlockState{
		Block:         testBlockID(-1, topShard, 51),
		StateRootHash: bytes32(0x51),
	}
	shard := testBlockID(0, topShard, 100)
	key := tnstore.ShardKeyFromBlock(shard)
	current := &tnstore.CurrentState{
		ShardClientSeqno: inclusionMaster.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         inclusionMaster,
			StateRootHash: bytes32(0x50),
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			key: {
				Block:          shard,
				Cell:           testShardStateCell(t, shard),
				MasterchainRef: &inclusionMaster,
			},
		},
	}

	resolver := newShardStateResolver(ctx, shardStateResolverConfig{
		current: current.Shards,
	})
	next, _, err := (&Service{log: zerolog.Nop()}).currentStateForNextMasterState(ctx, current, nextMaster, []ton.BlockIDExt{shard}, resolver)
	if err != nil {
		t.Fatalf("build next current state: %v", err)
	}

	got := next.Shards[key].MasterchainRef
	if got == nil {
		t.Fatal("unchanged shard masterchain ref was lost")
	}
	// Matches cppnode BlockHandle::masterchain_ref_block: the ref is the masterchain block
	// that first included this shard block, and unchanged shard blocks must not move it forward.
	if !got.Equals(&inclusionMaster) {
		t.Fatalf("unchanged shard masterchain ref = %s, want inclusion master %s", tnstore.FormatBlockRef(*got), tnstore.FormatBlockRef(inclusionMaster))
	}
	if got.Equals(&nextMaster.Block) {
		t.Fatalf("unchanged shard masterchain ref moved to next master %s", tnstore.FormatBlockRef(nextMaster.Block))
	}
}

func TestVerifiedMasterchainQueueAcceptsBroadcastOutsideCatchUp(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	downloaded := testPreparedMasterchainBlock(prev, next)

	svc := newMasterchainQueueTestService()
	svc.queuePreparedMasterchainBlockFromSource(downloaded, p2p.PeerID{})

	got, err := svc.takeQueuedMasterchainBlock(prev, next)
	if err != nil {
		t.Fatal("expected queued masterchain block to be available")
	}
	if !got.ID.Equals(&next) {
		t.Fatalf("queued block = %s, want %s", tnstore.FormatBlockRef(got.ID), tnstore.FormatBlockRef(next))
	}
}

func TestMasterchainBroadcastCandidateWaitsForValidation(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	downloaded := testVerifiedMasterchainBlock(prev, next)
	downloaded.Kind = "tonNode.blockBroadcast"
	downloaded.BlockBOC = []byte{1}
	downloaded.ProofBOC = []byte{2}
	downloaded.consensus = &masterchainConsensusProof{
		block:   next,
		prevRef: prev,
	}

	svc := newMasterchainQueueTestService()
	svc.queueMasterchainBroadcastCandidateFromSource(downloaded, testPeerID("peer-a"))

	if _, err := svc.takeQueuedMasterchainBlock(prev, next); err == nil {
		t.Fatal("unverified broadcast candidate must not be returned by verified fast path")
	}
	candidate, err := svc.peekQueuedMasterchainCandidate(prev, next)
	if err != nil {
		t.Fatal("expected queued masterchain broadcast candidate")
	}
	if candidate.sourcePeerID != testPeerID("peer-a") {
		t.Fatalf("candidate source = %q, want peer-a", candidate.sourcePeerID)
	}
	future, err := svc.queuedMasterchainFuture(testMasterBlockID(9))
	if err != nil || !future.block.Equals(&next) {
		t.Fatalf("candidate future = %v %s, want %s", err, tnstore.FormatBlockRef(future.block), tnstore.FormatBlockRef(next))
	}
}

func TestVerifiedMasterchainQueueDropsFarFutureWhenFull(t *testing.T) {
	svc := newMasterchainQueueTestService()
	for seqno := uint32(10); seqno < 10+nextMasterchainQueueLimit; seqno++ {
		prev := testMasterBlockID(seqno)
		next := testMasterBlockID(seqno + 1)
		svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(prev, next), p2p.PeerID{})
	}

	oldestPrev := testMasterBlockID(10)
	farPrev := testMasterBlockID(1000)
	farNext := testMasterBlockID(1001)
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(farPrev, farNext), p2p.PeerID{})

	oldestNext := testMasterBlockID(11)
	if got, err := svc.takeQueuedMasterchainBlock(oldestPrev, oldestNext); err != nil || !got.ID.Equals(&oldestNext) {
		t.Fatalf("expected oldest queued masterchain block to stay available, got err=%v block=%s", err, tnstore.FormatBlockRef(got.ID))
	}
	if _, err := svc.takeQueuedMasterchainBlock(farPrev, farNext); err == nil {
		t.Fatal("expected far future masterchain block to be dropped")
	}
}

func TestVerifiedMasterchainQueueSeqnoIndexDoesNotDeleteReplacement(t *testing.T) {
	oldPrev := testMasterBlockID(10)
	newPrev := testMasterBlockID(20)
	block := testMasterBlockID(21)
	svc := newMasterchainQueueTestService()

	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(oldPrev, block), testPeerID("old"))
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(newPrev, block), testPeerID("new"))

	if _, err := svc.takeQueuedMasterchainBlock(oldPrev, block); err == nil {
		t.Fatal("expected same-seqno replacement to remove old prev entry")
	}
	if got, err := svc.takeQueuedMasterchainBlock(newPrev, block); err != nil || !got.ID.Equals(&block) {
		t.Fatalf("expected replacement block, got err=%v block=%s", err, tnstore.FormatBlockRef(got.ID))
	}
}

func TestQueuedMasterchainBlockAheadReportsFutureBlock(t *testing.T) {
	current := testMasterBlockID(10)
	futurePrev := testMasterBlockID(12)
	future := testMasterBlockID(13)
	svc := newMasterchainQueueTestService()
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(futurePrev, future), p2p.PeerID{})

	got, err := svc.queuedMasterchainFuture(current)
	if err != nil {
		t.Fatal("expected queued future masterchain block")
	}
	if !got.block.Equals(&future) {
		t.Fatalf("queued future block = %s, want %s", tnstore.FormatBlockRef(got.block), tnstore.FormatBlockRef(future))
	}
}

func TestQueuedMasterchainFutureReportsMissingSeqnoAndSource(t *testing.T) {
	current := testMasterBlockID(10)
	futurePrev := testMasterBlockID(12)
	future := testMasterBlockID(13)
	svc := newMasterchainQueueTestService()
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(futurePrev, future), testPeerID("peer-a"))

	got, err := svc.queuedMasterchainFuture(current)
	if err != nil {
		t.Fatal("expected queued future masterchain block")
	}
	if !got.block.Equals(&future) {
		t.Fatalf("queued future block = %s, want %s", tnstore.FormatBlockRef(got.block), tnstore.FormatBlockRef(future))
	}
	if got.lowestMissingSeqno != current.SeqNo+1 {
		t.Fatalf("lowest missing seqno = %d, want %d", got.lowestMissingSeqno, current.SeqNo+1)
	}
	if got.sourcePeerID != testPeerID("peer-a") {
		t.Fatalf("source key = %q, want peer-a", got.sourcePeerID)
	}
}

func TestNextBlockBootstrapProbeDecisionWidensFanout(t *testing.T) {
	prev := testMasterBlockID(100)
	svc := newMasterchainProbeTestService(t)
	base := mustNextBlockBootstrapProbeDecision(t, svc, prev, 0, nextBlockBootstrapProbeState{})
	if base.peerLimit != nextBlockBootstrapProbePeers {
		t.Fatalf("base probe peers = %d, want %d", base.peerLimit, nextBlockBootstrapProbePeers)
	}

	urgent := mustNextBlockBootstrapProbeDecision(t, svc, prev, 0, nextBlockBootstrapProbeState{
		consecutiveMisses: nextBlockBootstrapUrgentMisses,
		liveTail:          true,
	})
	if urgent.peerLimit != nextBlockBootstrapUrgentPeers {
		t.Fatalf("urgent probe peers = %d, want %d", urgent.peerLimit, nextBlockBootstrapUrgentPeers)
	}

	wide := mustNextBlockBootstrapProbeDecision(t, svc, prev, 0, nextBlockBootstrapProbeState{
		consecutiveMisses: nextBlockBootstrapWideMisses,
		liveTail:          true,
	})
	if wide.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("wide probe peers = %d, want %d", wide.peerLimit, nextBlockBootstrapWidePeers)
	}

	catchUpWide := mustNextBlockBootstrapProbeDecision(t, svc, prev, 0, nextBlockBootstrapProbeState{
		consecutiveMisses: nextBlockBootstrapWideMisses,
	})
	if catchUpWide.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("catch-up wide probe peers = %d, want %d", catchUpWide.peerLimit, nextBlockBootstrapWidePeers)
	}
}

func TestNextBlockBootstrapProbeDecisionUsesFutureQueueAndLag(t *testing.T) {
	prev := testMasterBlockID(100)
	futurePrev := testMasterBlockID(101)
	future := testMasterBlockID(102)
	svc := newMasterchainProbeTestService(t)
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(futurePrev, future), testPeerID("peer-a"))

	queued := mustNextBlockBootstrapProbeDecision(t, svc, prev, 0, nextBlockBootstrapProbeState{})
	if queued.peerLimit != nextBlockBootstrapProbePeers {
		t.Fatalf("cold queued future probe peers = %d, want %d", queued.peerLimit, nextBlockBootstrapProbePeers)
	}
	if !queued.preferredSourcePeerID.IsZero() {
		t.Fatalf("cold queued future preferred source = %q, want empty", queued.preferredSourcePeerID)
	}

	queued = mustNextBlockBootstrapProbeDecision(t, svc, prev, 0, nextBlockBootstrapProbeState{liveTail: true})
	if queued.peerLimit != nextBlockBootstrapUrgentPeers {
		t.Fatalf("queued future probe peers = %d, want %d", queued.peerLimit, nextBlockBootstrapUrgentPeers)
	}
	if !queued.queuedFutureAhead || queued.aheadBlocks != future.SeqNo-prev.SeqNo {
		t.Fatalf("unexpected queued future decision %+v", queued)
	}
	if queued.preferredSourcePeerID != testPeerID("peer-a") {
		t.Fatalf("queued future preferred source = %q, want peer-a", queued.preferredSourcePeerID)
	}
	if queued.lowestMissingSeqno != prev.SeqNo+1 {
		t.Fatalf("queued future lowest missing = %d, want %d", queued.lowestMissingSeqno, prev.SeqNo+1)
	}

	baseLive := mustNextBlockBootstrapProbeDecision(t, svc, prev, 0, nextBlockBootstrapProbeState{liveTail: true})
	if baseLive.probeTimeout() != nextBlockBootstrapLiveProbeTimeout {
		t.Fatalf("live probe timeout = %s, want %s", baseLive.probeTimeout(), nextBlockBootstrapLiveProbeTimeout)
	}
	if baseLive.stagedPeerLimit() != nextBlockBootstrapWidePeers {
		t.Fatalf("live staged probe peers = %d, want %d", baseLive.stagedPeerLimit(), nextBlockBootstrapWidePeers)
	}

	oldUTime := time.Now().Add(-time.Duration(nextBlockBootstrapWideLagSeconds+1) * time.Second).Unix()
	lagged := mustNextBlockBootstrapProbeDecision(t, svc, prev, oldUTime, nextBlockBootstrapProbeState{liveTail: true})
	if lagged.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("lagged probe peers = %d, want %d", lagged.peerLimit, nextBlockBootstrapWidePeers)
	}

	lagged = mustNextBlockBootstrapProbeDecision(t, svc, prev, oldUTime, nextBlockBootstrapProbeState{})
	if lagged.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("lagged catch-up probe peers = %d, want %d", lagged.peerLimit, nextBlockBootstrapWidePeers)
	}
}

func TestNextBlockBootstrapRawBroadcastFanout(t *testing.T) {
	prev := testMasterBlockID(100)

	nextPending := nextBlockBootstrapProbeDecision{
		peerLimit:         nextBlockBootstrapProbePeers,
		liveTail:          true,
		prevSeqno:         prev.SeqNo,
		rawBroadcastAhead: true,
		rawBroadcastSeqno: prev.SeqNo + 1,
	}
	if !nextPending.rawBroadcastNextPending() {
		t.Fatal("raw broadcast at prev+1 should be next-pending")
	}
	if nextPending.shouldUseUrgentFanout() {
		t.Fatal("next-pending raw broadcast must not trigger urgent fanout")
	}

	beyond := nextPending
	beyond.rawBroadcastSeqno = prev.SeqNo + 2
	if beyond.rawBroadcastNextPending() {
		t.Fatal("raw broadcast beyond next must not be next-pending")
	}
	if !beyond.shouldUseUrgentFanout() {
		t.Fatal("raw broadcast beyond next should trigger urgent fanout")
	}
}

func TestNextMasterchainProbeHoldDelay(t *testing.T) {
	now := time.Now()
	state := &nextBlockBootstrapProbeState{liveTail: true}

	decision := &nextBlockBootstrapProbeDecision{liveTail: true, prevSeqno: 100}
	if hold := nextMasterchainProbeHoldDelay(decision, state, now); hold != 0 {
		t.Fatalf("hold without signals = %s, want 0", hold)
	}

	// raw broadcast for exactly the next block grants the decode grace once
	decision.rawBroadcastAhead = true
	decision.rawBroadcastSeqno = 101
	if hold := nextMasterchainProbeHoldDelay(decision, state, now); hold != nextBlockBootstrapDecodeGrace {
		t.Fatalf("grace hold = %s, want %s", hold, nextBlockBootstrapDecodeGrace)
	}
	if hold := nextMasterchainProbeHoldDelay(decision, state, now); hold != 0 {
		t.Fatalf("second grace hold = %s, want 0 (one-shot per seqno)", hold)
	}

	// catch-up mode never parks the probe
	catchUpState := &nextBlockBootstrapProbeState{}
	catchUpDecision := &nextBlockBootstrapProbeDecision{prevSeqno: 100, rawBroadcastAhead: true, rawBroadcastSeqno: 101}
	if hold := nextMasterchainProbeHoldDelay(catchUpDecision, catchUpState, now); hold != 0 {
		t.Fatalf("catch-up hold = %s, want 0", hold)
	}

	// download-only signals cancel the pace, plain live tail keeps it
	paced := &nextBlockBootstrapProbeState{liveTail: true, lastObtainAt: now, obtainInterval: time.Second, lastObtainFromBroadcast: true}
	seen := &nextBlockBootstrapProbeDecision{liveTail: true, prevSeqno: 100, seenAhead: true}
	if hold := nextMasterchainProbeHoldDelay(seen, paced, now); hold != 0 {
		t.Fatalf("seen-ahead hold = %s, want 0", hold)
	}
	pacedOnly := &nextBlockBootstrapProbeDecision{liveTail: true, prevSeqno: 100}
	if hold := nextMasterchainProbeHoldDelay(pacedOnly, paced, now); hold != time.Second+nextBlockBootstrapPaceHeadroom {
		t.Fatalf("pace hold = %s, want %s", hold, time.Second+nextBlockBootstrapPaceHeadroom)
	}

	// a lagged head disables the pace but keeps the decode grace
	lagged := &nextBlockBootstrapProbeDecision{
		liveTail:   true,
		prevSeqno:  100,
		hasLag:     true,
		lagSeconds: nextBlockBootstrapUrgentLagSeconds,
	}
	if hold := nextMasterchainProbeHoldDelay(lagged, paced, now); hold != 0 {
		t.Fatalf("lagged pace hold = %s, want 0", hold)
	}
	laggedRaw := *lagged
	laggedRaw.rawBroadcastAhead = true
	laggedRaw.rawBroadcastSeqno = 101
	laggedState := &nextBlockBootstrapProbeState{liveTail: true, lastObtainAt: now, obtainInterval: time.Second, lastObtainFromBroadcast: true}
	if hold := nextMasterchainProbeHoldDelay(&laggedRaw, laggedState, now); hold != nextBlockBootstrapDecodeGrace {
		t.Fatalf("lagged grace hold = %s, want %s", hold, nextBlockBootstrapDecodeGrace)
	}
}

func TestNextBlockBootstrapProbePace(t *testing.T) {
	start := time.Now()
	state := &nextBlockBootstrapProbeState{liveTail: true}

	if got := state.probeDelay(start); got != 0 {
		t.Fatalf("delay without samples = %s, want 0", got)
	}

	state.noteObtained(start, true)
	if got := state.probeDelay(start); got != 0 {
		t.Fatalf("delay after first obtain = %s, want 0", got)
	}

	state.noteObtained(start.Add(400*time.Millisecond), true)
	if state.obtainInterval != 400*time.Millisecond {
		t.Fatalf("interval = %s, want 400ms", state.obtainInterval)
	}
	// target = interval + headroom, so the probe deadline sits past the
	// typical broadcast arrival instead of racing it
	wantDelay := 400*time.Millisecond + nextBlockBootstrapPaceHeadroom - 100*time.Millisecond
	if got := state.probeDelay(start.Add(500 * time.Millisecond)); got != wantDelay {
		t.Fatalf("delay = %s, want %s", got, wantDelay)
	}
	if got := state.probeDelay(start.Add(400*time.Millisecond + 400*time.Millisecond + nextBlockBootstrapPaceHeadroom)); got != 0 {
		t.Fatalf("elapsed delay = %s, want 0", got)
	}

	// the miss retry loop must not be paced
	state.consecutiveMisses = 1
	if got := state.probeDelay(start.Add(500 * time.Millisecond)); got != 0 {
		t.Fatalf("miss retry delay = %s, want 0", got)
	}
	state.consecutiveMisses = 0

	// a download-sourced obtain means broadcasts are not delivering: the pace
	// is a bet on the next broadcast, so it must not be placed
	state.noteObtained(start.Add(800*time.Millisecond), false)
	if got := state.probeDelay(start.Add(850 * time.Millisecond)); got != 0 {
		t.Fatalf("download-sourced delay = %s, want 0", got)
	}
	// the next broadcast-sourced obtain re-arms the pace
	state.noteObtained(start.Add(1200*time.Millisecond), true)
	if got := state.probeDelay(start.Add(1250 * time.Millisecond)); got == 0 {
		t.Fatal("broadcast-sourced obtain did not re-arm the pace")
	}

	// one stalled block cannot inflate the pace beyond the slew limit
	state.noteObtained(start.Add(10*time.Second), true)
	if state.obtainInterval > time.Second {
		t.Fatalf("interval after stall = %s, want slew-limited", state.obtainInterval)
	}
}

func TestHoldNextMasterchainProbeReturnsQueuedBlock(t *testing.T) {
	prev := testMasterBlockID(100)
	next := testMasterBlockID(101)
	svc := newMasterchainProbeTestService(t)

	state := &nextBlockBootstrapProbeState{liveTail: true}
	type holdResult struct {
		cached cachedMasterchainBlockForApply
		err    error
	}
	done := make(chan holdResult, 1)
	go func() {
		cached, err := svc.holdNextMasterchainProbe(context.Background(), prev, masterchainSeqnoTarget(^uint32(0)), state, time.Second)
		done <- holdResult{cached: cached, err: err}
	}()

	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(prev, next), testPeerID("peer-a"))
	svc.wakeCurrentStateSync()

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("hold returned error: %v", res.err)
		}
		if !res.cached.block.ID.Equals(&next) {
			t.Fatalf("hold block = %s, want %s", res.cached.block.BlockRef(), tnstore.FormatBlockRef(next))
		}
		if res.cached.source != SyncBlockSourceBroadcastQueue {
			t.Fatalf("hold source = %q, want broadcast_queue", res.cached.source)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hold did not return the queued block")
	}
}

func TestHoldNextMasterchainProbeExpires(t *testing.T) {
	prev := testMasterBlockID(100)
	svc := newMasterchainProbeTestService(t)
	state := &nextBlockBootstrapProbeState{liveTail: true}

	started := time.Now()
	_, err := svc.holdNextMasterchainProbe(context.Background(), prev, masterchainSeqnoTarget(^uint32(0)), state, 30*time.Millisecond)
	if !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("hold error = %v, want not found", err)
	}
	if time.Since(started) < 30*time.Millisecond {
		t.Fatal("hold returned before the deadline")
	}
}

func TestExactBlockDownloadProbeDecisionWidensFanout(t *testing.T) {
	base := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started: time.Now(),
	})
	if base.peerLimit != exactBlockDownloadProbePeers {
		t.Fatalf("base exact probe peers = %d, want %d", base.peerLimit, exactBlockDownloadProbePeers)
	}
	if base.stagedPeerLimit != exactBlockDownloadProbePeers {
		t.Fatalf("base exact staged probe peers = %d, want %d", base.stagedPeerLimit, exactBlockDownloadProbePeers)
	}

	urgent := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started:           time.Now(),
		consecutiveMisses: nextBlockBootstrapUrgentMisses,
	})
	if urgent.peerLimit != nextBlockBootstrapUrgentPeers {
		t.Fatalf("urgent exact probe peers = %d, want %d", urgent.peerLimit, nextBlockBootstrapUrgentPeers)
	}
	if urgent.stagedPeerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("urgent exact staged probe peers = %d, want %d", urgent.stagedPeerLimit, nextBlockBootstrapWidePeers)
	}

	wide := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started:           time.Now(),
		consecutiveMisses: nextBlockBootstrapWideMisses,
	})
	if wide.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("wide exact probe peers = %d, want %d", wide.peerLimit, nextBlockBootstrapWidePeers)
	}

	waited := (&Service{}).exactBlockDownloadProbeDecision(exactBlockDownloadProbeState{
		started: time.Now().Add(-time.Duration(nextBlockBootstrapWideLagSeconds+1) * time.Second),
	})
	if waited.peerLimit != nextBlockBootstrapWidePeers {
		t.Fatalf("waited exact probe peers = %d, want %d", waited.peerLimit, nextBlockBootstrapWidePeers)
	}
}

func TestNextSyncRunnerShouldYieldBootstrapToArchive(t *testing.T) {
	oldUTime := time.Now().Add(-time.Duration(nextToArchiveLagSeconds+1) * time.Second).Unix()
	freshUTime := time.Now().Add(-time.Duration(nextToArchiveLagSeconds-1) * time.Second).Unix()

	runner := &nextSyncRunner{mode: nextSyncBootstrap}
	if !runner.shouldYieldBootstrapToArchive(oldUTime) {
		t.Fatal("expected unlimited bootstrap to yield when archive lag threshold is crossed")
	}
	if runner.shouldYieldBootstrapToArchive(freshUTime) {
		t.Fatal("did not expect unlimited bootstrap to yield below archive lag threshold")
	}

	runner.maxBlocks = 1
	if runner.shouldYieldBootstrapToArchive(oldUTime) {
		t.Fatal("did not expect bounded bootstrap to yield to archive")
	}
}

func TestCurrentStateWakeInterruptsLivePollDelay(t *testing.T) {
	svc := &Service{currentStateWake: make(chan struct{})}

	// Taken before the wake can fire, exactly as the production caller does.
	stateWake := svc.currentStateWakeChan()
	go func() {
		time.Sleep(10 * time.Millisecond)
		svc.wakeCurrentStateSync()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()
	if !svc.waitCurrentStatePoll(ctx, stateWake, time.Hour) {
		t.Fatal("expected current state poll wake")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("wake took %s", elapsed)
	}
}

// One wake must release every concurrent waiter: the previous single-token
// channel let any one of the sync selectors consume a wake meant for another,
// leaving that waiter to sit out its full retry or pace-hold timer.
func TestCurrentStateWakeReleasesAllConcurrentWaiters(t *testing.T) {
	svc := &Service{currentStateWake: make(chan struct{})}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const waiters = 2
	done := make(chan error, waiters)
	// One channel taken before the wake and shared by both waiters: the wake
	// must release every holder of it, not just the first.
	stateWake := svc.currentStateWakeChan()
	for i := 0; i < waiters; i++ {
		go func() {
			done <- svc.waitShardStateCatchUpRetry(ctx, stateWake, time.Hour)
		}()
	}

	time.Sleep(10 * time.Millisecond)
	svc.wakeCurrentStateSync()

	for i := 0; i < waiters; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("waiter %d returned error: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d was not released by a single wake", i)
		}
	}
}

func TestCurrentStateWakeInterruptsPersistWait(t *testing.T) {
	svc := &Service{currentStateWake: make(chan struct{})}
	svc.currentStatePersistMu.Lock()
	defer svc.currentStatePersistMu.Unlock()

	go func() {
		time.Sleep(10 * time.Millisecond)
		svc.wakeCurrentStateSync()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()
	woken, err := svc.waitCurrentStatePersistOrWake(ctx)
	if err != nil {
		t.Fatalf("wait current state persist or wake: %v", err)
	}
	if !woken {
		t.Fatal("expected current state wake")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("wake took %s", elapsed)
	}
}

func TestShardStateCatchUpRetryDelayIsShort(t *testing.T) {
	if shardStateCatchUpRetryDelay != 500*time.Millisecond {
		t.Fatalf("shard state catch-up retry delay = %s, want 500ms", shardStateCatchUpRetryDelay)
	}
}

func TestCurrentStateWakeInterruptsShardStateCatchUpRetry(t *testing.T) {
	svc := &Service{currentStateWake: make(chan struct{})}

	// Taken before the wake can fire, exactly as the production caller does.
	stateWake := svc.currentStateWakeChan()
	go func() {
		time.Sleep(10 * time.Millisecond)
		svc.wakeCurrentStateSync()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()
	if err := svc.waitShardStateCatchUpRetry(ctx, stateWake, time.Hour); err != nil {
		t.Fatalf("wait shard state catch-up retry: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("wake took %s", elapsed)
	}
}

func TestNextMasterchainApplyCandidateUsesBroadcastWhilePeerProbePrepares(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	svc := newMasterchainQueueTestService()
	result := make(chan nextBlockProbeResult, 1)

	queryCtx, cancelQuery := context.WithCancel(context.Background())
	defer cancelQuery()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got := make(chan nextMasterchainApplyCandidateTestResult, 1)
	go func() {
		block, source, prepareElapsed, err := svc.waitNextMasterchainApplyCandidate(
			ctx,
			queryCtx,
			prev,
			masterchainSeqnoTarget(^uint32(0)),
			nil,
			nil,
			result,
			cancelQuery,
		)
		got <- nextMasterchainApplyCandidateTestResult{
			block:          block,
			source:         source,
			prepareElapsed: prepareElapsed,
			err:            err,
		}
	}()

	time.Sleep(10 * time.Millisecond)
	svc.queuePreparedMasterchainBlockFromSource(testPreparedMasterchainBlock(prev, next), testPeerID("broadcast-peer"))
	svc.wakeCurrentStateSync()

	select {
	case res := <-got:
		if res.err != nil {
			t.Fatalf("wait next masterchain apply candidate: %v", res.err)
		}
		if !res.block.ID.Equals(&next) {
			t.Fatalf("candidate block = %s, want %s", tnstore.FormatBlockRef(res.block.ID), tnstore.FormatBlockRef(next))
		}
		if res.source != "broadcast_queue" {
			t.Fatalf("candidate source = %q, want broadcast_queue", res.source)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for broadcast candidate")
	}
}

func TestNextMasterchainApplyCandidateReturnsPreparedPeerResult(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	svc := newMasterchainQueueTestService()
	result := make(chan nextBlockProbeResult, 1)
	prepared := testPreparedMasterchainBlock(prev, next)
	prepared.PrepareElapsed = 7 * time.Millisecond
	result <- nextBlockProbeResult{
		block:          prepared,
		source:         SyncBlockSourcePeerProbe,
		prepareElapsed: prepared.PrepareElapsed,
	}

	queryCtx, cancelQuery := context.WithCancel(context.Background())
	defer cancelQuery()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	block, source, prepareElapsed, err := svc.waitNextMasterchainApplyCandidate(
		ctx,
		queryCtx,
		prev,
		masterchainSeqnoTarget(^uint32(0)),
		nil,
		nil,
		result,
		cancelQuery,
	)
	if err != nil {
		t.Fatalf("wait next masterchain apply candidate: %v", err)
	}
	if !block.ID.Equals(&next) {
		t.Fatalf("candidate block = %s, want %s", tnstore.FormatBlockRef(block.ID), tnstore.FormatBlockRef(next))
	}
	if source != "peer_probe" {
		t.Fatalf("candidate source = %q, want peer_probe", source)
	}
	if prepareElapsed != prepared.PrepareElapsed {
		t.Fatalf("prepare elapsed = %s, want %s", prepareElapsed, prepared.PrepareElapsed)
	}
}

func TestNextMasterchainApplyCandidateDoesNotTimeoutAfterProbeReturned(t *testing.T) {
	prev := testMasterBlockID(10)
	next := testMasterBlockID(11)
	svc := newMasterchainQueueTestService()
	result := make(chan nextBlockProbeResult, 1)
	probeReturned := make(chan struct{})
	close(probeReturned)

	queryCtx, cancelQuery := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelQuery()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got := make(chan nextMasterchainApplyCandidateTestResult, 1)
	go func() {
		block, source, prepareElapsed, err := svc.waitNextMasterchainApplyCandidate(
			ctx,
			queryCtx,
			prev,
			masterchainSeqnoTarget(^uint32(0)),
			nil,
			probeReturned,
			result,
			cancelQuery,
		)
		got <- nextMasterchainApplyCandidateTestResult{
			block:          block,
			source:         source,
			prepareElapsed: prepareElapsed,
			err:            err,
		}
	}()

	time.Sleep(30 * time.Millisecond)
	prepared := testPreparedMasterchainBlock(prev, next)
	prepared.PrepareElapsed = 5 * time.Millisecond
	result <- nextBlockProbeResult{
		block:          prepared,
		source:         SyncBlockSourcePeerProbe,
		prepareElapsed: prepared.PrepareElapsed,
	}

	select {
	case res := <-got:
		if res.err != nil {
			t.Fatalf("wait next masterchain apply candidate: %v", res.err)
		}
		if !res.block.ID.Equals(&next) {
			t.Fatalf("candidate block = %s, want %s", tnstore.FormatBlockRef(res.block.ID), tnstore.FormatBlockRef(next))
		}
		if res.source != "peer_probe" {
			t.Fatalf("candidate source = %q, want peer_probe", res.source)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for prepared peer candidate")
	}
}

type nextMasterchainApplyCandidateTestResult struct {
	block          PreparedBlock
	source         SyncBlockSource
	prepareElapsed time.Duration
	err            error
}

func testPreparedMasterchainBlock(prev ton.BlockIDExt, block ton.BlockIDExt) PreparedBlock {
	return PreparedBlock{
		ID: block,
		Meta: &tnstore.BlockMeta{
			ID:       block,
			PrevRefs: []ton.BlockIDExt{prev},
		},
	}
}

func testVerifiedMasterchainBlock(prev ton.BlockIDExt, block ton.BlockIDExt) VerifiedBlock {
	return VerifiedBlock{
		ID: block,
		Meta: &tnstore.BlockMeta{
			ID:       block,
			PrevRefs: []ton.BlockIDExt{prev},
		},
	}
}

func TestFlushStagedCurrentSyncPersistsAfterContextCancel(t *testing.T) {
	store := openTestPebbleStorage(t)
	current, baseKey, master, base := testCurrentStateForShutdownFlush(t, 70, 120)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	defer cancelShutdown()

	runner := &nextSyncRunner{
		service:      newCurrentStatePersistenceTestService(t, store, shutdownCtx),
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   newTestStateCellWindowCache(nil),
	}
	for _, state := range testCurrentBlockStates(current) {
		rememberFullCheckpointStateForTest(t, &runner.checkpointStates, state)
	}

	if err := runner.flushStagedCurrentSync("test_shutdown"); err != nil {
		t.Fatalf("flush staged current after cancel: %v", err)
	}
	if runner.stagedBlocks != 0 {
		t.Fatalf("staged blocks = %d, want 0", runner.stagedBlocks)
	}

	persisted, err := store.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("load persisted current state: %v", err)
	}
	if !persisted.Masterchain.Block.Equals(&master) {
		t.Fatalf("persisted masterchain = %s, want %s", tnstore.FormatBlockRef(persisted.Masterchain.Block), tnstore.FormatBlockRef(master))
	}
	if persisted.ShardClientSeqno != master.SeqNo {
		t.Fatalf("persisted shard client seqno = %d, want %d", persisted.ShardClientSeqno, master.SeqNo)
	}
	if got := persisted.Shards[baseKey].Block; !got.Equals(&base) {
		t.Fatalf("persisted basechain = %s, want %s", tnstore.FormatBlockRef(got), tnstore.FormatBlockRef(base))
	}
}

func TestFlushStagedCurrentSyncStopsWhenShutdownContextCanceled(t *testing.T) {
	store := openTestPebbleStorage(t)
	current, _, _, _ := testCurrentStateForShutdownFlush(t, 71, 121)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	cancelShutdown()

	runner := &nextSyncRunner{
		service:      newCurrentStatePersistenceTestService(t, store, shutdownCtx),
		ctx:          ctx,
		current:      current,
		stagedBlocks: 1,
		timing:       newCatchUpTiming(time.Now()),
		stateCells:   newTestStateCellWindowCache(nil),
	}
	for _, state := range testCurrentBlockStates(current) {
		rememberFullCheckpointStateForTest(t, &runner.checkpointStates, state)
	}

	err := runner.flushStagedCurrentSync("test_shutdown")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("flush staged current error = %v, want context canceled", err)
	}
	if runner.stagedBlocks != 1 {
		t.Fatalf("staged blocks = %d, want 1", runner.stagedBlocks)
	}
	if _, err = store.CurrentState(context.Background()); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("current state error = %v, want not found", err)
	}
}

func testCurrentStateForShutdownFlush(t *testing.T, masterSeqno, baseSeqno uint32) (*tnstore.CurrentState, tnstore.ShardKey, ton.BlockIDExt, ton.BlockIDExt) {
	t.Helper()

	master := testBlockID(-1, topShard, masterSeqno)
	base := testBlockID(0, topShard, baseSeqno)
	baseKey := tnstore.ShardKeyFromBlock(base)

	return &tnstore.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain: tnstore.BlockState{
			Block:         master,
			StateRootHash: master.RootHash,
		},
		Shards: map[tnstore.ShardKey]tnstore.BlockState{
			baseKey: {
				Block: base,
				Cell:  testShardStateCell(t, base),
			},
		},
	}, baseKey, master, base
}

func TestQueuedMasterchainBlockTooFarFromCurrentUsesPublishedHead(t *testing.T) {
	svc := newMasterchainQueueTestService()

	// No published head yet: queueing must never be blocked during bootstrap.
	if svc.queuedMasterchainBlockTooFarFromCurrent(1000) {
		t.Fatal("queueing rejected while no current state is published")
	}

	svc.currentStatus = &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{Block: testMasterBlockID(100)},
	}
	if svc.queuedMasterchainBlockTooFarFromCurrent(50) {
		t.Fatal("block behind the published head reported as too far")
	}
	if svc.queuedMasterchainBlockTooFarFromCurrent(100 + nextMasterchainQueueLimit - 1) {
		t.Fatal("block inside the queue window reported as too far")
	}
	if !svc.queuedMasterchainBlockTooFarFromCurrent(100 + nextMasterchainQueueLimit) {
		t.Fatal("block outside the queue window not reported as too far")
	}

	// A non-masterchain head (never published in practice) must not gate queueing.
	svc.currentStatus = &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{Block: testBlockID(0, topShard, 100)},
	}
	if svc.queuedMasterchainBlockTooFarFromCurrent(1000) {
		t.Fatal("queueing rejected for a non-masterchain published head")
	}
}

// The head check runs per queued masterchain broadcast while nextMasterchainMx
// is held, so it must not clone the shard map to read one seqno.
func TestQueuedMasterchainBlockTooFarFromCurrentDoesNotAllocate(t *testing.T) {
	svc := newMasterchainQueueTestService()
	shards := make(map[tnstore.ShardKey]tnstore.BlockState, 64)
	for i := 0; i < 64; i++ {
		block := testBlockID(0, int64(i+1)<<40, uint32(i))
		shards[tnstore.ShardKeyFromBlock(block)] = tnstore.BlockState{
			Block:         block,
			StateRootHash: bytes32(byte(i)),
		}
	}
	svc.currentStatus = &tnstore.CurrentState{
		Masterchain: tnstore.BlockState{Block: testMasterBlockID(100)},
		Shards:      shards,
	}

	allocs := testing.AllocsPerRun(100, func() {
		svc.queuedMasterchainBlockTooFarFromCurrent(101)
	})
	if allocs != 0 {
		t.Fatalf("queued masterchain head check allocated %.0f times per call, want 0", allocs)
	}
}
