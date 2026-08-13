package service

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errCellGenerationMigrationTest = errors.New("test cell generation migration failure")

func blockStateWithRoot(block ton.BlockIDExt, root *cell.Cell) storage.BlockState {
	rootHash := root.HashKey(0)
	return storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Clone(rootHash[:]),
		Cell:          root,
	}
}

func TestCellGenerationMigrationCanceledLeavesPendingIntent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &testCellGenerationMigrationStore{
		cancelOnBegin: cancel,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	err := state.runCellGenerationMigration(ctx, testBlockID(-1, topShard, 100))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("migration error = %v, want context.Canceled", err)
	}
	if !store.began {
		t.Fatal("migration did not begin candidate generation")
	}
}

func TestCellGenerationMigrationFailureLeavesPendingIntent(t *testing.T) {
	store := &testCellGenerationMigrationStore{
		blockMetaErr: errCellGenerationMigrationTest,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	err := state.runCellGenerationMigration(context.Background(), testBlockID(-1, topShard, 100))
	if !errors.Is(err, errCellGenerationMigrationTest) {
		t.Fatalf("migration error = %v, want test failure", err)
	}
	if !store.began {
		t.Fatal("migration did not begin candidate generation")
	}
}

func TestCellGenerationMigrationCanceledWithNonContextErrorLeavesPendingIntent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &testCellGenerationMigrationStore{
		cancelOnBegin:    cancel,
		blockMetaErr:     errCellGenerationMigrationTest,
		ignoreContextErr: true,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	err := state.runCellGenerationMigration(ctx, testBlockID(-1, topShard, 100))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("migration error = %v, want context.Canceled", err)
	}
	if !errors.Is(err, errCellGenerationMigrationTest) {
		t.Fatalf("migration error = %v, want test failure", err)
	}
}

func TestLoadCellGenerationMigrationProgressAttachesLazyRoots(t *testing.T) {
	generation := uint64(3)
	masterBlock := testBlockID(-1, topShard, 100)
	shardBlock := testBlockID(0, topShard, 200)
	masterRoot := cell.BeginCell().MustStoreUInt(0x10, 8).EndCell()
	shardRoot := cell.BeginCell().MustStoreUInt(0x20, 8).EndCell()
	master := blockStateWithRoot(masterBlock, masterRoot)
	shard := blockStateWithRoot(shardBlock, shardRoot)
	shard.MasterchainRef = &master.Block
	masterCellHash := masterRoot.HashKey()
	shardCellHash := shardRoot.HashKey()

	store := &testCellGenerationMigrationStore{
		migrationProgress: &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: masterBlock.SeqNo,
			Masterchain:      storage.BlockStateWithoutCells(&master),
			Shards: map[storage.ShardKey]storage.BlockState{
				storage.ShardKeyFromBlock(shardBlock): storage.BlockStateWithoutCells(&shard),
			},
		},
		lazyRoots: map[string]*cell.Cell{
			string(masterCellHash[:]): masterRoot,
			string(shardCellHash[:]):  shardRoot,
		},
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	candidate, err := state.loadCellGenerationMigrationProgress(context.Background(), store, generation)
	if err != nil {
		t.Fatalf("load migration progress: %v", err)
	}
	if candidate.current.Masterchain.Cell == nil {
		t.Fatal("masterchain progress cell was not restored")
	}
	if candidate.generation != generation {
		t.Fatalf("candidate generation = %d, want %d", candidate.generation, generation)
	}
	shardState := candidate.current.Shards[storage.ShardKeyFromBlock(shardBlock)]
	if shardState.Cell == nil {
		t.Fatal("shard progress cell was not restored")
	}
}

func TestCellGenerationCandidateResolverTreatsBlockOnlyStateAsMiss(t *testing.T) {
	store := &testCellGenerationMigrationStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})
	candidate := &cellGenerationCandidate{
		generation: 3,
		current: &storage.CurrentState{
			Shards: map[storage.ShardKey]storage.BlockState{},
		},
		cells: newTestStateCellWindowCache(nil),
	}

	resolver := state.newCellGenerationShardResolver(context.Background(), candidate, nil)
	_, err := resolver.loadState(context.Background(), storage.BlockState{
		Block: testBlockID(0, topShard, 65640691),
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("candidate block-only load error = %v, want ErrNotFound", err)
	}
}

func TestStartCellGenerationMigrationPersistsIntentBeforeAsyncRun(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	store := &testCellGenerationMigrationStore{
		current: &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: origin.SeqNo,
			Masterchain:      storage.BlockState{Block: origin},
			Shards:           map[storage.ShardKey]storage.BlockState{},
		},
		blockStateErr: errCellGenerationMigrationTest,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	err := state.StartCellGenerationMigration(context.Background(), origin.SeqNo)
	if err != nil {
		t.Fatalf("start migration: %v", err)
	}
	if store.beginCount.Load() == 0 {
		t.Fatal("migration intent was not persisted before start returned")
	}

	state.Wait()
}

func TestStartCellGenerationMigrationRejectsMissingPersistentStateBeforeIntent(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	store := &testCellGenerationMigrationStore{
		current: &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: origin.SeqNo,
			Masterchain:      storage.BlockState{Block: origin},
			Shards:           map[storage.ShardKey]storage.BlockState{},
		},
		persistentStateFileErr: storage.ErrNotFound,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	err := state.StartCellGenerationMigration(context.Background(), origin.SeqNo)
	if !errors.Is(err, errCellGenerationPersistentMissing) {
		t.Fatalf("start migration error = %v, want missing persistent state", err)
	}
	if store.beginCount.Load() != 0 {
		t.Fatal("migration intent was persisted before checking persistent state file")
	}
	if store.persistentStateFileCalls == 0 {
		t.Fatal("persistent state file was not checked")
	}
}

func TestRunCellGenerationMigrationDropsPendingWhenPersistentStateMissing(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	store := &testCellGenerationMigrationStore{
		pending:                &pending,
		persistentStateFileErr: storage.ErrNotFound,
		blockMetas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(origin): {
				ID:            origin,
				StateRootHash: bytes.Repeat([]byte{0x42}, 32),
			},
		},
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	err := state.runCellGenerationMigration(context.Background(), origin)
	if !errors.Is(err, errCellGenerationMigrationAborted) {
		t.Fatalf("run migration error = %v, want aborted", err)
	}
	if !store.dropped {
		t.Fatal("pending generation was not dropped")
	}
	if store.droppedGeneration != pending.ID {
		t.Fatalf("dropped generation = %d, want %d", store.droppedGeneration, pending.ID)
	}
}

func TestStartCellGenerationMigrationRespectsExclusiveTask(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	store := &testCellGenerationMigrationStore{
		current: &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: origin.SeqNo,
			Masterchain:      storage.BlockState{Block: origin},
			Shards:           map[storage.ShardKey]storage.BlockState{},
		},
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})
	state.maintenance.exclusiveTask = exclusiveServiceTaskStateSerialization

	err := state.StartCellGenerationMigration(context.Background(), origin.SeqNo)
	if !errors.Is(err, errStateSerializationRunning) {
		t.Fatalf("start migration error = %v, want serialization running", err)
	}
	if store.beginCount.Load() != 0 {
		t.Fatal("migration intent was persisted while serialization task was active")
	}
}

func TestStopCellGenerationMigrationAbortsPendingGeneration(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	store := &testCellGenerationMigrationStore{
		pending: &pending,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	if err := state.StopCellGenerationMigration(context.Background()); err != nil {
		t.Fatalf("stop migration: %v", err)
	}
	if !store.dropped {
		t.Fatal("pending generation was not dropped")
	}
	if store.droppedGeneration != pending.ID {
		t.Fatalf("dropped generation = %d, want %d", store.droppedGeneration, pending.ID)
	}
	if _, err := store.PendingCellGenerationMigration(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("pending migration after stop err = %v, want not found", err)
	}
}

func TestStopCellGenerationMigrationCancelsActiveRunBeforeDrop(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	store := &testCellGenerationMigrationStore{
		pending: &pending,
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	runCtx, run, err := state.beginCellGenerationMigrationRun(context.Background())
	if err != nil {
		t.Fatalf("begin migration run: %v", err)
	}

	if err = state.StopCellGenerationMigration(context.Background()); err != nil {
		t.Fatalf("stop migration: %v", err)
	}
	if !errors.Is(context.Cause(runCtx), errCellGenerationMigrationStopped) {
		t.Fatalf("migration run cause = %v, want stopped", context.Cause(runCtx))
	}
	select {
	case <-runCtx.Done():
	default:
		t.Fatal("migration run was not canceled before stop returned")
	}
	if !store.dropped {
		t.Fatal("pending generation was not dropped")
	}
	state.finishCellGenerationMigrationRun(run)
}

func TestStopCellGenerationMigrationWithoutPendingFails(t *testing.T) {
	store := &testCellGenerationMigrationStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	err := state.StopCellGenerationMigration(context.Background())
	if !errors.Is(err, errCellGenerationMigrationNotFound) {
		t.Fatalf("stop migration error = %v, want not running", err)
	}
}

func TestLogCellGenerationCandidateCatchUpProgress(t *testing.T) {
	var out bytes.Buffer
	state := NewStateLifecycle(
		zerolog.New(&out).Level(zerolog.InfoLevel),
		nil,
		newTestStatusTracker(nil, nil),
		StateLifecycleOptions{NextBlockCheckpointBlocks: 10},
	)
	candidate := &cellGenerationCandidate{
		generation: 2,
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 125)},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
		cells: newTestStateCellWindowCache(nil),
	}
	target := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 200)},
	}
	started := time.Unix(1000, 0)
	lastLog := started.Add(2 * time.Second)
	now := started.Add(10 * time.Second)

	state.logCellGenerationCandidateCatchUpProgress(
		candidate, target, 100, 25, 5, 10,
		started, lastLog, now, 3, 4, false,
	)

	got := out.String()
	for _, want := range []string{
		`"message":"cell generation migration catch-up progress"`,
		`"cell_generation":2`,
		`"catchup_method":"cell_generation_migration"`,
		`"processed_masterchain_blocks":25`,
		`"total_masterchain_blocks":100`,
		`"remaining":75`,
		`"pending_checkpoint_blocks":5`,
		`"applied_shard_blocks":3`,
		`"reused_shard_blocks":4`,
		`"progress":"25.0%"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("log entry missing %s: %s", want, got)
		}
	}
}

func TestCellGenerationCandidateCheckpointKeepsApplyingIntoNextWindow(t *testing.T) {
	firstHash := cell.Hash{1}
	secondHash := cell.Hash{2}
	cells := newTestStateCellWindowCache(nil)
	if err := cells.addPreparedRecords(storage.NewStateCellRecords([]storage.EncodedCellRecord{{
		Hash: firstHash,
		Data: []byte{1, 2, 3},
	}})); err != nil {
		t.Fatalf("add first checkpoint record: %v", err)
	}

	currentBlock := testBlockID(-1, topShard, 100)
	candidate := &cellGenerationCandidate{
		generation: 2,
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: currentBlock},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
		cells: cells,
	}
	store := &blockingCellGenerationCheckpointStore{
		testCellGenerationMigrationStore: &testCellGenerationMigrationStore{},
		started:                          make(chan struct{}),
		release:                          make(chan struct{}),
	}
	candidate.generationCells, _ = store.Cells(candidate.generation)
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	done := state.startCellGenerationCandidateCheckpoint(context.Background(), store, candidate)
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("checkpoint write did not start")
	}

	if err := cells.addPreparedRecords(storage.NewStateCellRecords([]storage.EncodedCellRecord{{
		Hash: secondHash,
		Data: []byte{4, 5},
	}})); err != nil {
		t.Fatalf("add next-window record: %v", err)
	}
	if got := cells.activeByteSize(); got != 2 {
		t.Fatalf("active next-window bytes = %d, want 2", got)
	}

	candidate.current.Masterchain.Block.SeqNo++
	close(store.release)
	if err := <-done; err != nil {
		t.Fatalf("finish checkpoint: %v", err)
	}

	if store.savedRecords != 1 {
		t.Fatalf("saved checkpoint records = %d, want 1", store.savedRecords)
	}
	if store.savedMigrationProgress == nil || !store.savedMigrationProgress.Masterchain.Block.Equals(&currentBlock) {
		t.Fatalf("saved checkpoint progress = %+v, want %s", store.savedMigrationProgress, storage.FormatBlockRef(currentBlock))
	}
	if got := cells.byteSize(); got != 2 {
		t.Fatalf("retained bytes after checkpoint = %d, want only next-window bytes", got)
	}
}

func TestCellGenerationSwitchRequestLifecycle(t *testing.T) {
	svc := &SyncCoordinator{
		log:              zerolog.Nop(),
		currentStateWake: make(chan struct{}),
	}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})
	target := testBlockID(-1, topShard, 200)

	wake := svc.currentStateWakeChan()
	state.requestCellGenerationSwitch(2, target)
	if !state.cellGenerationSwitchRequestActive() {
		t.Fatal("cell generation switch request is not active")
	}
	select {
	case <-wake:
	default:
		t.Fatal("cell generation switch request did not wake current state loop")
	}

	state.clearCellGenerationSwitchRequest()
	if state.cellGenerationSwitchRequestActive() {
		t.Fatal("cell generation switch request was not cleared")
	}

	state.requestCellGenerationSwitch(2, target)
	if !state.beginCellGenerationSwitch() {
		t.Fatal("cell generation switch did not begin")
	}
	state.finishCellGenerationSwitch()
	if state.cellGenerationSwitchRequestActive() {
		t.Fatal("finished cell generation switch left request active")
	}
	if state.cellGenerationSwitchActive() {
		t.Fatal("finished cell generation switch left switch active")
	}
}

func TestPendingCellGenerationMigrationLeaseIgnoresStartLimits(t *testing.T) {
	store := exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp + 1}
	maintenance := NewMaintenanceRunner(zerolog.Nop(), store, newTestStatusTracker(store, nil), MaintenanceRunnerOptions{})

	if _, err := maintenance.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskCellGenerationMigration); !errors.Is(err, errExclusiveServiceTaskHighReadAmp) {
		t.Fatalf("begin fresh migration error = %v, want high read amp", err)
	}

	lease, err := maintenance.beginPendingCellGenerationMigration(context.Background())
	if err != nil {
		t.Fatalf("begin pending migration: %v", err)
	}
	if !maintenance.cellGenerationMigrationActive() {
		t.Fatal("pending migration lease did not mark migration active")
	}
	lease.release()
	if maintenance.cellGenerationMigrationActive() {
		t.Fatal("pending migration lease did not release")
	}
}

func TestRunPendingCellGenerationMigrationChecksLeaseBeforeMetrics(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	store := &testCellGenerationMigrationStore{
		pending: &pending,
	}
	svc := &SyncCoordinator{log: zerolog.Nop(), storage: store}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})
	state.maintenance.exclusiveTask = exclusiveServiceTaskCellGenerationMigration

	ran, err := state.maintenance.runPendingCellGenerationMigration(context.Background())
	if !errors.Is(err, errCellGenerationMigrationRunning) {
		t.Fatalf("run pending migration error = %v, want migration running", err)
	}
	if ran {
		t.Fatal("pending migration ran while migration lease was already active")
	}
	if store.beginCount.Load() != 0 {
		t.Fatal("pending generation was opened while migration lease was already active")
	}
	if store.cellGenerationDBMetricsCalls != 0 {
		t.Fatal("pending generation metrics were loaded before migration lease")
	}
}

func TestRunPendingCellGenerationMigrationOpensGenerationBeforeCompactionWait(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	store := &testCellGenerationMigrationStore{
		pending: &pending,
		cellGenerationDBMetrics: storage.CellGenerationDBMetrics{
			MaxReadAmp: CellGenerationSwitchMaxReadAmp + 1,
		},
	}
	svc := &SyncCoordinator{log: zerolog.Nop(), storage: store}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})

	ran, err := state.maintenance.runPendingCellGenerationMigration(context.Background())
	if !errors.Is(err, errPendingCellGenerationCompaction) {
		t.Fatalf("run pending migration error = %v, want pending compaction", err)
	}
	if ran {
		t.Fatal("pending migration ran while waiting for compaction")
	}
	if store.beginCount.Load() != 1 {
		t.Fatalf("pending generation begin calls = %d, want 1", store.beginCount.Load())
	}
	if store.cellGenerationDBMetricsCalls != 1 {
		t.Fatalf("pending generation metrics calls = %d, want 1", store.cellGenerationDBMetricsCalls)
	}
	if state.maintenance.cellGenerationMigrationActive() {
		t.Fatal("pending migration lease was not released after compaction wait")
	}
	if state.cellGenerationMigrationRun != nil {
		t.Fatal("pending migration run was not finished after compaction wait")
	}
}

func TestPendingCellGenerationCompactionWait(t *testing.T) {
	pending := storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: testBlockID(-1, topShard, 100),
	}
	store := &testCellGenerationMigrationStore{
		cellGenerationDBMetrics: storage.CellGenerationDBMetrics{
			MaxReadAmp:               CellGenerationSwitchMaxReadAmp + 1,
			L0Files:                  12,
			L0Sublevels:              8,
			L0Size:                   512 << 20,
			CompactionDebt:           2 << 30,
			CompactionsInProgress:    1,
			CompactionInProgressSize: 256 << 20,
		},
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	metrics, waiting, err := state.cellGenerationCompactionWait(context.Background(), pending.ID)
	if err != nil {
		t.Fatalf("pending compaction wait: %v", err)
	}
	if !waiting {
		t.Fatal("pending generation did not wait for high read amp")
	}
	if metrics.MaxReadAmp != CellGenerationSwitchMaxReadAmp+1 {
		t.Fatalf("max read amp = %d, want %d", metrics.MaxReadAmp, CellGenerationSwitchMaxReadAmp+1)
	}

	store.cellGenerationDBMetrics.MaxReadAmp = CellGenerationSwitchMaxReadAmp
	_, waiting, err = state.cellGenerationCompactionWait(context.Background(), pending.ID)
	if err != nil {
		t.Fatalf("pending compaction wait at limit: %v", err)
	}
	if waiting {
		t.Fatal("pending generation waited at read amp limit")
	}
}

func TestCellGenerationMigrationThrottlesActiveCompactionsDuringCandidateCatchUp(t *testing.T) {
	store := &testCellGenerationMigrationStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})
	candidate := &cellGenerationCandidate{
		generation: 2,
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 100)},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
		cells: newTestStateCellWindowCache(nil),
	}
	target := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 101)},
	}

	release, err := state.throttleActiveCellGenerationCompactions(context.Background(), store, candidate, target)
	if err != nil {
		t.Fatalf("throttle active generation: %v", err)
	}
	if store.throttledGeneration != 1 {
		t.Fatalf("throttled generation = %d, want active generation 1", store.throttledGeneration)
	}
	if store.throttleCalls != 1 {
		t.Fatalf("throttle calls = %d, want 1", store.throttleCalls)
	}

	release()
	if store.throttleReleases != 1 {
		t.Fatalf("throttle releases = %d, want 1", store.throttleReleases)
	}
}

func TestCellGenerationMigrationDoesNotThrottleActiveCompactionsWithoutCatchUp(t *testing.T) {
	store := &testCellGenerationMigrationStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})
	block := testBlockID(-1, topShard, 100)
	candidate := &cellGenerationCandidate{
		generation: 2,
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: block},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
		cells: newTestStateCellWindowCache(nil),
	}
	target := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: block},
	}

	release, err := state.throttleActiveCellGenerationCompactions(context.Background(), store, candidate, target)
	if err != nil {
		t.Fatalf("throttle active generation without catch-up: %v", err)
	}
	release()
	if store.throttleCalls != 0 {
		t.Fatalf("throttle calls = %d, want 0", store.throttleCalls)
	}
}

func TestCellGenerationMigrationLoadsFlushedProgress(t *testing.T) {
	block := testBlockID(-1, topShard, 100)
	root := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	rootCellHash := root.HashKey()
	state := blockStateWithRoot(block, root)
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      state,
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	store := &testCellGenerationMigrationStore{
		migrationProgress: current,
		lazyRoots: map[string]*cell.Cell{
			string(rootCellHash[:]): root,
		},
	}
	lifecycle := newTestStateLifecycle(store, StateLifecycleOptions{})

	candidate, err := lifecycle.loadCellGenerationMigrationProgress(context.Background(), store, 2)
	if err != nil {
		t.Fatalf("load migration progress: %v", err)
	}
	if !candidate.current.Masterchain.Block.Equals(&block) {
		t.Fatalf("candidate current = %s, want %s", storage.FormatBlockRef(candidate.current.Masterchain.Block), storage.FormatBlockRef(block))
	}
}

func TestCellGenerationMigrationIgnoresProgressMissingCandidateRoot(t *testing.T) {
	block := testBlockID(-1, topShard, 100)
	root := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      blockStateWithRoot(block, root),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	store := &testCellGenerationMigrationStore{
		migrationProgress: current,
		lazyRoots:         map[string]*cell.Cell{},
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	_, err := state.loadCellGenerationMigrationProgress(context.Background(), store, 2)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load migration progress error = %v, want ErrNotFound", err)
	}
}

func TestCellGenerationMigrationLoadsSelfContainedProgress(t *testing.T) {
	block := testBlockID(-1, topShard, 100)
	root := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	rootCellHash := root.HashKey()
	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: block.SeqNo,
		Masterchain:      blockStateWithRoot(block, root),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	store := &testCellGenerationMigrationStore{
		migrationProgress: current,
		lazyRoots: map[string]*cell.Cell{
			string(rootCellHash[:]): root,
		},
	}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	candidate, err := state.loadCellGenerationMigrationProgress(context.Background(), store, 2)
	if err != nil {
		t.Fatalf("load migration progress: %v", err)
	}
	if candidate.current.Masterchain.Cell == nil {
		t.Fatal("masterchain progress cell was not restored")
	}
}

func TestPersistentImportStateMetadataUsesBlockMeta(t *testing.T) {
	block := testBlockID(0, topShard, 100)
	rootHash := bytes.Repeat([]byte{0x42}, 32)
	fileHash := bytes.Repeat([]byte{0x24}, 32)
	store := &testCellGenerationMigrationStore{
		blockMetas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(block): {
				ID:            block,
				StateRootHash: rootHash,
				StateFileHash: fileHash,
			},
		},
	}
	lifecycle := newTestStateLifecycle(store, StateLifecycleOptions{})

	state, err := lifecycle.persistentImportStateMetadata(context.Background(), store, block)
	if err != nil {
		t.Fatalf("load persistent import metadata: %v", err)
	}
	if !bytes.Equal(state.StateRootHash, rootHash) {
		t.Fatalf("state root hash = %x, want %x", state.StateRootHash, rootHash)
	}
	if !bytes.Equal(state.StateFileHash, fileHash) {
		t.Fatalf("state file hash = %x, want %x", state.StateFileHash, fileHash)
	}
	if state.MasterchainRef != nil {
		t.Fatalf("persistent import metadata copied masterchain ref: %s", storage.FormatBlockRef(*state.MasterchainRef))
	}
}

func TestNextBlockCatchUpYieldsForCellGenerationSwitchRequest(t *testing.T) {
	svc := &SyncCoordinator{
		log:              zerolog.Nop(),
		storage:          openTestPebbleStorage(t),
		currentStateWake: make(chan struct{}),
	}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 125)},
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}
	state.requestCellGenerationSwitch(2, testBlockID(-1, topShard, 125))
	runner := &nextSyncRunner{
		service:    svc,
		ctx:        context.Background(),
		mode:       nextSyncBootstrap,
		method:     "next_block_bootstrap",
		current:    current,
		stateCells: newTestStateCellWindowCache(nil),
	}

	if !runner.shouldReturnAfterCommit() {
		t.Fatal("next-block runner did not yield for cell generation switch request")
	}
	if runner.shouldReturnAfterCommit() {
		t.Fatal("next-block runner yielded again before throttle interval")
	}
}

func TestBootstrapRetryStopsForCellGenerationSwitchRequest(t *testing.T) {
	svc := &SyncCoordinator{
		log:              zerolog.Nop(),
		currentStateWake: make(chan struct{}),
	}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})
	runner := &nextSyncRunner{
		service: svc,
		ctx:     context.Background(),
	}

	state.requestCellGenerationSwitch(2, testBlockID(-1, topShard, 125))
	if runner.waitBootstrapRetry(svc.currentStateWakeChan()) {
		t.Fatal("bootstrap retry kept waiting after cell generation switch request")
	}
}

func TestCellGenerationSwitchNextBlockYieldThrottle(t *testing.T) {
	svc := &SyncCoordinator{
		log:              zerolog.Nop(),
		currentStateWake: make(chan struct{}),
	}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})
	now := time.Unix(1000, 0)

	if state.shouldYieldNextBlockForCellGenerationSwitch(now) {
		t.Fatal("next-block yielded without switch request")
	}

	state.requestCellGenerationSwitch(2, testBlockID(-1, topShard, 125))
	if !state.shouldYieldNextBlockForCellGenerationSwitch(now) {
		t.Fatal("first next-block yield after switch request was throttled")
	}
	if state.shouldYieldNextBlockForCellGenerationSwitch(now.Add(cellGenerationNextBlockYieldInterval - time.Second)) {
		t.Fatal("next-block yielded before throttle interval")
	}
	if !state.shouldYieldNextBlockForCellGenerationSwitch(now.Add(cellGenerationNextBlockYieldInterval)) {
		t.Fatal("next-block did not yield at throttle interval")
	}

	state.clearCellGenerationSwitchRequest()
	state.requestCellGenerationSwitch(2, testBlockID(-1, topShard, 126))
	if !state.shouldYieldNextBlockForCellGenerationSwitch(now.Add(cellGenerationNextBlockYieldInterval + time.Second)) {
		t.Fatal("new switch request did not reset next-block yield throttle")
	}
}

func TestCatchUpCurrentStateYieldsForActiveCellGenerationSwitch(t *testing.T) {
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 125)},
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}
	store := &testCellGenerationMigrationStore{current: current}
	svc := &SyncCoordinator{
		log:              zerolog.Nop(),
		storage:          store,
		currentStateWake: make(chan struct{}),
	}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})

	if !state.beginCellGenerationSwitch() {
		t.Fatal("begin cell generation switch")
	}
	defer state.finishCellGenerationSwitch()
	if err := svc.catchUpCurrentState(context.Background()); err != nil {
		t.Fatalf("catch up current state: %v", err)
	}

	locked := make(chan struct{})
	go func() {
		state.stateMu.Lock()
		close(locked)
		state.stateMu.Unlock()
	}()

	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("catch up current state did not release state lock")
	}
}

func TestCatchUpCurrentStateYieldsForCellGenerationSwitchRequest(t *testing.T) {
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 125)},
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}
	svc := &SyncCoordinator{
		log:              zerolog.Nop(),
		storage:          &testCellGenerationMigrationStore{current: current},
		currentStateWake: make(chan struct{}),
	}
	state := bindTestStateLifecycle(t, svc, StateLifecycleOptions{})

	state.requestCellGenerationSwitch(2, current.Masterchain.Block)
	if err := svc.catchUpCurrentState(context.Background()); err != nil {
		t.Fatalf("catch up current state: %v", err)
	}

	locked := make(chan struct{})
	go func() {
		state.stateMu.Lock()
		close(locked)
		state.stateMu.Unlock()
	}()

	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("catch up current state did not release state lock")
	}
}

type testCellGenerationMigrationStore struct {
	testStorage

	cancelOnBegin                context.CancelFunc
	blockStateErr                error
	blockMetaErr                 error
	ignoreContextErr             bool
	began                        bool
	beginCount                   atomic.Int32
	dropped                      bool
	droppedGeneration            uint64
	pending                      *storage.CellGenerationInfo
	current                      *storage.CurrentState
	cellGenerationDBMetrics      storage.CellGenerationDBMetrics
	cellGenerationDBMetricsCalls int
	throttledGeneration          uint64
	throttleCalls                int
	throttleReleases             int
	migrationProgress            *storage.CurrentState
	savedMigrationProgress       *storage.CurrentState
	saveMigrationProgresses      int
	blockStates                  map[storage.BlockRootHash]*storage.BlockState
	blockMetas                   map[storage.BlockRootHash]*storage.BlockMeta
	lazyRoots                    map[string]*cell.Cell
	persistentStateFileErr       error
	persistentStateFileCalls     int
	importStateCellTreeCalls     int
}

type blockingCellGenerationCheckpointStore struct {
	*testCellGenerationMigrationStore
	started      chan struct{}
	release      chan struct{}
	savedRecords int
}

type testCellGeneration struct {
	generation  uint64
	store       *testCellGenerationMigrationStore
	saveEncoded func(context.Context, []storage.EncodedCellRecord, bool) error
}

func (c testCellGeneration) ID() uint64 {
	return c.generation
}

func (c testCellGeneration) Save(context.Context, []*storage.CellRecord) error {
	return nil
}

func (c testCellGeneration) SaveEncoded(ctx context.Context, records []storage.EncodedCellRecord, sync bool) error {
	if c.saveEncoded != nil {
		return c.saveEncoded(ctx, records, sync)
	}
	return nil
}

func (c testCellGeneration) Record(context.Context, []byte) (*storage.CellRecord, error) {
	return nil, storage.ErrNotFound
}

func (c testCellGeneration) Load(_ context.Context, hash []byte) (*cell.Cell, error) {
	var key cell.Hash
	copy(key[:], hash)
	return c.Loader()(key)
}

func (c testCellGeneration) Loader() cell.LazyCellLoader {
	return func(hash cell.Hash) (*cell.Cell, error) {
		root := c.store.lazyRoots[string(hash[:])]
		if root == nil {
			return nil, storage.ErrNotFound
		}
		return root, nil
	}
}

func (c testCellGeneration) LoadLargeBOCMeta(context.Context, []cell.Hash, []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error) {
	return nil, storage.ErrNotFound
}

func (c testCellGeneration) LoadLargeBOCPayload(context.Context, []cell.Hash, []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error) {
	return nil, storage.ErrNotFound
}

func (c testCellGeneration) LoadLargeBOCCells(context.Context, []cell.Hash, []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error) {
	return nil, storage.ErrNotFound
}

func (c testCellGeneration) ImportStateCellTree(context.Context, ton.BlockIDExt, *cell.Cell, uint64) (*cell.Cell, error) {
	c.store.importStateCellTreeCalls++
	return nil, errCellGenerationMigrationTest
}

func (c testCellGeneration) ImportStateBOCView(context.Context, ton.BlockIDExt, *cell.BOCView) (*cell.Cell, error) {
	return nil, errCellGenerationMigrationTest
}

func (s *testCellGenerationMigrationStore) ActiveCells() (storage.CellGeneration, error) {
	return s.Cells(1)
}

func (s *testCellGenerationMigrationStore) Cells(generation uint64) (storage.CellGeneration, error) {
	return testCellGeneration{generation: generation, store: s}, nil
}

func (s *blockingCellGenerationCheckpointStore) Cells(generation uint64) (storage.CellGeneration, error) {
	return testCellGeneration{
		generation: generation,
		store:      s.testCellGenerationMigrationStore,
		saveEncoded: func(ctx context.Context, records []storage.EncodedCellRecord, _ bool) error {
			return s.saveEncoded(ctx, records)
		},
	}, nil
}

func (s *blockingCellGenerationCheckpointStore) saveEncoded(ctx context.Context, records []storage.EncodedCellRecord) error {
	s.savedRecords = len(records)
	close(s.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return nil
	}
}

func (s *testCellGenerationMigrationStore) ActiveCellGeneration(context.Context) (storage.CellGenerationInfo, error) {
	return storage.CellGenerationInfo{ID: 1}, nil
}

func (s *testCellGenerationMigrationStore) MaxReadAmp(context.Context) (int64, error) {
	return 0, nil
}

func (s *testCellGenerationMigrationStore) PendingCellGenerationMigration(context.Context) (storage.CellGenerationInfo, error) {
	if s.pending != nil {
		return *s.pending, nil
	}
	return storage.CellGenerationInfo{}, storage.ErrNotFound
}

func (s *testCellGenerationMigrationStore) CellGenerationDBMetrics(context.Context, uint64) (storage.CellGenerationDBMetrics, error) {
	s.cellGenerationDBMetricsCalls++
	return s.cellGenerationDBMetrics, nil
}

func (s *testCellGenerationMigrationStore) CellGenerationMigrationProgress(context.Context, uint64) (*storage.CurrentState, error) {
	if s.migrationProgress == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.migrationProgress), nil
}

func (s *testCellGenerationMigrationStore) SaveCellGenerationMigrationProgress(_ context.Context, _ uint64, current *storage.CurrentState) error {
	s.saveMigrationProgresses++
	s.savedMigrationProgress = storage.CloneCurrentState(current)
	return nil
}

func (s *testCellGenerationMigrationStore) BeginCellGeneration(_ context.Context, origin ton.BlockIDExt) (uint64, error) {
	s.beginCount.Add(1)
	if s.pending != nil {
		return s.pending.ID, nil
	}

	s.began = true
	s.pending = &storage.CellGenerationInfo{
		ID:                    2,
		OriginPersistentState: origin,
	}
	if s.cancelOnBegin != nil {
		s.cancelOnBegin()
	}
	return s.pending.ID, nil
}

func (s *testCellGenerationMigrationStore) DropPendingCellGeneration(_ context.Context, generation uint64) error {
	s.dropped = true
	s.droppedGeneration = generation
	if s.pending != nil && s.pending.ID == generation {
		s.pending = nil
	}
	return nil
}

func (s *testCellGenerationMigrationStore) CleanupCellGeneration(context.Context, uint64) error {
	return nil
}

func (s *testCellGenerationMigrationStore) ThrottleCellGenerationCompactions(_ context.Context, generation uint64) (func(), error) {
	s.throttledGeneration = generation
	s.throttleCalls++
	return func() {
		s.throttleReleases++
	}, nil
}

func (s *testCellGenerationMigrationStore) DeleteStateMetadataBeforeCellGenerationSwitch(context.Context, ton.BlockIDExt, *storage.CurrentState, []ton.BlockIDExt) (int, error) {
	return 0, nil
}

func (s *testCellGenerationMigrationStore) SwitchCellGeneration(context.Context, uint64, ton.BlockIDExt, ton.BlockIDExt, *storage.CurrentState) (uint64, error) {
	return 0, errCellGenerationMigrationTest
}

func (s *testCellGenerationMigrationStore) PersistentStateFile(_ context.Context, block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error) {
	s.persistentStateFileCalls++
	if s.persistentStateFileErr != nil {
		return nil, s.persistentStateFileErr
	}
	return &storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   effectiveShard,
		Ref:              &storage.ArtifactRef{Path: "test-state", Size: 1},
		FileHash:         bytes.Repeat([]byte{0x11}, 32),
		StateRootHash:    bytes.Repeat([]byte{0x22}, 32),
	}, nil
}

func (s *testCellGenerationMigrationStore) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if err := ctx.Err(); err != nil && !s.ignoreContextErr {
		return nil, err
	}
	if s.blockStateErr != nil {
		return nil, s.blockStateErr
	}
	if state := s.blockStates[storage.BlockKey(block)]; state != nil {
		return storage.CloneBlockState(state), nil
	}
	return nil, storage.ErrNotFound
}

func (s *testCellGenerationMigrationStore) BlockMeta(_ context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	if s.blockMetaErr != nil {
		return nil, s.blockMetaErr
	}
	if meta := s.blockMetas[storage.BlockKey(block)]; meta != nil {
		return meta.Clone(), nil
	}
	if state := s.blockStates[storage.BlockKey(block)]; state != nil {
		meta, err := storage.BuildBlockMetaFromState(*state)
		if err != nil {
			return nil, err
		}
		meta.GenUTime = uint32(time.Now().Unix())
		return meta, nil
	}
	return &storage.BlockMeta{GenUTime: uint32(time.Now().Unix())}, nil
}

func (s *testCellGenerationMigrationStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	if s.current == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.current), nil
}
