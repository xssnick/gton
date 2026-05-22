package service

import (
	"bytes"
	"context"
	"errors"
	"strings"
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
	rootCellHash := root.HashKey()
	return storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Clone(rootHash[:]),
		StateCellHash: bytes.Clone(rootCellHash[:]),
		Cell:          root,
	}
}

func TestCellGenerationMigrationCanceledLeavesPendingIntent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &testCellGenerationMigrationStore{
		cancelOnBegin: cancel,
	}
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}

	err := svc.runCellGenerationMigration(ctx, store, testBlockID(-1, topShard, 100))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("migration error = %v, want context.Canceled", err)
	}
	if !store.began {
		t.Fatal("migration did not begin candidate generation")
	}
	if store.aborted {
		t.Fatal("canceled migration aborted pending candidate generation")
	}
}

func TestCellGenerationMigrationFailureAbortsPendingIntent(t *testing.T) {
	store := &testCellGenerationMigrationStore{
		blockStateErr: errCellGenerationMigrationTest,
	}
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}

	err := svc.runCellGenerationMigration(context.Background(), store, testBlockID(-1, topShard, 100))
	if !errors.Is(err, errCellGenerationMigrationTest) {
		t.Fatalf("migration error = %v, want test failure", err)
	}
	if !store.began {
		t.Fatal("migration did not begin candidate generation")
	}
	if !store.aborted {
		t.Fatal("failed migration did not abort pending candidate generation")
	}
}

func TestCellGenerationMigrationCanceledWithNonContextErrorLeavesPendingIntent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &testCellGenerationMigrationStore{
		cancelOnBegin:    cancel,
		blockStateErr:    errCellGenerationMigrationTest,
		ignoreContextErr: true,
	}
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}

	err := svc.runCellGenerationMigration(ctx, store, testBlockID(-1, topShard, 100))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("migration error = %v, want context.Canceled", err)
	}
	if !errors.Is(err, errCellGenerationMigrationTest) {
		t.Fatalf("migration error = %v, want test failure", err)
	}
	if store.aborted {
		t.Fatal("canceled migration with non-context error aborted pending candidate generation")
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
	svc := &Service{
		log:             zerolog.Nop(),
		storage:         store,
		shutdownContext: context.Background(),
	}

	err := svc.StartCellGenerationMigration(context.Background(), origin.SeqNo)
	if err != nil {
		t.Fatalf("start migration: %v", err)
	}
	if store.beginCount == 0 {
		t.Fatal("migration intent was not persisted before start returned")
	}

	svc.Wait()
	if !store.aborted {
		t.Fatal("failed async migration did not abort pending generation")
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
	svc := &Service{
		log:           zerolog.Nop(),
		storage:       store,
		exclusiveTask: exclusiveServiceTaskStateSerialization,
	}

	err := svc.StartCellGenerationMigration(context.Background(), origin.SeqNo)
	if !errors.Is(err, errStateSerializationRunning) {
		t.Fatalf("start migration error = %v, want serialization running", err)
	}
	if store.beginCount != 0 {
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
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}

	if err := svc.StopCellGenerationMigration(context.Background()); err != nil {
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
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}

	runCtx, run, err := svc.beginCellGenerationMigrationRun(context.Background())
	if err != nil {
		t.Fatalf("begin migration run: %v", err)
	}

	if err = svc.StopCellGenerationMigration(context.Background()); err != nil {
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
	svc.finishCellGenerationMigrationRun(run)
}

func TestStopCellGenerationMigrationWithoutPendingFails(t *testing.T) {
	store := &testCellGenerationMigrationStore{}
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}

	err := svc.StopCellGenerationMigration(context.Background())
	if !errors.Is(err, errCellGenerationMigrationNotFound) {
		t.Fatalf("stop migration error = %v, want not running", err)
	}
}

func TestLogCellGenerationCandidateCatchUpProgress(t *testing.T) {
	var out bytes.Buffer
	svc := &Service{
		log:                       zerolog.New(&out).Level(zerolog.InfoLevel),
		nextBlockCheckpointBlocks: 10,
	}
	candidate := &cellGenerationCandidate{
		generation: 2,
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 125)},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
		cells: newStateCellWindowCache(nil),
	}
	target := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 200)},
	}
	started := time.Unix(1000, 0)
	lastLog := started.Add(2 * time.Second)
	now := started.Add(10 * time.Second)

	svc.logCellGenerationCandidateCatchUpProgress(
		candidate, target, 100, 25, 10,
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

func TestCellGenerationSwitchRequestLifecycle(t *testing.T) {
	svc := &Service{
		log:              zerolog.Nop(),
		currentStateWake: make(chan struct{}, 1),
	}
	target := testBlockID(-1, topShard, 200)

	svc.requestCellGenerationSwitch(2, target)
	if !svc.cellGenerationSwitchRequestActive() {
		t.Fatal("cell generation switch request is not active")
	}
	select {
	case <-svc.currentStateWake:
	default:
		t.Fatal("cell generation switch request did not wake current state loop")
	}

	svc.clearCellGenerationSwitchRequest()
	if svc.cellGenerationSwitchRequestActive() {
		t.Fatal("cell generation switch request was not cleared")
	}

	svc.requestCellGenerationSwitch(2, target)
	if !svc.beginCellGenerationSwitch() {
		t.Fatal("cell generation switch did not begin")
	}
	svc.finishCellGenerationSwitch()
	if svc.cellGenerationSwitchRequestActive() {
		t.Fatal("finished cell generation switch left request active")
	}
	if svc.cellGenerationSwitchActive() {
		t.Fatal("finished cell generation switch left switch active")
	}
}

func TestPendingCellGenerationMigrationLeaseIgnoresStartLimits(t *testing.T) {
	svc := &Service{
		storage: exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp + 1},
	}

	if _, err := svc.beginCellGenerationMigration(context.Background()); !errors.Is(err, errExclusiveServiceTaskHighReadAmp) {
		t.Fatalf("begin fresh migration error = %v, want high read amp", err)
	}

	lease, err := svc.beginPendingCellGenerationMigration(context.Background())
	if err != nil {
		t.Fatalf("begin pending migration: %v", err)
	}
	if !svc.cellGenerationMigrationActive() {
		t.Fatal("pending migration lease did not mark migration active")
	}
	lease.release()
	if svc.cellGenerationMigrationActive() {
		t.Fatal("pending migration lease did not release")
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
	svc := &Service{storage: store}

	metrics, waiting, err := svc.pendingCellGenerationCompactionWait(context.Background(), store, pending)
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
	_, waiting, err = svc.pendingCellGenerationCompactionWait(context.Background(), store, pending)
	if err != nil {
		t.Fatalf("pending compaction wait at limit: %v", err)
	}
	if waiting {
		t.Fatal("pending generation waited at read amp limit")
	}
}

func TestCellGenerationMigrationThrottlesActiveCompactionsDuringCandidateCatchUp(t *testing.T) {
	store := &testCellGenerationMigrationStore{}
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}
	candidate := &cellGenerationCandidate{
		generation: 2,
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 100)},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
		cells: newStateCellWindowCache(nil),
	}
	target := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 101)},
	}

	release, err := svc.throttleActiveCellGenerationCompactions(context.Background(), store, candidate, target)
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
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}
	block := testBlockID(-1, topShard, 100)
	candidate := &cellGenerationCandidate{
		generation: 2,
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: block},
			Shards:      map[storage.ShardKey]storage.BlockState{},
		},
		cells: newStateCellWindowCache(nil),
	}
	target := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: block},
	}

	release, err := svc.throttleActiveCellGenerationCompactions(context.Background(), store, candidate, target)
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
	svc := &Service{}

	candidate, err := svc.loadCellGenerationMigrationProgress(context.Background(), store, 2)
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
	svc := &Service{}

	_, err := svc.loadCellGenerationMigrationProgress(context.Background(), store, 2)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("load migration progress error = %v, want ErrNotFound", err)
	}
}

func TestImportSerializedPersistentBlockStateReusesMarkedImport(t *testing.T) {
	block := testBlockID(-1, topShard, 100)
	root := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	rootHash := root.HashKey(0)
	rootCellHash := root.HashKey()
	store := &testCellGenerationMigrationStore{
		importedStates: map[string]struct{}{
			storage.BlockKey(block): {},
		},
		blockStates: map[string]*storage.BlockState{
			storage.BlockKey(block): {
				Block:         block,
				StateRootHash: rootHash[:],
				StateCellHash: rootCellHash[:],
				Cell:          root,
			},
		},
		lazyRoots: map[string]*cell.Cell{
			string(rootCellHash[:]): root,
		},
	}
	svc := &Service{
		log:     zerolog.Nop(),
		storage: store,
	}

	state, err := svc.importSerializedPersistentBlockState(
		context.Background(), store, generationStateCellImporter{store: store, generation: 2},
		2, block, block, 0, rootHash[:],
	)
	if err != nil {
		t.Fatalf("import persistent block state: %v", err)
	}
	if state.CellGeneration != 2 {
		t.Fatalf("cell generation = %d, want 2", state.CellGeneration)
	}
	if state.Cell != root {
		t.Fatal("marked import did not load root from candidate generation")
	}
	if store.importStateCellTreeCalls != 0 {
		t.Fatalf("state file was imported again %d times", store.importStateCellTreeCalls)
	}
	if store.markedImports != 0 {
		t.Fatalf("marked import was marked again %d times", store.markedImports)
	}
}

func TestNextBlockCatchUpYieldsForCellGenerationSwitchRequest(t *testing.T) {
	svc := &Service{
		log:              zerolog.Nop(),
		currentStateWake: make(chan struct{}, 1),
	}
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 125)},
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}
	svc.requestCellGenerationSwitch(2, testBlockID(-1, topShard, 125))
	runner := &nextSyncRunner{
		service:    svc,
		ctx:        context.Background(),
		mode:       nextSyncBootstrap,
		method:     "next_block_bootstrap",
		current:    current,
		stateCells: newStateCellWindowCache(nil),
	}

	if !runner.shouldReturnAfterCommit() {
		t.Fatal("next-block runner did not yield for cell generation switch request")
	}
	if runner.shouldReturnAfterCommit() {
		t.Fatal("next-block runner yielded again before throttle interval")
	}
}

func TestCellGenerationSwitchNextBlockYieldThrottle(t *testing.T) {
	svc := &Service{
		log:              zerolog.Nop(),
		currentStateWake: make(chan struct{}, 1),
	}
	now := time.Unix(1000, 0)

	if svc.shouldYieldNextBlockForCellGenerationSwitch(now) {
		t.Fatal("next-block yielded without switch request")
	}

	svc.requestCellGenerationSwitch(2, testBlockID(-1, topShard, 125))
	if !svc.shouldYieldNextBlockForCellGenerationSwitch(now) {
		t.Fatal("first next-block yield after switch request was throttled")
	}
	if svc.shouldYieldNextBlockForCellGenerationSwitch(now.Add(cellGenerationNextBlockYieldInterval - time.Second)) {
		t.Fatal("next-block yielded before throttle interval")
	}
	if !svc.shouldYieldNextBlockForCellGenerationSwitch(now.Add(cellGenerationNextBlockYieldInterval)) {
		t.Fatal("next-block did not yield at throttle interval")
	}

	svc.clearCellGenerationSwitchRequest()
	svc.requestCellGenerationSwitch(2, testBlockID(-1, topShard, 126))
	if !svc.shouldYieldNextBlockForCellGenerationSwitch(now.Add(cellGenerationNextBlockYieldInterval + time.Second)) {
		t.Fatal("new switch request did not reset next-block yield throttle")
	}
}

func TestCatchUpCurrentStateYieldsForCellGenerationSwitchRequest(t *testing.T) {
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: testBlockID(-1, topShard, 125)},
		Shards:      map[storage.ShardKey]storage.BlockState{},
	}
	store := &testCellGenerationMigrationStore{current: current}
	svc := &Service{
		log:              zerolog.Nop(),
		storage:          store,
		currentStateWake: make(chan struct{}, 1),
	}

	svc.requestCellGenerationSwitch(2, current.Masterchain.Block)
	if err := svc.catchUpCurrentState(context.Background()); err != nil {
		t.Fatalf("catch up current state: %v", err)
	}

	locked := make(chan struct{})
	go func() {
		svc.stateMu.Lock()
		svc.stateMu.Unlock()
		close(locked)
	}()

	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("catch up current state did not release state lock")
	}
}

type testCellGenerationMigrationStore struct {
	storage.Storage

	cancelOnBegin            context.CancelFunc
	blockStateErr            error
	ignoreContextErr         bool
	began                    bool
	beginCount               int
	aborted                  bool
	abortedGeneration        uint64
	dropped                  bool
	droppedGeneration        uint64
	pending                  *storage.CellGenerationInfo
	current                  *storage.CurrentState
	cellGenerationDBMetrics  storage.CellGenerationDBMetrics
	throttledGeneration      uint64
	throttleCalls            int
	throttleReleases         int
	migrationProgress        *storage.CurrentState
	savedMigrationProgress   *storage.CurrentState
	saveMigrationProgresses  int
	importedStates           map[string]struct{}
	blockStates              map[string]*storage.BlockState
	lazyRoots                map[string]*cell.Cell
	importStateCellTreeCalls int
	markedImports            int
}

func (s *testCellGenerationMigrationStore) ActiveCellGeneration(context.Context) (storage.CellGenerationInfo, error) {
	return storage.CellGenerationInfo{ID: 1}, nil
}

func (s *testCellGenerationMigrationStore) PendingCellGenerationMigration(context.Context) (storage.CellGenerationInfo, error) {
	if s.pending != nil {
		return *s.pending, nil
	}
	return storage.CellGenerationInfo{}, storage.ErrNotFound
}

func (s *testCellGenerationMigrationStore) CellGenerationDBMetrics(context.Context, uint64) (storage.CellGenerationDBMetrics, error) {
	return s.cellGenerationDBMetrics, nil
}

func (s *testCellGenerationMigrationStore) CellGenerationStateImported(_ context.Context, _ uint64, block ton.BlockIDExt) error {
	if _, ok := s.importedStates[storage.BlockKey(block)]; ok {
		return nil
	}
	return storage.ErrNotFound
}

func (s *testCellGenerationMigrationStore) MarkCellGenerationStateImported(context.Context, uint64, ton.BlockIDExt) error {
	s.markedImports++
	return nil
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

func (s *testCellGenerationMigrationStore) BeginCellGeneration(context.Context, ton.BlockIDExt) (uint64, error) {
	s.began = true
	s.beginCount++
	if s.cancelOnBegin != nil {
		s.cancelOnBegin()
	}
	return 2, nil
}

func (s *testCellGenerationMigrationStore) AbortCellGeneration(_ context.Context, generation uint64) error {
	s.aborted = true
	s.abortedGeneration = generation
	if s.pending != nil && s.pending.ID == generation {
		s.pending = nil
	}
	return nil
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

func (s *testCellGenerationMigrationStore) ImportStateCellTreeInGeneration(context.Context, uint64, ton.BlockIDExt, *cell.Cell, uint64) (*cell.Cell, error) {
	s.importStateCellTreeCalls++
	return nil, errCellGenerationMigrationTest
}

func (s *testCellGenerationMigrationStore) ImportStateBOCViewInGeneration(context.Context, uint64, ton.BlockIDExt, *cell.BOCView) (*cell.Cell, error) {
	return nil, errCellGenerationMigrationTest
}

func (s *testCellGenerationMigrationStore) LazyCellLoaderInGeneration(uint64) cell.LazyCellLoader {
	return func(hash cell.Hash) (*cell.Cell, error) {
		root := s.lazyRoots[string(hash[:])]
		if root == nil {
			return nil, storage.ErrNotFound
		}
		return root, nil
	}
}

func (s *testCellGenerationMigrationStore) SaveEncodedCellsInGeneration(context.Context, uint64, []storage.EncodedCellRecord, bool) error {
	return nil
}

func (s *testCellGenerationMigrationStore) SwitchCellGeneration(context.Context, uint64, ton.BlockIDExt, ton.BlockIDExt, *storage.CurrentState) (uint64, error) {
	return 0, errCellGenerationMigrationTest
}

func (s *testCellGenerationMigrationStore) PersistentStateFile(context.Context, ton.BlockIDExt, ton.BlockIDExt, int64) (*storage.PersistentStateFile, error) {
	return nil, errCellGenerationMigrationTest
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

func (s *testCellGenerationMigrationStore) BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error) {
	return &storage.BlockMeta{GenUTime: uint32(time.Now().Unix())}, nil
}

func (s *testCellGenerationMigrationStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	if s.current == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.current), nil
}
