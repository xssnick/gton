package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
)

func TestExclusiveServiceTaskBlocksMigrationDuringSerialization(t *testing.T) {
	state := newExclusiveTaskTestState(0, nil)

	serialization, err := state.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization)
	if err != nil {
		t.Fatalf("begin serialization task: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskCellGenerationMigration); !errors.Is(err, errStateSerializationRunning) {
		t.Fatalf("migration can start error = %v, want state serialization running", err)
	}

	_, err = state.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskCellGenerationMigration)
	if !errors.Is(err, errStateSerializationRunning) {
		t.Fatalf("begin migration error = %v, want state serialization running", err)
	}
	serialization.release()

	migration, err := state.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskCellGenerationMigration)
	if err != nil {
		t.Fatalf("begin migration after serialization release: %v", err)
	}
	if !state.cellGenerationMigrationActive() {
		t.Fatal("migration task is not active")
	}
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskArchiveTTLGC); !errors.Is(err, errCellGenerationMigrationRunning) {
		t.Fatalf("archive gc can start during migration error = %v, want migration running", err)
	}

	migration.release()
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start after migration release: %v", err)
	}
}

func TestExclusiveServiceTaskBlocksSerializationDuringArchiveGC(t *testing.T) {
	state := newExclusiveTaskTestState(0, nil)

	persistentStateGC, err := state.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskPersistentStateGC)
	if err != nil {
		t.Fatalf("begin persistent state gc task: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskArchiveTTLGC); !errors.Is(err, errPersistentStateGCActive) {
		t.Fatalf("archive gc can start during persistent state gc error = %v, want persistent state gc active", err)
	}
	persistentStateGC.release()

	archiveGC, err := state.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC)
	if err != nil {
		t.Fatalf("begin archive gc task: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskStateSerialization); !errors.Is(err, errArchiveTTLGCActive) {
		t.Fatalf("serialization can start error = %v, want archive gc active", err)
	}

	archiveGC.release()
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskStateSerialization); err != nil {
		t.Fatalf("serialization should start after archive gc release: %v", err)
	}
}

func TestBackgroundTaskStatusReportsExclusiveTask(t *testing.T) {
	state := newExclusiveTaskTestState(0, nil)
	if got := state.BackgroundTaskStatus(); got != "idle" {
		t.Fatalf("background task = %q, want idle", got)
	}

	lease, err := state.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization)
	if err != nil {
		t.Fatalf("begin serialization task: %v", err)
	}
	if got := state.BackgroundTaskStatus(); got != "serializing state" {
		t.Fatalf("background task = %q, want serializing state", got)
	}
	lease.release()
	if got := state.BackgroundTaskStatus(); got != "idle" {
		t.Fatalf("background task after release = %q, want idle", got)
	}
}

func TestExclusiveServiceTaskBlocksHighReadAmp(t *testing.T) {
	state := newExclusiveTaskTestState(exclusiveServiceTaskMaxReadAmp+1, nil)

	err := canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskStateSerialization)
	if !errors.Is(err, errExclusiveServiceTaskHighReadAmp) {
		t.Fatalf("serialization can start error = %v, want high read amplification", err)
	}
}

func TestExclusiveServiceTaskAllowsCleanupDuringHighReadAmp(t *testing.T) {
	state := newExclusiveTaskTestState(exclusiveServiceTaskMaxReadAmp+1, nil)

	if err := canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskPersistentStateGC); err != nil {
		t.Fatalf("persistent state gc should start during high read amplification: %v", err)
	}
	if err := canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start during high read amplification: %v", err)
	}
}

func TestExclusiveServiceTaskAllowsReadAmpAtLimit(t *testing.T) {
	state := newExclusiveTaskTestState(exclusiveServiceTaskMaxReadAmp, nil)

	if err := canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskStateSerialization); err != nil {
		t.Fatalf("serialization should start at read amplification limit: %v", err)
	}
}

func TestExclusiveServiceTaskBlocksHighSyncLag(t *testing.T) {
	now := time.Now()
	master := testBlockID(-1, topShard, 100)
	base := testBlockID(0, topShard, 100)
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:  master,
			Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(now.Add(-time.Minute).Unix())},
		},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(base): {
				Block:  base,
				Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(now.Add(-exclusiveServiceTaskMaxLag - time.Second).Unix())},
			},
		},
	}
	state := newExclusiveTaskTestState(0, current)

	err := canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskStateSerialization)
	if !errors.Is(err, errExclusiveServiceTaskHighLag) {
		t.Fatalf("serialization can start error = %v, want high sync lag", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskPersistentStateGC); err != nil {
		t.Fatalf("persistent state gc should start during high sync lag: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, state, exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start during high sync lag: %v", err)
	}
}

func canStartExclusiveServiceTaskForTest(t *testing.T, state *MaintenanceRunner, task exclusiveServiceTask) error {
	t.Helper()

	lease, err := state.beginExclusiveServiceTask(context.Background(), task)
	if err != nil {
		return err
	}
	lease.release()
	return nil
}

func newExclusiveTaskTestState(maxReadAmp int64, current *storage.CurrentState) *MaintenanceRunner {
	store := exclusiveTaskTestStorage{maxReadAmp: maxReadAmp}

	return NewMaintenanceRunner(zerolog.Nop(), store, newTestStatusTrackerWithCurrent(store, current), MaintenanceRunnerOptions{})
}

func (s *MaintenanceRunner) cellGenerationMigrationActive() bool {
	s.exclusiveTaskMu.Lock()
	defer s.exclusiveTaskMu.Unlock()
	return s.exclusiveTask == exclusiveServiceTaskCellGenerationMigration
}

type exclusiveTaskTestStorage struct {
	testStorage
	maxReadAmp int64
}

func (s exclusiveTaskTestStorage) MaxReadAmp(context.Context) (int64, error) {
	return s.maxReadAmp, nil
}

func (s exclusiveTaskTestStorage) CurrentState(context.Context) (*storage.CurrentState, error) {
	return nil, storage.ErrNotFound
}
