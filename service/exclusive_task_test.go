package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
)

func TestExclusiveServiceTaskBlocksMigrationDuringSerialization(t *testing.T) {
	svc := &Service{storage: exclusiveTaskTestStorage{}}

	serialization, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization)
	if err != nil {
		t.Fatalf("begin serialization task: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskCellGenerationMigration); !errors.Is(err, errStateSerializationRunning) {
		t.Fatalf("migration can start error = %v, want state serialization running", err)
	}

	_, err = svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskCellGenerationMigration)
	if !errors.Is(err, errStateSerializationRunning) {
		t.Fatalf("begin migration error = %v, want state serialization running", err)
	}
	serialization.release()

	migration, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskCellGenerationMigration)
	if err != nil {
		t.Fatalf("begin migration after serialization release: %v", err)
	}
	if !svc.exclusiveServiceTaskActive(exclusiveServiceTaskCellGenerationMigration) {
		t.Fatal("migration task is not active")
	}
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskArchiveTTLGC); !errors.Is(err, errCellGenerationMigrationRunning) {
		t.Fatalf("archive gc can start during migration error = %v, want migration running", err)
	}

	migration.release()
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start after migration release: %v", err)
	}
}

func TestExclusiveServiceTaskBlocksSerializationDuringArchiveGC(t *testing.T) {
	svc := &Service{storage: exclusiveTaskTestStorage{}}

	persistentStateGC, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskPersistentStateGC)
	if err != nil {
		t.Fatalf("begin persistent state gc task: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskArchiveTTLGC); !errors.Is(err, errPersistentStateGCActive) {
		t.Fatalf("archive gc can start during persistent state gc error = %v, want persistent state gc active", err)
	}
	persistentStateGC.release()

	archiveGC, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC)
	if err != nil {
		t.Fatalf("begin archive gc task: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskStateSerialization); !errors.Is(err, errArchiveTTLGCActive) {
		t.Fatalf("serialization can start error = %v, want archive gc active", err)
	}

	archiveGC.release()
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskStateSerialization); err != nil {
		t.Fatalf("serialization should start after archive gc release: %v", err)
	}
}

func TestBackgroundTaskStatusReportsExclusiveTask(t *testing.T) {
	svc := &Service{storage: exclusiveTaskTestStorage{}}
	if got := svc.backgroundTaskStatus(); got != "idle" {
		t.Fatalf("background task = %q, want idle", got)
	}

	lease, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization)
	if err != nil {
		t.Fatalf("begin serialization task: %v", err)
	}
	if got := svc.backgroundTaskStatus(); got != "serializing state" {
		t.Fatalf("background task = %q, want serializing state", got)
	}
	lease.release()
	if got := svc.backgroundTaskStatus(); got != "idle" {
		t.Fatalf("background task after release = %q, want idle", got)
	}
}

func TestExclusiveServiceTaskBlocksHighReadAmp(t *testing.T) {
	svc := &Service{
		storage: exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp + 1},
	}

	err := canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskStateSerialization)
	if !errors.Is(err, errExclusiveServiceTaskHighReadAmp) {
		t.Fatalf("serialization can start error = %v, want high read amplification", err)
	}
}

func TestExclusiveServiceTaskAllowsCleanupDuringHighReadAmp(t *testing.T) {
	svc := &Service{
		storage: exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp + 1},
	}

	if err := canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskPersistentStateGC); err != nil {
		t.Fatalf("persistent state gc should start during high read amplification: %v", err)
	}
	if err := canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start during high read amplification: %v", err)
	}
}

func TestExclusiveServiceTaskAllowsReadAmpAtLimit(t *testing.T) {
	svc := &Service{
		storage: exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp},
	}

	if err := canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskStateSerialization); err != nil {
		t.Fatalf("serialization should start at read amplification limit: %v", err)
	}
}

func TestExclusiveServiceTaskBlocksHighSyncLag(t *testing.T) {
	now := time.Now()
	master := testBlockID(-1, topShard, 100)
	base := testBlockID(0, topShard, 100)
	svc := &Service{
		storage: exclusiveTaskTestStorage{},
		currentStatus: &storage.CurrentState{
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
		},
	}

	err := canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskStateSerialization)
	if !errors.Is(err, errExclusiveServiceTaskHighLag) {
		t.Fatalf("serialization can start error = %v, want high sync lag", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskPersistentStateGC); err != nil {
		t.Fatalf("persistent state gc should start during high sync lag: %v", err)
	}
	if err = canStartExclusiveServiceTaskForTest(t, svc, exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start during high sync lag: %v", err)
	}
}

func canStartExclusiveServiceTaskForTest(t *testing.T, svc *Service, task exclusiveServiceTask) error {
	t.Helper()

	lease, err := svc.beginExclusiveServiceTask(context.Background(), task)
	if err != nil {
		return err
	}
	lease.release()
	return nil
}

func (s *Service) exclusiveServiceTaskActive(task exclusiveServiceTask) bool {
	s.exclusiveTaskMu.Lock()
	defer s.exclusiveTaskMu.Unlock()
	return s.exclusiveTask == task
}

type exclusiveTaskTestStorage struct {
	storage.Storage
	maxReadAmp int64
}

func (s exclusiveTaskTestStorage) MaxReadAmp(context.Context) (int64, error) {
	return s.maxReadAmp, nil
}

func (s exclusiveTaskTestStorage) CurrentState(context.Context) (*storage.CurrentState, error) {
	return nil, storage.ErrNotFound
}
