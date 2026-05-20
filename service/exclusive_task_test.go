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
	svc := &Service{}

	serialization, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization)
	if err != nil {
		t.Fatalf("begin serialization task: %v", err)
	}
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskCellGenerationMigration); !errors.Is(err, errStateSerializationRunning) {
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
	if !svc.cellGenerationMigrationActive() {
		t.Fatal("migration task is not active")
	}
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC); !errors.Is(err, errCellGenerationMigrationRunning) {
		t.Fatalf("archive gc can start during migration error = %v, want migration running", err)
	}

	migration.release()
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start after migration release: %v", err)
	}
}

func TestExclusiveServiceTaskBlocksSerializationDuringArchiveGC(t *testing.T) {
	svc := &Service{}

	persistentStateGC, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskPersistentStateGC)
	if err != nil {
		t.Fatalf("begin persistent state gc task: %v", err)
	}
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC); !errors.Is(err, errPersistentStateGCActive) {
		t.Fatalf("archive gc can start during persistent state gc error = %v, want persistent state gc active", err)
	}
	persistentStateGC.release()

	archiveGC, err := svc.beginExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC)
	if err != nil {
		t.Fatalf("begin archive gc task: %v", err)
	}
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization); !errors.Is(err, errArchiveTTLGCActive) {
		t.Fatalf("serialization can start error = %v, want archive gc active", err)
	}

	archiveGC.release()
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization); err != nil {
		t.Fatalf("serialization should start after archive gc release: %v", err)
	}
}

func TestExclusiveServiceTaskBlocksHighReadAmp(t *testing.T) {
	svc := &Service{
		storage: exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp + 1},
	}

	err := svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization)
	if !errors.Is(err, errExclusiveServiceTaskHighReadAmp) {
		t.Fatalf("serialization can start error = %v, want high read amplification", err)
	}
}

func TestExclusiveServiceTaskAllowsCleanupDuringHighReadAmp(t *testing.T) {
	svc := &Service{
		storage: exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp + 1},
	}

	if err := svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskPersistentStateGC); err != nil {
		t.Fatalf("persistent state gc should start during high read amplification: %v", err)
	}
	if err := svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start during high read amplification: %v", err)
	}
}

func TestExclusiveServiceTaskAllowsReadAmpAtLimit(t *testing.T) {
	svc := &Service{
		storage: exclusiveTaskTestStorage{maxReadAmp: exclusiveServiceTaskMaxReadAmp},
	}

	if err := svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization); err != nil {
		t.Fatalf("serialization should start at read amplification limit: %v", err)
	}
}

func TestExclusiveServiceTaskBlocksHighSyncLag(t *testing.T) {
	now := time.Now()
	master := testBlockID(-1, topShard, 100)
	base := testBlockID(0, topShard, 100)
	svc := &Service{
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

	err := svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskStateSerialization)
	if !errors.Is(err, errExclusiveServiceTaskHighLag) {
		t.Fatalf("serialization can start error = %v, want high sync lag", err)
	}
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskPersistentStateGC); err != nil {
		t.Fatalf("persistent state gc should start during high sync lag: %v", err)
	}
	if err = svc.canStartExclusiveServiceTask(context.Background(), exclusiveServiceTaskArchiveTTLGC); err != nil {
		t.Fatalf("archive gc should start during high sync lag: %v", err)
	}
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
