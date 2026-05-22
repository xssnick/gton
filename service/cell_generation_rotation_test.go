package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errCellGenerationMigrationTest = errors.New("test cell generation migration failure")

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

type testCellGenerationMigrationStore struct {
	storage.Storage

	cancelOnBegin    context.CancelFunc
	blockStateErr    error
	ignoreContextErr bool
	began            bool
	beginCount       int
	aborted          bool
	current          *storage.CurrentState
}

func (s *testCellGenerationMigrationStore) ActiveCellGeneration(context.Context) (storage.CellGenerationInfo, error) {
	return storage.CellGenerationInfo{ID: 1}, nil
}

func (s *testCellGenerationMigrationStore) PendingCellGenerationMigration(context.Context) (storage.CellGenerationInfo, error) {
	return storage.CellGenerationInfo{}, storage.ErrNotFound
}

func (s *testCellGenerationMigrationStore) BeginCellGeneration(context.Context, ton.BlockIDExt) (uint64, error) {
	s.began = true
	s.beginCount++
	if s.cancelOnBegin != nil {
		s.cancelOnBegin()
	}
	return 2, nil
}

func (s *testCellGenerationMigrationStore) AbortCellGeneration(context.Context, uint64) error {
	s.aborted = true
	return nil
}

func (s *testCellGenerationMigrationStore) CleanupCellGeneration(context.Context, uint64) error {
	return nil
}

func (s *testCellGenerationMigrationStore) ImportStateCellTreeInGeneration(context.Context, uint64, ton.BlockIDExt, *cell.Cell, uint64) (*cell.Cell, error) {
	return nil, errCellGenerationMigrationTest
}

func (s *testCellGenerationMigrationStore) ImportStateBOCViewInGeneration(context.Context, uint64, ton.BlockIDExt, *cell.BOCView) (*cell.Cell, error) {
	return nil, errCellGenerationMigrationTest
}

func (s *testCellGenerationMigrationStore) LazyCellLoaderInGeneration(uint64) cell.LazyCellLoader {
	return nil
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
