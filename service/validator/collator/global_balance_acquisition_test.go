package collator

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

type delayedGlobalBalanceStore struct {
	*localAcquisitionTestStore
	ready   chan struct{}
	waiting chan struct{}
}

func (s *delayedGlobalBalanceStore) BlockState(ctx context.Context, id ton.BlockIDExt) (*storage.BlockState, error) {
	select {
	case <-s.ready:
		return s.localAcquisitionTestStore.BlockState(ctx, id)
	default:
		return nil, storage.ErrNotFound
	}
}

func (s *delayedGlobalBalanceStore) WaitBlockArtifacts(ctx context.Context, id ton.BlockIDExt) error {
	select {
	case s.waiting <- struct{}{}:
	default:
	}
	select {
	case <-s.ready:
		_, err := s.localAcquisitionTestStore.BlockState(ctx, id)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type globalBalanceResult struct {
	grams *big.Int
	err   error
}

func TestGlobalBalanceWaitsForExactShardState(t *testing.T) {
	master, source := globalBalanceAcquisitionFixture(t)
	store := &delayedGlobalBalanceStore{
		localAcquisitionTestStore: source,
		ready:                     make(chan struct{}), waiting: make(chan struct{}, 1),
	}
	wait := acquisitionInputWait{}
	calculator := localGlobalBalanceCalculator{
		acquisition: &LocalAcquisition{store: store},
		baseline:    master,
		mode:        acquisitionReadMode{waits: &wait},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan globalBalanceResult, 1)
	go func() {
		grams, err := calculator.CalculateGlobalBalance(ctx, master, 17)
		result <- globalBalanceResult{grams: grams, err: err}
	}()
	select {
	case <-store.waiting:
	case got := <-result:
		t.Fatalf("calculation returned before shard publication: %v", got.err)
	case <-time.After(time.Second):
		t.Fatal("calculation did not wait for shard publication")
	}
	close(store.ready)
	select {
	case got := <-result:
		if got.err != nil || got.grams == nil {
			t.Fatalf("calculation after publication = %v, %v", got.grams, got.err)
		}
		if wait.duration <= 0 {
			t.Fatal("input wait duration was not recorded")
		}
	case <-time.After(time.Second):
		t.Fatal("calculation did not resume after shard publication")
	}
}

func TestGlobalBalanceCancelsMissingShardWait(t *testing.T) {
	master, source := globalBalanceAcquisitionFixture(t)
	store := &delayedGlobalBalanceStore{
		localAcquisitionTestStore: source,
		ready:                     make(chan struct{}), waiting: make(chan struct{}, 1),
	}
	wait := acquisitionInputWait{}
	calculator := localGlobalBalanceCalculator{
		acquisition: &LocalAcquisition{store: store},
		baseline:    master,
		mode:        acquisitionReadMode{waits: &wait},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := calculator.CalculateGlobalBalance(ctx, master, 17)
		result <- err
	}()
	select {
	case <-store.waiting:
	case err := <-result:
		t.Fatalf("calculation returned before cancellation: %v", err)
	case <-time.After(time.Second):
		t.Fatal("calculation did not enter the missing-shard wait")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled calculation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled calculation did not stop")
	}
}

func TestGlobalBalanceImmediateReadDoesNotWait(t *testing.T) {
	master, source := globalBalanceAcquisitionFixture(t)
	store := &delayedGlobalBalanceStore{
		localAcquisitionTestStore: source,
		ready:                     make(chan struct{}), waiting: make(chan struct{}, 1),
	}
	calculator := localGlobalBalanceCalculator{acquisition: &LocalAcquisition{store: store}, baseline: master}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := calculator.CalculateGlobalBalance(ctx, master, 17); !errors.Is(err, ErrAcquisitionNotReady) {
		t.Fatalf("immediate missing-shard calculation = %v", err)
	}
	select {
	case <-store.waiting:
		t.Fatal("immediate calculation waited for shard state")
	default:
	}
}
