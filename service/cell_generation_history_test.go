package service

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCellGenerationHistoryRetentionBoundary(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	lifecycle := &StateLifecycle{retainMasterchainHistory: true}
	for _, minimum := range []uint32{0, 99, 100, 101} {
		current := &storage.CurrentState{Masterchain: storage.BlockState{
			Block:  testBlockID(-1, topShard, 200),
			Parsed: &tlb.ShardStateUnsplit{MinRefMCSeqno: minimum},
		}}
		err := lifecycle.checkCellGenerationHistory(current, origin)
		if minimum < origin.SeqNo {
			if !errors.Is(err, errCellGenerationHistoryReferenced) {
				t.Fatalf("floor %d: got %v, want referenced-history refusal", minimum, err)
			}
		} else if err != nil {
			t.Fatalf("floor %d: %v", minimum, err)
		}
	}
}

func TestCellGenerationHistoryRetentionIsExplicit(t *testing.T) {
	lifecycle := &StateLifecycle{}
	current := &storage.CurrentState{Masterchain: storage.BlockState{
		Block:  testBlockID(-1, topShard, 200),
		Parsed: &tlb.ShardStateUnsplit{MinRefMCSeqno: 0},
	}}
	if err := lifecycle.checkCellGenerationHistory(current, testBlockID(-1, topShard, 100)); err != nil {
		t.Fatalf("sync-only node acquired a validator retention requirement: %v", err)
	}
}

func TestCellGenerationHistoryPreventsCleanupAndSwitch(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	candidate := &cellGenerationCandidate{current: &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:  testBlockID(-1, topShard, 200),
			Parsed: &tlb.ShardStateUnsplit{MinRefMCSeqno: 99},
		},
	}}
	store := &historyRetentionStore{testCellGenerationMigrationStore: &testCellGenerationMigrationStore{}}
	lifecycle := &StateLifecycle{log: zerolog.Nop(), retainMasterchainHistory: true}
	if err := lifecycle.prepareCellGenerationSwitchCandidate(context.Background(), store, candidate, origin); !errors.Is(err, errCellGenerationHistoryReferenced) {
		t.Fatalf("prepare switch = %v", err)
	}
	if _, err := lifecycle.trySwitchCellGenerationCandidate(context.Background(), store, candidate, origin); !errors.Is(err, errCellGenerationHistoryReferenced) {
		t.Fatalf("switch = %v", err)
	}
	if store.deleted || store.switched {
		t.Fatal("referenced masterchain history was removed")
	}
}

type historyRetentionStore struct {
	*testCellGenerationMigrationStore
	deleted  bool
	switched bool
}

func (s *historyRetentionStore) DeleteStateMetadataBeforeCellGenerationSwitch(context.Context, ton.BlockIDExt, *storage.CurrentState, []ton.BlockIDExt) (int, error) {
	s.deleted = true
	return 0, nil
}

func (s *historyRetentionStore) SwitchCellGeneration(context.Context, uint64, ton.BlockIDExt, ton.BlockIDExt, *storage.CurrentState) (uint64, error) {
	s.switched = true
	return 1, nil
}

type historyRetentionWaitWriter struct {
	waiting chan struct{}
}

func (w historyRetentionWaitWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("waiting for referenced masterchain history")) {
		select {
		case w.waiting <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}

func TestCellGenerationHistoryWaitCancelsWithoutStoppingBlockProgress(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	root := cell.BeginCell().EndCell()
	master := blockStateWithRoot(testBlockID(-1, topShard, 200), root)
	master.Parsed = &tlb.ShardStateUnsplit{MinRefMCSeqno: 99}
	current := &storage.CurrentState{Masterchain: master}
	store := &historyRetentionStore{testCellGenerationMigrationStore: &testCellGenerationMigrationStore{
		current: current, migrationProgress: current,
		lazyRoots: map[string]*cell.Cell{string(root.Hash()): root},
	}}
	lifecycle := newTestStateLifecycle(store, StateLifecycleOptions{RetainMasterchainHistory: true})
	waiting := make(chan struct{}, 1)
	lifecycle.log = zerolog.New(historyRetentionWaitWriter{waiting: waiting})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- lifecycle.runCellGenerationMigration(ctx, origin)
	}()
	select {
	case <-waiting:
	case err := <-done:
		t.Fatalf("migration ended before history wait: %v", err)
	case <-time.After(time.Second):
		t.Fatal("migration did not reach history wait")
	}
	if lifecycle.cellGenerationSwitchRequestActive() || lifecycle.cellGenerationSwitchActive() {
		t.Fatal("history wait blocks normal chain progress")
	}
	if !lifecycle.stateMu.TryLock() {
		t.Fatal("history wait holds the state lock")
	}
	lifecycle.stateMu.Unlock()
	if !lifecycle.currentStatePersistMu.TryLock() {
		t.Fatal("history wait holds the checkpoint lock")
	}
	lifecycle.currentStatePersistMu.Unlock()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled history wait = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("history wait did not stop after cancellation")
	}
	if store.deleted || store.switched {
		t.Fatal("waiting migration removed referenced history")
	}
	if store.pending == nil || store.dropped {
		t.Fatal("cancellation discarded resumable migration progress")
	}
}
