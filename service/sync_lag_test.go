package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
)

func TestSyncLagUsesMasterchainFrontiers(t *testing.T) {
	type lagCase struct {
		name           string
		currentSeconds int64
		appliedSeconds int64
		want           time.Duration
	}
	tests := []lagCase{
		{name: "unchanged old shard", currentSeconds: 1, appliedSeconds: 1, want: 1700 * time.Millisecond},
		{name: "shard client behind", currentSeconds: 7, appliedSeconds: 1, want: 7700 * time.Millisecond},
		{name: "applied master behind", currentSeconds: 1, appliedSeconds: 4, want: 4700 * time.Millisecond},
		{name: "future timestamps", currentSeconds: -1, appliedSeconds: -2, want: -300 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				time.Sleep(700 * time.Millisecond)
				now := time.Now().Unix()
				current := syncLagMasterState(20, uint32(now-test.currentSeconds))
				applied := syncLagMasterState(21, uint32(now-test.appliedSeconds))
				tracker := newTestStatusTrackerWithCurrent(nil, &storage.CurrentState{
					ShardClientSeqno: current.Block.SeqNo,
					Masterchain:      current,
					Shards: map[storage.ShardKey]storage.BlockState{
						{Workchain: 0, Shard: topShard}: {
							Block:  testBlockID(0, topShard, 10),
							Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(now - 60)},
						},
					},
				})
				tracker.rememberAppliedMasterchain(&applied)

				lag, err := tracker.SyncLag()
				if err != nil || lag != test.want {
					t.Fatalf("admission lag = %s, %v; want %s", lag, err, test.want)
				}
				uiLag, err := tracker.SyncLagSeconds()
				if err != nil || uiLag != 60 {
					t.Fatalf("UI lag = %d, %v; want 60", uiLag, err)
				}
			})
		})
	}
}

func TestSyncLagRequiresBothMasterchainFrontiers(t *testing.T) {
	type missingFrontierCase struct {
		name   string
		mutate func(*StatusTracker)
	}
	tests := []missingFrontierCase{
		{name: "current state", mutate: func(t *StatusTracker) { t.current = nil }},
		{name: "current parsed", mutate: func(t *StatusTracker) { t.current.Masterchain.Parsed = nil }},
		{name: "current utime", mutate: func(t *StatusTracker) { t.current.Masterchain.Parsed.GenUTime = 0 }},
		{name: "current workchain", mutate: func(t *StatusTracker) { t.current.Masterchain.Block.Workchain = 0 }},
		{name: "current shard", mutate: func(t *StatusTracker) { t.current.Masterchain.Block.Shard = 0 }},
		{name: "applied state", mutate: func(t *StatusTracker) { t.appliedMasterchain = nil }},
		{name: "applied parsed", mutate: func(t *StatusTracker) { t.appliedMasterchain.Parsed = nil }},
		{name: "applied utime", mutate: func(t *StatusTracker) { t.appliedMasterchain.Parsed.GenUTime = 0 }},
		{name: "applied workchain", mutate: func(t *StatusTracker) { t.appliedMasterchain.Block.Workchain = 0 }},
		{name: "applied shard", mutate: func(t *StatusTracker) { t.appliedMasterchain.Block.Shard = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			utime := uint32(time.Now().Add(-time.Second).Unix())
			tracker := newTestStatusTrackerWithCurrent(nil, &storage.CurrentState{
				Masterchain: syncLagMasterState(20, utime),
				Shards: map[storage.ShardKey]storage.BlockState{
					{Workchain: 0, Shard: topShard}: {
						Block:  testBlockID(0, topShard, 10),
						Parsed: &tlb.ShardStateUnsplit{GenUTime: utime},
					},
				},
			})
			applied := syncLagMasterState(21, utime)
			tracker.rememberAppliedMasterchain(&applied)
			test.mutate(tracker)

			if _, err := tracker.SyncLag(); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("SyncLag error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestSyncLagCommittedStateInitializesBothFrontiers(t *testing.T) {
	node := newServiceTestNode(t)
	synctest.Test(t, func(t *testing.T) {
		tracker := newTestStatusTracker(nil, nil)
		coordinator := &SyncCoordinator{
			node:               node,
			status:             tracker,
			maintenance:        &MaintenanceRunner{},
			broadcastAdmission: newStatusTestBroadcastAdmission(),
		}
		master := syncLagMasterState(20, uint32(time.Now().Unix()-1))
		coordinator.publishCommittedCurrentState(&storage.CurrentState{
			Masterchain:      master,
			ShardClientSeqno: master.Block.SeqNo,
		})

		if lag, err := tracker.SyncLag(); err != nil || lag != time.Second {
			t.Fatalf("committed-state sync lag = %s, %v; want 1s", lag, err)
		}
	})
}

func TestSyncLagPersistedStartupInitializesBothFrontiers(t *testing.T) {
	master := syncLagMasterState(20, uint32(time.Now().Add(-time.Second).Unix()))
	store := &statusStartupStore{testCellGenerationMigrationStore: &testCellGenerationMigrationStore{
		testStorage: openTestPebbleStorage(t),
		current: &storage.CurrentState{
			Masterchain:      storage.BlockState{Block: master.Block},
			ShardClientSeqno: master.Block.SeqNo,
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{storage.BlockKey(master.Block): &master},
	}}
	coordinator := newCurrentStatePersistenceTestService(t, store, t.Context())
	synctest.Test(t, func(t *testing.T) {
		master.Parsed.GenUTime = uint32(time.Now().Unix() - 1)
		if err := coordinator.ensureCurrentState(t.Context()); !errors.Is(err, context.Canceled) {
			t.Fatalf("initialize from persisted checkpoint: %v", err)
		}

		if lag, err := coordinator.status.SyncLag(); err != nil || lag != time.Second {
			t.Fatalf("persisted-state sync lag = %s, %v; want 1s", lag, err)
		}
	})
	if store.current.Masterchain.Parsed != nil {
		t.Fatal("startup mutated the persisted metadata snapshot")
	}
}

func TestSyncLagPersistedStartupRejectsMissingMasterState(t *testing.T) {
	master := syncLagMasterState(20, uint32(time.Now().Add(-time.Second).Unix()))
	store := &testCellGenerationMigrationStore{
		testStorage: openTestPebbleStorage(t),
		current: &storage.CurrentState{
			Masterchain:      storage.BlockState{Block: master.Block},
			ShardClientSeqno: master.Block.SeqNo,
		},
	}
	coordinator := newCurrentStatePersistenceTestService(t, store, t.Context())
	if err := coordinator.ensureCurrentState(t.Context()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing persisted master state error = %v, want ErrNotFound", err)
	}
	if _, err := coordinator.status.SyncLag(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("unpublished sync lag error = %v, want ErrNotFound", err)
	}
	if coordinator.status.current != nil || coordinator.status.appliedMasterchain != nil {
		t.Fatal("missing master state published a status frontier")
	}
}

func TestSyncLagConcurrentPublication(t *testing.T) {
	utime := uint32(time.Now().Add(-time.Second).Unix())
	tracker := newTestStatusTracker(nil, nil)
	initial := syncLagMasterState(1, utime)
	tracker.rememberAppliedMasterchain(&initial)
	tracker.publishCurrentState(&storage.CurrentState{Masterchain: initial, ShardClientSeqno: 1})
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for seqno := uint32(2); seqno <= 500; seqno++ {
			master := syncLagMasterState(seqno, utime)
			tracker.rememberAppliedMasterchain(&master)
			tracker.publishCurrentState(&storage.CurrentState{Masterchain: master, ShardClientSeqno: seqno})
		}
	})
	for range 4 {
		workers.Go(func() {
			<-start
			for range 500 {
				if _, err := tracker.SyncLag(); err != nil {
					t.Errorf("SyncLag during publication: %v", err)
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	if tracker.current.Masterchain.Block.SeqNo != 500 || tracker.appliedMasterchain.Block.SeqNo != 500 {
		t.Fatal("frontiers did not advance to the final published block")
	}
}

func syncLagMasterState(seqno, utime uint32) storage.BlockState {
	return storage.BlockState{
		Block:  testBlockID(-1, topShard, seqno),
		Parsed: &tlb.ShardStateUnsplit{GenUTime: utime},
	}
}

type statusStartupStore struct {
	*testCellGenerationMigrationStore
	currentReads int
}

func (s *statusStartupStore) CurrentState(ctx context.Context) (*storage.CurrentState, error) {
	s.currentReads++
	if s.currentReads > 1 {
		// Stop at the first catch-up read, before a new block can be applied.
		return nil, context.Canceled
	}
	return s.testCellGenerationMigrationStore.CurrentState(ctx)
}
