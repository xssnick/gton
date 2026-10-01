package service

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
)

func TestDefaultPersistentStateKeepRecentIsOne(t *testing.T) {
	if DefaultPersistentStateKeepRecent != 1 {
		t.Fatalf("default persistent state keep recent = %d, want 1", DefaultPersistentStateKeepRecent)
	}
}

func TestComponentConstructorsKeepZeroTTLs(t *testing.T) {
	status := newTestStatusTracker(nil, nil)
	state := NewStateLifecycle(zerolog.Nop(), nil, status, StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, status, MaintenanceRunnerOptions{})

	if state.stateTTL != 0 {
		t.Fatalf("state ttl = %s, want 0", state.stateTTL)
	}
	if maintenance.archiveTTL != 0 {
		t.Fatalf("archive ttl = %s, want 0", maintenance.archiveTTL)
	}
	if maintenance.persistentStateKeepRecent != DefaultPersistentStateKeepRecent {
		t.Fatalf("persistent state keep recent = %d, want %d", maintenance.persistentStateKeepRecent, DefaultPersistentStateKeepRecent)
	}
}

func TestMaintenanceRunnerKeepsAllPersistentStates(t *testing.T) {
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, newTestStatusTracker(nil, nil), MaintenanceRunnerOptions{
		PersistentStateKeepRecent: PersistentStateKeepAll,
	})

	if maintenance.persistentStateKeepRecent != PersistentStateKeepAll {
		t.Fatalf("persistent state keep recent = %d, want %d", maintenance.persistentStateKeepRecent, PersistentStateKeepAll)
	}
}

func TestPersistentStateGCUsesConfiguredRetention(t *testing.T) {
	store := &testPersistentStateGCStore{}
	svc := &SyncCoordinator{log: zerolog.Nop(), storage: store}
	maintenance := bindTestMaintenanceRunner(t, svc, StateLifecycleOptions{}, MaintenanceRunnerOptions{PersistentStateKeepRecent: 7})

	if _, err := maintenance.runPersistentStateGCOnce(context.Background()); err != nil {
		t.Fatalf("persistent state gc: %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("persistent state gc calls = %d, want 1", store.calls)
	}
	if store.keepRecentGroups != 7 {
		t.Fatalf("persistent state keep recent = %d, want 7", store.keepRecentGroups)
	}
}

func TestPersistentStateGCSkipsWhenKeepingAll(t *testing.T) {
	store := &testPersistentStateGCStore{}
	svc := &SyncCoordinator{log: zerolog.Nop(), storage: store}
	maintenance := bindTestMaintenanceRunner(t, svc, StateLifecycleOptions{}, MaintenanceRunnerOptions{PersistentStateKeepRecent: PersistentStateKeepAll})

	if pruned, err := maintenance.runPersistentStateGCOnce(context.Background()); err != nil || pruned {
		t.Fatalf("persistent state gc = (%v, %v), want (false, nil)", pruned, err)
	}
	if store.calls != 0 {
		t.Fatalf("persistent state gc calls = %d, want 0", store.calls)
	}
}

type testPersistentStateGCStore struct {
	testStorage
	calls            int
	keepRecentGroups int
}

func (s *testPersistentStateGCStore) PruneExpiredPersistentStateFiles(_ context.Context, _ uint64, keepRecentGroups int, _ int) (storage.PersistentStatePruneStats, error) {
	s.calls++
	s.keepRecentGroups = keepRecentGroups
	return storage.PersistentStatePruneStats{}, nil
}

func TestServiceMaintenanceStopsAfterSyncUntilFrozen(t *testing.T) {
	svc := &SyncCoordinator{
		log:       zerolog.Nop(),
		node:      newFrozenTestNode(t),
		syncUntil: 200,
	}
	maintenance := bindTestMaintenanceRunner(t, svc, StateLifecycleOptions{}, MaintenanceRunnerOptions{})
	freezeSyncUntil(t, svc)

	maintenance.runServiceMaintenance(context.Background())
}

func TestMaintenanceTasksSkipAfterSyncUntilFrozen(t *testing.T) {
	svc := &SyncCoordinator{
		log:       zerolog.Nop(),
		node:      newFrozenTestNode(t),
		syncUntil: 200,
	}
	state, maintenance := bindTestStateAndMaintenance(t, svc, StateLifecycleOptions{}, MaintenanceRunnerOptions{ArchiveTTL: 24})
	freezeSyncUntil(t, svc)

	if pruned, err := maintenance.runPersistentStateGCOnce(context.Background()); err != nil || pruned {
		t.Fatalf("persistent state gc = (%v, %v), want (false, nil)", pruned, err)
	}
	if pruned, err := maintenance.runArchiveGCOnce(context.Background()); err != nil || pruned {
		t.Fatalf("archive gc = (%v, %v), want (false, nil)", pruned, err)
	}
	if ran, err := maintenance.runPendingCellGenerationMigration(context.Background()); err != nil || ran {
		t.Fatalf("pending migration = (%v, %v), want (false, nil)", ran, err)
	}
	if err := state.processPersistentStateSerialization(context.Background()); err != nil {
		t.Fatalf("state serialization: %v", err)
	}
	state.afterPersistentStateSerialized(context.Background(), testBlockID(-1, topShard, 100), PersistentStateSerializationAll)
}

func TestAutomaticStateSerializationWaitsForNextSync(t *testing.T) {
	svc := &SyncCoordinator{log: zerolog.Nop()}
	state, maintenance := bindTestStateAndMaintenance(t, svc, StateLifecycleOptions{}, MaintenanceRunnerOptions{})

	if err := state.processPersistentStateSerialization(context.Background()); err != nil {
		t.Fatalf("state serialization before next sync: %v", err)
	}
	if maintenance.automaticStateSerializationReady.Load() {
		t.Fatal("automatic state serialization is ready before next sync")
	}

	maintenance.enableAutomaticStateSerialization()
	if !maintenance.automaticStateSerializationReady.Load() {
		t.Fatal("automatic state serialization is not ready after next sync starts")
	}
	select {
	case <-maintenance.maintenanceWake:
	default:
		t.Fatal("maintenance worker was not woken after next sync started")
	}
}

func TestNextServiceMaintenanceTaskPriority(t *testing.T) {
	tests := []struct {
		name             string
		persistentGC     bool
		archiveGC        bool
		migrationPending bool
		serialization    bool
		want             serviceMaintenanceTask
	}{
		{
			name:             "migration before gc and serialization",
			persistentGC:     true,
			archiveGC:        true,
			migrationPending: true,
			serialization:    true,
			want:             serviceMaintenanceTaskCellGenerationMigration,
		},
		{
			name:          "archive gc before serialization",
			archiveGC:     true,
			serialization: true,
			want:          serviceMaintenanceTaskArchiveTTLGC,
		},
		{
			name:             "migration before serialization",
			migrationPending: true,
			serialization:    true,
			want:             serviceMaintenanceTaskCellGenerationMigration,
		},
		{
			name:          "serialization when no higher priority task is ready",
			serialization: true,
			want:          serviceMaintenanceTaskStateSerialization,
		},
		{
			name: "nothing ready",
			want: serviceMaintenanceTaskNone,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := nextServiceMaintenanceTask(test.persistentGC, test.archiveGC, test.migrationPending, test.serialization)
			if got != test.want {
				t.Fatalf("task = %d, want %d", got, test.want)
			}
		})
	}
}
