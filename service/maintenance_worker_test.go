package service

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

func TestServiceNewKeepsZeroTTLs(t *testing.T) {
	svc := New(zerolog.Nop(), nil, nil, nil, nil, Options{})

	if svc.stateTTL != 0 {
		t.Fatalf("state ttl = %s, want 0", svc.stateTTL)
	}
	if svc.archiveTTL != 0 {
		t.Fatalf("archive ttl = %s, want 0", svc.archiveTTL)
	}
}

func TestServiceMaintenanceStopsAfterSyncUntilFrozen(t *testing.T) {
	svc := &Service{
		log:             zerolog.Nop(),
		node:            newFrozenTestNode(t),
		syncUntil:       200,
		maintenanceWake: make(chan struct{}, 1),
	}

	svc.runServiceMaintenance(context.Background())
}

func TestMaintenanceTasksSkipAfterSyncUntilFrozen(t *testing.T) {
	svc := &Service{
		log:             zerolog.Nop(),
		node:            newFrozenTestNode(t),
		syncUntil:       200,
		archiveTTL:      24,
		maintenanceWake: make(chan struct{}, 1),
		stateSerializer: &stateSerializer{},
	}

	if pruned, err := svc.runPersistentStateGCOnce(context.Background()); err != nil || pruned {
		t.Fatalf("persistent state gc = (%v, %v), want (false, nil)", pruned, err)
	}
	if pruned, err := svc.runArchiveGCOnce(context.Background()); err != nil || pruned {
		t.Fatalf("archive gc = (%v, %v), want (false, nil)", pruned, err)
	}
	if ran, err := svc.runPendingCellGenerationMigration(context.Background()); err != nil || ran {
		t.Fatalf("pending migration = (%v, %v), want (false, nil)", ran, err)
	}
	if err := svc.processPersistentStateSerialization(context.Background()); err != nil {
		t.Fatalf("state serialization: %v", err)
	}
	svc.afterPersistentStateSerialized(context.Background(), testBlockID(-1, topShard, 100), PersistentStateSerializationAll)
}

func TestAutomaticStateSerializationWaitsForNextSync(t *testing.T) {
	svc := &Service{
		stateSerializer: &stateSerializer{},
		maintenanceWake: make(chan struct{}, 1),
	}

	if err := svc.processPersistentStateSerialization(context.Background()); err != nil {
		t.Fatalf("state serialization before next sync: %v", err)
	}
	if svc.automaticStateSerializationReady.Load() {
		t.Fatal("automatic state serialization is ready before next sync")
	}

	svc.enableAutomaticStateSerialization()
	if !svc.automaticStateSerializationReady.Load() {
		t.Fatal("automatic state serialization is not ready after next sync starts")
	}
	select {
	case <-svc.maintenanceWake:
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
