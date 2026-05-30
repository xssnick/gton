package service

import (
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
	if svc.disableArchiveBackfill {
		t.Fatal("archive backfill should be enabled by default")
	}
}

func TestServiceNewStoresArchiveBackfillDisable(t *testing.T) {
	svc := New(zerolog.Nop(), nil, nil, nil, nil, Options{DisableArchiveBackfill: true})

	if !svc.disableArchiveBackfill {
		t.Fatal("archive backfill disable flag was not stored")
	}
}

func TestNextServiceMaintenanceTaskPriority(t *testing.T) {
	tests := []struct {
		name             string
		persistentGC     bool
		archiveGC        bool
		migrationPending bool
		serialization    bool
		backfill         bool
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
			name:          "archive backfill before serialization",
			serialization: true,
			backfill:      true,
			want:          serviceMaintenanceTaskArchiveBackfill,
		},
		{
			name:          "serialization when no higher priority task is ready",
			serialization: true,
			want:          serviceMaintenanceTaskStateSerialization,
		},
		{
			name:     "archive backfill when no higher priority task is ready",
			backfill: true,
			want:     serviceMaintenanceTaskArchiveBackfill,
		},
		{
			name: "nothing ready",
			want: serviceMaintenanceTaskNone,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := nextServiceMaintenanceTask(test.persistentGC, test.archiveGC, test.migrationPending, test.serialization, test.backfill)
			if got != test.want {
				t.Fatalf("task = %d, want %d", got, test.want)
			}
		})
	}
}
