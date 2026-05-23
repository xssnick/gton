package service

import "testing"

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
