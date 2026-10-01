package collator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	core "github.com/xssnick/gton/service/validator/collator"
)

type extensionMetricsTestRegistry struct {
	registry *prometheus.Registry
}

func (r extensionMetricsTestRegistry) Namespace() string {
	return "gton"
}

func (r extensionMetricsTestRegistry) RegisterCollector(collector prometheus.Collector) error {
	return r.registry.Register(collector)
}

func TestStandaloneStatusCollectorExportsControllerAndStorage(t *testing.T) {
	registry := prometheus.NewRegistry()
	controller := &testController{status: core.ControllerStatus{
		Started:          true,
		ActiveSessions:   2,
		FutureSessions:   1,
		BackendSessions:  2,
		ObserverSessions: 3,
		Backend: core.Status{
			Started:          true,
			ActiveWindows:    1,
			RetryingWindows:  1,
			CompletedWindows: 7,
			FailedWindows:    2,
			LastCompleted:    time.Unix(123, 0),
			Storage: core.StorageStatus{
				Sessions: 2, Candidates: 9, PendingWrites: 1,
				DB: core.DBMetrics{DiskSize: 1024, LiveSize: 512, L0Files: 2},
			},
		},
	}}
	if err := registerStatusCollector(
		extensionMetricsTestRegistry{registry: registry},
		controller,
	); err != nil {
		t.Fatal(err)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	for _, name := range []string{
		"gton_collator_status_available",
		"gton_collator_controller_sessions",
		"gton_collator_status_windows",
		"gton_collator_storage_records",
		"gton_collator_storage_db_bytes",
	} {
		if _, found := names[name]; !found {
			t.Fatalf("metric family %s was not exported", name)
		}
	}
}
