package collator

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const statusCollectionTimeout = time.Second

type metricsRegistry interface {
	Namespace() string
	RegisterCollector(prometheus.Collector) error
}

type statusCollector struct {
	controller Controller

	available       *prometheus.Desc
	controllerState *prometheus.Desc
	sessions        *prometheus.Desc
	windows         *prometheus.Desc
	windowsTotal    *prometheus.Desc
	storageRecords  *prometheus.Desc
	pendingWrites   *prometheus.Desc
	dbBytes         *prometheus.Desc
	dbReadAmp       *prometheus.Desc
	dbL0Files       *prometheus.Desc
	dbL0Sublevels   *prometheus.Desc
	dbCompactions   *prometheus.Desc
	lastCompleted   *prometheus.Desc
}

func registerStatusCollector(value any, controller Controller) error {
	if value == nil {
		return nil
	}
	registry, ok := value.(metricsRegistry)
	if !ok {
		return errors.New("collator extension: node metrics registry has an incompatible type")
	}

	return registry.RegisterCollector(newStatusCollector(registry.Namespace(), controller))
}

func newStatusCollector(namespace string, controller Controller) *statusCollector {
	labels := []string{"mode"}
	return &statusCollector{
		controller: controller,
		available: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "status_available"),
			"Whether the latest standalone collator status scrape completed successfully.",
			labels,
			nil,
		),
		controllerState: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "controller_state"),
			"Standalone collator controller lifecycle state.",
			append(labels, "state"),
			nil,
		),
		sessions: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "controller_sessions"),
			"Standalone collator sessions by controller projection.",
			append(labels, "state"),
			nil,
		),
		windows: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "status_windows"),
			"Standalone collator windows by current backend state.",
			append(labels, "state"),
			nil,
		),
		windowsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "status_windows_total"),
			"Standalone collator cumulative window status counters.",
			append(labels, "result"),
			nil,
		),
		storageRecords: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "storage_records"),
			"Standalone collator durable records by type.",
			append(labels, "type"),
			nil,
		),
		pendingWrites: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "storage_pending_writes"),
			"Standalone collator storage writes awaiting completion.",
			labels,
			nil,
		),
		dbBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "storage_db_bytes"),
			"Standalone collator Pebble bytes by physical category.",
			append(labels, "type"),
			nil,
		),
		dbReadAmp: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "storage_db_read_amp"),
			"Standalone collator Pebble read amplification estimate.",
			labels,
			nil,
		),
		dbL0Files: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "storage_db_l0_files"),
			"Standalone collator Pebble L0 file count.",
			labels,
			nil,
		),
		dbL0Sublevels: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "storage_db_l0_sublevels"),
			"Standalone collator Pebble L0 sublevel count.",
			labels,
			nil,
		),
		dbCompactions: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "storage_db_compactions_in_progress"),
			"Standalone collator Pebble compactions currently in progress.",
			labels,
			nil,
		),
		lastCompleted: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "collator", "last_completed_timestamp_seconds"),
			"Unix timestamp of the last completed standalone collator window.",
			labels,
			nil,
		),
	}
}

func (c *statusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		c.available, c.controllerState, c.sessions, c.windows, c.windowsTotal,
		c.storageRecords, c.pendingWrites, c.dbBytes, c.dbReadAmp, c.dbL0Files,
		c.dbL0Sublevels, c.dbCompactions, c.lastCompleted,
	} {
		ch <- desc
	}
}

func (c *statusCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), statusCollectionTimeout)
	status, err := c.controller.Status(ctx)
	cancel()
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.available, prometheus.GaugeValue, 0, "standalone")

		return
	}

	ch <- prometheus.MustNewConstMetric(c.available, prometheus.GaugeValue, 1, "standalone")
	collectBoolState(ch, c.controllerState, "started", status.Started)
	collectBoolState(ch, c.controllerState, "closing", status.Closing)
	collectBoolState(ch, c.controllerState, "closed", status.Closed)

	for state, value := range map[string]int{
		"active":   status.ActiveSessions,
		"future":   status.FutureSessions,
		"backend":  status.BackendSessions,
		"observer": status.ObserverSessions,
	} {
		ch <- prometheus.MustNewConstMetric(c.sessions, prometheus.GaugeValue, float64(value), "standalone", state)
	}
	backend := status.Backend
	ch <- prometheus.MustNewConstMetric(c.windows, prometheus.GaugeValue, float64(backend.ActiveWindows), "standalone", "active")
	ch <- prometheus.MustNewConstMetric(c.windows, prometheus.GaugeValue, float64(backend.RetryingWindows), "standalone", "retrying")
	ch <- prometheus.MustNewConstMetric(c.windowsTotal, prometheus.CounterValue, float64(backend.CompletedWindows), "standalone", "success")
	ch <- prometheus.MustNewConstMetric(c.windowsTotal, prometheus.CounterValue, float64(backend.FailedWindows), "standalone", "error")

	storage := backend.Storage
	for recordType, value := range map[string]uint64{
		"sessions":   storage.Sessions,
		"candidates": storage.Candidates,
	} {
		ch <- prometheus.MustNewConstMetric(c.storageRecords, prometheus.GaugeValue, float64(value), "standalone", recordType)
	}
	ch <- prometheus.MustNewConstMetric(c.pendingWrites, prometheus.GaugeValue, float64(storage.PendingWrites), "standalone")

	db := storage.DB
	for byteType, value := range map[string]uint64{
		"disk":            db.DiskSize,
		"live":            db.LiveSize,
		"wal":             db.WALSize,
		"memtable":        db.MemTableSize,
		"compaction_debt": db.CompactionDebt,
	} {
		ch <- prometheus.MustNewConstMetric(c.dbBytes, prometheus.GaugeValue, float64(value), "standalone", byteType)
	}
	ch <- prometheus.MustNewConstMetric(c.dbReadAmp, prometheus.GaugeValue, float64(db.ReadAmp), "standalone")
	ch <- prometheus.MustNewConstMetric(c.dbL0Files, prometheus.GaugeValue, float64(db.L0Files), "standalone")
	ch <- prometheus.MustNewConstMetric(c.dbL0Sublevels, prometheus.GaugeValue, float64(db.L0Sublevels), "standalone")
	ch <- prometheus.MustNewConstMetric(c.dbCompactions, prometheus.GaugeValue, float64(db.CompactionsInProgress), "standalone")
	if !backend.LastCompleted.IsZero() {
		ch <- prometheus.MustNewConstMetric(
			c.lastCompleted,
			prometheus.GaugeValue,
			float64(backend.LastCompleted.Unix()),
			"standalone",
		)
	}
}

func collectBoolState(
	ch chan<- prometheus.Metric,
	desc *prometheus.Desc,
	state string,
	value bool,
) {
	number := float64(0)
	if value {
		number = 1
	}
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, number, "standalone", state)
}
