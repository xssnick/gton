package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	ChainMasterchain = "masterchain"
)

type Metrics struct {
	registry            *prometheus.Registry
	namespace           string
	serviceStatusReader func() service.StatusSnapshot
	dbStatusReader      func(context.Context) (pebblestore.DBStatus, error)
	lazyCellLoadReader  func() []storage.LazyCellLoadMetric
	artifactStatus      *storageArtifactStatusReader

	syncBlocks                *prometheus.CounterVec
	syncBlockOrigins          *prometheus.CounterVec
	syncBlockDownloadDuration *prometheus.HistogramVec
	syncBlockPrepareDuration  *prometheus.HistogramVec
	syncBlockApplyDuration    *prometheus.HistogramVec
	syncObtainDuration        *prometheus.HistogramVec
	syncPersistDuration       *prometheus.HistogramVec
	syncPersistQueueDuration  *prometheus.HistogramVec
	syncPersistStageDuration  *prometheus.HistogramVec
	syncCheckpoints           *prometheus.CounterVec

	p2pBroadcastPipelineStageDuration  *prometheus.HistogramVec
	p2pBroadcastPipelineStageObservers sync.Map
}

type RuntimeReaders struct {
	ServiceStatusReader func() service.StatusSnapshot
	DBStatusReader      func(context.Context) (pebblestore.DBStatus, error)
	LazyCellLoadReader  func() []storage.LazyCellLoadMetric
	ArchivePackagesDir  string
	StateFilesDir       string
}

type p2pBroadcastPipelineStageMetricKey struct {
	stage    string
	kind     string
	delivery string
	result   string
}

func New(namespace string) *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry:  registry,
		namespace: namespace,
		syncBlocks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "blocks_total",
			Help:      "Total synchronized blocks by pipeline, chain, source, result, and catch-up mode.",
		}, []string{"pipeline", "chain", "source", "result", "catch_up"}),
		syncBlockOrigins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "block_origins_total",
			Help:      "Total synchronized blocks by origin: broadcast, download, stored, or other.",
		}, []string{"pipeline", "chain", "origin", "result", "catch_up"}),
		syncBlockDownloadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "block_download_duration_seconds",
			Help:      "Synchronized block download duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"pipeline", "chain", "source", "result", "catch_up"}),
		syncBlockPrepareDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "block_prepare_duration_seconds",
			Help:      "Synchronized block post-download processing duration in seconds, including validation, consensus checks, and state cell preparation after the block is dequeued, excluding network download, queue wait, and state apply.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"pipeline", "chain", "shard", "source", "result", "catch_up"}),
		syncBlockApplyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "block_apply_duration_seconds",
			Help:      "Synchronized block state apply duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"pipeline", "chain", "result"}),
		syncObtainDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "master_shards_obtain_duration_seconds",
			Help:      "Duration spent obtaining a master block or shard blocks needed for one master transition, excluding state apply and checkpoint persistence.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{"pipeline", "stage", "result", "catch_up"}),
		syncPersistDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "persist_duration_seconds",
			Help:      "Current state checkpoint persistence duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"mode", "result"}),
		syncPersistQueueDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "persist_queue_seconds",
			Help:      "Current state checkpoint queue and lock wait duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"mode", "result"}),
		syncPersistStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "checkpoint_stage_duration_seconds",
			Help:      "Current state checkpoint duration split by storage and prewrite wait stage.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"mode", "stage", "result"}),
		syncCheckpoints: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "checkpoints_total",
			Help:      "Total current state checkpoints by mode and result.",
		}, []string{"mode", "result"}),
		p2pBroadcastPipelineStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "p2p",
			Name:      "broadcast_pipeline_stage_duration_seconds",
			Help:      "Inbound P2P broadcast pipeline stage duration in seconds after the overlay has delivered a TL broadcast to gton.",
			Buckets: []float64{
				0.00005, 0.0001, 0.00025, 0.0005,
				0.001, 0.0025, 0.005, 0.01, 0.025,
				0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
			},
		}, []string{"stage", "kind", "delivery", "result"}),
	}

	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.syncBlocks,
		m.syncBlockOrigins,
		m.syncBlockDownloadDuration,
		m.syncBlockPrepareDuration,
		m.syncBlockApplyDuration,
		m.syncObtainDuration,
		m.syncPersistDuration,
		m.syncPersistQueueDuration,
		m.syncPersistStageDuration,
		m.syncCheckpoints,
		m.p2pBroadcastPipelineStageDuration,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Namespace() string {
	if m == nil {
		return ""
	}
	return m.namespace
}

func (m *Metrics) RegisterCollector(collector prometheus.Collector) error {
	if m == nil {
		return errors.New("metrics registry is not configured")
	}
	return m.registry.Register(collector)
}

func (m *Metrics) RegisterRuntimeCollectors(readers RuntimeReaders) error {
	if readers.ServiceStatusReader == nil {
		return errors.New("service status metric reader is required")
	}
	if readers.DBStatusReader == nil {
		return errors.New("db status metric reader is required")
	}
	m.serviceStatusReader = readers.ServiceStatusReader
	m.dbStatusReader = readers.DBStatusReader
	m.lazyCellLoadReader = readers.LazyCellLoadReader
	artifactStatus, err := scanStorageArtifacts(context.Background(), readers.ArchivePackagesDir, readers.StateFilesDir)
	if err != nil {
		return fmt.Errorf("scan storage artifacts: %w", err)
	}
	m.artifactStatus = newStorageArtifactStatusReader(artifactStatus)
	for _, collector := range []prometheus.Collector{
		newServiceCollector(m, m.namespace),
		newDBCollector(m, m.namespace),
		newStorageArtifactCollector(m, m.namespace),
	} {
		if err := m.registry.Register(collector); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) serviceStatus() service.StatusSnapshot {
	return m.serviceStatusReader()
}

func (m *Metrics) dbStatus(ctx context.Context) (pebblestore.DBStatus, error) {
	return m.dbStatusReader(ctx)
}

func (m *Metrics) lazyCellLoads() []storage.LazyCellLoadMetric {
	if m.lazyCellLoadReader == nil {
		return nil
	}
	return m.lazyCellLoadReader()
}

func (m *Metrics) storageArtifactStatus() storageArtifactStatus {
	return m.artifactStatus.Status()
}

func (m *Metrics) AddArchivePackageBytes(delta int64) {
	if m == nil || m.artifactStatus == nil {
		return
	}
	m.artifactStatus.AddArchivePackageBytes(delta)
}

func (m *Metrics) AddPersistentStateBytes(delta int64) {
	if m == nil || m.artifactStatus == nil {
		return
	}
	m.artifactStatus.AddPersistentStateBytes(delta)
}

func (m *Metrics) ObserveSyncBlock(observation service.SyncBlockObservation) {
	if m == nil || m.syncBlocks == nil {
		return
	}
	pipeline := fallbackLabel(observation.Pipeline)
	chain := fallbackLabel(observation.Chain)
	shard := fallbackLabel(observation.Shard)
	source := fallbackLabel(string(observation.Source))
	origin := fallbackLabel(string(observation.Origin))
	result := fallbackLabel(observation.Result)
	catchUp := strconv.FormatBool(observation.CatchUp)

	m.syncBlocks.WithLabelValues(pipeline, chain, source, result, catchUp).Inc()
	if m.syncBlockOrigins != nil {
		m.syncBlockOrigins.WithLabelValues(pipeline, chain, origin, result, catchUp).Inc()
	}
	if observation.DownloadDuration > 0 && m.syncBlockDownloadDuration != nil {
		m.syncBlockDownloadDuration.WithLabelValues(pipeline, chain, source, result, catchUp).Observe(observation.DownloadDuration.Seconds())
	}
	if observation.PrepareDuration > 0 && m.syncBlockPrepareDuration != nil {
		m.syncBlockPrepareDuration.WithLabelValues(pipeline, chain, shard, source, result, catchUp).Observe(observation.PrepareDuration.Seconds())
	}
	if observation.ApplyDuration > 0 && m.syncBlockApplyDuration != nil {
		m.syncBlockApplyDuration.WithLabelValues(pipeline, chain, result).Observe(observation.ApplyDuration.Seconds())
	}
}

func (m *Metrics) ObserveSyncObtain(observation service.SyncObtainObservation) {
	if m == nil || m.syncObtainDuration == nil {
		return
	}
	pipeline := fallbackLabel(observation.Pipeline)
	stage := fallbackLabel(observation.Stage)
	result := fallbackLabel(observation.Result)
	catchUp := strconv.FormatBool(observation.CatchUp)
	duration := observation.Duration
	if duration < 0 {
		duration = 0
	}

	m.syncObtainDuration.WithLabelValues(pipeline, stage, result, catchUp).Observe(duration.Seconds())
}

func (m *Metrics) ObserveSyncPersist(observation service.SyncPersistObservation) {
	if m == nil || m.syncCheckpoints == nil {
		return
	}
	mode := fallbackLabel(observation.Mode)
	result := fallbackLabel(observation.Result)
	m.syncCheckpoints.WithLabelValues(mode, result).Inc()
	if observation.Duration > 0 && m.syncPersistDuration != nil {
		m.syncPersistDuration.WithLabelValues(mode, result).Observe(observation.Duration.Seconds())
	}
	if observation.QueueDuration > 0 && m.syncPersistQueueDuration != nil {
		m.syncPersistQueueDuration.WithLabelValues(mode, result).Observe(observation.QueueDuration.Seconds())
	}
	if m.syncPersistStageDuration != nil {
		for _, stage := range observation.Stages {
			if stage.Duration <= 0 {
				continue
			}
			m.syncPersistStageDuration.WithLabelValues(mode, fallbackLabel(stage.Stage), result).Observe(stage.Duration.Seconds())
		}
	}
}

func (m *Metrics) ObserveBroadcastPipelineStage(observation p2p.BroadcastPipelineStageObservation) {
	if m == nil || m.p2pBroadcastPipelineStageDuration == nil {
		return
	}
	duration := observation.Duration
	if duration < 0 {
		duration = 0
	}
	key := p2pBroadcastPipelineStageMetricKey{
		stage:    fallbackLabel(observation.Stage),
		kind:     fallbackLabel(observation.Kind),
		delivery: fallbackLabel(string(observation.Delivery)),
		result:   fallbackLabel(observation.Result),
	}

	m.p2pBroadcastPipelineStageObserver(key).Observe(duration.Seconds())
}

func (m *Metrics) p2pBroadcastPipelineStageObserver(key p2pBroadcastPipelineStageMetricKey) prometheus.Observer {
	if observer, ok := m.p2pBroadcastPipelineStageObservers.Load(key); ok {
		return observer.(prometheus.Observer)
	}

	observer := m.p2pBroadcastPipelineStageDuration.WithLabelValues(key.stage, key.kind, key.delivery, key.result)
	actual, _ := m.p2pBroadcastPipelineStageObservers.LoadOrStore(key, observer)
	return actual.(prometheus.Observer)
}

func fallbackLabel(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
