package metrics

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/xssnick/gton/liteserver"
	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	ChainMasterchain = "masterchain"
	ChainShardchain  = "shardchain"
)

var errMetricReaderNotConfigured = errors.New("metric reader is not configured")

type Metrics struct {
	mu                  sync.RWMutex
	registry            *prometheus.Registry
	serviceStatusReader func() service.StatusSnapshot
	dbStatusReader      func(context.Context) (pebblestore.DBStatus, error)

	liteserverQueryDuration *prometheus.HistogramVec
	liteserverQueryHandler  *prometheus.HistogramVec
	liteserverQueryWait     *prometheus.HistogramVec
	liteserverQueries       *prometheus.CounterVec
	liteserverInflight      prometheus.Gauge

	syncBlocks                *prometheus.CounterVec
	syncBlockDownloadDuration *prometheus.HistogramVec
	syncBlockApplyDuration    *prometheus.HistogramVec
	syncPersistDuration       *prometheus.HistogramVec
	syncPersistQueueDuration  *prometheus.HistogramVec
	syncCheckpoints           *prometheus.CounterVec
	syncLastPublishTimestamp  prometheus.Gauge
}

func New(namespace string) *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		liteserverQueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "query_duration_seconds",
			Help:      "Total liteserver query duration in seconds, including waitMasterchainSeqno wait time.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "response", "error_code"}),
		liteserverQueryHandler: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "query_handler_duration_seconds",
			Help:      "Liteserver query handler duration in seconds, without waitMasterchainSeqno wait time.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "response", "error_code"}),
		liteserverQueryWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "query_wait_seconds",
			Help:      "Liteserver waitMasterchainSeqno wait duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "response", "error_code"}),
		liteserverQueries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "queries_total",
			Help:      "Total liteserver queries by method and response.",
		}, []string{"method", "response", "error_code"}),
		liteserverInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "inflight_queries",
			Help:      "Current number of liteserver queries being handled.",
		}),
		syncBlocks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "blocks_total",
			Help:      "Total synchronized blocks by pipeline, chain, source, result, and catch-up mode.",
		}, []string{"pipeline", "chain", "source", "result", "catch_up"}),
		syncBlockDownloadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "block_download_duration_seconds",
			Help:      "Synchronized block download duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"pipeline", "chain", "source", "result", "catch_up"}),
		syncBlockApplyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "block_apply_duration_seconds",
			Help:      "Synchronized block apply or processing duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"pipeline", "chain", "result"}),
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
		syncCheckpoints: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "checkpoints_total",
			Help:      "Total current state checkpoints by mode and result.",
		}, []string{"mode", "result"}),
		syncLastPublishTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "sync",
			Name:      "last_publish_timestamp_seconds",
			Help:      "Unix timestamp of the last published current state.",
		}),
	}

	registry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		m.liteserverQueryDuration,
		m.liteserverQueryHandler,
		m.liteserverQueryWait,
		m.liteserverQueries,
		m.liteserverInflight,
		m.syncBlocks,
		m.syncBlockDownloadDuration,
		m.syncBlockApplyDuration,
		m.syncPersistDuration,
		m.syncPersistQueueDuration,
		m.syncCheckpoints,
		m.syncLastPublishTimestamp,
		newServiceCollector(m, namespace),
		newDBCollector(m, namespace),
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) SetServiceStatusReader(reader func() service.StatusSnapshot) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.serviceStatusReader = reader
	m.mu.Unlock()
}

func (m *Metrics) serviceStatus() (service.StatusSnapshot, error) {
	if m == nil {
		return service.StatusSnapshot{}, errMetricReaderNotConfigured
	}
	m.mu.RLock()
	reader := m.serviceStatusReader
	m.mu.RUnlock()
	if reader == nil {
		return service.StatusSnapshot{}, errMetricReaderNotConfigured
	}
	return reader(), nil
}

func (m *Metrics) SetDBStatusReader(reader func(context.Context) (pebblestore.DBStatus, error)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.dbStatusReader = reader
	m.mu.Unlock()
}

func (m *Metrics) dbStatus(ctx context.Context) (pebblestore.DBStatus, error) {
	if m == nil {
		return pebblestore.DBStatus{}, errMetricReaderNotConfigured
	}
	m.mu.RLock()
	reader := m.dbStatusReader
	m.mu.RUnlock()
	if reader == nil {
		return pebblestore.DBStatus{}, errMetricReaderNotConfigured
	}
	return reader(ctx)
}

func (m *Metrics) AddLiteserverInflight(delta int) {
	if m == nil || m.liteserverInflight == nil || delta == 0 {
		return
	}
	m.liteserverInflight.Add(float64(delta))
}

func (m *Metrics) ObserveLiteserverQuery(observation liteserver.QueryObservation) {
	if m == nil || m.liteserverQueryDuration == nil || m.liteserverQueries == nil {
		return
	}
	method := observation.Method
	if method == "" {
		method = "unknown"
	}
	response := observation.Response
	if response == "" {
		response = "unknown"
	}
	errorCode := strconv.FormatInt(int64(observation.ErrorCode), 10)
	duration := observation.Duration
	if duration < 0 {
		duration = 0
	}
	waitDuration := observation.WaitDuration
	if waitDuration < 0 {
		waitDuration = 0
	}

	totalDuration := duration + waitDuration
	m.liteserverQueryDuration.WithLabelValues(method, response, errorCode).Observe(totalDuration.Seconds())
	if m.liteserverQueryHandler != nil {
		m.liteserverQueryHandler.WithLabelValues(method, response, errorCode).Observe(duration.Seconds())
	}
	m.liteserverQueries.WithLabelValues(method, response, errorCode).Inc()
	if waitDuration > 0 && m.liteserverQueryWait != nil {
		m.liteserverQueryWait.WithLabelValues(method, response, errorCode).Observe(waitDuration.Seconds())
	}
}

func (m *Metrics) ObserveSyncCurrentState(_ service.SyncCurrentStateObservation) {
	if m == nil || m.syncLastPublishTimestamp == nil {
		return
	}
	m.syncLastPublishTimestamp.Set(float64(time.Now().Unix()))
}

func (m *Metrics) ObserveSyncBlock(observation service.SyncBlockObservation) {
	if m == nil || m.syncBlocks == nil {
		return
	}
	pipeline := fallbackLabel(observation.Pipeline)
	chain := fallbackLabel(observation.Chain)
	source := fallbackLabel(observation.Source)
	result := fallbackLabel(observation.Result)
	catchUp := strconv.FormatBool(observation.CatchUp)

	m.syncBlocks.WithLabelValues(pipeline, chain, source, result, catchUp).Inc()
	if observation.DownloadDuration > 0 && m.syncBlockDownloadDuration != nil {
		m.syncBlockDownloadDuration.WithLabelValues(pipeline, chain, source, result, catchUp).Observe(observation.DownloadDuration.Seconds())
	}
	if observation.ApplyDuration > 0 && m.syncBlockApplyDuration != nil {
		m.syncBlockApplyDuration.WithLabelValues(pipeline, chain, result).Observe(observation.ApplyDuration.Seconds())
	}
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
}

func fallbackLabel(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
