package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	ChainMasterchain = "masterchain"
	ChainShardchain  = "shardchain"
)

type Metrics struct {
	registry                *prometheus.Registry
	liteserverQueryDuration *prometheus.HistogramVec
	syncLag                 *prometheus.GaugeVec
}

func New() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		liteserverQueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "flexserver",
			Subsystem: "liteserver",
			Name:      "query_duration_seconds",
			Help:      "Liteserver query handling duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method"}),
		syncLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "flexserver",
			Subsystem: "sync",
			Name:      "lag_seconds",
			Help:      "Local chain lag in seconds by chain.",
		}, []string{"chain"}),
	}

	registry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		m.liteserverQueryDuration,
		m.syncLag,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveLiteserverQuery(method string, duration time.Duration) {
	if m == nil || m.liteserverQueryDuration == nil {
		return
	}
	if method == "" {
		method = "unknown"
	}
	if duration < 0 {
		duration = 0
	}
	m.liteserverQueryDuration.WithLabelValues(method).Observe(duration.Seconds())
}

func (m *Metrics) SetSyncLag(chain string, seconds float64) {
	if m == nil || m.syncLag == nil {
		return
	}
	if chain == "" {
		chain = "unknown"
	}
	m.syncLag.WithLabelValues(chain).Set(seconds)
}
