package liteserver

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	liteserverMetricsNoErrorCode          = "0"
	liteserverMetricsUnspecifiedErrorCode = "unspecified"
	liteserverMetricsNoErrorReason        = "none"
	liteserverMetricsUnspecifiedReason    = "unspecified"
)

type metricsRegistry interface {
	Namespace() string
	RegisterCollector(prometheus.Collector) error
}

type prometheusQueryObserver struct {
	queryDuration *prometheus.HistogramVec
	queryHandler  *prometheus.HistogramVec
	queryWait     *prometheus.HistogramVec
	queries       *prometheus.CounterVec
	inflight      prometheus.Gauge
}

func liteserverQueryObserver(metrics any) (QueryObserver, error) {
	if metrics == nil {
		return nil, nil
	}
	value := reflect.ValueOf(metrics)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return nil, nil
	}

	registry, ok := metrics.(metricsRegistry)
	if !ok {
		return nil, nil
	}

	observer := newPrometheusQueryObserver(registry.Namespace())
	for _, collector := range observer.collectors() {
		if err := registry.RegisterCollector(collector); err != nil {
			return nil, err
		}
	}
	return observer, nil
}

func newPrometheusQueryObserver(namespace string) *prometheusQueryObserver {
	return &prometheusQueryObserver{
		queryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "query_duration_seconds",
			Help:      "Total liteserver query duration in seconds, including waitMasterchainSeqno wait time.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "response", "error_code", "reason"}),
		queryHandler: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "query_handler_duration_seconds",
			Help:      "Liteserver query handler duration in seconds, without waitMasterchainSeqno wait time.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "response", "error_code", "reason"}),
		queryWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "query_wait_seconds",
			Help:      "Liteserver waitMasterchainSeqno wait duration in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "response", "error_code", "reason"}),
		queries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "queries_total",
			Help:      "Total liteserver queries by method and response.",
		}, []string{"method", "response", "error_code", "reason"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "liteserver",
			Name:      "inflight_queries",
			Help:      "Current number of liteserver queries being handled.",
		}),
	}
}

func (o *prometheusQueryObserver) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		o.queryDuration,
		o.queryHandler,
		o.queryWait,
		o.queries,
		o.inflight,
	}
}

func (o *prometheusQueryObserver) AddLiteserverInflight(delta int) {
	if o == nil || o.inflight == nil || delta == 0 {
		return
	}
	o.inflight.Add(float64(delta))
}

func (o *prometheusQueryObserver) ObserveLiteserverQuery(observation QueryObservation) {
	if o == nil || o.queryDuration == nil || o.queries == nil {
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
	errorCode := liteserverMetricsErrorCode(observation)
	errorReason := liteserverMetricsErrorReason(observation)
	duration := observation.Duration
	if duration < 0 {
		duration = 0
	}
	waitDuration := observation.WaitDuration
	if waitDuration < 0 {
		waitDuration = 0
	}

	totalDuration := duration + waitDuration
	o.queryDuration.WithLabelValues(method, response, errorCode, errorReason).Observe(totalDuration.Seconds())
	if o.queryHandler != nil {
		o.queryHandler.WithLabelValues(method, response, errorCode, errorReason).Observe(duration.Seconds())
	}
	o.queries.WithLabelValues(method, response, errorCode, errorReason).Inc()
	if waitDuration > 0 && o.queryWait != nil {
		o.queryWait.WithLabelValues(method, response, errorCode, errorReason).Observe(waitDuration.Seconds())
	}
}

func liteserverMetricsErrorCode(observation QueryObservation) string {
	if observation.ErrorCode != 0 {
		return strconv.FormatInt(int64(observation.ErrorCode), 10)
	}
	if observation.Error {
		return liteserverMetricsUnspecifiedErrorCode
	}
	return liteserverMetricsNoErrorCode
}

func liteserverMetricsErrorReason(observation QueryObservation) string {
	if !observation.Error {
		return liteserverMetricsNoErrorReason
	}

	reason := strings.TrimSpace(observation.ErrorReason)
	if reason == "" {
		return liteserverMetricsUnspecifiedReason
	}
	return reason
}
