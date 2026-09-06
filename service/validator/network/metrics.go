package network

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
)

// CandidateOutboundDropReason classifies candidates which were published on
// the public path but could not enter the private two-step sender queue.
type CandidateOutboundDropReason uint8

const (
	CandidateOutboundDropQueueFull CandidateOutboundDropReason = iota
	CandidateOutboundDropUnavailable
	candidateOutboundDropReasonCount
)

// CandidateTransportSendResult is the terminal state of one source-side
// two-step fan-out, measured by coverage rather than by the first error:
// two-step FEC reconstructs from (n-1)/2 parts, so a fan-out that lost a
// recipient is not a failed broadcast.
type CandidateTransportSendResult uint8

const (
	CandidateTransportSendSuccess CandidateTransportSendResult = iota
	CandidateTransportSendPartial
	CandidateTransportSendFailed
	candidateTransportSendResultCount
)

// CandidateTransportPeerOutcome is the per-recipient result inside one
// fan-out. The fault classes separate this node's own state - a connection it
// has not opened, a dial it deferred, a budget it imposed - from a fault the
// peer is answerable for.
type CandidateTransportPeerOutcome uint8

const (
	CandidateTransportPeerSent CandidateTransportPeerOutcome = iota
	CandidateTransportPeerOffline
	CandidateTransportPeerDialDeferred
	CandidateTransportPeerWriteDeadline
	CandidateTransportPeerContextDeadline
	CandidateTransportPeerCanceled
	CandidateTransportPeerError
	candidateTransportPeerOutcomeCount
)

type CandidateTransportSendObservation struct {
	Chain    collator.MetricChain
	Result   CandidateTransportSendResult
	Duration time.Duration
}

// CandidateTransportObserver exposes the bounded source queue and the actual
// two-step send budget. Queue age is sampled when an item leaves the queue for
// a send attempt; shutdown drains only correct the live queue-depth gauge.
type CandidateTransportObserver interface {
	AddCandidateOutboundQueue(collator.MetricChain, int)
	ObserveCandidateOutboundQueueAge(collator.MetricChain, time.Duration)
	AddCandidateOutboundDrop(collator.MetricChain, CandidateOutboundDropReason)
	ObserveCandidateTransportSend(CandidateTransportSendObservation)
	AddCandidateTransportPeerSends(collator.MetricChain, CandidateTransportPeerOutcome, int)
}

// PrometheusCandidateTransportMetrics owns the source-side candidate transport
// collectors. Observer returns a pre-bound hot-path view.
type PrometheusCandidateTransportMetrics struct {
	observer prometheusCandidateTransportObserver
}

func NewPrometheusCandidateTransportMetrics(
	registry validator.MetricsRegistry,
) (*PrometheusCandidateTransportMetrics, error) {
	if registry == nil {
		return nil, nil
	}

	durations := []float64{
		0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
		0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	}
	namespace := registry.Namespace()
	queueItems := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "validator",
		Name:      "candidate_outbound_queue_items",
		Help:      "Candidates waiting in source-side private two-step transport queues.",
	}, []string{"chain"})
	queueAge := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "validator",
		Name:      "candidate_outbound_queue_age_seconds",
		Help:      "Age of a candidate when it leaves the source-side private two-step transport queue.",
		Buckets:   durations,
	}, []string{"chain"})
	drops := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "validator",
		Name:      "candidate_outbound_dropped_total",
		Help:      "Candidates which could not enter the source-side private two-step transport queue.",
	}, []string{"chain", "reason"})
	sendDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "validator",
		Name:      "candidate_transport_send_duration_seconds",
		Help:      "Source-side private two-step candidate fan-out duration by terminal result.",
		Buckets:   durations,
	}, []string{"chain", "result"})
	peerSends := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "validator",
		Name:      "candidate_transport_peer_sends_total",
		Help:      "Source-side private two-step candidate parts by per-recipient outcome.",
	}, []string{"chain", "outcome"})

	metrics := &PrometheusCandidateTransportMetrics{}
	for chain := collator.MetricChain(0); chain < 2; chain++ {
		chainLabel := candidateTransportChainLabel(chain)
		metrics.observer.queueItems[chain] = queueItems.WithLabelValues(chainLabel)
		metrics.observer.queueAge[chain] = queueAge.WithLabelValues(chainLabel)
		for reason := CandidateOutboundDropReason(0); reason < candidateOutboundDropReasonCount; reason++ {
			metrics.observer.drops[chain][reason] = drops.WithLabelValues(
				chainLabel,
				candidateOutboundDropReasonLabel(reason),
			)
		}
		for result := CandidateTransportSendResult(0); result < candidateTransportSendResultCount; result++ {
			metrics.observer.sendDuration[chain][result] = sendDuration.WithLabelValues(
				chainLabel,
				candidateTransportSendResultLabel(result),
			)
		}
		for outcome := CandidateTransportPeerOutcome(0); outcome < candidateTransportPeerOutcomeCount; outcome++ {
			metrics.observer.peerSends[chain][outcome] = peerSends.WithLabelValues(
				chainLabel,
				candidateTransportPeerOutcomeLabel(outcome),
			)
		}
	}

	if err := registry.RegisterCollector(candidateTransportCollectorSet{
		queueItems,
		queueAge,
		drops,
		sendDuration,
		peerSends,
	}); err != nil {
		return nil, err
	}

	return metrics, nil
}

func (m *PrometheusCandidateTransportMetrics) Observer() CandidateTransportObserver {
	if m == nil {
		return nil
	}

	return &m.observer
}

type candidateTransportCollectorSet []prometheus.Collector

func (s candidateTransportCollectorSet) Describe(ch chan<- *prometheus.Desc) {
	for _, collector := range s {
		collector.Describe(ch)
	}
}

func (s candidateTransportCollectorSet) Collect(ch chan<- prometheus.Metric) {
	for _, collector := range s {
		collector.Collect(ch)
	}
}

type prometheusCandidateTransportObserver struct {
	queueItems   [2]prometheus.Gauge
	queueAge     [2]prometheus.Observer
	drops        [2][candidateOutboundDropReasonCount]prometheus.Counter
	sendDuration [2][candidateTransportSendResultCount]prometheus.Observer
	peerSends    [2][candidateTransportPeerOutcomeCount]prometheus.Counter
}

func (o *prometheusCandidateTransportObserver) AddCandidateOutboundQueue(
	chain collator.MetricChain,
	delta int,
) {
	o.queueItems[boundedCandidateTransportChain(chain)].Add(float64(delta))
}

func (o *prometheusCandidateTransportObserver) ObserveCandidateOutboundQueueAge(
	chain collator.MetricChain,
	age time.Duration,
) {
	o.queueAge[boundedCandidateTransportChain(chain)].Observe(nonNegativeCandidateTransportSeconds(age))
}

func (o *prometheusCandidateTransportObserver) AddCandidateOutboundDrop(
	chain collator.MetricChain,
	reason CandidateOutboundDropReason,
) {
	if reason >= candidateOutboundDropReasonCount {
		reason = CandidateOutboundDropUnavailable
	}
	o.drops[boundedCandidateTransportChain(chain)][reason].Inc()
}

func (o *prometheusCandidateTransportObserver) ObserveCandidateTransportSend(
	observation CandidateTransportSendObservation,
) {
	result := observation.Result
	if result >= candidateTransportSendResultCount {
		result = CandidateTransportSendFailed
	}
	o.sendDuration[boundedCandidateTransportChain(observation.Chain)][result].Observe(
		nonNegativeCandidateTransportSeconds(observation.Duration),
	)
}

func (o *prometheusCandidateTransportObserver) AddCandidateTransportPeerSends(
	chain collator.MetricChain,
	outcome CandidateTransportPeerOutcome,
	count int,
) {
	if count <= 0 {
		return
	}
	if outcome >= candidateTransportPeerOutcomeCount {
		outcome = CandidateTransportPeerError
	}
	o.peerSends[boundedCandidateTransportChain(chain)][outcome].Add(float64(count))
}

func boundedCandidateTransportChain(chain collator.MetricChain) collator.MetricChain {
	if chain != collator.MetricChainMasterchain {
		return collator.MetricChainShardchain
	}

	return chain
}

func candidateTransportChainLabel(chain collator.MetricChain) string {
	if chain == collator.MetricChainMasterchain {
		return "masterchain"
	}

	return "shardchain"
}

func candidateOutboundDropReasonLabel(reason CandidateOutboundDropReason) string {
	switch reason {
	case CandidateOutboundDropQueueFull:
		return "queue_full"
	default:
		return "unavailable"
	}
}

func candidateTransportSendResultLabel(result CandidateTransportSendResult) string {
	switch result {
	case CandidateTransportSendSuccess:
		return "success"
	case CandidateTransportSendPartial:
		return "partial"
	default:
		return "failed"
	}
}

func candidateTransportPeerOutcomeLabel(outcome CandidateTransportPeerOutcome) string {
	switch outcome {
	case CandidateTransportPeerSent:
		return "sent"
	case CandidateTransportPeerOffline:
		return "peer_offline"
	case CandidateTransportPeerDialDeferred:
		return "dial_deferred"
	case CandidateTransportPeerWriteDeadline:
		return "write_deadline"
	case CandidateTransportPeerContextDeadline:
		return "context_deadline"
	case CandidateTransportPeerCanceled:
		return "canceled"
	default:
		return "error"
	}
}

func nonNegativeCandidateTransportSeconds(duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}

	return duration.Seconds()
}
