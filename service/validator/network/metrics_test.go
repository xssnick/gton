package network

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xssnick/gton/service/validator/collator"
)

type candidateTransportMetricsTestRegistry struct {
	registry *prometheus.Registry
}

func (r candidateTransportMetricsTestRegistry) Namespace() string {
	return "gton"
}

func (r candidateTransportMetricsTestRegistry) RegisterCollector(collector prometheus.Collector) error {
	return r.registry.Register(collector)
}

func TestPrometheusCandidateTransportMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusCandidateTransportMetrics(candidateTransportMetricsTestRegistry{
		registry: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer()
	observer.AddCandidateOutboundQueue(collator.MetricChainShardchain, 1)
	observer.ObserveCandidateOutboundQueueAge(collator.MetricChainShardchain, 12*time.Millisecond)
	observer.AddCandidateOutboundDrop(
		collator.MetricChainShardchain,
		CandidateOutboundDropQueueFull,
	)
	observer.ObserveCandidateTransportSend(CandidateTransportSendObservation{
		Chain:    collator.MetricChainMasterchain,
		Result:   CandidateTransportSendPartial,
		Duration: 500 * time.Millisecond,
	})
	observer.AddCandidateTransportPeerSends(
		collator.MetricChainMasterchain,
		CandidateTransportPeerSent,
		14,
	)
	observer.AddCandidateTransportPeerSends(
		collator.MetricChainMasterchain,
		CandidateTransportPeerOffline,
		1,
	)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}

	if got := candidateTransportMetric(
		byName["gton_validator_candidate_outbound_queue_items"],
		map[string]string{"chain": "shardchain"},
	).GetGauge().GetValue(); got != 1 {
		t.Fatalf("shard candidate queue items = %v, want 1", got)
	}
	if got := candidateTransportMetric(
		byName["gton_validator_candidate_outbound_queue_age_seconds"],
		map[string]string{"chain": "shardchain"},
	).GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("shard candidate queue age samples = %d, want 1", got)
	}
	if got := candidateTransportMetric(
		byName["gton_validator_candidate_outbound_dropped_total"],
		map[string]string{"chain": "shardchain", "reason": "queue_full"},
	).GetCounter().GetValue(); got != 1 {
		t.Fatalf("full shard candidate queue drops = %v, want 1", got)
	}
	if got := candidateTransportMetric(
		byName["gton_validator_candidate_transport_send_duration_seconds"],
		map[string]string{"chain": "masterchain", "result": "partial"},
	).GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("master candidate partial fan-out samples = %d, want 1", got)
	}
	if got := candidateTransportMetric(
		byName["gton_validator_candidate_transport_peer_sends_total"],
		map[string]string{"chain": "masterchain", "outcome": "sent"},
	).GetCounter().GetValue(); got != 14 {
		t.Fatalf("delivered candidate recipients = %v, want 14", got)
	}
	if got := candidateTransportMetric(
		byName["gton_validator_candidate_transport_peer_sends_total"],
		map[string]string{"chain": "masterchain", "outcome": "peer_offline"},
	).GetCounter().GetValue(); got != 1 {
		t.Fatalf("offline candidate recipients = %v, want 1", got)
	}
}

func candidateTransportMetric(
	family *dto.MetricFamily,
	labels map[string]string,
) *dto.Metric {
	if family == nil {
		return &dto.Metric{}
	}
	for _, metric := range family.Metric {
		matched := true
		for label, want := range labels {
			found := false
			for _, pair := range metric.Label {
				if pair.GetName() == label && pair.GetValue() == want {
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			return metric
		}
	}

	return &dto.Metric{}
}
