package collator

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xssnick/tonutils-go/tvm"
)

type collatorMetricsTestRegistry struct {
	registry *prometheus.Registry
}

func (r collatorMetricsTestRegistry) Namespace() string {
	return "gton"
}

func (r collatorMetricsTestRegistry) RegisterCollector(collector prometheus.Collector) error {
	return r.registry.Register(collector)
}

func TestPrometheusCollationObserverExportsBoundedMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusMetrics(collatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer(MetricsModeStandalone)
	observer.AddCollationBuildInflight(MetricChainShardchain, 1)
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain: MetricChainShardchain, Result: CollationResultSuccess, Duration: 25 * time.Millisecond,
	})
	observer.ObserveCollationStage(CollationStageObservation{
		Chain: MetricChainShardchain, Stage: CollationStageCore, Duration: 20 * time.Millisecond,
	})
	observer.ObserveCollationCandidate(CandidateObservation{
		Chain: MetricChainShardchain,
		Kind:  CandidateKindBlock,
		Stats: Stats{
			Transactions:      7,
			ExternalIncluded:  2,
			InternalsImported: 3,
			GasUsed:           1000,
			OutQueueSize:      9,
			ExternalStop:      ExternalStopReadyDrained,
			Load:              LoadNormal,
		},
		BlockBytes: 1024, CollatedBytes: 512,
	})
	observer.ObserveCandidateProduction(CandidateProductionObservation{
		Chain: MetricChainShardchain, Kind: CandidateKindBlock,
		Result: CollationResultSuccess, Duration: 30 * time.Millisecond,
	})
	observer.ObserveCollationDeadline(
		MetricChainShardchain,
		CollationDeadlineSoft,
		DeadlineActionWait,
	)
	observer.ObserveCollationRetry(MetricChainShardchain, ProductionRetryNotReady)
	observer.AddCollationWindowInflight(MetricChainShardchain, 1)
	observer.ObserveCollationWindow(WindowObservation{
		Chain: MetricChainShardchain, Result: CollationResultSuccess, Duration: time.Second,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}
	for _, name := range []string{
		"gton_collator_builds_total",
		"gton_collator_build_duration_seconds",
		"gton_collator_stage_duration_seconds",
		"gton_collator_candidates_total",
		"gton_collator_candidate_production_duration_seconds",
		"gton_collator_deadline_events_total",
		"gton_collator_retries_total",
		"gton_collator_windows_total",
	} {
		if byName[name] == nil {
			t.Fatalf("metric family %s was not exported", name)
		}
	}
	if !metricFamilyHasLabels(byName["gton_collator_builds_total"], map[string]string{
		"mode": "standalone", "chain": "shardchain", "result": "success",
	}) {
		t.Fatal("successful standalone shardchain build labels were not exported")
	}
}

func metricFamilyHasLabels(family *dto.MetricFamily, labels map[string]string) bool {
	if family == nil {
		return false
	}
	for _, metric := range family.Metric {
		matched := 0
		for _, pair := range metric.Label {
			if labels[pair.GetName()] == pair.GetValue() {
				matched++
			}
		}
		if matched == len(labels) {
			return true
		}
	}
	return false
}

func BenchmarkCollationMetricsDisabledStageBoundary(b *testing.B) {
	service := &Service{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		started := service.metricStageStarted()
		service.observeMetricStage(MetricChainShardchain, CollationStageCore, started)
	}
}

func BenchmarkCollationMetricsEnabledObservation(b *testing.B) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusMetrics(collatorMetricsTestRegistry{registry: registry})
	if err != nil {
		b.Fatal(err)
	}
	observer := metrics.Observer(MetricsModeValidator)
	observation := CollationStageObservation{
		Chain: MetricChainShardchain, Stage: CollationStageCore, Duration: 10 * time.Millisecond,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observer.ObserveCollationStage(observation)
	}
}

// The origin label is what makes the candidate-shape metrics comparable: the
// same series read one way is what this node builds and the other way is what
// its peers build. That only holds if a validation observation reaches the
// shared histograms and stays out of the producer-only ones, which have no
// meaning for a block someone else made.
func TestPrometheusObserverSeparatesCollatedAndValidatedCandidates(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusMetrics(collatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer(MetricsModeValidator)

	stats := Stats{Transactions: 7, ExternalIncluded: 2, InternalsImported: 3, GasUsed: 1000}
	observer.ObserveCollationCandidate(CandidateObservation{
		Chain:      MetricChainShardchain,
		Origin:     CandidateOriginCollation,
		Kind:       CandidateKindBlock,
		BlockBytes: 1024, CollatedBytes: 512,
		Shape: stats.CollationShape(),
		Stats: stats,
	})
	observer.ObserveCollationCandidate(CandidateObservation{
		Chain:      MetricChainShardchain,
		Origin:     CandidateOriginValidation,
		Kind:       CandidateKindBlock,
		BlockBytes: 4096, CollatedBytes: 2048,
		Shape: CandidateShape{Transactions: 40, ExternalMessages: 11, InternalMessages: 19},
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}

	for _, want := range []struct {
		family string
		labels map[string]string
		sum    float64
	}{
		{"gton_collator_candidate_transactions", map[string]string{"chain": "shardchain", "origin": "collation"}, 7},
		{"gton_collator_candidate_transactions", map[string]string{"chain": "shardchain", "origin": "validation"}, 40},
		{"gton_collator_candidate_messages", map[string]string{"chain": "shardchain", "origin": "collation", "kind": "external"}, 2},
		{"gton_collator_candidate_messages", map[string]string{"chain": "shardchain", "origin": "collation", "kind": "internal"}, 3},
		{"gton_collator_candidate_messages", map[string]string{"chain": "shardchain", "origin": "validation", "kind": "external"}, 11},
		{"gton_collator_candidate_messages", map[string]string{"chain": "shardchain", "origin": "validation", "kind": "internal"}, 19},
		{"gton_collator_candidate_size_bytes", map[string]string{"chain": "shardchain", "origin": "collation", "part": "block"}, 1024},
		{"gton_collator_candidate_size_bytes", map[string]string{"chain": "shardchain", "origin": "validation", "part": "block"}, 4096},
		{"gton_collator_candidate_size_bytes", map[string]string{"chain": "shardchain", "origin": "validation", "part": "collated"}, 2048},
	} {
		sum, found := histogramSumWithLabels(byName[want.family], want.labels)
		if !found {
			t.Fatalf("%s has no series for %v", want.family, want.labels)
		}
		if sum != want.sum {
			t.Fatalf("%s%v observed %v, want %v", want.family, want.labels, sum, want.sum)
		}
	}

	// Gas is metered by the producer and cannot be recovered from a block, and
	// the candidate counter measures this node's own output. A validated block
	// must appear in neither.
	if _, found := histogramSumWithLabels(byName["gton_collator_candidate_gas_used"], map[string]string{
		"chain": "shardchain",
	}); !found {
		t.Fatal("collated candidate did not record gas")
	}
	var gasSamples uint64
	for _, metric := range byName["gton_collator_candidate_gas_used"].GetMetric() {
		gasSamples += metric.GetHistogram().GetSampleCount()
	}
	if gasSamples != 1 {
		t.Fatalf("gas has %d samples, want only the one candidate this node built", gasSamples)
	}
	var candidates float64
	for _, metric := range byName["gton_collator_candidates_total"].GetMetric() {
		candidates += metric.GetCounter().GetValue()
	}
	if candidates != 1 {
		t.Fatalf("candidates_total counted %v candidates, want only the one this node produced", candidates)
	}
}

func histogramSumWithLabels(family *dto.MetricFamily, labels map[string]string) (float64, bool) {
	if family == nil {
		return 0, false
	}
	for _, metric := range family.Metric {
		matched := 0
		for _, pair := range metric.Label {
			if value, ok := labels[pair.GetName()]; ok && value == pair.GetValue() {
				matched++
			}
		}
		if matched == len(labels) {
			return metric.GetHistogram().GetSampleSum(), true
		}
	}
	return 0, false
}

// The origin split is only worth charting if both sides count the same thing.
// Collation reports its own counters; validation recovers them from the block.
// This drives one real candidate through both and requires the two to agree
// exactly — the mapping from Stats to descriptor tags is the part that could
// silently drift, and a chart comparing two different quantities would read as
// a network anomaly rather than a bug.
func TestValidationRecoversTheShapeCollationReported(t *testing.T) {
	req, _ := benchMainnetRequest(t, benchMainnetFiller)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate: %v", err)
	}
	if candidate.Stats.Transactions == 0 ||
		candidate.Stats.ExternalIncluded == 0 ||
		candidate.Stats.InternalsImported == 0 {
		t.Fatalf("fixture exercises none of the three counters: %+v", candidate.Stats)
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verified, err := verifyCandidate(context.Background(), verification.Masterchain.Config, verification.Candidate)
	if err != nil {
		t.Fatalf("decode candidate: %v", err)
	}
	if err = verifyPreparedShardCandidate(context.Background(), verification, &verified); err != nil {
		t.Fatalf("verify candidate: %v", err)
	}

	if got, want := verified.shape, candidate.Stats.CollationShape(); got != want {
		t.Fatalf("validation recovered %+v, collation reported %+v", got, want)
	}
}
