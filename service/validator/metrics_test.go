package validator

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

type validatorMetricsTestRegistry struct {
	registry *prometheus.Registry
}

func (r validatorMetricsTestRegistry) Namespace() string {
	return "gton"
}

func (r validatorMetricsTestRegistry) RegisterCollector(collector prometheus.Collector) error {
	return r.registry.Register(collector)
}

func TestPrometheusValidationObserverExportsRuntimeAndCoreMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer()
	metricContext := ValidationContext{
		Chain: collator.MetricChainMasterchain, Kind: ValidationKindBlock, Origin: ValidationOriginRemote,
	}
	observer.AddValidationInflight(metricContext, 1)
	observer.ObserveValidationAttempt(ValidationAttemptObservation{
		Chain: collator.MetricChainMasterchain, Result: ValidationAttemptSuccess, Duration: 15 * time.Millisecond,
	})
	observer.ObserveValidationRetry(collator.MetricChainMasterchain, ValidationRetryStateNotReady)
	observer.ObserveValidationCandidateSize(collator.MetricChainMasterchain, 1024, 256)
	observer.ObserveValidationCoreStage(collator.ValidationCoreStageObservation{
		Chain: collator.MetricChainMasterchain, Stage: collator.ValidationCoreStageTransition,
		Duration: 10 * time.Millisecond,
	})
	observer.ObserveValidation(ValidationObservation{
		ValidationContext: metricContext,
		Result:            ValidationResultSuccess,
		DecisionDuration:  20 * time.Millisecond,
		ReadyDuration:     30 * time.Millisecond,
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
		"gton_validator_validations_total",
		"gton_validator_validations_inflight",
		"gton_validator_validation_decision_duration_seconds",
		"gton_validator_validation_ready_duration_seconds",
		"gton_validator_validation_stage_duration_seconds",
		"gton_validator_validation_attempt_duration_seconds",
		"gton_validator_validation_retries_total",
		"gton_validator_candidate_size_bytes",
	} {
		if byName[name] == nil {
			t.Fatalf("metric family %s was not exported", name)
		}
	}
	if !validatorMetricFamilyHasLabels(byName["gton_validator_validations_total"], map[string]string{
		"chain": "masterchain", "kind": "block", "origin": "remote", "result": "success",
	}) {
		t.Fatal("successful remote masterchain validation labels were not exported")
	}
	if !validatorMetricFamilyHasLabels(byName["gton_validator_validation_stage_duration_seconds"], map[string]string{
		"chain": "masterchain", "stage": "core_transition",
	}) {
		t.Fatal("validation core transition stage was not exported")
	}
}

func TestSessionRuntimeRecordsValidationTiming(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	storage := newRuntimeTestStorage()
	backend := newRuntimeTestBackend()
	backend.validation = func(
		context.Context,
		*ChainState,
		*CandidateArtifact,
	) (CandidateValidation, error) {
		return CandidateValidation{ValidAfter: time.Now().Add(time.Millisecond)}, nil
	}
	runtime, privateKey := prepareRuntimeTest(t, 0x6a, storage, newRuntimeTestNetwork(), backend)
	runtime.metrics = metrics.Observer()
	if err = runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err = runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err = runtime.validateCandidate(context.Background(), &artifact.Candidate); err != nil {
		t.Fatal(err)
	}
	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}
	labels := map[string]string{
		"chain": "shardchain", "kind": "block", "origin": "remote", "result": "success",
	}
	if count := validatorHistogramCount(
		byName["gton_validator_validation_ready_duration_seconds"],
		labels,
	); count != 1 {
		t.Fatalf("successful validation ready samples = %d, want 1", count)
	}
	if count := validatorHistogramCount(
		byName["gton_validator_validation_stage_duration_seconds"],
		map[string]string{"chain": "shardchain", "stage": "backend"},
	); count != 1 {
		t.Fatalf("backend validation stage samples = %d, want 1", count)
	}
}

func validatorMetricFamilyHasLabels(family *dto.MetricFamily, labels map[string]string) bool {
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

func validatorHistogramCount(family *dto.MetricFamily, labels map[string]string) uint64 {
	if family == nil {
		return 0
	}
	for _, metric := range family.Metric {
		matched := 0
		for _, pair := range metric.Label {
			if labels[pair.GetName()] == pair.GetValue() {
				matched++
			}
		}
		if matched == len(labels) {
			return metric.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func BenchmarkValidationMetricsDisabledStageBoundary(b *testing.B) {
	runtime := &sessionRuntime{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		started := runtime.validationStageStarted()
		runtime.observeValidationStage(
			collator.MetricChainShardchain,
			ValidationStageBackend,
			started,
		)
	}
}

func BenchmarkValidationMetricsEnabledObservation(b *testing.B) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		b.Fatal(err)
	}
	observer := metrics.Observer()
	observation := ValidationStageObservation{
		Chain: collator.MetricChainShardchain, Stage: ValidationStageBackend, Duration: 10 * time.Millisecond,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observer.ObserveValidationStage(observation)
	}
}
