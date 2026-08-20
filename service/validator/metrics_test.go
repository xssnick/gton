package validator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xssnick/gton/service/validator/blockstats"
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

func TestPrometheusValidationObserverExportsRuntimeAndSemanticMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	blockStats := blockstats.New()
	observer := NewBlockStatsObserver(blockStats, metrics.Observer())
	metricContext := ValidationContext{
		Chain: collator.MetricChainMasterchain, Kind: ValidationKindBlock, Origin: ValidationOriginRemote,
	}
	observer.AddValidationInflight(metricContext, 1)
	observer.ObserveValidationTask(ValidationTaskObservation{
		Chain: collator.MetricChainMasterchain, Result: ValidationTaskSuccess, Duration: 15 * time.Millisecond,
	})
	observer.AddCandidateRetentionCapped(collator.MetricChainMasterchain)
	observer.AddCandidatePersistFailure(collator.MetricChainMasterchain)
	observer.AddSelfRejectedCandidate(collator.MetricChainMasterchain)
	observer.AddCapturedFailedCandidate(collator.MetricChainShardchain)
	observer.AddStorageStatRecompute(collator.MetricChainShardchain)
	observer.ObserveValidationCandidateSize(collator.MetricChainMasterchain, 1024, 256)
	observer.ObserveSessionSpecRejection(SessionSpecRejectionObservation{
		Chain: collator.MetricChainMasterchain, Role: SessionSpecRoleValidator,
		Reason: SessionSpecRejectionUnsupportedSimplexProtocol,
	})
	observer.ObserveValidationCoreStage(collator.ValidationCoreStageObservation{
		Chain: collator.MetricChainMasterchain, Stage: collator.ValidationCoreStageTransition,
		Duration: 10 * time.Millisecond,
	})
	observer.ObserveValidationCoreStage(collator.ValidationCoreStageObservation{
		Chain: collator.MetricChainMasterchain, Stage: collator.ValidationCoreStageWaitInputs,
		Duration: 5 * time.Millisecond,
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
		"gton_validator_validation_semantic_stage_duration_seconds",
		"gton_validator_validation_task_duration_seconds",
		"gton_validator_candidate_size_bytes",
		"gton_validator_candidate_retention_capped_total",
		"gton_validator_candidate_persist_failures_total",
		"gton_validator_self_rejected_candidates_total",
		"gton_validator_captured_failed_candidates_total",
		"gton_validator_storage_stat_recomputes_total",
		"gton_validator_session_spec_rejections_total",
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
	if !validatorMetricFamilyHasLabels(
		byName["gton_validator_validation_semantic_stage_duration_seconds"],
		map[string]string{"chain": "masterchain", "stage": "verify_transition"},
	) {
		t.Fatal("semantic transition stage was not exported")
	}
	if !validatorMetricFamilyHasLabels(
		byName["gton_validator_validation_semantic_stage_duration_seconds"],
		map[string]string{"chain": "masterchain", "stage": "wait_inputs"},
	) {
		t.Fatal("event-driven input wait stage was not exported")
	}
	if validatorMetricFamilyHasLabels(byName["gton_validator_validation_stage_duration_seconds"], map[string]string{
		"chain": "masterchain", "stage": "verify_transition",
	}) {
		t.Fatal("nested semantic stage was exported in the runtime stage histogram")
	}
	if got := validatorCounterValue(byName["gton_validator_session_spec_rejections_total"], map[string]string{
		"chain": "masterchain", "role": "validator", "reason": "unsupported_simplex_protocol",
	}); got != 1 {
		t.Fatalf("unsupported validator session specification rejections = %v, want 1", got)
	}
	if got := validatorCounterValue(byName["gton_validator_candidate_retention_capped_total"], map[string]string{
		"chain": "masterchain",
	}); got != 1 {
		t.Fatalf("capped candidate retention sweeps = %v, want 1", got)
	}
	if got := validatorCounterValue(byName["gton_validator_candidate_persist_failures_total"], map[string]string{
		"chain": "masterchain",
	}); got != 1 {
		t.Fatalf("failed durable candidate writes = %v, want 1", got)
	}
	if got := validatorCounterValue(byName["gton_validator_self_rejected_candidates_total"], map[string]string{
		"chain": "masterchain",
	}); got != 1 {
		t.Fatalf("self-rejected candidates = %v, want 1", got)
	}
	if got := blockStats.BlockStats().Validated.Master.OK; got != 1 {
		t.Fatalf("masterchain validated blocks = %d, want 1", got)
	}
}

func TestBlockStatsValidationObserverCountsOnlyTerminalBlockValidations(t *testing.T) {
	stats := blockstats.New()
	observer := NewBlockStatsObserver(stats, nil)

	observer.ObserveValidationTask(ValidationTaskObservation{
		Chain: collator.MetricChainMasterchain, Result: ValidationTaskNotReady,
	})
	observer.ObserveValidation(ValidationObservation{
		ValidationContext: ValidationContext{
			Chain: collator.MetricChainMasterchain,
			Kind:  ValidationKindBlock,
		},
		Result: ValidationResultSuccess,
	})
	observer.ObserveValidation(ValidationObservation{
		ValidationContext: ValidationContext{
			Chain: collator.MetricChainMasterchain,
			Kind:  ValidationKindBlock,
		},
		Result: ValidationResultRejected,
	})
	observer.ObserveValidation(ValidationObservation{
		ValidationContext: ValidationContext{
			Chain: collator.MetricChainShardchain,
			Kind:  ValidationKindBlock,
		},
		Result: ValidationResultSuccess,
	})
	for _, result := range []ValidationResult{
		ValidationResultRejected,
		ValidationResultCanceled,
		ValidationResultDeadline,
		ValidationResultError,
	} {
		observer.ObserveValidation(ValidationObservation{
			ValidationContext: ValidationContext{
				Chain: collator.MetricChainShardchain,
				Kind:  ValidationKindBlock,
			},
			Result: result,
		})
	}
	observer.ObserveValidation(ValidationObservation{
		ValidationContext: ValidationContext{
			Chain: collator.MetricChainShardchain,
			Kind:  ValidationKindEmpty,
		},
		Result: ValidationResultSuccess,
	})

	got := stats.BlockStats()
	if got.Validated.Master.OK != 1 || got.Validated.Master.Error != 1 ||
		got.Validated.Shard.OK != 1 || got.Validated.Shard.Error != 4 {
		t.Fatalf("validation counters = %#v", got.Validated)
	}
}

func TestSessionSpecRejectionCounterCountsTransitions(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	supervisor := &sessionSupervisor{metrics: metrics.Observer()}
	failure := sessionSpecError{
		id: supervisorTestID(0xa1), chain: collator.MetricChainShardchain,
		role: SessionSpecRoleValidator, reason: SessionSpecRejectionUnsupportedSimplexProtocol,
	}

	rejections := supervisor.observeSessionSpecRejections(nil, []sessionSpecError{failure})
	rejections = supervisor.observeSessionSpecRejections(rejections, []sessionSpecError{failure})
	rejections = supervisor.observeSessionSpecRejections(rejections, nil)
	supervisor.observeSessionSpecRejections(rejections, []sessionSpecError{failure})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "gton_validator_session_spec_rejections_total" {
			continue
		}
		if got := validatorCounterValue(family, map[string]string{
			"chain": "shardchain", "role": "validator", "reason": "unsupported_simplex_protocol",
		}); got != 2 {
			t.Fatalf("session specification rejection transitions = %v, want 2", got)
		}
		return
	}
	t.Fatal("session specification rejection metric was not exported")
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
		_ context.Context,
		state *ChainState,
		artifact *CandidateArtifact,
	) (CandidateValidation, error) {
		return CandidateValidation{
			ValidAfter: time.Now().Add(time.Millisecond),
			State:      testValidatedCandidateState(t, state, artifact),
		}, nil
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
		map[string]string{"chain": "shardchain", "stage": "semantic_validation"},
	); count != 1 {
		t.Fatalf("semantic validation stage samples = %d, want 1", count)
	}
}

func TestSessionRuntimeExportsCandidateCacheGauges(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	storage := newRuntimeTestStorage()
	runtime, privateKey := prepareRuntimeTest(
		t,
		0x6b,
		storage,
		newRuntimeTestNetwork(),
		newRuntimeTestBackend(),
	)
	runtime.metrics = metrics.Observer()

	slot := uint32(4 * candidateCacheRetainedSlots)
	superseded := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
	if err = runtime.candidates.stage(superseded, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err = runtime.candidates.store(context.Background(), superseded.Candidate.ID); err != nil {
		t.Fatal(err)
	}
	current := runtimeOrdinaryArtifact(t, runtime.config, privateKey, slot, simplex.Genesis())
	if err = runtime.candidates.stage(current, []byte{0x02}); err != nil {
		t.Fatal(err)
	}
	runtime.publishCandidateCache(runtime.candidates.notifyFinalized(slot, retentionFloorNone))

	stats := runtime.candidates.cacheStats()
	if stats.Entries != 2 || stats.Candidates != 1 {
		t.Fatalf("candidate cache projection = %+v", stats)
	}
	entries := map[string]float64{"retained": 1, "released": 1}
	for state, want := range entries {
		if got := validatorGaugeValue(
			validatorMetricFamily(t, registry, "gton_validator_candidate_cache_entries"),
			map[string]string{"chain": "shardchain", "state": state},
		); got != want {
			t.Fatalf("candidate cache %s entries = %v, want %v", state, got, want)
		}
	}
	if got := validatorGaugeValue(
		validatorMetricFamily(t, registry, "gton_validator_candidate_cache_bytes"),
		map[string]string{"chain": "shardchain"},
	); got != float64(stats.Bytes) {
		t.Fatalf("candidate cache bytes = %v, want %d", got, stats.Bytes)
	}

	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"gton_validator_candidate_cache_entries",
		"gton_validator_candidate_cache_bytes",
	} {
		family := validatorMetricFamily(t, registry, name)
		for _, metric := range family.Metric {
			if metric.GetGauge().GetValue() != 0 {
				t.Fatalf("%s kept %v after the session was closed", name, metric.GetGauge().GetValue())
			}
		}
	}
}

func validatorMetricFamily(
	t *testing.T,
	registry *prometheus.Registry,
	name string,
) *dto.MetricFamily {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %s was not exported", name)

	return nil
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

func validatorGaugeValue(family *dto.MetricFamily, labels map[string]string) float64 {
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
			return metric.GetGauge().GetValue()
		}
	}
	return 0
}

func validatorCounterValue(family *dto.MetricFamily, labels map[string]string) float64 {
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
			return metric.GetCounter().GetValue()
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
			ValidationStageSemanticValidation,
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
		Chain: collator.MetricChainShardchain,
		Stage: ValidationStageSemanticValidation, Duration: 10 * time.Millisecond,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observer.ObserveValidationStage(observation)
	}
}

// TestObserverRuntimeReportsAFailedDurableCandidateWrite pins where a failed
// durable write of our own candidate goes on a runtime that has no voter.
//
// ConsensusObserver builds its runtime with ValidatorIndex = ObserverIndex, so
// Engine.SubmitCandidate returns before acceptCandidate and nothing ever re-runs
// the store. The detached write here is that runtime's only persistence of the
// candidate it just broadcast — the delegated and standalone collator route is
// exactly this shape — and the submit deliberately still succeeds, so without a
// counter the failure is invisible to everything but a log line.
func TestObserverRuntimeReportsAFailedDurableCandidateWrite(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	storage := newRuntimeTestStorage()
	storage.saveHook = func(_ SessionStorageID, _ CandidateRecord, done func(error)) {
		done(errors.New("no space left on device"))
	}
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	runtime, privateKey := prepareRuntimeTest(t, 0x6c, storage, network, backend)
	if runtime.config.Identity.Validator != nil {
		t.Fatal("the runtime under test is not the voterless one")
	}
	runtime.metrics = metrics.Observer()

	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(context.Background(), runtimeTestStart()) }()
	<-network.started
	var window LeaderWindow
	select {
	case window = <-backend.windows:
	case <-time.After(time.Second):
		t.Fatal("leader window was not delivered")
	}

	artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, window.Window.StartSlot, window.Window.Base)
	// The candidate is emitted, and the write that fails behind it is the only
	// durable copy this runtime will ever make of it.
	if err = window.Submit(context.Background(), artifact); err != nil {
		t.Fatalf("submit refused the candidate on a detached write failure: %v", err)
	}
	waitFor(t, func() bool {
		return validatorCounterValue(
			validatorMetricFamily(t, registry, "gton_validator_candidate_persist_failures_total"),
			map[string]string{"chain": "shardchain"},
		) == 1
	}, "the failed durable candidate write was not reported")

	if err = runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err = <-runResult; err != nil {
		t.Fatal(err)
	}
}
