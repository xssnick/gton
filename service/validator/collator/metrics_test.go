package collator

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xssnick/gton/service/validator/blockstats"
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
	blockStats := blockstats.New()
	observer := NewBlockStatsObserver(blockStats, metrics.Observer(MetricsModeStandalone))
	observer.AddCollationBuildInflight(MetricChainShardchain, 1)
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain: MetricChainShardchain, Result: CollationResultSuccess, Duration: 25 * time.Millisecond,
	})
	observer.ObserveCollationStage(CollationStageObservation{
		Chain: MetricChainShardchain, Stage: CollationStagePrepareState, Duration: 20 * time.Millisecond,
	})
	observer.ObserveCollationStage(CollationStageObservation{
		Chain: MetricChainShardchain, Stage: CollationStageWaitExternalMessages, Duration: 5 * time.Millisecond,
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
	// Both producer alarms, because both are conditions no other series carries:
	// a short collated proof rides on a SUCCESSFUL build and a mandatory-dequeue
	// overflow is one error among all the reasons a build can fail.
	observer.ObserveCollationAlarm(MetricChainShardchain, CollationAlarmShortCollatedProof)
	observer.ObserveCollationAlarm(MetricChainShardchain, CollationAlarmMandatoryDequeueOverflow)
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
		"gton_collator_alarms_total",
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
	for _, alarm := range []string{"short_collated_proof", "mandatory_dequeue_overflow"} {
		if !metricFamilyHasLabels(byName["gton_collator_alarms_total"], map[string]string{
			"mode": "standalone", "chain": "shardchain", "alarm": alarm,
		}) {
			t.Fatalf("alarm %s was not exported; a node shipping short proofs or losing every slot "+
				"would be invisible on :8085", alarm)
		}
	}
	for _, stage := range []string{
		"prepare_state",
		"cleanup_out_queue",
		"execute_internal_messages",
		"execute_external_messages",
		"finalize_accounts",
		"build_state_update",
		"serialize_candidate",
		"finalize_candidate",
	} {
		if !metricFamilyHasLabels(byName["gton_collator_stage_duration_seconds"], map[string]string{
			"mode": "standalone", "chain": "shardchain", "stage": stage,
		}) {
			t.Fatalf("candidate assembly stage %q was not exported", stage)
		}
	}
	if !metricFamilyHasLabels(byName["gton_collator_stage_duration_seconds"], map[string]string{
		"mode": "standalone", "chain": "shardchain", "stage": "wait_external_messages",
	}) {
		t.Fatal("external-message wait stage was not exported")
	}
	if byName["gton_collator_external_wait_duration_seconds"] != nil {
		t.Fatal("external wait was exported outside the exclusive stage histogram")
	}
	if got := blockStats.BlockStats().Collated.Shard.OK; got != 1 {
		t.Fatalf("shard collation blocks = %d, want 1", got)
	}
}

func TestBlockStatsCollationObserverCountsTerminalBuilds(t *testing.T) {
	stats := blockstats.New()
	observer := NewBlockStatsObserver(stats, nil)

	observer.ObserveCollationRetry(MetricChainMasterchain, ProductionRetryNotReady)
	observer.ObserveCandidateProduction(CandidateProductionObservation{
		Chain: MetricChainMasterchain, Kind: CandidateKindBlock, Result: CollationResultSuccess,
	})
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain: MetricChainMasterchain, Result: CollationResultSuccess,
	})
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain: MetricChainShardchain, Result: CollationResultError,
	})
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain: MetricChainShardchain, Result: CollationResultCanceled,
	})
	observer.ObserveCollationBuild(CollationBuildObservation{
		Chain: MetricChainShardchain, Result: CollationResultDeadline,
	})

	got := stats.BlockStats()
	if got.Collated.Master.OK != 1 || got.Collated.Master.Error != 0 ||
		got.Collated.Shard.OK != 0 || got.Collated.Shard.Error != 3 {
		t.Fatalf("collation counters = %#v", got.Collated)
	}
}

func TestCandidateAssemblyMetricsExportExclusiveBreakdown(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusMetrics(collatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	acquisition := LocalAcquisition{collationObserver: metrics.Observer(MetricsModeValidator)}
	var durations candidateAssemblyDurations
	want := []struct {
		stage CollationStage
		label string
		value time.Duration
	}{
		{CollationStagePrepareState, "prepare_state", 10 * time.Millisecond},
		{CollationStageCleanupOutQueue, "cleanup_out_queue", 20 * time.Millisecond},
		{CollationStageExecuteInternalMessages, "execute_internal_messages", 30 * time.Millisecond},
		{CollationStageExecuteExternalMessages, "execute_external_messages", 40 * time.Millisecond},
		{CollationStageFinalizeAccounts, "finalize_accounts", 50 * time.Millisecond},
		{CollationStageBuildStateUpdate, "build_state_update", 60 * time.Millisecond},
		{CollationStageSerializeCandidate, "serialize_candidate", 70 * time.Millisecond},
		{CollationStageFinalizeCandidate, "finalize_candidate", 80 * time.Millisecond},
		{CollationStageWaitExternalMessages, "wait_external_messages", 90 * time.Millisecond},
	}
	for _, stage := range want[:len(want)-1] {
		durations.stages[stage.stage] = stage.value
		durations.entered[stage.stage] = true
	}
	acquisition.observeCandidateAssembly(MetricChainShardchain, &durations, want[len(want)-1].value)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}

	for _, stage := range want {
		sum, found := histogramSumWithLabels(
			byName["gton_collator_stage_duration_seconds"],
			map[string]string{"mode": "validator", "chain": "shardchain", "stage": stage.label},
		)
		if !found {
			t.Fatalf("stage %q was not exported", stage.label)
		}
		if wantSeconds := stage.value.Seconds(); sum != wantSeconds {
			t.Fatalf("stage %q duration = %v seconds, want %v", stage.label, sum, wantSeconds)
		}
	}
	if metricFamilyHasLabels(byName["gton_collator_stage_duration_seconds"], map[string]string{
		"mode": "validator", "chain": "shardchain", "stage": "assemble_candidate",
	}) {
		t.Fatal("removed aggregate candidate assembly stage was still exported")
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
		service.observeMetricStage(MetricChainShardchain, CollationStagePrepareState, started)
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
		Chain: MetricChainShardchain, Stage: CollationStagePrepareState, Duration: 10 * time.Millisecond,
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
	} {
		sum, found := histogramSumWithLabels(byName[want.family], want.labels)
		if !found {
			t.Fatalf("%s has no series for %v", want.family, want.labels)
		}
		if sum != want.sum {
			t.Fatalf("%s%v observed %v, want %v", want.family, want.labels, sum, want.sum)
		}
	}
	for _, want := range []struct {
		family string
		labels map[string]string
		value  float64
	}{
		// The payload size is a gauge now, so it reports the last candidate of
		// its own (origin, part) rather than a bucketed distribution. Each
		// series here is written once, which keeps the expected values the same
		// as when this was a histogram sum and makes the change visible as a
		// change of kind rather than of meaning.
		{"gton_collator_candidate_size_bytes", map[string]string{"chain": "shardchain", "origin": "collation", "part": "block"}, 1024},
		{"gton_collator_candidate_size_bytes", map[string]string{"chain": "shardchain", "origin": "validation", "part": "block"}, 4096},
		{"gton_collator_candidate_size_bytes", map[string]string{"chain": "shardchain", "origin": "validation", "part": "collated"}, 2048},
		{"gton_collator_candidate_latest_transactions", map[string]string{"chain": "shardchain"}, 7},
		{"gton_collator_candidate_latest_messages", map[string]string{"chain": "shardchain", "kind": "external"}, 2},
		{"gton_collator_candidate_latest_messages", map[string]string{"chain": "shardchain", "kind": "internal"}, 3},
	} {
		value, found := gaugeValueWithLabels(byName[want.family], want.labels)
		if !found {
			t.Fatalf("%s has no series for %v", want.family, want.labels)
		}
		if value != want.value {
			t.Fatalf("%s%v = %v, want %v; validation must not overwrite the latest produced candidate", want.family, want.labels, value, want.value)
		}
	}

	// The cumulative counter beside the gauge is what replaced the histogram's
	// _sum, and it is the series a dashboard divides by the message count to
	// compare one producer's bytes per message against another's. A gauge alone
	// cannot answer that, so it is asserted here rather than left to drift.
	for _, want := range []struct {
		family string
		labels map[string]string
		value  float64
	}{
		{"gton_collator_candidate_size_bytes_total", map[string]string{"chain": "shardchain", "origin": "collation", "part": "block"}, 1024},
		{"gton_collator_candidate_size_bytes_total", map[string]string{"chain": "shardchain", "origin": "validation", "part": "block"}, 4096},
		{"gton_collator_candidate_size_bytes_total", map[string]string{"chain": "shardchain", "origin": "validation", "part": "collated"}, 2048},
	} {
		value, found := counterValueWithLabels(byName[want.family], want.labels)
		if !found {
			t.Fatalf("%s has no series for %v", want.family, want.labels)
		}
		if value != want.value {
			t.Fatalf("%s%v = %v, want %v", want.family, want.labels, value, want.value)
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

	observer.ObserveCollationCandidate(CandidateObservation{
		Chain:  MetricChainShardchain,
		Origin: CandidateOriginCollation,
		Kind:   CandidateKindEmpty,
	})
	families, err = registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		byName[family.GetName()] = family
	}
	for _, metric := range []struct {
		family string
		labels map[string]string
	}{
		{"gton_collator_candidate_latest_transactions", map[string]string{"chain": "shardchain"}},
		{"gton_collator_candidate_latest_messages", map[string]string{"chain": "shardchain", "kind": "external"}},
		{"gton_collator_candidate_latest_messages", map[string]string{"chain": "shardchain", "kind": "internal"}},
	} {
		value, found := gaugeValueWithLabels(byName[metric.family], metric.labels)
		if !found || value != 0 {
			t.Fatalf("%s%v = %v, found %v; empty candidate must reset it", metric.family, metric.labels, value, found)
		}
	}
}

func counterValueWithLabels(family *dto.MetricFamily, labels map[string]string) (float64, bool) {
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
			return metric.GetCounter().GetValue(), true
		}
	}

	return 0, false
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

func gaugeValueWithLabels(family *dto.MetricFamily, labels map[string]string) (float64, bool) {
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
			return metric.GetGauge().GetValue(), true
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
	prepared, err := prepareVerificationCandidate(
		context.Background(),
		verification.Masterchain.Config,
		verification.Candidate,
		[]PreviousBlock{verification.Previous},
	)
	if err != nil {
		t.Fatalf("decode candidate: %v", err)
	}
	if err = verifyPreparedShardCandidate(context.Background(), verification, prepared); err != nil {
		t.Fatalf("verify candidate: %v", err)
	}

	if got, want := prepared.verified.shape, candidate.Stats.CollationShape(); got != want {
		t.Fatalf("validation recovered %+v, collation reported %+v", got, want)
	}
}

// A substage is a span nested inside a stage and must not enter the stage
// histogram, whose per-stage sum is the build; it lands in its own family,
// keyed by the stage it sits in.
func TestPrometheusObserverRoutesSubstagesBesideTheStage(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusMetrics(collatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer(MetricsModeValidator)
	observer.ObserveCollationStage(CollationStageObservation{
		Chain: MetricChainShardchain, Stage: CollationStageAcquireInputs, Duration: 40 * time.Millisecond,
	})
	observer.ObserveCollationStage(CollationStageObservation{
		Chain: MetricChainShardchain, Stage: CollationStageAcquireInputs, Duration: 30 * time.Millisecond,
		Substage: "seed_source_from_state",
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}
	stageSum, found := histogramSumWithLabels(byName["gton_collator_stage_duration_seconds"],
		map[string]string{"chain": "shardchain", "stage": "acquire_inputs"})
	if !found || stageSum != 0.04 {
		t.Fatalf("stage histogram sum = %v (found %t), want exactly the stage's own 0.04 — the substage must not add to it", stageSum, found)
	}
	subSum, found := histogramSumWithLabels(byName["gton_collator_substage_duration_seconds"],
		map[string]string{"chain": "shardchain", "stage": "acquire_inputs", "substage": "seed_source_from_state"})
	if !found || subSum != 0.03 {
		t.Fatalf("substage histogram sum = %v (found %t), want 0.03", subSum, found)
	}
}

// Every schedule event must have a label bound in the observer. The array this
// guards used to be filled from a positional string literal while its bound came
// from scheduleEventCount, so adding an event without extending the literal left
// a nil prometheus.Observer in the last slot — and ObserveScheduleLateness
// dereferences it unconditionally, on the producer goroutine, at the first
// sample. A nil-map panic there is a lost leader window, not a missing series.
func TestScheduleLatenessObserverIsPopulatedForEveryEvent(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusMetrics(collatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer(MetricsModeStandalone)
	for _, chain := range []MetricChain{MetricChainShardchain, MetricChainMasterchain} {
		for event := ScheduleEvent(0); event < scheduleEventCount; event++ {
			if event.String() == "unknown" {
				t.Errorf("event %d has no label of its own", event)
			}
			// Panics if the slot was never filled.
			observer.ObserveScheduleLateness(chain, event, time.Millisecond)
		}
	}
}
