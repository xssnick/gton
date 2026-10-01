package validator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xssnick/gton/service/validator/collator"
)

// The input wait is sent on every candidate validation and its series is bound
// up front, but a stage filter written before the stage existed dropped every
// observation, so the dashboards read the wait as empty.
func TestPrometheusValidationObserverRecordsInputWaitStage(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer()

	observer.ObserveValidationCoreStage(collator.ValidationCoreStageObservation{
		Chain: collator.MetricChainShardchain, Stage: collator.ValidationCoreStageWaitInputs,
		Duration: 5 * time.Millisecond,
	})
	observer.ObserveValidationCoreStage(collator.ValidationCoreStageObservation{
		Chain: collator.MetricChainShardchain, Stage: collator.ValidationCoreStageWaitInputs + 1,
		Duration: 5 * time.Millisecond,
	})

	byName := consensusTestFamilies(t, registry)
	if got := validatorHistogramCount(
		byName["gton_validator_validation_semantic_stage_duration_seconds"],
		map[string]string{"chain": "shardchain", "stage": "wait_inputs"},
	); got != 1 {
		t.Fatalf("input wait stage samples = %d, want 1", got)
	}
}
