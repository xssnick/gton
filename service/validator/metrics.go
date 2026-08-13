package validator

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

// MetricsRegistry is the process metrics capability required by the validator
// extension.
type MetricsRegistry interface {
	Namespace() string
	RegisterCollector(prometheus.Collector) error
}

type ValidationKind uint8

const (
	ValidationKindBlock ValidationKind = iota
	ValidationKindEmpty
	validationKindCount
)

type ValidationOrigin uint8

const (
	ValidationOriginLocal ValidationOrigin = iota
	ValidationOriginRemote
	validationOriginCount
)

type ValidationResult uint8

const (
	ValidationResultSuccess ValidationResult = iota
	ValidationResultRejected
	ValidationResultCanceled
	ValidationResultDeadline
	ValidationResultError
	validationResultCount
)

type ValidationAttemptResult uint8

const (
	ValidationAttemptSuccess ValidationAttemptResult = iota
	ValidationAttemptRejected
	ValidationAttemptNotReady
	ValidationAttemptCanceled
	ValidationAttemptDeadline
	ValidationAttemptError
	validationAttemptResultCount
)

type ValidationStage uint8

const (
	ValidationStageLoadCandidate ValidationStage = iota
	ValidationStageResolveParent
	ValidationStageMinBlockIntervalWait
	ValidationStageBackend
	ValidationStageRetryWait
	ValidationStageValidAfterWait
	ValidationStageCoreRestoreState
	ValidationStageCoreMasterView
	ValidationStageCoreChainInputs
	ValidationStageCoreDecode
	ValidationStageCoreTransition
	validationStageCount
)

type ValidationRetryReason uint8

const (
	ValidationRetryStateNotReady ValidationRetryReason = iota
	ValidationRetryAttemptDeadline
	validationRetryReasonCount
)

type ValidationContext struct {
	Chain  collator.MetricChain
	Kind   ValidationKind
	Origin ValidationOrigin
}

type ValidationObservation struct {
	ValidationContext
	Result           ValidationResult
	DecisionDuration time.Duration
	ReadyDuration    time.Duration
}

type ValidationStageObservation struct {
	Chain    collator.MetricChain
	Stage    ValidationStage
	Duration time.Duration
}

type ValidationAttemptObservation struct {
	Chain    collator.MetricChain
	Result   ValidationAttemptResult
	Duration time.Duration
}

// ValidationObserver uses bounded enums only. Its Prometheus implementation
// pre-binds every label, so the synchronous validation path does no map lookup
// or label construction.
type ValidationObserver interface {
	collator.ValidationCoreObserver
	AddValidationInflight(ValidationContext, int)
	ObserveValidation(ValidationObservation)
	ObserveValidationStage(ValidationStageObservation)
	ObserveValidationAttempt(ValidationAttemptObservation)
	ObserveValidationRetry(collator.MetricChain, ValidationRetryReason)
	ObserveValidationCandidateSize(collator.MetricChain, int, int)
}

// PrometheusValidationMetrics owns the validator metric vectors. Observer is
// a pre-bound hot-path view created once for the validator extension.
type PrometheusValidationMetrics struct {
	validations      *prometheus.CounterVec
	inflight         *prometheus.GaugeVec
	decisionDuration *prometheus.HistogramVec
	readyDuration    *prometheus.HistogramVec
	stageDuration    *prometheus.HistogramVec
	attemptDuration  *prometheus.HistogramVec
	retries          *prometheus.CounterVec
	candidateSize    *prometheus.HistogramVec
}

// NewPrometheusValidationMetrics registers the validator collectors as one
// atomic collector set.
func NewPrometheusValidationMetrics(registry MetricsRegistry) (*PrometheusValidationMetrics, error) {
	if registry == nil {
		return nil, nil
	}

	durations := []float64{
		0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
		0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	}
	namespace := registry.Namespace()
	metrics := &PrometheusValidationMetrics{
		validations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validations_total",
			Help: "Consensus candidate validations by chain, candidate kind, origin, and terminal result.",
		}, []string{"chain", "kind", "origin", "result"}),
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validations_inflight",
			Help: "Consensus candidate validations currently in progress.",
		}, []string{"chain", "kind", "origin"}),
		decisionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validation_decision_duration_seconds",
			Help: "Wall-clock duration until a candidate is accepted or rejected, before ValidAfter waiting.", Buckets: durations,
		}, []string{"chain", "kind", "origin", "result"}),
		readyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validation_ready_duration_seconds",
			Help: "End-to-end validation duration until the candidate is ready for a consensus vote.", Buckets: durations,
		}, []string{"chain", "kind", "origin", "result"}),
		stageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validation_stage_duration_seconds",
			Help: "Wall-clock duration of validation runtime and deterministic core stages.", Buckets: durations,
		}, []string{"chain", "stage"}),
		attemptDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validation_attempt_duration_seconds",
			Help: "Duration of one backend validation attempt by result.", Buckets: durations,
		}, []string{"chain", "result"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validation_retries_total",
			Help: "Backend validation retries caused by local state lag or an attempt deadline.",
		}, []string{"chain", "reason"}),
		candidateSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "candidate_size_bytes",
			Help:    "Block and collated-data payload sizes entering candidate validation.",
			Buckets: prometheus.ExponentialBuckets(1_024, 2, 15),
		}, []string{"chain", "part"}),
	}

	if err := registry.RegisterCollector(validationCollectorSet{
		metrics.validations, metrics.inflight, metrics.decisionDuration, metrics.readyDuration,
		metrics.stageDuration, metrics.attemptDuration, metrics.retries, metrics.candidateSize,
	}); err != nil {
		return nil, err
	}

	return metrics, nil
}

type validationCollectorSet []prometheus.Collector

func (s validationCollectorSet) Describe(ch chan<- *prometheus.Desc) {
	for _, collector := range s {
		collector.Describe(ch)
	}
}

func (s validationCollectorSet) Collect(ch chan<- prometheus.Metric) {
	for _, collector := range s {
		collector.Collect(ch)
	}
}

type prometheusValidationObserver struct {
	validations      [2][validationKindCount][validationOriginCount][validationResultCount]prometheus.Counter
	inflight         [2][validationKindCount][validationOriginCount]prometheus.Gauge
	decisionDuration [2][validationKindCount][validationOriginCount][validationResultCount]prometheus.Observer
	readyDuration    [2][validationKindCount][validationOriginCount][validationResultCount]prometheus.Observer
	stageDuration    [2][validationStageCount]prometheus.Observer
	attemptDuration  [2][validationAttemptResultCount]prometheus.Observer
	retries          [2][validationRetryReasonCount]prometheus.Counter
	candidateSize    [2][2]prometheus.Observer
}

// Observer binds all validator labels once, before session processing starts.
func (m *PrometheusValidationMetrics) Observer() ValidationObserver {
	if m == nil {
		return nil
	}

	o := &prometheusValidationObserver{}
	chains := [...]string{"masterchain", "shardchain"}
	kinds := [...]string{"block", "empty"}
	origins := [...]string{"local", "remote"}
	results := [...]string{"success", "rejected", "canceled", "deadline", "error"}
	stages := [...]string{
		"load_candidate", "resolve_parent", "min_block_interval_wait", "backend",
		"retry_wait", "valid_after_wait", "core_restore_state", "core_master_view",
		"core_chain_inputs", "core_decode", "core_transition",
	}
	attemptResults := [...]string{"success", "rejected", "not_ready", "canceled", "deadline", "error"}
	for chain := collator.MetricChain(0); chain < 2; chain++ {
		chainLabel := chains[chain]
		for kind := ValidationKind(0); kind < validationKindCount; kind++ {
			for origin := ValidationOrigin(0); origin < validationOriginCount; origin++ {
				o.inflight[chain][kind][origin] = m.inflight.WithLabelValues(
					chainLabel, kinds[kind], origins[origin],
				)
				for result := ValidationResult(0); result < validationResultCount; result++ {
					o.validations[chain][kind][origin][result] = m.validations.WithLabelValues(
						chainLabel, kinds[kind], origins[origin], results[result],
					)
					o.decisionDuration[chain][kind][origin][result] = m.decisionDuration.WithLabelValues(
						chainLabel, kinds[kind], origins[origin], results[result],
					)
					o.readyDuration[chain][kind][origin][result] = m.readyDuration.WithLabelValues(
						chainLabel, kinds[kind], origins[origin], results[result],
					)
				}
			}
		}
		for stage := ValidationStage(0); stage < validationStageCount; stage++ {
			o.stageDuration[chain][stage] = m.stageDuration.WithLabelValues(chainLabel, stages[stage])
		}
		for result := ValidationAttemptResult(0); result < validationAttemptResultCount; result++ {
			o.attemptDuration[chain][result] = m.attemptDuration.WithLabelValues(chainLabel, attemptResults[result])
		}
		o.retries[chain][ValidationRetryStateNotReady] = m.retries.WithLabelValues(chainLabel, "state_not_ready")
		o.retries[chain][ValidationRetryAttemptDeadline] = m.retries.WithLabelValues(chainLabel, "attempt_deadline")
		o.candidateSize[chain][0] = m.candidateSize.WithLabelValues(chainLabel, "block")
		o.candidateSize[chain][1] = m.candidateSize.WithLabelValues(chainLabel, "collated")
	}

	return o
}

func (o *prometheusValidationObserver) AddValidationInflight(ctx ValidationContext, delta int) {
	if delta == 0 {
		return
	}
	ctx = boundedValidationContext(ctx)
	o.inflight[ctx.Chain][ctx.Kind][ctx.Origin].Add(float64(delta))
}

func (o *prometheusValidationObserver) ObserveValidation(observation ValidationObservation) {
	ctx := boundedValidationContext(observation.ValidationContext)
	result := observation.Result
	if result >= validationResultCount {
		result = ValidationResultError
	}
	o.validations[ctx.Chain][ctx.Kind][ctx.Origin][result].Inc()
	o.decisionDuration[ctx.Chain][ctx.Kind][ctx.Origin][result].Observe(
		validationNonNegativeSeconds(observation.DecisionDuration),
	)
	o.readyDuration[ctx.Chain][ctx.Kind][ctx.Origin][result].Observe(
		validationNonNegativeSeconds(observation.ReadyDuration),
	)
}

func (o *prometheusValidationObserver) ObserveValidationStage(observation ValidationStageObservation) {
	stage := observation.Stage
	if stage >= validationStageCount {
		stage = ValidationStageBackend
	}
	o.stageDuration[boundedValidationChain(observation.Chain)][stage].Observe(
		validationNonNegativeSeconds(observation.Duration),
	)
}

func (o *prometheusValidationObserver) ObserveValidationAttempt(observation ValidationAttemptObservation) {
	result := observation.Result
	if result >= validationAttemptResultCount {
		result = ValidationAttemptError
	}
	o.attemptDuration[boundedValidationChain(observation.Chain)][result].Observe(
		validationNonNegativeSeconds(observation.Duration),
	)
}

func (o *prometheusValidationObserver) ObserveValidationRetry(
	chain collator.MetricChain,
	reason ValidationRetryReason,
) {
	if reason >= validationRetryReasonCount {
		reason = ValidationRetryStateNotReady
	}
	o.retries[boundedValidationChain(chain)][reason].Inc()
}

func (o *prometheusValidationObserver) ObserveValidationCandidateSize(
	chain collator.MetricChain,
	blockBytes int,
	collatedBytes int,
) {
	chain = boundedValidationChain(chain)
	o.candidateSize[chain][0].Observe(float64(max(blockBytes, 0)))
	o.candidateSize[chain][1].Observe(float64(max(collatedBytes, 0)))
}

func (o *prometheusValidationObserver) ObserveValidationCoreStage(
	observation collator.ValidationCoreStageObservation,
) {
	stage := ValidationStageCoreRestoreState + ValidationStage(observation.Stage)
	if observation.Stage > collator.ValidationCoreStageTransition {
		stage = ValidationStageCoreTransition
	}
	o.ObserveValidationStage(ValidationStageObservation{
		Chain:    observation.Chain,
		Stage:    stage,
		Duration: observation.Duration,
	})
}

func boundedValidationContext(ctx ValidationContext) ValidationContext {
	ctx.Chain = boundedValidationChain(ctx.Chain)
	if ctx.Kind >= validationKindCount {
		ctx.Kind = ValidationKindBlock
	}
	if ctx.Origin >= validationOriginCount {
		ctx.Origin = ValidationOriginRemote
	}
	return ctx
}

func boundedValidationChain(chain collator.MetricChain) collator.MetricChain {
	if chain > collator.MetricChainShardchain {
		return collator.MetricChainShardchain
	}
	return chain
}

func validationNonNegativeSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}

func (r *sessionRuntime) validationMetricContext(candidate *simplex.Candidate) ValidationContext {
	kind := ValidationKindBlock
	if candidate.Empty {
		kind = ValidationKindEmpty
	}
	origin := ValidationOriginRemote
	if r.config.Identity.Validator != nil && candidate.Leader == r.config.Identity.Validator.Index {
		origin = ValidationOriginLocal
	}

	return ValidationContext{
		Chain:  r.validationChain(),
		Kind:   kind,
		Origin: origin,
	}
}

func (r *sessionRuntime) validationChain() collator.MetricChain {
	if r.config.Shard.IsMasterchain() {
		return collator.MetricChainMasterchain
	}

	return collator.MetricChainShardchain
}

func (r *sessionRuntime) validationStageStarted() time.Time {
	if r.metrics == nil {
		return time.Time{}
	}

	return time.Now()
}

func (r *sessionRuntime) observeValidationStage(
	chain collator.MetricChain,
	stage ValidationStage,
	started time.Time,
) {
	if r.metrics == nil {
		return
	}

	r.metrics.ObserveValidationStage(ValidationStageObservation{
		Chain:    chain,
		Stage:    stage,
		Duration: time.Since(started),
	})
}

func validationDecisionDuration(started time.Time) time.Duration {
	if started.IsZero() {
		return 0
	}

	return time.Since(started)
}

func validationResult(err error) ValidationResult {
	if err == nil {
		return ValidationResultSuccess
	}
	if errors.Is(err, ErrCandidateRejected) {
		return ValidationResultRejected
	}
	if errors.Is(err, context.Canceled) {
		return ValidationResultCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ValidationResultDeadline
	}

	return ValidationResultError
}

func validationAttemptResult(err error) ValidationAttemptResult {
	if err == nil {
		return ValidationAttemptSuccess
	}
	if errors.Is(err, ErrCandidateRejected) {
		return ValidationAttemptRejected
	}
	if errors.Is(err, ErrBlockNotReady) {
		return ValidationAttemptNotReady
	}
	if errors.Is(err, context.Canceled) {
		return ValidationAttemptCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ValidationAttemptDeadline
	}

	return ValidationAttemptError
}
