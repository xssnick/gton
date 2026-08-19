package validator

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xssnick/gton/service/validator/blockstats"
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

type ValidationTaskResult uint8

const (
	ValidationTaskSuccess ValidationTaskResult = iota
	ValidationTaskRejected
	ValidationTaskNotReady
	ValidationTaskCanceled
	ValidationTaskDeadline
	ValidationTaskError
	validationTaskResultCount
)

type ValidationStage uint8

const (
	ValidationStageLoadCandidate ValidationStage = iota
	ValidationStageResolveParent
	ValidationStageWaitMinBlockInterval
	ValidationStageSemanticValidation
	ValidationStageWaitValidAfter
	validationStageCount
)

const validationSemanticStageCount = int(collator.ValidationCoreStageWaitInputs) + 1

// SessionSpecRole identifies the local identity whose session specification
// was rejected before its runtime could be prepared.
type SessionSpecRole uint8

const (
	SessionSpecRoleValidator SessionSpecRole = iota
	SessionSpecRoleObserver
	sessionSpecRoleCount
)

// SessionSpecRejectionReason is a bounded classification of a session
// specification rejected by the supervisor.
type SessionSpecRejectionReason uint8

const (
	SessionSpecRejectionUnsupportedSimplexProtocol SessionSpecRejectionReason = iota
	SessionSpecRejectionMissingSimplexConfig
	SessionSpecRejectionMissingBlockchainConfig
	SessionSpecRejectionLocalValidatorKeyNotFound
	SessionSpecRejectionInvalidSessionSpec
	sessionSpecRejectionReasonCount
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

type ValidationTaskObservation struct {
	Chain    collator.MetricChain
	Result   ValidationTaskResult
	Duration time.Duration
}

// CandidateCacheDelta is a signed change in what one consensus session's
// candidate cache retains. It is a delta rather than an absolute value because
// the observer is process-wide while the cache is session-scoped: masterchain
// and shard, validator and observer, and two sessions overlapping across a
// rotation all report into the same chain label.
type CandidateCacheDelta struct {
	Retained int64
	Released int64
	Bytes    int64
}

type SessionSpecRejectionObservation struct {
	Chain  collator.MetricChain
	Role   SessionSpecRole
	Reason SessionSpecRejectionReason
}

// ValidationObserver uses bounded enums only. Its Prometheus implementation
// pre-binds every label, so the synchronous validation path does no map lookup
// or label construction.
type ValidationObserver interface {
	collator.ValidationCoreObserver
	AddValidationInflight(ValidationContext, int)
	ObserveValidation(ValidationObservation)
	ObserveValidationStage(ValidationStageObservation)
	ObserveValidationTask(ValidationTaskObservation)
	ObserveValidationCandidateSize(collator.MetricChain, int, int)
	AddCandidateCache(collator.MetricChain, CandidateCacheDelta)
	AddCandidateRetentionCapped(collator.MetricChain)
	// AddCandidatePersistFailure counts one failed durable write of a candidate
	// this node produced. It exists for the runtime that has nowhere else to
	// report one: an observer runtime has no voter, so nothing re-runs the store
	// and nothing fails a vote on it.
	AddCandidatePersistFailure(collator.MetricChain)
	// AddSelfRejectedCandidate counts one candidate this node produced and then
	// refused in its own validation. It is a collator/validator asymmetry inside
	// this node and it is otherwise invisible: the candidate simply never gets
	// our vote, and every rejection log that names it belongs to a peer.
	AddSelfRejectedCandidate(collator.MetricChain)
	// AddChainTipWaitBackstop counts one predecessor read that waited past the
	// backstop without the block becoming readable. It is an alarm, not a rate:
	// every block this session finalizes is published into the live view at
	// acceptance, so a firing backstop means either a genuine catch-up (crash
	// replay, or a block this node did not finalize itself) or a publication site
	// that raises no artifacts signal — and the second is a defect that would
	// otherwise be invisible, because the wait is silent by construction.
	AddChainTipWaitBackstop(collator.MetricChain)
	ObserveSessionSpecRejection(SessionSpecRejectionObservation)
	// ObserveLineageWalk records one leader-window lineage walk: how deep it
	// went, what each step cost and how long the whole thing took. It is the
	// direct measurement of what the retention floor trades away.
	ObserveLineageWalk(LineageWalkObservation)
	// RegisterConsensusSession and UnregisterConsensusSession publish a live
	// session's consensus position for scrape-time collection. They are on this
	// interface because it is already the one process-wide metrics boundary a
	// session runtime holds.
	RegisterConsensusSession(ConsensusSessionKey, ConsensusSessionSource)
	UnregisterConsensusSession(ConsensusSessionKey)
}

// NewBlockStatsObserver records validator-engine-compatible block counters at
// the same terminal observation point as the optional metrics observer.
func NewBlockStatsObserver(
	stats *blockstats.Accumulator,
	observer ValidationObserver,
) ValidationObserver {
	if stats == nil {
		return observer
	}

	return &blockStatsValidationObserver{stats: stats, observer: observer}
}

type blockStatsValidationObserver struct {
	stats    *blockstats.Accumulator
	observer ValidationObserver
}

func (o *blockStatsValidationObserver) AddValidationInflight(ctx ValidationContext, delta int) {
	if o.observer != nil {
		o.observer.AddValidationInflight(ctx, delta)
	}
}

func (o *blockStatsValidationObserver) ObserveValidation(observation ValidationObservation) {
	ctx := boundedValidationContext(observation.ValidationContext)
	if ctx.Kind == ValidationKindBlock {
		result := observation.Result
		if result >= validationResultCount {
			result = ValidationResultError
		}
		o.stats.ObserveValidation(
			ctx.Chain == collator.MetricChainMasterchain,
			result == ValidationResultSuccess,
		)
	}

	if o.observer != nil {
		o.observer.ObserveValidation(observation)
	}
}

func (o *blockStatsValidationObserver) ObserveValidationStage(observation ValidationStageObservation) {
	if o.observer != nil {
		o.observer.ObserveValidationStage(observation)
	}
}

func (o *blockStatsValidationObserver) ObserveValidationTask(observation ValidationTaskObservation) {
	if o.observer != nil {
		o.observer.ObserveValidationTask(observation)
	}
}

func (o *blockStatsValidationObserver) ObserveValidationCandidateSize(
	chain collator.MetricChain,
	blockBytes int,
	collatedBytes int,
) {
	if o.observer != nil {
		o.observer.ObserveValidationCandidateSize(chain, blockBytes, collatedBytes)
	}
}

func (o *blockStatsValidationObserver) AddCandidateCache(
	chain collator.MetricChain,
	delta CandidateCacheDelta,
) {
	if o.observer != nil {
		o.observer.AddCandidateCache(chain, delta)
	}
}

func (o *blockStatsValidationObserver) AddCandidateRetentionCapped(chain collator.MetricChain) {
	if o.observer != nil {
		o.observer.AddCandidateRetentionCapped(chain)
	}
}

func (o *blockStatsValidationObserver) AddCandidatePersistFailure(chain collator.MetricChain) {
	if o.observer != nil {
		o.observer.AddCandidatePersistFailure(chain)
	}
}

func (o *blockStatsValidationObserver) AddSelfRejectedCandidate(chain collator.MetricChain) {
	if o.observer != nil {
		o.observer.AddSelfRejectedCandidate(chain)
	}
}

func (o *blockStatsValidationObserver) AddChainTipWaitBackstop(chain collator.MetricChain) {
	if o.observer != nil {
		o.observer.AddChainTipWaitBackstop(chain)
	}
}

func (o *blockStatsValidationObserver) ObserveSessionSpecRejection(
	observation SessionSpecRejectionObservation,
) {
	if o.observer != nil {
		o.observer.ObserveSessionSpecRejection(observation)
	}
}

func (o *blockStatsValidationObserver) ObserveLineageWalk(observation LineageWalkObservation) {
	if o.observer != nil {
		o.observer.ObserveLineageWalk(observation)
	}
}

func (o *blockStatsValidationObserver) RegisterConsensusSession(
	key ConsensusSessionKey,
	source ConsensusSessionSource,
) {
	if o.observer != nil {
		o.observer.RegisterConsensusSession(key, source)
	}
}

func (o *blockStatsValidationObserver) UnregisterConsensusSession(key ConsensusSessionKey) {
	if o.observer != nil {
		o.observer.UnregisterConsensusSession(key)
	}
}

func (o *blockStatsValidationObserver) ObserveValidationCoreStage(
	observation collator.ValidationCoreStageObservation,
) {
	if o.observer != nil {
		o.observer.ObserveValidationCoreStage(observation)
	}
}

// PrometheusValidationMetrics owns the validator metric vectors. Observer is
// a pre-bound hot-path view created once for the validator extension.
type PrometheusValidationMetrics struct {
	validations           *prometheus.CounterVec
	inflight              *prometheus.GaugeVec
	decisionDuration      *prometheus.HistogramVec
	readyDuration         *prometheus.HistogramVec
	stageDuration         *prometheus.HistogramVec
	semanticStageDuration *prometheus.HistogramVec
	taskDuration          *prometheus.HistogramVec
	candidateSize         *prometheus.HistogramVec
	candidateCacheEntries *prometheus.GaugeVec
	candidateCacheBytes   *prometheus.GaugeVec
	retentionCapped       *prometheus.CounterVec
	persistFailures       *prometheus.CounterVec
	selfRejections        *prometheus.CounterVec
	chainTipWaitBackstops *prometheus.CounterVec
	sessionRejections     *prometheus.CounterVec
	lineageWalkCandidates *prometheus.HistogramVec
	lineageWalkDuration   *prometheus.HistogramVec
	lineageWalkSteps      *prometheus.CounterVec
	consensus             *consensusCollector
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
			Help: "Wall-clock duration of non-overlapping validation runtime stages.", Buckets: durations,
		}, []string{"chain", "stage"}),
		semanticStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validation_semantic_stage_duration_seconds",
			Help: "Wall-clock duration of nested deterministic semantic-validation stages.", Buckets: durations,
		}, []string{"chain", "stage"}),
		taskDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "validation_task_duration_seconds",
			Help: "Duration of the single semantic-validation task by result.", Buckets: durations,
		}, []string{"chain", "result"}),
		candidateSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "candidate_size_bytes",
			Help:    "Block and collated-data payload sizes entering candidate validation.",
			Buckets: prometheus.ExponentialBuckets(1_024, 2, 15),
		}, []string{"chain", "part"}),
		candidateCacheEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "validator", Name: "candidate_cache_entries",
			Help: "Consensus candidate cache entries by whether their payload is still in memory.",
		}, []string{"chain", "state"}),
		candidateCacheBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "validator", Name: "candidate_cache_bytes",
			Help: "Candidate wire, block, and collated-data bytes retained by live consensus sessions.",
		}, []string{"chain"}),
		retentionCapped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "candidate_retention_capped_total",
			Help: "Finalizations whose candidate retention pruned past the lineage the local producer still needs.",
		}, []string{"chain"}),
		persistFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "candidate_persist_failures_total",
			Help: "Failed durable writes of a candidate produced locally, counted where the write was submitted.",
		}, []string{"chain"}),
		selfRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "self_rejected_candidates_total",
			Help: "Candidates produced by this node and then rejected by its own validation. Always a defect in this node.",
		}, []string{"chain"}),
		chainTipWaitBackstops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "chain_tip_wait_backstops_total",
			Help: "Predecessor reads that waited past the backstop for a block to become readable. " +
				"Expected only during catch-up; otherwise a publication site raises no artifacts signal.",
		}, []string{"chain"}),
		sessionRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "session_spec_rejections_total",
			Help: "Transitions of local validator or observer session specifications into rejection.",
		}, []string{"chain", "role", "reason"}),
		lineageWalkCandidates: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "lineage_walk_candidates",
			Help: "Candidates visited by one leader-window lineage walk. Compared against " +
				"consensus_retention_lag_slots it separates a real production backlog from a run " +
				"of skipped slots.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}, []string{"chain"}),
		lineageWalkDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "validator", Name: "lineage_walk_duration_seconds",
			Help:    "Wall-clock duration of one leader-window lineage walk, including the anchor state load.",
			Buckets: durations,
		}, []string{"chain", "result"}),
		lineageWalkSteps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "validator", Name: "lineage_walk_steps_total",
			Help: "Lineage walk steps by where the candidate had to come from. Storage and peer " +
				"steps rising with consensus_retention_capped is the retention floor giving up, " +
				"made visible.",
		}, []string{"chain", "source"}),
		consensus: newConsensusCollector(namespace),
	}

	if err := registry.RegisterCollector(validationCollectorSet{
		metrics.validations, metrics.inflight, metrics.decisionDuration, metrics.readyDuration,
		metrics.stageDuration, metrics.semanticStageDuration, metrics.taskDuration,
		metrics.candidateSize,
		metrics.candidateCacheEntries, metrics.candidateCacheBytes, metrics.retentionCapped,
		metrics.persistFailures, metrics.selfRejections, metrics.chainTipWaitBackstops,
		metrics.sessionRejections,
		metrics.lineageWalkCandidates, metrics.lineageWalkDuration, metrics.lineageWalkSteps,
		metrics.consensus,
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
	validations           [2][validationKindCount][validationOriginCount][validationResultCount]prometheus.Counter
	inflight              [2][validationKindCount][validationOriginCount]prometheus.Gauge
	decisionDuration      [2][validationKindCount][validationOriginCount][validationResultCount]prometheus.Observer
	readyDuration         [2][validationKindCount][validationOriginCount][validationResultCount]prometheus.Observer
	stageDuration         [2][validationStageCount]prometheus.Observer
	semanticStageDuration [2][validationSemanticStageCount]prometheus.Observer
	taskDuration          [2][validationTaskResultCount]prometheus.Observer
	candidateSize         [2][2]prometheus.Observer
	candidateCacheEntries [2][2]prometheus.Gauge
	candidateCacheBytes   [2]prometheus.Gauge
	retentionCapped       [2]prometheus.Counter
	persistFailures       [2]prometheus.Counter
	selfRejections        [2]prometheus.Counter
	chainTipWaitBackstops [2]prometheus.Counter
	sessionRejections     [2][sessionSpecRoleCount][sessionSpecRejectionReasonCount]prometheus.Counter
	lineageWalkCandidates [2]prometheus.Observer
	lineageWalkDuration   [2][lineageWalkResultCount]prometheus.Observer
	lineageWalkSteps      [2][lineageStepSourceCount]prometheus.Counter
	consensus             *consensusCollector
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
		"load_candidate", "resolve_parent", "wait_min_block_interval",
		"semantic_validation", "wait_valid_after",
	}
	semanticStages := [...]string{
		"restore_state", "prepare_master_view", "resolve_chain_inputs",
		"decode_candidate", "verify_transition", "wait_inputs",
	}
	taskResults := [...]string{"success", "rejected", "not_ready", "canceled", "deadline", "error"}
	sessionRoles := [...]string{"validator", "observer"}
	sessionRejectionReasons := [...]string{
		"unsupported_simplex_protocol", "missing_simplex_config", "missing_blockchain_config",
		"local_validator_key_not_found", "invalid_session_spec",
	}
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
		for stage := collator.ValidationCoreStage(0); stage <= collator.ValidationCoreStageWaitInputs; stage++ {
			o.semanticStageDuration[chain][stage] = m.semanticStageDuration.WithLabelValues(
				chainLabel,
				semanticStages[stage],
			)
		}
		for result := ValidationTaskResult(0); result < validationTaskResultCount; result++ {
			o.taskDuration[chain][result] = m.taskDuration.WithLabelValues(chainLabel, taskResults[result])
		}
		o.candidateSize[chain][0] = m.candidateSize.WithLabelValues(chainLabel, "block")
		o.candidateSize[chain][1] = m.candidateSize.WithLabelValues(chainLabel, "collated")
		o.candidateCacheEntries[chain][0] = m.candidateCacheEntries.WithLabelValues(chainLabel, "retained")
		o.candidateCacheEntries[chain][1] = m.candidateCacheEntries.WithLabelValues(chainLabel, "released")
		o.candidateCacheBytes[chain] = m.candidateCacheBytes.WithLabelValues(chainLabel)
		o.retentionCapped[chain] = m.retentionCapped.WithLabelValues(chainLabel)
		o.persistFailures[chain] = m.persistFailures.WithLabelValues(chainLabel)
		o.selfRejections[chain] = m.selfRejections.WithLabelValues(chainLabel)
		o.chainTipWaitBackstops[chain] = m.chainTipWaitBackstops.WithLabelValues(chainLabel)
		for role := SessionSpecRole(0); role < sessionSpecRoleCount; role++ {
			for reason := SessionSpecRejectionReason(0); reason < sessionSpecRejectionReasonCount; reason++ {
				o.sessionRejections[chain][role][reason] = m.sessionRejections.WithLabelValues(
					chainLabel, sessionRoles[role], sessionRejectionReasons[reason],
				)
			}
		}
		o.lineageWalkCandidates[chain] = m.lineageWalkCandidates.WithLabelValues(chainLabel)
		for result, label := range [...]string{"ok", "error"} {
			o.lineageWalkDuration[chain][result] = m.lineageWalkDuration.WithLabelValues(chainLabel, label)
		}
		for source, label := range [...]string{"memory", "storage", "peer"} {
			o.lineageWalkSteps[chain][source] = m.lineageWalkSteps.WithLabelValues(chainLabel, label)
		}
	}
	o.consensus = m.consensus

	return o
}

// ObserveLineageWalk records one completed lineage walk.
func (o *prometheusValidationObserver) ObserveLineageWalk(observation LineageWalkObservation) {
	chain := boundedValidationChain(observation.Chain)
	result := observation.Result
	if result >= lineageWalkResultCount {
		result = LineageWalkFailure
	}
	o.lineageWalkCandidates[chain].Observe(float64(observation.Candidates))
	o.lineageWalkDuration[chain][result].Observe(observation.Duration.Seconds())
	for source, steps := range observation.Steps {
		if steps > 0 {
			o.lineageWalkSteps[chain][source].Add(float64(steps))
		}
	}
}

// RegisterConsensusSession publishes one live session to the scrape-time
// consensus collector.
func (o *prometheusValidationObserver) RegisterConsensusSession(
	key ConsensusSessionKey,
	source ConsensusSessionSource,
) {
	o.consensus.register(key, source)
}

// UnregisterConsensusSession takes the session's final reading and drops it.
func (o *prometheusValidationObserver) UnregisterConsensusSession(key ConsensusSessionKey) {
	o.consensus.unregister(key)
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
		return
	}
	o.stageDuration[boundedValidationChain(observation.Chain)][stage].Observe(
		validationNonNegativeSeconds(observation.Duration),
	)
}

func (o *prometheusValidationObserver) ObserveValidationTask(observation ValidationTaskObservation) {
	result := observation.Result
	if result >= validationTaskResultCount {
		result = ValidationTaskError
	}
	o.taskDuration[boundedValidationChain(observation.Chain)][result].Observe(
		validationNonNegativeSeconds(observation.Duration),
	)
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

func (o *prometheusValidationObserver) AddCandidateCache(
	chain collator.MetricChain,
	delta CandidateCacheDelta,
) {
	chain = boundedValidationChain(chain)
	if delta.Retained != 0 {
		o.candidateCacheEntries[chain][0].Add(float64(delta.Retained))
	}
	if delta.Released != 0 {
		o.candidateCacheEntries[chain][1].Add(float64(delta.Released))
	}
	if delta.Bytes != 0 {
		o.candidateCacheBytes[chain].Add(float64(delta.Bytes))
	}
}

func (o *prometheusValidationObserver) AddCandidateRetentionCapped(chain collator.MetricChain) {
	o.retentionCapped[boundedValidationChain(chain)].Inc()
}

func (o *prometheusValidationObserver) AddCandidatePersistFailure(chain collator.MetricChain) {
	o.persistFailures[boundedValidationChain(chain)].Inc()
}

func (o *prometheusValidationObserver) AddSelfRejectedCandidate(chain collator.MetricChain) {
	o.selfRejections[boundedValidationChain(chain)].Inc()
}

func (o *prometheusValidationObserver) AddChainTipWaitBackstop(chain collator.MetricChain) {
	o.chainTipWaitBackstops[boundedValidationChain(chain)].Inc()
}

func (o *prometheusValidationObserver) ObserveSessionSpecRejection(
	observation SessionSpecRejectionObservation,
) {
	role := observation.Role
	if role >= sessionSpecRoleCount {
		role = SessionSpecRoleValidator
	}
	reason := observation.Reason
	if reason >= sessionSpecRejectionReasonCount {
		reason = SessionSpecRejectionInvalidSessionSpec
	}
	o.sessionRejections[boundedValidationChain(observation.Chain)][role][reason].Inc()
}

func (o *prometheusValidationObserver) ObserveValidationCoreStage(
	observation collator.ValidationCoreStageObservation,
) {
	if observation.Stage > collator.ValidationCoreStageTransition {
		return
	}
	o.semanticStageDuration[boundedValidationChain(observation.Chain)][observation.Stage].Observe(
		validationNonNegativeSeconds(observation.Duration),
	)
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

func validationTaskResult(err error) ValidationTaskResult {
	if err == nil {
		return ValidationTaskSuccess
	}
	if errors.Is(err, ErrCandidateRejected) {
		return ValidationTaskRejected
	}
	if errors.Is(err, ErrBlockNotReady) {
		return ValidationTaskNotReady
	}
	if errors.Is(err, context.Canceled) {
		return ValidationTaskCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ValidationTaskDeadline
	}

	return ValidationTaskError
}
