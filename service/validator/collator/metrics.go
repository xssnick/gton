package collator

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xssnick/gton/service/validator/blockstats"
)

// MetricsRegistry is the process metrics capability used by the validator
// extension. Keeping the boundary here avoids coupling collation to the node's
// concrete metrics service.
type MetricsRegistry interface {
	Namespace() string
	RegisterCollector(prometheus.Collector) error
}

// MetricsMode separates candidates produced by an integrated validator from
// candidates produced by the standalone collator in the same process.
type MetricsMode uint8

const (
	MetricsModeValidator MetricsMode = iota
	MetricsModeStandalone
	metricsModeCount
)

// MetricChain is the bounded chain label used by collation observers.
type MetricChain uint8

const (
	MetricChainMasterchain MetricChain = iota
	MetricChainShardchain
	metricChainCount
)

// CollationStage is a non-overlapping wall-clock stage around the candidate
// producer. Build is deliberately not a stage: it is the enclosing operation.
type CollationStage uint8

const (
	CollationStageAcquireInputs CollationStage = iota
	CollationStagePrepareState
	CollationStageCleanupOutQueue
	CollationStageExecuteInternalMessages
	CollationStageExecuteExternalMessages
	CollationStageFinalizeAccounts
	CollationStageBuildStateUpdate
	CollationStageSerializeCandidate
	CollationStageFinalizeCandidate
	CollationStageWaitExternalMessages
	CollationStageResolveCandidateState
	CollationStageRestoreCandidate
	CollationStageSignCandidate
	CollationStagePersistCandidate
	CollationStageCommitCandidateState
	CollationStageWaitBroadcastSlot
	CollationStageDeliverCandidate
	// The five below subdivide what the single build_state_update span used to
	// cover: the finish work between account finalization and candidate
	// serialization. They are exclusive spans like every other stage, so the
	// per-stage sum still adds up to the build; build_state_update itself now
	// means exactly the state serialization plus the Merkle update.
	CollationStageProcessedInfo
	CollationStageClaimedLocalCleanup
	CollationStageFlushBatches
	CollationStageValidationClosure
	CollationStageSerializeBlock
	collationStageCount
)

// CollationAlarm is a bounded producer fault that needs an operator rather than
// a trend line. Every member is a condition this collator can detect about its
// own output and nothing downstream can: a peer that rejects the resulting
// block reports the symptom, never the cause; a slot that produced no block at
// all reports nothing anywhere; and a block rebuilt to fit looks exactly like
// one that fit the first time.
type CollationAlarm uint8

const (
	// CollationAlarmShortCollatedProof is a block that shipped with a collated
	// proof this node knows does not cover the predecessor-queue scan a
	// proof-backed validator will run over it. The block is on the wire and is
	// very likely to be rejected for "augmented dict has special cells in tree
	// structure", which names the cell and not the reason. See
	// traceProcessedQueueValidationClosure and Stats.ProcessedScanTraceError.
	CollationAlarmShortCollatedProof CollationAlarm = iota
	// CollationAlarmMandatoryDequeueOverflow is a slot lost because the
	// predecessor's already-processed own-shard queue entries do not fit one
	// block. The drain cannot be truncated, so no block is produced at all. See
	// cleanupMandatoryDrain.
	CollationAlarmMandatoryDequeueOverflow
	// CollationAlarmSizeLimitRetry is a collation that had to be rebuilt because
	// the finished block did not fit the consensus size limit — the size
	// estimate the collator admitted against was wrong by enough to throw a
	// whole build away. It fires whether the rebuild eventually fit or the slot
	// was lost, because both mean the same defect upstream of the retry.
	//
	// It needs its own series because a successful retry is otherwise invisible:
	// the block that finally fits is indistinguishable from a block that fit at
	// once, and the failed attempt's error is swallowed by the loop that
	// replaced it. See retryUnderSizeLimit and Stats.CollationAttempts.
	CollationAlarmSizeLimitRetry
	collationAlarmCount
)

func (a CollationAlarm) String() string {
	switch a {
	case CollationAlarmShortCollatedProof:
		return "short_collated_proof"
	case CollationAlarmMandatoryDequeueOverflow:
		return "mandatory_dequeue_overflow"
	case CollationAlarmSizeLimitRetry:
		return "size_limit_retry"
	default:
		return "unknown"
	}
}

// CollationResult is a bounded terminal result for builds, productions, and
// windows. Detailed error strings stay in logs instead of metric labels.
type CollationResult uint8

const (
	CollationResultSuccess CollationResult = iota
	CollationResultError
	CollationResultCanceled
	CollationResultDeadline
	// CollationResultNotReady is a build that stopped because a local input it
	// needs has not arrived yet. It is not an error: the window producer retries
	// it on its own schedule and the very next attempt usually succeeds. It has
	// its own result because it used to be counted as CollationResultError,
	// where it was 99.7% of everything under that label and made the collator
	// look like it was failing 39-50% of its builds.
	CollationResultNotReady
	collationResultCount
)

// CandidateKind identifies the producer path without exposing candidate IDs.
type CandidateKind uint8

const (
	CandidateKindBlock CandidateKind = iota
	CandidateKindEmpty
	CandidateKindRecovered
	candidateKindCount
)

// CollationDeadline identifies the schedule boundary reached by production.
type CollationDeadline uint8

const (
	CollationDeadlineSoft CollationDeadline = iota
	CollationDeadlineHard
	collationDeadlineCount
)

// DeadlineAction is the bounded decision made at a producer deadline.
type DeadlineAction uint8

const (
	DeadlineActionWait DeadlineAction = iota
	DeadlineActionEmitEmpty
	DeadlineActionAbort
	// DeadlineActionWaitNoEmpty is a soft deadline this producer had to keep
	// waiting through because the session has not finalized for
	// NoEmptyBlocksOnErrTimeout and an empty block is therefore forbidden. It is
	// distinguished from an ordinary wait because the two have different causes
	// and only one of them is about this node's own build being slow.
	DeadlineActionWaitNoEmpty
	deadlineActionCount
)

// ProductionRetryReason distinguishes local-state lag from other retryable
// producer failures without placing error strings in labels.
type ProductionRetryReason uint8

const (
	ProductionRetryNotReady ProductionRetryReason = iota
	ProductionRetryOther
	productionRetryReasonCount
)

// ScheduleEvent identifies a slot schedule boundary whose lateness is sampled.
type ScheduleEvent uint8

const (
	ScheduleEventBuildStart ScheduleEvent = iota
	ScheduleEventBroadcast
	// ScheduleEventBuildLead is the head start a pipelined shard build won: the
	// distance between the instant its predecessor's state was committed, where
	// the build actually began, and the instant the schedule said it should
	// begin. Lateness is clamped to zero, so this reads as milliseconds gained
	// and is zero on every build that started late — which is every build the
	// producer loop starts on its own.
	//
	// It is also the correction term for the build and production durations: a
	// pipelined build's timer starts at the head start, not at its slot, so
	// those histograms include the lead and must be read against it.
	ScheduleEventBuildLead
	scheduleEventCount
)

// String is what the label carries. It exists so the observer can fill its
// per-event array by walking the enum, the way the alarm array is filled: the
// positional literal it replaced silently left a nil prometheus.Observer behind
// for any member added past the end of the list, and ObserveScheduleLateness
// dereferences that slot without checking.
func (e ScheduleEvent) String() string {
	switch e {
	case ScheduleEventBuildStart:
		return "build_start"
	case ScheduleEventBroadcast:
		return "broadcast"
	case ScheduleEventBuildLead:
		return "build_lead"
	default:
		return "unknown"
	}
}

// PipelineHandoffOutcome is what became of one predecessor handed to the
// producer before it was committed. Every path through the handoff reports
// exactly one of these, so the counter answers both halves of the question an
// operator has: how often the pipeline runs, and when it does not, which gate
// declined it.
type PipelineHandoffOutcome uint8

const (
	// PipelineHandoffStarted is a successor build that was started and parked.
	PipelineHandoffStarted PipelineHandoffOutcome = iota
	// PipelineHandoffDeclinedBusy is the only exact detector of three live
	// builds: a handoff arriving while the slot it would fill is already taken.
	PipelineHandoffDeclinedBusy
	PipelineHandoffDeclinedMasterchain
	PipelineHandoffDeclinedWindowEnd
	PipelineHandoffDeclinedLead
	PipelineHandoffDeclinedEmpty
	// PipelineHandoffDeclinedBaseUnseeded is a successor refused because
	// installing its queue base would have meant seeding the message branch
	// under the session lock, in front of the predecessor's own commit.
	PipelineHandoffDeclinedBaseUnseeded
	// PipelineHandoffAbandonedRetry is a successor thrown away because its
	// predecessor overflowed the consensus size limit and is being rebuilt: the
	// root it was started on is one no block will ever carry.
	PipelineHandoffAbandonedRetry
	PipelineHandoffAbandonedFailed
	// PipelineHandoffAbandonedSuperseded covers every other way the producer
	// ends up committing something other than what the offer described.
	PipelineHandoffAbandonedSuperseded
	// PipelineHandoffSpeculativeStarted is a first slot begun before its leader
	// window was observed, on the candidate this node bet the window would open
	// on. The members below are the same accounting for that bet: how often it
	// runs, and when it does not, which gate declined it.
	PipelineHandoffSpeculativeStarted
	// PipelineHandoffSpeculativeAdopted is a speculative first slot the producer
	// went on to use, which is the only member that means time was saved.
	PipelineHandoffSpeculativeAdopted
	// PipelineHandoffSpeculativeMissed is a speculative first slot dropped
	// because the window opened on a different base — the bet the producer is
	// allowed to lose, and the one whose rate says whether it is worth taking.
	PipelineHandoffSpeculativeMissed
	// PipelineHandoffSpeculativeDeclined is a bet never placed: the session was
	// not in the window before ours, the slot was already speculated on, or the
	// producer for that window had already started.
	PipelineHandoffSpeculativeDeclined
	// PipelineHandoffSpeculativeFailed is a bet whose build could not serve the
	// window even though the window opened on the base it bet on: its
	// acquisition found an input it may not seed, or the build errored. It is
	// counted here and NOWHERE ELSE — a speculative build is deliberately kept
	// out of gton_collator_builds_total and out of "block collation failed",
	// because a lost bet costs the collation nothing and its errors would read
	// as the collator failing. This member is therefore the only place a bet
	// that never becomes a block is visible, and without it 85 bets could be
	// started, none adopted, and nothing say so.
	PipelineHandoffSpeculativeFailed
	pipelineHandoffOutcomeCount
)

// String is the label. Written as a switch and walked by the enum wherever the
// observer fills a per-outcome array, never as a positional literal: a member
// added past the end of such a literal leaves a nil observer behind and panics
// on the producer goroutine at the first sample.
func (o PipelineHandoffOutcome) String() string {
	switch o {
	case PipelineHandoffStarted:
		return "started"
	case PipelineHandoffDeclinedBusy:
		return "declined_busy"
	case PipelineHandoffDeclinedMasterchain:
		return "declined_masterchain"
	case PipelineHandoffDeclinedWindowEnd:
		return "declined_window_end"
	case PipelineHandoffDeclinedLead:
		return "declined_lead"
	case PipelineHandoffDeclinedEmpty:
		return "declined_empty"
	case PipelineHandoffDeclinedBaseUnseeded:
		return "declined_base_unseeded"
	case PipelineHandoffAbandonedRetry:
		return "abandoned_retry"
	case PipelineHandoffAbandonedFailed:
		return "abandoned_failed"
	case PipelineHandoffAbandonedSuperseded:
		return "abandoned_superseded"
	case PipelineHandoffSpeculativeStarted:
		return "speculative_started"
	case PipelineHandoffSpeculativeAdopted:
		return "speculative_adopted"
	case PipelineHandoffSpeculativeMissed:
		return "speculative_missed"
	case PipelineHandoffSpeculativeDeclined:
		return "speculative_declined"
	case PipelineHandoffSpeculativeFailed:
		return "speculative_failed"
	default:
		return "unknown"
	}
}

// ValidationCoreStage identifies the expensive deterministic work delegated
// from the validator runtime to local acquisition.
type ValidationCoreStage uint8

const (
	ValidationCoreStageRestoreState ValidationCoreStage = iota
	ValidationCoreStageMasterView
	ValidationCoreStageChainInputs
	ValidationCoreStageDecode
	ValidationCoreStageTransition
	ValidationCoreStageWaitInputs
	validationCoreStageCount
)

type CollationBuildObservation struct {
	Chain    MetricChain
	Result   CollationResult
	Duration time.Duration
}

type CollationStageObservation struct {
	Chain    MetricChain
	Stage    CollationStage
	Duration time.Duration
	// Substage, when set, names a span NESTED inside Stage rather than the
	// stage itself. The stage histogram is a partition of the build — its
	// per-stage sum is the build — so a nested span must not land there; it
	// goes to the substage histogram, keyed by the stage it sits in. It exists
	// for the spans whose wall time is invisible to profiles: acquisition and
	// the validation closure both spend most of their time in store reads,
	// which neither the CPU nor the blocking profile can attribute.
	Substage string
}

var candidateAssemblyStages = [...]CollationStage{
	CollationStagePrepareState,
	CollationStageCleanupOutQueue,
	CollationStageExecuteInternalMessages,
	CollationStageExecuteExternalMessages,
	CollationStageFinalizeAccounts,
	CollationStageProcessedInfo,
	CollationStageClaimedLocalCleanup,
	CollationStageFlushBatches,
	CollationStageBuildStateUpdate,
	CollationStageValidationClosure,
	CollationStageSerializeBlock,
	CollationStageSerializeCandidate,
	CollationStageFinalizeCandidate,
}

// candidateAssemblyDurations accumulates the exclusive wall-clock stages of
// every serialized-size attempt belonging to one build. It is installed only
// when a runtime observer exists, so deterministic builds and tests pay no
// clock reads.
//
// entered marks a stage the build actually passed through, which a duration
// cannot: the finish stages are individually short enough that the monotonic
// clock can return the same reading at both ends, so a measured stage is not
// always a non-zero one.
type candidateAssemblyDurations struct {
	stages  [collationStageCount]time.Duration
	entered [collationStageCount]bool
	// substages are spans nested inside a stage, reported beside it when the
	// build's durations are flushed. See CollationStageObservation.Substage.
	substages []candidateAssemblySubstage
}

type candidateAssemblySubstage struct {
	stage    CollationStage
	name     string
	duration time.Duration
}

// substage records one nested span. It is safe to call from a goroutine the
// stage fanned out only if the caller serializes it; the closure's tasks
// report through their own slots and are merged by the main goroutine.
func (d *candidateAssemblyDurations) substage(stage CollationStage, name string, duration time.Duration) {
	if d == nil {
		return
	}
	d.substages = append(d.substages, candidateAssemblySubstage{stage: stage, name: name, duration: duration})
}

type candidateAssemblyStageTimer struct {
	durations *candidateAssemblyDurations
	stage     CollationStage
	started   time.Time
}

func (d *candidateAssemblyDurations) start(stage CollationStage) candidateAssemblyStageTimer {
	if d == nil {
		return candidateAssemblyStageTimer{}
	}

	return candidateAssemblyStageTimer{durations: d, stage: stage, started: time.Now()}
}

// stop is idempotent so finish can leave one deferred stop guarding every
// error return while moving the same timer through successive stages.
func (t *candidateAssemblyStageTimer) stop() {
	if t.durations == nil {
		return
	}

	t.durations.stages[t.stage] += time.Since(t.started)
	t.durations.entered[t.stage] = true
	t.durations = nil
}

// CandidateOrigin separates a candidate this node built from one it only
// checked. Both carry the same shape — payload sizes, transaction count,
// message mix — so the two belong on one metric where the series can be read
// against each other; a shard whose own blocks are much smaller than the ones
// it validates is a statement about this node, not about the network.
type CandidateOrigin uint8

const (
	CandidateOriginCollation CandidateOrigin = iota
	CandidateOriginValidation
	candidateOriginCount
)

// CandidateShape is the part of a candidate that both paths can report. A
// collation reads it off its own counters; a validation recovers it from the
// block, counted inside walks the replay already performs, so neither path
// pays a pass it would not otherwise make.
//
// The two definitions line up exactly. ExternalMessages counts msg_import_ext,
// which collation writes once per included external, and InternalMessages
// counts msg_import_fin plus msg_import_tr, the two descriptors it writes for
// an internal taken off a queue — executed here or relayed onward.
type CandidateShape struct {
	Transactions     uint32
	ExternalMessages uint32
	InternalMessages uint32
}

// CollationStats reports the shape a collation observed, for the paths that
// hand a full Stats to the observer.
func (s Stats) CollationShape() CandidateShape {
	return CandidateShape{
		Transactions:     s.Transactions,
		ExternalMessages: s.ExternalIncluded,
		InternalMessages: s.InternalsImported,
	}
}

type CandidateObservation struct {
	Chain         MetricChain
	Origin        CandidateOrigin
	Kind          CandidateKind
	BlockBytes    int
	CollatedBytes int
	Shape         CandidateShape
	// Stats carries the counters only a producer has. It is read solely for
	// CandidateOriginCollation; a validation leaves it zero.
	Stats Stats
}

type CandidateProductionObservation struct {
	Chain    MetricChain
	Kind     CandidateKind
	Result   CollationResult
	Duration time.Duration
}

type WindowObservation struct {
	Chain    MetricChain
	Result   CollationResult
	Duration time.Duration
}

type ValidationCoreStageObservation struct {
	Chain    MetricChain
	Stage    ValidationCoreStage
	Duration time.Duration
}

// CollationObserver is called synchronously only at coarse stage boundaries.
// Prometheus implementations bind every label at construction, so these calls
// do not allocate or build dynamic label sets on the collation hot path.
type CollationObserver interface {
	AddCollationBuildInflight(MetricChain, int)
	ObserveCollationBuild(CollationBuildObservation)
	ObserveCollationStage(CollationStageObservation)
	ObserveCollationCandidate(CandidateObservation)
	ObserveCandidateProduction(CandidateProductionObservation)
	ObserveCollationDeadline(MetricChain, CollationDeadline, DeadlineAction)
	ObserveCollationRetry(MetricChain, ProductionRetryReason)
	// ObserveCollationAlarm counts one producer fault an operator has to act on.
	// It is separate from the terminal build result because neither member is
	// visible there: a short collated proof rides on a SUCCESSFUL build, and a
	// mandatory-dequeue overflow is one error among all the others that end a
	// build.
	ObserveCollationAlarm(MetricChain, CollationAlarm)
	ObservePipelineHandoff(MetricChain, PipelineHandoffOutcome)
	ObservePipelineHandoffPickup(MetricChain, time.Duration)
	ObservePipelineOverlap(MetricChain, time.Duration)
	ObserveScheduleLateness(MetricChain, ScheduleEvent, time.Duration)
	AddCollationWindowInflight(MetricChain, int)
	ObserveCollationWindow(WindowObservation)
}

// NewBlockStatsObserver records validator-engine-compatible block counters at
// the same terminal observation point as the optional metrics observer.
func NewBlockStatsObserver(
	stats *blockstats.Accumulator,
	observer CollationObserver,
) CollationObserver {
	if stats == nil {
		return observer
	}

	return &blockStatsObserver{stats: stats, observer: observer}
}

type blockStatsObserver struct {
	stats    *blockstats.Accumulator
	observer CollationObserver
}

func (o *blockStatsObserver) AddCollationBuildInflight(chain MetricChain, delta int) {
	if o.observer != nil {
		o.observer.AddCollationBuildInflight(chain, delta)
	}
}

func (o *blockStatsObserver) ObserveCollationBuild(observation CollationBuildObservation) {
	switch boundedCollationResult(observation.Result) {
	case CollationResultSuccess:
		o.stats.ObserveCollation(observation.Chain == MetricChainMasterchain, true)
	case CollationResultNotReady:
		// Not an attempt at all as far as validator-engine's two-valued counter
		// is concerned: the inputs were not there, the producer retried, and the
		// attempt that eventually ran is the one this counter should see.
	default:
		// validator-engine exposes only ok and error, so every non-successful
		// terminal build is represented by the error counter.
		o.stats.ObserveCollation(observation.Chain == MetricChainMasterchain, false)
	}

	if o.observer != nil {
		o.observer.ObserveCollationBuild(observation)
	}
}

func (o *blockStatsObserver) ObserveCollationStage(observation CollationStageObservation) {
	if o.observer != nil {
		o.observer.ObserveCollationStage(observation)
	}
}

func (o *blockStatsObserver) ObserveCollationCandidate(observation CandidateObservation) {
	if o.observer != nil {
		o.observer.ObserveCollationCandidate(observation)
	}
}

func (o *blockStatsObserver) ObserveCandidateProduction(observation CandidateProductionObservation) {
	if o.observer != nil {
		o.observer.ObserveCandidateProduction(observation)
	}
}

func (o *blockStatsObserver) ObserveCollationDeadline(
	chain MetricChain,
	deadline CollationDeadline,
	action DeadlineAction,
) {
	if o.observer != nil {
		o.observer.ObserveCollationDeadline(chain, deadline, action)
	}
}

func (o *blockStatsObserver) ObserveCollationRetry(chain MetricChain, reason ProductionRetryReason) {
	if o.observer != nil {
		o.observer.ObserveCollationRetry(chain, reason)
	}
}

func (o *blockStatsObserver) ObserveCollationAlarm(chain MetricChain, alarm CollationAlarm) {
	if o.observer != nil {
		o.observer.ObserveCollationAlarm(chain, alarm)
	}
}

func (o *blockStatsObserver) ObservePipelineHandoff(chain MetricChain, outcome PipelineHandoffOutcome) {
	if o.observer != nil {
		o.observer.ObservePipelineHandoff(chain, outcome)
	}
}

func (o *blockStatsObserver) ObservePipelineHandoffPickup(chain MetricChain, waited time.Duration) {
	if o.observer != nil {
		o.observer.ObservePipelineHandoffPickup(chain, waited)
	}
}

func (o *blockStatsObserver) ObservePipelineOverlap(chain MetricChain, overlap time.Duration) {
	if o.observer != nil {
		o.observer.ObservePipelineOverlap(chain, overlap)
	}
}

func (o *blockStatsObserver) ObserveScheduleLateness(
	chain MetricChain,
	event ScheduleEvent,
	duration time.Duration,
) {
	if o.observer != nil {
		o.observer.ObserveScheduleLateness(chain, event, duration)
	}
}

func (o *blockStatsObserver) AddCollationWindowInflight(chain MetricChain, delta int) {
	if o.observer != nil {
		o.observer.AddCollationWindowInflight(chain, delta)
	}
}

func (o *blockStatsObserver) ObserveCollationWindow(observation WindowObservation) {
	if o.observer != nil {
		o.observer.ObserveCollationWindow(observation)
	}
}

// ValidationCoreObserver is the narrow local-acquisition part of validator
// metrics. Standalone collation leaves it nil.
type ValidationCoreObserver interface {
	ObserveValidationCoreStage(ValidationCoreStageObservation)
}

// PrometheusMetrics owns process-wide collator collectors. Observer returns a
// lightweight mode-bound view; one collector set can therefore serve both
// integrated and standalone roles without duplicate registration.
type PrometheusMetrics struct {
	builds             *prometheus.CounterVec
	buildsInflight     *prometheus.GaugeVec
	buildDuration      *prometheus.HistogramVec
	stageDuration      *prometheus.HistogramVec
	substageDuration   *prometheus.HistogramVec
	candidates         *prometheus.CounterVec
	productionDuration *prometheus.HistogramVec
	candidateSize      *prometheus.GaugeVec
	candidateSizeTotal *prometheus.CounterVec
	transactions       *prometheus.HistogramVec
	latestTransactions *prometheus.GaugeVec
	gasUsed            *prometheus.HistogramVec
	messages           *prometheus.HistogramVec
	latestMessages     *prometheus.GaugeVec
	outQueueMessages   *prometheus.HistogramVec
	externalBatches    *prometheus.HistogramVec
	externalStops      *prometheus.CounterVec
	queueCleanupStops  *prometheus.CounterVec
	queueCleaned       *prometheus.HistogramVec
	loadClasses        *prometheus.CounterVec
	overloads          *prometheus.CounterVec
	deadlineEvents     *prometheus.CounterVec
	retries            *prometheus.CounterVec
	alarms             *prometheus.CounterVec
	pipelineHandoffs   *prometheus.CounterVec
	handoffPickup      *prometheus.HistogramVec
	pipelineOverlap    *prometheus.HistogramVec
	scheduleLateness   *prometheus.HistogramVec
	windowsInflight    *prometheus.GaugeVec
	windows            *prometheus.CounterVec
	windowDuration     *prometheus.HistogramVec
}

// NewPrometheusMetrics registers all collator collectors as one atomic set.
func NewPrometheusMetrics(registry MetricsRegistry) (*PrometheusMetrics, error) {
	if registry == nil {
		return nil, nil
	}

	namespace := registry.Namespace()
	durations := []float64{
		0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
		0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	}
	counts := []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 4_096, 16_384, 65_536, 262_144}

	metrics := &PrometheusMetrics{
		builds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "builds_total",
			Help: "Candidate build attempts by collator mode, chain, and terminal result.",
		}, []string{"mode", "chain", "result"}),
		buildsInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "collator", Name: "builds_inflight",
			Help: "Candidate builds currently acquiring inputs or collating.",
		}, []string{"mode", "chain"}),
		buildDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "build_duration_seconds",
			Help: "Wall-clock candidate build duration from producer future creation through acquisition and serialization.", Buckets: durations,
		}, []string{"mode", "chain", "result"}),
		stageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "stage_duration_seconds",
			Help: "Wall-clock duration of non-overlapping candidate production stages.", Buckets: durations,
		}, []string{"mode", "chain", "stage"}),
		substageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "substage_duration_seconds",
			Help:    "Wall-clock duration of spans nested inside one production stage. Substages of a stage may overlap each other and do not sum to the stage.",
			Buckets: durations,
		}, []string{"mode", "chain", "stage", "substage"}),
		candidates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidates_total",
			Help: "Newly signed candidates by collator mode, chain, and kind.",
		}, []string{"mode", "chain", "kind"}),
		productionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_production_duration_seconds",
			Help: "Wall-clock duration from candidate production start through persistence, commit, and emission.", Buckets: durations,
		}, []string{"mode", "chain", "kind", "result"}),
		// A gauge, not a histogram, and the reason is specific. Payload sizes
		// live between 0.5 and 2 MiB, which a power-of-two ladder covers with a
		// SINGLE bucket from 1 to 2 MiB, so histogram_quantile had to
		// interpolate across it and reported a p95 pinned near the bucket
		// ceiling. Measured against the raw producer log over one loaded window:
		// the true p95 was 1.396 MiB and the largest block of the window was
		// 1.472 MiB, while the panel drawn from the buckets read 1.892 MiB —
		// 35% high, and flat, which is exactly the reading that made a block
		// look like it was pressed against the 2 MiB limit when it was not.
		// Finer buckets would have moved the error rather than removed it, since
		// the interesting range is precisely where a block limit is enforced and
		// a quantile there must be exact.
		//
		// The gauge holds the last payload emitted for its chain, origin and
		// part, so a dashboard reads real bytes and quantile_over_time works on
		// values instead of bucket edges. What it cannot do is total, so the
		// counter beside it carries the exact sum; the two together answer both
		// "how big was the last one" and "how many bytes per message did this
		// producer spend", which is what the bucket _sum used to answer.
		candidateSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_size_bytes",
			Help: "Size of the most recent candidate block and collated-data payload, by whether this node produced or checked the block.",
		}, []string{"mode", "chain", "origin", "part"}),
		candidateSizeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_size_bytes_total",
			Help: "Cumulative candidate block and collated-data payload bytes. Divide by the matching candidate_messages_total to compare what one producer spends per message against another.",
		}, []string{"mode", "chain", "origin", "part"}),
		transactions: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_transactions",
			Help:    "Transactions in a non-empty candidate, by whether this node produced or checked the block.",
			Buckets: counts,
		}, []string{"mode", "chain", "origin"}),
		latestTransactions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_latest_transactions",
			Help: "Transactions in the latest candidate produced by this process.",
		}, []string{"mode", "chain"}),
		gasUsed: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_gas_used",
			Help: "Gas used by a produced non-empty candidate.", Buckets: prometheus.ExponentialBuckets(1_000, 4, 13),
		}, []string{"mode", "chain"}),
		messages: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_messages",
			Help:    "External and imported internal messages in a candidate, by whether this node produced or checked the block.",
			Buckets: counts,
		}, []string{"mode", "chain", "origin", "kind"}),
		latestMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_latest_messages",
			Help: "External and imported internal messages in the latest candidate produced by this process.",
		}, []string{"mode", "chain", "kind"}),
		outQueueMessages: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_out_queue_messages",
			Help: "Outbound queue size reported after producing a candidate.", Buckets: counts,
		}, []string{"mode", "chain"}),
		externalBatches: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "external_batches",
			Help: "Ready external-message batches consumed by a collation.", Buckets: counts,
		}, []string{"mode", "chain"}),
		externalStops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "external_stop_total",
			Help: "External-message processing stop reasons.",
		}, []string{"mode", "chain", "reason"}),
		queueCleanupStops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "queue_cleanup_stop_total",
			Help: "Outbound queue cleanup stop reasons.",
		}, []string{"mode", "chain", "reason"}),
		queueCleaned: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "candidate_queue_cleaned_messages",
			Help: "Outbound queue entries dequeued by the cleanup phase.", Buckets: counts,
		}, []string{"mode", "chain"}),
		loadClasses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "load_class_total",
			Help: "Produced candidates by final block load class.",
		}, []string{"mode", "chain", "class"}),
		overloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "overload_total",
			Help: "Produced candidates by overload cause.",
		}, []string{"mode", "chain", "reason"}),
		deadlineEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "deadline_events_total",
			Help: "Soft and hard collation deadline decisions.",
		}, []string{"mode", "chain", "deadline", "action"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "retries_total",
			Help: "Retried producer windows by bounded failure reason.",
		}, []string{"mode", "chain", "reason"}),
		alarms: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "alarms_total",
			Help: "Producer faults an operator must act on: a shipped block with a knowingly short collated proof, or a slot lost because the mandatory own-shard dequeue does not fit.",
		}, []string{"mode", "chain", "alarm"}),
		pipelineHandoffs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "pipeline_handoffs_total",
			Help: "Predecessors handed to the producer before they were committed, by what became of each: started, the gate that declined it, or the reason it was abandoned.",
		}, []string{"mode", "chain", "outcome"}),
		handoffPickup: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "pipeline_handoff_pickup_seconds",
			Help: "Time a started successor waited parked before the producer reached its slot and adopted it.", Buckets: durations,
		}, []string{"mode", "chain"}),
		pipelineOverlap: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "pipeline_overlap_seconds",
			Help: "How long a successor ran beside the rest of its predecessor's block, from the handoff to the end of that build or the withdrawal of the offer, whichever came first. This is the time the pipeline actually saved.", Buckets: durations,
		}, []string{"mode", "chain"}),
		scheduleLateness: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "schedule_lateness_seconds",
			Help: "Positive lateness at masterchain or continuation shard build starts and any broadcast slot boundary.", Buckets: durations,
		}, []string{"mode", "chain", "event"}),
		windowsInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "collator", Name: "windows_inflight",
			Help: "Leader windows currently being produced.",
		}, []string{"mode", "chain"}),
		windows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "collator", Name: "windows_total",
			Help: "Completed producer windows by terminal result.",
		}, []string{"mode", "chain", "result"}),
		windowDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "collator", Name: "window_duration_seconds",
			Help: "Wall-clock producer window duration, including retries.", Buckets: durations,
		}, []string{"mode", "chain", "result"}),
	}

	if err := registry.RegisterCollector(collectorSet{
		metrics.builds, metrics.buildsInflight, metrics.buildDuration, metrics.stageDuration, metrics.substageDuration,
		metrics.candidates, metrics.productionDuration, metrics.candidateSize, metrics.candidateSizeTotal,
		metrics.transactions,
		metrics.latestTransactions, metrics.gasUsed, metrics.messages, metrics.latestMessages,
		metrics.outQueueMessages,
		metrics.externalBatches, metrics.externalStops, metrics.queueCleanupStops, metrics.queueCleaned,
		metrics.loadClasses, metrics.overloads,
		metrics.deadlineEvents, metrics.retries, metrics.alarms,
		metrics.pipelineHandoffs, metrics.handoffPickup, metrics.pipelineOverlap,
		metrics.scheduleLateness, metrics.windowsInflight,
		metrics.windows, metrics.windowDuration,
	}); err != nil {
		return nil, err
	}

	return metrics, nil
}

type collectorSet []prometheus.Collector

func (s collectorSet) Describe(ch chan<- *prometheus.Desc) {
	for _, collector := range s {
		collector.Describe(ch)
	}
}

func (s collectorSet) Collect(ch chan<- prometheus.Metric) {
	for _, collector := range s {
		collector.Collect(ch)
	}
}

type prometheusObserver struct {
	// metrics, modeLabel, chainLabels and stageLabels serve the substage
	// histogram, whose substage label is open-ended and so cannot be
	// pre-resolved into a fixed array like the stage observers below.
	metrics     *PrometheusMetrics
	modeLabel   string
	chainLabels [metricChainCount]string
	stageLabels [collationStageCount]string

	builds             [metricChainCount][collationResultCount]prometheus.Counter
	buildsInflight     [metricChainCount]prometheus.Gauge
	buildDuration      [metricChainCount][collationResultCount]prometheus.Observer
	stageDuration      [metricChainCount][collationStageCount]prometheus.Observer
	candidates         [metricChainCount][candidateKindCount]prometheus.Counter
	productionDuration [metricChainCount][candidateKindCount][collationResultCount]prometheus.Observer
	candidateSize      [metricChainCount][candidateOriginCount][2]prometheus.Gauge
	candidateSizeTotal [metricChainCount][candidateOriginCount][2]prometheus.Counter
	transactions       [metricChainCount][candidateOriginCount]prometheus.Observer
	latestTransactions [metricChainCount]prometheus.Gauge
	gasUsed            [metricChainCount]prometheus.Observer
	messages           [metricChainCount][candidateOriginCount][2]prometheus.Observer
	latestMessages     [metricChainCount][2]prometheus.Gauge
	outQueueMessages   [metricChainCount]prometheus.Observer
	externalBatches    [metricChainCount]prometheus.Observer
	externalStops      [metricChainCount][6]prometheus.Counter
	queueCleanupStops  [metricChainCount][cleanupStopReasonCount]prometheus.Counter
	queueCleaned       [metricChainCount]prometheus.Observer
	loadClasses        [metricChainCount][6]prometheus.Counter
	overloads          [metricChainCount][5]prometheus.Counter
	deadlineEvents     [metricChainCount][collationDeadlineCount][deadlineActionCount]prometheus.Counter
	retries            [metricChainCount][productionRetryReasonCount]prometheus.Counter
	alarms             [metricChainCount][collationAlarmCount]prometheus.Counter
	pipelineHandoffs   [metricChainCount][pipelineHandoffOutcomeCount]prometheus.Counter
	handoffPickup      [metricChainCount]prometheus.Observer
	pipelineOverlap    [metricChainCount]prometheus.Observer
	scheduleLateness   [metricChainCount][scheduleEventCount]prometheus.Observer
	windowsInflight    [metricChainCount]prometheus.Gauge
	windows            [metricChainCount][collationResultCount]prometheus.Counter
	windowDuration     [metricChainCount][collationResultCount]prometheus.Observer
}

// Observer binds the mode label once, outside candidate processing.
func (m *PrometheusMetrics) Observer(mode MetricsMode) CollationObserver {
	if m == nil {
		return nil
	}
	if mode >= metricsModeCount {
		mode = MetricsModeValidator
	}

	o := &prometheusObserver{}
	modeLabel := [...]string{"validator", "standalone"}[mode]
	chains := [...]string{"masterchain", "shardchain"}
	results := [...]string{"success", "error", "canceled", "deadline", "not_ready"}
	stages := [...]string{
		"acquire_inputs", "prepare_state", "cleanup_out_queue",
		"execute_internal_messages", "execute_external_messages",
		"finalize_accounts", "build_state_update", "serialize_candidate",
		"finalize_candidate", "wait_external_messages",
		"resolve_candidate_state", "restore_candidate", "sign_candidate",
		"persist_candidate", "commit_candidate_state", "wait_broadcast_slot",
		"deliver_candidate",
		"processed_info", "claimed_local_cleanup", "flush_batches",
		"validation_closure", "serialize_block",
	}
	kinds := [...]string{"block", "empty", "recovered"}
	origins := [...]string{"collation", "validation"}
	o.metrics = m
	o.modeLabel = modeLabel
	o.chainLabels = chains
	o.stageLabels = stages
	for chain := MetricChain(0); chain < metricChainCount; chain++ {
		chainLabel := chains[chain]
		o.buildsInflight[chain] = m.buildsInflight.WithLabelValues(modeLabel, chainLabel)
		o.windowsInflight[chain] = m.windowsInflight.WithLabelValues(modeLabel, chainLabel)
		for result := CollationResult(0); result < collationResultCount; result++ {
			o.builds[chain][result] = m.builds.WithLabelValues(modeLabel, chainLabel, results[result])
			o.buildDuration[chain][result] = m.buildDuration.WithLabelValues(modeLabel, chainLabel, results[result])
			o.windows[chain][result] = m.windows.WithLabelValues(modeLabel, chainLabel, results[result])
			o.windowDuration[chain][result] = m.windowDuration.WithLabelValues(modeLabel, chainLabel, results[result])
		}
		for stage := CollationStage(0); stage < collationStageCount; stage++ {
			o.stageDuration[chain][stage] = m.stageDuration.WithLabelValues(modeLabel, chainLabel, stages[stage])
		}
		for kind := CandidateKind(0); kind < candidateKindCount; kind++ {
			o.candidates[chain][kind] = m.candidates.WithLabelValues(modeLabel, chainLabel, kinds[kind])
			for result := CollationResult(0); result < collationResultCount; result++ {
				o.productionDuration[chain][kind][result] = m.productionDuration.WithLabelValues(
					modeLabel, chainLabel, kinds[kind], results[result],
				)
			}
		}
		for origin, originLabel := range origins {
			o.candidateSize[chain][origin][0] = m.candidateSize.WithLabelValues(modeLabel, chainLabel, originLabel, "block")
			o.candidateSize[chain][origin][1] = m.candidateSize.WithLabelValues(modeLabel, chainLabel, originLabel, "collated")
			o.candidateSizeTotal[chain][origin][0] = m.candidateSizeTotal.WithLabelValues(modeLabel, chainLabel, originLabel, "block")
			o.candidateSizeTotal[chain][origin][1] = m.candidateSizeTotal.WithLabelValues(modeLabel, chainLabel, originLabel, "collated")
			o.transactions[chain][origin] = m.transactions.WithLabelValues(modeLabel, chainLabel, originLabel)
			o.messages[chain][origin][0] = m.messages.WithLabelValues(modeLabel, chainLabel, originLabel, "external")
			o.messages[chain][origin][1] = m.messages.WithLabelValues(modeLabel, chainLabel, originLabel, "internal")
		}
		o.latestTransactions[chain] = m.latestTransactions.WithLabelValues(modeLabel, chainLabel)
		o.latestMessages[chain][0] = m.latestMessages.WithLabelValues(modeLabel, chainLabel, "external")
		o.latestMessages[chain][1] = m.latestMessages.WithLabelValues(modeLabel, chainLabel, "internal")
		o.gasUsed[chain] = m.gasUsed.WithLabelValues(modeLabel, chainLabel)
		o.outQueueMessages[chain] = m.outQueueMessages.WithLabelValues(modeLabel, chainLabel)
		o.externalBatches[chain] = m.externalBatches.WithLabelValues(modeLabel, chainLabel)
		for reason, label := range [...]string{"unknown", "ready_drained", "soft_limit", "deadline", "attempt_limit", "queue_brake"} {
			o.externalStops[chain][reason] = m.externalStops.WithLabelValues(modeLabel, chainLabel, label)
		}
		for reason := CleanupStopReason(0); reason < cleanupStopReasonCount; reason++ {
			o.queueCleanupStops[chain][reason] = m.queueCleanupStops.WithLabelValues(
				modeLabel, chainLabel, reason.String(),
			)
		}
		o.queueCleaned[chain] = m.queueCleaned.WithLabelValues(modeLabel, chainLabel)
		for class, label := range [...]string{"underload", "normal", "soft", "medium", "hard", "unknown"} {
			o.loadClasses[chain][class] = m.loadClasses.WithLabelValues(modeLabel, chainLabel, label)
		}
		for reason, label := range [...]string{"none", "block_limit", "force_split_queue", "long_collation", "unknown"} {
			o.overloads[chain][reason] = m.overloads.WithLabelValues(modeLabel, chainLabel, label)
		}
		for deadline, deadlineLabel := range [...]string{"soft", "hard"} {
			for action, actionLabel := range [...]string{"wait", "emit_empty", "abort", "wait_no_empty"} {
				o.deadlineEvents[chain][deadline][action] = m.deadlineEvents.WithLabelValues(
					modeLabel, chainLabel, deadlineLabel, actionLabel,
				)
			}
		}
		for reason, label := range [...]string{"not_ready", "other"} {
			o.retries[chain][reason] = m.retries.WithLabelValues(modeLabel, chainLabel, label)
		}
		for alarm := CollationAlarm(0); alarm < collationAlarmCount; alarm++ {
			o.alarms[chain][alarm] = m.alarms.WithLabelValues(modeLabel, chainLabel, alarm.String())
		}
		for outcome := PipelineHandoffOutcome(0); outcome < pipelineHandoffOutcomeCount; outcome++ {
			o.pipelineHandoffs[chain][outcome] = m.pipelineHandoffs.WithLabelValues(
				modeLabel, chainLabel, outcome.String(),
			)
		}
		o.handoffPickup[chain] = m.handoffPickup.WithLabelValues(modeLabel, chainLabel)
		o.pipelineOverlap[chain] = m.pipelineOverlap.WithLabelValues(modeLabel, chainLabel)
		for event := ScheduleEvent(0); event < scheduleEventCount; event++ {
			o.scheduleLateness[chain][event] = m.scheduleLateness.WithLabelValues(modeLabel, chainLabel, event.String())
		}
	}

	return o
}

func (o *prometheusObserver) AddCollationBuildInflight(chain MetricChain, delta int) {
	if delta != 0 {
		o.buildsInflight[boundedChain(chain)].Add(float64(delta))
	}
}

func (o *prometheusObserver) ObserveCollationBuild(observation CollationBuildObservation) {
	chain := boundedChain(observation.Chain)
	result := boundedCollationResult(observation.Result)
	o.builds[chain][result].Inc()
	o.buildDuration[chain][result].Observe(nonNegativeSeconds(observation.Duration))
}

func (o *prometheusObserver) ObserveCollationStage(observation CollationStageObservation) {
	stage := observation.Stage
	if stage >= collationStageCount {
		return
	}
	if observation.Substage != "" {
		o.metrics.substageDuration.WithLabelValues(
			o.modeLabel, o.chainLabels[boundedChain(observation.Chain)], o.stageLabels[stage], observation.Substage,
		).Observe(nonNegativeSeconds(observation.Duration))
		return
	}
	o.stageDuration[boundedChain(observation.Chain)][stage].Observe(nonNegativeSeconds(observation.Duration))
}

func (o *prometheusObserver) ObserveCollationCandidate(observation CandidateObservation) {
	chain := boundedChain(observation.Chain)
	origin := observation.Origin
	if origin >= candidateOriginCount {
		origin = CandidateOriginCollation
	}
	kind := observation.Kind
	if kind >= candidateKindCount {
		kind = CandidateKindBlock
	}
	if origin == CandidateOriginCollation {
		// Counts candidates this node produced. A validated block was produced
		// by someone else and would double-count the network's block rate.
		o.candidates[chain][kind].Inc()
		shape := observation.Shape
		o.latestTransactions[chain].Set(float64(shape.Transactions))
		o.latestMessages[chain][0].Set(float64(shape.ExternalMessages))
		o.latestMessages[chain][1].Set(float64(shape.InternalMessages))
	}
	blockBytes := float64(max(observation.BlockBytes, 0))
	collatedBytes := float64(max(observation.CollatedBytes, 0))
	o.candidateSize[chain][origin][0].Set(blockBytes)
	o.candidateSize[chain][origin][1].Set(collatedBytes)
	o.candidateSizeTotal[chain][origin][0].Add(blockBytes)
	o.candidateSizeTotal[chain][origin][1].Add(collatedBytes)
	if kind != CandidateKindBlock {
		return
	}
	shape := observation.Shape
	o.transactions[chain][origin].Observe(float64(shape.Transactions))
	o.messages[chain][origin][0].Observe(float64(shape.ExternalMessages))
	o.messages[chain][origin][1].Observe(float64(shape.InternalMessages))
	if origin != CandidateOriginCollation {
		// Everything below is a producer's own bookkeeping — gas it metered,
		// how long it waited for externals, why it stopped. A validator sees
		// none of it in the block.
		return
	}

	stats := observation.Stats
	o.gasUsed[chain].Observe(float64(stats.GasUsed))
	o.outQueueMessages[chain].Observe(float64(stats.OutQueueSize))
	o.externalBatches[chain].Observe(float64(stats.ExternalBatches))
	o.externalStops[chain][boundedExternalStop(stats.ExternalStop)].Inc()
	o.queueCleanupStops[chain][boundedCleanupStop(stats.QueueCleanupStop)].Inc()
	o.queueCleaned[chain].Observe(float64(stats.QueueCleaned))
	o.loadClasses[chain][boundedLoadClass(stats.Load)].Inc()
	o.overloads[chain][boundedOverload(stats.OverloadReason)].Inc()
}

func (o *prometheusObserver) ObserveCandidateProduction(observation CandidateProductionObservation) {
	chain := boundedChain(observation.Chain)
	kind := observation.Kind
	if kind >= candidateKindCount {
		kind = CandidateKindBlock
	}
	result := boundedCollationResult(observation.Result)
	o.productionDuration[chain][kind][result].Observe(nonNegativeSeconds(observation.Duration))
}

func (o *prometheusObserver) ObserveCollationDeadline(
	chain MetricChain,
	deadline CollationDeadline,
	action DeadlineAction,
) {
	if deadline >= collationDeadlineCount {
		deadline = CollationDeadlineHard
	}
	if action >= deadlineActionCount {
		action = DeadlineActionAbort
	}
	o.deadlineEvents[boundedChain(chain)][deadline][action].Inc()
}

func (o *prometheusObserver) ObserveCollationRetry(chain MetricChain, reason ProductionRetryReason) {
	if reason >= productionRetryReasonCount {
		reason = ProductionRetryOther
	}
	o.retries[boundedChain(chain)][reason].Inc()
}

func (o *prometheusObserver) ObserveCollationAlarm(chain MetricChain, alarm CollationAlarm) {
	if alarm >= collationAlarmCount {
		return
	}
	o.alarms[boundedChain(chain)][alarm].Inc()
}

func (o *prometheusObserver) ObservePipelineHandoff(chain MetricChain, outcome PipelineHandoffOutcome) {
	if outcome >= pipelineHandoffOutcomeCount {
		return
	}
	o.pipelineHandoffs[boundedChain(chain)][outcome].Inc()
}

func (o *prometheusObserver) ObservePipelineHandoffPickup(chain MetricChain, waited time.Duration) {
	o.handoffPickup[boundedChain(chain)].Observe(nonNegativeSeconds(waited))
}

func (o *prometheusObserver) ObservePipelineOverlap(chain MetricChain, overlap time.Duration) {
	o.pipelineOverlap[boundedChain(chain)].Observe(nonNegativeSeconds(overlap))
}

func (o *prometheusObserver) ObserveScheduleLateness(
	chain MetricChain,
	event ScheduleEvent,
	duration time.Duration,
) {
	if event >= scheduleEventCount {
		event = ScheduleEventBuildStart
	}
	o.scheduleLateness[boundedChain(chain)][event].Observe(nonNegativeSeconds(duration))
}

func (o *prometheusObserver) AddCollationWindowInflight(chain MetricChain, delta int) {
	if delta != 0 {
		o.windowsInflight[boundedChain(chain)].Add(float64(delta))
	}
}

func (o *prometheusObserver) ObserveCollationWindow(observation WindowObservation) {
	chain := boundedChain(observation.Chain)
	result := boundedCollationResult(observation.Result)
	o.windows[chain][result].Inc()
	o.windowDuration[chain][result].Observe(nonNegativeSeconds(observation.Duration))
}

func metricChain(masterchain bool) MetricChain {
	if masterchain {
		return MetricChainMasterchain
	}
	return MetricChainShardchain
}

func boundedChain(chain MetricChain) MetricChain {
	if chain >= metricChainCount {
		return MetricChainShardchain
	}
	return chain
}

func boundedCollationResult(result CollationResult) CollationResult {
	if result >= collationResultCount {
		return CollationResultError
	}
	return result
}

func boundedExternalStop(reason ExternalStopReason) int {
	if reason > ExternalStopQueueBrake {
		return 0
	}
	return int(reason)
}

// boundedCleanupStop keeps an unexpected reason out of the label space rather
// than dropping the observation: a budget nobody can see is a budget nobody
// will ever tune.
func boundedCleanupStop(reason CleanupStopReason) CleanupStopReason {
	if reason >= cleanupStopReasonCount {
		return CleanupStopExhausted
	}
	return reason
}

func boundedLoadClass(class LoadClass) int {
	if class > LoadHard {
		return 5
	}
	return int(class)
}

func boundedOverload(reason OverloadReason) int {
	if reason > OverloadLongCollation {
		return 4
	}
	return int(reason)
}

func nonNegativeSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}

func (s *Service) metricStageStarted() time.Time {
	if s.opts.Observer == nil {
		return time.Time{}
	}

	return time.Now()
}

func (s *Service) observePipelineHandoff(chain MetricChain, outcome PipelineHandoffOutcome) {
	if s.opts.Observer == nil {
		return
	}
	s.opts.Observer.ObservePipelineHandoff(chain, outcome)
}

func (s *Service) observePipelineHandoffPickup(chain MetricChain, request BuildRequest) {
	if s.opts.Observer == nil || request.PreviousPending == nil {
		return
	}
	s.opts.Observer.ObservePipelineHandoffPickup(chain, time.Since(request.PreviousPending.handoffAt))
}

func (a *LocalAcquisition) observePipelineOverlap(chain MetricChain, overlap time.Duration) {
	if a.collationObserver == nil {
		return
	}
	a.collationObserver.ObservePipelineOverlap(chain, overlap)
}

func (s *Service) observeMetricStage(
	chain MetricChain,
	stage CollationStage,
	started time.Time,
) {
	if s.opts.Observer == nil {
		return
	}

	s.opts.Observer.ObserveCollationStage(CollationStageObservation{
		Chain:    chain,
		Stage:    stage,
		Duration: time.Since(started),
	})
}

func (s *Service) observeCandidateProduction(
	chain MetricChain,
	kind CandidateKind,
	started time.Time,
	err error,
) {
	if s.opts.Observer == nil {
		return
	}

	s.opts.Observer.ObserveCandidateProduction(CandidateProductionObservation{
		Chain:    chain,
		Kind:     kind,
		Result:   collationResult(err),
		Duration: time.Since(started),
	})
}

func collationResult(err error) CollationResult {
	if err == nil {
		return CollationResultSuccess
	}
	if errors.Is(err, ErrAcquisitionNotReady) {
		return CollationResultNotReady
	}
	if errors.Is(err, context.Canceled) {
		return CollationResultCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errHardBuildDeadline) {
		return CollationResultDeadline
	}

	return CollationResultError
}

func productionRetryReason(err error) ProductionRetryReason {
	if errors.Is(err, ErrAcquisitionNotReady) {
		return ProductionRetryNotReady
	}

	return ProductionRetryOther
}
