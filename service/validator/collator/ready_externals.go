package collator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// readyExternalSource is the whole of what the wave loop does to the external
// pool: take whatever is admitted right now, or park until more arrives. It is
// an interface rather than *msgpool.ExternalStream so a test can decide WHEN a
// wave becomes visible by control flow instead of by sleeping.
//
// That distinction is not cosmetic. Whether wave two is served from TakeReady —
// in the same pass, before processNewMessages — or from Next, after it, changes
// the logical time the wave-two transaction runs at and therefore the bytes of
// the produced block, and the two shapes report IDENTICAL Stats counters
// (ExternalBatches, ExternalIncluded, ImmediateDelivered, EnqueuedMessages,
// ExternalStop all equal). Nothing observable downstream separates them, so a
// golden driven by a real pool plus a sleep pins whichever one the scheduler
// happened to produce. *msgpool.ExternalStream satisfies this as it stands;
// production wiring is unchanged.
type readyExternalSource interface {
	TakeReady(limit int) []msgpool.ExternalSnapshot
	Next(ctx context.Context, limit int) ([]msgpool.ExternalSnapshot, error)
}

type readyExternalStats struct {
	wait    time.Duration
	batches uint32
	stop    ExternalStopReason
	// attempts counts the collation attempts retryUnderSizeLimit ran. Only
	// attempt zero observes the pool, so wait and batches belong to it alone and
	// are carried unchanged into any replay. Without this counter a failure on a
	// replay attempt is indistinguishable in the field log from a failure inside
	// the live wave loop -- exactly the ambiguity that left five of the field's
	// order-violation events unattributable.
	attempts uint32
	// failedAt names the phase of the live wave loop an attempt died in. Every
	// error return inside buildShardReadyAttempt / buildMasterReadyAttempt sets
	// it; stop is written by a defer so it survives those returns too.
	//
	// Before this existed, stop was assigned only AFTER the loop, i.e. only on
	// success, so every one of the 71 field order-violation events logged
	// external_stop="unknown" — the zero value's String() — and the field looked
	// like three phenomena when it was one. "unknown" on a failure carried no
	// information at all, and it excluded the retry-replay hypothesis only by
	// accident. This one string would have attributed all 71 on sight.
	failedAt string
	// shape is the collation state at the moment the attempt ended. These
	// counters live on the candidate's Stats, and the candidate is nil on every
	// failure — which is why block_bytes and collated_bytes were 0 on all 71
	// events and no field record said how many externals were pre-admitted or
	// how far the internal import had got. Lifting them here is the same fix as
	// the earlier "lineage walk stats survive error returns" one, and with
	// metrics disabled on the field nodes the zerolog record is the only channel.
	shape readyExternalShape
}

// readyExternalShape is the minimum needed to reconstruct which route an attempt
// took without a candidate to read it off.
type readyExternalShape struct {
	// preAdmitted is len(req.Externals) — the batch that made external_batches
	// == 1 reachable with a real wait, which was read in the field as an
	// unreachable third path when it is the ordinary route with an empty
	// pre-admitted snapshot.
	preAdmitted        int
	internalsImported  uint32
	immediateDelivered uint32
	enqueuedMessages   uint32
	startLT            uint64
	lastProcLT         uint64
	valid              bool
}

func (s *readyExternalStats) observe(c *collation, preAdmitted int) {
	if c == nil {
		return
	}
	s.shape = readyExternalShape{
		preAdmitted:        preAdmitted,
		internalsImported:  c.stats.InternalsImported,
		immediateDelivered: c.stats.ImmediateDelivered,
		enqueuedMessages:   c.stats.EnqueuedMessages,
		startLT:            c.header.StartLt,
		lastProcLT:         c.lastProcLT,
		valid:              true,
	}
}

// buildShardWithReadyExternals runs the reference shardchain schedule: prepare
// the candidate early, drain every external already admitted by ingress,
// process generated messages, and then wait for more ready messages until the
// slot boundary. A serialized-size retry never waits or observes the pool a
// second time: attempt one replays the frozen transcript, while attempt two
// skips externals exactly like the reference collator.
// startedAt is when the node began assembling this candidate, before the
// acquisition that waits for predecessor states. Together with a non-zero
// waitUntil it arms the CPU-bound split heuristic; either being zero leaves it
// inert, which is what the deterministic tests rely on.
func (b *Builder) buildShardWithReadyExternals(
	ctx context.Context,
	req ShardRequest,
	stream readyExternalSource,
	processUntil time.Time,
	waitUntil time.Time,
	batchLimit int,
	startedAt time.Time,
) (*Candidate, readyExternalStats, error) {
	var live readyExternalStats
	timer := req.assembly.start(CollationStagePrepareState)
	defer func() {
		timer.stop()
	}()
	if batchLimit <= 0 {
		return nil, live, fmt.Errorf("%w: ready external batch limit must be positive", ErrInvalidInput)
	}
	if req.MaxExternalAttempts <= 0 {
		return nil, live, fmt.Errorf("%w: external attempt limit must be positive", ErrInvalidInput)
	}
	common := shardCollationRequest(req)
	if err := validateCollationRequest(&common); err != nil {
		return nil, live, err
	}
	timer.stop()

	transcript := make([]ExternalInput, 0, len(req.Externals))
	attempt := 0
	// A size retry re-enters collation, so the body span restarts with it while
	// the span that began before acquisition does not — the same asymmetry the
	// reference collator gets from a member initializer and a per-call
	// assignment.
	armed := !startedAt.IsZero() && !waitUntil.IsZero()
	attemptPace := func() collationPace {
		if !armed {
			return collationPace{}
		}
		return collationPace{
			started:      startedAt,
			body:         time.Now(),
			externalWait: func() time.Duration { return live.wait },
		}
	}
	candidate, err := retryUnderSizeLimit(func(narrowing sizeBudgetCap) (*Candidate, error) {
		current := attempt
		attempt++
		live.attempts++
		if current == 0 {
			return b.buildShardReadyAttempt(
				ctx,
				req,
				narrowing,
				stream,
				processUntil,
				waitUntil,
				batchLimit,
				&transcript,
				&live,
				attemptPace(),
			)
		}

		replay := req
		if current < 2 {
			replay.Externals = transcript
		} else {
			replay.Externals = nil
		}
		return b.buildShardAttemptPaced(ctx, replay, narrowing, attemptPace())
	})
	if candidate != nil {
		candidate.Stats.ExternalWait = live.wait
		candidate.Stats.ExternalBatches = live.batches
		candidate.Stats.ExternalStop = live.stop
	}
	if err == nil {
		err = sealBuiltCandidate(candidate)
	}

	return candidate, live, err
}

func (b *Builder) buildShardReadyAttempt(
	ctx context.Context,
	req ShardRequest,
	narrowing sizeBudgetCap,
	stream readyExternalSource,
	processUntil time.Time,
	waitUntil time.Time,
	batchLimit int,
	transcript *[]ExternalInput,
	live *readyExternalStats,
	pace collationPace,
) (*Candidate, error) {
	c, err := b.prepareShardPhases(ctx, req, narrowing)
	if err != nil {
		live.failedAt = externalPhasePrepare
		return nil, err
	}
	c.pace = pace

	waitCtx := ctx
	cancel := func() {}
	if !waitUntil.IsZero() {
		waitCtx, cancel = context.WithDeadline(ctx, waitUntil)
	}
	defer cancel()

	stop := ExternalStopUnknown
	// Written by a defer, not after the loop. Assigned at the tail it was only
	// ever set on success, which is what made external_stop="unknown" mean
	// nothing but "it failed" on all 71 field events. The shape snapshot goes the
	// same way, because the candidate that carries those counters is nil on every
	// failing return.
	defer func() {
		live.stop = stop
		live.observe(c, len(req.Externals))
	}()

	// The batch timers below are stopped by hand, unlike the phase timers in
	// build.go and master.go, and that is not an oversight to tidy up. They are
	// declared inside loop bodies, where a deferred stop would not run at the
	// end of the iteration that started it: every iteration's timer would pile
	// up and close together when the whole collation returned, charging the
	// external stage once per batch for the entire remaining build.
	if len(req.Externals) > 0 {
		live.batches++
		timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
		result, processErr := c.processExternalBatch(req.Externals, processUntil)
		timer.stop()
		*transcript = append(*transcript, req.Externals[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			live.failedAt = externalPhasePreAdmittedBatch
			return nil, err
		}
	}
	for {
		for stop == ExternalStopUnknown {
			timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
			snapshots := stream.TakeReady(batchLimit)
			if len(snapshots) == 0 {
				timer.stop()
				break
			}
			inputs, prepareErr := prepareExternalSnapshots(snapshots)
			if prepareErr != nil {
				timer.stop()
				live.failedAt = externalPhaseTakeReadyBatch
				return nil, prepareErr
			}
			live.batches++
			result, processErr := c.processExternalBatch(inputs, processUntil)
			timer.stop()
			*transcript = append(*transcript, inputs[:result.consumed]...)
			stop, err = result.stop, processErr
			if err != nil {
				live.failedAt = externalPhaseTakeReadyBatch
				return nil, err
			}
		}

		timer := c.req.assembly.start(CollationStageExecuteInternalMessages)
		if err = c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete()); err != nil {
			timer.stop()
			// The phase every one of the field's seven external_batches==1 events
			// died in, and the only one that can raise the order violation:
			// advanceProcessedBound's "generated" caller is deliverImmediate, which
			// is reachable from here alone.
			live.failedAt = externalPhaseProcessNewMessages
			return nil, err
		}
		if stop != ExternalStopUnknown {
			timer.stop()
			break
		}
		c.updateCollatedEstimate()
		if !c.limits.fits(LoadSoft) {
			timer.stop()
			stop = ExternalStopSoftLimit
			break
		}
		timer.stop()
		if waitUntil.IsZero() {
			stop = ExternalStopReadyDrained
			break
		}

		started := time.Now()
		snapshots, nextErr := stream.Next(waitCtx, batchLimit)
		live.wait += time.Since(started)
		if nextErr != nil {
			if errors.Is(nextErr, context.DeadlineExceeded) && errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				stop = ExternalStopDeadline
				break
			}
			live.failedAt = externalPhaseWait
			return nil, nextErr
		}
		timer = c.req.assembly.start(CollationStageExecuteExternalMessages)
		inputs, prepareErr := prepareExternalSnapshots(snapshots)
		if prepareErr != nil {
			timer.stop()
			live.failedAt = externalPhaseWaitedBatch
			return nil, prepareErr
		}
		live.batches++
		result, processErr := c.processExternalBatch(inputs, processUntil)
		timer.stop()
		*transcript = append(*transcript, inputs[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			live.failedAt = externalPhaseWaitedBatch
			return nil, err
		}
	}

	candidate, finishErr := c.finishShard()
	if finishErr != nil {
		live.failedAt = externalPhaseFinish
	}
	return candidate, finishErr
}

func prepareExternalSnapshots(snapshots []msgpool.ExternalSnapshot) ([]ExternalInput, error) {
	inputs := make([]ExternalInput, len(snapshots))
	for i := range snapshots {
		input, err := NewExternalInput(snapshots[i])
		if err != nil {
			return nil, fmt.Errorf("prepare ready external message %d: %w", i, err)
		}
		inputs[i] = input
	}

	return inputs, nil
}

// buildMasterWithReadyExternals drains the immutable ready snapshot installed
// at masterchain collation start. It never waits for later admissions. The
// first size retry replays only the messages actually considered; the second
// skips externals exactly like the reference collator.
func (b *Builder) buildMasterWithReadyExternals(
	ctx context.Context,
	req MasterRequest,
	stream readyExternalSource,
	processUntil time.Time,
	batchLimit int,
) (*Candidate, readyExternalStats, error) {
	var live readyExternalStats
	timer := req.assembly.start(CollationStagePrepareState)
	defer func() {
		timer.stop()
	}()
	if batchLimit <= 0 {
		return nil, live, fmt.Errorf("%w: ready external batch limit must be positive", ErrInvalidInput)
	}
	if req.MaxExternalAttempts <= 0 {
		return nil, live, fmt.Errorf("%w: external attempt limit must be positive", ErrInvalidInput)
	}
	common := masterCollationRequest(req)
	if err := validateCollationRequest(&common); err != nil {
		return nil, live, err
	}
	timer.stop()

	transcript := make([]ExternalInput, 0, len(req.Externals))
	attempt := 0
	candidate, err := retryUnderSizeLimit(func(narrowing sizeBudgetCap) (*Candidate, error) {
		current := attempt
		attempt++
		live.attempts++
		if current == 0 {
			return b.buildMasterReadyAttempt(
				ctx,
				req,
				narrowing,
				stream,
				processUntil,
				batchLimit,
				&transcript,
				&live,
			)
		}

		replay := req
		if current < 2 {
			replay.Externals = transcript
		} else {
			replay.Externals = nil
		}
		return b.buildMasterAttempt(ctx, replay, narrowing)
	})
	if candidate != nil {
		candidate.Stats.ExternalBatches = live.batches
		candidate.Stats.ExternalStop = live.stop
	}
	if err == nil {
		err = sealBuiltCandidate(candidate)
	}

	return candidate, live, err
}

func (b *Builder) buildMasterReadyAttempt(
	ctx context.Context,
	req MasterRequest,
	narrowing sizeBudgetCap,
	stream readyExternalSource,
	processUntil time.Time,
	batchLimit int,
	transcript *[]ExternalInput,
	live *readyExternalStats,
) (*Candidate, error) {
	c, err := b.prepareMasterPhases(ctx, req, narrowing)
	if err != nil {
		live.failedAt = externalPhasePrepare
		return nil, err
	}

	stop := ExternalStopUnknown
	defer func() {
		live.stop = stop
		live.observe(c, len(req.Externals))
	}()

	if len(req.Externals) > 0 {
		live.batches++
		timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
		result, processErr := c.processExternalBatch(req.Externals, processUntil)
		timer.stop()
		*transcript = append(*transcript, req.Externals[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			live.failedAt = externalPhasePreAdmittedBatch
			return nil, err
		}
	}
	for stop == ExternalStopUnknown {
		timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
		snapshots := stream.TakeReady(batchLimit)
		if len(snapshots) == 0 {
			timer.stop()
			stop = ExternalStopReadyDrained
			break
		}
		inputs, prepareErr := prepareExternalSnapshots(snapshots)
		if prepareErr != nil {
			timer.stop()
			live.failedAt = externalPhaseTakeReadyBatch
			return nil, prepareErr
		}
		live.batches++
		result, processErr := c.processExternalBatch(inputs, processUntil)
		timer.stop()
		*transcript = append(*transcript, inputs[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			live.failedAt = externalPhaseTakeReadyBatch
			return nil, err
		}
	}

	candidate, finishErr := c.finishMaster(req)
	if finishErr != nil {
		live.failedAt = externalPhaseFinish
	}
	return candidate, finishErr
}

// The phases an attempt can die in. They are strings and not an enum because
// they only ever reach a log field, and a new one must never require a String()
// method to be remembered — the ExternalStopReason enum's forgotten default
// branch is exactly how "unknown" came to mean two different things.
const (
	externalPhasePrepare            = "prepare"
	externalPhasePreAdmittedBatch   = "pre_admitted_batch"
	externalPhaseTakeReadyBatch     = "take_ready_batch"
	externalPhaseProcessNewMessages = "process_new_messages"
	externalPhaseWait               = "wait"
	externalPhaseWaitedBatch        = "waited_batch"
	externalPhaseFinish             = "finish"
)
