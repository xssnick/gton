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

// readyExternalAttempt owns the bookkeeping shared by every external batch in
// one collation attempt. The shard and masterchain loops retain their distinct
// schedules; only admission accounting and transcript capture live here.
type readyExternalAttempt struct {
	transcript *[]ExternalInput
	live       *readyExternalStats
	stop       ExternalStopReason
}

func (a *readyExternalAttempt) record(
	inputs []ExternalInput,
	result externalBatchResult,
	err error,
	failurePhase string,
) {
	a.live.batches++
	*a.transcript = append(*a.transcript, inputs[:result.consumed]...)
	a.stop = result.stop
	if err != nil {
		a.live.failedAt = failurePhase
	}
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
// paceArmed decides whether the CPU-bound split heuristic may measure this
// build at all.
//
// Three conditions, and the third is the one that is not obvious. The heuristic
// asks whether collation filled more than three fifths of the time available and
// waited for external messages less than a fifth of it; neither ratio means
// anything when the numerator started before the denominator did. A build the
// producer handed over early does exactly that, and clamping its spans to the
// schedule instead — which is what this used to do — makes the body span equal
// the total span, so the three-fifths filter becomes unconditionally true and
// the shard declares itself overloaded on arithmetic nobody meant. The verdict
// goes into header.WantSplit, which validators mask, so it would be wrong
// silently.
//
// The cost is stated and accepted: a pipelined shard stops reporting
// OverloadLongCollation. It is self-correcting — a shard that genuinely cannot
// collate inside its slot has no spare time to hand a successor and so cannot
// start one early, which re-arms the heuristic exactly when it matters. The
// block-limit and force-split overload reasons are untouched.
func paceArmed(started, scheduled, waitUntil time.Time) bool {
	return !started.IsZero() && !waitUntil.IsZero() && !started.Before(scheduled)
}

func (b *Builder) buildShardWithReadyExternals(
	ctx context.Context,
	req ShardRequest,
	stream readyExternalSource,
	processUntil time.Time,
	waitUntil time.Time,
	batchLimit int,
	// startedAt is when this node began assembling the candidate; scheduledAt is
	// when it was due to begin. They differ only for a build the producer started
	// ahead of its slot, and that difference is the whole of what scheduledAt is
	// for — see the arming condition below.
	startedAt time.Time,
	scheduledAt time.Time,
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
	// A size retry re-enters collation, so the body span restarts with it while
	// the span that began before acquisition does not — the same asymmetry the
	// reference collator gets from a member initializer and a per-call
	// assignment.
	armed := paceArmed(startedAt, scheduledAt, waitUntil)
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
	candidate, err := retryUnderSizeLimit(ctx, func(attempt collationAttempt) (*Candidate, error) {
		live.attempts++
		if attempt.index == 0 {
			return b.buildShardReadyAttempt(
				ctx,
				req,
				attempt,
				stream,
				processUntil,
				waitUntil,
				batchLimit,
				&transcript,
				&live,
				attemptPace(),
			)
		}

		// A rebuild never returns to the stream. It replays what the first
		// attempt admitted, in the order it admitted it, so the only difference
		// between attempts is what this one declines — and once the attempt
		// declines externals outright the transcript is not offered at all,
		// which spares the rebuild the prewarm of messages it will not read.
		replay := req
		replay.Externals = transcript
		if attempt.skipExternals() {
			replay.Externals = nil
		}
		return b.buildShardAttemptPaced(ctx, replay, attempt, attemptPace())
	})
	if candidate != nil {
		candidate.Stats.ExternalWait = live.wait
		candidate.Stats.ExternalBatches = live.batches
		candidate.Stats.ExternalStop = live.stop
	}
	if err == nil {
		err = sealBuiltCandidate(candidate)
	}
	if err != nil {
		// A block that was handed over and then failed to finish leaves a
		// successor speculating on a slot that will produce nothing.
		req.successor.revokeOffered(PipelineHandoffAbandonedFailed)
	}

	return candidate, live, err
}

func (b *Builder) buildShardReadyAttempt(
	ctx context.Context,
	req ShardRequest,
	attempt collationAttempt,
	stream readyExternalSource,
	processUntil time.Time,
	waitUntil time.Time,
	batchLimit int,
	transcript *[]ExternalInput,
	live *readyExternalStats,
	pace collationPace,
) (*Candidate, error) {
	c, err := b.prepareShardPhases(ctx, req, attempt)
	if err != nil {
		live.failedAt = externalPhasePrepare
		return nil, err
	}
	defer c.stopWaves()
	c.pace = pace

	waitCtx := ctx
	cancel := func() {}
	if !waitUntil.IsZero() {
		waitCtx, cancel = context.WithDeadline(ctx, waitUntil)
	}
	defer cancel()

	ready := readyExternalAttempt{
		transcript: transcript,
		live:       live,
	}
	// Written by a defer, not after the loop. Assigned at the tail it was only
	// ever set on success, which is what made external_stop="unknown" mean
	// nothing but "it failed" on all 71 field events. The shape snapshot goes the
	// same way, because the candidate that carries those counters is nil on every
	// failing return.
	defer func() {
		live.stop = ready.stop
		live.observe(c, len(req.Externals))
	}()

	// The batch timers below are stopped by hand, unlike the phase timers in
	// build.go and master.go, and that is not an oversight to tidy up. They are
	// declared inside loop bodies, where a deferred stop would not run at the
	// end of the iteration that started it: every iteration's timer would pile
	// up and close together when the whole collation returned, charging the
	// external stage once per batch for the entire remaining build.
	if len(req.Externals) > 0 {
		timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
		result, processErr := c.processExternalBatch(req.Externals, processUntil)
		ready.record(req.Externals, result, processErr, externalPhasePreAdmittedBatch)
		timer.stop()
		if processErr != nil {
			return nil, processErr
		}
	}
	for {
		for ready.stop == ExternalStopUnknown {
			// A closed intake takes nothing out of the pool at all. Every
			// message TakeReady returned would get the same skip verdict, at
			// the price of a snapshot, a parse and an account prewarm each —
			// and, because a skip does not consume the message, the next block
			// would pay it all again. Left untaken, the messages keep their
			// generation and their place, and this build spends nothing on
			// them. The wait branch below stays live: admissions that arrive
			// during the slot are answered there with the same cheap skip.
			if c.externalIntakeClosed() {
				break
			}
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
			result, processErr := c.processExternalBatch(inputs, processUntil)
			ready.record(inputs, result, processErr, externalPhaseTakeReadyBatch)
			timer.stop()
			if processErr != nil {
				return nil, processErr
			}
		}

		timer := c.req.assembly.start(CollationStageExecuteInternalMessages)
		if err = c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete()); err != nil {
			timer.stop()
			// The phase every one of the field's seven external_batches==1 events
			// died in, and the only one that can raise the order violation:
			// advanceProcessedBound's "generated" caller is retireGeneratedImmediate, which
			// is reachable from here alone.
			live.failedAt = externalPhaseProcessNewMessages
			return nil, err
		}
		if ready.stop != ExternalStopUnknown {
			timer.stop()
			break
		}
		c.updateCollatedEstimate()
		if !c.limits.fits(LoadSoft) {
			timer.stop()
			ready.stop = ExternalStopSoftLimit
			break
		}
		timer.stop()
		if waitUntil.IsZero() {
			ready.stop = externalPhaseStop(c, ExternalStopReadyDrained)
			break
		}

		// Before parking. The externals ready now are taken, the generated
		// messages are drained, and this build is about to wait out the rest of
		// its slot in case more arrive; if the queue still owes it work and the
		// reserved half of the budget is going unused, that is where it goes.
		// The loop is re-entered rather than fallen through, so externals that
		// landed during the top-up are taken before the wait; topUpInternals is
		// one-shot, so the re-entry cannot repeat.
		if !c.toppedUp && c.internalsRemain() {
			if err = c.topUpInternals(); err != nil {
				live.failedAt = externalPhaseTopUp
				return nil, err
			}
			if c.toppedUp {
				continue
			}
		}

		started := time.Now()
		snapshots, nextErr := stream.Next(waitCtx, batchLimit)
		live.wait += time.Since(started)
		if nextErr != nil {
			if errors.Is(nextErr, context.DeadlineExceeded) && errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				ready.stop = externalPhaseStop(c, ExternalStopDeadline)
				break
			}
			live.failedAt = externalPhaseWait
			return nil, nextErr
		}
		// The same closed-intake shortcut as above, for the batch the wait
		// delivered: the skip verdict is recorded from the snapshot's reference
		// alone, so the batch is never parsed and never prewarmed. It is not
		// counted as a consumed batch either — nothing of it entered the block.
		if c.externalIntakeClosed() {
			for i := range snapshots {
				c.recordExternal(snapshots[i].Reference(), msgpool.ExternalSkippedLimit)
			}
			continue
		}
		timer = c.req.assembly.start(CollationStageExecuteExternalMessages)
		inputs, prepareErr := prepareExternalSnapshots(snapshots)
		if prepareErr != nil {
			timer.stop()
			live.failedAt = externalPhaseWaitedBatch
			return nil, prepareErr
		}
		result, processErr := c.processExternalBatch(inputs, processUntil)
		ready.record(inputs, result, processErr, externalPhaseWaitedBatch)
		timer.stop()
		if processErr != nil {
			return nil, processErr
		}
	}

	candidate, finishErr := c.finishShard()
	if finishErr != nil {
		live.failedAt = externalPhaseFinish
	}
	return candidate, finishErr
}

// externalPhaseStop is the reason the phase reports when it ends of natural
// causes — drained or slot deadline — while the outbound-queue brake is closed.
// Both natural reasons are then technically true and completely misleading: the
// phase consumed nothing because the intake was refusing everything. Stops that
// name a positive cause (soft limit, attempt limit) are never rewritten.
func externalPhaseStop(c *collation, natural ExternalStopReason) ExternalStopReason {
	if c.externalIntakeClosed() {
		return ExternalStopQueueBrake
	}
	return natural
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
	candidate, err := retryUnderSizeLimit(ctx, func(attempt collationAttempt) (*Candidate, error) {
		live.attempts++
		if attempt.index == 0 {
			return b.buildMasterReadyAttempt(
				ctx,
				req,
				attempt,
				stream,
				processUntil,
				batchLimit,
				&transcript,
				&live,
			)
		}

		replay := req
		replay.Externals = transcript
		if attempt.skipExternals() {
			replay.Externals = nil
		}
		return b.buildMasterAttempt(ctx, replay, attempt)
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
	attempt collationAttempt,
	stream readyExternalSource,
	processUntil time.Time,
	batchLimit int,
	transcript *[]ExternalInput,
	live *readyExternalStats,
) (*Candidate, error) {
	c, err := b.prepareMasterPhases(ctx, req, attempt)
	if err != nil {
		live.failedAt = externalPhasePrepare
		return nil, err
	}
	defer c.stopWaves()

	ready := readyExternalAttempt{
		transcript: transcript,
		live:       live,
	}
	defer func() {
		live.stop = ready.stop
		live.observe(c, len(req.Externals))
	}()

	if len(req.Externals) > 0 {
		timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
		result, processErr := c.processExternalBatch(req.Externals, processUntil)
		ready.record(req.Externals, result, processErr, externalPhasePreAdmittedBatch)
		timer.stop()
		if processErr != nil {
			return nil, processErr
		}
	}
	for ready.stop == ExternalStopUnknown {
		timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
		snapshots := stream.TakeReady(batchLimit)
		if len(snapshots) == 0 {
			timer.stop()
			ready.stop = ExternalStopReadyDrained
			break
		}
		inputs, prepareErr := prepareExternalSnapshots(snapshots)
		if prepareErr != nil {
			timer.stop()
			live.failedAt = externalPhaseTakeReadyBatch
			return nil, prepareErr
		}
		result, processErr := c.processExternalBatch(inputs, processUntil)
		ready.record(inputs, result, processErr, externalPhaseTakeReadyBatch)
		timer.stop()
		if processErr != nil {
			return nil, processErr
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
	externalPhaseTopUp              = "top_up_internals"
	externalPhaseWaitedBatch        = "waited_batch"
	externalPhaseFinish             = "finish"
)
