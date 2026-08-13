package collator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type readyExternalStats struct {
	wait    time.Duration
	batches uint32
	stop    ExternalStopReason
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
	stream *msgpool.ExternalStream,
	processUntil time.Time,
	waitUntil time.Time,
	batchLimit int,
	startedAt time.Time,
) (*Candidate, error) {
	if batchLimit <= 0 {
		return nil, fmt.Errorf("%w: ready external batch limit must be positive", ErrInvalidInput)
	}
	if req.MaxExternalAttempts <= 0 {
		return nil, fmt.Errorf("%w: external attempt limit must be positive", ErrInvalidInput)
	}
	common := shardCollationRequest(req)
	if err := validateCollationRequest(&common); err != nil {
		return nil, err
	}

	transcript := make([]ExternalInput, 0, len(req.Externals))
	attempt := 0
	var live readyExternalStats
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

	return candidate, err
}

func (b *Builder) buildShardReadyAttempt(
	ctx context.Context,
	req ShardRequest,
	narrowing sizeBudgetCap,
	stream *msgpool.ExternalStream,
	processUntil time.Time,
	waitUntil time.Time,
	batchLimit int,
	transcript *[]ExternalInput,
	live *readyExternalStats,
	pace collationPace,
) (*Candidate, error) {
	c, err := b.prepareShardPhases(ctx, req, narrowing)
	if err != nil {
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
	if len(req.Externals) > 0 {
		live.batches++
		result, processErr := c.processExternalBatch(req.Externals, processUntil)
		*transcript = append(*transcript, req.Externals[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			return nil, err
		}
	}
	for {
		for stop == ExternalStopUnknown {
			snapshots := stream.TakeReady(batchLimit)
			if len(snapshots) == 0 {
				break
			}
			inputs, prepareErr := prepareExternalSnapshots(snapshots)
			if prepareErr != nil {
				return nil, prepareErr
			}
			live.batches++
			result, processErr := c.processExternalBatch(inputs, processUntil)
			*transcript = append(*transcript, inputs[:result.consumed]...)
			stop, err = result.stop, processErr
			if err != nil {
				return nil, err
			}
		}

		if err = c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete()); err != nil {
			return nil, err
		}
		if stop != ExternalStopUnknown {
			break
		}
		c.updateCollatedEstimate()
		if !c.limits.fits(LoadSoft) {
			stop = ExternalStopSoftLimit
			break
		}
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
			return nil, nextErr
		}
		inputs, prepareErr := prepareExternalSnapshots(snapshots)
		if prepareErr != nil {
			return nil, prepareErr
		}
		live.batches++
		result, processErr := c.processExternalBatch(inputs, processUntil)
		*transcript = append(*transcript, inputs[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			return nil, err
		}
	}

	live.stop = stop
	return c.finishShard()
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
	stream *msgpool.ExternalStream,
	processUntil time.Time,
	batchLimit int,
) (*Candidate, error) {
	if batchLimit <= 0 {
		return nil, fmt.Errorf("%w: ready external batch limit must be positive", ErrInvalidInput)
	}
	if req.MaxExternalAttempts <= 0 {
		return nil, fmt.Errorf("%w: external attempt limit must be positive", ErrInvalidInput)
	}
	common := masterCollationRequest(req)
	if err := validateCollationRequest(&common); err != nil {
		return nil, err
	}

	transcript := make([]ExternalInput, 0, len(req.Externals))
	attempt := 0
	var live readyExternalStats
	candidate, err := retryUnderSizeLimit(func(narrowing sizeBudgetCap) (*Candidate, error) {
		current := attempt
		attempt++
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

	return candidate, err
}

func (b *Builder) buildMasterReadyAttempt(
	ctx context.Context,
	req MasterRequest,
	narrowing sizeBudgetCap,
	stream *msgpool.ExternalStream,
	processUntil time.Time,
	batchLimit int,
	transcript *[]ExternalInput,
	live *readyExternalStats,
) (*Candidate, error) {
	c, err := b.prepareMasterPhases(ctx, req, narrowing)
	if err != nil {
		return nil, err
	}

	stop := ExternalStopUnknown
	if len(req.Externals) > 0 {
		live.batches++
		result, processErr := c.processExternalBatch(req.Externals, processUntil)
		*transcript = append(*transcript, req.Externals[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			return nil, err
		}
	}
	for stop == ExternalStopUnknown {
		snapshots := stream.TakeReady(batchLimit)
		if len(snapshots) == 0 {
			stop = ExternalStopReadyDrained
			break
		}
		inputs, prepareErr := prepareExternalSnapshots(snapshots)
		if prepareErr != nil {
			return nil, prepareErr
		}
		live.batches++
		result, processErr := c.processExternalBatch(inputs, processUntil)
		*transcript = append(*transcript, inputs[:result.consumed]...)
		stop, err = result.stop, processErr
		if err != nil {
			return nil, err
		}
	}

	live.stop = stop
	return c.finishMaster(req)
}
