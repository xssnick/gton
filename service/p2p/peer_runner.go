package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type peerRequestOptions struct {
	parallelism          int
	stageParallelism     int
	stageDelay           time.Duration
	hedgeDelay           time.Duration
	collectAfterSuccess  time.Duration
	maxElapsed           time.Duration
	earlyFailureCount    int
	earlyFailureDelay    time.Duration
	cancelOnFirstSuccess bool
	onFailure            func(*overlayPeer, error)
	onCollectElapsed     func(ready int, pending int)
}

type peerRequestResult[T any] struct {
	peer  *overlayPeer
	value T
}

func runPeerRequests[T any](ctx context.Context, peers []*overlayPeer, opts peerRequestOptions, query func(context.Context, *overlayPeer) (T, error)) ([]peerRequestResult[T], []error) {
	parallelism := minInt(opts.parallelism, len(peers))
	if parallelism <= 0 {
		return nil, []error{errors.New("overlay has no connected peers")}
	}
	stageParallelism := minInt(opts.stageParallelism, len(peers))
	if stageParallelism <= parallelism {
		stageParallelism = 0
	}
	targetParallelism := parallelism
	startedAt := time.Now()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		peer  *overlayPeer
		value T
		err   error
	}

	results := make(chan result, len(peers))
	var (
		nextIdx  int
		inFlight int
	)

	launch := func(peer *overlayPeer) {
		inFlight++
		go func() {
			value, err := query(ctx, peer)
			if err != nil {
				if ctx.Err() == nil && opts.onFailure != nil {
					opts.onFailure(peer, err)
				}
				err = fmt.Errorf("%s: %w", peer.addr, err)
			}

			select {
			case results <- result{peer: peer, value: value, err: err}:
			case <-ctx.Done():
			}
		}()
	}

	for nextIdx < len(peers) && inFlight < targetParallelism {
		launch(peers[nextIdx])
		nextIdx++
	}

	var hedgeTimer *time.Timer
	if opts.hedgeDelay > 0 && nextIdx < len(peers) {
		hedgeTimer = time.NewTimer(opts.hedgeDelay)
		defer hedgeTimer.Stop()
	}
	var stageTimer *time.Timer
	if opts.stageDelay > 0 && stageParallelism > 0 && nextIdx < len(peers) {
		stageTimer = time.NewTimer(opts.stageDelay)
		defer stageTimer.Stop()
	}
	var maxElapsedTimer *time.Timer
	if opts.maxElapsed > 0 {
		maxElapsedTimer = time.NewTimer(opts.maxElapsed)
		defer maxElapsedTimer.Stop()
	}
	var earlyFailureTimer *time.Timer
	var earlyFailureC <-chan time.Time

	var collectTimer *time.Timer
	var collectC <-chan time.Time
	defer func() {
		if collectTimer != nil {
			collectTimer.Stop()
		}
		if earlyFailureTimer != nil {
			earlyFailureTimer.Stop()
		}
	}()

	successes := make([]peerRequestResult[T], 0, 1)
	var errs []error
	startEarlyFailureTimer := func() bool {
		if opts.earlyFailureCount <= 0 || len(successes) > 0 || len(errs) < opts.earlyFailureCount || nextIdx < len(peers) {
			return false
		}

		if opts.earlyFailureDelay <= 0 {
			return true
		}
		remaining := opts.earlyFailureDelay - time.Since(startedAt)
		if remaining <= 0 {
			return true
		}
		if earlyFailureTimer == nil {
			earlyFailureTimer = time.NewTimer(remaining)
			earlyFailureC = earlyFailureTimer.C
		}
		return false
	}
	for inFlight > 0 || nextIdx < len(peers) {
		var hedgeC <-chan time.Time
		if hedgeTimer != nil {
			hedgeC = hedgeTimer.C
		}
		var stageC <-chan time.Time
		if stageTimer != nil {
			stageC = stageTimer.C
		}
		var maxElapsedC <-chan time.Time
		if maxElapsedTimer != nil {
			maxElapsedC = maxElapsedTimer.C
		}

		select {
		case <-ctx.Done():
			if len(successes) > 0 {
				return successes, errs
			}
			if len(errs) > 0 {
				return nil, errs
			}
			return nil, []error{ctx.Err()}
		case <-maxElapsedC:
			cancel()
			if len(successes) > 0 {
				return successes, errs
			}
			if len(errs) > 0 {
				return nil, errs
			}
			return nil, []error{context.DeadlineExceeded}
		case <-earlyFailureC:
			cancel()
			return nil, errs
		case <-collectC:
			if opts.onCollectElapsed != nil {
				opts.onCollectElapsed(len(successes), inFlight+len(peers)-nextIdx)
			}
			cancel()
			return successes, errs
		case <-hedgeC:
			if nextIdx < len(peers) {
				launch(peers[nextIdx])
				nextIdx++
			}
			if nextIdx < len(peers) {
				hedgeTimer.Reset(opts.hedgeDelay)
			} else {
				hedgeTimer = nil
			}
		case <-stageC:
			if targetParallelism < stageParallelism {
				targetParallelism++
			}
			for nextIdx < len(peers) && inFlight < targetParallelism {
				launch(peers[nextIdx])
				nextIdx++
			}
			if targetParallelism < stageParallelism && nextIdx < len(peers) {
				stageTimer.Reset(opts.stageDelay)
			} else {
				stageTimer = nil
			}
		case res := <-results:
			inFlight--
			if res.err == nil {
				successes = append(successes, peerRequestResult[T]{
					peer:  res.peer,
					value: res.value,
				})
				if opts.cancelOnFirstSuccess || opts.collectAfterSuccess <= 0 {
					cancel()
					return successes, errs
				}
				if collectTimer == nil && inFlight > 0 {
					collectTimer = time.NewTimer(opts.collectAfterSuccess)
					collectC = collectTimer.C
				}
				continue
			}

			errs = append(errs, res.err)
			if nextIdx < len(peers) {
				launch(peers[nextIdx])
				nextIdx++
			}
			if startEarlyFailureTimer() {
				cancel()
				return nil, errs
			}
		}
	}

	return successes, errs
}

func runFirstPeerRequest[T any](ctx context.Context, peers []*overlayPeer, opts peerRequestOptions, query func(context.Context, *overlayPeer) (T, error)) (peerRequestResult[T], error) {
	opts.cancelOnFirstSuccess = true
	results, errs := runPeerRequests(ctx, peers, opts, query)
	if len(results) > 0 {
		return results[0], nil
	}
	return peerRequestResult[T]{}, errors.Join(errs...)
}
