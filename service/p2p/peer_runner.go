package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type peerRequestOptions struct {
	parallelism          int
	hedgeDelay           time.Duration
	collectAfterSuccess  time.Duration
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

	for nextIdx < len(peers) && inFlight < parallelism {
		launch(peers[nextIdx])
		nextIdx++
	}

	var hedgeTimer *time.Timer
	if opts.hedgeDelay > 0 && nextIdx < len(peers) {
		hedgeTimer = time.NewTimer(opts.hedgeDelay)
		defer hedgeTimer.Stop()
	}

	var collectTimer *time.Timer
	var collectC <-chan time.Time
	defer func() {
		if collectTimer != nil {
			collectTimer.Stop()
		}
	}()

	successes := make([]peerRequestResult[T], 0, 1)
	var errs []error
	for inFlight > 0 || nextIdx < len(peers) {
		var hedgeC <-chan time.Time
		if hedgeTimer != nil {
			hedgeC = hedgeTimer.C
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
