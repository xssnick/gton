package p2p

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
)

type externalBroadcastPacer struct {
	mx             sync.Mutex
	bytesPerSecond int64
	maxDelay       time.Duration
	nextAvailable  time.Time
	now            func() time.Time
}

func newExternalBroadcastPacer(opts ExternalBroadcastCapacityOptions) (*externalBroadcastPacer, error) {
	if opts.BytesPerSecond < 0 {
		return nil, fmt.Errorf("external broadcast capacity bytes per second cannot be negative")
	}
	if opts.MaxDelay < 0 {
		return nil, fmt.Errorf("external broadcast capacity max delay cannot be negative")
	}
	if opts.BytesPerSecond == 0 {
		return nil, nil
	}

	return &externalBroadcastPacer{
		bytesPerSecond: opts.BytesPerSecond,
		maxDelay:       opts.MaxDelay,
		now:            time.Now,
	}, nil
}

func (p *externalBroadcastPacer) Wait(ctx context.Context, costBytes int64) error {
	if p == nil || costBytes <= 0 {
		return nil
	}

	now := p.now()
	sendAt, err := p.reserve(now, costBytes)
	if err != nil {
		return err
	}

	delay := sendAt.Sub(now)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *externalBroadcastPacer) reserve(now time.Time, costBytes int64) (time.Time, error) {
	p.mx.Lock()
	defer p.mx.Unlock()

	sendAt := now
	if p.nextAvailable.After(sendAt) {
		sendAt = p.nextAvailable
	}

	if delay := sendAt.Sub(now); delay > p.maxDelay {
		return time.Time{}, extmsg.ErrExternalBroadcastCapacityExceeded
	}

	p.nextAvailable = sendAt.Add(p.duration(costBytes))
	return sendAt, nil
}

func (p *externalBroadcastPacer) duration(costBytes int64) time.Duration {
	nanos := costBytes * int64(time.Second) / p.bytesPerSecond
	if nanos == 0 && costBytes > 0 {
		nanos = 1
	}
	return time.Duration(nanos)
}
