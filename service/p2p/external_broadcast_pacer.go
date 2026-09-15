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

	return &externalBroadcastPacer{
		bytesPerSecond: opts.BytesPerSecond,
		maxDelay:       opts.MaxDelay,
		now:            time.Now,
	}, nil
}

// Reserve books costBytes of broadcast capacity without waiting and returns
// the time the broadcast may be sent.
func (p *externalBroadcastPacer) Reserve(costBytes int64) (time.Time, error) {
	now := p.now()
	if costBytes <= 0 || p.bytesPerSecond == 0 {
		return now, nil
	}
	return p.reserve(now, costBytes)
}

// Cancel returns a reservation whose broadcast was never queued. Only the
// latest reservation is returned: the ones booked after it are already
// scheduled behind its slot, as in x/time/rate Reservation.Cancel.
func (p *externalBroadcastPacer) Cancel(sendAt time.Time, costBytes int64) {
	if costBytes <= 0 || p.bytesPerSecond == 0 {
		return
	}

	p.mx.Lock()
	defer p.mx.Unlock()

	if p.nextAvailable.Equal(sendAt.Add(p.duration(costBytes))) {
		p.nextAvailable = sendAt
	}
}

func (p *externalBroadcastPacer) Wait(ctx context.Context, sendAt time.Time) error {
	delay := sendAt.Sub(p.now())
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
