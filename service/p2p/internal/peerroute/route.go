package peerroute

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// RetryPolicy controls the delay between failed QUIC dial attempts.
type RetryPolicy struct {
	MinDelay time.Duration
	MaxDelay time.Duration
}

func (p RetryPolicy) delay(failures uint32) time.Duration {
	delay := p.MinDelay
	for failure := uint32(1); failure < failures && delay < p.MaxDelay; failure++ {
		delay *= 2
	}
	if delay > p.MaxDelay {
		return p.MaxDelay
	}

	return delay
}

// Route owns the learned QUIC endpoint and all per-peer dial coordination.
// A Route must not be copied after first use.
type Route struct {
	quicAddress      atomic.Pointer[string]
	quicAddressStale atomic.Bool
	retryState       atomic.Int64
	retryMu          sync.Mutex
	dialFailures     uint32
	dialDone         chan struct{}
	backgroundDial   atomic.Bool
	lastUsedUnixNano atomic.Int64
	retryPolicy      RetryPolicy
}

func NewRoute(quicAddress string, retryPolicy RetryPolicy) *Route {
	route := &Route{retryPolicy: retryPolicy}
	route.SetQUICAddress(quicAddress)

	return route
}

func (r *Route) SetQUICAddress(address string) {
	if address == "" {
		return
	}

	if r.quicAddress.CompareAndSwap(nil, &address) {
		r.quicAddressStale.Store(false)
	}
}

func (r *Route) RefreshQUICAddress(address string) {
	r.quicAddress.Store(&address)
	r.quicAddressStale.Store(false)
}

func (r *Route) QUICAddress() string {
	address := r.quicAddress.Load()
	if address == nil {
		return ""
	}

	return *address
}

func (r *Route) MarkQUICAddressStale() {
	r.quicAddressStale.Store(true)
}

func (r *Route) QUICAddressStale() bool {
	return r.quicAddressStale.Load()
}

// QUICReady reports whether the retry gate permits a dial attempt. Callers
// must check it explicitly before dialing because an existing live path may
// bypass the address resolver that claims the retry gate.
func (r *Route) QUICReady(now time.Time) bool {
	retryState := r.retryState.Load()

	return retryState <= 0 || now.UnixNano() >= retryState
}

// QUICDialPermitted checks the retry gate without claiming it. It prevents a
// background dial goroutine from being spawned while a dial is already in
// flight or the route is inside its retry window.
func (r *Route) QUICDialPermitted(now time.Time) bool {
	retryState := r.retryState.Load()

	return retryState >= 0 && retryState <= now.UnixNano()
}

func (r *Route) BeginQUICDial(now time.Time) bool {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()

	nowUnixNano := now.UnixNano()
	retryState := r.retryState.Load()
	if retryState < 0 || retryState > nowUnixNano {
		return false
	}
	if !r.retryState.CompareAndSwap(retryState, -1) {
		return false
	}
	r.dialDone = make(chan struct{})

	return true
}

func (r *Route) QUICDialInFlight() bool {
	return r.retryState.Load() < 0
}

// WaitQUICDial waits for the current route owner, if any. Completion is
// signalled independently of the transport library's non-context dial mutex,
// so a latency-bounded caller can stop waiting on its own deadline.
func (r *Route) WaitQUICDial(ctx context.Context) error {
	r.retryMu.Lock()
	if r.retryState.Load() >= 0 {
		r.retryMu.Unlock()
		return nil
	}
	done := r.dialDone
	r.retryMu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Route) FinishQUICDial() {
	r.retryMu.Lock()
	if r.retryState.CompareAndSwap(-1, 0) {
		r.dialFailures = 0
		r.finishQUICDialLocked()
	}
	r.retryMu.Unlock()
}

func (r *Route) SucceedQUICDial(claimed bool) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()

	if claimed {
		if !r.retryState.CompareAndSwap(-1, 0) {
			return
		}
		r.finishQUICDialLocked()
	} else {
		for {
			retryState := r.retryState.Load()
			if retryState < 0 {
				return
			}
			if r.retryState.CompareAndSwap(retryState, 0) {
				break
			}
		}
	}

	r.dialFailures = 0
	r.quicAddressStale.Store(false)
}

func (r *Route) FailQUICDial(now time.Time) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()

	failures := r.dialFailures + 1
	retryAt := now.Add(r.retryPolicy.delay(failures)).UnixNano()
	for current := r.retryState.Load(); current < retryAt; {
		if r.retryState.CompareAndSwap(current, retryAt) {
			r.dialFailures = failures
			if current < 0 {
				r.finishQUICDialLocked()
			}
			return
		}
		current = r.retryState.Load()
	}
}

func (r *Route) finishQUICDialLocked() {
	close(r.dialDone)
	r.dialDone = nil
}

func (r *Route) DeferQUICDial(now time.Time) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()

	nowUnixNano := now.UnixNano()
	for {
		current := r.retryState.Load()
		if current < 0 || current > nowUnixNano {
			return
		}

		failures := r.dialFailures + 1
		retryAt := now.Add(r.retryPolicy.delay(failures)).UnixNano()
		if r.retryState.CompareAndSwap(current, retryAt) {
			r.dialFailures = failures
			return
		}
	}
}

// ClaimBackgroundQUICDial bounds background dials to one goroutine per route.
func (r *Route) ClaimBackgroundQUICDial() bool {
	return r.backgroundDial.CompareAndSwap(false, true)
}

func (r *Route) ReleaseBackgroundQUICDial() {
	r.backgroundDial.Store(false)
}

func (r *Route) touch(now time.Time) {
	r.lastUsedUnixNano.Store(now.UnixNano())
}

func (r *Route) lastUsed() int64 {
	return r.lastUsedUnixNano.Load()
}
