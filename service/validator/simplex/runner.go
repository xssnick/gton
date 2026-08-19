package simplex

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// runnerBatchSize bounds the time spent serving one wakeup. Returning to the
// outer loop between batches keeps cancellation and consensus timers live
// even when network ingress continuously replenishes the queue.
const runnerBatchSize = 256

// Runner is the production wrapper around Engine: it owns the event-loop
// goroutine, serializes all inputs and drives the engine timers off the wall
// clock. Candidate services and event handlers fire on the loop goroutine;
// asynchronous candidate completions are routed back by the callbacks the
// engine supplies to those services.
type Runner struct {
	eng *Engine

	mu       sync.Mutex
	queue    []func()
	priority []func()
	wake     chan struct{}
	done     chan struct{}
	stopped  bool

	// snapshot is the engine's own view, republished at the end of each loop
	// turn so an off-goroutine reader never has to enter the loop.
	snapshot atomic.Pointer[Stats]
	// lastSigned is the per-validator participation slice currently published.
	// It is owned by the loop goroutine, and once published it is never written
	// again — a change allocates a new slice, so every snapshot handed out stays
	// valid. See publishStats.
	lastSigned []uint32
}

// NewRunner wraps an engine that has NOT been started yet; Run starts it on
// the loop goroutine. The runner also takes over asynchronous journal
// completion routing: Journal done callbacks become safe to invoke from any
// goroutine once the engine is wrapped.
func NewRunner(eng *Engine) *Runner {
	r := &Runner{
		eng:  eng,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	eng.post = r.post
	return r
}

// Run starts the engine and serves its event loop until ctx is cancelled or
// Stop is called. It returns the engine start error, or nil on a clean stop.
// A fatal session error ends the loop as soon as it happens — including on the
// timer paths, where the engine reports no further deadlines — and is returned
// from every exit, cancellation included.
func (r *Runner) Run(ctx context.Context) error {
	return r.run(ctx, nil)
}

// RunWithStartup is Run with an explicit initialization barrier. It sends
// exactly one value after Engine.Start returns: nil when recovery completed
// and the loop is ready, or the startup error. The caller must provide a
// buffered channel or receive concurrently.
func (r *Runner) RunWithStartup(ctx context.Context, startup chan<- error) error {
	return r.run(ctx, startup)
}

func (r *Runner) run(ctx context.Context, startup chan<- error) (resultErr error) {
	startupSent := false
	signalStartup := func(err error) {
		if startup == nil || startupSent {
			return
		}
		startupSent = true
		startup <- err
	}
	defer func() { signalStartup(resultErr) }()

	defer func() {
		r.mu.Lock()
		r.stopped = true
		r.queue = nil
		r.priority = nil
		r.mu.Unlock()

		close(r.done)
	}()

	if err := r.eng.Start(); err != nil {
		return err
	}
	signalStartup(nil)

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		if ctx.Err() != nil {
			err := r.eng.Err()
			r.eng.Stop()
			return err
		}
		if r.stopRequested() {
			r.eng.Stop()
			return r.eng.Err()
		}
		if next := r.eng.NextWakeup(); !next.IsZero() && !next.After(time.Now()) {
			r.eng.Advance()
			r.publishStats()
			if err := r.eng.Err(); err != nil {
				return err
			}
			continue
		}

		r.armTimer(timer)
		select {
		case <-ctx.Done():
			err := r.eng.Err()
			r.eng.Stop()
			return err
		case <-timer.C:
			r.eng.Advance()
			r.publishStats()
			// A timer path can fail the session just like an input can (a
			// journal write for a timeout skip vote, a certificate built from
			// it). Without this check NextWakeup would report "no deadline"
			// and the loop would sleep until the next input instead of
			// surfacing the error.
			if err := r.eng.Err(); err != nil {
				return err
			}
		case <-r.wake:
			q, stopped, more := r.takeBatch()
			if stopped {
				r.eng.Stop()
				return r.eng.Err()
			}
			for _, f := range q {
				f()
			}
			// The batch admitted its votes without checking their signatures;
			// verifying them together is what puts the arrival burst of one
			// vote round on all cores instead of on this goroutine alone.
			r.eng.FlushVerify()
			r.publishStats()
			if more {
				r.signal()
			}
			if err := r.eng.Err(); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) stopRequested() bool {
	r.mu.Lock()
	stopped := r.stopped
	r.mu.Unlock()
	return stopped
}

func (r *Runner) takeBatch() (queue []func(), stopped, more bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		r.queue = nil
		r.priority = nil
		return nil, true, false
	}

	n := min(len(r.priority)+len(r.queue), runnerBatchSize)
	if n <= len(r.priority) {
		queue = r.priority[:n]
		if n == len(r.priority) {
			r.priority = nil
		} else {
			r.priority = r.priority[n:]
		}
		return queue, false, len(r.priority) != 0 || len(r.queue) != 0
	}

	// Candidate work reaches the queue beside ordinary direct-message work
	// only during a leader window. Keep the normal message path allocation-free
	// and copy only for this mixed, latency-sensitive batch.
	queue = make([]func(), 0, n)
	queue = append(queue, r.priority...)
	r.priority = nil
	ordinary := n - len(queue)
	queue = append(queue, r.queue[:ordinary]...)
	if ordinary == len(r.queue) {
		r.queue = nil
	} else {
		r.queue = r.queue[ordinary:]
	}
	return queue, false, len(r.priority) != 0 || len(r.queue) != 0
}

func (r *Runner) armTimer(timer *time.Timer) {
	timer.Stop()
	next := r.eng.NextWakeup()
	if next.IsZero() {
		timer.Reset(time.Hour)
		return
	}
	d := time.Until(next)
	if d < 0 {
		d = 0
	}
	timer.Reset(d)
}

func (r *Runner) post(f func()) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.queue = append(r.queue, f)
	r.mu.Unlock()
	r.signal()
}

// postPriority admits candidate-related work ahead of direct vote and
// certificate ingress already queued by the transport. Candidate production
// is bounded by a leader window; postponing it behind a standstill replay can
// otherwise turn a ready local candidate into a skip-only round.
func (r *Runner) postPriority(f func()) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.priority = append(r.priority, f)
	r.mu.Unlock()
	r.signal()
}

func (r *Runner) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// HandleMessage enqueues a protocol message for the engine.
func (r *Runner) HandleMessage(src PeerID, srcValidator int, data []byte) {
	msg := make([]byte, len(data))
	copy(msg, data)
	r.post(func() { r.eng.handleMessageBatched(src, srcValidator, msg) })
}

// SubmitCandidate synchronously admits a candidate proposal on the engine
// loop. The return value confirms that the engine accepted or deliberately
// ignored the candidate; a stopped runner returns ErrStopped. It must not be
// called from an engine callback because callbacks already run on the loop.
func (r *Runner) SubmitCandidate(c *Candidate) error {
	var err error
	if !r.doPriority(func() { err = r.eng.SubmitCandidate(c) }) {
		return ErrStopped
	}

	return err
}

// PrecheckCandidateBroadcast serializes the cheap pre-FEC admission gate on
// the engine loop. Transports call it before buffering/reassembling candidate
// data and commit the broadcast id only after source authentication succeeds.
func (r *Runner) PrecheckCandidateBroadcast(
	slot uint32,
	broadcastID [32]byte,
	signatureChecked bool,
) error {
	var err error
	if !r.doPriority(func() { err = r.eng.PrecheckCandidateBroadcast(slot, broadcastID, signatureChecked) }) {
		return ErrStopped
	}

	return err
}

// do runs f on the loop goroutine and waits for it. It returns false when the
// loop is already gone, in which case f never ran.
//
// Run must be in progress. It must not be called from an engine callback:
// callbacks already run on the loop goroutine and waiting for it there would
// deadlock.
func (r *Runner) do(f func()) bool {
	return r.doWithPost(r.post, f)
}

func (r *Runner) doPriority(f func()) bool {
	return r.doWithPost(r.postPriority, f)
}

func (r *Runner) doWithPost(post func(func()), f func()) bool {
	done := make(chan struct{})
	post(func() {
		f()
		close(done)
	})
	select {
	case <-done:
		return true
	case <-r.done:
		// The loop may have drained the queue on its way out; prefer the
		// completed call over the shutdown signal.
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}

// Stats returns a snapshot of the engine counters, taken on the loop
// goroutine. It returns the zero value once the loop has stopped.
//
// It is for deterministic tests and for callers that need the exact current
// turn. Anything that samples periodically — a metrics scrape above all — must
// use StatsSnapshot instead: this round-trips onto the loop goroutine, so a
// scrape taken while that goroutine is wedged would block on the very
// condition it is being scraped to diagnose.
func (r *Runner) Stats() Stats {
	var s Stats
	r.do(func() { s = r.eng.Stats() })
	return s
}

// StatsSnapshot returns the counters published at the end of the most recent
// loop turn, without touching the loop goroutine. Its freshness is one turn,
// which for the timer paths is bounded by the standstill timeout.
func (r *Runner) StatsSnapshot() Stats {
	if stats := r.snapshot.Load(); stats != nil {
		return *stats
	}

	return Stats{}
}

// publishStats republishes the snapshot. It runs on the loop goroutine at the
// end of every turn, so what it costs is paid on every consensus turn there is.
//
// A published snapshot is immutable — a scrape may be copying the previous one
// right now — so the turn does allocate the Stats it stores, and that is the
// whole of what it allocates. The per-validator participation slice used to be
// copied with it, a roster-sized allocation on every turn including the ones
// that changed nothing about it; it is copied here only when it differs from
// the slice already published, which is when a certificate was stored. The
// comparison is a roster-length scan and allocates nothing, and the previously
// published slice is never written again, so reusing it is safe for readers
// still holding the older snapshot.
func (r *Runner) publishStats() {
	stats := r.eng.statsAliased()
	if !slices.Equal(r.lastSigned, stats.LastSignedSlot) {
		r.lastSigned = append([]uint32(nil), stats.LastSignedSlot...)
	}
	stats.LastSignedSlot = r.lastSigned
	r.snapshot.Store(&stats)
}

// DebugDump renders the pool state in the canonical debug format, on the loop
// goroutine.
func (r *Runner) DebugDump() string {
	var s string
	r.do(func() { s = r.eng.DebugDump() })
	return s
}

// Err returns the fatal session error, if any.
func (r *Runner) Err() error {
	var err error
	if !r.do(func() { err = r.eng.Err() }) {
		// The loop is gone; the engine is no longer mutated, so a direct read
		// is safe and is the only way to learn why it stopped.
		return r.eng.Err()
	}
	return err
}

// UpdateParams installs a new noncritical parameter set on the loop goroutine.
func (r *Runner) UpdateParams(p Params) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if !r.do(func() { _ = r.eng.UpdateParams(p) }) {
		return ErrStopped
	}

	return nil
}

// Stop terminates the loop and waits for it to exit.
func (r *Runner) Stop() {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	r.signal()
	<-r.done
}
