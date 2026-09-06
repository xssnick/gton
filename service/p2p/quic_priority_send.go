package p2p

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// quicPrioritySendWaitBound caps how long a relay write defers to this node's
// own candidate symbol in flight to the same peer. Stand, 2026-09-03: QUIC
// egress under load was ~270 Mbit/s over ~15 committee connections, ~18 Mbit/s
// each, so one ~200 kB two-step symbol of a 1.4 MB candidate occupies the
// connection for ~90 ms; behind up to 14 relayed symbols of other leaders it
// waited ~1.3 s (candidate_transport_send_duration p50 0.10 s, p90 1.84 s,
// p99 4.97 s). One nominal symbol time, not two: on the validator private
// overlays the only relay writer is tonutils' two-step relay dispatcher, and
// every forward it makes runs under DefaultTwoStepRelayPeerTimeout = 750 ms
// (adnl/overlay/broadcast-two-step.go), so the wait is paid out of that
// budget; the 5 s peerRebroadcastTimeout of the flexserver rebroadcast loops
// never applies there (relaysFECBroadcasts, relaysSimpleBroadcasts and
// runsTwoStepRebroadcastWorker are all false for a private overlay without
// legacy broadcasts). The latch orders only stream openings: quic-go's framer
// keeps round-robining STREAM frames of the relay streams already in flight,
// so the candidate rarely has the connection to itself and a longer wait
// mostly turns forwards that would have finished in the last part of their
// 750 ms into dispatcher timeouts, dropping another leader's symbol instead of
// reordering it.
const quicPrioritySendWaitBound = 100 * time.Millisecond

// The wait is reported through the broadcast pipeline observer, the p2p
// package's only Prometheus hook: gton_p2p_broadcast_pipeline_stage_duration_
// seconds{stage="priority_send_wait",kind="quic_relay_write",delivery="two_step"}
// with the result saying how the wait ended. The delivery is the one the
// deferred write carries: the latch is raised only on the private overlay's
// peers, and every relay write there forwards a two-step symbol. Only writes
// that actually waited are observed; the fast path records nothing.
const (
	prioritySendWaitStage    = "priority_send_wait"
	prioritySendWaitKind     = "quic_relay_write"
	prioritySendWaitDelivery = DeliveryTwoStep
	prioritySendWaitCleared  = "cleared"
	prioritySendWaitBounded  = "bound"
	prioritySendWaitCanceled = "canceled"
)

type prioritySendWaitResult uint8

const (
	prioritySendNotWaited prioritySendWaitResult = iota
	prioritySendCleared
	prioritySendBounded
	prioritySendCanceled
)

func prioritySendWaitResultLabel(result prioritySendWaitResult) string {
	switch result {
	case prioritySendCleared:
		return prioritySendWaitCleared
	case prioritySendBounded:
		return prioritySendWaitBounded
	default:
		return prioritySendWaitCanceled
	}
}

// quicPrioritySendLatch is raised while this node's own candidate symbol is
// being written to a peer. quic-go has no stream priorities and every message
// is its own stream, so the only lever over the order on a connection is who
// opens the next stream: relay writes to the peer consult the latch before
// theirs, the candidate write never does. C++ has no send priority either
// (ADNL priority_ is an address category); this changes only the order per
// connection, everyone still receives everything.
//
// The zero value is idle and ready to use.
type quicPrioritySendLatch struct {
	// raised mirrors inFlight > 0 so the relay fast path is one atomic load,
	// with no lock and no allocation.
	raised atomic.Bool

	mu       sync.Mutex
	inFlight int
	// cleared is closed when the last priority write returns; nil while idle.
	cleared chan struct{}
}

func (l *quicPrioritySendLatch) raise() {
	l.mu.Lock()
	l.inFlight++
	if l.inFlight == 1 {
		l.cleared = make(chan struct{})
		l.raised.Store(true)
	}
	l.mu.Unlock()
}

func (l *quicPrioritySendLatch) lower() {
	l.mu.Lock()
	l.inFlight--
	if l.inFlight == 0 {
		l.raised.Store(false)
		close(l.cleared)
		l.cleared = nil
	}
	l.mu.Unlock()
}

// clearedChan returns the channel a waiter blocks on, or nil when no priority
// write is in flight. A latch lowered between the flag and the lock reads as
// idle, which is the truth by then.
func (l *quicPrioritySendLatch) clearedChan() chan struct{} {
	if !l.raised.Load() {
		return nil
	}
	l.mu.Lock()
	cleared := l.cleared
	l.mu.Unlock()
	return cleared
}

// await blocks a relay write until every priority write to the peer has
// returned, the bound elapses, or ctx ends, and reports which of the three
// happened together with the time spent. A write already in progress is
// never touched: only the decision to open the next stream is deferred.
func (l *quicPrioritySendLatch) await(
	ctx context.Context,
	bound time.Duration,
) (prioritySendWaitResult, time.Duration) {
	cleared := l.clearedChan()
	if cleared == nil {
		return prioritySendNotWaited, 0
	}
	// A write whose context has already ended fails on it at OpenStreamSync
	// whether or not it waited; reporting that as a canceled wait would charge
	// the latch with failures it did not cause.
	if ctx.Err() != nil {
		return prioritySendNotWaited, 0
	}

	started := time.Now()
	timer := time.NewTimer(bound)
	defer timer.Stop()
	for {
		select {
		case <-cleared:
		case <-timer.C:
			return prioritySendBounded, time.Since(started)
		case <-ctx.Done():
			return prioritySendCanceled, time.Since(started)
		}
		// A second candidate may have raised the latch again between the
		// close and this check; the one timer still bounds the whole wait.
		if cleared = l.clearedChan(); cleared == nil {
			return prioritySendCleared, time.Since(started)
		}
	}
}

// awaitPrioritySend is the relay side of the latch: called right before a
// relay write opens its stream to this peer.
func (p *overlayPeer) awaitPrioritySend(ctx context.Context) {
	result, waited := p.prioritySend.await(ctx, quicPrioritySendWaitBound)
	if result == prioritySendNotWaited {
		return
	}
	p.node.observeBroadcastPipelineStageDuration(
		prioritySendWaitStage,
		prioritySendWaitKind,
		prioritySendWaitDelivery,
		prioritySendWaitResultLabel(result),
		waited,
	)
}

// quicPriorityBroadcastPeer is the send-side identity of this node's own
// candidate: the same route-bound write as quicRouteBroadcastPeer, but it
// raises the peer's latch for the duration of the write - before the stream
// is opened, until the write returns or fails - instead of honouring it.
// Only PrivateOverlay.BroadcastTwoStep hands these out; every forwarding and
// relay path keeps the plain type.
type quicPriorityBroadcastPeer struct {
	route quicRouteBroadcastPeer
}

var _ overlay.BroadcastPeer = quicPriorityBroadcastPeer{}
var _ overlay.PreparedBroadcastPeer = quicPriorityBroadcastPeer{}

func (p quicPriorityBroadcastPeer) ID() []byte {
	return p.route.ID()
}

func (p quicPriorityBroadcastPeer) SendCustomMessage(ctx context.Context, req tl.Serializable) error {
	latch := &p.route.peer.prioritySend
	latch.raise()
	defer latch.lower()

	peer, err := p.route.outbound()
	if err != nil {
		return err
	}
	return p.route.writeMessage(ctx, peer, req)
}

func (p quicPriorityBroadcastPeer) SendPreparedCustomMessage(ctx context.Context, body []byte) error {
	latch := &p.route.peer.prioritySend
	latch.raise()
	defer latch.lower()

	peer, err := p.route.outbound()
	if err != nil {
		return err
	}
	return p.route.writePrepared(ctx, peer, body)
}

// prioritizeTwoStepPeerSet marks the peers of this node's own candidate
// fan-out. The freshly resolved set is rewritten in place, so the only cost
// over the plain set is the interface value per QUIC peer; RLDP peers have no
// shared connection to order and stay as they are.
func prioritizeTwoStepPeerSet(peers overlay.StaticBroadcastPeerSet) overlay.StaticBroadcastPeerSet {
	for i, peer := range peers {
		if route, ok := peer.(quicRouteBroadcastPeer); ok {
			peers[i] = quicPriorityBroadcastPeer{route: route}
		}
	}
	return peers
}
