package p2p

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// Custom fixed overlays have no organic outbound traffic: this node mostly
// receives broadcasts, so a broken ADNL pair (channel desync, reinit
// mismatch, stale peer-side address) is never repaired and inbound freezes
// until a full restart. The probe keeps a low-rate bidirectional exchange
// with every fixed member and escalates through recovery stages while the
// pair stays silent.
//
// The probe query is overlay.ping, which tonutils-go overlay peers answer
// with overlay.pong. Peers that do not implement it still emit an ADNL-level
// nop after receiving the packet, so the shared-pair inbound timestamp can
// recover even when the query itself times out.
const (
	fixedProbeMinDelay = 20 * time.Second
	fixedProbeJitter   = 10 * time.Second
	fixedProbeTimeout  = 3 * time.Second

	// A soft recovery needs fixedProbeSoftFailures consecutive probe
	// failures while nothing arrived on the shared ADNL pair for
	// fixedProbeSilence.
	fixedProbeSoftFailures    uint32 = 3
	fixedProbeSilence                = 60 * time.Second
	fixedSoftRecoveryCooldown        = 90 * time.Second

	// A hard recovery needs fixedHardMinSoftAttempts soft recoveries that
	// did not bring the pair back and fixedHardSilence of total silence.
	fixedHardMinSoftAttempts   uint32 = 2
	fixedHardSilence                  = 5 * time.Minute
	fixedHardRecoveryCooldown         = 5 * time.Minute
	fixedPendingChannelTimeout        = 5 * time.Minute

	// When the pair is demonstrably alive but overlay broadcasts stopped,
	// send a low-impact ADNL nudge, but far less aggressively.
	fixedRxStaleThreshold = 10 * time.Minute
	fixedRxStaleCooldown  = 10 * time.Minute

	fixedProbeParallelism = 4
)

type fixedProbePolicy struct {
	minDelay              time.Duration
	jitter                time.Duration
	timeout               time.Duration
	softFailures          uint32
	silence               time.Duration
	softRecoveryCooldown  time.Duration
	hardMinSoftAttempts   uint32
	hardSilence           time.Duration
	hardRecoveryCooldown  time.Duration
	pendingChannelTimeout time.Duration
	rxStaleThreshold      time.Duration
	rxStaleCooldown       time.Duration
}

func defaultFixedProbePolicy() fixedProbePolicy {
	return fixedProbePolicy{
		minDelay:              fixedProbeMinDelay,
		jitter:                fixedProbeJitter,
		timeout:               fixedProbeTimeout,
		softFailures:          fixedProbeSoftFailures,
		silence:               fixedProbeSilence,
		softRecoveryCooldown:  fixedSoftRecoveryCooldown,
		hardMinSoftAttempts:   fixedHardMinSoftAttempts,
		hardSilence:           fixedHardSilence,
		hardRecoveryCooldown:  fixedHardRecoveryCooldown,
		pendingChannelTimeout: fixedPendingChannelTimeout,
		rxStaleThreshold:      fixedRxStaleThreshold,
		rxStaleCooldown:       fixedRxStaleCooldown,
	}
}

type adnlReiniter interface {
	Reinit()
}

func nextFixedProbeDelay(policy fixedProbePolicy) time.Duration {
	if policy.jitter <= 0 {
		return policy.minDelay
	}
	return policy.minDelay + time.Duration(rand.Int64N(int64(policy.jitter)))
}

func (s *overlaySubscription) startProbeFixedPeers(ctx context.Context, policy fixedProbePolicy) {
	if !s.spec.runsFixedPeerProbes() || !s.beginFixedProbe() {
		return
	}
	s.node.runAsync(func() {
		defer s.endFixedProbe()
		s.probeFixedPeers(ctx, policy)
	})
}

func (s *overlaySubscription) probeFixedPeers(ctx context.Context, policy fixedProbePolicy) {
	if !s.isActive() || !s.spec.runsFixedPeerProbes() {
		return
	}
	s.runPeerMaintenance(ctx, s.peersSnapshot(), fixedProbeParallelism, func(ctx context.Context, peer *overlayPeer) {
		s.probeFixedPeer(ctx, peer, policy)
	})
}

func (s *overlaySubscription) probeFixedPeer(ctx context.Context, peer *overlayPeer, policy fixedProbePolicy) {
	queryCtx, cancel := context.WithTimeout(ctx, policy.timeout)
	var pong overlay.Pong
	err := peer.overlay.Query(queryCtx, overlay.Ping{}, &pong)
	cancel()

	now := time.Now()
	if err == nil {
		peer.notePongReceived(now)
	} else {
		failures := peer.noteProbeFailed()
		s.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Uint32("failures", failures).
			Msg("custom overlay probe failed")
	}

	if ctx.Err() != nil {
		return
	}
	s.evaluateFixedPeerRecovery(ctx, peer, now, policy)
}

func (p *overlayPeer) notePongReceived(now time.Time) {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.lastPongAt = now
	p.probeFailures = 0
	p.softRecoveriesSinceSeen = 0
}

// noteProbeFailed intentionally does not touch failedQueries or
// unreliability: a remote that never implements overlay.ping would otherwise
// inflate them forever, and probe health is reported separately in status.
func (p *overlayPeer) noteProbeFailed() uint32 {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	p.probeFailures++
	return p.probeFailures
}

// adnlPairStats reports transport-level statistics of the ADNL pair shared by
// every overlay attached to this peer.
func (p *overlayPeer) adnlPairStats() (adnl.PeerStats, bool) {
	if p.overlay == nil || p.overlay.ADNLWrapper == nil || p.overlay.ADNL == nil {
		return adnl.PeerStats{}, false
	}
	return p.overlay.Stats(), true
}

type fixedRecoveryState struct {
	attachedAt    time.Time
	lastPongAt    time.Time
	lastReceiveAt time.Time
	probeFailures uint32
	lastSoftAt    time.Time
	lastHardAt    time.Time
	softAttempts  uint32
}

func (s *overlaySubscription) evaluateFixedPeerRecovery(ctx context.Context, peer *overlayPeer, now time.Time, policy fixedProbePolicy) {
	if !s.spec.runsFixedPeerProbes() {
		return
	}
	stats, ok := peer.adnlPairStats()
	if !ok {
		return
	}

	state := peer.fixedRecoverySnapshot(now, stats.Inbound.LastPacketAt, policy)
	pairSeenAt := latestTime(state.attachedAt, state.lastPongAt, stats.Inbound.LastPacketAt)
	pairSilence := now.Sub(pairSeenAt)

	// Root packets are not proof that the negotiated channel works. A
	// createChannel race can leave the peers with different keys forever:
	// probes still receive root nops while broadcasts sent through the remote
	// channel are unreadable. Channel negotiation is retried by every probe,
	// so a channel that remains pending through this timeout needs a fresh
	// transport even when ADNL inbound stays current.
	pendingSince := latestTime(state.attachedAt, stats.CreatedAt, stats.Reinitialization.LastPeerEventAt)
	if stats.Channel.State == adnl.PeerChannelStatePending &&
		now.Sub(pendingSince) >= policy.pendingChannelTimeout &&
		now.Sub(state.lastHardAt) >= policy.hardRecoveryCooldown {
		s.hardRecoverFixedPeer(ctx, peer, "channel_pending")
		return
	}

	// Hard: the pair stayed dead through soft recoveries; tear the shared
	// transport down and rebuild it from a fresh DHT resolve.
	if pairSilence >= policy.hardSilence &&
		state.softAttempts >= policy.hardMinSoftAttempts &&
		now.Sub(state.lastHardAt) >= policy.hardRecoveryCooldown {
		s.hardRecoverFixedPeer(ctx, peer, "pair_silent")
		return
	}

	// Soft: probes keep failing and nothing arrives on the shared pair; send
	// a nop. Once the pair has crossed tonutils-go's idle threshold, the
	// transport sends that packet in root format with a fresh address list.
	if state.probeFailures >= policy.softFailures &&
		pairSilence >= policy.silence &&
		now.Sub(state.lastSoftAt) >= policy.softRecoveryCooldown {
		s.softRecoverFixedPeer(ctx, peer, "pair_silent")
		return
	}

	// The transport answers but overlay broadcasts STOPPED: nudge the remote
	// in case its overlay-side state about us went stale. Gated on rx having
	// flowed at least once, so legitimately quiet overlays (e.g. a
	// sender-only role that never receives broadcasts) do not churn forever.
	if pairSilence < policy.silence &&
		!state.lastReceiveAt.IsZero() &&
		now.Sub(state.lastReceiveAt) >= policy.rxStaleThreshold &&
		now.Sub(state.lastSoftAt) >= policy.rxStaleCooldown {
		s.softRecoverFixedPeer(ctx, peer, "rx_stale")
	}
}

// fixedRecoverySnapshot copies the watchdog inputs and resets the failure
// counters when the pair produced a fresh signal: probe failures against a
// demonstrably alive pair (e.g. a remote that receives our pings but does
// not implement overlay.ping) are not evidence of a dead pair, and
// escalation to hard must restart from scratch after any sign of life.
func (p *overlayPeer) fixedRecoverySnapshot(now time.Time, adnlInboundAt time.Time, policy fixedProbePolicy) fixedRecoveryState {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	pairSeenAt := latestTime(p.attachedAt, p.lastPongAt, adnlInboundAt)
	if now.Sub(pairSeenAt) < policy.silence {
		p.softRecoveriesSinceSeen = 0
		p.probeFailures = 0
	}

	return fixedRecoveryState{
		attachedAt:    p.attachedAt,
		lastPongAt:    p.lastPongAt,
		lastReceiveAt: p.lastReceiveAt,
		probeFailures: p.probeFailures,
		lastSoftAt:    p.lastSoftRecoveryAt,
		lastHardAt:    p.lastHardRecoveryAt,
		softAttempts:  p.softRecoveriesSinceSeen,
	}
}

// softRecoverFixedPeer nudges a silent pair with outbound traffic. It
// deliberately does NOT reinit the shared ADNL in place: the old channel
// processor stays registered until a new channel is confirmed, so an
// in-place reinit lets stale in-flight channel packets poison the restarted
// sequence state, and two overlays sharing the pair could collide on channel
// re-creation. The nop instead drives the transport's own recovery: after
// two minutes of pair silence tonutils-go sends root-format packets with our
// address list, which any remote can decrypt regardless of channel state.
func (s *overlaySubscription) softRecoverFixedPeer(ctx context.Context, peer *overlayPeer, reason string) {
	peer.statsMx.Lock()
	peer.lastSoftRecoveryAt = time.Now()
	peer.softRecoveriesSinceSeen++
	peer.statsMx.Unlock()

	s.sendCustomFixedNop(ctx, peer)
	s.noteFixedRecovery(false)

	s.log.Info().
		Str("peer", peer.addr).
		Str("peer_id", peer.id.String()).
		Str("reason", reason).
		Msg("custom overlay peer soft recovery: nudged silent ADNL pair")
}

func (s *overlaySubscription) hardRecoverFixedPeer(ctx context.Context, peer *overlayPeer, reason string) {
	peer.statsMx.Lock()
	peer.lastHardRecoveryAt = time.Now()
	peer.softRecoveriesSinceSeen = 0
	peer.statsMx.Unlock()

	s.noteFixedRecovery(true)
	s.log.Warn().
		Str("peer", peer.addr).
		Str("peer_id", peer.id.String()).
		Str("reason", reason).
		Msg("custom overlay peer hard recovery: closing shared ADNL transport")

	// Closing the shared transport fires the disconnect cascade for every
	// subscription holding this peer; removing our entry synchronously makes
	// the follow-up discovery pass reconnect without waiting for the
	// asynchronous part of the cascade.
	if peer.overlay != nil && peer.overlay.ADNLWrapper != nil && peer.overlay.ADNL != nil {
		peer.overlay.ADNL.Close()
	}
	s.removePeerIfCurrent(peer)
	s.startPeerDiscovery(ctx)
}

func (s *overlaySubscription) noteFixedRecovery(hard bool) {
	s.mx.Lock()
	if hard {
		s.hardRecoveries++
	} else {
		s.softRecoveries++
	}
	s.mx.Unlock()
}

func latestTime(values ...time.Time) time.Time {
	var latest time.Time
	for _, value := range values {
		if value.After(latest) {
			latest = value
		}
	}
	return latest
}
