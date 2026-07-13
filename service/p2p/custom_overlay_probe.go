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
//
// Variables instead of constants so tests can shrink the timings.
var (
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
)

const fixedProbeParallelism = 4

type adnlReiniter interface {
	Reinit()
}

func nextFixedProbeDelay() time.Duration {
	if fixedProbeJitter <= 0 {
		return fixedProbeMinDelay
	}
	return fixedProbeMinDelay + time.Duration(rand.Int64N(int64(fixedProbeJitter)))
}

func (s *overlaySubscription) startProbeFixedPeers(ctx context.Context) {
	if s.spec.Kind != overlayKindCustomFixed || !s.beginFixedProbe() {
		return
	}
	s.runMaintenanceAsync(func() {
		defer s.endFixedProbe()
		s.probeFixedPeers(ctx)
	})
}

func (s *overlaySubscription) probeFixedPeers(ctx context.Context) {
	if !s.isActive() || s.spec.Kind != overlayKindCustomFixed {
		return
	}
	s.runPeerMaintenance(ctx, s.peersSnapshot(), fixedProbeParallelism, s.probeFixedPeer)
}

func (s *overlaySubscription) probeFixedPeer(ctx context.Context, peer *overlayPeer) {
	queryCtx, cancel := context.WithTimeout(ctx, fixedProbeTimeout)
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
	s.evaluateFixedPeerRecovery(ctx, peer, now)
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
	if p == nil || p.overlay == nil || p.overlay.ADNLWrapper == nil || p.overlay.ADNL == nil {
		return adnl.PeerStats{}, false
	}
	return p.overlay.ADNL.Stats(), true
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

func (s *overlaySubscription) evaluateFixedPeerRecovery(ctx context.Context, peer *overlayPeer, now time.Time) {
	if s.spec.Kind != overlayKindCustomFixed {
		return
	}
	stats, ok := peer.adnlPairStats()
	if !ok {
		return
	}

	state := peer.fixedRecoverySnapshot(now, stats.Inbound.LastPacketAt)
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
		now.Sub(pendingSince) >= fixedPendingChannelTimeout &&
		now.Sub(state.lastHardAt) >= fixedHardRecoveryCooldown {
		s.hardRecoverFixedPeer(ctx, peer, "channel_pending")
		return
	}

	// Hard: the pair stayed dead through soft recoveries; tear the shared
	// transport down and rebuild it from a fresh DHT resolve.
	if pairSilence >= fixedHardSilence &&
		state.softAttempts >= fixedHardMinSoftAttempts &&
		now.Sub(state.lastHardAt) >= fixedHardRecoveryCooldown {
		s.hardRecoverFixedPeer(ctx, peer, "pair_silent")
		return
	}

	// Soft: probes keep failing and nothing arrives on the shared pair; send
	// a nop. Once the pair has crossed tonutils-go's idle threshold, the
	// transport sends that packet in root format with a fresh address list.
	if state.probeFailures >= fixedProbeSoftFailures &&
		pairSilence >= fixedProbeSilence &&
		now.Sub(state.lastSoftAt) >= fixedSoftRecoveryCooldown {
		s.softRecoverFixedPeer(ctx, peer, "pair_silent")
		return
	}

	// The transport answers but overlay broadcasts STOPPED: nudge the remote
	// in case its overlay-side state about us went stale. Gated on rx having
	// flowed at least once, so legitimately quiet overlays (e.g. a
	// sender-only role that never receives broadcasts) do not churn forever.
	if pairSilence < fixedProbeSilence &&
		!state.lastReceiveAt.IsZero() &&
		now.Sub(state.lastReceiveAt) >= fixedRxStaleThreshold &&
		now.Sub(state.lastSoftAt) >= fixedRxStaleCooldown {
		s.softRecoverFixedPeer(ctx, peer, "rx_stale")
	}
}

// fixedRecoverySnapshot copies the watchdog inputs and resets the failure
// counters when the pair produced a fresh signal: probe failures against a
// demonstrably alive pair (e.g. a remote that receives our pings but does
// not implement overlay.ping) are not evidence of a dead pair, and
// escalation to hard must restart from scratch after any sign of life.
func (p *overlayPeer) fixedRecoverySnapshot(now time.Time, adnlInboundAt time.Time) fixedRecoveryState {
	p.statsMx.Lock()
	defer p.statsMx.Unlock()

	pairSeenAt := latestTime(p.attachedAt, p.lastPongAt, adnlInboundAt)
	if now.Sub(pairSeenAt) < fixedProbeSilence {
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
