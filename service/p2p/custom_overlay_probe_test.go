package p2p

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func overlayWrapperForTest(base *testOverlayADNL) *overlay.ADNLWrapper {
	return overlay.CreateExtendedADNL(base)
}

func newProbeTestPeer(t *testing.T) (*overlaySubscription, *overlayPeer, *testOverlayADNL) {
	t.Helper()

	wrapper, base := newTestOverlayWrapper()
	sub := testOverlaySubscription(&overlaySubscription{
		spec:  overlaySpec{Name: "custom.probe", Kind: overlayKindCustomFixed},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	peer := &overlayPeer{
		id:          testPeerID("probe-peer"),
		addr:        "127.0.0.1:14001",
		fixedMember: true,
		overlay:     wrapper,
		release:     func() {},
		attachedAt:  time.Now().Add(-time.Hour),
	}
	sub.peers[peer.id] = peer
	return sub, peer, base
}

func testPairInboundAt(base *testOverlayADNL, at *time.Time) {
	base.statsFn = func() adnl.PeerStats {
		var stats adnl.PeerStats
		if !at.IsZero() {
			stats.Inbound.LastPacketAt = *at
		}
		return stats
	}
}

func (s *overlaySubscription) recoverySnapshot() (uint64, uint64) {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.softRecoveries, s.hardRecoveries
}

func TestFixedProbeSoftRecoveryAfterFailuresAndSilence(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	inboundAt := now.Add(-2 * time.Minute)
	testPairInboundAt(base, &inboundAt)

	for i := 0; i < 3; i++ {
		peer.noteProbeFailed()
	}

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())

	if got := base.nops.Load(); got != 1 {
		t.Fatalf("recovery nops = %d, want 1", got)
	}
	if got := base.reinits.Load(); got != 0 {
		t.Fatalf("adnl reinits = %d, want 0: soft recovery must not reinit a live shared pair", got)
	}
	if soft, hard := sub.recoverySnapshot(); soft != 1 || hard != 0 {
		t.Fatalf("recoveries = %d soft / %d hard, want 1 / 0", soft, hard)
	}

	// Cooldown suppresses an immediate second soft recovery.
	sub.evaluateFixedPeerRecovery(context.Background(), peer, now.Add(time.Second), defaultFixedProbePolicy())
	if got := base.nops.Load(); got != 1 {
		t.Fatalf("recovery nops after cooldown window = %d, want 1", got)
	}
}

func TestFixedProbeNoSoftRecoveryWhilePairInboundFresh(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	inboundAt := now.Add(-time.Second)
	testPairInboundAt(base, &inboundAt)

	for i := 0; i < 5; i++ {
		peer.noteProbeFailed()
	}
	peer.statsMx.Lock()
	peer.lastReceiveAt = now.Add(-time.Minute)
	peer.statsMx.Unlock()

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())

	if got := base.nops.Load(); got != 0 {
		t.Fatalf("recovery nops = %d, want 0 while pair inbound is fresh", got)
	}
	if soft, hard := sub.recoverySnapshot(); soft != 0 || hard != 0 {
		t.Fatalf("recoveries = %d soft / %d hard, want none", soft, hard)
	}
	if stats := peer.statsSnapshot(); stats.probeFailures != 0 {
		t.Fatalf("probe failures = %d, want reset while the pair is demonstrably alive", stats.probeFailures)
	}
}

func TestFixedProbeAttachGraceSuppressesRecovery(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	peer.statsMx.Lock()
	peer.attachedAt = now
	peer.statsMx.Unlock()
	var inboundAt time.Time
	testPairInboundAt(base, &inboundAt)

	for i := 0; i < 10; i++ {
		peer.noteProbeFailed()
	}

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())

	if got := base.nops.Load(); got != 0 {
		t.Fatalf("recovery nops = %d, want 0 for freshly attached peer", got)
	}
}

func TestFixedProbeHardRecoveryClosesTransportAndRemovesPeer(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	inboundAt := now.Add(-6 * time.Minute)
	testPairInboundAt(base, &inboundAt)

	peer.statsMx.Lock()
	peer.probeFailures = 10
	peer.softRecoveriesSinceSeen = 2
	peer.lastSoftRecoveryAt = now.Add(-2 * time.Minute)
	peer.statsMx.Unlock()

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())

	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("hard recovery did not close the shared ADNL transport")
	}
	sub.mx.Lock()
	_, stillThere := sub.peers[peer.id]
	sub.mx.Unlock()
	if stillThere {
		t.Fatal("hard recovery did not remove the peer from the subscription")
	}
	if soft, hard := sub.recoverySnapshot(); soft != 0 || hard != 1 {
		t.Fatalf("recoveries = %d soft / %d hard, want 0 / 1", soft, hard)
	}
}

func TestFixedProbeHardRecoveryForStuckPendingChannelWithFreshADNL(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	pendingSince := now.Add(-6 * time.Minute)

	peer.statsMx.Lock()
	peer.attachedAt = pendingSince
	peer.lastReceiveAt = now.Add(-14 * time.Minute)
	peer.statsMx.Unlock()
	base.statsFn = func() adnl.PeerStats {
		return adnl.PeerStats{
			CreatedAt: pendingSince,
			Channel: adnl.PeerChannelStats{
				State: adnl.PeerChannelStatePending,
			},
			Inbound: adnl.PeerInboundStats{
				LastPacketAt: now.Add(-time.Second),
			},
		}
	}

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())

	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("stuck pending channel did not close despite fresh root ADNL traffic")
	}
	if soft, hard := sub.recoverySnapshot(); soft != 0 || hard != 1 {
		t.Fatalf("recoveries = %d soft / %d hard, want 0 / 1", soft, hard)
	}
}

func TestFixedProbeRxStaleWithLivePairTriggersSingleSoftRecovery(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	inboundAt := now.Add(-time.Second)
	testPairInboundAt(base, &inboundAt)

	peer.statsMx.Lock()
	peer.lastPongAt = now.Add(-time.Second)
	peer.lastReceiveAt = now.Add(-11 * time.Minute)
	peer.statsMx.Unlock()

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())
	if got := base.nops.Load(); got != 1 {
		t.Fatalf("recovery nops = %d, want 1 for stale rx over live pair", got)
	}

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now.Add(time.Second), defaultFixedProbePolicy())
	if got := base.nops.Load(); got != 1 {
		t.Fatalf("recovery nops = %d, want cooldown to suppress the second recovery", got)
	}
}

func TestFixedProbeRxStaleRequiresPriorTraffic(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	inboundAt := now.Add(-time.Second)
	testPairInboundAt(base, &inboundAt)

	// The pair is alive, the peer attached long ago, but this overlay never
	// delivered anything: a legitimately quiet overlay must not churn.
	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())

	if got := base.nops.Load(); got != 0 {
		t.Fatalf("recovery nops = %d, want 0 when rx never flowed", got)
	}
	if soft, _ := sub.recoverySnapshot(); soft != 0 {
		t.Fatalf("soft recoveries = %d, want 0 when rx never flowed", soft)
	}
}

func TestFixedProbePongResetsFailureState(t *testing.T) {
	_, peer, _ := newProbeTestPeer(t)

	for i := 0; i < 3; i++ {
		peer.noteProbeFailed()
	}
	peer.statsMx.Lock()
	peer.softRecoveriesSinceSeen = 2
	peer.statsMx.Unlock()

	peer.notePongReceived(time.Now())

	stats := peer.statsSnapshot()
	if stats.probeFailures != 0 {
		t.Fatalf("probe failures = %d, want 0 after pong", stats.probeFailures)
	}
	if stats.lastPongAt.IsZero() {
		t.Fatal("pong timestamp was not recorded")
	}
	peer.statsMx.Lock()
	softAttempts := peer.softRecoveriesSinceSeen
	peer.statsMx.Unlock()
	if softAttempts != 0 {
		t.Fatalf("soft attempts = %d, want reset after pong", softAttempts)
	}
}

func TestFixedProbeFreshPairSignalResetsSoftAttempts(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	now := time.Now()
	inboundAt := now.Add(-time.Second)
	testPairInboundAt(base, &inboundAt)

	peer.statsMx.Lock()
	peer.softRecoveriesSinceSeen = 2
	peer.lastReceiveAt = now
	peer.statsMx.Unlock()

	sub.evaluateFixedPeerRecovery(context.Background(), peer, now, defaultFixedProbePolicy())

	peer.statsMx.Lock()
	softAttempts := peer.softRecoveriesSinceSeen
	peer.statsMx.Unlock()
	if softAttempts != 0 {
		t.Fatalf("soft attempts = %d, want reset once the pair produced a signal", softAttempts)
	}
	if got := base.reinits.Load(); got != 0 {
		t.Fatalf("adnl reinits = %d, want 0", got)
	}
}

func TestFixedProbeQueryOutcomeUpdatesPeerState(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	peer.statsMx.Lock()
	peer.attachedAt = time.Now()
	peer.statsMx.Unlock()

	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		return nil
	}
	sub.probeFixedPeer(context.Background(), peer, defaultFixedProbePolicy())
	if stats := peer.statsSnapshot(); stats.lastPongAt.IsZero() || stats.probeFailures != 0 {
		t.Fatalf("pong was not recorded: %+v", stats)
	}

	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		return errors.New("no route")
	}
	sub.probeFixedPeer(context.Background(), peer, defaultFixedProbePolicy())
	// The pair is demonstrably alive (fresh pong just above), so the
	// watchdog pass inside probeFixedPeer clears the recorded failure:
	// failures only accumulate against a silent pair.
	if stats := peer.statsSnapshot(); stats.probeFailures != 0 {
		t.Fatalf("probe failures = %d, want 0 against a live pair", stats.probeFailures)
	}
}

func TestReinitFreshFixedTransportOnlyBeforeInbound(t *testing.T) {
	base := newTestOverlayADNL()
	pooled := &pooledPeer{
		id:   testPeerID("fresh-transport"),
		adnl: overlayWrapperForTest(base),
	}

	reinitFreshFixedTransport(pooled)
	if got := base.reinits.Load(); got != 1 {
		t.Fatalf("reinits = %d, want 1 for a transport with no inbound traffic", got)
	}

	base.statsFn = func() adnl.PeerStats {
		return adnl.PeerStats{Inbound: adnl.PeerInboundStats{Packets: 5}}
	}
	reinitFreshFixedTransport(pooled)
	if got := base.reinits.Load(); got != 1 {
		t.Fatalf("reinits = %d, want no reinit once the pair has inbound traffic", got)
	}

	base.statsFn = func() adnl.PeerStats {
		return adnl.PeerStats{Outbound: adnl.PeerOutboundStats{Packets: 1}}
	}
	reinitFreshFixedTransport(pooled)
	if got := base.reinits.Load(); got != 1 {
		t.Fatalf("reinits = %d, want no reinit once a handshake may be in flight", got)
	}
}

func TestPeerPoolPrunesClosedTransports(t *testing.T) {
	base := newTestOverlayADNL()
	id := testPeerID("closed-transport")
	pooled := &pooledPeer{id: id, adnl: overlayWrapperForTest(base)}
	pool := &peerPool{peers: map[PeerID]*pooledPeer{id: pooled}}

	if got := pool.retainByID(id); got != pooled {
		t.Fatal("live pooled transport was not retained")
	}

	base.Close()
	if got := pool.retainByID(id); got != nil {
		t.Fatal("closed pooled transport was retained instead of pruned")
	}
	if _, ok := pool.peers[id]; ok {
		t.Fatal("closed pooled transport is still registered in the pool")
	}
}

func TestFixedProbeSkipsNonFixedOverlays(t *testing.T) {
	sub, peer, base := newProbeTestPeer(t)
	sub.spec.Kind = overlayKindPublicShard

	base.queryResponder = func(req tl.Serializable, result tl.Serializable) error {
		t.Fatal("probe query must not be sent for non-fixed overlays")
		return nil
	}
	sub.probeFixedPeers(context.Background(), defaultFixedProbePolicy())

	sub.evaluateFixedPeerRecovery(context.Background(), peer, time.Now(), defaultFixedProbePolicy())
	if got := base.reinits.Load(); got != 0 {
		t.Fatalf("adnl reinits = %d, want 0 for non-fixed overlay", got)
	}
}
