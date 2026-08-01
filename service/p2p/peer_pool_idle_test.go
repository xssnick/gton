package p2p

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func TestPeerPoolAcquireOverlayRejectsPrunedPeer(t *testing.T) {
	pool, pooled, _ := newTestLeasedPooledPeer("pruned-before-overlay-attach")
	t.Cleanup(pooled.close)
	overlayID := testPeerID("pruned-before-overlay-attach-id")
	receiver, err := overlay.NewBroadcastReceiver(overlayID[:], maxOverlayPayloadSize, true, false)
	if err != nil {
		t.Fatalf("create receiver: %v", err)
	}
	t.Cleanup(receiver.Close)

	pool.mx.Lock()
	delete(pool.peers, pooled.id)
	pool.mx.Unlock()

	_, _, release, err := pool.acquireOverlay(pooled, receiver, false)
	if !errors.Is(err, errPooledPeerUnavailable) {
		t.Fatalf("acquire pruned peer error = %v, want %v", err, errPooledPeerUnavailable)
	}
	if release != nil {
		t.Fatal("failed overlay acquire returned a release function")
	}
	if pooled.refs != 0 {
		t.Fatalf("failed overlay acquire retained %d references", pooled.refs)
	}
}

func TestPeerPoolIdleCapEvictsOldestAndNeverActive(t *testing.T) {
	now := time.Now()
	oldest, oldestBase := newIdleTestPooledPeer("oldest-idle", now.Add(-4*peerPoolIdleTTL), 0)
	middle, middleBase := newIdleTestPooledPeer("middle-idle", now.Add(-3*peerPoolIdleTTL), 0)
	newest, newestBase := newIdleTestPooledPeer("newest-idle", now.Add(-2*peerPoolIdleTTL), 0)
	active, activeBase := newIdleTestPooledPeer("active-old", now.Add(-5*peerPoolIdleTTL), 1)
	pool := &peerPool{peers: map[PeerID]*pooledPeer{
		oldest.id: oldest,
		middle.id: middle,
		newest.id: newest,
		active.id: active,
	}}
	t.Cleanup(func() {
		pool.mx.Lock()
		active.refs = 0
		pool.mx.Unlock()
		for _, pooled := range pool.snapshot() {
			pool.closeIfUnused(pooled)
		}
	})

	// All three idle peers are past the TTL, so all three go; the referenced
	// one is never a candidate however old it looks.
	if got := pool.pruneIdle(now); got != 3 {
		t.Fatalf("evicted peers = %d, want 3", got)
	}
	if len(pool.snapshot()) != 1 {
		t.Fatalf("pool size after pruning = %d, want 1", len(pool.snapshot()))
	}
	for name, base := range map[string]*testOverlayADNL{
		"oldest": oldestBase,
		"middle": middleBase,
		"newest": newestBase,
	} {
		select {
		case <-base.GetCloserCtx().Done():
		default:
			t.Fatalf("%s idle transport was not closed", name)
		}
	}
	select {
	case <-activeBase.GetCloserCtx().Done():
		t.Fatal("referenced transport was evicted")
	default:
	}
	if pool.peers[active.id] != active {
		t.Fatal("referenced transport was removed by the idle sweep")
	}
}

func TestPeerPoolIdleTTLUsesAuthenticatedADNLActivity(t *testing.T) {
	now := time.Now()
	pooled, base := newIdleTestPooledPeer("authenticated-activity", now.Add(-peerPoolIdleTTL-time.Second), 0)
	lastPacketAt := now.Add(-time.Second)
	base.statsFn = func() adnl.PeerStats {
		return adnl.PeerStats{
			Inbound: adnl.PeerInboundStats{LastPacketAt: lastPacketAt},
		}
	}
	pool := &peerPool{peers: map[PeerID]*pooledPeer{pooled.id: pooled}}
	t.Cleanup(pooled.close)

	if got := pool.pruneIdle(now); got != 0 {
		t.Fatalf("pruned active authenticated transport = %d, want 0", got)
	}
	if pool.peers[pooled.id] != pooled {
		t.Fatal("authenticated ADNL activity did not keep the transport in the pool")
	}

	lastPacketAt = now.Add(-peerPoolIdleTTL - time.Second)
	if got := pool.pruneIdle(now); got != 1 {
		t.Fatalf("pruned expired authenticated transport = %d, want 1", got)
	}
}

// TestPeerPoolClosesIdleTransportBelowCap is the behaviour this pool used to
// get wrong: an unreferenced transport that went quiet is closed on its TTL, no
// matter how small the pool is. Before, the sweep returned early below the cap,
// so peers accumulated until the pool passed 2048 - which never happened.
func TestPeerPoolClosesIdleTransportBelowCap(t *testing.T) {
	now := time.Now()
	pooled, base := newIdleTestPooledPeer("idle-below-cap", now.Add(-10*peerPoolIdleTTL), 0)
	pool := &peerPool{peers: map[PeerID]*pooledPeer{pooled.id: pooled}}
	t.Cleanup(pooled.close)

	if got := pool.pruneIdle(now); got != 1 {
		t.Fatalf("pruned idle transport below cap = %d, want 1", got)
	}
	if pool.peers[pooled.id] != nil {
		t.Fatal("expired idle transport stayed in the pool")
	}
	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("expired idle transport was dropped from the pool but not closed")
	}
}

// TestPeerPoolKeepsIdleTransportWithinTTL pins the grace period that makes
// release-then-reacquire cheap: a peer released a moment ago is still there.
func TestPeerPoolKeepsIdleTransportWithinTTL(t *testing.T) {
	now := time.Now()
	pooled, base := newIdleTestPooledPeer("idle-within-ttl", now.Add(-peerPoolIdleTTL/2), 0)
	pool := &peerPool{peers: map[PeerID]*pooledPeer{pooled.id: pooled}}
	t.Cleanup(pooled.close)

	if got := pool.pruneIdle(now); got != 0 {
		t.Fatalf("pruned transport inside the grace period = %d, want 0", got)
	}
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("transport inside the grace period was closed")
	default:
	}
}

// TestPeerPoolKeepsIdleTransportCarryingTraffic covers the peer that nothing
// references but that still exchanges ADNL packets: lastUsed reads the
// transport's own stats, so it must survive.
func TestPeerPoolKeepsIdleTransportCarryingTraffic(t *testing.T) {
	now := time.Now()
	pooled, base := newIdleTestPooledPeer("idle-with-traffic", now.Add(-10*peerPoolIdleTTL), 0)
	base.statsFn = func() adnl.PeerStats {
		return adnl.PeerStats{
			Inbound: adnl.PeerInboundStats{LastPacketAt: now.Add(-time.Second)},
		}
	}
	pool := &peerPool{peers: map[PeerID]*pooledPeer{pooled.id: pooled}}
	t.Cleanup(pooled.close)

	if got := pool.pruneIdle(now); got != 0 {
		t.Fatalf("pruned transport still carrying packets = %d, want 0", got)
	}
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("transport still carrying packets was closed")
	default:
	}
}

// TestPeerPoolReleasePathSkipsTTLScan pins the split: releasing a peer only
// enforces the hard cap, so it never walks the pool calling Stats(). The TTL
// belongs to the periodic sweep.
func TestPeerPoolReleasePathSkipsTTLScan(t *testing.T) {
	now := time.Now()
	pooled, base := newIdleTestPooledPeer("release-path", now.Add(-10*peerPoolIdleTTL), 0)
	pool := &peerPool{peers: map[PeerID]*pooledPeer{pooled.id: pooled}}
	t.Cleanup(pooled.close)

	pool.mx.Lock()
	stale := pool.pruneOverCapLocked(now)
	pool.mx.Unlock()
	if len(stale) != 0 {
		t.Fatalf("release path pruned %d transports below the cap, want 0", len(stale))
	}
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("release path closed a transport below the cap")
	default:
	}

	if got := pool.pruneIdle(now); got != 1 {
		t.Fatalf("periodic sweep pruned %d transports, want 1", got)
	}
}

func TestPeerPoolIdleCapEvictsOldestBeyondCap(t *testing.T) {
	now := time.Now()
	peers := make(map[PeerID]*pooledPeer, peerPoolMaxIdle+1)
	var oldest PeerID
	for i := 0; i <= peerPoolMaxIdle; i++ {
		var id PeerID
		binary.LittleEndian.PutUint32(id[:], uint32(i+1))
		peer := &pooledPeer{
			id:              id,
			adnl:            overlay.CreateExtendedADNL(newTestOverlayADNL()),
			adnlOverlayRefs: map[*overlay.ADNLOverlayWrapper]int{},
			rldpOverlayRefs: map[*overlay.RLDPOverlayWrapper]int{},
		}
		// Every peer is inside the TTL, so only the cap can evict.
		peer.touch(now.Add(-time.Duration(i+1) * time.Millisecond))
		peers[id] = peer
		oldest = id
	}
	pool := &peerPool{peers: peers}

	if got := pool.pruneIdle(now); got != 1 {
		t.Fatalf("pruned transports at the cap = %d, want 1", got)
	}
	if len(pool.peers) != peerPoolMaxIdle {
		t.Fatalf("transports after cap pruning = %d, want %d", len(pool.peers), peerPoolMaxIdle)
	}
	if pool.peers[oldest] != nil {
		t.Fatal("oldest idle transport survived cap pruning")
	}
}

func TestPeerPoolOverlayGenerationReleaseDoesNotDetachReplacement(t *testing.T) {
	pool, pooled, base := newTestLeasedPooledPeer("overlay-generation")
	t.Cleanup(pooled.close)
	overlayID := testPeerID("generation-overlay")
	oldReceiver, err := overlay.NewBroadcastReceiver(overlayID[:], maxOverlayPayloadSize, true, false)
	if err != nil {
		t.Fatalf("create old receiver: %v", err)
	}
	oldOverlay, _, releaseOld, err := pool.acquireOverlay(pooled, oldReceiver, false)
	if err != nil {
		t.Fatalf("attach old receiver: %v", err)
	}
	oldReceiver.Close()

	newReceiver, err := overlay.NewBroadcastReceiver(overlayID[:], maxOverlayPayloadSize, true, false)
	if err != nil {
		t.Fatalf("create replacement receiver: %v", err)
	}
	t.Cleanup(newReceiver.Close)
	newOverlay, _, releaseNew, err := pool.acquireOverlay(pooled, newReceiver, false)
	if err != nil {
		t.Fatalf("attach replacement receiver: %v", err)
	}
	if newOverlay == oldOverlay {
		t.Fatal("replacement receiver reused the closed overlay generation")
	}

	releaseOld()
	attached, err := pooled.adnl.AttachOverlay(newReceiver)
	if err != nil {
		t.Fatalf("look up replacement overlay after stale release: %v", err)
	}
	if attached != newOverlay {
		t.Fatal("stale overlay release detached the replacement generation")
	}
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("stale overlay release closed a transport used by the replacement")
	default:
	}

	releaseNew()
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("last overlay release skipped the idle transport grace period")
	default:
	}

	// The grace period is what makes release-then-reacquire cheap, but it does
	// expire: once the transport is past the TTL the sweep closes it.
	pooled.touch(time.Now().Add(-peerPoolIdleTTL - time.Second))
	if got := pool.pruneIdle(time.Now()); got != 1 {
		t.Fatalf("pruned expired idle transports = %d, want 1", got)
	}
	select {
	case <-base.GetCloserCtx().Done():
	default:
		t.Fatal("expired idle transport was not closed")
	}
}

func newIdleTestPooledPeer(label string, lastUsed time.Time, refs int) (*pooledPeer, *testOverlayADNL) {
	base := newTestOverlayADNL()
	id := testPeerID(label)
	pooled := &pooledPeer{
		id:              id,
		addr:            label,
		adnl:            overlay.CreateExtendedADNL(base),
		refs:            refs,
		adnlOverlayRefs: map[*overlay.ADNLOverlayWrapper]int{},
		rldpOverlayRefs: map[*overlay.RLDPOverlayWrapper]int{},
	}
	pooled.touch(lastUsed)
	return pooled, base
}
