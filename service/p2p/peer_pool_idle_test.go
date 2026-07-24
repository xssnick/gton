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

	if got := pool.pruneIdleLockedForTest(now, 2); got != 1 {
		t.Fatalf("evicted peers = %d, want 1", got)
	}
	if pool.peers[oldest.id] != nil {
		t.Fatal("oldest idle peer survived cap pruning")
	}
	if len(pool.snapshot()) != 3 {
		t.Fatalf("pool size after cap pruning = %d, want 3", len(pool.snapshot()))
	}
	select {
	case <-oldestBase.GetCloserCtx().Done():
	default:
		t.Fatal("oldest idle transport was not closed")
	}
	for name, base := range map[string]*testOverlayADNL{
		"middle": middleBase,
		"newest": newestBase,
		"active": activeBase,
	} {
		select {
		case <-base.GetCloserCtx().Done():
			t.Fatalf("%s transport was evicted", name)
		default:
		}
	}
	if pool.peers[active.id] != active {
		t.Fatal("active transport was removed by idle cap")
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

	if got := pool.pruneIdleLockedForTest(now, 0); got != 0 {
		t.Fatalf("pruned active authenticated transport = %d, want 0", got)
	}
	if pool.peers[pooled.id] != pooled {
		t.Fatal("authenticated ADNL activity did not keep the transport in the pool")
	}

	lastPacketAt = now.Add(-peerPoolIdleTTL - time.Second)
	if got := pool.pruneIdleLockedForTest(now, 0); got != 1 {
		t.Fatalf("pruned expired authenticated transport = %d, want 1", got)
	}
}

func TestPeerPoolKeepsIdleTransportBelowCppNodeCap(t *testing.T) {
	now := time.Now()
	pooled, _ := newIdleTestPooledPeer("idle-below-cap", now.Add(-10*peerPoolIdleTTL), 0)
	pool := &peerPool{peers: map[PeerID]*pooledPeer{pooled.id: pooled}}
	t.Cleanup(pooled.close)

	if got := pool.pruneIdleLockedForTest(now, peerPoolMaxIdle); got != 0 {
		t.Fatalf("pruned idle transport below cap = %d, want 0", got)
	}
	if pool.peers[pooled.id] != pooled {
		t.Fatal("idle transport below the C++ node cap was removed")
	}
}

func TestPeerPoolCppNodeIdleCapEvictsOnlyOldestExcess(t *testing.T) {
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
		peer.touch(now.Add(-time.Duration(i+1) * time.Millisecond))
		peers[id] = peer
		oldest = id
	}
	pool := &peerPool{peers: peers}

	if got := pool.pruneIdle(now); got != 0 {
		t.Fatalf("pruned recent unreferenced transports = %d, want 0", got)
	}
	for _, peer := range pool.peers {
		order := binary.LittleEndian.Uint32(peer.id[:])
		peer.touch(now.Add(-peerPoolIdleTTL - time.Duration(order)*time.Second))
	}
	if got := pool.pruneIdle(now); got != 1 {
		t.Fatalf("pruned idle transports at C++ node cap = %d, want 1", got)
	}
	if len(pool.peers) != peerPoolMaxIdle {
		t.Fatalf("idle transports after cap = %d, want %d", len(pool.peers), peerPoolMaxIdle)
	}
	if pool.peers[oldest] != nil {
		t.Fatal("oldest idle transport survived cap pruning")
	}
}

func (p *peerPool) pruneIdleLockedForTest(now time.Time, maxIdle int) int {
	if maxIdle < 0 || maxIdle > peerPoolMaxIdle {
		panic("invalid test idle peer limit")
	}

	p.mx.Lock()
	placeholderADNL := overlay.CreateExtendedADNL(newTestOverlayADNL())
	placeholderIDs := make(map[PeerID]struct{}, peerPoolMaxIdle-maxIdle)
	for seed := uint32(1); len(placeholderIDs) < peerPoolMaxIdle-maxIdle; seed++ {
		var id PeerID
		binary.LittleEndian.PutUint32(id[:], seed)
		id[len(id)-1] = 0xff
		if p.peers[id] != nil {
			continue
		}
		peer := &pooledPeer{id: id, adnl: placeholderADNL}
		peer.touch(now.Add(-peerPoolIdleTTL))
		p.peers[id] = peer
		placeholderIDs[id] = struct{}{}
	}

	stale := p.pruneIdleLocked(now)
	for id := range placeholderIDs {
		delete(p.peers, id)
	}
	p.mx.Unlock()
	placeholderADNL.Close()

	pruned := 0
	for _, peer := range stale {
		if _, placeholder := placeholderIDs[peer.id]; placeholder {
			continue
		}
		peer.close()
		pruned++
	}
	return pruned
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
	pooled.touch(time.Now().Add(-peerPoolIdleTTL - time.Second))
	if got := pool.pruneIdle(time.Now()); got != 0 {
		t.Fatalf("pruned idle transports below cap = %d, want 0", got)
	}
	select {
	case <-base.GetCloserCtx().Done():
		t.Fatal("idle replacement transport below cap was closed")
	default:
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
