package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/ton"
)

func TestSetActiveShardOverlaysDefersLeasedSubscriptionDeactivation(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, 32)
	node.SetMonitorMinSplitDepth(0, 1)

	leftShard := int64(0x4000000000000000)
	rightShard := int64(-0x4000000000000000)
	if err := node.SetActiveShardOverlays([]ton.BlockIDExt{{Workchain: 0, Shard: leftShard}}); err != nil {
		t.Fatalf("set active left overlay: %v", err)
	}
	leftSub := testSubscriptionForOverlay(t, node, 0, leftShard)
	release, err := leftSub.beginArchiveUse()
	if err != nil {
		t.Fatalf("begin archive use: %v", err)
	}

	if err = node.SetActiveShardOverlays([]ton.BlockIDExt{{Workchain: 0, Shard: rightShard}}); err != nil {
		t.Fatalf("set active right overlay: %v", err)
	}
	if !leftSub.isActive() {
		t.Fatal("active shard update deactivated leased historical overlay")
	}

	expiredAt := time.Now().Add(inactiveShardOverlayTTL + time.Second)
	node.stopExpiredInactiveSubscriptions(expiredAt)
	if _, ok := findSubscriptionForOverlay(t, node, 0, leftShard); !ok {
		t.Fatal("inactive cleanup removed leased historical overlay")
	}

	release()
	if leftSub.isActive() {
		t.Fatal("pending shard deactivation was not applied after lease release")
	}
	node.stopExpiredInactiveSubscriptions(expiredAt)
	if _, ok := findSubscriptionForOverlay(t, node, 0, leftShard); ok {
		t.Fatal("released historical overlay was not removed after its TTL")
	}
}

func TestArchiveUseLeasePreventsSubscriptionDeactivationAndRemoval(t *testing.T) {
	node, sub, key := newArchiveLeaseTestSubscription(t)
	release, err := sub.beginArchiveUse()
	if err != nil {
		t.Fatalf("begin archive use: %v", err)
	}
	defer release()

	deleteAt := time.Now().Add(-time.Second)
	if sub.setActive(false, deleteAt) {
		t.Fatal("leased subscription was marked inactive")
	}
	if !sub.isActive() {
		t.Fatal("archive use lease did not keep subscription active")
	}

	node.stopExpiredInactiveSubscriptions(time.Now())
	node.subscriptionsMx.RLock()
	retained := node.subscriptions[key] == sub
	node.subscriptionsMx.RUnlock()
	if !retained {
		t.Fatal("inactive cleanup removed leased subscription")
	}
}

func TestArchiveUseLeaseAppliesPendingDeactivationAfterRelease(t *testing.T) {
	node, sub, key := newArchiveLeaseTestSubscription(t)
	release, err := sub.beginArchiveUse()
	if err != nil {
		t.Fatalf("begin archive use: %v", err)
	}

	deleteAt := time.Now().Add(-time.Second)
	sub.setActive(false, deleteAt)
	release()

	if sub.isActive() {
		t.Fatal("pending deactivation was not applied after archive use")
	}
	if got, ok := sub.inactiveExpiresAt(); !ok || !got.Equal(deleteAt) {
		t.Fatalf("inactive expiry = %v, ok=%v, want %v", got, ok, deleteAt)
	}

	node.stopExpiredInactiveSubscriptions(time.Now())
	node.subscriptionsMx.RLock()
	retained := node.subscriptions[key] == sub
	node.subscriptionsMx.RUnlock()
	if retained {
		t.Fatal("released inactive subscription was not removed")
	}
}

func TestInactiveSubscriptionReplacementWaitsForReceiverGenerationClose(t *testing.T) {
	node := newTestNode(t)
	pool, pooled, _ := newTestLeasedPooledPeer("inactive-generation")
	node.pool = pool
	t.Cleanup(pooled.close)

	spec := overlaySpec{
		Name:        "inactive-generation",
		Kind:        overlayKindPublicShard,
		ShortID:     testPeerID("inactive-generation-overlay").Bytes(),
		RandomPeers: false,
	}
	oldSub := mustGetOrCreateSubscription(t, node, spec)
	_, _, releaseOld, err := pool.acquireOverlay(pooled, oldSub.broadcastReceiver, false)
	if err != nil {
		t.Fatalf("attach old receiver generation: %v", err)
	}
	t.Cleanup(releaseOld)

	deleteAt := time.Now().Add(-time.Second)
	oldSub.setActive(false, deleteAt)
	cancelEntered := make(chan struct{})
	allowCancel := make(chan struct{})
	oldSub.mx.Lock()
	oldSub.cancel = func() {
		close(cancelEntered)
		<-allowCancel
	}
	oldSub.mx.Unlock()

	deleted := make(chan bool, 1)
	go func() {
		deleted <- node.deleteInactiveSubscription(overlaySpecKey(spec), oldSub, time.Now())
	}()
	select {
	case <-cancelEntered:
	case <-time.After(time.Second):
		t.Fatal("inactive subscription close did not reach the cancellation barrier")
	}

	var replacement *overlaySubscription
	created := make(chan error, 1)
	go func() {
		var createErr error
		replacement, createErr = node.getOrCreateSubscription(spec)
		created <- createErr
	}()
	select {
	case createErr := <-created:
		if createErr != nil {
			t.Fatalf("create replacement subscription: %v", createErr)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement subscription remained blocked after old receiver closed")
	}
	if replacement == oldSub {
		t.Fatal("inactive subscription generation was reused")
	}
	if replacement.peerByID(pooled.id) == nil {
		t.Fatal("replacement receiver failed to attach the existing pooled peer")
	}

	close(allowCancel)
	if !<-deleted {
		t.Fatal("inactive subscription was not deleted")
	}
}

func TestArchiveUseLeaseKeepsEnsurePeersActive(t *testing.T) {
	_, sub, _ := newArchiveLeaseTestSubscription(t)
	peerID := testPeerID("archive-lease-peer")
	sub.peers[peerID] = &overlayPeer{
		id:        peerID,
		announced: &overlay.Node{Version: int32(time.Now().Unix())},
		overlay:   &overlay.ADNLOverlayWrapper{},
		release:   func() {},
		alive:     true,
	}

	release, err := sub.beginArchiveUse()
	if err != nil {
		t.Fatalf("begin archive use: %v", err)
	}
	defer release()
	sub.setActive(false, time.Now().Add(time.Minute))

	if err = sub.ensurePeers(context.Background()); err != nil {
		t.Fatalf("ensure peers while archive use is leased: %v", err)
	}
}

func TestArchivePeerPoolUseLeasesSubscription(t *testing.T) {
	_, sub, _ := newArchiveLeaseTestSubscription(t)
	deleteAt := time.Now().Add(time.Minute)
	sub.setActive(false, deleteAt)

	session := sub.node.BeginArchiveSession()
	defer session.Close()
	_, release, err := session.useArchivePeerPool(sub)
	if err != nil {
		t.Fatalf("use archive peer pool: %v", err)
	}
	if !sub.isActive() {
		t.Fatal("archive peer pool use did not activate subscription")
	}

	sub.setActive(false, deleteAt)
	release()
	if sub.isActive() {
		t.Fatal("subscription remained active after archive peer pool release")
	}
}

func newArchiveLeaseTestSubscription(t *testing.T) (*Node, *overlaySubscription, string) {
	t.Helper()

	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name: "archive-lease",
			Kind: overlayKindPublicShard,
		},
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{},
		peerNotify: make(chan struct{}, 1),
	})
	key := overlaySpecKey(sub.spec)
	node.subscriptionsMx.Lock()
	node.subscriptions[key] = sub
	node.subscriptionsMx.Unlock()
	return node, sub, key
}
