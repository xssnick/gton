package p2p

import (
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// Skipping the attachment must not skip the roster. On the attached path
// attachPooledPeer refuses a non-member before it can ask anything, and the
// QUIC ingress gates the identical surface in authorizeQUICPeer - so without
// the same check here a stranger who knows a private overlay's short id (it is
// derivable from the public ADNL ids of its members) reaches block, proof,
// archive and persistent-state queries over ADNL/RLDP that the very same node
// refuses over QUIC.
func TestDetachedSubscriptionEnforcesOverlayMembership(t *testing.T) {
	memberID := testPeerID("detached-member")
	outsiderID := testPeerID("detached-outsider")
	overlayID := testPeerID("detached-overlay").Bytes()

	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:          "detached-private",
			Kind:          overlayKindCustomFixed,
			ShortID:       overlayID,
			AcceptQueries: true,
			FixedNodeIDs:  map[PeerID]struct{}{memberID: {}},
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	if got := node.detachedSubscription(&pooledPeer{id: outsiderID}, overlayID); got != nil {
		t.Fatal("detached path served a peer outside the private overlay roster")
	}
	if got := node.detachedSubscription(&pooledPeer{id: memberID}, overlayID); got != sub {
		t.Fatal("detached path refused a member of the private overlay")
	}

	// A public overlay stays open: it has no roster to check against.
	publicID := testPeerID("detached-public-overlay").Bytes()
	public := testOverlaySubscription(&overlaySubscription{
		node:  node,
		spec:  overlaySpec{Name: "detached-public", Kind: overlayKindPublicShard, ShortID: publicID},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(publicID)] = public
	node.subscriptionsMx.Unlock()

	if got := node.detachedSubscription(&pooledPeer{id: outsiderID}, publicID); got != public {
		t.Fatal("detached path refused a stranger on a public overlay")
	}
}

// The gate has to be wired into the entry points, not merely available: this
// drives Node.serveDetachedADNLQuery itself, so replacing detachedSubscription
// with a bare subscription lookup fails here instead of passing CI.
func TestServeDetachedADNLQueryRejectsNonMember(t *testing.T) {
	memberID := testPeerID("detached-wired-member")
	outsiderID := testPeerID("detached-wired-outsider")
	overlayID := testPeerID("detached-wired-overlay").Bytes()

	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "detached-wired",
			Kind:    overlayKindCustomFixed,
			ShortID: overlayID,
			// The node answers queries in this overlay, so the only thing left
			// standing between an outsider and block/proof/archive queries is
			// the roster check.
			AcceptQueries: true,
			FixedNodeIDs:  map[PeerID]struct{}{memberID: {}},
		},
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	node.subscriptionsMx.Lock()
	node.subscriptions[string(overlayID)] = sub
	node.subscriptionsMx.Unlock()

	msg := &adnl.MessageQuery{
		ID:   make([]byte, 32),
		Data: overlay.WrapQuery(overlayID, overlay.Ping{}),
	}
	if err := node.serveDetachedADNLQuery(&pooledPeer{id: outsiderID}, msg); !errors.Is(
		err,
		errDetachedUnknownOverlay,
	) {
		t.Fatalf("detached ADNL query from an outsider = %v, want unknown overlay", err)
	}
}
