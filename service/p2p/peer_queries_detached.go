package p2p

import (
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
)

var errDetachedUnknownOverlay = errors.New("query for an overlay this node does not serve")

// detachedSubscription resolves the overlay a detached query targets and
// applies the same membership gate an attachment would have applied.
//
// Skipping only the attachment must not skip the roster: acceptsPeerID is what
// keeps a private custom overlay and a fast-sync overlay closed, and on the
// attached path attachPooledPeer enforces it before a peer can ask anything.
// The QUIC ingress gates the identical surface in authorizeQUICPeer, so without
// this a stranger who knows the overlay's short id - derivable from the public
// ADNL ids of its members - would reach block, proof, archive and persistent
// state queries over ADNL/RLDP that the same node refuses over QUIC.
func (n *Node) detachedSubscription(pooled *pooledPeer, overlayID []byte) *overlaySubscription {
	if overlayID == nil {
		return nil
	}
	sub := n.subscriptionByOverlayShortID(overlayID)
	if sub == nil || !sub.acceptsPeerID(pooled.id) {
		return nil
	}
	return sub
}

// serveDetachedADNLQuery routes an overlay query that arrived on a transport
// with no attachment for the target overlay. Answering these is what makes it
// safe to keep only a small live set: a peer we did not attach must still be
// served, otherwise it stops considering us a usable neighbour.
func (n *Node) serveDetachedADNLQuery(pooled *pooledPeer, msg *adnl.MessageQuery) error {
	unwrapped, overlayID := overlay.UnwrapQuery(msg.Data)
	sub := n.detachedSubscription(pooled, overlayID)
	if sub == nil {
		return fmt.Errorf("%w: %x", errDetachedUnknownOverlay, overlayID)
	}
	return sub.serveDetachedADNLQuery(pooled, msg, unwrapped)
}

func (n *Node) serveDetachedRLDPQuery(pooled *pooledPeer, transferID []byte, query *rldp.Query) error {
	unwrapped, overlayID := overlay.UnwrapQuery(query.Data)
	sub := n.detachedSubscription(pooled, overlayID)
	if sub == nil {
		return fmt.Errorf("%w: %x", errDetachedUnknownOverlay, overlayID)
	}
	return sub.serveDetachedRLDPQuery(pooled, transferID, query, unwrapped)
}
