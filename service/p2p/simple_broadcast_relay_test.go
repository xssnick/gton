package p2p

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// spentRelayEgressBudget is a relay budget of a byte a second with its one-byte
// burst spent: no forwarded part is admitted for minutes.
func spentRelayEgressBudget(t *testing.T) *relayEgressBudget {
	t.Helper()

	start := time.Now()
	budget := newRelayEgressBudget(8, start)
	if !budget.allow(start, 1) {
		t.Fatal("the burst must admit a full second of the budget")
	}
	return budget
}

// Simple broadcasts — the external messages of a few hundred bytes — are
// relayed outside the FEC relay budget: with the budget spent on FEC parts the
// FEC relay drops, while the simple relay still reaches every sampled peer.
func TestSimpleBroadcastRelayIsNotMeteredByTheFECBudget(t *testing.T) {
	node := newTestNode(t)
	node.relayEgress = spentRelayEgressBudget(t)

	const peers = 3
	roster := make(map[PeerID]*overlayPeer, peers)
	inner := make([]*recordingRelayPeer, 0, peers)
	for i := 0; i < peers; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("simple-relay-%d", i))
		recorder := &recordingRelayPeer{}
		peer.broadcastPeer = recorder
		roster[peer.id] = peer
		inner = append(inner, recorder)
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		log:   discardLogger(),
		spec:  overlaySpec{Name: "test"},
		peers: roster,
	})

	part := overlay.NewPreparedBroadcastMessage(make([]byte, 400))
	sendAll := func(set overlay.BroadcastPeerSet) {
		for _, peer := range set.Peers() {
			framed, ok := peer.(overlay.PreparedBroadcastMessagePeer)
			if !ok {
				t.Fatal("a relay peer must keep the prepared-frame fast path")
			}
			if err := framed.SendPreparedBroadcastMessage(context.Background(), part); err != nil {
				t.Fatal(err)
			}
		}
	}

	sendAll(overlayFECRelayPeerSet{sub: sub})
	for i, recorder := range inner {
		if recorder.framed != 0 {
			t.Fatalf("FEC relay peer %d received %d parts beyond the spent budget", i, recorder.framed)
		}
	}

	sendAll(overlaySimpleRelayPeerSet{sub: sub})
	for i, recorder := range inner {
		if recorder.framed != 1 {
			t.Fatalf("simple relay peer %d received %d broadcasts, want 1 regardless of the FEC budget", i, recorder.framed)
		}
	}

	node.relayEgress.mu.Lock()
	dropped := node.relayEgress.droppedParts
	node.relayEgress.mu.Unlock()
	if dropped != peers {
		t.Fatalf("relay budget drops = %d, want only the %d FEC parts", dropped, peers)
	}
}

// The public receiver forwards an accepted external message broadcast to its
// neighbour while the relay budget is spent: the simple relay it is wired to is
// not the budgeted FEC one.
func TestSimpleBroadcastRelayIgnoresSpentBudgetThroughReceiver(t *testing.T) {
	node := newTestNode(t)
	node.relayEgress = spentRelayEgressBudget(t)

	overlayID := testPeerID("simple-relay-budget-overlay")
	sub := mustGetOrCreateSubscription(t, node, overlaySpec{
		Name:    "public.simple-relay-budget",
		Kind:    overlayKindPublicShard,
		ShortID: overlayID.Bytes(),
	})

	relayTransport := &testSimpleRelayPeer{
		id:   testPeerID("simple-relay-budget-target").Bytes(),
		sent: make(chan tl.Serializable, 1),
	}
	relayPeer := testRebroadcastQueuePeer("simple-relay-budget-target")
	relayPeer.broadcastPeer = relayTransport
	sub.mx.Lock()
	sub.peers[relayPeer.id] = relayPeer
	sub.neighbours = append(sub.neighbours, relayPeer.id)
	sub.notifyPeersChangedLocked()
	sub.mx.Unlock()

	inbound := newTestOverlayADNL()
	inbound.id = testPeerID("simple-relay-budget-source").Bytes()
	attached, err := overlay.CreateExtendedADNL(inbound).AttachOverlay(sub.broadcastReceiver)
	if err != nil {
		t.Fatalf("attach public broadcast receiver: %v", err)
	}
	t.Cleanup(attached.Close)

	msg := signedTestSimpleBroadcast(t, tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: testExternalMessageBOC(t)},
	})
	if err = inbound.customHandler(&adnl.MessageCustom{Data: []tl.Serializable{
		overlay.Message{Overlay: overlayID.Bytes()},
		msg,
	}}); err != nil {
		t.Fatalf("process public simple external broadcast: %v", err)
	}

	select {
	case <-relayTransport.sent:
	case <-time.After(5 * time.Second):
		t.Fatal("simple broadcast was not relayed with the relay budget spent")
	}
}

// The simple relay forwards to the bounded relay sample, not to the roster.
func TestSimpleBroadcastRelayTargetsAreBounded(t *testing.T) {
	sub := newRelayFanoutSubscription(t, 300, 16)
	if got := len(overlaySimpleRelayPeerSet{sub: sub}.Peers()); got != broadcastFECRelayFanout {
		t.Fatalf("simple relay targets = %d, want %d", got, broadcastFECRelayFanout)
	}
}
