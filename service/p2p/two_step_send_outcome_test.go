package p2p

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// A budget this node imposed, a dial it deferred and a connection it has not
// opened are local state, not peer faults. Charging them to peer health used to
// evict validators from the private overlay this node must reach.
func TestTwoStepSendOutcomeChargesOnlyGenuinePeerFaults(t *testing.T) {
	now := int32(time.Now().Unix())
	id := testPeerID("two-step-outcome-peer")

	newSubscription := func() (*overlaySubscription, *overlayPeer) {
		peerOverlay, _ := newTestOverlayWrapper()
		peer := &overlayPeer{
			id:        id,
			overlay:   peerOverlay,
			announced: &overlay.Node{Version: now},
			alive:     true,
		}

		return testOverlaySubscription(&overlaySubscription{
			log:        discardLogger(),
			peers:      map[PeerID]*overlayPeer{id: peer},
			neighbours: []PeerID{id},
		}), peer
	}

	selfInflicted := []struct {
		name  string
		err   error
		fault TwoStepSendFault
	}{
		{"our own budget", context.DeadlineExceeded, TwoStepSendFaultContextDeadline},
		{"our own cancellation", context.Canceled, TwoStepSendFaultCanceled},
		{
			"a connection we never opened",
			fmt.Errorf("send prepared quic overlay message: %w", errQUICPeerOffline),
			TwoStepSendFaultOffline,
		},
		{
			"a dial we deferred",
			fmt.Errorf("dial QUIC peer: %w", errQUICDialDeferred),
			TwoStepSendFaultDialDeferred,
		},
		{
			"a stream deadline we set",
			fmt.Errorf("quic: write message: %w", os.ErrDeadlineExceeded),
			TwoStepSendFaultWriteDeadline,
		},
	}
	for _, testCase := range selfInflicted {
		sub, _ := newSubscription()
		outcome := sub.twoStepSendOutcome(overlay.BroadcastTwoStepSendResult{
			Attempted: 2,
			Sent:      1,
			Failed: []overlay.BroadcastTwoStepPeerError{
				{PeerID: id[:], Err: testCase.err},
			},
		})
		if outcome.Failed() != 1 {
			t.Fatalf("%s: failed recipients = %d, want 1", testCase.name, outcome.Failed())
		}
		if outcome.Faults[testCase.fault] != 1 {
			t.Fatalf("%s: classified as %+v", testCase.name, outcome.Faults)
		}
		if outcome.Faults[TwoStepSendFaultOther] != 0 {
			t.Fatalf("%s was charged to the peer: %+v", testCase.name, outcome.Faults)
		}
		if _, ok := sub.peers[id]; !ok {
			t.Fatalf("%s evicted the peer from the overlay", testCase.name)
		}
	}

	sub, _ := newSubscription()
	outcome := sub.twoStepSendOutcome(overlay.BroadcastTwoStepSendResult{
		Attempted: 2,
		Sent:      1,
		Failed: []overlay.BroadcastTwoStepPeerError{
			{PeerID: id[:], Err: adnl.ErrPeerConnClosed},
		},
	})
	if outcome.Faults[TwoStepSendFaultOther] != 1 {
		t.Fatalf("closed connection classified as %+v, want a peer fault", outcome.Faults)
	}
	if _, ok := sub.peers[id]; ok {
		t.Fatal("a genuine transport fault no longer reaches peer health")
	}
}
