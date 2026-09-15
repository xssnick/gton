package p2p

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
)

// A local external message refused by the p2p address limiter must not reach
// admission: admission puts it into the node's pool, and the client would be
// told it failed while the collator can still include it.
func TestSendExternalMessageAddressLimitRejectsBeforeAdmission(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission

	body := testExternalMessageBOC(t)
	parsed, err := parseExternalMessageData(body)
	if err != nil {
		t.Fatalf("parse test external message: %v", err)
	}
	now := time.Now()
	for i := 0; i < extmsg.DefaultAddressLimit; i++ {
		if err = node.externalMessageLimiter.Add(parsed.address, now); err != nil {
			t.Fatalf("fill address limiter at %d: %v", i, err)
		}
	}

	if err = sendTestExternalMessage(t, node, body); !errors.Is(err, extmsg.ErrAddressRateLimited) {
		t.Fatalf("send external message error = %v, want address rate limited", err)
	}
	if len(admission.events) != 0 {
		t.Fatalf("admission events = %d, want 0 for an address-limited message", len(admission.events))
	}
	if got, ok := peer.localRebroadcastQueue.TryPop(); ok {
		t.Fatalf("address-limited external message was queued: %#v", got)
	}
}

// A message refused for broadcast capacity must not reach admission either, and
// its address-limit slot must be returned.
func TestSendExternalMessageCapacityRejectsBeforeAdmission(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission

	pacer, err := newExternalBroadcastPacer(ExternalBroadcastCapacityOptions{
		BytesPerSecond: 1,
		MaxDelay:       0,
	})
	if err != nil {
		t.Fatalf("new external broadcast pacer: %v", err)
	}
	node.externalBroadcastPacer = pacer

	first := testExternalMessageBOCWithBodyByte(t, 1)
	parsed, err := parseExternalMessageData(first)
	if err != nil {
		t.Fatalf("parse test external message: %v", err)
	}
	// Leave room for exactly two messages to the address.
	now := time.Now()
	for i := 0; i < extmsg.DefaultAddressLimit-2; i++ {
		if err = node.externalMessageLimiter.Add(parsed.address, now); err != nil {
			t.Fatalf("fill address limiter at %d: %v", i, err)
		}
	}

	if err = sendTestExternalMessage(t, node, first); err != nil {
		t.Fatalf("first send external message failed: %v", err)
	}
	if err = sendTestExternalMessage(t, node, testExternalMessageBOCWithBodyByte(t, 2)); !errors.Is(err, extmsg.ErrExternalBroadcastCapacityExceeded) {
		t.Fatalf("second send external message error = %v, want capacity exceeded", err)
	}
	if len(admission.events) != 1 {
		t.Fatalf("admission events = %d, want 1: the capacity-refused message must not be admitted", len(admission.events))
	}

	pacer.mx.Lock()
	pacer.nextAvailable = time.Time{}
	pacer.mx.Unlock()
	if err = sendTestExternalMessage(t, node, testExternalMessageBOCWithBodyByte(t, 3)); err != nil {
		t.Fatalf("third send external message failed, the refused message kept its address slot: %v", err)
	}
}

// A reservation whose message never reached a peer queue is returned, so the
// retry is not refused for capacity the failed attempt never used.
func TestSendExternalMessageQueueRefusalReturnsPacerReservation(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

	pacer, err := newExternalBroadcastPacer(ExternalBroadcastCapacityOptions{
		BytesPerSecond: 1,
		MaxDelay:       0,
	})
	if err != nil {
		t.Fatalf("new external broadcast pacer: %v", err)
	}
	node.externalBroadcastPacer = pacer

	for i := 0; i < peerRebroadcastQueueItems; i++ {
		if !peer.localRebroadcastQueue.Push(rebroadcastRequest{
			kind:    "tonNode.externalMessageBroadcast",
			payload: []byte{byte(i)},
			local:   true,
		}) {
			t.Fatalf("fill local rebroadcast queue at item %d", i)
		}
	}

	body := testExternalMessageBOC(t)
	if err = sendTestExternalMessage(t, node, body); err == nil {
		t.Fatalf("expected full local queue error")
	}

	for {
		if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
			break
		}
	}

	if err = sendTestExternalMessage(t, node, body); err != nil {
		t.Fatalf("retry send external message failed: %v", err)
	}
	if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatalf("expected retry to enqueue local rebroadcast")
	}
}

// Only the latest reservation is returned; one booked behind a canceled slot
// keeps the pacer from reusing that slot until it is itself canceled.
func TestExternalBroadcastPacerCancelReturnsOnlyTheLatestReservation(t *testing.T) {
	now := time.Unix(100, 0)
	pacer := &externalBroadcastPacer{
		bytesPerSecond: 1000,
		maxDelay:       time.Hour,
		now:            func() time.Time { return now },
	}

	firstAt, err := pacer.Reserve(1000)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	secondAt, err := pacer.Reserve(1000)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}

	pacer.Cancel(firstAt, 1000)
	if want := now.Add(2 * time.Second); !pacer.nextAvailable.Equal(want) {
		t.Fatalf("next available after canceling the earlier reservation = %s, want %s", pacer.nextAvailable, want)
	}

	pacer.Cancel(secondAt, 1000)
	if want := now.Add(time.Second); !pacer.nextAvailable.Equal(want) {
		t.Fatalf("next available after canceling the latest reservation = %s, want %s", pacer.nextAvailable, want)
	}
}

// A sender that gives up while waiting for its slot returns the slot: the next
// message is scheduled where the canceled one would have been.
func TestSendExternalMessageCanceledWaitReturnsPacerReservation(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

	pacer, err := newExternalBroadcastPacer(ExternalBroadcastCapacityOptions{
		BytesPerSecond: 1,
		MaxDelay:       time.Hour,
	})
	if err != nil {
		t.Fatalf("new external broadcast pacer: %v", err)
	}
	node.externalBroadcastPacer = pacer

	if err = sendTestExternalMessage(t, node, testExternalMessageBOCWithBodyByte(t, 1)); err != nil {
		t.Fatalf("first send external message failed: %v", err)
	}
	pacer.mx.Lock()
	firstEnd := pacer.nextAvailable
	pacer.mx.Unlock()

	body := testExternalMessageBOCWithBodyByte(t, 2)
	parsed, err := parseExternalMessageData(body)
	if err != nil {
		t.Fatalf("parse test external message: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = node.SendExternalMessage(ctx, body, parsed.message.DstAddr); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second send external message error = %v, want deadline exceeded", err)
	}

	pacer.mx.Lock()
	nextAvailable := pacer.nextAvailable
	pacer.mx.Unlock()
	if !nextAvailable.Equal(firstEnd) {
		t.Fatalf("pacer next available = %s, want the first reservation end %s", nextAvailable, firstEnd)
	}
	if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatal("expected the first external message to be queued")
	}
	if got, ok := peer.localRebroadcastQueue.TryPop(); ok {
		t.Fatalf("canceled external message was queued: %#v", got)
	}
}
