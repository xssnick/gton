package p2p

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/tonutils-go/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSendExternalMessageRetryAfterFullQueuesRebroadcasts(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

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
	if err := sendTestExternalMessage(t, node, body); err == nil {
		t.Fatalf("expected full local queue error")
	}

	for {
		if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
			break
		}
	}

	if err := sendTestExternalMessage(t, node, body); err != nil {
		t.Fatalf("retry send external message failed: %v", err)
	}
	if got, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatalf("expected retry to enqueue local rebroadcast")
	} else if got.kind != "tonNode.externalMessageBroadcast" || !got.local {
		t.Fatalf("unexpected queued rebroadcast: %#v", got)
	}
}

func TestSendExternalMessageIgnoresBroadcastDeduper(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

	body := testExternalMessageBOC(t)
	payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: body},
	}, true)
	if err != nil {
		t.Fatalf("serialize external message broadcast: %v", err)
	}
	node.deduper.Mark(broadcastFingerprint(sub.spec.ShortID, payload), time.Now())

	if err = sendTestExternalMessage(t, node, body); err != nil {
		t.Fatalf("send external message failed: %v", err)
	}
	if got, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatalf("expected local rebroadcast despite general broadcast dedupe")
	} else if got.kind != "tonNode.externalMessageBroadcast" || !got.local {
		t.Fatalf("unexpected queued rebroadcast: %#v", got)
	}
}

func TestSendExternalMessageRunsAdmissionBeforeQueue(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

	admission := &testExternalMessageAdmission{}
	node.externalMessageAdmission = admission

	body := testExternalMessageBOC(t)
	if err := sendTestExternalMessage(t, node, body); err != nil {
		t.Fatalf("send external message failed: %v", err)
	}

	if len(admission.events) != 1 {
		t.Fatalf("admission events = %d, want 1", len(admission.events))
	}
	event := admission.events[0]
	if !bytes.Equal(event.Body, body) {
		t.Fatalf("admission body mismatch")
	}
	if event.Root != nil || event.Message != nil {
		t.Fatalf("send external message parsed data = (%v, %v), want nil", event.Root, event.Message)
	}
	if !event.IsLocal {
		t.Fatalf("send external message IsLocal = false, want true")
	}
	if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatalf("expected queued local rebroadcast after admission")
	}
}

func TestSendExternalMessageDropsWhenAdmissionFails(t *testing.T) {
	node, sub := newSendExternalMessageTestNode(t)
	peer := testRebroadcastQueuePeer("peer-a")
	sub.peers[peer.id] = peer

	wantErr := errors.New("reject external")
	node.externalMessageAdmission = &testExternalMessageAdmission{err: wantErr}

	if err := sendTestExternalMessage(t, node, testExternalMessageBOC(t)); !errors.Is(err, wantErr) {
		t.Fatalf("send external message error = %v, want %v", err, wantErr)
	}
	if _, ok := peer.localRebroadcastQueue.TryPop(); ok {
		t.Fatalf("external message was queued after admission failure")
	}
}

func TestSendExternalMessageRejectsOffline(t *testing.T) {
	node, _ := newSendExternalMessageTestNode(t)
	node.EnterOffline("test")

	if err := node.SendExternalMessage(context.Background(), []byte{1}, nil); !errors.Is(err, ErrOffline) {
		t.Fatalf("send external message error = %v, want %v", err, ErrOffline)
	}
}

func TestSendCheckedExternalMessageRunsCheckedAdmission(t *testing.T) {
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
	if err = node.SendCheckedExternalMessage(context.Background(), body, parsed.message.DstAddr, nil, nil); err != nil {
		t.Fatalf("send checked external message failed: %v", err)
	}
	if len(admission.events) != 1 {
		t.Fatalf("admission events = %d, want 1", len(admission.events))
	}
	event := admission.events[0]
	if !event.IsLocal {
		t.Fatalf("checked external message IsLocal = false, want true")
	}
	if event.Root != nil || event.Message != nil {
		t.Fatalf("checked external message parsed data = (%v, %v), want nil", event.Root, event.Message)
	}
	if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatalf("expected checked external message to be queued")
	}
}

func TestSendCheckedExternalMessageMarksAdmissionLocal(t *testing.T) {
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
	if err = node.SendCheckedExternalMessage(context.Background(), body, parsed.message.DstAddr, parsed.root, parsed.message); err != nil {
		t.Fatalf("send checked external message failed: %v", err)
	}

	if len(admission.events) != 1 {
		t.Fatalf("admission events = %d, want 1", len(admission.events))
	}
	event := admission.events[0]
	if !event.IsLocal {
		t.Fatalf("checked external message IsLocal = false, want true")
	}
	if event.Root != parsed.root || event.Message != parsed.message {
		t.Fatalf("checked external message parsed data mismatch")
	}
	if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatalf("expected checked external message to be queued")
	}
}

func TestSendExternalMessageCustomOverlayCanSkipPublic(t *testing.T) {
	node, publicSub := newSendExternalMessageTestNode(t)
	publicPeer := testRebroadcastQueuePeer("public-peer")
	publicSub.peers[publicPeer.id] = publicPeer

	customSpec := overlaySpec{
		Name:              "custom.private-a",
		Kind:              overlayKindCustomFixed,
		ShortID:           bytes.Repeat([]byte{0x22}, 32),
		MsgSenders:        map[PeerID]int{node.localID: 3},
		SkipPublicMsgSend: true,
	}
	customSub, _ := node.getOrCreateSubscription(customSpec)
	customSub.setActive(true, time.Time{})
	customPeer := testRebroadcastQueuePeer("custom-peer")
	customSub.peers[customPeer.id] = customPeer

	body := testExternalMessageBOC(t)
	if err := sendTestExternalMessage(t, node, body); err != nil {
		t.Fatalf("send external message failed: %v", err)
	}
	if got, ok := customSub.customTwoStepQueueStatusSnapshot(); !ok {
		t.Fatalf("expected custom two-step rebroadcast queue")
	} else if got.Items != 1 {
		t.Fatalf("custom two-step queued items = %d, want 1", got.Items)
	}
	if got, ok := customPeer.localRebroadcastQueue.TryPop(); ok {
		t.Fatalf("unexpected ordinary custom rebroadcast: %#v", got)
	}
	if got, ok := publicPeer.localRebroadcastQueue.TryPop(); ok {
		t.Fatalf("unexpected public rebroadcast: %#v", got)
	}
}

func TestSendExternalMessageRejectsWhenBroadcastCapacityExceeded(t *testing.T) {
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

	if err = sendTestExternalMessage(t, node, testExternalMessageBOCWithBodyByte(t, 1)); err != nil {
		t.Fatalf("first send external message failed: %v", err)
	}
	if err = sendTestExternalMessage(t, node, testExternalMessageBOCWithBodyByte(t, 2)); !errors.Is(err, extmsg.ErrExternalBroadcastCapacityExceeded) {
		t.Fatalf("second send external message error = %v, want capacity exceeded", err)
	}

	if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatal("expected first external message to be queued")
	}
	if got, ok := peer.localRebroadcastQueue.TryPop(); ok {
		t.Fatalf("capacity-rejected external message was queued: %#v", got)
	}
}

func TestExternalBroadcastPacerRejectsWhenDelayExceedsBudget(t *testing.T) {
	now := time.Unix(100, 0)
	pacer := &externalBroadcastPacer{
		bytesPerSecond: 1000,
		maxDelay:       time.Second,
		now:            func() time.Time { return now },
	}

	sendAt, err := pacer.reserve(now, 1000)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if !sendAt.Equal(now) {
		t.Fatalf("first send at = %s, want %s", sendAt, now)
	}

	sendAt, err = pacer.reserve(now, 1000)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if delay := sendAt.Sub(now); delay != time.Second {
		t.Fatalf("second delay = %s, want 1s", delay)
	}

	if _, err = pacer.reserve(now, 1); !errors.Is(err, extmsg.ErrExternalBroadcastCapacityExceeded) {
		t.Fatalf("third reserve error = %v, want capacity exceeded", err)
	}
}

func newSendExternalMessageTestNode(t *testing.T) (*Node, *overlaySubscription) {
	t.Helper()

	node := newTestNode(t)
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	node.runCtx = runCtx
	node.zeroStateFileHash = bytes.Repeat([]byte{0x11}, 32)

	spec, err := buildOverlaySpec(node.zeroStateFileHash, 0, topShard, overlayName(0, topShard))
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}
	sub, _ := node.getOrCreateSubscription(spec)
	sub.setActive(true, time.Time{})
	return node, sub
}

func sendTestExternalMessage(t *testing.T, node *Node, body []byte) error {
	t.Helper()

	parsed, err := parseExternalMessageData(body)
	if err != nil {
		t.Fatalf("parse test external message: %v", err)
	}
	return node.SendExternalMessage(context.Background(), body, parsed.message.DstAddr)
}

func testExternalMessageBOCWithBodyByte(t *testing.T, body byte) []byte {
	t.Helper()

	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111"),
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().MustStoreUInt(uint64(body), 8).EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	return root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
}
