package p2p

import (
	"bytes"
	"context"
	"testing"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
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
	if err := node.SendExternalMessage(context.Background(), body); err == nil {
		t.Fatalf("expected full local queue error")
	}

	for {
		if _, ok := peer.localRebroadcastQueue.TryPop(); !ok {
			break
		}
	}

	if err := node.SendExternalMessage(context.Background(), body); err != nil {
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

	if err = node.SendExternalMessage(context.Background(), body); err != nil {
		t.Fatalf("send external message failed: %v", err)
	}
	if got, ok := peer.localRebroadcastQueue.TryPop(); !ok {
		t.Fatalf("expected local rebroadcast despite general broadcast dedupe")
	} else if got.kind != "tonNode.externalMessageBroadcast" || !got.local {
		t.Fatalf("unexpected queued rebroadcast: %#v", got)
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
	if err := node.SendExternalMessage(context.Background(), body); err != nil {
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
