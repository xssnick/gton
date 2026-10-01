package p2p

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func broadcastParticipationTestSubscription(t *testing.T, node *Node, workchain int32) *overlaySubscription {
	t.Helper()
	spec, err := buildOverlaySpec(make([]byte, 32), workchain, topShard, "broadcast participation")
	if err != nil {
		t.Fatal(err)
	}
	return mustGetOrCreateSubscription(t, node, spec)
}

func TestChainBroadcastPauseKeepsQueriesAndMembershipActive(t *testing.T) {
	node := newTestNode(t)
	sub := broadcastParticipationTestSubscription(t, node, -1)
	node.SetChainBroadcastsEnabled(false)
	if !sub.isActive() || sub.broadcastReceiver.IsActive() {
		t.Fatal("pause must disable broadcasts while keeping the overlay active")
	}
	for _, query := range []any{overlay.Ping{}, overlay.GetRandomPeers{}, GetCapabilities{}, GetArchiveInfo{}} {
		if _, err := sub.dispatchPeerQuery(t.Context(), query); err != nil {
			t.Fatalf("query %T during archive catch-up: %v", query, err)
		}
	}
	for _, enabled := range []bool{false, true} {
		node.SetChainBroadcastsEnabled(enabled)
		response, err := sub.dispatchPeerQuery(t.Context(), overlay.GetRandomPeersV2{})
		if err != nil {
			t.Fatal(err)
		}
		local := response.(overlay.NodesV2).Nodes[0]
		if (local.Flags&overlayMemberDoNotReceiveBroadcasts == 0) != enabled {
			t.Fatalf("enabled=%v: local flags=%d", enabled, local.Flags)
		}
		if err := local.CheckSignature(); err != nil {
			t.Fatalf("signed V2 flags: %v", err)
		}
	}
}

func TestChainBroadcastPauseSurvivesSubscriptionLifecycle(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	private, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID: []byte("extension broadcast participation"), Members: []PeerID{node.localID},
	}, PrivateOverlayCallbacks{})
	if err != nil {
		t.Fatal(err)
	}
	defer private.Close()

	sub := broadcastParticipationTestSubscription(t, node, -1)
	node.SetChainBroadcastsEnabled(false)
	if !private.sub.broadcastReceiver.IsActive() {
		t.Fatal("chain pause disabled extension-owned private broadcasts")
	}
	created := broadcastParticipationTestSubscription(t, node, 0)
	if created.broadcastReceiver.IsActive() {
		t.Fatal("new subscription enabled broadcasts while paused")
	}
	created.setActive(false, time.Now().Add(time.Minute))
	release, err := created.beginArchiveUse()
	if err != nil {
		t.Fatal(err)
	}
	if !created.isActive() || created.broadcastReceiver.IsActive() {
		t.Fatal("archive lease enabled broadcasts")
	}
	release()
	node.SetChainBroadcastsEnabled(true)
	if !sub.broadcastReceiver.IsActive() || created.broadcastReceiver.IsActive() {
		t.Fatal("resume did not preserve active/inactive shard state")
	}
	created.setActive(true, time.Time{})
	if !created.broadcastReceiver.IsActive() {
		t.Fatal("reactivated live overlay did not resume broadcasts")
	}
}

func TestChainBroadcastPauseSkipsADNLValidationAndResumesDelivery(t *testing.T) {
	node := newTestNode(t)
	sub := broadcastParticipationTestSubscription(t, node, -1)
	base := newTestOverlayADNL()
	base.id = testPeerID("broadcast transport").Bytes()
	wrapper := overlay.CreateExtendedADNL(base)
	wrapper.SetBroadcastReceiverResolver(node.resolvePublicBroadcastReceiver)
	attached, err := wrapper.AttachOverlay(sub.broadcastReceiver)
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Close()
	delivered := 0
	sub.broadcastReceiver.SetBroadcastHandlerWithInfo(func(tl.Serializable, overlay.BroadcastInfo) overlay.BroadcastDisposition {
		delivered++
		return overlay.BroadcastDispositionAcceptAndRelay
	})
	valid := signedTestSimpleBroadcast(t, IhrMessageBroadcast{Message: IhrMessage{Data: []byte{1}}})
	invalid := valid
	invalid.Signature = []byte{1}
	for _, attachedPeer := range []bool{true, false} {
		if !attachedPeer {
			attached.Close()
		}
		node.SetChainBroadcastsEnabled(false)
		if err := base.customHandler(&adnl.MessageCustom{Data: overlay.WrapMessage(sub.spec.ShortID, invalid)}); err != nil {
			t.Fatalf("paused ADNL validated broadcast: %v", err)
		}
		if delivered != 0 {
			t.Fatal("paused ADNL delivered a broadcast")
		}
	}
	node.SetChainBroadcastsEnabled(true)
	if err := base.customHandler(&adnl.MessageCustom{Data: overlay.WrapMessage(sub.spec.ShortID, valid)}); err != nil {
		t.Fatal(err)
	}
	if delivered != 1 {
		t.Fatal("resumed receiver did not deliver the broadcast")
	}
}

func TestChainBroadcastPauseSkipsPlumtreeBeforeDecode(t *testing.T) {
	node := newTestNode(t)
	sub := broadcastParticipationTestSubscription(t, node, -1)
	peer := &authenticatedQUICPeer{id: testPeerID("paused QUIC sender")}
	body := make([]byte, 4)
	binary.LittleEndian.PutUint32(body, broadcastPlumtreeSimpleConstructorID)
	wire, _ := sub.quicEnvelope.WrapMessageBody(body)
	node.SetChainBroadcastsEnabled(false)
	if err := node.handleQUICMessage(t.Context(), peer, wire); err != nil {
		t.Fatalf("paused QUIC parsed a broadcast: %v", err)
	}
	if err := sub.plumtree.HandleMessage(t.Context(), peer.id, wire, body); err != nil {
		t.Fatalf("paused Plumtree parsed a broadcast: %v", err)
	}
	answer, err := sub.plumtree.HandleRepairQuery(t.Context(), peer.id, RepairPlumtreePart{})
	if err != nil {
		t.Fatal(err)
	}
	var notFound BroadcastNotFound
	if _, err := tl.Parse(&notFound, answer, true); err != nil {
		t.Fatalf("paused repair did not return broadcastNotFound: %v", err)
	}
	if node.canAcceptBroadcast("tonNode.externalMessageBroadcast", true) {
		t.Fatal("local broadcasts bypassed archive pause")
	}
	if _, err := node.resolvePublicBroadcastReceiver(sub.spec.ShortID); !errors.Is(err, overlay.ErrBroadcastReceiverNotFound) {
		t.Fatalf("paused detached broadcast receiver: %v", err)
	}
	node.SetChainBroadcastsEnabled(true)
	if err := sub.plumtree.HandleMessage(t.Context(), peer.id, wire, body); err == nil {
		t.Fatal("resumed Plumtree did not validate the truncated payload")
	}
}

func TestChainBroadcastPauseConcurrentReactivation(t *testing.T) {
	node := newTestNode(t)
	sub := broadcastParticipationTestSubscription(t, node, -1)
	var workers sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		workers.Go(func() {
			for i := 0; i < 100; i++ {
				node.SetChainBroadcastsEnabled(i%2 == 0)
				sub.setActive(true, time.Time{})
				_ = sub.broadcastTargetsSnapshot()
			}
		})
	}
	workers.Wait()
	node.SetChainBroadcastsEnabled(false)
	if sub.broadcastReceiver.IsActive() {
		t.Fatal("concurrent reactivation lost the final broadcast pause")
	}
}
