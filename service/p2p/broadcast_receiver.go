package p2p

import (
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type publicBroadcastReceiverSnapshot struct {
	receivers map[string]*overlay.BroadcastReceiver
}

func (n *Node) newOverlaySubscription(spec overlaySpec) (*overlaySubscription, error) {
	receiver, err := overlay.NewBroadcastReceiver(
		spec.ShortID,
		maxOverlayPayloadSize,
		spec.Kind != overlayKindCustomFixed,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("create broadcast receiver for %s: %w", spec.Name, err)
	}

	sub := &overlaySubscription{
		node:              n,
		spec:              spec,
		log:               n.log.With().Str("overlay", spec.Name).Logger(),
		peers:             map[PeerID]*overlayPeer{},
		peerNotify:        make(chan struct{}, 1),
		broadcastReceiver: receiver,
	}
	receiver.SetBroadcastHandlerWithInfo(sub.handleReceivedBroadcast)
	if len(spec.AuthorizedKeys) > 0 {
		receiver.SetAuthorizedKeys(spec.AuthorizedKeys)
	}

	if spec.Kind == overlayKindCustomFixed {
		receiver.SetBroadcastPrecheckHandler(sub.checkCustomTwoStepBroadcastSource)
		receiver.EnableBroadcastTwoStep(n.localID.Bytes(), customTwoStepPeerSet{sub: sub})
	} else {
		receiver.SetFECBroadcastLimits(publicBroadcastFECMaxActiveStreams, overlay.DefaultFECBroadcastMaxActiveBytes)
		receiver.EnableBroadcastFECRelay(n.localID.Bytes(), overlayFECRelayPeerSet{sub: sub})
	}
	return sub, nil
}

func (n *Node) publishPublicBroadcastReceiversLocked() {
	receivers := make(map[string]*overlay.BroadcastReceiver, len(n.subscriptions))
	for key, sub := range n.subscriptions {
		if sub.spec.Kind != overlayKindPublicShard {
			continue
		}
		receivers[key] = sub.broadcastReceiver
	}
	n.publicBroadcastReceivers.Store(&publicBroadcastReceiverSnapshot{receivers: receivers})
}

func (n *Node) resolvePublicBroadcastReceiver(id []byte) (*overlay.BroadcastReceiver, error) {
	snapshot := n.publicBroadcastReceivers.Load()
	if snapshot == nil {
		return nil, overlay.ErrBroadcastReceiverNotFound
	}

	receiver := snapshot.receivers[string(id)]
	if receiver == nil || !receiver.IsActive() {
		return nil, overlay.ErrBroadcastReceiverNotFound
	}
	return receiver, nil
}

func (s *overlaySubscription) handleReceivedBroadcast(msg tl.Serializable, info overlay.BroadcastInfo) overlay.BroadcastDisposition {
	delivery, ok := broadcastDelivery(info.Delivery)
	if !ok {
		s.node.noteBroadcastDrop(s.spec.Name, broadcastKindLabel(msg), "invalid_delivery")
		return overlay.BroadcastDispositionIgnore
	}

	var immediatePeer *overlayPeer
	var active bool
	immediatePeerID, immediatePeerErr := NewPeerID(info.ImmediatePeerID)
	if immediatePeerErr == nil {
		immediatePeer, active = s.rosterPeerIfActive(immediatePeerID)
		if immediatePeer == nil {
			s.node.pool.touchByID(immediatePeerID, time.Now())
		}
	} else {
		active = s.isActive()
	}
	direction := "received_unlisted"
	if immediatePeer != nil {
		direction = "received_roster"
	}
	kind := broadcastKindLabel(msg)
	s.node.noteBroadcast(direction, s.spec.Name, kind)

	sourcePeerID, err := NewPeerID(info.SourceID)
	if err != nil {
		s.node.noteBroadcastDrop(s.spec.Name, kind, "invalid_source")
		return overlay.BroadcastDispositionIgnore
	}
	if info.DecodeTime > 0 {
		s.node.observeBroadcastPipelineStageDuration(
			broadcastPipelineStageFECDecode,
			kind,
			delivery,
			broadcastPipelineResultSuccess,
			info.DecodeTime,
		)
	}
	if !active {
		return overlay.BroadcastDispositionRetry
	}

	return s.handleOverlayBroadcastPayload(
		immediatePeer,
		msg,
		newKnownIdentifiedBroadcastPayload(info.Payload, info.BroadcastID),
		delivery,
		info.Trusted,
		sourcePeerID,
	)
}

func broadcastDelivery(delivery overlay.BroadcastDelivery) (Delivery, bool) {
	switch delivery {
	case overlay.BroadcastDeliverySimple:
		return DeliverySimple, true
	case overlay.BroadcastDeliveryFEC:
		return DeliveryFEC, true
	case overlay.BroadcastDeliveryTwoStepSimple, overlay.BroadcastDeliveryTwoStepFEC:
		return DeliveryTwoStep, true
	default:
		return "", false
	}
}
