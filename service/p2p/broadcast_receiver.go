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
	if spec.usesFastSyncRoster() && spec.FastSync == nil {
		return nil, fmt.Errorf(
			"create broadcast receiver for %s: missing fast sync spec",
			spec.Name,
		)
	}

	var memberCertificate *overlay.MemberCertificate
	if spec.FastSync != nil {
		memberCertificate = spec.FastSync.certificate
	}
	quicEnvelope, err := newQUICOverlayEnvelope(
		spec.ShortID,
		memberCertificate,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create QUIC envelope for %s: %w",
			spec.Name,
			err,
		)
	}

	receiver, err := overlay.NewBroadcastReceiver(
		spec.ShortID,
		spec.unauthorizedBroadcastLimit(),
		spec.relaysFECBroadcasts(),
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
		quicEnvelope:      quicEnvelope,
	}
	receiver.SetBroadcastHandlerWithInfo(sub.handleReceivedBroadcast)
	if len(spec.AuthorizedKeys) > 0 {
		receiver.SetAuthorizedKeys(spec.AuthorizedKeys)
	}

	if spec.usesFastSyncRoster() {
		sub.fastSync, err = newFastSyncOverlayRuntime(
			n,
			spec.ShortID,
			*spec.FastSync,
			quicEnvelope,
		)
		if err != nil {
			receiver.Close()
			return nil, fmt.Errorf(
				"create FastSync runtime for %s: %w",
				spec.Name,
				err,
			)
		}
	}

	if spec.authorizesBroadcastSenders() {
		receiver.SetBroadcastPrecheckHandler(sub.checkCustomTwoStepBroadcastSource)
	}
	if spec.usesTwoStepDelivery() {
		receiver.EnableBroadcastTwoStep(n.localID.Bytes(), twoStepPeerSet{sub: sub})
	}
	if spec.usesPlumtree() {
		sub.plumtree, err = newPlumtreeRuntime(sub)
		if err != nil {
			receiver.Close()
			return nil, fmt.Errorf("create Plumtree runtime for %s: %w", spec.Name, err)
		}
	}
	if spec.relaysFECBroadcasts() {
		receiver.SetFECBroadcastLimits(publicBroadcastFECMaxActiveStreams, overlay.DefaultFECBroadcastMaxActiveBytes)
		receiver.EnableBroadcastFECRelay(n.localID.Bytes(), overlayFECRelayPeerSet{sub: sub})
	}
	return sub, nil
}

func (n *Node) publishPublicBroadcastReceiversLocked() {
	receivers := make(map[string]*overlay.BroadcastReceiver, len(n.subscriptions))
	for key, sub := range n.subscriptions {
		if !sub.spec.servesPublicIngress() {
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
			// A peer feeding us broadcasts without holding an attachment is as
			// alive as one that does; keep its directory row warm so it stays
			// advertised and promotable.
			s.noteDirectoryActivity(immediatePeerID, nil, "")
		}
	} else {
		active = s.isActive()
	}
	direction := "received_unlisted"
	if immediatePeer != nil {
		direction = "received_roster"
	}
	kind := broadcastKindLabel(msg)
	s.node.noteBroadcast(direction, s.spec.Name, kind, delivery)

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
