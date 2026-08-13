package p2p

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type publicBroadcastReceiverSnapshot struct {
	receivers map[string]*overlay.BroadcastReceiver
}

func (n *Node) newOverlaySubscription(spec overlaySpec) (*overlaySubscription, error) {
	return n.newOverlaySubscriptionWithPrivate(spec, nil)
}

func (n *Node) newPrivateOverlaySubscription(
	spec overlaySpec,
	runtime *privateOverlayRuntime,
) (*overlaySubscription, error) {
	if !spec.isPrivateOverlay() {
		return nil, fmt.Errorf("create private overlay subscription: invalid overlay kind")
	}
	return n.newOverlaySubscriptionWithPrivate(spec, runtime)
}

func (n *Node) newOverlaySubscriptionWithPrivate(
	spec overlaySpec,
	private *privateOverlayRuntime,
) (*overlaySubscription, error) {
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
		private:           private,
	}
	if private != nil {
		receiver.SetBroadcastHandlerWithInfo(sub.handlePrivateOverlayBroadcast)
		receiver.SetBroadcastPrecheckHandler(sub.precheckPrivateOverlayBroadcast)
	} else {
		receiver.SetBroadcastHandlerWithInfo(sub.handleReceivedBroadcast)
	}
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
	if spec.isPrivateOverlay() && spec.PrivateAllowLegacyBroadcasts {
		receiver.EnableBroadcastSimpleRelay(n.localID.Bytes(), overlayFECRelayPeerSet{sub: sub})
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
			s.noteDirectoryActivity(immediatePeerID, "")
		}
	} else {
		active = s.isActive()
	}
	kind := broadcastKindLabel(msg)
	s.node.noteBroadcast("received", s.spec.Name, kind, delivery)

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

func (s *overlaySubscription) precheckPrivateOverlayBroadcast(info overlay.BroadcastPrecheckInfo) error {
	if !s.isActive() || s.private == nil || !s.private.begin() {
		return ErrPrivateOverlayClosed
	}
	defer s.private.done()

	request, err := privateOverlayBroadcastPrecheck(info)
	if err != nil {
		return err
	}
	if !s.spec.PrivateAllowLegacyBroadcasts &&
		(request.Delivery == DeliverySimple || request.Delivery == DeliveryFEC) {
		return errors.New("legacy private overlay broadcasts are disabled")
	}
	callback := s.private.callbacks.BroadcastPrecheck
	if callback == nil {
		return nil
	}
	return callback(s.node.runCtx, request)
}

func (s *overlaySubscription) handlePrivateOverlayBroadcast(
	message tl.Serializable,
	info overlay.BroadcastInfo,
) overlay.BroadcastDisposition {
	if !s.isActive() || s.private == nil || !s.private.begin() {
		return overlay.BroadcastDispositionRetry
	}
	defer s.private.done()

	callback := s.private.callbacks.Broadcast
	if callback == nil {
		return overlay.BroadcastDispositionIgnore
	}
	broadcast, err := privateOverlayBroadcast(message, info)
	if err != nil {
		return overlay.BroadcastDispositionIgnore
	}
	if !s.spec.PrivateAllowLegacyBroadcasts &&
		(broadcast.Delivery == DeliverySimple || broadcast.Delivery == DeliveryFEC) {
		return overlay.BroadcastDispositionIgnore
	}
	switch callback(s.node.runCtx, broadcast) {
	case PrivateOverlayBroadcastAcceptAndRelay:
		return overlay.BroadcastDispositionAcceptAndRelay
	case PrivateOverlayBroadcastRetry:
		return overlay.BroadcastDispositionRetry
	default:
		return overlay.BroadcastDispositionIgnore
	}
}

func privateOverlayBroadcastPrecheck(
	info overlay.BroadcastPrecheckInfo,
) (PrivateOverlayBroadcastPrecheck, error) {
	source, err := NewPeerID(info.SourceID)
	if err != nil {
		return PrivateOverlayBroadcastPrecheck{}, fmt.Errorf("parse private overlay broadcast source: %w", err)
	}
	id, err := privateOverlayBroadcastID(info.BroadcastID)
	if err != nil {
		return PrivateOverlayBroadcastPrecheck{}, err
	}
	delivery, ok := broadcastDelivery(info.Delivery)
	if !ok {
		return PrivateOverlayBroadcastPrecheck{}, errors.New("unsupported private overlay broadcast delivery")
	}
	return PrivateOverlayBroadcastPrecheck{
		Source:           source,
		SourceKey:        info.SourceKey,
		SourceADNL:       info.SourceADNL,
		ImmediatePeer:    privateOverlayOptionalPeerID(info.ImmediatePeerID),
		ID:               id,
		Extra:            info.Extra,
		Delivery:         delivery,
		Trusted:          info.Trusted,
		SignatureChecked: info.SignatureChecked,
	}, nil
}

func privateOverlayBroadcast(
	message tl.Serializable,
	info overlay.BroadcastInfo,
) (PrivateOverlayBroadcast, error) {
	source, err := NewPeerID(info.SourceID)
	if err != nil {
		return PrivateOverlayBroadcast{}, fmt.Errorf("parse private overlay broadcast source: %w", err)
	}
	id, err := privateOverlayBroadcastID(info.BroadcastID)
	if err != nil {
		return PrivateOverlayBroadcast{}, err
	}
	delivery, ok := broadcastDelivery(info.Delivery)
	if !ok {
		return PrivateOverlayBroadcast{}, errors.New("unsupported private overlay broadcast delivery")
	}
	return PrivateOverlayBroadcast{
		Source:        source,
		SourceKey:     info.SourceKey,
		SourceADNL:    info.SourceADNL,
		ImmediatePeer: privateOverlayOptionalPeerID(info.ImmediatePeerID),
		ID:            id,
		Message:       message,
		Payload:       info.Payload,
		Extra:         info.Extra,
		Delivery:      delivery,
		Trusted:       info.Trusted,
	}, nil
}

func privateOverlayBroadcastID(raw []byte) ([32]byte, error) {
	if len(raw) != sha256.Size {
		return [32]byte{}, fmt.Errorf("private overlay broadcast id must be %d bytes", sha256.Size)
	}
	var id [sha256.Size]byte
	copy(id[:], raw)
	return id, nil
}

func privateOverlayOptionalPeerID(raw []byte) PeerID {
	id, _ := NewPeerID(raw)
	return id
}
