package p2p

import "github.com/xssnick/tonutils-go/adnl/overlay"

func newSerializedBroadcastPayload(msg any) *broadcastPayload {
	return &broadcastPayload{msg: msg}
}

func newKnownBroadcastPayload(payload []byte) *broadcastPayload {
	return newKnownIdentifiedBroadcastPayload(payload, payload)
}

func newIdentifiedBroadcastPayload(msg any, identity []byte) *broadcastPayload {
	return &broadcastPayload{
		msg:      msg,
		identity: identity,
	}
}

func (s *overlaySubscription) handleOverlayBroadcast(peer *overlayPeer, msg any, delivery Delivery, trusted bool, sourcePeerID PeerID) error {
	disposition := s.handleOverlayBroadcastPayload(peer, msg, newSerializedBroadcastPayload(msg), delivery, trusted, sourcePeerID)
	if disposition == overlay.BroadcastDispositionAcceptAndRelay {
		return nil
	}
	return overlay.ErrBroadcastRejected
}

func (s *overlaySubscription) classifyBroadcast(peer *overlayPeer, msg any, payload []byte, delivery Delivery, trusted bool, sourcePeerID PeerID) *acceptedBroadcast {
	if len(payload) == 0 {
		return nil
	}
	result, _ := s.classifyBroadcastPayload(peer, msg, newKnownBroadcastPayload(payload), delivery, trusted, sourcePeerID)
	if result.disposition == broadcastDispositionIgnore {
		return nil
	}
	return &result.accepted
}
