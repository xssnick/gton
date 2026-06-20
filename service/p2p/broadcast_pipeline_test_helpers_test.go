package p2p

func newSerializedBroadcastPayload(msg any) *broadcastPayload {
	return &broadcastPayload{msg: msg}
}

func (s *overlaySubscription) handleOverlayBroadcast(peer *overlayPeer, msg any, delivery Delivery, trusted bool, sourcePeerID PeerID) error {
	return s.handleOverlayBroadcastPayload(peer, msg, newSerializedBroadcastPayload(msg), delivery, trusted, sourcePeerID)
}

func (s *overlaySubscription) classifyBroadcast(peer *overlayPeer, msg any, payload []byte, delivery Delivery, trusted bool, sourcePeerID PeerID) *acceptedBroadcast {
	if len(payload) == 0 {
		return nil
	}
	accepted, _ := s.classifyBroadcastPayload(peer, msg, newKnownBroadcastPayload(payload), delivery, trusted, sourcePeerID)
	return accepted
}
