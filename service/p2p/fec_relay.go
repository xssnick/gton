package p2p

import "github.com/xssnick/tonutils-go/adnl/overlay"

const publicBroadcastFECMaxActiveStreams = 4096

type overlayFECRelayPeerSet struct {
	sub *overlaySubscription
}

func (set overlayFECRelayPeerSet) Peers() []overlay.BroadcastPeer {
	if set.sub == nil {
		return nil
	}
	// Called for every received FEC part; the relay skips source/completed
	// peers itself, so the shared cached snapshot is returned as is.
	return set.sub.broadcastPeersSnapshot()
}

func (s *overlaySubscription) configureBroadcastFECRelay(peer *overlayPeer) {
	if !s.broadcastFECRelayEnabled() || peer == nil || peer.overlay == nil {
		return
	}

	peer.overlay.EnableBroadcastFECRelay(s.node.localID.Bytes(), overlayFECRelayPeerSet{sub: s}, s.broadcastFECRelayState())
}

func (s *overlaySubscription) broadcastFECRelayEnabled() bool {
	return s.spec.Kind != overlayKindCustomFixed
}

func (s *overlaySubscription) broadcastFECRelayState() *overlay.BroadcastFECRelayState {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.fecRelayState == nil {
		state := overlay.NewBroadcastFECRelayState()
		state.SetLimits(publicBroadcastFECMaxActiveStreams, overlay.DefaultFECBroadcastMaxActiveBytes)
		s.fecRelayState = state
	}
	return s.fecRelayState
}
