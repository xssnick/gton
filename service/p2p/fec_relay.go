package p2p

import "github.com/xssnick/tonutils-go/adnl/overlay"

type overlayFECRelayPeerSet struct {
	sub *overlaySubscription
}

func (set overlayFECRelayPeerSet) Peers() []overlay.BroadcastPeer {
	if set.sub == nil {
		return nil
	}

	candidates := set.sub.rebroadcastCandidates()
	peers := make([]overlay.BroadcastPeer, 0, len(candidates))
	for _, peer := range candidates {
		if peer == nil || peer.overlay == nil {
			continue
		}
		peers = append(peers, peer.overlay)
	}
	return peers
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
		s.fecRelayState = overlay.NewBroadcastFECRelayState()
	}
	return s.fecRelayState
}
