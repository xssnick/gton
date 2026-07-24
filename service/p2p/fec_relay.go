package p2p

import "github.com/xssnick/tonutils-go/adnl/overlay"

const publicBroadcastFECMaxActiveStreams = 4096

type overlayFECRelayPeerSet struct {
	sub *overlaySubscription
}

func (set overlayFECRelayPeerSet) Peers() []overlay.BroadcastPeer {
	// Called for every received FEC part; the relay skips source/completed
	// peers itself, so the shared cached snapshot is returned as is.
	return set.sub.broadcastTargetsSnapshot().broadcast
}

func (s *overlaySubscription) broadcastFECRelayEnabled() bool {
	return s.spec.Kind != overlayKindCustomFixed
}
